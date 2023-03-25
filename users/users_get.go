// SPDX-License-Identifier: ice License 1.0

package users

import (
	"context"
	"fmt"
	"strings"
	stdlibtime "time"

	"github.com/pkg/errors"

	"github.com/ice-blockchain/go-tarantool-client"
	"github.com/ice-blockchain/wintr/log"
	"github.com/ice-blockchain/wintr/time"
)

func (r *repository) getUserByID(ctx context.Context, id UserID) (*User, error) {
	if ctx.Err() != nil {
		return nil, errors.Wrap(ctx.Err(), "get user failed because context failed")
	}
	result := new(User)
	if err := r.db.GetTyped("USERS", "pk_unnamed_USERS_3", tarantool.StringKey{S: id}, result); err != nil {
		return nil, errors.Wrapf(err, "failed to get user by id %v", id)
	}
	if result.ID == "" {
		return nil, ErrNotFound
	}

	return result, nil
}

func (r *repository) GetUserByID(ctx context.Context, userID string) (*UserProfile, error) { //nolint:revive,funlen // Its fine.
	if ctx.Err() != nil {
		return nil, errors.Wrap(ctx.Err(), "get user failed because context failed")
	}
	if userID != requestingUserID(ctx) {
		return r.getOtherUserByID(ctx, userID)
	}
	sql := `
	SELECT  u.*,
			count(distinct t1.id) AS total_t1_referral_count,
			count(t2.id) AS total_t2_referral_count
	FROM users u 
			LEFT JOIN USERS t1
                	ON t1.referred_by = u.id
					AND t1.id != u.id
					AND t1.referred_by != t1.id
					AND t1.username != t1.id
						LEFT JOIN USERS t2
								ON t2.referred_by = t1.id
								AND t2.id != t1.id
								AND t2.referred_by != t2.id
								AND t2.username != t2.id
	WHERE u.id = :userId`
	var rows []*UserProfile
	if err := r.db.PrepareExecuteTyped(sql, map[string]any{"userId": userID}, &rows); err != nil {
		return nil, errors.Wrapf(err, "failed to select user by id %v", userID)
	}
	if len(rows) == 0 || rows[0].ID == "" { //nolint:revive // False negative.
		return nil, errors.Wrapf(ErrNotFound, "no user found with id %v", userID)
	}
	res := rows[0]
	r.sanitizeUser(res.User).sanitizeForUI()

	return res, nil
}

func (r *repository) getOtherUserByID(ctx context.Context, userID string) (*UserProfile, error) { //nolint:funlen // Better to be in one place.
	if ctx.Err() != nil {
		return nil, errors.Wrap(ctx.Err(), "get user failed because context failed")
	}
	usr, err := r.getUserByID(ctx, userID)
	if err != nil {
		return nil, err
	}
	*usr = User{
		HiddenProfileElements: usr.HiddenProfileElements,
		PublicUserInformation: usr.PublicUserInformation,
	}
	referralCountNeeded := true
	if usr.HiddenProfileElements != nil {
		for _, element := range *usr.HiddenProfileElements {
			if element == ReferralCountHiddenProfileElement {
				referralCountNeeded = false

				break
			}
		}
	}
	if !referralCountNeeded {
		resp := new(UserProfile)
		resp.User = r.sanitizeUser(usr)

		return resp, nil
	}

	sql := `SELECT  u.id,
					count(distinct t1.id) AS total_t1_referral_count,
					count(t2.id) AS total_t2_referral_count
			FROM users u 
					LEFT JOIN USERS t1
							ON t1.referred_by = u.id
							AND t1.id != u.id
							AND t1.referred_by != t1.id
							AND t1.username != t1.id
								LEFT JOIN USERS t2
										ON t2.referred_by = t1.id
										AND t2.id != t1.id
										AND t2.username != t2.id
										AND t2.referred_by != t2.id
			WHERE u.id = :userId`
	var result []*struct {
		ID              string
		T1ReferralCount uint64
		T2ReferralCount uint64
	}
	if err = r.db.PrepareExecuteTyped(sql, map[string]any{"userId": userID}, &result); err != nil {
		return nil, errors.Wrapf(err, "failed to select referralCount for user by id %v", userID)
	}
	if len(result) == 0 || result[0].ID == "" { //nolint:revive // False negative.
		return nil, errors.Wrapf(ErrNotFound, "no user found with id %v", userID)
	}
	resp := new(UserProfile)
	dbRes := result[0]
	resp.T1ReferralCount = &dbRes.T1ReferralCount
	resp.T2ReferralCount = &dbRes.T2ReferralCount
	resp.User = r.sanitizeUser(usr)

	return resp, nil
}

func (r *repository) GetUserByUsername(ctx context.Context, username string) (*UserProfile, error) {
	if ctx.Err() != nil {
		return nil, errors.Wrap(ctx.Err(), "get user failed because context failed")
	}
	result := new(User)
	if err := r.db.GetTyped("USERS", "unique_unnamed_USERS_4", tarantool.StringKey{S: username}, result); err != nil {
		return nil, errors.Wrapf(err, "failed to get user by username %v", username)
	}
	if result.ID == "" {
		return nil, errors.Wrapf(ErrNotFound, "no user found with username %v", username)
	}
	resp := new(UserProfile)
	resp.User = new(User)
	resp.PublicUserInformation = result.PublicUserInformation
	r.sanitizeUser(resp.User).sanitizeForUI()

	return resp, nil
}

//nolint:funlen // Big sql.
func (r *repository) GetUsers(ctx context.Context, keyword string, limit, offset uint64) (result []*MinimalUserProfile, err error) {
	if ctx.Err() != nil {
		return nil, errors.Wrap(ctx.Err(), "get users failed because context failed")
	}
	before2 := time.Now()
	defer func() {
		if elapsed := stdlibtime.Since(*before2.Time); elapsed > 100*stdlibtime.Millisecond {
			log.Info(fmt.Sprintf("[response]GetUsers took: %v", elapsed))
		}
	}()
	sql := fmt.Sprintf(`
			SELECT COALESCE(u.last_mining_ended_at,1)                                                              AS last_mining_ended_at,
				   (CASE
						WHEN user_requesting_this.id != u.id AND (u.referred_by = user_requesting_this.id OR u.id = user_requesting_this.referred_by)
							THEN (CASE 
									WHEN COALESCE(u.last_mining_ended_at,0) < :nowNanos 
									    THEN COALESCE(u.last_ping_cooldown_ended_at,1) 
								   	ELSE :nowNanos 
							      END)
						ELSE null
					END)                                                                                             AS last_ping_cooldown_ended_at,
				   (CASE
						WHEN user_requesting_this.id = u.id 
								OR (
									NULLIF(u.phone_number_hash,'') IS NOT NULL
									AND 
									POSITION(u.phone_number_hash, user_requesting_this.agenda_phone_number_hashes) > 0
								   )
							THEN u.phone_number
						ELSE ''
				    END) 																							 AS phone_number_,
				   ''                                                                                              	 AS email,
				   u.id                                                                                              AS id,
				   u.username                                                                                        AS username,
				   %v                                                                   							 AS profile_picture_url,
				   u.country 																						 AS country,
				   '' 																								 AS city,
				   (CASE
						WHEN NULLIF(u.phone_number_hash,'') IS NOT NULL
				  				AND user_requesting_this.id != u.id
								AND POSITION(u.phone_number_hash, user_requesting_this.agenda_phone_number_hashes) > 0
							THEN 'CONTACTS'
						WHEN u.id = user_requesting_this.referred_by OR u.referred_by = user_requesting_this.id 
							THEN 'T1'
						WHEN t0.referred_by = user_requesting_this.id and t0.id != t0.referred_by
							THEN 'T2'
						ELSE ''
					END)                                                                                             AS referral_type
			FROM users u
					 JOIN USERS t0
						  ON t0.id = u.referred_by
						 AND t0.referred_by != t0.id
						 AND t0.username != t0.id
					 JOIN users user_requesting_this
						  ON user_requesting_this.id = :userId
						 AND user_requesting_this.username != user_requesting_this.id
						 AND user_requesting_this.referred_by != user_requesting_this.id
			WHERE (
					(u.username != u.id AND u.username LIKE :keyword ESCAPE '\')
					OR
					(u.first_name IS NOT NULL AND LOWER(u.first_name) LIKE :keyword ESCAPE '\')
					OR
					(u.last_name IS NOT NULL AND LOWER(u.last_name) LIKE :keyword ESCAPE '\')
				  ) 
				  AND referral_type != '' AND u.username != u.id AND u.referred_by != u.id
			ORDER BY
				u.id = user_requesting_this.referred_by DESC,
				(phone_number_ != '' AND phone_number_ != null) DESC,
				t0.id = user_requesting_this.id DESC,
				t0.referred_by = user_requesting_this.id DESC,
				u.username DESC
			LIMIT %v OFFSET :offset`, r.pictureClient.SQLAliasDownloadURL(`u.profile_picture_name`), limit)
	params := map[string]any{
		"keyword":  fmt.Sprintf("%v%%", strings.ReplaceAll(strings.ReplaceAll(strings.ToLower(keyword), "_", "\\_"), "%", "\\%")),
		"offset":   offset,
		"nowNanos": time.Now(),
		"userId":   requestingUserID(ctx),
	}
	err = r.db.PrepareExecuteTyped(sql, params, &result)

	return result, errors.Wrapf(err, "failed to select for users by %#v", params)
}
