// SPDX-License-Identifier: ice License 1.0

package users

import (
	"context"
	"sync"

	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"

	"github.com/ice-blockchain/wintr/connectors/storage"
)

func (r *repository) DeleteUser(ctx context.Context, userID UserID) error {
	if ctx.Err() != nil {
		return errors.Wrap(ctx.Err(), "context failed")
	}
	gUser, err := r.getUserByID(ctx, userID)
	if err != nil {
		return errors.Wrapf(err, "failed to get user for userID:%v", userID)
	}
	if err = r.deleteUser(ctx, gUser); err != nil {
		return errors.Wrapf(err, "failed to deleteUser for:%#v", gUser)
	}
	u := &UserSnapshot{Before: r.sanitizeUser(gUser)}
	if err = r.sendUserSnapshotMessage(ctx, u); err != nil {
		return errors.Wrapf(err, "failed to send deleted user message for %#v", u)
	}
	if err = r.sendTombstonedUserMessage(ctx, userID); err != nil {
		return errors.Wrapf(err, "failed to sendTombstonedUserMessage for userID:%v", userID)
	}

	return nil
}

func (r *repository) deleteUser(ctx context.Context, usr *User) error { //nolint:revive // .
	if ctx.Err() != nil {
		return errors.Wrap(ctx.Err(), "delete user failed because context failed")
	}
	if err := r.deleteUserReferences(ctx, usr.ID); err != nil {
		return errors.Wrapf(err, "failed to deleteUserReferences for userID:%v", usr.ID)
	}
	if err := r.updateReferredByForAllT1Referrals(ctx, usr.ID); err != nil {
		for err != nil && (errors.Is(err, storage.ErrRelationNotFound) || errors.Is(err, storage.ErrNotFound)) {
			err = r.updateReferredByForAllT1Referrals(ctx, usr.ID)
		}
		if err != nil {
			return errors.Wrapf(err, "failed to update referredBy for all t1 referrals of userID:%v", usr.ID)
		}
	}
	gUser, err := r.getUserByID(ctx, usr.ID)
	if err != nil {
		return errors.Wrapf(err, "failed to get user for userID:%v", usr.ID)
	}
	*usr = *gUser
	sql := `DELETE FROM users WHERE id = :user_id`
	args := map[string]any{"user_id": usr.ID}
	if err = storage.CheckSQLDMLErr(r.db.PrepareExecute(sql, args)); err != nil {
		if errors.Is(err, storage.ErrRelationNotFound) {
			return r.deleteUser(ctx, usr)
		}

		return errors.Wrapf(err, "failed to delete user with id %v", usr.ID)
	}

	return nil
}

func (r *repository) deleteUserReferences(ctx context.Context, userID UserID) error {
	if ctx.Err() != nil {
		return errors.Wrap(ctx.Err(), "delete user failed because context failed")
	}
	wg := new(sync.WaitGroup)
	wg.Add(1)
	errChan := make(chan error, 1)
	go func() {
		defer wg.Done()
		errChan <- errors.Wrapf(r.DeleteAllDeviceMetadata(ctx, userID), "failed to DeleteAllDeviceMetadata for userID:%v", userID)
	}()
	wg.Wait()
	close(errChan)
	errs := make([]error, 0, 1)
	for err := range errChan {
		errs = append(errs, err)
	}

	return multierror.Append(nil, errs...).ErrorOrNil() //nolint:wrapcheck // Not needed.
}

//nolint:funlen // It's better to isolate everything together to decrease complexity; and it has some SQL, so...
func (r *repository) updateReferredByForAllT1Referrals(ctx context.Context, userID UserID) error {
	if ctx.Err() != nil {
		return errors.Wrap(ctx.Err(), "context failed")
	}
	sql := `SELECT (	SELECT X.ID 
						FROM (	SELECT X.ID 
								FROM (  SELECT r.id 
										FROM users r
										WHERE 1=1
											  AND r.id != :user_id 
											  AND r.id != u.id 
											  AND r.referred_by != u.id 
											  AND r.referred_by != r.id 
											  AND r.username != r.id 
											  AND r.referred_by != :user_id
										ORDER BY RANDOM() 
										LIMIT 1
									 ) X
			
								UNION ALL 
								 
								SELECT u.id AS ID
							  ) X
						LIMIT 1
				   ) new_referred_by,
				   u.*
			FROM users u
			WHERE u.referred_by = :user_id
			  AND u.id != :user_id`
	var resp []*struct {
		NewReferredBy UserID
		User
	}
	if err := r.db.PrepareExecuteTyped(sql, map[string]any{"user_id": userID}, &resp); err != nil {
		return errors.Wrapf(err, "failed to select for all t1 referrals of userID:%v + their new random referralID", userID)
	}

	wg := new(sync.WaitGroup)
	wg.Add(len(resp))
	errChan := make(chan error, len(resp))
	for ii := range resp {
		go func(ix int) {
			defer wg.Done()
			errChan <- errors.Wrapf(r.updateReferredBy(ctx, &resp[ix].User, resp[ix].NewReferredBy, true),
				"failed to update referred by for userID:%v", resp[ix].User.ID)
		}(ii)
	}
	wg.Wait()
	close(errChan)
	errs := make([]error, 0, len(resp))
	for err := range errChan {
		errs = append(errs, err)
	}

	return errors.Wrap(multierror.Append(nil, errs...).ErrorOrNil(), "failed to update referred by for some/all of user's t1 referrals")
}

func (r *repository) deleteUserTracking(ctx context.Context, usr *UserSnapshot) error {
	if usr.Before != nil && usr.User == nil {
		return errors.Wrapf(r.trackingClient.DeleteUser(ctx, usr.Before.ID), "failed to delete tracking data for userID:%v", usr.Before.ID)
	}

	return nil
}
