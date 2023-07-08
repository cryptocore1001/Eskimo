// SPDX-License-Identifier: ice License 1.0

package emaillinkiceauth

import (
	"context"

	"github.com/pkg/errors"

	"github.com/ice-blockchain/wintr/connectors/storage/v2"
)

func (c *client) Status(ctx context.Context, loginSession string) (tokens *Tokens, emailConfirmed bool, err error) {
	var token loginFlowToken
	if err = parseJwtToken(loginSession, c.cfg.LoginSession.JwtSecret, &token); err != nil {
		return nil, false, errors.Wrapf(err, "can't parse login session:%v", loginSession)
	}
	id := loginID{Email: token.Subject, DeviceUniqueID: token.DeviceUniqueID}
	els, err := c.getConfirmedEmailLinkSignIn(ctx, &id, token.ConfirmationCode)
	if err != nil {
		if storage.IsErr(err, storage.ErrNotFound) {
			return nil, false, errors.Wrapf(ErrNoPendingLoginSession, "no pending login session:%v,id:%#v", loginSession, id)
		}

		return nil, false, errors.Wrapf(err, "failed to get confirmed email link sign in for loginSession:%v,id:%#v", loginSession, id)
	}
	if els.UserID == nil || els.OTP != *els.UserID {
		return nil, false, errors.Wrapf(ErrStatusNotVerified, "not verified for id:%#v", id)
	}
	if els.ConfirmationCode == *els.UserID {
		return nil, false, errors.Wrapf(ErrNoPendingLoginSession, "tokens already provided for id:%#v", id)
	}
	tokens, err = c.generateTokens(els.TokenIssuedAt, els, els.IssuedTokenSeq)
	if err != nil {
		return nil, false, errors.Wrapf(err, "can't generate tokens for id:%#v", id)
	}
	if rErr := c.resetLoginSession(ctx, &id, els, token.ConfirmationCode); rErr != nil {
		return nil, false, errors.Wrapf(rErr, "can't reset login session for id:%#v", id)
	}
	emailConfirmed = els.EmailConfirmedAt != nil

	return tokens, emailConfirmed, nil
}

func (c *client) resetLoginSession(ctx context.Context, id *loginID, els *emailLinkSignIn, prevConfirmationCode string) error {
	sql := `UPDATE email_link_sign_ins
				   	  SET confirmation_code = $1
				WHERE email = $2
					  AND device_unique_id = $3
					  AND otp = $4
					  AND confirmation_code = $5
					  AND issued_token_seq = $6`
	_, err := storage.Exec(ctx, c.db, sql, els.UserID, id.Email, id.DeviceUniqueID, els.OTP, prevConfirmationCode, els.IssuedTokenSeq)

	return errors.Wrapf(err, "failed to reset login session by id:%#v and confirmationCode:%v", id, prevConfirmationCode)
}
