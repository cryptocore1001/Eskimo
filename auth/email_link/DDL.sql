-- SPDX-License-Identifier: ice License 1.0

CREATE TABLE IF NOT EXISTS email_link_sign_ins (
           created_at                             timestamp NOT NULL,
           token_issued_at                        timestamp,
           blocked_until                          timestamp,
           email_confirmed_at                     timestamp,
           issued_token_seq                       BIGINT DEFAULT 0 NOT NULL,
           previously_issued_token_seq            BIGINT DEFAULT 0 NOT NULL,
           confirmation_code_wrong_attempts_count BIGINT DEFAULT 0 NOT NULL,
           email                                  TEXT NOT NULL,
           confirmation_code                      TEXT,
           user_id                                TEXT,
           phone_number_to_email_migration_user_id TEXT,
           device_unique_id                       TEXT,
           primary key(email, device_unique_id))
           WITH (FILLFACTOR = 70);
CREATE INDEX IF NOT EXISTS email_link_sign_ins_user_id_ix ON email_link_sign_ins (user_id);

CREATE TABLE IF NOT EXISTS account_metadata (
           user_id                                TEXT PRIMARY KEY,
           metadata                               JSONB NOT NULL)
           WITH (FILLFACTOR = 70);

ALTER TABLE email_link_sign_ins
    ADD COLUMN IF NOT EXISTS previously_issued_token_seq BIGINT DEFAULT 0 NOT NULL;

ALTER TABLE email_link_sign_ins
    ADD COLUMN IF NOT EXISTS phone_number_to_email_migration_user_id TEXT;

CREATE TABLE IF NOT EXISTS sign_ins_per_ip (
       login_session_number  BIGINT DEFAULT 0 NOT NULL,
       login_attempts        BIGINT DEFAULT 0 NOT NULL CONSTRAINT sign_ins_per_ip_login_attempts_count CHECK (login_attempts <= 10),
       ip                    TEXT NOT NULL,
       PRIMARY KEY (login_session_number, ip)
);