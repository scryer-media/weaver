// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

pub use weaver_server_auth::auth::repository::*;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Database;
    use crate::auth::encode_jwt_secret;
    use crate::settings::persistence::SettingsPersistenceQueries;
    use crate::settings::queries::SettingsQueries;

    fn browser_session(token_hash: &str, created_at: i64) -> BrowserSession {
        BrowserSession {
            token_hash: token_hash.to_string(),
            csrf_verifier: format!("csrf-{token_hash}"),
            origin: "http://localhost".to_string(),
            client_ip: Some("127.0.0.1".to_string()),
            remembered: true,
            created_at,
            expires_at: created_at + 3_600,
            revoked_at: None,
        }
    }

    #[test]
    fn jwt_signing_secret_is_persisted() {
        let db = Database::open_in_memory().unwrap();
        let first = db.get_or_create_jwt_signing_secret().unwrap();
        let second = db.get_or_create_jwt_signing_secret().unwrap();
        assert_eq!(first, second);

        let stored = db
            .get_setting(JWT_SIGNING_SECRET_SETTING_KEY)
            .unwrap()
            .unwrap();
        assert!(crate::persistence::encryption::is_encrypted(&stored));
        assert_eq!(
            decode_stored_jwt_signing_secret(&stored, db.encryption_key()).unwrap(),
            first
        );
    }

    #[test]
    fn malformed_jwt_signing_secret_fails_closed() {
        let db = Database::open_in_memory().unwrap();
        db.set_setting(JWT_SIGNING_SECRET_SETTING_KEY, "not-hex")
            .unwrap();
        let err = db.get_or_create_jwt_signing_secret().unwrap_err();
        assert!(err.to_string().contains("invalid JWT signing secret"));
    }

    #[test]
    fn encrypted_jwt_signing_secret_with_wrong_key_is_rotated() {
        let db = Database::open_in_memory().unwrap();
        let stale_secret = [7u8; 32];
        let wrong_key = crate::persistence::encryption::EncryptionKey::generate();
        let stale_stored = crate::persistence::encryption::encrypt_value(
            &wrong_key,
            &encode_jwt_secret(&stale_secret),
        )
        .unwrap();
        db.set_setting(JWT_SIGNING_SECRET_SETTING_KEY, &stale_stored)
            .unwrap();

        let rotated = db.get_or_create_jwt_signing_secret().unwrap();
        assert_ne!(rotated, stale_secret);

        let stored = db
            .get_setting(JWT_SIGNING_SECRET_SETTING_KEY)
            .unwrap()
            .unwrap();
        assert!(crate::persistence::encryption::is_encrypted(&stored));
        assert_eq!(
            decode_stored_jwt_signing_secret(&stored, db.encryption_key()).unwrap(),
            rotated
        );
    }

    #[test]
    fn jwt_signing_secret_rotation_replaces_persisted_value() {
        let db = Database::open_in_memory().unwrap();
        let first = db.get_or_create_jwt_signing_secret().unwrap();
        let rotated = db.rotate_jwt_signing_secret().unwrap();
        assert_ne!(first, rotated);
        assert_eq!(db.get_or_create_jwt_signing_secret().unwrap(), rotated);
    }

    #[test]
    fn plaintext_jwt_signing_secret_is_reencrypted() {
        let db = Database::open_in_memory().unwrap();
        let secret = [11u8; 32];
        db.set_setting(JWT_SIGNING_SECRET_SETTING_KEY, &encode_jwt_secret(&secret))
            .unwrap();

        assert_eq!(db.get_or_create_jwt_signing_secret().unwrap(), secret);
        let stored = db
            .get_setting(JWT_SIGNING_SECRET_SETTING_KEY)
            .unwrap()
            .unwrap();
        assert!(crate::persistence::encryption::is_encrypted(&stored));
    }

    #[test]
    fn password_verification_is_session_bound_and_revocation_is_live() {
        let db = Database::open_in_memory().unwrap();
        db.create_browser_session(&browser_session("first", 1_000))
            .unwrap();
        db.create_browser_session(&browser_session("second", 1_000))
            .unwrap();

        db.verify_browser_session_password("first", 1_100).unwrap();
        assert_eq!(
            db.browser_session_password_verified_at("first", 1_101)
                .unwrap(),
            Some(1_100)
        );
        assert_eq!(
            db.browser_session_password_verified_at("second", 1_101)
                .unwrap(),
            Some(1_000)
        );

        db.revoke_all_browser_sessions(1_102).unwrap();
        assert_eq!(
            db.browser_session_password_verified_at("first", 1_103)
                .unwrap(),
            None
        );
        assert_eq!(
            db.browser_session_password_verified_at("second", 1_103)
                .unwrap(),
            None
        );
    }
}
