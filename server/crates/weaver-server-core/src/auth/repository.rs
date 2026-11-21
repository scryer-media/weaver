// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

pub use weaver_server_auth::auth::repository::*;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Database;

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
    fn signing_keys_need_the_master_key() {
        let directory = tempfile::tempdir().unwrap();
        let mut db = Database::open(&directory.path().join("keys.db")).unwrap();
        assert!(db.encryption_key().is_none());
        assert!(matches!(
            crate::auth::SigningKeys::for_database(&db),
            Err(crate::auth::SigningKeyError::MissingMasterKey)
        ));

        let key = crate::persistence::encryption::EncryptionKey::generate();
        let expected = crate::auth::SigningKeys::derive(&key).unwrap();
        db.set_encryption_key(key);
        assert_eq!(
            crate::auth::SigningKeys::for_database(&db).unwrap(),
            expected
        );
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
