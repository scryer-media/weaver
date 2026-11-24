// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::operations::recovery::credentials::StoredCredentialQueries;
use crate::servers::persistence::ServersPersistenceQueries;
use weaver_server_auth::auth::repository::AuthRepositoryQueries;

#[test]
fn persisted_credentials_validate_the_selected_key_before_startup() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = crate::Database::open(&directory.path().join("weaver.db")).unwrap();
    let persisted_key = EncryptionKey::generate();
    db.set_encryption_key(persisted_key.clone());
    db.insert_server(&crate::servers::ServerConfig {
        id: 1,
        host: "news.example.invalid".to_string(),
        port: 119,
        tls: false,
        username: Some("weaver".to_string()),
        password: Some("persisted-secret".to_string()),
        connections: 1,
        active: true,
        supports_pipelining: false,
        pipelining_depth: None,
        tls_name_mismatch_certificate_der: None,
        priority: 0,
        backfill: false,
        retention_days: 0,
        max_download_speed: 0,
        download_quota: Default::default(),
        tls_ca_cert: None,
    })
    .unwrap();

    assert!(db.has_encrypted_credentials().unwrap());
    db.validate_encrypted_credentials(&persisted_key).unwrap();

    let wrong_key = EncryptionKey::generate();
    let error = db.validate_encrypted_credentials(&wrong_key).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("cannot decrypt persisted server credential 1")
    );
}

#[test]
fn signing_keys_are_derived_and_hold_no_encrypted_state() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = crate::Database::open(&directory.path().join("weaver.db")).unwrap();
    let persisted_key = EncryptionKey::generate();
    db.set_encryption_key(persisted_key.clone());
    db.set_auth_credentials("admin", "$argon2id$hash").unwrap();
    let keys = weaver_server_auth::auth::SigningKeys::for_database(&db).unwrap();

    // Login and script-run keys are derived, never stored, so they give the
    // key check nothing to decrypt.
    assert!(!db.has_encrypted_credentials().unwrap());
    db.validate_encrypted_credentials(&EncryptionKey::generate())
        .unwrap();
    assert_eq!(
        weaver_server_auth::auth::SigningKeys::derive(&persisted_key).unwrap(),
        keys
    );
}
