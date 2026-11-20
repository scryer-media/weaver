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
fn persisted_jwt_secret_is_encrypted_state_and_rejects_the_wrong_key() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = crate::Database::open(&directory.path().join("weaver.db")).unwrap();
    let persisted_key = EncryptionKey::generate();
    db.set_encryption_key(persisted_key.clone());
    let persisted_secret = db.get_or_create_jwt_signing_secret().unwrap();

    assert!(db.has_encrypted_credentials().unwrap());
    db.validate_encrypted_credentials(&persisted_key).unwrap();

    let wrong_key = EncryptionKey::generate();
    assert!(db.validate_encrypted_credentials(&wrong_key).is_err());
    assert_eq!(
        db.get_or_create_jwt_signing_secret().unwrap(),
        persisted_secret,
        "validation must not rotate persisted JWT state"
    );
}
