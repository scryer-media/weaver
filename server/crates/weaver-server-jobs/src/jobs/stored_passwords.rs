// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A job's archive passwords: sealed as they are stored, opened as they
// are read.

use weaver_server_db::persistence::StateError;

// Submission values are always plaintext, even when they resemble our stored
// envelope prefix. Encrypt them once at the database boundary.
pub fn encrypt_archive_password(
    key: Option<&weaver_server_db::persistence::encryption::EncryptionKey>,
    password: Option<&str>,
) -> Result<Option<String>, StateError> {
    let Some(password) = password else {
        return Ok(None);
    };
    if password.is_empty() {
        return Ok(Some(String::new()));
    }
    let key = key.ok_or_else(|| {
        StateError::Database("encryption key is required to store archive passwords".into())
    })?;
    weaver_server_db::persistence::encryption::encrypt_value(key, password)
        .map(Some)
        .map_err(StateError::Database)
}

// A job's supplied passwords from their stored form. A value that does not
// decrypt is `Read`. A decrypted value this build cannot parse, or a source
// it does not have, is `Rows`.
pub fn decrypt_supplied_passwords(
    key: Option<&weaver_server_db::persistence::encryption::EncryptionKey>,
    stored: Option<String>,
) -> Result<
    Vec<crate::jobs::ArchivePasswordCandidate>,
    weaver_server_db::persistence::error::LoadError,
> {
    use weaver_server_db::persistence::error::LoadError;
    let json = match decrypt_archive_password(key, stored).map_err(LoadError::Read)? {
        Some(json) if !json.is_empty() => json,
        _ => return Ok(Vec::new()),
    };
    let pairs: Vec<(String, String)> = serde_json::from_str(&json).map_err(|error| {
        LoadError::Rows(StateError::Database(format!(
            "invalid archive passwords: {error}"
        )))
    })?;
    pairs
        .into_iter()
        .map(|(label, value)| {
            crate::jobs::ArchivePasswordSource::from_label(&label)
                .map(|source| crate::jobs::ArchivePasswordCandidate::new(source, value))
                .ok_or_else(|| {
                    LoadError::Rows(StateError::Database(format!(
                        "unknown archive password source {label}"
                    )))
                })
        })
        .collect()
}

pub fn decrypt_archive_password(
    key: Option<&weaver_server_db::persistence::encryption::EncryptionKey>,
    password: Option<String>,
) -> Result<Option<String>, StateError> {
    let Some(password) = password else {
        return Ok(None);
    };
    if !weaver_server_db::persistence::encryption::is_encrypted(&password) {
        return Ok(Some(password));
    }
    let key = key.ok_or_else(|| {
        StateError::Database("encryption key is required to restore archive passwords".into())
    })?;
    weaver_server_db::persistence::encryption::decrypt_value(key, &password)
        .map(Some)
        .map_err(StateError::Database)
}
