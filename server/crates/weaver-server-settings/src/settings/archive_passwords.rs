// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::io::Read;

use weaver_server_db::persistence::encryption::{decrypt_value, encrypt_value, is_encrypted};
use weaver_server_db::persistence::sql_runtime::{SqlArg, SqlRuntime};
use weaver_server_db::persistence::{StateError, connection::Database, error::LoadError};
use weaver_server_jobs::jobs::model::{ArchivePasswordCandidate, ArchivePasswordSource};

pub const PASSWORDS: &str = "archive_passwords";
const PASSWORD_FILE: &str = "archive_password_file";
const MAX_PASSWORD_BYTES: usize = 64 * 1024;
const MAX_FILE_BYTES: u64 = 1024 * 1024;
// A job's archive-password candidate list is four buckets in this order, each
// with its own cap, so no job is ever offered more than 80 candidates: every
// candidate costs a key derivation at every archive gate the job reaches.
// - integration, 10: the job's own password first, then the passwords a
//   calling integration handed over beside it;
// - header, 10: `X-DNZB-Password` values from weaver's own NZB fetch, in the
//   order received;
// - NZB, 10: the NZB's `<meta type="password">` entries in document order,
//   then its file name's `{{password}}`;
// - global, 50: the Settings list, then the password file. The Settings list
//   refuses to save past it and the password file fills what the list leaves.
// The buckets are de-duplicated by value across the list, the earliest keeping
// its label, and a value dropped as a repeat does not take a slot.
pub const MAX_INTEGRATION_ARCHIVE_PASSWORDS: usize = 10;
pub const MAX_HEADER_ARCHIVE_PASSWORDS: usize = 10;
pub const MAX_NZB_ARCHIVE_PASSWORDS: usize = 10;
pub const MAX_ARCHIVE_PASSWORDS: usize = 50;

pub trait SettingsArchivePasswordsQueries {
    fn archive_password_settings(&self) -> Result<(bool, Option<String>), StateError>;
    fn save_archive_password_settings(
        &self,
        passwords: Option<Vec<String>>,
        password_file: Option<Option<String>>,
    ) -> Result<(), StateError>;
}

impl SettingsArchivePasswordsQueries for Database {
    fn archive_password_settings(&self) -> Result<(bool, Option<String>), StateError> {
        let (passwords, file) = self.load_archive_password_settings()?;
        Ok((!passwords.is_empty(), file))
    }

    fn save_archive_password_settings(
        &self,
        passwords: Option<Vec<String>>,
        password_file: Option<Option<String>>,
    ) -> Result<(), StateError> {
        let sealed = passwords
            .map(|passwords| {
                let passwords = ordered_passwords(passwords);
                if passwords.len() > MAX_ARCHIVE_PASSWORDS {
                    return Err(StateError::Conflict(format!(
                        "archive password list holds more than {MAX_ARCHIVE_PASSWORDS} entries"
                    )));
                }
                let value = serde_json::to_string(&passwords)
                    .map_err(|_| StateError::Conflict("invalid archive passwords".into()))?;
                if value.len() > MAX_PASSWORD_BYTES {
                    return Err(StateError::Conflict(
                        "archive password list is too large".into(),
                    ));
                }
                let key = self
                    .encryption_key()
                    .ok_or_else(|| StateError::Conflict("encryption key required".into()))?;
                encrypt_value(key, &value)
                    .map_err(|_| StateError::Conflict("could not encrypt archive passwords".into()))
            })
            .transpose()?;
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "save_archive_password_settings", |tx| {
                let sealed = sealed.clone();
                let password_file = password_file.clone();
                Box::pin(async move {
                    for (key, value) in [(PASSWORDS, sealed), (PASSWORD_FILE, password_file.map(|file| file.unwrap_or_default()))] {
                        if let Some(value) = value {
                            tx.execute(
                                "INSERT INTO settings (key, value) VALUES ({}, {}) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                                &[SqlArg::Text(key.into()), SqlArg::Text(value)],
                            ).await?;
                        }
                    }
                    Ok(())
                })
            }).await
        })
    }
}

trait SettingsArchivePasswordsLocalQueries {
    fn load_archive_password_settings(&self) -> Result<(Vec<String>, Option<String>), LoadError>;
}

impl SettingsArchivePasswordsLocalQueries for Database {
    // The global list and the password file's path. A failed read, or a list
    // that does not decrypt, is `Read`. A value this build cannot take, a
    // decrypted list included, is `Rows`.
    fn load_archive_password_settings(&self) -> Result<(Vec<String>, Option<String>), LoadError> {
        let datastore = self.datastore();
        let rows = self.run_sql_blocking_read(async move {
            SqlRuntime::fetch_all(datastore.read_exec(),
                "SELECT key, value FROM settings WHERE key IN ('archive_passwords', 'archive_password_file')", &[]).await
        }).map_err(LoadError::Read)?;
        let typed = || -> Result<_, StateError> {
            let (mut sealed, mut file) = (None, None);
            for row in &rows {
                match row.text("key")?.as_str() {
                    PASSWORDS => sealed = Some(row.text("value")?),
                    PASSWORD_FILE => file = Some(row.text("value")?),
                    _ => {}
                }
            }
            Ok((sealed, file))
        };
        let (sealed, file) = typed().map_err(LoadError::Rows)?;
        let passwords = match sealed {
            None => Vec::new(),
            Some(sealed) => {
                let unreadable =
                    |message: &str| LoadError::Read(StateError::Conflict(message.into()));
                let key = self
                    .encryption_key()
                    .ok_or_else(|| unreadable("encryption key required"))?;
                if !is_encrypted(&sealed) {
                    return Err(unreadable("archive passwords are not encrypted"));
                }
                let value = decrypt_value(key, &sealed)
                    .map_err(|_| unreadable("could not decrypt archive passwords"))?;
                serde_json::from_str(&value).map_err(|_| {
                    LoadError::Rows(StateError::Conflict("invalid archive passwords".into()))
                })?
            }
        };
        Ok((passwords, file.filter(|file| !file.is_empty())))
    }
}

pub trait SettingsArchivePasswordsInternalQueries {
    fn global_archive_password_candidates(
        &self,
    ) -> Result<Vec<ArchivePasswordCandidate>, LoadError>;
}

impl SettingsArchivePasswordsInternalQueries for Database {
    fn global_archive_password_candidates(
        &self,
    ) -> Result<Vec<ArchivePasswordCandidate>, LoadError> {
        // Read past the global cap by the three job buckets' caps, so the
        // harvest still fills its global bucket after dropping the values a job
        // bucket already holds; the harvest applies `MAX_ARCHIVE_PASSWORDS`.
        let keep = MAX_ARCHIVE_PASSWORDS
            + MAX_INTEGRATION_ARCHIVE_PASSWORDS
            + MAX_HEADER_ARCHIVE_PASSWORDS
            + MAX_NZB_ARCHIVE_PASSWORDS;
        let (passwords, file) = self.load_archive_password_settings()?;
        let mut candidates: Vec<_> = passwords
            .into_iter()
            .take(keep)
            .map(|password| {
                ArchivePasswordCandidate::new(ArchivePasswordSource::Settings, password)
            })
            .collect();
        if let Some(file) = file {
            match read_password_file(&file) {
                Ok(passwords) => {
                    for password in passwords {
                        if candidates.len() >= keep {
                            break;
                        }
                        if !candidates
                            .iter()
                            .any(|candidate| candidate.value() == password)
                        {
                            candidates.push(ArchivePasswordCandidate::new(
                                ArchivePasswordSource::PasswordFile,
                                password,
                            ));
                        }
                    }
                }
                Err(_) => tracing::warn!("could not read archive password file"),
            }
        }
        Ok(candidates)
    }
}

fn ordered_passwords(passwords: Vec<String>) -> Vec<String> {
    let mut result = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for password in passwords {
        if !password.trim().is_empty() && seen.insert(password.clone()) {
            result.push(password);
        }
    }
    result
}

fn read_password_file(path: &str) -> std::io::Result<Vec<String>> {
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NONBLOCK);
    }
    let file = options.open(path)?;
    if !file.metadata()?.is_file() {
        return Err(std::io::Error::other(
            "password file must be a regular file",
        ));
    }
    let mut contents = String::new();
    file.take(MAX_FILE_BYTES + 1)
        .read_to_string(&mut contents)?;
    if contents.len() as u64 > MAX_FILE_BYTES {
        return Err(std::io::Error::other("password file is too large"));
    }
    Ok(ordered_passwords(
        contents.lines().map(str::to_owned).collect(),
    ))
}
