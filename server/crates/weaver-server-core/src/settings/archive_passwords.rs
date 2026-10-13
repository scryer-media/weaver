use std::io::Read;

use crate::jobs::model::{ArchivePasswordCandidate, ArchivePasswordSource};
use crate::persistence::encryption::{decrypt_value, encrypt_value, is_encrypted};
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};
use crate::persistence::{Database, StateError};

const PASSWORDS: &str = "archive_passwords";
const PASSWORD_FILE: &str = "archive_password_file";
const MAX_PASSWORD_BYTES: usize = 64 * 1024;
const MAX_FILE_BYTES: u64 = 1024 * 1024;
// The most global archive passwords any consumer is handed, Settings list and
// password file together. Every candidate an archive gate tries costs a key
// derivation, so this is the one place the list is bounded: the Settings list
// refuses to save past it, and the password file fills whatever the Settings
// list leaves. Every consumer, direct store and finalize alike, tries the
// whole bounded list.
pub(crate) const MAX_ARCHIVE_PASSWORDS: usize = 50;

impl Database {
    pub fn archive_password_settings(&self) -> Result<(bool, Option<String>), StateError> {
        let (passwords, file) = self.load_archive_password_settings()?;
        Ok((!passwords.is_empty(), file))
    }

    pub fn save_archive_password_settings(
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

    fn load_archive_password_settings(&self) -> Result<(Vec<String>, Option<String>), StateError> {
        let datastore = self.datastore();
        let (sealed, file) = self.run_sql_blocking_read(async move {
            let rows = SqlRuntime::fetch_all(datastore.read_exec(),
                "SELECT key, value FROM settings WHERE key IN ('archive_passwords', 'archive_password_file')", &[]).await?;
            let (mut sealed, mut file) = (None, None);
            for row in rows {
                match row.text("key")?.as_str() {
                    PASSWORDS => sealed = Some(row.text("value")?),
                    PASSWORD_FILE => file = Some(row.text("value")?),
                    _ => {}
                }
            }
            Ok((sealed, file))
        })?;
        let passwords = match sealed {
            None => Vec::new(),
            Some(sealed) => {
                let key = self
                    .encryption_key()
                    .ok_or_else(|| StateError::Conflict("encryption key required".into()))?;
                if !is_encrypted(&sealed) {
                    return Err(StateError::Conflict(
                        "archive passwords are not encrypted".into(),
                    ));
                }
                let value = decrypt_value(key, &sealed).map_err(|_| {
                    StateError::Conflict("could not decrypt archive passwords".into())
                })?;
                serde_json::from_str(&value)
                    .map_err(|_| StateError::Conflict("invalid archive passwords".into()))?
            }
        };
        Ok((passwords, file.filter(|file| !file.is_empty())))
    }

    pub(crate) fn global_archive_password_candidates(
        &self,
    ) -> Result<Vec<ArchivePasswordCandidate>, StateError> {
        let (passwords, file) = self.load_archive_password_settings()?;
        let mut candidates: Vec<_> = passwords
            .into_iter()
            .take(MAX_ARCHIVE_PASSWORDS)
            .map(|password| {
                ArchivePasswordCandidate::new(ArchivePasswordSource::Settings, password)
            })
            .collect();
        if let Some(file) = file {
            match read_password_file(&file) {
                Ok(passwords) => {
                    for password in passwords {
                        if candidates.len() >= MAX_ARCHIVE_PASSWORDS {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn archive_passwords_are_encrypted_and_files_refresh() {
        let db = Database::open_in_memory().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("passwords.txt");
        std::fs::write(&path, " synthetic \r\nfile\nfile\n").unwrap();
        db.save_archive_password_settings(
            Some(vec!["enc:v1:literal".into(), " synthetic ".into()]),
            Some(Some(path.to_str().unwrap().into())),
        )
        .unwrap();
        let stored = db.get_setting(PASSWORDS).unwrap().unwrap();
        assert!(is_encrypted(&stored));
        assert!(!stored.contains("synthetic"));
        let candidates = db.global_archive_password_candidates().unwrap();
        assert_eq!(
            candidates
                .iter()
                .map(|candidate| candidate.value())
                .collect::<Vec<_>>(),
            ["enc:v1:literal", " synthetic ", "file"]
        );
        assert!(!format!("{candidates:?}").contains("synthetic"));
        std::fs::write(&path, "replacement\n").unwrap();
        assert_eq!(
            db.global_archive_password_candidates()
                .unwrap()
                .last()
                .unwrap()
                .value(),
            "replacement"
        );
        std::fs::remove_file(&path).unwrap();
        assert_eq!(db.global_archive_password_candidates().unwrap().len(), 2);
        db.save_archive_password_settings(Some(vec![]), Some(None))
            .unwrap();
        assert_eq!(db.archive_password_settings().unwrap(), (false, None));
    }

    #[test]
    fn archive_passwords_are_bounded_at_fifty_across_both_sources() {
        let db = Database::open_in_memory().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("passwords.txt");
        let numbered = |prefix: &str, count: usize| -> Vec<String> {
            (0..count)
                .map(|index| format!("{prefix}-{index}"))
                .collect()
        };

        let too_many = numbered("settings", MAX_ARCHIVE_PASSWORDS + 1);
        let error = db
            .save_archive_password_settings(Some(too_many), None)
            .unwrap_err();
        assert!(matches!(error, StateError::Conflict(_)));

        // Duplicates and blanks are dropped before the count is taken.
        let mut padded = numbered("settings", MAX_ARCHIVE_PASSWORDS);
        padded.push("settings-0".into());
        padded.push("   ".into());
        db.save_archive_password_settings(Some(padded), None)
            .unwrap();
        assert_eq!(
            db.global_archive_password_candidates().unwrap().len(),
            MAX_ARCHIVE_PASSWORDS
        );

        // The file fills what the Settings list leaves, and no more.
        std::fs::write(&path, numbered("file", 40).join("\n")).unwrap();
        db.save_archive_password_settings(
            Some(numbered("settings", 30)),
            Some(Some(path.to_str().unwrap().into())),
        )
        .unwrap();
        let candidates = db.global_archive_password_candidates().unwrap();
        assert_eq!(candidates.len(), MAX_ARCHIVE_PASSWORDS);
        assert_eq!(
            candidates
                .iter()
                .filter(|candidate| candidate.source() == ArchivePasswordSource::PasswordFile)
                .count(),
            MAX_ARCHIVE_PASSWORDS - 30
        );
        assert_eq!(candidates.last().unwrap().value(), "file-19");
    }
}
