// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::jobs::JobsArchivePasswordInternalQueries;
use crate::settings::queries::SettingsQueries;
use crate::{Database, StateError};
use weaver_server_auth::auth::repository::AuthRepositoryInternalQueries;

// The start-up credential reads of the queue. Each predicate is the one a
// partial index of migration 90 is built on, so the reads walk only the jobs
// that hold the credential.
pub(crate) const ACTIVE_JOB_PASSWORDS_SQL: &str =
    "SELECT job_id AS id, password FROM active_jobs WHERE password IS NOT NULL";
pub(crate) const ACTIVE_JOB_PASSWORD_CANDIDATES_SQL: &str = "SELECT job_id AS id, password_candidates AS password FROM active_jobs WHERE password_candidates IS NOT NULL";

// Each column the sealing pass seals: what it holds, whether its table has
// a row that holds one, and one stored value of it. A seen item does not
// count: its id is a digest no key is needed for, and a link that does not
// decrypt reads as none, so a key lost never refuses a start for one.
const SEALED_COLUMNS: [(&str, &str, &str); 3] = [
    (
        "torrent metainfo",
        "SELECT 1 AS held FROM torrents WHERE metainfo IS NOT NULL LIMIT 1",
        "SELECT metainfo AS sample FROM torrents WHERE metainfo IS NOT NULL LIMIT 1",
    ),
    (
        "torrent tracker URL",
        "SELECT 1 AS held FROM torrent_trackers LIMIT 1",
        "SELECT url AS sample FROM torrent_trackers LIMIT 1",
    ),
    (
        "RSS feed URL",
        "SELECT 1 AS held FROM rss_feeds WHERE url <> '' LIMIT 1",
        "SELECT url AS sample FROM rss_feeds WHERE url <> '' LIMIT 1",
    ),
];

pub trait StoredCredentialQueries {
    fn has_encrypted_credentials(&self) -> Result<bool, StateError>;
    fn validate_encrypted_credentials(
        &self,
        key: &crate::persistence::encryption::EncryptionKey,
    ) -> Result<(), StateError>;
    fn migrate_plaintext_credentials(&self) -> Result<(), StateError>;
}

impl StoredCredentialQueries for Database {
    // Return whether any persisted credential requires the encryption key.
    // The active-job reads are served by the partial indexes of migration
    // 90, so they walk only the jobs with a password, not the whole queue.
    // This check intentionally reads raw storage before a key is installed so
    // startup can refuse to replace a missing key over encrypted state.
    fn has_encrypted_credentials(&self) -> Result<bool, StateError> {
        use crate::persistence::encryption::is_encrypted;
        use crate::persistence::sql_runtime::SqlRuntime;

        if self.automatic_backup_ciphertext()?.is_some()
            || !self.validated_archive_password_ciphertexts()?.is_empty()
        {
            return Ok(true);
        }
        let datastore = self.datastore();
        let encrypted_credentials_exist = self.run_sql_blocking_read(async move {
            for query in [
                "SELECT password FROM servers WHERE password IS NOT NULL",
                "SELECT password FROM rss_feeds WHERE password IS NOT NULL",
                "SELECT password FROM proxy_profiles WHERE password IS NOT NULL",
                ACTIVE_JOB_PASSWORDS_SQL,
                ACTIVE_JOB_PASSWORD_CANDIDATES_SQL,
                "SELECT source_password AS password FROM semantic_duplicate_candidates WHERE source_password IS NOT NULL",
                "SELECT value AS password FROM secrets",
                "SELECT value AS password FROM settings WHERE key = 'archive_passwords'",
                "SELECT sealed_value AS password FROM script_instance_inputs WHERE sealed_value IS NOT NULL",
            ] {
                let rows = SqlRuntime::fetch_all(datastore.read_exec(), query, &[]).await?;
                for row in rows {
                    if is_encrypted(&row.text("password")?) {
                        return Ok(true);
                    }
                }
            }
            Ok(false)
        })?;
        if encrypted_credentials_exist {
            return Ok(true);
        }
        if self.has_sealed_secrets()? {
            return Ok(true);
        }
        self.has_encrypted_persisted_jwt_signing_secret()
    }

    // Prove that every encrypted persisted credential can be authenticated by
    // the selected key before startup exposes any decrypted configuration.
    fn validate_encrypted_credentials(
        &self,
        key: &crate::persistence::encryption::EncryptionKey,
    ) -> Result<(), StateError> {
        use crate::persistence::encryption::{decrypt_value, is_encrypted};
        use crate::persistence::sql_runtime::SqlRuntime;

        for ciphertext in self.validated_archive_password_ciphertexts()? {
            if !is_encrypted(&ciphertext) {
                return Err(StateError::Conflict(
                    "validated archive password is not encrypted".into(),
                ));
            }
            decrypt_value(key, &ciphertext).map_err(|_| {
                StateError::Conflict("cannot decrypt validated archive password".into())
            })?;
        }
        if let Some(ciphertext) = self.automatic_backup_ciphertext()? {
            if !is_encrypted(&ciphertext) {
                return Err(StateError::Conflict(
                    "persisted automatic backup key is not encrypted".into(),
                ));
            }
            decrypt_value(key, &ciphertext).map_err(|error| {
                StateError::Conflict(format!(
                    "cannot decrypt persisted automatic backup key: {error}"
                ))
            })?;
        }
        self.validate_sealed_secrets(key)?;
        let datastore = self.datastore();
        let credential_key = key.clone();
        self.run_sql_blocking_read(async move {
            for (kind, query) in [
                (
                    "server",
                    "SELECT id, password FROM servers WHERE password IS NOT NULL",
                ),
                (
                    "RSS feed",
                    "SELECT id, password FROM rss_feeds WHERE password IS NOT NULL",
                ),
                (
                    "proxy",
                    "SELECT id, password FROM proxy_profiles WHERE password IS NOT NULL",
                ),
                (
                    "archive job",
                    ACTIVE_JOB_PASSWORDS_SQL,
                ),
                (
                    "archive job candidates",
                    ACTIVE_JOB_PASSWORD_CANDIDATES_SQL,
                ),
                (
                    "archive candidate",
                    "SELECT job_id AS id, source_password AS password FROM semantic_duplicate_candidates WHERE source_password IS NOT NULL",
                ),
            ] {
                let rows = SqlRuntime::fetch_all(datastore.read_exec(), query, &[]).await?;
                for row in rows {
                    let id = row.i64("id")?;
                    let password = row.text("password")?;
                    if !is_encrypted(&password) {
                        continue;
                    }
                    decrypt_value(&credential_key, &password).map_err(|error| {
                        StateError::Conflict(format!(
                            "cannot decrypt persisted {kind} credential {id}: {error}"
                        ))
                    })?;
                }
            }
            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT id, name, value FROM secrets UNION ALL SELECT 'archive_passwords' AS id, 'archive passwords' AS name, value FROM settings WHERE key = 'archive_passwords'",
                &[],
            )
            .await?;
            for row in rows {
                let value = row.text("value")?;
                if !is_encrypted(&value) {
                    continue;
                }
                decrypt_value(&credential_key, &value).map_err(|error| {
                    StateError::Conflict(format!(
                        "cannot decrypt secret {} ({}): {error}",
                        row.text("name").unwrap_or_default(),
                        row.text("id").unwrap_or_default(),
                    ))
                })?;
            }
            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT instance_id, name, sealed_value FROM script_instance_inputs
                  WHERE sealed_value IS NOT NULL",
                &[],
            )
            .await?;
            for row in rows {
                let value = row.text("sealed_value")?;
                if !is_encrypted(&value) {
                    continue;
                }
                decrypt_value(&credential_key, &value).map_err(|error| {
                    StateError::Conflict(format!(
                        "cannot decrypt secret input {} of script instance {}: {error}",
                        row.text("name").unwrap_or_default(),
                        row.text("instance_id").unwrap_or_default(),
                    ))
                })?;
            }
            Ok(())
        })?;
        self.validate_persisted_jwt_signing_secret(key)
    }

    // Re-encrypt any plaintext passwords in the database.
    //
    // On upgrade from a version without encryption, passwords are stored as
    // plaintext. This reads each one and re-writes it, which triggers the
    // encrypt-on-write path. Idempotent — already-encrypted values pass through.
    fn migrate_plaintext_credentials(&self) -> Result<(), StateError> {
        use crate::persistence::encryption::{is_encrypted, maybe_encrypt};
        use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};

        let Some(key) = self.encryption_key() else {
            return Ok(()); // no key set, nothing to do
        };
        let key = key.clone();
        let datastore = self.datastore();

        self.run_sql_blocking(async move {
            let server_rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT id, password FROM servers WHERE password IS NOT NULL",
                &[],
            )
            .await?
            .into_iter()
            .map(|row| Ok((row.i64("id")?, row.text("password")?)))
            .collect::<Result<Vec<_>, StateError>>()?
            .into_iter()
            .filter(|(_, pw)| !pw.is_empty() && !is_encrypted(pw))
            .collect::<Vec<_>>();

            for (id, plaintext) in &server_rows {
                let val = Some(plaintext.clone());
                if let Some(encrypted) =
                    maybe_encrypt(Some(&key), &val).map_err(StateError::Database)?
                {
                    SqlRuntime::execute(
                        datastore.write_exec(),
                        "UPDATE servers SET password = {} WHERE id = {}",
                        &[SqlArg::Text(encrypted), SqlArg::I64(*id)],
                    )
                    .await?;
                }
            }
            if !server_rows.is_empty() {
                tracing::info!(
                    count = server_rows.len(),
                    "encrypted plaintext server passwords"
                );
            }

            let feed_rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT id, password FROM rss_feeds WHERE password IS NOT NULL",
                &[],
            )
            .await?
            .into_iter()
            .map(|row| Ok((row.i64("id")?, row.text("password")?)))
            .collect::<Result<Vec<_>, StateError>>()?
            .into_iter()
            .filter(|(_, pw)| !pw.is_empty() && !is_encrypted(pw))
            .collect::<Vec<_>>();

            for (id, plaintext) in &feed_rows {
                let val = Some(plaintext.clone());
                if let Some(encrypted) =
                    maybe_encrypt(Some(&key), &val).map_err(StateError::Database)?
                {
                    SqlRuntime::execute(
                        datastore.write_exec(),
                        "UPDATE rss_feeds SET password = {} WHERE id = {}",
                        &[SqlArg::Text(encrypted), SqlArg::I64(*id)],
                    )
                    .await?;
                }
            }
            if !feed_rows.is_empty() {
                tracing::info!(
                    count = feed_rows.len(),
                    "encrypted plaintext RSS feed passwords"
                );
            }

            for (select, update) in [
                (
                    ACTIVE_JOB_PASSWORDS_SQL,
                    "UPDATE active_jobs SET password = {} WHERE job_id = {} AND password = {}",
                ),
                (
                    "SELECT job_id AS id, source_password AS password FROM semantic_duplicate_candidates WHERE source_password IS NOT NULL",
                    "UPDATE semantic_duplicate_candidates SET source_password = {} WHERE job_id = {} AND source_password = {}",
                ),
            ] {
                let rows = SqlRuntime::fetch_all(datastore.read_exec(), select, &[]).await?;
                for row in rows {
                    let password = row.text("password")?;
                    if password.is_empty() || is_encrypted(&password) {
                        continue;
                    }
                    let encrypted = crate::persistence::encryption::encrypt_value(&key, &password)
                        .map_err(StateError::Database)?;
                    SqlRuntime::execute(
                        datastore.write_exec(),
                        update,
                        &[
                            SqlArg::Text(encrypted),
                            SqlArg::I64(row.i64("id")?),
                            SqlArg::Text(password),
                        ],
                    )
                    .await?;
                }
            }

            Ok(())
        })
    }
}

trait SealedCredentialQueries {
    fn has_sealed_secrets(&self) -> Result<bool, StateError>;
    fn validate_sealed_secrets(
        &self,
        key: &crate::persistence::encryption::EncryptionKey,
    ) -> Result<(), StateError>;
}

impl SealedCredentialQueries for Database {
    // Whether any torrent or RSS secret is sealed under the instance key:
    // a torrent's metainfo or tracker URL, or a feed's URL. The sealing pass marks itself before its first write, and
    // with its mark set every such value is sealed or about to be, so each
    // table is asked only whether it holds a row: one existence read a
    // column, none of them reading a payload. Without the mark nothing was sealed,
    // and no table is read.
    fn has_sealed_secrets(&self) -> Result<bool, StateError> {
        use crate::persistence::sql_runtime::SqlRuntime;

        if self
            .get_setting(crate::torrents::sealed::SEALED_MARK)?
            .is_none()
        {
            return Ok(false);
        }
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            for query in SEALED_COLUMNS.map(|(_, exists, _)| exists) {
                if SqlRuntime::fetch_optional(datastore.read_exec(), query, &[])
                    .await?
                    .is_some()
                {
                    return Ok(true);
                }
            }
            Ok(false)
        })
    }

    // Prove the selected key opens the sealed torrent and RSS secrets: one
    // stored value of each sealed column is decrypted, so a wrong key is
    // refused at start rather than found torrent by torrent. A column whose
    // sample is not sealed yet proves nothing and passes.
    fn validate_sealed_secrets(
        &self,
        key: &crate::persistence::encryption::EncryptionKey,
    ) -> Result<(), StateError> {
        use crate::persistence::encryption::{decrypt_value, is_encrypted};
        use crate::persistence::sql_runtime::SqlRuntime;

        if self
            .get_setting(crate::torrents::sealed::SEALED_MARK)?
            .is_none()
        {
            return Ok(());
        }
        let datastore = self.datastore();
        let key = key.clone();
        self.run_sql_blocking_read(async move {
            for (kind, _, sample) in SEALED_COLUMNS {
                let Some(row) =
                    SqlRuntime::fetch_optional(datastore.read_exec(), sample, &[]).await?
                else {
                    continue;
                };
                let opened = if kind == "torrent metainfo" {
                    let stored = row.bytes("sample")?;
                    crate::torrents::sealed::is_sealed(&stored).then(|| {
                        crate::torrents::sealed::open_metainfo(Some(&key), stored).map(drop)
                    })
                } else {
                    let stored = row.text("sample")?;
                    is_encrypted(&stored).then(|| decrypt_value(&key, &stored).map(drop))
                };
                if let Some(Err(error)) = opened {
                    return Err(StateError::Conflict(format!(
                        "cannot decrypt a stored {kind}: {error}"
                    )));
                }
            }
            Ok(())
        })
    }
}
