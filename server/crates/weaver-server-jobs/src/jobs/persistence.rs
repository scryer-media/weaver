// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::database::{JobsDatabase, ScriptEffectsParameters};
use crate::jobs::model::{FieldUpdate, JobUpdate};
use crate::jobs::record::{ActiveFileIdentity, ActiveJob};
use sqlx::{Postgres, QueryBuilder, Sqlite};
use weaver_server_base::jobs::ids::JobId;
use weaver_server_db::persistence::StateError;
use weaver_server_db::persistence::sql_runtime::{
    POSTGRES_BATCH_BIND_LIMIT, SQLITE_BATCH_BIND_LIMIT, SqlArg, SqlEngine, SqlRuntime, SqlTx,
    max_rows_for_tx,
};

pub use crate::jobs::stored_passwords::encrypt_archive_password;

// A job's supplied archive passwords, sealed as one value the way its
// password is: a JSON list of `[source label, password]` pairs. An empty list
// stores nothing.
pub fn encrypt_supplied_passwords(
    key: Option<&weaver_server_db::persistence::encryption::EncryptionKey>,
    candidates: &[crate::jobs::ArchivePasswordCandidate],
) -> Result<Option<String>, StateError> {
    if candidates.is_empty() {
        return Ok(None);
    }
    let pairs: Vec<(&str, &str)> = candidates
        .iter()
        .map(|candidate| (candidate.source().as_str(), candidate.value()))
        .collect();
    let json = serde_json::to_string(&pairs)
        .map_err(|error| StateError::Database(format!("invalid archive passwords: {error}")))?;
    encrypt_archive_password(key, Some(&json))
}

pub use crate::jobs::stored_passwords::decrypt_archive_password;

pub async fn active_job_exists_tx(tx: &mut SqlTx<'_>, job_id: JobId) -> Result<bool, StateError> {
    let sql = match tx {
        SqlTx::Postgres(_) => "SELECT 1 FROM active_jobs WHERE job_id = {} FOR KEY SHARE",
        SqlTx::Sqlite(_) => "SELECT 1 FROM active_jobs WHERE job_id = {} LIMIT 1",
    };
    Ok(tx
        .fetch_optional(sql, &[SqlArg::I64(job_id.0 as i64)])
        .await?
        .is_some())
}

pub use crate::jobs::active_lock::{lock_active_job_for_delete_tx, lock_active_job_for_write_tx};

pub fn db_err(error: impl std::fmt::Display) -> StateError {
    StateError::Database(error.to_string())
}

pub use crate::jobs::metadata_json::metadata_json;

#[expect(
    clippy::disallowed_methods,
    reason = "writer gate: a batch on the transaction run_in_transaction opened under the gate; Postgres has no writer gate"
)]
pub async fn bulk_upsert_file_identities_tx(
    tx: &mut SqlTx<'_>,
    job_id: JobId,
    identities: &[ActiveFileIdentity],
) -> Result<(), StateError> {
    if identities.is_empty() {
        return Ok(());
    }

    let chunk_size = max_rows_for_tx(tx, 9);
    let conflict_sql = " ON CONFLICT(job_id, file_index) DO UPDATE SET
                            source_filename = excluded.source_filename,
                            current_filename = excluded.current_filename,
                            canonical_filename = excluded.canonical_filename,
                            classification_kind = excluded.classification_kind,
                            classification_set_name = excluded.classification_set_name,
                            classification_volume_index = excluded.classification_volume_index,
                            classification_source = excluded.classification_source";
    match tx {
        SqlTx::Sqlite(tx) => {
            for chunk in identities.chunks(chunk_size) {
                let mut builder = QueryBuilder::<Sqlite>::new(
                    "INSERT INTO active_file_identities
                     (job_id, file_index, source_filename, current_filename, canonical_filename,
                      classification_kind, classification_set_name, classification_volume_index,
                      classification_source) ",
                );
                builder.push_values(chunk, |mut row, identity| {
                    row.push_bind(job_id.0 as i64)
                        .push_bind(identity.file_index as i64)
                        .push_bind(identity.source_filename.clone())
                        .push_bind(identity.current_filename.clone())
                        .push_bind(identity.canonical_filename.clone())
                        .push_bind(
                            identity
                                .classification
                                .as_ref()
                                .map(|classification| classification.kind.as_str().to_string()),
                        )
                        .push_bind(
                            identity
                                .classification
                                .as_ref()
                                .map(|classification| classification.set_name.clone()),
                        )
                        .push_bind(
                            identity
                                .classification
                                .as_ref()
                                .and_then(|classification| classification.volume_index)
                                .map(|value| value as i64),
                        )
                        .push_bind(identity.classification_source.as_str().to_string());
                });
                builder.push(conflict_sql);
                builder.build().execute(&mut **tx).await.map_err(db_err)?;
            }
        }
        SqlTx::Postgres(tx) => {
            for chunk in identities.chunks(chunk_size) {
                let mut builder = QueryBuilder::<Postgres>::new(
                    "INSERT INTO active_file_identities
                     (job_id, file_index, source_filename, current_filename, canonical_filename,
                      classification_kind, classification_set_name, classification_volume_index,
                      classification_source) ",
                );
                builder.push_values(chunk, |mut row, identity| {
                    row.push_bind(job_id.0 as i64)
                        .push_bind(identity.file_index as i64)
                        .push_bind(identity.source_filename.clone())
                        .push_bind(identity.current_filename.clone())
                        .push_bind(identity.canonical_filename.clone())
                        .push_bind(
                            identity
                                .classification
                                .as_ref()
                                .map(|classification| classification.kind.as_str().to_string()),
                        )
                        .push_bind(
                            identity
                                .classification
                                .as_ref()
                                .map(|classification| classification.set_name.clone()),
                        )
                        .push_bind(
                            identity
                                .classification
                                .as_ref()
                                .and_then(|classification| classification.volume_index)
                                .map(|value| value as i64),
                        )
                        .push_bind(identity.classification_source.as_str().to_string());
                });
                builder.push(conflict_sql);
                builder.build().execute(&mut **tx).await.map_err(db_err)?;
            }
        }
    }
    Ok(())
}

// Update `active_files.filename` for each identity's `current_filename`, the
// sibling write [`Database::save_file_identity`] performs alongside the
// identity upsert. Rows whose `active_files` entry does not yet exist (file
// not completed) are simply not matched, exactly as the single-row path. Runs
// inside the caller's transaction: Postgres batches with `UPDATE ... FROM
// (VALUES ...)`, SQLite issues per-row updates (local to the serialized
// writer, so the extra statements are cheap).
#[expect(
    clippy::disallowed_methods,
    reason = "writer gate: a batch on the transaction run_in_transaction opened under the gate; Postgres has no writer gate"
)]
pub async fn bulk_update_active_files_filenames_tx(
    tx: &mut SqlTx<'_>,
    job_id: JobId,
    identities: &[ActiveFileIdentity],
) -> Result<(), StateError> {
    if identities.is_empty() {
        return Ok(());
    }

    if matches!(tx, SqlTx::Sqlite(_)) {
        for identity in identities {
            tx.execute(
                "UPDATE active_files SET filename = {}
                 WHERE job_id = {} AND file_index = {}",
                &[
                    SqlArg::Text(identity.current_filename.clone()),
                    SqlArg::I64(job_id.0 as i64),
                    SqlArg::I64(identity.file_index as i64),
                ],
            )
            .await?;
        }
        return Ok(());
    }

    if let SqlTx::Postgres(tx) = tx {
        // 3 binds per row (job_id, file_index, filename).
        let chunk_size = (POSTGRES_BATCH_BIND_LIMIT / 3).max(1);
        for chunk in identities.chunks(chunk_size) {
            let mut builder = QueryBuilder::<Postgres>::new(
                "UPDATE active_files SET filename = v.filename FROM (",
            );
            builder.push_values(chunk, |mut row, identity| {
                row.push_bind(job_id.0 as i64)
                    .push_bind(identity.file_index as i64)
                    .push_bind(identity.current_filename.clone());
            });
            builder.push(
                ") AS v(job_id, file_index, filename)
                 WHERE active_files.job_id = v.job_id
                   AND active_files.file_index = v.file_index",
            );
            builder.build().execute(&mut **tx).await.map_err(db_err)?;
        }
    }
    Ok(())
}

// How a persisted completed-file MD5 was obtained. Legacy rows predate this
// distinction (NULL in `active_files.md5_provenance`) and are never trusted
// for quick verification — they can identify a file, not certify it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletedHashProvenance {
    // Folded from the decode pass over the downloaded bytes themselves.
    Streamed,
    // Confirmed by an actual PAR2 verification pass over the file.
    Verified,
}

impl CompletedHashProvenance {
    pub(crate) fn as_sql(self) -> &'static str {
        match self {
            Self::Streamed => "streamed",
            Self::Verified => "verified",
        }
    }
}

// Build the Postgres autocommit statement that upserts `row_count` completed
// files guarded by a `FOR KEY SHARE` lock on the owning job, mirroring the
// single-row [`Database::complete_file_with_optional_hash`] shape. `$1` is the
// guard `job_id`; each row then binds `(job_id, file_index, filename, md5)`.
fn build_complete_files_pg_sql(row_count: usize) -> String {
    let mut values = String::new();
    for row in 0..row_count {
        if row > 0 {
            values.push_str(", ");
        }
        if row == 0 {
            values.push_str("({}::bigint, {}::bigint, {}::text, {}::bytea, {}::text)");
        } else {
            values.push_str("({}, {}, {}, {}, {})");
        }
    }
    format!(
        "WITH active_complete_files_active AS (
             SELECT 1 FROM active_jobs WHERE job_id = {{}} FOR KEY SHARE
         )
         INSERT INTO active_files (job_id, file_index, filename, md5, md5_provenance)
         SELECT d.job_id, d.file_index, d.filename, d.md5, d.md5_provenance
         FROM (VALUES {values}) AS d(job_id, file_index, filename, md5, md5_provenance),
              active_complete_files_active
         ON CONFLICT(job_id, file_index) DO UPDATE SET
            filename = excluded.filename,
            md5 = excluded.md5,
            md5_provenance = excluded.md5_provenance"
    )
}

pub trait JobsPersistenceQueries {
    fn create_active_job(&self, job: &ActiveJob) -> Result<(), StateError>;
    fn update_active_job(&self, job_id: JobId, update: &JobUpdate) -> Result<(), StateError>;
    fn update_active_job_output_dir(
        &self,
        job_id: JobId,
        output_dir: &std::path::Path,
    ) -> Result<(), StateError>;
    fn move_active_job_output_dir(
        &self,
        job_id: JobId,
        from: &std::path::Path,
        to: &std::path::Path,
    ) -> Result<bool, StateError>;
    fn update_active_job_queue_positions(
        &self,
        positions: &[(JobId, i64)],
    ) -> Result<(), StateError>;
    fn load_active_job_queue_positions(
        &self,
    ) -> Result<std::collections::HashMap<JobId, i64>, StateError>;
    fn set_active_job_status(
        &self,
        job_id: JobId,
        status: &str,
        error: Option<&str>,
    ) -> Result<(), StateError>;
    #[expect(
        clippy::too_many_arguments,
        reason = "active job runtime is persisted as one atomic SQL update"
    )]
    fn set_active_job_runtime(
        &self,
        job_id: JobId,
        status: &str,
        download_state: Option<&str>,
        post_state: Option<&str>,
        run_state: Option<&str>,
        error: Option<&str>,
        queued_repair_at_epoch_ms: Option<f64>,
        queued_extract_at_epoch_ms: Option<f64>,
        paused_resume_status: Option<&str>,
        paused_resume_download_state: Option<&str>,
        paused_resume_post_state: Option<&str>,
    ) -> Result<(), StateError>;
    #[expect(
        clippy::too_many_arguments,
        reason = "active job runtime is persisted as one atomic SQL update"
    )]
    fn set_active_job_runtime_at(
        &self,
        job_id: JobId,
        output_dir: Option<&str>,
        post_processing_outcome: Option<&str>,
        status: &str,
        download_state: Option<&str>,
        post_state: Option<&str>,
        run_state: Option<&str>,
        error: Option<&str>,
        queued_repair_at_epoch_ms: Option<f64>,
        queued_extract_at_epoch_ms: Option<f64>,
        paused_resume_status: Option<&str>,
        paused_resume_download_state: Option<&str>,
        paused_resume_post_state: Option<&str>,
    ) -> Result<(), StateError>;
    fn complete_file(
        &self,
        job_id: JobId,
        file_index: u32,
        filename: &str,
        md5: &[u8; 16],
    ) -> Result<(), StateError>;
    fn complete_file_with_optional_hash(
        &self,
        job_id: JobId,
        file_index: u32,
        filename: &str,
        md5: Option<&[u8; 16]>,
    ) -> Result<(), StateError>;
    fn complete_file_with_provenance(
        &self,
        job_id: JobId,
        file_index: u32,
        filename: &str,
        md5: Option<&[u8; 16]>,
        provenance: CompletedHashProvenance,
    ) -> Result<(), StateError>;
    fn complete_files(
        &self,
        job_id: JobId,
        entries: &[(u32, String, Option<[u8; 16]>)],
        provenance: CompletedHashProvenance,
    ) -> Result<(), StateError>;
    fn save_file_identity(
        &self,
        job_id: JobId,
        identity: &ActiveFileIdentity,
    ) -> Result<(), StateError>;
    fn save_file_identities(
        &self,
        job_id: JobId,
        identities: &[ActiveFileIdentity],
    ) -> Result<(), StateError>;
}

impl<D: JobsDatabase> JobsPersistenceQueries for D {
    fn create_active_job(&self, job: &ActiveJob) -> Result<(), StateError> {
        let datastore = self.borrow().datastore();
        let job_id = job.job_id;
        let nzb_zstd: std::sync::Arc<[u8]> = job.nzb_zstd.as_slice().into();
        let args = vec![
            SqlArg::I64(job.job_id.0 as i64),
            SqlArg::Bytes(job.nzb_hash.to_vec()),
            SqlArg::Text(job.nzb_path.to_str().unwrap_or("").to_string()),
            SqlArg::Text(job.output_dir.to_str().unwrap_or("").to_string()),
            SqlArg::Text(job.status.to_string()),
            SqlArg::Text(job.download_state.to_string()),
            SqlArg::Text(job.post_state.to_string()),
            SqlArg::Text(job.run_state.to_string()),
            SqlArg::I64(job.created_at as i64),
            SqlArg::OptText(job.category.clone()),
            SqlArg::OptText(metadata_json(&job.metadata)?),
            SqlArg::OptText(job.paused_resume_status.map(str::to_string)),
            SqlArg::OptText(job.paused_resume_download_state.map(str::to_string)),
            SqlArg::OptText(job.paused_resume_post_state.map(str::to_string)),
            SqlArg::OptText(encrypt_archive_password(
                self.borrow().encryption_key(),
                job.password_override.as_deref(),
            )?),
            SqlArg::OptText(encrypt_supplied_passwords(
                self.borrow().encryption_key(),
                &job.supplied_password_candidates,
            )?),
        ];
        self.borrow().run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "create_active_job", |tx| {
                let args = args.clone();
                let nzb_zstd = nzb_zstd.clone();
                Box::pin(async move {
                    tx.execute(
                        "INSERT INTO active_jobs
                         (job_id, nzb_hash, nzb_path, output_dir, status, download_state, post_state, run_state, created_at, category, metadata, paused_resume_status, paused_resume_download_state, paused_resume_post_state, password, password_candidates)
                         VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})",
                        &args,
                    )
                    .await?;
                    crate::jobs::nzb_persistence::write_active_job_nzb_tx(tx, job_id, &nzb_zstd).await?;
                    Ok(())
                })
            })
            .await
        })
    }

    fn update_active_job(&self, job_id: JobId, update: &JobUpdate) -> Result<(), StateError> {
        let datastore = self.borrow().datastore();
        let mut update = update.clone();
        if let FieldUpdate::Set(password) = &mut update.password {
            *password = encrypt_archive_password(self.borrow().encryption_key(), Some(password))?
                .expect("supplied password");
        }
        let _effects_writer = self.lock_job_script_effects_writer();
        let rewritten_effects = self.borrow().run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "update_active_job", |tx| {
                let update = update.clone();
                Box::pin(async move {
                    tx.execute(
                        "UPDATE script_output_state SET next_seq = next_seq WHERE singleton = 1",
                        &[],
                    )
                    .await?;
                    let mut rewritten_effects = None;
                    match &update.category {
                        FieldUpdate::Unchanged => {}
                        FieldUpdate::Clear => {
                            tx.execute(
                                "UPDATE active_jobs SET category = NULL WHERE job_id = {}",
                                &[SqlArg::I64(job_id.0 as i64)],
                            )
                            .await?;
                        }
                        FieldUpdate::Set(category) => {
                            tx.execute(
                                "UPDATE active_jobs SET category = {} WHERE job_id = {}",
                                &[SqlArg::Text(category.clone()), SqlArg::I64(job_id.0 as i64)],
                            )
                            .await?;
                        }
                    }

                    if (!matches!(update.metadata, FieldUpdate::Unchanged)
                        || !update.metadata_edits.is_empty()
                        || update.name.is_some())
                        && let Some(active) = tx
                            .fetch_optional(
                                "SELECT metadata FROM active_jobs WHERE job_id = {}",
                                &[SqlArg::I64(job_id.0 as i64)],
                            )
                            .await?
                    {
                        let mut metadata: Vec<(String, String)> = active
                            .opt_text("metadata")?
                            .map(|text| serde_json::from_str(&text).map_err(db_err))
                            .transpose()?
                            .unwrap_or_default();
                        update.apply_to_metadata(&mut metadata);
                        if let Some(name) = &update.name {
                            weaver_server_base::ingest::metadata::set_renamed_title(&mut metadata, name);
                        }
                        if let Some(row) = tx
                            .fetch_optional(
                                "SELECT state FROM script_job_state WHERE job_id = {}",
                                &[SqlArg::I64(job_id.0 as i64)],
                            )
                            .await?
                        {
                            let mut effects: D::ScriptEffects =
                                serde_json::from_str(&row.text("state")?)
                                    .map_err(|error| StateError::Database(error.to_string()))?;
                            if !matches!(update.metadata, FieldUpdate::Unchanged) {
                                for value in effects.parameters_mut().values_mut() {
                                    value.clear();
                                }
                                effects.parameters_mut().extend(metadata.clone());
                            } else {
                                let renamed = update.name.as_ref().map(|name| {
                                    (
                                        weaver_server_base::ingest::metadata::RENAMED_TITLE_METADATA_KEY.to_string(),
                                        Some(name.clone()),
                                    )
                                });
                                for (key, value) in
                                    update.metadata_edits.iter().chain(renamed.iter())
                                {
                                    for (existing, previous) in effects.parameters_mut() {
                                        if existing.eq_ignore_ascii_case(key) {
                                            previous.clear();
                                        }
                                    }
                                    effects
                                        .parameters_mut()
                                        .insert(key.clone(), value.clone().unwrap_or_default());
                                }
                            }
                            tx.execute(
                                "UPDATE script_job_state SET state = {} WHERE job_id = {}",
                                &[
                                    SqlArg::Text(serde_json::to_string(&effects).map_err(
                                        |error| StateError::Database(error.to_string()),
                                    )?),
                                    SqlArg::I64(job_id.0 as i64),
                                ],
                            )
                            .await?;
                            rewritten_effects = Some(effects);
                        }
                        tx.execute(
                            "UPDATE active_jobs SET metadata = {} WHERE job_id = {}",
                            &[
                                SqlArg::OptText(metadata_json(&metadata)?),
                                SqlArg::I64(job_id.0 as i64),
                            ],
                        )
                        .await?;
                    }

                    // Password override column: NULL = no override (restore
                    // keeps the NZB-derived password), '' = explicitly no
                    // password, anything else = the encrypted override.
                    match update.password {
                        FieldUpdate::Unchanged => {}
                        FieldUpdate::Clear => {
                            tx.execute(
                                "UPDATE active_jobs SET password = {} WHERE job_id = {}",
                                &[SqlArg::Text(String::new()), SqlArg::I64(job_id.0 as i64)],
                            )
                            .await?;
                        }
                        FieldUpdate::Set(password) => {
                            tx.execute(
                                "UPDATE active_jobs SET password = {} WHERE job_id = {}",
                                &[SqlArg::Text(password), SqlArg::I64(job_id.0 as i64)],
                            )
                            .await?;
                        }
                    }
                    Ok(rewritten_effects)
                })
            })
            .await
        })?;
        if let Some(effects) = rewritten_effects {
            self.cache_job_script_effects(job_id.0, effects);
        }
        Ok(())
    }

    fn update_active_job_output_dir(
        &self,
        job_id: JobId,
        output_dir: &std::path::Path,
    ) -> Result<(), StateError> {
        let datastore = self.borrow().datastore();
        let output_dir = output_dir.to_string_lossy().into_owned();
        self.borrow().run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "update_active_job_output_dir", |tx| {
                let output_dir = output_dir.clone();
                Box::pin(async move {
                    tx.execute(
                        "UPDATE active_jobs SET output_dir = {} WHERE job_id = {}",
                        &[SqlArg::Text(output_dir), SqlArg::I64(job_id.0 as i64)],
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
        })
    }

    // The job's row names `to` if it still names `from`: whether it did. A
    // row that has gone, or names another directory, is left as it is.
    fn move_active_job_output_dir(
        &self,
        job_id: JobId,
        from: &std::path::Path,
        to: &std::path::Path,
    ) -> Result<bool, StateError> {
        let datastore = self.borrow().datastore();
        let from = from.to_string_lossy().into_owned();
        let to = to.to_string_lossy().into_owned();
        self.borrow().run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "move_active_job_output_dir", |tx| {
                let (from, to) = (from.clone(), to.clone());
                Box::pin(async move {
                    let changed = tx
                        .execute(
                            "UPDATE active_jobs SET output_dir = {} WHERE job_id = {} AND output_dir = {}",
                            &[
                                SqlArg::Text(to),
                                SqlArg::I64(job_id.0 as i64),
                                SqlArg::Text(from),
                            ],
                        )
                        .await?;
                    Ok(changed > 0)
                })
            })
            .await
        })
    }

    // Persist queue positions as one write transaction through the writer
    // gate. The caller passes only the rows whose position changed; each
    // statement binds a bounded number of them, so a long queue never builds
    // one unbounded statement.
    fn update_active_job_queue_positions(
        &self,
        positions: &[(JobId, i64)],
    ) -> Result<(), StateError> {
        if positions.is_empty() {
            return Ok(());
        }
        let datastore = self.borrow().datastore();
        let positions = std::sync::Arc::new(positions.to_vec());
        self.borrow().run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "update_active_job_queue_positions", |tx| {
                let positions = positions.clone();
                Box::pin(async move {
                    // Three binds a row: the CASE arm's id and position, and
                    // the id in the IN list.
                    let chunk_size = max_rows_for_tx(tx, 3);
                    for chunk in positions.chunks(chunk_size) {
                        let arms = "WHEN {} THEN {} ".repeat(chunk.len());
                        let ids = vec!["{}"; chunk.len()].join(",");
                        let sql = format!(
                            "UPDATE active_jobs
                                SET queue_position = CASE job_id {arms}END
                              WHERE job_id IN ({ids})"
                        );
                        let mut args = Vec::with_capacity(chunk.len() * 3);
                        for (job_id, position) in chunk {
                            args.push(SqlArg::I64(job_id.0 as i64));
                            args.push(SqlArg::I64(*position));
                        }
                        args.extend(chunk.iter().map(|(job_id, _)| SqlArg::I64(job_id.0 as i64)));
                        tx.execute(&sql, &args).await?;
                    }
                    Ok(())
                })
            })
            .await
        })
    }

    // Every active job's stored queue position, for the actor to compare a
    // new order against.
    fn load_active_job_queue_positions(
        &self,
    ) -> Result<std::collections::HashMap<JobId, i64>, StateError> {
        let datastore = self.borrow().datastore();
        self.borrow().run_sql_blocking_read(async move {
            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT job_id, queue_position FROM active_jobs WHERE queue_position IS NOT NULL",
                &[],
            )
            .await?;
            rows.into_iter()
                .map(|row| Ok((JobId(row.i64("job_id")? as u64), row.i64("queue_position")?)))
                .collect()
        })
    }

    fn set_active_job_status(
        &self,
        job_id: JobId,
        status: &str,
        error: Option<&str>,
    ) -> Result<(), StateError> {
        self.set_active_job_runtime(
            job_id, status, None, None, None, error, None, None, None, None, None,
        )
    }

    fn set_active_job_runtime(
        &self,
        job_id: JobId,
        status: &str,
        download_state: Option<&str>,
        post_state: Option<&str>,
        run_state: Option<&str>,
        error: Option<&str>,
        queued_repair_at_epoch_ms: Option<f64>,
        queued_extract_at_epoch_ms: Option<f64>,
        paused_resume_status: Option<&str>,
        paused_resume_download_state: Option<&str>,
        paused_resume_post_state: Option<&str>,
    ) -> Result<(), StateError> {
        self.set_active_job_runtime_at(
            job_id,
            None,
            None,
            status,
            download_state,
            post_state,
            run_state,
            error,
            queued_repair_at_epoch_ms,
            queued_extract_at_epoch_ms,
            paused_resume_status,
            paused_resume_download_state,
            paused_resume_post_state,
        )
    }

    // [`Self::set_active_job_runtime`] that also moves the job's recorded
    // output directory in the same statement, so a restart can never read the
    // new runtime against the old location or the other way round. `None`
    // keeps the directory already recorded. `post_processing_outcome`, when
    // given, is kept in the same statement too: the moving row carries what
    // the delivery ruled.
    fn set_active_job_runtime_at(
        &self,
        job_id: JobId,
        output_dir: Option<&str>,
        post_processing_outcome: Option<&str>,
        status: &str,
        download_state: Option<&str>,
        post_state: Option<&str>,
        run_state: Option<&str>,
        error: Option<&str>,
        queued_repair_at_epoch_ms: Option<f64>,
        queued_extract_at_epoch_ms: Option<f64>,
        paused_resume_status: Option<&str>,
        paused_resume_download_state: Option<&str>,
        paused_resume_post_state: Option<&str>,
    ) -> Result<(), StateError> {
        let datastore = self.borrow().datastore();
        let args = vec![
            SqlArg::OptText(output_dir.map(str::to_string)),
            SqlArg::OptText(post_processing_outcome.map(str::to_string)),
            SqlArg::Text(status.to_string()),
            SqlArg::OptText(download_state.map(str::to_string)),
            SqlArg::OptText(post_state.map(str::to_string)),
            SqlArg::OptText(run_state.map(str::to_string)),
            SqlArg::OptText(error.map(str::to_string)),
            SqlArg::OptF64(queued_repair_at_epoch_ms),
            SqlArg::OptF64(queued_extract_at_epoch_ms),
            SqlArg::OptText(paused_resume_status.map(str::to_string)),
            SqlArg::OptText(paused_resume_download_state.map(str::to_string)),
            SqlArg::OptText(paused_resume_post_state.map(str::to_string)),
            SqlArg::I64(job_id.0 as i64),
        ];
        self.borrow().run_sql_blocking(async move {
            match datastore.engine() {
                SqlEngine::Sqlite => {
                    SqlRuntime::run_in_transaction(&datastore, "set_active_job_runtime", |tx| {
                        let args = args.clone();
                        Box::pin(async move {
                            tx.execute(
                                "UPDATE active_jobs
                                 SET output_dir = COALESCE({}, output_dir),
                                     post_processing_outcome = COALESCE({}, post_processing_outcome),
                                     status = {},
                                     download_state = {},
                                     post_state = {},
                                     run_state = {},
                                     error = {},
                                     queued_repair_at_epoch_ms = {},
                                     queued_extract_at_epoch_ms = {},
                                     paused_resume_status = {},
                                     paused_resume_download_state = {},
                                     paused_resume_post_state = {}
                                 WHERE job_id = {}",
                                &args,
                            )
                            .await?;
                            Ok(())
                        })
                    })
                    .await
                }
                SqlEngine::Postgres => {
                    SqlRuntime::execute(
                        datastore.write_exec(),
                        "UPDATE active_jobs
                         SET output_dir = COALESCE({}, output_dir),
                             post_processing_outcome = COALESCE({}, post_processing_outcome),
                             status = {},
                             download_state = {},
                             post_state = {},
                             run_state = {},
                             error = {},
                             queued_repair_at_epoch_ms = {},
                             queued_extract_at_epoch_ms = {},
                             paused_resume_status = {},
                             paused_resume_download_state = {},
                             paused_resume_post_state = {}
                         WHERE job_id = {}",
                        &args,
                    )
                    .await?;
                    Ok(())
                }
            }
        })
    }

    fn complete_file(
        &self,
        job_id: JobId,
        file_index: u32,
        filename: &str,
        md5: &[u8; 16],
    ) -> Result<(), StateError> {
        self.complete_file_with_optional_hash(job_id, file_index, filename, Some(md5))
    }

    fn complete_file_with_optional_hash(
        &self,
        job_id: JobId,
        file_index: u32,
        filename: &str,
        md5: Option<&[u8; 16]>,
    ) -> Result<(), StateError> {
        self.complete_file_with_provenance(
            job_id,
            file_index,
            filename,
            md5,
            CompletedHashProvenance::Streamed,
        )
    }

    fn complete_file_with_provenance(
        &self,
        job_id: JobId,
        file_index: u32,
        filename: &str,
        md5: Option<&[u8; 16]>,
        provenance: CompletedHashProvenance,
    ) -> Result<(), StateError> {
        let datastore = self.borrow().datastore();
        let filename = filename.to_string();
        let md5 = md5.map(|md5| md5.to_vec());
        let provenance = md5.is_some().then(|| provenance.as_sql().to_string());
        self.borrow().run_sql_blocking(async move {
            match datastore.engine() {
                SqlEngine::Sqlite => {
                    SqlRuntime::run_in_transaction(&datastore, "complete_file", |tx| {
                        let filename = filename.clone();
                        let md5 = md5.clone();
                        let provenance = provenance.clone();
                        Box::pin(async move {
                            if active_job_exists_tx(tx, job_id).await? {
                                tx.execute(
                                    "INSERT INTO active_files (job_id, file_index, filename, md5, md5_provenance)
                                     VALUES ({}, {}, {}, {}, {})
                                     ON CONFLICT(job_id, file_index) DO UPDATE SET
                                        filename = excluded.filename,
                                        md5 = excluded.md5,
                                        md5_provenance = excluded.md5_provenance",
                                    &[
                                        SqlArg::I64(job_id.0 as i64),
                                        SqlArg::I64(file_index as i64),
                                        SqlArg::Text(filename),
                                        SqlArg::OptBytes(md5),
                                        SqlArg::OptText(provenance),
                                    ],
                                )
                                .await?;
                            }
                            Ok(())
                        })
                    })
                    .await
                }
                SqlEngine::Postgres => {
                    let started = std::time::Instant::now();
                    let result = SqlRuntime::execute(
                        datastore.write_exec(),
                        "WITH active_file_complete_active AS (
                             SELECT 1
                             FROM active_jobs
                             WHERE job_id = {}
                             FOR KEY SHARE
                         )
                             INSERT INTO active_files (job_id, file_index, filename, md5, md5_provenance)
                             SELECT {}, {}, {}, {}, {}
                             FROM active_file_complete_active
                             ON CONFLICT(job_id, file_index) DO UPDATE SET
                                filename = excluded.filename,
                                md5 = excluded.md5,
                                md5_provenance = excluded.md5_provenance",
                        &[
                            SqlArg::I64(job_id.0 as i64),
                            SqlArg::I64(job_id.0 as i64),
                            SqlArg::I64(file_index as i64),
                            SqlArg::Text(filename),
                            SqlArg::OptBytes(md5),
                            SqlArg::OptText(provenance),
                        ],
                    )
                    .await
                    .map(|_| ());
                    weaver_server_base::runtime::perf_probe::record_sql_op(
                        "postgres",
                        "complete_file",
                        started.elapsed(),
                    );
                    result
                }
            }
        })
    }

    // Complete a batch of files for a single job, the bulk counterpart to
    // [`Self::complete_file_with_optional_hash`]. Each entry is
    // `(file_index, filename, optional md5)`; the guard semantics match the
    // single-file path (an absent `active_jobs` row inserts nothing). Postgres
    // runs one autocommit `FOR KEY SHARE`-guarded multi-row upsert per chunk;
    // SQLite guards once inside a transaction then bulk-upserts in chunks.
    #[expect(
        clippy::disallowed_methods,
        reason = "writer gate: a batch on the transaction run_in_transaction opened under the gate; Postgres has no writer gate"
    )]
    fn complete_files(
        &self,
        job_id: JobId,
        entries: &[(u32, String, Option<[u8; 16]>)],
        provenance: CompletedHashProvenance,
    ) -> Result<(), StateError> {
        // 5 value binds per row (job_id, file_index, filename, md5,
        // md5_provenance) plus the single guard bind reused across the chunk.
        // Batched completions carry decode-streamed digests, so a present md5
        // records 'streamed' provenance.
        const BINDS_PER_ROW: usize = 5;

        if entries.is_empty() {
            return Ok(());
        }

        let datastore = self.borrow().datastore();
        let entries = entries.to_vec();
        self.borrow().run_sql_blocking(async move {
            match datastore.engine() {
                SqlEngine::Sqlite => {
                    let chunk_size = (SQLITE_BATCH_BIND_LIMIT / BINDS_PER_ROW).max(1);
                    SqlRuntime::run_in_transaction(&datastore, "complete_files", |tx| {
                        let entries = entries.clone();
                        Box::pin(async move {
                            if !active_job_exists_tx(tx, job_id).await? {
                                return Ok(());
                            }
                            let SqlTx::Sqlite(tx) = tx else {
                                return Ok(());
                            };
                            for chunk in entries.chunks(chunk_size) {
                                let mut builder = QueryBuilder::<Sqlite>::new(
                                    "INSERT INTO active_files (job_id, file_index, filename, md5, md5_provenance) ",
                                );
                                builder.push_values(
                                    chunk,
                                    |mut row, (file_index, filename, md5)| {
                                        row.push_bind(job_id.0 as i64)
                                            .push_bind(*file_index as i64)
                                            .push_bind(filename.clone())
                                            .push_bind(md5.map(|md5| md5.to_vec()))
                                            .push_bind(md5.map(|_| provenance.as_sql()));
                                    },
                                );
                                builder.push(
                                    " ON CONFLICT(job_id, file_index) DO UPDATE SET
                                        filename = excluded.filename,
                                        md5 = excluded.md5,
                                        md5_provenance = excluded.md5_provenance",
                                );
                                builder.build().execute(&mut **tx).await.map_err(db_err)?;
                            }
                            Ok(())
                        })
                    })
                    .await
                }
                SqlEngine::Postgres => {
                    let chunk_size = (POSTGRES_BATCH_BIND_LIMIT / BINDS_PER_ROW).max(1);
                    let started = std::time::Instant::now();
                    for chunk in entries.chunks(chunk_size) {
                        let sql = build_complete_files_pg_sql(chunk.len());
                        let mut args = Vec::with_capacity(1 + chunk.len() * BINDS_PER_ROW);
                        args.push(SqlArg::I64(job_id.0 as i64));
                        for (file_index, filename, md5) in chunk {
                            args.push(SqlArg::I64(job_id.0 as i64));
                            args.push(SqlArg::I64(*file_index as i64));
                            args.push(SqlArg::Text(filename.clone()));
                            args.push(SqlArg::OptBytes(md5.map(|md5| md5.to_vec())));
                            args.push(SqlArg::OptText(
                                md5.map(|_| provenance.as_sql().to_string()),
                            ));
                        }
                        SqlRuntime::execute(datastore.write_exec(), &sql, &args).await?;
                    }
                    weaver_server_base::runtime::perf_probe::record_sql_op(
                        "postgres",
                        "complete_files",
                        started.elapsed(),
                    );
                    Ok(())
                }
            }
        })
    }

    fn save_file_identity(
        &self,
        job_id: JobId,
        identity: &ActiveFileIdentity,
    ) -> Result<(), StateError> {
        let datastore = self.borrow().datastore();
        let identity = identity.clone();
        self.borrow().run_sql_blocking(async move {
            let classification_kind = identity
                .classification
                .as_ref()
                .map(|classification| classification.kind.as_str().to_string());
            let classification_set_name = identity
                .classification
                .as_ref()
                .map(|classification| classification.set_name.clone());
            let classification_volume_index = identity
                .classification
                .as_ref()
                .and_then(|classification| classification.volume_index)
                .map(|value| value as i64);
            let classification_source = identity.classification_source.as_str().to_string();
            let active_file_index = identity.file_index;
            let active_filename = identity.current_filename.clone();
            match datastore.engine() {
                SqlEngine::Sqlite => {
                    SqlRuntime::run_in_transaction(&datastore, "save_file_identity", |tx| {
                        let identity = identity.clone();
                        let classification_kind = classification_kind.clone();
                        let classification_set_name = classification_set_name.clone();
                        let classification_source = classification_source.clone();
                        let active_filename = active_filename.clone();
                        Box::pin(async move {
                            tx.execute(
                                "INSERT INTO active_file_identities
                                 (job_id, file_index, source_filename, current_filename, canonical_filename,
                                  classification_kind, classification_set_name, classification_volume_index,
                                  classification_source)
                                 SELECT {}, {}, {}, {}, {}, {}, {}, {}, {}
                                 WHERE EXISTS (SELECT 1 FROM active_jobs WHERE job_id = {})
                                 ON CONFLICT(job_id, file_index) DO UPDATE SET
                                    source_filename = excluded.source_filename,
                                    current_filename = excluded.current_filename,
                                    canonical_filename = excluded.canonical_filename,
                                    classification_kind = excluded.classification_kind,
                                    classification_set_name = excluded.classification_set_name,
                                    classification_volume_index = excluded.classification_volume_index,
                                    classification_source = excluded.classification_source",
                                &[
                                    SqlArg::I64(job_id.0 as i64),
                                    SqlArg::I64(active_file_index as i64),
                                    SqlArg::Text(identity.source_filename),
                                    SqlArg::Text(identity.current_filename),
                                    SqlArg::OptText(identity.canonical_filename),
                                    SqlArg::OptText(classification_kind),
                                    SqlArg::OptText(classification_set_name),
                                    SqlArg::OptI64(classification_volume_index),
                                    SqlArg::Text(classification_source),
                                    SqlArg::I64(job_id.0 as i64),
                                ],
                            )
                            .await?;
                            tx.execute(
                                "UPDATE active_files
                                 SET filename = {}
                                 WHERE job_id = {} AND file_index = {}",
                                &[
                                    SqlArg::Text(active_filename),
                                    SqlArg::I64(job_id.0 as i64),
                                    SqlArg::I64(active_file_index as i64),
                                ],
                            )
                            .await?;
                            Ok(())
                        })
                    })
                    .await
                }
                // Both writes ride one autocommit statement: the identity upsert
                // and the sibling filename update are data-modifying CTEs behind
                // a shared FOR KEY SHARE guard, so the statement no-ops when the
                // parent job row is gone and blocks a concurrent archive/delete
                // for its duration. Callers sit on the pipeline task, so the
                // round trips a BEGIN/COMMIT pair would add are latency the
                // actor itself pays.
                SqlEngine::Postgres => {
                    let started = std::time::Instant::now();
                    let result = SqlRuntime::execute(
                        datastore.write_exec(),
                        "WITH active_file_identity_parent AS (
                            SELECT 1 FROM active_jobs WHERE job_id = {} FOR KEY SHARE
                         ),
                         identity_upsert AS (
                            INSERT INTO active_file_identities
                            (job_id, file_index, source_filename, current_filename, canonical_filename,
                             classification_kind, classification_set_name, classification_volume_index,
                             classification_source)
                            SELECT {}, {}, {}, {}, {}, {}, {}, {}, {}
                            FROM active_file_identity_parent
                            ON CONFLICT(job_id, file_index) DO UPDATE SET
                               source_filename = excluded.source_filename,
                               current_filename = excluded.current_filename,
                               canonical_filename = excluded.canonical_filename,
                               classification_kind = excluded.classification_kind,
                               classification_set_name = excluded.classification_set_name,
                               classification_volume_index = excluded.classification_volume_index,
                               classification_source = excluded.classification_source
                         )
                         UPDATE active_files
                            SET filename = {}
                           FROM active_file_identity_parent
                          WHERE active_files.job_id = {} AND active_files.file_index = {}",
                        &[
                            SqlArg::I64(job_id.0 as i64),
                            SqlArg::I64(job_id.0 as i64),
                            SqlArg::I64(active_file_index as i64),
                            SqlArg::Text(identity.source_filename),
                            SqlArg::Text(identity.current_filename),
                            SqlArg::OptText(identity.canonical_filename),
                            SqlArg::OptText(classification_kind),
                            SqlArg::OptText(classification_set_name),
                            SqlArg::OptI64(classification_volume_index),
                            SqlArg::Text(classification_source),
                            SqlArg::Text(active_filename),
                            SqlArg::I64(job_id.0 as i64),
                            SqlArg::I64(active_file_index as i64),
                        ],
                    )
                    .await
                    .map(|_| ());
                    weaver_server_base::runtime::perf_probe::record_sql_op(
                        "postgres",
                        "save_file_identity",
                        started.elapsed(),
                    );
                    result
                }
            }
        })
    }

    // Persist a batch of file identities for a single job in ONE transaction.
    //
    // This is the bulk counterpart to [`Self::save_file_identity`]: it upserts
    // every `active_file_identities` row via [`bulk_upsert_file_identities_tx`]
    // AND performs the sibling `active_files.filename` update that the bulk tx
    // helper omits, so callers converting a per-identity loop keep identical
    // semantics. Postgres runs one `UPDATE ... FROM (VALUES ...)` per chunk;
    // SQLite issues per-row `UPDATE`s inside the same transaction (cheap and
    // local to the serialized writer).
    fn save_file_identities(
        &self,
        job_id: JobId,
        identities: &[ActiveFileIdentity],
    ) -> Result<(), StateError> {
        if identities.is_empty() {
            return Ok(());
        }

        let datastore = self.borrow().datastore();
        let identities = identities.to_vec();
        self.borrow().run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "save_file_identities", |tx| {
                let identities = identities.clone();
                Box::pin(async move {
                    if !active_job_exists_tx(tx, job_id).await? {
                        return Ok(());
                    }
                    bulk_upsert_file_identities_tx(tx, job_id, &identities).await?;
                    bulk_update_active_files_filenames_tx(tx, job_id, &identities).await?;
                    Ok(())
                })
            })
            .await
        })
    }
}
