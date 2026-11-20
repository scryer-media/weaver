// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The admission writes of an active job that also reach history, tags,
// outlines and scripts. The plain active job writes are in weaver-server-jobs.

use crate::post_processing::database::PostprocDatabase;
use crate::post_processing::effects::PostProcessingEffectsInternalQueries;
use crate::post_processing::events::PostProcessingEventsInvalidateQueueScriptAdmissionInternalQueries;
use weaver_server_base::jobs::ids::JobId;
use weaver_server_db::persistence::StateError;
use weaver_server_db::persistence::sql_runtime::{SqlArg, SqlRuntime, SqlTx};
use weaver_server_history::history::cache::HistoryCacheQueries;
use weaver_server_jobs::jobs::persistence::*;
use weaver_server_jobs::jobs::record::{ActiveFileIdentity, ActiveJob};
use weaver_server_settings::tags::TagsQueries;

pub trait JobsPersistenceInternalQueries {
    #[allow(dead_code)] // retained for restoration paths that bypass admission.
    fn create_active_job_with_file_identities(
        &self,
        job: &ActiveJob,
        identities: &[ActiveFileIdentity],
    ) -> Result<(), StateError>;
    fn semantic_materialization_is_current(
        &self,
        job_id: JobId,
        generation: i64,
    ) -> Result<bool, StateError>;
    fn semantic_promotion_materialization_is_current(
        &self,
        job_id: JobId,
        generation: i64,
    ) -> Result<bool, StateError>;
    #[cfg(any(test, feature = "test-support"))]
    fn materialize_admission(
        &self,
        job: &ActiveJob,
        identities: &[ActiveFileIdentity],
        semantic_materialization_generation: Option<i64>,
        semantic_promotion_generation: Option<i64>,
        restart: bool,
    ) -> Result<bool, StateError>;
    fn materialize_admission_with_outline(
        &self,
        job: &ActiveJob,
        identities: &[ActiveFileIdentity],
        outline: Option<&weaver_server_ingest::ingest::outline::NzbOutline>,
        semantic_materialization_generation: Option<i64>,
        semantic_promotion_generation: Option<i64>,
        restart: bool,
    ) -> Result<bool, StateError>;
    fn refresh_script_job_inputs(
        &self,
        job_id: u64,
        facts: &mut crate::post_processing::runner::CompatibilityFacts,
    ) -> Result<(), StateError>;
    fn set_active_job_staging_dir(
        &self,
        job_id: JobId,
        staging_dir: Option<&std::path::Path>,
    ) -> Result<(), StateError>;
    fn set_nzb_pause_release(&self, job_id: JobId, pending: bool) -> Result<(), StateError>;
    fn set_nzb_admitted(&self, job_id: JobId, admitted: bool) -> Result<(), StateError>;
}

impl<D: PostprocDatabase> JobsPersistenceInternalQueries for D {
    fn create_active_job_with_file_identities(
        &self,
        job: &ActiveJob,
        identities: &[ActiveFileIdentity],
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        let job_id = job.job_id;
        let identities = identities.to_vec();
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
                self.encryption_key(),
                job.password_override.as_deref(),
            )?),
            SqlArg::OptText(encrypt_supplied_passwords(
                self.encryption_key(),
                &job.supplied_password_candidates,
            )?),
        ];
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(
                &datastore,
                "create_active_job_with_file_identities",
                |tx| {
                    let args = args.clone();
                    let identities = identities.clone();
                    let nzb_zstd = nzb_zstd.clone();
                    Box::pin(async move {
                        tx.execute(
                            "INSERT INTO active_jobs
                             (job_id, nzb_hash, nzb_path, output_dir, status, download_state, post_state, run_state, created_at, category, metadata, paused_resume_status, paused_resume_download_state, paused_resume_post_state, password, password_candidates)
                             VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})",
                            &args,
                        )
                        .await?;
                        weaver_server_jobs::jobs::nzb_persistence::write_active_job_nzb_tx(tx, job_id, &nzb_zstd)
                            .await?;
                        bulk_upsert_file_identities_tx(tx, job_id, &identities).await?;
                        Ok(())
                    })
                },
            )
            .await
        })
    }

    // Performs the inexpensive first half of SCORE materialization before a
    // pipeline creates a work directory. The transactional check below remains
    // authoritative; this keeps a known-stale submit from allocating anything.
    fn semantic_materialization_is_current(
        &self,
        job_id: JobId,
        generation: i64,
    ) -> Result<bool, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            Ok(SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT 1 FROM semantic_duplicate_candidates
                 WHERE job_id = {} AND candidate_state = {} AND materialization_generation = {}",
                &[
                    SqlArg::I64(job_id.0 as i64),
                    SqlArg::Text("active".to_string()),
                    SqlArg::I64(generation),
                ],
            )
            .await?
            .is_some())
        })
    }

    // Checks an owned promotion lease before the scheduler allocates a
    // working directory. The materialization transaction repeats this CAS.
    fn semantic_promotion_materialization_is_current(
        &self,
        job_id: JobId,
        generation: i64,
    ) -> Result<bool, StateError> {
        let datastore = self.datastore();
        let now = (weaver_server_jobs::jobs::model::epoch_ms_now() / 1_000.0) as i64;
        self.run_sql_blocking(async move {
            Ok(SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT 1 FROM semantic_duplicate_candidates
                 WHERE job_id = {} AND candidate_state = {}
                   AND promotion_state = {} AND promotion_generation = {}
                   AND promotion_lease_expires_at > {}",
                &[
                    SqlArg::I64(job_id.0 as i64),
                    SqlArg::Text("active".to_string()),
                    SqlArg::Text("claimed".to_string()),
                    SqlArg::I64(generation),
                    SqlArg::I64(now),
                ],
            )
            .await?
            .is_some())
        })
    }

    // Atomically turns a reserved duplicate admission into an active job. A
    // SCORE caller supplies the generation it received at admission; parking
    // or superseding that candidate invalidates the generation before any
    // stale request can create scheduler work.
    #[cfg(any(test, feature = "test-support"))]
    fn materialize_admission(
        &self,
        job: &ActiveJob,
        identities: &[ActiveFileIdentity],
        semantic_materialization_generation: Option<i64>,
        semantic_promotion_generation: Option<i64>,
        restart: bool,
    ) -> Result<bool, StateError> {
        self.materialize_admission_with_outline(
            job,
            identities,
            None,
            semantic_materialization_generation,
            semantic_promotion_generation,
            restart,
        )
    }

    // As `materialize_admission`, writing the job's NZB outline in the same
    // transaction when it was read.
    fn materialize_admission_with_outline(
        &self,
        job: &ActiveJob,
        identities: &[ActiveFileIdentity],
        outline: Option<&weaver_server_ingest::ingest::outline::NzbOutline>,
        semantic_materialization_generation: Option<i64>,
        semantic_promotion_generation: Option<i64>,
        restart: bool,
    ) -> Result<bool, StateError> {
        let datastore = self.datastore();
        let job_id = job.job_id;
        let identities = identities.to_vec();
        let nzb_zstd: std::sync::Arc<[u8]> = job.nzb_zstd.as_slice().into();
        let outline = outline
            .map(|outline| {
                weaver_server_ingest::ingest::outline_persistence::OutlineHead::of(
                    self.encryption_key(),
                    outline,
                )
            })
            .transpose()?;
        let now = (weaver_server_jobs::jobs::model::epoch_ms_now() / 1_000.0) as i64;
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
                self.encryption_key(),
                job.password_override.as_deref(),
            )?),
            SqlArg::OptText(encrypt_supplied_passwords(
                self.encryption_key(),
                &job.supplied_password_candidates,
            )?),
        ];
        let result = self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(
                &datastore,
                "materialize_active_job_with_file_identities",
                |tx| {
                    let args = args.clone();
                    let identities = identities.clone();
                    let nzb_zstd = nzb_zstd.clone();
                    let outline = outline.clone();
                    Box::pin(async move {
                        if restart {
                            let deleted = crate::post_processing::history_bundles::delete_job_history_bundles_tx(tx, &[job_id]).await?;
                            if deleted.is_empty() {
                                return Err(StateError::Database("restart history row disappeared".into()));
                            }
                            weaver_server_jobs::jobs::child_rows::delete_active_job_child_rows_tx(tx, job_id).await?;
                        }
                        if let Some(generation) = semantic_materialization_generation {
                            let lock = match tx {
                                SqlTx::Postgres(_) => " FOR UPDATE",
                                SqlTx::Sqlite(_) => "",
                            };
                            let Some(group) = tx
                                .fetch_optional(
                                    "SELECT group_id FROM semantic_duplicate_candidates WHERE job_id = {}",
                                    &[SqlArg::I64(job_id.0 as i64)],
                                )
                                .await?
                            else {
                                return Ok(false);
                            };
                            tx.fetch_optional(
                                &format!(
                                    "SELECT group_id FROM semantic_duplicate_groups WHERE group_id = {{}}{lock}"
                                ),
                                &[SqlArg::I64(group.i64("group_id")?)],
                            )
                            .await?
                            .ok_or_else(|| {
                                StateError::Database(
                                    "semantic duplicate group disappeared during materialization"
                                        .to_string(),
                                )
                            })?;
                            let claimed = tx
                                .execute(
                                    "UPDATE semantic_duplicate_candidates
                                     SET updated_at = {}
                                     WHERE job_id = {} AND candidate_state = {}
                                       AND materialization_generation = {}",
                                    &[
                                        SqlArg::I64(now),
                                        SqlArg::I64(job_id.0 as i64),
                                        SqlArg::Text("active".to_string()),
                                        SqlArg::I64(generation),
                                    ],
                                )
                                .await?;
                            if claimed != 1 {
                                return Ok(false);
                            }
                        }
                        if let Some(generation) = semantic_promotion_generation {
                            let lock = match tx {
                                SqlTx::Postgres(_) => " FOR UPDATE",
                                SqlTx::Sqlite(_) => "",
                            };
                            let Some(group) = tx
                                .fetch_optional(
                                    "SELECT group_id FROM semantic_duplicate_candidates WHERE job_id = {}",
                                    &[SqlArg::I64(job_id.0 as i64)],
                                )
                                .await?
                            else {
                                return Ok(false);
                            };
                            tx.fetch_optional(
                                &format!(
                                    "SELECT group_id FROM semantic_duplicate_groups WHERE group_id = {{}}{lock}"
                                ),
                                &[SqlArg::I64(group.i64("group_id")?)],
                            )
                            .await?
                            .ok_or_else(|| {
                                StateError::Database(
                                    "semantic duplicate group disappeared during promotion"
                                        .to_string(),
                                )
                            })?;
                            let claimed = tx
                                .execute(
                                    "UPDATE semantic_duplicate_candidates
                                     SET updated_at = {}
                                     WHERE job_id = {} AND candidate_state = {}
                                       AND promotion_state = {} AND promotion_generation = {}
                                       AND promotion_lease_expires_at > {}",
                                    &[
                                        SqlArg::I64(now),
                                        SqlArg::I64(job_id.0 as i64),
                                        SqlArg::Text("active".to_string()),
                                        SqlArg::Text("claimed".to_string()),
                                        SqlArg::I64(generation),
                                        SqlArg::I64(now),
                                    ],
                                )
                                .await?;
                            if claimed != 1 {
                                return Ok(false);
                            }
                        }
                        tx.execute(
                            "UPDATE duplicate_job_snapshots
                             SET lifecycle = {}, reservation_expires_at = NULL, updated_at = {}
                             WHERE job_id = {}",
                            &[
                                SqlArg::Text("active".to_string()),
                                SqlArg::I64(now),
                                SqlArg::I64(job_id.0 as i64),
                            ],
                        )
                        .await?;
                        tx.execute(
                            "INSERT INTO active_jobs
                             (job_id, nzb_hash, nzb_path, output_dir, status, download_state, post_state, run_state, created_at, category, metadata, paused_resume_status, paused_resume_download_state, paused_resume_post_state, password, password_candidates)
                             VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})",
                            &args,
                        )
                        .await?;
                        weaver_server_jobs::jobs::nzb_persistence::write_active_job_nzb_tx(tx, job_id, &nzb_zstd)
                            .await?;
                        bulk_upsert_file_identities_tx(tx, job_id, &identities).await?;
                        if let Some(head) = &outline {
                            weaver_server_ingest::ingest::outline_persistence::write_outline_tx(tx, job_id, head).await?;
                        }
                        Ok(true)
                    })
                },
            )
            .await
        });
        if restart && matches!(result, Ok(true)) {
            self.forget_script_effects_for(&[job_id.0]);
            self.invalidate_job_history_cache_for(&[job_id.0]);
            self.notify_script_events_changed();
        }
        result
    }

    fn refresh_script_job_inputs(
        &self,
        job_id: u64,
        facts: &mut crate::post_processing::runner::CompatibilityFacts,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        let row = self.run_sql_blocking_read(async move {
            SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT metadata, password FROM active_jobs WHERE job_id = {}",
                &[SqlArg::I64(job_id as i64)],
            )
            .await
        })?;
        if let Some(row) = row {
            facts.parameters = row
                .opt_text("metadata")?
                .map(|text| serde_json::from_str(&text).map_err(db_err))
                .transpose()?
                .unwrap_or_default();
            facts.password =
                decrypt_archive_password(self.encryption_key(), row.opt_text("password")?)?;
        }
        facts.tags = self.job_tags(JobId(job_id))?;
        Ok(())
    }

    // Where the job stages the files it delivers; None when it stages
    // nothing apart from its working directory.
    fn set_active_job_staging_dir(
        &self,
        job_id: JobId,
        staging_dir: Option<&std::path::Path>,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        let args = vec![
            SqlArg::OptText(staging_dir.map(|dir| dir.to_string_lossy().into_owned())),
            SqlArg::I64(job_id.0 as i64),
        ];
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.write_exec(),
                "UPDATE active_jobs SET staging_dir = {} WHERE job_id = {}",
                &args,
            )
            .await?;
            Ok(())
        })
    }

    fn set_nzb_pause_release(&self, job_id: JobId, pending: bool) -> Result<(), StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "set_nzb_pause_release", |tx| {
                Box::pin(async move {
                    lock_active_job_for_write_tx(tx, job_id).await?;
                    tx.execute(
                        "UPDATE active_jobs SET nzb_release_pending = {} WHERE job_id = {}",
                        &[
                            SqlArg::I64(i64::from(pending)),
                            SqlArg::I64(job_id.0 as i64),
                        ],
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
        })
    }

    fn set_nzb_admitted(&self, job_id: JobId, admitted: bool) -> Result<(), StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "set_nzb_admitted", |tx| {
                Box::pin(async move {
                    lock_active_job_for_write_tx(tx, job_id).await?;
                    if admitted && !active_job_exists_tx(tx, job_id).await? {
                        return Err(StateError::Database(format!(
                            "cannot admit missing active NZB {}",
                            job_id.0
                        )));
                    }
                    tx.execute(
                        "UPDATE active_jobs SET nzb_admitted = {} WHERE job_id = {}",
                        &[
                            SqlArg::I64(i64::from(admitted)),
                            SqlArg::I64(job_id.0 as i64),
                        ],
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
        })
    }
}
