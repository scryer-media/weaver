use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use age::Encryptor;
use async_graphql::Result;
use chrono::Utc;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::sync::{Notify, RwLock};

use crate::auth::graphql_error;
use crate::history::query::load_job_detail_snapshot;
use crate::history::types::{
    DiagnosticRedownloadAcceptance, JobDetailSnapshot, history_diagnostic_stage_from_core,
};
use crate::system::types::PipelineEventGql;
use weaver_server_core::events::model::PipelineEvent;
use weaver_server_core::ingest::{decode_persisted_nzb_bytes, next_submission_job_id};
use weaver_server_core::operations::snapshot_service_logs;
use weaver_server_core::settings::{Config, SharedConfig};
use weaver_server_core::{
    Database, DiagnosticRunRow, DiagnosticRunStage, JobHistoryRow, JobId, SchedulerError,
    SchedulerHandle, diagnostic_cleanup_cutoff_ms, diagnostic_source_job_id,
};

const DIAGNOSTIC_ROOT_DIR: &str = ".diagnostics";
const DEFAULT_DIAGNOSTIC_UPLOAD_URL: &str = "https://diagnostics.scryer.media";
const EVENTS_FILE: &str = "diagnostic_attempt/events.ndjson";
const METRICS_FILE: &str = "diagnostic_attempt/metrics.ndjson";
const LOG_FILE: &str = "diagnostic_attempt/runtime.log";
const SERVER_ATTEMPTS_FILE: &str = "diagnostic_attempt/server_attempts.ndjson";
const UPLOAD_SESSION_FILE: &str = "upload_session.json";
const UPLOAD_RECEIPT_FILE: &str = "upload_receipt.json";

#[derive(Clone)]
pub(crate) struct DiagnosticManager {
    db: Database,
    handle: SchedulerHandle,
    config: SharedConfig,
    log_buffer: weaver_server_core::runtime::log_buffer::LogRingBuffer,
    http_client: reqwest::Client,
    wake: Arc<Notify>,
    tracked: Arc<RwLock<HashMap<u64, TrackedDiagnosticRun>>>,
}

#[derive(Debug, Clone)]
struct TrackedDiagnosticRun {
    source_job_id: u64,
    diagnostic_job_id: u64,
}

#[derive(Debug, Serialize)]
struct DiagnosticEventRecord {
    occurred_at_ms: i64,
    event: PipelineEventGql,
}

#[derive(Debug, Serialize)]
struct DiagnosticMetricsRecord {
    occurred_at_ms: i64,
    metrics: weaver_server_core::MetricsSnapshot,
    is_globally_paused: bool,
}

#[derive(Debug, Serialize)]
struct DiagnosticServerAttemptRecord {
    occurred_at_ms: i64,
    segment_id: String,
    file_id: String,
    server_id: u16,
    server_alias: String,
    attempt: u32,
    retry_count: u32,
    latency_ms: u64,
    outcome: String,
    error: Option<String>,
    is_recovery: bool,
}

#[derive(Debug, Serialize)]
struct DiagnosticManifest {
    source_job_id: u64,
    diagnostic_job_id: u64,
    include_server_hostnames: bool,
    created_at_ms: i64,
    rerun_succeeded: Option<bool>,
}

#[derive(Debug, Serialize)]
struct ComparisonSummary {
    source_job_id: u64,
    diagnostic_job_id: u64,
    first_attempt_status: Option<String>,
    first_attempt_error: Option<String>,
    diagnostic_attempt_status: Option<String>,
    diagnostic_attempt_error: Option<String>,
    first_attempt_event_count: usize,
    diagnostic_attempt_event_count: usize,
}

#[derive(Debug, Serialize)]
struct SanitizedBundleConfig {
    buffer_pool: Option<weaver_server_core::settings::BufferPoolOverrides>,
    tuner: Option<weaver_server_core::settings::TunerOverrides>,
    retry: Option<weaver_server_core::settings::RetryOverrides>,
    max_download_speed: Option<u64>,
    cleanup_after_extract: Option<bool>,
    isp_bandwidth_cap: Option<weaver_server_core::bandwidth::IspBandwidthCapConfig>,
    diagnostic_upload_url_present: bool,
    categories: Vec<SanitizedCategory>,
    servers: Vec<SanitizedServer>,
}

#[derive(Debug, Serialize)]
struct SanitizedCategory {
    id: u32,
    name: String,
    aliases: String,
    has_custom_dest_dir: bool,
}

#[derive(Debug, Serialize)]
struct SanitizedServer {
    id: u32,
    host: String,
    port: u16,
    tls: bool,
    active: bool,
    supports_pipelining: bool,
    priority: u32,
    connections: u16,
    has_username: bool,
    has_password: bool,
    tls_ca_cert_sha256: Option<String>,
}

#[derive(Debug, Serialize)]
struct DiagnosticServiceCreateUploadRequest {
    source_job_id: u64,
    diagnostic_job_id: u64,
    include_server_hostnames: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DiagnosticServiceCreateUploadResponse {
    diagnostic_id: String,
    public_key: String,
    key_id: String,
    #[serde(rename = "expires_at")]
    _expires_at: Option<String>,
}

#[derive(Debug, Serialize)]
struct DiagnosticServiceFinalizeUploadRequest<'a> {
    key_id: &'a str,
    size_bytes: u64,
    sha256: &'a str,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DiagnosticUploadReceipt {
    diagnostic_id: String,
    uploaded_at_ms: i64,
}

#[derive(Debug, Error)]
enum DiagnosticBundleError {
    #[error("diagnostic upload URL is not configured")]
    MissingUploadUrl,
    #[error("diagnostic run not found for job {0}")]
    MissingDiagnosticRun(u64),
    #[error("job {0} is not available in history or runtime")]
    MissingJob(u64),
    #[error("state error: {0}")]
    State(String),
    #[error("io error: {0}")]
    Io(String),
    #[error("http error: {0}")]
    Http(String),
    #[error("encryption error: {0}")]
    Encryption(String),
    #[error("serialization error: {0}")]
    Serialization(String),
}

impl DiagnosticManager {
    pub(crate) fn new(
        db: Database,
        handle: SchedulerHandle,
        config: SharedConfig,
        log_buffer: weaver_server_core::runtime::log_buffer::LogRingBuffer,
        http_client: reqwest::Client,
    ) -> Self {
        Self {
            db,
            handle,
            config,
            log_buffer,
            http_client,
            wake: Arc::new(Notify::new()),
            tracked: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub(crate) fn spawn_worker(&self) {
        let this = self.clone();
        tokio::spawn(async move {
            this.run_event_worker().await;
        });

        let this = self.clone();
        tokio::spawn(async move {
            this.run_log_worker().await;
        });

        let this = self.clone();
        tokio::spawn(async move {
            this.run_housekeeping_worker().await;
        });
    }

    pub(crate) async fn start_diagnostic_redownload(
        &self,
        source_job_id: u64,
        include_server_hostnames: bool,
    ) -> Result<DiagnosticRedownloadAcceptance> {
        if self.current_upload_url().await.is_none() {
            return Err(graphql_error(
                "DIAGNOSTICS_DISABLED",
                "diagnostic uploads are not configured",
            ));
        }

        let db = self.db.clone();
        let existing =
            tokio::task::spawn_blocking(move || db.get_diagnostic_run_for_source(source_job_id))
                .await
                .map_err(|error| graphql_error("INTERNAL", error.to_string()))?
                .map_err(|error| graphql_error("INTERNAL", error.to_string()))?;
        if existing.as_ref().is_some_and(|row| row.stage.is_active()) {
            return Err(graphql_error(
                "CONFLICT",
                "a diagnostic rerun is already active for this history item",
            ));
        }

        let diagnostic_job_id = next_submission_job_id().0;
        let now = Utc::now().timestamp_millis();
        let row = DiagnosticRunRow {
            source_job_id,
            diagnostic_job_id,
            diagnostic_id: None,
            stage: DiagnosticRunStage::Queued,
            include_server_hostnames,
            rerun_succeeded: None,
            error_message: None,
            created_at_epoch_ms: now,
            updated_at_epoch_ms: now,
            last_activity_at_epoch_ms: now,
        };

        let db = self.db.clone();
        let row_for_insert = row.clone();
        tokio::task::spawn_blocking(move || db.insert_diagnostic_run(&row_for_insert))
            .await
            .map_err(|error| graphql_error("INTERNAL", error.to_string()))?
            .map_err(|error| graphql_error("INTERNAL", error.to_string()))?;

        self.ensure_staging_dir(&row)
            .await
            .map_err(bundle_graphql_error)?;
        if let Err(error) = self
            .handle
            .start_diagnostic_redownload(
                JobId(source_job_id),
                JobId(diagnostic_job_id),
                include_server_hostnames,
            )
            .await
        {
            let db = self.db.clone();
            let cleanup_source_job_id = source_job_id;
            let _ = tokio::task::spawn_blocking(move || {
                db.delete_diagnostic_run(cleanup_source_job_id)
            })
            .await;
            let _ =
                tokio::fs::remove_dir_all(self.staging_dir(&TrackedDiagnosticRun::from_row(&row)))
                    .await;
            return Err(scheduler_graphql_error(error));
        }

        self.track_run(&row).await;
        self.wake.notify_one();

        Ok(DiagnosticRedownloadAcceptance {
            source_job_id,
            diagnostic_job_id,
            stage: history_diagnostic_stage_from_core(DiagnosticRunStage::Queued),
            include_server_hostnames,
        })
    }

    async fn run_event_worker(self) {
        let mut rx = self.handle.subscribe_events();
        loop {
            match rx.recv().await {
                Ok(event) => {
                    if let Err(error) = self.process_event(event).await {
                        tracing::error!(error = %error, "diagnostic event worker failed");
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                    tracing::warn!(skipped, "diagnostic event worker lagged");
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    }

    async fn run_log_worker(self) {
        let mut rx = self.log_buffer.subscribe();
        loop {
            match rx.recv().await {
                Ok(line) => {
                    let runs = self.active_runs().await;
                    for run in runs {
                        if let Err(error) = self
                            .append_text_line(self.staging_dir(&run).join(LOG_FILE), &line)
                            .await
                        {
                            tracing::warn!(
                                error = %error,
                                diagnostic_job_id = run.diagnostic_job_id,
                                "failed to append diagnostic log line"
                            );
                        }
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                    tracing::warn!(skipped, "diagnostic log worker lagged");
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    }

    async fn run_housekeeping_worker(self) {
        self.recover_pending_runs().await;
        self.cleanup_stale_runs().await;

        let mut metrics_interval = tokio::time::interval(Duration::from_secs(15));
        metrics_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut cleanup_interval = tokio::time::interval(Duration::from_secs(60 * 60));
        cleanup_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = self.wake.notified() => {
                    self.recover_pending_runs().await;
                }
                _ = metrics_interval.tick() => {
                    if let Err(error) = self.record_metrics_snapshots().await {
                        tracing::warn!(error = %error, "failed to record diagnostic metrics snapshot");
                    }
                }
                _ = cleanup_interval.tick() => {
                    self.cleanup_stale_runs().await;
                }
            }
        }
    }

    async fn process_event(
        &self,
        event: PipelineEvent,
    ) -> std::result::Result<(), DiagnosticBundleError> {
        let job_id = match &event {
            PipelineEvent::ServerAttempt { segment_id, .. } => Some(segment_id.file_id.job_id.0),
            _ => PipelineEventGql::from(&event).job_id,
        };
        let Some(job_id) = job_id else {
            return Ok(());
        };
        let Some(run) = self.lookup_run(job_id).await? else {
            return Ok(());
        };

        self.ensure_staging_dir_for_tracked(&run).await?;
        if let PipelineEvent::ServerAttempt {
            segment_id,
            server_id,
            attempt,
            retry_count,
            latency_ms,
            outcome,
            error,
            is_recovery,
        } = &event
        {
            let outcome = match outcome {
                weaver_server_core::events::model::ServerAttemptOutcome::Success => "success",
                weaver_server_core::events::model::ServerAttemptOutcome::NotFound => "not_found",
                weaver_server_core::events::model::ServerAttemptOutcome::AuthenticationFailure => {
                    "authentication_failure"
                }
                weaver_server_core::events::model::ServerAttemptOutcome::TransientFailure => {
                    "transient_failure"
                }
                weaver_server_core::events::model::ServerAttemptOutcome::PermanentFailure => {
                    "permanent_failure"
                }
            };
            self.append_json_line(
                self.staging_dir(&run).join(SERVER_ATTEMPTS_FILE),
                &DiagnosticServerAttemptRecord {
                    occurred_at_ms: Utc::now().timestamp_millis(),
                    segment_id: segment_id.to_string(),
                    file_id: segment_id.file_id.to_string(),
                    server_id: server_id.0,
                    server_alias: format!("server-{}", u32::from(server_id.0) + 1),
                    attempt: *attempt,
                    retry_count: *retry_count,
                    latency_ms: *latency_ms,
                    outcome: outcome.to_string(),
                    error: error.clone(),
                    is_recovery: *is_recovery,
                },
            )
            .await?;
            self.touch_run(run.source_job_id).await?;
            return Ok(());
        }

        let event_gql = PipelineEventGql::from(&event);
        self.append_json_line(
            self.staging_dir(&run).join(EVENTS_FILE),
            &DiagnosticEventRecord {
                occurred_at_ms: Utc::now().timestamp_millis(),
                event: event_gql.clone(),
            },
        )
        .await?;
        self.touch_run(run.source_job_id).await?;

        match &event {
            PipelineEvent::JobCreated { .. }
            | PipelineEvent::JobResumed { .. }
            | PipelineEvent::DownloadStarted { .. } => {
                self.update_stage(job_id, DiagnosticRunStage::Running, None, None, None)
                    .await?;
            }
            PipelineEvent::JobCompleted { .. } => {
                self.update_stage(
                    job_id,
                    DiagnosticRunStage::Collecting,
                    None,
                    Some(true),
                    None,
                )
                .await?;
                let this = self.clone();
                tokio::spawn(async move {
                    if let Err(error) = this.collect_and_upload(job_id).await {
                        tracing::error!(error = %error, diagnostic_job_id = job_id, "diagnostic upload failed");
                    }
                });
            }
            PipelineEvent::JobFailed { error, .. } => {
                self.update_stage(
                    job_id,
                    DiagnosticRunStage::Collecting,
                    None,
                    Some(false),
                    Some(error.clone()),
                )
                .await?;
                let this = self.clone();
                tokio::spawn(async move {
                    if let Err(error) = this.collect_and_upload(job_id).await {
                        tracing::error!(error = %error, diagnostic_job_id = job_id, "diagnostic upload failed");
                    }
                });
            }
            _ => {}
        }

        Ok(())
    }

    async fn recover_pending_runs(&self) {
        let db = self.db.clone();
        let rows = match tokio::task::spawn_blocking(move || db.list_pending_diagnostic_runs())
            .await
        {
            Ok(Ok(rows)) => rows,
            Ok(Err(error)) => {
                tracing::error!(error = %error, "failed to list pending diagnostic runs");
                return;
            }
            Err(error) => {
                tracing::error!(error = %error, "failed to join pending diagnostic run recovery");
                return;
            }
        };

        for row in rows {
            self.track_run(&row).await;
            if let Err(error) = self.ensure_staging_dir(&row).await {
                tracing::warn!(
                    error = %error,
                    diagnostic_job_id = row.diagnostic_job_id,
                    "failed to prepare diagnostic staging dir"
                );
            }
            let live = self.handle.get_job(JobId(row.diagnostic_job_id)).is_ok();
            if !live {
                let db = self.db.clone();
                let diagnostic_job_id = row.diagnostic_job_id;
                let history_exists =
                    tokio::task::spawn_blocking(move || db.get_job_history(diagnostic_job_id))
                        .await
                        .ok()
                        .and_then(|result| result.ok())
                        .flatten()
                        .is_some();
                if history_exists
                    || matches!(
                        row.stage,
                        DiagnosticRunStage::Collecting | DiagnosticRunStage::Uploading
                    )
                {
                    let this = self.clone();
                    tokio::spawn(async move {
                        if let Err(error) = this.collect_and_upload(diagnostic_job_id).await {
                            tracing::error!(
                                error = %error,
                                diagnostic_job_id,
                                "failed to recover pending diagnostic upload"
                            );
                        }
                    });
                }
            }
        }
    }

    async fn record_metrics_snapshots(&self) -> std::result::Result<(), DiagnosticBundleError> {
        let runs = self.active_runs().await;
        if runs.is_empty() {
            return Ok(());
        }
        let record = DiagnosticMetricsRecord {
            occurred_at_ms: Utc::now().timestamp_millis(),
            metrics: self.handle.get_metrics(),
            is_globally_paused: self.handle.is_globally_paused(),
        };
        for run in runs {
            self.append_json_line(self.staging_dir(&run).join(METRICS_FILE), &record)
                .await?;
        }
        Ok(())
    }

    async fn cleanup_stale_runs(&self) {
        let cutoff = diagnostic_cleanup_cutoff_ms(Utc::now().timestamp_millis());
        let db = self.db.clone();
        let rows = match tokio::task::spawn_blocking(move || db.list_stale_diagnostic_runs(cutoff))
            .await
        {
            Ok(Ok(rows)) => rows,
            Ok(Err(error)) => {
                tracing::error!(error = %error, "failed to list stale diagnostic runs");
                return;
            }
            Err(error) => {
                tracing::error!(error = %error, "failed to join stale diagnostic run cleanup");
                return;
            }
        };

        for row in rows {
            if self.handle.get_job(JobId(row.diagnostic_job_id)).is_ok() {
                continue;
            }
            if let Err(error) = self.delete_run_state(&row).await {
                tracing::warn!(
                    error = %error,
                    diagnostic_job_id = row.diagnostic_job_id,
                    "failed to purge stale diagnostic run"
                );
            }
        }
    }

    async fn collect_and_upload(
        &self,
        diagnostic_job_id: u64,
    ) -> std::result::Result<(), DiagnosticBundleError> {
        let mut run = self.load_run_by_job(diagnostic_job_id).await?.ok_or(
            DiagnosticBundleError::MissingDiagnosticRun(diagnostic_job_id),
        )?;
        let staging_dir = self.staging_dir(&TrackedDiagnosticRun::from_row(&run));
        if self
            .resume_finalized_upload_if_present(&run, &staging_dir)
            .await?
        {
            return Ok(());
        }
        self.ensure_staging_dir(&run).await?;
        self.update_stage(
            diagnostic_job_id,
            DiagnosticRunStage::Collecting,
            run.diagnostic_id.clone(),
            run.rerun_succeeded,
            run.error_message.clone(),
        )
        .await?;

        let source_snapshot = self.load_snapshot(run.source_job_id).await?;
        let diagnostic_snapshot = self.load_snapshot(diagnostic_job_id).await?;
        let source_history_row = self
            .load_history_row(run.source_job_id)
            .await?
            .ok_or(DiagnosticBundleError::MissingJob(run.source_job_id))?;
        let diagnostic_history_row = self
            .load_history_row(diagnostic_job_id)
            .await?
            .ok_or(DiagnosticBundleError::MissingJob(diagnostic_job_id))?;
        let config = self.current_config().await;
        let redactions = build_redactions(
            &config,
            &source_history_row,
            &diagnostic_history_row,
            run.include_server_hostnames,
        );

        let bundle_dir = staging_dir.join("bundle");
        if tokio::fs::try_exists(&bundle_dir).await.unwrap_or(false) {
            let _ = tokio::fs::remove_dir_all(&bundle_dir).await;
        }
        create_dir(&bundle_dir).await?;

        write_json_file(
            &bundle_dir.join("manifest.json"),
            &DiagnosticManifest {
                source_job_id: run.source_job_id,
                diagnostic_job_id,
                include_server_hostnames: run.include_server_hostnames,
                created_at_ms: Utc::now().timestamp_millis(),
                rerun_succeeded: run.rerun_succeeded,
            },
        )
        .await?;

        let mut source_snapshot = source_snapshot;
        sanitize_snapshot(&mut source_snapshot);
        write_json_file(
            &bundle_dir.join("first_attempt/job_detail_snapshot.json"),
            &source_snapshot,
        )
        .await?;
        let source_raw_nzb = self
            .load_raw_nzb_text(run.source_job_id, source_history_row.nzb_path.as_deref())
            .await?;
        write_raw_nzb_file(
            bundle_dir.join("first_attempt/raw.nzb.xml"),
            source_raw_nzb.as_deref(),
            &redactions,
        )
        .await?;

        let mut diagnostic_snapshot = diagnostic_snapshot;
        sanitize_snapshot(&mut diagnostic_snapshot);
        write_json_file(
            &bundle_dir.join("diagnostic_attempt/job_detail_snapshot.json"),
            &diagnostic_snapshot,
        )
        .await?;
        let diagnostic_raw_nzb = self
            .load_raw_nzb_text(
                diagnostic_job_id,
                diagnostic_history_row.nzb_path.as_deref(),
            )
            .await?;
        write_raw_nzb_file(
            bundle_dir.join("diagnostic_attempt/raw.nzb.xml"),
            diagnostic_raw_nzb.as_deref(),
            &redactions,
        )
        .await?;
        copy_if_exists(
            staging_dir.join(EVENTS_FILE),
            bundle_dir.join(EVENTS_FILE),
            &redactions,
        )
        .await?;
        copy_if_exists(
            staging_dir.join(METRICS_FILE),
            bundle_dir.join(METRICS_FILE),
            &redactions,
        )
        .await?;
        copy_if_exists(
            staging_dir.join(LOG_FILE),
            bundle_dir.join(LOG_FILE),
            &redactions,
        )
        .await?;
        copy_if_exists(
            staging_dir.join(SERVER_ATTEMPTS_FILE),
            bundle_dir.join(SERVER_ATTEMPTS_FILE),
            &redactions,
        )
        .await?;

        let log_snapshot = snapshot_service_logs(&self.log_buffer, 2000)
            .into_iter()
            .map(|line| redactions.apply_to_text(&line))
            .collect::<Vec<_>>()
            .join("\n");
        write_text_file(
            &bundle_dir.join("diagnostic_attempt/log_snapshot.log"),
            &log_snapshot,
        )
        .await?;

        write_json_file(
            &bundle_dir.join("comparison/summary.json"),
            &ComparisonSummary {
                source_job_id: run.source_job_id,
                diagnostic_job_id,
                first_attempt_status: source_snapshot
                    .history_item
                    .as_ref()
                    .map(|item| format!("{:?}", item.state)),
                first_attempt_error: source_snapshot
                    .history_item
                    .as_ref()
                    .and_then(|item| item.error.clone()),
                diagnostic_attempt_status: diagnostic_snapshot
                    .history_item
                    .as_ref()
                    .map(|item| format!("{:?}", item.state))
                    .or_else(|| {
                        diagnostic_snapshot
                            .queue_item
                            .as_ref()
                            .map(|item| format!("{:?}", item.state))
                    }),
                diagnostic_attempt_error: diagnostic_snapshot
                    .history_item
                    .as_ref()
                    .and_then(|item| item.error.clone())
                    .or_else(|| {
                        diagnostic_snapshot
                            .queue_item
                            .as_ref()
                            .and_then(|item| item.error.clone())
                    }),
                first_attempt_event_count: source_snapshot.job_events.len(),
                diagnostic_attempt_event_count: diagnostic_snapshot.job_events.len(),
            },
        )
        .await?;

        let sanitized_config = sanitize_config(&config, run.include_server_hostnames);
        write_json_file(&bundle_dir.join("config/config.json"), &sanitized_config).await?;

        let archive_path = staging_dir.join("bundle.tar.zst");
        tar_zstd_directory(&bundle_dir, &archive_path).await?;

        let upload_url = self
            .current_upload_url()
            .await
            .ok_or(DiagnosticBundleError::MissingUploadUrl)?;
        let session = self
            .load_or_create_upload_session(&upload_url, &mut run, &staging_dir)
            .await?;
        self.update_stage(
            diagnostic_job_id,
            DiagnosticRunStage::Uploading,
            run.diagnostic_id.clone(),
            run.rerun_succeeded,
            run.error_message.clone(),
        )
        .await?;

        let ciphertext_path = staging_dir.join("bundle.tar.zst.age");
        encrypt_bundle(&archive_path, &ciphertext_path, &session.public_key).await?;
        let (size_bytes, sha256_hex) = file_size_and_sha256(&ciphertext_path).await?;
        self.upload_ciphertext(&upload_url, &session.diagnostic_id, &ciphertext_path)
            .await?;
        self.finalize_upload(
            &upload_url,
            &session.diagnostic_id,
            &session.key_id,
            size_bytes,
            &sha256_hex,
        )
        .await?;

        let uploaded_at = Utc::now().timestamp_millis();
        let receipt = DiagnosticUploadReceipt {
            diagnostic_id: session.diagnostic_id.clone(),
            uploaded_at_ms: uploaded_at,
        };
        write_json_file(&staging_dir.join(UPLOAD_RECEIPT_FILE), &receipt).await?;
        self.persist_diagnostic_receipt_and_cleanup(&run, &receipt)
            .await?;
        Ok(())
    }

    async fn load_or_create_upload_session(
        &self,
        base_url: &str,
        run: &mut DiagnosticRunRow,
        staging_dir: &Path,
    ) -> std::result::Result<DiagnosticServiceCreateUploadResponse, DiagnosticBundleError> {
        let session_path = staging_dir.join(UPLOAD_SESSION_FILE);
        if let Some(session) =
            read_json_file_if_exists::<DiagnosticServiceCreateUploadResponse>(&session_path).await?
        {
            if let Some(existing) = &run.diagnostic_id
                && existing != &session.diagnostic_id
            {
                return Err(DiagnosticBundleError::State(
                    "diagnostic upload session id did not match local run state".to_string(),
                ));
            }
            if run.diagnostic_id.is_none() {
                run.diagnostic_id = Some(session.diagnostic_id.clone());
                self.update_stage(
                    run.diagnostic_job_id,
                    run.stage,
                    run.diagnostic_id.clone(),
                    run.rerun_succeeded,
                    run.error_message.clone(),
                )
                .await?;
            }
            return Ok(session);
        }

        let session = self.create_upload_session(base_url, run).await?;
        write_json_file(&session_path, &session).await?;
        run.diagnostic_id = Some(session.diagnostic_id.clone());
        self.update_stage(
            run.diagnostic_job_id,
            run.stage,
            run.diagnostic_id.clone(),
            run.rerun_succeeded,
            run.error_message.clone(),
        )
        .await?;
        Ok(session)
    }

    async fn create_upload_session(
        &self,
        base_url: &str,
        run: &DiagnosticRunRow,
    ) -> std::result::Result<DiagnosticServiceCreateUploadResponse, DiagnosticBundleError> {
        let url = format!(
            "{}/api/v1/metadata/diagnostics",
            base_url.trim_end_matches('/')
        );
        let response = self
            .http_client
            .post(url)
            .json(&DiagnosticServiceCreateUploadRequest {
                source_job_id: run.source_job_id,
                diagnostic_job_id: run.diagnostic_job_id,
                include_server_hostnames: run.include_server_hostnames,
            })
            .send()
            .await
            .map_err(|error| DiagnosticBundleError::Http(error.to_string()))?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(DiagnosticBundleError::Http(format!(
                "diagnostic service create upload failed: {status} {body}",
            )));
        }
        response
            .json()
            .await
            .map_err(|error| DiagnosticBundleError::Http(error.to_string()))
    }

    async fn upload_ciphertext(
        &self,
        base_url: &str,
        diagnostic_id: &str,
        ciphertext_path: &Path,
    ) -> std::result::Result<(), DiagnosticBundleError> {
        let url = format!(
            "{}/api/v1/metadata/diagnostics/{diagnostic_id}/bundle",
            base_url.trim_end_matches('/')
        );
        let bytes = tokio::fs::read(ciphertext_path)
            .await
            .map_err(|error| DiagnosticBundleError::Io(error.to_string()))?;
        let response = self
            .http_client
            .put(url)
            .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
            .body(bytes)
            .send()
            .await
            .map_err(|error| DiagnosticBundleError::Http(error.to_string()))?;
        let status = response.status();
        if status.is_success() {
            Ok(())
        } else {
            let body = response.text().await.unwrap_or_default();
            Err(DiagnosticBundleError::Http(format!(
                "diagnostic service upload failed: {status} {body}",
            )))
        }
    }

    async fn finalize_upload(
        &self,
        base_url: &str,
        diagnostic_id: &str,
        key_id: &str,
        size_bytes: u64,
        sha256_hex: &str,
    ) -> std::result::Result<(), DiagnosticBundleError> {
        let url = format!(
            "{}/api/v1/metadata/diagnostics/{diagnostic_id}/finalize",
            base_url.trim_end_matches('/')
        );
        let response = self
            .http_client
            .post(url)
            .json(&DiagnosticServiceFinalizeUploadRequest {
                key_id,
                size_bytes,
                sha256: sha256_hex,
            })
            .send()
            .await
            .map_err(|error| DiagnosticBundleError::Http(error.to_string()))?;
        let status = response.status();
        if status.is_success() {
            Ok(())
        } else {
            let body = response.text().await.unwrap_or_default();
            Err(DiagnosticBundleError::Http(format!(
                "diagnostic service finalize failed: {status} {body}",
            )))
        }
    }

    async fn load_snapshot(
        &self,
        job_id: u64,
    ) -> std::result::Result<JobDetailSnapshot, DiagnosticBundleError> {
        for _ in 0..20 {
            match load_job_detail_snapshot(self.handle.clone(), self.db.clone(), job_id).await {
                Ok(snapshot)
                    if snapshot.history_item.is_some() || snapshot.queue_item.is_some() =>
                {
                    return Ok(snapshot);
                }
                Ok(_) => {}
                Err(_) => {}
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        load_job_detail_snapshot(self.handle.clone(), self.db.clone(), job_id)
            .await
            .map_err(|error| DiagnosticBundleError::State(format!("{error:?}")))
    }

    async fn load_history_row(
        &self,
        job_id: u64,
    ) -> std::result::Result<Option<JobHistoryRow>, DiagnosticBundleError> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || db.get_job_history(job_id))
            .await
            .map_err(|error| DiagnosticBundleError::State(error.to_string()))?
            .map_err(|error| DiagnosticBundleError::State(error.to_string()))
    }

    async fn load_raw_nzb_text(
        &self,
        job_id: u64,
        fallback_path: Option<&str>,
    ) -> std::result::Result<Option<String>, DiagnosticBundleError> {
        let db = self.db.clone();
        let persisted =
            tokio::task::spawn_blocking(move || db.load_history_job_persisted_nzb(job_id))
                .await
                .map_err(|error| DiagnosticBundleError::State(error.to_string()))?
                .map_err(|error| DiagnosticBundleError::State(error.to_string()))?;
        if let Some((_, Some(nzb_zstd))) = persisted {
            let decoded = decode_persisted_nzb_bytes(&nzb_zstd)
                .map_err(|error| DiagnosticBundleError::Io(error.to_string()))?;
            let content = String::from_utf8(decoded)
                .map_err(|error| DiagnosticBundleError::Io(error.to_string()))?;
            return Ok(Some(content));
        }

        let Some(nzb_path) = fallback_path else {
            return Ok(None);
        };
        match tokio::fs::read_to_string(nzb_path).await {
            Ok(content) => Ok(Some(content)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(DiagnosticBundleError::Io(error.to_string())),
        }
    }

    async fn load_run_by_job(
        &self,
        diagnostic_job_id: u64,
    ) -> std::result::Result<Option<DiagnosticRunRow>, DiagnosticBundleError> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || db.get_diagnostic_run_by_job(diagnostic_job_id))
            .await
            .map_err(|error| DiagnosticBundleError::State(error.to_string()))?
            .map_err(|error| DiagnosticBundleError::State(error.to_string()))
    }

    async fn update_stage(
        &self,
        diagnostic_job_id: u64,
        stage: DiagnosticRunStage,
        diagnostic_id: Option<String>,
        rerun_succeeded: Option<bool>,
        error_message: Option<String>,
    ) -> std::result::Result<(), DiagnosticBundleError> {
        let Some(mut row) = self.load_run_by_job(diagnostic_job_id).await? else {
            return Ok(());
        };
        row.stage = stage;
        row.diagnostic_id = diagnostic_id;
        row.rerun_succeeded = rerun_succeeded;
        row.error_message = error_message;
        let now = Utc::now().timestamp_millis();
        row.updated_at_epoch_ms = now;
        row.last_activity_at_epoch_ms = now;
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || db.update_diagnostic_run(&row))
            .await
            .map_err(|error| DiagnosticBundleError::State(error.to_string()))?
            .map_err(|error| DiagnosticBundleError::State(error.to_string()))?;
        Ok(())
    }

    async fn touch_run(
        &self,
        source_job_id: u64,
    ) -> std::result::Result<(), DiagnosticBundleError> {
        let db = self.db.clone();
        let now = Utc::now().timestamp_millis();
        tokio::task::spawn_blocking(move || db.touch_diagnostic_run(source_job_id, now))
            .await
            .map_err(|error| DiagnosticBundleError::State(error.to_string()))?
            .map_err(|error| DiagnosticBundleError::State(error.to_string()))?;
        Ok(())
    }

    async fn delete_run_state(
        &self,
        row: &DiagnosticRunRow,
    ) -> std::result::Result<(), DiagnosticBundleError> {
        let db = self.db.clone();
        let source_job_id = row.source_job_id;
        tokio::task::spawn_blocking(move || db.delete_diagnostic_run(source_job_id))
            .await
            .map_err(|error| DiagnosticBundleError::State(error.to_string()))?
            .map_err(|error| DiagnosticBundleError::State(error.to_string()))?;
        self.tracked.write().await.remove(&row.diagnostic_job_id);
        let _ =
            tokio::fs::remove_dir_all(self.staging_dir(&TrackedDiagnosticRun::from_row(row))).await;
        Ok(())
    }

    async fn resume_finalized_upload_if_present(
        &self,
        run: &DiagnosticRunRow,
        staging_dir: &Path,
    ) -> std::result::Result<bool, DiagnosticBundleError> {
        let Some(receipt) = read_json_file_if_exists::<DiagnosticUploadReceipt>(
            &staging_dir.join(UPLOAD_RECEIPT_FILE),
        )
        .await?
        else {
            return Ok(false);
        };
        self.persist_diagnostic_receipt_and_cleanup(run, &receipt)
            .await?;
        Ok(true)
    }

    async fn persist_diagnostic_receipt_and_cleanup(
        &self,
        run: &DiagnosticRunRow,
        receipt: &DiagnosticUploadReceipt,
    ) -> std::result::Result<(), DiagnosticBundleError> {
        let db = self.db.clone();
        let source_job_id = run.source_job_id;
        let diagnostic_id = receipt.diagnostic_id.clone();
        let uploaded_at_ms = receipt.uploaded_at_ms;
        tokio::task::spawn_blocking(move || {
            db.persist_job_history_diagnostic_receipt(source_job_id, &diagnostic_id, uploaded_at_ms)
        })
        .await
        .map_err(|error| DiagnosticBundleError::State(error.to_string()))?
        .map_err(|error| DiagnosticBundleError::State(error.to_string()))?;
        self.delete_run_state(run).await
    }

    async fn lookup_run(
        &self,
        diagnostic_job_id: u64,
    ) -> std::result::Result<Option<TrackedDiagnosticRun>, DiagnosticBundleError> {
        if let Some(run) = self.tracked.read().await.get(&diagnostic_job_id).cloned() {
            return Ok(Some(run));
        }
        if let Some(row) = self.load_run_by_job(diagnostic_job_id).await? {
            self.track_run(&row).await;
            return Ok(Some(TrackedDiagnosticRun::from_row(&row)));
        }
        let live_job = self.handle.get_job(JobId(diagnostic_job_id)).ok();
        if let Some(job) = live_job
            && let Some(source_job_id) = diagnostic_source_job_id(&job.metadata)
        {
            let run = TrackedDiagnosticRun {
                source_job_id,
                diagnostic_job_id,
            };
            self.tracked
                .write()
                .await
                .insert(diagnostic_job_id, run.clone());
            return Ok(Some(run));
        }
        Ok(None)
    }

    async fn track_run(&self, row: &DiagnosticRunRow) {
        self.tracked
            .write()
            .await
            .insert(row.diagnostic_job_id, TrackedDiagnosticRun::from_row(row));
    }

    async fn active_runs(&self) -> Vec<TrackedDiagnosticRun> {
        self.tracked.read().await.values().cloned().collect()
    }

    async fn current_upload_url(&self) -> Option<String> {
        resolved_diagnostic_upload_url(self.config.read().await.diagnostic_upload_url.as_deref())
    }

    async fn current_config(&self) -> Config {
        self.config.read().await.clone()
    }

    fn staging_dir(&self, run: &TrackedDiagnosticRun) -> PathBuf {
        let root = futures_staging_root(&self.config);
        root.join(format!("{}-{}", run.source_job_id, run.diagnostic_job_id))
    }

    async fn ensure_staging_dir(
        &self,
        row: &DiagnosticRunRow,
    ) -> std::result::Result<(), DiagnosticBundleError> {
        self.ensure_staging_dir_for_tracked(&TrackedDiagnosticRun::from_row(row))
            .await
    }

    async fn ensure_staging_dir_for_tracked(
        &self,
        run: &TrackedDiagnosticRun,
    ) -> std::result::Result<(), DiagnosticBundleError> {
        create_dir(&self.staging_dir(run)).await
    }

    async fn append_json_line<T: Serialize>(
        &self,
        path: PathBuf,
        value: &T,
    ) -> std::result::Result<(), DiagnosticBundleError> {
        let mut line = serde_json::to_vec(value)
            .map_err(|error| DiagnosticBundleError::Serialization(error.to_string()))?;
        line.push(b'\n');
        self.append_bytes(path, &line).await
    }

    async fn append_text_line(
        &self,
        path: PathBuf,
        line: &str,
    ) -> std::result::Result<(), DiagnosticBundleError> {
        let mut content = line.as_bytes().to_vec();
        content.push(b'\n');
        self.append_bytes(path, &content).await
    }

    async fn append_bytes(
        &self,
        path: PathBuf,
        bytes: &[u8],
    ) -> std::result::Result<(), DiagnosticBundleError> {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|error| DiagnosticBundleError::Io(error.to_string()))?;
        }
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await
            .map_err(|error| DiagnosticBundleError::Io(error.to_string()))?;
        file.write_all(bytes)
            .await
            .map_err(|error| DiagnosticBundleError::Io(error.to_string()))
    }
}

impl TrackedDiagnosticRun {
    fn from_row(row: &DiagnosticRunRow) -> Self {
        Self {
            source_job_id: row.source_job_id,
            diagnostic_job_id: row.diagnostic_job_id,
        }
    }
}

#[derive(Debug, Clone, Default)]
struct RedactionPlan {
    replacements: Vec<(String, String)>,
}

impl RedactionPlan {
    fn apply_to_text(&self, value: &str) -> String {
        let mut redacted = value.to_string();
        for (from, to) in &self.replacements {
            if !from.is_empty() {
                redacted = redacted.replace(from, to);
            }
        }
        if let Ok(ip_regex) = regex::Regex::new(r"\b(?:\d{1,3}\.){3}\d{1,3}\b") {
            redacted = ip_regex.replace_all(&redacted, "[redacted-ip]").to_string();
        }
        redacted
    }
}

fn build_redactions(
    config: &Config,
    source_history: &JobHistoryRow,
    diagnostic_history: &JobHistoryRow,
    include_server_hostnames: bool,
) -> RedactionPlan {
    let mut replacements = Vec::new();
    replacements.push((config.data_dir.clone(), "[redacted-path]".to_string()));
    if let Some(path) = &config.intermediate_dir {
        replacements.push((path.clone(), "[redacted-path]".to_string()));
    }
    if let Some(path) = &config.complete_dir {
        replacements.push((path.clone(), "[redacted-path]".to_string()));
    }
    for category in &config.categories {
        if let Some(dest_dir) = &category.dest_dir {
            replacements.push((dest_dir.clone(), "[redacted-path]".to_string()));
        }
    }
    for (index, server) in config.servers.iter().enumerate() {
        if !include_server_hostnames {
            replacements.push((server.host.clone(), format!("server-{}", index + 1)));
        }
        if let Some(username) = &server.username {
            replacements.push((username.clone(), "[redacted-username]".to_string()));
        }
        if let Some(password) = &server.password {
            replacements.push((password.clone(), "[redacted-password]".to_string()));
        }
        if let Some(path) = &server.tls_ca_cert {
            replacements.push((
                path.to_string_lossy().to_string(),
                "[redacted-cert-path]".to_string(),
            ));
        }
    }
    for path in [
        source_history.output_dir.as_deref(),
        source_history.nzb_path.as_deref(),
        diagnostic_history.output_dir.as_deref(),
        diagnostic_history.nzb_path.as_deref(),
    ]
    .into_iter()
    .flatten()
    {
        replacements.push((path.to_string(), "[redacted-path]".to_string()));
    }
    RedactionPlan { replacements }
}

fn sanitize_snapshot(snapshot: &mut JobDetailSnapshot) {
    if let Some(queue_item) = snapshot.queue_item.as_mut() {
        queue_item.output_dir = None;
    }
    if let Some(history_item) = snapshot.history_item.as_mut() {
        history_item.output_dir = None;
    }
}

fn sanitize_config(config: &Config, include_server_hostnames: bool) -> SanitizedBundleConfig {
    SanitizedBundleConfig {
        buffer_pool: config.buffer_pool.clone(),
        tuner: config.tuner.clone(),
        retry: config.retry.clone(),
        max_download_speed: config.max_download_speed,
        cleanup_after_extract: config.cleanup_after_extract,
        isp_bandwidth_cap: config.isp_bandwidth_cap.clone(),
        diagnostic_upload_url_present: resolved_diagnostic_upload_url(
            config.diagnostic_upload_url.as_deref(),
        )
        .is_some(),
        categories: config
            .categories
            .iter()
            .map(|category| SanitizedCategory {
                id: category.id,
                name: category.name.clone(),
                aliases: category.aliases.clone(),
                has_custom_dest_dir: category.dest_dir.is_some(),
            })
            .collect(),
        servers: config
            .servers
            .iter()
            .enumerate()
            .map(|(index, server)| SanitizedServer {
                id: server.id,
                host: if include_server_hostnames {
                    server.host.clone()
                } else {
                    format!("server-{}", index + 1)
                },
                port: server.port,
                tls: server.tls,
                active: server.active,
                supports_pipelining: server.supports_pipelining,
                priority: server.priority,
                connections: server.connections,
                has_username: server.username.is_some(),
                has_password: server.password.is_some(),
                tls_ca_cert_sha256: server
                    .tls_ca_cert
                    .as_ref()
                    .and_then(|path| std::fs::read(path).ok())
                    .map(|bytes| hex::encode(Sha256::digest(bytes))),
            })
            .collect(),
    }
}

async fn create_dir(path: &Path) -> std::result::Result<(), DiagnosticBundleError> {
    tokio::fs::create_dir_all(path)
        .await
        .map_err(|error| DiagnosticBundleError::Io(error.to_string()))
}

async fn write_json_file<T: Serialize>(
    path: &Path,
    value: &T,
) -> std::result::Result<(), DiagnosticBundleError> {
    if let Some(parent) = path.parent() {
        create_dir(parent).await?;
    }
    let bytes = serde_json::to_vec_pretty(value)
        .map_err(|error| DiagnosticBundleError::Serialization(error.to_string()))?;
    tokio::fs::write(path, bytes)
        .await
        .map_err(|error| DiagnosticBundleError::Io(error.to_string()))
}

async fn write_text_file(
    path: &Path,
    value: &str,
) -> std::result::Result<(), DiagnosticBundleError> {
    if let Some(parent) = path.parent() {
        create_dir(parent).await?;
    }
    tokio::fs::write(path, value)
        .await
        .map_err(|error| DiagnosticBundleError::Io(error.to_string()))
}

async fn read_json_file_if_exists<T: DeserializeOwned>(
    path: &Path,
) -> std::result::Result<Option<T>, DiagnosticBundleError> {
    let bytes = match tokio::fs::read(path).await {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(DiagnosticBundleError::Io(error.to_string())),
    };
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|error| DiagnosticBundleError::Serialization(error.to_string()))
}

async fn write_raw_nzb_file(
    destination: PathBuf,
    content: Option<&str>,
    redactions: &RedactionPlan,
) -> std::result::Result<(), DiagnosticBundleError> {
    let Some(content) = content else {
        return Ok(());
    };
    write_text_file(&destination, &redactions.apply_to_text(content)).await
}

async fn copy_if_exists(
    source: PathBuf,
    destination: PathBuf,
    redactions: &RedactionPlan,
) -> std::result::Result<(), DiagnosticBundleError> {
    let content = match tokio::fs::read_to_string(&source).await {
        Ok(content) => content,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(DiagnosticBundleError::Io(error.to_string())),
    };
    write_text_file(&destination, &redactions.apply_to_text(&content)).await
}

async fn tar_zstd_directory(
    directory: &Path,
    archive_path: &Path,
) -> std::result::Result<(), DiagnosticBundleError> {
    let directory = directory.to_path_buf();
    let archive_path = archive_path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let file = std::fs::File::create(&archive_path)?;
        let encoder = zstd::Encoder::new(file, 9)?;
        let mut tar = tar::Builder::new(encoder.auto_finish());
        tar.append_dir_all(".", &directory)?;
        tar.finish()?;
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    })
    .await
    .map_err(|error| DiagnosticBundleError::Io(error.to_string()))?
    .map_err(|error| DiagnosticBundleError::Io(error.to_string()))
}

async fn encrypt_bundle(
    plaintext_path: &Path,
    ciphertext_path: &Path,
    recipient: &str,
) -> std::result::Result<(), DiagnosticBundleError> {
    let plaintext_path = plaintext_path.to_path_buf();
    let ciphertext_path = ciphertext_path.to_path_buf();
    let recipient = recipient.to_string();
    tokio::task::spawn_blocking(move || {
        let recipient = recipient
            .parse::<age::x25519::Recipient>()
            .map_err(|error| DiagnosticBundleError::Encryption(error.to_string()))?;
        let recipients = [&recipient as &dyn age::Recipient];
        let encryptor = Encryptor::with_recipients(recipients.into_iter())
            .map_err(|error| DiagnosticBundleError::Encryption(error.to_string()))?;
        let input = std::fs::File::open(&plaintext_path)
            .map_err(|error| DiagnosticBundleError::Io(error.to_string()))?;
        let output = std::fs::File::create(&ciphertext_path)
            .map_err(|error| DiagnosticBundleError::Io(error.to_string()))?;
        let mut writer = encryptor
            .wrap_output(output)
            .map_err(|error| DiagnosticBundleError::Encryption(error.to_string()))?;
        let mut reader = std::io::BufReader::new(input);
        std::io::copy(&mut reader, &mut writer)
            .map_err(|error| DiagnosticBundleError::Io(error.to_string()))?;
        writer
            .finish()
            .map_err(|error| DiagnosticBundleError::Encryption(error.to_string()))?;
        Ok::<(), DiagnosticBundleError>(())
    })
    .await
    .map_err(|error| DiagnosticBundleError::Encryption(error.to_string()))?
}

async fn file_size_and_sha256(
    path: &Path,
) -> std::result::Result<(u64, String), DiagnosticBundleError> {
    let bytes = tokio::fs::read(path)
        .await
        .map_err(|error| DiagnosticBundleError::Io(error.to_string()))?;
    let sha = Sha256::digest(&bytes);
    Ok((bytes.len() as u64, hex::encode(sha)))
}

fn futures_staging_root(config: &SharedConfig) -> PathBuf {
    let guard = config.blocking_read();
    Path::new(&guard.data_dir).join(DIAGNOSTIC_ROOT_DIR)
}

fn scheduler_graphql_error(error: SchedulerError) -> async_graphql::Error {
    match error {
        SchedulerError::JobNotFound(_) => graphql_error("NOT_FOUND", error.to_string()),
        SchedulerError::InvalidInput(_) => graphql_error("INVALID_INPUT", error.to_string()),
        SchedulerError::JobExists(_) => graphql_error("CONFLICT", error.to_string()),
        SchedulerError::Conflict(_) => graphql_error("CONFLICT", error.to_string()),
        SchedulerError::ChannelClosed => graphql_error("INTERNAL", error.to_string()),
        SchedulerError::Assembly(_)
        | SchedulerError::Io(_)
        | SchedulerError::State(_)
        | SchedulerError::Internal(_)
        | SchedulerError::Other(_) => graphql_error("INTERNAL", error.to_string()),
    }
}

fn resolved_diagnostic_upload_url(configured_url: Option<&str>) -> Option<String> {
    configured_url
        .map(str::trim)
        .filter(|url| !url.is_empty())
        .map(str::to_owned)
        .or_else(|| Some(DEFAULT_DIAGNOSTIC_UPLOAD_URL.to_string()))
}

fn bundle_graphql_error(error: DiagnosticBundleError) -> async_graphql::Error {
    match error {
        DiagnosticBundleError::MissingUploadUrl => {
            graphql_error("DIAGNOSTICS_DISABLED", error.to_string())
        }
        DiagnosticBundleError::MissingDiagnosticRun(_) | DiagnosticBundleError::MissingJob(_) => {
            graphql_error("NOT_FOUND", error.to_string())
        }
        DiagnosticBundleError::State(_)
        | DiagnosticBundleError::Io(_)
        | DiagnosticBundleError::Http(_)
        | DiagnosticBundleError::Encryption(_)
        | DiagnosticBundleError::Serialization(_) => graphql_error("INTERNAL", error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::{DEFAULT_DIAGNOSTIC_UPLOAD_URL, resolved_diagnostic_upload_url};

    #[test]
    fn defaults_to_public_diagnostics_service_when_unset() {
        assert_eq!(
            resolved_diagnostic_upload_url(None).as_deref(),
            Some(DEFAULT_DIAGNOSTIC_UPLOAD_URL),
        );
        assert_eq!(
            resolved_diagnostic_upload_url(Some("   ")).as_deref(),
            Some(DEFAULT_DIAGNOSTIC_UPLOAD_URL),
        );
    }

    #[test]
    fn preserves_trimmed_explicit_diagnostics_override() {
        assert_eq!(
            resolved_diagnostic_upload_url(Some(" https://example.test/diagnostics ")).as_deref(),
            Some("https://example.test/diagnostics"),
        );
    }
}
