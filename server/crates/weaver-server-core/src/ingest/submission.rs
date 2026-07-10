use std::future::Future;
use std::io::Read;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use tracing::{info, warn};

use crate::Database;
use crate::jobs::JobSpec;
use crate::jobs::ids::JobId;
use crate::security::{ResolvedFetchTarget, RuntimeSecurityConfig, resolve_fetch_target};
use crate::settings::SharedConfig;
use crate::{SchedulerError, SchedulerHandle};
use weaver_nzb::Nzb;

use super::persisted_nzb;
use crate::ingest::{append_original_title_metadata, derive_release_name, nzb_password_candidates};
use crate::jobs::{
    CallerScopedIdempotency, DuplicateAction, DuplicateAdmission, DuplicateAdmissionRequest,
    DuplicateBackfillEntry, DuplicateDecision, DuplicateMode, FileSpec, FingerprintEvidence,
    SegmentSpec, SemanticAdmission, SemanticCandidateSource, SemanticDuplicate,
    SemanticDuplicateLifecycleEvent, SemanticPromotionClaim, SubmissionOrigin,
    record_duplicate_admission_metric, record_semantic_duplicate_lifecycle_metric,
};
use weaver_model::files::{FileRole, unique_download_filenames};

static NEXT_API_JOB_ID: AtomicU64 = AtomicU64::new(10_000);
const MAX_FETCH_REDIRECTS: usize = 10;

#[derive(Clone)]
pub struct SubmittedJob {
    pub job_id: JobId,
    pub job_hash: [u8; 32],
    pub spec: JobSpec,
    pub created_at_epoch_ms: f64,
    pub duplicate_outcome: SubmissionDuplicateOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubmissionDuplicateOutcome {
    Accepted {
        decision: DuplicateDecision,
        semantic: Option<SemanticAdmission>,
    },
    Parked {
        decision: DuplicateDecision,
        semantic: SemanticAdmission,
    },
    IdempotentReplay,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DuplicateBackfillReport {
    pub scanned: usize,
    pub backfilled: usize,
    pub skipped: usize,
    pub completed: bool,
}

struct PreparedSubmission {
    nzb_zstd: Vec<u8>,
    filename: Option<String>,
    password: Option<String>,
    category: Option<String>,
    metadata: Vec<(String, String)>,
    submit_started: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CategoryResolutionMode {
    ResolveConfigured,
    PreserveSubmitted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubmissionOptions {
    pub category_resolution: CategoryResolutionMode,
    pub add_paused: bool,
    pub duplicate_mode: DuplicateMode,
    pub semantic_duplicate: Option<SemanticDuplicate>,
    pub origin: SubmissionOrigin,
    pub idempotency: Option<CallerScopedIdempotency>,
}

impl Default for SubmissionOptions {
    fn default() -> Self {
        Self {
            category_resolution: CategoryResolutionMode::ResolveConfigured,
            add_paused: false,
            duplicate_mode: DuplicateMode::Enforce,
            semantic_duplicate: None,
            origin: SubmissionOrigin::Api,
            idempotency: None,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SubmitNzbError {
    #[error("NZB parse error: {0}")]
    Parse(#[from] weaver_nzb::NzbError),
    #[error("NZB contains no files")]
    Empty,
    #[error("failed to save NZB: {0}")]
    Save(std::io::Error),
    #[error("failed to read uploaded NZB: {0}")]
    Upload(std::io::Error),
    #[error("scheduler error: {0}")]
    Scheduler(#[from] crate::SchedulerError),
    #[error("state error: {0}")]
    State(#[from] crate::StateError),
    #[error("NZB fetch failed: {0}")]
    Fetch(String),
    #[error("NZB response was not valid XML")]
    NotXml,
    #[error("duplicate submission blocked")]
    DuplicateBlocked { decision: DuplicateDecision },
    #[error("idempotency key is already bound to job {job_id}")]
    IdempotencyConflict { job_id: u64 },
}

pub fn init_job_counter(start: u64) {
    NEXT_API_JOB_ID.store(start.max(10_000), Ordering::Relaxed);
}

pub fn next_submission_job_id() -> JobId {
    JobId(NEXT_API_JOB_ID.fetch_add(1, Ordering::Relaxed))
}

pub fn nzb_to_submission_spec(
    nzb: &Nzb,
    filename: Option<&str>,
    password: Option<String>,
    category: Option<String>,
    metadata: Vec<(String, String)>,
) -> JobSpec {
    let metadata = append_original_title_metadata(
        metadata,
        filename.and_then(|value| value.strip_suffix(".nzb")),
        nzb.meta.title.as_deref(),
    );

    let name = derive_release_name(
        filename.and_then(|value| value.strip_suffix(".nzb")),
        nzb.meta.title.as_deref(),
    );

    let password_path = filename
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("upload.nzb"));
    let password = nzb_password_candidates(nzb, &password_path, password.as_deref())
        .into_iter()
        .next()
        .map(|candidate| candidate.value().to_string());

    let mut files = Vec::with_capacity(nzb.files.len());
    let mut total_bytes: u64 = 0;
    let filename_candidates = nzb
        .files
        .iter()
        .map(|nzb_file| {
            // Prefer the enhanced extraction (handles PRiVATE format, higher confidence).
            // If the extracted name is obfuscated, keep it - PAR2 deobfuscation will
            // rename it post-repair using 16KB hash matching.
            nzb_file
                .extract_filename()
                .map(|(name, _confidence)| name)
                .or_else(|| nzb_file.filename().map(str::to_string))
                .unwrap_or_else(|| "unknown".to_string())
        })
        .collect::<Vec<_>>();
    let filenames = unique_download_filenames(filename_candidates.iter().map(String::as_str));

    for (nzb_file, filename) in nzb.files.iter().zip(filenames) {
        let role = FileRole::from_filename(&filename);

        let segments: Vec<SegmentSpec> = nzb_file
            .segments
            .iter()
            .enumerate()
            .map(|(ordinal, segment)| SegmentSpec {
                ordinal: ordinal as u32,
                article_number: segment.number,
                bytes: segment.bytes,
                message_id: segment.message_id.clone(),
            })
            .collect();

        let file_bytes: u64 = nzb_file.total_bytes();
        total_bytes += file_bytes;

        files.push(FileSpec {
            filename,
            role,
            groups: nzb_file.groups.clone(),
            posted_at_epoch: (nzb_file.date > 0).then_some(nzb_file.date),
            segments,
        });
    }

    JobSpec {
        name,
        password,
        files,
        total_bytes,
        category,
        metadata,
    }
}

pub async fn resolve_submission_category(
    config: &SharedConfig,
    category: Option<&str>,
) -> Option<String> {
    resolve_submission_category_with_mode(
        config,
        category,
        CategoryResolutionMode::ResolveConfigured,
    )
    .await
}

pub async fn resolve_submission_category_with_mode(
    config: &SharedConfig,
    category: Option<&str>,
    mode: CategoryResolutionMode,
) -> Option<String> {
    let category = category?;
    if matches!(mode, CategoryResolutionMode::PreserveSubmitted) {
        return Some(category.to_string());
    }

    let cfg = config.read().await;
    if cfg.categories.is_empty() {
        return Some(category.to_string());
    }

    match crate::categories::resolve_category(&cfg.categories, category) {
        Some(canonical) => Some(canonical),
        None => {
            warn!(category = %category, "unknown category, submitting without category");
            None
        }
    }
}

async fn submit_prepared_nzb(
    db: &Database,
    handle: &SchedulerHandle,
    config: &SharedConfig,
    nzb: Nzb,
    prepared: PreparedSubmission,
    options: SubmissionOptions,
) -> Result<SubmittedJob, SubmitNzbError> {
    if nzb.files.is_empty() {
        return Err(SubmitNzbError::Empty);
    }

    let PreparedSubmission {
        nzb_zstd,
        filename,
        password,
        category,
        metadata,
        submit_started,
    } = prepared;

    let resolved_category = resolve_submission_category_with_mode(
        config,
        category.as_deref(),
        options.category_resolution,
    )
    .await;
    let job_hash = persisted_nzb::hash_persisted_nzb_bytes(&nzb_zstd);
    let spec = nzb_to_submission_spec(
        &nzb,
        filename.as_deref(),
        password,
        resolved_category,
        metadata,
    );
    // Fingerprints are CPU-heavy canonical encodings of the validated parser
    // result. Keep them off HTTP/RSS/watch-folder async workers and the
    // scheduler loop.
    let evidence = {
        let spec = spec.clone();
        tokio::task::spawn_blocking(move || {
            FingerprintEvidence::from_validated_spec(&spec, job_hash)
        })
        .await
        .map_err(|error| {
            SubmitNzbError::State(crate::StateError::Database(format!(
                "fingerprint worker panicked: {error}"
            )))
        })?
    };
    let duplicate_policy = {
        let cfg = config.read().await;
        cfg.duplicate_policy
    };
    let admission = {
        let db = db.clone();
        let request = DuplicateAdmissionRequest {
            evidence,
            mode: options.duplicate_mode,
            semantic: options.semantic_duplicate.clone(),
            semantic_source: options
                .semantic_duplicate
                .as_ref()
                .map(|_| SemanticCandidateSource {
                    nzb_zstd: nzb_zstd.clone(),
                    filename: filename.clone(),
                    password: spec.password.clone(),
                    category: spec.category.clone(),
                    metadata: spec.metadata.clone(),
                }),
            origin: options.origin.clone(),
            idempotency: options.idempotency.clone(),
            policy: duplicate_policy,
        };
        tokio::task::spawn_blocking(move || db.admit_duplicate_submission(&request))
            .await
            .map_err(|error| {
                SubmitNzbError::State(crate::StateError::Database(format!(
                    "duplicate admission worker panicked: {error}"
                )))
            })??
    };
    let (
        job_id,
        duplicate_pause,
        created_at_epoch_ms,
        duplicate_outcome,
        superseded_job_id,
        semantic_materialization_generation,
        materialization_decision,
        materialization_semantic,
    ) = match admission {
        DuplicateAdmission::Accepted {
            job_id,
            decision,
            created_at,
            semantic,
        } => {
            let status = if decision.force_bypassed {
                "force_accepted"
            } else {
                match decision.action {
                    DuplicateAction::Accept => "accepted",
                    DuplicateAction::Warn => "warned",
                    DuplicateAction::Pause => "paused",
                    DuplicateAction::Block => "blocked",
                }
            };
            record_duplicate_admission_metric(&options.origin, status);
            let materialization_decision = decision.clone();
            let materialization_semantic = semantic.clone();
            let superseded_job_id = semantic
                .as_ref()
                .and_then(|semantic| semantic.superseded_job_id);
            let semantic_materialization_generation = semantic
                .as_ref()
                .filter(|semantic| {
                    matches!(semantic.state, crate::jobs::SemanticCandidateState::Active)
                })
                .map(|semantic| semantic.materialization_generation);
            (
                job_id,
                matches!(decision.action, DuplicateAction::Pause),
                created_at as f64 * 1000.0,
                SubmissionDuplicateOutcome::Accepted { decision, semantic },
                superseded_job_id,
                semantic_materialization_generation,
                materialization_decision,
                materialization_semantic,
            )
        }
        DuplicateAdmission::Parked {
            job_id,
            decision,
            created_at,
            semantic,
        } => {
            record_duplicate_admission_metric(&options.origin, "parked");
            return Ok(SubmittedJob {
                job_id,
                job_hash,
                spec,
                created_at_epoch_ms: created_at as f64 * 1000.0,
                duplicate_outcome: SubmissionDuplicateOutcome::Parked { decision, semantic },
            });
        }
        DuplicateAdmission::Idempotent { job_id, created_at } => {
            record_duplicate_admission_metric(&options.origin, "idempotent_replay");
            return Ok(SubmittedJob {
                job_id,
                job_hash,
                spec,
                created_at_epoch_ms: created_at as f64 * 1000.0,
                duplicate_outcome: SubmissionDuplicateOutcome::IdempotentReplay,
            });
        }
        DuplicateAdmission::IdempotencyConflict { job_id } => {
            record_duplicate_admission_metric(&options.origin, "idempotency_conflict");
            return Err(SubmitNzbError::IdempotencyConflict { job_id: job_id.0 });
        }
        DuplicateAdmission::Blocked { decision } => {
            record_duplicate_admission_metric(&options.origin, "blocked");
            return Err(SubmitNzbError::DuplicateBlocked { decision });
        }
    };
    if let Some(superseded_job_id) = superseded_job_id {
        match handle.cancel_semantic_superseded(superseded_job_id).await {
            Ok(()) | Err(crate::SchedulerError::JobNotFound(_)) => {}
            Err(crate::SchedulerError::Conflict(error)) => {
                let rollback_db = db.clone();
                let _ = tokio::task::spawn_blocking(move || {
                    rollback_db.rollback_semantic_supersession(job_id, superseded_job_id)
                })
                .await;
                let release_db = db.clone();
                let _ = tokio::task::spawn_blocking(move || {
                    release_db.release_duplicate_admission(job_id)
                })
                .await;
                return Err(SubmitNzbError::Scheduler(crate::SchedulerError::Conflict(
                    error,
                )));
            }
            Err(error) => {
                let rollback_db = db.clone();
                let _ = tokio::task::spawn_blocking(move || {
                    rollback_db.rollback_semantic_supersession(job_id, superseded_job_id)
                })
                .await;
                let release_db = db.clone();
                let _ = tokio::task::spawn_blocking(move || {
                    release_db.release_duplicate_admission(job_id)
                })
                .await;
                return Err(error.into());
            }
        }
    }
    let nzb_path = PathBuf::from(
        filename
            .clone()
            .unwrap_or_else(|| format!("job-{}.nzb", job_id.0)),
    );

    if let Err(error) = handle
        .add_job_with_options(
            job_id,
            spec.clone(),
            nzb_path.clone(),
            nzb_zstd,
            crate::jobs::AddJobOptions {
                initially_paused: options.add_paused || duplicate_pause,
                semantic_materialization_generation,
                semantic_promotion_generation: None,
            },
        )
        .await
    {
        if matches!(error, SchedulerError::SemanticSuperseded)
            && let Some(mut semantic) = materialization_semantic
        {
            // The higher-score submit has already durably parked this source and
            // invalidated its materialization generation.  Releasing the admission
            // here would delete the only retained fallback payload.
            semantic.state = crate::jobs::SemanticCandidateState::Parked;
            record_duplicate_admission_metric(&options.origin, "parked");
            return Ok(SubmittedJob {
                job_id,
                job_hash,
                spec,
                created_at_epoch_ms,
                duplicate_outcome: SubmissionDuplicateOutcome::Parked {
                    decision: materialization_decision,
                    semantic,
                },
            });
        }
        let db = db.clone();
        let _ = tokio::task::spawn_blocking(move || db.release_duplicate_admission(job_id)).await;
        return Err(error.into());
    }

    info!(
        job_id = job_id.0,
        name = %spec.name,
        category = spec.category,
        metadata_len = spec.metadata.len(),
        has_password = spec.password.is_some(),
        elapsed_ms = submit_started.elapsed().as_millis() as u64,
        "submitted NZB job"
    );

    Ok(SubmittedJob {
        job_id,
        job_hash,
        spec,
        created_at_epoch_ms,
        duplicate_outcome,
    })
}

/// Materializes a durable SCORE claim without re-entering normal duplicate
/// admission. This keeps the candidate's reserved job ID/source stable across
/// retry and creates no work directory until scheduler enqueue succeeds.
pub async fn materialize_semantic_promotion(
    db: &Database,
    handle: &SchedulerHandle,
    claim: SemanticPromotionClaim,
) -> Result<JobId, SubmitNzbError> {
    if handle.get_job(claim.job_id).is_ok() {
        let job_id = claim.job_id;
        let generation = claim.generation;
        let db = db.clone();
        let completed = tokio::task::spawn_blocking(move || {
            db.complete_semantic_promotion_claim(job_id, generation)
        })
        .await
        .map_err(|error| {
            SubmitNzbError::State(crate::StateError::Database(format!(
                "promotion completion worker panicked: {error}"
            )))
        })??;
        if !completed {
            return Err(crate::SchedulerError::SemanticSuperseded.into());
        }
        return Ok(job_id);
    }

    let SemanticPromotionClaim {
        job_id,
        generation,
        source,
        ..
    } = claim;
    let nzb = match persisted_nzb::parse_persisted_nzb_bytes(&source.nzb_zstd) {
        Ok(nzb) => nzb,
        Err(persisted_nzb::PersistedNzbError::Io(error)) => {
            record_semantic_duplicate_lifecycle_metric(
                SemanticDuplicateLifecycleEvent::PromotionFailure,
            );
            let db = db.clone();
            let _ = tokio::task::spawn_blocking(move || {
                db.release_semantic_promotion_claim(job_id, generation)
            })
            .await;
            return Err(SubmitNzbError::Save(error));
        }
        Err(persisted_nzb::PersistedNzbError::Parse(error)) => {
            record_semantic_duplicate_lifecycle_metric(
                SemanticDuplicateLifecycleEvent::PromotionFailure,
            );
            let db = db.clone();
            let _ = tokio::task::spawn_blocking(move || {
                db.release_semantic_promotion_claim(job_id, generation)
            })
            .await;
            return Err(SubmitNzbError::Parse(error));
        }
    };
    if nzb.files.is_empty() {
        record_semantic_duplicate_lifecycle_metric(
            SemanticDuplicateLifecycleEvent::PromotionFailure,
        );
        let db = db.clone();
        let _ = tokio::task::spawn_blocking(move || {
            db.release_semantic_promotion_claim(job_id, generation)
        })
        .await;
        return Err(SubmitNzbError::Empty);
    }

    let spec = nzb_to_submission_spec(
        &nzb,
        source.filename.as_deref(),
        source.password,
        source.category,
        source.metadata,
    );
    let nzb_path = PathBuf::from(
        source
            .filename
            .unwrap_or_else(|| format!("job-{}.nzb", job_id.0)),
    );
    if let Err(error) = handle
        .add_job_with_options(
            job_id,
            spec,
            nzb_path,
            source.nzb_zstd,
            crate::jobs::AddJobOptions {
                semantic_promotion_generation: Some(generation),
                ..crate::jobs::AddJobOptions::default()
            },
        )
        .await
    {
        record_semantic_duplicate_lifecycle_metric(
            SemanticDuplicateLifecycleEvent::PromotionFailure,
        );
        let db = db.clone();
        let _ = tokio::task::spawn_blocking(move || {
            db.release_semantic_promotion_claim(job_id, generation)
        })
        .await;
        return Err(error.into());
    }
    let db = db.clone();
    let completed = tokio::task::spawn_blocking(move || {
        db.complete_semantic_promotion_claim(job_id, generation)
    })
    .await
    .map_err(|error| {
        SubmitNzbError::State(crate::StateError::Database(format!(
            "promotion completion worker panicked: {error}"
        )))
    })??;
    if !completed {
        return Err(crate::SchedulerError::SemanticSuperseded.into());
    }
    Ok(job_id)
}

/// Replays durable promotion claims after scheduler restoration. Claims whose
/// job is already restored are completed without a duplicate enqueue; failed
/// materializations are returned to parked state by the materializer.
pub async fn reconcile_semantic_promotions(
    db: &Database,
    handle: &SchedulerHandle,
) -> Result<usize, SubmitNzbError> {
    let db_for_claims = db.clone();
    let claims =
        tokio::task::spawn_blocking(move || db_for_claims.reconcile_semantic_promotion_claims(64))
            .await
            .map_err(|error| {
                SubmitNzbError::State(crate::StateError::Database(format!(
                    "promotion recovery worker panicked: {error}"
                )))
            })??;
    let mut materialized = 0;
    for claim in claims {
        match materialize_semantic_promotion(db, handle, claim).await {
            Ok(_) => materialized += 1,
            Err(error) => {
                tracing::warn!(error = %error, "failed to recover semantic promotion claim")
            }
        }
    }
    Ok(materialized)
}

/// Parses one ordered historical NZB page off the async and scheduler threads,
/// then commits its fingerprints and checkpoint atomically. Missing or malformed
/// payloads advance the cursor with a bounded count so startup recovery cannot
/// loop forever on old broken history.
pub async fn run_duplicate_fingerprint_backfill_batch(
    db: &Database,
) -> Result<DuplicateBackfillReport, SubmitNzbError> {
    const BATCH_SIZE: usize = 64;
    let db_for_state = db.clone();
    let state = tokio::task::spawn_blocking(move || db_for_state.duplicate_backfill_state())
        .await
        .map_err(|error| {
            SubmitNzbError::State(crate::StateError::Database(format!(
                "duplicate backfill state worker panicked: {error}"
            )))
        })??;
    if state
        .as_ref()
        .is_some_and(|state| state.completed_at.is_some())
    {
        return Ok(DuplicateBackfillReport {
            completed: true,
            ..DuplicateBackfillReport::default()
        });
    }
    let cursor = state.and_then(|state| state.cursor_job_id);
    let db_for_sources = db.clone();
    let sources = tokio::task::spawn_blocking(move || {
        db_for_sources.load_duplicate_backfill_sources(cursor, BATCH_SIZE)
    })
    .await
    .map_err(|error| {
        SubmitNzbError::State(crate::StateError::Database(format!(
            "duplicate backfill source worker panicked: {error}"
        )))
    })??;
    let scanned = sources.len();
    let next_cursor = sources.last().map(|source| source.job_id);
    let completed = scanned < BATCH_SIZE;
    let (entries, skipped) = tokio::task::spawn_blocking(move || {
        let mut entries = Vec::with_capacity(sources.len());
        let mut skipped = 0;
        for source in sources {
            let Some(nzb_zstd) = source.nzb_zstd else {
                skipped += 1;
                continue;
            };
            let Ok(nzb) = persisted_nzb::parse_persisted_nzb_bytes(&nzb_zstd) else {
                skipped += 1;
                continue;
            };
            if nzb.files.is_empty() {
                skipped += 1;
                continue;
            }
            let spec = nzb_to_submission_spec(&nzb, None, None, None, Vec::new());
            let raw_job_hash = source
                .raw_job_hash
                .unwrap_or_else(|| persisted_nzb::hash_persisted_nzb_bytes(&nzb_zstd));
            entries.push(DuplicateBackfillEntry {
                job_id: source.job_id,
                lifecycle: source.lifecycle,
                created_at: source.created_at,
                evidence: FingerprintEvidence::from_validated_spec(&spec, raw_job_hash),
            });
        }
        (entries, skipped)
    })
    .await
    .map_err(|error| {
        SubmitNzbError::State(crate::StateError::Database(format!(
            "duplicate backfill parser worker panicked: {error}"
        )))
    })?;
    let backfilled = entries.len();
    let db_for_commit = db.clone();
    tokio::task::spawn_blocking(move || {
        db_for_commit.commit_duplicate_backfill_batch(&entries, next_cursor, completed)
    })
    .await
    .map_err(|error| {
        SubmitNzbError::State(crate::StateError::Database(format!(
            "duplicate backfill commit worker panicked: {error}"
        )))
    })??;
    Ok(DuplicateBackfillReport {
        scanned,
        backfilled,
        skipped,
        completed,
    })
}

/// Bounded startup catch-up. Subsequent starts resume from the durable cursor;
/// a complete marker prevents any further scans.
pub async fn reconcile_duplicate_fingerprint_backfill(
    db: &Database,
    max_batches: usize,
) -> Result<DuplicateBackfillReport, SubmitNzbError> {
    let mut total = DuplicateBackfillReport::default();
    for _ in 0..max_batches.max(1) {
        let report = run_duplicate_fingerprint_backfill_batch(db).await?;
        total.scanned += report.scanned;
        total.backfilled += report.backfilled;
        total.skipped += report.skipped;
        total.completed = report.completed;
        if report.completed {
            break;
        }
    }
    Ok(total)
}

#[allow(clippy::too_many_arguments)]
pub async fn submit_nzb_bytes(
    db: &Database,
    handle: &SchedulerHandle,
    config: &SharedConfig,
    nzb_bytes: &[u8],
    filename: Option<String>,
    password: Option<String>,
    category: Option<String>,
    metadata: Vec<(String, String)>,
) -> Result<SubmittedJob, SubmitNzbError> {
    submit_nzb_bytes_with_options(
        db,
        handle,
        config,
        nzb_bytes,
        filename,
        password,
        category,
        metadata,
        SubmissionOptions::default(),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn submit_nzb_bytes_with_options(
    db: &Database,
    handle: &SchedulerHandle,
    config: &SharedConfig,
    nzb_bytes: &[u8],
    filename: Option<String>,
    password: Option<String>,
    category: Option<String>,
    metadata: Vec<(String, String)>,
    options: SubmissionOptions,
) -> Result<SubmittedJob, SubmitNzbError> {
    let submit_started = Instant::now();
    let nzb = weaver_nzb::parse_nzb(nzb_bytes)?;
    let nzb_zstd = persisted_nzb::compress_nzb_bytes(nzb_bytes).map_err(SubmitNzbError::Save)?;
    submit_prepared_nzb(
        db,
        handle,
        config,
        nzb,
        PreparedSubmission {
            nzb_zstd,
            filename,
            password,
            category,
            metadata,
            submit_started,
        },
        options,
    )
    .await
}

pub async fn fetch_nzb_from_url(
    _client: &reqwest::Client,
    url: &str,
) -> Result<(Vec<u8>, Option<String>), SubmitNzbError> {
    fetch_nzb_from_url_with_resolver(url, |current_url| async move {
        resolve_fetch_target(&current_url, false).await
    })
    .await
}

async fn fetch_nzb_from_url_with_resolver<R, Fut>(
    url: &str,
    mut resolve_target: R,
) -> Result<(Vec<u8>, Option<String>), SubmitNzbError>
where
    R: FnMut(reqwest::Url) -> Fut,
    Fut: Future<Output = Result<ResolvedFetchTarget, String>>,
{
    let limits = RuntimeSecurityConfig::from_env_or_default_for_tests();
    let mut current_url =
        reqwest::Url::parse(url).map_err(|error| SubmitNzbError::Fetch(error.to_string()))?;

    for redirect_count in 0..=MAX_FETCH_REDIRECTS {
        let target = resolve_target(current_url.clone())
            .await
            .map_err(SubmitNzbError::Fetch)?;
        let client = target
            .apply_dns_override(
                reqwest::Client::builder()
                    .redirect(reqwest::redirect::Policy::none())
                    .timeout(std::time::Duration::from_secs(60))
                    .user_agent("weaver/0.1")
                    .gzip(true)
                    .brotli(true)
                    .deflate(true)
                    .zstd(true),
            )
            .build()
            .map_err(|error| SubmitNzbError::Fetch(format!("client build failed: {error}")))?;

        let response = client
            .get(target.url.clone())
            .send()
            .await
            .map_err(|error| SubmitNzbError::Fetch(format!("request failed: {error}")))?;

        if response.status().is_redirection() {
            if redirect_count == MAX_FETCH_REDIRECTS {
                return Err(SubmitNzbError::Fetch("too many redirects".to_string()));
            }
            let location = response
                .headers()
                .get(reqwest::header::LOCATION)
                .and_then(|value| value.to_str().ok())
                .ok_or_else(|| SubmitNzbError::Fetch("redirect missing Location".to_string()))?;
            current_url = current_url
                .join(location)
                .map_err(|error| SubmitNzbError::Fetch(format!("invalid redirect: {error}")))?;
            continue;
        }

        if !response.status().is_success() {
            return Err(SubmitNzbError::Fetch(format!("HTTP {}", response.status())));
        }

        let filename = extract_filename_from_response(&response, current_url.as_str());
        let nzb_bytes =
            read_response_with_limit(response, limits.nzb_decompressed_limit_bytes).await?;

        if nzb_bytes.is_empty() {
            return Err(SubmitNzbError::Fetch("empty response body".into()));
        }

        let nzb = weaver_nzb::parse_nzb(&nzb_bytes)?;
        if nzb.files.is_empty() {
            return Err(SubmitNzbError::Empty);
        }

        return Ok((nzb_bytes, filename));
    }

    Err(SubmitNzbError::Fetch("too many redirects".to_string()))
}

async fn read_response_with_limit(
    mut response: reqwest::Response,
    limit: u64,
) -> Result<Vec<u8>, SubmitNzbError> {
    if response
        .content_length()
        .is_some_and(|length| length > limit)
    {
        return Err(SubmitNzbError::Fetch(format!(
            "response exceeds {limit} bytes"
        )));
    }

    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| SubmitNzbError::Fetch(format!("body read failed: {error}")))?
    {
        let next_len = body.len().saturating_add(chunk.len());
        if next_len as u64 > limit {
            return Err(SubmitNzbError::Fetch(format!(
                "response exceeds {limit} bytes"
            )));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

#[allow(clippy::too_many_arguments)]
pub async fn submit_uploaded_nzb_reader<R>(
    db: &Database,
    handle: &SchedulerHandle,
    config: &SharedConfig,
    source: R,
    filename: Option<String>,
    password: Option<String>,
    category: Option<String>,
    metadata: Vec<(String, String)>,
) -> Result<SubmittedJob, SubmitNzbError>
where
    R: Read + Send + 'static,
{
    submit_uploaded_nzb_reader_with_options(
        db,
        handle,
        config,
        source,
        filename,
        password,
        category,
        metadata,
        SubmissionOptions::default(),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn submit_uploaded_nzb_reader_with_options<R>(
    db: &Database,
    handle: &SchedulerHandle,
    config: &SharedConfig,
    source: R,
    filename: Option<String>,
    password: Option<String>,
    category: Option<String>,
    metadata: Vec<(String, String)>,
    options: SubmissionOptions,
) -> Result<SubmittedJob, SubmitNzbError>
where
    R: Read + Send + 'static,
{
    let submit_started = Instant::now();
    let persist_result = tokio::task::spawn_blocking(move || {
        let mut source = source;
        super::persisted_nzb::persist_decoded_nzb_reader_to_zstd(&mut source)
    })
    .await
    .map_err(|error| SubmitNzbError::Upload(std::io::Error::other(error.to_string())))?;
    let (nzb_zstd, nzb) = match persist_result {
        Ok(values) => values,
        Err(super::persisted_nzb::PersistedNzbError::Io(error)) => {
            return Err(SubmitNzbError::Save(error));
        }
        Err(super::persisted_nzb::PersistedNzbError::Parse(error)) => {
            return Err(SubmitNzbError::Parse(error));
        }
    };
    submit_prepared_nzb(
        db,
        handle,
        config,
        nzb,
        PreparedSubmission {
            nzb_zstd,
            filename,
            password,
            category,
            metadata,
            submit_started,
        },
        options,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn submit_staged_nzb_zstd(
    db: &Database,
    handle: &SchedulerHandle,
    config: &SharedConfig,
    nzb_zstd: Vec<u8>,
    filename: Option<String>,
    password: Option<String>,
    category: Option<String>,
    metadata: Vec<(String, String)>,
) -> Result<SubmittedJob, SubmitNzbError> {
    submit_staged_nzb_zstd_with_options(
        db,
        handle,
        config,
        nzb_zstd,
        filename,
        password,
        category,
        metadata,
        SubmissionOptions::default(),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn submit_staged_nzb_zstd_with_options(
    db: &Database,
    handle: &SchedulerHandle,
    config: &SharedConfig,
    nzb_zstd: Vec<u8>,
    filename: Option<String>,
    password: Option<String>,
    category: Option<String>,
    metadata: Vec<(String, String)>,
    options: SubmissionOptions,
) -> Result<SubmittedJob, SubmitNzbError> {
    let submit_started = Instant::now();
    let nzb = match persisted_nzb::parse_persisted_nzb_bytes(&nzb_zstd) {
        Ok(nzb) => nzb,
        Err(persisted_nzb::PersistedNzbError::Io(error)) => {
            return Err(SubmitNzbError::Save(error));
        }
        Err(persisted_nzb::PersistedNzbError::Parse(error)) => {
            return Err(SubmitNzbError::Parse(error));
        }
    };
    submit_prepared_nzb(
        db,
        handle,
        config,
        nzb,
        PreparedSubmission {
            nzb_zstd,
            filename,
            password,
            category,
            metadata,
            submit_started,
        },
        options,
    )
    .await
}

fn extract_filename_from_response(response: &reqwest::Response, url: &str) -> Option<String> {
    if let Some(content_disposition) = response.headers().get(reqwest::header::CONTENT_DISPOSITION)
        && let Ok(value) = content_disposition.to_str()
        && let Some(name) = parse_content_disposition_filename(value)
    {
        return Some(name);
    }

    let path = url.split('?').next().unwrap_or(url);
    let segment = path
        .rsplit('/')
        .next()
        .filter(|segment| !segment.is_empty() && segment.contains('.'))?;
    Some(segment.to_string())
}

fn parse_content_disposition_filename(header: &str) -> Option<String> {
    let lower = header.to_ascii_lowercase();
    let idx = lower.find("filename=")?;
    let rest = &header[idx + "filename=".len()..];
    let rest = rest.trim_start();
    if let Some(stripped) = rest.strip_prefix('"') {
        let end = stripped.find('"')?;
        let name = &stripped[..end];
        if name.is_empty() {
            return None;
        }
        Some(name.to_string())
    } else {
        let name = rest.split(';').next()?.trim();
        if name.is_empty() {
            return None;
        }
        Some(name.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn minimal_nzb(name: &str) -> String {
        format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
  <file poster="test@test.com" date="1234567890" subject="{name} - &quot;file.rar&quot; yEnc (1/1)">
    <groups><group>alt.binaries.test</group></groups>
    <segments><segment bytes="500000" number="1">{name}-seg1@test.com</segment></segments>
  </file>
</nzb>"#
        )
    }

    async fn read_http_request(stream: &mut tokio::net::TcpStream) -> String {
        let mut request = Vec::new();
        let mut buf = [0_u8; 1024];
        loop {
            let read = stream.read(&mut buf).await.unwrap();
            if read == 0 {
                break;
            }
            request.extend_from_slice(&buf[..read]);
            if request.windows(4).any(|window| window == b"\r\n\r\n") {
                break;
            }
        }
        String::from_utf8_lossy(&request).into_owned()
    }

    fn pinned_target(url: reqwest::Url, addr: SocketAddr) -> ResolvedFetchTarget {
        let host = url.host_str().unwrap().to_string();
        ResolvedFetchTarget {
            url,
            host,
            addrs: vec![addr],
        }
    }

    #[tokio::test]
    async fn fetch_nzb_uses_pinned_hostname_resolution() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let xml = minimal_nzb("Pinned.Release");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read_http_request(&mut stream).await;
            let response = format!(
                "HTTP/1.1 200 OK\r\n\
                 Content-Type: application/x-nzb\r\n\
                 Content-Disposition: attachment; filename=\"Pinned.Release.nzb\"\r\n\
                 Content-Length: {}\r\n\
                 Connection: close\r\n\
                 \r\n\
                 {}",
                xml.len(),
                xml
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            request
        });

        let url = format!("http://public.test:{}/download.nzb", addr.port());
        let (bytes, filename) = fetch_nzb_from_url_with_resolver(&url, move |url| async move {
            Ok(pinned_target(url, addr))
        })
        .await
        .unwrap();
        let request = server.await.unwrap();

        assert_eq!(filename.as_deref(), Some("Pinned.Release.nzb"));
        assert!(!bytes.is_empty());
        assert!(request.starts_with("GET /download.nzb HTTP/1.1"));
        assert!(request.contains(&format!("host: public.test:{}", addr.port())));
    }

    #[tokio::test]
    async fn fetch_nzb_rejects_redirect_to_private_before_request() {
        let redirect_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let redirect_addr = redirect_listener.local_addr().unwrap();
        let private_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let private_addr = private_listener.local_addr().unwrap();
        let redirect_server = tokio::spawn(async move {
            let (mut stream, _) = redirect_listener.accept().await.unwrap();
            let _request = read_http_request(&mut stream).await;
            let response = format!(
                "HTTP/1.1 302 Found\r\n\
                 Location: http://127.0.0.1:{}/private.nzb\r\n\
                 Content-Length: 0\r\n\
                 Connection: close\r\n\
                 \r\n",
                private_addr.port()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
        });
        let private_accept = tokio::spawn(async move {
            tokio::time::timeout(Duration::from_millis(200), private_listener.accept())
                .await
                .is_ok()
        });

        let url = format!("http://public.test:{}/redirect.nzb", redirect_addr.port());
        let error = fetch_nzb_from_url_with_resolver(&url, move |url| async move {
            if url.host_str() == Some("public.test") {
                Ok(pinned_target(url, redirect_addr))
            } else {
                resolve_fetch_target(&url, false).await
            }
        })
        .await
        .unwrap_err();
        redirect_server.await.unwrap();
        let private_was_hit = private_accept.await.unwrap();

        assert!(error.to_string().contains("not allowed"));
        assert!(!private_was_hit);
    }

    #[test]
    fn pinned_target_preserves_original_host_for_dns_override() {
        let url = reqwest::Url::parse("http://public.test:8080/file.nzb").unwrap();
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080);
        let target = pinned_target(url, addr);

        assert_eq!(target.host, "public.test");
        assert_eq!(target.addrs, vec![addr]);
    }
}
