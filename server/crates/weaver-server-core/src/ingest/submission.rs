// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::io::{Cursor, Read};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use tracing::{info, warn};

use crate::Database;
use crate::jobs::JobSpec;
use crate::jobs::ids::JobId;
use crate::security::RuntimeSecurityConfig;
use crate::settings::SharedConfig;
use crate::{SchedulerError, SchedulerHandle};
use weaver_nzb::Nzb;

use super::persisted_nzb;
use crate::ingest::{
    append_original_title_metadata, derive_release_name, nzb_password_candidates,
    strip_nzb_source_suffix,
};
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
    pub summary: SubmittedJobSummary,
    pub created_at_epoch_ms: f64,
    pub duplicate_outcome: SubmissionDuplicateOutcome,
}

#[derive(Clone)]
pub struct SubmittedJobSummary {
    pub name: String,
    pub password: Option<String>,
    pub category: Option<String>,
    pub metadata: Vec<(String, String)>,
    pub total_bytes: u64,
    pub file_count: u32,
    pub remaining_par_count: u32,
}

impl SubmittedJobSummary {
    fn from_spec(spec: &JobSpec) -> Self {
        Self {
            name: spec.name.clone(),
            password: spec.password.clone(),
            category: spec.category.clone(),
            metadata: spec.metadata.clone(),
            total_bytes: spec.total_bytes,
            file_count: spec.files.len() as u32,
            remaining_par_count: spec.par2_volume_count() as u32,
        }
    }
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
    raw_job_hash: [u8; 32],
    filename: Option<String>,
    password: Option<String>,
    category: Option<String>,
    metadata: Vec<(String, String)>,
    submit_started: Instant,
}

#[derive(Clone)]
pub struct StagedSubmissionPreparation {
    pub spec: JobSpec,
    pub evidence: FingerprintEvidence,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubmissionOptions {
    pub add_paused: bool,
    pub duplicate_mode: DuplicateMode,
    pub semantic_duplicate: Option<SemanticDuplicate>,
    pub origin: SubmissionOrigin,
    pub idempotency: Option<CallerScopedIdempotency>,
    // Archive passwords supplied beside the job's own: a calling integration's
    // (`integration_archive_password_candidates`), then those weaver read from
    // its own NZB fetch (`header_archive_password_candidates`), in that order.
    pub supplied_password_candidates: Vec<crate::jobs::ArchivePasswordCandidate>,
}

impl Default for SubmissionOptions {
    fn default() -> Self {
        Self {
            add_paused: false,
            duplicate_mode: DuplicateMode::Enforce,
            semantic_duplicate: None,
            origin: SubmissionOrigin::Api,
            idempotency: None,
            supplied_password_candidates: Vec::new(),
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
    #[error("invalid category: {0}")]
    InvalidCategory(#[from] crate::categories::CategoryValidationError),
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
    // A name chosen by a scan script or a client carries no source suffix.
    let title = filename.map(|value| strip_nzb_source_suffix(value).unwrap_or(value));
    let metadata = append_original_title_metadata(metadata, title, nzb.meta.title.as_deref());

    let name = derive_release_name(title, nzb.meta.title.as_deref());

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
) -> Result<Option<String>, crate::categories::CategoryValidationError> {
    let cfg = config.read().await;
    crate::categories::resolve_submission_category(&cfg.categories, category)
}

async fn rollback_accepted_submission(
    db: &Database,
    job_id: JobId,
    superseded_job_id: Option<JobId>,
) {
    if let Some(superseded_job_id) = superseded_job_id {
        let rollback_db = db.clone();
        let _ = tokio::task::spawn_blocking(move || {
            rollback_db.rollback_semantic_supersession(job_id, superseded_job_id)
        })
        .await;
    }
    let release_db = db.clone();
    let _ =
        tokio::task::spawn_blocking(move || release_db.release_duplicate_admission(job_id)).await;
}

async fn submit_prepared_nzb(
    db: &Database,
    handle: &SchedulerHandle,
    config: &SharedConfig,
    nzb: Option<&Nzb>,
    prepared: PreparedSubmission,
    options: SubmissionOptions,
    staged_preparation: Option<StagedSubmissionPreparation>,
) -> Result<SubmittedJob, SubmitNzbError> {
    let PreparedSubmission {
        nzb_zstd,
        raw_job_hash: job_hash,
        filename,
        password,
        category,
        metadata,
        submit_started,
    } = prepared;

    let (spec, evidence) = if let Some(mut preparation) = staged_preparation {
        preparation.spec.category =
            resolve_submission_category(config, category.as_deref()).await?;
        (preparation.spec, preparation.evidence)
    } else {
        let nzb = nzb.expect("non-staged submissions provide a parsed NZB");
        if nzb.files.is_empty() {
            return Err(SubmitNzbError::Empty);
        }

        let resolved_category = resolve_submission_category(config, category.as_deref()).await?;
        // With no password from the caller, the first supplied one is the
        // job's primary password, ahead of anything the NZB names.
        let password = crate::ingest::normalize_archive_password_candidate(password.as_deref())
            .or_else(|| {
                options
                    .supplied_password_candidates
                    .first()
                    .map(|candidate| candidate.value().to_string())
            });
        let spec = nzb_to_submission_spec(
            nzb,
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
        (spec, evidence)
    };
    let submitted_summary = SubmittedJobSummary::from_spec(&spec);
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
                summary: submitted_summary,
                created_at_epoch_ms: created_at as f64 * 1000.0,
                duplicate_outcome: SubmissionDuplicateOutcome::Parked { decision, semantic },
            });
        }
        DuplicateAdmission::Idempotent { job_id, created_at } => {
            record_duplicate_admission_metric(&options.origin, "idempotent_replay");
            return Ok(SubmittedJob {
                job_id,
                job_hash,
                summary: submitted_summary,
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
                rollback_accepted_submission(db, job_id, Some(superseded_job_id)).await;
                return Err(SubmitNzbError::Scheduler(crate::SchedulerError::Conflict(
                    error,
                )));
            }
            Err(error) => {
                rollback_accepted_submission(db, job_id, Some(superseded_job_id)).await;
                return Err(error.into());
            }
        }
    }
    let nzb_path = PathBuf::from(
        filename
            .clone()
            .unwrap_or_else(|| format!("job-{}.nzb", job_id.0)),
    );
    // Captured before `spec` moves into the scheduler; the submission origin is
    // only knowable here, so this is where the submitted counter is recorded.
    let submitted_category = spec.category.clone();

    if let Err(error) = handle
        .add_job_with_options(
            job_id,
            spec,
            nzb_path.clone(),
            nzb_zstd,
            crate::jobs::AddJobOptions {
                initially_paused: options.add_paused || duplicate_pause,
                semantic_materialization_generation,
                semantic_promotion_generation: None,
                supplied_password_candidates: options.supplied_password_candidates.clone(),
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
                summary: submitted_summary,
                created_at_epoch_ms,
                duplicate_outcome: SubmissionDuplicateOutcome::Parked {
                    decision: materialization_decision,
                    semantic,
                },
            });
        }
        rollback_accepted_submission(db, job_id, superseded_job_id).await;
        return Err(error.into());
    }

    handle.note_job_submitted(options.origin, submitted_category.as_deref());
    if submitted_summary
        .metadata
        .iter()
        .any(|(key, value)| key == crate::post_processing::scan::ADD_TO_TOP_KEY && value == "1")
        && let Err(error) = handle
            .reorder_jobs(vec![(job_id, crate::QueueMoveTarget::Top)])
            .await
    {
        warn!(job_id = job_id.0, %error, "submitted job could not be moved to top");
    }

    info!(
        job_id = job_id.0,
        name = %submitted_summary.name,
        category = ?submitted_summary.category,
        metadata_len = submitted_summary.metadata.len(),
        has_password = submitted_summary.password.is_some(),
        elapsed_ms = submit_started.elapsed().as_millis() as u64,
        "submitted NZB job"
    );

    Ok(SubmittedJob {
        job_id,
        job_hash,
        summary: submitted_summary,
        created_at_epoch_ms,
        duplicate_outcome,
    })
}

// Materializes a durable SCORE claim without re-entering normal duplicate
// admission. This keeps the candidate's reserved job ID/source stable across
// retry and creates no work directory until scheduler enqueue succeeds.
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

    // Captured before the source's fields move into the spec; the promoted job
    // needs the same `(origin, category)` labels an ordinary submission gets.
    let promoted_category = source.category.clone();
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

    // A promotion puts a job in the queue that `submit_nzb` never counted: the
    // candidate was parked at admission time, before it had a queue entry. The
    // claim carries no intake origin of its own — the durable snapshot's
    // `origin` column is free-form text that also holds non-submission values
    // like the fingerprint backfill marker, so it cannot be read back as a
    // `SubmissionOrigin` — and this *is* an internal requeue of content whose
    // first attempt did not finish, which is what `internal-redownload` names.
    // Recorded after the scheduler accepted the job, exactly as the ordinary
    // submission path does.
    handle.note_job_submitted(
        crate::jobs::SubmissionOrigin::InternalRedownload,
        promoted_category.as_deref(),
    );

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

// Replays durable promotion claims after scheduler restoration. Claims whose
// job is already restored are completed without a duplicate enqueue; failed
// materializations are returned to parked state by the materializer.
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

// Parses one ordered historical NZB page off the async and scheduler threads,
// then commits its fingerprints and checkpoint atomically. Missing or malformed
// payloads advance the cursor with a bounded count so startup recovery cannot
// loop forever on old broken history.
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

// Bounded startup catch-up. Subsequent starts resume from the durable cursor;
// a complete marker prevents any further scans.
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
    if crate::post_processing::scan::enabled(db, category.as_deref(), &metadata).await? {
        let scanned = crate::post_processing::scan::scan_reader(
            db,
            config,
            Cursor::new(nzb_bytes.to_vec()),
            filename,
            password.clone(),
            category,
            metadata,
            options,
        )
        .await?;
        return submit_scanned_nzb(db, handle, config, scanned, password, submit_started).await;
    }
    let mut source = Cursor::new(nzb_bytes);
    let prepared = match persisted_nzb::persist_decoded_nzb_reader_to_zstd(&mut source) {
        Ok(prepared) => prepared,
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
        Some(&prepared.nzb),
        PreparedSubmission {
            nzb_zstd: prepared.nzb_zstd,
            raw_job_hash: prepared.raw_job_hash,
            filename,
            password,
            category,
            metadata,
            submit_started,
        },
        options,
        None,
    )
    .await
}

// An NZB weaver fetched itself: its bytes, the file name the response named,
// and every `X-DNZB-Password` value the response carried, raw and in the order
// received.
pub struct FetchedNzb {
    pub bytes: Vec<u8>,
    pub filename: Option<String>,
    pub header_passwords: Vec<String>,
}

impl std::fmt::Debug for FetchedNzb {
    // Never prints a password.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FetchedNzb")
            .field("bytes", &self.bytes.len())
            .field("filename", &self.filename)
            .field("header_passwords", &self.header_passwords.len())
            .finish()
    }
}

// Fetches an NZB on the Default outbound route.
pub async fn fetch_nzb_from_url(
    runtime: &crate::proxies::ProxyRuntime,
    url: &str,
) -> Result<FetchedNzb, SubmitNzbError> {
    let limits = RuntimeSecurityConfig::from_env_or_default_for_tests();
    let url = reqwest::Url::parse(url).map_err(|error| SubmitNzbError::Fetch(error.to_string()))?;
    let options = crate::proxies::FetchOptions {
        label: "NZB",
        timeout: std::time::Duration::from_secs(60),
        redirects: MAX_FETCH_REDIRECTS,
        user_agent: "weaver/0.1",
        https_only: false,
        allow_private_network: runtime.allow_private_network(),
    };
    let response = crate::proxies::routed_fetch(
        runtime,
        crate::proxies::Consumer::Outbound,
        &url,
        &options,
        |request, _, _| request,
    )
    .await
    .map_err(SubmitNzbError::Fetch)?;
    if !response.status().is_success() {
        return Err(SubmitNzbError::Fetch(format!("HTTP {}", response.status())));
    }
    let filename = extract_filename_from_response(&response, response.url().as_str());
    // Only the response that carries the NZB speaks for its passwords; a
    // redirect's headers are not read.
    let header_passwords = crate::ingest::dnzb_header_passwords(response.headers());
    let bytes = crate::proxies::read_body(response, limits.nzb_decompressed_limit_bytes)
        .await
        .map_err(SubmitNzbError::Fetch)?;
    if bytes.is_empty() {
        return Err(SubmitNzbError::Fetch("empty response body".into()));
    }
    // Submission runs Scan scripts before parsing these fetched bytes.
    Ok(FetchedNzb {
        bytes,
        filename,
        header_passwords,
    })
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
    if crate::post_processing::scan::enabled(db, category.as_deref(), &metadata).await? {
        let scanned = crate::post_processing::scan::scan_reader(
            db,
            config,
            source,
            filename,
            password.clone(),
            category,
            metadata,
            options,
        )
        .await?;
        return submit_scanned_nzb(db, handle, config, scanned, password, submit_started).await;
    }
    let persist_result = tokio::task::spawn_blocking(move || {
        let mut source = source;
        super::persisted_nzb::persist_decoded_nzb_reader_to_zstd(&mut source)
    })
    .await
    .map_err(|error| SubmitNzbError::Upload(std::io::Error::other(error.to_string())))?;
    let prepared = match persist_result {
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
        Some(&prepared.nzb),
        PreparedSubmission {
            nzb_zstd: prepared.nzb_zstd,
            raw_job_hash: prepared.raw_job_hash,
            filename,
            password,
            category,
            metadata,
            submit_started,
        },
        options,
        None,
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
    if crate::post_processing::scan::enabled(db, category.as_deref(), &metadata).await? {
        let source = zstd::stream::read::Decoder::new(Cursor::new(nzb_zstd))
            .map_err(SubmitNzbError::Save)?;
        return submit_uploaded_nzb_reader_with_options(
            db, handle, config, source, filename, password, category, metadata, options,
        )
        .await;
    }
    let (nzb, raw_job_hash) = match persisted_nzb::parse_and_hash_persisted_nzb_bytes(&nzb_zstd) {
        Ok(parsed) => parsed,
        Err(persisted_nzb::PersistedNzbError::Io(error)) => {
            return Err(SubmitNzbError::Save(error));
        }
        Err(persisted_nzb::PersistedNzbError::Parse(error)) => {
            return Err(SubmitNzbError::Parse(error));
        }
    };
    submit_staged_parsed_nzb_with_hash(
        db,
        handle,
        config,
        &nzb,
        nzb_zstd,
        raw_job_hash,
        filename,
        password,
        category,
        metadata,
        options,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn submit_staged_parsed_nzb_with_options(
    db: &Database,
    handle: &SchedulerHandle,
    config: &SharedConfig,
    nzb: &Nzb,
    nzb_zstd: Vec<u8>,
    filename: Option<String>,
    password: Option<String>,
    category: Option<String>,
    metadata: Vec<(String, String)>,
    options: SubmissionOptions,
) -> Result<SubmittedJob, SubmitNzbError> {
    let raw_job_hash = persisted_nzb::hash_persisted_nzb_bytes(&nzb_zstd);
    submit_staged_parsed_nzb_with_hash(
        db,
        handle,
        config,
        nzb,
        nzb_zstd,
        raw_job_hash,
        filename,
        password,
        category,
        metadata,
        options,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn submit_staged_parsed_nzb_with_hash(
    db: &Database,
    handle: &SchedulerHandle,
    config: &SharedConfig,
    nzb: &Nzb,
    nzb_zstd: Vec<u8>,
    raw_job_hash: [u8; 32],
    filename: Option<String>,
    password: Option<String>,
    category: Option<String>,
    metadata: Vec<(String, String)>,
    options: SubmissionOptions,
) -> Result<SubmittedJob, SubmitNzbError> {
    if crate::post_processing::scan::enabled(db, category.as_deref(), &metadata).await? {
        let source = zstd::stream::read::Decoder::new(Cursor::new(nzb_zstd))
            .map_err(SubmitNzbError::Save)?;
        return submit_uploaded_nzb_reader_with_options(
            db, handle, config, source, filename, password, category, metadata, options,
        )
        .await;
    }
    submit_prepared_nzb(
        db,
        handle,
        config,
        Some(nzb),
        PreparedSubmission {
            nzb_zstd,
            raw_job_hash,
            filename,
            password,
            category,
            metadata,
            submit_started: Instant::now(),
        },
        options,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn submit_staged_prepared_nzb_with_options(
    db: &Database,
    handle: &SchedulerHandle,
    config: &SharedConfig,
    preparation: StagedSubmissionPreparation,
    nzb_zstd: Vec<u8>,
    filename: Option<String>,
    options: SubmissionOptions,
) -> Result<SubmittedJob, SubmitNzbError> {
    let category = preparation.spec.category.clone();
    if crate::post_processing::scan::enabled(db, category.as_deref(), &preparation.spec.metadata)
        .await?
    {
        let source = zstd::stream::read::Decoder::new(Cursor::new(nzb_zstd))
            .map_err(SubmitNzbError::Save)?;
        return submit_uploaded_nzb_reader_with_options(
            db,
            handle,
            config,
            source,
            filename,
            preparation.spec.password,
            category,
            preparation.spec.metadata,
            options,
        )
        .await;
    }
    let raw_job_hash = preparation.evidence.raw_job_hash;
    submit_prepared_nzb(
        db,
        handle,
        config,
        None,
        PreparedSubmission {
            nzb_zstd,
            raw_job_hash,
            filename,
            password: None,
            category,
            metadata: Vec::new(),
            submit_started: Instant::now(),
        },
        options,
        Some(preparation),
    )
    .await
}

async fn submit_scanned_nzb(
    db: &Database,
    handle: &SchedulerHandle,
    config: &SharedConfig,
    scanned: crate::post_processing::scan::ScannedSubmission,
    password: Option<String>,
    submit_started: Instant,
) -> Result<SubmittedJob, SubmitNzbError> {
    let crate::post_processing::scan::ScannedSubmission {
        mut source,
        scratch,
        filename,
        category,
        metadata,
        options,
    } = scanned;
    let prepared = tokio::task::spawn_blocking(move || {
        let result = persisted_nzb::persist_decoded_nzb_reader_to_zstd(&mut source);
        drop(source);
        drop(scratch);
        result
    })
    .await
    .map_err(|error| SubmitNzbError::Save(std::io::Error::other(error)))?
    .map_err(|error| match error {
        persisted_nzb::PersistedNzbError::Io(error) => SubmitNzbError::Save(error),
        persisted_nzb::PersistedNzbError::Parse(error) => SubmitNzbError::Parse(error),
    })?;
    submit_prepared_nzb(
        db,
        handle,
        config,
        Some(&prepared.nzb),
        PreparedSubmission {
            nzb_zstd: prepared.nzb_zstd,
            raw_job_hash: prepared.raw_job_hash,
            filename,
            password,
            category,
            metadata,
            submit_started,
        },
        options,
        None,
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

    #[tokio::test]
    async fn fetch_nzb_reads_the_named_file() {
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

        let url = format!("http://{addr}/download.nzb");
        let FetchedNzb {
            bytes,
            filename,
            header_passwords,
        } = fetch_nzb_from_url(&crate::proxies::direct_runtime(), &url)
            .await
            .unwrap();
        let request = server.await.unwrap();
        assert!(header_passwords.is_empty());

        assert_eq!(filename.as_deref(), Some("Pinned.Release.nzb"));
        assert!(!bytes.is_empty());
        assert!(request.starts_with("GET /download.nzb HTTP/1.1"));
        assert!(request.contains(&format!("host: {addr}")));
    }

    fn numbered(prefix: &str, count: usize) -> Vec<String> {
        (1..=count)
            .map(|index| format!("{prefix}-{index:02}"))
            .collect()
    }

    fn labelled(candidates: &[crate::jobs::ArchivePasswordCandidate]) -> Vec<(&'static str, &str)> {
        candidates
            .iter()
            .map(|candidate| (candidate.source().as_str(), candidate.value()))
            .collect()
    }

    fn bucket(label: &'static str, values: &[String]) -> Vec<(&'static str, String)> {
        values.iter().map(|value| (label, value.clone())).collect()
    }

    fn owned(pairs: Vec<(&'static str, &str)>) -> Vec<(&'static str, String)> {
        pairs
            .into_iter()
            .map(|(label, value)| (label, value.to_string()))
            .collect()
    }

    fn settings_candidates(values: &[String]) -> Vec<crate::jobs::ArchivePasswordCandidate> {
        values
            .iter()
            .map(|value| {
                crate::jobs::ArchivePasswordCandidate::new(
                    crate::jobs::ArchivePasswordSource::Settings,
                    value.clone(),
                )
            })
            .collect()
    }

    fn twelve_meta_nzb() -> weaver_nzb::Nzb {
        // `meta-01` twice: a repeat inside the NZB takes no slot either.
        let metas: String = std::iter::once("meta-01".to_string())
            .chain(numbered("meta", 12))
            .map(|value| format!(r#"<meta type="password">{value}</meta>"#))
            .collect();
        let xml = minimal_nzb("Capped.Meta")
            .replace("<file ", &format!("<head>{metas}</head>\n  <file "));
        let nzb = weaver_nzb::parse_nzb(xml.as_bytes()).unwrap();
        assert!(nzb.meta.passwords.len() >= 12);
        nzb
    }

    #[test]
    fn integration_bucket_keeps_the_spec_password_then_nine_given() {
        let given = numbered("given", 12);
        let supplied = crate::ingest::integration_archive_password_candidates(
            std::iter::once("  ").chain(given.iter().map(String::as_str)),
        );
        let candidates =
            crate::pipeline::assemble_archive_password_candidates(Some("spec"), &supplied, &[]);
        let mut expected = vec![("explicit", "spec".to_string())];
        expected.extend(bucket("integration", &given[..9]));
        assert_eq!(owned(labelled(&candidates)), expected);

        // The spec password repeating a given one frees that slot.
        let candidates =
            crate::pipeline::assemble_archive_password_candidates(Some("given-03"), &supplied, &[]);
        let mut expected = vec![("explicit", "given-03".to_string())];
        expected.extend(
            bucket("integration", &given[..10])
                .into_iter()
                .filter(|(_, v)| v != "given-03"),
        );
        assert_eq!(candidates.len(), 10);
        assert_eq!(owned(labelled(&candidates)), expected);
    }

    #[test]
    fn header_bucket_keeps_ten_in_the_order_received() {
        let received = numbered("header", 12);
        let supplied = crate::ingest::header_archive_password_candidates(
            std::iter::once("header-01").chain(received.iter().map(String::as_str)),
        );
        let candidates =
            crate::pipeline::assemble_archive_password_candidates(None, &supplied, &[]);
        assert_eq!(
            owned(labelled(&candidates)),
            bucket("dnzb_header", &received[..10])
        );

        // Values the integration bucket already holds take no header slot.
        let mut carried =
            crate::ingest::integration_archive_password_candidates(["header-01", "header-02"]);
        carried.extend(supplied);
        let candidates = crate::pipeline::assemble_archive_password_candidates(None, &carried, &[]);
        let mut expected = bucket("integration", &received[..2]);
        expected.extend(bucket("dnzb_header", &received[2..12]));
        assert_eq!(owned(labelled(&candidates)), expected);
    }

    #[test]
    fn nzb_bucket_keeps_ten_meta_in_document_order_with_the_file_name_last() {
        let nzb = twelve_meta_nzb();
        let meta = numbered("meta", 12);
        let path = std::path::Path::new("Capped.Meta{{from-name}}.nzb");

        let carried = crate::ingest::nzb_password_candidates(&nzb, path, None);
        let candidates = crate::pipeline::assemble_archive_password_candidates(None, &carried, &[]);
        assert_eq!(
            owned(labelled(&candidates)),
            bucket("nzb_meta", &meta[..10])
        );

        // The file name's password counts inside the bucket, after the meta.
        let few = weaver_nzb::parse_nzb(
            minimal_nzb("Few.Meta")
                .replace(
                    "<file ",
                    "<head><meta type=\"password\">meta-01</meta></head>\n  <file ",
                )
                .as_bytes(),
        )
        .unwrap();
        let carried = crate::ingest::nzb_password_candidates(&few, path, None);
        let candidates = crate::pipeline::assemble_archive_password_candidates(None, &carried, &[]);
        assert_eq!(
            labelled(&candidates),
            vec![
                ("nzb_meta", "meta-01"),
                ("filename_convention", "from-name")
            ]
        );

        // Values an earlier bucket holds take no NZB slot, and a restart's
        // re-harvest of the same NZB keeps enough to fill it.
        let mut carried =
            crate::ingest::integration_archive_password_candidates(["meta-01", "meta-02"]);
        carried.extend(crate::ingest::nzb_password_candidates(&nzb, path, None));
        let candidates = crate::pipeline::assemble_archive_password_candidates(None, &carried, &[]);
        let mut expected = bucket("integration", &meta[..2]);
        expected.extend(bucket("nzb_meta", &meta[2..12]));
        assert_eq!(owned(labelled(&candidates)), expected);
    }

    #[test]
    fn global_bucket_keeps_fifty() {
        let global = numbered("global", 60);
        let candidates = crate::pipeline::assemble_archive_password_candidates(
            None,
            &[],
            &settings_candidates(&global),
        );
        assert_eq!(
            owned(labelled(&candidates)),
            bucket("settings", &global[..50])
        );
    }

    #[test]
    fn four_full_buckets_make_eighty_in_bucket_order() {
        use crate::settings::archive_passwords::{
            MAX_ARCHIVE_PASSWORDS, MAX_HEADER_ARCHIVE_PASSWORDS, MAX_INTEGRATION_ARCHIVE_PASSWORDS,
            MAX_NZB_ARCHIVE_PASSWORDS,
        };
        assert_eq!(
            MAX_INTEGRATION_ARCHIVE_PASSWORDS
                + MAX_HEADER_ARCHIVE_PASSWORDS
                + MAX_NZB_ARCHIVE_PASSWORDS
                + MAX_ARCHIVE_PASSWORDS,
            80
        );
        let given = numbered("given", 12);
        let received = numbered("header", 12);
        let meta = numbered("meta", 12);
        let global = numbered("global", 60);

        let mut carried = crate::ingest::integration_archive_password_candidates(
            given.iter().map(String::as_str),
        );
        carried.extend(crate::ingest::header_archive_password_candidates(
            received.iter().map(String::as_str),
        ));
        carried.extend(crate::ingest::nzb_password_candidates(
            &twelve_meta_nzb(),
            std::path::Path::new("Capped.Meta{{from-name}}.nzb"),
            None,
        ));
        let candidates = crate::pipeline::assemble_archive_password_candidates(
            Some("spec"),
            &carried,
            &settings_candidates(&global),
        );

        assert_eq!(candidates.len(), 80);
        let mut expected = vec![("explicit", "spec".to_string())];
        expected.extend(bucket("integration", &given[..9]));
        expected.extend(bucket("dnzb_header", &received[..10]));
        expected.extend(bucket("nzb_meta", &meta[..10]));
        expected.extend(bucket("settings", &global[..50]));
        assert_eq!(owned(labelled(&candidates)), expected);
    }

    // The header is repeatable and each occurrence is one password, matched
    // case-insensitively; a value holding a comma is one password, not two.
    #[tokio::test]
    async fn fetch_nzb_reads_every_dnzb_password_header_value() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let xml = minimal_nzb("Header.Passwords");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            read_http_request(&mut stream).await;
            let response = format!(
                "HTTP/1.1 200 OK\r\n\
                 Content-Type: application/x-nzb\r\n\
                 X-DNZB-Password: first-key\r\n\
                 x-dnzb-password: second-key\r\n\
                 X-Dnzb-Password: comma,in,one\r\n\
                 Content-Length: {}\r\n\
                 Connection: close\r\n\
                 \r\n\
                 {}",
                xml.len(),
                xml
            );
            stream.write_all(response.as_bytes()).await.unwrap();
        });

        let url = format!("http://{addr}/download.nzb");
        let fetched = fetch_nzb_from_url(&crate::proxies::direct_runtime(), &url)
            .await
            .unwrap();
        server.await.unwrap();

        assert_eq!(
            fetched.header_passwords,
            ["first-key", "second-key", "comma,in,one"]
        );
        let candidates = crate::ingest::header_archive_password_candidates(
            fetched.header_passwords.iter().map(String::as_str),
        );
        assert_eq!(
            candidates
                .iter()
                .map(|candidate| (candidate.source().as_str(), candidate.value()))
                .collect::<Vec<_>>(),
            [
                ("dnzb_header", "first-key"),
                ("dnzb_header", "second-key"),
                ("dnzb_header", "comma,in,one"),
            ]
        );
        assert!(!format!("{fetched:?}").contains("first-key"));
    }

    #[tokio::test]
    async fn fetch_nzb_keeps_unparsed_payload_for_scan_scripts() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let payload = b"incomplete NZB awaiting scan repair";
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            read_http_request(&mut stream).await;
            let headers = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                payload.len()
            );
            stream.write_all(headers.as_bytes()).await.unwrap();
            stream.write_all(payload).await.unwrap();
        });
        let url = format!("http://{addr}/repairable.nzb");
        let bytes = fetch_nzb_from_url(&crate::proxies::direct_runtime(), &url)
            .await
            .unwrap()
            .bytes;
        server.await.unwrap();
        assert_eq!(bytes, payload);
        assert!(weaver_nzb::parse_nzb(&bytes).is_err());
    }

    #[tokio::test]
    async fn fetch_nzb_refuses_a_private_destination_before_connecting() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let url = format!("http://{}/private.nzb", listener.local_addr().unwrap());

        let runtime = crate::proxies::direct_runtime();
        runtime.set_allow_private_network(false);
        let error = fetch_nzb_from_url(&runtime, &url).await.unwrap_err();

        assert!(error.to_string().contains("not allowed"), "{error}");
        // Nothing connected: the refusal came before any request.
        assert_eq!(
            listener.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
    }
}
