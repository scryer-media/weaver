//! Durable duplicate-admission identity records.

use std::collections::{BTreeMap, BTreeSet};

use crate::StateError;
use crate::jobs::duplicate::{
    CallerScopedIdempotency, DuplicateAction, DuplicateDecision, DuplicateJobLifecycle,
    DuplicateJobSnapshot, DuplicateMatch, DuplicateMode, DuplicatePolicy, FingerprintEvidence,
    FingerprintKind, JobFingerprint, SemanticDuplicate, SemanticDuplicateLifecycleEvent,
    SemanticTerminalCause, SubmissionOrigin, classify_semantic_terminal_cause,
    record_semantic_duplicate_lifecycle_metric,
};
use crate::jobs::ids::JobId;
use crate::persistence::Database;
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime, SqlTx};
use crate::settings::persistence::reserve_next_job_id_tx;

const RESERVATION_TTL_SECONDS: i64 = 5 * 60;
const PROMOTION_LEASE_SECONDS: i64 = 30;
const BACKFILL_KEY: &str = "duplicate-fingerprints/v1";
const DUPLICATE_SUMMARY_BATCH_LIMIT: usize = 256;

#[derive(Debug, Clone)]
pub struct DuplicateAdmissionRequest {
    pub evidence: FingerprintEvidence,
    pub mode: DuplicateMode,
    pub semantic: Option<SemanticDuplicate>,
    /// Present for SCORE candidates so a parked candidate can be promoted
    /// after restart without creating a queue/history placeholder first.
    pub semantic_source: Option<SemanticCandidateSource>,
    pub origin: SubmissionOrigin,
    pub idempotency: Option<CallerScopedIdempotency>,
    pub policy: DuplicatePolicy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SemanticCandidateSource {
    pub nzb_zstd: Vec<u8>,
    pub filename: Option<String>,
    pub password: Option<String>,
    pub category: Option<String>,
    pub metadata: Vec<(String, String)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SemanticCandidateState {
    Active,
    Parked,
    Nonblocking,
    Suppressed,
}

impl SemanticCandidateState {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Parked => "parked",
            Self::Nonblocking => "nonblocking",
            Self::Suppressed => "suppressed",
        }
    }

    fn from_persisted(value: &str) -> Option<Self> {
        match value {
            "active" => Some(Self::Active),
            "parked" => Some(Self::Parked),
            "nonblocking" => Some(Self::Nonblocking),
            "suppressed" => Some(Self::Suppressed),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SemanticPromotionState {
    None,
    Pending,
    Claimed,
}

impl SemanticPromotionState {
    const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Pending => "pending",
            Self::Claimed => "claimed",
        }
    }

    fn from_persisted(value: &str) -> Option<Self> {
        match value {
            "none" => Some(Self::None),
            "pending" => Some(Self::Pending),
            "claimed" => Some(Self::Claimed),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SemanticAdmission {
    pub group_id: i64,
    pub normalized_key: String,
    pub score: i64,
    pub state: SemanticCandidateState,
    /// Monotonic authority generation consumed atomically with active-job
    /// persistence. A superseded pre-enqueue request cannot materialize.
    pub materialization_generation: i64,
    pub superseded_job_id: Option<JobId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SemanticCandidateSnapshot {
    pub job_id: JobId,
    pub group_id: i64,
    pub normalized_key: String,
    pub score: i64,
    pub state: SemanticCandidateState,
    pub source_stored: bool,
    pub terminal_cause: Option<SemanticTerminalCause>,
    pub promotion_state: SemanticPromotionState,
}

/// Durable duplicate identity and evidence for one job. The bulk loader below
/// returns these in one bounded query so queue/history renderers do not need
/// per-job snapshot and fingerprint lookups.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DuplicateJobSummary {
    pub snapshot: DuplicateJobSnapshot,
    /// Exact action persisted at admission time; consumers must not infer it
    /// from the job's current lifecycle.
    pub admission_action: DuplicateAction,
    /// Fingerprint kind that produced the persisted admission action. This is
    /// absent for backfilled jobs and semantic-only decisions.
    pub admission_reason: Option<FingerprintKind>,
    pub fingerprints: Vec<JobFingerprint>,
    pub semantic: Option<SemanticCandidateSnapshot>,
}

/// Private source-bearing claim returned only to backend materialization code.
/// It is deliberately not projected through GraphQL/history APIs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SemanticPromotionClaim {
    pub trigger_job_id: JobId,
    pub job_id: JobId,
    pub group_id: i64,
    /// Monotonic lease generation. Complete/release are no-ops for an expired
    /// or superseded owner.
    pub generation: i64,
    pub source: SemanticCandidateSource,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DuplicateAdmission {
    Accepted {
        job_id: JobId,
        decision: DuplicateDecision,
        created_at: i64,
        semantic: Option<SemanticAdmission>,
    },
    /// The candidate has a durable identity and source NZB, but intentionally
    /// has no scheduler job, working directory, or visible history row.
    Parked {
        job_id: JobId,
        decision: DuplicateDecision,
        created_at: i64,
        semantic: SemanticAdmission,
    },
    Idempotent {
        job_id: JobId,
        created_at: i64,
    },
    IdempotencyConflict {
        job_id: JobId,
    },
    Blocked {
        decision: DuplicateDecision,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DuplicateBackfillState {
    pub cursor_job_id: Option<JobId>,
    pub completed_at: Option<i64>,
}

/// A bounded unit of historical NZB material to parse outside the database
/// and scheduler threads. It is intentionally not a public/API type.
#[derive(Debug, Clone)]
pub struct DuplicateBackfillSource {
    pub job_id: JobId,
    pub raw_job_hash: Option<[u8; 32]>,
    pub nzb_zstd: Option<Vec<u8>>,
    pub lifecycle: DuplicateJobLifecycle,
    pub created_at: i64,
}

/// Parser-validated evidence ready for one atomic backfill commit.
#[derive(Debug, Clone)]
pub struct DuplicateBackfillEntry {
    pub job_id: JobId,
    pub lifecycle: DuplicateJobLifecycle,
    pub created_at: i64,
    pub evidence: FingerprintEvidence,
}

impl Database {
    /// Atomically allocates a job id, records the evidence and claims its
    /// identities. The short-lived reservation is recovered if the scheduler
    /// never materializes the active job after a process crash.
    pub fn admit_duplicate_submission(
        &self,
        request: &DuplicateAdmissionRequest,
    ) -> Result<DuplicateAdmission, StateError> {
        let datastore = self.datastore();
        let request = request.clone();
        let now = epoch_seconds();
        let outcome = self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "admit_duplicate_submission", |tx| {
                let request = request.clone();
                Box::pin(async move { admit_duplicate_submission_tx(tx, &request, now).await })
            })
            .await
        });
        if let Ok(admission) = &outcome {
            match admission {
                DuplicateAdmission::Parked { .. } => record_semantic_duplicate_lifecycle_metric(
                    SemanticDuplicateLifecycleEvent::Park,
                ),
                DuplicateAdmission::Accepted {
                    semantic: Some(semantic),
                    ..
                } if semantic.superseded_job_id.is_some() => {
                    record_semantic_duplicate_lifecycle_metric(
                        SemanticDuplicateLifecycleEvent::Supersede,
                    );
                }
                _ => {}
            }
        }
        outcome
    }

    pub fn activate_duplicate_admission(&self, job_id: JobId) -> Result<(), StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.read_exec(),
                "UPDATE duplicate_job_snapshots
                 SET lifecycle = {}, updated_at = {}
                 WHERE job_id = {}",
                &[
                    SqlArg::Text(DuplicateJobLifecycle::Active.as_str().to_string()),
                    SqlArg::I64(epoch_seconds()),
                    SqlArg::I64(job_id.0 as i64),
                ],
            )
            .await?;
            Ok(())
        })
    }

    /// Removes a reservation that did not become a scheduler-owned job. This
    /// is intentionally separate from visible-history deletion.
    pub fn release_duplicate_admission(&self, job_id: JobId) -> Result<(), StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "release_duplicate_admission", |tx| {
                Box::pin(async move { forget_duplicate_identity_tx(tx, job_id).await })
            })
            .await
        })
    }

    /// Explicitly forgets durable duplicate identity. Normal history deletion
    /// must not call this: duplicate identity deliberately outlives retention.
    pub fn forget_duplicate_identity(&self, job_id: JobId) -> Result<bool, StateError> {
        let datastore = self.datastore();
        let now = epoch_seconds();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "forget_duplicate_identity", |tx| {
                Box::pin(async move {
                    let exists = tx
                        .fetch_optional(
                            "SELECT 1 AS known WHERE EXISTS (
                                SELECT 1 FROM duplicate_job_snapshots WHERE job_id = {}
                             ) OR EXISTS (
                                SELECT 1 FROM active_jobs WHERE job_id = {}
                             ) OR EXISTS (
                                SELECT 1 FROM job_history WHERE job_id = {}
                             ) OR EXISTS (
                                SELECT 1 FROM forgotten_duplicate_identities WHERE job_id = {}
                             )",
                            &[
                                SqlArg::I64(job_id.0 as i64),
                                SqlArg::I64(job_id.0 as i64),
                                SqlArg::I64(job_id.0 as i64),
                                SqlArg::I64(job_id.0 as i64),
                            ],
                        )
                        .await?
                        .is_some();
                    if exists {
                        tx.execute(
                            "INSERT INTO forgotten_duplicate_identities (job_id, forgotten_at)
                             VALUES ({}, {})
                             ON CONFLICT(job_id) DO UPDATE SET forgotten_at = excluded.forgotten_at",
                            &[SqlArg::I64(job_id.0 as i64), SqlArg::I64(now)],
                        )
                        .await?;
                        forget_duplicate_identity_tx(tx, job_id).await?;
                    }
                    Ok(exists)
                })
            })
            .await
        })
    }

    pub fn cleanup_stale_duplicate_admissions(&self) -> Result<usize, StateError> {
        let datastore = self.datastore();
        let now = epoch_seconds();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "cleanup_stale_duplicate_admissions", |tx| {
                Box::pin(async move { cleanup_stale_admissions_tx(tx, now).await })
            })
            .await
        })
    }

    pub fn duplicate_snapshot(
        &self,
        job_id: JobId,
    ) -> Result<Option<DuplicateJobSnapshot>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT job_id, lifecycle, normalized_name, created_at, updated_at, reservation_expires_at
                 FROM duplicate_job_snapshots WHERE job_id = {}",
                &[SqlArg::I64(job_id.0 as i64)],
            )
            .await?
            .map(snapshot_from_row)
            .transpose()
        })
    }

    pub fn semantic_candidate_snapshot(
        &self,
        job_id: JobId,
    ) -> Result<Option<SemanticCandidateSnapshot>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT c.job_id, c.group_id, g.normalized_key, c.score, c.candidate_state,
                        c.source_nzb_zstd, c.terminal_cause, c.promotion_state
                 FROM semantic_duplicate_candidates AS c
                 INNER JOIN semantic_duplicate_groups AS g ON g.group_id = c.group_id
                 WHERE c.job_id = {}",
                &[SqlArg::I64(job_id.0 as i64)],
            )
            .await?
            .map(|row| {
                let state = row
                    .text("candidate_state")
                    .ok()
                    .and_then(|value| SemanticCandidateState::from_persisted(&value))
                    .ok_or_else(|| {
                        StateError::Database("invalid semantic candidate state".to_string())
                    })?;
                Ok(SemanticCandidateSnapshot {
                    job_id: JobId(row.i64("job_id")? as u64),
                    group_id: row.i64("group_id")?,
                    normalized_key: row.text("normalized_key")?,
                    score: row.i64("score")?,
                    state,
                    source_stored: row
                        .bytes("source_nzb_zstd")
                        .map(|bytes| !bytes.is_empty())
                        .unwrap_or(false),
                    terminal_cause: row
                        .opt_text("terminal_cause")?
                        .as_deref()
                        .and_then(SemanticTerminalCause::from_persisted),
                    promotion_state: row
                        .text("promotion_state")
                        .ok()
                        .and_then(|value| SemanticPromotionState::from_persisted(&value))
                        .ok_or_else(|| {
                            StateError::Database("invalid promotion state".to_string())
                        })?,
                })
            })
            .transpose()
        })
    }

    /// Loads at most 256 requested duplicate summaries with one bounded query
    /// on both SQLite and PostgreSQL. Missing identities are omitted.
    pub fn duplicate_summaries(
        &self,
        job_ids: &[JobId],
    ) -> Result<BTreeMap<JobId, DuplicateJobSummary>, StateError> {
        let job_ids = job_ids
            .iter()
            .copied()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .take(DUPLICATE_SUMMARY_BATCH_LIMIT)
            .collect::<Vec<_>>();
        if job_ids.is_empty() {
            return Ok(BTreeMap::new());
        }
        let placeholders = std::iter::repeat_n("{}", job_ids.len())
            .collect::<Vec<_>>()
            .join(", ");
        let args = job_ids
            .iter()
            .map(|job_id| SqlArg::I64(job_id.0 as i64))
            .collect::<Vec<_>>();
        let query = format!(
            "SELECT s.job_id, s.lifecycle, s.normalized_name, s.admission_action,
                    s.admission_reason, s.created_at,
                    s.updated_at, s.reservation_expires_at,
                    f.fingerprint_kind, f.fingerprint_version, f.fingerprint_digest,
                    c.group_id AS semantic_group_id, g.normalized_key AS semantic_normalized_key,
                    c.score AS semantic_score, c.candidate_state AS semantic_candidate_state,
                    c.source_nzb_zstd AS semantic_source_nzb_zstd,
                    c.terminal_cause AS semantic_terminal_cause,
                    c.promotion_state AS semantic_promotion_state
             FROM duplicate_job_snapshots AS s
             LEFT JOIN job_fingerprints AS f ON f.job_id = s.job_id
             LEFT JOIN semantic_duplicate_candidates AS c ON c.job_id = s.job_id
             LEFT JOIN semantic_duplicate_groups AS g ON g.group_id = c.group_id
             WHERE s.job_id IN ({placeholders})
             ORDER BY s.job_id ASC, f.fingerprint_kind ASC, f.fingerprint_version ASC"
        );
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let rows = SqlRuntime::fetch_all(datastore.read_exec(), &query, &args).await?;
            let mut summaries = BTreeMap::new();
            for row in rows {
                let job_id = JobId(row.i64("job_id")? as u64);
                let snapshot = duplicate_snapshot_from_summary_row(&row)?;
                let admission_action = row
                    .text("admission_action")
                    .ok()
                    .and_then(|value| DuplicateAction::from_persisted(&value))
                    .ok_or_else(|| {
                        StateError::Database("invalid duplicate admission action".to_string())
                    })?;
                let admission_reason = match row.opt_text("admission_reason")? {
                    Some(value) => {
                        Some(FingerprintKind::from_persisted(&value).ok_or_else(|| {
                            StateError::Database("invalid duplicate admission reason".to_string())
                        })?)
                    }
                    None => None,
                };
                let entry = summaries
                    .entry(job_id)
                    .or_insert_with(|| DuplicateJobSummary {
                        snapshot,
                        admission_action,
                        admission_reason,
                        fingerprints: Vec::new(),
                        semantic: None,
                    });
                if let Some(kind) = row.opt_text("fingerprint_kind")? {
                    let kind = FingerprintKind::from_persisted(&kind).ok_or_else(|| {
                        StateError::Database("invalid duplicate fingerprint kind".to_string())
                    })?;
                    let digest = row.bytes("fingerprint_digest")?;
                    let digest = digest.try_into().map_err(|_| {
                        StateError::Database("invalid duplicate fingerprint digest".to_string())
                    })?;
                    entry.fingerprints.push(JobFingerprint {
                        kind,
                        version: row.i64("fingerprint_version")? as u16,
                        digest,
                    });
                }
                if entry.semantic.is_none() && row.opt_text("semantic_candidate_state")?.is_some() {
                    entry.semantic = Some(semantic_snapshot_from_summary_row(&row)?);
                }
            }
            for summary in summaries.values_mut() {
                summary.fingerprints.sort();
                summary.fingerprints.dedup();
            }
            Ok(summaries)
        })
    }

    /// Marks a successful candidate as good and permanently suppresses its
    /// lower-or-equal parked alternatives. Higher-score upgrades remain valid.
    pub fn mark_semantic_candidate_good(&self, job_id: JobId) -> Result<bool, StateError> {
        let datastore = self.datastore();
        let now = epoch_seconds();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "mark_semantic_candidate_good", |tx| {
                Box::pin(async move {
                    let Some(row) = tx
                        .fetch_optional(
                            "SELECT c.group_id, c.score
                             FROM semantic_duplicate_candidates AS c
                             INNER JOIN duplicate_job_snapshots AS s ON s.job_id = c.job_id
                             WHERE c.job_id = {} AND c.candidate_state = {}
                               AND s.lifecycle = {}",
                            &[
                                SqlArg::I64(job_id.0 as i64),
                                SqlArg::Text(SemanticCandidateState::Active.as_str().to_string()),
                                SqlArg::Text(DuplicateJobLifecycle::Succeeded.as_str().to_string()),
                            ],
                        )
                        .await?
                    else {
                        return Ok(false);
                    };
                    let group_id = row.i64("group_id")?;
                    let score = row.i64("score")?;
                    tx.execute(
                        "UPDATE semantic_duplicate_candidates
                         SET candidate_state = {}, promotion_state = {}, promotion_claimed_at = NULL,
                             updated_at = {}
                         WHERE group_id = {} AND job_id <> {} AND score <= {}
                           AND candidate_state = {}",
                        &[
                            SqlArg::Text(SemanticCandidateState::Suppressed.as_str().to_string()),
                            SqlArg::Text(SemanticPromotionState::None.as_str().to_string()),
                            SqlArg::I64(now),
                            SqlArg::I64(group_id),
                            SqlArg::I64(job_id.0 as i64),
                            SqlArg::I64(score),
                            SqlArg::Text(SemanticCandidateState::Parked.as_str().to_string()),
                        ],
                    )
                    .await?;
                    tx.execute(
                        "UPDATE semantic_duplicate_candidates
                         SET promotion_state = {}, promotion_claimed_at = NULL, updated_at = {}
                         WHERE job_id = {}",
                        &[
                            SqlArg::Text(SemanticPromotionState::None.as_str().to_string()),
                            SqlArg::I64(now),
                            SqlArg::I64(job_id.0 as i64),
                        ],
                    )
                    .await?;
                    Ok(true)
                })
            })
            .await
        })
    }

    /// Explicitly marks a candidate bad. This is the only operator override
    /// that schedules semantic promotion regardless of the raw failure text.
    pub fn mark_semantic_candidate_bad(&self, job_id: JobId) -> Result<bool, StateError> {
        let datastore = self.datastore();
        let now = epoch_seconds();
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.read_exec(),
                "UPDATE semantic_duplicate_candidates
                 SET candidate_state = {}, terminal_cause = {}, promotion_state = {},
                     promotion_claimed_at = NULL, updated_at = {}
                 WHERE job_id = {} AND candidate_state <> {}",
                &[
                    SqlArg::Text(SemanticCandidateState::Nonblocking.as_str().to_string()),
                    SqlArg::Text(SemanticTerminalCause::UnknownFailure.as_str().to_string()),
                    SqlArg::Text(SemanticPromotionState::Pending.as_str().to_string()),
                    SqlArg::I64(now),
                    SqlArg::I64(job_id.0 as i64),
                    SqlArg::Text(SemanticCandidateState::Suppressed.as_str().to_string()),
                ],
            )
            .await
            .map(|changed| changed > 0)
        })
    }

    /// Reverts an operator mark-bad only when no scheduler cancellation was
    /// issued. This prevents a post-processing race from leaving a phantom
    /// pending promotion behind.
    pub fn clear_semantic_bad_transition(&self, job_id: JobId) -> Result<(), StateError> {
        let datastore = self.datastore();
        let now = epoch_seconds();
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.read_exec(),
                "UPDATE semantic_duplicate_candidates
                 SET candidate_state = {}, terminal_cause = NULL, promotion_state = {},
                     promotion_claimed_at = NULL, updated_at = {}
                 WHERE job_id = {} AND candidate_state = {} AND terminal_cause = {}
                   AND promotion_state = {}",
                &[
                    SqlArg::Text(SemanticCandidateState::Active.as_str().to_string()),
                    SqlArg::Text(SemanticPromotionState::None.as_str().to_string()),
                    SqlArg::I64(now),
                    SqlArg::I64(job_id.0 as i64),
                    SqlArg::Text(SemanticCandidateState::Nonblocking.as_str().to_string()),
                    SqlArg::Text(SemanticTerminalCause::UnknownFailure.as_str().to_string()),
                    SqlArg::Text(SemanticPromotionState::Pending.as_str().to_string()),
                ],
            )
            .await?;
            Ok(())
        })
    }

    /// Atomically chooses the best parked candidate for a promotable group and
    /// marks it claimed before any scheduler work is created. The winner rule
    /// is score descending, then oldest creation time, then lowest job ID.
    pub fn claim_semantic_promotion(
        &self,
        trigger_job_id: JobId,
    ) -> Result<Option<SemanticPromotionClaim>, StateError> {
        let datastore = self.datastore();
        let now = epoch_seconds();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "claim_semantic_promotion", |tx| {
                Box::pin(async move { claim_semantic_promotion_tx(tx, trigger_job_id, now).await })
            })
            .await
        })
    }

    /// Reclaims only an expired promotion lease. The expected generation makes
    /// concurrent runtime/API recovery attempts single-winner operations.
    fn reclaim_semantic_promotion_claim(
        &self,
        job_id: JobId,
        expected_generation: i64,
    ) -> Result<Option<SemanticPromotionClaim>, StateError> {
        let datastore = self.datastore();
        let now = epoch_seconds();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "reclaim_semantic_promotion_claim", |tx| {
                Box::pin(async move {
                    let lock = match tx {
                        SqlTx::Postgres(_) => " FOR UPDATE",
                        SqlTx::Sqlite(_) => "",
                    };
                    let Some(group) = tx
                        .fetch_optional(
                            "SELECT group_id FROM semantic_duplicate_candidates
                             WHERE job_id = {} AND promotion_state = {}
                               AND promotion_generation = {} AND promotion_lease_expires_at <= {}",
                            &[
                                SqlArg::I64(job_id.0 as i64),
                                SqlArg::Text(SemanticPromotionState::Claimed.as_str().to_string()),
                                SqlArg::I64(expected_generation),
                                SqlArg::I64(now),
                            ],
                        )
                        .await?
                    else {
                        return Ok(None);
                    };
                    let group_id = group.i64("group_id")?;
                    tx.fetch_optional(
                        &format!(
                            "SELECT group_id FROM semantic_duplicate_groups WHERE group_id = {{}}{lock}"
                        ),
                        &[SqlArg::I64(group_id)],
                    )
                    .await?
                    .ok_or_else(|| {
                        StateError::Database("semantic duplicate group disappeared".to_string())
                    })?;
                    let Some(candidate) = tx
                        .fetch_optional(
                            "SELECT job_id, source_nzb_zstd, source_filename, source_password,
                                    source_category, source_metadata_json
                             FROM semantic_duplicate_candidates AS c
                             WHERE c.job_id = {} AND c.promotion_state = {}
                               AND c.promotion_generation = {} AND c.promotion_lease_expires_at <= {}
                               AND NOT EXISTS (SELECT 1 FROM active_jobs AS a WHERE a.job_id = c.job_id)",
                            &[
                                SqlArg::I64(job_id.0 as i64),
                                SqlArg::Text(SemanticPromotionState::Claimed.as_str().to_string()),
                                SqlArg::I64(expected_generation),
                                SqlArg::I64(now),
                            ],
                        )
                        .await?
                    else {
                        return Ok(None);
                    };
                    let source = semantic_candidate_source_from_row(candidate)?;
                    let generation = expected_generation + 1;
                    let changed = tx
                        .execute(
                            "UPDATE semantic_duplicate_candidates
                             SET promotion_generation = {}, promotion_claimed_at = {},
                                 promotion_lease_expires_at = {}, updated_at = {}
                             WHERE job_id = {} AND promotion_state = {}
                               AND promotion_generation = {} AND promotion_lease_expires_at <= {}",
                            &[
                                SqlArg::I64(generation),
                                SqlArg::I64(now),
                                SqlArg::I64(now + PROMOTION_LEASE_SECONDS),
                                SqlArg::I64(now),
                                SqlArg::I64(job_id.0 as i64),
                                SqlArg::Text(SemanticPromotionState::Claimed.as_str().to_string()),
                                SqlArg::I64(expected_generation),
                                SqlArg::I64(now),
                            ],
                        )
                        .await?;
                    if changed != 1 {
                        return Ok(None);
                    }
                    Ok(Some(SemanticPromotionClaim {
                        trigger_job_id: job_id,
                        job_id,
                        group_id,
                        generation,
                        source,
                    }))
                })
            })
            .await
        })
    }

    /// Reconciles crash windows after startup: completed scheduler-owned claims
    /// are finalized, while claimed-without-active-job and pending groups are
    /// returned for idempotent materialization by the scheduler owner.
    pub fn reconcile_semantic_promotion_claims(
        &self,
        limit: usize,
    ) -> Result<Vec<SemanticPromotionClaim>, StateError> {
        let datastore = self.datastore();
        let limit = limit.clamp(1, 256) as i64;
        let now = epoch_seconds();
        let (active_claims, stale_claims, pending_triggers) =
            self.run_sql_blocking(async move {
                let active_claims = SqlRuntime::fetch_all(
                    datastore.read_exec(),
                    "SELECT c.job_id, c.promotion_generation FROM semantic_duplicate_candidates AS c
                 WHERE c.promotion_state = {}
                   AND EXISTS (SELECT 1 FROM active_jobs AS a WHERE a.job_id = c.job_id)
                 ORDER BY c.job_id ASC LIMIT {}",
                    &[
                        SqlArg::Text(SemanticPromotionState::Claimed.as_str().to_string()),
                        SqlArg::I64(limit),
                    ],
                )
                .await?
                .into_iter()
                .map(|row| Ok((JobId(row.i64("job_id")? as u64), row.i64("promotion_generation")?)))
                .collect::<Result<Vec<_>, StateError>>()?;
                let stale_claims = SqlRuntime::fetch_all(
                    datastore.read_exec(),
                    "SELECT c.job_id, c.promotion_generation
                 FROM semantic_duplicate_candidates AS c
                 WHERE c.promotion_state = {}
                   AND NOT EXISTS (SELECT 1 FROM active_jobs AS a WHERE a.job_id = c.job_id)
                   AND c.promotion_lease_expires_at <= {}
                 ORDER BY c.promotion_claimed_at ASC, c.job_id ASC LIMIT {}",
                    &[
                        SqlArg::Text(SemanticPromotionState::Claimed.as_str().to_string()),
                        SqlArg::I64(now),
                        SqlArg::I64(limit),
                    ],
                )
                .await?;
                let stale_claims = stale_claims
                    .into_iter()
                    .map(|row| {
                        Ok((
                            JobId(row.i64("job_id")? as u64),
                            row.i64("promotion_generation")?,
                        ))
                    })
                    .collect::<Result<Vec<_>, StateError>>()?;
                let pending_triggers = SqlRuntime::fetch_all(
                    datastore.read_exec(),
                    "SELECT job_id FROM semantic_duplicate_candidates
                 WHERE promotion_state = {} ORDER BY updated_at ASC, job_id ASC LIMIT {}",
                    &[
                        SqlArg::Text(SemanticPromotionState::Pending.as_str().to_string()),
                        SqlArg::I64(limit),
                    ],
                )
                .await?
                .into_iter()
                .map(|row| Ok(JobId(row.i64("job_id")? as u64)))
                .collect::<Result<Vec<_>, StateError>>()?;
                Ok((active_claims, stale_claims, pending_triggers))
            })?;

        for (job_id, generation) in active_claims {
            let _ = self.complete_semantic_promotion_claim(job_id, generation)?;
        }
        let mut claims = Vec::new();
        for (job_id, generation) in stale_claims {
            if let Some(claim) = self.reclaim_semantic_promotion_claim(job_id, generation)? {
                record_semantic_duplicate_lifecycle_metric(
                    SemanticDuplicateLifecycleEvent::PromotionRecovery,
                );
                claims.push(claim);
            }
        }
        for trigger_job_id in pending_triggers {
            if claims.len() >= limit as usize {
                break;
            }
            if let Some(claim) = self.claim_semantic_promotion(trigger_job_id)? {
                claims.push(claim);
            }
        }
        Ok(claims)
    }

    /// Marks a successfully materialized claim active. A scheduler enqueue
    /// failure must call [`Self::release_semantic_promotion_claim`] instead.
    pub fn complete_semantic_promotion_claim(
        &self,
        job_id: JobId,
        generation: i64,
    ) -> Result<bool, StateError> {
        let datastore = self.datastore();
        let now = epoch_seconds();
        let completed = self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "complete_semantic_promotion_claim", |tx| {
                Box::pin(async move {
                    let group_id = tx
                        .fetch_optional(
                            "SELECT group_id FROM semantic_duplicate_candidates
                             WHERE job_id = {} AND promotion_state = {} AND promotion_generation = {}",
                            &[
                                SqlArg::I64(job_id.0 as i64),
                                SqlArg::Text(SemanticPromotionState::Claimed.as_str().to_string()),
                                SqlArg::I64(generation),
                            ],
                        )
                        .await?
                        .map(|row| row.i64("group_id"))
                        .transpose()?;
                    let Some(group_id) = group_id else {
                        return Ok(false);
                    };
                    let changed = tx.execute(
                        "UPDATE semantic_duplicate_candidates
                         SET promotion_state = {}, promotion_claimed_at = NULL,
                             promotion_lease_expires_at = NULL, updated_at = {}
                         WHERE job_id = {} AND promotion_state = {} AND promotion_generation = {}",
                        &[
                            SqlArg::Text(SemanticPromotionState::None.as_str().to_string()),
                            SqlArg::I64(now),
                            SqlArg::I64(job_id.0 as i64),
                            SqlArg::Text(SemanticPromotionState::Claimed.as_str().to_string()),
                            SqlArg::I64(generation),
                        ],
                    )
                    .await?;
                    if changed != 1 {
                        return Ok(false);
                    }
                    tx.execute(
                        "UPDATE semantic_duplicate_candidates
                         SET promotion_state = {}, updated_at = {}
                         WHERE group_id = {} AND promotion_state = {}",
                        &[
                            SqlArg::Text(SemanticPromotionState::None.as_str().to_string()),
                            SqlArg::I64(now),
                            SqlArg::I64(group_id),
                            SqlArg::Text(SemanticPromotionState::Pending.as_str().to_string()),
                        ],
                    )
                    .await?;
                    Ok(true)
                })
            })
            .await
        });
        if matches!(completed, Ok(true)) {
            record_semantic_duplicate_lifecycle_metric(SemanticDuplicateLifecycleEvent::Promote);
        }
        completed
    }

    /// Restores the incumbent and parks a higher-score replacement when the
    /// scheduler's last-moment post-processing guard rejects supersession.
    pub fn rollback_semantic_supersession(
        &self,
        replacement_job_id: JobId,
        incumbent_job_id: JobId,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        let now = epoch_seconds();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "rollback_semantic_supersession", |tx| {
                Box::pin(async move {
                    tx.execute(
                        "UPDATE semantic_duplicate_candidates
                         SET candidate_state = {}, updated_at = {}
                         WHERE job_id = {} AND candidate_state = {}",
                        &[
                            SqlArg::Text(SemanticCandidateState::Parked.as_str().to_string()),
                            SqlArg::I64(now),
                            SqlArg::I64(replacement_job_id.0 as i64),
                            SqlArg::Text(SemanticCandidateState::Active.as_str().to_string()),
                        ],
                    )
                    .await?;
                    tx.execute(
                        "UPDATE duplicate_job_snapshots
                         SET lifecycle = {}, reservation_expires_at = NULL, updated_at = {}
                         WHERE job_id = {}",
                        &[
                            SqlArg::Text(DuplicateJobLifecycle::Parked.as_str().to_string()),
                            SqlArg::I64(now),
                            SqlArg::I64(replacement_job_id.0 as i64),
                        ],
                    )
                    .await?;
                    tx.execute(
                        "UPDATE semantic_duplicate_candidates
                         SET candidate_state = {}, updated_at = {}
                         WHERE job_id = {} AND candidate_state IN ({}, {})",
                        &[
                            SqlArg::Text(SemanticCandidateState::Active.as_str().to_string()),
                            SqlArg::I64(now),
                            SqlArg::I64(incumbent_job_id.0 as i64),
                            SqlArg::Text(SemanticCandidateState::Parked.as_str().to_string()),
                            SqlArg::Text(SemanticCandidateState::Suppressed.as_str().to_string()),
                        ],
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
        })
    }

    pub fn release_semantic_promotion_claim(
        &self,
        job_id: JobId,
        generation: i64,
    ) -> Result<bool, StateError> {
        let datastore = self.datastore();
        let now = epoch_seconds();
        let released = self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "release_semantic_promotion_claim", |tx| {
                Box::pin(async move {
                    let changed = tx.execute(
                        "UPDATE semantic_duplicate_candidates
                         SET candidate_state = {}, promotion_state = {}, promotion_claimed_at = NULL,
                             promotion_lease_expires_at = NULL, updated_at = {}
                         WHERE job_id = {} AND promotion_state = {} AND promotion_generation = {}",
                        &[
                            SqlArg::Text(SemanticCandidateState::Parked.as_str().to_string()),
                            SqlArg::Text(SemanticPromotionState::None.as_str().to_string()),
                            SqlArg::I64(now),
                            SqlArg::I64(job_id.0 as i64),
                            SqlArg::Text(SemanticPromotionState::Claimed.as_str().to_string()),
                            SqlArg::I64(generation),
                        ],
                    )
                    .await?;
                    if changed != 1 {
                        return Ok(false);
                    }
                    tx.execute(
                        "UPDATE duplicate_job_snapshots
                         SET lifecycle = {}, reservation_expires_at = NULL, updated_at = {}
                         WHERE job_id = {}",
                        &[
                            SqlArg::Text(DuplicateJobLifecycle::Parked.as_str().to_string()),
                            SqlArg::I64(now),
                            SqlArg::I64(job_id.0 as i64),
                        ],
                    )
                    .await?;
                    Ok(true)
                })
            })
            .await
        });
        if matches!(released, Ok(true)) {
            record_semantic_duplicate_lifecycle_metric(
                SemanticDuplicateLifecycleEvent::PromotionRetry,
            );
        }
        released
    }

    pub fn duplicate_backfill_state(&self) -> Result<Option<DuplicateBackfillState>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT cursor_job_id, completed_at FROM duplicate_backfill_state WHERE backfill_key = {}",
                &[SqlArg::Text(BACKFILL_KEY.to_string())],
            )
            .await?
            .map(|row| {
                Ok(DuplicateBackfillState {
                    cursor_job_id: row.opt_i64("cursor_job_id")?.map(|id| JobId(id as u64)),
                    completed_at: row.opt_i64("completed_at")?,
                })
            })
            .transpose()
        })
    }

    pub fn save_duplicate_backfill_state(
        &self,
        state: DuplicateBackfillState,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.read_exec(),
                "INSERT INTO duplicate_backfill_state
                    (backfill_key, cursor_job_id, completed_at, updated_at)
                 VALUES ({}, {}, {}, {})
                 ON CONFLICT(backfill_key) DO UPDATE SET
                    cursor_job_id = excluded.cursor_job_id,
                    completed_at = excluded.completed_at,
                    updated_at = excluded.updated_at",
                &[
                    SqlArg::Text(BACKFILL_KEY.to_string()),
                    SqlArg::OptI64(state.cursor_job_id.map(|job_id| job_id.0 as i64)),
                    SqlArg::OptI64(state.completed_at),
                    SqlArg::I64(epoch_seconds()),
                ],
            )
            .await?;
            Ok(())
        })
    }

    /// Reads a small ordered page of persisted NZBs. Parsing is deliberately
    /// left to the async caller's blocking worker, never the scheduler or SQL
    /// runtime thread. Active rows win only while not yet archived.
    pub fn load_duplicate_backfill_sources(
        &self,
        cursor_job_id: Option<JobId>,
        limit: usize,
    ) -> Result<Vec<DuplicateBackfillSource>, StateError> {
        let datastore = self.datastore();
        let after = cursor_job_id.map_or(0, |job_id| job_id.0.min(i64::MAX as u64) as i64);
        let limit = limit.clamp(1, 256) as i64;
        self.run_sql_blocking(async move {
            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT job_id, raw_job_hash, nzb_zstd, lifecycle, created_at FROM (
                     SELECT a.job_id, a.nzb_hash AS raw_job_hash, a.nzb_zstd,
                            'active' AS lifecycle, a.created_at
                       FROM active_jobs AS a
                      WHERE NOT EXISTS (
                            SELECT 1 FROM job_history AS h WHERE h.job_id = a.job_id
                        )
                     UNION ALL
                     SELECT h.job_id, h.job_hash AS raw_job_hash, h.nzb_zstd,
                            CASE
                                WHEN h.status IN ('complete', 'succeeded') THEN 'succeeded'
                                WHEN h.status = 'cancelled' THEN 'cancelled'
                                ELSE 'failed'
                            END AS lifecycle,
                            h.created_at
                       FROM job_history AS h
                 ) AS source
                 WHERE job_id > {}
                   AND NOT EXISTS (
                       SELECT 1 FROM forgotten_duplicate_identities AS forgotten
                        WHERE forgotten.job_id = source.job_id
                   )
                 ORDER BY job_id ASC
                 LIMIT {}",
                &[SqlArg::I64(after), SqlArg::I64(limit)],
            )
            .await?;
            rows.into_iter()
                .map(|row| {
                    let lifecycle = row
                        .text("lifecycle")
                        .ok()
                        .and_then(|value| DuplicateJobLifecycle::from_persisted(&value))
                        .ok_or_else(|| {
                            StateError::Database("invalid duplicate backfill lifecycle".to_string())
                        })?;
                    let raw_job_hash = row
                        .opt_bytes("raw_job_hash")?
                        .and_then(|bytes| bytes.try_into().ok());
                    Ok(DuplicateBackfillSource {
                        job_id: JobId(row.i64("job_id")? as u64),
                        raw_job_hash,
                        nzb_zstd: row.opt_bytes("nzb_zstd")?,
                        lifecycle,
                        created_at: row.i64("created_at")?,
                    })
                })
                .collect()
        })
    }

    /// Commits all parser-validated evidence and the cursor together. A crash
    /// before commit repeats an idempotent page; a crash after commit never
    /// loses its checkpoint.
    pub fn commit_duplicate_backfill_batch(
        &self,
        entries: &[DuplicateBackfillEntry],
        cursor_job_id: Option<JobId>,
        completed: bool,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        let entries = entries.to_vec();
        let now = epoch_seconds();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "commit_duplicate_backfill_batch", |tx| {
                let entries = entries.clone();
                Box::pin(async move {
                    for entry in &entries {
                        backfill_duplicate_evidence_tx(tx, entry, now).await?;
                    }
                    save_duplicate_backfill_state_tx(
                        tx,
                        DuplicateBackfillState {
                            cursor_job_id,
                            completed_at: completed.then_some(now),
                        },
                        now,
                    )
                    .await
                })
            })
            .await
        })
    }
}

async fn backfill_duplicate_evidence_tx(
    tx: &mut SqlTx<'_>,
    entry: &DuplicateBackfillEntry,
    now: i64,
) -> Result<(), StateError> {
    // Re-check source state under the commit transaction. The asynchronous
    // parser may have observed an active row immediately before it archived;
    // writing the old lifecycle here would otherwise strand an Active snapshot.
    if tx
        .fetch_optional(
            "SELECT job_id FROM forgotten_duplicate_identities WHERE job_id = {}",
            &[SqlArg::I64(entry.job_id.0 as i64)],
        )
        .await?
        .is_some()
    {
        return Ok(());
    }
    let lifecycle = if let Some(row) = tx
        .fetch_optional(
            "SELECT status FROM job_history WHERE job_id = {}",
            &[SqlArg::I64(entry.job_id.0 as i64)],
        )
        .await?
    {
        match row.text("status")?.as_str() {
            "complete" | "succeeded" => DuplicateJobLifecycle::Succeeded,
            "cancelled" => DuplicateJobLifecycle::Cancelled,
            _ => DuplicateJobLifecycle::Failed,
        }
    } else if tx
        .fetch_optional(
            "SELECT job_id FROM active_jobs WHERE job_id = {}",
            &[SqlArg::I64(entry.job_id.0 as i64)],
        )
        .await?
        .is_some()
    {
        DuplicateJobLifecycle::Active
    } else {
        // The source was deleted while parsing. Advancing the durable batch
        // checkpoint is still correct, but do not recreate identity from stale
        // parsed material.
        return Ok(());
    };
    tx.execute(
        "INSERT INTO duplicate_job_snapshots
            (job_id, lifecycle, normalized_name, admission_action, origin, created_at, updated_at, reservation_expires_at)
         VALUES ({}, {}, {}, {}, {}, {}, {}, NULL)
         ON CONFLICT(job_id) DO NOTHING",
        &[
            SqlArg::I64(entry.job_id.0 as i64),
            SqlArg::Text(lifecycle.as_str().to_string()),
            SqlArg::Text(entry.evidence.normalized_name.clone()),
            SqlArg::Text(DuplicateAction::Accept.as_str().to_string()),
            SqlArg::Text("backfill".to_string()),
            SqlArg::I64(entry.created_at),
            SqlArg::I64(now),
        ],
    )
    .await?;

    let mut fingerprints = entry.evidence.fingerprints.clone();
    fingerprints.sort();
    for fingerprint in &fingerprints {
        tx.execute(
            "INSERT INTO job_fingerprints
                (job_id, fingerprint_kind, fingerprint_version, fingerprint_digest, created_at)
             VALUES ({}, {}, {}, {}, {})
             ON CONFLICT(job_id, fingerprint_kind, fingerprint_version) DO UPDATE SET
                fingerprint_digest = excluded.fingerprint_digest",
            &[
                SqlArg::I64(entry.job_id.0 as i64),
                SqlArg::Text(fingerprint.kind.as_str().to_string()),
                SqlArg::I64(fingerprint.version as i64),
                SqlArg::Bytes(fingerprint.digest.to_vec()),
                SqlArg::I64(now),
            ],
        )
        .await?;
        tx.execute(
            "INSERT INTO duplicate_admission_claims
                (fingerprint_kind, fingerprint_version, fingerprint_digest, job_id, claimed_at)
             VALUES ({}, {}, {}, {}, {})
             ON CONFLICT(fingerprint_kind, fingerprint_version, fingerprint_digest) DO NOTHING",
            &[
                SqlArg::Text(fingerprint.kind.as_str().to_string()),
                SqlArg::I64(fingerprint.version as i64),
                SqlArg::Bytes(fingerprint.digest.to_vec()),
                SqlArg::I64(entry.job_id.0 as i64),
                SqlArg::I64(now),
            ],
        )
        .await?;
    }
    Ok(())
}

async fn save_duplicate_backfill_state_tx(
    tx: &mut SqlTx<'_>,
    state: DuplicateBackfillState,
    now: i64,
) -> Result<(), StateError> {
    tx.execute(
        "INSERT INTO duplicate_backfill_state
            (backfill_key, cursor_job_id, completed_at, updated_at)
         VALUES ({}, {}, {}, {})
         ON CONFLICT(backfill_key) DO UPDATE SET
            cursor_job_id = excluded.cursor_job_id,
            completed_at = excluded.completed_at,
            updated_at = excluded.updated_at",
        &[
            SqlArg::Text(BACKFILL_KEY.to_string()),
            SqlArg::OptI64(state.cursor_job_id.map(|job_id| job_id.0 as i64)),
            SqlArg::OptI64(state.completed_at),
            SqlArg::I64(now),
        ],
    )
    .await?;
    Ok(())
}

impl Database {
    /// Applies terminal policy from pipeline-provided typed provenance. This is
    /// queued immediately after archival, so it supersedes the legacy
    /// raw-error classifier without parsing an operator-visible error string.
    pub fn apply_semantic_terminal_cause(
        &self,
        job_id: JobId,
        cause: SemanticTerminalCause,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        let now = epoch_seconds();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "apply_semantic_terminal_cause", |tx| {
                Box::pin(async move {
                    let promotion_state = if cause.is_promotable() {
                        SemanticPromotionState::Pending
                    } else {
                        SemanticPromotionState::None
                    };
                    let candidate_state = if matches!(cause, SemanticTerminalCause::Success) {
                        SemanticCandidateState::Active
                    } else {
                        SemanticCandidateState::Nonblocking
                    };
                    tx.execute(
                        "UPDATE semantic_duplicate_candidates
                         SET candidate_state = {}, terminal_cause = {}, promotion_state = {},
                             promotion_claimed_at = NULL, promotion_lease_expires_at = NULL,
                             updated_at = {}
                         WHERE job_id = {} AND candidate_state IN ({}, {})",
                        &[
                            SqlArg::Text(candidate_state.as_str().to_string()),
                            SqlArg::Text(cause.as_str().to_string()),
                            SqlArg::Text(promotion_state.as_str().to_string()),
                            SqlArg::I64(now),
                            SqlArg::I64(job_id.0 as i64),
                            SqlArg::Text(SemanticCandidateState::Active.as_str().to_string()),
                            SqlArg::Text(SemanticCandidateState::Nonblocking.as_str().to_string()),
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

#[allow(clippy::too_many_arguments)]
pub(crate) async fn transition_duplicate_snapshot_for_history_tx(
    tx: &mut SqlTx<'_>,
    job_id: JobId,
    history_status: &str,
    raw_error: Option<&str>,
    typed_terminal_cause: Option<SemanticTerminalCause>,
    health: u32,
    failed_bytes: u64,
    now: i64,
) -> Result<(), StateError> {
    let lifecycle = match history_status {
        "complete" | "succeeded" => DuplicateJobLifecycle::Succeeded,
        "failed" => DuplicateJobLifecycle::Failed,
        "cancelled" => DuplicateJobLifecycle::Cancelled,
        _ => return Ok(()),
    };
    tx.execute(
        "UPDATE duplicate_job_snapshots
         SET lifecycle = {}, reservation_expires_at = NULL, updated_at = {}
         WHERE job_id = {}",
        &[
            SqlArg::Text(lifecycle.as_str().to_string()),
            SqlArg::I64(now),
            SqlArg::I64(job_id.0 as i64),
        ],
    )
    .await?;
    if matches!(lifecycle, DuplicateJobLifecycle::Succeeded) {
        tx.execute(
            "UPDATE semantic_duplicate_candidates
             SET terminal_cause = {}, promotion_state = {}, promotion_claimed_at = NULL,
                 promotion_lease_expires_at = NULL, updated_at = {}
             WHERE job_id = {} AND candidate_state = {}",
            &[
                SqlArg::Text(SemanticTerminalCause::Success.as_str().to_string()),
                SqlArg::Text(SemanticPromotionState::None.as_str().to_string()),
                SqlArg::I64(now),
                SqlArg::I64(job_id.0 as i64),
                SqlArg::Text(SemanticCandidateState::Active.as_str().to_string()),
            ],
        )
        .await?;
    } else if matches!(
        lifecycle,
        DuplicateJobLifecycle::Failed | DuplicateJobLifecycle::Cancelled
    ) {
        let cause = typed_terminal_cause.unwrap_or_else(|| {
            classify_semantic_terminal_cause(history_status, raw_error, health, failed_bytes)
        });
        let promotion_state = if cause.is_promotable() {
            SemanticPromotionState::Pending
        } else {
            SemanticPromotionState::None
        };
        tx.execute(
            "UPDATE semantic_duplicate_candidates
             SET candidate_state = {}, terminal_cause = {}, promotion_state = {},
                 promotion_claimed_at = NULL, promotion_lease_expires_at = NULL, updated_at = {}
             WHERE job_id = {} AND candidate_state = {}",
            &[
                SqlArg::Text(SemanticCandidateState::Nonblocking.as_str().to_string()),
                SqlArg::Text(cause.as_str().to_string()),
                SqlArg::Text(promotion_state.as_str().to_string()),
                SqlArg::I64(now),
                SqlArg::I64(job_id.0 as i64),
                SqlArg::Text(SemanticCandidateState::Active.as_str().to_string()),
            ],
        )
        .await?;
    }
    Ok(())
}

async fn admit_duplicate_submission_tx(
    tx: &mut SqlTx<'_>,
    request: &DuplicateAdmissionRequest,
    now: i64,
) -> Result<DuplicateAdmission, StateError> {
    cleanup_stale_admissions_tx(tx, now).await?;

    if let Some(key) = &request.idempotency
        && let Some(existing) = load_idempotency_tx(tx, key).await?
    {
        return Ok(existing_idempotency_outcome(existing, request));
    }

    let job_id = JobId(reserve_next_job_id_tx(tx).await?);
    let mode = if request.origin.bypasses_duplicate_policy() {
        DuplicateMode::Force
    } else {
        request.mode
    };
    let expires_at = now + RESERVATION_TTL_SECONDS;
    tx.execute(
        "INSERT INTO duplicate_job_snapshots
            (job_id, lifecycle, normalized_name, admission_action, origin, created_at, updated_at, reservation_expires_at)
         VALUES ({}, {}, {}, {}, {}, {}, {}, {})",
        &[
            SqlArg::I64(job_id.0 as i64),
            SqlArg::Text(DuplicateJobLifecycle::Reserved.as_str().to_string()),
            SqlArg::Text(request.evidence.normalized_name.clone()),
            SqlArg::Text(DuplicateAction::Accept.as_str().to_string()),
            SqlArg::Text(request.origin.as_str().to_string()),
            SqlArg::I64(now),
            SqlArg::I64(now),
            SqlArg::I64(expires_at),
        ],
    )
    .await?;

    if let Some(key) = &request.idempotency {
        let claimed = tx
            .execute(
                "INSERT INTO submission_idempotency
                    (caller_scope, idempotency_key, request_hash, job_id, created_at)
                 VALUES ({}, {}, {}, {}, {}) ON CONFLICT(caller_scope, idempotency_key) DO NOTHING",
                &[
                    SqlArg::Text(key.caller.clone()),
                    SqlArg::Text(key.key.clone()),
                    SqlArg::Bytes(request.evidence.raw_job_hash.to_vec()),
                    SqlArg::I64(job_id.0 as i64),
                    SqlArg::I64(now),
                ],
            )
            .await?;
        if claimed == 0 {
            let existing = load_idempotency_tx(tx, key)
                .await?
                .ok_or_else(|| StateError::Database("idempotency claim disappeared".to_string()))?;
            forget_duplicate_identity_tx(tx, job_id).await?;
            return Ok(existing_idempotency_outcome(existing, request));
        }
    }

    let mut fingerprints = request.evidence.fingerprints.clone();
    fingerprints.sort();
    for fingerprint in &fingerprints {
        insert_fingerprint_tx(tx, job_id, fingerprint).await?;
        tx.execute(
            "INSERT INTO duplicate_admission_claims (fingerprint_kind, fingerprint_version, fingerprint_digest, job_id, claimed_at)
             VALUES ({}, {}, {}, {}, {})
             ON CONFLICT(fingerprint_kind, fingerprint_version, fingerprint_digest) DO NOTHING",
            &[
                SqlArg::Text(fingerprint.kind.as_str().to_string()),
                SqlArg::I64(fingerprint.version as i64),
                SqlArg::Bytes(fingerprint.digest.to_vec()),
                SqlArg::I64(job_id.0 as i64),
                SqlArg::I64(now),
            ],
        )
        .await?;
    }

    let matches = load_duplicate_matches_tx(tx, job_id, &fingerprints).await?;
    let decision = request.policy.decide(mode, matches);
    if matches!(decision.action, DuplicateAction::Block) {
        forget_duplicate_identity_tx(tx, job_id).await?;
        return Ok(DuplicateAdmission::Blocked { decision });
    }

    let semantic = if mode.enables_semantic_arbitration() {
        match &request.semantic {
            Some(semantic) => {
                let source = request.semantic_source.as_ref().ok_or_else(|| {
                    StateError::Database(
                        "SCORE duplicate admission requires durable source material".to_string(),
                    )
                })?;
                Some(admit_semantic_candidate_tx(tx, job_id, semantic, source, now).await?)
            }
            None => None,
        }
    } else {
        None
    };

    let admission_reason = decision
        .matches
        .iter()
        .filter(|candidate| {
            request
                .policy
                .action_for(candidate.fingerprint.kind, candidate.lifecycle)
                == decision.action
        })
        .map(|candidate| candidate.fingerprint.kind)
        .min();
    tx.execute(
        "UPDATE duplicate_job_snapshots
         SET admission_action = {}, admission_reason = {}
         WHERE job_id = {}",
        &[
            SqlArg::Text(decision.action.as_str().to_string()),
            SqlArg::OptText(admission_reason.map(|kind| kind.as_str().to_string())),
            SqlArg::I64(job_id.0 as i64),
        ],
    )
    .await?;

    if let Some(semantic) = semantic.clone()
        && matches!(semantic.state, SemanticCandidateState::Parked)
    {
        tx.execute(
            "UPDATE duplicate_job_snapshots
             SET lifecycle = {}, reservation_expires_at = NULL, updated_at = {}
             WHERE job_id = {}",
            &[
                SqlArg::Text(DuplicateJobLifecycle::Parked.as_str().to_string()),
                SqlArg::I64(now),
                SqlArg::I64(job_id.0 as i64),
            ],
        )
        .await?;
        return Ok(DuplicateAdmission::Parked {
            job_id,
            decision,
            created_at: now,
            semantic,
        });
    }

    Ok(DuplicateAdmission::Accepted {
        job_id,
        decision,
        created_at: now,
        semantic,
    })
}

async fn insert_fingerprint_tx(
    tx: &mut SqlTx<'_>,
    job_id: JobId,
    fingerprint: &JobFingerprint,
) -> Result<(), StateError> {
    tx.execute(
        "INSERT INTO job_fingerprints
            (job_id, fingerprint_kind, fingerprint_version, fingerprint_digest, created_at)
         VALUES ({}, {}, {}, {}, {})",
        &[
            SqlArg::I64(job_id.0 as i64),
            SqlArg::Text(fingerprint.kind.as_str().to_string()),
            SqlArg::I64(fingerprint.version as i64),
            SqlArg::Bytes(fingerprint.digest.to_vec()),
            SqlArg::I64(epoch_seconds()),
        ],
    )
    .await?;
    Ok(())
}

async fn load_duplicate_matches_tx(
    tx: &mut SqlTx<'_>,
    new_job_id: JobId,
    fingerprints: &[JobFingerprint],
) -> Result<Vec<DuplicateMatch>, StateError> {
    let mut matches = Vec::new();
    for fingerprint in fingerprints {
        let rows = tx
            .fetch_all(
                "SELECT s.job_id, s.lifecycle
                 FROM job_fingerprints AS f
                 INNER JOIN duplicate_job_snapshots AS s ON s.job_id = f.job_id
                 WHERE f.fingerprint_kind = {} AND f.fingerprint_version = {}
                   AND f.fingerprint_digest = {} AND f.job_id <> {}",
                &[
                    SqlArg::Text(fingerprint.kind.as_str().to_string()),
                    SqlArg::I64(fingerprint.version as i64),
                    SqlArg::Bytes(fingerprint.digest.to_vec()),
                    SqlArg::I64(new_job_id.0 as i64),
                ],
            )
            .await?;
        for row in rows {
            let lifecycle = row
                .text("lifecycle")
                .ok()
                .and_then(|value| DuplicateJobLifecycle::from_persisted(&value))
                .ok_or_else(|| StateError::Database("invalid duplicate lifecycle".to_string()))?;
            matches.push(DuplicateMatch {
                job_id: JobId(row.i64("job_id")? as u64),
                fingerprint: fingerprint.clone(),
                lifecycle,
            });
        }
    }
    Ok(matches)
}

struct ActiveSemanticCandidate {
    job_id: JobId,
    score: i64,
    lifecycle: DuplicateJobLifecycle,
    download_state: String,
    post_state: String,
}

impl ActiveSemanticCandidate {
    /// A SCORE replacement may interrupt download work, but never a job that
    /// has started post-processing. Missing active-job state is treated as
    /// non-supersedable so recovery remains conservative after a crash.
    fn can_be_superseded(&self) -> bool {
        matches!(
            self.lifecycle,
            DuplicateJobLifecycle::Reserved | DuplicateJobLifecycle::Succeeded
        ) || (matches!(self.lifecycle, DuplicateJobLifecycle::Active)
            && matches!(
                (self.download_state.as_str(), self.post_state.as_str()),
                ("queued", "idle") | ("downloading", "idle")
            ))
    }
}

async fn admit_semantic_candidate_tx(
    tx: &mut SqlTx<'_>,
    job_id: JobId,
    semantic: &SemanticDuplicate,
    source: &SemanticCandidateSource,
    now: i64,
) -> Result<SemanticAdmission, StateError> {
    tx.execute(
        "INSERT INTO semantic_duplicate_groups (normalized_key, created_at, updated_at)
         VALUES ({}, {}, {}) ON CONFLICT(normalized_key) DO NOTHING",
        &[
            SqlArg::Text(semantic.normalized_key.clone()),
            SqlArg::I64(now),
            SqlArg::I64(now),
        ],
    )
    .await?;

    let lock = match tx {
        SqlTx::Postgres(_) => " FOR UPDATE",
        SqlTx::Sqlite(_) => "",
    };
    let candidate_lock = match tx {
        // PostgreSQL rejects an unqualified FOR UPDATE when a LEFT JOIN is
        // present because the nullable side cannot be locked. The candidate
        // row is the admission state this transaction may mutate.
        SqlTx::Postgres(_) => " FOR UPDATE OF c",
        SqlTx::Sqlite(_) => "",
    };
    let group_id = tx
        .fetch_optional(
            &format!(
                "SELECT group_id FROM semantic_duplicate_groups WHERE normalized_key = {{}}{lock}"
            ),
            &[SqlArg::Text(semantic.normalized_key.clone())],
        )
        .await?
        .ok_or_else(|| StateError::Database("semantic duplicate group disappeared".to_string()))?
        .i64("group_id")?;

    let incumbent = tx
        .fetch_optional(
            &format!(
                "SELECT c.job_id, c.score, s.lifecycle,
                        COALESCE(a.download_state, '') AS download_state,
                        COALESCE(a.post_state, '') AS post_state
                 FROM semantic_duplicate_candidates AS c
                 INNER JOIN duplicate_job_snapshots AS s ON s.job_id = c.job_id
                 LEFT JOIN active_jobs AS a ON a.job_id = c.job_id
                 WHERE c.group_id = {{}} AND c.candidate_state = {{}}
                 ORDER BY c.created_at ASC, c.job_id ASC LIMIT 1{candidate_lock}"
            ),
            &[
                SqlArg::I64(group_id),
                SqlArg::Text(SemanticCandidateState::Active.as_str().to_string()),
            ],
        )
        .await?
        .map(|row| {
            let lifecycle = row
                .text("lifecycle")
                .ok()
                .and_then(|value| DuplicateJobLifecycle::from_persisted(&value))
                .ok_or_else(|| StateError::Database("invalid duplicate lifecycle".to_string()))?;
            Ok::<ActiveSemanticCandidate, StateError>(ActiveSemanticCandidate {
                job_id: JobId(row.i64("job_id")? as u64),
                score: row.i64("score")?,
                lifecycle,
                download_state: row.text("download_state")?,
                post_state: row.text("post_state")?,
            })
        })
        .transpose()?;

    // A completed semantic winner remains authoritative for admission purposes even
    // when a higher-score candidate is later allowed to replace it.  It must never
    // become a fallback download if that upgrade fails.
    let successful_score = tx
        .fetch_optional(
            "SELECT c.score
             FROM semantic_duplicate_candidates AS c
             INNER JOIN duplicate_job_snapshots AS s ON s.job_id = c.job_id
             WHERE c.group_id = {} AND s.lifecycle = {}
             ORDER BY c.score DESC, c.created_at ASC, c.job_id ASC LIMIT 1",
            &[
                SqlArg::I64(group_id),
                SqlArg::Text(DuplicateJobLifecycle::Succeeded.as_str().to_string()),
            ],
        )
        .await?
        .map(|row| row.i64("score"))
        .transpose()?;

    let mut superseded_job_id = None;
    let state = if successful_score.is_some_and(|score| semantic.score <= score) {
        SemanticCandidateState::Parked
    } else {
        match incumbent {
            None => SemanticCandidateState::Active,
            Some(incumbent)
                if matches!(
                    incumbent.lifecycle,
                    DuplicateJobLifecycle::Failed | DuplicateJobLifecycle::Cancelled
                ) =>
            {
                update_semantic_candidate_state_tx(
                    tx,
                    incumbent.job_id,
                    SemanticCandidateState::Nonblocking,
                    now,
                )
                .await?;
                SemanticCandidateState::Active
            }
            Some(incumbent)
                if semantic.score > incumbent.score && incumbent.can_be_superseded() =>
            {
                update_semantic_candidate_state_tx(
                    tx,
                    incumbent.job_id,
                    if matches!(incumbent.lifecycle, DuplicateJobLifecycle::Succeeded) {
                        SemanticCandidateState::Suppressed
                    } else {
                        SemanticCandidateState::Parked
                    },
                    now,
                )
                .await?;
                superseded_job_id = Some(incumbent.job_id);
                SemanticCandidateState::Active
            }
            Some(_) => SemanticCandidateState::Parked,
        }
    };

    let materialization_generation = matches!(state, SemanticCandidateState::Active) as i64;
    let metadata_json = serde_json::to_string(&source.metadata)
        .map_err(|error| StateError::Database(format!("semantic source metadata: {error}")))?;
    tx.execute(
        "INSERT INTO semantic_duplicate_candidates
            (job_id, group_id, score, candidate_state, source_nzb_zstd, source_filename,
             source_password, source_category, source_metadata_json, materialization_generation,
             created_at, updated_at)
         VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})",
        &[
            SqlArg::I64(job_id.0 as i64),
            SqlArg::I64(group_id),
            SqlArg::I64(semantic.score),
            SqlArg::Text(state.as_str().to_string()),
            SqlArg::Bytes(source.nzb_zstd.clone()),
            SqlArg::OptText(source.filename.clone()),
            SqlArg::OptText(source.password.clone()),
            SqlArg::OptText(source.category.clone()),
            SqlArg::Text(metadata_json),
            SqlArg::I64(materialization_generation),
            SqlArg::I64(now),
            SqlArg::I64(now),
        ],
    )
    .await?;

    Ok(SemanticAdmission {
        group_id,
        normalized_key: semantic.normalized_key.clone(),
        score: semantic.score,
        state,
        materialization_generation,
        superseded_job_id,
    })
}

async fn update_semantic_candidate_state_tx(
    tx: &mut SqlTx<'_>,
    job_id: JobId,
    state: SemanticCandidateState,
    now: i64,
) -> Result<(), StateError> {
    tx.execute(
        "UPDATE semantic_duplicate_candidates
         SET candidate_state = {}, materialization_generation = materialization_generation + 1,
             updated_at = {} WHERE job_id = {}",
        &[
            SqlArg::Text(state.as_str().to_string()),
            SqlArg::I64(now),
            SqlArg::I64(job_id.0 as i64),
        ],
    )
    .await?;
    Ok(())
}

async fn claim_semantic_promotion_tx(
    tx: &mut SqlTx<'_>,
    trigger_job_id: JobId,
    now: i64,
) -> Result<Option<SemanticPromotionClaim>, StateError> {
    let Some(trigger) = tx
        .fetch_optional(
            "SELECT group_id FROM semantic_duplicate_candidates
             WHERE job_id = {} AND promotion_state = {}",
            &[
                SqlArg::I64(trigger_job_id.0 as i64),
                SqlArg::Text(SemanticPromotionState::Pending.as_str().to_string()),
            ],
        )
        .await?
    else {
        return Ok(None);
    };
    let group_id = trigger.i64("group_id")?;
    let lock = match tx {
        SqlTx::Postgres(_) => " FOR UPDATE",
        SqlTx::Sqlite(_) => "",
    };
    // Lock the group before observing either pending or claimed alternatives.
    tx.fetch_optional(
        &format!("SELECT group_id FROM semantic_duplicate_groups WHERE group_id = {{}}{lock}"),
        &[SqlArg::I64(group_id)],
    )
    .await?
    .ok_or_else(|| StateError::Database("semantic duplicate group disappeared".to_string()))?;

    if tx
        .fetch_optional(
            &format!(
                "SELECT job_id FROM semantic_duplicate_candidates
                 WHERE group_id = {{}} AND promotion_state = {{}} LIMIT 1{lock}"
            ),
            &[
                SqlArg::I64(group_id),
                SqlArg::Text(SemanticPromotionState::Claimed.as_str().to_string()),
            ],
        )
        .await?
        .is_some()
    {
        return Ok(None);
    }

    let Some(candidate) = tx
        .fetch_optional(
            &format!(
                "SELECT c.job_id, c.promotion_generation, c.source_nzb_zstd, c.source_filename,
                        c.source_password, c.source_category, c.source_metadata_json
                 FROM semantic_duplicate_candidates AS c
                 INNER JOIN duplicate_job_snapshots AS s ON s.job_id = c.job_id
                 WHERE c.group_id = {{}} AND c.candidate_state = {{}}
                   AND s.lifecycle != {{}}
                   AND NOT EXISTS (
                       SELECT 1
                       FROM semantic_duplicate_candidates AS successful
                       INNER JOIN duplicate_job_snapshots AS successful_snapshot
                           ON successful_snapshot.job_id = successful.job_id
                       WHERE successful.group_id = c.group_id
                         AND successful_snapshot.lifecycle = {{}}
                         AND successful.score >= c.score
                   )
                 ORDER BY c.score DESC, c.created_at ASC, c.job_id ASC LIMIT 1{lock}"
            ),
            &[
                SqlArg::I64(group_id),
                SqlArg::Text(SemanticCandidateState::Parked.as_str().to_string()),
                SqlArg::Text(DuplicateJobLifecycle::Succeeded.as_str().to_string()),
                SqlArg::Text(DuplicateJobLifecycle::Succeeded.as_str().to_string()),
            ],
        )
        .await?
    else {
        tx.execute(
            "UPDATE semantic_duplicate_candidates
             SET promotion_state = {}, promotion_claimed_at = NULL,
                 promotion_lease_expires_at = NULL, updated_at = {}
             WHERE job_id = {} AND promotion_state = {}",
            &[
                SqlArg::Text(SemanticPromotionState::None.as_str().to_string()),
                SqlArg::I64(now),
                SqlArg::I64(trigger_job_id.0 as i64),
                SqlArg::Text(SemanticPromotionState::Pending.as_str().to_string()),
            ],
        )
        .await?;
        record_semantic_duplicate_lifecycle_metric(
            SemanticDuplicateLifecycleEvent::PromotionNoFallback,
        );
        return Ok(None);
    };
    let job_id = JobId(candidate.i64("job_id")? as u64);
    let generation = candidate.i64("promotion_generation")? + 1;
    let source = semantic_candidate_source_from_row(candidate)?;
    tx.execute(
        "UPDATE semantic_duplicate_candidates
         SET candidate_state = {}, promotion_state = {}, promotion_generation = {},
             promotion_claimed_at = {}, promotion_lease_expires_at = {}, updated_at = {}
         WHERE job_id = {} AND candidate_state = {} AND promotion_state = {}",
        &[
            SqlArg::Text(SemanticCandidateState::Active.as_str().to_string()),
            SqlArg::Text(SemanticPromotionState::Claimed.as_str().to_string()),
            SqlArg::I64(generation),
            SqlArg::I64(now),
            SqlArg::I64(now + PROMOTION_LEASE_SECONDS),
            SqlArg::I64(now),
            SqlArg::I64(job_id.0 as i64),
            SqlArg::Text(SemanticCandidateState::Parked.as_str().to_string()),
            SqlArg::Text(SemanticPromotionState::None.as_str().to_string()),
        ],
    )
    .await?;
    tx.execute(
        "UPDATE duplicate_job_snapshots
         SET lifecycle = {}, reservation_expires_at = NULL, updated_at = {}
         WHERE job_id = {}",
        &[
            SqlArg::Text(DuplicateJobLifecycle::Reserved.as_str().to_string()),
            SqlArg::I64(now),
            SqlArg::I64(job_id.0 as i64),
        ],
    )
    .await?;
    Ok(Some(SemanticPromotionClaim {
        trigger_job_id,
        job_id,
        group_id,
        generation,
        source,
    }))
}

fn semantic_candidate_source_from_row(
    row: crate::persistence::sql_runtime::SqlRow,
) -> Result<SemanticCandidateSource, StateError> {
    let metadata = row
        .opt_text("source_metadata_json")?
        .map(|value| serde_json::from_str(&value))
        .transpose()
        .map_err(|error| {
            StateError::Database(format!("invalid semantic source metadata: {error}"))
        })?
        .unwrap_or_default();
    Ok(SemanticCandidateSource {
        nzb_zstd: row.bytes("source_nzb_zstd").map_err(|_| {
            StateError::Database("semantic candidate source NZB is missing".to_string())
        })?,
        filename: row.opt_text("source_filename")?,
        password: row.opt_text("source_password")?,
        category: row.opt_text("source_category")?,
        metadata,
    })
}

struct IdempotencyRecord {
    job_id: JobId,
    request_hash: Vec<u8>,
    created_at: i64,
}

async fn load_idempotency_tx(
    tx: &mut SqlTx<'_>,
    key: &CallerScopedIdempotency,
) -> Result<Option<IdempotencyRecord>, StateError> {
    let lock = match tx {
        SqlTx::Postgres(_) => " FOR UPDATE",
        SqlTx::Sqlite(_) => "",
    };
    tx.fetch_optional(
        &format!(
            "SELECT job_id, request_hash, created_at FROM submission_idempotency
             WHERE caller_scope = {{}} AND idempotency_key = {{{}}}{lock}",
            ""
        ),
        &[
            SqlArg::Text(key.caller.clone()),
            SqlArg::Text(key.key.clone()),
        ],
    )
    .await?
    .map(|row| {
        Ok(IdempotencyRecord {
            job_id: JobId(row.i64("job_id")? as u64),
            request_hash: row.bytes("request_hash")?,
            created_at: row.i64("created_at")?,
        })
    })
    .transpose()
}

fn existing_idempotency_outcome(
    existing: IdempotencyRecord,
    request: &DuplicateAdmissionRequest,
) -> DuplicateAdmission {
    if existing.request_hash == request.evidence.raw_job_hash {
        DuplicateAdmission::Idempotent {
            job_id: existing.job_id,
            created_at: existing.created_at,
        }
    } else {
        DuplicateAdmission::IdempotencyConflict {
            job_id: existing.job_id,
        }
    }
}

async fn cleanup_stale_admissions_tx(tx: &mut SqlTx<'_>, now: i64) -> Result<usize, StateError> {
    let rows = tx
        .fetch_all(
            "SELECT s.job_id FROM duplicate_job_snapshots AS s
             WHERE s.lifecycle IN ({}, {}) AND s.reservation_expires_at IS NOT NULL
               AND s.reservation_expires_at <= {}
               AND NOT EXISTS (SELECT 1 FROM active_jobs AS a WHERE a.job_id = s.job_id)",
            &[
                SqlArg::Text(DuplicateJobLifecycle::Reserved.as_str().to_string()),
                SqlArg::Text(DuplicateJobLifecycle::Active.as_str().to_string()),
                SqlArg::I64(now),
            ],
        )
        .await?;
    let count = rows.len();
    for row in rows {
        forget_duplicate_identity_tx(tx, JobId(row.i64("job_id")? as u64)).await?;
    }
    Ok(count)
}

async fn forget_duplicate_identity_tx(tx: &mut SqlTx<'_>, job_id: JobId) -> Result<(), StateError> {
    tx.execute(
        "DELETE FROM semantic_duplicate_candidates WHERE job_id = {}",
        &[SqlArg::I64(job_id.0 as i64)],
    )
    .await?;
    tx.execute(
        "DELETE FROM duplicate_admission_claims WHERE job_id = {}",
        &[SqlArg::I64(job_id.0 as i64)],
    )
    .await?;
    tx.execute(
        "DELETE FROM job_fingerprints WHERE job_id = {}",
        &[SqlArg::I64(job_id.0 as i64)],
    )
    .await?;
    tx.execute(
        "DELETE FROM submission_idempotency WHERE job_id = {}",
        &[SqlArg::I64(job_id.0 as i64)],
    )
    .await?;
    tx.execute(
        "DELETE FROM duplicate_job_snapshots WHERE job_id = {}",
        &[SqlArg::I64(job_id.0 as i64)],
    )
    .await?;
    tx.execute(
        "DELETE FROM semantic_duplicate_groups
         WHERE NOT EXISTS (
            SELECT 1 FROM semantic_duplicate_candidates AS c
            WHERE c.group_id = semantic_duplicate_groups.group_id
         )",
        &[],
    )
    .await?;
    Ok(())
}

fn snapshot_from_row(
    row: crate::persistence::sql_runtime::SqlRow,
) -> Result<DuplicateJobSnapshot, StateError> {
    duplicate_snapshot_from_summary_row(&row)
}

fn duplicate_snapshot_from_summary_row(
    row: &crate::persistence::sql_runtime::SqlRow,
) -> Result<DuplicateJobSnapshot, StateError> {
    let lifecycle = row
        .text("lifecycle")
        .ok()
        .and_then(|value| DuplicateJobLifecycle::from_persisted(&value))
        .ok_or_else(|| StateError::Database("invalid duplicate lifecycle".to_string()))?;
    Ok(DuplicateJobSnapshot {
        job_id: JobId(row.i64("job_id")? as u64),
        lifecycle,
        normalized_name: row.text("normalized_name")?,
        created_at: row.i64("created_at")?,
        updated_at: row.i64("updated_at")?,
        reservation_expires_at: row.opt_i64("reservation_expires_at")?,
    })
}

fn semantic_snapshot_from_summary_row(
    row: &crate::persistence::sql_runtime::SqlRow,
) -> Result<SemanticCandidateSnapshot, StateError> {
    let state = row
        .text("semantic_candidate_state")
        .ok()
        .and_then(|value| SemanticCandidateState::from_persisted(&value))
        .ok_or_else(|| StateError::Database("invalid semantic candidate state".to_string()))?;
    let promotion_state = row
        .text("semantic_promotion_state")
        .ok()
        .and_then(|value| SemanticPromotionState::from_persisted(&value))
        .ok_or_else(|| StateError::Database("invalid promotion state".to_string()))?;
    Ok(SemanticCandidateSnapshot {
        job_id: JobId(row.i64("job_id")? as u64),
        group_id: row.i64("semantic_group_id")?,
        normalized_key: row.text("semantic_normalized_key")?,
        score: row.i64("semantic_score")?,
        state,
        source_stored: row
            .bytes("semantic_source_nzb_zstd")
            .map(|bytes| !bytes.is_empty())
            .unwrap_or(false),
        terminal_cause: row
            .opt_text("semantic_terminal_cause")?
            .as_deref()
            .and_then(SemanticTerminalCause::from_persisted),
        promotion_state,
    })
}

fn epoch_seconds() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}
