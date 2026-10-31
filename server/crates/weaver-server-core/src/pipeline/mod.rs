// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

mod activation;
pub mod archive;
mod capacity;
mod completion;
mod decode;
pub mod diagnostics;
pub mod download;
mod extraction;
pub mod graph;
mod health;
pub(crate) mod held;
mod infrastructure_retry;
mod nzb_admission;
mod orchestrator;
mod post_health;
mod progress;
pub mod queue;
mod server_attribution;
mod support_facts;
#[cfg(any(test, feature = "test-support"))]
pub mod test_support;

pub(crate) use orchestrator::is_terminal_status;

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;

use crate::pipeline::download::HeldDownloadRefill;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::{broadcast, mpsc, oneshot};
use tracing::{debug, error, info, warn};

#[cfg(test)]
use crate::RestoreJobRequest;
use crate::events::model::PipelineEvent;
#[cfg(test)]
use crate::jobs::assembly::JobAssembly;
use crate::jobs::ids::{JobId, NzbFileId, SegmentId};
use crate::jobs::{ArchivePasswordCandidate, ArchivePasswordSource};
use crate::jobs::{JobPhase, PhaseCounters};
use crate::runtime::buffers::{BufferHandle, BufferPool};
use crate::runtime::system_profile::SystemProfile;
use crate::{
    DownloadPressureReason, DownloadPressureState, DownloadQueue, DownloadWork, JobInfo, JobSpec,
    JobState, JobStatus, NntpRuntimeActivation, PipelineMetrics, RuntimeTuner, SharedPipelineState,
    TokenBucket,
};
#[cfg(test)]
use par2_rs::checksum;
use weaver_nntp::NntpClient;

use self::download::{DownloadLaneMode, DownloadLaneRuntimeState, LaneParkReason};
pub(crate) use self::extraction::ExtractionError;
pub(crate) use self::extraction::safety::ProcessMemoryPermit;
use self::extraction::{ExtractionLimits, ProcessMemoryBudget};

// Maximum number of retries for a single segment before giving up.
const MAX_SEGMENT_RETRIES: u32 = 3;
// Consecutive established-transport failures of one segment before its retry
// is held back past the server recovery backoff, so the next recovery probe
// carries different work.
const SEGMENT_TRANSPORT_STREAK_HOLD: u32 = 2;
// Consecutive established-transport failures of one segment after which the
// fault is the article's, provided other segments downloaded meanwhile.
const SEGMENT_TRANSPORT_STREAK_ARTICLE_LOCAL: u32 = 3;
// Longer than the longest server recovery backoff.
const SEGMENT_TRANSPORT_HOLD_DELAY: std::time::Duration = std::time::Duration::from_secs(90);

// A segment's run of established-transport failures.
#[derive(Debug, Clone, Copy)]
pub(super) struct TransportFailureStreak {
    pub(super) failures: u32,
    // `segments_downloaded` when the run began; any advance since proves the
    // servers were serving other articles while this one kept failing.
    pub(super) downloaded_at_start: u64,
}
const DOWNLOAD_RESTART_CHECKPOINT_BYTES: u64 = 256 * 1024 * 1024;
const DOWNLOAD_RESTART_MAX_DURABLE_LEAD_MULTIPLIER: u64 = 4;
const STALLED_DOWNLOAD_CHECK_INTERVAL: Duration = Duration::from_secs(5 * 60);
const STALLED_DOWNLOAD_IDLE_THRESHOLD: Duration = Duration::from_secs(5 * 60);

fn download_restart_checkpoint_bytes() -> u64 {
    static CHECKPOINT_BYTES: OnceLock<u64> = OnceLock::new();
    *CHECKPOINT_BYTES.get_or_init(|| {
        std::env::var("WEAVER_E2E_DOWNLOAD_RESTART_CHECKPOINT_BYTES")
            .ok()
            .and_then(|value| value.trim().parse::<u64>().ok())
            .filter(|bytes| *bytes > 0)
            .unwrap_or(DOWNLOAD_RESTART_CHECKPOINT_BYTES)
    })
}

fn health_milli(total: u64, failed_bytes: u64) -> u32 {
    total
        .saturating_sub(failed_bytes)
        .saturating_mul(1000)
        .checked_div(total)
        .unwrap_or(1000) as u32
}

// Password candidates carried by a persisted NZB: its `<meta
// type="password">` and the `{{password}}` convention in its file name, in
// harvest order. The spec's explicit password is not included.
pub(crate) fn persisted_nzb_password_candidates(
    nzb_path: &std::path::Path,
    nzb_zstd: &[u8],
) -> Result<Vec<ArchivePasswordCandidate>, crate::ingest::PersistedNzbError> {
    let nzb = crate::ingest::parse_persisted_nzb_bytes(nzb_zstd)?;
    Ok(crate::ingest::nzb_password_candidates(&nzb, nzb_path, None))
}

// The candidates a job carries, as `JobState::nzb_password_candidates` holds
// them: those supplied with the job (`Integration`, `DnzbHeader`), then its
// NZB's. The harvest sorts them into buckets by label.
pub(crate) fn job_carried_password_candidates(
    supplied: &[ArchivePasswordCandidate],
    nzb: Vec<ArchivePasswordCandidate>,
) -> Vec<ArchivePasswordCandidate> {
    let mut carried = supplied.to_vec();
    carried.extend(nzb);
    carried
}

// Builds the list from the job's carried candidates and the global ones, in
// four buckets, each capped as
// `settings::archive_passwords` states: integration (`Explicit` from the spec,
// then `Integration`), header (`DnzbHeader`), NZB (`NzbMeta`, then
// `FilenameConvention`), and global (`Settings`, then `PasswordFile`). A value
// already in the list is skipped without taking a slot, so the earliest
// source keeps its label.
pub(crate) fn assemble_archive_password_candidates(
    spec_password: Option<&str>,
    carried: &[ArchivePasswordCandidate],
    global: &[ArchivePasswordCandidate],
) -> Vec<ArchivePasswordCandidate> {
    use crate::settings::archive_passwords::{
        MAX_ARCHIVE_PASSWORDS, MAX_HEADER_ARCHIVE_PASSWORDS, MAX_INTEGRATION_ARCHIVE_PASSWORDS,
        MAX_NZB_ARCHIVE_PASSWORDS,
    };

    let spec = crate::ingest::normalize_archive_password_candidate(spec_password)
        .map(|value| ArchivePasswordCandidate::new(ArchivePasswordSource::Explicit, value));
    let job = || carried.iter();
    let with = |sources: &'static [ArchivePasswordSource]| {
        move |candidate: &&ArchivePasswordCandidate| sources.contains(&candidate.source())
    };
    let buckets: [(Vec<&ArchivePasswordCandidate>, usize); 4] = [
        (
            spec.iter()
                .chain(job().filter(with(&[ArchivePasswordSource::Integration])))
                .collect(),
            MAX_INTEGRATION_ARCHIVE_PASSWORDS,
        ),
        (
            job()
                .filter(with(&[ArchivePasswordSource::DnzbHeader]))
                .collect(),
            MAX_HEADER_ARCHIVE_PASSWORDS,
        ),
        (
            job()
                .filter(with(&[ArchivePasswordSource::NzbMeta]))
                .chain(job().filter(with(&[ArchivePasswordSource::FilenameConvention])))
                .collect(),
            MAX_NZB_ARCHIVE_PASSWORDS,
        ),
        (global.iter().collect(), MAX_ARCHIVE_PASSWORDS),
    ];

    let mut candidates: Vec<ArchivePasswordCandidate> = Vec::new();
    for (bucket, cap) in buckets {
        let mut kept = 0;
        for candidate in bucket {
            if kept >= cap {
                break;
            }
            if candidates
                .iter()
                .any(|existing| existing.value() == candidate.value())
            {
                continue;
            }
            candidates.push(candidate.clone());
            kept += 1;
        }
    }
    candidates
}

// The job half of the harvest, read from the database: the passwords
// supplied with the job, then its persisted NZB's, and whether the NZB's
// parse succeeded (`harvested`). A missing row is an empty list; a row that
// did not load is the loader's error.
pub(crate) fn read_job_password_candidates(
    db: &crate::Database,
    job_id: JobId,
) -> Result<(Vec<ArchivePasswordCandidate>, bool), crate::LoadError> {
    Ok(match db.load_active_job_password_sources(job_id)? {
        Some(((nzb_path, Some(nzb_zstd)), supplied)) => {
            match persisted_nzb_password_candidates(&nzb_path, &nzb_zstd) {
                Ok(nzb) => (job_carried_password_candidates(&supplied, nzb), true),
                Err(error) => {
                    warn!(
                        job_id = job_id.0,
                        error = %error,
                        "failed to parse persisted NZB for password candidates"
                    );
                    (Vec::new(), false)
                }
            }
        }
        Some(((_, None), supplied)) => (supplied, true),
        None => (Vec::new(), true),
    })
}

impl Pipeline {
    // Consumption boundaries may follow an await in the same actor turn. Read
    // a recorded override there even when it has not reached the runtime spec.
    pub(crate) fn job_password_at_boundary(
        &self,
        job_id: JobId,
        fallback: Option<&str>,
    ) -> Option<String> {
        self.shared_state.queue.read(|inner| {
            inner
                .jobs
                .iter()
                .find(|row| row.id == job_id)
                .and_then(|row| row.intents.update.as_ref())
                .and_then(|update| match &update.password {
                    crate::jobs::FieldUpdate::Unchanged => None,
                    crate::jobs::FieldUpdate::Clear => Some(None),
                    crate::jobs::FieldUpdate::Set(password) => Some(Some(password.clone())),
                })
                .unwrap_or_else(|| fallback.map(str::to_owned))
        })
    }

    // The one place a job's archive-password candidate list is built. Every
    // consumer takes it from here: the direct store's `-hp` and `-mhe` header
    // admission, the restart seam, archive probe and topology, finalize and
    // nested extraction.
    //
    // The job half (the passwords supplied with the job and its NZB's) is a
    // database read, a decrypt and a parse, memoized per job in
    // `nzb_password_candidates` once it has been read (a missing row or NZB is
    // remembered as empty, a failed read or parse is retried). The global half
    // is read fresh on every call, so a caller on a per-article path must
    // memoize the result itself.
    pub(crate) fn archive_password_candidates_for_job(
        &self,
        job_id: JobId,
    ) -> Vec<ArchivePasswordCandidate> {
        let state = self.jobs.get(&job_id);
        let spec_password = self.job_password_at_boundary(
            job_id,
            state.and_then(|state| state.spec.password.as_deref()),
        );
        let carried = match state.and_then(|state| state.nzb_password_candidates.get()) {
            Some(cached) => cached.clone(),
            None => {
                let (candidates, harvested) = read_job_password_candidates(&self.db, job_id)
                    .unwrap_or_else(|error| {
                        warn!(
                            job_id = job_id.0,
                            error = %error,
                            "failed to load password candidates for job"
                        );
                        (Vec::new(), false)
                    });
                // A job in the pipeline already has its row, and nothing
                // adds NZB bytes or supplied passwords to a row later, so a
                // missing row or NZB stays missing and its list is
                // remembered too. A failed read or parse is not, so it is
                // retried.
                if harvested && let Some(state) = state {
                    let _ = state.nzb_password_candidates.set(candidates.clone());
                }
                candidates
            }
        };

        let global = match self.db.global_archive_password_candidates() {
            Ok(global) => global,
            Err(_) => {
                warn!(job_id = job_id.0, "could not load global archive passwords");
                Vec::new()
            }
        };

        assemble_archive_password_candidates(spec_password.as_deref(), &carried, &global)
    }

    // Bring the job's record of full file names up to date after files were
    // renamed on disk.
    pub(crate) fn record_file_renames(
        &mut self,
        job_id: JobId,
        renames: &[crate::ingest::FileRename],
    ) {
        let Some(state) = self.jobs.get_mut(&job_id) else {
            return;
        };
        if !crate::ingest::record_file_renames(&mut state.spec.metadata, renames) {
            return;
        }
        let update = crate::jobs::JobUpdate {
            metadata: crate::jobs::FieldUpdate::Set(state.spec.metadata.clone()),
            ..Default::default()
        };
        if self.db.update_active_job(job_id, &update).is_err() {
            warn!(job_id = job_id.0, "could not persist renamed file names");
        }
    }
}

pub(super) struct DownloadLaneOwner {
    // Weak lease counters: only live response contexts keep them alive.
    body_activity: Vec<(JobId, u64, std::sync::Weak<std::sync::atomic::AtomicU64>)>,
    job_id: JobId,
    mode: DownloadLaneMode,
    completion_critical: bool,
    // The server this lane is connected to, once a refill has told the
    // scheduler where the lane lives. `None` until the first refill.
    server_idx: Option<usize>,
    connection: bool,
    outstanding: HashMap<SegmentId, DownloadWork>,
}

pub(super) struct DownloadBatchLease {
    pub(super) body_activity: Arc<std::sync::atomic::AtomicU64>,
    pub(super) lane_id: u64,
    pub(super) job_id: JobId,
    pub(super) runtime_generation: u64,
    pub(super) lane_mode: DownloadLaneMode,
    pub(super) server_modes: Vec<(usize, DownloadLaneMode)>,
    // The class every work in this batch is counted under. A batch is cut
    // from one job and one class; the worker reports it back on park so the
    // class gauges are released against what was booked.
    pub(super) completion_critical: bool,
    // The job's retention exclusions — the set server ordering and lane
    // acquisition use. Job-derived, so a server-config change applies without
    // rewriting queued work, and the same for every article on the lane:
    // each result unions it with its own work's failure ledger rather than
    // reporting this set alone.
    pub(super) effective_exclude_servers: Vec<usize>,
    // The servers the lane's adopt or dial must not use. A batch is cut for
    // one server, so this is every other server: the connection goes where
    // the scheduler's answer was for. Kept apart from the excludes above,
    // which reach the failure ledger — a pin is not a failure.
    pub(super) dial_exclude_servers: Vec<usize>,
    // Immutable common-refinement geometry captured when this batch was
    // leased. Each response carries this same snapshot through durable commit
    // so grids admitted later cannot reinterpret old decoder output.
    pub(super) checkpoint_plan: weaver_yenc::CheckpointPlan,
    // Byte pressure at lease time. Carried onto every observation this lease
    // produces so the depth explorer can discard distorted samples.
    pub(super) pressure_clear: bool,
    pub(super) works: Vec<DownloadWork>,
}

pub(super) struct DownloadLaneRefillRequest {
    pub(super) lane_id: u64,
    pub(super) runtime_generation: u64,
    pub(super) server_idx: usize,
    pub(super) supports_pipelining: bool,
    // The mode the scheduler last **booked** this lane's depth gauge under —
    // not necessarily the one it is running. A lane started on a lease mode
    // of `Pipelined { depth: 2 }` that fell back to `Sequential` is still
    // counted at depth 2 until a refill moves it, so reporting the running
    // mode here decremented a gauge nothing had incremented and underflowed
    // `download_lanes_active{mode="sequential"}`. Lanes therefore carry the
    // booked mode forward and update it from each granted lease.
    pub(super) current_mode: DownloadLaneMode,
    pub(super) response_tx: oneshot::Sender<DownloadLaneRefillResponse>,
}

pub(super) struct DownloadLaneRefillResponse {
    pub(super) lease: Option<DownloadBatchLease>,
    pub(super) park_reason: LaneParkReason,
}

pub(super) struct DownloadLaneParked {
    pub(super) lane_id: u64,
    pub(super) job_id: JobId,
    pub(super) mode: DownloadLaneMode,
    pub(super) completion_critical: bool,
    pub(super) reason: LaneParkReason,
    pub(super) release_connection_slot: bool,
}

pub(super) enum OwnedDownloadLaneEvent {
    LeaseObsolete {
        lease: DownloadBatchLease,
    },
    AcquireFailed {
        lease: DownloadBatchLease,
        error: weaver_nntp::client::BlockingBodyLaneAcquireError,
    },
    BatchComplete {
        lane_id: u64,
        results: Vec<DownloadResult>,
        unrequested_works: Vec<DownloadWork>,
        stats: weaver_nntp::blocking::BlockingLaneStats,
        // Present only on a lane's final event. Streamed per-article events
        // carry none: the bounded channel is their backpressure, and the
        // rendezvous exists to order this lane's results ahead of the park
        // message that follows on `parked_tx`.
        ack: Option<std::sync::mpsc::SyncSender<()>>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DownloadResultOrigin {
    NormalPrimary,
    Recovery,
    CompletionCriticalPrimary,
    CompletionCriticalRecovery,
}

impl DownloadResultOrigin {
    pub(super) fn from_work(is_recovery: bool, completion_critical: bool) -> Self {
        match (is_recovery, completion_critical) {
            (false, false) => Self::NormalPrimary,
            (true, false) => Self::Recovery,
            (false, true) => Self::CompletionCriticalPrimary,
            (true, true) => Self::CompletionCriticalRecovery,
        }
    }

    pub(super) fn is_recovery(self) -> bool {
        matches!(self, Self::Recovery | Self::CompletionCriticalRecovery)
    }

    pub(super) fn is_completion_critical(self) -> bool {
        matches!(
            self,
            Self::CompletionCriticalPrimary | Self::CompletionCriticalRecovery
        )
    }
}

// Result of a download task.
pub(super) struct DownloadResult {
    pub(super) lane_id: u64,
    // Job this article belongs to. Carried on the result itself so the
    // completion path never has to ask which job owns the lane it arrived on.
    pub(super) job_id: JobId,
    pub(super) segment_id: SegmentId,
    pub(super) runtime_generation: u64,
    pub(super) data: std::result::Result<DownloadPayload, DownloadError>,
    pub(super) attempts: Vec<weaver_nntp::client::FetchAttemptTrace>,
    pub(super) lane_observation: Option<DownloadLaneObservation>,
    // Server that successfully served this payload, if known.
    pub(super) source_server_idx: Option<usize>,
    // Scheduler attribution for metrics, warmup, and retry semantics.
    pub(super) origin: DownloadResultOrigin,
    // How many times this segment has been retried so far.
    pub(super) retry_count: u32,
    // Servers intentionally excluded for this fetch attempt.
    pub(super) exclude_servers: Vec<usize>,
    // Whether this result releases one NNTP connection dispatch slot.
    pub(super) release_connection_slot: bool,
}

// A delayed retry re-entering the download queue, tagged with the NNTP pool
// generation it was scheduled under so the orchestrator can drop stale
// `exclude_servers` indices after a `RebuildNntp` reshaped the pool.
pub(in crate::pipeline) struct RetryWork {
    pub(in crate::pipeline) scheduled_pool_generation: u64,
    pub(in crate::pipeline) infrastructure_retry: bool,
    pub(in crate::pipeline) work: DownloadWork,
}

#[derive(Debug, Clone)]
pub(super) struct DownloadWaitStatus {
    pub(super) reason: &'static str,
    pub(super) retry_at_epoch_ms: Option<f64>,
    pub(super) pending_count: usize,
}

#[derive(Debug, Clone)]
pub(super) struct DownloadLaneObservation {
    pub(super) server_idx: Option<usize>,
    pub(super) mode: DownloadLaneMode,
    pub(super) supports_pipelining: bool,
    // Command-to-status-line wait this one response measured, present only
    // when the lane could take an unbiased sample (nothing else outstanding
    // when the request went out).
    pub(super) latency_sample: Option<Duration>,
    // This response was its connection's first. Its timings carry setup
    // costs no later response repeats, so the depth explorer keeps it out
    // of the link model and the rung comparison.
    pub(super) cold: bool,
    // Status-line-to-terminator wait: what the article cost on the wire,
    // smoothed over the lane's warm responses.
    pub(super) transfer: Option<Duration>,
    // Decoded payload of this one response, for the depth explorer's
    // throughput window.
    pub(super) payload_bytes: u64,
    // This response's elapsed time with deliberate throttle waits removed.
    pub(super) policy_elapsed: Duration,
    // Whether byte pressure was clear when the batch was leased. Samples
    // taken under pressure say nothing about the depth under test.
    pub(super) pressure_clear: bool,
    pub(super) batch_complete: bool,
    pub(super) batch_clean: bool,
    pub(super) unresolved_count: u64,
    pub(super) connection_discarded: bool,
}

pub(super) enum DownloadPayload {
    #[cfg(test)]
    Raw(Bytes),
    Decoded(DecodeResult),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DownloadFailureKind {
    ArticleNotFound,
    CapacityUnavailable,
    ServerQuota,
    LaneUnavailable,
    Unrequested,
    ConnectionEstablishment,
    EstablishedTransport,
    Auth,
    ContentOrProtocol,
}

impl DownloadFailureKind {
    fn preserves_article_retry_budget(&self) -> bool {
        matches!(
            self,
            Self::CapacityUnavailable
                | Self::ServerQuota
                | Self::LaneUnavailable
                | Self::Unrequested
                | Self::ConnectionEstablishment
                | Self::EstablishedTransport
                | Self::Auth
        )
    }

    fn infrastructure_wait_reason(&self) -> Option<&'static str> {
        match self {
            Self::CapacityUnavailable => Some("provider connection capacity unavailable"),
            Self::ServerQuota => Some("NNTP server quota blocked"),
            Self::LaneUnavailable => Some("no eligible NNTP server"),
            Self::Unrequested => Some("NNTP lane replaced"),
            Self::ConnectionEstablishment => Some("NNTP connection unavailable"),
            Self::EstablishedTransport => Some("NNTP BODY transport unavailable"),
            Self::Auth => Some("NNTP authentication blocked"),
            Self::ArticleNotFound | Self::ContentOrProtocol => None,
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct DownloadFailure {
    pub(super) kind: DownloadFailureKind,
    pub(super) message: String,
    pub(super) retry_after: Option<Duration>,
    pub(super) quota_rejection: Option<weaver_nntp::transfer::QuotaRejection>,
}

impl DownloadFailure {
    pub(super) fn new(kind: DownloadFailureKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            retry_after: None,
            quota_rejection: None,
        }
    }

    pub(super) fn server_quota(
        message: impl Into<String>,
        rejection: weaver_nntp::transfer::QuotaRejection,
    ) -> Self {
        let retry_after = rejection
            .retry_at
            .map(|deadline| deadline.saturating_duration_since(Instant::now()));
        Self {
            kind: DownloadFailureKind::ServerQuota,
            message: message.into(),
            retry_after,
            quota_rejection: Some(rejection),
        }
    }

    fn infrastructure_kind(
        error: &weaver_nntp::NntpError,
        transport_kind: DownloadFailureKind,
    ) -> Option<DownloadFailureKind> {
        use weaver_nntp::NntpError;

        match error {
            // A 412 learns the GROUP prologue needed by the next connection;
            // no article content has been received or rejected yet.
            NntpError::NoGroupSelected => Some(DownloadFailureKind::ConnectionEstablishment),
            // The recovery gate holding the server open for one probe: local
            // admission, self-clearing, and no verdict about the article.
            NntpError::ServerRecovering
            | NntpError::PoolExhausted
            | NntpError::PoolShutdown
            | NntpError::TooManyConnections
            | NntpError::ServerOverLimit { .. }
            // Never got a socket before the deadline: local lane capacity, not
            // a transport fault of the server.
            | NntpError::AcquireTimeout(_) => Some(DownloadFailureKind::CapacityUnavailable),
            // The lease's own guard turned the work away before a command
            // went out: nothing was requested, and nothing is known about the
            // article or the server.
            NntpError::WorkObsolete => Some(DownloadFailureKind::Unrequested),
            NntpError::AuthenticationFailed
            | NntpError::AuthenticationRejected
            | NntpError::AuthenticationRequired
            | NntpError::AccessDenied => Some(DownloadFailureKind::Auth),
            // Incomplete transport framing is not evidence of corrupt article
            // content. Server health and parked retries bound repeated faults.
            NntpError::ServiceUnavailable
            | NntpError::Timeout
            | NntpError::SoftTimeout(_)
            | NntpError::ConnectionClosed
            // A session that expired part-way through a batch costs the
            // connection, not the server: the articles are re-asked on a
            // freshly authenticated one.
            | NntpError::SessionExpired
            | NntpError::ServerDisconnectedMidBody
            | NntpError::TruncatedMultilineBody
            | NntpError::MalformedMultilineTerminator
            | NntpError::Io(_) => Some(transport_kind),
            _ => None,
        }
    }

    pub(super) fn from_lane_acquire_failure(error: Option<&weaver_nntp::NntpError>) -> Self {
        use weaver_nntp::NntpError;

        let Some(error) = error else {
            return Self::new(
                DownloadFailureKind::LaneUnavailable,
                "failed to acquire BODY lane",
            );
        };
        if let NntpError::QuotaBlocked(rejection) = error {
            return Self::server_quota(error.to_string(), rejection.as_ref().clone());
        }

        let kind = Self::infrastructure_kind(error, DownloadFailureKind::ConnectionEstablishment)
            .unwrap_or(match error {
                NntpError::NoSuchGroup
                | NntpError::CommandNotRecognized
                | NntpError::TlsRequired
                | NntpError::UnexpectedResponse { .. }
                | NntpError::MalformedResponse(_) => DownloadFailureKind::ContentOrProtocol,
                _ => DownloadFailureKind::LaneUnavailable,
            });

        Self::new(kind, error.to_string())
    }

    pub(super) fn from_nntp(error: weaver_nntp::NntpError) -> Self {
        use weaver_nntp::NntpError;

        if let NntpError::QuotaBlocked(rejection) = &error {
            return Self::server_quota(error.to_string(), rejection.as_ref().clone());
        }
        if matches!(&error, NntpError::BodyNotRequestedDueToQuota { .. }) {
            return Self::new(DownloadFailureKind::Unrequested, error.to_string());
        }

        let kind = match &error {
            NntpError::ArticleNotFound
            | NntpError::NoSuchArticle { .. }
            | NntpError::NoArticleWithNumber => DownloadFailureKind::ArticleNotFound,
            error => Self::infrastructure_kind(error, DownloadFailureKind::EstablishedTransport)
                .unwrap_or(DownloadFailureKind::ContentOrProtocol),
        };

        Self::new(kind, error.to_string())
    }
}

#[derive(Debug, Clone)]
pub(super) enum DownloadError {
    Fetch(DownloadFailure),
    // Local cache or resource failure; never evidence that an article is missing.
    Local { raw_size: u64, error: String },
    Decode { raw_size: u64, error: String },
}

impl DownloadError {
    pub(super) fn local(error: String) -> Self {
        Self::Local { raw_size: 0, error }
    }

    #[cfg(test)]
    pub(super) fn fetch(kind: DownloadFailureKind, message: impl Into<String>) -> Self {
        Self::Fetch(DownloadFailure::new(kind, message))
    }

    pub(super) fn from_nntp(error: weaver_nntp::NntpError) -> Self {
        Self::Fetch(DownloadFailure::from_nntp(error))
    }

    // Whether this failure left the NNTP connection in a known-good state:
    // the server's response was read to completion and the socket can carry
    // the next BODY.
    //
    // `ArticleNotFound` (430) is a complete, bodyless server answer — it says
    // nothing about the connection. Treating it as a transport fault used to
    // QUIT the TLS session, abandon the rest of the leased batch, and block
    // the server's pipelining proof, all because one article lives on another
    // provider. `ServerQuota` / `Unrequested` are local policy outcomes where
    // no BODY was ever issued, so they are clean for the same reason (they
    // still park the lane, just without discarding it).
    //
    // Decode failures stay dirty: the yEnc decoder can fail on a body the
    // transport never finished delivering.
    pub(super) fn leaves_connection_clean(&self) -> bool {
        match self {
            Self::Fetch(failure) => matches!(
                failure.kind,
                DownloadFailureKind::ArticleNotFound
                    | DownloadFailureKind::ServerQuota
                    | DownloadFailureKind::Unrequested
            ),
            Self::Decode { .. } | Self::Local { .. } => false,
        }
    }
}

// Whether a download outcome leaves the lane's connection reusable.
//
// See [`DownloadError::leaves_connection_clean`].
pub(super) fn download_outcome_keeps_connection(
    data: &std::result::Result<DownloadPayload, DownloadError>,
) -> bool {
    match data {
        Ok(_) => true,
        Err(error) => error.leaves_connection_clean(),
    }
}

// Successful download payload waiting for decode scheduling.
pub(super) struct PendingDecodeWork {
    pub(super) segment_id: SegmentId,
    pub(super) raw: Bytes,
    pub(super) source_server_idx: Option<usize>,
    pub(super) exclude_servers: Vec<usize>,
}

// Native outcomes share the existing repair completion queue. Dispatch occurs
// once per operation; block reads and native evidence stay format-specific.
#[derive(Debug)]
pub(super) struct MoveToCompleteResult {
    pub(super) moved_entries: u32,
    // Delivered files the deobfuscation pass renamed on the way out.
    pub(super) renamed_members: Vec<crate::ingest::FileRename>,
}

// A final-move refusal that must never enter terminal post-processing versus
// an ordinary move failure, whose legacy script handling remains intact.
#[derive(Debug)]
pub(super) enum MoveToCompleteFailure {
    Security(String),
    Operational(String),
}

impl std::fmt::Display for MoveToCompleteFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Security(message) | Self::Operational(message) => formatter.write_str(message),
        }
    }
}

// A failed job's working directory, released off the actor: its history
// is recorded once this comes back.
pub(super) struct FailedJobReleased {
    pub(super) job_id: JobId,
    pub(super) error: String,
}

pub(super) struct MoveToCompleteDone {
    pub(super) job_id: JobId,
    pub(super) dest: PathBuf,
    pub(super) result: Result<MoveToCompleteResult, MoveToCompleteFailure>,
}

pub(super) enum TerminalPostProcessingEvent {
    QueueAdmitted(JobId),
    QueueDone(JobId, Result<(), crate::StateError>),
    AddedScriptsDone(JobId, Result<(), crate::StateError>),
    Started(JobId),
    Done(TerminalPostProcessingDone),
    CancelJobsDone(orchestrator::CancelJobsDone),
}

pub(super) struct TerminalPostProcessingDone {
    pub(super) job_id: JobId,
    pub(super) primary_failure: Option<String>,
    pub(super) result: Result<
        crate::post_processing::executor::JobPostProcessingReport,
        crate::post_processing::executor::PostProcessingExecutorError,
    >,
}

// Map a yEnc CRC outcome onto the pipeline's `crc_valid` flag.
//
// `crc_valid` here means "not known bad", which is what the CRC-error metric
// and the segment event are counting. An article whose `=yend` carried no
// usable `crc32=`/`pcrc32=` is *unverifiable*, not corrupt, and must not be
// reported as a CRC failure — use [`weaver_yenc::CrcVerification::Verified`]
// directly wherever real verification is the question.
pub(super) fn crc_not_mismatched(status: weaver_yenc::CrcVerification) -> bool {
    status != weaver_yenc::CrcVerification::Mismatch
}

// Whether an article that could not be checked also carries evidence that its
// body was cut short.
//
// An unverifiable article is ordinarily accepted: absent a checksum there is
// nothing to fail it on. But a missing `=yend` — or a length the headers
// themselves disagree about — is evidence the response ended early, and
// silently accepting those bytes writes a hole into the file that no later
// stage can see. Such an article is worth asking another server for.
//
// A verified checksum always wins: it proves the bytes whatever the size
// fields say. A mismatch keeps its own handling.
pub(super) fn yenc_truncation_suspected(result: &weaver_yenc::DecodeResult) -> bool {
    if result.crc_status != weaver_yenc::CrcVerification::Unverified {
        return false;
    }
    let defects = result.defects;
    // The codec treats any article carrying a part or a begin as multipart,
    // and only fills in the size defect that matches that reading.
    let multipart = result.metadata.part.is_some() || result.metadata.begin.is_some();
    let multipart_length_confirmed = result.metadata.begin.is_some()
        && result.metadata.end.is_some()
        && !defects.ypart_size_mismatch;
    let single_part_length_confirmed = !multipart
        && !defects.missing_size
        && !defects.invalid_size
        && !defects.ybegin_size_mismatch;
    let length_confirmed = multipart_length_confirmed || single_part_length_confirmed;
    defects.ypart_size_mismatch
        || defects.yend_size_mismatch
        || defects.ybegin_size_mismatch
        || (!result.has_trailer && !length_confirmed)
}

// How a segment was encoded on the wire, and therefore what evidence it
// carries into the pipeline.
//
// This is not cosmetic. A yEnc article declares where its bytes belong and
// what they check to, and the whole zero-I/O verification story is built on
// that. A uuencode article declares neither, so several stages that are
// correct for yEnc are unsound for uuencode and gate on this.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SegmentEncoding {
    // yEnc: per-part offsets and CRC, block-aligned CRC segments.
    Yenc,
    // uuencode: decoded bytes and a segment index, nothing more.
    Uu(UuSegmentFacts),
}

// The only things a uuencode article establishes beyond its bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct UuSegmentFacts {
    // A line in this part failed to decode. Its bytes are kept regardless —
    // PAR2 adjudicates, not the decoder — but the file is no longer clean.
    pub(super) damaged: bool,
    // This part carried the uuencode `end` marker, so it ends the file.
    pub(super) ended: bool,
}

impl SegmentEncoding {
    // uuencode segments cannot be placed from the article itself, so they are
    // assembled sequentially rather than by declared offset.
    pub(super) fn is_uu(self) -> bool {
        matches!(self, Self::Uu(_))
    }

    pub(super) fn uu_facts(self) -> Option<UuSegmentFacts> {
        match self {
            Self::Uu(facts) => Some(facts),
            Self::Yenc => None,
        }
    }
}

// The decoded bytes of a part held until it can be placed: a uuencode part
// waiting for its prefix, or an article that declared no usable start
// waiting for the ordinal before it.
//
// Disk-backed entries retain their trusted decoded length alongside the
// spool file's path, under the job's working directory. The spool file's
// metadata is never used for allocation or validation when the part is
// released. Dropping an entry leaves its file: a part let go removes it, and
// one the process stops with stays for the job's restart to take back.
pub(super) enum ParkedBytes {
    Memory(DecodedChunk),
    Spilled { path: PathBuf, decoded_bytes: usize },
}

impl ParkedBytes {
    fn decoded_bytes(&self) -> usize {
        match self {
            Self::Memory(chunk) => chunk.len_bytes(),
            Self::Spilled { decoded_bytes, .. } => *decoded_bytes,
        }
    }

    fn is_spilled(&self) -> bool {
        matches!(self, Self::Spilled { .. })
    }
}

// An article that declared no usable start, held until the ordinal before
// it is placed. Its decoded bytes are in `bytes`; `result` keeps everything
// else it decoded to, with no data of its own.
pub(super) struct UnanchoredPart {
    pub(super) result: DecodeResult,
    pub(super) bytes: ParkedBytes,
    pub(super) source: SegmentSource,
}

// Sequential-assembly state for one uuencode file.
//
// A uuencode part's position is the cumulative *decoded* length of every part
// before it, which is only knowable once that whole prefix has arrived. So
// assembly is strictly sequential: a part that arrives early has to wait, and
// it has to wait until its prefix provides an offset.
// That is the structural difference from yEnc, whose out-of-order parts can be
// persisted immediately at the offset their own header declares.
#[derive(Default)]
pub(super) struct UuFileAssembly {
    // The next segment ordinal the cursor will place.
    pub(super) next_index: u32,
    // The decoded byte offset that ordinal will be placed at.
    pub(super) next_offset: u64,
    // Parts that arrived ahead of the cursor, keyed by ordinal.
    //
    // Bounded by the same per-file limit the write reorder buffer uses; see
    // the admission check at the placement seam for what happens on overflow.
    parked: BTreeMap<u32, ParkedBytes>,
    // A part decoded with damage, or a gap was closed by shifting later parts
    // down over a part that never arrived.
    pub(super) damaged: bool,
    // The uuencode `end` marker was seen on some part.
    pub(super) saw_end: bool,
    // The name from the uuencode `begin` header, from whichever part carried
    // it. First non-empty wins.
    //
    // yEnc repeats `name=` on every article, so the article that completes a
    // file always carries the name to the identity seam. uuencode states it
    // once, on the part that opens the body — which is normally the first
    // part, and is emphatically not the last. Both reference decoders apply
    // the name whichever part it arrives on, so it is retained here rather
    // than read off the completing article.
    pub(super) filename: Option<String>,
}

impl Pipeline {
    // The cursor of a uuencoded file, made on its first part. A restored
    // job's root already holds the parts laid before the restart, so the
    // cursor starts where they end.
    pub(in crate::pipeline) fn uu_file(&mut self, file_id: NzbFileId) -> &mut UuFileAssembly {
        let graph_jobs = &self.graph_jobs;
        self.uu_files.entry(file_id).or_insert_with(|| {
            let (next_index, next_offset, damaged) =
                Self::graph_ordered_front(graph_jobs, file_id).unwrap_or_default();
            UuFileAssembly {
                next_index,
                next_offset,
                damaged,
                ..UuFileAssembly::default()
            }
        })
    }

    // Move the cursor past every ordinal from it on that will never arrive:
    // no part of it is coming, so the parts behind it are placed one hole
    // early and the file is damaged from there. The repair, or the file's
    // verdict, rules on what that leaves.
    pub(in crate::pipeline) fn pass_given_up_uu(&mut self, file_id: NzbFileId) {
        let Some(mut next) = self.uu_files.get(&file_id).map(|uu| uu.next_index) else {
            return;
        };
        let from = next;
        while self.graph_article_given_up(SegmentId {
            file_id,
            segment_number: next,
        }) {
            next = next.saturating_add(1);
        }
        if next != from
            && let Some(uu) = self.uu_files.get_mut(&file_id)
        {
            uu.damaged = true;
            uu.next_index = next;
        }
    }
}

// Result of a decode task.
pub(super) struct DecodeResult {
    pub(super) segment_id: SegmentId,
    pub(super) raw_size: u64,
    // How the article was encoded. Everything below that reads like a
    // declared placement or a checksum is meaningful only for
    // [`SegmentEncoding::Yenc`].
    pub(super) encoding: SegmentEncoding,
    pub(super) yenc_layout: YencLayoutAssertions,
    pub(super) crc_valid: bool,
    // Evidence the body was cut short, with no checksum to say so. Such an
    // article is written but never counted as coverage: another server is
    // asked for it first. See [`yenc_truncation_suspected`].
    pub(super) truncation_suspected: bool,
    pub(super) part_crc_verified: bool,
    pub(super) part_crc: u32,
    pub(super) expected_file_crc: Option<u32>,
    pub(super) data: DecodedChunk,
    // Original filename from the yEnc header (for swap detection observability).
    pub(super) yenc_name: String,
    // Geometry actually applied by the decoder for this response.
    pub(super) checkpoint_plan: weaver_yenc::CheckpointPlan,
    // The decode pass's CRC32 segments, cut at PAR2 block boundaries when the
    // recovery set's block size was known to the decoder. [`Self::part_crc`] is
    // their fold, so they add evidence without changing any verdict.
    pub(super) segments: Vec<weaver_yenc::Segment>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct YencLayoutAssertions {
    pub(super) file_size: u64,
    pub(super) part: Option<u32>,
    pub(super) total: Option<u32>,
    pub(super) begin: Option<u64>,
    pub(super) end: Option<u64>,
}

#[derive(Debug)]
pub(super) struct SegmentSource {
    pub(super) source_server_idx: Option<usize>,
    pub(super) exclude_servers: Vec<usize>,
}

// Completion of a decode task, including explicit failures so backlog
// accounting is always drained.
pub(super) enum DecodeDone {
    Success {
        result: DecodeResult,
        source: SegmentSource,
    },
    Failed {
        segment_id: SegmentId,
        raw_size: u64,
        error: String,
        source_server_idx: Option<usize>,
        exclude_servers: Vec<usize>,
    },
}

pub(super) enum DecodedChunk {
    Contiguous(Bytes),
    Batches { chunks: Vec<Bytes>, len: usize },
    Shared(Arc<download::repeated::SharedArticle>),
    // Decoded straight into a pool slot and carried to the writer as-is, so
    // the article never needs a second heap allocation for its decoded
    // bytes. Dropping it returns the slot to the pool instead of freeing on
    // whichever thread finished with it.
    Pooled(BufferHandle),
}

impl DecodedChunk {
    pub(super) fn len_bytes(&self) -> usize {
        match self {
            Self::Contiguous(bytes) => bytes.len(),
            Self::Batches { len, .. } => *len,
            Self::Shared(body) => body.data.len_bytes(),
            Self::Pooled(buffer) => buffer.len(),
        }
    }

    // The decoded bytes as one view, adopting a single buffer rather than
    // copying it: a pooled slot goes back to its pool when the last view of
    // it drops, and a shared body stays shared. Only output the decoder left
    // in several batches is joined, and a part that declares its length is
    // never left in several.
    pub(super) fn into_bytes(self) -> Bytes {
        match self {
            Self::Contiguous(bytes) => bytes,
            Self::Pooled(buffer) => Bytes::from_owner(buffer),
            Self::Shared(body) => match &body.data {
                Self::Contiguous(bytes) => bytes.clone(),
                Self::Pooled(buffer) => Bytes::from_owner(buffer.clone()),
                data => data.joined(),
            },
            data @ Self::Batches { .. } => data.joined(),
        }
    }

    fn joined(&self) -> Bytes {
        let mut bytes = bytes::BytesMut::with_capacity(self.len_bytes());
        self.for_each_slice(|slice| bytes.extend_from_slice(slice));
        bytes.freeze()
    }

    pub(super) fn for_each_slice<F>(&self, mut f: F)
    where
        F: FnMut(&[u8]),
    {
        match self {
            Self::Contiguous(bytes) => f(bytes),
            Self::Pooled(buffer) => f(buffer.as_slice()),
            Self::Shared(body) => body.data.for_each_slice(f),
            Self::Batches { chunks, .. } => {
                for chunk in chunks {
                    f(chunk.as_ref());
                }
            }
        }
    }

    pub(super) fn write_to<W>(&self, writer: &mut W) -> std::io::Result<()>
    where
        W: std::io::Write,
    {
        match self {
            Self::Contiguous(bytes) => writer.write_all(bytes),
            Self::Pooled(buffer) => writer.write_all(buffer.as_slice()),
            Self::Shared(body) => body.data.write_to(writer),
            Self::Batches { chunks, .. } => {
                for chunk in chunks {
                    writer.write_all(chunk.as_ref())?;
                }
                Ok(())
            }
        }
    }
}

impl Pipeline {
    // Publishes one per-article event on the segment stream rather than the
    // job-level broadcast, so the always-on job-level subscribers are not
    // woken several times per article to discard it. The event is built only
    // when someone is listening.
    pub(crate) fn send_segment_event(&self, event: impl FnOnce() -> PipelineEvent) {
        self.shared_state.publish_segment_event(event);
    }
}

impl From<Vec<u8>> for DecodedChunk {
    fn from(value: Vec<u8>) -> Self {
        Self::Contiguous(Bytes::from(value))
    }
}

impl From<Vec<Box<[u8]>>> for DecodedChunk {
    fn from(chunks: Vec<Box<[u8]>>) -> Self {
        // `Bytes::from(Box<[u8]>)` adopts the allocation, so the decoder's
        // batches reach the writer and the router without being copied once.
        let mut chunks: Vec<Bytes> = chunks
            .into_iter()
            .filter(|chunk| !chunk.is_empty())
            .map(Bytes::from)
            .collect();
        match chunks.len() {
            0 => Self::Contiguous(Bytes::new()),
            1 => Self::Contiguous(chunks.pop().expect("single chunk")),
            _ => {
                let len = chunks.iter().map(|chunk| chunk.len()).sum();
                Self::Batches { chunks, len }
            }
        }
    }
}

// The one way a segment can stop being outstanding without arriving.
//
// A segment reaches exactly one of these, exactly once, and the job's
// `failed_bytes` is the sum of the *declared* sizes of the segments that hold
// one. Delivery is the fourth terminal state and is recorded where it already
// was — the assembly bitmap — so a delivered segment never appears here at
// all.
//
// The distinction between the variants is diagnostic; every one of them
// contributes the same declared bytes. What matters is that there is one
// place a segment can acquire a state and no place at all where bytes are
// added without one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::pipeline) enum SegmentTerminalState {
    // Every configured server refused the article.
    Missing,
    // The download retry budget ran out without a usable body.
    RetriesExhausted,
    // Bodies arrived but no attempt decoded into the declared placement.
    DecodeExhausted,
    // No server may fetch the article: one that would refuses the
    // credentials. The servers never said the article is gone.
    Unservable,
    // The feed stopped asking for the article once it gave the post up.
    Abandoned,
}

// What the settlement concluded a delivered job actually delivered.
//
// Built once, from the claim census, at the last gate before the payload
// leaves the working directory — while every settlement fact that decided the
// job is still in hand. The terminal record is written from this rather than
// from the live wire counters, which know only what the download layer saw and
// nothing about what repair, verification or a discard did with it afterwards.
#[derive(Debug, Clone, Default)]
pub(in crate::pipeline) struct TerminalReconciliation {
    // Declared bytes of the delivered files that really are short.
    pub(in crate::pipeline) failed_bytes: u64,
    // Health over the delivered files alone, 0-1000.
    pub(in crate::pipeline) health: u32,
    // Files that left the accounting, and why.
    pub(in crate::pipeline) discards: Vec<crate::jobs::model::TerminalDiscard>,
}

// The pipeline engine. Owns the scheduler loop and drives work through
// download → decode → commit → verify → repair → extract stages.
pub struct Pipeline {
    _control_runtime: queue::RuntimeGuard,
    // Broadcasts pipeline events to subscribers (API, journal, etc).
    pub(super) event_tx: broadcast::Sender<PipelineEvent>,
    // NNTP client for fetching articles.
    pub(super) nntp: Arc<NntpClient>,
    // Buffer pool used as decode scratch space only.
    pub(super) buffers: Arc<BufferPool>,
    // Runtime tuner for adaptive concurrency.
    pub(super) tuner: RuntimeTuner,
    // The operator's hardware profile, or the recommendation standing in
    // for one never chosen. In force whenever no schedule rule is.
    pub(super) configured_hardware_profile: crate::runtime::HardwareProfile,
    // The profile the schedule has in force over the operator's choice, as
    // the schedule asked for it. It governs only while this machine can
    // honour it.
    pub(super) scheduled_hardware_profile: Option<crate::runtime::HardwareProfile>,
    // Shared atomic metrics.
    pub(super) metrics: Arc<PipelineMetrics>,
    // Per-job state.
    pub(super) jobs: HashMap<JobId, JobState>,
    // The graph runtime of each job holding its articles, given when they
    // are built.
    pub(super) graph_jobs: HashMap<JobId, graph::job::GraphJob>,
    // The graph runtimes of jobs that dropped their articles while something
    // of them was still out on a thread: each is stopped, and kept only until
    // the last of its mail is heard.
    pub(super) retiring_graph_jobs: HashMap<JobId, graph::job::GraphJob>,
    // The repair slots graph jobs share, and what each job's recovery
    // objects said when it last advanced.
    pub(super) graph_status: graph::status::GraphStatus,
    // The holds accountant every graph job's namespaces share, so the
    // budgets bound the process, not each job.
    pub(super) graph_holds: Arc<weaver_graph::holds::HoldsAccountant>,
    // Where the engines graph jobs run say what they found.
    pub(super) graph_runs: graph::engine::RunMail,
    // Each graph job's health probe, and where its rounds say what they
    // found.
    pub(super) post_probes: post_health::Probes,
    pub(crate) nzb_admitted: HashSet<JobId>,
    pub(crate) nzb_seen: HashSet<JobId>,
    pub(crate) nzb_pause_releases: HashSet<JobId>,
    pub(crate) nzb_reprocess_pending: HashSet<JobId>,
    // Typed terminal provenance retained until the ordered history archive has
    // durably updated duplicate-promotion eligibility.
    pub(super) semantic_terminal_causes: HashMap<JobId, crate::jobs::SemanticTerminalCause>,
    // The stage a failed job was in when it failed, kept until its history
    // row records the failure kind.
    pub(super) pipeline_failure_stages:
        HashMap<JobId, crate::post_processing::model::PipelineFailureStage>,
    // Job dispatch order (FIFO by submission). First Downloading job is active.
    pub(super) job_order: Vec<JobId>,
    // Number of in-flight article downloads (primary + recovery).
    pub(super) active_downloads: usize,
    // Number of NNTP connection tasks currently fetching articles.
    pub(super) active_download_connections: usize,
    // Active connection lanes carrying completion-critical PAR2 work.
    pub(super) active_completion_critical_connections: usize,
    // Number of in-flight recovery downloads (subset of active_downloads).
    pub(super) active_recovery: usize,
    // Runtime-only per-server BODY depth explorers. Seeded from the persisted
    // depth on first observation; the measurements themselves never persist.
    pub(super) download_lane_runtime: DownloadLaneRuntimeState,
    // Refill requests the scheduler had nothing for yet, waiting for the
    // next wake or for their hold to run out.
    pub(super) held_download_refills: Vec<HeldDownloadRefill>,
    // A lane parked and its connection slot came back; the run loop owes a
    // dispatch pass.
    //
    // The loop turns once per pipeline event and dispatches at the top of the
    // turn, so a park observed part-way through a turn used to wait out the
    // rest of it — up to `DOWNLOAD_RESULTS_PER_TURN` awaited result ingests,
    // which measured as a median 340 ms and a worst case of 2.3 s before the
    // freed connection was handed back out. Setting this makes the park
    // itself the wake, with no timer to poll.
    pub(super) download_dispatch_wake: bool,
    // A `RebuildNntp` has activated a new pool whose predecessor still holds
    // sockets at the provider. Fresh dials wait until the old generation has
    // drained: the provider counts both generations against one allowance,
    // so a dial now would be refused as over the limit and park the new pool
    // — a healthy server — for the whole holdoff window.
    pub(super) nntp_handoff_draining: bool,
    // Jobs currently inside an active article download pass.
    pub(super) active_download_passes: HashSet<JobId>,
    // Jobs that still have decode/write pipeline work after network downloads finished.
    pub(super) jobs_finalizing_download: HashSet<JobId>,
    // Released download results waiting to be committed into decode/write state.
    pub(super) pending_released_download_results_by_job: HashMap<JobId, usize>,
    // Estimated decoded/raw bytes held by released results that are not committed yet.
    pub(super) pending_released_download_result_bytes_by_job: HashMap<JobId, u64>,
    // In-flight article download count per job.
    pub(super) active_downloads_by_job: HashMap<JobId, usize>,
    // In-flight NNTP connection task count per job.
    pub(super) active_download_connections_by_job: HashMap<JobId, usize>,
    // Completion-critical connection lanes per job. Kept separately from
    // article counts because pipelined lanes can carry several articles.
    pub(super) active_completion_critical_connections_by_job: HashMap<JobId, usize>,
    // In-flight article download count per file.
    pub(super) active_downloads_by_file: HashMap<NzbFileId, usize>,
    // In-flight decode task count per job.
    pub(super) active_decodes_by_job: decode::ActiveDecodes,
    // In-flight decode task count per file.
    pub(super) active_decodes_by_file: HashMap<NzbFileId, usize>,
    // Raw article bytes reserved until the actor consumes each decode result.
    pub(super) active_decode_bytes: HashMap<SegmentId, u64>,
    // Last time a job made observable progress in the download stage.
    pub(super) job_last_download_activity: HashMap<JobId, Instant>,
    // Delayed retry tasks that have been scheduled but not yet re-queued.
    pub(super) pending_retries_by_job: HashMap<JobId, usize>,
    // Delayed retry tasks by exact segment.
    pub(super) pending_retries_by_segment: HashMap<SegmentId, usize>,
    // Runs of established-transport failures by exact segment. Transport
    // faults keep the article retry budget, so an article that breaks every
    // connection it is fetched on would otherwise retry forever — and, as the
    // highest-priority work, be every recovery probe of its server.
    pub(super) transport_failure_streaks: HashMap<SegmentId, TransportFailureStreak>,
    pub(super) download_wait_by_job: HashMap<JobId, DownloadWaitStatus>,
    // The one terminal state each segment reached, and the only thing the
    // per-job failed-byte ledger is derived from.
    pub(in crate::pipeline) segment_terminal_states: HashMap<SegmentId, SegmentTerminalState>,
    // Each job's articles that will never arrive, this run's and those its
    // graph root was restored holding lost: what the feed weighs a post on.
    pub(in crate::pipeline) given_up: HashMap<JobId, post_health::GivenUp>,
    // What the claim census concluded for a job on its way out, keyed until
    // the terminal record has been written from it.
    pub(in crate::pipeline) terminal_reconciliations: HashMap<JobId, TerminalReconciliation>,
    // Files already counted into `weaver_files_missing_total`. A completion
    // check re-enters many times per job; this keeps the counter per-file
    // rather than per-check. Per-file, so the set is bounded by the job's
    // file count and never touched from a per-segment path.
    pub(in crate::pipeline) files_counted_missing: HashSet<NzbFileId>,
    // Work parked specifically on per-server quota capacity or policy changes.
    pub(super) server_quota_parked: HashSet<SegmentId>,
    // The egress each piece of work in `server_quota_parked` waits on, when
    // an egress download quota rather than a server quota parked it.
    pub(super) egress_quota_parked: HashMap<SegmentId, u32>,
    // Directory for active downloads (per-job subdirectories).
    pub(super) intermediate_dir: PathBuf,
    // Directory for completed downloads (category subdirectories).
    pub(super) complete_dir: PathBuf,
    // Legacy logical NZB path base retained for compatibility with existing rows and tests.
    pub(super) nzb_dir: PathBuf,
    // Jobs whose provider shares have moved since the last checkpoint.
    pub(super) dirty_server_attribution: HashSet<JobId>,
    // Jobs whose support facts changed since the last checkpoint.
    pub(super) dirty_support_facts: HashSet<JobId>,
    pub(super) server_attribution_checkpoint_at: Instant,
    // Forces the PAR2 ignore-extension list for a test, so the "override
    // disables it" case can be exercised without mutating a process-global
    // environment variable while other tests are running.
    #[cfg(test)]
    pub(super) par2_ignore_extensions_override: Option<Vec<String>>,
    // Downloaded article bodies waiting for decode scheduling.
    pub(super) pending_decode: VecDeque<PendingDecodeWork>,
    // Jobs that should re-enter completion/post-processing on the next loop pass.
    pub(super) pending_completion_checks: VecDeque<JobId>,
    // Presence gates every attempt to resume a partially restored job.
    pub(crate) blocked_restores: HashMap<JobId, crate::jobs::handle::RestoreJobRequest>,
    // Channels for pipeline stage results.
    pub(super) download_done_tx: mpsc::Sender<DownloadResult>,
    pub(super) download_done_rx: mpsc::Receiver<DownloadResult>,
    pub(super) download_refill_tx: mpsc::Sender<DownloadLaneRefillRequest>,
    pub(super) download_refill_rx: mpsc::Receiver<DownloadLaneRefillRequest>,
    pub(super) download_lane_parked_tx: mpsc::Sender<DownloadLaneParked>,
    pub(super) download_lane_parked_rx: mpsc::Receiver<DownloadLaneParked>,
    pub(super) owned_download_lane_event_tx: mpsc::Sender<OwnedDownloadLaneEvent>,
    pub(super) owned_download_lane_event_rx: mpsc::Receiver<OwnedDownloadLaneEvent>,
    pub(super) owned_download_lane_pool: download::owned_lane::OwnedDownloadLanePool,
    pub(super) decode_done_tx: mpsc::Sender<DecodeDone>,
    pub(super) decode_done_rx: mpsc::Receiver<DecodeDone>,
    // Channel through which due retries re-enter the pipeline loop.
    pub(in crate::pipeline) retry_tx: mpsc::Sender<RetryWork>,
    pub(in crate::pipeline) retry_rx: mpsc::Receiver<RetryWork>,
    // Pipeline-owned infrastructure retries, drained in deadline batches.
    pub(in crate::pipeline) infrastructure_retries:
        infrastructure_retry::InfrastructureRetryQueue<RetryWork>,
    // Monotonic NNTP pool generation, bumped on every `RebuildNntp`. Server
    // indices in `DownloadWork::exclude_servers` are only meaningful within
    // the generation they were computed under; delayed retries carry the
    // generation they were scheduled under so stale indices can be dropped
    // when they re-enter after a rebuild.
    pub(super) pool_generation: u64,
    // Per-server article attempt counters for the active NNTP generation,
    // indexed by runtime `server_idx`.
    //
    // Held as a plain `Vec` with no lock: the completion path indexes it
    // directly and only ever does `Relaxed` `fetch_add`s through the `Arc`s.
    // It is rebuilt exactly when a new NNTP generation is activated, from
    // `metrics.server_metrics`, which re-uses the counters of any stable
    // server id it has already seen so lifetime totals survive a config
    // reload. A stale generation may hand us an out-of-range index; the
    // completion path bounds-checks and skips.
    pub(super) server_counters: Vec<Arc<crate::operations::instrumentation::ServerCounters>>,
    // Wall-clock start of each in-flight job stage, keyed by `(job, stage)`.
    //
    // Per-job, not per-segment: a job passes through each stage at most a
    // handful of times, so a `HashMap` here costs nothing measurable and never
    // appears on an article path.
    pub(super) job_stage_started_at:
        HashMap<(JobId, crate::operations::instrumentation::JobStageKind), Instant>,
    // The generation whose predecessor pool finished draining; see
    // `nntp_handoff_draining`.
    pub(super) nntp_handoff_drained_tx: mpsc::Sender<u64>,
    pub(super) nntp_handoff_drained_rx: mpsc::Receiver<u64>,
    // Channel for background final-move results.
    pub(super) job_activated_tx: mpsc::Sender<activation::JobActivated>,
    pub(super) job_activated_rx: mpsc::Receiver<activation::JobActivated>,
    pub(super) move_done_tx: mpsc::Sender<MoveToCompleteDone>,
    pub(super) move_done_rx: mpsc::Receiver<MoveToCompleteDone>,
    // Channel for failed jobs whose working directory was released.
    pub(super) failed_release_tx: mpsc::Sender<FailedJobReleased>,
    pub(super) failed_release_rx: mpsc::Receiver<FailedJobReleased>,
    pub(super) terminal_post_processing_done_tx: mpsc::Sender<TerminalPostProcessingEvent>,
    pub(super) terminal_post_processing_done_rx: mpsc::Receiver<TerminalPostProcessingEvent>,
    pub(super) script_effects_rx: tokio::sync::watch::Receiver<()>,
    pub(super) terminal_post_processing_executor:
        crate::post_processing::executor::PostProcessingExecutor,
    pub(super) inflight_terminal_post_processing: HashSet<JobId>,
    pub(super) queue_script_waiters: HashSet<JobId>,
    pub(super) queue_scripts_completed: HashSet<JobId>,
    // Jobs kept from downloading while a blocking instance runs on their
    // arrival.
    pub(super) added_script_holds: HashSet<JobId>,
    pub(super) script_data_dir: PathBuf,
    pub(super) terminal_post_processing_cancellations:
        HashMap<JobId, tokio::sync::watch::Sender<bool>>,
    // Whether all downloads are globally paused.
    pub(super) global_paused: bool,
    pub(super) applied_speed_test_pause: bool,
    pub(super) applied_random_read_iops: Option<f64>,
    pub(super) applied_sequential_write_mbps: Option<f64>,
    // Whether the active global pause came from a bandwidth schedule rather
    // than an operator action. Only meaningful while `global_paused` is true;
    // it selects the Scheduled vs ManualPause download-block presentation.
    pub(super) scheduled_pause: bool,
    // Estimated bytes charged to the speed limiter for in-flight downloads.
    pub(crate) rate_limit_reservations: HashMap<SegmentId, u64>,
    // Persisted/general speed limit restored when no schedule speed action is active.
    pub(super) configured_rate_limit: u64,
    // Active schedule speed action, if any.
    pub(super) scheduled_rate_limit: Option<u64>,
    // Effective bandwidth rate limiter.
    pub(super) rate_limiter: TokenBucket,
    // Max pending segments per write reorder buffer (memory-adaptive).
    pub(super) write_buf_max_pending: usize,
    // Max in-memory raw article bytes queued or active for decode.
    pub(super) decode_backlog_budget_bytes: usize,
    // Max in-memory decoded backlog before degrading to direct offset writes.
    pub(super) write_backlog_budget_bytes: usize,
    // Whether raw decode backlog is in a hard-pressure drain cycle.
    pub(super) download_decode_hard_pressure_latched: bool,
    // Whether decoded write backlog is in a hard-pressure drain cycle.
    pub(super) download_write_hard_pressure_latched: bool,
    // Start time of the current hard pressure stall, if downloads are blocked.
    pub(super) download_pressure_hard_stall_started_at: Option<Instant>,
    // Next time soft byte pressure may issue a replacement article.
    pub(super) download_pressure_soft_dispatch_after: Option<Instant>,
    // Whether the last dispatch pass was held back by a gate that lifts with
    // time alone (a schedule window, the rate limiter, a download quota,
    // byte pressure) or found eligible work it could not place, so the idle
    // tick must try again. Without it nothing would wake dispatch when such
    // a gate lifts.
    pub(super) download_dispatch_retry: bool,
    // Whether the last metrics refresh still showed a non-zero rate. The
    // rate windows only move when sampled, so the fast tick runs until the
    // gauges read zero.
    pub(super) metrics_rates_moving: bool,
    // Entries in `phase_progress` for a phase other than the download,
    // kept so liveness is a comparison rather than a walk of every job's
    // phases.
    pub(super) post_download_phases: usize,
    // Whether nothing has happened since the last state reconcile, so the
    // next one would find exactly what the last one did.
    pub(super) reconcile_clean: bool,
    // What each reconcile candidate looked like when its completion check
    // was last scheduled by the reconcile; an unchanged job is not checked
    // again.
    pub(super) reconcile_signatures: HashMap<JobId, (u64, u64, u64, usize, usize)>,
    // Per-job delay after restart-durable-lead throttling parks primary work.
    pub(super) download_restart_durable_lead_retry_after: HashMap<JobId, Instant>,
    // The one over-limit article reserved until its result is processed or returned.
    pub(super) checkpoint_progress_articles: HashMap<JobId, (u64, SegmentId)>,
    pub(super) download_lane_owners: HashMap<u64, DownloadLaneOwner>,
    // Set by the download-dispatch test fixtures: every dispatch pass and
    // every lane result batch then checks that the in-flight gauges are what
    // the lane records hold (see `Pipeline::audit_lane_holdings`).
    #[cfg(test)]
    pub(super) audit_lane_holdings: bool,
    // When each deferred job's articles become old enough to fetch.
    //
    // Absent means the question has not been asked yet or was answered
    // "eligible" — see [`Pipeline::propagation_hold_until`], which is where the
    // answer is computed and cached as a monotonic deadline plus a stable
    // epoch-millisecond timestamp for clients. The dispatch gate and run-loop
    // sleep use the monotonic deadline, so eligibility wakes without polling.
    pub(super) propagation_ready_at: HashMap<JobId, (Instant, i64)>,
    // Configured minimum post age, updated through the scheduler command channel.
    pub(super) propagation_delay: Duration,
    #[cfg(test)]
    pub(super) propagation_delay_forced: Option<Duration>,
    // Last time we logged a queued/no-active-download liveness stall.
    pub(super) last_download_dispatch_stall_log_at: Option<Instant>,
    // The dispatch stall the watchdog is timing toward a kick or a release.
    pub(super) download_dispatch_stall: Option<download::DispatchStallWatch>,
    // The available fill servers of the dispatch attempt running now, in
    // its order; `None` outside one. Read by the rotation hint, which keeps
    // an article off its hinted server only while one of these could take it.
    pub(super) download_pass_fill_servers: Option<Vec<usize>>,
    // The provider policy sampled once at the top of the dispatch pass running
    // now, read by every candidate of the pass; `None` outside one.
    pub(super) download_pass_provider_policy:
        Option<Arc<weaver_nntp::provider_policy::ProviderPolicy>>,
    // Whether the stall watchdog escalates (kick, then release). Read once
    // from `WEAVER_DOWNLOAD_STALL_RECOVERY` at construction; on by default.
    pub(super) download_stall_recovery: bool,
    // Retry-storm window: when it opened, and the retry and download counts
    // it opened with. A pipeline that retries without ever finishing an
    // article is as stalled as one with no active downloads, but it looks
    // busy from every gauge the stall log reads, so it gets its own window.
    pub(super) download_retry_storm_window: Option<(Instant, u64, u64)>,
    // Rate limiter, per job, for the owned blocking lane acquire warning.
    pub(super) owned_lane_acquire_failure_log_throttle: download::JobLogThrottle,
    // Rate limiter, per job, for the "a pass found this job ineligible"
    // warning. Every dispatch wake re-visits every job, so this one fires as
    // fast as the actor is woken until the job leaves the phase it is in.
    pub(super) dispatch_ineligible_log_throttle: download::JobLogThrottle,
    // Last time an owned blocking lane failed to be acquired at all, warned
    // about or not. The under-cap report below is gated on it: lanes below
    // their cap are only a fault when a lane actually failed to open.
    pub(super) last_owned_lane_acquire_failure_at: Option<Instant>,
    // When the servers were first seen below their configured connection cap
    // while work was queued, and the last time that was reported. Cleared as
    // soon as a pass finds the lanes filled, so only a *sustained* underfill
    // is ever logged.
    pub(super) download_lanes_under_cap_since: Option<Instant>,
    pub(super) last_download_lanes_under_cap_log_at: Option<Instant>,
    // Current in-memory decoded backlog retained for sequential write ordering.
    pub(super) write_buffered_bytes: usize,
    // Current in-memory decoded segment count retained for sequential write ordering.
    pub(super) write_buffered_segments: usize,
    // Disk-backed uuencode parts waiting for their missing prefix.
    pub(super) uu_spooled_bytes: usize,
    // Disk-backed uuencode segment count waiting for their missing prefix.
    pub(super) uu_spooled_segments: usize,
    // Every UU part held ahead of its cursor, regardless of storage form.
    //
    // This bounds map and temporary-path overhead even when decoded bytes are
    // tiny.
    pub(super) uu_parked_segments: usize,
    // Aggregate disk-backed UU byte admission limit.
    pub(super) uu_spool_max_bytes: usize,
    // Aggregate ahead-of-cursor UU entry admission limit.
    pub(super) uu_spool_max_segments: usize,
    // Per-file bound on the uuencode reorder park. A uuencode file is placed
    // strictly in ordinal order, and with several lanes each fetching its
    // own run of the file the parts furthest from the cursor arrive first
    // and stay parked until every lane behind them has drained. That is the
    // aggregate spool's budget to hold, so the bound is the same number; it
    // must never be the yEnc reorder depth, which is sized for a window of a
    // few articles and displaces a long file's tail on every pass.
    pub(super) uu_park_max_segments: usize,
    // Free space preserved on the intermediate filesystem while spilling UU.
    pub(super) uu_spool_min_free_bytes: u64,
    // The spool filesystem's latest free-space reading, from the
    // intermediate root's sampler. Never probes on the pipeline task.
    pub(super) uu_spool_capacity: crate::operations::CapacityReader,
    // Spills admitted against the current reading, so a burst between
    // refreshes cannot each see the same headroom.
    pub(super) uu_spool_debits: crate::operations::CapacityDebits,
    // One background free-space sampler per configured root. Everything on
    // the pipeline that gates work on free space reads these readings.
    pub(super) storage_capacity: Arc<crate::operations::StorageCapacity>,
    // Largest refused UU spill. Dispatch preserves cursor progress until
    // this many bytes can be parked in memory or admitted to the spool.
    pub(super) uu_spool_blocked_spill_bytes: Option<usize>,
    #[cfg(test)]
    // Test-only free-space reading; `Some(None)` exercises a filesystem that
    // has never produced a reading.
    pub(super) uu_spool_available_bytes_for_test: Option<Option<u64>>,
    // Sequential-assembly state for uuencode files, created on the first
    // uuencode part of a file and dropped with that file's write buffer.
    pub(super) uu_files: HashMap<NzbFileId, UuFileAssembly>,
    // How often each uuencode segment has been displaced by park pressure and
    // requeued while its file's cursor stood still, paired with the cursor
    // position that count was taken at. Purely a livelock bound — deliberately
    // NOT the decode-failure counter, because park pressure is an ordering
    // condition and must never spend a segment's retry budget; and the count
    // restarts whenever the cursor has moved, because a displacement behind a
    // moving cursor is progress, not a cycle.
    pub(super) uu_park_requeues: HashMap<SegmentId, (u32, u32)>,
    // Articles that declared no usable start and arrived before the ordinal
    // they follow, held decoded until that ordinal is placed. Their bytes are
    // charged to the write-buffer ledger while they wait in memory, and to
    // the spool's once their paused job spills them.
    pub(super) unanchored_parked: HashMap<NzbFileId, BTreeMap<u32, UnanchoredPart>>,
    // How often each article that declared no usable start has been sent back
    // for the ordinal before it because the park above had no room for it,
    // paired with how many ordinals of its file were placed when that count
    // was taken. Like the park counter above, a livelock bound and never a
    // retry budget: the bytes are not at fault, and the count restarts
    // whenever the file has placed another part.
    pub(super) unanchored_requeues: HashMap<SegmentId, (u32, usize)>,
    // Ordinals that reached a terminal state while their damaged bytes stayed
    // on disk, and that something parked above is waiting to start after.
    // Booking a terminal state cannot write a part, so the release runs at the
    // next seam that can.
    pub(super) pending_unanchored_release: Vec<SegmentId>,
    // Tracks decode failure retries per segment. When yEnc decode fails (CRC/size
    // mismatch), the segment is re-downloaded. After `MAX_SEGMENT_RETRIES` decode
    // failures, the segment is marked permanently failed.
    pub(super) decode_retries: HashMap<SegmentId, u32>,
    // Transient byte-progress state for active user-visible phases.
    pub(super) phase_progress: HashMap<(JobId, JobPhase), progress::JobPhaseRuntime>,
    // Cached per-job retention exclusions (pool server indices whose
    // retention window is older than the job). TTL'd; cleared on NNTP
    // client rebuilds and job removal.
    pub(super) job_retention_exclude_cache: HashMap<JobId, (Instant, Arc<Vec<usize>>)>,
    // Rate limiter, per job, for the "no eligible news server" warning.
    pub(super) no_eligible_server_warn_throttle: download::JobLogThrottle,
    // Rate limiter, per job, for BODY work waiting on local lane capacity.
    //
    // Kept apart from the warning above: the two answer different questions,
    // and one window shared between them let the benign report hide the one
    // that says the servers are unusable.
    pub(super) body_lane_capacity_log_throttle: download::JobLogThrottle,
    // Rate limiter, per job, for representative NNTP BODY fetch failures at
    // info level.
    pub(super) body_fetch_failure_log_throttle: download::JobLogThrottle,
    // Jobs currently performing their final move into the complete directory.
    pub(super) inflight_moves: HashSet<JobId>,
    // Failed jobs whose working directory is being released; each records
    // its history when the release comes back.
    pub(super) releasing_failed_jobs: HashSet<JobId>,
    pending_graph_terminals: HashMap<JobId, health::PendingGraphTerminal>,
    // Final moves admitted after the post-processing hold is lifted.
    pub(super) deferred_moves: HashSet<JobId>,
    // Complete destinations reserved for in-flight moves so concurrent jobs do not collide.
    pub(super) reserved_complete_destinations: HashMap<JobId, PathBuf>,
    // Shared state for control plane reads (API handlers read without channel round-trip).
    pub(super) shared_state: SharedPipelineState,
    // SQLite database for durable history.
    pub(super) db: crate::Database,
    // Tracks detached `db_fire_and_forget` writes so shutdown `drain` can join
    // them (bounded) instead of dropping in-flight writes. `db_fire_and_forget`
    // only has `&self`, so the JoinSet lives behind a shared mutex; the lock is
    // only ever held to spawn/drain, never across an await.
    pub(super) fire_and_forget_tasks: Arc<std::sync::Mutex<tokio::task::JoinSet<()>>>,
    // Shared config for runtime category lookups (dest_dir overrides).
    pub(super) config: crate::settings::SharedConfig,
    // What the hardware profile in force grants graph work as it starts:
    // the low-priority post-processing pool repairs run inside, niced on
    // Unix so the OS scheduler prefers download/decode threads when CPU is
    // contended, and the budgets of a run and a repair.
    pub(super) graph_hardware: crate::pipeline::graph::hardware::Hardware,
    // Environment-derived, always-on extraction ceilings.
    pub(super) extraction_limits: Arc<ExtractionLimits>,
    // What every job holding its articles is charged for them, from the
    // moment they are built until the job leaves the pipeline.
    pub(super) job_article_memory: HashMap<JobId, ProcessMemoryPermit>,
    // The number the next read of a job's articles is told by.
    pub(super) next_activation: u64,
    // Paused jobs still holding their articles while what was out for them
    // on a connection or in the decoder comes back.
    pub(super) parking_jobs: HashSet<JobId>,
    // Parked jobs whose held parts are still in memory, to spill to their
    // spool on the next turn.
    pub(super) spilling_jobs: HashSet<JobId>,
    pub(super) repeated_articles: HashMap<JobId, Arc<download::repeated::RepeatedArticles>>,
    // Shared allowance for scheduling, repair metadata, and extraction decoders.
    pub(super) process_memory_budget: Arc<ProcessMemoryBudget>,
}

#[cfg(test)]
pub(crate) mod tests;
