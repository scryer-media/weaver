// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The scheduler's plain control types: what the handle takes and returns,
// with no reach into the pipeline itself.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;

use crate::jobs::assembly::DetectedArchiveIdentity;
use crate::jobs::record::ActiveFileIdentity;
use weaver_server_base::events::model::PipelineEvent;
use weaver_server_base::jobs::ids::JobId;

use crate::jobs::error::SchedulerError;
use crate::jobs::model::{HeldClass, JobSpec, JobStatus, JobUpdate};

pub const FINISHED_JOBS_RUNTIME_CAP: usize = 1_000;
pub const PROPAGATION_WAIT_REASON: &str = "propagation_delay";
// The job waits for room on a disk, in whatever phase it is in.
pub const DISK_SPACE_WAIT_REASON: &str = "disk_space";
// The job has more to fetch than its disk holds above the reserve even
// empty: it never starts there, and never fails for it.
pub const DISK_TOO_SMALL_WAIT_REASON: &str = "disk_too_small";
// The job waits on a fill server refusing the login until the refusal is
// the verdict. The stored reason is this code, a colon, and the servers as
// `host:port`, joined by ", ".
pub const LOGIN_REFUSED_WAIT_REASON: &str = "login_refused";

// What the download lanes have learned about one server's BODY transport.
#[derive(Debug, Clone, PartialEq)]
pub struct ServerTransportHealth {
    // Pool server index this describes.
    pub server_idx: usize,
    // Command-to-status-line wait.
    pub latency_ms: Option<f64>,
    // Status-line-to-terminator wait for one article.
    pub transfer_ms: Option<f64>,
    // "good", "moderate" or "slow"; absent until a latency is measured.
    pub latency_band: Option<String>,
    // BODY pipelining depth in use, where 1 means sequential.
    pub pipeline_depth: u32,
    // Set while a server is held sequential after twice failing a pipelined
    // batch; cleared by a clean trial once the hold lapses.
    pub pinned_sequential: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DownloadBlockKind {
    None,
    ManualPause,
    SpeedTest,
    Scheduled,
    EgressQuota,
    ServerQuota,
}

impl DownloadBlockKind {
    // Every variant, in gate-reason label order. The Prometheus exporter
    // renders one series per variant; deriving the label set from `ALL` keeps
    // a new gate reason from silently vanishing off `/metrics`.
    pub const ALL: [Self; 6] = [
        Self::None,
        Self::ManualPause,
        Self::SpeedTest,
        Self::Scheduled,
        Self::EgressQuota,
        Self::ServerQuota,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::ManualPause => "manual_pause",
            Self::SpeedTest => "speed_test",
            Self::Scheduled => "scheduled",
            Self::EgressQuota => "egress_quota",
            Self::ServerQuota => "server_quota",
        }
    }
}

// Where a manual queue reorder should land a job. The order only breaks ties
// within a dispatch priority band: a LOW job moved to the top still yields to
// HIGH/NORMAL work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum QueueMoveTarget {
    Top,
    Bottom,
    Offset(i64),
}

// What the queue asks of one job, whoever owns it.
#[derive(Debug, Clone)]
pub enum QueueAction {
    Pause,
    Resume,
    Cancel(CancellationOrigin),
    Update(JobUpdate),
    Move(QueueMoveTarget),
}

// Who put a hold in force, in the order their reasons take precedence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum HoldOrigin {
    Manual,
    Scheduled,
    SpeedTest,
}

impl HoldOrigin {
    // Whether a pause or resume from this origin replaces or lifts a hold
    // from `other`. The operator and the schedule share one pause, each
    // overriding the other's; a speed test's hold is its own.
    const fn shares(self, other: Self) -> bool {
        matches!(
            (self, other),
            (Self::SpeedTest, Self::SpeedTest)
                | (
                    Self::Manual | Self::Scheduled,
                    Self::Manual | Self::Scheduled
                )
        )
    }

    // The reason an item a hold from this origin stops gives.
    pub const fn reason(self) -> &'static str {
        match self {
            Self::Manual => "paused",
            Self::Scheduled => "scheduled",
            Self::SpeedTest => "speed test",
        }
    }
}

pub use crate::jobs::model::HoldScope;

// Which items a hold covers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum HoldOwner {
    // Every item in the queue.
    All,
    // One class's jobs alone: only a class whose row has a hold of its own.
    Class(HeldClass),
}

impl HoldOwner {
    // Every owner an operator's hold is kept for across a restart, with the
    // setting it is kept in: the whole queue, then each class the protocol
    // table gives a hold of its own.
    pub fn kept() -> impl Iterator<Item = (Self, &'static str)> {
        std::iter::once((Self::All, "global_paused"))
            .chain(HeldClass::all().map(|class| (Self::Class(class), class.hold().setting)))
    }
}

// One hold on the queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Hold {
    pub origin: HoldOrigin,
    pub scope: HoldScope,
    pub owner: HoldOwner,
}

impl Hold {
    // The operator's hold over `owner`: the queue's pause leaves what is
    // complete seeding; a class's pause stops what its row in the protocol
    // table says.
    pub const fn manual(owner: HoldOwner) -> Self {
        Self {
            origin: HoldOrigin::Manual,
            scope: match owner {
                HoldOwner::All => HoldScope::Downloads,
                HoldOwner::Class(class) => class.hold().scope,
            },
            owner,
        }
    }

    // Whether the hold covers `target` and what it stops of it, before
    // what the target says of the schedule.
    const fn reaches(self, target: HoldTarget) -> bool {
        let covers = match (self.owner, target.owner) {
            (HoldOwner::All, _) => true,
            (HoldOwner::Class(held), HoldOwner::Class(of)) => {
                held.protocol().index() == of.protocol().index()
            }
            (HoldOwner::Class(_), HoldOwner::All) => false,
        };
        covers
            && match self.scope {
                HoldScope::Downloads => !target.complete,
                HoldScope::Everything => true,
            }
    }

    // Whether the hold stops `target`. The schedule's holds leave a target
    // that runs through them.
    const fn stops(self, target: HoldTarget) -> bool {
        self.reaches(target)
            && !(target.runs_through_schedule && matches!(self.origin, HoldOrigin::Scheduled))
    }
}

// An item a hold is asked about: whose it is, whether it has finished
// downloading, and whether it runs through the schedule's holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HoldTarget {
    pub owner: HoldOwner,
    pub complete: bool,
    pub runs_through_schedule: bool,
}

impl HoldTarget {
    // An item of `owner` that every hold over it may stop.
    pub const fn of(owner: HoldOwner, complete: bool) -> Self {
        Self {
            owner,
            complete,
            runs_through_schedule: false,
        }
    }
}

// The holds in force. Every owner of queue items follows them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HoldSet(Vec<Hold>);

impl HoldSet {
    // Put `hold` in force in place of any its origin shares over the same
    // items.
    pub fn hold(&mut self, hold: Hold) {
        self.release(hold.origin, hold.owner);
        self.0.push(hold);
        self.0.sort_unstable();
    }

    // Lift what `origin` shares of the holds over `owner`.
    pub fn release(&mut self, origin: HoldOrigin, owner: HoldOwner) {
        self.0
            .retain(|hold| hold.owner != owner || !origin.shares(hold.origin));
    }

    // Whether any hold stops an item of `owner`, given whether it has
    // finished downloading.
    pub fn stops(&self, owner: HoldOwner, complete: bool) -> bool {
        self.reason(owner, complete).is_some()
    }

    // Who stops that item, by precedence, while anyone does.
    pub fn reason(&self, owner: HoldOwner, complete: bool) -> Option<HoldOrigin> {
        self.reason_for(HoldTarget::of(owner, complete))
    }

    // Who stops `target`, by precedence, while anyone does.
    pub fn reason_for(&self, target: HoldTarget) -> Option<HoldOrigin> {
        self.0
            .iter()
            .filter(|hold| hold.stops(target))
            .map(|hold| hold.origin)
            .min()
    }

    // Whether `target` runs only because it runs through the schedule's
    // holds: one of them reaches it and no hold stops it.
    pub fn spares(&self, target: HoldTarget) -> bool {
        self.reason_for(target).is_none() && self.0.iter().any(|hold| hold.reaches(target))
    }

    // Whether `origin` holds the items of `owner`.
    pub fn held_by(&self, origin: HoldOrigin, owner: HoldOwner) -> bool {
        self.0
            .iter()
            .any(|hold| hold.origin == origin && hold.owner == owner)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DownloadBlockState {
    pub kind: DownloadBlockKind,
    // The egress whose download quota is reached, while one is. The byte
    // and window fields below describe that egress's quota.
    #[serde(default)]
    pub egress_id: Option<u32>,
    #[serde(default)]
    pub egress_name: Option<String>,
    pub used_bytes: u64,
    pub limit_bytes: u64,
    pub remaining_bytes: u64,
    pub window_starts_at_epoch_ms: Option<f64>,
    pub window_ends_at_epoch_ms: Option<f64>,
    pub timezone_name: String,
    // Download limit imposed by the active schedule.
    #[serde(default)]
    pub scheduled_speed_limit: Option<u64>,
    // Download limit the downloads run under now: the tighter of the
    // configured one and the schedule's; none is unlimited.
    #[serde(default)]
    pub speed_limit_in_force: Option<u64>,
    // Whether a schedule window sets the limit in force, even to unlimited.
    #[serde(default)]
    pub speed_limit_by_schedule: bool,
    // Why a schedule rule is holding new downloads, while one is.
    #[serde(default)]
    pub schedule_hold_reason: Option<String>,
}

// An egress download quota that holds work back: no leg of the route the
// work needs has quota left.
#[derive(Debug, Clone, PartialEq)]
pub struct EgressQuotaBlock {
    pub egress_id: u32,
    pub egress_name: String,
    pub used_bytes: u64,
    pub limit_bytes: u64,
    pub remaining_bytes: u64,
    pub window_starts_at_epoch_ms: Option<f64>,
    pub window_ends_at_epoch_ms: Option<f64>,
    pub timezone_name: String,
}

impl Default for DownloadBlockState {
    fn default() -> Self {
        Self {
            kind: DownloadBlockKind::None,
            egress_id: None,
            egress_name: None,
            used_bytes: 0,
            limit_bytes: 0,
            remaining_bytes: 0,
            window_starts_at_epoch_ms: None,
            window_ends_at_epoch_ms: None,
            timezone_name: chrono::Local::now().offset().to_string(),
            scheduled_speed_limit: None,
            speed_limit_in_force: None,
            speed_limit_by_schedule: false,
            schedule_hold_reason: None,
        }
    }
}

// Commands sent to the scheduler's main loop.
#[derive(Clone)]
pub struct RestoreJobRequest {
    pub job_id: JobId,
    pub job_hash: [u8; 32],
    pub spec: JobSpec,
    pub detected_archives: HashMap<u32, DetectedArchiveIdentity>,
    pub file_identities: HashMap<u32, ActiveFileIdentity>,
    pub status: JobStatus,
    pub download_state: Option<crate::jobs::model::DownloadState>,
    pub post_state: Option<crate::jobs::model::PostState>,
    pub run_state: Option<crate::jobs::model::RunState>,
    pub queued_repair_at_epoch_ms: Option<f64>,
    pub queued_extract_at_epoch_ms: Option<f64>,
    pub paused_resume_status: Option<JobStatus>,
    pub paused_resume_download_state: Option<crate::jobs::model::DownloadState>,
    pub paused_resume_post_state: Option<crate::jobs::model::PostState>,
    pub working_dir: PathBuf,
    // What a restart read of the job's row, when the request comes from one.
    pub restart: Option<RestartRead>,
}

// What a restart read of a job's active row besides its request: the runtime
// columns as stored, so the install writes back only what it changes, and
// the summaries of the article lists it let go, so a job waiting its turn
// installs without reading them back.
#[derive(Clone, Debug)]
pub struct RestartRead {
    pub runtime: StoredRuntimeValues,
    pub summaries: crate::jobs::model::FileSummaries,
    // When the job was added, as its row stores it.
    pub created_at_epoch_ms: f64,
    // The job is entering the queue window again, not arriving: it raises
    // no creation event.
    pub reentry: bool,
    // The job has no stored NZB outline yet: it holds no file, and its
    // install reads its stored NZB once and stores the outline it reads. It
    // installs wherever it stands, so its row says what it holds; one out
    // of the window leaves it again once read.
    pub outline_pending: bool,
    // The job has a checkpoint: some of its bytes are on disk.
    pub on_disk: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AddJobOptions {
    pub initially_paused: bool,
    // SCORE admission generation that must still be authoritative when the
    // scheduler durably materializes this job.
    pub semantic_materialization_generation: Option<i64>,
    // Promotion-lease generation that owns this candidate materialization.
    // Unlike admission generations, this must also still hold an unexpired
    // durable promotion lease when the active job row is inserted.
    pub semantic_promotion_generation: Option<i64>,
    // Archive passwords supplied beside the job's own (`Integration`, then
    // `DnzbHeader`), persisted with it so a restart still offers them: they are
    // not in the NZB bytes.
    pub supplied_password_candidates: Vec<crate::jobs::ArchivePasswordCandidate>,
}

// Internal provenance for cancellation. Semantic cancellation is intentionally
// narrower than a user cancel: it may interrupt only download-lane work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancellationOrigin {
    User,
    UserDuplicate,
    // A user cancel that leaves the job's files on disk.
    UserKeepingFiles,
    SemanticBad,
    SemanticSuperseded,
}

impl CancellationOrigin {
    pub fn is_user(self) -> bool {
        matches!(
            self,
            Self::User | Self::UserDuplicate | Self::UserKeepingFiles
        )
    }
}

// Per-id acceptance: `Ok` means the cancellation intent was recorded and
// hidden from the live queue; teardown and durable archival follow later.
pub type CancelJobsOutcome = BTreeMap<JobId, Result<(), SchedulerError>>;

// Which hardware profile's limits the pipeline is running under, and why.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HardwareProfileInForce {
    // The profile whose limits the next download, decode and extraction use.
    pub active: weaver_server_base::runtime::hardware_profile::HardwareProfile,
    // The profile a schedule rule put in force over the operator's choice.
    // `None` when no rule does, or when the rule's profile is one this
    // machine cannot honour and the choice stands instead.
    pub scheduled: Option<weaver_server_base::runtime::hardware_profile::HardwareProfile>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NntpRuntimeActivation {
    pub generation: u64,
    pub configured_connections: usize,
}

// What deleting one history record left behind.
//
// The record itself is always gone when this is returned. A working
// directory whose ownership marker names the job but no longer matches the
// directory is not removed — nothing proves it is still the job's — and is
// listed here so the caller can say so.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HistoryDeleteOutcome {
    pub left_in_place: Vec<std::path::PathBuf>,
}

// How many history ids one delete command carries. A batch is one
// transaction, one statement per table, and one reply the caller waits for,
// while the queue scripts of each job in it are stopped one after another
// before its directory goes. So a batch is also what one database error or
// one slow script fails whole: small enough to keep that to a few dozen
// rows, large enough that the transaction, lock and sweep a batch pays once
// are still shared across many.
pub const HISTORY_DELETE_BATCH_SIZE: usize = 50;

// What one batched history delete did, id by id. A refused id never holds
// back the rest of the batch.
#[derive(Debug, Default)]
pub struct HistoryDeleteBatchOutcome {
    // Every id whose record is gone, with what its cleanup did: what it left
    // on disk, or why it did not finish. A cleanup that failed still leaves
    // the record deleted, so the id is here either way and is announced as
    // removed; the error is reported beside that, never instead of it.
    pub deleted: BTreeMap<JobId, Result<HistoryDeleteOutcome, SchedulerError>>,
    // Ids whose record was not deleted, and why.
    pub refused: BTreeMap<JobId, SchedulerError>,
}

// What archiving one history entry did. The entry stays either way.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchiveHistoryOutcome {
    // Archived: folders Weaver manages were on disk and are gone.
    Archived,
    // Archived with nothing on disk, or the entry was already archived.
    AlreadyGone,
    // A folder of the job is on disk without proof that Weaver manages it,
    // so it is left as it is. Folders that are proven the job's still go.
    NotManaged,
    // The job is in the queue, or another history operation holds it.
    Busy,
    // There is no history entry with this id.
    NotFound,
    // Removing stopped on an error. A retry removes what is left.
    Failed,
}

impl ArchiveHistoryOutcome {
    // The code an operation target keeps for the outcome.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Archived => "archived",
            Self::AlreadyGone => "already_gone",
            Self::NotManaged => "not_managed",
            Self::Busy => "busy",
            Self::NotFound => "not_found",
            Self::Failed => "failed",
        }
    }

    // The outcome a kept code names; `None` for any other code, such as a
    // archive a delete superseded.
    pub fn parse(code: &str) -> Option<Self> {
        Some(match code {
            "archived" => Self::Archived,
            "already_gone" => Self::AlreadyGone,
            "not_managed" => Self::NotManaged,
            "busy" => Self::Busy,
            "not_found" => Self::NotFound,
            "failed" => Self::Failed,
            _ => return None,
        })
    }

    // Whether the entry ends with nothing left to remove: its target
    // completes. Every other outcome fails the target, which the entry then
    // shows.
    pub fn settles(self) -> bool {
        matches!(self, Self::Archived | Self::AlreadyGone | Self::NotFound)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveHistoryResult {
    pub outcome: ArchiveHistoryOutcome,
    // When the entry was archived, in epoch seconds, once it says so.
    pub archived_at: Option<i64>,
    pub message: Option<String>,
}

impl ArchiveHistoryResult {
    pub fn new(
        outcome: ArchiveHistoryOutcome,
        archived_at: Option<i64>,
        message: Option<String>,
    ) -> Self {
        Self {
            outcome,
            archived_at,
            message,
        }
    }
}

// One `(job, bytes per second)` pair per job whose download phase currently
// reports a rate. The rate is the value the independent ticker published for
// that job on the metrics tick, so it is the same estimator and the same
// instant as the global speed gauge.
pub fn job_download_rates(jobs: &[JobInfo]) -> Vec<(JobId, u64)> {
    jobs.iter()
        .filter_map(|job| {
            job.phase_progress
                .iter()
                .find(|phase| phase.phase == crate::jobs::phase_progress::JobPhase::Downloading)
                .and_then(|phase| phase.rate_bps)
                .map(|rate_bps| (job.job_id, rate_bps))
        })
        .collect()
}

impl JobInfo {
    /// The event that says how a finished job ended, as the queue hears it.
    /// None while the job has not ended.
    pub fn ended_event(&self) -> Option<PipelineEvent> {
        let job_id = self.job_id;
        match &self.status {
            JobStatus::Complete => Some(PipelineEvent::JobCompleted { job_id }),
            JobStatus::Failed { error } => Some(PipelineEvent::JobFailed {
                job_id,
                error: error.clone(),
            }),
            _ => None,
        }
    }
}

// Summary info about a job (returned by queries). Its `Debug` never shows
// the archive password.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct JobInfo {
    pub job_id: JobId,
    #[serde(default)]
    pub protocol: crate::jobs::model::Protocol,
    pub job_hash: Option<[u8; 32]>,
    pub name: String,
    pub status: JobStatus,
    pub download_state: crate::jobs::model::DownloadState,
    // Network download is complete, but decode/write or PAR2 discovery work remains.
    #[serde(default)]
    pub finalizing_download: bool,
    // Weaver is fetching the minimum PAR2 metadata or recovery data needed to finish.
    #[serde(default)]
    pub fetching_repair_data: bool,
    pub post_state: crate::jobs::model::PostState,
    pub run_state: crate::jobs::model::RunState,
    pub progress: f64,
    pub total_bytes: u64,
    pub downloaded_bytes: u64,
    pub optional_recovery_bytes: u64,
    pub optional_recovery_downloaded_bytes: u64,
    #[serde(default)]
    pub phase_progress: Vec<crate::jobs::phase_progress::JobPhaseProgress>,
    // Bytes from segments that are permanently lost (430 / max retries).
    pub failed_bytes: u64,
    // Job health 0-1000 (1000 = perfect). Drops as articles fail.
    //
    // For a job that reached a terminal status this is the settled figure —
    // what the claim census concluded was delivered — not the live wire
    // counter, which knows nothing of the repairs and discards that answered
    // its misses.
    pub health: u32,
    // Files the settlement dropped from the delivery, with the bytes each one
    // took out of the accounting. Empty for every job that discarded nothing.
    #[serde(default)]
    pub terminal_discards: Vec<crate::jobs::model::TerminalDiscard>,
    // Total files in the NZB (all roles).
    #[serde(default)]
    pub total_files: u32,
    // Files fully downloaded and committed.
    #[serde(default)]
    pub completed_files: u32,
    // Each NZB file's bytes not yet downloaded, in the NZB's file order, as
    // the post declares their sizes. Empty when no download runs for the job.
    #[serde(default)]
    pub file_remaining_bytes: Vec<u64>,
    // Of each NZB file's bytes not yet downloaded, those of recovery held
    // back until a loss asks for it, in the same order. Empty when no
    // download runs for the job.
    #[serde(default)]
    pub file_parked_bytes: Vec<u64>,
    // The post's encoded bytes still to come: every article neither in hand
    // nor lost for good, less the recovery parked until something asks for
    // it. A clean post ends its download at zero; recovery fetched after a
    // loss counts from the moment it is asked for.
    #[serde(default)]
    pub remaining_bytes: u64,
    // The post's encoded bytes the download is out to bring: every article
    // but the recovery still parked. What is done of it is this less
    // `remaining_bytes`, so the two measure the same declared bytes and the
    // download ends at exactly 100%.
    #[serde(default)]
    pub wanted_bytes: u64,
    // PAR2 recovery volumes not yet fetched (fetched on demand).
    #[serde(default)]
    pub remaining_par_files: u32,
    pub password: Option<String>,
    // Optional category (e.g. "tv", "movies").
    pub category: Option<String>,
    // Arbitrary key-value metadata from the submitting client.
    pub metadata: Vec<(String, String)>,
    // Output directory where extracted files land.
    pub output_dir: Option<String>,
    // Which servers served this job's articles, and how much each carried.
    //
    // Reporting only, and only as far as attribution reached: articles
    // Weaver could not name a server for are absent, so these counts are a
    // floor. Empty for a job downloaded before this was recorded.
    #[serde(default)]
    pub server_attribution: Vec<crate::jobs::server_attribution::JobServerContribution>,
    // Error message (only set when status is Failed).
    pub error: Option<String>,
    #[serde(default)]
    pub download_wait_reason: Option<String>,
    #[serde(default)]
    pub download_retry_at_epoch_ms: Option<f64>,
    // While the job waits to start for its disk: the bytes it still has to
    // fetch, and the room the disk leaves for them, `None` without a fresh
    // reading; for `disk_too_small`, the most the disk ever holds above its
    // reserve. Kept in memory only, never stored.
    #[serde(skip)]
    pub disk_need_bytes: Option<u64>,
    #[serde(skip)]
    pub disk_free_bytes: Option<u64>,
    // Wall-clock creation time (Unix epoch milliseconds).
    pub created_at_epoch_ms: f64,
}

impl std::fmt::Debug for JobInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Destructured whole, so a new field is a compile error here rather
        // than a field missing from the output.
        let Self {
            job_id,
            protocol,
            job_hash,
            name,
            status,
            download_state,
            finalizing_download,
            fetching_repair_data,
            post_state,
            run_state,
            progress,
            total_bytes,
            downloaded_bytes,
            optional_recovery_bytes,
            optional_recovery_downloaded_bytes,
            phase_progress,
            failed_bytes,
            health,
            terminal_discards,
            total_files,
            completed_files,
            file_remaining_bytes,
            file_parked_bytes,
            remaining_bytes,
            wanted_bytes,
            remaining_par_files,
            password,
            category,
            metadata,
            output_dir,
            server_attribution,
            error,
            download_wait_reason,
            download_retry_at_epoch_ms,
            disk_need_bytes,
            disk_free_bytes,
            created_at_epoch_ms,
        } = self;
        f.debug_struct("JobInfo")
            .field("job_id", job_id)
            .field("protocol", protocol)
            .field("job_hash", job_hash)
            .field("name", name)
            .field("status", status)
            .field("download_state", download_state)
            .field("finalizing_download", finalizing_download)
            .field("fetching_repair_data", fetching_repair_data)
            .field("post_state", post_state)
            .field("run_state", run_state)
            .field("progress", progress)
            .field("total_bytes", total_bytes)
            .field("downloaded_bytes", downloaded_bytes)
            .field("optional_recovery_bytes", optional_recovery_bytes)
            .field(
                "optional_recovery_downloaded_bytes",
                optional_recovery_downloaded_bytes,
            )
            .field("phase_progress", phase_progress)
            .field("failed_bytes", failed_bytes)
            .field("health", health)
            .field("terminal_discards", terminal_discards)
            .field("total_files", total_files)
            .field("completed_files", completed_files)
            .field("file_remaining_bytes", file_remaining_bytes)
            .field("file_parked_bytes", file_parked_bytes)
            .field("remaining_bytes", remaining_bytes)
            .field("wanted_bytes", wanted_bytes)
            .field("remaining_par_files", remaining_par_files)
            .field("password", &password.as_ref().map(|_| "[REDACTED]"))
            .field("category", category)
            .field(
                "metadata",
                &metadata
                    .iter()
                    .map(|(key, value)| {
                        let value = if crate::jobs::attributes::is_password_attribute_key(key) {
                            "[REDACTED]"
                        } else {
                            value.as_str()
                        };
                        (key.as_str(), value)
                    })
                    .collect::<Vec<_>>(),
            )
            .field("output_dir", output_dir)
            .field("server_attribution", server_attribution)
            .field("error", error)
            .field("download_wait_reason", download_wait_reason)
            .field("download_retry_at_epoch_ms", download_retry_at_epoch_ms)
            .field("disk_need_bytes", disk_need_bytes)
            .field("disk_free_bytes", disk_free_bytes)
            .field("created_at_epoch_ms", created_at_epoch_ms)
            .finish()
    }
}

// A job's runtime columns in its active row, as the pipeline writes them.
#[derive(Debug, Clone, PartialEq)]
pub struct ActiveRuntimeValues {
    pub status: &'static str,
    pub download_state: Option<&'static str>,
    pub post_state: Option<&'static str>,
    pub run_state: Option<&'static str>,
    pub error: Option<String>,
    pub queued_repair_at_epoch_ms: Option<f64>,
    pub queued_extract_at_epoch_ms: Option<f64>,
    pub paused_resume_status: Option<&'static str>,
    pub paused_resume_download_state: Option<&'static str>,
    pub paused_resume_post_state: Option<&'static str>,
}

// A job's runtime columns as its active row holds them, read by a restart.
#[derive(Debug, Clone, PartialEq)]
pub struct StoredRuntimeValues {
    status: String,
    download_state: Option<String>,
    post_state: Option<String>,
    run_state: Option<String>,
    error: Option<String>,
    queued_repair_at_epoch_ms: Option<f64>,
    queued_extract_at_epoch_ms: Option<f64>,
    paused_resume_status: Option<String>,
    paused_resume_download_state: Option<String>,
    paused_resume_post_state: Option<String>,
}

impl StoredRuntimeValues {
    pub fn of(row: &crate::jobs::record::RecoveredJob) -> Self {
        Self {
            status: row.status.clone(),
            download_state: row.download_state.clone(),
            post_state: row.post_state.clone(),
            run_state: row.run_state.clone(),
            error: row.error.clone(),
            queued_repair_at_epoch_ms: row.queued_repair_at_epoch_ms,
            queued_extract_at_epoch_ms: row.queued_extract_at_epoch_ms,
            paused_resume_status: row.paused_resume_status.clone(),
            paused_resume_download_state: row.paused_resume_download_state.clone(),
            paused_resume_post_state: row.paused_resume_post_state.clone(),
        }
    }

    // Whether the row already holds every one of `values`.
    pub fn holds(&self, values: &ActiveRuntimeValues) -> bool {
        self.status == values.status
            && self.download_state.as_deref() == values.download_state
            && self.post_state.as_deref() == values.post_state
            && self.run_state.as_deref() == values.run_state
            && self.error == values.error
            && self.queued_repair_at_epoch_ms == values.queued_repair_at_epoch_ms
            && self.queued_extract_at_epoch_ms == values.queued_extract_at_epoch_ms
            && self.paused_resume_status.as_deref() == values.paused_resume_status
            && self.paused_resume_download_state.as_deref() == values.paused_resume_download_state
            && self.paused_resume_post_state.as_deref() == values.paused_resume_post_state
    }
}
