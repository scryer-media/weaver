// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The parts of the job model that hold pipeline state: a running job's
// queues and articles, and the memory charge for them.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

pub use weaver_server_composition::jobs::model::*;

use crate::jobs::assembly::{DetectedArchiveIdentity, JobAssembly};
use crate::jobs::record::ActiveFileIdentity;
use crate::jobs::server_attribution::JobServerAttribution;
use crate::pipeline::download::queue::DownloadQueue;
use weaver_server_base::jobs::ids::{JobId, NzbFileId};

pub(crate) trait SchedulingMemoryEstimate {
    fn scheduling_memory_estimate(&self) -> u64;
}

impl SchedulingMemoryEstimate for JobSpec {
    // Conservative estimate of everything a running job holds: the spec's
    // article lists, queue entries, assembly indexes, and their cloned
    // strings. Count placements even when message IDs repeat: each placement
    // still owns scheduling and output state. Charged when the job's articles
    // are built and held until it leaves the pipeline.
    fn scheduling_memory_estimate(&self) -> u64 {
        let per_file = (8 * std::mem::size_of::<crate::jobs::assembly::FileAssembly>()
            + 4 * std::mem::size_of::<FileSpec>()) as u64;
        let per_segment = (4 * std::mem::size_of::<crate::DownloadWork>()
            + 2 * std::mem::size_of::<SegmentSpec>()
            + 64) as u64;
        self.files.iter().fold(0u64, |total, file| {
            let file_bytes = file.groups.iter().fold(
                per_file.saturating_add((file.filename.len() as u64).saturating_mul(4)),
                |bytes, group| bytes.saturating_add((group.len() as u64).saturating_mul(4)),
            );
            file.segments
                .iter()
                .fold(total.saturating_add(file_bytes), |bytes, segment| {
                    bytes.saturating_add(per_segment).saturating_add(
                        segment.message_ids.all().iter().fold(0u64, |ids, id| {
                            ids.saturating_add((id.len() as u64).saturating_mul(4))
                        }),
                    )
                })
        })
    }
}

// Internal state for a running job.
pub struct JobState {
    pub job_id: JobId,
    pub job_hash: [u8; 32],
    pub spec: JobSpec,
    // Runtime status used by the pipeline choreography. The lane fields below
    // are compatibility projections and must not drive core lifecycle logic.
    pub status: JobStatus,
    pub download_state: DownloadState,
    pub post_state: PostState,
    pub run_state: RunState,
    // The job's per-article state, by where the job is in its run: nothing
    // per article while it waits its turn, everything once it is first handed
    // work. See [`JobArticles`].
    pub articles: JobArticles,
    pub created_at: std::time::Instant,
    // Wall-clock creation time (Unix epoch milliseconds).
    pub created_at_epoch_ms: f64,
    // When this job most recently entered the bounded repair queue.
    pub queued_repair_at_epoch_ms: Option<f64>,
    // When this job most recently entered the bounded extraction queue.
    pub queued_extract_at_epoch_ms: Option<f64>,
    // Safe status to restore when a paused job is resumed.
    pub paused_resume_status: Option<JobStatus>,
    // Compatibility lane targets retained for v0.2.9-era persisted rows.
    pub paused_resume_download_state: Option<DownloadState>,
    pub paused_resume_post_state: Option<PostState>,
    // Last terminal error associated with this job.
    pub failure_error: Option<String>,
    // What the graph ruled of a delivery restored after its final move
    // began, read back from the job's row; the graph itself is not restored.
    pub delivered: Option<crate::post_processing::model::DeliveredVerdict>,
    // The consumed inputs the final move leaves in the working directory,
    // relative to it: set when the move begins, or read back from the row of
    // a move a restart interrupted.
    pub delivery_left_behind: Vec<PathBuf>,
    // The working directory the final move delivers from: set when the move
    // begins, or read back from the row of a move a restart interrupted.
    pub delivery_source: Option<PathBuf>,
    // Per-job working directory (subdirectory under intermediate_dir).
    // All download/decode/verify/repair/extract I/O happens here.
    pub working_dir: PathBuf,
    // Bytes downloaded for this specific job.
    pub downloaded_bytes: u64,
    // What `downloaded_bytes` read as the job's last checkpoint was taken:
    // the figure the job goes back to with that checkpoint, when a write
    // that found the disk full sends it back there.
    pub checkpointed_download_bytes: u64,
    // Encoded bytes received off the wire for this job, credited when an
    // article lands and before it is decoded. Includes retries and articles
    // that failed to decode, so it is never a progress measure: it feeds only
    // the download-phase rate, which must integrate the same bytes as the
    // global speed gauge so a queue row and the nav counter agree.
    pub downloaded_wire_bytes: u64,
    // Which servers served this job's articles, counted as they land.
    //
    // Reporting only: nothing in the scheduling or failover path reads it.
    // An article whose server cannot be named is left uncounted, so this
    // understates rather than misattributes.
    pub server_attribution: JobServerAttribution,
    // The job's first direct-store demotion and its article gaps, kept for
    // the support report. Reporting only, and fixed in size.
    pub support_facts: crate::jobs::support_facts::JobSupportFacts,
    // Conservative restored progress floor from persisted file-write checkpoints.
    // This is only used for reporting after restart and must not affect scheduling.
    pub restored_download_floor_bytes: u64,
    // Bytes from segments that are permanently lost (430 / max retries
    // exhausted).
    //
    // Derived state: the sum of the *declared* sizes of the segments holding a
    // terminal state. Only the pipeline's single terminal-state transition
    // moves it, and nothing may add to it directly.
    pub failed_bytes: u64,
    // Total bytes of PAR2 recovery files, cached from spec at job creation.
    pub par2_bytes: u64,
    // A repair came back applied in this process. Reporting only: the
    // monitoring ledger counts the job repaired when it finishes.
    pub repaired: bool,
    // Persisted probe facts used for pre-extraction classification.
    pub detected_archives: std::collections::HashMap<u32, DetectedArchiveIdentity>,
    // Mutable file identity used by runtime/archive logic after rename/classification.
    pub file_identities: std::collections::HashMap<u32, ActiveFileIdentity>,
    // Staging directory for extraction output (under complete_dir).
    // Extraction writes chunks and assembled files here instead of
    // working_dir, keeping local storage usage low for NFS setups.
    pub staging_dir: Option<PathBuf>,
    // Shared decoded-byte counter for this job's category.
    //
    // Resolved once, when the job enters the pipeline, so the per-segment byte
    // accounting site never looks a category up: it does a single `Relaxed`
    // `fetch_add` through this already-resolved pointer. `None` only while a
    // test builds a bare state; the accounting site simply skips then.
    pub category_bytes: Option<Arc<AtomicU64>>,
    // Password candidates derived from the job's persisted NZB (its
    // `<meta type="password">` and the `{{password}}` file-name convention),
    // in harvest order and without the spec's explicit password.
    //
    // Filled when the job enters the pipeline, or by the first harvest that
    // reads the job's row; a row with no NZB fills it with an empty list. The
    // NZB never changes under a live job, so once filled the harvest reads
    // this instead of reloading and re-parsing the NZB on every archive file.
    // Left unset when the NZB could not be read or parsed, which keeps a
    // transient failure retryable.
    pub nzb_password_candidates: std::sync::OnceLock<Vec<ArchivePasswordCandidate>>,
}

// Everything a job holds per article, built in one place when the job is
// first handed work and dropped with the job.
pub struct ActiveArticles {
    pub assembly: JobAssembly,
    // Primary download queue (data files, PAR2 index, RAR volumes).
    pub download_queue: DownloadQueue,
    // Recovery download queue (PAR2 repair blocks, spare bandwidth).
    pub recovery_queue: DownloadQueue,
}

// Where a job is in its run, as far as its articles go.
//
// A job waits its turn holding nothing per article: its spec keeps every
// file's name and identity, its article lists are empty, and these summaries
// say what the lists held. When the scheduler first reaches it the job is
// activating while its article lists are read back from the stored NZB, off
// the pipeline thread. Once they are in, the job is active and holds its
// article lists, assembly and queues while it runs. A paused job goes back to
// waiting, holding its summaries again, and is activated afresh from its
// stored NZB and checkpoint when it is next reached.
pub enum JobArticles {
    Queued(FileSummaries),
    // The number tells this read from any earlier one of the same job: only
    // the reply that carries it builds the job's articles.
    Activating(FileSummaries, u64),
    Active(Box<ActiveArticles>),
}

impl JobArticles {
    pub fn active(&self) -> Option<&ActiveArticles> {
        match self {
            Self::Active(active) => Some(active),
            Self::Queued(_) | Self::Activating(..) => None,
        }
    }

    pub fn active_mut(&mut self) -> Option<&mut ActiveArticles> {
        match self {
            Self::Active(active) => Some(active),
            Self::Queued(_) | Self::Activating(..) => None,
        }
    }

    // The job's primary queue, or nothing while the job holds no articles.
    pub fn download_queue(&self) -> Option<&DownloadQueue> {
        self.active().map(|active| &active.download_queue)
    }

    pub fn download_queue_mut(&mut self) -> Option<&mut DownloadQueue> {
        self.active_mut().map(|active| &mut active.download_queue)
    }

    pub fn recovery_queue(&self) -> Option<&DownloadQueue> {
        self.active().map(|active| &active.recovery_queue)
    }

    pub fn recovery_queue_mut(&mut self) -> Option<&mut DownloadQueue> {
        self.active_mut().map(|active| &mut active.recovery_queue)
    }

    // A file's newsgroups for work built for one of its articles: the list
    // the job's queues already share among the file's items, so the work
    // holds no copy of its own, or a new one from `groups` when neither queue
    // has held an item of the file.
    pub fn file_groups(&self, file_id: NzbFileId, groups: &[String]) -> Arc<[String]> {
        self.active()
            .and_then(|active| {
                active
                    .download_queue
                    .file_groups(file_id)
                    .or_else(|| active.recovery_queue.file_groups(file_id))
            })
            .map_or_else(|| Arc::from(groups), Arc::clone)
    }

    pub fn assembly(&self) -> Option<&JobAssembly> {
        self.active().map(|active| &active.assembly)
    }

    pub fn assembly_mut(&mut self) -> Option<&mut JobAssembly> {
        self.active_mut().map(|active| &mut active.assembly)
    }

    // Articles still waiting in the primary queue. A job whose articles are
    // not built yet has every article not in hand still to fetch, which is
    // what its queue holds once they are built.
    pub fn queued_download_len(&self) -> usize {
        match self {
            Self::Queued(summaries) | Self::Activating(summaries, _) => {
                summaries.0.iter().map(|file| file.remaining as usize).sum()
            }
            Self::Active(active) => active.download_queue.len(),
        }
    }

    // Articles parked in the recovery queue; none before the job's articles
    // are built.
    pub fn queued_recovery_len(&self) -> usize {
        self.recovery_queue().map_or(0, DownloadQueue::len)
    }

    // Whether the job still has articles to fetch in its primary queue. A
    // job whose articles are not built yet has every one of them still to
    // fetch.
    pub fn has_queued_download_work(&self) -> bool {
        self.download_queue().is_none_or(|queue| !queue.is_empty())
    }
}

impl JobState {
    // One file's article count and size, whether the job holds its articles
    // or only their summary.
    pub fn file_summary(&self, file_index: usize) -> FileSummary {
        match &self.articles {
            JobArticles::Queued(summaries) | JobArticles::Activating(summaries, _) => {
                summaries.0.get(file_index).copied().unwrap_or_default()
            }
            JobArticles::Active(_) => self
                .spec
                .files
                .get(file_index)
                .map(FileSummary::of)
                .unwrap_or_default(),
        }
    }

    // The recovery files' total size and what of them has arrived; nothing
    // has arrived of a job not yet handed work.
    pub fn optional_recovery_bytes(&self) -> (u64, u64) {
        if let Some(assembly) = self.articles.assembly() {
            return assembly.optional_recovery_bytes();
        }
        if !self
            .spec
            .files
            .iter()
            .any(|file| file.role.counts_toward_health())
        {
            return (0, 0);
        }
        let recovery = (0..self.spec.files.len())
            .filter(|index| self.spec.files[*index].role.is_recovery())
            .map(|index| self.file_summary(index).bytes)
            .sum();
        (recovery, 0)
    }

    // The bytes still to come and the bytes wanted of a job no graph root
    // speaks for, as the post declares them: every file but the recovery
    // volumes, which a run parks unless a loss asks for them, each counting
    // its articles not yet in hand at their share of its bytes.
    pub fn unstarted_remaining(&self) -> (u64, u64) {
        (0..self.spec.files.len())
            .filter(|index| !self.spec.files[*index].role.is_recovery())
            .map(|index| self.file_summary(index))
            .fold((0u64, 0u64), |(remaining, wanted), file| {
                let left = if file.articles == 0 {
                    0
                } else {
                    (u128::from(file.bytes) * u128::from(file.remaining.min(file.articles))
                        / u128::from(file.articles)) as u64
                };
                (remaining + left, wanted + file.bytes)
            })
    }

    pub fn total_file_count(&self) -> usize {
        self.articles
            .assembly()
            .map_or(self.spec.files.len(), JobAssembly::total_file_count)
    }

    pub fn complete_file_count(&self) -> usize {
        self.articles
            .assembly()
            .map_or(0, JobAssembly::complete_file_count)
    }

    // The share of the job's files assembled; none of a job not yet handed
    // work.
    pub fn assembly_progress(&self) -> f64 {
        self.articles.assembly().map_or(0.0, JobAssembly::progress)
    }

    pub fn refresh_runtime_lanes_from_status(&mut self) {
        let (download_state, post_state, run_state) =
            runtime_lanes_from_status_snapshot(&self.status);
        self.download_state = download_state;
        self.post_state = post_state;
        self.run_state = run_state;
    }

    pub fn refresh_legacy_status(&mut self) {
        self.status = derive_legacy_job_status(
            self.download_state,
            self.post_state,
            self.run_state,
            self.failure_error.as_deref(),
        );
    }

    pub fn set_failure(&mut self, error: impl Into<String>) {
        let error = error.into();
        self.failure_error = Some(error.clone());
        self.status = JobStatus::Failed { error };
        self.refresh_runtime_lanes_from_status();
    }
}
