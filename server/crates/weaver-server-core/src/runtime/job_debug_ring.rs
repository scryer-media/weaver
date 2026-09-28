//! The last few debug lines of each job, kept whatever the log level is.
//!
//! A stall is usually reported at INFO, where the lines that explain it —
//! the job's own DEBUG and TRACE events — were filtered out long before the
//! report fired. The binary installs a tracing layer that feeds every such
//! event carrying a `job_id` field into a small ring per job, and the code that
//! reports a stall calls [`dump`] to write that ring out once, beside the
//! report, as a short burst of "stall diagnostics" records.
//!
//! Capture sits on the download and decode hot paths, so it is kept cheap:
//! rings are sharded by job so threads working different jobs do not share a
//! lock, and a line is stamped with the raw wall-clock time, which is only
//! rendered as text when a dump writes it out.
//!
//! Rings are bounded per job and in the number of jobs held, and dropped when
//! the job's runtime is purged, so a process that never stalls pays only the
//! capture itself.

use std::collections::{HashMap, VecDeque};
use std::sync::{Mutex, MutexGuard, OnceLock};
use std::time::SystemTime;

use tracing::warn;

/// Lines kept per job. Older lines fall off the front.
pub const LINES_PER_JOB: usize = 256;

/// Jobs holding a ring at once. A purge normally removes a job's ring; this
/// only bounds what a job that is never purged can leave behind.
const MAX_JOBS: usize = 512;

/// Lock shards. Each job lives in one shard, so threads working different jobs
/// rarely meet on the same lock.
const SHARDS: usize = 64;

/// One captured line: when it was recorded and what it said.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DebugLine {
    pub at: SystemTime,
    pub text: String,
}

impl DebugLine {
    /// The capture time as local RFC 3339 with microseconds.
    pub fn timestamp(&self) -> String {
        chrono::DateTime::<chrono::Local>::from(self.at)
            .to_rfc3339_opts(chrono::SecondsFormat::Micros, true)
    }
}

type Shard = HashMap<u64, VecDeque<DebugLine>>;

/// Per-job rings of debug lines.
pub struct JobDebugRings {
    lines_per_job: usize,
    jobs_per_shard: usize,
    shards: Box<[Mutex<Shard>]>,
}

impl JobDebugRings {
    pub fn new(lines_per_job: usize, max_jobs: usize) -> Self {
        let max_jobs = max_jobs.max(1);
        // Never more shards than jobs, so the per-shard bound multiplied back
        // out never exceeds `max_jobs` by more than rounding.
        let shard_count = max_jobs.min(SHARDS);
        Self {
            lines_per_job: lines_per_job.max(1),
            jobs_per_shard: max_jobs.div_ceil(shard_count),
            shards: (0..shard_count)
                .map(|_| Mutex::new(HashMap::new()))
                .collect(),
        }
    }

    /// Appends one line to the job's ring, stamped with the current time.
    pub fn record(&self, job_id: u64, text: String) {
        let line = DebugLine {
            at: SystemTime::now(),
            text,
        };
        let mut shard = self.shard(job_id);
        if !shard.contains_key(&job_id) && shard.len() >= self.jobs_per_shard {
            // Nothing tells the rings which job is oldest, and a job that is
            // still running keeps refilling its own. Starting this shard over
            // costs the diagnostics of a stall nobody has reported yet,
            // nothing more.
            shard.clear();
        }
        let ring = shard
            .entry(job_id)
            .or_insert_with(|| VecDeque::with_capacity(self.lines_per_job.min(16)));
        if ring.len() >= self.lines_per_job {
            ring.pop_front();
        }
        ring.push_back(line);
    }

    /// Removes and returns the job's ring, oldest line first.
    pub fn take(&self, job_id: u64) -> Vec<DebugLine> {
        self.shard(job_id)
            .remove(&job_id)
            .map(Vec::from)
            .unwrap_or_default()
    }

    pub fn forget(&self, job_id: u64) {
        self.shard(job_id).remove(&job_id);
    }

    fn shard(&self, job_id: u64) -> MutexGuard<'_, Shard> {
        // Job ids are handed out in sequence, so the low bits already spread
        // concurrent jobs across shards.
        let index = (job_id % self.shards.len() as u64) as usize;
        self.shards[index]
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    #[cfg(test)]
    fn job_count(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| shard.lock().unwrap().len())
            .sum()
    }
}

static RINGS: OnceLock<JobDebugRings> = OnceLock::new();

/// Turns capture on for this process. Until it is called, [`record`] and
/// [`dump`] do nothing, so a process without the layer holds no rings.
pub fn install() {
    let _ = RINGS.get_or_init(|| JobDebugRings::new(LINES_PER_JOB, MAX_JOBS));
}

/// Appends one formatted line to the job's ring, stamped with the current time.
pub fn record(job_id: u64, line: String) {
    if let Some(rings) = RINGS.get() {
        rings.record(job_id, line);
    }
}

/// Removes and returns the job's ring, oldest line first, without writing it.
pub fn take(job_id: u64) -> Vec<DebugLine> {
    RINGS
        .get()
        .map(|rings| rings.take(job_id))
        .unwrap_or_default()
}

/// Drops the job's ring without writing it.
pub fn forget(job_id: u64) {
    if let Some(rings) = RINGS.get() {
        rings.forget(job_id);
    }
}

/// Writes the job's ring out once, at WARN, and clears it: a header record
/// giving the count, then one record per captured line, so a log viewer shows
/// one row per line.
///
/// Callers are the throttled stall reports, so this runs at most as often as
/// they do; an empty ring writes nothing.
pub fn dump(job_id: u64, reason: &'static str) {
    let Some(rings) = RINGS.get() else {
        return;
    };
    let lines = rings.take(job_id);
    if lines.is_empty() {
        return;
    }
    warn!(
        job_id,
        reason,
        lines = lines.len(),
        "stall diagnostics: {} recent debug lines follow",
        lines.len()
    );
    for line in &lines {
        warn!(
            job_id,
            reason,
            at = %line.timestamp(),
            line = %line.text,
            "stall diagnostics line"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn texts(lines: Vec<DebugLine>) -> Vec<String> {
        lines.into_iter().map(|line| line.text).collect()
    }

    #[test]
    fn a_ring_keeps_only_its_newest_lines() {
        let rings = JobDebugRings::new(3, 8);
        for index in 0..5 {
            rings.record(7, format!("line {index}"));
        }

        assert_eq!(texts(rings.take(7)), vec!["line 2", "line 3", "line 4"]);
        assert!(rings.take(7).is_empty(), "taking a ring clears it");
    }

    #[test]
    fn rings_are_kept_per_job_and_forgotten_per_job() {
        let rings = JobDebugRings::new(4, 8);
        rings.record(1, "one".to_string());
        rings.record(2, "two".to_string());

        rings.forget(1);

        assert!(rings.take(1).is_empty());
        assert_eq!(texts(rings.take(2)), vec!["two"]);
    }

    #[test]
    fn the_number_of_jobs_held_is_bounded() {
        let rings = JobDebugRings::new(4, 2);
        rings.record(1, "one".to_string());
        rings.record(2, "two".to_string());
        rings.record(3, "three".to_string());

        assert!(rings.job_count() <= 2);
        assert_eq!(texts(rings.take(3)), vec!["three"]);
    }

    #[test]
    fn the_job_bound_holds_across_every_shard() {
        let rings = JobDebugRings::new(2, MAX_JOBS);
        for job_id in 0..(MAX_JOBS as u64 * 3) {
            rings.record(job_id, "line".to_string());
        }

        assert!(rings.job_count() <= MAX_JOBS, "{}", rings.job_count());
        assert_eq!(
            texts(rings.take(MAX_JOBS as u64 * 3 - 1)),
            vec!["line"],
            "the newest job keeps its ring"
        );
    }

    #[test]
    fn a_line_renders_its_capture_time_only_on_request() {
        let line = DebugLine {
            at: SystemTime::UNIX_EPOCH + std::time::Duration::from_micros(1_500_000),
            text: "stamped".to_string(),
        };

        let rendered = line.timestamp();
        let parsed = chrono::DateTime::parse_from_rfc3339(&rendered).expect(&rendered);
        assert_eq!(parsed.timestamp_micros(), 1_500_000, "{rendered}");
    }
}
