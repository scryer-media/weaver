//! The last few debug lines of each job, kept whatever the log level is.
//!
//! A stall is usually reported at INFO, where the lines that explain it —
//! the job's own DEBUG and TRACE events — were filtered out long before the
//! report fired. The binary installs a tracing layer that feeds every such
//! event carrying a `job_id` field into a small ring per job, and the code that
//! reports a stall calls [`dump`] to write that ring out once, beside the
//! report, as a single "stall diagnostics" record.
//!
//! Rings are bounded per job and in the number of jobs held, and dropped when
//! the job's runtime is purged, so a process that never stalls pays only the
//! capture itself.

use std::collections::{HashMap, VecDeque};
use std::sync::{Mutex, OnceLock};

use tracing::warn;

/// Lines kept per job. Older lines fall off the front.
pub const LINES_PER_JOB: usize = 256;

/// Jobs holding a ring at once. A purge normally removes a job's ring; this
/// only bounds what a job that is never purged can leave behind.
const MAX_JOBS: usize = 512;

/// Per-job rings of formatted debug lines.
pub struct JobDebugRings {
    lines_per_job: usize,
    max_jobs: usize,
    rings: Mutex<HashMap<u64, VecDeque<String>>>,
}

impl JobDebugRings {
    pub fn new(lines_per_job: usize, max_jobs: usize) -> Self {
        Self {
            lines_per_job: lines_per_job.max(1),
            max_jobs: max_jobs.max(1),
            rings: Mutex::new(HashMap::new()),
        }
    }

    pub fn record(&self, job_id: u64, line: String) {
        let mut rings = self.lock();
        if !rings.contains_key(&job_id) && rings.len() >= self.max_jobs {
            // Nothing tells the rings which job is oldest, and a job that is
            // still running keeps refilling its own. Starting over costs the
            // diagnostics of a stall nobody has reported yet, nothing more.
            rings.clear();
        }
        let ring = rings.entry(job_id).or_default();
        if ring.len() >= self.lines_per_job {
            ring.pop_front();
        }
        ring.push_back(line);
    }

    /// Removes and returns the job's ring, oldest line first.
    pub fn take(&self, job_id: u64) -> Vec<String> {
        self.lock()
            .remove(&job_id)
            .map(Vec::from)
            .unwrap_or_default()
    }

    pub fn forget(&self, job_id: u64) {
        self.lock().remove(&job_id);
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<u64, VecDeque<String>>> {
        self.rings
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

static RINGS: OnceLock<JobDebugRings> = OnceLock::new();

/// Turns capture on for this process. Until it is called, [`record`] and
/// [`dump`] do nothing, so a process without the layer holds no rings.
pub fn install() {
    let _ = RINGS.get_or_init(|| JobDebugRings::new(LINES_PER_JOB, MAX_JOBS));
}

/// Appends one formatted line to the job's ring.
pub fn record(job_id: u64, line: String) {
    if let Some(rings) = RINGS.get() {
        rings.record(job_id, line);
    }
}

/// Removes and returns the job's ring, oldest line first, without writing it.
pub fn take(job_id: u64) -> Vec<String> {
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

/// Writes the job's ring out once, at WARN, as one record, and clears it.
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
        "stall diagnostics: recent debug lines for this job\n{}",
        lines.join("\n")
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_ring_keeps_only_its_newest_lines() {
        let rings = JobDebugRings::new(3, 8);
        for index in 0..5 {
            rings.record(7, format!("line {index}"));
        }

        assert_eq!(rings.take(7), vec!["line 2", "line 3", "line 4"]);
        assert!(rings.take(7).is_empty(), "taking a ring clears it");
    }

    #[test]
    fn rings_are_kept_per_job_and_forgotten_per_job() {
        let rings = JobDebugRings::new(4, 8);
        rings.record(1, "one".to_string());
        rings.record(2, "two".to_string());

        rings.forget(1);

        assert!(rings.take(1).is_empty());
        assert_eq!(rings.take(2), vec!["two"]);
    }

    #[test]
    fn the_number_of_jobs_held_is_bounded() {
        let rings = JobDebugRings::new(4, 2);
        rings.record(1, "one".to_string());
        rings.record(2, "two".to_string());
        rings.record(3, "three".to_string());

        assert!(rings.lock().len() <= 2);
        assert_eq!(rings.take(3), vec!["three"]);
    }
}
