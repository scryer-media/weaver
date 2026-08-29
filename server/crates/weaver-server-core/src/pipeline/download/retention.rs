use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::*;

/// How long a job's computed retention exclusions stay fresh. Retention is
/// day-granular; the TTL only exists to pick up server-config edits and
/// day-boundary crossings without recomputing per lease.
const JOB_RETENTION_EXCLUDES_TTL: Duration = Duration::from_secs(60);

impl Pipeline {
    /// Newest known post date across the job's files. `None` when the NZB
    /// carried no usable dates — such jobs are never retention-skipped.
    pub(crate) fn job_posted_at_epoch(spec: &JobSpec) -> Option<u64> {
        spec.files
            .iter()
            .filter_map(|file| file.posted_at_epoch)
            .max()
    }

    /// When this job's articles become old enough to fetch, or `None` if they
    /// already are.
    ///
    /// `Some` means dispatch holds off. The job stays exactly where it is —
    /// `Queued`, in the ordinary queue, with no status of its own — because a
    /// deferral is not a state the user needs a word for: it resolves by
    /// itself, on a clock, within minutes. Pause, resume and delete all behave
    /// as they always did, since none of them consults this.
    ///
    /// The answer is computed once per job and cached. It is derived from wall
    /// time (the NZB's date is an epoch second) but stored as an [`Instant`],
    /// so a system-clock adjustment after the job was admitted cannot move a
    /// deadline the pipeline has already committed to — the same reason the
    /// restart-lead retry beside it stores one.
    ///
    /// A job with no parseable dates is never deferred: `job_posted_at_epoch`
    /// is `None` and the question has no answer, which is different from
    /// answering "wait". Missing dates on *some* files contribute nothing —
    /// the anchor is the newest date the NZB does carry, because that is the
    /// article most likely still in flight.
    pub(in crate::pipeline) fn propagation_hold_until(&mut self, job_id: JobId) -> Option<Instant> {
        let delay = self
            .propagation_delay_forced
            .unwrap_or_else(crate::pipeline::propagation_delay);
        if delay.is_zero() {
            return None;
        }
        if let Some(ready_at) = self.propagation_ready_at.get(&job_id).copied() {
            if ready_at > Instant::now() {
                return Some(ready_at);
            }
            self.propagation_ready_at.remove(&job_id);
            return None;
        }

        let posted_at = self
            .jobs
            .get(&job_id)
            .and_then(|state| Self::job_posted_at_epoch(&state.spec))?;
        let now_epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| elapsed.as_secs())
            .unwrap_or(0);
        // A post dated in the future is treated as posted now rather than
        // deferred by however far the clock disagrees: saturating here means a
        // skewed poster's date can cost at most `delay`, never hours.
        let age = Duration::from_secs(now_epoch.saturating_sub(posted_at));
        let remaining = delay.saturating_sub(age);
        if remaining.is_zero() {
            return None;
        }

        let ready_at = Instant::now() + remaining;
        self.propagation_ready_at.insert(job_id, ready_at);
        // Once per job, because the gate is consulted on every dispatch pass.
        info!(
            job_id = job_id.0,
            posted_at_epoch = posted_at,
            eligible_in_secs = remaining.as_secs(),
            delay_secs = delay.as_secs(),
            "deferring download start: the post is still propagating"
        );
        crate::runtime::perf_probe::record(
            "download.propagation.deferred",
            std::time::Duration::from_nanos(1),
        );
        Some(ready_at)
    }

    /// How long until the earliest deferred job becomes eligible, for the run
    /// loop's sleep. `None` when nothing is deferred.
    pub(crate) fn next_propagation_delay(&self) -> Option<Duration> {
        let now = Instant::now();
        self.propagation_ready_at
            .values()
            .copied()
            .min()
            .map(|ready_at| ready_at.saturating_duration_since(now))
    }

    /// Pool server indices whose retention window is shorter than this job's
    /// post age. These servers are skipped for the job's articles without a
    /// network attempt, carry no health penalty, and count toward
    /// per-article exhaustion.
    pub(in crate::pipeline) fn job_retention_excludes(&mut self, job_id: JobId) -> Arc<Vec<usize>> {
        let now = Instant::now();
        if let Some((computed_at, excludes)) = self.job_retention_exclude_cache.get(&job_id)
            && now.duration_since(*computed_at) < JOB_RETENTION_EXCLUDES_TTL
        {
            return Arc::clone(excludes);
        }
        let excludes = Arc::new(self.compute_job_retention_excludes(job_id));
        self.job_retention_exclude_cache
            .insert(job_id, (now, Arc::clone(&excludes)));
        excludes
    }

    fn compute_job_retention_excludes(&self, job_id: JobId) -> Vec<usize> {
        let retention_days = self.nntp.pool().server_retention_days();
        if retention_days.iter().all(|days| *days == 0) {
            return Vec::new();
        }
        let Some(posted_at) = self
            .jobs
            .get(&job_id)
            .and_then(|state| Self::job_posted_at_epoch(&state.spec))
        else {
            return Vec::new();
        };
        let now_epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| elapsed.as_secs())
            .unwrap_or(0);
        let age_secs = now_epoch.saturating_sub(posted_at);
        retention_days
            .iter()
            .enumerate()
            .filter(|(_, days)| **days > 0 && age_secs > u64::from(**days) * 86_400)
            .map(|(idx, _)| idx)
            .collect()
    }

    /// Union of a work item's failure exclusions and the job's retention
    /// exclusions — the effective exclude set for server ordering. Failure
    /// excludes stay per-article on the work item; retention excludes stay
    /// job-derived so a server-config change applies without rewriting
    /// queued work.
    pub(in crate::pipeline) fn effective_exclude_servers(
        &mut self,
        job_id: JobId,
        failure_excludes: &[usize],
    ) -> Vec<usize> {
        let retention = self.job_retention_excludes(job_id);
        if retention.is_empty() {
            return failure_excludes.to_vec();
        }
        let mut merged = failure_excludes.to_vec();
        for idx in retention.iter() {
            if !merged.contains(idx) {
                merged.push(*idx);
            }
        }
        merged
    }

    /// Number of distinct, currently-valid pool indices unavailable to this
    /// article: the union of its failure exclusions and the job's retention
    /// exclusions. Indices outside the current pool are ignored — a server
    /// config rebuild can shrink or reorder the pool, and stale indices must
    /// not inflate exhaustion math into spurious "article missing" verdicts.
    pub(in crate::pipeline) fn unavailable_server_count(
        &mut self,
        job_id: JobId,
        failure_excludes: &[usize],
    ) -> usize {
        let server_count = self.nntp.pool().server_count();
        let retention = self.job_retention_excludes(job_id);
        Self::unavailable_server_count_from_excludes(server_count, failure_excludes, &retention)
    }

    pub(in crate::pipeline) fn unavailable_server_count_from_excludes(
        server_count: usize,
        failure_excludes: &[usize],
        retention_excludes: &[usize],
    ) -> usize {
        failure_excludes
            .iter()
            .filter(|idx| **idx < server_count)
            .count()
            + retention_excludes
                .iter()
                .filter(|idx| **idx < server_count && !failure_excludes.contains(idx))
                .count()
    }

    pub(in crate::pipeline) fn clear_job_retention_excludes(&mut self, job_id: JobId) {
        self.job_retention_exclude_cache.remove(&job_id);
    }

    pub(in crate::pipeline) fn clear_retention_exclude_cache(&mut self) {
        self.job_retention_exclude_cache.clear();
    }
}
