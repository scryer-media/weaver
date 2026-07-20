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
