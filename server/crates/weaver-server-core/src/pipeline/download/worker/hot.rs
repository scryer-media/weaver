use super::*;

const MAX_HOT_SUCCESSORS: usize = 2;

impl Pipeline {
    pub(in crate::pipeline::download::worker) fn status_allows_download_dispatch(
        status: &JobStatus,
    ) -> bool {
        matches!(
            status,
            JobStatus::Queued
                | JobStatus::Downloading
                | JobStatus::Checking
                | JobStatus::Verifying
                | JobStatus::QueuedRepair
                | JobStatus::Repairing
                | JobStatus::QueuedExtract
                | JobStatus::Extracting
        )
    }

    pub(in crate::pipeline::download::worker) fn job_has_dispatchable_work(
        &self,
        job_id: JobId,
    ) -> bool {
        self.jobs.get(&job_id).is_some_and(|state| {
            !state.download_queue.is_empty() && Self::status_allows_download_dispatch(&state.status)
        })
    }

    /// Whether the job has critical work still waiting for a lane. An active
    /// critical body continues safely on its own lane, but must not claim the
    /// next newly available one.
    fn job_has_queued_completion_critical_work(&self, job_id: JobId) -> bool {
        self.jobs.get(&job_id).is_some_and(|state| {
            state.download_queue.has_completion_critical_work()
                && Self::status_allows_download_dispatch(&state.status)
        })
    }

    pub(in crate::pipeline::download::worker) fn job_has_active_download_work(
        &self,
        job_id: JobId,
    ) -> bool {
        self.active_downloads_by_job
            .get(&job_id)
            .copied()
            .unwrap_or(0)
            > 0
            || self
                .active_download_connections_by_job
                .get(&job_id)
                .copied()
                .unwrap_or(0)
                > 0
    }

    fn job_can_remain_hot(&self, job_id: JobId) -> bool {
        self.jobs.get(&job_id).is_some_and(|state| {
            Self::status_allows_download_dispatch(&state.status)
                && (!state.download_queue.is_empty() || self.job_has_active_download_work(job_id))
        })
    }

    fn job_rank(&self, job_id: JobId) -> Option<(u8, usize)> {
        let state = self.jobs.get(&job_id)?;
        let index = self.job_order.iter().position(|id| *id == job_id)?;
        Some((Self::job_dispatch_priority(state), index))
    }

    pub(in crate::pipeline::download::worker) fn start_hot_dispatch_period(
        &mut self,
        job_id: JobId,
    ) {
        if self.hot_dispatch_job != Some(job_id) {
            self.hot_dispatch_job = Some(job_id);
            self.hot_dispatch_throughput_window.clear();
        }
    }

    pub(in crate::pipeline::download::worker) fn clear_hot_dispatch_period(&mut self) {
        self.hot_dispatch_job = None;
        self.hot_dispatch_throughput_window.clear();
        self.publish_hot_dispatch_metrics(Instant::now());
    }

    pub(in crate::pipeline::download::worker) fn hot_dispatch_speed_bps(
        &mut self,
        now: Instant,
    ) -> u64 {
        self.hot_dispatch_throughput_window.bps(now)
    }

    pub(in crate::pipeline::download::worker) fn publish_hot_dispatch_metrics(
        &mut self,
        now: Instant,
    ) {
        let hot_job_id = self.hot_dispatch_job.map(|id| id.0).unwrap_or(0);
        let active_non_hot_connections = self
            .active_download_connections_by_job
            .iter()
            .filter(|(job_id, _)| **job_id != self.hot_dispatch_job.unwrap_or(JobId(0)))
            .map(|(_, count)| *count)
            .sum::<usize>();
        let mode = if active_non_hot_connections > 0 {
            DispatchShareMode::Shared
        } else {
            DispatchShareMode::Exclusive
        };

        self.metrics
            .hot_dispatch_job_id
            .store(hot_job_id, Ordering::Relaxed);
        self.metrics
            .hot_dispatch_mode
            .store(mode.as_code(), Ordering::Relaxed);
        let hot_speed_bps = self.hot_dispatch_speed_bps(now);
        self.metrics
            .hot_dispatch_hot_speed_bps
            .store(hot_speed_bps, Ordering::Relaxed);
    }

    pub(in crate::pipeline::download::worker) fn job_dispatch_priority(state: &JobState) -> u8 {
        state
            .spec
            .metadata
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case("priority"))
            .map(|(_, value)| {
                if value.eq_ignore_ascii_case("high") {
                    0
                } else if value.eq_ignore_ascii_case("low") {
                    2
                } else {
                    1
                }
            })
            .unwrap_or(1)
    }

    pub(in crate::pipeline::download::worker) fn select_hot_dispatch_job(
        &mut self,
        eligible: &[(u8, usize, JobId)],
    ) -> Option<JobId> {
        let current = self
            .hot_dispatch_job
            .filter(|job_id| self.job_can_remain_hot(*job_id));
        let critical = eligible
            .iter()
            .copied()
            .find(|(_, _, job_id)| self.job_has_queued_completion_critical_work(*job_id));
        let current_critical =
            current.filter(|job_id| self.job_has_queued_completion_critical_work(*job_id));

        let selected = match (critical, current_critical) {
            (Some((priority, index, candidate)), Some(current)) => {
                if self
                    .job_rank(current)
                    .is_some_and(|current_rank| current_rank <= (priority, index))
                {
                    current
                } else {
                    candidate
                }
            }
            (Some((_, _, candidate)), None) => candidate,
            (None, Some(current)) => {
                let Some((priority, index, candidate)) = eligible.first().copied() else {
                    return Some(current);
                };
                if self
                    .job_rank(current)
                    .is_some_and(|current_rank| current_rank <= (priority, index))
                {
                    current
                } else {
                    candidate
                }
            }
            (None, None) => {
                let Some((priority, index, candidate)) = eligible.first().copied() else {
                    return current;
                };
                current
                    .filter(|current| {
                        self.job_rank(*current)
                            .is_some_and(|current_rank| current_rank <= (priority, index))
                    })
                    .unwrap_or(candidate)
            }
        };
        self.start_hot_dispatch_period(selected);
        Some(selected)
    }

    pub(in crate::pipeline::download::worker) fn hot_dispatch_successors(
        eligible: &[(u8, usize, JobId)],
        hot_job_id: JobId,
    ) -> impl Iterator<Item = JobId> + '_ {
        eligible
            .iter()
            .map(|(_, _, job_id)| *job_id)
            .filter(move |job_id| *job_id != hot_job_id)
            .take(MAX_HOT_SUCCESSORS)
    }

    pub(in crate::pipeline::download::worker) fn can_dispatch_successor(
        &self,
        hot_job_id: JobId,
        successor_job_id: JobId,
    ) -> bool {
        if self
            .active_download_connections_by_job
            .contains_key(&successor_job_id)
        {
            return true;
        }
        self.active_download_connections_by_job
            .keys()
            .filter(|job_id| **job_id != hot_job_id)
            .count()
            < MAX_HOT_SUCCESSORS
    }
}
