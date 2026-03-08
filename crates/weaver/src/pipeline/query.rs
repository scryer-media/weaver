use super::*;

impl Pipeline {
    /// Set a job's status.
    pub(super) fn set_job_status(
        &mut self,
        job_id: JobId,
        status: JobStatus,
    ) -> Result<(), weaver_scheduler::SchedulerError> {
        match self.jobs.get_mut(&job_id) {
            Some(state) => {
                state.status = status;
                Ok(())
            }
            None => Err(weaver_scheduler::SchedulerError::JobNotFound(job_id)),
        }
    }

    /// Promote recovery segments for a job from its recovery queue to its primary
    /// queue at high priority. Called when damage is detected and repair blocks
    /// are needed immediately.
    pub(super) fn promote_recovery(&mut self, job_id: JobId) {
        let Some(state) = self.jobs.get_mut(&job_id) else { return };
        let mut promoted = 0usize;
        while let Some(mut work) = state.recovery_queue.pop() {
            work.priority = 2; // Just below PAR2 index (0) and first RAR (1)
            state.download_queue.push(work);
            promoted += 1;
        }
        if promoted > 0 {
            info!(
                job_id = job_id.0,
                promoted,
                "promoted recovery segments to primary queue"
            );
        }
    }

    /// Get info about a job.
    pub(super) fn get_job_info(&self, job_id: JobId) -> Result<JobInfo, weaver_scheduler::SchedulerError> {
        match self.jobs.get(&job_id) {
            Some(state) => {
                let total = state.spec.total_bytes;
                let health = if total == 0 {
                    1000
                } else {
                    ((total.saturating_sub(state.failed_bytes)) * 1000 / total) as u32
                };
                Ok(JobInfo {
                    job_id,
                    name: state.spec.name.clone(),
                    status: state.status.clone(),
                    progress: state.assembly.progress(),
                    total_bytes: total,
                    downloaded_bytes: state.downloaded_bytes,
                    failed_bytes: state.failed_bytes,
                    health,
                    password: state.spec.password.clone(),
                    category: state.spec.category.clone(),
                    metadata: state.spec.metadata.clone(),
                    output_dir: Some(state.working_dir.display().to_string()),
                })
            }
            None => {
                // Check finished jobs (recovered history).
                if let Some(info) = self.finished_jobs.iter().find(|j| j.job_id == job_id) {
                    return Ok(info.clone());
                }
                Err(weaver_scheduler::SchedulerError::JobNotFound(job_id))
            }
        }
    }

    /// List all jobs.
    pub(super) fn list_jobs(&self) -> Vec<JobInfo> {
        let mut list: Vec<JobInfo> = self.jobs
            .values()
            .map(|state| {
                let total = state.spec.total_bytes;
                let health = if total == 0 {
                    1000
                } else {
                    ((total.saturating_sub(state.failed_bytes)) * 1000 / total) as u32
                };
                JobInfo {
                    job_id: state.job_id,
                    name: state.spec.name.clone(),
                    status: state.status.clone(),
                    progress: state.assembly.progress(),
                    total_bytes: total,
                    downloaded_bytes: state.downloaded_bytes,
                    failed_bytes: state.failed_bytes,
                    health,
                    password: state.spec.password.clone(),
                    category: state.spec.category.clone(),
                    metadata: state.spec.metadata.clone(),
                    output_dir: Some(state.working_dir.display().to_string()),
                }
            })
            .collect();
        list.extend(self.finished_jobs.iter().cloned());
        list
    }
}
