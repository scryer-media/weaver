use super::*;
use crate::jobs::support_facts::{GapKind, GapPosition};

fn gap_kind(state: SegmentTerminalState) -> GapKind {
    match state {
        SegmentTerminalState::Missing => GapKind::Missing,
        SegmentTerminalState::RetriesExhausted | SegmentTerminalState::DecodeExhausted => {
            GapKind::Failed
        }
    }
}

fn gap_position(segment_id: SegmentId) -> GapPosition {
    GapPosition(segment_id.file_id.file_index, segment_id.segment_number)
}

impl Pipeline {
    /// A set's demotion, recorded with the job's status at the time.
    pub(super) fn note_demotion_support_fact(&mut self, job_id: JobId, reason: &'static str) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_secs());
        if let Some(state) = self.jobs.get_mut(&job_id) {
            let stage = state.status.persisted_status();
            state.support_facts.note_demotion(reason, stage, now);
            self.dirty_support_facts.insert(job_id);
        }
    }

    /// A segment's pending-to-terminal edge.
    pub(super) fn note_gap_support_fact(
        &mut self,
        segment_id: SegmentId,
        state: SegmentTerminalState,
    ) {
        let job_id = segment_id.file_id.job_id;
        if let Some(job) = self.jobs.get_mut(&job_id) {
            job.support_facts
                .note_gap(gap_kind(state), gap_position(segment_id));
            self.dirty_support_facts.insert(job_id);
        }
    }

    /// The servers that refused a gap just booked, by pool index. An index the
    /// pool cannot name durably is left out rather than guessed.
    pub(super) fn note_gap_servers_support_fact(
        &mut self,
        job_id: JobId,
        server_indices: &[usize],
    ) {
        if server_indices.is_empty() {
            return;
        }
        let server_ids: Vec<u32> = server_indices
            .iter()
            .filter_map(|idx| {
                self.nntp
                    .pool()
                    .stable_server_id(weaver_nntp::pool::ServerId(*idx))
                    .map(|stable| stable.0)
            })
            .collect();
        if let Some(job) = self.jobs.get_mut(&job_id) {
            job.support_facts.note_gap_servers(server_ids);
            self.dirty_support_facts.insert(job_id);
        }
    }

    /// A gap a verified late replacement filled.
    pub(super) fn forget_gap_support_fact(
        &mut self,
        segment_id: SegmentId,
        state: SegmentTerminalState,
    ) {
        let job_id = segment_id.file_id.job_id;
        if let Some(job) = self.jobs.get_mut(&job_id) {
            job.support_facts
                .forget_gap(gap_kind(state), gap_position(segment_id));
            self.dirty_support_facts.insert(job_id);
        }
    }

    /// The job's terminal segment states were dropped; its gaps go with them.
    pub(super) fn clear_gap_support_facts(&mut self, job_id: JobId) {
        if let Some(job) = self.jobs.get_mut(&job_id)
            && !job.support_facts.gaps.is_empty()
        {
            job.support_facts.clear_gaps();
            self.dirty_support_facts.insert(job_id);
        }
    }

    /// Checkpoint changed support facts onto the active rows, on the same
    /// cadence and writer lane as the server attribution: checkpoints keep
    /// their order, and a job's archive waits for the ones queued before it.
    pub(crate) fn flush_support_facts(&mut self) {
        if self.dirty_support_facts.is_empty() {
            return;
        }
        let snapshots: Vec<_> = self
            .dirty_support_facts
            .iter()
            .filter_map(|id| Some((*id, self.jobs.get(id)?.support_facts.to_storage_json())))
            .collect();
        if snapshots.is_empty() {
            self.dirty_support_facts.clear();
            return;
        }
        match self
            .db
            .try_queue_server_attribution_write(move |db| db.save_active_support_facts(snapshots))
        {
            Ok(_) => self.dirty_support_facts.clear(),
            Err(error) => tracing::error!(%error, "failed to queue support facts checkpoint"),
        }
    }

    /// Write one finishing job's facts ahead of its history archive, which
    /// copies them from the active row. Queued as one of the job's own writes,
    /// so it lands before the archive queued after it.
    pub(super) fn persist_support_facts_before_archive(&mut self, job_id: JobId) {
        self.dirty_support_facts.remove(&job_id);
        let Some(json) = self
            .jobs
            .get(&job_id)
            .and_then(|state| state.support_facts.to_storage_json())
        else {
            return;
        };
        if let Err(error) = self
            .db
            .try_queue_job_write(job_id, "active_support_facts", move |db| {
                db.save_active_support_facts(vec![(job_id, Some(json))])
            })
        {
            tracing::error!(%error, job_id = job_id.0, "failed to queue support facts before archive");
        }
    }
}
