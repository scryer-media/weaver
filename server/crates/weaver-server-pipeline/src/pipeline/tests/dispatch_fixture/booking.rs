// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

impl DispatchFixture {
    pub fn set_retention(&mut self, days: &[u32]) {
        self.0.nntp = Arc::new(retention_client(days));
    }

    pub fn set_job_age(&mut self, id: JobId, days: u64) {
        age_job_days(&mut self.0, id, days);
        self.0.clear_job_retention_excludes(id);
    }

    pub fn start_recording_spent_ids(&mut self) {
        self.0.spent_message_ids = Some(Vec::new());
    }

    pub fn spent_ids(&self) -> &[String] {
        self.0
            .spent_message_ids
            .as_deref()
            .expect("recording spent IDs")
    }

    pub fn booking_depth(&self) -> usize {
        self.0.unservable_booking_depth.1
    }

    pub fn booked_missing(&self, id: SegmentId) -> bool {
        self.0.segment_terminal_states.get(&id) == Some(&SegmentTerminalState::Missing)
    }

    pub fn job_if_present(&self, id: JobId) -> Option<&JobState> {
        self.0.jobs.get(&id)
    }

    pub fn open_pass(&mut self, id: JobId) {
        self.0.active_download_passes.insert(id);
    }

    pub fn pass_open(&self, id: JobId) -> bool {
        self.0.active_download_passes.contains(&id)
    }

    pub fn completion_check_pending(&self, id: JobId) -> bool {
        self.0.pending_completion_checks.contains(&id)
    }

    pub fn retry_work(&self, id: SegmentId) -> DownloadWork {
        self.0
            .download_retry_work(id, 0, weaver_nntp::ServerSet::EMPTY)
            .unwrap()
    }

    pub fn enqueue(&mut self, work: DownloadWork) {
        self.0.enqueue_download_work(work);
    }

    pub fn retire_unservable(&mut self, id: JobId) -> usize {
        self.0.retire_unservable_queued_work(id)
    }
}
