use super::*;

impl Pipeline {
    pub(in crate::pipeline) fn next_download_lane_id() -> u64 {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        NEXT.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .expect("download lane identity exhausted")
    }

    pub(in crate::pipeline) fn download_lane_is_live(&self, lane_id: u64) -> bool {
        // Standalone fixtures can inject outcomes without starting a transport.
        #[cfg(test)]
        if lane_id == 0 {
            return true;
        }
        self.download_lane_owners.contains_key(&lane_id)
    }

    pub(super) fn book_download_lane_owner(
        &mut self,
        lease: &DownloadBatchLease,
        connection: bool,
    ) {
        let owner = self
            .download_lane_owners
            .entry(lease.lane_id)
            .or_insert_with(|| DownloadLaneOwner {
                job_id: lease.job_id,
                mode: lease.lane_mode,
                spillover_loan_kind: lease.spillover_loan_kind,
                completion_critical: lease.compatibility.completion_critical,
                connection,
                ip_replacement: !connection,
                outstanding: HashMap::new(),
            });
        owner.mode = lease.lane_mode;
        owner.completion_critical = lease.compatibility.completion_critical;
        owner.outstanding.extend(
            lease
                .works
                .iter()
                .cloned()
                .map(|work| (work.segment_id, work)),
        );
    }

    pub(super) fn accept_lane_work(&mut self, lane_id: u64, segment: SegmentId) -> bool {
        #[cfg(test)]
        if lane_id == 0 {
            return true;
        }
        let Some(owner) = self.download_lane_owners.get_mut(&lane_id) else {
            return false;
        };
        let accepted = owner.outstanding.remove(&segment).is_some();
        if owner.outstanding.is_empty() && !owner.connection && !owner.ip_replacement {
            self.download_lane_owners.remove(&lane_id);
        }
        accepted
    }

    pub(in crate::pipeline) fn retire_stalled_download_lanes(&mut self, job_id: JobId) -> usize {
        let ids: Vec<_> = self
            .download_lane_owners
            .iter()
            .filter_map(|(id, owner)| (owner.job_id == job_id).then_some(*id))
            .collect();
        let mut returned = 0;
        for lane_id in ids {
            let works = self
                .download_lane_owners
                .get_mut(&lane_id)
                .map(|owner| std::mem::take(&mut owner.outstanding))
                .unwrap_or_default();
            returned += works.len();
            for work in works.into_values() {
                self.restore_owned_lane_unrequested_work(lane_id, work);
            }
            let owner = &self.download_lane_owners[&lane_id];
            self.handle_download_lane_parked(DownloadLaneParked {
                lane_id,
                job_id,
                mode: owner.mode,
                spillover_loan_kind: owner.spillover_loan_kind,
                completion_critical: owner.completion_critical,
                reason: LaneParkReason::Error,
                release_connection_slot: owner.connection,
                release_ip_replacement_burst: owner.ip_replacement,
            });
        }
        self.download_restart_durable_lead_retry_after
            .remove(&job_id);
        returned
    }
}
