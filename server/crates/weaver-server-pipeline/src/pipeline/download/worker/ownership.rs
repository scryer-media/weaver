// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

// What an article the post gives no size is reserved at
// (`Pipeline::reserved_bytes_of`).
const UNSIZED_ARTICLE_BYTES: u64 = 1 << 20;

impl Pipeline {
    pub fn next_download_lane_id() -> u64 {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        NEXT.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .expect("download lane identity exhausted")
    }

    pub(in crate::pipeline) fn download_lane_is_live(&self, lane_id: u64) -> bool {
        // Standalone fixtures can inject outcomes without starting a transport.
        #[cfg(any(test, feature = "pipeline-test-fixtures"))]
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
                body_activity: Vec::new(),
                job_id: lease.job_id,
                mode: lease.lane_mode,
                completion_critical: lease.completion_critical,
                server_idx: None,
                connection,
                outstanding: HashMap::new(),
                outstanding_bytes: 0,
            });
        owner.mode = lease.lane_mode;
        owner
            .body_activity
            .retain(|(_, _, bytes)| bytes.strong_count() != 0);
        owner.body_activity.push((
            lease.job_id,
            lease.runtime_generation,
            Arc::downgrade(&lease.body_activity),
        ));
        owner.completion_critical = lease.completion_critical;
        self.hold_lane_works(lease.lane_id, lease.works.iter().cloned());
    }

    // Record that a live lane now holds `works`.
    //
    // This and [`Self::accept_lane_work`] are the only writers of the
    // in-flight gauges (`articles_in_flight`, `active_recovery`,
    // `articles_in_flight_by_job`, `articles_in_flight_by_file`): an article
    // raises them when it enters a lane's `outstanding` record and lowers them
    // when it leaves, so each gauge is the count of what the live lanes hold
    // and cannot drift from it. A lease that names an article the lane already
    // holds adds nothing, because the lane owes one result for it, not two.
    //
    // Each work comes reserved on its job's volume (`reserve_lane_work`).
    // The record gives the reservation back as the article leaves it
    // (`note_lane_work_left`); one it does not take, the one a duplicate
    // replaces or all of them on a lane that is gone, is given back here.
    fn hold_lane_works(&mut self, lane_id: u64, works: impl IntoIterator<Item = DownloadWork>) {
        let Some(owner) = self.download_lane_owners.get_mut(&lane_id) else {
            for work in works {
                self.release_lane_work(&work);
            }
            return;
        };
        let mut entered = Vec::new();
        let mut replaced_works = Vec::new();
        for work in works {
            let segment_id = work.segment_id;
            let is_recovery = work.is_recovery;
            owner.outstanding_bytes += u64::from(work.byte_estimate);
            match owner.outstanding.insert(segment_id, work) {
                None => entered.push((segment_id, is_recovery)),
                Some(replaced) => {
                    owner.outstanding_bytes -= u64::from(replaced.byte_estimate);
                    replaced_works.push(replaced);
                }
            }
        }
        for replaced in replaced_works {
            self.release_lane_work(&replaced);
        }
        for (segment_id, is_recovery) in entered {
            self.articles_in_flight += 1;
            if is_recovery {
                self.active_recovery += 1;
            }
            *self
                .articles_in_flight_by_job
                .entry(segment_id.file_id.job_id)
                .or_default() += 1;
            *self
                .articles_in_flight_by_file
                .entry(segment_id.file_id)
                .or_default() += 1;
        }
    }

    // Standalone fixtures inject results from lane 0 without starting a
    // transport; this books what such a result releases, through the same
    // record a dispatched lease writes.
    #[cfg(all(feature = "pipeline-test-fixtures", not(test)))]
    pub(in crate::pipeline) fn hold_test_lane_works(
        &mut self,
        works: Vec<DownloadWork>,
        connection: bool,
    ) {
        let Some(first) = works.first() else {
            return;
        };
        let job_id = first.segment_id.file_id.job_id;
        let completion_critical = works.iter().any(|work| work.completion_critical);
        // Booked as a lease books them: reserved, whatever room is left.
        for work in &works {
            self.reserve_test_lane_work(work);
        }
        self.download_lane_owners
            .entry(0)
            .or_insert_with(|| DownloadLaneOwner {
                body_activity: Vec::new(),
                job_id,
                mode: DownloadLaneMode::Sequential,
                completion_critical,
                server_idx: None,
                connection,
                outstanding: HashMap::new(),
                outstanding_bytes: 0,
            });
        self.hold_lane_works(0, works);
    }

    // Every live lane answers what it holds and parks with nothing left to
    // do, as the lane tasks would report once their batches were done.
    #[cfg(all(feature = "pipeline-test-fixtures", not(test)))]
    pub(in crate::pipeline) fn finish_test_lanes(&mut self) {
        let lanes: Vec<(u64, Vec<SegmentId>)> = self
            .download_lane_owners
            .iter()
            .map(|(&lane_id, owner)| (lane_id, owner.outstanding.keys().copied().collect()))
            .collect();
        for (lane_id, segments) in lanes {
            let Some(owner) = self.download_lane_owners.get(&lane_id) else {
                continue;
            };
            let parked = DownloadLaneParked {
                lane_id,
                job_id: owner.job_id,
                mode: owner.mode,
                completion_critical: owner.completion_critical,
                reason: LaneParkReason::NoWork,
                release_connection_slot: owner.connection,
            };
            for segment in segments {
                self.accept_lane_work(lane_id, segment);
            }
            self.handle_download_lane_parked(parked);
        }
    }

    // The bytes an article is reserved at on its job's volume: its size in
    // the post, which is never less than what arrives for it. An article
    // the post gives no size is reserved at a whole article's worth.
    pub(in crate::pipeline) fn reserved_bytes_of(byte_estimate: u32) -> u64 {
        match byte_estimate {
            0 => UNSIZED_ARTICLE_BYTES,
            bytes => u64::from(bytes),
        }
    }

    // The volume the job's articles are written to, when it writes through
    // a writer.
    fn article_volume(
        &self,
        job_id: JobId,
    ) -> Option<Arc<weaver_server_disk::operations::disk::volume::Volume>> {
        if let Some(reserved) = self.volume_reserved_by_job.get(&job_id) {
            return Some(Arc::clone(reserved.volume()));
        }
        self.graph_jobs
            .get(&job_id)
            .and_then(|graph| graph.disk())
            .map(|disk| Arc::clone(disk.working().volume()))
    }

    // Reserve `work` on the volume its job writes to, as it is leased:
    // false when it does not fit below the lease limit there
    // (`VolumeLeases::lease_limit`). A job that writes through no writer
    // reserves nothing.
    pub(in crate::pipeline) fn reserve_lane_work(&mut self, work: &DownloadWork) -> bool {
        let job_id = work.segment_id.file_id.job_id;
        let bytes = Self::reserved_bytes_of(work.byte_estimate);
        if let Some(reserved) = self.volume_reserved_by_job.get_mut(&job_id) {
            let share = reserved.volume().loop_share();
            return reserved.reserve(bytes, share);
        }
        let Some(volume) = self.article_volume(job_id) else {
            return true;
        };
        let share = volume.loop_share();
        let mut reserved = Reservation::none(volume);
        if !reserved.reserve(bytes, share) {
            return false;
        }
        self.volume_reserved_by_job.insert(job_id, reserved);
        true
    }

    // Standalone fixtures book articles no lease reserved: they are
    // reserved here, whatever room the volume has, so what they release
    // was counted.
    #[cfg(all(feature = "pipeline-test-fixtures", not(test)))]
    pub(in crate::pipeline) fn reserve_test_lane_work(&mut self, work: &DownloadWork) {
        let job_id = work.segment_id.file_id.job_id;
        let Some(volume) = self.article_volume(job_id) else {
            return;
        };
        let bytes = Self::reserved_bytes_of(work.byte_estimate);
        self.volume_reserved_by_job
            .entry(job_id)
            .or_insert_with(|| Reservation::none(volume))
            .count(bytes);
    }

    // Give back the reservation of `work`, which left its lane or was
    // never booked on one.
    pub(in crate::pipeline) fn release_lane_work(&mut self, work: &DownloadWork) {
        self.settle_lane_work(work, 0);
    }

    // Give back the reservation of `work`, which left its lane with
    // `arrived` bytes of it: those count from here as arrived, in the same
    // step (`Reservation::settle`), until the loop hands them on. Whoever
    // asked to be woken at the line the volume has now reached is woken.
    fn settle_lane_work(&mut self, work: &DownloadWork, arrived: u64) {
        let job_id = work.segment_id.file_id.job_id;
        let Some(reserved) = self.volume_reserved_by_job.get_mut(&job_id) else {
            if arrived != 0 {
                self.note_volume_arrived(job_id, arrived);
            }
            return;
        };
        let bytes = Self::reserved_bytes_of(work.byte_estimate).min(reserved.bytes());
        let mut part = reserved.split(bytes);
        if reserved.bytes() == 0 {
            self.volume_reserved_by_job.remove(&job_id);
        }
        let woke = if arrived != 0 {
            self.hold_volume_arrived(job_id, part, arrived, true)
        } else {
            part.give_back(bytes);
            self.volume_writers.take_wake(part.volume())
        };
        if woke {
            self.volume_room_changed();
        }
    }

    // The other half of [`Self::hold_lane_works`]: `work` has left the lane
    // that held it, and its reservation with it: `arrived` bytes of it,
    // the article's result, count on as arrived (`settle_lane_work`).
    fn note_lane_work_left(&mut self, work: &DownloadWork, arrived: u64) {
        self.settle_lane_work(work, arrived);
        let segment_id = work.segment_id;
        self.articles_in_flight = self.articles_in_flight.saturating_sub(1);
        if work.is_recovery {
            self.active_recovery = self.active_recovery.saturating_sub(1);
        }
        let job_id = segment_id.file_id.job_id;
        if let Some(in_flight) = self.articles_in_flight_by_job.get_mut(&job_id) {
            *in_flight = in_flight.saturating_sub(1);
            if *in_flight == 0 {
                self.articles_in_flight_by_job.remove(&job_id);
            }
        }
        if let Some(in_flight) = self.articles_in_flight_by_file.get_mut(&segment_id.file_id) {
            *in_flight = in_flight.saturating_sub(1);
            if *in_flight == 0 {
                self.articles_in_flight_by_file.remove(&segment_id.file_id);
            }
        }
    }

    // Re-book a live lane for the batch it was just granted: its job, the
    // server it turned out to be on, the class and depth it now runs, and the
    // articles it owes results for. Moves the per-job connection gauges when
    // the job or the class changed, so what a park releases is what is
    // booked here.
    pub(in crate::pipeline) fn rebook_download_lane_owner(
        &mut self,
        lane_id: u64,
        job_id: JobId,
        server_idx: usize,
        mode: DownloadLaneMode,
        completion_critical: bool,
        works: Vec<DownloadWork>,
    ) {
        let Some(owner) = self.download_lane_owners.get_mut(&lane_id) else {
            for work in &works {
                self.release_lane_work(work);
            }
            return;
        };
        let previous_job = owner.job_id;
        let previous_critical = owner.completion_critical;
        let connection = owner.connection;
        owner.job_id = job_id;
        owner.server_idx = Some(server_idx);
        owner.mode = mode;
        owner.completion_critical = completion_critical;
        self.hold_lane_works(lane_id, works);
        if !connection {
            return;
        }
        if previous_job != job_id {
            if let Some(in_flight) = self
                .active_download_connections_by_job
                .get_mut(&previous_job)
            {
                *in_flight = in_flight.saturating_sub(1);
                if *in_flight == 0 {
                    self.active_download_connections_by_job
                        .remove(&previous_job);
                }
            }
            *self
                .active_download_connections_by_job
                .entry(job_id)
                .or_default() += 1;
        }
        if (previous_job, previous_critical) != (job_id, completion_critical) {
            if previous_critical {
                self.release_completion_critical_connection(previous_job);
            }
            if completion_critical {
                self.book_completion_critical_connection(job_id);
            }
        }
    }

    pub(in crate::pipeline) fn book_completion_critical_connection(&mut self, job_id: JobId) {
        self.active_completion_critical_connections += 1;
        *self
            .active_completion_critical_connections_by_job
            .entry(job_id)
            .or_default() += 1;
    }

    pub(in crate::pipeline) fn release_completion_critical_connection(&mut self, job_id: JobId) {
        self.active_completion_critical_connections = self
            .active_completion_critical_connections
            .saturating_sub(1);
        if let Some(in_flight) = self
            .active_completion_critical_connections_by_job
            .get_mut(&job_id)
        {
            *in_flight = in_flight.saturating_sub(1);
            if *in_flight == 0 {
                self.active_completion_critical_connections_by_job
                    .remove(&job_id);
            }
        }
    }

    // Take `segment` back from the lane that holds it, lowering the in-flight
    // gauges once. `false` when the lane does not hold it — it was retired,
    // reset or never handed this article, or already gave it back — and then
    // nothing is lowered, because whatever took the article off the lane's
    // record lowered them already. A result comes back through
    // `accept_lane_result`; this is for fixtures that end lanes whole.
    #[cfg(all(feature = "pipeline-test-fixtures", not(test)))]
    pub(super) fn accept_lane_work(&mut self, lane_id: u64, segment: SegmentId) -> bool {
        self.accept_lane_work_with(lane_id, segment, 0, |_| {})
    }

    // Take back the article whose result came back with `arrived` bytes,
    // as `accept_lane_work` does: what arrived stays counted on its volume
    // in place of the article's reservation.
    pub(in crate::pipeline) fn accept_lane_result(
        &mut self,
        lane_id: u64,
        segment: SegmentId,
        arrived: u64,
    ) -> bool {
        self.accept_lane_work_with(lane_id, segment, arrived, |_| {})
    }

    // Take back an article a lane returns unasked, as `accept_lane_work`
    // does, with the tried servers and hint from the lane's record rather
    // than the lease's copy: the record is carried across a pool rebuild,
    // the copy a lane holds is numbered by the pool that leased it.
    pub(super) fn accept_returned_lane_work(
        &mut self,
        lane_id: u64,
        work: &mut DownloadWork,
    ) -> bool {
        let segment = work.segment_id;
        self.accept_lane_work_with(lane_id, segment, 0, |held| {
            work.exclude_servers = held.exclude_servers;
            work.avoid_server = held.avoid_server;
        })
    }

    // Carry the tried servers and hint of every article a lane holds into
    // a rebuilt pool. One pass over the held articles per rebuild.
    pub(in crate::pipeline) fn remap_lane_holdings(&mut self, remap: &weaver_nntp::ServerRemap) {
        for owner in self.download_lane_owners.values_mut() {
            for work in owner.outstanding.values_mut() {
                work.exclude_servers = remap.set(work.exclude_servers);
                work.avoid_server = work.avoid_server.and_then(|server| remap.index(server));
            }
        }
    }

    fn accept_lane_work_with(
        &mut self,
        lane_id: u64,
        segment: SegmentId,
        arrived: u64,
        held: impl FnOnce(&DownloadWork),
    ) -> bool {
        let Some(owner) = self.download_lane_owners.get_mut(&lane_id) else {
            // Standalone fixtures inject outcomes from lane 0 without
            // booking it; such an outcome was never counted.
            #[cfg(any(test, feature = "pipeline-test-fixtures"))]
            if lane_id == 0 {
                if arrived != 0 {
                    self.note_volume_arrived(segment.file_id.job_id, arrived);
                }
                return true;
            }
            return false;
        };
        let work = owner.outstanding.remove(&segment);
        if let Some(work) = &work {
            owner.outstanding_bytes -= u64::from(work.byte_estimate);
        }
        if owner.outstanding.is_empty() && !owner.connection {
            self.download_lane_owners.remove(&lane_id);
        }
        let Some(work) = work else {
            return false;
        };
        held(&work);
        self.note_lane_work_left(&work, arrived);
        true
    }

    // Take back every article `lane_id` holds that `take` names, lowering the
    // gauges for each, as [`Self::accept_lane_work`] does one at a time.
    fn take_lane_works(
        &mut self,
        lane_id: u64,
        mut take: impl FnMut(&SegmentId) -> bool,
    ) -> Vec<DownloadWork> {
        let Some(owner) = self.download_lane_owners.get_mut(&lane_id) else {
            return Vec::new();
        };
        let works: Vec<DownloadWork> = owner
            .outstanding
            .extract_if(|segment, _| take(segment))
            .map(|(_, work)| work)
            .collect();
        owner.outstanding_bytes -= works
            .iter()
            .map(|work| u64::from(work.byte_estimate))
            .sum::<u64>();
        if owner.outstanding.is_empty() && !owner.connection {
            self.download_lane_owners.remove(&lane_id);
        }
        for work in &works {
            self.note_lane_work_left(work, 0);
        }
        works
    }

    // Take back every article of `job_id` a lane holds, and tear down every
    // lane still fetching for it. A lane booked to another job that still
    // carries this job's tail keeps running for its own job; only this job's
    // articles come off its record, and their late results are refused. A
    // lane the job owns only on paper, with nothing in flight, is a hot
    // worker between leases: its next refill is answered from whichever job
    // the scheduler names, so it stays up, cached connection and all, rather
    // than dying with the job it last served.
    pub(in crate::pipeline) fn retire_stalled_download_lanes(&mut self, job_id: JobId) -> usize {
        // A hot lane can survive this attempt. Its old response contexts
        // must not refresh the watchdog after this job is resumed or retried.
        for owner in self.download_lane_owners.values_mut() {
            owner.body_activity.retain(|(job, _, _)| *job != job_id);
        }
        let ids: Vec<_> = self
            .download_lane_owners
            .iter()
            .filter_map(|(id, owner)| {
                let booked = owner.job_id == job_id && !owner.outstanding.is_empty();
                let carries = owner
                    .outstanding
                    .keys()
                    .any(|segment| segment.file_id.job_id == job_id);
                (booked || carries).then_some((*id, booked))
            })
            .collect();
        let mut returned = 0;
        for (lane_id, booked) in ids {
            let works = if booked {
                self.take_lane_works(lane_id, |_| true)
            } else {
                self.take_lane_works(lane_id, |segment| segment.file_id.job_id == job_id)
            };
            returned += works.len();
            for work in works {
                self.restore_owned_lane_unrequested_work(lane_id, work);
            }
            if !booked {
                continue;
            }
            let Some(owner) = self.download_lane_owners.get(&lane_id) else {
                continue;
            };
            self.handle_download_lane_parked(DownloadLaneParked {
                lane_id,
                job_id,
                mode: owner.mode,
                completion_critical: owner.completion_critical,
                reason: LaneParkReason::Error,
                release_connection_slot: owner.connection,
            });
        }
        self.download_restart_durable_lead_retry_after
            .remove(&job_id);
        returned
    }

    // What the in-flight gauges should read, counted from the lane records,
    // set against what they do read. Empty when they agree.
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub(in crate::pipeline) fn lane_holdings_discrepancies(&self) -> Vec<String> {
        let mut total = 0usize;
        let mut recovery = 0usize;
        let mut by_job: HashMap<JobId, usize> = HashMap::new();
        let mut by_file: HashMap<NzbFileId, usize> = HashMap::new();
        let mut connections_by_job: HashMap<JobId, usize> = HashMap::new();
        let mut critical_by_job: HashMap<JobId, usize> = HashMap::new();
        for owner in self.download_lane_owners.values() {
            for (segment, work) in &owner.outstanding {
                total += 1;
                recovery += usize::from(work.is_recovery);
                *by_job.entry(segment.file_id.job_id).or_default() += 1;
                *by_file.entry(segment.file_id).or_default() += 1;
            }
            if owner.connection {
                *connections_by_job.entry(owner.job_id).or_default() += 1;
                if owner.completion_critical {
                    *critical_by_job.entry(owner.job_id).or_default() += 1;
                }
            }
        }
        let mut discrepancies = Vec::new();
        if self.articles_in_flight != total {
            discrepancies.push(format!(
                "articles_in_flight = {}, lanes hold {total}",
                self.articles_in_flight
            ));
        }
        if self.active_recovery != recovery {
            discrepancies.push(format!(
                "active_recovery = {}, lanes hold {recovery}",
                self.active_recovery
            ));
        }
        let nonzero = |map: &HashMap<JobId, usize>| -> HashMap<JobId, usize> {
            map.iter()
                .filter(|(_, count)| **count != 0)
                .map(|(job_id, count)| (*job_id, *count))
                .collect()
        };
        if nonzero(&self.articles_in_flight_by_job) != by_job {
            discrepancies.push(format!(
                "articles_in_flight_by_job = {:?}, lanes hold {by_job:?}",
                self.articles_in_flight_by_job
            ));
        }
        let files: HashMap<NzbFileId, usize> = self
            .articles_in_flight_by_file
            .iter()
            .filter(|(_, count)| **count != 0)
            .map(|(file_id, count)| (*file_id, *count))
            .collect();
        if files != by_file {
            discrepancies.push(format!(
                "articles_in_flight_by_file = {:?}, lanes hold {by_file:?}",
                self.articles_in_flight_by_file
            ));
        }
        // The connection gauges of a job that is gone are dropped with it,
        // while a lane between leases may still be booked to it.
        let live = |map: HashMap<JobId, usize>| -> HashMap<JobId, usize> {
            map.into_iter()
                .filter(|(job_id, _)| self.jobs.contains_key(job_id))
                .collect()
        };
        let booked_connections = live(nonzero(&self.active_download_connections_by_job));
        let connections_by_job = live(connections_by_job);
        if booked_connections != connections_by_job {
            discrepancies.push(format!(
                "active_download_connections_by_job = {booked_connections:?}, lanes hold {connections_by_job:?}"
            ));
        }
        let booked_critical = live(nonzero(&self.active_completion_critical_connections_by_job));
        let critical_by_job = live(critical_by_job);
        if booked_critical != critical_by_job {
            discrepancies.push(format!(
                "active_completion_critical_connections_by_job = {booked_critical:?}, lanes hold {critical_by_job:?}"
            ));
        }
        discrepancies
    }

    // Fail the test when the in-flight gauges disagree with the lane records.
    // Called after every dispatch pass and every lane result batch; a no-op
    // unless the fixture asked for it.
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub(in crate::pipeline) fn audit_lane_holdings(&self) {
        if !self.audit_lane_holdings {
            return;
        }
        let discrepancies = self.lane_holdings_discrepancies();
        assert!(
            discrepancies.is_empty(),
            "in-flight gauges disagree with what the live lanes hold: {discrepancies:#?}"
        );
    }
}
