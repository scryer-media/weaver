// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Where an article's bytes count: on the volume they land on, from the
// lease to the step that hands them on. A job whose working directory and
// delivered files lie on two volumes counts each article on the one its
// bytes are written to, so a full output volume stops dispatch for itself
// and never holds the loop up in a write.

use super::*;
use weaver_graph::MemberId;
use weaver_server_disk::operations::disk::volume::Volume;

// One job's bytes counted on the volumes it writes to: a reservation for
// each volume that counts some of them, found by the volume itself. A job
// writes to at most two (its working and its output volume); a volume it
// no longer writes to keeps what it counted until those bytes move on,
// where they fit, or are handed on.
#[derive(Debug, Default)]
pub struct VolumeHolds(Vec<Reservation>);

impl VolumeHolds {
    // Every byte the job counts, on all its volumes.
    pub fn bytes(&self) -> u64 {
        self.0.iter().map(Reservation::bytes).sum()
    }

    // The bytes the job counts on `volume`.
    pub fn on(&self, volume: &Arc<Volume>) -> u64 {
        self.0
            .iter()
            .find(|held| Arc::ptr_eq(held.volume(), volume))
            .map_or(0, Reservation::bytes)
    }

    // What the job holds on `volume`, made empty when it holds nothing
    // there. A volume left holding nothing makes way for it.
    pub(super) fn entry(&mut self, volume: &Arc<Volume>) -> &mut Reservation {
        let at = match self
            .0
            .iter()
            .position(|held| Arc::ptr_eq(held.volume(), volume))
        {
            Some(at) => at,
            None => {
                self.0.retain(|held| held.bytes() != 0);
                self.0.push(Reservation::none(Arc::clone(volume)));
                self.0.len() - 1
            }
        };
        &mut self.0[at]
    }

    // Take `bytes` of what the job holds, first from `first`, then from its
    // other volumes, each part handed to `part` with its own volume. Fewer
    // when the job holds fewer.
    pub(super) fn take(
        &mut self,
        first: Option<&Arc<Volume>>,
        bytes: u64,
        mut part: impl FnMut(Reservation),
    ) {
        let mut left = bytes;
        if let Some(first) = first
            && let Some(held) = self
                .0
                .iter_mut()
                .find(|held| Arc::ptr_eq(held.volume(), first))
        {
            let taken = left.min(held.bytes());
            left -= taken;
            if taken != 0 {
                part(held.split(taken));
            }
        }
        self.take_besides(first, left, part);
    }

    // Take `bytes` of what the job holds on volumes other than `than`, each
    // part handed to `part`. Fewer when it holds fewer there.
    pub(super) fn take_besides(
        &mut self,
        than: Option<&Arc<Volume>>,
        mut bytes: u64,
        mut part: impl FnMut(Reservation),
    ) {
        for held in &mut self.0 {
            if bytes == 0 {
                break;
            }
            if than.is_some_and(|than| Arc::ptr_eq(held.volume(), than)) {
                continue;
            }
            let taken = bytes.min(held.bytes());
            if taken != 0 {
                bytes -= taken;
                part(held.split(taken));
            }
        }
    }

    // Every reservation the job holds, taken.
    pub(super) fn into_parts(self) -> Vec<Reservation> {
        self.0
    }
}

// Where an article's bytes land as the loop hands them on: the volume, and
// for a member not yet decided, the other volume its deciding article may
// land on as well (`NamespaceWriter::landing`).
#[derive(Clone)]
pub(in crate::pipeline) struct Landing {
    pub(super) volume: Arc<Volume>,
    pub(super) or: Option<Arc<Volume>>,
}

// What the loop prepaid for the writes of one step: on the volume the
// article lands on, and, for a member not yet decided, on the other.
#[must_use = "what was prepaid stays counted until it is settled"]
#[derive(Default)]
pub struct StepCredit([Option<Prepaid>; 2]);

// What arming a step came to.
pub(super) enum Armed {
    Credit(StepCredit),
    // `volume`, where the article's bytes would count, has no room for
    // `bytes` of them (`Volume::fits`), and a wake is asked for when it may.
    Park { volume: Arc<Volume>, bytes: u64 },
}

// Released results whose bytes land on a volume that had no room for them.
// Only their job waits: its results stay in the order they came, the front
// one parked for room on one volume, the others behind it. A volume's room
// is looked at only for the fronts parked on it, in the order they parked,
// up to the first that still does not fit; a result behind a front is not
// looked at until the front is handed on.
// The jobs whose front waits for room on one volume, in the order they
// parked, with the bytes each front needs there.
type ParkedFronts = VecDeque<(JobId, u64)>;

#[derive(Default)]
pub struct ParkedResults {
    // Each job's results waiting. The front is the one that parked, taken
    // out while it is tried again; the entry stays meanwhile, so what comes
    // for the job still waits behind it.
    by_job: HashMap<JobId, VecDeque<DownloadResult>>,
    // For each volume a front parked on: those jobs, in the order they
    // parked, with the bytes each front needs there.
    fronts: Vec<(Arc<Volume>, ParkedFronts)>,
    // Fronts their volume has room for, handed on before new results.
    retrying: VecDeque<DownloadResult>,
    // How many were tried again, for tests that count what a wake costs.
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    tried: usize,
}

impl ParkedResults {
    // `result` is its job's front, parked for room for `bytes` on `volume`,
    // ahead of whatever of the job waits behind it.
    fn park_front(&mut self, result: DownloadResult, volume: Arc<Volume>, bytes: u64) {
        let job_id = result.job_id;
        self.by_job.entry(job_id).or_default().push_front(result);
        match self
            .fronts
            .iter_mut()
            .find(|(on, _)| Arc::ptr_eq(on, &volume))
        {
            Some((_, jobs)) => jobs.push_back((job_id, bytes)),
            None => self
                .fronts
                .push((volume, VecDeque::from([(job_id, bytes)]))),
        }
    }

    // A volume may have room: each front that fits on its volume now, with
    // the fronts ahead of it there, is taken out to be tried again. A
    // volume's first front that does not fit ends its look.
    fn take_fitting(&mut self) {
        let Self {
            by_job,
            fronts,
            retrying,
            ..
        } = self;
        for (volume, jobs) in fronts.iter_mut() {
            let mut fitting = 0u64;
            while let Some(&(job_id, bytes)) = jobs.front() {
                let after = fitting.saturating_add(bytes);
                if !volume.fits(after) {
                    break;
                }
                fitting = after;
                jobs.pop_front();
                if let Some(front) = by_job.get_mut(&job_id).and_then(VecDeque::pop_front) {
                    retrying.push_back(front);
                }
            }
        }
        fronts.retain(|(_, jobs)| !jobs.is_empty());
    }

    // The job's front tried again was handed on: the next one waiting
    // behind it is tried next, or the job no longer waits.
    fn front_handed_on(&mut self, job_id: JobId) {
        let Some(waiting) = self.by_job.get_mut(&job_id) else {
            return;
        };
        match waiting.pop_front() {
            Some(next) => self.retrying.push_back(next),
            None => {
                self.by_job.remove(&job_id);
            }
        }
    }

    // Everything of the job that waits, taken: the job has ended.
    fn take_job(&mut self, job_id: JobId) -> Vec<DownloadResult> {
        let Some(waiting) = self.by_job.remove(&job_id) else {
            return Vec::new();
        };
        for (_, jobs) in &mut self.fronts {
            jobs.retain(|(parked, _)| *parked != job_id);
        }
        self.fronts.retain(|(_, jobs)| !jobs.is_empty());
        let mut taken: Vec<_> = Vec::with_capacity(waiting.len() + 1);
        if let Some(at) = self
            .retrying
            .iter()
            .position(|result| result.job_id == job_id)
        {
            taken.extend(self.retrying.remove(at));
        }
        taken.extend(waiting);
        taken
    }

    // Everything that waits, each job's results in order.
    fn take_all(&mut self) -> Vec<DownloadResult> {
        self.fronts.clear();
        let mut taken: Vec<_> = std::mem::take(&mut self.retrying).into();
        // A job's front out for its retry is ahead of the rest of it.
        taken.extend(self.by_job.drain().flat_map(|(_, results)| results));
        taken
    }
}

impl Pipeline {
    // Where the bytes of an article of `file` land (`NamespaceWriter::landing`).
    pub(in crate::pipeline) fn article_landing(&self, file: NzbFileId) -> Option<Landing> {
        let graph = self.graph_jobs.get(&file.job_id)?;
        let (volume, or) = graph
            .writer()
            .landing(graph.tree(), MemberId(file.file_index))?;
        Some(Landing {
            volume: Arc::clone(volume),
            or: or.map(Arc::clone),
        })
    }

    // Where the bytes of the job land when no file is named: its working
    // volume, for fixtures that book bytes of no file.
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub(in crate::pipeline) fn working_landing(&self, job_id: JobId) -> Option<Landing> {
        let disk = self.graph_jobs.get(&job_id)?.disk()?;
        Some(Landing {
            volume: Arc::clone(disk.working().volume()),
            or: None,
        })
    }

    // Ask for the room wake of `volume` for the test `bytes` failed
    // (`Volume::fits`): at the line they fit below, or lower, at its resume
    // line, so the loop is not woken per write; and no lower than what the
    // volume counts besides its writer's buffer, so the wake comes once the
    // writer has written what it holds, which fits as well. Counted is read
    // before buffered: a write between the two only raises the line. Whether
    // it was asked: false when the volume is there already.
    fn ask_landing_wake(&self, job_id: JobId, volume: &Arc<Volume>, bytes: u64) -> bool {
        let Some(submitter) = self
            .graph_jobs
            .get(&job_id)
            .and_then(|graph| graph.disk())
            .and_then(|disk| {
                disk.each()
                    .find(|submitter| Arc::ptr_eq(submitter.volume(), volume))
            })
        else {
            return false;
        };
        let (_, hard) = volume.buffer_limits();
        let counted = volume.unwritten_bytes();
        let buffered = volume.buffered_bytes.load(Ordering::SeqCst);
        let line = volume
            .resume_line()
            .min(hard.saturating_sub(bytes))
            .max(counted.saturating_sub(buffered));
        let asked = submitter.wake_below(line);
        if asked {
            self.volume_budget_held
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
        asked
    }

    // Whether `bytes` may be counted on `volume` now, or, when `may_park`,
    // whether the step parks for its room wake instead. A step that may not
    // park does without.
    fn landing_room(
        &self,
        job_id: JobId,
        volume: &Arc<Volume>,
        bytes: u64,
        may_park: bool,
    ) -> Result<bool, ()> {
        if bytes == 0 || volume.fits(bytes) {
            return Ok(true);
        }
        if may_park && self.ask_landing_wake(job_id, volume, bytes) {
            return Err(());
        }
        // Asked too late: the volume fell to the line meanwhile.
        Ok(volume.fits(bytes))
    }

    // The loop is about to hand on `bytes` of the job that arrived, landing
    // as `landing` says: they are prepaid on the volume they land on for the
    // writes of this step, which take them first (`Owner::prepay`). Bytes of
    // the article counted on the job's other volume, because it was leased
    // or arrived while its member was undecided, move to the volume it
    // lands on only when they fit there (`Volume::fits`); a member still
    // undecided is prepaid on its other volume too, when they fit there,
    // since the article may decide it. Where they do not fit, a step that
    // may park parks for the volume's room wake, and nothing moved or was
    // counted; one that may not does without that credit. A job with no
    // bytes counted for it, or none that land through a writer, prepays
    // nothing.
    pub(super) fn arm_or_park(
        &mut self,
        job_id: JobId,
        landing: Option<Landing>,
        bytes: u64,
        may_park: bool,
    ) -> Armed {
        let (Some(landing), Some(holds)) = (landing, self.volume_arrived_by_job.get(&job_id))
        else {
            return Armed::Credit(StepCredit::default());
        };
        let Landing { volume, or } = landing;
        let here = holds.on(&volume);
        let elsewhere = holds.bytes() - here;
        let mut moving = bytes.saturating_sub(here).min(elsewhere);
        match self.landing_room(job_id, &volume, moving, may_park) {
            Ok(true) => {}
            Ok(false) => moving = 0,
            Err(()) => {
                return Armed::Park {
                    volume,
                    bytes: moving,
                };
            }
        }
        let or = match or {
            Some(other) => match self.landing_room(job_id, &other, bytes, may_park) {
                Ok(true) => Some(other),
                Ok(false) => None,
                Err(()) => {
                    return Armed::Park {
                        volume: other,
                        bytes,
                    };
                }
            },
            None => None,
        };
        let writers = &self.volume_writers;
        let holds = self
            .volume_arrived_by_job
            .get_mut(&job_id)
            .expect("looked up above");
        let mut woke = false;
        if moving != 0 {
            // Given back on the volume each part counts on, and counted
            // here, where they fit (`Reservation::join`). Whoever waits for
            // room on the volume left is woken at its line.
            let mut moved = Reservation::none(Arc::clone(&volume));
            holds.take_besides(Some(&volume), moving, |part| {
                let left = Arc::clone(part.volume());
                left.arrived_bytes.fetch_sub(part.bytes(), Ordering::SeqCst);
                volume
                    .arrived_bytes
                    .fetch_add(part.bytes(), Ordering::SeqCst);
                moved.join(part);
                woke |= writers.take_wake(&left);
            });
            holds.entry(&volume).join(moved);
        }
        let held = holds.entry(&volume);
        let paid = bytes.min(held.bytes());
        volume.arrived_bytes.fetch_sub(paid, Ordering::SeqCst);
        let mut credit = StepCredit::default();
        credit.0[0] = Some(self.volume_writers.prepay(held, paid));
        if let Some(other) = or {
            // The article may decide its member here and land on the other
            // volume: those writes find their bytes counted ahead as well.
            let mut ahead = Reservation::none(other);
            ahead.count(bytes);
            credit.0[1] = Some(self.volume_writers.prepay(&mut ahead, bytes));
        }
        if woke {
            self.volume_room_taken();
        }
        Armed::Credit(credit)
    }

    // `arm_or_park` for a step that may not park: one never booked as
    // waiting, or one at the stop.
    pub(super) fn arm_volume_credit(
        &mut self,
        job_id: JobId,
        landing: Option<Landing>,
        bytes: u64,
    ) -> StepCredit {
        match self.arm_or_park(job_id, landing, bytes, false) {
            Armed::Credit(credit) => credit,
            // Never: only a step that may park is parked.
            Armed::Park { .. } => StepCredit::default(),
        }
    }

    // The step is done: what was prepaid and its writes did not take no
    // longer counts.
    pub(super) fn settle_volume_credit(&mut self, credit: StepCredit) {
        let mut woke = false;
        for prepaid in credit.0.into_iter().flatten() {
            let left = prepaid.settle();
            woke |= left != 0 && self.volume_writers.take_wake(prepaid.volume());
        }
        if woke {
            self.volume_room_taken();
        }
    }

    // A volume may have room: dispatch stopped for one looks again
    // (`Self::volume_room_changed`), and so do the results parked for room
    // (`Self::park_download_result`).
    pub(in crate::pipeline) fn volume_room_taken(&mut self) {
        self.parked_results_room = true;
        self.volume_room_changed();
    }

    // Keep `result`, its job's front, until `volume` has room for `bytes`
    // (`ParkedResults`): only its job waits, in the order its results came.
    pub(super) fn park_download_front(
        &mut self,
        result: DownloadResult,
        volume: Arc<Volume>,
        bytes: u64,
    ) {
        self.parked_download_results
            .park_front(result, volume, bytes);
    }

    // Keep `result` behind its job's results already waiting for room.
    pub(super) fn park_behind_download_results(&mut self, result: DownloadResult) {
        self.parked_download_results
            .by_job
            .entry(result.job_id)
            .or_default()
            .push_back(result);
    }

    // Whether the job's results wait for room: the next one waits behind.
    pub(super) fn job_results_parked(&self, job_id: JobId) -> bool {
        self.parked_download_results.by_job.contains_key(&job_id)
    }

    // The job's front tried again was handed on: the next of it is tried.
    pub(super) fn parked_front_handed_on(&mut self, job_id: JobId) {
        self.parked_download_results.front_handed_on(job_id);
    }

    // A volume may have room since results were parked: the fronts parked
    // on each volume that fit there now are taken out to be tried again,
    // ahead of new results (`Self::take_retried_download_result`). One
    // that still finds no room parks again with a wake asked anew. Nothing
    // is looked at until a volume may have room.
    pub(in crate::pipeline) fn unpark_download_results(&mut self) {
        if std::mem::take(&mut self.parked_results_room) {
            self.parked_download_results.take_fitting();
        }
    }

    // The next front taken out to be tried again.
    pub(in crate::pipeline) fn take_retried_download_result(&mut self) -> Option<DownloadResult> {
        let result = self.parked_download_results.retrying.pop_front();
        #[cfg(any(test, feature = "pipeline-test-fixtures"))]
        {
            self.parked_download_results.tried += usize::from(result.is_some());
        }
        result
    }

    // The job has ended: whatever of it waits for room is let go as a result
    // of a job that takes no more is (`process_download_done`), its decoded
    // bytes dropped, and once it has no result left waiting what it holds
    // on each volume is given back (`Self::note_volume_handed_on`).
    pub(in crate::pipeline) fn drop_parked_download_results(&mut self, job_id: JobId) {
        for result in self.parked_download_results.take_job(job_id) {
            let lead_bytes = Self::released_download_result_lead_bytes(&result);
            self.finish_checkpoint_progress_article(result.lane_id, result.segment_id);
            drop(result);
            self.finish_released_download_result_processing(job_id, lead_bytes);
        }
    }

    // The loop stops: every parked result is handed on without parking, its
    // writes bounded by the stop's span (`VolumeWriters::end_waits_at`).
    pub(in crate::pipeline) async fn hand_on_parked_results_at_stop(&mut self) {
        for result in self.parked_download_results.take_all() {
            self.process_released_download_done_with(result, false, false)
                .await;
        }
    }

    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub fn parked_download_results_for_test(&self) -> usize {
        let parked = &self.parked_download_results;
        parked.by_job.values().map(VecDeque::len).sum::<usize>() + parked.retrying.len()
    }

    // How many parked results were tried again since the pipeline began.
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub fn parked_results_tried_for_test(&self) -> usize {
        self.parked_download_results.tried
    }

    // A volume may have room, as a step that gave some back says.
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub fn volume_room_taken_for_test(&mut self) {
        self.volume_room_taken();
    }

    // How many fronts wait for room on `volume`.
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub fn parked_fronts_on_for_test(&self, volume: &Arc<Volume>) -> usize {
        self.parked_download_results
            .fronts
            .iter()
            .find(|(on, _)| Arc::ptr_eq(on, volume))
            .map_or(0, |(_, jobs)| jobs.len())
    }
}

// Fixtures that lease, take in and hand on articles of a graph job as the
// loop does, with no lane: what each article counts, where, at each step.
#[cfg(any(test, feature = "pipeline-test-fixtures"))]
impl Pipeline {
    fn work_for_test(segment: SegmentId, bytes: u32) -> DownloadWork {
        DownloadWork {
            counters: None,
            segment_id: segment,
            message_id: crate::MessageId::new("landing@example.test"),
            groups: Arc::from(["alt.binaries.test".into()]),
            priority: 0,
            byte_estimate: bytes,
            retry_count: 0,
            is_recovery: false,
            completion_critical: false,
            exclude_servers: weaver_nntp::ServerSet::EMPTY,
            avoid_server: None,
        }
    }

    // The volume an article of `file` lands on now.
    pub fn landing_volume_for_test(&self, file: NzbFileId) -> Option<Arc<Volume>> {
        self.article_landing(file).map(|landing| landing.volume)
    }

    // Lease an article of `segment` of `bytes`, as dispatch reserves one:
    // false when its volume refuses it.
    pub fn lease_for_test(&mut self, segment: SegmentId, bytes: u32) -> bool {
        self.reserve_lane_work(&Self::work_for_test(segment, bytes))
    }

    // The article leased at `leased` bytes came back with `arrived`: its
    // lease is settled to what arrived, which waits for the loop.
    pub fn arrive_for_test(&mut self, segment: SegmentId, leased: u32, arrived: u64) {
        self.settle_lane_work(&Self::work_for_test(segment, leased), arrived);
        self.note_released_download_result_pending(segment.file_id.job_id, arrived);
    }

    // `arrived` bytes of the job wait for the loop counted on `volume`, as
    // those of an article leased while it was thought to land there.
    pub fn arrive_on_for_test(&mut self, job_id: JobId, volume: &Arc<Volume>, arrived: u64) {
        if self.hold_volume_arrived(
            job_id,
            Reservation::none(Arc::clone(volume)),
            arrived,
            false,
        ) {
            self.volume_room_taken();
        }
        self.note_released_download_result_pending(job_id, arrived);
    }

    // The loop hands on the waiting result `decoded` as it does in a turn:
    // false when it parked for its volume's room.
    pub async fn hand_on_for_test(&mut self, decoded: DecodeResult) -> bool {
        let segment_id = decoded.segment_id;
        self.process_released_download_done(DownloadResult {
            lane_id: 0,
            job_id: segment_id.file_id.job_id,
            runtime_generation: self.pool_generation,
            segment_id,
            data: Ok(DownloadPayload::Decoded(decoded)),
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: Some(0),
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count: 0,
            exclude_servers: weaver_nntp::ServerSet::EMPTY,
            release_connection_slot: false,
        })
        .await
    }

    // A volume's writer said there is room, and the loop hears it as a
    // turn does. Waits for the word.
    pub async fn hear_volume_room_for_test(&mut self) {
        self.volume_rooms.recv().await;
        self.volume_room_heard();
        self.parked_results_room = true;
    }

    // The results parked for room are tried again, as a turn does once a
    // volume had room: how many were handed on.
    pub async fn hand_on_parked_for_test(&mut self) -> usize {
        self.unpark_download_results();
        let mut handed = 0;
        while let Some(result) = self.take_retried_download_result() {
            handed += usize::from(self.process_retried_download_done(result).await);
        }
        handed
    }

    // Whether results of the job are booked as waiting for the loop.
    pub fn results_waiting_for_test(&self, job_id: JobId) -> bool {
        self.pending_released_download_results_by_job
            .contains_key(&job_id)
    }

    // One waiting result of the job of `bytes` is let go unwritten, as one
    // of a job that takes no more is.
    pub fn let_go_result_for_test(&mut self, job_id: JobId, bytes: u64) {
        self.finish_released_download_result_processing(job_id, bytes);
    }

    // Whether the loop asks a room wake for the job's refused dispatch
    // (`Self::ask_room_wake`): false when its volumes are at their line
    // already, so a lease may be tried again at once.
    pub fn ask_room_wake_for_test(&self, job_id: JobId) -> bool {
        self.ask_room_wake(job_id)
    }

    // Whether dispatch refuses the job for a volume at its lease limit: the
    // WriteBuffer clause (`Self::queued_work_may_pass`).
    pub fn volume_refuses_dispatch_for_test(&self, job_id: JobId) -> bool {
        self.volume_refuses_job(job_id)
    }
}
