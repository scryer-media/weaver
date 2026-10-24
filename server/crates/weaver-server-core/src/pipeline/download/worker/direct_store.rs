// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::pipeline::direct_store::router::HeaderProbe as DirectHeaderProbe;

pub(in crate::pipeline::download) struct DirectStoreAdmission {
    job_id: JobId,
    // The set's index among its job's sets.
    set_index: usize,
    files: HashSet<u32>,
    // Room the set had when the handout began: its holds admission room
    // with every byte already on its way to it counted.
    room: u64,
    // The same room judged by the set's own limit alone.
    own_room: u64,
    // What the handout being built has leased of the set so far.
    leased: u64,
    available: u64,
    // `own_room` less what the handout has leased.
    own_available: u64,
    probes: Vec<SegmentId>,
    probe_lease: ProbeLease,
    // The article an idle set with a settled layout routes next: the
    // lowest queued article of its earliest unfinished volume. Judged by
    // the set's own room only, so what other sets hold can slow this set to
    // one article at a time but never stop it.
    frontier: Option<SegmentId>,
    // The set cannot place another byte from the wire: the earliest volume
    // whose headers are unread has nothing left to arrive, so no article can
    // let its holds drain, and those holds leave no room for any article it
    // still has queued. Read by [`Pipeline::settle_direct_unplaceable_sets`],
    // which hands such a set over at once.
    unplaceable: bool,
}

// What leasing one of the set's articles does to its header probes.
#[derive(Clone, Copy)]
enum ProbeLease {
    // The earliest-volume probe is only asked of an idle set; anything of
    // the set out on a lane closes it.
    CloseAll,
    // A container probes each end until that end's volume has an article
    // in flight; leasing one closes only the probe of that volume.
    CloseFile,
}

impl DirectStoreAdmission {
    pub(in crate::pipeline::download) fn allows(&self, work: &DownloadWork) -> bool {
        !self.files.contains(&work.segment_id.file_id.file_index)
            || work.byte_estimate as u64 <= self.available
            || self.probes.contains(&work.segment_id)
            || (self.frontier == Some(work.segment_id)
                && work.byte_estimate as u64 <= self.own_available)
    }

    // Whether this set could admit *any* queued article of `file_index`,
    // given the smallest `byte_estimate` among them.
    //
    // [`Self::allows`] admits an article of one of the set's files only when
    // its estimate fits the room left or it is one of the set's probes. When
    // even the smallest article does not fit, no article of the file fits,
    // so only a probe or the set's frontier can get through; either in this
    // file keeps the answer `true`. Both are matched by file index alone,
    // which can only make the answer more permissive than [`Self::allows`].
    pub(in crate::pipeline::download) fn may_admit_from_file(
        &self,
        file_index: u32,
        smallest: u32,
    ) -> bool {
        !self.files.contains(&file_index)
            || smallest as u64 <= self.available
            || self
                .probes
                .iter()
                .any(|probe| probe.file_id.file_index == file_index)
            || self
                .frontier
                .is_some_and(|frontier| frontier.file_id.file_index == file_index)
    }

    // See the field.
    pub(in crate::pipeline::download) fn unplaceable(&self) -> bool {
        self.unplaceable
    }

    // Charge an article the handout being built has just taken, so the
    // next one is admitted against what the set will hold with it.
    pub(in crate::pipeline::download) fn note_leased(&mut self, work: &DownloadWork) {
        let file = work.segment_id.file_id;
        if file.job_id != self.job_id || !self.files.contains(&file.file_index) {
            return;
        }
        // The holds admission room falls byte for byte with what is
        // committed to the set, down to zero, so charging the lease against
        // the room taken at the start equals asking the router again with
        // the lease counted as incoming.
        self.leased = self.leased.saturating_add(work.byte_estimate as u64);
        self.available = self.room.saturating_sub(self.leased);
        self.own_available = self.own_room.saturating_sub(self.leased);
        // Anything of the set out on a lane makes it busy, and a busy set
        // waits for that article rather than reaching past the shared room
        // for another.
        self.frontier = None;
        match self.probe_lease {
            ProbeLease::CloseAll => self.probes.clear(),
            ProbeLease::CloseFile => self
                .probes
                .retain(|probe| probe.file_id.file_index != file.file_index),
        }
    }
}

impl Pipeline {
    // Hands over every set an admission pass found unplaceable, judged again
    // now: the pass that noted the job cannot demote from where it runs.
    //
    // Demoting is what the set's scratch ceiling would end it with anyway, so
    // it is demoted under that reason and keeps its holds exactly as it would
    // then; doing it now is what keeps the rest of the set from being fetched
    // one article at a time into holds that nothing left to arrive can place.
    pub(in crate::pipeline) async fn settle_direct_unplaceable_sets(&mut self) {
        let jobs = std::mem::take(&mut self.direct_store.unplaceable_verdicts);
        for job_id in jobs {
            let unplaceable: Vec<usize> = self
                .direct_store_admission(job_id)
                .into_iter()
                .filter(DirectStoreAdmission::unplaceable)
                .map(|set| set.set_index)
                .collect();
            for set_index in unplaceable {
                warn!(
                    job_id = job_id.0,
                    set_index,
                    "direct-store set cannot place its holds: its earliest unread volume has nothing left to arrive"
                );
                self.demote_direct_set(
                    job_id,
                    set_index,
                    crate::pipeline::direct_store::router::DemotionReason::HoldsScratchCeiling,
                )
                .await;
            }
        }
    }

    // Per-set disk retention is separate from shared RAM pressure. Include
    // arrivals already committed to the wire/decode pipeline before allowing
    // another article to start; work leased by the handout being built is
    // charged as it is taken, through [`DirectStoreAdmission::note_leased`].
    pub(super) fn direct_store_admission(&self, job_id: JobId) -> Vec<DirectStoreAdmission> {
        let Some(state) = self.jobs.get(&job_id) else {
            return Vec::new();
        };
        self.direct_store
            .sets_for(job_id)
            .iter()
            .enumerate()
            .filter(|(_, set)| !set.is_demoted() && !set.is_finalized())
            .map(|(set_index, set)| {
                let files: HashSet<u32> = set.plan().files.keys().copied().collect();
                let owns =
                    |file: NzbFileId| file.job_id == job_id && files.contains(&file.file_index);
                let wire = self
                    .rate_limit_reservations
                    .iter()
                    .filter(|(segment, _)| owns(segment.file_id))
                    .map(|(_, bytes)| *bytes);
                let decoding = self
                    .active_decode_bytes
                    .iter()
                    .filter(|(segment, _)| owns(segment.file_id))
                    .map(|(_, bytes)| *bytes);
                let pending = self
                    .pending_decode
                    .iter()
                    .filter(|work| owns(work.segment_id.file_id))
                    .map(|work| work.raw.len() as u64);
                // Released lane results are tracked per job, so charge them
                // conservatively to each set until their actor event arrives.
                let released = self
                    .pending_released_download_result_bytes_by_job
                    .get(&job_id)
                    .copied()
                    .unwrap_or(0);
                let incoming = wire
                    .chain(decoding)
                    .chain(pending)
                    .fold(released, u64::saturating_add);
                let busy = incoming != 0
                    || self
                        .active_downloads_by_file
                        .iter()
                        .any(|(file, count)| owns(*file) && *count != 0)
                    || self
                        .active_decodes_by_file
                        .iter()
                        .any(|(file, count)| owns(*file) && *count != 0);
                let rooms = set.router.holds_admission_rooms(incoming);
                let room = rooms.shared;

                // Permit queued articles past the limit to resolve the layout.
                // Ordinals need not match yEnc offsets: serialized progress
                // also handles rotated NZBs and headers spanning several
                // articles. If an earlier volume has a retry pending, wait for
                // it instead of spending probe room on later volumes.
                // Terminally missing articles have no retry; keep progressing
                // toward the existing repair/demotion path.
                let unresolved = |file: &u32| {
                    state.download_queue.queued_count_for_file(NzbFileId {
                        job_id,
                        file_index: *file,
                    }) != 0
                        || self.pending_retries_by_segment.iter().any(|(id, count)| {
                            id.file_id.job_id == job_id
                                && id.file_id.file_index == *file
                                && *count != 0
                        })
                };
                // Which end of which volume resolves this set's layout. For RAR
                // it is always the front of the earliest volume whose headers
                // are unread; a container is read at both ends, because its
                // volumes state their lengths one article each and its map sits
                // at the very end of the last one.
                let probe_lease = match set.header_probe() {
                    DirectHeaderProbe::Earliest => ProbeLease::CloseAll,
                    _ => ProbeLease::CloseFile,
                };
                // The volume a RAR set's layout waits on: the earliest whose
                // headers are unread, whether or not anything of it is still to
                // come, and only when every header is read, the earliest volume
                // still owed an article. A later volume is never asked for past
                // the room in its place: its bytes are placed through the
                // volumes before it, so while one of those is unread they can
                // only be held.
                let earliest = matches!(set.header_probe(), DirectHeaderProbe::Earliest)
                    .then(|| {
                        set.plan()
                            .volumes
                            .iter()
                            .filter(|(volume, file)| {
                                unresolved(file) || set.router.volume_needs_header(**volume)
                            })
                            .min_by_key(|(volume, _)| {
                                (!set.router.volume_needs_header(**volume), **volume)
                            })
                            .map(|(volume, file)| (*volume, *file))
                    })
                    .flatten();
                // That volume's headers are unread and nothing of it is queued,
                // retrying or in flight: no article the set could still be
                // given would let a held byte place.
                let header_unreachable = !busy
                    && earliest.is_some_and(|(volume, file)| {
                        set.router.volume_needs_header(volume) && !unresolved(&file)
                    });
                let probes = match set.header_probe() {
                    DirectHeaderProbe::Settled => Vec::new(),
                    DirectHeaderProbe::Earliest if busy => Vec::new(),
                    DirectHeaderProbe::Earliest => earliest
                        .filter(|(_, file)| unresolved(file))
                        .and_then(|(_, file)| {
                            state.download_queue.peek_first_matching(|work| {
                                work.segment_id.file_id.file_index == file
                            })
                        })
                        .map(|work| vec![work.segment_id])
                        .unwrap_or_default(),
                    // Released lane results are only attributable to the job,
                    // so while any are outstanding nothing here can tell which
                    // volume they answer; wait for them rather than probe a
                    // volume whose article is already on its way back.
                    DirectHeaderProbe::Container { .. } if released != 0 => Vec::new(),
                    // Two articles, at the two ends of the set, and independent
                    // of one another: volume zero's front carries the part size
                    // and the start header, the tail carries the map. Asking
                    // for both together is what keeps a set from staging every
                    // volume ahead of the last one before it can read its map.
                    // The bound is one article in flight per end.
                    DirectHeaderProbe::Container { front, tail } => {
                        let mut inflight: HashSet<u32> = HashSet::new();
                        let mut note = |file: NzbFileId| {
                            if owns(file) {
                                inflight.insert(file.file_index);
                            }
                        };
                        for segment in self.rate_limit_reservations.keys() {
                            note(segment.file_id);
                        }
                        for segment in self.active_decode_bytes.keys() {
                            note(segment.file_id);
                        }
                        for work in &self.pending_decode {
                            note(work.segment_id.file_id);
                        }
                        for (file, count) in &self.active_downloads_by_file {
                            if *count != 0 {
                                note(*file);
                            }
                        }
                        for (file, count) in &self.active_decodes_by_file {
                            if *count != 0 {
                                note(*file);
                            }
                        }
                        let probeable = |volume: &u32| {
                            set.plan()
                                .volumes
                                .get(volume)
                                .filter(|file| unresolved(file) && !inflight.contains(file))
                                .copied()
                        };
                        let mut probes = Vec::with_capacity(2);
                        if let Some(file) = front.as_ref().and_then(probeable)
                            && let Some(work) = state.download_queue.peek_first_matching(|work| {
                                work.segment_id.file_id.file_index == file
                            })
                        {
                            probes.push(work.segment_id);
                        }
                        if let Some(file) = tail.as_ref().and_then(probeable)
                            && let Some(work) = state.download_queue.peek_last_matching(|work| {
                                work.segment_id.file_id.file_index == file
                            })
                            && !probes.contains(&work.segment_id)
                        {
                            probes.push(work.segment_id);
                        }
                        probes
                    }
                };
                // The shared room bounds how far holds may grow, not whether
                // a set moves at all. Other sets' holds — a paused job's, one
                // waiting on a retried gap — can leave less shared room than
                // one article, and a settled set has no probe to fall back on,
                // so an idle one is always given the article it routes next.
                // Idle is what keeps this from growing holds past the shared
                // total: one article at a time, each landing before the next
                // is asked for. With nothing of the set in flight, the lowest
                // article still queued in the earliest unfinished volume is
                // the earliest byte the set lacks; while that volume has a
                // retry pending, the retry is, and the set waits for it. A RAR
                // set whose unread header cannot arrive has no probe left, so
                // it moves the same way, held to its own room.
                let frontier = ((matches!(set.header_probe(), DirectHeaderProbe::Settled)
                    || header_unreachable)
                    && !busy
                    && rooms.shared < rooms.own)
                    .then(|| {
                        set.plan()
                            .volumes
                            .values()
                            .copied()
                            .find(|file| unresolved(file))
                    })
                    .flatten()
                    .filter(|file| {
                        !self.pending_retries_by_segment.iter().any(|(id, count)| {
                            id.file_id.job_id == job_id
                                && id.file_id.file_index == *file
                                && *count != 0
                        })
                    })
                    .and_then(|file| {
                        state
                            .download_queue
                            .peek_lowest_matching(|work| work.segment_id.file_id.file_index == file)
                            .map(|work| work.segment_id)
                    });
                // With no probe to resolve the layout, the set's own room is all
                // that can admit it, and once no queued article of the set fits
                // that room the set can neither place nor hold anything more.
                let unplaceable = header_unreachable
                    && files
                        .iter()
                        .filter_map(|file_index| {
                            state
                                .download_queue
                                .min_queued_byte_estimate_for_file(NzbFileId {
                                    job_id,
                                    file_index: *file_index,
                                })
                        })
                        .min()
                        .is_some_and(|smallest| u64::from(smallest) > rooms.own);
                DirectStoreAdmission {
                    job_id,
                    set_index,
                    files,
                    room,
                    own_room: rooms.own,
                    leased: 0,
                    available: room,
                    own_available: rooms.own,
                    probes,
                    probe_lease,
                    frontier,
                    unplaceable,
                }
            })
            .collect()
    }
}
