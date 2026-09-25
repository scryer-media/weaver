use super::*;
use crate::pipeline::direct_store::router::HeaderProbe as DirectHeaderProbe;

pub(in crate::pipeline::download) struct DirectStoreAdmission {
    files: HashSet<u32>,
    available: u64,
    probes: Vec<SegmentId>,
}

impl DirectStoreAdmission {
    pub(in crate::pipeline::download) fn allows(&self, work: &DownloadWork) -> bool {
        !self.files.contains(&work.segment_id.file_id.file_index)
            || work.byte_estimate as u64 <= self.available
            || self.probes.contains(&work.segment_id)
    }
}

impl Pipeline {
    /// Per-set disk retention is separate from shared RAM pressure. Include
    /// arrivals already committed to the wire/decode pipeline, and work in the
    /// lease being built, before allowing another article to start.
    pub(super) fn direct_store_admission(
        &self,
        job_id: JobId,
        leased: &[DownloadWork],
    ) -> Vec<DirectStoreAdmission> {
        let Some(state) = self.jobs.get(&job_id) else {
            return Vec::new();
        };
        self.direct_store
            .sets_for(job_id)
            .iter()
            .filter(|set| !set.is_demoted() && !set.is_finalized())
            .map(|set| {
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
                let leasing = leased
                    .iter()
                    .filter(|work| owns(work.segment_id.file_id))
                    .map(|work| work.byte_estimate as u64);
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
                    .chain(leasing)
                    .fold(released, u64::saturating_add);
                let busy = incoming != 0
                    || self
                        .active_downloads_by_file
                        .iter()
                        .any(|(file, count)| owns(*file) && *count != 0)
                    || self
                        .active_decodes_by_file
                        .iter()
                        .any(|(file, count)| owns(*file) && *count != 0)
                    || leased.iter().any(|work| owns(work.segment_id.file_id));
                let available = set
                    .router
                    .holds_admission_limit()
                    .saturating_sub(set.router.staged_bytes().saturating_add(incoming));

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
                let probes = match set.header_probe() {
                    DirectHeaderProbe::Settled => Vec::new(),
                    DirectHeaderProbe::Earliest if busy => Vec::new(),
                    DirectHeaderProbe::Earliest => set
                        .plan()
                        .volumes
                        .iter()
                        .filter(|(_, file)| unresolved(file))
                        .min_by_key(|(volume, _)| {
                            (!set.router.volume_needs_header(**volume), **volume)
                        })
                        .and_then(|(_, file)| {
                            state.download_queue.peek_first_matching(|work| {
                                work.segment_id.file_id.file_index == *file
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
                        for work in leased {
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
                DirectStoreAdmission {
                    files,
                    available,
                    probes,
                }
            })
            .collect()
    }
}
