use super::*;
use crate::pipeline::direct_store::router::HeaderProbe as DirectHeaderProbe;

pub(in crate::pipeline::download) struct DirectStoreAdmission {
    job_id: JobId,
    files: HashSet<u32>,
    limit: u64,
    /// Staged bytes, plus every byte already on its way to the set, plus
    /// what the handout being built has leased of it.
    committed: u64,
    available: u64,
    probes: Vec<SegmentId>,
    probe_lease: ProbeLease,
}

/// What leasing one of the set's articles does to its header probes.
#[derive(Clone, Copy)]
enum ProbeLease {
    /// The earliest-volume probe is only asked of an idle set; anything of
    /// the set out on a lane closes it.
    CloseAll,
    /// A container probes each end until that end's volume has an article
    /// in flight; leasing one closes only the probe of that volume.
    CloseFile,
}

impl DirectStoreAdmission {
    pub(in crate::pipeline::download) fn allows(&self, work: &DownloadWork) -> bool {
        !self.files.contains(&work.segment_id.file_id.file_index)
            || work.byte_estimate as u64 <= self.available
            || self.probes.contains(&work.segment_id)
    }

    /// Whether this set could admit *any* queued article of `file_index`,
    /// given the smallest `byte_estimate` among them.
    ///
    /// [`Self::allows`] admits an article of one of the set's files only when
    /// its estimate fits the room left or it is one of the set's probes. When
    /// even the smallest article does not fit, no article of the file fits,
    /// so only a probe can get through; a probe in this file keeps the answer
    /// `true`. Probes are matched by file index alone, which can only make
    /// the answer more permissive than [`Self::allows`].
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
    }

    /// Charge an article the handout being built has just taken, so the
    /// next one is admitted against what the set will hold with it.
    pub(in crate::pipeline::download) fn note_leased(&mut self, work: &DownloadWork) {
        let file = work.segment_id.file_id;
        if file.job_id != self.job_id || !self.files.contains(&file.file_index) {
            return;
        }
        self.committed = self.committed.saturating_add(work.byte_estimate as u64);
        self.available = self.limit.saturating_sub(self.committed);
        match self.probe_lease {
            ProbeLease::CloseAll => self.probes.clear(),
            ProbeLease::CloseFile => self
                .probes
                .retain(|probe| probe.file_id.file_index != file.file_index),
        }
    }
}

impl Pipeline {
    /// Per-set disk retention is separate from shared RAM pressure. Include
    /// arrivals already committed to the wire/decode pipeline before allowing
    /// another article to start; work leased by the handout being built is
    /// charged as it is taken, through [`DirectStoreAdmission::note_leased`].
    pub(super) fn direct_store_admission(&self, job_id: JobId) -> Vec<DirectStoreAdmission> {
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
                let limit = set.router.holds_admission_limit();
                let committed = set.router.staged_bytes().saturating_add(incoming);

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
                    job_id,
                    files,
                    limit,
                    committed,
                    available: limit.saturating_sub(committed),
                    probes,
                    probe_lease,
                }
            })
            .collect()
    }
}
