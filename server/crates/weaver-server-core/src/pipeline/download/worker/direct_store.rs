use super::*;

pub(super) struct DirectStoreAdmission {
    files: HashSet<u32>,
    available: u64,
    probe: Option<SegmentId>,
}

impl DirectStoreAdmission {
    pub(super) fn allows(&self, work: &DownloadWork) -> bool {
        !self.files.contains(&work.segment_id.file_id.file_index)
            || work.byte_estimate as u64 <= self.available
            || self.probe == Some(work.segment_id)
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

                // When drained, permit exactly one queued article from the
                // earliest unresolved volume. Ordinals need not match yEnc
                // offsets: serialized progress also handles rotated NZBs and
                // headers spanning several articles. If an earlier volume has
                // a retry pending, wait for it instead of spending probe room
                // on later volumes. Terminally missing articles have no retry;
                // keep progressing toward the existing repair/demotion path.
                let probe = if busy {
                    None
                } else {
                    set.plan()
                        .volumes
                        .iter()
                        .filter(|(_, file)| {
                            state.download_queue.queued_count_for_file(NzbFileId {
                                job_id,
                                file_index: **file,
                            }) != 0
                                || self.pending_retries_by_segment.iter().any(|(id, count)| {
                                    id.file_id.job_id == job_id
                                        && id.file_id.file_index == **file
                                        && *count != 0
                                })
                        })
                        .min_by_key(|(volume, _)| {
                            (!set.router.volume_needs_header(**volume), **volume)
                        })
                        .and_then(|(_, file)| {
                            state.download_queue.peek_next_matching(|work| {
                                work.segment_id.file_id.file_index == *file
                            })
                        })
                        .map(|work| work.segment_id)
                };
                DirectStoreAdmission {
                    files,
                    available,
                    probe,
                }
            })
            .collect()
    }
}
