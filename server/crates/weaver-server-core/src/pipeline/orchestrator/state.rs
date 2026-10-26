// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

impl Pipeline {
    pub(crate) fn deterministic_extraction_staging_dir(&self, job_id: JobId) -> PathBuf {
        self.complete_dir
            .join(".weaver-staging")
            .join(job_id.0.to_string())
    }

    pub(crate) fn note_write_buffered(&mut self, bytes: usize, segments: usize) {
        self.write_buffered_bytes = self.write_buffered_bytes.saturating_add(bytes);
        self.write_buffered_segments = self.write_buffered_segments.saturating_add(segments);
        self.publish_write_backlog_metrics();
    }

    pub(crate) fn release_write_buffered(&mut self, bytes: usize, segments: usize) {
        debug_assert!(
            self.write_buffered_bytes >= bytes,
            "resident write backlog over-release: have {}, releasing {bytes}",
            self.write_buffered_bytes
        );
        debug_assert!(
            self.write_buffered_segments >= segments,
            "resident write segment backlog over-release: have {}, releasing {segments}",
            self.write_buffered_segments
        );
        self.write_buffered_bytes = self.write_buffered_bytes.saturating_sub(bytes);
        self.write_buffered_segments = self.write_buffered_segments.saturating_sub(segments);
        self.publish_write_backlog_metrics();
    }

    pub(crate) fn note_uu_spooled(&mut self, bytes: usize, segments: usize) {
        self.uu_spooled_bytes = self.uu_spooled_bytes.saturating_add(bytes);
        self.uu_spooled_segments = self.uu_spooled_segments.saturating_add(segments);
        self.publish_write_backlog_metrics();
    }

    pub(crate) fn release_uu_spooled(&mut self, bytes: usize, segments: usize) {
        debug_assert!(
            self.uu_spooled_bytes >= bytes,
            "UU spool backlog over-release: have {}, releasing {bytes}",
            self.uu_spooled_bytes
        );
        debug_assert!(
            self.uu_spooled_segments >= segments,
            "UU spool segment backlog over-release: have {}, releasing {segments}",
            self.uu_spooled_segments
        );
        self.uu_spooled_bytes = self.uu_spooled_bytes.saturating_sub(bytes);
        self.uu_spooled_segments = self.uu_spooled_segments.saturating_sub(segments);
        self.publish_write_backlog_metrics();
    }

    pub(crate) fn note_uu_parked_segment(&mut self) {
        self.uu_parked_segments = self.uu_parked_segments.saturating_add(1);
    }

    pub(crate) fn release_uu_parked_segment(&mut self) {
        debug_assert!(
            self.uu_parked_segments > 0,
            "UU parked segment over-release"
        );
        self.uu_parked_segments = self.uu_parked_segments.saturating_sub(1);
    }

    pub(crate) fn publish_write_backlog_metrics(&self) {
        self.metrics
            .write_buffered_bytes
            .store(self.write_buffered_bytes as u64, Ordering::Relaxed);
        self.metrics
            .write_buffered_segments
            .store(self.write_buffered_segments, Ordering::Relaxed);
        self.metrics.write_pending_bytes.store(
            self.write_buffered_bytes
                .saturating_add(self.uu_spooled_bytes) as u64,
            Ordering::Relaxed,
        );
        self.metrics
            .uu_spooled_bytes
            .store(self.uu_spooled_bytes as u64, Ordering::Relaxed);
        self.metrics
            .uu_spooled_segments
            .store(self.uu_spooled_segments, Ordering::Relaxed);
    }

    pub(crate) fn publish_active_stage_metrics(&self) {
        self.metrics.set_active_downloads(self.active_downloads);
        self.metrics.active_decodes.store(
            self.active_decodes_by_job.values().sum::<usize>(),
            Ordering::Relaxed,
        );
    }

    pub(crate) fn clear_job_write_backlog(&mut self, job_id: JobId) {
        // A uuencode file keeps its entry in `uu_files` until teardown drops
        // it here. A file can also hold nothing but parked articles still
        // waiting for the part before them.
        let file_ids: std::collections::HashSet<NzbFileId> = self
            .uu_files
            .keys()
            .chain(self.unanchored_parked.keys())
            .copied()
            .filter(|file_id| file_id.job_id == job_id)
            .collect();

        let mut released_bytes = 0usize;
        let mut released_segments = 0usize;
        let mut released_spooled_bytes = 0usize;
        let mut released_spooled_segments = 0usize;
        let mut released_uu_parked_segments = 0usize;
        for file_id in file_ids {
            // Memory and disk parks use distinct ledgers. Removing the entry
            // drops `TempPath` and unlinks every remaining per-job spool file.
            if let Some(uu) = self.uu_files.remove(&file_id) {
                released_bytes += uu.parked_memory_bytes();
                released_segments += uu.parked.len() - uu.parked_spooled_segments();
                released_spooled_bytes += uu.parked_spooled_bytes();
                released_spooled_segments += uu.parked_spooled_segments();
                released_uu_parked_segments += uu.parked.len();
            }
            self.uu_park_requeues
                .retain(|segment_id, _| segment_id.file_id != file_id);
            self.unanchored_requeues
                .retain(|segment_id, _| segment_id.file_id != file_id);
            self.pending_unanchored_release
                .retain(|segment_id| segment_id.file_id != file_id);
            if let Some(parked) = self.unanchored_parked.remove(&file_id) {
                released_bytes += parked
                    .values()
                    .map(|(result, _)| result.data.len_bytes())
                    .sum::<usize>();
                released_segments += parked.len();
            }
        }

        if released_bytes > 0 || released_segments > 0 {
            self.release_write_buffered(released_bytes, released_segments);
        }
        if released_spooled_bytes > 0 || released_spooled_segments > 0 {
            self.release_uu_spooled(released_spooled_bytes, released_spooled_segments);
        }
        for _ in 0..released_uu_parked_segments {
            self.release_uu_parked_segment();
        }
        remove_empty_uu_park_dir(&self.uu_spool_root, job_id);
    }
}
