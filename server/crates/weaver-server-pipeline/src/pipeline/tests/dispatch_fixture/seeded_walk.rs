// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

impl DispatchFixture {
    pub fn cap_uu_spool(&mut self, file: NzbFileId) {
        self.0.uu_files.insert(file, UuFileAssembly::default());
        self.0.uu_spool_max_segments = 0;
        self.0.uu_spool_available_bytes_for_test = Some(Some(u64::MAX));
    }

    pub fn set_checkpoint_progress(&mut self, id: JobId, progress: Option<SegmentId>) {
        if let Some(segment) = progress {
            self.0.checkpoint_progress_articles.insert(id, (0, segment));
        } else {
            self.0.checkpoint_progress_articles.remove(&id);
        }
    }

    pub fn set_uu_cursor(&mut self, file: NzbFileId, index: u32) {
        self.0
            .uu_files
            .get_mut(&file)
            .unwrap()
            .next_index
            .set(index);
    }

    pub fn lose_index(&mut self, id: JobId) {
        self.0
            .graph_jobs
            .get_mut(&id)
            .unwrap()
            .lost(
                weaver_graph::VolumeIndex(0),
                0,
                &mut crate::pipeline::graph::status::RepairSlots::default(),
                0,
            )
            .unwrap();
    }
}
