// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::{collections::HashSet, sync::Arc};

use crate::core::lengths::ValidPieceIndex;

use crate::{
    chunk_tracker::{ChunkTracker, HaveNeededSelected},
    piece_priority::PiecePriority,
    type_aliases::FileStorage,
};

use super::{ManagedTorrentShared, TorrentMetadata, streaming::TorrentStreams};

pub struct TorrentStatePaused {
    pub(crate) shared: Arc<ManagedTorrentShared>,
    pub(crate) metadata: Arc<TorrentMetadata>,
    pub(crate) files: FileStorage,
    pub(crate) chunk_tracker: ChunkTracker,
    pub(crate) streams: Arc<TorrentStreams>,
}

impl TorrentStatePaused {
    pub(crate) fn update_only_files(&mut self, only_files: &HashSet<usize>) -> anyhow::Result<()> {
        self.chunk_tracker
            .update_only_files(&self.metadata.file_infos, only_files)?;
        Ok(())
    }

    pub(crate) fn set_piece_priorities(
        &mut self,
        priorities: impl IntoIterator<Item = (ValidPieceIndex, PiecePriority)>,
    ) {
        self.chunk_tracker.set_piece_priorities(priorities);
    }

    pub(crate) fn hns(&self) -> &HaveNeededSelected {
        self.chunk_tracker.get_hns()
    }
}
