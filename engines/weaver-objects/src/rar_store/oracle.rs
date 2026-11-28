// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What the store's placement number promises, checked: every volume the
// store does not say moved since a number it gave answers `locate` as it
// did when it gave it. Each time a reader may ask, the answers for the whole
// of every volume held are kept beside the number; the next time, every
// volume not listed as moved since is asked again over the same bytes.

use std::collections::BTreeMap;

use weaver_graph::{ByteRange, Placement, PlacementMoves, VolumeIndex};

use super::{RarStore, Volume};

// Each volume's answer over its bytes from the first to `end`.
pub(super) type Answers = BTreeMap<VolumeIndex, (u64, Vec<Placement>)>;

// As far as anything the store knows of a volume reaches.
fn end(found: &Volume) -> u64 {
    let kept = found
        .kept
        .runs()
        .last()
        .map_or(0, |(range, _, _)| range.end);
    let blocks = found
        .walk
        .blocks
        .iter()
        .map(|block| block.data.end)
        .max()
        .unwrap_or(0);
    [
        found.length.unwrap_or(0),
        found.walk.reach().unwrap_or(0),
        kept,
        blocks,
    ]
    .into_iter()
    .max()
    .unwrap_or(0)
}

impl RarStore {
    fn answer(&self, volume: VolumeIndex, end: u64) -> Vec<Placement> {
        let mut out = Vec::new();
        if end > 0 {
            self.place(volume, ByteRange::new(0, end), &mut out);
        }
        out
    }

    // Every volume not said moved since the last check answers as it did
    // then; then the answers as they stand now are kept for the next.
    pub(super) fn check_moves(&self) {
        let mut kept = self
            .oracle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some((number, answers)) = kept.take()
            && let PlacementMoves::Volumes(moved) = self.placed.since(number)
        {
            for (volume, (end, answer)) in answers {
                if moved.contains(&volume) {
                    continue;
                }
                assert_eq!(
                    self.answer(volume, end),
                    answer,
                    "{volume:?} answers otherwise than at {number} and is not said moved since"
                );
            }
        }
        let answers = self
            .volumes
            .iter()
            .map(|(&volume, found)| {
                let end = end(found);
                (volume, (end, self.answer(volume, end)))
            })
            .collect();
        *kept = Some((self.placed.number(), answers));
    }
}
