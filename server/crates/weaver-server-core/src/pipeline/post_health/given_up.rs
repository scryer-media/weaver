// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The articles of one post that will never arrive. Each is written down
// where it is given up, and a restored root lays back the ones it holds
// lost from a run before the restart. Everything the feed weighs a post on
// is kept as it changes, so reading it costs nothing of the post's size.

use std::collections::btree_map::BTreeMap;
use std::collections::hash_map::{Entry, HashMap};

use crate::jobs::ids::SegmentId;
use crate::pipeline::SegmentTerminalState;

// What one file of the post lost.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(in crate::pipeline) struct FileLosses {
    // Articles every server said are gone.
    pub(in crate::pipeline) missing: usize,
    // Articles given up for any end but the feed's having stopped asking.
    pub(in crate::pipeline) lost: usize,
    // The health-counted bytes of every article given up.
    pub(in crate::pipeline) bytes: u64,
}

#[derive(Debug, Default)]
pub(in crate::pipeline) struct GivenUp {
    // Each article with the end this run saw it come to: `None` for one
    // lost in a run before a restart, which every server had failed.
    articles: HashMap<SegmentId, Option<SegmentTerminalState>>,
    // By file index.
    files: BTreeMap<u32, FileLosses>,
    bytes: u64,
    missing_bytes: u64,
}

impl GivenUp {
    // The article will never arrive, after `end`, and weighs `bytes`. The
    // first end an article reaches is the one it holds, so an article told
    // again, by a later path or by a restore, is already held.
    pub(in crate::pipeline) fn give_up(
        &mut self,
        segment: SegmentId,
        end: Option<SegmentTerminalState>,
        bytes: u64,
    ) {
        match self.articles.entry(segment) {
            Entry::Occupied(_) => return,
            Entry::Vacant(slot) => {
                slot.insert(end);
            }
        }
        let file = self.files.entry(segment.file_id.file_index).or_default();
        file.bytes += bytes;
        self.bytes += bytes;
        if end != Some(SegmentTerminalState::Abandoned) {
            file.lost += 1;
        }
        if end.is_none_or(|end| end == SegmentTerminalState::Missing) {
            file.missing += 1;
            self.missing_bytes += bytes;
        }
    }

    pub(in crate::pipeline) fn holds(&self, segment: SegmentId) -> bool {
        self.articles.contains_key(&segment)
    }

    // The health-counted bytes of every article given up.
    pub(in crate::pipeline) fn bytes(&self) -> u64 {
        self.bytes
    }

    // The health-counted bytes of the articles every server said are gone.
    pub(in crate::pipeline) fn missing_bytes(&self) -> u64 {
        self.missing_bytes
    }

    pub(in crate::pipeline) fn file(&self, index: u32) -> FileLosses {
        self.files.get(&index).copied().unwrap_or_default()
    }

    // The files that lost an article every server said is gone.
    pub(in crate::pipeline) fn missing_files(&self) -> impl Iterator<Item = u32> + '_ {
        self.files
            .iter()
            .filter(|(_, file)| file.missing > 0)
            .map(|(index, _)| *index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::ids::{JobId, NzbFileId};

    fn segment(file_index: u32, segment_number: u32) -> SegmentId {
        SegmentId {
            file_id: NzbFileId {
                job_id: JobId(1),
                file_index,
            },
            segment_number,
        }
    }

    #[test]
    fn an_article_told_again_is_held_once_at_its_first_end() {
        let mut given_up = GivenUp::default();
        given_up.give_up(segment(0, 1), None, 100);
        given_up.give_up(segment(0, 1), Some(SegmentTerminalState::Abandoned), 100);
        given_up.give_up(segment(0, 2), Some(SegmentTerminalState::Abandoned), 50);
        given_up.give_up(segment(0, 2), Some(SegmentTerminalState::Missing), 50);
        given_up.give_up(segment(1, 0), Some(SegmentTerminalState::Unservable), 10);
        assert_eq!(given_up.bytes(), 160);
        assert_eq!(given_up.missing_bytes(), 100);
        assert_eq!(
            given_up.file(0),
            FileLosses {
                missing: 1,
                lost: 1,
                bytes: 150
            }
        );
        assert_eq!(given_up.missing_files().collect::<Vec<_>>(), vec![0]);
        assert!(given_up.holds(segment(1, 0)));
        assert!(!given_up.holds(segment(1, 1)));
    }
}
