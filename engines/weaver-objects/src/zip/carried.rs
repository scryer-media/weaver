// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A recovery set the container carries inside itself. An inside-insertion
// writes the archive, then the set, then a copy of the archive's end
// records, so the end record at the tail describes records that close
// before the container does, and the set's first packet opens where they
// close. Only the end record, and a zip64 archive's record, say where that
// is: the set is found whether the directory reads, the walk reads the
// local headers instead, or neither has yet. It is offered over the rest
// of the container, which it covers, and repairs it in place.

use weaver_graph::{ByteRange, Emit, Handed, VolumeIndex};

use super::Zip;
use super::records::{self, END64_LEN, LOCATOR_LEN, TAIL};
use crate::RECOVERY;

// A zip64 record's fields ahead of the size it states, which counts the rest.
const END64_LEAD: u64 = 12;

impl Zip {
    pub(super) fn find_recovery(&mut self, out: &mut Vec<Emit>) {
        if self.recovery_said {
            return;
        }
        let Some(total) = self.total() else {
            return;
        };
        let Some(closed) = self.closed(total) else {
            return;
        };
        let Some(closed) = closed.filter(|closed| *closed < total) else {
            self.recovery_said = true;
            return;
        };
        let probe = ByteRange::new(closed, total.min(closed + 8));
        self.want(probe, true);
        let Some(bytes) = self.kept.read(probe) else {
            return;
        };
        self.recovery_said = true;
        let Some((_, kind)) = RECOVERY.iter().find(|(magic, _)| bytes == magic.as_slice()) else {
            return;
        };
        let mut pieces = Vec::new();
        self.to_volumes(ByteRange::new(closed, total), &mut pieces);
        out.extend(pieces.into_iter().map(|(volume, range)| Emit::RecoveryAt {
            volume,
            range,
            kind: *kind,
            handed: Handed::default(),
        }));
    }

    // Where the records the tail's end record describes close: past the
    // directory, or past the zip64 record and its locator, then the end
    // record and its comment. The end record is read from the bytes that run
    // unbroken to the container's end, so a gap ahead of it holds nothing
    // up. `None` until the bytes that say are in, and `Some(None)` when they
    // say nothing.
    fn closed(&mut self, total: u64) -> Option<Option<u64>> {
        let tail = ByteRange::new(total.saturating_sub(TAIL), total);
        self.kept.want(tail);
        let bytes = self.kept.suffix(tail);
        let Some(found) = records::find_end(bytes) else {
            return (bytes.len() as u64 == tail.len()).then_some(None);
        };
        let at = total - bytes.len() as u64 + found.at;
        let closing = total - at;
        if !found.wide {
            return Some(
                self.geometry
                    .global(VolumeIndex(found.directory_disk), found.offset)
                    .and_then(|start| start.checked_add(found.size)?.checked_add(closing)),
            );
        }
        let Some(locator) = at
            .checked_sub(LOCATOR_LEN)
            .map(|start| ByteRange::new(start, at))
        else {
            return Some(None);
        };
        self.want(locator, true);
        let Some((disk, offset)) = records::locator(self.kept.read(locator)?) else {
            return Some(None);
        };
        let Some(start) = self
            .geometry
            .global(VolumeIndex(disk), offset)
            .filter(|start| start.checked_add(END64_LEN).is_some())
        else {
            return Some(None);
        };
        let record = ByteRange::at(start, END64_LEN);
        self.want(record, true);
        let bytes = self.kept.read(record)?;
        let size = records::end64(bytes, found).and(records::u64_at(bytes, 4));
        Some(size.and_then(|size| {
            start
                .checked_add(END64_LEAD)?
                .checked_add(size)?
                .checked_add(LOCATOR_LEN + closing)
        }))
    }
}
