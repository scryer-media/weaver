// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A PAR volume as a recovery object fetches it: what its name says it
// carries, and the ranges of it still to come.

use weaver_graph::{ByteRange, Coverage};

// The recovery blocks a volume's name says it carries, as every PAR creator
// names a volume: `<stem>.vol<start>+<count>` and the format's `extension`,
// so `name.vol07+08.par2` carries eight.
pub(crate) fn named_blocks(name: &str, extension: &str) -> Option<u64> {
    let lower = name.to_ascii_lowercase();
    let (_, tail) = lower.strip_suffix(extension)?.rsplit_once(".vol")?;
    let (start, count) = tail.split_once('+')?;
    start.parse::<u64>().ok()?;
    count.parse().ok()
}

// Each range of a volume not yet in hand, the last open-ended: the volume's
// length is the parent's to know, and the root clamps an open end to it.
pub(crate) fn gaps(arrived: &Coverage, mut each: impl FnMut(ByteRange)) {
    let mut cursor = 0;
    for run in arrived.runs() {
        if run.range.start > cursor {
            each(ByteRange::new(cursor, run.range.start));
        }
        cursor = run.range.end;
    }
    each(ByteRange::new(cursor, u64::MAX));
}

#[cfg(test)]
mod tests {
    use super::named_blocks;

    #[test]
    fn a_volume_name_says_how_many_blocks_it_carries() {
        assert_eq!(named_blocks("set.vol07+08.par2", ".par2"), Some(8));
        assert_eq!(named_blocks("SET.VOL000+128.PAR2", ".par2"), Some(128));
        assert_eq!(named_blocks("set.vol0+4.par3", ".par3"), Some(4));
        assert_eq!(named_blocks("set.vol07+08.par2", ".par3"), None);
        assert_eq!(named_blocks("set.par2", ".par2"), None);
        assert_eq!(named_blocks("set.vol1.par2", ".par2"), None);
        assert_eq!(named_blocks("set.volx+4.par2", ".par2"), None);
    }
}
