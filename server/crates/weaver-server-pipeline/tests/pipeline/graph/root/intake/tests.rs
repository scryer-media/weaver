// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

fn segment(start: u64, end: u64, crc32: u32) -> Segment {
    Segment {
        range: ByteRange::new(start, end),
        crc32,
        verified: true,
    }
}

// An article delivered again replaces the checksums its range overlaps,
// straight out of the kept map: the same checksums kept and the same
// starts marked, and nothing allocated to find them.
#[test]
fn a_redelivery_replaces_what_it_overlaps_without_allocating() {
    let volume = VolumeIndex(3);
    let mut checksums = Checksums::default();
    checksums.record(
        volume,
        &[
            segment(0, 10, 1),
            segment(10, 20, 2),
            segment(20, 30, 3),
            segment(30, 40, 4),
        ],
    );
    let marked = checksums.marks[&volume].at();
    let redelivery = [segment(0, 25, 9)];

    let before = crate::alloc_probe::allocations();
    checksums.record(volume, &redelivery);
    assert_eq!(
        crate::alloc_probe::allocations(),
        before,
        "replacing overlapped checksums allocates nothing"
    );

    assert_eq!(checksums.kept, 2);
    let mut held = Vec::new();
    checksums.segments_for(NodeId::ROOT, volume, ByteRange::new(0, 40), &mut held);
    assert_eq!(held, vec![segment(0, 25, 9), segment(30, 40, 4)]);
    assert_eq!(
        checksums.marks[&volume].since(marked).collect::<Vec<_>>(),
        vec![20, 10, 0],
        "each overlapped start is marked, nearest first; the new segment \
             marks its start again last"
    );
}
