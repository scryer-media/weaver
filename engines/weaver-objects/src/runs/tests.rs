// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use weaver_graph::{ByteRange, Provenance, SliceVerdict};

use super::{CrcRuns, gate};
use crate::grid::crc32;

fn bytes() -> Vec<u8> {
    (0..3000u32).map(|index| (index * 7 + 3) as u8).collect()
}

fn fed(cuts: &[(u64, u64)], data: &[u8], provenance: Provenance) -> CrcRuns {
    let mut runs = CrcRuns::default();
    for (start, end) in cuts {
        let range = ByteRange::new(*start, *end);
        runs.feed(
            range,
            crc32(&data[*start as usize..*end as usize]),
            provenance,
        );
    }
    runs
}

#[test]
fn runs_that_tile_compose_to_the_whole_crc_and_a_gap_has_none() {
    let data = bytes();
    let runs = fed(&[(1000, 3000), (0, 1000)], &data, Provenance::Fresh);
    assert_eq!(runs.compose(0, 3000), Some(crc32(&data)));
    assert_eq!(runs.compose(1000, 2000), Some(crc32(&data[1000..])));
    assert_eq!(runs.compose(0, 500), None, "a run is never split");
    let holed = fed(&[(0, 1000), (1100, 3000)], &data, Provenance::Fresh);
    assert_eq!(holed.compose(0, 3000), None);
    assert_eq!(
        gate(&holed, 3000, crc32(&data), false, None).0,
        SliceVerdict::Unknown
    );
}

#[test]
fn a_duplicate_never_advances_the_composition_and_a_repair_replaces() {
    let data = bytes();
    let mut wrong = data.clone();
    wrong[1500] ^= 0xff;
    let mut runs = fed(
        &[(0, 1000), (1000, 2000), (2000, 3000)],
        &wrong,
        Provenance::Fresh,
    );
    assert_eq!(
        gate(&runs, 3000, crc32(&data), false, None).0,
        SliceVerdict::Damaged
    );
    // A duplicate of the damaged run, right this time, is still a duplicate.
    runs.feed(
        ByteRange::new(1000, 2000),
        crc32(&data[1000..2000]),
        Provenance::Fresh,
    );
    assert_eq!(
        gate(&runs, 3000, crc32(&data), false, None).0,
        SliceVerdict::Damaged
    );
    // A repair over part of it leaves the rest of the run unrecorded.
    let gaps = runs.overwrite(1400, 200, crc32(&data[1400..1600]));
    assert_eq!(gaps, vec![(1000, 1400), (1600, 2000)]);
    assert_eq!(runs.compose(0, 3000), None);
    runs.feed(
        ByteRange::new(1000, 2000),
        crc32(&data[1000..2000]),
        Provenance::Repaired,
    );
    assert_eq!(
        gate(&runs, 3000, crc32(&data), false, None).0,
        SliceVerdict::Intact
    );
}

#[test]
fn a_keyed_header_compares_the_folded_crc_and_needs_its_key() {
    let data = bytes();
    let key = [7u8; 32];
    let runs = fed(&[(0, 3000)], &data, Provenance::Fresh);
    let keyed = unrar_rs::convert_crc32_to_mac(crc32(&data), &key);
    assert_eq!(
        gate(&runs, 3000, keyed, true, Some(&key)).0,
        SliceVerdict::Intact
    );
    assert_eq!(
        gate(&runs, 3000, crc32(&data), true, Some(&key)).0,
        SliceVerdict::Damaged
    );
    assert_eq!(
        gate(&runs, 3000, keyed, true, None).0,
        SliceVerdict::Unknown
    );
}

#[test]
fn held_bytes_count_runs_once_and_follow_a_repair() {
    let data = bytes();
    let mut runs = fed(&[(0, 1000), (1000, 3000)], &data, Provenance::Fresh);
    assert_eq!(runs.held(), 3000);
    // A duplicate advances nothing.
    runs.feed(
        ByteRange::new(0, 1000),
        crc32(&data[..1000]),
        Provenance::Fresh,
    );
    assert_eq!(runs.held(), 3000);
    // A repair across two runs holds only the rewritten span.
    runs.feed(
        ByteRange::new(500, 1500),
        crc32(&data[500..1500]),
        Provenance::Repaired,
    );
    assert_eq!(runs.held(), 1000);
    assert_eq!(
        gate(&runs, 3000, crc32(&data), false, None).0,
        SliceVerdict::Unknown
    );
    runs.feed(
        ByteRange::new(0, 500),
        crc32(&data[..500]),
        Provenance::Fresh,
    );
    runs.feed(
        ByteRange::new(1500, 3000),
        crc32(&data[1500..]),
        Provenance::Fresh,
    );
    assert_eq!(runs.held(), 3000);
    assert_eq!(
        gate(&runs, 3000, crc32(&data), false, None).0,
        SliceVerdict::Intact
    );
}

#[test]
fn a_damaged_run_keeps_the_gate_unknown_until_its_repair_is_judged() {
    let data = bytes();
    let mut wrong = data.clone();
    wrong[1500] ^= 0xff;
    for (repair, verdict) in [
        (&data, SliceVerdict::Intact),
        (&wrong, SliceVerdict::Damaged),
    ] {
        let mut runs = fed(&[(0, 1000), (2000, 3000)], &data, Provenance::Fresh);
        runs.feed(
            ByteRange::new(1000, 2000),
            crc32(&wrong[1000..2000]),
            Provenance::Damaged,
        );
        assert_eq!(runs.held(), 3000, "damaged bytes are in");
        assert_eq!(runs.compose(0, 3000), None);
        assert_eq!(
            gate(&runs, 3000, crc32(&data), false, None).0,
            SliceVerdict::Unknown,
            "nothing is said over damaged bytes"
        );
        // A clean duplicate is still a duplicate: only a repair replaces.
        runs.feed(
            ByteRange::new(1000, 2000),
            crc32(&data[1000..2000]),
            Provenance::Fresh,
        );
        assert_eq!(
            gate(&runs, 3000, crc32(&data), false, None).0,
            SliceVerdict::Unknown
        );
        runs.feed(
            ByteRange::new(1000, 2000),
            crc32(&repair[1000..2000]),
            Provenance::Repaired,
        );
        assert_eq!(gate(&runs, 3000, crc32(&data), false, None).0, verdict);
    }
}
