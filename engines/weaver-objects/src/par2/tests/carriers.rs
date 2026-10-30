// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

fn rows(object: &Par2) -> usize {
    object.sets.values().map(|set| set.recovery.len()).sum()
}

fn post(
    object: &mut dyn Object,
    volume: VolumeIndex,
    offset: u64,
    bytes: &[u8],
    provenance: Provenance,
) {
    object.deliver(
        weaver_graph::Delivery {
            volume,
            offset,
            bytes,
            provenance,
        },
        &mut |_, _| false,
        &mut Vec::new(),
    );
}

fn fixture() -> (Par2, Vec<u8>, ByteRange, ByteRange) {
    let body = payload(117, 4096);
    let files = par2_set("carrier", &[("payload.bin", &body)], BLOCK, 4, 1);
    assert_eq!(files.len(), 2);
    let mut object = Par2::new(OWN);
    object.bound(
        VolumeIndex(0),
        &files[0].0,
        &[],
        Some(files[0].1.len() as u64),
    );
    object.sized(VolumeIndex(0), files[0].1.len() as u64, &mut Vec::new());
    post(
        &mut object,
        VolumeIndex(0),
        0,
        &files[0].1,
        Provenance::Fresh,
    );
    object.volume_closed(VolumeIndex(0), &mut Vec::new());
    let bytes = files[1].1.clone();
    let mut at = 0;
    let first = loop {
        let length = u64::from_le_bytes(bytes[at + 8..at + 16].try_into().unwrap()) as usize;
        if &bytes[at + 48..at + 64] == b"PAR 2.0\0RecvSlic" {
            break ByteRange::at(at as u64, length as u64);
        }
        at += length;
    };
    let hole = ByteRange::new(first.start + 100, first.start + 200);
    object.bound(VolumeIndex(1), &files[1].0, &[], None);
    (object, bytes, first, hole)
}

#[test]
fn closing_a_known_carrier_steps_over_only_absent_bytes() {
    let (mut object, bytes, first, hole) = fixture();
    object.sized(VolumeIndex(1), bytes.len() as u64, &mut Vec::new());
    post(
        &mut object,
        VolumeIndex(1),
        0,
        &bytes[..hole.start as usize],
        Provenance::Damaged,
    );
    post(
        &mut object,
        VolumeIndex(1),
        hole.end,
        &bytes[hole.end as usize..],
        Provenance::Damaged,
    );
    assert_eq!(
        rows(&object),
        0,
        "an open out-of-order carrier waits for its missing bytes"
    );
    assert!(object.volumes[1].lost.is_empty());
    object.volume_closed(VolumeIndex(1), &mut Vec::new());
    assert_eq!(
        rows(&object),
        3,
        "three intact packets beyond the absent range remain usable despite transport damage"
    );
    assert_eq!(
        object.volumes[1].lost.iter().copied().collect::<Vec<_>>(),
        vec![hole]
    );
    object.volume_closed(VolumeIndex(1), &mut Vec::new());
    assert_eq!(
        rows(&object),
        3,
        "repeated closure does not count packets twice"
    );
    object.unposted(VolumeIndex(1), first);
    assert!(!object.volumes[1].closed);
    assert!(object.volumes[1].lost.is_empty());
    post(
        &mut object,
        VolumeIndex(1),
        first.start,
        &bytes[first.start as usize..first.end as usize],
        Provenance::Repaired,
    );
    object.volume_closed(VolumeIndex(1), &mut Vec::new());
    assert_eq!(
        rows(&object),
        4,
        "rewriting the lost packet uses the existing unposted/rewalk path"
    );
}

#[test]
fn an_unknown_carrier_end_is_not_guessed_when_it_closes() {
    let (mut object, bytes, _, hole) = fixture();
    post(
        &mut object,
        VolumeIndex(1),
        0,
        &bytes[..hole.start as usize],
        Provenance::Fresh,
    );
    post(
        &mut object,
        VolumeIndex(1),
        hole.end,
        &bytes[hole.end as usize..],
        Provenance::Fresh,
    );
    object.volume_closed(VolumeIndex(1), &mut Vec::new());
    assert_eq!(rows(&object), 0);
    assert!(object.volumes[1].lost.is_empty());
    let facts = object.facts();
    object.sized(VolumeIndex(1), bytes.len() as u64, &mut Vec::new());
    assert_eq!(
        rows(&object),
        3,
        "authoritative length arriving after closure establishes the absent range"
    );
    let mut restored = crate::restore(&facts, weaver_graph::Budget::default()).unwrap();
    restored.sized(VolumeIndex(1), bytes.len() as u64, &mut Vec::new());
    post(
        &mut *restored,
        VolumeIndex(1),
        0,
        &bytes[..hole.start as usize],
        Provenance::Fresh,
    );
    post(
        &mut *restored,
        VolumeIndex(1),
        hole.end,
        &bytes[hole.end as usize..],
        Provenance::Fresh,
    );
    restored.volume_closed(VolumeIndex(1), &mut Vec::new());
    let mut sets = Vec::new();
    restored.recovery().unwrap().sets(&mut sets);
    assert_eq!(sets.iter().map(|set| set.blocks_reachable).sum::<u64>(), 3);
}

#[test]
fn damaged_but_present_carrier_bytes_are_still_checked_after_restore() {
    let (mut object, bytes, _, _) = fixture();
    object.sized(VolumeIndex(1), bytes.len() as u64, &mut Vec::new());
    post(&mut object, VolumeIndex(1), 0, &bytes, Provenance::Damaged);
    object.volume_closed(VolumeIndex(1), &mut Vec::new());
    assert_eq!(rows(&object), 4);
    assert!(object.volumes[1].lost.is_empty());
    let mut restored = crate::restore(&object.facts(), weaver_graph::Budget::default()).unwrap();
    restored.sized(VolumeIndex(1), bytes.len() as u64, &mut Vec::new());
    post(
        &mut *restored,
        VolumeIndex(1),
        0,
        &bytes,
        Provenance::Damaged,
    );
    restored.volume_closed(VolumeIndex(1), &mut Vec::new());
    let mut sets = Vec::new();
    restored.recovery().unwrap().sets(&mut sets);
    assert_eq!(sets.iter().map(|set| set.blocks_reachable).sum::<u64>(), 4);
}
