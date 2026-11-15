// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use weaver_graph::{Facts, Kind, Seek, When};

use super::super::facts::{Par2Facts, VolumeFacts, par2};
use super::*;

// The bytes of a volume's front that arrive with its first article: one
// packet header, so no recovery block is whole in hand.
const FRONT: usize = 64;

struct Post {
    job: Job<Par2>,
    data: Vec<u8>,
    member: MemberId,
    volumes: Vec<(VolumeIndex, Vec<u8>)>,
}

fn post_at(object: &mut Par2, volume: VolumeIndex, offset: u64, bytes: &[u8]) {
    object.deliver(
        weaver_graph::Delivery {
            volume,
            offset,
            bytes: &weaver_graph::Evidenced::from(bytes.to_vec()),
            provenance: Provenance::Fresh,
        },
        &mut |_, _| false,
        &mut Vec::new(),
    );
}

// A set with one damaged slice whose recovery volumes are named
// `set.vol-01.par2`, `set.vol-02.par2`: names that carry no block count.
// The index arrived whole; of each volume only its front arrived.
fn countless_post(seed: u64) -> Post {
    let data = payload(seed, 5000);
    let files = par2_set("set", &[("payload.bin", &data)], BLOCK, 4, 2);
    assert!(files.len() >= 3);
    let mut job = job(&files[..1]);
    let mut volumes = Vec::new();
    for (at, (_, bytes)) in files[1..].iter().enumerate() {
        let name = format!("set.vol-{:02}.par2", at + 1);
        assert_eq!(crate::recovery_volume::named_blocks(&name, ".par2"), None);
        let volume = job.name(&name, bytes);
        job.object
            .sized(volume, bytes.len() as u64, &mut Vec::new());
        post_at(&mut job.object, volume, 0, &bytes[..FRONT]);
        volumes.push((volume, bytes.clone()));
    }
    let (member, bad) = damaged_member(&mut job, "payload.bin", &data, 300);
    bind(&mut job);
    let wrong = job.store.members[&member].clone();
    feed(&mut job, member, &wrong, Some(bad));
    assert_eq!(
        job.object
            .sets
            .values()
            .map(|state| state.recovery.len())
            .sum::<usize>(),
        0,
        "no recovery block is in hand"
    );
    Post {
        job,
        data,
        member,
        volumes,
    }
}

fn asked_now(object: &Par2) -> Vec<VolumeIndex> {
    let mut seeks: Vec<Seek> = Vec::new();
    Recovery::wants(object, &mut seeks);
    let mut now: Vec<VolumeIndex> = seeks
        .iter()
        .filter(|seek| seek.when == When::Now)
        .map(|seek| seek.volume)
        .collect();
    now.sort();
    now.dedup();
    now
}

fn repair(job: &mut Job<Par2>) -> RepairOutcome {
    job.object
        .repair(&mut job.store, weaver_graph::Budget::default())
}

// Recovery volumes whose names carry no block count, only their fronts in
// hand: the set waits for them and asks for their bytes. Once the rest of
// one arrives, the repair rebuilds the slice.
#[test]
fn a_set_waits_on_volumes_whose_names_carry_no_block_count() {
    let Post {
        mut job,
        data,
        member,
        volumes,
    } = countless_post(71);
    assert_eq!(repair(&mut job), RepairOutcome::Waiting);
    assert_eq!(job.object.outcome(), Some(RepairOutcome::Waiting));
    let mut every: Vec<VolumeIndex> = volumes.iter().map(|(volume, _)| *volume).collect();
    every.sort();
    assert_eq!(asked_now(&job.object), every);

    let (volume, bytes) = &volumes[0];
    for (at, piece) in bytes[FRONT..].chunks(ARTICLE as usize).enumerate() {
        let offset = FRONT as u64 + at as u64 * ARTICLE;
        post_at(&mut job.object, *volume, offset, piece);
    }
    job.object.volume_closed(*volume, &mut Vec::new());
    assert_eq!(repair(&mut job), RepairOutcome::Repaired { slices: 1 });
    assert!(job.store.members[&member] == data);
}

// The same volumes lost past their fronts and closed: the set waits while
// one may still bring a block, and once none may, the repair is
// impossible.
#[test]
fn a_set_short_once_its_countless_volumes_close_is_impossible() {
    let Post {
        mut job, volumes, ..
    } = countless_post(72);
    assert_eq!(repair(&mut job), RepairOutcome::Waiting);
    let lose = |job: &mut Job<Par2>, (volume, bytes): &(VolumeIndex, Vec<u8>)| {
        let mut out = Vec::new();
        let rest = ByteRange::new(FRONT as u64, bytes.len() as u64);
        job.object.volume_lost(*volume, rest, &mut out);
        job.object.volume_closed(*volume, &mut out);
    };
    let (last, others) = volumes.split_last().expect("volumes");
    for volume in others {
        lose(&mut job, volume);
        assert_eq!(job.object.outcome(), Some(RepairOutcome::Waiting));
    }
    lose(&mut job, last);
    assert_eq!(
        job.object.outcome(),
        None,
        "due again once nothing may come"
    );
    assert_eq!(
        repair(&mut job),
        RepairOutcome::Impossible {
            damaged: 1,
            reachable: 0
        }
    );
    assert!(asked_now(&job.object).is_empty());
}

// A volume read up to its end before it closes may bring nothing more:
// what its bytes past the walk could hold bounds what it may still bring.
#[test]
fn a_countless_volume_walked_to_its_end_brings_nothing_more() {
    let Post {
        mut job, volumes, ..
    } = countless_post(73);
    let packet = job.object.smallest_packet();
    let (volume, bytes) = &volumes[0];
    let held = &job.object.volumes[volume.0 as usize];
    assert!(held.left(packet) > 0);
    assert!(held.left(packet) <= (bytes.len() as u64).div_ceil(packet.unwrap()));
    assert_eq!(held.coming(packet), 0, "a count-less name promises none");
    post_at(&mut job.object, *volume, FRONT as u64, &bytes[FRONT..]);
    let held = &job.object.volumes[volume.0 as usize];
    assert!(!held.closed);
    assert_eq!(held.left(packet), 0);
}

// The facts of the waiting set restore to a set that still waits, and
// facts written before a volume kept its length, whose volumes restore
// with no length, wait too while a count-less volume is open.
#[test]
fn a_waiting_set_of_countless_volumes_waits_after_a_restore() {
    let Post { mut job, .. } = countless_post(74);
    assert_eq!(repair(&mut job), RepairOutcome::Waiting);
    let facts = job.object.facts();

    let restored = par2(&facts).expect("restores");
    assert!(restored.facts() == facts);
    let mut again = countless_post(74);
    again.job.object = restored;
    assert_eq!(again.job.object.outcome(), Some(RepairOutcome::Waiting));
    assert_eq!(repair(&mut again.job), RepairOutcome::Waiting);

    let mut kept: Par2Facts = facts.read(Kind::Par2).expect("decodes");
    for volume in &mut kept.volumes {
        volume.length = None;
    }
    let mut old = countless_post(74);
    old.job.object = par2(&Facts::of(Kind::Par2, &kept)).expect("restores");
    assert_eq!(old.job.object.outcome(), Some(RepairOutcome::Waiting));
    assert_eq!(repair(&mut old.job), RepairOutcome::Waiting);
    assert!(!asked_now(&old.job.object).is_empty());
}

// Volume facts encoded before the length was kept still decode, with no
// length.
#[test]
fn par2_volume_facts_written_without_a_length_still_decode() {
    use std::collections::BTreeSet;

    #[derive(serde::Serialize)]
    struct Before {
        front: u64,
        walk_lost: Vec<ByteRange>,
        arrived: weaver_graph::Coverage,
        lost: Vec<ByteRange>,
        closed: bool,
        named_blocks: Option<u64>,
        walked_blocks: u64,
        sets: BTreeSet<[u8; 16]>,
    }

    let before = Before {
        front: 64,
        walk_lost: Vec::new(),
        arrived: weaver_graph::Coverage::new(),
        lost: Vec::new(),
        closed: false,
        named_blocks: None,
        walked_blocks: 0,
        sets: BTreeSet::new(),
    };
    let bytes = rmp_serde::to_vec(&before).expect("encodes");
    let decoded: VolumeFacts = rmp_serde::from_slice(&bytes).expect("decodes");
    assert_eq!(decoded.length, None);
}
