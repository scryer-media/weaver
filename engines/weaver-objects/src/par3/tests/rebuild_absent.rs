// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A file no member carries is rebuilt whole from the blocks its set holds:
// from recovery alone, with a block the set carries as data, and from one
// block the file names in many places.

use par3_rs::{
    ChunkDescription, ChunkTail, DataPacket, FilePacket, Packet, PacketBody, RootPacket,
};
use weaver_graph::recovery::RepairOutcome;
use weaver_graph::{Budget, ByteRange, MemberId, ObjectId, Recovery};

use super::super::Par3;
use crate::testing::{Job, par3_set, payload};

const OWN: ObjectId = ObjectId(11);

fn bind(job: &mut Job<Par3>) {
    let mut renamed = Vec::new();
    job.object.bind(&mut job.namespace, &mut renamed);
}

// A file announced at its described length with none of its bytes, as the
// namespace announces a file no member carries.
fn absent(job: &mut Job<Par3>, name: &str, length: u64) -> MemberId {
    let member = job.namespace.announce(name, Some(length));
    job.store.members.insert(member, vec![0; length as usize]);
    job.store
        .holes
        .insert(member, vec![ByteRange::new(0, length)]);
    member
}

// A file nothing of was posted, beside one that was, is rebuilt whole from
// a set that holds exactly as many recovery blocks as the file has blocks,
// the least it can be rebuilt from, whatever the file's length.
#[test]
fn an_honest_rebuild_from_recovery_alone_completes() {
    const BLOCK: u64 = 1024;
    for (seed, length) in [
        (1, 1),
        (2, 39),
        (3, 40),
        (4, 1024),
        (5, 5000),
        (6, 9 * 1024 + 7),
    ] {
        let kept = payload(seed * 2, 3_000 + seed as usize * 311);
        let gone = payload(seed * 2 + 1, length);
        let blocks = (length as u64).div_ceil(BLOCK);
        let files = par3_set(
            "set",
            &[("kept.bin", kept.as_slice()), ("gone.bin", gone.as_slice())],
            BLOCK,
            blocks,
        );
        let mut job = Job::new(Par3::new(OWN), OWN);
        for (name, bytes) in &files {
            job.volume(name, bytes, 700, false);
        }
        let posted = job.member("kept.bin", &kept);
        let member = absent(&mut job, "gone.bin", length as u64);
        bind(&mut job);
        assert!(job.object.covers(member), "length {length}");
        let outcome = job.object.repair(&mut job.store, Budget::default());
        assert!(
            matches!(outcome, RepairOutcome::Repaired { .. }),
            "length {length}: {outcome:?}"
        );
        assert_eq!(job.store.members[&member], gone, "length {length}");
        assert_eq!(job.store.members[&posted], kept, "length {length}");
    }
}

// A file nothing of was posted that the namespace has no room for is
// withheld: the set solves its blocks with the rest, writes nothing into
// it, repairs the damaged file that arrived beside it, and says it could
// not repair what it covers, so it vouches for nothing it did not write.
// The withheld file is three blocks and a tail its set keeps inline.
#[test]
fn a_withheld_absent_file_gets_nothing_and_the_arrived_file_is_repaired() {
    const BLOCK: u64 = 1024;
    let kept = payload(41, 4 * BLOCK as usize);
    let gone = payload(42, 3 * BLOCK as usize + 10);
    let files = par3_set(
        "set",
        &[("kept.bin", kept.as_slice()), ("gone.bin", gone.as_slice())],
        BLOCK,
        5,
    );
    let mut job = Job::new(Par3::new(OWN), OWN);
    for (name, bytes) in &files {
        job.volume(name, bytes, 700, false);
    }
    let posted = job.member("kept.bin", &kept);
    job.store.members.get_mut(&posted).unwrap()[BLOCK as usize + 3] ^= 0x5a;
    let member = absent(&mut job, "gone.bin", gone.len() as u64);
    job.store.withheld.insert(member);
    bind(&mut job);
    assert!(job.object.covers(member));
    let outcome = job.object.repair(&mut job.store, Budget::default());
    assert_eq!(
        outcome,
        RepairOutcome::Impossible {
            damaged: 3,
            reachable: 5,
        }
    );
    // The ruling stands, so the set is not due to rule again.
    assert_eq!(job.object.outcome(), Some(outcome));
    assert_eq!(
        job.store.members[&posted], kept,
        "the arrived file is repaired"
    );
    assert_eq!(job.store.members[&member], vec![0; gone.len()]);
    assert_eq!(
        job.store.holes[&member],
        vec![ByteRange::new(0, gone.len() as u64)],
        "nothing is written into the withheld file"
    );
}

// An input block the set carries as data is one the rebuild need not
// solve: a file of seven blocks, one carried, is rebuilt from six recovery
// blocks.
#[test]
fn a_carried_block_and_recovery_rebuild_an_absent_file() {
    const BLOCK: u64 = 1024;
    let gone = payload(91, 6 * BLOCK as usize + 100);
    let mut files = par3_set("carry", &[("gone.bin", gone.as_slice())], BLOCK, 6);
    let id = par3_rs::scan_packets(&files[0].1).unwrap()[0]
        .1
        .input_set_id();
    let carried = Packet::new(
        id,
        PacketBody::Data(DataPacket {
            block_index: 0,
            data: gone[..BLOCK as usize].to_vec(),
        }),
    );
    files[0].1.extend_from_slice(&carried.to_bytes());
    let mut job = Job::new(Par3::new(OWN), OWN);
    for (name, bytes) in &files {
        job.volume(name, bytes, 700, false);
    }
    let member = absent(&mut job, "gone.bin", gone.len() as u64);
    bind(&mut job);
    let outcome = job.object.repair(&mut job.store, Budget::default());
    assert!(
        matches!(outcome, RepairOutcome::Repaired { .. }),
        "{outcome:?}"
    );
    assert_eq!(job.store.members[&member], gone);
}

// A file of ten blocks that are all one input block, as a set that stores
// a repeated block once describes it, is rebuilt whole from the one
// recovery block that block needs, though the file is ten blocks long.
#[test]
fn an_absent_file_of_one_block_named_ten_times_rebuilds_from_one_recovery_block() {
    const BLOCK: u64 = 1024;
    const TIMES: usize = 10;
    let block = payload(57, BLOCK as usize);
    // A set over that one block, whose start, matrix and recovery block the
    // set below shares: a recovery block is the same bytes whatever file
    // names the input block it covers.
    let made = par3_set("one", &[("one.bin", block.as_slice())], BLOCK, 1);
    let packets: Vec<Packet> = made
        .iter()
        .flat_map(|(_, bytes)| par3_rs::scan_packets(bytes).unwrap())
        .map(|(_, packet)| packet)
        .collect();
    let find = |want: fn(&PacketBody) -> bool| {
        packets
            .iter()
            .find(|packet| want(packet.body()))
            .cloned()
            .expect("the made set holds the packet")
    };
    let start = find(|body| matches!(body, PacketBody::Start(_)));
    let matrix = find(|body| matches!(body, PacketBody::CauchyMatrix(_)));
    let PacketBody::RecoveryData(mut recovery) =
        find(|body| matches!(body, PacketBody::RecoveryData(_))).into_body()
    else {
        unreachable!()
    };
    let id = start.input_set_id();
    let file = Packet::new(
        id,
        PacketBody::File(FilePacket {
            name: "gone.bin".to_owned(),
            quick_rolling_hash: 0,
            fingerprint: [0; 16],
            option_hashes: Vec::new(),
            chunks: (0..TIMES)
                .map(|_| ChunkDescription::Protected {
                    length: BLOCK,
                    first_block_index: Some(0),
                    tail: ChunkTail::None,
                })
                .collect(),
        }),
    );
    let root = Packet::new(
        id,
        PacketBody::Root(RootPacket {
            lowest_unused_block_index: 1,
            attributes: 0,
            option_hashes: Vec::new(),
            children: vec![file.hash()],
        }),
    );
    recovery.root_hash = root.hash();
    recovery.matrix_hash = matrix.hash();
    let recovery = Packet::new(id, PacketBody::RecoveryData(recovery));
    let index: Vec<u8> = [&start, &root, &file, &matrix]
        .into_iter()
        .flat_map(Packet::to_bytes)
        .collect();
    let mut volume = index.clone();
    volume.extend_from_slice(&recovery.to_bytes());

    let mut job = Job::new(Par3::new(OWN), OWN);
    job.volume("gone.par3", &index, 700, false);
    job.volume("gone.vol0+1.par3", &volume, 700, false);
    let member = absent(&mut job, "gone.bin", BLOCK * TIMES as u64);
    bind(&mut job);
    assert!(job.object.covers(member));
    let outcome = job.object.repair(&mut job.store, Budget::default());
    assert!(
        matches!(outcome, RepairOutcome::Repaired { .. }),
        "{outcome:?}"
    );
    assert_eq!(job.store.members[&member], block.repeat(TIMES));
}
