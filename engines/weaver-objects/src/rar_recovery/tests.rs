// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use weaver_graph::Provenance;

use super::*;
use crate::grid::crc32;
use crate::testing::{Job, payload};

const OWN: ObjectId = ObjectId(9);

// A RAR5 recovery volume's header and no parity: the counts, its row, and a
// table of the data volumes' sizes when `sizes` names them.
fn rev5(data: u16, rec: u16, row: u16, sizes: &[u64]) -> Vec<u8> {
    let mut raw = vec![1u8];
    raw.extend_from_slice(&data.to_le_bytes());
    raw.extend_from_slice(&rec.to_le_bytes());
    raw.extend_from_slice(&row.to_le_bytes());
    raw.extend_from_slice(&0u32.to_le_bytes());
    for size in sizes {
        raw.extend_from_slice(&size.to_le_bytes());
        raw.extend_from_slice(&0u32.to_le_bytes());
    }
    let size = (raw.len() as u32).to_le_bytes();
    let mut rev = REV5_SIGN.to_vec();
    rev.extend_from_slice(&crc32(&[size.as_slice(), raw.as_slice()].concat()).to_le_bytes());
    rev.extend_from_slice(&size);
    rev.extend_from_slice(&raw);
    rev.extend_from_slice(&[0x5A; 40]);
    rev
}

// A RAR3 recovery volume: parity, then the footer.
fn rev3(data: u8, width: usize) -> Vec<u8> {
    let mut rev = payload(3, width);
    rev.extend_from_slice(&[data - 1, 0, 0]);
    let crc = crc32(&rev);
    rev.extend_from_slice(&crc.to_le_bytes());
    rev
}

fn job() -> Job<RarRecovery> {
    Job::new(RarRecovery::new(OWN), OWN)
}

// A recovery volume as the tree hands one over: claimed, bound, sized,
// delivered in articles, closed.
fn rev(job: &mut Job<RarRecovery>, name: &str, bytes: &[u8]) -> Option<VolumeIndex> {
    let front = &bytes[..bytes.len().min(64)];
    let volume = match job.object.claim(name, front) {
        Some(volume) => volume,
        None if job.store.volumes.is_empty() => VolumeIndex(0),
        None => return None,
    };
    let member = job.namespace.announce(name, Some(bytes.len() as u64));
    job.arrive(member, 0, bytes, Provenance::Fresh);
    job.namespace.bind(member, OWN);
    job.object.bound(volume, name, front);
    job.store.volumes.insert(volume, bytes.to_vec());
    let mut out = Vec::new();
    job.object.sized(volume, bytes.len() as u64, &mut out);
    for (at, piece) in bytes.chunks(16).enumerate() {
        job.object.deliver(
            Delivery {
                volume,
                offset: (at * 16) as u64,
                bytes: piece,
                provenance: Provenance::Fresh,
            },
            &mut |_, _| false,
            &mut out,
        );
    }
    job.object.volume_closed(volume, &mut out);
    Some(volume)
}

fn data(job: &mut Job<RarRecovery>, name: &str) -> MemberId {
    let member = job.member(name, &payload(5, 300));
    job.namespace.get_mut(member).expect("announced").closed = true;
    member
}

fn bind(job: &mut Job<RarRecovery>) {
    job.object.bind(&mut job.namespace, &mut Vec::new());
}

#[test]
fn a_rar5_header_reads_once_its_front_is_whole() {
    let bytes = rev5(3, 1, 3, &[300, 300, 120]);
    let Read::Short(needed) = header::rar5(&bytes[..10]) else {
        panic!("a sign alone is short");
    };
    assert_eq!(needed, 16);
    let Read::Short(needed) = header::rar5(&bytes[..16]) else {
        panic!("a prefix alone is short");
    };
    let Read::Header(read) = header::rar5(&bytes[..needed]) else {
        panic!("the whole header reads");
    };
    assert_eq!((read.data_count, read.rec_count), (3, 1));
    assert_eq!(read.sizes, vec![300, 300, 120]);
    let mut torn = bytes.clone();
    torn[17] ^= 1;
    assert!(matches!(header::rar5(&torn), Read::Not));
    assert!(matches!(header::rar5(b"Rar!\x1a\x07\x01\x00"), Read::Not));
}

#[test]
fn a_rar3_footer_gives_the_set_and_its_width() {
    let bytes = rev3(4, 250);
    let footer: [u8; RAR3_FOOTER] = bytes[bytes.len() - RAR3_FOOTER..].try_into().unwrap();
    let read = header::rar3(&footer, bytes.len() as u64).expect("a footer");
    assert_eq!((read.data_count, read.rec_count), (4, 1));
    assert_eq!(read.sizes, vec![250; 4]);
}

#[test]
fn recovery_volumes_of_one_set_are_claimed_and_another_sets_are_not() {
    let mut job = job();
    let sizes = [300, 300, 300];
    assert_eq!(
        rev(&mut job, "lantern.part1.rev", &rev5(3, 2, 3, &sizes)),
        Some(VolumeIndex(0))
    );
    assert_eq!(
        rev(&mut job, "Lantern.part2.REV", &rev5(3, 2, 4, &[])),
        Some(VolumeIndex(1))
    );
    assert_eq!(
        rev(&mut job, "beacon.part1.rev", &rev5(3, 1, 3, &sizes)),
        None
    );
    assert_eq!(
        job.object
            .claim("lantern.part2.rar", b"Rar!\x1a\x07\x01\x00"),
        None
    );
}

#[test]
fn the_set_binds_by_its_naming_and_names_the_volumes_no_member_carries() {
    let mut job = job();
    rev(
        &mut job,
        "lantern.part1.rev",
        &rev5(4, 2, 4, &[300, 300, 300, 90]),
    );
    let first = data(&mut job, "lantern.part01.rar");
    let third = data(&mut job, "lantern.part03.rar");
    let stray = data(&mut job, "beacon.part02.rar");
    bind(&mut job);
    assert!(job.object.covers(first) && job.object.covers(third));
    assert!(!job.object.covers(stray));
    let mut absent = Vec::new();
    job.object.absent(&mut absent);
    assert_eq!(
        absent,
        vec![
            ("lantern.part02.rar".to_owned(), 300),
            ("lantern.part04.rar".to_owned(), 90)
        ]
    );
}

#[test]
fn a_set_rules_on_how_many_volumes_are_missing_against_its_recovery_volumes() {
    let mut job = job();
    rev(
        &mut job,
        "lantern.part1.rev",
        &rev5(3, 1, 3, &[300, 300, 300]),
    );
    for name in [
        "lantern.part01.rar",
        "lantern.part02.rar",
        "lantern.part03.rar",
    ] {
        data(&mut job, name);
    }
    bind(&mut job);
    assert_eq!(job.object.repair(&mut job.store), RepairOutcome::Clean);

    let mut job = self::job();
    rev(
        &mut job,
        "lantern.part1.rev",
        &rev5(3, 1, 3, &[300, 300, 300]),
    );
    let whole = data(&mut job, "lantern.part01.rar");
    bind(&mut job);
    assert_eq!(
        job.object.repair(&mut job.store),
        RepairOutcome::Impossible {
            damaged: 2,
            reachable: 1
        }
    );
    let mut verdicts = Vec::new();
    job.object.verdicts(whole, &mut verdicts);
    assert_eq!(verdicts, vec![SliceVerdict::Intact]);
}

// A namespace that keeps no files lends no scratch, and the restore, which
// works on files, cannot run.
#[test]
fn a_set_that_could_rebuild_without_scratch_is_impossible() {
    let mut job = job();
    rev(&mut job, "lantern.part1.rev", &rev5(2, 1, 2, &[300, 300]));
    data(&mut job, "lantern.part01.rar");
    bind(&mut job);
    assert_eq!(
        job.object.repair(&mut job.store),
        RepairOutcome::Impossible {
            damaged: 1,
            reachable: 1
        }
    );
}

#[test]
fn a_set_survives_a_restart() {
    let mut job = job();
    rev(&mut job, "lantern.part1.rev", &rev5(2, 1, 2, &[300, 300]));
    data(&mut job, "lantern.part01.rar");
    bind(&mut job);
    let back = crate::restore(&job.object.facts()).expect("restores");
    assert_eq!(back.kind(), Kind::RarRecovery);
    let mut absent = Vec::new();
    back.recovery().expect("a recovery").absent(&mut absent);
    assert_eq!(absent, vec![("lantern.part02.rar".to_owned(), 300)]);
}
