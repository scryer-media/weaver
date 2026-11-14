// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A member is read back a bounded piece at a time to be verified, repaired
// or identified, however long it is or says it is: no read is longer than a
// piece, the buffer a read fills holds no more than one, and the bytes read
// are those the set describes, once.

use weaver_graph::recovery::{MemberAccess, RepairOutcome};
use weaver_graph::{Budget, ByteRange, MemberId, ObjectId, Provenance, Recovery};

use super::super::Par3;
use super::super::rebuild::READ;
use crate::testing::{Job, Store, par3_set, payload};

const OWN: ObjectId = ObjectId(11);

// The member store, as the namespace answers a repair, with each read of a
// member measured. A member may say it is `stated` bytes long with only its
// first bytes real: the rest arrived damaged, and reads back as zeros, as a
// sparse file's tail does.
struct Measured {
    store: Store,
    stated: Option<(MemberId, u64)>,
    // The longest read, the most a read's buffer held after it, and the
    // bytes read in all, of members.
    longest: u64,
    held: u64,
    read: u64,
}

impl Measured {
    fn new(store: Store) -> Self {
        Self {
            store,
            stated: None,
            longest: 0,
            held: 0,
            read: 0,
        }
    }
}

impl MemberAccess for Measured {
    fn read(&mut self, member: MemberId, range: ByteRange, into: &mut Vec<u8>) -> bool {
        self.longest = self.longest.max(range.len());
        self.read += range.len();
        let real = self.store.members.get(&member).map_or(0, Vec::len) as u64;
        let read = match self.stated {
            Some((stated, _)) if stated == member && range.end > real => {
                let within = ByteRange::new(range.start.min(real), real);
                let read = within.is_empty() || self.store.read(member, within, into);
                into.resize(into.len() + (range.len() - within.len()) as usize, 0);
                read
            }
            _ => self.store.read(member, range, into),
        };
        self.held = self.held.max(into.capacity() as u64);
        read
    }
    fn write_repaired(&mut self, member: MemberId, offset: u64, bytes: &[u8]) {
        self.store.write_repaired(member, offset, bytes);
    }
    fn length(&self, member: MemberId) -> Option<u64> {
        match self.stated {
            Some((stated, length)) if stated == member => Some(length),
            _ => self.store.length(member),
        }
    }
    fn arrived(&self, member: MemberId, range: ByteRange, out: &mut Vec<ByteRange>) {
        self.store.arrived(member, range, out);
    }
    fn read_volume(
        &mut self,
        volume: weaver_graph::VolumeIndex,
        range: ByteRange,
        into: &mut Vec<u8>,
    ) -> bool {
        self.store.read_volume(volume, range, into)
    }
}

fn job(files: &[(String, Vec<u8>)]) -> Job<Par3> {
    let mut job = Job::new(Par3::new(OWN), OWN);
    for (name, bytes) in files {
        job.volume(name, bytes, 64 << 10, false);
    }
    job
}

fn bind(job: &mut Job<Par3>) {
    let mut renamed = Vec::new();
    job.object.bind(&mut job.namespace, &mut renamed);
}

// Verify and repair `data`, changed at `damage`, through a measured store:
// the outcome, the member's bytes after, and the store.
fn verify(data: &[u8], damage: &[usize], block: u64) -> (RepairOutcome, Vec<u8>, Measured) {
    let files = par3_set("set", &[("big.bin", data)], block, 4);
    let mut job = job(&files);
    let mut wrong = data.to_vec();
    for &at in damage {
        wrong[at] ^= 0x42;
    }
    let member = job.member("big.bin", &wrong);
    for &at in damage {
        job.arrive(member, at as u64, &wrong[at..at + 1], Provenance::Damaged);
    }
    bind(&mut job);
    let mut access = Measured::new(std::mem::take(&mut job.store));
    let outcome = job.object.repair(&mut access, Budget::default());
    let bytes = access.store.members[&member].clone();
    (outcome, bytes, access)
}

// A member several pieces long is verified clean and repaired with no read
// longer than a piece and no buffer holding more than one, reading each
// described byte once to verify it; its verdict and bytes are those of a
// member shorter than one piece.
#[test]
fn par3_a_member_longer_than_a_piece_is_read_a_piece_at_a_time() {
    let large = payload(41, (3 * READ + READ / 2) as usize);
    let small = payload(42, 9_000);
    let block = 64 << 10;

    let (outcome, bytes, access) = verify(&large, &[], block);
    let (reference, _, _) = verify(&small, &[], 1024);
    assert_eq!(outcome, RepairOutcome::Clean);
    assert_eq!(outcome, reference);
    assert!(bytes == large);
    assert_eq!(access.read, large.len() as u64, "each byte read once");
    assert!(access.longest <= READ, "{} read at once", access.longest);
    assert!(access.held <= READ, "{} held", access.held);

    let damage = [READ as usize / 3, 2 * READ as usize + 77];
    let (outcome, bytes, access) = verify(&large, &damage, block);
    let (reference, repaired, _) = verify(&small, &[1_500, 6_500], 1024);
    assert_eq!(outcome, RepairOutcome::Repaired { slices: 2 });
    assert_eq!(outcome, reference);
    assert!(repaired == small);
    assert!(bytes == large);
    assert!(access.longest <= READ, "{} read at once", access.longest);
    assert!(access.held <= READ, "{} held", access.held);
}

// A member whose first bytes are real and whose stated length runs to a
// tebibyte, the tail never arriving, is read only as far as the set
// describes it, a piece at a time; under a name no described file answers
// to, at a length none has, it is not read at all.
#[test]
fn par3_a_member_stating_a_huge_length_is_read_only_as_far_as_described() {
    let data = payload(43, (2 * READ + 5) as usize);
    let files = par3_set("set", &[("big.bin", &data)], 64 << 10, 2);
    let stated = 1u64 << 40;

    let mut job = job(&files);
    let member = job.member("big.bin", &data);
    bind(&mut job);
    let mut access = Measured::new(std::mem::take(&mut job.store));
    access.stated = Some((member, stated));
    let outcome = job.object.repair(&mut access, Budget::default());
    assert_eq!(outcome, RepairOutcome::Clean);
    assert_eq!(access.read, data.len() as u64);
    assert!(access.longest <= READ, "{} read at once", access.longest);
    assert!(access.held <= READ, "{} held", access.held);

    let mut job = self::job(&files);
    let hidden = job.namespace.announce("3f9a0c71e25b84d6", Some(stated));
    job.arrive(hidden, 0, &data, Provenance::Fresh);
    job.store.members.insert(hidden, data.clone());
    bind(&mut job);
    let mut access = Measured::new(std::mem::take(&mut job.store));
    access.stated = Some((hidden, stated));
    job.object.repair(&mut access, Budget::default());
    assert!(!job.object.covers(hidden));
    assert_eq!(access.read, 0);
}

// A member under a name no described file answers to is identified by
// reading it back a piece at a time no longer than a read-back chunk.
#[test]
fn par3_a_member_identified_by_content_is_read_a_piece_at_a_time() {
    let data = payload(44, (READ + 3) as usize);
    let files = par3_set("set", &[("big.bin", &data)], 64 << 10, 2);
    let mut job = job(&files);
    let hidden = job.member("5b84d63f9a0c71e2", &data);
    bind(&mut job);
    let mut access = Measured::new(std::mem::take(&mut job.store));
    let outcome = job.object.repair(&mut access, Budget::default());
    assert_eq!(outcome, RepairOutcome::Clean);
    assert!(job.object.covers(hidden));
    assert!(access.longest <= READ, "{} read at once", access.longest);
    assert!(access.held <= READ, "{} held", access.held);
}
