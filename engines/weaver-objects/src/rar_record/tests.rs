// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::grid::crc32;
use crate::testing::{Job, Store, payload};

const VOLUME: &str = "lantern.part01.rar";

// A RAR5 volume: the signature and `body` protected, a stand-in for the
// `RR` header, the record with `parity` units, an end.
fn rar5_volume(body: &[u8], parity: u16) -> Vec<u8> {
    let mut volume = b"Rar!\x1a\x07\x01\x00".to_vec();
    volume.extend_from_slice(body);
    let record = encode::rar5(&volume, parity);
    volume.extend_from_slice(&[0x33; 24]);
    volume.extend(record);
    volume.extend_from_slice(&[0x1d, 0x77, 0x56, 0x51, 0x03, 0x05, 0x04, 0x00]);
    volume
}

// A RAR3 block: CRC16, type, flags, size, body.
fn block(kind: u8, flags: u16, body: &[u8]) -> Vec<u8> {
    let mut rest = vec![kind];
    rest.extend(flags.to_le_bytes());
    rest.extend(((7 + body.len()) as u16).to_le_bytes());
    rest.extend_from_slice(body);
    let mut out = (crc32(&rest) as u16).to_le_bytes().to_vec();
    out.extend(rest);
    out
}

// A RAR3 volume: the signature, the main header, `data` stored as one
// file, then the record with `slots` parity sectors over all of that, and
// the end.
fn rar3_volume(data: &[u8], slots: u32) -> Vec<u8> {
    let mut volume = b"Rar!\x1a\x07\x00".to_vec();
    volume.extend(block(0x73, 0, &[0; 6]));
    let mut file = (data.len() as u32).to_le_bytes().to_vec();
    file.extend((data.len() as u32).to_le_bytes());
    file.push(3);
    file.extend(crc32(data).to_le_bytes());
    file.extend(0x5a21_0000u32.to_le_bytes());
    file.push(29);
    file.push(0x30);
    file.extend(9u16.to_le_bytes());
    file.extend(0u32.to_le_bytes());
    file.extend_from_slice(b"ember.bin");
    volume.extend(block(0x74, 0x8000, &file));
    volume.extend_from_slice(data);
    let record = encode::rar3(&volume, slots);
    volume.extend(encode::rar3_header(volume.len() as u64, slots));
    volume.extend(record);
    volume.extend(block(0x7b, 0, &[]));
    volume
}

const PASSWORD: &str = "lantern-key";

// `rar3_volume` with every header past the main one enciphered under
// `PASSWORD`: the main header says so, and each later header is its salt
// and its cipher. The record protects what comes before its header's salt.
fn sealed_rar3_volume(data: &[u8], slots: u32) -> Vec<u8> {
    let mut volume = b"Rar!\x1a\x07\x00".to_vec();
    volume.extend(block(0x73, 0x0080, &[0; 6]));
    let mut file = (data.len() as u32).to_le_bytes().to_vec();
    file.extend((data.len() as u32).to_le_bytes());
    file.push(3);
    file.extend(crc32(data).to_le_bytes());
    file.extend(0x5a21_0000u32.to_le_bytes());
    file.push(29);
    file.push(0x30);
    file.extend(9u16.to_le_bytes());
    file.extend(0u32.to_le_bytes());
    file.extend_from_slice(b"ember.bin");
    volume.extend(encode::seal_rar3(
        &block(0x74, 0x8000, &file),
        PASSWORD,
        [1; 8],
    ));
    volume.extend_from_slice(data);
    let record = encode::rar3(&volume, slots);
    let header = encode::rar3_header(volume.len() as u64, slots);
    volume.extend(encode::seal_rar3(&header, PASSWORD, [2; 8]));
    volume.extend(record);
    volume.extend(encode::seal_rar3(&block(0x7b, 0, &[]), PASSWORD, [3; 8]));
    volume
}

// A namespace's face that cannot read the bytes `lost` names, as one whose
// articles never came, and keeps the ranges the record found damaged.
struct Lossy<'a> {
    store: &'a mut Store,
    lost: Vec<ByteRange>,
    found: Vec<ByteRange>,
}

impl MemberAccess for Lossy<'_> {
    fn read(&mut self, member: MemberId, range: ByteRange, into: &mut Vec<u8>) -> bool {
        !self.lost.iter().any(|lost| lost.intersect(range).is_some())
            && self.store.read(member, range, into)
    }

    fn write_repaired(&mut self, member: MemberId, offset: u64, bytes: &[u8]) {
        self.store.write_repaired(member, offset, bytes);
    }

    fn arrived(&self, _member: MemberId, range: ByteRange, out: &mut Vec<ByteRange>) {
        let mut at = range.start;
        for lost in &self.lost {
            if let Some(lost) = lost.intersect(range) {
                out.push(ByteRange::new(at, lost.start));
                at = lost.end;
            }
        }
        out.push(ByteRange::new(at, range.end));
        out.retain(|run| !run.is_empty());
    }

    fn found_damaged(&mut self, _member: MemberId, range: ByteRange) {
        self.found.push(range);
    }

    fn length(&self, member: MemberId) -> Option<u64> {
        self.store.length(member)
    }
}

// The record as the tree makes it: built over the range, bound under the
// volume's name, and bound in the namespace that holds the volume, every
// byte of which arrived with no check of its own.
fn job(volume: &[u8]) -> (Job<RarRecord>, MemberId) {
    let mut job = Job::new(RarRecord::new(), weaver_graph::ObjectId(9));
    let member = job.namespace.announce(VOLUME, Some(volume.len() as u64));
    job.store.members.insert(member, volume.to_vec());
    job.object.bound(VolumeIndex(0), VOLUME, &[], None);
    bind(&mut job);
    job.object.feed(
        member,
        &[Segment {
            range: ByteRange::new(0, volume.len() as u64),
            crc32: 0,
            verified: false,
        }],
    );
    (job, member)
}

fn bind(job: &mut Job<RarRecord>) {
    job.object.bind(&mut job.namespace, &mut Vec::new());
}

fn close(job: &mut Job<RarRecord>) {
    job.object.volume_closed(VolumeIndex(0), &mut Vec::new());
}

fn verdicts(job: &Job<RarRecord>, member: MemberId) -> Vec<SliceVerdict> {
    let mut out = Vec::new();
    job.object.verdicts(member, &mut out);
    out
}

#[test]
fn the_record_covers_the_volume_it_sits_in_and_nothing_beside_it() {
    let volume = rar5_volume(&payload(1, 5000), 4);
    let (mut job, member) = job(&volume);
    let beside = job.namespace.announce("lantern.part02.rar", Some(10));
    bind(&mut job);
    assert!(job.object.covers(member));
    assert!(!job.object.covers(beside));
    assert_eq!(verdicts(&job, member), vec![SliceVerdict::Unknown]);
    assert!(verdicts(&job, beside).is_empty());
}

#[test]
fn a_ruling_while_the_volume_is_open_waits_until_it_closes() {
    let volume = rar5_volume(&payload(2, 5000), 4);
    let (mut job, _) = job(&volume);
    assert_eq!(job.object.repair(&mut job.store), RepairOutcome::Waiting);
    assert_eq!(job.object.outcome(), Some(RepairOutcome::Waiting));
    assert_eq!(job.store.reads, 0);
    close(&mut job);
    assert_eq!(job.object.outcome(), None, "due again once closed");
    assert_eq!(job.object.repair(&mut job.store), RepairOutcome::Clean);
}

#[test]
fn a_volume_that_passed_whole_is_clean_without_a_read() {
    let volume = rar5_volume(&payload(3, 5000), 4);
    let (mut job, member) = job(&volume);
    job.object.feed(
        member,
        &[Segment {
            range: ByteRange::new(0, volume.len() as u64),
            crc32: crc32(&volume),
            verified: true,
        }],
    );
    close(&mut job);
    assert_eq!(job.object.repair(&mut job.store), RepairOutcome::Clean);
    assert_eq!(job.store.reads, 0);
    assert_eq!(verdicts(&job, member), vec![SliceVerdict::Intact]);
}

#[test]
fn an_unproven_volume_the_record_finds_intact_is_clean_and_nothing_is_written() {
    let volume = rar5_volume(&payload(4, 70_000), 4);
    let (mut job, member) = job(&volume);
    job.object.feed(
        member,
        &[Segment {
            range: ByteRange::new(0, 100),
            crc32: 0,
            verified: false,
        }],
    );
    close(&mut job);
    assert_eq!(job.object.repair(&mut job.store), RepairOutcome::Clean);
    assert!(job.store.reads > 0);
    assert_eq!(job.store.writes, 0);
}

#[test]
fn a_rar5_volume_with_damaged_bytes_is_rebuilt_from_its_record_in_place() {
    let volume = rar5_volume(&payload(5, 150_000), 6);
    let (mut job, member) = job(&volume);
    let damaged = job.store.members.get_mut(&member).unwrap();
    for byte in &mut damaged[10_000..12_000] {
        *byte ^= 0x5a;
    }
    close(&mut job);
    let outcome = job.object.repair(&mut job.store);
    assert!(
        matches!(outcome, RepairOutcome::Repaired { slices } if slices >= 2),
        "{outcome:?}"
    );
    assert!(job.store.members[&member] == volume, "rebuilt bytes");
    assert_eq!(verdicts(&job, member), vec![SliceVerdict::Intact]);
}

#[test]
fn bytes_the_namespace_cannot_read_are_rebuilt_from_the_record() {
    let volume = rar5_volume(&payload(6, 150_000), 6);
    let (mut job, member) = job(&volume);
    let lost = ByteRange::new(40_961, 43_007);
    job.store.members.get_mut(&member).unwrap()[lost.start as usize..lost.end as usize].fill(0x77);
    close(&mut job);
    let mut access = Lossy {
        store: &mut job.store,
        lost: vec![lost],
        found: Vec::new(),
    };
    let outcome = job.object.repair(&mut access);
    assert!(
        matches!(outcome, RepairOutcome::Repaired { .. }),
        "{outcome:?}"
    );
    assert!(job.store.members[&member] == volume, "rebuilt bytes");
}

#[test]
fn a_rar5_volume_damaged_past_its_parity_is_impossible_and_nothing_is_written() {
    let volume = rar5_volume(&payload(7, 150_000), 2);
    let (mut job, member) = job(&volume);
    let damaged = job.store.members.get_mut(&member).unwrap();
    for at in [5_000, 30_000, 60_000, 90_000] {
        damaged[at] ^= 0xff;
    }
    close(&mut job);
    let mut access = Lossy {
        store: &mut job.store,
        lost: Vec::new(),
        found: Vec::new(),
    };
    assert_eq!(
        job.object.repair(&mut access),
        RepairOutcome::Impossible {
            damaged: 4,
            reachable: 2
        }
    );
    // Each damaged unit is named, the byte it holds among it.
    assert_eq!(access.found.len(), 4, "{:?}", access.found);
    for at in [5_000, 30_000, 60_000, 90_000] {
        assert!(
            access.found.iter().any(|range| range.contains(at)),
            "{at}: {:?}",
            access.found
        );
    }
    assert_eq!(job.store.writes, 0);
    assert_eq!(verdicts(&job, member), vec![SliceVerdict::Damaged]);
}

#[test]
fn damaged_parity_is_named_and_what_the_record_proves_ends_at_its_header() {
    let body = payload(12, 150_000);
    let volume = rar5_volume(&body, 3);
    let (mut job, member) = job(&volume);
    let protected = (8 + body.len()) as u64;
    let parity = protected + 24 + 10;
    job.store.members.get_mut(&member).unwrap()[parity as usize] ^= 1;
    job.store.members.get_mut(&member).unwrap()[100] ^= 1;
    close(&mut job);
    let mut access = Lossy {
        store: &mut job.store,
        lost: Vec::new(),
        found: Vec::new(),
    };
    assert_eq!(
        job.object.repair(&mut access),
        RepairOutcome::Repaired { slices: 1 }
    );
    assert_eq!(access.found.len(), 1, "{:?}", access.found);
    assert!(access.found[0].contains(parity), "{:?}", access.found);
    assert!(access.found[0].start >= protected, "{:?}", access.found);
    assert_eq!(
        job.object.proves(member, volume.len() as u64),
        ByteRange::new(0, protected)
    );
}

#[test]
fn a_record_that_never_finds_its_volume_waits_and_once_closed_says_nothing() {
    let mut job = Job::new(RarRecord::new(), weaver_graph::ObjectId(9));
    job.object.bound(VolumeIndex(0), VOLUME, &[], None);
    bind(&mut job);
    assert_eq!(job.object.repair(&mut job.store), RepairOutcome::Waiting);
    close(&mut job);
    assert_eq!(job.object.repair(&mut job.store), RepairOutcome::Clean);
    assert_eq!(job.object.outcome(), Some(RepairOutcome::Clean));
}

#[test]
fn bytes_to_come_again_reopen_the_volume_and_take_back_what_was_said() {
    let volume = rar5_volume(&payload(13, 5000), 2);
    let (mut job, _) = job(&volume);
    close(&mut job);
    assert_eq!(job.object.repair(&mut job.store), RepairOutcome::Clean);
    job.object
        .unposted(VolumeIndex(0), ByteRange::new(0, volume.len() as u64));
    assert_eq!(job.object.outcome(), None);
    assert_eq!(job.object.repair(&mut job.store), RepairOutcome::Waiting);
}

#[test]
fn a_rar3_volume_rebuilds_one_sector_per_slot_and_no_more() {
    let volume = rar3_volume(&payload(8, 6_000), 4);
    let (mut job, member) = job(&volume);
    job.store.members.get_mut(&member).unwrap()[700] ^= 1;
    close(&mut job);
    assert_eq!(
        job.object.repair(&mut job.store),
        RepairOutcome::Repaired { slices: 1 }
    );
    assert!(job.store.members[&member] == volume, "rebuilt bytes");

    let (mut job, member) = self::job(&volume);
    let damaged = job.store.members.get_mut(&member).unwrap();
    damaged[700] ^= 1;
    damaged[700 + 4 * 512] ^= 1;
    close(&mut job);
    assert_eq!(
        job.object.repair(&mut job.store),
        RepairOutcome::Impossible {
            damaged: 2,
            reachable: 4
        }
    );
}

// An enciphered volume asks for a candidate once closed, and the next while
// its front refutes the ones offered; the one that unseals the front ends
// the asking, and the ruling reads under it.
#[test]
fn a_volume_with_enciphered_headers_is_rebuilt_under_the_candidate_that_opens_them() {
    let volume = sealed_rar3_volume(&payload(8, 6_000), 4);
    let (mut job, member) = job(&volume);
    job.object
        .bound(VolumeIndex(0), VOLUME, &volume[..256], None);
    job.store.members.get_mut(&member).unwrap()[700] ^= 1;
    let mut asked = Vec::new();
    job.object.volume_closed(VolumeIndex(0), &mut asked);
    assert_eq!(asked, vec![Emit::NeedsPassword]);
    for (secret, more) in [("not-the-key", true), (PASSWORD, false)] {
        let mut asked = Vec::new();
        job.object.offer_password(secret, &mut asked);
        assert_eq!(asked.contains(&Emit::NeedsPassword), more, "{secret}");
    }
    assert_eq!(
        job.object.repair(&mut job.store),
        RepairOutcome::Repaired { slices: 1 }
    );
    assert!(job.store.members[&member] == volume, "rebuilt bytes");
}

// A front too short to tell a candidate by holds each one offered and asks
// for the next, so the ruling reads under whichever leads to the record.
#[test]
fn a_front_too_short_to_tell_holds_every_candidate_offered() {
    let volume = sealed_rar3_volume(&payload(8, 6_000), 4);
    let (mut job, member) = job(&volume);
    job.object
        .bound(VolumeIndex(0), VOLUME, &volume[..24], None);
    job.store.members.get_mut(&member).unwrap()[700] ^= 1;
    close(&mut job);
    for secret in ["not-the-key", PASSWORD] {
        let mut asked = Vec::new();
        job.object.offer_password(secret, &mut asked);
        assert_eq!(asked, vec![Emit::NeedsPassword], "{secret}");
    }
    assert_eq!(
        job.object.repair(&mut job.store),
        RepairOutcome::Repaired { slices: 1 }
    );
}

// The front is kept with what the record said, so a restored record offered
// again the candidate that unsealed it holds it and asks for no other.
#[test]
fn a_restored_record_offered_its_candidate_again_holds_it_and_asks_no_more() {
    let volume = sealed_rar3_volume(&payload(8, 6_000), 4);
    let (mut job, member) = job(&volume);
    job.object
        .bound(VolumeIndex(0), VOLUME, &volume[..256], None);
    close(&mut job);
    job.object.offer_password(PASSWORD, &mut Vec::new());
    let mut restored = crate::restore(&job.object.facts()).expect("restores");
    let mut asked = Vec::new();
    restored.offer_password(PASSWORD, &mut asked);
    assert!(asked.is_empty(), "{asked:?}");
    job.store.members.get_mut(&member).unwrap()[700] ^= 1;
    restored
        .recovery_mut()
        .expect("a recovery object")
        .bind(&mut job.namespace, &mut Vec::new());
    assert_eq!(
        restored
            .recovery_mut()
            .expect("a recovery object")
            .repair(&mut job.store),
        RepairOutcome::Repaired { slices: 1 }
    );
    assert!(job.store.members[&member] == volume, "rebuilt bytes");
}

// No candidate that opens the headers leads to no record: the record could
// not be read, and says nothing of its volume.
#[test]
fn a_volume_with_enciphered_headers_and_no_candidate_that_opens_them_says_nothing() {
    let volume = sealed_rar3_volume(&payload(8, 6_000), 4);
    let (mut job, member) = job(&volume);
    job.object
        .bound(VolumeIndex(0), VOLUME, &volume[..64], None);
    job.store.members.get_mut(&member).unwrap()[700] ^= 1;
    close(&mut job);
    job.object.offer_password("not-the-key", &mut Vec::new());
    assert_eq!(job.object.repair(&mut job.store), RepairOutcome::Clean);
    assert!(!job.object.covers(member));
    assert!(verdicts(&job, member).is_empty());
    assert!(job.store.members[&member] != volume, "nothing written");
}

// A record that cannot be read covers nothing and rules clean on that
// nothing, as a recovery object with nothing to rule on does: it vouches
// for no byte of its volume.
#[test]
fn a_volume_whose_record_cannot_be_read_says_nothing_of_it() {
    let mut volume = rar5_volume(&payload(9, 5000), 2);
    let record = 8 + 5000 + 24;
    let end = volume.len() - 8;
    volume[record..end].fill(0);
    let (mut job, member) = job(&volume);
    close(&mut job);
    assert_eq!(job.object.repair(&mut job.store), RepairOutcome::Clean);
    assert!(!job.object.covers(member));
    assert!(verdicts(&job, member).is_empty());
}

#[test]
fn the_member_stays_the_record_s_once_found_whatever_it_is_called() {
    let volume = rar5_volume(&payload(10, 5000), 2);
    let (mut job, member) = job(&volume);
    job.namespace.get_mut(member).unwrap().name = "9f3a7c1e".to_owned();
    bind(&mut job);
    assert!(job.object.covers(member));
}

#[test]
fn what_the_record_said_survives_a_restore() {
    let volume = rar5_volume(&payload(11, 150_000), 6);
    let (mut job, member) = job(&volume);
    job.store.members.get_mut(&member).unwrap()[20_000] ^= 1;
    close(&mut job);
    let ruled = job.object.repair(&mut job.store);
    assert!(matches!(ruled, RepairOutcome::Repaired { .. }), "{ruled:?}");
    let restored = crate::restore(&job.object.facts()).expect("restores");
    let recovery = restored.recovery().expect("a recovery object");
    assert_eq!(recovery.outcome(), Some(ruled));
    assert!(recovery.covers(member));
}
