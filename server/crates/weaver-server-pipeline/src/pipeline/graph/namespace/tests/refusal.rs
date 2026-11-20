// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A refused member's losses: what never arrived is never written when the
// object that held the member gives it back. The file keeps a hole there,
// its length covers it once it closes, and the record keeps the loss.

use super::*;
use crate::pipeline::graph::leaf::counting;
use weaver_graph::Run;

struct Refused {
    job: Job,
    member: MemberId,
    child: NodeId,
    // Bytes read from and written to the job's files by the refusal alone.
    read: u64,
    written: u64,
}

impl Refused {
    fn root(&self) -> Vec<(u64, u64, Provenance)> {
        runs(&self.job.root().get(self.member).unwrap().coverage)
    }

    fn file_len(&self) -> u64 {
        std::fs::metadata(self.job.dir.path().join("pack.bin"))
            .unwrap()
            .len()
    }

    fn closed(mut self) -> Vec<u8> {
        self.job.close(self.member);
        self.job.file("pack.bin")
    }
}

fn runs(coverage: &Coverage) -> Vec<(u64, u64, Provenance)> {
    coverage
        .runs()
        .map(|Run { range, provenance }| (range.start, range.end, *provenance))
        .collect()
}

// "pack.bin" holds the pretend store's header and an `inner`-byte member.
// The member's `arrived` pieces came, its `lost` ranges never will, then
// whatever `between` does, and the store gives the volume back.
fn refuse_after_losses(
    inner: u64,
    arrived: &[(u64, &[u8])],
    lost: &[ByteRange],
    between: impl FnOnce(&mut Job, NodeId),
) -> Refused {
    let mut job = Job::new(PLENTY);
    job.inner = inner;
    let member = job.member("pack.bin", Some(HEADER + inner));
    job.send(member, 0, SIGNATURE, Provenance::Fresh);
    for (offset, bytes) in arrived {
        job.send(member, HEADER + offset, bytes, Provenance::Fresh);
    }
    let (child, _) = job.tree.child_of(NodeId::ROOT, member).expect("bound");
    let mut build = job.build();
    for range in lost {
        let mut effects = Vec::new();
        job.tree.damage(
            NodeId::ROOT,
            VolumeIndex(member.0),
            range.shift(HEADER as i64),
            &mut effects,
        );
        job.writer
            .apply_all(&mut job.tree, &mut build, &Source, effects, &mut job.out)
            .expect("apply the loss");
    }
    between(&mut job, child);
    job.out.clear();
    let (read, written) = counting::so_far();
    job.writer
        .said(
            &mut job.tree,
            &mut build,
            &Source,
            child,
            Emit::Refuse(Why::Compressed),
            &mut job.out,
        )
        .expect("refuse");
    let (read_after, written_after) = counting::so_far();
    assert_eq!(
        job.out,
        [Effect::Refused {
            node: NodeId::ROOT,
            member,
            why: Why::Compressed
        }]
    );
    Refused {
        job,
        member,
        child,
        read: read_after - read,
        written: written_after - written,
    }
}

// The bytes the volume holds once it closes: what arrived where it arrived,
// zeros everywhere else.
fn volume(len: u64, arrived: &[(u64, &[u8])]) -> Vec<u8> {
    let mut bytes = vec![0; len as usize];
    bytes[..SIGNATURE.len()].copy_from_slice(SIGNATURE);
    for (offset, piece) in arrived {
        let at = (HEADER + offset) as usize;
        bytes[at..at + piece.len()].copy_from_slice(piece);
    }
    bytes
}

// An article declares a member far longer than the posting brought, and the
// tail it declares never comes. Giving the volume back writes and reads only
// the bytes that arrived, however long the loss.
#[test]
fn a_refusal_never_writes_a_lost_tail_however_long() {
    let tail = 500 << 30;
    let refused = refuse_after_losses(
        5 + tail,
        &[(0, b"hello")],
        &[ByteRange::at(5, tail)],
        |_, _| {},
    );
    let delivered = HEADER + 5;
    // Only the bytes that arrived are read and written, as for any refused
    // volume: the loss adds nothing.
    assert_eq!(refused.written, 2 * delivered);
    assert_eq!(refused.read, delivered);
    // Every byte is accounted for, so the member closed: its file has its
    // length, and the loss is a hole in it.
    assert_eq!(refused.file_len(), delivered + tail);
    assert_eq!(
        refused.root(),
        [
            (0, delivered, Provenance::Fresh),
            (delivered, delivered + tail, Provenance::Lost)
        ]
    );
}

// The same loss at a size that reads back: the closed file is what the
// refusal left before, header and member then zeros to its length, with
// none of the zeros written.
#[test]
fn a_refused_lost_tail_closes_to_the_same_bytes_unwritten() {
    let tail = 3 * READ_BACK + 17;
    let arrived: &[(u64, &[u8])] = &[(0, b"hello")];
    let refused = refuse_after_losses(5 + tail, arrived, &[ByteRange::at(5, tail)], |_, _| {});
    assert_eq!(refused.written, 2 * (HEADER + 5));
    assert_eq!(refused.read, HEADER + 5);
    assert_eq!(
        refused.root(),
        [
            (0, HEADER + 5, Provenance::Fresh),
            (HEADER + 5, HEADER + 5 + tail, Provenance::Lost)
        ]
    );
    assert_eq!(refused.closed(), volume(HEADER + 5 + tail, arrived));
}

// An honest posting that lost one article in the middle and one at the end:
// the closed file holds every byte that came, at its place, and zeros over
// both losses, as it did when the zeros were written. Both losses stay
// losses in the record.
#[test]
fn a_refused_member_with_a_lost_middle_and_end_closes_byte_for_byte() {
    let arrived: &[(u64, &[u8])] = &[(0, b"hello"), (10, b"world")];
    let lost = [ByteRange::new(5, 10), ByteRange::new(15, 20)];
    let refused = refuse_after_losses(20, arrived, &lost, |_, _| {});
    assert_eq!(refused.written, 2 * (HEADER + 10));
    assert_eq!(
        refused.root(),
        [
            (0, HEADER + 5, Provenance::Fresh),
            (HEADER + 5, HEADER + 10, Provenance::Lost),
            (HEADER + 10, HEADER + 15, Provenance::Fresh),
            (HEADER + 15, HEADER + 20, Provenance::Lost),
        ]
    );
    let closed = refused.closed();
    assert_eq!(closed, volume(HEADER + 20, arrived));
    assert_eq!(crc32(&closed), 1_230_375_625);
}

// The member is longer than the store's map reached: the same, with the
// file closing at the member's length.
#[test]
fn a_refused_member_longer_than_its_losses_closes_byte_for_byte() {
    let arrived: &[(u64, &[u8])] = &[(0, b"hello"), (10, b"world")];
    let lost = [ByteRange::new(5, 10), ByteRange::new(15, 20)];
    let refused = refuse_after_losses(30, arrived, &lost, |_, _| {});
    assert_eq!(refused.written, 2 * (HEADER + 10));
    let closed = refused.closed();
    assert_eq!(closed, volume(HEADER + 30, arrived));
    assert_eq!(crc32(&closed), 2_966_687_080);
}

// A lost front of the member: the bytes after it land where they came.
#[test]
fn a_refused_member_with_a_lost_front_closes_byte_for_byte() {
    let arrived: &[(u64, &[u8])] = &[(5, b"hello")];
    let refused = refuse_after_losses(20, arrived, &[ByteRange::new(0, 5)], |_, _| {});
    assert_eq!(refused.written, 2 * (HEADER + 5));
    assert_eq!(
        refused.root(),
        [
            (0, HEADER, Provenance::Fresh),
            (HEADER, HEADER + 5, Provenance::Lost),
            (HEADER + 5, HEADER + 10, Provenance::Fresh),
        ]
    );
    let closed = refused.closed();
    assert_eq!(closed, volume(HEADER + 20, arrived));
    assert_eq!(crc32(&closed), 1_565_932_046);
}

// What a repair put over a loss in the store's member did arrive there, and
// goes back into the volume at its place when the store gives it up.
#[test]
fn a_repair_over_a_loss_goes_back_into_the_refused_volume() {
    let arrived: &[(u64, &[u8])] = &[(0, b"hello")];
    let refused = refuse_after_losses(10, arrived, &[ByteRange::new(5, 10)], |job, child| {
        job.send_to(child, MemberId(0), 5, b"fixed", Provenance::Repaired);
    });
    let inner = runs(
        &refused
            .job
            .tree
            .namespace(refused.child)
            .unwrap()
            .get(MemberId(0))
            .unwrap()
            .coverage,
    );
    assert_eq!(
        inner,
        [(0, 5, Provenance::Fresh), (5, 10, Provenance::Repaired)]
    );
    assert_eq!(refused.file_len(), HEADER + 10);
    assert_eq!(
        refused.closed(),
        volume(HEADER + 10, &[(0, b"hello"), (5, b"fixed")])
    );
}
