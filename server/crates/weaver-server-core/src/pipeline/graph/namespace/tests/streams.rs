// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A namespace whose source is a stream's run, not the articles: what the run
// says lands where any delivery lands, so a store below it keeps its
// envelopes and a recovery set below it keeps its volume, and both read
// back.

use super::*;

const XZ: &[u8] = &[0xFD, 0x37, 0x7A, 0x58, 0x5A, 0x00, 0x00, 0x04];

// A pretend stream: its volume is its envelope, and its one member comes
// from its run, which the test plays by saying the run's emits.
pub(super) struct Decoded;

impl Object for Decoded {
    fn kind(&self) -> Kind {
        Kind::Xz
    }

    fn deliver(
        &mut self,
        _: Delivery<'_>,
        _: &mut dyn FnMut(&weaver_graph::Placement, &mut Vec<u8>) -> bool,
        _: &mut Vec<Emit>,
    ) {
    }
    fn volume_closed(&mut self, _: VolumeIndex, _: &mut Vec<Emit>) {}
    fn volume_lost(&mut self, _: VolumeIndex, _: ByteRange, _: &mut Vec<Emit>) {}
    fn wants(&mut self, _: &mut Vec<Seek>) {}
    fn pulled(&mut self, _: Seek, _: Pull, _: &mut Vec<Emit>) {}

    fn locate(&self, volume: VolumeIndex, range: ByteRange, out: &mut Vec<Placement>) {
        out.push(Placement::Envelope {
            envelope: volume.0,
            range,
        });
    }

    fn extent(&self, _: MemberId, _: ByteRange, _: &mut Vec<Extent>) {}

    fn envelope(&self, _: u32, _: ByteRange) -> Option<Bytes> {
        None
    }

    fn reads_in_order(&self) -> bool {
        true
    }
}

// A root member bound to the pretend stream, and the stream's node.
fn stream(job: &mut Job) -> NodeId {
    let member = job.member("pack.xz", None);
    job.send(member, 0, XZ, Provenance::Fresh);
    let (node, _) = job
        .tree
        .child_of(NodeId::ROOT, member)
        .expect("bound to the stream");
    node
}

impl Job {
    fn say(&mut self, node: NodeId, emit: Emit) {
        let mut build = self.build();
        self.writer
            .said(
                &mut self.tree,
                &mut build,
                &Source,
                node,
                emit,
                &mut self.out,
            )
            .expect("said");
    }

    fn say_member(&mut self, node: NodeId, member: u32, name: &str, length: Option<u64>) {
        self.say(
            node,
            Emit::Member {
                member: MemberId(member),
                name: name.into(),
                length,
            },
        );
    }

    fn say_bytes(&mut self, node: NodeId, member: u32, offset: u64, bytes: &[u8]) {
        self.say(
            node,
            Emit::Bytes {
                member: MemberId(member),
                offset,
                bytes: Bytes::copy_from_slice(bytes),
                provenance: Provenance::Fresh,
            },
        );
    }
}

#[test]
fn a_store_a_stream_feeds_reads_its_volume_back_through_its_envelope() {
    let mut job = Job::new(PLENTY);
    let node = stream(&mut job);
    job.say_member(node, 0, "pack.bin", None);
    job.say_bytes(node, 0, 0, &SIGNATURE[..3]);
    let mut rest = SIGNATURE[3..].to_vec();
    rest.extend_from_slice(b"hello");
    job.say_bytes(node, 0, 3, &rest);
    assert!(job.tree.child_of(node, MemberId(0)).is_some(), "a store");
    assert_eq!(job.file("inner.bin"), b"hello");

    let mut access = job.writer.access(&job.tree, node);
    let mut posted = Vec::new();
    assert!(access.read(MemberId(0), ByteRange::new(0, 12), &mut posted));
    assert_eq!(posted, [SIGNATURE, b"hello"].concat());
}

#[test]
fn a_set_a_stream_feeds_reads_its_volume_back_from_its_file() {
    let mut job = Job::new(PLENTY);
    let node = stream(&mut job);
    job.say_member(node, 0, "set.par2", Some(PAR2_SIGNATURE.len() as u64));
    job.say_bytes(node, 0, 0, PAR2_SIGNATURE);
    assert!(
        job.tree
            .recovers(job.tree.child_of(node, MemberId(0)).expect("a set").0)
    );
    assert_eq!(job.file("set.par2"), PAR2_SIGNATURE);

    let mut build = job.build();
    job.writer
        .repair(&mut job.tree, &mut build, &Source, node, &mut job.out)
        .expect("repair");
    assert_eq!(
        job.par.lock().unwrap().volume.as_deref(),
        Some(PAR2_SIGNATURE),
        "the set reads its own volume back"
    );
}
