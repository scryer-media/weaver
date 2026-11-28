// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A refused set that closed a member whole keeps it under its name, until a
// read-back gives the set's last volume to the store beside it and the set
// goes. Its member's file goes with it, as after a burial: a member the
// store then closes under the same name takes that name, and nothing waits
// on the name the gone set's file sat at.

use std::collections::BTreeMap;

use weaver_graph::{Carried, Claim, Clean, Why};

use super::*;

const LENGTH: u64 = 64;
const TAIL: u64 = 4;
const NUMBERED: &[u8] = b"#001";
const OUTPUT: &str = "extra.bin";

// A pretend store. One that `owns` a name places that name at 0 and nothing
// else, and carries none of its volumes. Any other places `first` at 0, and
// another member at 1 only when the last four bytes it is offered with say
// `#001`; short of them it asks for them.
#[derive(Default)]
struct Tailed {
    owns: Option<&'static str>,
    held: BTreeMap<VolumeIndex, String>,
}

impl Tailed {
    fn answer(&self, name: &str, tail: &[u8], length: Option<u64>) -> Claim {
        if let Some(owns) = self.owns {
            return match name == owns {
                true => vec![VolumeIndex(0)].into(),
                false => Claim::default(),
            };
        }
        if name == "first" {
            return vec![VolumeIndex(0)].into();
        }
        match (tail, length) {
            (NUMBERED, _) => vec![VolumeIndex(1)].into(),
            ([], Some(length)) => Claim::Wants(ByteRange::new(length - TAIL, length)),
            _ => Claim::default(),
        }
    }
}

impl Object for Tailed {
    fn unposted(&mut self, _: VolumeIndex, _: ByteRange) {}

    fn kind(&self) -> Kind {
        Kind::RarStore
    }

    fn deliver(
        &mut self,
        _: Delivery<'_>,
        _: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        _: &mut Vec<Emit>,
    ) {
    }

    fn volume_closed(&mut self, _: VolumeIndex, _: &mut Vec<Emit>) {}
    fn volume_lost(&mut self, _: VolumeIndex, _: ByteRange, _: &mut Vec<Emit>) {}
    fn wants(&self, _: &mut Vec<Seek>) {}
    fn pulled(&mut self, _: Seek, _: Pull, _: &mut Vec<Emit>) {}

    fn locate(&self, volume: VolumeIndex, range: ByteRange, out: &mut Vec<Placement>) {
        out.push(Placement::Envelope {
            envelope: volume.0,
            range,
        });
    }

    fn extent(&self, _: MemberId, _: ByteRange, _: &mut Vec<Extent>) -> Located {
        Located::Never
    }

    fn envelope(&self, _: u32, _: ByteRange) -> Option<Bytes> {
        None
    }

    fn claim(&mut self, name: &str, clean: Clean<'_>, length: Option<u64>) -> Claim {
        self.answer(name, clean.tail, length)
    }

    fn bound(&mut self, volume: VolumeIndex, name: &str, _: &[u8], _: Option<u64>) {
        self.held.insert(volume, name.to_owned());
    }

    fn carried(&self, volume: VolumeIndex) -> Option<&(dyn std::any::Any + Send + Sync)> {
        if self.owns.is_some() {
            return None;
        }
        self.held
            .get(&volume)
            .map(|name| name as &(dyn std::any::Any + Send + Sync))
    }

    fn carriable(&self, _: &mut Vec<VolumeIndex>) {}

    fn give_back(&mut self, volume: VolumeIndex) -> Option<Carried> {
        self.carried(volume)?;
        self.held
            .remove(&volume)
            .map(|name| Box::new(name) as Carried)
    }

    fn taken(&mut self, volume: VolumeIndex) {
        self.held.remove(&volume);
    }

    fn reclaim_told(
        &self,
        name: &str,
        _: &[u8],
        tail: &[u8],
        length: Option<u64>,
        _: &(dyn std::any::Any + Send + Sync),
    ) -> Claim {
        self.answer(name, tail, length)
    }
}

// Builds the stores in the order they are founded.
struct Stores(Vec<Tailed>);

impl Builder for Stores {
    fn build(&mut self, kind: Kind, _: ObjectId) -> Option<Box<dyn Object>> {
        (kind == Kind::RarStore && !self.0.is_empty())
            .then(|| Box::new(self.0.remove(0)) as Box<dyn Object>)
    }

    fn read(&self, _: &Tree, _: NodeId, _: VolumeIndex, _: &Placement, _: &mut Vec<u8>) -> bool {
        false
    }
}

// A volume posted whole: the signature, filler, and `tail` as its last
// four bytes.
fn post(job: &mut Job, stores: &mut Stores, name: &str, tail: &[u8]) -> MemberId {
    let member = job.tree.announce_root(name, Some(LENGTH), &mut Vec::new());
    job.writer
        .announced(
            &mut job.tree,
            stores,
            &Source,
            NodeId::ROOT,
            member,
            &mut job.out,
        )
        .expect("announce");
    let mut bytes = SIGNATURE.to_vec();
    bytes.resize((LENGTH - TAIL) as usize, 0x5a);
    bytes.extend_from_slice(tail);
    let arrival = Arrival {
        node: NodeId::ROOT,
        member,
        offset: 0,
        bytes: Bytes::copy_from_slice(&bytes).into(),
        provenance: Provenance::Fresh,
    };
    job.writer
        .deliver(&mut job.tree, stores, &Source, arrival, &mut job.out)
        .expect("deliver");
    member
}

// What the object at `node` says, applied as a job applies it.
fn say(job: &mut Job, stores: &mut Stores, node: NodeId, emits: Vec<Emit>) {
    let mut effects = Vec::new();
    job.tree
        .emitted(node, emits, &mut job.writer.placing(stores), &mut effects);
    job.writer
        .apply_all(&mut job.tree, stores, &Source, effects, &mut job.out)
        .expect("applied");
}

// The object at `node` closes one member whole under `OUTPUT`.
fn closes_output(job: &mut Job, stores: &mut Stores, node: NodeId, bytes: &'static [u8]) {
    say(
        job,
        stores,
        node,
        vec![
            Emit::Member {
                member: MemberId(0),
                name: OUTPUT.into(),
                length: Some(bytes.len() as u64),
            },
            Emit::Bytes {
                member: MemberId(0),
                offset: 0,
                bytes: Bytes::from_static(bytes).into(),
                provenance: Provenance::Fresh,
            },
            Emit::MemberClosed(MemberId(0)),
        ],
    );
}

// Checkpoints committed as the write lane commits them, a bounded number of
// times: what still waits after them waits on a name nothing frees.
fn commit_some(job: &mut Job) {
    job.writer.deliver_names();
    for _ in 0..4 {
        if !job.writer.moves_waiting() {
            return;
        }
        let stamp = job.writer.committed + 1;
        job.writer.recorded(stamp);
        job.writer
            .commit_moves(&mut job.tree, stamp)
            .expect("moves");
    }
}

#[test]
fn a_set_a_read_back_removes_leaves_its_names_and_files_to_the_member_after_it() {
    let mut job = Job::new(PLENTY);
    let stores = &mut Stores(vec![
        Tailed {
            owns: Some("second"),
            ..Tailed::default()
        },
        Tailed::default(),
    ]);
    let second = post(&mut job, stores, "second", NUMBERED);
    let (set, _) = job
        .tree
        .child_of(NodeId::ROOT, second)
        .expect("the set founds on its own volume");
    let first = post(&mut job, stores, "first", b"\0\0\0\0");
    let (holder, _) = job
        .tree
        .child_of(NodeId::ROOT, first)
        .expect("the store founds on its first volume");
    assert_ne!(set, holder);
    // The set closes a member whole under its name, and the file lands there.
    closes_output(&mut job, stores, set, b"whole");
    commit_some(&mut job);
    assert!(!job.writer.moves_waiting());
    assert_eq!(job.file(OUTPUT), b"whole");
    // Refused, it keeps that member; its volume is offered to the store
    // beside it, which asks for its tail, and the read-back places it there.
    say(
        &mut job,
        stores,
        set,
        vec![Emit::Refuse(Why::FirstBytesLost)],
    );
    assert_eq!(
        job.tree.child_of(NodeId::ROOT, second),
        Some((holder, VolumeIndex(1))),
        "{:?}",
        job.out
    );
    assert!(job.tree.volumes(set).is_none(), "the emptied set goes");
    assert!(
        !job.writer.files.contains_key(&set),
        "the gone set's files go with it"
    );
    // The store closes a member under the same name: it takes the name, and
    // its file moves there.
    closes_output(&mut job, stores, holder, b"again");
    commit_some(&mut job);
    assert!(!job.writer.moves_waiting(), "no move waits on a gone file");
    assert_eq!(job.file(OUTPUT), b"again");
}
