// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The index of what each set covers, by the namespace the set is homed in,
// is what each set says it covers: a set that learns a member is listed
// over it, one that drops it is not, a set moved takes its members with it,
// one that goes leaves none, and one out on loan keeps what it covered as it
// left. A set is read again only when its coverage version moved.

use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use super::*;
use crate::deliver::Posted;
use crate::facts::{Durable, Facts};
use crate::recovery::{MemberAccess, RepairOutcome};
use crate::{Segment, SliceVerdict};

// What the test says the set covers, the version it says it at, and how
// many times the tree read the list.
#[derive(Default)]
struct Said {
    covered: BTreeSet<MemberId>,
    version: u64,
    listings: u64,
}

type Shared = Arc<Mutex<Said>>;

// A pretend PAR2 set that covers what the test says.
#[derive(Default, Serialize, Deserialize)]
struct Set {
    #[serde(skip)]
    said: Shared,
}

impl Durable for Set {
    const VERSION: u16 = 1;
}

impl Object for Set {
    fn unposted(&mut self, _: crate::VolumeIndex, _: crate::ByteRange) {}

    fn kind(&self) -> Kind {
        Kind::Par2
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
    fn recovery(&self) -> Option<&dyn Recovery> {
        Some(self)
    }
    fn recovery_mut(&mut self) -> Option<&mut dyn Recovery> {
        Some(self)
    }
    fn facts(&self) -> Facts {
        Facts::of(self.kind(), self)
    }
}

impl Recovery for Set {
    // Its bind is the default, which changes nothing it covers.
    fn take_newly(&mut self, _: &mut Vec<(MemberId, String)>) {}

    fn rebuilds(&self) -> bool {
        true
    }
    fn carried_inside(&self) -> bool {
        false
    }
    fn outgrows_recovery(&self) -> bool {
        false
    }
    fn covers(&self, member: MemberId) -> bool {
        self.said.lock().unwrap().covered.contains(&member)
    }
    fn covers_some(&self) -> bool {
        !self.said.lock().unwrap().covered.is_empty()
    }
    fn covered_members(&self, out: &mut Vec<MemberId>) {
        let mut said = self.said.lock().unwrap();
        said.listings += 1;
        out.extend(said.covered.iter().copied());
    }
    fn coverage_version(&self) -> u64 {
        self.said.lock().unwrap().version
    }
    fn feed(&mut self, _: MemberId, _: &[Segment]) {}
    fn verdicts(&self, _: MemberId, _: &mut Vec<SliceVerdict>) {}
    fn wants(&self, _: &mut Vec<Seek>) {}
    fn repair(&mut self, _: &mut dyn MemberAccess, _: crate::Budget) -> RepairOutcome {
        RepairOutcome::Clean
    }
    fn outcome(&self) -> Option<RepairOutcome> {
        None
    }
}

// The store doubles of the parent module, and the one set, sharing what the
// test says it covers.
struct Sets(Shared);

impl Builder for Sets {
    fn build(&mut self, kind: Kind, id: ObjectId) -> Option<Box<dyn Object>> {
        match kind {
            Kind::Par2 => Some(Box::new(Set {
                said: Arc::clone(&self.0),
            })),
            _ => Build.build(kind, id),
        }
    }

    fn read(&self, _: &Tree, _: NodeId, _: VolumeIndex, _: &Placement, _: &mut Vec<u8>) -> bool {
        false
    }
}

fn deliver(tree: &mut Tree, sets: &mut Sets, member: MemberId, offset: u64, bytes: &[u8]) {
    tree.deliver(
        NodeId::ROOT,
        member,
        Posted {
            offset,
            bytes: Bytes::copy_from_slice(bytes).into(),
            provenance: Provenance::Fresh,
        },
        sets,
        &mut Vec::new(),
    );
}

// The tree, a plain file and a store at the root, and a set posted beside
// them, with what the set says it covers.
struct Job {
    tree: Tree,
    sets: Sets,
    said: Shared,
    leaf: MemberId,
    show: MemberId,
    store: NodeId,
    set: NodeId,
    volume: MemberId,
}

fn job() -> Job {
    let said = Shared::default();
    let mut sets = Sets(Arc::clone(&said));
    let mut tree = Tree::new();
    let leaf = tree.announce_root("show.bin", None, &mut Vec::new());
    deliver(&mut tree, &mut sets, leaf, 0, b"plain bytes, no signature");
    let show = tree.announce_root("show.rar", None, &mut Vec::new());
    let mut front = SIGNATURE.to_vec();
    front.extend_from_slice(b"hello");
    deliver(&mut tree, &mut sets, show, 0, &front);
    let store = tree.child_of(NodeId::ROOT, show).expect("a store").0;
    let volume = tree.announce_root("show.par2", None, &mut Vec::new());
    let mut header = b"PAR2\0PKT".to_vec();
    header.extend_from_slice(&64u64.to_le_bytes());
    header.resize(64, 0);
    deliver(&mut tree, &mut sets, volume, 0, &header);
    let set = tree.child_of(NodeId::ROOT, volume).expect("a set").0;
    Job {
        tree,
        sets,
        said,
        leaf,
        show,
        store,
        set,
        volume,
    }
}

impl Job {
    // The set says it covers `covered`, under a new version, and is told
    // something, as a call that hands it input tells it.
    fn says(&mut self, covered: &[MemberId]) {
        {
            let mut said = self.said.lock().unwrap();
            said.covered = covered.iter().copied().collect();
            said.version += 1;
        }
        self.told();
    }

    fn told(&mut self) {
        self.tree
            .with_recovery(self.set, |recovery, _| recovery.feed(MemberId(0), &[]));
        self.tree.bring_coverage_up();
    }

    fn listings(&self) -> u64 {
        self.said.lock().unwrap().listings
    }

    // The index, as (namespace, member, how many sets) rows.
    fn index(&self) -> Vec<(NodeId, MemberId, u32)> {
        self.tree
            .covered
            .iter()
            .map(|(&(home, member), &count)| (home, member, count))
            .collect()
    }
}

#[test]
fn a_set_that_learns_a_member_is_listed_over_it_and_one_that_drops_it_is_not() {
    let mut job = job();
    assert_eq!(job.index(), []);
    job.says(&[job.leaf]);
    assert_eq!(job.index(), [(NodeId::ROOT, job.leaf, 1)]);
    job.says(&[job.leaf, job.show]);
    assert_eq!(
        job.index(),
        [(NodeId::ROOT, job.leaf, 1), (NodeId::ROOT, job.show, 1)]
    );
    // It drops one, as a RAR recovery set its ruling refutes covers none.
    job.says(&[job.show]);
    assert_eq!(job.index(), [(NodeId::ROOT, job.show, 1)]);
    job.says(&[]);
    assert_eq!(job.index(), []);
}

#[test]
fn a_set_moved_takes_its_members_to_its_new_home() {
    let mut job = job();
    job.says(&[job.leaf]);
    job.tree.move_recovery(job.set, job.store);
    assert_eq!(job.tree.home(job.set), Some(job.store));
    assert_eq!(job.index(), [(job.store, job.leaf, 1)]);
}

#[test]
fn a_set_that_goes_leaves_nothing_listed() {
    let mut job = job();
    job.says(&[job.leaf, job.show]);
    job.tree.retract(NodeId::ROOT, job.volume, &mut Vec::new());
    assert!(!job.tree.is_live(job.set) || job.tree.object(job.set).is_none());
    assert_eq!(job.index(), []);
    assert!(job.tree.coverage.is_empty());
}

#[test]
fn a_set_out_on_loan_keeps_what_it_covered_as_it_left() {
    let mut job = job();
    job.says(&[job.leaf]);
    let object = job.tree.lend(job.set).expect("the set is lent");
    // What it says while out is not read: it is not in its node.
    {
        let mut said = job.said.lock().unwrap();
        said.covered = BTreeSet::from([job.show]);
        said.version += 1;
    }
    job.tree.bring_coverage_up();
    assert_eq!(job.index(), [(NodeId::ROOT, job.leaf, 1)]);
    // Back in its node, it is read again.
    job.tree
        .take_back(job.set, object, &mut job.sets, &mut Vec::new());
    job.tree.bring_coverage_up();
    assert_eq!(job.index(), [(NodeId::ROOT, job.show, 1)]);
}

#[test]
fn a_call_that_moves_no_version_reads_no_set_again() {
    let mut job = job();
    job.says(&[job.leaf]);
    let listings = job.listings();
    // Told something, its version where it was.
    job.told();
    // Bytes of its own volume.
    let (_, volumes) = job.tree.volumes(job.set).expect("the set is bound");
    let volume = volumes[&VolumeIndex(0)];
    deliver(&mut job.tree, &mut job.sets, volume, 64, &[0; 16]);
    job.tree.bring_coverage_up();
    // Nothing marked since.
    job.tree.bring_coverage_up();
    assert_eq!(job.listings(), listings);
    assert_eq!(job.index(), [(NodeId::ROOT, job.leaf, 1)]);
}
