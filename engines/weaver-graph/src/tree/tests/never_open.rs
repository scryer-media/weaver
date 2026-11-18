// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A member whose name the tree is told never to open stays a leaf whatever
// its front signs, at the root and inside an archive. A name it is not told
// of is opened by its bytes as before.

use super::*;
use crate::deliver::Posted;

const INNER: &str = "setup.exe";

// A pretend store with one member, `setup.exe`, whose bytes are the
// store's own bytes past its seven-byte header.
struct Outer {
    announced: bool,
}

impl Object for Outer {
    fn unposted(&mut self, _: VolumeIndex, _: ByteRange) {}

    fn kind(&self) -> Kind {
        Kind::RarStore
    }
    fn deliver(
        &mut self,
        delivery: Delivery<'_>,
        _: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        out: &mut Vec<Emit>,
    ) {
        if !self.announced {
            self.announced = true;
            out.push(Emit::Member {
                member: MemberId(0),
                name: INNER.into(),
                length: None,
            });
        }
        let skip = HEADER
            .saturating_sub(delivery.offset)
            .min(delivery.bytes.len() as u64);
        if skip == delivery.bytes.len() as u64 {
            return;
        }
        out.push(Emit::Bytes {
            member: MemberId(0),
            offset: delivery.offset + skip - HEADER,
            bytes: delivery.bytes[skip as usize..].to_vec().into(),
            provenance: delivery.provenance,
        });
    }
    fn volume_closed(&mut self, _: VolumeIndex, _: &mut Vec<Emit>) {}
    fn volume_lost(&mut self, _: VolumeIndex, _: ByteRange, _: &mut Vec<Emit>) {}
    fn wants(&self, _: &mut Vec<Seek>) {}
    fn pulled(&mut self, _: Seek, _: Pull, _: &mut Vec<Emit>) {}
    fn locate(&self, _: VolumeIndex, range: ByteRange, out: &mut Vec<Placement>) {
        out.push(Placement::Member {
            member: MemberId(0),
            range: ByteRange::new(
                range.start.saturating_sub(HEADER),
                range.end.saturating_sub(HEADER),
            ),
        });
    }
    fn extent(&self, _: MemberId, _: ByteRange, _: &mut Vec<Extent>) -> Located {
        Located::NotYet
    }
    fn envelope(&self, _: u32, _: ByteRange) -> Option<Bytes> {
        None
    }
}

struct Stores;
impl Builder for Stores {
    fn build(&mut self, kind: Kind, _: ObjectId) -> Option<Box<dyn Object>> {
        (kind == Kind::RarStore).then(|| Box::new(Outer { announced: false }) as Box<dyn Object>)
    }
    fn read(&self, _: &Tree, _: NodeId, _: VolumeIndex, _: &Placement, _: &mut Vec<u8>) -> bool {
        false
    }
}

fn never_exe() -> NeverOpen {
    std::sync::Arc::new(|name: &str| {
        Some(
            name.rsplit_once('.')
                .is_some_and(|(_, extension)| extension.eq_ignore_ascii_case("exe")),
        )
    })
}

// A tree that roots `name` and delivers `bytes` to it whole.
fn rooted(name: &str, bytes: Vec<u8>, never_open: Option<NeverOpen>) -> (Tree, MemberId) {
    let mut tree = Tree::new();
    if let Some(never_open) = never_open {
        tree.never_open(never_open);
    }
    let member = tree.announce_root(name, Some(bytes.len() as u64), &mut Vec::new());
    tree.deliver(
        NodeId::ROOT,
        member,
        Posted {
            offset: 0,
            bytes: bytes.into(),
            provenance: Provenance::Fresh,
        },
        &mut Stores,
        &mut Vec::new(),
    );
    (tree, member)
}

fn signed(payload: &[u8]) -> Vec<u8> {
    let mut bytes = SIGNATURE.to_vec();
    bytes.extend_from_slice(payload);
    bytes
}

#[test]
fn a_listed_member_with_store_bytes_at_the_root_stays_a_leaf() {
    let (tree, member) = rooted(INNER, signed(b"hello"), Some(never_exe()));
    assert_eq!(tree.child_of(NodeId::ROOT, member), None);
    assert!(tree.never_opens("SETUP.EXE"));
    assert!(!tree.never_opens("show.rar"));
}

#[test]
fn an_unlisted_member_with_store_bytes_at_the_root_is_opened() {
    let (tree, member) = rooted(INNER, signed(b"hello"), None);
    assert!(tree.child_of(NodeId::ROOT, member).is_some());
    assert!(!tree.never_opens(INNER));
}

#[test]
fn a_listed_member_inside_a_store_stays_a_leaf() {
    // The outer store holds `setup.exe`, whose own bytes sign a store.
    let (tree, outer) = rooted("show.rar", signed(&signed(b"hello")), Some(never_exe()));
    let (child, _) = tree
        .child_of(NodeId::ROOT, outer)
        .expect("the outer store opens");
    assert_eq!(
        tree.namespace(child)
            .and_then(|namespace| namespace.get(MemberId(0)))
            .map(|member| member.name.as_str()),
        Some(INNER)
    );
    assert_eq!(tree.child_of(child, MemberId(0)), None);
}

#[test]
fn an_unlisted_member_inside_a_store_is_opened() {
    let (tree, outer) = rooted("show.rar", signed(&signed(b"hello")), None);
    let (child, _) = tree
        .child_of(NodeId::ROOT, outer)
        .expect("the outer store opens");
    assert!(tree.child_of(child, MemberId(0)).is_some());
}

// A rule the owner has not read yet answers `None` for every name until it
// is set, and `exe` is listed once it is.
fn later(known: &std::sync::Arc<std::sync::atomic::AtomicBool>) -> NeverOpen {
    let known = std::sync::Arc::clone(known);
    let exe = never_exe();
    std::sync::Arc::new(move |name: &str| {
        known
            .load(std::sync::atomic::Ordering::SeqCst)
            .then(|| exe(name) == Some(true))
    })
}

#[test]
fn nothing_is_opened_before_the_rule_is_known_and_each_member_is_asked_once_after() {
    use std::sync::atomic::{AtomicBool, Ordering};

    let known = std::sync::Arc::new(AtomicBool::new(false));
    // The outer store's bytes hold `setup.exe`, whose bytes sign a store.
    let (mut tree, outer) = rooted("show.rar", signed(&signed(b"hello")), Some(later(&known)));
    let unlisted = tree.announce_root("pack.bin", Some(12), &mut Vec::new());
    tree.deliver(
        NodeId::ROOT,
        unlisted,
        Posted {
            offset: 0,
            bytes: signed(b"hello").into(),
            provenance: Provenance::Fresh,
        },
        &mut Stores,
        &mut Vec::new(),
    );
    assert_eq!(tree.child_of(NodeId::ROOT, outer), None);
    assert_eq!(tree.child_of(NodeId::ROOT, unlisted), None);
    assert!(tree.is_undecided(NodeId::ROOT, outer));
    assert!(tree.is_undecided(NodeId::ROOT, unlisted));
    assert!(tree.never_opens(INNER) && tree.never_opens("show.rar"));
    assert_eq!(tree.unruled(), 2);

    // Still unknown: asking again opens nothing and keeps both waiting.
    tree.rule_known(&mut Stores, &mut Vec::new());
    assert_eq!(tree.child_of(NodeId::ROOT, outer), None);
    assert_eq!(tree.unruled(), 2);

    known.store(true, Ordering::SeqCst);
    let mut effects = Vec::new();
    tree.rule_known(&mut Stores, &mut effects);
    assert_eq!(tree.unruled(), 0);
    assert!(tree.child_of(NodeId::ROOT, unlisted).is_some());
    assert!(
        tree.child_of(NodeId::ROOT, outer).is_some(),
        "the outer store opens once the rule is known"
    );
    assert!(
        effects
            .iter()
            .any(|effect| matches!(effect, Effect::Bound { member, .. } if *member == outer))
    );
    // The owner hands the new store what the member held, on `Bound`; the
    // rule for the `setup.exe` inside it is the one the tests above cover.
    assert!(!tree.never_opens("show.rar"));
    assert!(tree.never_opens(INNER));
}
