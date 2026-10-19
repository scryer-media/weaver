// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Sampling and bootstrap: what the root fetches before any object asks.
//
// The first article of every member leads the payload. One article per member
// is the cheapest sample of whether the post is still there, and it is the
// member's front, which the namespace keeps (its first 16 KiB, the span a
// PAR2 set identifies a file by) so the sniffer and a recovery object's name
// match can run. The root delivers it; the namespace keeps it.
//
// Once a recovery object has said what the members are called, the sets they
// form are known before any of their objects has asked for anything, so the
// first volumes of each set are bid into the window at once.

use std::collections::BTreeMap;

use weaver_graph::{ByteRange, MemberId, NodeId, Tree, VERDICT_BYTES, VolumeIndex, Weight};

use super::source::ArticleMap;
use super::window::{Bid, Window};

// How many of a set's leading volumes the bootstrap asks for.
pub(crate) const BOOTSTRAP_VOLUMES: usize = 4;

// Bid the leading volumes of every set still being fetched: from its first
// volume on, past the ones already in hand, stopping at the first volume no
// member has been bound to yet.
pub(crate) fn bootstrap(tree: &Tree, waiting: impl Fn(VolumeIndex) -> bool, window: &mut Window) {
    let Some(root) = tree.namespace(NodeId::ROOT) else {
        return;
    };
    let mut sets: BTreeMap<NodeId, BTreeMap<VolumeIndex, VolumeIndex>> = BTreeMap::new();
    for member in root.members() {
        if let Some((child, volume)) = tree.child_of(NodeId::ROOT, member.id) {
            sets.entry(child)
                .or_default()
                .insert(volume, root_volume(member.id));
        }
    }
    for (set, volumes) in sets {
        if tree.recovers(set) {
            continue;
        }
        let mut asked = 0;
        for (expected, (volume, root)) in volumes.iter().enumerate() {
            if volume.0 as usize != expected || asked == BOOTSTRAP_VOLUMES {
                break;
            }
            if !waiting(*root) {
                continue;
            }
            window.bid(Bid {
                volume: *root,
                range: ByteRange::new(0, u64::MAX),
                weight: Weight::FIRST,
                bidder: set,
                in_order: tree.reads_in_order(set),
            });
            asked += 1;
        }
    }
}

// Queue the front of every root member that has not yet said what it is, up
// to the most bytes any verdict reads, each round until it decides. Nothing
// else wants an undecided member's bytes, since no object owns it yet, so
// without this a parked recovery volume would stay undecided until a repair
// asked for it, and the repair cannot ask before it decides. The sample asks
// for the front's first article; this carries on from it, so the first
// article of every member still leads, and most members decide on it alone.
// It raises nothing: the order of what is queued stays as it is.
pub(crate) fn fronts(tree: &Tree, map: &mut ArticleMap) {
    let Some(root) = tree.namespace(NodeId::ROOT) else {
        return;
    };
    for member in tree.undecided(NodeId::ROOT) {
        if root.first_bytes(member).is_empty() {
            continue;
        }
        map.queue(root_volume(member), ByteRange::new(0, VERDICT_BYTES as u64));
    }
}

// A root member is the root volume of the same index.
pub(crate) fn root_volume(member: MemberId) -> VolumeIndex {
    VolumeIndex(member.0)
}
