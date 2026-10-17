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

use weaver_graph::{ByteRange, Kind, MemberId, NodeId, Tree, VolumeIndex, Weight};

use super::source::ArticleMap;
use super::window::{Bid, Window};

// How many of a set's leading volumes the bootstrap asks for.
pub(crate) const BOOTSTRAP_VOLUMES: usize = 4;

// Bid the leading volumes of every set still being fetched: from its first
// volume on, past the ones already in hand, stopping at the first volume no
// member has been bound to yet.
pub(crate) fn bootstrap(
    tree: &Tree,
    map: &ArticleMap,
    kind: impl Fn(NodeId) -> Option<Kind>,
    window: &mut Window,
) {
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
        let Some(set_kind) = kind(set) else {
            continue;
        };
        if set_kind.is_recovery() {
            continue;
        }
        let mut asked = 0;
        for (expected, (volume, root)) in volumes.iter().enumerate() {
            if volume.0 as usize != expected || asked == BOOTSTRAP_VOLUMES {
                break;
            }
            if !map.volume_waiting(*root) {
                continue;
            }
            window.bid(Bid {
                volume: *root,
                range: ByteRange::new(0, u64::MAX),
                weight: Weight::FIRST,
                bidder: set,
                in_order: reads_in_order(set_kind),
            });
            asked += 1;
        }
    }
}

// A root member is the root volume of the same index.
pub(crate) fn root_volume(member: MemberId) -> VolumeIndex {
    VolumeIndex(member.0)
}

// An engine or a stream can use a volume only once every volume before it
// has been read.
pub(crate) fn reads_in_order(kind: Kind) -> bool {
    kind.is_engine() || kind.is_stream()
}
