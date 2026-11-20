// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Sampling and bootstrap: what the root fetches before any object asks.
//
// The first article of every member leads the payload: it is the member's
// front, which the namespace keeps (its first 16 KiB, the span a
// PAR2 set identifies a file by) so the sniffer and a recovery object's name
// match can run. The root delivers it; the namespace keeps it.
//
// The index members come next, whole: an index is what names the members
// and the sets they form, so the leading volumes of each set wait on it, and
// so does the payload the root namespace holds while an index is to come.
// They are the members the post declares as indexes by name.
//
// Once a recovery object has said what the members are called, the sets they
// form are known before any of their objects has asked for anything, so the
// first volumes of each set are bid into the window at once. The payload
// follows in posted order, except that a set's volumes are fetched by their
// place in the set once it is known: from their names, their headers or a
// recovery object's names, whichever says it first. Sets keep the order they
// were first known in, and a volume whose place is not known yet stays where
// it was posted, after them. A set's own bid for a volume waits for the places
// before it the same way, so the window too is drawn in place order.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use weaver_graph::{
    ByteRange, MemberId, Namespace, NodeId, Tree, VERDICT_BYTES, VolumeIndex, Weight,
};
use weaver_model::files::FileRole;

use super::source::ArticleMap;
use super::window::{Bid, Window};

// Whether the post declares a file an index by its name.
pub(crate) fn declared_index(name: &str) -> bool {
    matches!(
        FileRole::from_filename(name),
        FileRole::Par2 { is_index: true, .. } | FileRole::Par3 { is_index: true }
    )
}

// Bid every declared index still waiting, whole, ahead of every set's bid:
// the root is the bidder, which the draw takes first at the same weight.
pub(crate) fn indexes(indexes: &[VolumeIndex], map: &ArticleMap, window: &mut Window) {
    for volume in indexes {
        if !map.volume_waiting(*volume) {
            continue;
        }
        window.bid(Bid {
            volume: *volume,
            range: ByteRange::new(0, u64::MAX),
            weight: Weight::FIRST,
            bidder: NodeId::ROOT,
            in_order: false,
        });
    }
}

// How many of a set's leading volumes the bootstrap asks for.
pub(crate) const BOOTSTRAP_VOLUMES: usize = 4;

// The root volumes still waited on that are a set's volume, filed by set and
// place. A volume is filed again only when the map says it began or stopped
// being waited on, or the root namespace marked its member, which every
// change to the member's record, its owner or its posting does.
#[derive(Debug, Default)]
pub(crate) struct Leading {
    // Each set's waiting volumes, as (place, volume).
    sets: BTreeMap<NodeId, BTreeSet<(VolumeIndex, VolumeIndex)>>,
    // Where each filed volume is filed: its set and place.
    filed: HashMap<VolumeIndex, (NodeId, VolumeIndex)>,
    // The root namespace's generation the files are up to date with, once
    // they are built.
    at: Option<u64>,
    // Each set's turn in the fetch order: the order the sets were first
    // filed in.
    turns: HashMap<NodeId, u32>,
    // The volumes filed at a new place since the queue last took them, each
    // with the rank its articles are fetched at.
    ranked: Vec<(VolumeIndex, u32)>,
    // Each set's first place no member is bound at, as of the last round.
    unbound: HashMap<NodeId, VolumeIndex>,
    // Some member of the post had not yet said what it is, as of the last
    // round, so a place still unbound may yet be.
    undecided: bool,
}

// A volume's rank is its set's turn, then its place in the set.
const PLACE_BITS: u32 = 20;
const MAX_PLACE: u32 = (1 << PLACE_BITS) - 1;
const MAX_TURN: u32 = (1 << (32 - PLACE_BITS)) - 1;

impl Leading {
    // File the volume afresh: out of where it was, and into its set at its
    // place when it waits and its member is a set's volume. A further
    // posting of a member is the member's, not a volume of its own. A
    // volume filed at a place it was not filed at is ranked by it, unless
    // its set recovers: a recovery volume is fetched only when asked.
    fn refile(&mut self, tree: &Tree, root: &Namespace, volume: VolumeIndex, waiting: bool) {
        let was = self.filed.remove(&volume);
        if let Some((set, place)) = was
            && let Some(volumes) = self.sets.get_mut(&set)
        {
            volumes.remove(&(place, volume));
            if volumes.is_empty() {
                self.sets.remove(&set);
            }
        }
        if !waiting {
            return;
        }
        let member = MemberId(volume.0);
        if root.get(member).is_none() || root.copy_of(member).is_some() {
            return;
        }
        let Some((set, place)) = tree.child_of(NodeId::ROOT, member) else {
            return;
        };
        self.sets.entry(set).or_default().insert((place, volume));
        self.filed.insert(volume, (set, place));
        if was != Some((set, place)) && !tree.recovers(set) {
            let next = self.turns.len() as u32;
            self.turns.entry(set).or_insert(next);
            if let Some(rank) = self.rank_of(volume) {
                self.ranked.push((volume, rank));
            }
        }
    }

    // The rank of a waiting volume whose place in its set is known.
    pub(crate) fn rank_of(&self, volume: VolumeIndex) -> Option<u32> {
        let (set, place) = self.filed.get(&volume)?;
        let turn = self.turns.get(set)?;
        Some((turn.min(&MAX_TURN) << PLACE_BITS) | place.0.min(MAX_PLACE))
    }

    // The volumes ranked since the last call, each with its rank.
    pub(crate) fn take_ranked(&mut self) -> Vec<(VolumeIndex, u32)> {
        std::mem::take(&mut self.ranked)
    }

    // Whether a set's bid for one of its own volumes may take a place in the
    // window: once every place before the volume's is bound, or every member
    // of the post has said what it is, so no further place will be. The
    // window is then drawn in place order however the set was posted. A bid
    // for anything else is ready.
    pub(crate) fn ready(&self, bid: &Bid) -> bool {
        if !self.undecided {
            return true;
        }
        match self.filed.get(&bid.volume) {
            Some((set, place)) if *set == bid.bidder => {
                self.unbound.get(set).is_none_or(|unbound| place < unbound)
            }
            _ => true,
        }
    }
}

// Bid the leading volumes of every set still being fetched: from its first
// volume on, past the ones already in hand, stopping at the first volume no
// member has been bound to yet. Only the root volumes whose waiting or member
// changed since the last round are asked again which set and place they are.
pub(crate) fn bootstrap(
    tree: &Tree,
    map: &mut ArticleMap,
    leading: &mut Leading,
    window: &mut Window,
) {
    let Some(root) = tree.namespace(NodeId::ROOT) else {
        return;
    };
    let reopened = map.take_reopened();
    match leading.at {
        Some(at) if at <= root.generation() => {
            let mut volumes = reopened;
            volumes.extend(root.changed_since(at).map(root_volume));
            for volume in volumes {
                let waiting = map.volume_waiting(volume);
                leading.refile(tree, root, volume, waiting);
            }
        }
        _ => {
            *leading = Leading::default();
            let waiting: Vec<VolumeIndex> = map.waiting_volumes().collect();
            for volume in waiting {
                leading.refile(tree, root, volume, true);
            }
        }
    }
    leading.at = Some(root.generation());
    leading.undecided = tree.undecided(NodeId::ROOT).next().is_some();
    let bids = leading_bids(tree, &leading.sets, &mut leading.unbound);
    #[cfg(test)]
    assert_eq!(bids, reference_bids(tree, map), "the bootstrap's bids");
    for bid in bids {
        window.bid(bid);
    }
}

// Each set's bids, set by set, each in place order, noting each set's first
// unbound place.
fn leading_bids(
    tree: &Tree,
    sets: &BTreeMap<NodeId, BTreeSet<(VolumeIndex, VolumeIndex)>>,
    unbounds: &mut HashMap<NodeId, VolumeIndex>,
) -> Vec<Bid> {
    unbounds.clear();
    let mut bids = Vec::new();
    for (set, volumes) in sets {
        let set = *set;
        if tree.recovers(set) {
            continue;
        }
        let unbound = first_unbound(tree, set);
        unbounds.insert(set, unbound);
        for (_, volume) in volumes
            .iter()
            .take_while(|(place, _)| *place < unbound)
            .take(BOOTSTRAP_VOLUMES)
        {
            bids.push(Bid {
                volume: *volume,
                range: ByteRange::new(0, u64::MAX),
                weight: Weight::FIRST,
                bidder: set,
                in_order: tree.reads_in_order(set),
            });
        }
    }
    bids
}

// The bids a walk of every waiting volume makes, which the filed sets must
// match after every round.
#[cfg(test)]
pub(crate) fn reference_bids(tree: &Tree, map: &ArticleMap) -> Vec<Bid> {
    let Some(root) = tree.namespace(NodeId::ROOT) else {
        return Vec::new();
    };
    let mut waiting: Vec<(NodeId, VolumeIndex, VolumeIndex)> = map
        .waiting_volumes_unseen()
        .into_iter()
        .filter_map(|volume| {
            let member = MemberId(volume.0);
            if root.get(member).is_none() || root.copy_of(member).is_some() {
                return None;
            }
            let (set, place) = tree.child_of(NodeId::ROOT, member)?;
            Some((set, place, volume))
        })
        .collect();
    waiting.sort_unstable();
    let mut bids = Vec::new();
    for leading in waiting.chunk_by(|one, other| one.0 == other.0) {
        let set = leading[0].0;
        if tree.recovers(set) {
            continue;
        }
        let unbound = first_unbound(tree, set);
        for (_, _, volume) in leading
            .iter()
            .take_while(|(_, place, _)| *place < unbound)
            .take(BOOTSTRAP_VOLUMES)
        {
            bids.push(Bid {
                volume: *volume,
                range: ByteRange::new(0, u64::MAX),
                weight: Weight::FIRST,
                bidder: set,
                in_order: tree.reads_in_order(set),
            });
        }
    }
    bids
}

// The first place of the set no member is bound at. Places are distinct, so
// when the last is one short of how many there are they run from the first
// without a gap, and only a set with a gap is walked to it.
fn first_unbound(tree: &Tree, set: NodeId) -> VolumeIndex {
    let Some((_, bound)) = tree.volumes(set) else {
        return VolumeIndex(0);
    };
    match bound.keys().next_back() {
        Some(last) if last.0 as usize + 1 == bound.len() => VolumeIndex(last.0 + 1),
        _ => VolumeIndex(
            bound
                .keys()
                .zip(0u32..)
                .take_while(|(place, expected)| place.0 == *expected)
                .count() as u32,
        ),
    }
}

// Queue the front of every root member that has not yet said what it is, up
// to the most bytes any verdict reads, each round until it decides. Nothing
// else wants an undecided member's bytes, since no object owns it yet, so
// without this a parked recovery volume would stay undecided until a repair
// asked for it, and the repair cannot ask before it decides. The sample asks
// for the front's first article; this carries on from it, so the first
// article of every member still leads, and most members decide on it alone.
// A first article that was lost is carried on from too: what follows it
// tells where the loss ends, and the tree hears its front is gone. It raises
// nothing: the order of what is queued stays as it is.
pub(crate) fn fronts(tree: &Tree, map: &mut ArticleMap) {
    for member in tree.undecided(NodeId::ROOT) {
        let volume = root_volume(member);
        if map.head_waiting(volume) {
            continue;
        }
        map.queue(volume, ByteRange::new(0, VERDICT_BYTES as u64));
    }
}

// A root member is the root volume of the same index.
pub(crate) fn root_volume(member: MemberId) -> VolumeIndex {
    VolumeIndex(member.0)
}
