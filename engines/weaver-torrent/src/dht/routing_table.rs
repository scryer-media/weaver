// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::{
    collections::HashSet,
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::Instant,
};

use crate::core::hash_id::Id20;
use rand::Rng;
use serde::{Deserialize, Serialize, Serializer, ser::SerializeStruct};
use tracing::{debug, trace};

use crate::dht::{INACTIVITY_TIMEOUT, SEARCH_MIN_SEEDS};

// BEP 5's k: the nodes a near bucket holds, and the replacements any
// bucket keeps.
const BUCKET_SIZE: usize = 8;
// The farthest four buckets hold 16, 8, 4 and 2 times k, the others k:
// libtorrent's `routing_table::bucket_limit` (routing_table.cpp:277-285,
// v2.0.15) with `dht_extended_routing_table`, on by default
// (settings_pack.cpp:231). A bucket's index counts from the farthest, 0.
const EXTENDED: [usize; 4] = [16, 8, 4, 2];

pub(crate) fn bucket_limit(index: usize) -> usize {
    BUCKET_SIZE * EXTENDED.get(index).copied().unwrap_or(1)
}
// The failures in a row after which a node leaves a bucket that has no
// replacement to take its place: libtorrent's `dht_max_fail_count`
// (settings_pack.cpp:391, v2.0.15), read in `routing_table::node_failed`
// (routing_table.cpp:1063).
const MAX_FAIL_COUNT: usize = 20;

/// Whether `addr` is an IPv4 address written as IPv6 (`::ffff:a.b.c.d`). It is
/// never a node of either family: an IPv6 node is asked over IPv6, and an
/// IPv4 node is kept under its IPv4 address.
pub fn is_v4_mapped(addr: SocketAddr) -> bool {
    matches!(addr, SocketAddr::V6(a) if a.ip().to_ipv4_mapped().is_some())
}

// A node heard of, not yet in its bucket: libtorrent's replacement bucket
// (routing_table.cpp:879-900, v2.0.15), up to BUCKET_SIZE per bucket. A new
// one takes the place of the oldest that has not answered, an asked one
// first, and is dropped when all have answered. It enters the bucket only
// once it answers: at once when the bucket has room, else as a node of the
// bucket fails and leaves (`fill_from_replacements`, 520-539).
#[derive(Clone, Debug)]
struct Replacement {
    id: Id20,
    addr: SocketAddr,
    asked: bool,
    // When it answered, while its bucket had no room for it.
    answered: Option<Instant>,
}

#[derive(Clone, Debug)]
pub struct LeafBucket {
    // Sorted by id, so a node is found by a binary search.
    pub nodes: Vec<RoutingTableNode>,
    pub last_refreshed: Instant,
    // Not saved: a restored table hears of its nodes again.
    replacements: Vec<Replacement>,
    // The subnet of each of `nodes`, one node per subnet: whether a bucket
    // of up to 128 nodes holds one costs a lookup, not a scan.
    subnets: HashSet<Subnet>,
}

impl LeafBucket {
    fn with(mut nodes: Vec<RoutingTableNode>, replacements: Vec<Replacement>) -> Self {
        nodes.sort_by_key(|n| n.id);
        Self {
            subnets: nodes.iter().map(|n| subnet(n.addr)).collect(),
            nodes,
            last_refreshed: Instant::now(),
            replacements,
        }
    }

    // Where the node `id` is among `nodes`, or would go.
    fn position(&self, id: &Id20) -> Result<usize, usize> {
        self.nodes.binary_search_by_key(id, |n| n.id)
    }

    fn holds_subnet_of(&self, addr: SocketAddr) -> bool {
        self.subnets.contains(&subnet(addr))
    }

    // Adds a node not held, of a subnet not held.
    fn insert(&mut self, node: RoutingTableNode) {
        let (Err(pos) | Ok(pos)) = self.position(&node.id);
        self.subnets.insert(subnet(node.addr));
        self.nodes.insert(pos, node);
    }

    fn remove(&mut self, pos: usize) -> RoutingTableNode {
        let node = self.nodes.remove(pos);
        self.subnets.remove(&subnet(node.addr));
        node
    }
}

/// A node to ask into the table (see [`RoutingTable::next_refresh`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Refresh {
    pub id: Id20,
    pub addr: SocketAddr,
    /// A random id in the node's bucket, for a get_peers.
    pub target: Id20,
    /// The bucket is full: ping the node instead.
    pub ping: bool,
}

impl Serialize for LeafBucket {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_struct("LeafBucket", 2)?;
        s.serialize_field("nodes", &self.nodes)?;
        s.serialize_field(
            "last_refreshed",
            &format!("{:?}", self.last_refreshed.elapsed()),
        )?;
        s.end()
    }
}

impl<'de> Deserialize<'de> for LeafBucket {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Tmp {
            nodes: Vec<RoutingTableNode>,
        }
        Tmp::deserialize(deserializer).map(|t| Self::with(t.nodes, Vec::new()))
    }
}

impl Default for LeafBucket {
    fn default() -> Self {
        Self::with(Vec::new(), Vec::new())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum BucketTreeNodeData {
    Leaf(LeafBucket),
    LeftRight(usize, usize),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BucketTreeNode {
    bits: u8,
    #[serde(serialize_with = "crate::dht::utils::serialize_id20")]
    start: Id20,
    #[serde(serialize_with = "crate::dht::utils::serialize_id20")]
    end_inclusive: Id20,
    data: BucketTreeNodeData,
}

impl BucketTreeNode {
    fn holds(&self, id: &Id20) -> bool {
        self.start <= *id && *id <= self.end_inclusive
    }

    // The nodes this bucket holds at most. Only the bucket holding our id
    // splits, so a bucket of `bits` that does not hold it is the one 159 -
    // `bits` from the farthest, and the one that does is the last, 160 -
    // `bits`: libtorrent's list of buckets, farthest first.
    fn limit(&self, self_id: &Id20) -> usize {
        let index = 160 - usize::from(self.bits);
        bucket_limit(index.saturating_sub(usize::from(!self.holds(self_id))))
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BucketTree {
    data: Vec<BucketTreeNode>,
    size: usize,
    max_size: usize,
}

pub struct BucketTreeIteratorItem<'a> {
    pub bits: u8,
    pub start: &'a Id20,
    pub end_inclusive: &'a Id20,
    pub leaf: &'a LeafBucket,
}

impl BucketTreeIteratorItem<'_> {
    pub fn random_within(&self) -> Id20 {
        generate_random_id(self.start, self.bits)
    }
}

struct BucketTreeIterator<'a> {
    tree: &'a BucketTree,
    queue: Vec<usize>,
}

impl<'a> BucketTreeIterator<'a> {
    fn new(tree: &'a BucketTree) -> Self {
        let queue = vec![0];
        BucketTreeIterator { tree, queue }
    }
}

impl<'a> Iterator for BucketTreeIterator<'a> {
    type Item = BucketTreeIteratorItem<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let idx = self.queue.pop()?;
            match self.tree.data.get(idx) {
                Some(node) => match &node.data {
                    BucketTreeNodeData::Leaf(leaf) => {
                        return Some(BucketTreeIteratorItem {
                            bits: node.bits,
                            start: &node.start,
                            end_inclusive: &node.end_inclusive,
                            leaf,
                        });
                    }
                    BucketTreeNodeData::LeftRight(left, right) => {
                        self.queue.push(*right);
                        self.queue.push(*left);
                        continue;
                    }
                },
                None => continue,
            }
        }
    }
}

pub fn generate_random_id(start: &Id20, bits: u8) -> Id20 {
    let mut data = [0u8; 20];
    rand::rng().fill_bytes(&mut data);
    let mut data = Id20::new(data);
    let remaining_bits = 160 - bits;
    for bit in 0..remaining_bits {
        data.set_bit(bit, start.get_bit(bit));
    }
    data
}

// The id a refresh of the node `id` asks for: random past the bits it shares
// with our id and the next one, which it takes from ours too, so the answer
// names nodes nearer us than `id` (libtorrent `node::send_single_refresh`,
// node.cpp:683-688, v2.0.15, with `bucket` 159 - `distance_exp`, node.cpp:671).
fn refresh_target(self_id: &Id20, id: &Id20) -> Id20 {
    let shared = (0..160).take_while(|&bit| self_id.get_bit(bit) == id.get_bit(bit));
    let kept = (shared.count() + 1).min(160);
    generate_random_id(self_id, u8::try_from(160 - kept).expect("at most 160 bits"))
}

// The /24 (v4) or /64 (v6) of an address: two addresses of one are close,
// as libtorrent's `compare_ip_cidr` has it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Subnet {
    V4([u8; 3]),
    V6([u8; 8]),
}

fn subnet(addr: SocketAddr) -> Subnet {
    match addr.ip().to_canonical() {
        IpAddr::V4(a) => {
            let [a, b, c, _] = a.octets();
            Subnet::V4([a, b, c])
        }
        IpAddr::V6(a) => {
            let o = a.octets();
            Subnet::V6([o[0], o[1], o[2], o[3], o[4], o[5], o[6], o[7]])
        }
    }
}

fn same_subnet(a: SocketAddr, b: SocketAddr) -> bool {
    subnet(a) == subnet(b)
}

fn compute_split_start_end(
    start: Id20,
    end_inclusive: Id20,
    bits: u8,
) -> ((Id20, Id20), (Id20, Id20)) {
    let changing_bit = 160 - bits;
    let new_left_end = {
        let mut c = end_inclusive;
        c.set_bit(changing_bit, false);
        c
    };
    let new_right_start = {
        let mut c = start;
        c.set_bit(changing_bit, true);
        c
    };
    debug_assert!(
        start < new_left_end,
        "expected start({start:?}) < new_left_end({new_left_end:?}); start={start:?}, end={end_inclusive:?}, bits={bits}",
    );
    debug_assert!(
        new_left_end < new_right_start,
        "expected new_left_end({new_left_end:?}) < new_right_start({new_right_start:?}); start={start:?}, end={end_inclusive:?}, bits={bits}",
    );
    debug_assert!(
        new_right_start < end_inclusive,
        "expected new_right_start({new_right_start:?}) < end_inclusive({end_inclusive:?}); start={start:?}, end={end_inclusive:?}, bits={bits}",
    );
    ((start, new_left_end), (new_right_start, end_inclusive))
}

/// What [`RoutingTable::mark_outgoing_request`] marked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Marked {
    /// A node of the table.
    Node,
    /// A node heard of, not asked before.
    HeardOf,
    /// Nothing: no such node, or one heard of and asked already.
    Nothing,
}

#[derive(Debug)]
pub enum InsertResult {
    WasExisting,
    ReplacedBad(RoutingTableNode),
    Added,
    Ignored,
}

impl BucketTree {
    pub fn new(max_size: usize) -> Self {
        BucketTree {
            data: vec![BucketTreeNode {
                bits: 160,
                start: Id20::new([0u8; 20]),
                end_inclusive: Id20::new([0xff; 20]),
                data: BucketTreeNodeData::Leaf(Default::default()),
            }],
            size: 0,
            max_size,
        }
    }

    fn iter_leaves(&self) -> BucketTreeIterator<'_> {
        BucketTreeIterator::new(self)
    }

    fn iter(&self) -> impl Iterator<Item = &'_ RoutingTableNode> + '_ {
        self.iter_leaves().flat_map(|l| l.leaf.nodes.iter())
    }

    // The `k` nodes nearest `target` that `keep` accepts, nearest first.
    // Buckets split on id prefixes, so every node under the child that shares
    // the target's bit at a split is nearer than every node under the other:
    // walking the near child first meets the nodes in distance order, bucket
    // by bucket, and stops once `k` are found, with no copy of the table.
    // Of a bucket's nodes, up to 128 in a far one, the few still wanted are
    // selected in linear time and only they are sorted.
    fn closest(
        &self,
        target: &Id20,
        k: usize,
        mut keep: impl FnMut(&RoutingTableNode) -> bool,
    ) -> Vec<&RoutingTableNode> {
        let mut out = Vec::with_capacity(k);
        let mut stack = vec![0usize];
        while out.len() < k {
            let Some(idx) = stack.pop() else { break };
            let node = &self.data[idx];
            match &node.data {
                BucketTreeNodeData::Leaf(leaf) => {
                    let from = out.len();
                    out.extend(leaf.nodes.iter().filter(|n| keep(n)));
                    let wanted = k - from;
                    let found = &mut out[from..];
                    if found.len() > wanted {
                        found.select_nth_unstable_by_key(wanted - 1, |n| target.distance(&n.id));
                    }
                    out.truncate(k);
                    out[from..].sort_unstable_by_key(|n| target.distance(&n.id));
                }
                BucketTreeNodeData::LeftRight(left, right) => {
                    let (near, far) = if target.get_bit(160 - node.bits) {
                        (*right, *left)
                    } else {
                        (*left, *right)
                    };
                    stack.push(far);
                    stack.push(near);
                }
            }
        }
        out
    }

    fn get_leaf(&self, id: &Id20) -> usize {
        let mut idx = 0;
        loop {
            let node = &self.data[idx];
            match node.data {
                BucketTreeNodeData::Leaf(_) => return idx,
                BucketTreeNodeData::LeftRight(left_idx, right_idx) => {
                    let left = &self.data[left_idx];
                    if *id >= left.start && *id <= left.end_inclusive {
                        idx = left_idx;
                        continue;
                    };
                    idx = right_idx;
                }
            }
        }
    }

    pub fn get_mut(
        &mut self,
        id: &Id20,
        refresh: Option<Instant>,
    ) -> Option<&mut RoutingTableNode> {
        let idx = self.get_leaf(id);
        match &mut self.data[idx].data {
            BucketTreeNodeData::Leaf(leaf) => {
                let pos = leaf.position(id).ok()?;
                if let Some(refresh) = refresh {
                    leaf.last_refreshed = refresh;
                }
                Some(&mut leaf.nodes[pos])
            }
            BucketTreeNodeData::LeftRight(_, _) => unreachable!(),
        }
    }

    // Adds the node `id`; a split may move nodes out of the table, which
    // go to `moved_out` (see `split`).
    fn add_node(
        &mut self,
        self_id: &Id20,
        id: Id20,
        addr: SocketAddr,
        moved_out: &mut Vec<RoutingTableNode>,
    ) -> InsertResult {
        if is_v4_mapped(addr) {
            return InsertResult::Ignored;
        }
        let idx = self.get_leaf(&id);
        let res = self.insert_into_leaf(idx, self_id, id, addr, moved_out);
        let idx = self.get_leaf(&id);
        let leaf = self.leaf_mut(idx);
        match res {
            InsertResult::Added | InsertResult::ReplacedBad(_) => {
                leaf.replacements.retain(|r| r.id != id);
            }
            // A node heard of that answered with no room for it waits to
            // take the place of a node that fails (libtorrent
            // routing_table.cpp:871-876, `set_pinged`).
            InsertResult::Ignored => {
                if let Some(r) = leaf
                    .replacements
                    .iter_mut()
                    .find(|r| r.id == id && r.addr == addr)
                {
                    r.answered = Some(Instant::now());
                }
            }
            InsertResult::WasExisting => {}
        }
        res
    }

    // The node `id` at `addr`, if a bucket holds it.
    fn node(&self, id: &Id20, addr: SocketAddr) -> Option<&RoutingTableNode> {
        let BucketTreeNodeData::Leaf(leaf) = &self.data[self.get_leaf(id)].data else {
            unreachable!()
        };
        let node = &leaf.nodes[leaf.position(id).ok()?];
        (node.addr == addr).then_some(node)
    }

    // After the node `id` failed: it leaves its bucket at once when the
    // bucket has a replacement, else once it has failed MAX_FAIL_COUNT
    // times in a row or if it never answered, and the replacements that
    // answered take the room it leaves (libtorrent
    // `routing_table::node_failed`, routing_table.cpp:1016-1073, v2.0.15).
    // The nodes that entered in its place, if it left.
    fn drop_failed(&mut self, self_id: &Id20, id: &Id20) -> Option<Vec<Id20>> {
        let idx = self.get_leaf(id);
        let leaf = self.leaf_mut(idx);
        let pos = leaf.position(id).ok()?;
        let node = &leaf.nodes[pos];
        let leaves = !leaf.replacements.is_empty()
            || node.errors_in_a_row >= MAX_FAIL_COUNT
            || node.last_response.is_none();
        if !leaves {
            return None;
        }
        let gone = leaf.remove(pos);
        self.size -= 1;
        debug!(?gone, "failed node left the table");
        Some(self.fill_from_replacements(self_id, idx))
    }

    // The replacements of the leaf `idx` that answered enter it while it
    // has room, each one not in the /24 or /64 of a node it holds
    // (libtorrent `routing_table::fill_from_replacements`,
    // routing_table.cpp:520-539, v2.0.15). The ones that entered.
    fn fill_from_replacements(&mut self, self_id: &Id20, idx: usize) -> Vec<Id20> {
        let limit = self.data[idx].limit(self_id);
        let mut entered = Vec::new();
        loop {
            if self.size >= self.max_size {
                return entered;
            }
            let leaf = self.leaf_mut(idx);
            if leaf.nodes.len() >= limit {
                return entered;
            }
            let Some(pos) = leaf
                .replacements
                .iter()
                .position(|r| r.answered.is_some() && !leaf.holds_subnet_of(r.addr))
            else {
                return entered;
            };
            let r = leaf.replacements.remove(pos);
            leaf.insert(RoutingTableNode {
                id: r.id,
                addr: r.addr,
                last_request: r.answered,
                last_response: r.answered,
                last_query: None,
                last_queried: r.answered,
                errors_in_a_row: 0,
            });
            self.size += 1;
            entered.push(r.id);
        }
    }

    fn leaf_mut(&mut self, idx: usize) -> &mut LeafBucket {
        match &mut self.data[idx].data {
            BucketTreeNodeData::Leaf(leaf) => leaf,
            BucketTreeNodeData::LeftRight(_, _) => unreachable!(),
        }
    }

    // A node heard of enters the replacements of its bucket, unless the
    // bucket or its replacements hold it, or a node of its /24 or /64
    // already (libtorrent `routing_table::heard_about`, routing_table.cpp:
    // 1085-1090, which adds the node unpinged, so `add_node_impl` keeps it
    // in the replacement bucket at 879-900). Whether it entered.
    fn heard_about(&mut self, self_id: &Id20, id: Id20, addr: SocketAddr) -> bool {
        if id == *self_id || is_v4_mapped(addr) {
            return false;
        }
        let idx = self.get_leaf(&id);
        let leaf = self.leaf_mut(idx);
        if leaf.position(&id).is_ok()
            || leaf.holds_subnet_of(addr)
            || leaf
                .replacements
                .iter()
                .any(|r| r.id == id || same_subnet(r.addr, addr))
        {
            return false;
        }
        if leaf.replacements.len() >= BUCKET_SIZE {
            let unanswered = |r: &Replacement| r.answered.is_none();
            let Some(gone) = leaf
                .replacements
                .iter()
                .position(|r| r.asked && unanswered(r))
                .or_else(|| leaf.replacements.iter().position(unanswered))
            else {
                return false;
            };
            leaf.replacements.remove(gone);
        }
        leaf.replacements.push(Replacement {
            id,
            addr,
            asked: false,
            answered: None,
        });
        true
    }

    // Marks the replacement `id` as asked; whether there was one not
    // asked yet.
    fn mark_replacement_asked(&mut self, id: &Id20) -> bool {
        self.set_replacement_asked(id, true)
    }

    // Marks the replacement `id` as not asked; whether there was one asked.
    fn unmark_replacement_asked(&mut self, id: &Id20) -> bool {
        self.set_replacement_asked(id, false)
    }

    // Whether there was a replacement `id` it changed.
    fn set_replacement_asked(&mut self, id: &Id20, asked: bool) -> bool {
        let idx = self.get_leaf(id);
        match self
            .leaf_mut(idx)
            .replacements
            .iter_mut()
            .find(|r| r.id == *id)
        {
            Some(r) if r.asked != asked => {
                r.asked = asked;
                true
            }
            _ => false,
        }
    }

    // The node to refresh next, as libtorrent's `routing_table::next_refresh`
    // picks it (routing_table.cpp:392-445, v2.0.15), nearest bucket first: a
    // node of the bucket never asked, else a replacement not asked yet of a
    // bucket that has room for it or can split, else, once every bucket is
    // walked, the node asked least recently. A node is asked as it answers
    // and as it is picked here; a replacement picked is marked as asked.
    fn next_refresh(&mut self, self_id: &Id20, now: Instant) -> Option<Refresh> {
        let room = self.size < self.max_size;
        // Nearer buckets cover fewer bits; of the two nearest, ours first.
        let mut leaves: Vec<((u8, bool), usize, usize)> = self
            .data
            .iter()
            .enumerate()
            .filter(|(_, node)| matches!(node.data, BucketTreeNodeData::Leaf(_)))
            .map(|(idx, node)| ((node.bits, !node.holds(self_id)), idx, node.limit(self_id)))
            .collect();
        leaves.sort_unstable();
        // The node asked least recently: when, its leaf, its place there.
        let mut oldest: Option<(Instant, usize, usize)> = None;
        let mut picked = None;
        'walk: for ((_, not_ours), idx, limit) in leaves {
            let leaf = self.leaf_mut(idx);
            for (pos, node) in leaf.nodes.iter().enumerate() {
                match node.last_queried {
                    None => {
                        picked = Some((idx, Ok(pos)));
                        break 'walk;
                    }
                    Some(at) => {
                        if oldest.is_none_or(|(oldest, ..)| at < oldest) {
                            oldest = Some((at, idx, pos));
                        }
                    }
                }
            }
            if room
                && (!not_ours || leaf.nodes.len() < limit)
                && let Some(r) = leaf.replacements.iter().position(|r| !r.asked)
            {
                picked = Some((idx, Err(r)));
                break;
            }
        }
        let (idx, which) = picked.or(oldest.map(|(_, idx, pos)| (idx, Ok(pos))))?;
        let limit = self.data[idx].limit(self_id);
        let leaf = self.leaf_mut(idx);
        // A full bucket with a full set of replacements is pinged; any
        // other is asked for nodes near a random id between us and the
        // node (`node::send_single_refresh`, node.cpp:676-716,
        // `routing_table::is_full`, routing_table.cpp:1213-1223).
        let ping = leaf.nodes.len() >= limit && leaf.replacements.len() >= BUCKET_SIZE;
        let (id, addr) = match which {
            Ok(pos) => {
                let node = &mut leaf.nodes[pos];
                node.last_queried = Some(now);
                (node.id, node.addr)
            }
            Err(pos) => {
                let r = &mut leaf.replacements[pos];
                r.asked = true;
                (r.id, r.addr)
            }
        };
        Some(Refresh {
            id,
            addr,
            target: refresh_target(self_id, &id),
            ping,
        })
    }

    fn insert_into_leaf(
        &mut self,
        mut idx: usize,
        self_id: &Id20,
        id: Id20,
        addr: SocketAddr,
        moved_out: &mut Vec<RoutingTableNode>,
    ) -> InsertResult {
        // Loops as the bucket splits, until the bucket the node falls in
        // takes it or cannot split.
        let now = Instant::now();
        loop {
            let limit = self.data[idx].limit(self_id);
            let ours = self.data[idx].holds(self_id);
            let nodes = match &mut self.data[idx].data {
                BucketTreeNodeData::Leaf(nodes) => nodes,
                BucketTreeNodeData::LeftRight(_, _) => unreachable!(),
            };
            // if already found, quit
            if nodes.position(&id).is_ok() {
                return InsertResult::WasExisting;
            }
            // One node per /24 (v4) or /64 (v6) in a bucket: a node close to
            // one we hold, under another id, is not trusted (libtorrent
            // `routing_table::add_node_impl` with `dht_restrict_routing_ips`,
            // `compare_ip_cidr`).
            if nodes.holds_subnet_of(addr) {
                return InsertResult::Ignored;
            }

            let new_node = RoutingTableNode {
                id,
                addr,
                last_request: None,
                last_response: None,
                last_query: None,
                last_queried: None,
                errors_in_a_row: 0,
            };

            // As libtorrent's `add_node_impl` (routing_table.cpp:811-851,
            // v2.0.15): a bucket with room takes the node, then the bucket
            // holding our id splits, and only a full bucket that cannot
            // split gives a bad node's place to it, the one that failed most
            // (`replace_node_impl`, 165-178). The table never holds more than
            // `max_size`.
            let full = self.size >= self.max_size;
            if !full && nodes.nodes.len() < limit {
                nodes.insert(new_node);
                nodes.last_refreshed = now;
                self.size += 1;
                return InsertResult::Added;
            }

            if full || !ours {
                if let Some(bad) = nodes
                    .nodes
                    .iter()
                    .enumerate()
                    .filter(|(_, r)| matches!(r.status(now), NodeStatus::Bad))
                    .min_by_key(|(_, r)| std::cmp::Reverse(r.errors_in_a_row))
                    .map(|(pos, _)| pos)
                {
                    let gone = nodes.remove(bad);
                    nodes.insert(new_node);
                    debug!("replaced bad node {:?}", gone);
                    nodes.last_refreshed = now;
                    return InsertResult::ReplacedBad(gone);
                }
                if full {
                    trace!(
                        "can't add node to routing table, max size of {} reached",
                        self.max_size
                    );
                }
                return InsertResult::Ignored;
            }

            self.split(idx, self_id, moved_out);
            idx = self.get_leaf(&id);
        }
    }

    // Splits the leaf `idx`, which holds our id, in two halves, as
    // libtorrent's `routing_table::split_bucket` and the loop over it in
    // `add_node` do (routing_table.cpp:905-966 and 575-590, v2.0.15). The
    // far half keeps the leaf's place, so its limit is the leaf's. The near
    // half holds our id and is one place nearer, so its limit may be
    // smaller: while it holds more, it splits again, and the far half of
    // that split keeps its limit's worth, the nodes that answered first,
    // the others becoming its replacements up to a bucket's worth and the
    // rest leaving the table into `moved_out`. With 8-node buckets no half
    // ever holds more than its limit; with the far buckets 128, 64, 32 and
    // 16, a near half can.
    fn split(&mut self, mut idx: usize, self_id: &Id20, moved_out: &mut Vec<RoutingTableNode>) {
        loop {
            let leaf = &mut self.data[idx];
            let (bits, start, end) = (leaf.bits, leaf.start, leaf.end_inclusive);
            let BucketTreeNodeData::Leaf(nodes) = &mut leaf.data else {
                unreachable!()
            };
            let ((ls, le), (rs, re)) = compute_split_start_end(start, end, bits);
            let (ld, rd): (Vec<_>, Vec<_>) = nodes.nodes.drain(..).partition(|n| n.id < rs);
            let (lr, rr) = nodes.replacements.drain(..).partition(|r| r.id < rs);
            let half = |start, end_inclusive, nodes, replacements| BucketTreeNode {
                bits: bits - 1,
                start,
                end_inclusive,
                data: BucketTreeNodeData::Leaf(LeafBucket::with(nodes, replacements)),
            };
            let left_idx = self.data.len();
            self.data.push(half(ls, le, ld, lr));
            self.data.push(half(rs, re, rd, rr));
            self.data[idx].data = BucketTreeNodeData::LeftRight(left_idx, left_idx + 1);

            let (near, far) = if self.data[left_idx].holds(self_id) {
                (left_idx, left_idx + 1)
            } else {
                (left_idx + 1, left_idx)
            };
            self.trim(far, self_id, moved_out);
            if self.leaf_mut(near).nodes.len() <= self.data[near].limit(self_id) {
                return;
            }
            idx = near;
        }
    }

    // The leaf `idx`, past its limit after a split, keeps its limit's worth
    // of nodes, the ones that answered first; of the others, those that fit
    // join its replacements and the rest leave the table.
    fn trim(&mut self, idx: usize, self_id: &Id20, moved_out: &mut Vec<RoutingTableNode>) {
        let limit = self.data[idx].limit(self_id);
        let leaf = self.leaf_mut(idx);
        if leaf.nodes.len() <= limit {
            return;
        }
        let mut order: Vec<usize> = (0..leaf.nodes.len()).collect();
        order.sort_by_key(|&pos| leaf.nodes[pos].last_response.is_none());
        let mut out: Vec<usize> = order.split_off(limit);
        out.sort_unstable_by(|a, b| b.cmp(a));
        let mut gone: Vec<RoutingTableNode> = out.into_iter().map(|pos| leaf.remove(pos)).collect();
        gone.sort_by_key(|n| n.last_response.is_none());
        let room = BUCKET_SIZE.saturating_sub(leaf.replacements.len());
        leaf.replacements
            .extend(gone.iter().take(room).map(|node| Replacement {
                id: node.id,
                addr: node.addr,
                asked: node.last_request.is_some(),
                answered: node.last_response,
            }));
        self.size -= gone.len();
        moved_out.extend(gone);
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct RoutingTableNode {
    #[serde(serialize_with = "crate::dht::utils::serialize_id20")]
    id: Id20,
    addr: SocketAddr,
    #[serde(skip)]
    last_request: Option<Instant>,
    #[serde(skip)]
    last_response: Option<Instant>,
    #[serde(skip)]
    last_query: Option<Instant>,
    // When it last answered us or was last picked to be refreshed: none for
    // a node never asked (libtorrent `node_entry::last_queried`).
    #[serde(skip)]
    last_queried: Option<Instant>,
    #[serde(skip)]
    errors_in_a_row: usize,
}

impl Serialize for RoutingTableNode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut s = serializer.serialize_struct("RoutingTableNode", 3)?;
        s.serialize_field("id", &self.id.as_string())?;
        s.serialize_field("addr", &self.addr)?;
        s.serialize_field("status", &self.status(Instant::now()))?;
        if let Some(l) = self.last_request {
            s.serialize_field("last_request_ago", &l.elapsed())?;
        }
        if let Some(l) = self.last_response {
            s.serialize_field("last_response_ago", &l.elapsed())?;
        }
        if let Some(l) = self.last_query {
            s.serialize_field("last_query_ago", &l.elapsed())?;
        }
        s.serialize_field("errors_in_a_row", &self.errors_in_a_row)?;
        s.end()
    }
}

#[derive(Serialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeStatus {
    Good,
    Questionable,
    Bad,
    Unknown,
}

impl RoutingTableNode {
    pub fn id(&self) -> Id20 {
        self.id
    }
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }
    pub fn status(&self, now: Instant) -> NodeStatus {
        match (self.last_request, self.last_response, self.last_query) {
            // A node that failed once and has not answered since is stale:
            // no node we count on or hand out, and the first to make way
            // for a new one (libtorrent `node_entry::confirmed`,
            // node_entry.hpp:62, and `replace_node_impl`, routing_table.cpp:
            // 165-178, v2.0.15).
            _ if self.errors_in_a_row > 0 => NodeStatus::Bad,

            // A good node is a node has responded to one of our queries within the last 15 minutes.
            // A node is also good if it has ever responded to one of our queries and has sent
            // us a query within the last 15 minutes.
            (Some(_), Some(last_incoming), _) | (Some(_), Some(_), Some(last_incoming))
                if now - last_incoming < INACTIVITY_TIMEOUT =>
            {
                NodeStatus::Good
            }

            // After 15 minutes of inactivity, a node becomes questionable.
            // The moment we send a request to it, it stops becoming questionable and becomes Unknown / Bad.
            (last_outgoing, _, Some(last_incoming)) | (last_outgoing, Some(last_incoming), _)
                if now - last_incoming > INACTIVITY_TIMEOUT
                    && last_outgoing
                        .map(|e| now - e > INACTIVITY_TIMEOUT)
                        .unwrap_or(true) =>
            {
                NodeStatus::Questionable
            }
            _ => NodeStatus::Unknown,
        }
    }

    pub fn mark_outgoing_request(&mut self, now: Instant) {
        self.last_request = Some(now);
    }

    pub fn mark_last_query(&mut self, now: Instant) {
        self.last_query = Some(now);
    }

    pub fn mark_response(&mut self, now: Instant) {
        self.last_response = Some(now);
        self.last_queried = Some(now);
        if self.last_request.is_none() {
            self.last_request = Some(now);
        }
        self.errors_in_a_row = 0;
    }

    pub fn mark_error(&mut self) {
        self.errors_in_a_row += 1;
    }

    /// Whether the node has answered us and has not failed since: one a
    /// search can count on, unlike a node only heard of or restored from an
    /// earlier run, or one that stopped answering.
    pub fn is_confirmed(&self, now: Instant) -> bool {
        self.last_response.is_some() && self.status(now) != NodeStatus::Bad
    }

    #[cfg(test)]
    pub fn errors_in_a_row(&self) -> usize {
        self.errors_in_a_row
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingTable {
    #[serde(serialize_with = "crate::dht::utils::serialize_id20")]
    id: Id20,
    size: usize,
    buckets: BucketTree,
    // Rings as a node enters the table.
    #[serde(skip)]
    entered: Arc<tokio::sync::Notify>,
    // How many nodes have entered the table, a node that took a bad one's
    // place among them.
    #[serde(skip)]
    entries: u64,
    // Holds a permit once a node is heard of (see `heard_about`).
    #[serde(skip)]
    heard: Arc<tokio::sync::Notify>,
    // How many of its nodes are confirmed (see `RoutingTableNode::
    // is_confirmed`), kept as each is marked. A restored node is not.
    #[serde(skip)]
    confirmed: usize,
    // Rings as `confirmed` reaches SEARCH_MIN_SEEDS.
    #[serde(skip)]
    seeded: Arc<tokio::sync::Notify>,
}

impl RoutingTable {
    // The most nodes a table holds. libtorrent has no such cap. With the
    // far buckets at 128, 64, 32 and 16 and 8 after them, a table of n
    // buckets holds at most 240 + 8 (n - 4) nodes, so it reaches 512 only
    // with 38 buckets; a table about as deep as libtorrent's own remark
    // puts tables in the wild, 26 or 27 (routing_table.cpp:577-580,
    // v2.0.15), holds at most 424.
    const DEFAULT_MAX_SIZE: usize = 512;

    pub fn new(id: Id20, max_size: Option<usize>) -> Self {
        Self {
            id,
            buckets: BucketTree::new(max_size.unwrap_or(Self::DEFAULT_MAX_SIZE)),
            size: 0,
            entered: Default::default(),
            entries: 0,
            heard: Default::default(),
            confirmed: 0,
            seeded: Default::default(),
        }
    }

    /// A restored table rebuilt with only the nodes of the IPv4 family when
    /// `is_v4`, the IPv6 one when not, never an IPv4 address written as
    /// IPv6. Each node enters as a new one would, so the table keeps every
    /// bucket's limit and at most `DEFAULT_MAX_SIZE` nodes, whatever the
    /// saved file says: a table saved with 8-node buckets loads whole, and
    /// a larger or altered one loads within the bounds.
    pub fn of_family(self, is_v4: bool) -> Self {
        let keeps = |addr: SocketAddr| addr.is_ipv4() == is_v4 && !is_v4_mapped(addr);
        let max_size = self.buckets.max_size.min(Self::DEFAULT_MAX_SIZE);
        let mut table = Self::new(self.id, Some(max_size));
        for node in self.iter().filter(|n| keeps(n.addr)) {
            table.add_node(node.id, node.addr);
        }
        // Restored, as the nodes it kept were: none has entered yet.
        table.entries = 0;
        table
    }

    /// Holds a permit once a node is heard of that the table may ask into
    /// it (see [`Self::next_refresh`]).
    pub fn heard(&self) -> Arc<tokio::sync::Notify> {
        self.heard.clone()
    }

    /// A node a search, a reply or a query told us of. It does not enter
    /// the table: it waits among its bucket's replacements, up to a
    /// bucket's worth of them, until [`Self::next_refresh`] has it asked and
    /// it answers. Whether it was new.
    pub fn heard_about(&mut self, id: Id20, addr: SocketAddr) -> bool {
        // A node of the table heard of at its own address has its failures
        // forgiven (libtorrent `heard_about`, routing_table.cpp:1085-1089,
        // v2.0.15, whose `add_node_impl` zeroes the fail count of the node
        // at that address and id, 671-675).
        if self.buckets.node(&id, addr).is_some() {
            self.mark(&id, None, |n| n.errors_in_a_row = 0);
            return false;
        }
        let heard = self.buckets.heard_about(&self.id, id, addr);
        if heard {
            self.heard.notify_one();
        }
        heard
    }

    /// The node heard of to ask next, if any is due, marked as asked so it
    /// is asked once (see `BucketTree::next_refresh`).
    pub fn next_refresh(&mut self, now: Instant) -> Option<Refresh> {
        self.buckets.next_refresh(&self.id, now)
    }

    /// How many nodes heard of wait to be asked into the table, asked ones
    /// included.
    pub fn heard_of(&self) -> usize {
        self.buckets
            .iter_leaves()
            .map(|l| l.leaf.replacements.len())
            .sum()
    }
    /// Rings each time a node enters the table.
    pub fn entered(&self) -> Arc<tokio::sync::Notify> {
        self.entered.clone()
    }
    /// Whether the table holds SEARCH_MIN_SEEDS confirmed nodes: a search
    /// from it does not lean on the routers.
    pub fn is_seeded(&self) -> bool {
        self.confirmed >= SEARCH_MIN_SEEDS
    }
    /// Rings as the table becomes seeded (see [`Self::is_seeded`]), once
    /// each time its count of confirmed nodes reaches SEARCH_MIN_SEEDS.
    pub fn seeded(&self) -> Arc<tokio::sync::Notify> {
        self.seeded.clone()
    }
    pub fn id(&self) -> Id20 {
        self.id
    }
    pub fn len(&self) -> usize {
        self.size
    }
    /// How many nodes have entered the table since it was made or restored,
    /// one that took a bad node's place included: it grows whenever `len`
    /// does, and also as nodes are replaced.
    pub fn entries(&self) -> u64 {
        self.entries
    }
    /// The `k` nodes nearest `id` that `keep` accepts, nearest first.
    pub fn closest(
        &self,
        id: &Id20,
        k: usize,
        keep: impl FnMut(&RoutingTableNode) -> bool,
    ) -> Vec<&RoutingTableNode> {
        self.buckets.closest(id, k, keep)
    }

    pub fn iter_buckets(&self) -> impl Iterator<Item = BucketTreeIteratorItem<'_>> + '_ {
        self.buckets.iter_leaves()
    }

    pub fn iter(&self) -> impl Iterator<Item = &'_ RoutingTableNode> + '_ {
        self.buckets.iter()
    }

    /// How deep the table reaches towards our own id: past the farthest
    /// bucket, the buckets in a row, far to near, that hold at least half a
    /// bucket of confirmed nodes (libtorrent `routing_table::depth`,
    /// routing_table.cpp:368-391, v2.0.15, over its `live_nodes`). An empty
    /// table, or one that has not split, is 0 deep.
    pub fn depth(&self, now: Instant) -> usize {
        let mut buckets: Vec<(u8, bool, usize)> = self
            .iter_buckets()
            .map(|b| {
                let ours = *b.start <= self.id && self.id <= *b.end_inclusive;
                let confirmed = b.leaf.nodes.iter().filter(|n| n.is_confirmed(now));
                (b.bits, ours, confirmed.count())
            })
            .collect();
        // A bucket nearer our id covers fewer bits; of the two nearest, the
        // one holding our id is last, as libtorrent's last bucket is.
        buckets.sort_unstable_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
        buckets
            .iter()
            .skip(1)
            .take_while(|(_, _, confirmed)| *confirmed >= BUCKET_SIZE / 2)
            .count()
    }

    pub fn add_node(&mut self, id: Id20, addr: SocketAddr) -> InsertResult {
        let mut moved_out = Vec::new();
        let res = self.buckets.add_node(&self.id, id, addr, &mut moved_out);
        let now = Instant::now();
        for node in &moved_out {
            self.size -= 1;
            if node.is_confirmed(now) {
                self.confirmed -= 1;
            }
            debug!(?node, "a split moved a node out of the table");
        }
        let entered = match &res {
            InsertResult::WasExisting | InsertResult::Ignored => false,
            InsertResult::ReplacedBad(..) => true,
            InsertResult::Added => {
                // A node that takes a bad one's place leaves the count as
                // it was.
                self.size += 1;
                true
            }
        };
        if entered {
            self.entries += 1;
            self.entered.notify_waiters();
        }
        res
    }
    // Applies `mark` to the node `id`, if the table holds it, keeping the
    // count of confirmed nodes; `refresh`, if given, refreshes its bucket.
    // Whether the table holds `id`.
    fn mark(
        &mut self,
        id: &Id20,
        refresh: Option<Instant>,
        mark: impl FnOnce(&mut RoutingTableNode),
    ) -> bool {
        let Some(node) = self.buckets.get_mut(id, refresh) else {
            return false;
        };
        let now = Instant::now();
        let was = node.is_confirmed(now);
        mark(node);
        match (was, node.is_confirmed(now)) {
            (false, true) => {
                self.confirmed += 1;
                if self.confirmed == SEARCH_MIN_SEEDS {
                    self.seeded.notify_waiters();
                }
            }
            (true, false) => self.confirmed -= 1,
            _ => {}
        }
        true
    }

    /// Marks the node `id` as asked; a node heard of but not in the table
    /// is marked too, so it is not asked again for the table. What it
    /// marked.
    pub fn mark_outgoing_request(&mut self, id: &Id20, now: Instant) -> Marked {
        if self.mark(id, None, |n| n.mark_outgoing_request(now)) {
            Marked::Node
        } else if self.buckets.mark_replacement_asked(id) {
            Marked::HeardOf
        } else {
            Marked::Nothing
        }
    }

    /// Takes back the mark [`Self::mark_outgoing_request`] gave the node
    /// heard of `id` when it returned [`Marked::HeardOf`]: the question
    /// was withdrawn and its answer never came, so it says nothing of the
    /// node. It may be asked into the table again, so `heard` rings.
    pub fn unmark_heard_of(&mut self, id: &Id20) {
        if self.buckets.unmark_replacement_asked(id) {
            self.heard.notify_one();
        }
    }

    pub fn mark_response(&mut self, id: &Id20, now: Instant) -> bool {
        self.mark(id, Some(now), |n| n.mark_response(now))
    }

    /// The node `id` at `addr` failed to answer: a node at another address
    /// under its id is not the node (libtorrent `routing_table::node_failed`,
    /// routing_table.cpp:1048-1051, v2.0.15). It leaves the table as
    /// `BucketTree::drop_failed` says, and replacements that answered take
    /// its place. Whether the table held it.
    pub fn mark_error(&mut self, id: &Id20, addr: SocketAddr) -> bool {
        if self.buckets.node(id, addr).is_none() {
            return false;
        }
        self.mark(id, None, |n| n.mark_error());
        // Failed, it no longer counts as confirmed, so only the count of
        // nodes changes as it leaves.
        if let Some(entered) = self.buckets.drop_failed(&self.id, id) {
            self.size -= 1;
            for id in entered {
                self.size += 1;
                self.entered_confirmed();
                debug!(?id, "a replacement that answered entered the table");
            }
        }
        true
    }

    // A replacement that answered entered: it is confirmed.
    fn entered_confirmed(&mut self) {
        self.entries += 1;
        self.entered.notify_waiters();
        self.confirmed += 1;
        if self.confirmed == SEARCH_MIN_SEEDS {
            self.seeded.notify_waiters();
        }
    }

    /// A query from the node `id` at `addr`: a node of the table at that
    /// address is marked as having queried us, its failures forgiven
    /// (libtorrent node.cpp:831-832 and routing_table.cpp:671-675, v2.0.15).
    /// Whether the table holds it.
    pub fn mark_last_query(&mut self, id: &Id20, addr: SocketAddr, now: Instant) -> bool {
        if self.buckets.node(id, addr).is_none() {
            return false;
        }
        self.mark(id, None, |n| {
            n.mark_last_query(now);
            n.errors_in_a_row = 0;
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::Cursor,
        net::{Ipv4Addr, SocketAddr, SocketAddrV4},
        str::FromStr,
    };

    use crate::core::hash_id::Id20;
    use rand::RngExt;

    use crate::dht::routing_table::{RoutingTableNode, compute_split_start_end};

    use super::{RoutingTable, generate_random_id};

    #[test]
    fn compute_split_start_end_root() {
        let start = Id20::new([0u8; 20]);
        let end = Id20::new([0xff; 20]);
        assert_eq!(
            compute_split_start_end(start, end, 160),
            (
                (
                    start,
                    Id20::from_str("7fffffffffffffffffffffffffffffffffffffff").unwrap()
                ),
                (
                    Id20::from_str("8000000000000000000000000000000000000000").unwrap(),
                    end
                )
            )
        )
    }

    #[test]
    fn compute_split_start_end_second_split() {
        let start = Id20::from_str("8000000000000000000000000000000000000000").unwrap();
        let end = Id20::new([0xff; 20]);
        assert_eq!(
            compute_split_start_end(start, end, 159),
            (
                (
                    start,
                    Id20::from_str("bfffffffffffffffffffffffffffffffffffffff").unwrap()
                ),
                (
                    Id20::from_str("c000000000000000000000000000000000000000").unwrap(),
                    end
                )
            )
        )
    }

    #[test]
    fn compute_split_start_end_3() {
        let start = Id20::from_str("8000000000000000000000000000000000000000").unwrap();
        let end = Id20::new([0xff; 20]);
        assert_eq!(
            compute_split_start_end(start, end, 159),
            (
                (
                    start,
                    Id20::from_str("bfffffffffffffffffffffffffffffffffffffff").unwrap()
                ),
                (
                    Id20::from_str("c000000000000000000000000000000000000000").unwrap(),
                    end
                )
            )
        )
    }

    fn random_id_20() -> Id20 {
        let mut id20 = [0u8; 20];
        rand::rng().fill(&mut id20);
        Id20::new(id20)
    }

    fn generate_socket_addr() -> SocketAddr {
        let mut ipv4_addr = [0u8; 6];
        rand::rng().fill(&mut ipv4_addr);
        let ip = Ipv4Addr::new(ipv4_addr[0], ipv4_addr[1], ipv4_addr[2], ipv4_addr[3]);
        let port = ((ipv4_addr[4] as u16) << 8) + (ipv4_addr[5] as u16);
        SocketAddrV4::new(ip, port).into()
    }

    fn generate_table(length: Option<usize>) -> RoutingTable {
        let my_id = random_id_20();
        let mut rtable = RoutingTable::new(my_id, None);
        for _ in 0..length.unwrap_or(16536) {
            let other_id = random_id_20();
            let addr = generate_socket_addr();
            rtable.add_node(other_id, addr);
        }
        rtable
    }

    #[test]
    fn test_iter_is_ordered() {
        let table = generate_table(None);
        let mut it = table.buckets.iter();
        let mut previous = it.next().unwrap();
        for node in it {
            assert!(node.id() > previous.id());
            previous = node;
        }
    }

    // The walk gives exactly what copying the table, filtering it and sorting
    // it by distance gives, for every k and filter.
    #[test]
    fn closest_matches_a_full_sort() {
        for size in [0, 1, 7, 8, 9, 64, 512] {
            let rtable = generate_table(Some(size));
            for _ in 0..50 {
                let target = random_id_20();
                let keep = |n: &RoutingTableNode| !n.id.0[19].is_multiple_of(3);
                let mut want: Vec<_> = rtable.iter().filter(|n| keep(n)).collect();
                want.sort_by_key(|n| target.distance(&n.id));
                for k in [1, 8, 20, size + 1] {
                    let got: Vec<_> = rtable
                        .closest(&target, k, keep)
                        .iter()
                        .map(|n| n.id)
                        .collect();
                    let want: Vec<_> = want.iter().take(k).map(|n| n.id).collect();
                    assert_eq!(got, want, "size {size} k {k} target {target:?}");
                }
                let own = rtable.closest(&rtable.id(), 8, |_| true);
                let mut all: Vec<_> = rtable.iter().collect();
                all.sort_by_key(|n| rtable.id().distance(&n.id));
                assert_eq!(
                    own.iter().map(|n| n.id).collect::<Vec<_>>(),
                    all.iter().take(8).map(|n| n.id).collect::<Vec<_>>()
                );
            }
        }
    }

    #[test]
    fn serialize_deserialize_routing_table() {
        let table = generate_table(Some(1000));
        let v = serde_json::to_vec(&table).unwrap();
        let _: RoutingTable = serde_json::from_reader(Cursor::new(v)).unwrap();
    }

    #[test]
    fn test_generate_random_id() {
        let start = Id20::from_str("3000000000000000000000000000000000000000").unwrap();
        let end = Id20::from_str("3fffffffffffffffffffffffffffffffffffffff").unwrap();
        let bits = 156;
        for _ in 0..100 {
            let id = dbg!(generate_random_id(&start, bits));
            assert!(id >= start && id <= end, "{:?}", id);
        }
    }

    // libtorrent `routing_table::add_node_impl` with `dht_restrict_routing_ips`
    // and `compare_ip_cidr`: a bucket holds one node per /24 (v4) and /64 (v6).
    #[test]
    fn test_a_bucket_holds_one_node_per_subnet() {
        use super::InsertResult;

        let mut table = RoutingTable::new(Id20::new([0; 20]), None);
        let mut add =
            |id: u8, addr: &str| table.add_node(Id20::new([id; 20]), addr.parse().unwrap());
        assert!(matches!(add(0x80, "5.6.7.1:6881"), InsertResult::Added));
        assert!(matches!(add(0x81, "5.6.7.2:6881"), InsertResult::Ignored));
        assert!(matches!(add(0x82, "5.6.8.1:6881"), InsertResult::Added));
        assert!(matches!(
            add(0x83, "[2001:db8:1:2::1]:6881"),
            InsertResult::Added
        ));
        assert!(matches!(
            add(0x84, "[2001:db8:1:2:ff::1]:6881"),
            InsertResult::Ignored
        ));
        assert!(matches!(
            add(0x85, "[2001:db8:1:3::1]:6881"),
            InsertResult::Added
        ));
        // The same node again is the same node.
        assert!(matches!(
            add(0x80, "5.6.7.1:6881"),
            InsertResult::WasExisting
        ));
    }

    // The restriction is per bucket: a node shares a /24 with a node of
    // another bucket.
    #[test]
    fn test_the_subnet_restriction_is_per_bucket() {
        use super::InsertResult;

        let mut table = RoutingTable::new(Id20::new([0; 20]), None);
        // FAR far nodes fill the root bucket, and one more splits it: the
        // far half is full and cannot split.
        for i in 0..FAR {
            let (id, addr) = far(i);
            assert!(matches!(table.add_node(id, addr), InsertResult::Added));
        }
        let (id, addr) = other_far();
        assert!(matches!(table.add_node(id, addr), InsertResult::Ignored));
        assert_eq!(table.len(), usize::from(FAR));
        let mut add = |id: u8, addr: [u8; 4]| {
            table.add_node(Id20::new([id; 20]), SocketAddr::from((addr, 6881)))
        };
        // A near node in a far node's /24 goes to the near bucket.
        assert!(matches!(add(0x01, [5, 6, 0, 2]), InsertResult::Added));
        assert!(matches!(add(0x02, [5, 6, 0, 3]), InsertResult::Ignored));
    }

    // Our id is 0x00..: the farthest bucket, index 0, holds the ids that
    // start with a 1 bit, and holds bucket_limit(0) of them, 128.
    const FAR: u8 = 128;

    // The far node `i`, below FAR, each in a /24 of its own.
    fn far(i: u8) -> (Id20, SocketAddr) {
        assert_eq!(usize::from(FAR), super::bucket_limit(0));
        (
            Id20::new([0x80 | i; 20]),
            SocketAddr::from(([5, 6, i, 1], 6881)),
        )
    }

    // A far node none of `far` is, nor shares a /24 with.
    fn other_far() -> (Id20, SocketAddr) {
        let mut id = [0; 20];
        id[0] = 0x90;
        (Id20::new(id), SocketAddr::from(([6, 6, 0, 1], 6881)))
    }

    // The node `j` of the bucket `index` from the farthest, our id being
    // 0x00..: its id has `index` 0 bits, then a 1. Each in a /24 of its own.
    fn at(index: u8, j: u8) -> (Id20, SocketAddr) {
        let mut id = [0; 20];
        id[0] = 0x80 >> index;
        id[1] = j;
        (
            Id20::new(id),
            SocketAddr::from(([10 + index, 0, j, 1], 6881)),
        )
    }

    // How many nodes each bucket holds, farthest first, ours last.
    fn bucket_sizes(table: &RoutingTable) -> Vec<usize> {
        let mut sizes: Vec<_> = table
            .iter_buckets()
            .map(|b| {
                let ours = *b.start <= table.id() && table.id() <= *b.end_inclusive;
                ((std::cmp::Reverse(b.bits), ours), b.leaf.nodes.len())
            })
            .collect();
        sizes.sort();
        sizes.into_iter().map(|(_, len)| len).collect()
    }

    // libtorrent `routing_table::bucket_limit` (routing_table.cpp:277-285,
    // v2.0.15) with `dht_extended_routing_table` on: the farthest four
    // buckets hold 128, 64, 32 and 16 nodes, every other 8. Offered 200
    // nodes for each of the six farthest buckets, far first or near first,
    // the table holds that many of each, and its count is its nodes'.
    #[test]
    fn test_the_far_buckets_hold_more() {
        let want = vec![128, 64, 32, 16, 8, 8, 0];
        for near_first in [false, true] {
            let mut table = RoutingTable::new(Id20::new([0; 20]), None);
            let mut indexes: Vec<u8> = (0..6).collect();
            if near_first {
                indexes.reverse();
            }
            for index in indexes {
                for j in 0..200 {
                    let (id, addr) = at(index, j);
                    table.add_node(id, addr);
                }
            }
            assert_eq!(bucket_sizes(&table), want, "near first: {near_first}");
            assert_eq!(table.len(), want.iter().sum::<usize>());
            assert_eq!(table.len(), table.iter().count());
        }
    }

    // A split whose near half holds more than its limit splits again, and
    // a far half past its limit keeps the nodes that answered: 128 nodes of
    // the third bucket fill the root, then the first far node splits it
    // down to that bucket, which keeps 32, every answered one among them.
    // Of the rest, a bucket's worth wait as its replacements, those that
    // answered first, and the others leave; the count of confirmed nodes
    // follows.
    #[test]
    fn test_a_split_past_a_limit_keeps_the_nodes_that_answered() {
        let now = std::time::Instant::now();
        let mut table = RoutingTable::new(Id20::new([0; 20]), None);
        // Every other one answers: 64 confirmed nodes.
        for j in 0..128 {
            let (id, addr) = at(2, j);
            table.add_node(id, addr);
            if j % 2 == 0 {
                table.mark_response(&id, now);
            }
        }
        assert_eq!(bucket_sizes(&table), vec![128]);
        assert!(table.is_seeded());
        let (id, addr) = at(0, 0);
        table.add_node(id, addr);
        assert_eq!(bucket_sizes(&table), vec![1, 0, 32, 0]);
        assert_eq!(table.len(), 33);
        assert_eq!(
            table.iter().filter(|n| n.is_confirmed(now)).count(),
            32,
            "the 32 that answered stay"
        );
        assert_eq!(table.heard_of(), 8);
        assert!(table.is_seeded());
        // A node of that bucket that fails makes way for a replacement that
        // answered.
        let (gone, addr) = at(2, 0);
        assert!(table.mark_error(&gone, addr));
        assert_eq!(table.len(), 33);
        assert!(!table.iter().any(|n| n.id == gone));
    }

    // A table saved by the code that kept 8 nodes a bucket: the root split
    // once, its far half full with eight nodes, its near half holding three.
    // It loads whole, and its far bucket has room for more.
    #[test]
    fn test_a_table_saved_with_eight_node_buckets_loads() {
        use super::InsertResult;

        let hex = |id: Id20| id.as_string();
        let node = |(id, addr): (Id20, SocketAddr)| {
            format!(
                r#"{{"id":"{}","addr":"{addr}","status":"Unknown","errors_in_a_row":0}}"#,
                hex(id)
            )
        };
        let leaf = |nodes: Vec<(Id20, SocketAddr)>| {
            let nodes: Vec<_> = nodes.into_iter().map(node).collect();
            format!(
                r#"{{"Leaf":{{"nodes":[{}],"last_refreshed":"1.5s"}}}}"#,
                nodes.join(",")
            )
        };
        let far_nodes: Vec<_> = (0..8).map(far).collect();
        let near_nodes: Vec<_> = (0..3).map(|j| at(3, j)).collect();
        let zero = hex(Id20::new([0; 20]));
        let json = format!(
            r#"{{"id":"{zero}","size":11,"buckets":{{"data":[
                {{"bits":160,"start":"{zero}","end_inclusive":"{ff}","data":{{"LeftRight":[1,2]}}}},
                {{"bits":159,"start":"{zero}","end_inclusive":"{left_end}","data":{near}}},
                {{"bits":159,"start":"{right_start}","end_inclusive":"{ff}","data":{far}}}
            ],"size":11,"max_size":512}}}}"#,
            ff = hex(Id20::new([0xff; 20])),
            left_end = "7fffffffffffffffffffffffffffffffffffffff",
            right_start = "8000000000000000000000000000000000000000",
            near = leaf(near_nodes.clone()),
            far = leaf(far_nodes.clone()),
        );
        let saved: RoutingTable = serde_json::from_str(&json).unwrap();
        let mut table = saved.of_family(true);
        assert_eq!(table.len(), 11);
        for (id, _) in far_nodes.iter().chain(&near_nodes) {
            assert!(table.iter().any(|n| n.id == *id));
        }
        let (id, addr) = far(8);
        assert!(matches!(table.add_node(id, addr), InsertResult::Added));
    }

    // A table saved with the far buckets full loads whole; a saved table
    // larger than any the code makes, or one claiming a larger bound, loads
    // within the bucket limits and DEFAULT_MAX_SIZE.
    #[test]
    fn test_a_larger_saved_table_loads_within_the_bounds() {
        let mut table = RoutingTable::new(Id20::new([0; 20]), None);
        for index in 0..6 {
            for j in 0..200 {
                let (id, addr) = at(index, j);
                table.add_node(id, addr);
            }
        }
        let json = serde_json::to_string(&table).unwrap();
        let loaded = serde_json::from_str::<RoutingTable>(&json)
            .unwrap()
            .of_family(true);
        // The same buckets, but for the empty one the extra nodes offered
        // split off near our id.
        let held = |t: &RoutingTable| {
            let mut sizes = bucket_sizes(t);
            sizes.retain(|&n| n > 0);
            sizes
        };
        assert_eq!(held(&loaded), held(&table));
        let ids = |t: &RoutingTable| t.iter().map(|n| n.id).collect::<Vec<_>>();
        assert_eq!(ids(&loaded), ids(&table));

        // One bucket of 4096 nodes of every distance, and a bound of a
        // million: rebuilt, at most DEFAULT_MAX_SIZE, each bucket within its
        // limit.
        let nodes: Vec<String> = (0..4096u32)
            .map(|i| {
                let mut id = [0u8; 20];
                id[..4].copy_from_slice(&i.wrapping_mul(0x9e37_79b9).to_be_bytes());
                id[4..8].copy_from_slice(&i.to_be_bytes());
                let [_, _, a, b] = i.to_be_bytes();
                format!(
                    r#"{{"id":"{}","addr":"20.{a}.{b}.1:6881"}}"#,
                    Id20::new(id).as_string()
                )
            })
            .collect();
        let json = format!(
            r#"{{"id":"{zero}","size":4096,"buckets":{{"data":[{{"bits":160,"start":"{zero}","end_inclusive":"{ff}","data":{{"Leaf":{{"nodes":[{}]}}}}}}],"size":4096,"max_size":1000000}}}}"#,
            nodes.join(","),
            zero = Id20::new([0; 20]).as_string(),
            ff = Id20::new([0xff; 20]).as_string(),
        );
        let saved: RoutingTable = serde_json::from_str(&json).unwrap();
        assert_eq!(saved.iter().count(), 4096);
        let loaded = saved.of_family(true);
        assert!(loaded.len() <= RoutingTable::DEFAULT_MAX_SIZE);
        assert_eq!(loaded.len(), loaded.iter().count());
        for b in loaded.iter_buckets() {
            let node = loaded
                .buckets
                .data
                .iter()
                .find(|n| n.start == *b.start)
                .unwrap();
            assert!(b.leaf.nodes.len() <= node.limit(&loaded.id()));
        }
        // The cap is reached only past 38 buckets (see DEFAULT_MAX_SIZE):
        // these 4096 nodes fill far fewer.
        assert!(loaded.len() < RoutingTable::DEFAULT_MAX_SIZE);
    }

    // Our id is 0x00..; the far bucket is 0x80.. once the root splits. The
    // node `id` answers, then fails once: bad, and still in a bucket that
    // has no replacement.
    fn bad(table: &mut RoutingTable, id: Id20) {
        let now = std::time::Instant::now();
        table.mark_outgoing_request(&id, now);
        table.mark_response(&id, now);
        let addr = table.iter().find(|n| n.id == id).unwrap().addr;
        assert!(table.mark_error(&id, addr));
    }

    // libtorrent routing_table.cpp:811-816: a bucket with room takes a new
    // node beside a bad one, which keeps its place until the bucket is full.
    #[test]
    fn test_a_bucket_with_room_keeps_its_bad_node() {
        use super::InsertResult;

        let mut table = RoutingTable::new(Id20::new([0; 20]), None);
        let first = Id20::new([0x80; 20]);
        table.add_node(first, SocketAddr::from(([5, 6, 0, 1], 6881)));
        bad(&mut table, first);
        assert!(matches!(
            table.add_node(
                Id20::new([0x81; 20]),
                SocketAddr::from(([5, 6, 1, 1], 6881))
            ),
            InsertResult::Added
        ));
        assert_eq!(table.len(), 2);
        assert!(table.iter().any(|n| n.id == first));
    }

    // A full bucket that cannot split gives a bad node's place to a new one
    // (routing_table.cpp:851, `replace_node_impl`), and the count stays.
    #[test]
    fn test_a_full_far_bucket_gives_a_bad_nodes_place() {
        use super::InsertResult;

        let mut table = RoutingTable::new(Id20::new([0; 20]), None);
        for i in 0..FAR {
            let (id, addr) = far(i);
            table.add_node(id, addr);
        }
        // A near node splits the root: the FAR far ones are a bucket of
        // their own, full, without our id.
        table.add_node(
            Id20::new([0x01; 20]),
            SocketAddr::from(([5, 7, 0, 1], 6881)),
        );
        assert_eq!(table.len(), usize::from(FAR) + 1);
        let (new, addr) = other_far();
        assert!(matches!(table.add_node(new, addr), InsertResult::Ignored));
        bad(&mut table, Id20::new([0x83; 20]));
        assert!(matches!(
            table.add_node(new, addr),
            InsertResult::ReplacedBad(_)
        ));
        assert_eq!(table.len(), usize::from(FAR) + 1);
        assert!(table.iter().any(|n| n.id == new));
    }

    // An IPv4 address written as IPv6 is no node: it neither enters the
    // table nor is heard of.
    #[test]
    fn test_a_mapped_address_is_never_a_node() {
        use super::InsertResult;

        let mut table = RoutingTable::new(Id20::new([0; 20]), None);
        let mapped: SocketAddr = "[::ffff:5.6.7.8]:6881".parse().unwrap();
        assert!(matches!(
            table.add_node(Id20::new([0x80; 20]), mapped),
            InsertResult::Ignored
        ));
        assert!(!table.heard_about(Id20::new([0x80; 20]), mapped));
        assert_eq!(table.len(), 0);
        assert_eq!(table.heard_of(), 0);
    }

    // A node heard of waits among its bucket's replacements; it is asked
    // once, the nearest bucket's first, and leaves them as it enters.
    #[test]
    fn test_nodes_heard_of_are_asked_once_nearest_first() {
        let now = std::time::Instant::now();
        let mut table = RoutingTable::new(Id20::new([0; 20]), None);
        assert_eq!(table.next_refresh(now), None);
        let far = (
            Id20::new([0x80; 20]),
            SocketAddr::from(([5, 6, 0, 1], 6881)),
        );
        let near = (
            Id20::new([0x01; 20]),
            SocketAddr::from(([5, 7, 0, 1], 6881)),
        );
        assert!(table.heard_about(far.0, far.1));
        assert!(table.heard_about(near.0, near.1));
        // Heard again, or at a /24 already heard of: nothing new.
        assert!(!table.heard_about(far.0, far.1));
        assert!(!table.heard_about(
            Id20::new([0x82; 20]),
            SocketAddr::from(([5, 6, 0, 2], 6881))
        ));
        assert_eq!(table.len(), 0);
        assert_eq!(table.heard_of(), 2);

        // One bucket: both are as near; the first heard goes first.
        let first = table.next_refresh(now).unwrap();
        assert_eq!((first.id, first.addr, first.ping), (far.0, far.1, false));
        let second = table.next_refresh(now).unwrap();
        assert_eq!(second.id, near.0);
        assert_eq!(table.next_refresh(now), None);

        // It answers and enters: it is no longer heard of.
        table.add_node(near.0, near.1);
        assert_eq!(table.heard_of(), 1);
        assert_eq!(table.len(), 1);
    }

    // The replacements of a bucket that splits go with their ids, and the
    // near bucket's are asked first. A full far bucket's replacement waits
    // while the node asked least recently is refreshed instead, until one of
    // the bucket's nodes fails: with a replacement waiting, that node leaves
    // at once (routing_table.cpp:1071-1074, v2.0.15), and the replacement is
    // asked next.
    #[test]
    fn test_replacements_follow_a_split_and_the_near_bucket_goes_first() {
        use super::InsertResult;

        let now = std::time::Instant::now();
        let mut table = RoutingTable::new(Id20::new([0; 20]), None);
        for i in 0..FAR {
            let (id, addr) = far(i);
            table.add_node(id, addr);
        }
        let far = other_far();
        let near = Id20::new([0x02; 20]);
        table.heard_about(far.0, far.1);
        table.heard_about(near, SocketAddr::from(([6, 7, 0, 1], 6881)));
        table.add_node(
            Id20::new([0x01; 20]),
            SocketAddr::from(([5, 7, 0, 1], 6881)),
        );
        let ids: Vec<_> = table.iter().map(|n| n.id).collect();
        for id in &ids {
            table.mark_response(id, now);
        }
        assert_eq!(table.heard_of(), 2);
        let next = table.next_refresh(now).unwrap();
        assert_eq!((next.id, next.ping), (near, false));
        assert!(!next.target.get_bit(0), "{:?}", next.target);
        let next = table.next_refresh(now).unwrap();
        assert_ne!(next.id, far.0);
        assert!(ids.contains(&next.id));

        let failed = Id20::new([0x83; 20]);
        bad(&mut table, failed);
        assert!(!table.iter().any(|n| n.id == failed));
        assert_eq!(table.len(), usize::from(FAR));
        let next = table.next_refresh(now).unwrap();
        assert_eq!((next.id, next.ping), (far.0, false));
        assert!(matches!(table.add_node(far.0, far.1), InsertResult::Added));
        assert_eq!(table.heard_of(), 1);
    }

    // A node of a full far bucket that fails leaves at once when a
    // replacement has answered, and that replacement takes its place, a node
    // we can count on (libtorrent `node_failed` and `fill_from_replacements`,
    // routing_table.cpp:1071-1074 and 520-539, v2.0.15).
    #[test]
    fn test_a_failed_node_makes_way_for_a_replacement_that_answered() {
        use super::InsertResult;

        let now = std::time::Instant::now();
        let mut table = full_far_bucket(now);
        let far = other_far();
        // Heard of, then answering a query of ours: the full bucket has no
        // room for it, so it waits among the replacements.
        assert!(table.heard_about(far.0, far.1));
        assert!(matches!(
            table.add_node(far.0, far.1),
            InsertResult::Ignored
        ));
        assert!(!table.iter().any(|n| n.id == far.0));
        let entered = table.entries();

        let failed = Id20::new([0x83; 20]);
        assert!(table.mark_error(&failed, SocketAddr::from(([5, 6, 3, 1], 6881))));
        assert!(!table.iter().any(|n| n.id == failed));
        let node = table.iter().find(|n| n.id == far.0).unwrap();
        assert!(node.is_confirmed(now));
        assert_eq!(table.len(), usize::from(FAR) + 1);
        assert_eq!(table.entries(), entered + 1);
        assert_eq!(table.heard_of(), 0);
    }

    // The FAR far nodes 0x80.. and the near node 0x01, all answered: the
    // far bucket is full and cannot split.
    fn full_far_bucket(now: std::time::Instant) -> RoutingTable {
        let mut table = RoutingTable::new(Id20::new([0; 20]), None);
        for i in 0..FAR {
            let (id, addr) = far(i);
            table.add_node(id, addr);
        }
        table.add_node(
            Id20::new([0x01; 20]),
            SocketAddr::from(([5, 7, 0, 1], 6881)),
        );
        let ids: Vec<_> = table.iter().map(|n| n.id).collect();
        for id in &ids {
            table.mark_response(id, now);
        }
        assert_eq!(table.len(), usize::from(FAR) + 1);
        table
    }

    // With no replacement to take its place, a node that has answered stays
    // through nineteen failures in a row and leaves at the twentieth
    // (`dht_max_fail_count`, routing_table.cpp:1063, v2.0.15).
    #[test]
    fn test_a_node_with_no_replacement_leaves_at_its_twentieth_failure() {
        let now = std::time::Instant::now();
        let mut table = RoutingTable::new(Id20::new([0; 20]), None);
        let (id, addr) = (
            Id20::new([0x80; 20]),
            SocketAddr::from(([5, 6, 0, 1], 6881)),
        );
        table.add_node(id, addr);
        table.mark_response(&id, now);
        for _ in 0..19 {
            assert!(table.mark_error(&id, addr));
        }
        assert_eq!(
            table.iter().find(|n| n.id == id).unwrap().errors_in_a_row(),
            19
        );
        assert!(table.mark_error(&id, addr));
        assert_eq!(table.len(), 0);
        assert!(!table.iter().any(|n| n.id == id));
    }

    // A node that never answered, as a restored one, leaves at its first
    // failure (routing_table.cpp:1063-1067, v2.0.15).
    #[test]
    fn test_a_node_that_never_answered_leaves_at_its_first_failure() {
        let mut table = RoutingTable::new(Id20::new([0; 20]), None);
        let (id, addr) = (
            Id20::new([0x80; 20]),
            SocketAddr::from(([5, 6, 0, 1], 6881)),
        );
        table.add_node(id, addr);
        assert!(table.mark_error(&id, addr));
        assert_eq!(table.len(), 0);
    }

    // A failure, a query or a hearing at another address than the node's is
    // not the node's (routing_table.cpp:1048-1051, 671-675, v2.0.15). At its
    // own address a query from it, or a hearing of it, forgives its failures.
    #[test]
    fn test_only_the_nodes_own_address_fails_or_forgives_it() {
        let now = std::time::Instant::now();
        let mut table = RoutingTable::new(Id20::new([0; 20]), None);
        let (id, addr) = (
            Id20::new([0x80; 20]),
            SocketAddr::from(([5, 6, 0, 1], 6881)),
        );
        let elsewhere = SocketAddr::from(([5, 9, 0, 1], 6881));
        table.add_node(id, addr);
        table.mark_response(&id, now);
        let errors =
            |table: &RoutingTable| table.iter().find(|n| n.id == id).unwrap().errors_in_a_row();

        assert!(!table.mark_error(&id, elsewhere));
        assert_eq!(errors(&table), 0);
        assert!(table.mark_error(&id, addr));
        assert_eq!(errors(&table), 1);
        assert!(!table.mark_last_query(&id, elsewhere, now));
        assert!(!table.heard_about(id, elsewhere));
        assert_eq!(errors(&table), 1);
        assert!(table.mark_last_query(&id, addr, now));
        assert_eq!(errors(&table), 0);
        assert!(table.mark_error(&id, addr));
        assert!(!table.heard_about(id, addr));
        assert_eq!(errors(&table), 0);
    }

    // A full bucket gives the place of the node that failed most to a new
    // node (libtorrent `replace_node_impl`, routing_table.cpp:165-178,
    // v2.0.15).
    #[test]
    fn test_a_new_node_takes_the_place_of_the_node_that_failed_most() {
        use super::InsertResult;

        let now = std::time::Instant::now();
        let mut table = full_far_bucket(now);
        let addr_of = |i: u8| SocketAddr::from(([5, 6, i, 1], 6881));
        assert!(table.mark_error(&Id20::new([0x83; 20]), addr_of(3)));
        assert!(table.mark_error(&Id20::new([0x85; 20]), addr_of(5)));
        assert!(table.mark_error(&Id20::new([0x85; 20]), addr_of(5)));
        let (new, addr) = other_far();
        match table.add_node(new, addr) {
            InsertResult::ReplacedBad(gone) => assert_eq!(gone.id, Id20::new([0x85; 20])),
            other => panic!("{other:?}"),
        }
        assert!(table.iter().any(|n| n.id == Id20::new([0x83; 20])));
        assert_eq!(table.len(), usize::from(FAR) + 1);
    }

    // Each node of the table is refreshed in turn: one never asked first,
    // then the one asked least recently.
    #[test]
    fn test_the_node_asked_least_recently_is_refreshed_next() {
        let now = std::time::Instant::now();
        let mut table = RoutingTable::new(Id20::new([0; 20]), None);
        let ids: Vec<_> = (0..3u8).map(|i| Id20::new([0x80 + i; 20])).collect();
        for (i, id) in (0u8..).zip(&ids) {
            table.add_node(*id, SocketAddr::from(([5, 6, i, 1], 6881)));
        }
        table.mark_response(&ids[0], now);
        table.mark_response(&ids[2], now + std::time::Duration::from_secs(1));
        // Never asked: the node 0x81.
        let asked = now + std::time::Duration::from_secs(2);
        assert_eq!(table.next_refresh(asked).unwrap().id, ids[1]);
        let later = now + std::time::Duration::from_secs(3);
        let order: Vec<_> = (0..3)
            .map(|_| table.next_refresh(later).unwrap().id)
            .collect();
        assert_eq!(order, vec![ids[0], ids[2], ids[1]]);
    }

    // A bucket keeps at most a bucket's worth of replacements, an asked one
    // making way first.
    #[test]
    fn test_replacements_are_bounded() {
        let now = std::time::Instant::now();
        let mut table = RoutingTable::new(Id20::new([0; 20]), None);
        for i in 0..8 {
            table.heard_about(
                Id20::new([0x80 + i; 20]),
                SocketAddr::from(([5, 6, i, 1], 6881)),
            );
        }
        let asked = table.next_refresh(now).unwrap().id;
        assert_eq!(asked, Id20::new([0x80; 20]));
        table.heard_about(
            Id20::new([0x90; 20]),
            SocketAddr::from(([5, 6, 9, 1], 6881)),
        );
        assert_eq!(table.heard_of(), 8);
        let mut left = Vec::new();
        while let Some(next) = table.next_refresh(now) {
            left.push(next.id);
        }
        assert_eq!(left.len(), 8);
        assert!(!left.contains(&asked));
    }

    // A restored table keeps only its own family's nodes.
    #[test]
    fn test_a_table_of_a_family_drops_the_others() {
        let mut table = RoutingTable::new(Id20::new([0; 20]), None);
        table.add_node(
            Id20::new([0x80; 20]),
            SocketAddr::from(([5, 6, 0, 1], 6881)),
        );
        table.add_node(Id20::new([0x81; 20]), "[2001:db8::1]:6881".parse().unwrap());
        let v6 = table.clone().of_family(false);
        assert_eq!(v6.len(), 1);
        assert!(v6.iter().all(|n| n.addr.is_ipv6()));
        assert_eq!(v6.entries(), 0);
        let v4 = table.clone().of_family(true);
        assert_eq!(v4.len(), 1);
        assert!(v4.iter().all(|n| n.addr.is_ipv4()));

        // A table saved with an IPv4 address written as IPv6 keeps it in
        // neither family.
        let json = serde_json::to_string(&table)
            .unwrap()
            .replace("\"5.6.0.1:6881\"", "\"[::ffff:5.6.0.1]:6881\"");
        let saved: RoutingTable = serde_json::from_str(&json).unwrap();
        assert!(saved.iter().any(|n| super::is_v4_mapped(n.addr)));
        assert_eq!(saved.clone().of_family(true).len(), 0);
        let v6 = saved.of_family(false);
        assert_eq!(v6.len(), 1);
        assert!(v6.iter().all(|n| !super::is_v4_mapped(n.addr)));
    }

    // Nodes that are in the table but have not answered, as restored ones,
    // do not seed it. It is seeded, and rings, as the third of them
    // answers, though none entered then; a node that fails once is no
    // longer counted, and the table rings again as one more answers.
    #[test]
    fn test_the_table_is_seeded_as_its_third_node_answers() {
        use crate::dht::SEARCH_MIN_SEEDS;
        use futures::FutureExt;
        use std::time::Instant;

        let mut table = RoutingTable::new(Id20::new([0; 20]), None);
        let ids: Vec<_> = (1..=4u8).map(|i| Id20::new([0x80 | i; 20])).collect();
        for (i, id) in (1u8..).zip(&ids) {
            table.add_node(*id, SocketAddr::from(([5, 6, i, 1], 6881)));
        }
        assert!(!table.is_seeded());
        let seeded = table.seeded();
        let now = Instant::now();
        for id in &ids[..SEARCH_MIN_SEEDS - 1] {
            table.mark_response(id, now);
        }
        assert!(!table.is_seeded());
        let mut rang = std::pin::pin!(seeded.notified());
        rang.as_mut().enable();
        table.mark_response(&ids[SEARCH_MIN_SEEDS - 1], now);
        assert!(table.is_seeded());
        assert!(rang.as_mut().now_or_never().is_some());
        // Answering again counts once.
        table.mark_response(&ids[0], now);
        let mut rang = std::pin::pin!(seeded.notified());
        rang.as_mut().enable();
        assert!(table.mark_error(&ids[0], SocketAddr::from(([5, 6, 1, 1], 6881))));
        assert!(!table.is_seeded());
        table.mark_response(&ids[3], now);
        assert!(table.is_seeded());
        assert!(rang.as_mut().now_or_never().is_some());
    }
}
