// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::{
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::Instant,
};

use crate::core::hash_id::Id20;
use rand::Rng;
use serde::{Deserialize, Serialize, Serializer, ser::SerializeStruct};
use tracing::{debug, trace};

use crate::dht::INACTIVITY_TIMEOUT;

// The nodes a bucket holds: BEP 5's k.
const BUCKET_SIZE: usize = 8;

/// Whether `addr` is an IPv4 address written as IPv6 (`::ffff:a.b.c.d`). It is
/// never a node of either family: an IPv6 node is asked over IPv6, and an
/// IPv4 node is kept under its IPv4 address.
pub fn is_v4_mapped(addr: SocketAddr) -> bool {
    matches!(addr, SocketAddr::V6(a) if a.ip().to_ipv4_mapped().is_some())
}

// A node heard of, not yet in its bucket: libtorrent's replacement bucket
// (routing_table.cpp:879-900, v2.0.15), up to BUCKET_SIZE per bucket, the
// oldest asked one, else the oldest, making way for a new one. It enters the
// bucket only once it answers.
#[derive(Clone, Debug)]
struct Replacement {
    id: Id20,
    addr: SocketAddr,
    asked: bool,
}

#[derive(Clone, Debug)]
pub struct LeafBucket {
    pub nodes: Vec<RoutingTableNode>,
    pub last_refreshed: Instant,
    // Not saved: a restored table hears of its nodes again.
    replacements: Vec<Replacement>,
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
        Tmp::deserialize(deserializer).map(|t| Self {
            nodes: t.nodes,
            last_refreshed: Instant::now(),
            replacements: Vec::new(),
        })
    }
}

impl Default for LeafBucket {
    fn default() -> Self {
        Self {
            nodes: Default::default(),
            last_refreshed: Instant::now(),
            replacements: Vec::new(),
        }
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

// Whether two addresses are in the same /24 (v4) or /64 (v6), libtorrent's
// `compare_ip_cidr`.
fn same_subnet(a: SocketAddr, b: SocketAddr) -> bool {
    match (a.ip().to_canonical(), b.ip().to_canonical()) {
        (IpAddr::V4(a), IpAddr::V4(b)) => a.octets()[..3] == b.octets()[..3],
        (IpAddr::V6(a), IpAddr::V6(b)) => a.octets()[..8] == b.octets()[..8],
        _ => false,
    }
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
    // by bucket, and stops once `k` are found, with no copy of the table and
    // no sort beyond a bucket's eight.
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
                    out[from..].sort_unstable_by_key(|n| target.distance(&n.id));
                    out.truncate(k);
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
                let r = leaf.nodes.iter_mut().find(|b| b.id == *id);
                if r.is_some()
                    && let Some(refresh) = refresh
                {
                    leaf.last_refreshed = refresh;
                }
                r
            }
            BucketTreeNodeData::LeftRight(_, _) => unreachable!(),
        }
    }

    pub fn add_node(&mut self, self_id: &Id20, id: Id20, addr: SocketAddr) -> InsertResult {
        if is_v4_mapped(addr) {
            return InsertResult::Ignored;
        }
        let idx = self.get_leaf(&id);
        let res = self.insert_into_leaf(idx, self_id, id, addr);
        if matches!(res, InsertResult::Added | InsertResult::ReplacedBad(_)) {
            let idx = self.get_leaf(&id);
            if let BucketTreeNodeData::Leaf(leaf) = &mut self.data[idx].data {
                leaf.replacements.retain(|r| r.id != id);
            }
        }
        res
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
        if leaf
            .nodes
            .iter()
            .any(|n| n.id == id || same_subnet(n.addr, addr))
            || leaf
                .replacements
                .iter()
                .any(|r| r.id == id || same_subnet(r.addr, addr))
        {
            return false;
        }
        if leaf.replacements.len() >= BUCKET_SIZE {
            let gone = leaf.replacements.iter().position(|r| r.asked).unwrap_or(0);
            leaf.replacements.remove(gone);
        }
        leaf.replacements.push(Replacement {
            id,
            addr,
            asked: false,
        });
        true
    }

    // Marks the replacement `id` as asked; whether there was one.
    fn mark_replacement_asked(&mut self, id: &Id20) -> bool {
        let idx = self.get_leaf(id);
        match self
            .leaf_mut(idx)
            .replacements
            .iter_mut()
            .find(|r| r.id == *id)
        {
            Some(r) => {
                r.asked = true;
                true
            }
            None => false,
        }
    }

    // The replacement to ask next, marked as asked: one not asked yet, of
    // the nearest bucket that has room for it, can split, or holds a bad
    // node it may take the place of (libtorrent `routing_table::
    // next_refresh`, routing_table.cpp:392-445, which looks for an unpinged
    // replacement in a bucket that is not full or can split, nearest bucket
    // first).
    fn next_refresh(&mut self, self_id: &Id20, now: Instant) -> Option<Refresh> {
        let room = self.size < self.max_size;
        // Nearer buckets cover fewer bits; of the two nearest, ours first.
        let mut best: Option<((u8, bool), usize)> = None;
        for (idx, node) in self.data.iter().enumerate() {
            let BucketTreeNodeData::Leaf(leaf) = &node.data else {
                continue;
            };
            if !leaf.replacements.iter().any(|r| !r.asked) {
                continue;
            }
            let ours = node.start <= *self_id && *self_id <= node.end_inclusive;
            let fits = (room && (leaf.nodes.len() < BUCKET_SIZE || ours))
                || leaf.nodes.iter().any(|n| n.status(now) == NodeStatus::Bad);
            let key = (node.bits, !ours);
            if fits && best.is_none_or(|(best, _)| key < best) {
                best = Some((key, idx));
            }
        }
        let (_, idx) = best?;
        let (start, bits) = (self.data[idx].start, self.data[idx].bits);
        let leaf = self.leaf_mut(idx);
        let ping = leaf.nodes.len() >= BUCKET_SIZE;
        let r = leaf.replacements.iter_mut().find(|r| !r.asked)?;
        r.asked = true;
        Some(Refresh {
            id: r.id,
            addr: r.addr,
            target: generate_random_id(&start, bits),
            ping,
        })
    }

    fn insert_into_leaf(
        &mut self,
        mut idx: usize,
        self_id: &Id20,
        id: Id20,
        addr: SocketAddr,
    ) -> InsertResult {
        // The loop here is for this case:
        // in case we split a node into two, and it degenerates into all the leaves
        // being on one side, we'll need to split again "recursively" until there's space
        // for the new node.
        // The loop is to remove the recursion. NOTE: it might have compiled to tail recursion
        // anyway, but whatever, did not check.
        let now = Instant::now();
        loop {
            let leaf = &mut self.data[idx];
            let nodes = match &mut leaf.data {
                BucketTreeNodeData::Leaf(nodes) => nodes,
                BucketTreeNodeData::LeftRight(_, _) => unreachable!(),
            };
            // if already found, quit
            if nodes.nodes.iter().any(|r| r.id == id) {
                return InsertResult::WasExisting;
            }
            // One node per /24 (v4) or /64 (v6) in a bucket: a node close to
            // one we hold, under another id, is not trusted (libtorrent
            // `routing_table::add_node_impl` with `dht_restrict_routing_ips`,
            // `compare_ip_cidr`).
            if nodes.nodes.iter().any(|r| same_subnet(r.addr, addr)) {
                return InsertResult::Ignored;
            }

            let mut new_node = RoutingTableNode {
                id,
                addr,
                last_request: None,
                last_response: None,
                last_query: None,
                errors_in_a_row: 0,
            };

            // As libtorrent's `add_node_impl` (routing_table.cpp:811-851,
            // v2.0.15): a bucket with room takes the node, then the bucket
            // holding our id splits, and only a full bucket that cannot
            // split gives a bad node's place to it. The table never holds
            // more than `max_size`.
            let full = self.size >= self.max_size;
            if !full && nodes.nodes.len() < BUCKET_SIZE {
                nodes.nodes.push(new_node);
                nodes.nodes.sort_by_key(|n| n.id);
                nodes.last_refreshed = now;
                self.size += 1;
                return InsertResult::Added;
            }

            let ours = leaf.start <= *self_id && *self_id <= leaf.end_inclusive;
            if full || !ours {
                if let Some(bad_node) = nodes
                    .nodes
                    .iter_mut()
                    .find(|r| matches!(r.status(now), NodeStatus::Bad))
                {
                    std::mem::swap(bad_node, &mut new_node);
                    nodes.nodes.sort_by_key(|n| n.id);
                    debug!("replaced bad node {:?}", new_node);
                    nodes.last_refreshed = now;
                    return InsertResult::ReplacedBad(new_node);
                }
                if full {
                    trace!(
                        "can't add node to routing table, max size of {} reached",
                        self.max_size
                    );
                }
                return InsertResult::Ignored;
            }

            // Split
            let ((ls, le), (rs, re)) =
                compute_split_start_end(leaf.start, leaf.end_inclusive, leaf.bits);
            let (mut ld, mut rd) = (Vec::new(), Vec::new());
            for node in nodes.nodes.drain(0..) {
                if node.id < rs {
                    ld.push(node);
                } else {
                    rd.push(node)
                }
            }
            let (lr, rr) = nodes.replacements.drain(0..).partition(|r| r.id < rs);

            let left = BucketTreeNode {
                bits: leaf.bits - 1,
                start: ls,
                end_inclusive: le,
                data: BucketTreeNodeData::Leaf(LeafBucket {
                    nodes: ld,
                    replacements: lr,
                    ..Default::default()
                }),
            };
            let right = BucketTreeNode {
                bits: leaf.bits - 1,
                start: rs,
                end_inclusive: re,
                data: BucketTreeNodeData::Leaf(LeafBucket {
                    nodes: rd,
                    replacements: rr,
                    ..Default::default()
                }),
            };

            let left_idx = {
                let l = self.data.len();
                self.data.push(left);
                l
            };
            let right_idx = {
                let l = self.data.len();
                self.data.push(right);
                l
            };

            self.data[idx].data = BucketTreeNodeData::LeftRight(left_idx, right_idx);
            if id < rs {
                idx = left_idx
            } else {
                idx = right_idx
            }
        }
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
            // Nodes become bad when they fail to respond to multiple queries in a row.
            (Some(_), _, _) if self.errors_in_a_row >= 2 => NodeStatus::Bad,

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
}

impl RoutingTable {
    const DEFAULT_MAX_SIZE: usize = 512;

    pub fn new(id: Id20, max_size: Option<usize>) -> Self {
        Self {
            id,
            buckets: BucketTree::new(max_size.unwrap_or(Self::DEFAULT_MAX_SIZE)),
            size: 0,
            entered: Default::default(),
            entries: 0,
            heard: Default::default(),
        }
    }

    /// The table with only the nodes of the IPv4 family when `is_v4`, the
    /// IPv6 one when not, never an IPv4 address written as IPv6. A table
    /// that holds no other is returned as it is.
    pub fn of_family(self, is_v4: bool) -> Self {
        let keeps = |addr: SocketAddr| addr.is_ipv4() == is_v4 && !is_v4_mapped(addr);
        if self.iter().all(|n| keeps(n.addr)) {
            return self;
        }
        let mut table = Self::new(self.id, Some(self.buckets.max_size));
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
        let res = self.buckets.add_node(&self.id, id, addr);
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
    /// Marks the node `id` as asked; a node heard of but not in the table
    /// is marked too, so it is not asked again for the table. Whether the
    /// table holds `id`.
    pub fn mark_outgoing_request(&mut self, id: &Id20, now: Instant) -> bool {
        let r = match self.buckets.get_mut(id, None) {
            Some(r) => r,
            None => {
                self.buckets.mark_replacement_asked(id);
                return false;
            }
        };
        r.mark_outgoing_request(now);
        true
    }

    pub fn mark_response(&mut self, id: &Id20, now: Instant) -> bool {
        let r = match self.buckets.get_mut(id, Some(now)) {
            Some(r) => r,
            None => return false,
        };
        r.mark_response(now);
        true
    }

    pub fn mark_error(&mut self, id: &Id20) -> bool {
        let r = match self.buckets.get_mut(id, None) {
            Some(r) => r,
            None => return false,
        };
        r.mark_error();
        true
    }

    pub fn mark_last_query(&mut self, id: &Id20, now: Instant) -> bool {
        let r = match self.buckets.get_mut(id, None) {
            Some(r) => r,
            None => return false,
        };
        r.mark_last_query(now);
        true
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
        let mut add = |id: u8, addr: [u8; 4]| {
            table.add_node(Id20::new([id; 20]), SocketAddr::from((addr, 6881)))
        };
        // Eight far nodes fill the root bucket, and a ninth splits it.
        for i in 0..8 {
            assert!(matches!(add(0x80 + i, [5, 6, i, 1]), InsertResult::Added));
        }
        assert!(matches!(add(0x88, [5, 6, 8, 1]), InsertResult::Ignored));
        // A near node in a far node's /24 goes to the near bucket.
        assert!(matches!(add(0x01, [5, 6, 0, 2]), InsertResult::Added));
        assert!(matches!(add(0x02, [5, 6, 0, 3]), InsertResult::Ignored));
    }

    // Our id is 0x00..; the far bucket is 0x80.. once the root splits.
    fn bad(table: &mut RoutingTable, id: Id20) {
        let now = std::time::Instant::now();
        table.mark_outgoing_request(&id, now);
        table.mark_error(&id);
        table.mark_error(&id);
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
        for i in 0..8 {
            table.add_node(
                Id20::new([0x80 + i; 20]),
                SocketAddr::from(([5, 6, i, 1], 6881)),
            );
        }
        // A near node splits the root: the eight far ones are a bucket of
        // their own, full, without our id.
        table.add_node(
            Id20::new([0x01; 20]),
            SocketAddr::from(([5, 7, 0, 1], 6881)),
        );
        assert_eq!(table.len(), 9);
        let far = Id20::new([0x90; 20]);
        assert!(matches!(
            table.add_node(far, SocketAddr::from(([5, 6, 9, 1], 6881))),
            InsertResult::Ignored
        ));
        bad(&mut table, Id20::new([0x83; 20]));
        assert!(matches!(
            table.add_node(far, SocketAddr::from(([5, 6, 9, 1], 6881))),
            InsertResult::ReplacedBad(_)
        ));
        assert_eq!(table.len(), 9);
        assert!(table.iter().any(|n| n.id == far));
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
    // near bucket's are asked first.
    #[test]
    fn test_replacements_follow_a_split_and_the_near_bucket_goes_first() {
        let now = std::time::Instant::now();
        let mut table = RoutingTable::new(Id20::new([0; 20]), None);
        for i in 0..8 {
            table.add_node(
                Id20::new([0x80 + i; 20]),
                SocketAddr::from(([5, 6, i, 1], 6881)),
            );
        }
        let far = Id20::new([0x90; 20]);
        let near = Id20::new([0x02; 20]);
        table.heard_about(far, SocketAddr::from(([6, 6, 0, 1], 6881)));
        table.heard_about(near, SocketAddr::from(([6, 7, 0, 1], 6881)));
        table.add_node(
            Id20::new([0x01; 20]),
            SocketAddr::from(([5, 7, 0, 1], 6881)),
        );
        assert_eq!(table.heard_of(), 2);
        let next = table.next_refresh(now).unwrap();
        assert_eq!((next.id, next.ping), (near, false));
        assert!(!next.target.get_bit(0), "{:?}", next.target);
        // The far bucket is full and cannot split: its replacement waits.
        assert_eq!(table.next_refresh(now), None);
        // Until one of its nodes turns bad; then it is pinged.
        bad(&mut table, Id20::new([0x83; 20]));
        let next = table.next_refresh(now).unwrap();
        assert_eq!((next.id, next.ping), (far, true));
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
}
