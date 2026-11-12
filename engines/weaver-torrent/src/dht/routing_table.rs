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

#[derive(Clone, Debug)]
pub struct LeafBucket {
    pub nodes: Vec<RoutingTableNode>,
    pub last_refreshed: Instant,
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
        })
    }
}

impl Default for LeafBucket {
    fn default() -> Self {
        Self {
            nodes: Default::default(),
            last_refreshed: Instant::now(),
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
        let idx = self.get_leaf(&id);
        self.insert_into_leaf(idx, self_id, id, addr)
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

            // Try replace a bad node

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

            // if max size reached, don't bother
            if self.size == self.max_size {
                trace!(
                    "can't add node to routing table, max size of {} reached",
                    self.max_size
                );
                return InsertResult::Ignored;
            }

            if nodes.nodes.len() < BUCKET_SIZE {
                nodes.nodes.push(new_node);
                nodes.nodes.sort_by_key(|n| n.id);
                nodes.last_refreshed = now;
                self.size += 1;
                return InsertResult::Added;
            }

            // if our id is not inside, don't bother.
            if *self_id < leaf.start || *self_id > leaf.end_inclusive {
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

            let left = BucketTreeNode {
                bits: leaf.bits - 1,
                start: ls,
                end_inclusive: le,
                data: BucketTreeNodeData::Leaf(LeafBucket {
                    nodes: ld,
                    ..Default::default()
                }),
            };
            let right = BucketTreeNode {
                bits: leaf.bits - 1,
                start: rs,
                end_inclusive: re,
                data: BucketTreeNodeData::Leaf(LeafBucket {
                    nodes: rd,
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
        }
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
    pub fn mark_outgoing_request(&mut self, id: &Id20, now: Instant) -> bool {
        let r = match self.buckets.get_mut(id, None) {
            Some(r) => r,
            None => return false,
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
}
