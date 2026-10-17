// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A job's tree as plain rows: what the owner writes at a checkpoint and
// reads back to rebuild the tree after a restart. Node ids, member ids and
// volume indexes are the ones the tree used, since the owner's rows and
// partial files are keyed by them. Member bytes are not here: the owner
// re-reads them from its partials and delivers them again. Holds are not
// here either: a hold still in memory is fetched again.

use serde::{Deserialize, Serialize};

use crate::deliver::{Provenance, Why};
use crate::facts::Facts;
use crate::member::MemberId;
use crate::object::{Kind, ObjectId};
use crate::tree::NodeId;
use crate::volume::{ByteRange, VolumeIndex};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TreeRecord {
    // Parents before their children.
    pub nodes: Vec<NodeRecord>,
    // The id the next node is given, so ids are never reused.
    pub next_node: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeState {
    Live,
    // The object gave its volumes up, or one above it did; it stays only
    // for its owner to read back where its bytes went.
    Retired,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeRecord {
    pub node: NodeId,
    // `None` for the root.
    pub parent: Option<NodeId>,
    // The members of the parent's namespace that are this node's volumes.
    pub volumes: Vec<(VolumeIndex, MemberId)>,
    // The range of its one volume's member a window is bound to.
    pub window: Option<ByteRange>,
    // `None` for the root, whose namespace the source produces.
    pub kind: Option<Kind>,
    pub state: NodeState,
    pub reason: Option<Why>,
    pub facts: Option<Facts>,
    pub members: Vec<MemberRecord>,
    pub coverage: Vec<CoverageRecord>,
    pub envelopes: Vec<EnvelopeRecord>,
    pub crypt: Vec<CryptRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemberRecord {
    pub member: MemberId,
    pub name: String,
    // The name the ledger gave it, which is the name on disk.
    pub output_name: String,
    pub length: Option<u64>,
    pub closed: bool,
    pub bound_to: Option<ObjectId>,
    // The child it is a volume of, and which volume.
    pub owner: Option<(NodeId, VolumeIndex)>,
    // What it was to a retired object that gave it up, until it is buried.
    pub given_up: Option<(NodeId, VolumeIndex)>,
    // Its first bytes have not decided an owner yet.
    pub undecided: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CoverageRecord {
    pub member: MemberId,
    pub start: u64,
    pub end: u64,
    pub provenance: Provenance,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvelopeRecord {
    pub volume: VolumeIndex,
    pub start: u64,
    pub end: u64,
    pub bytes: Vec<u8>,
}

// One cipher block's seed of a member, at the start of the run it opens;
// the padding plaintext at `run_start` -1.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CryptRecord {
    pub member: MemberId,
    pub run_start: i64,
    pub seed: [u8; 16],
}
