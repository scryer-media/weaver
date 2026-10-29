// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Serde for the graph's vocabulary types that facts and records carry,
// through mirror definitions, so the types themselves stay plain values and
// their encoding lives in one place.

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::deliver::{Provenance, Why};
use crate::member::{Coverage, MemberId, Run};
use crate::recovery::RepairOutcome;
use crate::tree::NodeId;
use crate::volume::ByteRange;

macro_rules! through {
    ($ty:ty, $mirror:ident) => {
        impl Serialize for $ty {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                $mirror::serialize(self, serializer)
            }
        }

        impl<'de> Deserialize<'de> for $ty {
            fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                $mirror::deserialize(deserializer)
            }
        }
    };
}

#[derive(Serialize, Deserialize)]
#[serde(remote = "NodeId")]
struct NodeIdMirror(u32);
through!(NodeId, NodeIdMirror);

#[derive(Serialize, Deserialize)]
#[serde(remote = "MemberId")]
struct MemberIdMirror(u32);
through!(MemberId, MemberIdMirror);

#[derive(Serialize, Deserialize)]
#[serde(remote = "Provenance")]
enum ProvenanceMirror {
    Fresh,
    Damaged,
    Repaired,
}
through!(Provenance, ProvenanceMirror);

#[derive(Serialize, Deserialize)]
#[serde(remote = "Why")]
enum WhyMirror {
    Compressed,
    TailUnreachable,
    EncryptedHeader,
    Encrypted,
    UnboundedEntry,
    NotAFile,
    Unrepairable,
    FirstBytesLost,
    HoldsBreached,
    Unsupported,
    Unmapped,
    TooDeep,
    OrderUnknown,
    InvalidArchive,
}
through!(Why, WhyMirror);

#[derive(Serialize, Deserialize)]
#[serde(remote = "RepairOutcome")]
enum RepairOutcomeMirror {
    Clean,
    Repaired { slices: u64 },
    Impossible { damaged: u64, reachable: u64 },
    Waiting,
    Unread,
}
through!(RepairOutcome, RepairOutcomeMirror);

#[derive(Serialize, Deserialize)]
#[serde(remote = "Run")]
struct RunMirror {
    range: ByteRange,
    provenance: Provenance,
}
through!(Run, RunMirror);

// Coverage travels as its runs, and only runs it could have held come back:
// non-empty, disjoint and in offset order.
impl Serialize for Coverage {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(self.runs())
    }
}

impl<'de> Deserialize<'de> for Coverage {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let runs = Vec::<Run>::deserialize(deserializer)?;
        let ordered = runs
            .windows(2)
            .all(|pair| pair[0].range.end <= pair[1].range.start);
        if !ordered || runs.iter().any(|run| run.range.is_empty()) {
            return Err(D::Error::custom("coverage runs overlap or are empty"));
        }
        let mut coverage = Coverage::new();
        for run in runs {
            coverage.write(run.range, run.provenance);
        }
        Ok(coverage)
    }
}
