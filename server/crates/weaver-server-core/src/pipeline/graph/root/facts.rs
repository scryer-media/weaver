// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The article root's facts: what each article of the post came to, the
// offsets the decoder told exactly, the checksums it computed, and the next
// node id of the tree. They are the root row's blob, versioned like any
// object's. The post's own sizes are not here: a restore has the spec.

use serde::{Deserialize, Serialize};
use weaver_graph::{ByteRange, VolumeIndex};

use super::ArticleRoot;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RootFacts {
    pub(crate) next_node: u32,
    pub(crate) volumes: Vec<VolumeFacts>,
    // How many member reads the job's repairs made. The writer's count,
    // kept here because the root row is the job's one row.
    pub(crate) read_backs: u64,
}

impl RootFacts {
    pub(crate) const VERSION: u16 = 1;
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct VolumeFacts {
    // In ordinal order, one per article of the post's file.
    pub(crate) articles: Vec<(u32, ArticleFacts)>,
    // Boundary index and the decoded offset it is known to be at.
    pub(crate) known: Vec<(u64, u64)>,
    pub(crate) ordered_only: bool,
    // start, end, crc32, verified.
    pub(crate) segments: Vec<(u64, u64, u32, bool)>,
    // Waiting articles raised above the ordinary class, by ordinal: a
    // repair's ask for recovery blocks is made once, so the restored queue
    // must hold it again.
    pub(crate) raised: Vec<(u32, ClassFacts)>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum ClassFacts {
    Critical,
    Recovery,
    FirstArticle,
    Window(u32),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum ArticleFacts {
    Waiting,
    // Its decoded range, `None` when an earlier run fetched it unseen.
    Arrived(Option<(u64, u64)>),
    Lost { damaged: bool },
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum RootFactsError {
    #[error("the root's facts are version {0}, which this build does not read")]
    Version(u16),
    #[error("the root's facts do not decode: {0}")]
    Decode(String),
    #[error("the root's facts do not match the post: {0}")]
    Mismatch(&'static str),
}

impl ArticleRoot {
    pub(crate) fn facts(&self, next_node: u32) -> RootFacts {
        let mut volumes = self.map.facts();
        self.checksums.facts(&mut volumes);
        RootFacts {
            next_node,
            volumes,
            read_backs: 0,
        }
    }

    // The root at the state its facts describe, over the post's articles.
    pub(crate) fn restore(
        volumes: &[Vec<(u32, u32)>],
        facts: &RootFacts,
    ) -> Result<Self, RootFactsError> {
        let mut root = Self::new(volumes);
        root.map.restore(&facts.volumes)?;
        root.checksums.restore(&facts.volumes);
        Ok(root)
    }

    pub(crate) fn matches(&self, volume: VolumeIndex, range: ByteRange, bytes: &[u8]) -> bool {
        self.checksums.matches(volume, range, bytes)
    }
}

pub(crate) fn encode(facts: &RootFacts) -> Vec<u8> {
    // A plain struct of integers always encodes.
    rmp_serde::to_vec(facts).unwrap_or_default()
}

pub(crate) fn decode(version: u16, bytes: &[u8]) -> Result<RootFacts, RootFactsError> {
    if version != RootFacts::VERSION {
        return Err(RootFactsError::Version(version));
    }
    rmp_serde::from_slice(bytes).map_err(|error| RootFactsError::Decode(error.to_string()))
}
