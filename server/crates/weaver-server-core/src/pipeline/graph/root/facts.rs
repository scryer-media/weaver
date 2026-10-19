// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The root row's blob: the tree's next node id, what the job keeps of itself
// (its count of repair reads and the phase it last reported), and the root's
// own facts, named by the root that wrote them and versioned by it, so a
// restore reads them only into a root of the same kind.
//
// The article root's facts are the post's file names, what each article of
// the post came to, the offsets the decoder told exactly and the checksums
// it computed. The post's own sizes are not here: a restore has the spec,
// and the names are only there to prove it is the same post.

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use weaver_graph::{ByteRange, VolumeIndex};

use super::phase::RootPhase;
use super::{ArticleRoot, Root};
use crate::pipeline::graph::job::RootFile;

// The root row as it is written now.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RootRow {
    pub(crate) next_node: u32,
    pub(crate) job: JobFacts,
    pub(crate) root: RootBlob,
}

impl RootRow {
    // Version 2 was the article root's facts with the job's beside them.
    pub(crate) const VERSION: u16 = 3;
}

// What the job keeps of itself in the root row, whatever its root.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct JobFacts {
    // How many member reads the job's repairs made: the writer's count.
    pub(crate) read_backs: u64,
    // The phase the job last reported, so a restored job reports only a
    // change from it.
    pub(crate) reported: Option<RootPhase>,
}

// A root's own facts, as the root that wrote them encoded them.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RootBlob {
    pub(crate) root: String,
    pub(crate) version: u16,
    pub(crate) bytes: Vec<u8>,
}

impl RootBlob {
    pub(crate) fn of<R: Root>(root: &R) -> Result<Self, RootFactsError> {
        Ok(Self {
            root: R::NAME.to_owned(),
            version: R::FACTS_VERSION,
            bytes: rmp_serde::to_vec(&root.facts())
                .map_err(|error| RootFactsError::Encode(error.to_string()))?,
        })
    }

    // The facts, for a root of the kind that wrote them.
    pub(crate) fn read<R: Root>(&self) -> Result<R::Facts, RootFactsError> {
        if self.root != R::NAME {
            return Err(RootFactsError::Mismatch("another kind of root"));
        }
        read_version(R::FACTS_VERSION, self.version, &self.bytes)
    }
}

// The article root's facts.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PostFacts {
    // Each file's name as the post lists it, in post order.
    pub(crate) posted: Vec<String>,
    pub(crate) volumes: Vec<VolumeFacts>,
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
    #[error("the root's facts do not encode: {0}")]
    Encode(String),
    #[error("the root's facts do not match the post: {0}")]
    Mismatch(&'static str),
}

impl ArticleRoot {
    pub(crate) fn post_facts(&self) -> PostFacts {
        let mut volumes = self.map.facts();
        self.checksums.facts(&mut volumes);
        PostFacts {
            posted: self.posted.clone(),
            volumes,
        }
    }

    // The root at the state its facts describe, over the post's articles.
    // The post must be the one they were taken from: its files by name, as
    // the map checks them by their articles.
    pub(crate) fn restore(files: &[RootFile], facts: &PostFacts) -> Result<Self, RootFactsError> {
        let mut root = Self::new(files);
        if root.posted != facts.posted {
            return Err(RootFactsError::Mismatch("another file name"));
        }
        root.map.restore(&facts.volumes)?;
        root.checksums.restore(&facts.volumes);
        Ok(root)
    }

    pub(crate) fn matches(&self, volume: VolumeIndex, range: ByteRange, bytes: &[u8]) -> bool {
        self.checksums.matches(volume, range, bytes)
    }
}

impl PostFacts {
    pub(crate) const VERSION: u16 = 1;
}

// The root row of version 2: the article root's facts, with the next node
// and the job's own beside them. A checkpoint written before the reported
// phase was kept has none.
#[derive(Deserialize)]
struct Version2 {
    next_node: u32,
    posted: Vec<String>,
    volumes: Vec<VolumeFacts>,
    read_backs: u64,
    #[serde(default)]
    reported: Option<RootPhase>,
}

pub(crate) fn encode(row: &RootRow) -> Vec<u8> {
    // Plain structs of integers, strings and bytes always encode.
    rmp_serde::to_vec(row).unwrap_or_default()
}

pub(crate) fn decode(version: u16, bytes: &[u8]) -> Result<RootRow, RootFactsError> {
    if version != 2 {
        return read_version(RootRow::VERSION, version, bytes);
    }
    let before: Version2 = read_version(2, version, bytes)?;
    let post = PostFacts {
        posted: before.posted,
        volumes: before.volumes,
    };
    Ok(RootRow {
        next_node: before.next_node,
        job: JobFacts {
            read_backs: before.read_backs,
            reported: before.reported,
        },
        root: RootBlob {
            root: ArticleRoot::NAME.to_owned(),
            version: PostFacts::VERSION,
            bytes: rmp_serde::to_vec(&post).unwrap_or_default(),
        },
    })
}

fn read_version<T: DeserializeOwned>(
    expected: u16,
    version: u16,
    bytes: &[u8],
) -> Result<T, RootFactsError> {
    if version != expected {
        return Err(RootFactsError::Version(version));
    }
    rmp_serde::from_slice(bytes).map_err(|error| RootFactsError::Decode(error.to_string()))
}
