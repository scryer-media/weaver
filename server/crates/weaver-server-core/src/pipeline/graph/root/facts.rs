// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The article root's facts: the post's file names, what each article of the
// post came to, the offsets the decoder told exactly, the checksums it
// computed, and the next node id of the tree. They are the root row's blob,
// versioned like any object's. The post's own sizes are not here: a restore
// has the spec, and the names are only there to prove it is the same post.
//
// What grows with the post is kept as parts, a chunk of articles to a part,
// so a step that changes one article encodes and writes one part.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use weaver_graph::{ByteRange, Part, Sealed, Shelf, VolumeIndex, decode_parts, encode_parts};

use super::ArticleRoot;
use super::phase::RootPhase;
use crate::pipeline::graph::job::RootFile;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RootFacts {
    pub(crate) next_node: u32,
    // Each file's name as the post lists it, in post order.
    pub(crate) posted: Vec<String>,
    pub(crate) volumes: Vec<VolumeFacts>,
    // How many member reads the job's repairs made. The writer's count,
    // kept here because the root row is the job's one row.
    pub(crate) read_backs: u64,
    // The phase the job last reported, so a restored job reports only a
    // change from it. A checkpoint written before this was kept has none.
    #[serde(default)]
    pub(crate) reported: Option<RootPhase>,
}

impl RootFacts {
    // 3: each volume's lists shelved a chunk of articles to a part.
    pub(crate) const VERSION: u16 = 3;
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
            posted: self.posted.clone(),
            volumes,
            read_backs: 0,
            reported: None,
        }
    }

    // The root at the state its facts describe, over the post's articles.
    // The post must be the one they were taken from: its files by name, as
    // the map checks them by their articles.
    pub(crate) fn restore(files: &[RootFile], facts: &RootFacts) -> Result<Self, RootFactsError> {
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

// Articles to a part.
const CHUNK: u32 = 16;

// The facts as they are encoded: kept from one checkpoint to the next, so a
// part whose entries did not change keeps its bytes and is not encoded again.
#[derive(Default, Serialize, Deserialize)]
pub(crate) struct Encoded {
    next_node: u32,
    posted: Sealed<Vec<String>>,
    volumes: Vec<EncodedVolume>,
    read_backs: u64,
    reported: Option<RootPhase>,
}

#[derive(Default, Serialize, Deserialize)]
struct EncodedVolume {
    // By ordinal.
    articles: Shelf<Vec<(u32, ArticleFacts)>>,
    // By boundary index.
    known: Shelf<Vec<(u64, u64)>>,
    ordered_only: bool,
    // By the ordinal of the article whose decoded range starts at or before
    // the segment.
    segments: Shelf<Vec<(u64, u64, u32, bool)>>,
    // By ordinal.
    raised: Shelf<Vec<(u32, ClassFacts)>>,
}

// Entries grouped by the chunk each lies in.
fn chunked<T: Clone>(entries: &[T], chunk: impl Fn(&T) -> u32) -> BTreeMap<u32, Vec<T>> {
    let mut out: BTreeMap<u32, Vec<T>> = BTreeMap::new();
    for entry in entries {
        out.entry(chunk(entry)).or_default().push(entry.clone());
    }
    out
}

fn of_ordinal(ordinal: u32) -> u32 {
    ordinal / CHUNK
}

fn of_boundary(boundary: u64) -> u32 {
    u32::try_from(boundary / u64::from(CHUNK)).unwrap_or(u32::MAX)
}

impl EncodedVolume {
    fn refresh(&mut self, facts: &VolumeFacts) {
        let starts: BTreeMap<u64, u32> = facts
            .articles
            .iter()
            .filter_map(|(ordinal, article)| match article {
                ArticleFacts::Arrived(Some((start, _))) => Some((*start, *ordinal)),
                _ => None,
            })
            .collect();
        self.articles.set(chunked(&facts.articles, |(ordinal, _)| {
            of_ordinal(*ordinal)
        }));
        self.known.set(chunked(&facts.known, |(boundary, _)| {
            of_boundary(*boundary)
        }));
        self.ordered_only = facts.ordered_only;
        self.segments.set(chunked(&facts.segments, |(start, ..)| {
            starts
                .range(..=*start)
                .next_back()
                .map_or(0, |(_, ordinal)| of_ordinal(*ordinal))
        }));
        self.raised
            .set(chunked(&facts.raised, |(ordinal, _)| of_ordinal(*ordinal)));
    }

    fn facts(&self) -> VolumeFacts {
        fn joined<T: Clone + Serialize, K: Ord>(
            shelf: &Shelf<Vec<T>>,
            key: impl Fn(&T) -> K,
        ) -> Vec<T> {
            let mut out: Vec<T> = shelf
                .chunks()
                .flat_map(|(_, entries)| entries.iter().cloned())
                .collect();
            out.sort_by_key(key);
            out
        }
        VolumeFacts {
            articles: joined(&self.articles, |(ordinal, _)| *ordinal),
            known: joined(&self.known, |(boundary, _)| *boundary),
            ordered_only: self.ordered_only,
            segments: joined(&self.segments, |(start, ..)| *start),
            raised: joined(&self.raised, |(ordinal, _)| *ordinal),
        }
    }
}

impl Encoded {
    fn refresh(&mut self, facts: &RootFacts) {
        self.next_node = facts.next_node;
        self.posted.set(facts.posted.clone());
        self.volumes
            .resize_with(facts.volumes.len(), EncodedVolume::default);
        for (encoded, volume) in self.volumes.iter_mut().zip(&facts.volumes) {
            encoded.refresh(volume);
        }
        self.read_backs = facts.read_backs;
        self.reported = facts.reported;
    }

    fn facts(&self) -> RootFacts {
        RootFacts {
            next_node: self.next_node,
            posted: self.posted.get().clone(),
            volumes: self.volumes.iter().map(EncodedVolume::facts).collect(),
            read_backs: self.read_backs,
            reported: self.reported,
        }
    }
}

// The facts' bytes and their parts, encoding again only the parts that
// changed since `encoded` last encoded.
pub(crate) fn encode(encoded: &mut Encoded, facts: &RootFacts) -> (Vec<u8>, Vec<Part>) {
    encoded.refresh(facts);
    // A plain struct of integers always encodes.
    encode_parts(encoded).unwrap_or_default()
}

pub(crate) fn decode(
    version: u16,
    bytes: &[u8],
    parts: &[Part],
) -> Result<RootFacts, RootFactsError> {
    if version != RootFacts::VERSION {
        return Err(RootFactsError::Version(version));
    }
    decode_parts::<Encoded>(bytes, parts)
        .map(|encoded| encoded.facts())
        .map_err(RootFactsError::Decode)
}
