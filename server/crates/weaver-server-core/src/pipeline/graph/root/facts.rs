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
//
// What grows with the post is kept as parts, a chunk of articles to a part,
// so a step that changes one article encodes and writes one part.

use std::collections::BTreeMap;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use weaver_graph::{
    ByteRange, Durable, Facts, Give, Part, Sealed, Shelf, Take, VolumeIndex, decode_parts,
    encode_parts,
};

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

// A root's own facts, as the root that wrote them encoded them: the blob
// in the root row, and the parts the blob names, each its own row.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RootBlob {
    pub(crate) root: String,
    pub(crate) version: u16,
    pub(crate) bytes: Vec<u8>,
    #[serde(skip)]
    pub(crate) parts: Vec<Part>,
}

impl RootBlob {
    // The root's facts as the checkpoint keeps them. Facts that do not
    // encode are facts nothing could restore, so they say so as
    // unsupported, which a restore refuses.
    pub(crate) fn of<R: Root>(root: &R, kept: &mut R::Kept) -> Self {
        let (version, bytes, mut parts) = match root.encode(kept) {
            Ok((bytes, parts)) => (R::FACTS_VERSION, bytes, parts),
            Err(_) => (Facts::UNSUPPORTED, Vec::new(), Vec::new()),
        };
        parts.sort_by_key(|part| part.key);
        Self {
            root: R::NAME.to_owned(),
            version,
            bytes,
            parts,
        }
    }

    // The facts, for a root of the kind that wrote them.
    pub(crate) fn read<R: Root>(&self) -> Result<R::Facts, RootFactsError> {
        if self.root != R::NAME {
            return Err(RootFactsError::Mismatch("another kind of root"));
        }
        if self.version == Facts::UNSUPPORTED {
            return Err(RootFactsError::Unsupported);
        }
        if self.version != R::FACTS_VERSION {
            return Err(RootFactsError::Version(self.version));
        }
        R::decode(&self.bytes, &self.parts)
    }
}

// The article root's facts.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PostFacts {
    // Each file's name as the post lists it, in post order.
    pub(crate) posted: Vec<String>,
    pub(crate) volumes: Vec<VolumeFacts>,
}

impl PostFacts {
    // 1: the whole post in the blob. 2: each volume's lists shelved a
    // chunk of articles to a part. 3: a sealed value encodes as nothing,
    // numbered by its owner's walk.
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
    #[error("the root's facts do not encode: {0}")]
    Encode(String),
    #[error("the root's facts did not encode, so nothing restores them")]
    Unsupported,
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

// The root row of version 2: the article root's facts whole, with the next
// node and the job's own beside them. A checkpoint written before the
// reported phase was kept has none.
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

// The root row from its blob and the root's part rows.
pub(crate) fn decode(
    version: u16,
    bytes: &[u8],
    parts: &[Part],
) -> Result<RootRow, RootFactsError> {
    let mut parts = parts.to_vec();
    parts.sort_by_key(|part| part.key);
    if version != 2 {
        let mut row: RootRow = read_version(RootRow::VERSION, version, bytes)?;
        row.root.parts = parts;
        return Ok(row);
    }
    let before: Version2 = read_version(2, version, bytes)?;
    let post = PostFacts {
        posted: before.posted,
        volumes: before.volumes,
    };
    let (bytes, parts) = encode_post(&mut Encoded::default(), &post)?;
    Ok(RootRow {
        next_node: before.next_node,
        job: JobFacts {
            read_backs: before.read_backs,
            reported: before.reported,
        },
        root: RootBlob {
            root: ArticleRoot::NAME.to_owned(),
            version: PostFacts::VERSION,
            bytes,
            parts,
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

// Articles to a part.
const CHUNK: u32 = 16;

// The post's facts as they are encoded: kept from one checkpoint to the
// next, so a part whose entries did not change keeps its bytes and is not
// encoded again.
#[derive(Default, Serialize, Deserialize)]
pub(crate) struct Encoded {
    posted: Sealed<Vec<String>>,
    volumes: Vec<EncodedVolume>,
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

impl Durable for Encoded {
    const VERSION: u16 = PostFacts::VERSION;

    fn parts(&self, take: &mut Take) -> Result<(), String> {
        take.sealed(&self.posted)?;
        for volume in &self.volumes {
            take.shelf(&volume.articles)?;
            take.shelf(&volume.known)?;
            take.shelf(&volume.segments)?;
            take.shelf(&volume.raised)?;
        }
        Ok(())
    }

    fn restore_parts(&mut self, give: &mut Give) -> Result<(), String> {
        give.sealed(&mut self.posted)?;
        for volume in &mut self.volumes {
            give.shelf(&mut volume.articles)?;
            give.shelf(&mut volume.known)?;
            give.shelf(&mut volume.segments)?;
            give.shelf(&mut volume.raised)?;
        }
        Ok(())
    }
}

impl Encoded {
    fn refresh(&mut self, facts: &PostFacts) {
        self.posted.set(facts.posted.clone());
        self.volumes
            .resize_with(facts.volumes.len(), EncodedVolume::default);
        for (encoded, volume) in self.volumes.iter_mut().zip(&facts.volumes) {
            encoded.refresh(volume);
        }
    }

    fn facts(&self) -> PostFacts {
        PostFacts {
            posted: self.posted.get().clone(),
            volumes: self.volumes.iter().map(EncodedVolume::facts).collect(),
        }
    }
}

// The post's facts' bytes and their parts, encoding again only the parts
// that changed since `encoded` last encoded. A part that does not encode
// fails the whole: the blob then says so as unsupported.
pub(crate) fn encode_post(
    encoded: &mut Encoded,
    facts: &PostFacts,
) -> Result<(Vec<u8>, Vec<Part>), RootFactsError> {
    encoded.refresh(facts);
    encode_parts(encoded).map_err(RootFactsError::Encode)
}

pub(crate) fn decode_post(bytes: &[u8], parts: &[Part]) -> Result<PostFacts, RootFactsError> {
    decode_parts::<Encoded>(bytes, parts)
        .map(|encoded| encoded.facts())
        .map_err(RootFactsError::Decode)
}
