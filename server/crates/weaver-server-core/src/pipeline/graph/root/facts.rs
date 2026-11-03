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
// so a step that changes one article encodes and writes one part. The root's
// state marks the chunks each change touches, so an encoding reads again
// only the chunks marked since it last read.

use std::collections::BTreeSet;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use weaver_graph::{
    ByteRange, Change, Durable, Facts, Give, Part, Sealed, Shelf, Take, VolumeIndex, decode_parts,
    encode_since,
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
    // Version 2 was the article root's facts with the job's beside them; no
    // released build wrote it, nor 3, the root's facts in a blob of their
    // own. 8: the job's facts name the members whose whole-file check is
    // still to come, and the checkpoint names why a range was lost and a
    // range that never came; numbered past every version a 0.16 build
    // wrote of the facts this row replaces.
    pub(crate) const VERSION: u16 = 8;
}

// What the job keeps of itself in the root row, whatever its root.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct JobFacts {
    // How many member reads the job's repairs made: the writer's count.
    pub(crate) read_backs: u64,
    // The phase the job last reported, so a restored job reports only a
    // change from it.
    pub(crate) reported: Option<RootPhase>,
    // Namespace members whose whole-file verification has not been acknowledged.
    pub(crate) pending_verifications: Vec<(u32, u32)>,
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

// The parts one encoding of a root's facts names.
pub(crate) enum Parts {
    // Every part the facts have.
    All(Vec<Part>),
    // The parts changed since the encoding before.
    Changed(Vec<Change>),
}

impl RootBlob {
    // The root's facts whole, every part with them: an encoding into
    // nothing kept reads the root whole.
    #[cfg(test)]
    pub(crate) fn of<R: Root>(root: &R) -> Self {
        let (mut blob, parts) = Self::encode(root, &mut R::Kept::default());
        blob.parts = match parts {
            Parts::All(parts) => parts,
            Parts::Changed(_) => unreachable!("a fresh encoding is whole"),
        };
        blob.parts.sort_by_key(|part| part.key);
        blob
    }

    // The root's facts as the checkpoint keeps them, encoding again only
    // what changed since `kept` last encoded; the parts come apart from the
    // blob. Facts that do not encode are facts nothing could restore, so
    // they say so as unsupported, which a restore refuses.
    pub(crate) fn encode<R: Root>(root: &R, kept: &mut R::Kept) -> (Self, Parts) {
        let (version, bytes, parts) = match root.encode(kept) {
            Ok((bytes, parts)) => (R::FACTS_VERSION, bytes, parts),
            Err(_) => (Facts::UNSUPPORTED, Vec::new(), Parts::All(Vec::new())),
        };
        let blob = Self {
            root: R::NAME.to_owned(),
            version,
            bytes,
            parts: Vec::new(),
        };
        (blob, parts)
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

// Facts kept whole: their bytes name no parts.
pub(crate) fn whole<T: Serialize>(facts: &T) -> Result<(Vec<u8>, Parts), RootFactsError> {
    rmp_serde::to_vec(facts)
        .map(|bytes| (bytes, Parts::All(Vec::new())))
        .map_err(|error| RootFactsError::Encode(error.to_string()))
}

pub(crate) fn from_whole<T: DeserializeOwned>(
    bytes: &[u8],
    parts: &[Part],
) -> Result<T, RootFactsError> {
    if !parts.is_empty() {
        return Err(RootFactsError::Decode(
            "parts of facts kept whole".to_owned(),
        ));
    }
    rmp_serde::from_slice(bytes).map_err(|error| RootFactsError::Decode(error.to_string()))
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
        let checksums = &root.checksums;
        root.map
            .vouch(|volume, range| checksums.verified(volume, range));
        Ok(root)
    }

    pub(crate) fn matches(&self, volume: VolumeIndex, range: ByteRange, bytes: &[u8]) -> bool {
        self.checksums.matches(volume, range, bytes)
    }
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
    if version != RootRow::VERSION {
        return Err(RootFactsError::Version(version));
    }
    let mut row: RootRow =
        rmp_serde::from_slice(bytes).map_err(|error| RootFactsError::Decode(error.to_string()))?;
    row.root.parts = parts.to_vec();
    row.root.parts.sort_by_key(|part| part.key);
    Ok(row)
}

// Articles to a part.
pub(crate) const CHUNK: u32 = 16;

// A chunk of one volume's facts: of its articles and their raised classes,
// of its known boundaries, or of its segments.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Chunk {
    Articles(u32),
    Known(u32),
    Segments(u32),
}

// The post's facts as they are encoded: kept from one checkpoint to the
// next, so a part whose entries did not change keeps its bytes and is not
// encoded again.
#[derive(Default, Serialize, Deserialize)]
pub(crate) struct Encoded {
    posted: Sealed<Vec<String>>,
    volumes: Vec<EncodedVolume>,
    // The generation of each of its sealed values when its parts were last
    // taken, by number in the walk.
    #[serde(skip)]
    taken: Vec<u64>,
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
    // The generations of the root's map and checksums for the volume when
    // this encoding last read them; `None` until it has read them whole.
    #[serde(skip)]
    read: Option<(u64, u64)>,
}

pub(crate) fn of_ordinal(ordinal: u32) -> u32 {
    ordinal / CHUNK
}

pub(crate) fn of_boundary(boundary: u64) -> u32 {
    u32::try_from(boundary / u64::from(CHUNK)).unwrap_or(u32::MAX)
}

impl EncodedVolume {
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
    fn facts(&self) -> PostFacts {
        PostFacts {
            posted: self.posted.get().clone(),
            volumes: self.volumes.iter().map(EncodedVolume::facts).collect(),
        }
    }
}

fn some<T>(entries: Vec<T>) -> Option<Vec<T>> {
    (!entries.is_empty()).then_some(entries)
}

// Each chunk read, kept where it holds anything.
fn filled<T>(chunks: &[u32], read: impl Fn(u32) -> Vec<T>) -> Vec<(u32, Vec<T>)> {
    chunks
        .iter()
        .filter_map(|chunk| Some((*chunk, some(read(*chunk))?)))
        .collect()
}

// The post's facts encoded into `encoded`, reading of the root only the
// chunks marked since `encoded` last read it, and all of it the first
// time. Facts that do not encode fail the whole, and the next encoding
// reads the root whole again.
pub(crate) fn encode_post(
    root: &ArticleRoot,
    encoded: &mut Encoded,
) -> Result<(Vec<u8>, Parts), RootFactsError> {
    let read = encoded.volumes.len() == root.map.volume_count()
        && encoded.volumes.iter().all(|volume| volume.read.is_some());
    let done = match read {
        true => root.encode_changed(encoded),
        false => root.encode_whole(encoded),
    };
    done.map_err(|error| {
        for volume in &mut encoded.volumes {
            volume.read = None;
        }
        RootFactsError::Encode(error)
    })
}

pub(crate) fn decode_post(bytes: &[u8], parts: &[Part]) -> Result<PostFacts, RootFactsError> {
    decode_parts::<Encoded>(bytes, parts)
        .map(|encoded| encoded.facts())
        .map_err(RootFactsError::Decode)
}

impl ArticleRoot {
    fn encode_whole(&self, encoded: &mut Encoded) -> Result<(Vec<u8>, Parts), String> {
        encoded.posted.set(self.posted.clone());
        encoded
            .volumes
            .resize_with(self.map.volume_count(), EncodedVolume::default);
        for (index, volume) in encoded.volumes.iter_mut().enumerate() {
            let read = self.marked(index);
            let (mut articles, mut known, mut segments) = (Vec::new(), Vec::new(), Vec::new());
            for chunk in self.map.chunks(index).collect::<BTreeSet<_>>() {
                match chunk {
                    Chunk::Articles(chunk) => articles.push(chunk),
                    Chunk::Known(chunk) => known.push(chunk),
                    Chunk::Segments(chunk) => segments.push(chunk),
                }
            }
            volume.articles.set(filled(&articles, |chunk| {
                self.map.articles_in(index, chunk)
            }));
            volume
                .raised
                .set(filled(&articles, |chunk| self.map.raised_in(index, chunk)));
            volume
                .known
                .set(filled(&known, |chunk| self.map.known_in(index, chunk)));
            volume
                .segments
                .set(filled(&segments, |chunk| self.segments_in(index, chunk)));
            volume.ordered_only = self.map.ordered_only(index);
            volume.read = Some(read);
        }
        let taken = encode_since(encoded, None)?;
        encoded.taken = taken.at;
        Ok((taken.bytes, Parts::All(taken.parts)))
    }

    fn encode_changed(&self, encoded: &mut Encoded) -> Result<(Vec<u8>, Parts), String> {
        for (index, volume) in encoded.volumes.iter_mut().enumerate() {
            let now = self.marked(index);
            volume.ordered_only = self.map.ordered_only(index);
            let Some((map, sums)) = volume.read.filter(|read| *read != now) else {
                continue;
            };
            let mut chunks: BTreeSet<Chunk> = self.map.marked_since(index, map).collect();
            chunks.extend(
                self.checksums
                    .marked_since(VolumeIndex(index as u32), sums)
                    .map(|offset| Chunk::Segments(self.map.owner(index, offset))),
            );
            for chunk in chunks {
                match chunk {
                    Chunk::Articles(chunk) => {
                        volume
                            .articles
                            .put(chunk, some(self.map.articles_in(index, chunk)));
                        volume
                            .raised
                            .put(chunk, some(self.map.raised_in(index, chunk)));
                    }
                    Chunk::Known(chunk) => volume
                        .known
                        .put(chunk, some(self.map.known_in(index, chunk))),
                    Chunk::Segments(chunk) => volume
                        .segments
                        .put(chunk, some(self.segments_in(index, chunk))),
                }
            }
            volume.read = Some(now);
        }
        let taken = encode_since(encoded, Some(&encoded.taken))?;
        encoded.taken = taken.at;
        Ok((taken.bytes, Parts::Changed(taken.changes)))
    }

    fn marked(&self, volume: usize) -> (u64, u64) {
        (
            self.map.marked(volume),
            self.checksums.marked(VolumeIndex(volume as u32)),
        )
    }

    // One chunk of a volume's segments, in start order.
    fn segments_in(&self, volume: usize, chunk: u32) -> Vec<(u64, u64, u32, bool)> {
        let mut out = Vec::new();
        for starts in self.map.owned_by(volume, chunk) {
            self.checksums
                .segments_in(VolumeIndex(volume as u32), starts, &mut out);
        }
        out.sort_by_key(|(start, ..)| *start);
        out
    }
}
