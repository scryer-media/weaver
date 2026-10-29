// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The article root's facts: the post's file names, what each article of the
// post came to, the offsets the decoder told exactly, the checksums it
// computed, and the next node id of the tree. They are the root row's blob,
// versioned like any object's. The post's own sizes are not here: a restore
// has the spec, and the names are only there to prove it is the same post.
//
// What grows with the post is kept as parts, a chunk of articles to a part,
// so a step that changes one article encodes and writes one part. The root's
// state marks the chunks each change touches, so an encoding reads again
// only the chunks marked since it last read.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use weaver_graph::{
    ByteRange, Change, Durable, Facts, Give, Part, Sealed, Shelf, Take, VolumeIndex, decode_parts,
    encode_since,
};

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
    // Namespace members whose whole-file verification has not been acknowledged.
    #[serde(default)]
    pub(crate) pending_verifications: Vec<(u32, u32)>,
}

impl RootFacts {
    // 3: each volume's lists shelved a chunk of articles to a part.
    // 4: a sealed value encodes as nothing, numbered by its owner's walk.
    // 5: sparse outstanding namespace checksum checks in the root row.
    pub(crate) const VERSION: u16 = 5;
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
pub(crate) const CHUNK: u32 = 16;

// A chunk of one volume's facts: of its articles and their raised classes,
// of its known boundaries, or of its segments.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Chunk {
    Articles(u32),
    Known(u32),
    Segments(u32),
}

// The facts as they are encoded: kept from one checkpoint to the next, so a
// part whose entries did not change keeps its bytes and is not encoded again.
#[derive(Default, Serialize, Deserialize)]
pub(crate) struct Encoded {
    next_node: u32,
    posted: Sealed<Vec<String>>,
    volumes: Vec<EncodedVolume>,
    read_backs: u64,
    reported: Option<RootPhase>,
    #[serde(default)]
    pending_verifications: Vec<(u32, u32)>,
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
    const VERSION: u16 = RootFacts::VERSION;

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
    fn facts(&self) -> RootFacts {
        RootFacts {
            next_node: self.next_node,
            posted: self.posted.get().clone(),
            volumes: self.volumes.iter().map(EncodedVolume::facts).collect(),
            read_backs: self.read_backs,
            reported: self.reported,
            pending_verifications: self.pending_verifications.clone(),
        }
    }
}

// One encoding of the root's facts: the root row's version and bytes, and
// its parts.
pub(crate) struct Encoding {
    pub(crate) version: u16,
    pub(crate) bytes: Vec<u8>,
    pub(crate) parts: Parts,
}

pub(crate) enum Parts {
    // Every part the facts have.
    All(Vec<Part>),
    // The parts changed since the encoding before.
    Changed(Vec<Change>),
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

impl ArticleRoot {
    // The root's facts encoded into `encoded`, reading of the root only the
    // chunks marked since `encoded` last read it, and all of it the first
    // time. Facts that do not encode say so as unsupported, as `encode`
    // does, and the next encoding reads the root whole again.
    pub(crate) fn encode(
        &self,
        encoded: &mut Encoded,
        next_node: u32,
        read_backs: u64,
        reported: Option<RootPhase>,
        pending_verifications: Vec<(u32, u32)>,
    ) -> Encoding {
        encoded.next_node = next_node;
        encoded.read_backs = read_backs;
        encoded.reported = reported;
        encoded.pending_verifications = pending_verifications;
        let read = encoded.volumes.len() == self.map.volume_count()
            && encoded.volumes.iter().all(|volume| volume.read.is_some());
        let done = match read {
            true => self.encode_changed(encoded),
            false => self.encode_whole(encoded),
        };
        done.unwrap_or_else(|_| {
            for volume in &mut encoded.volumes {
                volume.read = None;
            }
            Encoding {
                version: Facts::UNSUPPORTED,
                bytes: Vec::new(),
                parts: Parts::All(Vec::new()),
            }
        })
    }

    fn encode_whole(&self, encoded: &mut Encoded) -> Result<Encoding, String> {
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
        Ok(Encoding {
            version: RootFacts::VERSION,
            bytes: taken.bytes,
            parts: Parts::All(taken.parts),
        })
    }

    fn encode_changed(&self, encoded: &mut Encoded) -> Result<Encoding, String> {
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
        Ok(Encoding {
            version: RootFacts::VERSION,
            bytes: taken.bytes,
            parts: Parts::Changed(taken.changes),
        })
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

pub(crate) fn decode(
    version: u16,
    bytes: &[u8],
    parts: &[Part],
) -> Result<RootFacts, RootFactsError> {
    if version != 4 && version != RootFacts::VERSION {
        return Err(RootFactsError::Version(version));
    }
    decode_parts::<Encoded>(bytes, parts)
        .map(|encoded| encoded.facts())
        .map_err(RootFactsError::Decode)
}
