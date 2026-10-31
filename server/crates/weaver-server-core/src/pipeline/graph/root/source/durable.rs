// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The article map out as facts and back, and what a restore asks of it: the
// articles it may read back from disk, and the ones it found were not there.

use std::collections::BTreeMap;
use std::ops::Bound;

use weaver_graph::{ByteRange, VolumeIndex};

use super::{ArticleMap, Class, Move, State, Volume};
use crate::pipeline::graph::root::facts::{
    ArticleFacts, CHUNK, Chunk, ClassFacts, RootFactsError, VolumeFacts, of_boundary, of_ordinal,
};

impl ArticleMap {
    // Lay the facts over a map made from the post. The post must be the one
    // they were taken from: the same files, the same articles. A restore
    // replays what reached the graph, bytes and losses alike: every arrived
    // article is read back and delivered again, and every lost one is told
    // again, whether or not the run that went down had told it. What an
    // object heard of a loss then is in its facts only if it was in its node
    // to hear it; one lent out for its repair was waiting on it, and the
    // facts it is rebuilt from are those it left with. Each state is laid
    // down the one way a state changes, so the open articles are filed again
    // from the states alone.
    pub(crate) fn restore(&mut self, facts: &[VolumeFacts]) -> Result<(), RootFactsError> {
        if facts.len() != self.volumes.len() {
            return Err(RootFactsError::Mismatch("another number of files"));
        }
        for (index, (volume, facts)) in self.volumes.iter_mut().zip(facts).enumerate() {
            if volume.articles.len() != facts.articles.len() {
                return Err(RootFactsError::Mismatch("another number of articles"));
            }
            for (at, (ordinal, state)) in facts.articles.iter().enumerate() {
                if volume.articles[at].ordinal != *ordinal {
                    return Err(RootFactsError::Mismatch("another article ordinal"));
                }
                let state = match *state {
                    ArticleFacts::Waiting => State::Waiting,
                    ArticleFacts::Arrived(range) => State::Arrived(
                        range
                            .filter(|(start, end)| start <= end)
                            .map(|(start, end)| ByteRange::new(start, end)),
                    ),
                    ArticleFacts::Lost { .. } => State::Lost { damaged: false },
                };
                volume.set_state(&mut self.open, at, state);
            }
            let last = volume.articles.len() as u64;
            let known: BTreeMap<usize, u64> = facts
                .known
                .iter()
                .filter(|(index, _)| *index <= last)
                .map(|(index, offset)| (*index as usize, *offset))
                .collect();
            let gone: Vec<usize> = volume
                .known
                .keys()
                .filter(|boundary| !known.contains_key(boundary))
                .copied()
                .collect();
            for boundary in gone {
                volume.unknow(boundary);
            }
            for (boundary, offset) in known {
                volume.know(boundary, offset);
            }
            volume.ordered_only = facts.ordered_only;
            // A tail found short is told again, as a lost article is.
            for at in 0..volume.articles.len() {
                volume.cut_after(&mut self.open, at);
            }
            // Raised again through the moves, so the queue built from the
            // post is reclassed as the one that went down was.
            for (ordinal, raised) in &facts.raised {
                let Some(at) = volume.index(*ordinal) else {
                    return Err(RootFactsError::Mismatch("a raised article not in the post"));
                };
                if volume.articles[at].state != State::Waiting {
                    continue;
                }
                volume.set_class(at, class(*raised));
                self.moves.push(Move {
                    volume: VolumeIndex(index as u32),
                    ordinal: *ordinal,
                    class: volume.articles[at].class,
                });
            }
        }
        Ok(())
    }

    // An article an earlier run brought is intact when `whole` says the
    // bytes delivered over its range were.
    pub(crate) fn vouch(&mut self, mut whole: impl FnMut(VolumeIndex, ByteRange) -> bool) {
        for (index, found) in self.volumes.iter_mut().enumerate() {
            for article in &mut found.articles {
                if let State::Arrived(Some(range)) = article.state {
                    article.intact = whole(VolumeIndex(index as u32), range);
                }
            }
        }
    }

    // Each article of the volume that arrived with a known range, in
    // ordinal order: the bytes a restore reads back.
    pub(crate) fn arrived_ranges(&self, volume: VolumeIndex) -> Vec<(u32, ByteRange)> {
        let Some(found) = self.volumes.get(volume.0 as usize) else {
            return Vec::new();
        };
        found
            .articles
            .iter()
            .filter_map(|article| match article.state {
                State::Arrived(Some(range)) => Some((article.ordinal, range)),
                _ => None,
            })
            .collect()
    }

    // Where an arrived article's bytes were delivered, when that is known.
    pub(crate) fn arrived_range(&self, volume: VolumeIndex, ordinal: u32) -> Option<ByteRange> {
        let found = self.volumes.get(volume.0 as usize)?;
        match found.articles[found.index(ordinal)?].state {
            State::Arrived(range) => range,
            State::Waiting | State::Lost { .. } => None,
        }
    }

    // An arrived article whose bytes are not on disk is fetched again; an
    // article an earlier run fetched unseen is too, since nothing says where
    // its bytes went.
    pub(crate) fn unarrive(&mut self, volume: VolumeIndex, ordinal: u32) {
        let Some(found) = self.volumes.get_mut(volume.0 as usize) else {
            return;
        };
        if let Some(index) = found.index(ordinal)
            && matches!(found.articles[index].state, State::Arrived(_))
        {
            found.set_state(&mut self.open, index, State::Waiting);
        }
    }

    pub(crate) fn unseen(&self) -> Vec<(VolumeIndex, u32)> {
        self.each(|state| state == State::Arrived(None))
    }

    // Articles this run does not fetch: in hand, or lost for good.
    pub(crate) fn settled(&self) -> Vec<(VolumeIndex, u32)> {
        self.each(|state| state != State::Waiting)
    }

    // The post's encoded bytes of the articles `settled` names: what a
    // checkpoint taken now spares a restore fetching again.
    pub(crate) fn settled_bytes(&self) -> u64 {
        self.volumes.iter().map(|volume| volume.settled_bytes).sum()
    }

    fn each(&self, mut keep: impl FnMut(State) -> bool) -> Vec<(VolumeIndex, u32)> {
        let mut out = Vec::new();
        for (index, found) in self.volumes.iter().enumerate() {
            for article in &found.articles {
                if keep(article.state) {
                    out.push((VolumeIndex(index as u32), article.ordinal));
                }
            }
        }
        out
    }

    pub(crate) fn volume_count(&self) -> usize {
        self.volumes.len()
    }

    // The generation of the volume's last change, and the chunks changed
    // after `at`.
    pub(crate) fn marked(&self, volume: usize) -> u64 {
        self.volumes[volume].marks.at()
    }

    pub(crate) fn marked_since(&self, volume: usize, at: u64) -> impl Iterator<Item = Chunk> + '_ {
        self.volumes[volume].marks.since(at)
    }

    // Every chunk the volume's facts can have something in.
    pub(crate) fn chunks(&self, volume: usize) -> impl Iterator<Item = Chunk> + '_ {
        let found = &self.volumes[volume];
        let articles = found
            .articles
            .iter()
            .map(|article| Chunk::Articles(of_ordinal(article.ordinal)));
        let known = found
            .known
            .keys()
            .map(|boundary| Chunk::Known(of_boundary(*boundary as u64)));
        let segments = found
            .starts
            .values()
            .filter_map(|held| held.last())
            .map(|ordinal| Chunk::Segments(of_ordinal(*ordinal)))
            .chain([Chunk::Segments(0)]);
        articles.chain(known).chain(segments)
    }

    pub(crate) fn ordered_only(&self, volume: usize) -> bool {
        self.volumes[volume].ordered_only
    }

    // The articles of one chunk, in ordinal order.
    pub(crate) fn articles_in(&self, volume: usize, chunk: u32) -> Vec<(u32, ArticleFacts)> {
        in_chunk(&self.volumes[volume], chunk)
            .map(|article| (article.ordinal, article_facts(article.state)))
            .collect()
    }

    // The waiting articles of one chunk raised above the ordinary class.
    pub(crate) fn raised_in(&self, volume: usize, chunk: u32) -> Vec<(u32, ClassFacts)> {
        in_chunk(&self.volumes[volume], chunk)
            .filter(|article| article.state == State::Waiting)
            .filter_map(|article| Some((article.ordinal, class_facts(article.class)?)))
            .collect()
    }

    // The known boundaries of one chunk, by boundary index.
    pub(crate) fn known_in(&self, volume: usize, chunk: u32) -> Vec<(u64, u64)> {
        let first = chunk as usize * CHUNK as usize;
        self.volumes[volume]
            .known
            .range(first..first + CHUNK as usize)
            .map(|(index, offset)| (*index as u64, *offset))
            .collect()
    }

    // The decoded offsets whose segments are the chunk's: from each start
    // one of its articles holds up to the next start, and for the first
    // chunk whatever lies before every start.
    pub(crate) fn owned_by(&self, volume: usize, chunk: u32) -> Vec<(Bound<u64>, Bound<u64>)> {
        let found = &self.volumes[volume];
        let mut out = Vec::new();
        if chunk == 0 {
            let end = found
                .starts
                .keys()
                .next()
                .map_or(Bound::Unbounded, |start| Bound::Excluded(*start));
            out.push((Bound::Unbounded, end));
        }
        for article in in_chunk(found, chunk) {
            let State::Arrived(Some(range)) = article.state else {
                continue;
            };
            let start = range.start;
            if found.starts.get(&start).and_then(|held| held.last()) != Some(&article.ordinal) {
                continue;
            }
            let end = found
                .starts
                .range((Bound::Excluded(start), Bound::Unbounded))
                .next()
                .map_or(Bound::Unbounded, |(next, _)| Bound::Excluded(*next));
            out.push((Bound::Included(start), end));
        }
        out
    }

    // The chunk whose segments hold a segment starting at `offset`.
    pub(crate) fn owner(&self, volume: usize, offset: u64) -> u32 {
        self.volumes[volume].owner(offset)
    }
}

fn in_chunk(volume: &Volume, chunk: u32) -> impl Iterator<Item = &super::Article> + '_ {
    let first = chunk.saturating_mul(CHUNK);
    let from = volume
        .articles
        .partition_point(|article| article.ordinal < first);
    volume.articles[from..]
        .iter()
        .take_while(move |article| of_ordinal(article.ordinal) == chunk)
}

fn article_facts(state: State) -> ArticleFacts {
    match state {
        State::Waiting => ArticleFacts::Waiting,
        State::Arrived(range) => ArticleFacts::Arrived(range.map(|range| (range.start, range.end))),
        State::Lost { damaged } => ArticleFacts::Lost { damaged },
    }
}

fn class_facts(class: Class) -> Option<ClassFacts> {
    Some(match class {
        Class::Critical => ClassFacts::Critical,
        Class::Recovery => ClassFacts::Recovery,
        Class::FirstArticle => ClassFacts::FirstArticle,
        Class::Window(rank) => ClassFacts::Window(rank),
        Class::Ordinary | Class::Parked => return None,
    })
}

fn class(facts: ClassFacts) -> Class {
    match facts {
        ClassFacts::Critical => Class::Critical,
        ClassFacts::Recovery => Class::Recovery,
        ClassFacts::FirstArticle => Class::FirstArticle,
        ClassFacts::Window(rank) => Class::Window(rank),
    }
}
