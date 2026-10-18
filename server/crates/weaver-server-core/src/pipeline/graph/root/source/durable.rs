// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The article map out as facts and back, and what a restore asks of it: the
// articles it may read back from disk, and the ones it found were not there.

use weaver_graph::{ByteRange, VolumeIndex};

use super::{ArticleMap, Class, Move, State};
use crate::pipeline::graph::root::facts::{ArticleFacts, ClassFacts, RootFactsError, VolumeFacts};

impl ArticleMap {
    pub(crate) fn facts(&self) -> Vec<VolumeFacts> {
        self.volumes
            .iter()
            .map(|volume| VolumeFacts {
                articles: volume
                    .articles
                    .iter()
                    .map(|article| (article.ordinal, article_facts(article.state)))
                    .collect(),
                known: volume
                    .known
                    .iter()
                    .map(|(index, offset)| (*index as u64, *offset))
                    .collect(),
                ordered_only: volume.ordered_only,
                segments: Vec::new(),
                raised: volume
                    .articles
                    .iter()
                    .filter(|article| article.state == State::Waiting)
                    .filter_map(|article| Some((article.ordinal, class_facts(article.class)?)))
                    .collect(),
            })
            .collect()
    }

    // Lay the facts over a map made from the post. The post must be the one
    // they were taken from: the same files, the same articles.
    pub(crate) fn restore(&mut self, facts: &[VolumeFacts]) -> Result<(), RootFactsError> {
        if facts.len() != self.volumes.len() {
            return Err(RootFactsError::Mismatch("another number of files"));
        }
        for (index, (volume, facts)) in self.volumes.iter_mut().zip(facts).enumerate() {
            if volume.articles.len() != facts.articles.len() {
                return Err(RootFactsError::Mismatch("another number of articles"));
            }
            for (article, (ordinal, state)) in volume.articles.iter_mut().zip(&facts.articles) {
                if article.ordinal != *ordinal {
                    return Err(RootFactsError::Mismatch("another article ordinal"));
                }
                article.state = match *state {
                    ArticleFacts::Waiting => State::Waiting,
                    ArticleFacts::Arrived(range) => State::Arrived(
                        range
                            .filter(|(start, end)| start <= end)
                            .map(|(start, end)| ByteRange::new(start, end)),
                    ),
                    ArticleFacts::Lost { damaged } => State::Lost { damaged },
                };
            }
            let last = volume.articles.len() as u64;
            volume.known = facts
                .known
                .iter()
                .filter(|(index, _)| *index <= last)
                .map(|(index, offset)| (*index as usize, *offset))
                .collect();
            volume.ordered_only = facts.ordered_only;
            // Raised again through the moves, so the queue built from the
            // post is reclassed as the one that went down was.
            for (ordinal, raised) in &facts.raised {
                let Some(at) = volume.index(*ordinal) else {
                    return Err(RootFactsError::Mismatch("a raised article not in the post"));
                };
                let article = &mut volume.articles[at];
                if article.state != State::Waiting {
                    continue;
                }
                article.class = class(*raised);
                self.moves.push(Move {
                    volume: VolumeIndex(index as u32),
                    ordinal: *ordinal,
                    class: article.class,
                });
            }
        }
        Ok(())
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
            found.articles[index].state = State::Waiting;
        }
    }

    pub(crate) fn unseen(&self) -> Vec<(VolumeIndex, u32)> {
        self.each(|state| state == State::Arrived(None))
    }

    // Articles this run does not fetch: in hand, or lost for good.
    pub(crate) fn settled(&self) -> Vec<(VolumeIndex, u32)> {
        self.each(|state| state != State::Waiting)
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
        Class::Ordinary => return None,
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
