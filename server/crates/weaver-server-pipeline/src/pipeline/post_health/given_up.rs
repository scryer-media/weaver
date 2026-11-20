// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The articles of one post that will never arrive. Each is written down
// where it is given up, and a restored root lays back the ones it holds
// lost from a run before the restart. Everything the feed weighs a post on
// is kept as it changes, so reading it costs nothing of the post's size.
//
// Every file is a place in the post, and a post that names one volume
// twice, under names numbered alike, posts that place twice. A byte of a
// place is lost only where every posting of it lost that byte, and the post
// is the size of one posting of each place. A place posted once is lost
// where its one posting is.

use std::collections::btree_map::BTreeMap;
use std::collections::hash_map::{Entry, HashMap};

use super::first_articles::FirstArticles;
use super::names_a_place;
use crate::jobs::model::JobSpec;
use crate::pipeline::SegmentTerminalState;
use weaver_server_base::jobs::ids::SegmentId;

// What one file of the post lost.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FileLosses {
    // Articles every server said are gone.
    pub missing: usize,
    // Articles given up for any end but the feed's having stopped asking.
    pub lost: usize,
    // The health-counted bytes of every article given up.
    pub bytes: u64,
    // The health-counted bytes of the articles every server said are gone.
    pub missing_bytes: u64,
}

#[derive(Debug, Default)]
pub struct GivenUp {
    // Each article with the end this run saw it come to: `None` for one
    // lost in a run before a restart, which every server had failed.
    articles: HashMap<SegmentId, Option<SegmentTerminalState>>,
    // By file index.
    files: BTreeMap<u32, FileLosses>,
    bytes: u64,
    missing_bytes: u64,
    // Where each article of each file starts, in the bytes the post declares
    // for the file, by file index and ordinal.
    starts: Vec<Vec<u64>>,
    // Each file's place, by file index, and each place's postings.
    places: Vec<usize>,
    postings: Vec<Vec<u32>>,
    // Each file's ranges every server said are gone, from start to end.
    gone: HashMap<u32, BTreeMap<u64, u64>>,
    // The health-counted bytes of every posting of a place past the largest
    // one.
    again_bytes: u64,
    // The post's first-article sample, once the feed has read it.
    first_articles: Option<FirstArticles>,
}

impl GivenUp {
    // The losses of the post `spec` names, none yet.
    pub fn new(spec: &JobSpec) -> Self {
        let mut given_up = Self::default();
        let mut named = HashMap::new();
        for (file, index) in spec.files.iter().zip(0u32..) {
            let place = match names_a_place(&file.filename) {
                Some(name) => *named.entry(name).or_insert(given_up.postings.len()),
                None => given_up.postings.len(),
            };
            if place == given_up.postings.len() {
                given_up.postings.push(Vec::new());
            }
            given_up.postings[place].push(index);
            given_up.places.push(place);
            let mut segments: Vec<_> = file.segments.iter().collect();
            segments.sort_by_key(|segment| segment.ordinal);
            let mut start = 0;
            given_up.starts.push(
                segments
                    .iter()
                    .map(|segment| {
                        let at = start;
                        start += u64::from(segment.bytes);
                        at
                    })
                    .collect(),
            );
        }
        let declared = |index: &u32| -> u64 {
            let file = &spec.files[*index as usize];
            if !file.role.counts_toward_health() {
                return 0;
            }
            file.segments
                .iter()
                .map(|segment| u64::from(segment.bytes))
                .sum()
        };
        given_up.again_bytes = given_up
            .postings
            .iter()
            .map(|files| {
                let sizes = files.iter().map(declared);
                sizes.clone().sum::<u64>() - sizes.max().unwrap_or(0)
            })
            .sum();
        given_up
    }

    // How much of `range` of the file at `index` every other posting of its
    // place lost too: what the place lost once the file lost it.
    fn lost_everywhere(&self, index: u32, range: (u64, u64)) -> u64 {
        let others = self
            .places
            .get(index as usize)
            .map_or(&[][..], |place| &self.postings[*place][..])
            .iter()
            .filter(|other| **other != index);
        let mut left = vec![range];
        for other in others {
            let gone = self.gone.get(other);
            left = left
                .into_iter()
                .flat_map(|range| overlaps(gone, range))
                .collect();
        }
        left.iter().map(|(start, end)| end - start).sum()
    }

    // The article will never arrive, after `end`, and weighs `bytes`. The
    // first end an article reaches is the one it holds, so an article told
    // again, by a later path or by a restore, is already held.
    pub fn give_up(&mut self, segment: SegmentId, end: Option<SegmentTerminalState>, bytes: u64) {
        // Even an article already held: the end it is booked at now may
        // change where its file's first article stands.
        if let Some(first_articles) = self.first_articles.as_mut() {
            first_articles.touch(segment.file_id.file_index, segment.segment_number);
        }
        match self.articles.entry(segment) {
            Entry::Occupied(_) => return,
            Entry::Vacant(slot) => {
                slot.insert(end);
            }
        }
        let index = segment.file_id.file_index;
        let file = self.files.entry(index).or_default();
        file.bytes += bytes;
        self.bytes += bytes;
        if end != Some(SegmentTerminalState::Abandoned) {
            file.lost += 1;
        }
        if gone_at(end) {
            file.missing += 1;
            file.missing_bytes += bytes;
            if let Some(first_articles) = self.first_articles.as_mut() {
                first_articles.lose(index, bytes);
            }
            let start = self
                .starts
                .get(index as usize)
                .and_then(|starts| starts.get(segment.segment_number as usize))
                .copied()
                .unwrap_or(0);
            let range = (start, start + bytes);
            self.missing_bytes += self.lost_everywhere(index, range);
            self.gone.entry(index).or_default().insert(range.0, range.1);
        }
    }

    pub fn holds(&self, segment: SegmentId) -> bool {
        self.articles.contains_key(&segment)
    }

    // Whether every server said the article is gone: what `missing_bytes`
    // already counts it in.
    pub(in crate::pipeline) fn gone(&self, segment: SegmentId) -> bool {
        self.articles.get(&segment).is_some_and(|end| gone_at(*end))
    }

    // The health-counted bytes of every article given up.
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    // The health-counted bytes of the articles every server said are gone,
    // each place counted where every posting of it lost them.
    pub fn missing_bytes(&self) -> u64 {
        self.missing_bytes
    }

    // The health-counted bytes the post declares past one posting of each
    // place: what its size counts twice.
    pub fn again_bytes(&self) -> u64 {
        self.again_bytes
    }

    // The post's first-article sample as the feed last read it, if it has.
    pub(in crate::pipeline) fn first_articles(&self) -> Option<&FirstArticles> {
        self.first_articles.as_ref()
    }

    pub(in crate::pipeline) fn take_first_articles(&mut self) -> Option<FirstArticles> {
        self.first_articles.take()
    }

    // The sample is kept here, so an article given up from now on marks
    // its file's head to be read again.
    pub(in crate::pipeline) fn keep_first_articles(&mut self, first_articles: FirstArticles) {
        self.first_articles = Some(first_articles);
    }

    pub fn file(&self, index: u32) -> FileLosses {
        self.files.get(&index).copied().unwrap_or_default()
    }

    // The files that lost an article every server said is gone.
    pub fn missing_files(&self) -> impl Iterator<Item = u32> + '_ {
        self.files
            .iter()
            .filter(|(_, file)| file.missing > 0)
            .map(|(index, _)| *index)
    }
}

// Whether an article given up at `end` is one every server said is gone:
// `None` is one lost before a restart, which every server had failed.
fn gone_at(end: Option<SegmentTerminalState>) -> bool {
    end.is_none_or(|end| {
        matches!(
            end,
            SegmentTerminalState::Missing
                | SegmentTerminalState::Unservable
                | SegmentTerminalState::UnservableNotFound
        )
    })
}

// The parts of `range` the ranges in `gone` cover.
fn overlaps(gone: Option<&BTreeMap<u64, u64>>, (start, end): (u64, u64)) -> Vec<(u64, u64)> {
    let Some(gone) = gone else {
        return Vec::new();
    };
    let first = gone
        .range(..=start)
        .next_back()
        .filter(|(_, until)| **until > start)
        .map_or(start, |(from, _)| *from);
    gone.range(first..end)
        .map(|(from, until)| ((*from).max(start), (*until).min(end)))
        .filter(|(from, until)| from < until)
        .collect()
}
