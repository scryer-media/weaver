// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The seek-up side of the root: which articles carry which bytes of each of
// the post's files, and how soon each one is fetched.
//
// An article's decoded range is known exactly only once it is decoded. Until
// then the post's encoded sizes bound it: encoding only ever adds bytes, so
// the decoded bytes between two known boundaries are at most the encoded
// bytes between them. Every exactly known boundary (the start of a file, the
// edges of each decoded article, a file's declared length) tightens the
// bounds of the articles around it.

mod durable;

use std::collections::{BTreeMap, BTreeSet, HashMap};

use weaver_graph::{ByteRange, Pull, VolumeIndex};

use weaver_graph::Marks;

use super::facts::{Chunk, of_boundary, of_ordinal};

// How soon an article is fetched, soonest first: the download queue's
// dispatch classes, with the unlock window's draw order inside its own.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum Class {
    // Something cannot progress until these bytes land.
    Critical,
    // Recovery blocks a repair is waiting on.
    Recovery,
    // The first article of a member, which samples the post.
    FirstArticle,
    // A volume an in-order reader will reach next, by draw order.
    Window(u32),
    Ordinary,
    // Held in the job's recovery queue: fetched only once something asks.
    Parked,
}

impl Class {
    // Whether something is waiting on the article: the queue fetches it as
    // completion work, ahead of the payload and never parked, on every
    // attempt until it lands or every server has failed it.
    pub(crate) fn waited_on(self) -> bool {
        matches!(self, Self::Critical | Self::Recovery)
    }
}

// A change of class the download queue has to apply.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Move {
    pub(crate) volume: VolumeIndex,
    pub(crate) ordinal: u32,
    pub(crate) class: Class,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum State {
    Waiting,
    // `None` when it arrived in an earlier run, whose placement this run
    // never saw.
    Arrived(Option<ByteRange>),
    // Every server failed it. `damaged` once its range went to the tree.
    Lost { damaged: bool },
}

#[derive(Clone, Copy, Debug)]
struct Article {
    ordinal: u32,
    // The post's encoded bytes of the file before this article.
    encoded_start: u64,
    state: State,
    class: Class,
}

#[derive(Debug, Default)]
struct Volume {
    // In ordinal order.
    articles: Vec<Article>,
    encoded_len: u64,
    // Decoded offsets known exactly, by boundary index: boundary `k` is
    // where article `k` starts, and boundary `articles.len()` is the end.
    known: BTreeMap<usize, u64>,
    // The articles carry no position of their own, so the volume can only
    // be placed in order, from the front.
    ordered_only: bool,
    // The ordinals of the articles arrived at each decoded start. The last
    // of them holds the segments from that start to the next one.
    starts: BTreeMap<u64, BTreeSet<u32>>,
    // The chunks of the volume's facts each change touched.
    marks: Marks<Chunk>,
}

impl Volume {
    fn new(sizes: &[(u32, u32)]) -> Self {
        let mut sizes = sizes.to_vec();
        sizes.sort_unstable_by_key(|(ordinal, _)| *ordinal);
        let mut encoded = 0;
        let articles = sizes
            .iter()
            .map(|(ordinal, bytes)| {
                let article = Article {
                    ordinal: *ordinal,
                    encoded_start: encoded,
                    state: State::Waiting,
                    class: Class::Ordinary,
                };
                encoded += u64::from(*bytes);
                article
            })
            .collect();
        Self {
            articles,
            encoded_len: encoded,
            known: BTreeMap::from([(0, 0)]),
            ordered_only: false,
            starts: BTreeMap::new(),
            marks: Marks::default(),
        }
    }

    // The one way an article's state changes: its chunk is marked, and a
    // start it gains or loses moves segments between chunks, so the chunk
    // they leave and the one they join are marked too.
    fn set_state(&mut self, index: usize, state: State) {
        let Article {
            ordinal,
            state: was,
            ..
        } = self.articles[index];
        if was == state {
            return;
        }
        self.articles[index].state = state;
        self.marks.mark(Chunk::Articles(of_ordinal(ordinal)));
        let (from, to) = (start_of(was), start_of(state));
        if from == to {
            return;
        }
        if let Some(start) = from {
            self.move_start(start, |held| {
                held.remove(&ordinal);
            });
        }
        if let Some(start) = to {
            self.move_start(start, |held| {
                held.insert(ordinal);
            });
        }
    }

    fn move_start(&mut self, start: u64, change: impl FnOnce(&mut BTreeSet<u32>)) {
        let before = self.owner(start);
        let held = self.starts.entry(start).or_default();
        change(held);
        if held.is_empty() {
            self.starts.remove(&start);
        }
        let after = self.owner(start);
        self.marks.mark(Chunk::Segments(before));
        self.marks.mark(Chunk::Segments(after));
    }

    // The chunk whose segments part holds a segment starting at `offset`:
    // that of the article arrived at the greatest start at or before it, the
    // last ordinal of several at one start, and the first chunk before any.
    fn owner(&self, offset: u64) -> u32 {
        self.starts
            .range(..=offset)
            .next_back()
            .and_then(|(_, held)| held.last())
            .map_or(0, |ordinal| of_ordinal(*ordinal))
    }

    fn set_class(&mut self, index: usize, class: Class) {
        let article = &mut self.articles[index];
        if article.class == class {
            return;
        }
        article.class = class;
        let chunk = of_ordinal(article.ordinal);
        self.marks.mark(Chunk::Articles(chunk));
    }

    fn know(&mut self, boundary: usize, offset: u64) {
        if self.known.insert(boundary, offset) != Some(offset) {
            self.marks.mark(Chunk::Known(of_boundary(boundary as u64)));
        }
    }

    fn unknow(&mut self, boundary: usize) {
        if self.known.remove(&boundary).is_some() {
            self.marks.mark(Chunk::Known(of_boundary(boundary as u64)));
        }
    }

    fn index(&self, ordinal: u32) -> Option<usize> {
        self.articles
            .binary_search_by_key(&ordinal, |article| article.ordinal)
            .ok()
    }

    fn encoded_at(&self, boundary: usize) -> u64 {
        self.articles
            .get(boundary)
            .map_or(self.encoded_len, |article| article.encoded_start)
    }

    // Where boundary `k` can lie, from the nearest known boundary on each
    // side. A bound, not a proof: a post that understates its sizes makes it
    // miss, which costs order, never bytes.
    fn bounds(&self, boundary: usize) -> (u64, u64) {
        let encoded = self.encoded_at(boundary);
        let (below, below_at) = self
            .known
            .range(..=boundary)
            .next_back()
            .map_or((0, 0), |(index, offset)| (*index, *offset));
        let mut low = below_at;
        let mut high = below_at.saturating_add(encoded - self.encoded_at(below));
        if let Some((above, above_at)) = self.known.range(boundary..).next() {
            high = high.min(*above_at);
            low = low.max(above_at.saturating_sub(self.encoded_at(*above) - encoded));
        }
        (low, high.max(low))
    }

    // The articles whose bytes may fall in the range.
    fn covering(&self, range: ByteRange) -> impl Iterator<Item = usize> + '_ {
        (0..self.articles.len()).filter(move |index| {
            self.bounds(*index).0 < range.end && self.bounds(index + 1).1 > range.start
        })
    }

    // Whether the article's bytes certainly include some of the range.
    fn surely_in(&self, index: usize, range: ByteRange) -> bool {
        let start = self.bounds(index).1;
        let end = self.bounds(index + 1).0;
        start < end && start < range.end && end > range.start
    }

    // How far the volume is in hand without a gap from its start.
    fn front(&self) -> u64 {
        let mut front = 0;
        for article in &self.articles {
            match article.state {
                State::Arrived(Some(range)) if range.start <= front => {
                    front = front.max(range.end);
                }
                _ => break,
            }
        }
        front
    }

    fn waiting(&self) -> bool {
        self.articles
            .iter()
            .any(|article| article.state == State::Waiting)
    }

    // Whether an article the queue fetches unasked can still arrive.
    fn to_come(&self) -> bool {
        self.articles
            .iter()
            .any(|article| article.state == State::Waiting && article.class != Class::Parked)
    }

    // Runs of lost articles whose range can now be told: both edges are
    // known, or nothing else in the volume can arrive to tell them better,
    // in which case the run takes its widest bound. In a volume placed only
    // in order, every article after a lost one sits where the order put it,
    // short of the bytes the loss took, so the run's damage reaches the
    // volume's end, which is told once nothing else can arrive.
    fn take_damage(&mut self, out: &mut Vec<ByteRange>) {
        let settled = !self.waiting();
        let mut index = 0;
        while index < self.articles.len() {
            if !matches!(self.articles[index].state, State::Lost { .. }) {
                index += 1;
                continue;
            }
            let start = index;
            while index < self.articles.len()
                && matches!(self.articles[index].state, State::Lost { .. })
            {
                index += 1;
            }
            let fresh = self.articles[start..index]
                .iter()
                .any(|article| article.state == State::Lost { damaged: false });
            let end = match self.ordered_only {
                true => self.articles.len(),
                false => index,
            };
            let told = !self.ordered_only
                && self.known.contains_key(&start)
                && self.known.contains_key(&end);
            if !fresh || !(told || settled) {
                continue;
            }
            let range = ByteRange::new(self.bounds(start).0, self.bounds(end).1);
            if !range.is_empty() {
                out.push(range);
            }
            for at in start..index {
                self.set_state(at, State::Lost { damaged: true });
            }
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct ArticleMap {
    volumes: Vec<Volume>,
    moves: Vec<Move>,
}

impl ArticleMap {
    // One entry per volume, in volume order: each article's ordinal and the
    // encoded size the post declares for it.
    pub(crate) fn new(volumes: &[Vec<(u32, u32)>]) -> Self {
        Self {
            volumes: volumes.iter().map(|sizes| Volume::new(sizes)).collect(),
            moves: Vec::new(),
        }
    }

    // An article was decoded and placed at `range`.
    pub(crate) fn arrived(&mut self, volume: VolumeIndex, ordinal: u32, range: ByteRange) {
        let Some(found) = self.volumes.get_mut(volume.0 as usize) else {
            return;
        };
        let Some(index) = found.index(ordinal) else {
            return;
        };
        found.set_state(index, State::Arrived(Some(range)));
        found.know(index, range.start);
        found.know(index + 1, range.end);
    }

    // The file's decoded length, which the first decoded article declares.
    pub(crate) fn set_length(&mut self, volume: VolumeIndex, length: u64) {
        if let Some(found) = self.volumes.get_mut(volume.0 as usize) {
            let end = found.articles.len();
            found.know(end, length);
        }
    }

    pub(crate) fn set_ordered_only(&mut self, volume: VolumeIndex) {
        if let Some(found) = self.volumes.get_mut(volume.0 as usize) {
            found.ordered_only = true;
        }
    }

    // Every server failed the article.
    pub(crate) fn lost(&mut self, volume: VolumeIndex, ordinal: u32) {
        let Some(found) = self.volumes.get_mut(volume.0 as usize) else {
            return;
        };
        if let Some(index) = found.index(ordinal)
            && found.articles[index].state == State::Waiting
        {
            found.set_state(index, State::Lost { damaged: false });
        }
    }

    // Articles this run will not fetch because an earlier run already did.
    pub(crate) fn arrived_before(&mut self, mut queued: impl FnMut(VolumeIndex, u32) -> bool) {
        for (index, found) in self.volumes.iter_mut().enumerate() {
            let volume = VolumeIndex(index as u32);
            for at in 0..found.articles.len() {
                let article = found.articles[at];
                if article.state == State::Waiting && !queued(volume, article.ordinal) {
                    found.set_state(at, State::Arrived(None));
                }
            }
        }
    }

    // The class the article is fetched under now.
    pub(crate) fn class(&self, volume: VolumeIndex, ordinal: u32) -> Option<Class> {
        let found = self.volumes.get(volume.0 as usize)?;
        Some(found.articles[found.index(ordinal)?].class)
    }

    // Whether the volume's opening article can still arrive.
    pub(crate) fn head_waiting(&self, volume: VolumeIndex) -> bool {
        self.volumes
            .get(volume.0 as usize)
            .and_then(|found| found.articles.first())
            .is_some_and(|article| article.state == State::Waiting)
    }

    pub(crate) fn volume_waiting(&self, volume: VolumeIndex) -> bool {
        self.volumes
            .get(volume.0 as usize)
            .is_some_and(Volume::waiting)
    }

    pub(crate) fn volume_to_come(&self, volume: VolumeIndex) -> bool {
        self.volumes
            .get(volume.0 as usize)
            .is_some_and(Volume::to_come)
    }

    // The ranges of lost articles that can now be marked damaged.
    pub(crate) fn take_damage(&mut self, out: &mut Vec<(VolumeIndex, ByteRange)>) {
        let mut ranges = Vec::new();
        for (index, found) in self.volumes.iter_mut().enumerate() {
            ranges.clear();
            found.take_damage(&mut ranges);
            out.extend(
                ranges
                    .iter()
                    .map(|range| (VolumeIndex(index as u32), *range)),
            );
        }
    }

    // Answer a seek, and move the articles that carry it to `class` when
    // one is given. A seek with no class is only answered: a bid the window
    // did not draw keeps its articles where they are. A lost article loses
    // the seek, but every article of the range that can still come is
    // raised all the same: one lost article does not leave the rest of the
    // range where the normal order would never reach it, and the lost range
    // itself reaches every object it lands in at its exact bounds as damage.
    //
    // `Parked` is the one class that lowers: it takes only articles still
    // in the normal order, so one already raised keeps its class, and a
    // later raise takes a parked one as it takes any. A deferral is never
    // lost or unseekable to the asker: its answer is how many it parked.
    pub(crate) fn seek(
        &mut self,
        volume: VolumeIndex,
        range: ByteRange,
        class: Option<Class>,
    ) -> Pull {
        let parks = class == Some(Class::Parked);
        let Some(found) = self.volumes.get_mut(volume.0 as usize) else {
            return if parks { Pull::Moved(0) } else { Pull::Lost };
        };
        // The clamp for open-ended seeks: a recovery object does not know how
        // long its volume is and asks to the end of the address space, so the
        // range is cut at the furthest the volume's end can lie.
        let end = found.bounds(found.articles.len()).1;
        let range = ByteRange::new(range.start.min(end), range.end.min(end));
        if found.ordered_only {
            // Without positions only the front can be told apart, and it is
            // already here.
            return if parks || range.end <= found.front() {
                Pull::Moved(0)
            } else {
                Pull::Unseekable
            };
        }
        let covering: Vec<usize> = found.covering(range).collect();
        let lost = covering.iter().any(|index| {
            matches!(found.articles[*index].state, State::Lost { .. })
                && found.surely_in(*index, range)
        });
        let mut moved = 0;
        for index in covering {
            let article = found.articles[index];
            let moves = |class: &Class| match class {
                Class::Parked => article.class == Class::Ordinary,
                _ => *class < article.class,
            };
            if let Some(class) = class.filter(moves)
                && article.state == State::Waiting
            {
                found.set_class(index, class);
                self.moves.push(Move {
                    volume,
                    ordinal: article.ordinal,
                    class,
                });
                moved += 1;
            }
        }
        if lost && !parks {
            Pull::Lost
        } else {
            Pull::Moved(moved)
        }
    }

    // Queue the articles that may carry `range` without raising them: they
    // are wanted, but ahead of nothing, so queued work keeps its order and
    // only parked work joins it. An article already raised keeps its class.
    pub(crate) fn queue(&mut self, volume: VolumeIndex, range: ByteRange) {
        let Some(found) = self.volumes.get_mut(volume.0 as usize) else {
            return;
        };
        let covering: Vec<usize> = found.covering(range).collect();
        for index in covering {
            let article = found.articles[index];
            if article.state == State::Waiting && article.class >= Class::Ordinary {
                found.set_class(index, Class::Ordinary);
                self.moves.push(Move {
                    volume,
                    ordinal: article.ordinal,
                    class: Class::Ordinary,
                });
            }
        }
    }

    // The first article of every volume leads the payload.
    pub(crate) fn sample(&mut self) {
        for index in 0..self.volumes.len() {
            let volume = VolumeIndex(index as u32);
            let found = &mut self.volumes[index];
            let Some(first) = found.articles.first().copied() else {
                continue;
            };
            if first.state == State::Waiting && Class::FirstArticle < first.class {
                found.set_class(0, Class::FirstArticle);
                self.moves.push(Move {
                    volume,
                    ordinal: first.ordinal,
                    class: Class::FirstArticle,
                });
            }
        }
    }

    // The class changes since the last call, soonest first.
    pub(crate) fn take_moves(&mut self) -> Vec<Move> {
        let mut moves = std::mem::take(&mut self.moves);
        // A later raise of the same article supersedes an earlier one.
        let mut latest: HashMap<(VolumeIndex, u32), Class> = HashMap::new();
        for found in &moves {
            latest.insert((found.volume, found.ordinal), found.class);
        }
        moves.retain(|found| latest.get(&(found.volume, found.ordinal)) == Some(&found.class));
        moves.sort_by_key(|found| (found.class, found.volume, found.ordinal));
        moves.dedup();
        moves
    }
}

fn start_of(state: State) -> Option<u64> {
    match state {
        State::Arrived(Some(range)) => Some(range.start),
        _ => None,
    }
}
