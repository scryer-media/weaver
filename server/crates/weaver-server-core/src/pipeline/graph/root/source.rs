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

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::Bound;

use weaver_graph::{ByteRange, Marks, Pull, VolumeIndex};

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
    // The copy in hand passed its own checksum. An arrival says so; for an
    // article an earlier run brought, the decoder's checksums kept for its
    // range say so. An article placed by order alone keeps none, so the
    // arrival is its only word.
    intact: bool,
}

// The articles not in hand, by volume, filed on the one path that changes a
// state: a reader asks the volumes that hold them, never walks the articles
// of every volume. Derived from the states, so a restore rebuilds it by
// laying the states down again and nothing of it is kept.
#[derive(Debug, Default)]
struct Open {
    // The articles still waited on, by index. A volume with none has no
    // entry.
    waiting: BTreeMap<VolumeIndex, BTreeSet<usize>>,
    // The articles every server failed, by index. A volume with none has no
    // entry.
    lost: BTreeMap<VolumeIndex, BTreeSet<usize>>,
    // The volumes holding a lost article or a short tail not yet told.
    untold: BTreeSet<VolumeIndex>,
    // The volumes and articles the readers looked at, for the tests that
    // hold them to the ones still open.
    #[cfg(test)]
    visits: [std::sync::atomic::AtomicUsize; 2],
}

impl Open {
    fn file(&mut self, volume: VolumeIndex, index: usize, state: State) {
        file_in(&mut self.waiting, volume, index, state == State::Waiting);
        file_in(
            &mut self.lost,
            volume,
            index,
            matches!(state, State::Lost { .. }),
        );
        if state == (State::Lost { damaged: false }) {
            self.untold.insert(volume);
        }
    }

    fn visit(&self, _volumes: usize, _articles: usize) {
        #[cfg(test)]
        {
            use std::sync::atomic::Ordering::Relaxed;
            self.visits[0].fetch_add(_volumes, Relaxed);
            self.visits[1].fetch_add(_articles, Relaxed);
        }
    }
}

fn file_in(
    sets: &mut BTreeMap<VolumeIndex, BTreeSet<usize>>,
    volume: VolumeIndex,
    index: usize,
    held: bool,
) {
    match (sets.entry(volume), held) {
        (Entry::Vacant(entry), true) => {
            entry.insert(BTreeSet::from([index]));
        }
        (Entry::Occupied(mut entry), true) => {
            entry.get_mut().insert(index);
        }
        (Entry::Occupied(mut entry), false) => {
            entry.get_mut().remove(&index);
            if entry.get().is_empty() {
                entry.remove();
            }
        }
        (Entry::Vacant(_), false) => {}
    }
}

#[derive(Debug)]
struct Volume {
    // Where the volume is in the post.
    at: VolumeIndex,
    // In ordinal order.
    articles: Vec<Article>,
    encoded_len: u64,
    // The post's encoded bytes of the articles no longer waited on: in hand,
    // or lost for good. Kept by the one path that changes a state.
    settled_bytes: u64,
    // Decoded offsets known exactly, by boundary index: boundary `k` is
    // where article `k` starts, and boundary `articles.len()` is the end.
    known: BTreeMap<usize, u64>,
    // How many pairs of neighbouring known boundaries fall: the later
    // offset below the earlier. None while the post's placements agree,
    // and then every boundary's bounds rise with its index.
    disorder: usize,
    // The articles a deferral can still park: waiting, in the normal
    // order. Filed on the two paths that change a state or a class.
    parkable: BTreeSet<usize>,
    // The articles a queueing can bring back: waiting, and parked. Filed
    // beside `parkable`.
    parked: BTreeSet<usize>,
    // The articles carry no position of their own, so the volume can only
    // be placed in order, from the front.
    ordered_only: bool,
    // The ordinals of the articles arrived at each decoded start. The last
    // of them holds the segments from that start to the next one.
    starts: BTreeMap<u64, BTreeSet<u32>>,
    // The articles that arrived short of where the bytes after them start,
    // whose missing tail went to the tree as lost. Not kept: a restored
    // root tells it again, as it tells every lost article again.
    cut: BTreeSet<usize>,
    // The tails found short since the damage was last taken.
    cuts: Vec<ByteRange>,
    // The chunks of the volume's facts each change touched.
    marks: Marks<Chunk>,
}

impl Volume {
    fn new(at: VolumeIndex, sizes: &[(u32, u32)]) -> Self {
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
                    intact: false,
                };
                encoded += u64::from(*bytes);
                article
            })
            .collect();
        Self {
            at,
            articles,
            encoded_len: encoded,
            settled_bytes: 0,
            known: BTreeMap::from([(0, 0)]),
            disorder: 0,
            parkable: (0..sizes.len()).collect(),
            parked: BTreeSet::new(),
            ordered_only: false,
            starts: BTreeMap::new(),
            cut: BTreeSet::new(),
            cuts: Vec::new(),
            marks: Marks::default(),
        }
    }

    // The one way an article's state changes: it is filed in `open`, its
    // chunk is marked, and a start it gains or loses moves segments between
    // chunks, so the chunk they leave and the one they join are marked too.
    fn set_state(&mut self, open: &mut Open, index: usize, state: State) {
        let Article {
            ordinal,
            state: was,
            ..
        } = self.articles[index];
        if was == state {
            return;
        }
        // Another copy, or none: what its tail lacked is asked again.
        self.cut.remove(&index);
        if (was == State::Waiting) != (state == State::Waiting) {
            let size = self.encoded_at(index + 1) - self.encoded_at(index);
            self.settled_bytes = match state {
                State::Waiting => self.settled_bytes - size,
                _ => self.settled_bytes + size,
            };
        }
        self.articles[index].state = state;
        open.file(self.at, index, state);
        self.file_parkable(index);
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
        self.file_parkable(index);
        self.marks.mark(Chunk::Articles(chunk));
    }

    fn file_parkable(&mut self, index: usize) {
        let article = self.articles[index];
        let waiting = article.state == State::Waiting;
        if waiting && article.class == Class::Ordinary {
            self.parkable.insert(index);
        } else {
            self.parkable.remove(&index);
        }
        if waiting && article.class == Class::Parked {
            self.parked.insert(index);
        } else {
            self.parked.remove(&index);
        }
    }

    fn know(&mut self, boundary: usize, offset: u64) {
        self.disorder -= self.falls_around(boundary);
        let was = self.known.insert(boundary, offset);
        self.disorder += self.falls_around(boundary);
        if was != Some(offset) {
            self.marks.mark(Chunk::Known(of_boundary(boundary as u64)));
        }
    }

    fn unknow(&mut self, boundary: usize) {
        self.disorder -= self.falls_around(boundary);
        let was = self.known.remove(&boundary);
        self.disorder += self.falls_around(boundary);
        if was.is_some() {
            self.marks.mark(Chunk::Known(of_boundary(boundary as u64)));
        }
    }

    // The falling pairs among the known boundaries next to `boundary`'s
    // place: the two it makes when known, the one across it when not.
    fn falls_around(&self, boundary: usize) -> usize {
        let below = self.known.range(..boundary).next_back().map(|(_, at)| *at);
        let above = self.known.range(boundary + 1..).next().map(|(_, at)| *at);
        let falls = |low: Option<u64>, high: Option<u64>| {
            usize::from(matches!((low, high), (Some(low), Some(high)) if low > high))
        };
        match self.known.get(&boundary) {
            Some(at) => falls(below, Some(*at)) + falls(Some(*at), above),
            None => falls(below, above),
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

    // The covering articles a queueing can bring back. With none parked
    // there is no run to find.
    fn parked_covering(&self, range: ByteRange) -> impl Iterator<Item = usize> + '_ {
        (!self.parked.is_empty())
            .then(|| self.run(range))
            .into_iter()
            .flat_map(move |run| self.parked.range(run).copied())
            .filter(move |index| self.may_carry(*index, range))
    }

    // The articles whose bytes may fall in the range.
    fn covering(&self, range: ByteRange) -> impl Iterator<Item = usize> + '_ {
        self.run(range)
            .filter(move |index| self.may_carry(*index, range))
    }

    // The covering articles a deferral can still park.
    fn parkable_covering(&self, range: ByteRange) -> impl Iterator<Item = usize> + '_ {
        self.parkable
            .range(self.run(range))
            .copied()
            .filter(move |index| self.may_carry(*index, range))
    }

    // The articles that may carry the range lie in this run. While no
    // known boundaries fall, an article's bounds rise with its index, so
    // they are one run, found by halving.
    fn run(&self, range: ByteRange) -> std::ops::Range<usize> {
        let all = self.articles.len();
        match self.disorder {
            0 => {
                let start = first_of(all, |index| self.bounds(index + 1).1 > range.start);
                let end = first_of(all, |index| self.bounds(index).0 >= range.end);
                start..end.max(start)
            }
            _ => 0..all,
        }
    }

    fn may_carry(&self, index: usize, range: ByteRange) -> bool {
        self.bounds(index).0 < range.end && self.bounds(index + 1).1 > range.start
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

    // Runs of lost articles whose range can now be told: both edges are
    // known, or nothing else in the volume can arrive to tell them better,
    // in which case the run takes its widest bound. In a volume placed only
    // in order, every article after a lost one sits where the order put it,
    // short of the bytes the loss took, so the run's damage reaches the
    // volume's end, which is told once nothing else can arrive. The runs are
    // read off the lost articles `open` files, and the volume leaves the
    // untold ones once none of its runs waits to be told.
    fn take_damage(&mut self, open: &mut Open, out: &mut Vec<ByteRange>) {
        let settled = !open.waiting.contains_key(&self.at);
        let mut runs = Vec::new();
        if let Some(lost) = open.lost.get(&self.at) {
            let mut lost = lost.iter().copied().peekable();
            while let Some(start) = lost.next() {
                let mut index = start + 1;
                while lost.next_if_eq(&index).is_some() {
                    index += 1;
                }
                runs.push((start, index));
            }
        }
        let mut untold = false;
        for (start, index) in runs {
            open.visit(0, index - start);
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
            if !fresh {
                continue;
            }
            if !(told || settled) {
                untold = true;
                continue;
            }
            let range = ByteRange::new(self.bounds(start).0, self.bounds(end).1);
            if !range.is_empty() {
                out.push(range);
            }
            for at in start..index {
                self.set_state(open, at, State::Lost { damaged: true });
            }
        }
        out.append(&mut self.cuts);
        if !untold {
            open.untold.remove(&self.at);
        }
    }

    // The tail of an article that arrived short: one whose bytes end before
    // the boundary after it, which its own header, the next article's
    // begin or the file's declared end tells, lost the bytes between, and
    // they are told as a lost article's are, for every recovery kind and
    // every store alike. Told once both edges are in hand; a volume placed
    // only in order has no edge but its order, so nothing there is short of
    // anything.
    fn cut_after(&mut self, open: &mut Open, index: usize) {
        if self.ordered_only || self.cut.contains(&index) {
            return;
        }
        let Some(State::Arrived(Some(range))) =
            self.articles.get(index).map(|article| article.state)
        else {
            return;
        };
        if let Some(end) = self
            .known
            .get(&(index + 1))
            .copied()
            .filter(|end| *end > range.end)
        {
            self.cuts.push(ByteRange::new(range.end, end));
            self.cut.insert(index);
            open.untold.insert(self.at);
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct ArticleMap {
    volumes: Vec<Volume>,
    open: Open,
    moves: Vec<Move>,
}

impl ArticleMap {
    // One entry per volume, in volume order: each article's ordinal and the
    // encoded size the post declares for it. Every article waits.
    pub(crate) fn new(volumes: &[Vec<(u32, u32)>]) -> Self {
        let volumes: Vec<Volume> = volumes
            .iter()
            .zip(0u32..)
            .map(|(sizes, at)| Volume::new(VolumeIndex(at), sizes))
            .collect();
        let waiting = volumes
            .iter()
            .filter(|found| !found.articles.is_empty())
            .map(|found| (found.at, (0..found.articles.len()).collect()))
            .collect();
        Self {
            volumes,
            open: Open {
                waiting,
                ..Open::default()
            },
            moves: Vec::new(),
        }
    }

    // The volumes and articles the readers looked at since the last call.
    #[cfg(test)]
    pub(crate) fn visits(&self) -> (usize, usize) {
        use std::sync::atomic::Ordering::Relaxed;
        let [volumes, articles] = &self.open.visits;
        (volumes.swap(0, Relaxed), articles.swap(0, Relaxed))
    }

    // An article was decoded and placed at `range`, `intact` when it passed
    // its own checksum. Where the next one begins is what its header
    // declares, never where its bytes ended: a damaged body can decode
    // longer or shorter than its part.
    pub(crate) fn arrived(
        &mut self,
        volume: VolumeIndex,
        ordinal: u32,
        range: ByteRange,
        declared_end: Option<u64>,
        intact: bool,
    ) {
        let Some(found) = self.volumes.get_mut(volume.0 as usize) else {
            return;
        };
        let Some(index) = found.index(ordinal) else {
            return;
        };
        found.articles[index].intact = intact;
        found.set_state(&mut self.open, index, State::Arrived(Some(range)));
        found.know(index, range.start);
        if let Some(end) = declared_end {
            found.know(index + 1, end);
        }
        if let Some(before) = index.checked_sub(1) {
            found.cut_after(&mut self.open, before);
        }
        found.cut_after(&mut self.open, index);
    }

    // The file's decoded length, which the first decoded article declares.
    pub(crate) fn set_length(&mut self, volume: VolumeIndex, length: u64) {
        if let Some(found) = self.volumes.get_mut(volume.0 as usize) {
            let end = found.articles.len();
            found.know(end, length);
            if let Some(last) = end.checked_sub(1) {
                found.cut_after(&mut self.open, last);
            }
        }
    }

    pub(crate) fn set_ordered_only(&mut self, volume: VolumeIndex) {
        if let Some(found) = self.volumes.get_mut(volume.0 as usize) {
            found.ordered_only = true;
        }
    }

    // Whether this run holds an intact copy of the article that no seek has
    // sent back for: that copy stands, and a further one brings nothing.
    pub(crate) fn holds_intact(&self, volume: VolumeIndex, ordinal: u32) -> bool {
        self.volumes.get(volume.0 as usize).is_some_and(|found| {
            found.index(ordinal).is_some_and(|index| {
                let article = found.articles[index];
                article.intact && matches!(article.state, State::Arrived(Some(_)))
            })
        })
    }

    // Every server failed the article.
    pub(crate) fn lost(&mut self, volume: VolumeIndex, ordinal: u32) {
        let Some(found) = self.volumes.get_mut(volume.0 as usize) else {
            return;
        };
        if let Some(index) = found.index(ordinal)
            && found.articles[index].state == State::Waiting
        {
            found.set_state(&mut self.open, index, State::Lost { damaged: false });
        }
    }

    // Articles this run will not fetch because an earlier run already did.
    pub(crate) fn arrived_before(&mut self, mut queued: impl FnMut(VolumeIndex, u32) -> bool) {
        for (index, found) in self.volumes.iter_mut().enumerate() {
            let volume = VolumeIndex(index as u32);
            for at in 0..found.articles.len() {
                let article = found.articles[at];
                if article.state == State::Waiting && !queued(volume, article.ordinal) {
                    found.set_state(&mut self.open, at, State::Arrived(None));
                }
            }
        }
    }

    // Whether the article is in hand or lost for good. An article the map
    // does not hold was never posted here, so nothing has settled it.
    pub(crate) fn article_settled(&self, volume: VolumeIndex, ordinal: u32) -> bool {
        self.volumes.get(volume.0 as usize).is_some_and(|found| {
            found
                .index(ordinal)
                .is_some_and(|index| found.articles[index].state != State::Waiting)
        })
    }

    // Every article every server failed, with the encoded size the post
    // declares for it.
    pub(crate) fn lost_articles(&self) -> impl Iterator<Item = (VolumeIndex, u32, u64)> + '_ {
        self.open.lost.iter().flat_map(|(volume, lost)| {
            let found = &self.volumes[volume.0 as usize];
            lost.iter().map(move |at| {
                let size = found.encoded_at(at + 1) - found.encoded_at(*at);
                (*volume, found.articles[*at].ordinal, size)
            })
        })
    }

    // Where a reader laying the volume in order stands: the first article
    // still to come, the decoded offset it is laid at, and whether an
    // article before it was lost. An article in hand whose range this run
    // never saw stops the walk, as nothing says where it ends.
    pub(crate) fn ordered_front(&self, volume: VolumeIndex) -> (u32, u64, bool) {
        let mut front = (0, 0, false);
        let Some(found) = self.volumes.get(volume.0 as usize) else {
            return front;
        };
        for article in &found.articles {
            front.0 = article.ordinal;
            match article.state {
                State::Arrived(Some(range)) => front.1 = range.end,
                State::Lost { .. } => front.2 = true,
                State::Waiting | State::Arrived(None) => return front,
            }
            front.0 = article.ordinal.saturating_add(1);
        }
        front
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

    // Each volume's encoded bytes still waited on, as the post declares
    // them: the articles neither in hand nor lost for good.
    pub(crate) fn remaining(&self) -> Vec<u64> {
        self.volumes
            .iter()
            .map(|found| found.encoded_len - found.settled_bytes)
            .collect()
    }

    // How many volumes have every article in hand.
    pub(crate) fn whole_count(&self) -> usize {
        (0..self.volumes.len())
            .filter(|&index| self.volume_whole(VolumeIndex(index as u32)))
            .count()
    }

    pub(crate) fn volume_waiting(&self, volume: VolumeIndex) -> bool {
        self.open.visit(1, 0);
        self.open.waiting.contains_key(&volume)
    }

    // Whether every article of the volume is in hand: none waits and every
    // server failed none.
    pub(crate) fn volume_whole(&self, volume: VolumeIndex) -> bool {
        self.open.visit(1, 0);
        (volume.0 as usize) < self.volumes.len()
            && !self.open.waiting.contains_key(&volume)
            && !self.open.lost.contains_key(&volume)
    }

    // Whether an article the queue fetches unasked can still arrive: one
    // waits that is not parked.
    pub(crate) fn volume_to_come(&self, volume: VolumeIndex) -> bool {
        self.open
            .waiting
            .get(&volume)
            .is_some_and(|waiting| self.to_come(volume, waiting))
    }

    // The volumes with an article the queue fetches unasked still to come,
    // in volume order.
    pub(crate) fn coming(&self) -> impl Iterator<Item = VolumeIndex> + '_ {
        self.open
            .waiting
            .iter()
            .filter(|(volume, waiting)| self.to_come(**volume, waiting))
            .map(|(volume, _)| *volume)
    }

    // The volumes with an article still waited on, parked or not, in
    // volume order.
    pub(crate) fn waiting_volumes(&self) -> impl Iterator<Item = VolumeIndex> + '_ {
        self.open
            .waiting
            .keys()
            .inspect(|_| self.open.visit(1, 0))
            .copied()
    }

    // Parked articles are waiting ones, so the volume has one to come
    // unasked when it waits on more than it parked.
    fn to_come(&self, volume: VolumeIndex, waiting: &BTreeSet<usize>) -> bool {
        self.open.visit(1, 0);
        self.volumes
            .get(volume.0 as usize)
            .is_some_and(|found| waiting.len() > found.parked.len())
    }

    // The ranges of lost articles that can now be marked damaged: only the
    // volumes holding one not yet told are visited.
    pub(crate) fn take_damage(&mut self, out: &mut Vec<(VolumeIndex, ByteRange)>) {
        let mut ranges = Vec::new();
        let mut next = self.open.untold.first().copied();
        while let Some(volume) = next {
            self.open.visit(1, 0);
            ranges.clear();
            if let Some(found) = self.volumes.get_mut(volume.0 as usize) {
                found.take_damage(&mut self.open, &mut ranges);
            }
            out.extend(ranges.iter().map(|range| (volume, *range)));
            next = self
                .open
                .untold
                .range((Bound::Excluded(volume), Bound::Unbounded))
                .next()
                .copied();
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
        // A deferral moves only what it can park, and is never lost.
        let covering: Vec<usize> = if parks {
            found.parkable_covering(range).collect()
        } else {
            found.covering(range).collect()
        };
        let lost = !parks
            && covering.iter().any(|index| {
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
    // only parked work joins it, which is the one move this makes. An
    // article already raised keeps its class, and one already queued is
    // left as it is.
    pub(crate) fn queue(&mut self, volume: VolumeIndex, range: ByteRange) {
        let Some(found) = self.volumes.get_mut(volume.0 as usize) else {
            return;
        };
        let parked: Vec<usize> = found.parked_covering(range).collect();
        for index in parked {
            let article = found.articles[index];
            if article.state == State::Waiting && article.class > Class::Ordinary {
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

// The first index below `len` at which `rises` holds, which then holds at
// every index after it; `len` when it holds at none.
fn first_of(len: usize, rises: impl Fn(usize) -> bool) -> usize {
    let (mut low, mut high) = (0, len);
    while low < high {
        let middle = low + (high - low) / 2;
        if rises(middle) {
            high = middle;
        } else {
            low = middle + 1;
        }
    }
    low
}

fn start_of(state: State) -> Option<u64> {
    match state {
        State::Arrived(Some(range)) => Some(range.start),
        _ => None,
    }
}
