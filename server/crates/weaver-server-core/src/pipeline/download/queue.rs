// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use crate::jobs::ids::{JobId, MessageId, NzbFileId, SegmentId};
use crate::jobs::model::JobSpec;

// A work item representing a segment to download.
#[derive(Clone)]
pub struct DownloadWork {
    // Attached at dispatch; queued articles do not allocate counters.
    pub counters: Option<Arc<crate::pipeline::queue::JobCounters>>,
    pub segment_id: SegmentId,
    pub message_id: MessageId,
    // The file's newsgroups, shared by every segment of the file rather than
    // cloned per segment: a job's queue used to carry one `Vec<String>` per
    // article, and every lease and refill cloned it again.
    pub groups: Arc<[String]>,
    pub priority: u32,
    pub byte_estimate: u32,
    pub retry_count: u32,
    // Whether this segment belongs to a recovery file (PAR2 repair blocks).
    pub is_recovery: bool,
    // Whether pipeline progress is explicitly waiting for this segment.
    //
    // This is deliberately orthogonal to `is_recovery`: PAR2 completion work
    // and the bounded direct-store identity probe wave both need to lead the
    // ordinary queue. The flag changes dispatch eligibility only.
    pub completion_critical: bool,
    // Servers to skip for this download (e.g. after decode failure from that server).
    pub exclude_servers: Vec<usize>,
    // Transport-rotation hint: the server whose established connection just
    // failed for this segment. Selection avoids it on the next attempt so the
    // retry lands elsewhere when an alternative exists. Unlike `exclude_servers`, it
    // never counts toward article-not-found exhaustion, so one transient
    // timeout can never help declare an article missing. Replaced (not
    // accumulated) on each transport failure; advisory only, so an index left
    // stale by a pool rebuild merely skips one server for one attempt.
    pub avoid_server: Option<usize>,
}

impl DownloadWork {
    // The fields a queue predicate reads, as a queued item shows them.
    pub fn view(&self) -> QueuedWork<'_> {
        QueuedWork {
            segment_id: self.segment_id,
            priority: self.priority,
            byte_estimate: self.byte_estimate,
            retry_count: self.retry_count,
            is_recovery: self.is_recovery,
            completion_critical: self.completion_critical,
            exclude_servers: &self.exclude_servers,
            avoid_server: self.avoid_server,
        }
    }
}

// What a queue predicate sees of one queued item: everything dispatch
// decides on, read in place, without the message id or newsgroups the item
// is fetched by.
#[derive(Clone, Copy, Debug)]
pub struct QueuedWork<'a> {
    pub segment_id: SegmentId,
    pub priority: u32,
    pub byte_estimate: u32,
    pub retry_count: u32,
    pub is_recovery: bool,
    pub completion_critical: bool,
    pub exclude_servers: &'a [usize],
    pub avoid_server: Option<usize>,
}

impl QueuedWork<'_> {
    // Whether the work is steered off a server: excluded from one or hinted
    // away from one.
    #[cfg(test)]
    fn is_steered(&self) -> bool {
        !self.exclude_servers.is_empty() || self.avoid_server.is_some()
    }
}

// One item taken off a queue, or on its way onto one. An article the queue
// was built with carries no message id: it is read from the job's article
// list when the item is dispatched, so a queued article holds no copy of it.
// Work that came back from dispatch keeps the id it was fetched by.
#[derive(Clone)]
pub struct QueueItem {
    pub segment_id: SegmentId,
    pub groups: Arc<[String]>,
    pub priority: u32,
    pub byte_estimate: u32,
    pub retry_count: u32,
    pub is_recovery: bool,
    pub completion_critical: bool,
    pub exclude_servers: Vec<usize>,
    pub avoid_server: Option<usize>,
    message_id: Option<MessageId>,
    counters: Option<Arc<crate::pipeline::queue::JobCounters>>,
}

impl QueueItem {
    fn is_steered(&self) -> bool {
        !self.exclude_servers.is_empty() || self.avoid_server.is_some()
    }

    // The work to dispatch. The message id of an article the queue was built
    // with is the one its part has in use in `spec`; None when `spec` holds
    // no such part, which a job's own queue never asks.
    pub fn into_work(self, spec: &JobSpec) -> Option<DownloadWork> {
        let message_id = match self.message_id {
            Some(message_id) => message_id,
            None => MessageId::new(
                spec.files
                    .get(self.segment_id.file_id.file_index as usize)?
                    .segment(self.segment_id.segment_number)?
                    .message_ids
                    .in_use(),
            ),
        };
        Some(DownloadWork {
            counters: self.counters,
            segment_id: self.segment_id,
            message_id,
            groups: self.groups,
            priority: self.priority,
            byte_estimate: self.byte_estimate,
            retry_count: self.retry_count,
            is_recovery: self.is_recovery,
            completion_critical: self.completion_critical,
            exclude_servers: self.exclude_servers,
            avoid_server: self.avoid_server,
        })
    }
}

impl From<DownloadWork> for QueueItem {
    fn from(work: DownloadWork) -> Self {
        Self {
            segment_id: work.segment_id,
            groups: work.groups,
            priority: work.priority,
            byte_estimate: work.byte_estimate,
            retry_count: work.retry_count,
            is_recovery: work.is_recovery,
            completion_critical: work.completion_critical,
            exclude_servers: work.exclude_servers,
            avoid_server: work.avoid_server,
            message_id: Some(work.message_id),
            counters: work.counters,
        }
    }
}

// Dispatch classes, in the order the queue serves them:
//
// 0. completion-critical work that is not recovery — PAR2 index bootstrap,
//    metadata discovery, the direct-store identity probe wave;
// 1. promoted PAR2 recovery blocks, which share the payload's connections and
//    lead it, because a repair cannot start until they land;
// 2. first articles — one article per file, marked by the owner of the queue.
//    They lead the payload so that the first round trips of a job sample
//    every file in it rather than the first file in it, which is what makes
//    "this post is not there any more" answerable in seconds instead of
//    after a whole file's worth of articles;
// 3. ordinary payload.
//
// Classes 0 and 1 are the completion-critical class and 2 and 3 the
// ordinary one; the rank leads the key, so the critical class is served
// first.
const COMPLETION_RANK_CRITICAL: u8 = 0;
const COMPLETION_RANK_PROMOTED_RECOVERY: u8 = 1;
const COMPLETION_RANK_FIRST_ARTICLE: u8 = 2;
const COMPLETION_RANK_ORDINARY: u8 = 3;

fn completion_rank_for(completion_critical: bool, is_recovery: bool, first_article: bool) -> u8 {
    if !completion_critical {
        if first_article {
            COMPLETION_RANK_FIRST_ARTICLE
        } else {
            COMPLETION_RANK_ORDINARY
        }
    } else if is_recovery {
        COMPLETION_RANK_PROMOTED_RECOVERY
    } else {
        COMPLETION_RANK_CRITICAL
    }
}

// The dispatch key of one queued item, packed into two words: the queue
// serves keys in ascending order of (class, priority, rank, sequence), where
// a lower priority number is served first, an unranked item sorts as the
// highest rank, and the sequence is insertion order. The sequence is unique
// within a queue, so no two items share a key, and the bit below it, which
// says whether a rank was given, never decides an order.
//
// hi: class (2 bits) | priority (32) | rank, high 30 bits
// lo: rank, low 2 bits | sequence (61) | ranked (1)
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Hash)]
struct Key(u64, u64);

const SEQUENCE_BITS: u32 = 61;
const SEQUENCE_MASK: u64 = (1 << SEQUENCE_BITS) - 1;

impl Key {
    // No item: a sequence no queue reaches.
    const NONE: Key = Key(u64::MAX, u64::MAX);
    // The first key of the ordinary class.
    const ORDINARY: Key = Key((COMPLETION_RANK_FIRST_ARTICLE as u64) << 62, 0);

    fn new(completion_rank: u8, priority: u32, rank: Option<u32>, sequence: u64) -> Self {
        debug_assert!(sequence < SEQUENCE_MASK);
        let rank_value = u64::from(rank.unwrap_or(u32::MAX));
        Key(
            (u64::from(completion_rank) << 62) | (u64::from(priority) << 30) | (rank_value >> 2),
            ((rank_value & 3) << 62)
                | ((sequence & SEQUENCE_MASK) << 1)
                | u64::from(rank.is_some()),
        )
    }

    fn completion_rank(self) -> u8 {
        (self.0 >> 62) as u8
    }

    fn completion_critical(self) -> bool {
        self.completion_rank() < COMPLETION_RANK_FIRST_ARTICLE
    }

    fn priority(self) -> u32 {
        (self.0 >> 30) as u32
    }

    fn rank(self) -> Option<u32> {
        (self.1 & 1 == 1).then_some((((self.0 & ((1 << 30) - 1)) << 2) | (self.1 >> 62)) as u32)
    }

    fn sequence(self) -> u64 {
        (self.1 >> 1) & SEQUENCE_MASK
    }

    fn with_priority_rank(self, priority: u32, rank: Option<u32>) -> Self {
        Key::new(self.completion_rank(), priority, rank, self.sequence())
    }
}

// One queued item in dispatch order: its place in the job, its size and
// whether it is recovery. Everything else an article the queue was built
// with is fetched by is the same for every article of its file, or read from
// the job's article list when it is dispatched; an item that carries more
// keeps it in `DownloadQueue::extras`.
#[derive(Clone, Copy)]
struct Entry {
    file_index: u32,
    segment_number: u32,
    byte_estimate: u32,
    // Bit 0: recovery. Above it, one past the item's index in `extras`, or
    // zero for an item without extras.
    meta: u32,
}

impl Entry {
    fn is_recovery(self) -> bool {
        self.meta & 1 == 1
    }

    fn extra(self) -> Option<usize> {
        (self.meta >> 1).checked_sub(1).map(|index| index as usize)
    }
}

// What a queued item carries beyond its entry: anything work back from
// dispatch, or work of another job, holds.
struct Extra {
    // The item's job when it is not the queue's.
    job_id: Option<JobId>,
    message_id: Option<MessageId>,
    // The item's newsgroups when they are not its file's.
    groups: Option<Arc<[String]>>,
    counters: Option<Arc<crate::pipeline::queue::JobCounters>>,
    retry_count: u32,
    exclude_servers: Vec<usize>,
    avoid_server: Option<usize>,
}

// The extras of the items that have them, by index, reusing freed slots.
#[derive(Default)]
struct Extras {
    slots: Vec<Option<Extra>>,
    free: Vec<u32>,
}

impl Extras {
    fn insert(&mut self, extra: Extra) -> u32 {
        if let Some(index) = self.free.pop() {
            self.slots[index as usize] = Some(extra);
            index
        } else {
            self.slots.push(Some(extra));
            (self.slots.len() - 1) as u32
        }
    }

    fn get(&self, index: usize) -> Option<&Extra> {
        self.slots.get(index).and_then(Option::as_ref)
    }

    fn take(&mut self, index: usize) -> Option<Extra> {
        let extra = self.slots.get_mut(index)?.take();
        if extra.is_some() {
            self.free.push(index as u32);
        }
        extra
    }

    fn iter_mut(&mut self) -> impl Iterator<Item = &mut Extra> {
        self.slots.iter_mut().flatten()
    }
}

// The keys of one file's queued items, by segment number: the place to
// find an item by its segment without a walk of the queue.
#[derive(Default)]
struct FileSlots {
    // Queued items of the file.
    queued: u32,
    // The newsgroups of the file's items that carry none of their own.
    groups: Option<Arc<[String]>>,
    // One key per segment number, `Key::NONE` where none is queued.
    dense: Vec<Key>,
    // A second queued item of a segment, or an item whose segment number is
    // far past the dense run.
    more: BTreeSet<(u32, Key)>,
}

impl FileSlots {
    fn insert(&mut self, segment_number: u32, key: Key) {
        self.queued += 1;
        let index = segment_number as usize;
        // The run grows to a segment number within the room reserved for the
        // file, or within twice its length: a stray far number cannot make
        // it allocate past what the file's articles need.
        if index >= self.dense.len()
            && (index < self.dense.capacity() || index <= self.dense.len().max(1024) * 2)
        {
            self.dense.resize(index + 1, Key::NONE);
        }
        match self.dense.get_mut(index) {
            Some(slot) if *slot == Key::NONE => *slot = key,
            _ => {
                self.more.insert((segment_number, key));
            }
        }
    }

    fn remove(&mut self, segment_number: u32, key: Key) {
        match self.dense.get_mut(segment_number as usize) {
            Some(slot) if *slot == key => *slot = Key::NONE,
            _ => {
                if !self.more.remove(&(segment_number, key)) {
                    return;
                }
            }
        }
        self.queued -= 1;
    }

    fn rekey(&mut self, segment_number: u32, old: Key, new: Key) {
        match self.dense.get_mut(segment_number as usize) {
            Some(slot) if *slot == old => *slot = new,
            _ => {
                if self.more.remove(&(segment_number, old)) {
                    self.more.insert((segment_number, new));
                }
            }
        }
    }

    fn keys_of(&self, segment_number: u32, out: &mut Vec<Key>) {
        if let Some(key) = self.dense.get(segment_number as usize)
            && *key != Key::NONE
        {
            out.push(*key);
        }
        if !self.more.is_empty() {
            out.extend(
                self.more
                    .range((segment_number, Key(0, 0))..=(segment_number, Key::NONE))
                    .map(|(_, key)| *key),
            );
        }
    }

    fn keys(&self) -> impl Iterator<Item = Key> + '_ {
        self.dense
            .iter()
            .copied()
            .filter(|key| *key != Key::NONE)
            .chain(self.more.iter().map(|(_, key)| *key))
    }
}

// Priority queue for download work items.
//
// Each queued item is one ordered-map entry of 32 bytes, its key in its
// file's slots, and, only for an item back from dispatch, an extra. The
// message id of an article the queue was built with is not held here: it is
// read from the job's article list when the item is dispatched.
pub struct DownloadQueue {
    // Every queued item in dispatch order.
    //
    // An ordered map rather than a heap so that a matching read can walk the
    // queue in dispatch order and stop at the first hit, and a removal takes
    // out only the matched entry: skipping an ineligible head costs a read,
    // not a pop and re-push of everything ahead of the match.
    order: BTreeMap<Key, Entry>,
    // Items in the completion-critical class.
    critical_len: usize,
    next_sequence: u64,
    // The job of the first item queued: an item of it names only its file
    // index.
    job_id: Option<JobId>,
    extras: Extras,
    // Queued items steered off a server: failure exclusions (escalated work
    // that may need a backfill lane) or a rotation hint. Maintained on
    // push/pop.
    excluded_work: usize,
    // Queued recovery items, which the restart checkpoint never holds back.
    // Maintained on push/pop.
    recovery_work: usize,
    // Where each file's queued items sit, maintained on every push, removal
    // and re-key. A caller that names the segments it moves finds them here,
    // not by a walk of the queue. A file's slots stay once made, so an item
    // put back costs the same as the first.
    files: HashMap<NzbFileId, FileSlots>,
    // One article per file — the lowest ordinal each file has. Held here
    // rather than on the work item so that every path that puts an article
    // back, including a requeue after a lane gave one up, lands it in the
    // leading class again without having to know it was a first article.
    first_articles: HashSet<SegmentId>,
}

impl DownloadQueue {
    pub fn new() -> Self {
        Self {
            order: BTreeMap::new(),
            critical_len: 0,
            next_sequence: 0,
            job_id: None,
            extras: Extras::default(),
            excluded_work: 0,
            recovery_work: 0,
            files: HashMap::new(),
            first_articles: HashSet::new(),
        }
    }

    // Marks an article as its file's first, which is the class it is served
    // in from now on, however often it is requeued.
    pub fn note_first_article(&mut self, segment_id: SegmentId) {
        self.first_articles.insert(segment_id);
    }

    // Every article this queue holds as a first article, whether or not it is
    // still queued.
    pub fn first_articles(&self) -> impl Iterator<Item = SegmentId> + '_ {
        self.first_articles.iter().copied()
    }

    // Room for a file's items with segment numbers below `segments`, sized
    // once, for a queue built from the file's whole article list.
    pub(crate) fn reserve_file(&mut self, file_id: NzbFileId, segments: u32) {
        let slots = self.files.entry(file_id).or_default();
        slots
            .dense
            .reserve_exact((segments as usize).saturating_sub(slots.dense.len()));
    }

    // The newsgroups this queue shares among a file's items, once it has
    // queued one of them.
    pub(crate) fn file_groups(&self, file_id: NzbFileId) -> Option<&Arc<[String]>> {
        self.files.get(&file_id)?.groups.as_ref()
    }

    // Queues an article of the job as its article list gives it: fetched by
    // the id its part has in use when it is dispatched, from `groups`, never
    // tried, steered nowhere.
    pub(crate) fn push_article(
        &mut self,
        segment_id: SegmentId,
        groups: &Arc<[String]>,
        priority: u32,
        byte_estimate: u32,
    ) {
        let job_id = *self.job_id.get_or_insert(segment_id.file_id.job_id);
        if job_id != segment_id.file_id.job_id
            || self
                .files
                .get(&segment_id.file_id)
                .and_then(|slots| slots.groups.as_ref())
                .is_some_and(|own| !Arc::ptr_eq(own, groups))
        {
            self.push(QueueItem {
                segment_id,
                groups: Arc::clone(groups),
                priority,
                byte_estimate,
                retry_count: 0,
                is_recovery: false,
                completion_critical: false,
                exclude_servers: Vec::new(),
                avoid_server: None,
                message_id: None,
                counters: None,
            });
            return;
        }
        let sequence = self.take_sequence();
        let first_article = self.first_articles.contains(&segment_id);
        let key = Key::new(
            completion_rank_for(false, false, first_article),
            priority,
            None,
            sequence,
        );
        let slots = self.files.entry(segment_id.file_id).or_default();
        if slots.groups.is_none() {
            slots.groups = Some(Arc::clone(groups));
        }
        slots.insert(segment_id.segment_number, key);
        self.order.insert(
            key,
            Entry {
                file_index: segment_id.file_id.file_index,
                segment_number: segment_id.segment_number,
                byte_estimate,
                meta: 0,
            },
        );
    }

    fn take_sequence(&mut self) -> u64 {
        let sequence = self.next_sequence;
        self.next_sequence += 1;
        sequence
    }

    pub fn push(&mut self, item: impl Into<QueueItem>) {
        let item = item.into();
        let sequence = self.take_sequence();
        let steered = item.is_steered();
        if steered {
            self.excluded_work += 1;
        }
        self.recovery_work += usize::from(item.is_recovery);
        let first_article = self.first_articles.contains(&item.segment_id);
        let key = Key::new(
            completion_rank_for(item.completion_critical, item.is_recovery, first_article),
            item.priority,
            None,
            sequence,
        );
        if item.completion_critical {
            self.critical_len += 1;
        }
        let file_id = item.segment_id.file_id;
        let job_id = *self.job_id.get_or_insert(file_id.job_id);
        let slots = self.files.entry(file_id).or_default();
        let own_groups = match &slots.groups {
            None => {
                slots.groups = Some(item.groups);
                None
            }
            Some(groups) if Arc::ptr_eq(groups, &item.groups) => None,
            Some(_) => Some(item.groups),
        };
        slots.insert(item.segment_id.segment_number, key);
        let extra = (job_id != file_id.job_id
            || item.message_id.is_some()
            || own_groups.is_some()
            || item.counters.is_some()
            || item.retry_count != 0
            || steered)
            .then(|| Extra {
                job_id: (job_id != file_id.job_id).then_some(file_id.job_id),
                message_id: item.message_id,
                groups: own_groups,
                counters: item.counters,
                retry_count: item.retry_count,
                exclude_servers: item.exclude_servers,
                avoid_server: item.avoid_server,
            });
        let extra = extra.map_or(0, |extra| self.extras.insert(extra) + 1);
        self.order.insert(
            key,
            Entry {
                file_index: file_id.file_index,
                segment_number: item.segment_id.segment_number,
                byte_estimate: item.byte_estimate,
                meta: (extra << 1) | u32::from(item.is_recovery),
            },
        );
    }

    // The predicate's view of one queued item.
    fn view<'a>(
        job_id: Option<JobId>,
        extras: &'a Extras,
        key: Key,
        entry: Entry,
    ) -> QueuedWork<'a> {
        let extra = entry.extra().and_then(|index| extras.get(index));
        QueuedWork {
            segment_id: SegmentId {
                file_id: NzbFileId {
                    job_id: extra
                        .and_then(|extra| extra.job_id)
                        .or(job_id)
                        .unwrap_or(JobId(0)),
                    file_index: entry.file_index,
                },
                segment_number: entry.segment_number,
            },
            priority: key.priority(),
            byte_estimate: entry.byte_estimate,
            retry_count: extra.map_or(0, |extra| extra.retry_count),
            is_recovery: entry.is_recovery(),
            completion_critical: key.completion_critical(),
            exclude_servers: extra.map_or(&[], |extra| extra.exclude_servers.as_slice()),
            avoid_server: extra.and_then(|extra| extra.avoid_server),
        }
    }

    // Takes the item at `key` out of the queue, with everything it carries.
    fn take(&mut self, key: Key) -> Option<QueueItem> {
        let entry = self.order.remove(&key)?;
        Some(self.taken(key, entry))
    }

    // The item of an entry just removed from `order`, with its counts and
    // slot let go.
    fn taken(&mut self, key: Key, entry: Entry) -> QueueItem {
        let extra = entry.extra().and_then(|index| self.extras.take(index));
        let job_id = extra
            .as_ref()
            .and_then(|extra| extra.job_id)
            .or(self.job_id)
            .unwrap_or(JobId(0));
        let file_id = NzbFileId {
            job_id,
            file_index: entry.file_index,
        };
        let slots = self.files.get_mut(&file_id);
        let file_groups = slots.and_then(|slots| {
            slots.remove(entry.segment_number, key);
            slots.groups.as_ref()
        });
        let completion_critical = key.completion_critical();
        if completion_critical {
            self.critical_len -= 1;
        }
        self.recovery_work = self
            .recovery_work
            .saturating_sub(usize::from(entry.is_recovery()));
        let segment_id = SegmentId {
            file_id,
            segment_number: entry.segment_number,
        };
        match extra {
            None => QueueItem {
                segment_id,
                groups: file_groups.map_or_else(|| Arc::from([]), Arc::clone),
                priority: key.priority(),
                byte_estimate: entry.byte_estimate,
                retry_count: 0,
                is_recovery: entry.is_recovery(),
                completion_critical,
                exclude_servers: Vec::new(),
                avoid_server: None,
                message_id: None,
                counters: None,
            },
            Some(extra) => {
                if !extra.exclude_servers.is_empty() || extra.avoid_server.is_some() {
                    self.excluded_work = self.excluded_work.saturating_sub(1);
                }
                QueueItem {
                    segment_id,
                    groups: extra
                        .groups
                        .unwrap_or_else(|| file_groups.map_or_else(|| Arc::from([]), Arc::clone)),
                    priority: key.priority(),
                    byte_estimate: entry.byte_estimate,
                    retry_count: extra.retry_count,
                    is_recovery: entry.is_recovery(),
                    completion_critical,
                    exclude_servers: extra.exclude_servers,
                    avoid_server: extra.avoid_server,
                    message_id: extra.message_id,
                    counters: extra.counters,
                }
            }
        }
    }

    pub fn pop(&mut self) -> Option<QueueItem> {
        let (key, entry) = self.order.pop_first()?;
        Some(self.taken(key, entry))
    }

    // Number of queued recovery items, in O(1).
    pub fn recovery_work_count(&self) -> usize {
        self.recovery_work
    }

    // Number of queued items with failure exclusions or a rotation hint.
    pub fn excluded_work_count(&self) -> usize {
        self.excluded_work
    }

    // Drop failure exclusions from all queued work. Used when the server
    // config is rebuilt: exclusion indices refer to the old pool layout and
    // would mis-target (or spuriously exhaust) servers in the new one.
    pub fn clear_exclude_servers(&mut self) {
        if self.excluded_work == 0 {
            return;
        }
        // Only an item with extras can hold either; neither is in the key.
        for extra in self.extras.iter_mut() {
            extra.exclude_servers.clear();
            extra.avoid_server = None;
        }
        self.excluded_work = 0;
    }

    // Drop the rotation hint from all queued work, keeping exclusions: a 430
    // is a fact about the article, a hint only about one failed attempt.
    // Returns how many hints were dropped.
    pub fn clear_rotation_hints(&mut self) -> usize {
        if self.excluded_work == 0 {
            return 0;
        }
        let mut cleared = 0;
        for extra in self.extras.iter_mut() {
            if extra.avoid_server.take().is_some() {
                cleared += 1;
                if extra.exclude_servers.is_empty() {
                    self.excluded_work = self.excluded_work.saturating_sub(1);
                }
            }
        }
        cleared
    }

    // Queued items for one file, in O(1).
    pub fn queued_count_for_file(&self, file_id: NzbFileId) -> u32 {
        self.files.get(&file_id).map_or(0, |slots| slots.queued)
    }

    // Every queued item in dispatch order, with its key and the fields its
    // class sets, for the tests that compare two queues.
    #[cfg(test)]
    #[allow(clippy::type_complexity)]
    pub(crate) fn snapshot(&self) -> Vec<(bool, u8, u32, Option<u32>, u64, SegmentId, u32, bool)> {
        self.iter_keyed()
            .map(|(key, work)| {
                (
                    key.completion_critical(),
                    key.completion_rank(),
                    key.priority(),
                    key.rank(),
                    key.sequence(),
                    work.segment_id,
                    work.priority,
                    work.is_recovery,
                )
            })
            .collect()
    }

    // The derived counts as a walk of every queued item finds them, for the
    // tests that hold the maintained ones to it.
    #[cfg(test)]
    pub(crate) fn counts_match_a_walk(&self) -> bool {
        let mut by_file: HashMap<NzbFileId, u32> = HashMap::new();
        for work in self.iter() {
            *by_file.entry(work.segment_id.file_id).or_default() += 1;
        }
        let slots_match = self.iter_keyed().all(|(key, work)| {
            self.files
                .get(&work.segment_id.file_id)
                .is_some_and(|slots| {
                    let mut keys = Vec::new();
                    slots.keys_of(work.segment_id.segment_number, &mut keys);
                    keys.contains(&key)
                })
        });
        let slot_count: u32 = self
            .files
            .values()
            .map(|slots| slots.keys().count() as u32)
            .sum();
        slots_match
            && slot_count as usize == self.order.len()
            && self.excluded_work == self.iter().filter(|work| work.is_steered()).count()
            && self.recovery_work == self.iter().filter(|work| work.is_recovery).count()
            && self.critical_len == self.iter().filter(|work| work.completion_critical).count()
            && self.files.iter().all(|(file, slots)| {
                slots.queued == by_file.get(file).copied().unwrap_or(0)
                    && slots.keys().count() as u32 == slots.queued
            })
    }

    #[cfg(test)]
    pub fn count_matching(&self, mut predicate: impl FnMut(&QueuedWork<'_>) -> bool) -> usize {
        self.iter().filter(|work| predicate(work)).count()
    }

    #[cfg(test)]
    pub fn drain_all(&mut self) -> Vec<QueueItem> {
        std::iter::from_fn(|| self.pop()).collect()
    }

    // Every queued item with its key, in dispatch order.
    fn iter_keyed(&self) -> impl Iterator<Item = (Key, QueuedWork<'_>)> {
        let (job_id, extras) = (self.job_id, &self.extras);
        self.order
            .iter()
            .map(move |(key, entry)| (*key, Self::view(job_id, extras, *key, *entry)))
    }

    // Every queued item, completion-critical class first, each class in
    // dispatch order.
    fn iter(&self) -> impl Iterator<Item = QueuedWork<'_>> {
        self.iter_keyed().map(|(_, work)| work)
    }

    // The items of one class, in dispatch order.
    fn class_range(
        &self,
        completion_critical: bool,
    ) -> std::collections::btree_map::Range<'_, Key, Entry> {
        if completion_critical {
            self.order.range(..Key::ORDINARY)
        } else {
            self.order.range(Key::ORDINARY..)
        }
    }

    // The head of one class, when `matches` takes it.
    fn class_head_matching(
        &self,
        completion_critical: bool,
        matches: impl FnOnce(&QueuedWork<'_>) -> bool,
    ) -> Option<Key> {
        let (key, entry) = self.class_range(completion_critical).next()?;
        matches(&Self::view(self.job_id, &self.extras, *key, *entry)).then_some(*key)
    }

    pub fn pop_next_matching(
        &mut self,
        matches: impl FnOnce(&QueuedWork<'_>) -> bool,
    ) -> Option<QueueItem> {
        let key = self.class_head_matching(self.critical_len != 0, matches)?;
        self.take(key)
    }

    pub fn pop_next_matching_in_class(
        &mut self,
        completion_critical: bool,
        matches: impl FnOnce(&QueuedWork<'_>) -> bool,
    ) -> Option<QueueItem> {
        let key = self.class_head_matching(completion_critical, matches)?;
        self.take(key)
    }

    // Removes the highest-priority item matching `matches`, even when another
    // work class currently owns the head. The queue is walked in dispatch
    // order until the first match, so the cost is O(log n) plus one predicate
    // call per item skipped ahead of the match; the skipped items are only
    // read, never moved. With an eligible head it is the same O(log n) as
    // [`Self::pop`].
    // Every dispatch takes this path; the head-only paths above are not
    // used by dispatch.
    pub fn pop_first_matching(
        &mut self,
        mut matches: impl FnMut(&QueuedWork<'_>) -> bool,
    ) -> Option<QueueItem> {
        let (job_id, extras) = (self.job_id, &self.extras);
        let key = self.order.iter().find_map(|(key, entry)| {
            matches(&Self::view(job_id, extras, *key, *entry)).then_some(*key)
        })?;
        self.take(key)
    }

    pub fn pop_first_matching_in_class(
        &mut self,
        completion_critical: bool,
        mut matches: impl FnMut(&QueuedWork<'_>) -> bool,
    ) -> Option<QueueItem> {
        let (job_id, extras) = (self.job_id, &self.extras);
        let key = self
            .class_range(completion_critical)
            .find_map(|(key, entry)| {
                matches(&Self::view(job_id, extras, *key, *entry)).then_some(*key)
            })?;
        self.take(key)
    }

    // The keys of every queued item of the segments, in dispatch order.
    fn keys_of(&self, segments: impl IntoIterator<Item = SegmentId>) -> Vec<Key> {
        let mut found = Vec::new();
        for segment in segments {
            if let Some(slots) = self.files.get(&segment.file_id) {
                slots.keys_of(segment.segment_number, &mut found);
            }
        }
        found.sort_unstable();
        found
    }

    // Removes and returns every queued item of the segments, in the order
    // `extract_matching` returns them, leaving the rest queued in place.
    pub fn extract_segments(
        &mut self,
        segments: impl IntoIterator<Item = SegmentId>,
    ) -> Vec<QueueItem> {
        let keys = self.keys_of(segments);
        let mut extracted = Vec::with_capacity(keys.len());
        for key in keys {
            extracted.extend(self.take(key));
        }
        extracted
    }

    // Sets the priority and rank of every queued item of each segment, as
    // `reprioritize_matching_with_rank` does for the items it matches.
    // Returns how many items moved.
    pub fn reprioritize_segments(
        &mut self,
        segments: impl IntoIterator<Item = (SegmentId, u32, Option<u32>)>,
    ) -> usize {
        let mut changed = 0;
        for (segment, priority, rank) in segments {
            for key in self.keys_of([segment]) {
                if key.priority() == priority && key.rank() == rank {
                    continue;
                }
                self.rekey(key, key.with_priority_rank(priority, rank));
                changed += 1;
            }
        }
        changed
    }

    // Sets the rank of the file's ordinary queued items at `priority`: its
    // first articles and its items in any other class or at any other
    // priority keep their keys. Only the file's own slots are read. Returns
    // how many items moved.
    pub fn rank_ordinary_file(&mut self, file_id: NzbFileId, priority: u32, rank: u32) -> usize {
        let Some(slots) = self.files.get(&file_id) else {
            return 0;
        };
        if slots.queued == 0 {
            return 0;
        }
        let moving: Vec<Key> = slots
            .keys()
            .filter(|key| {
                key.completion_rank() == COMPLETION_RANK_ORDINARY
                    && key.priority() == priority
                    && key.rank() != Some(rank)
            })
            .collect();
        for key in &moving {
            self.rekey(*key, key.with_priority_rank(priority, Some(rank)));
        }
        moving.len()
    }

    // Moves one queued item to a new key, and its slot with it.
    fn rekey(&mut self, old: Key, new: Key) {
        let Some(entry) = self.order.remove(&old) else {
            return;
        };
        self.order.insert(new, entry);
        let job_id = entry
            .extra()
            .and_then(|index| self.extras.get(index))
            .and_then(|extra| extra.job_id)
            .or(self.job_id)
            .unwrap_or(JobId(0));
        if let Some(slots) = self.files.get_mut(&NzbFileId {
            job_id,
            file_index: entry.file_index,
        }) {
            slots.rekey(entry.segment_number, old, new);
        }
    }

    pub fn peek_next_matching(
        &self,
        matches: impl FnOnce(&QueuedWork<'_>) -> bool,
    ) -> Option<QueuedWork<'_>> {
        let (key, entry) = self.class_range(self.critical_len != 0).next()?;
        let work = Self::view(self.job_id, &self.extras, *key, *entry);
        matches(&work).then_some(work)
    }

    // Read the same candidate as `pop_first_matching`, including work hidden
    // behind an ineligible head: the queue is walked in dispatch order and
    // the walk stops at the first match, so an eligible head is O(log n).
    pub fn peek_first_matching(
        &self,
        mut matches: impl FnMut(&QueuedWork<'_>) -> bool,
    ) -> Option<QueuedWork<'_>> {
        self.iter().find(|work| matches(work))
    }

    // The head of one dispatch class without removing it, in O(log n).
    //
    // For decisions that are about the *shape* of the work rather than the
    // work itself — which newsgroups a connection for this job would have to
    // be opened for, ahead of any lease being cut.
    pub fn peek_in_class(&self, completion_critical: bool) -> Option<QueuedWork<'_>> {
        self.class_range(completion_critical)
            .next()
            .map(|(key, entry)| Self::view(self.job_id, &self.extras, *key, *entry))
    }

    pub fn len(&self) -> usize {
        self.order.len()
    }

    pub fn is_empty(&self) -> bool {
        self.order.is_empty()
    }

    // Queued items in one dispatch class, in O(1).
    //
    // Lease sizing divides the remaining work of the class it is about to
    // lease from, so it must never pay for a queue scan: this is a count,
    // read once per lease.
    pub fn len_in_class(&self, completion_critical: bool) -> usize {
        if completion_critical {
            self.critical_len
        } else {
            self.order.len() - self.critical_len
        }
    }

    // Removes and returns every queued item matching the predicate, leaving
    // the rest queued in place.
    pub fn extract_matching(
        &mut self,
        mut predicate: impl FnMut(&QueuedWork<'_>) -> bool,
    ) -> Vec<QueueItem> {
        let (job_id, extras) = (self.job_id, &self.extras);
        let keys: Vec<Key> = self
            .order
            .iter()
            .filter(|(key, entry)| predicate(&Self::view(job_id, extras, **key, **entry)))
            .map(|(key, _)| *key)
            .collect();
        if keys.len() == self.order.len() {
            let order = std::mem::take(&mut self.order);
            return order
                .into_iter()
                .map(|(key, entry)| self.taken(key, entry))
                .collect();
        }
        keys.into_iter().filter_map(|key| self.take(key)).collect()
    }

    // Adds every queued segment id to `out`.
    //
    // For callers that build work items from a spec rather than from the
    // queue and so must not re-queue an article the queue already owns —
    // pushing a second copy would download it twice.
    pub fn extend_segment_ids(&self, out: &mut HashSet<SegmentId>) {
        out.extend(self.iter().map(|work| work.segment_id));
    }

    // Recompute priorities and optional intra-priority ranks for selected queued
    // work. Unranked equal-priority work remains ordered by original insertion.
    pub fn reprioritize_matching_with_rank(
        &mut self,
        mut priority_for: impl FnMut(&QueuedWork<'_>) -> Option<(u32, Option<u32>)>,
    ) -> usize {
        // Only the items whose key changes move; each keeps its sequence, so
        // equal keys still serve in insertion order.
        let rekeyed: Vec<(Key, Key)> = self
            .iter_keyed()
            .filter_map(|(key, work)| {
                let (priority, rank) = priority_for(&work)?;
                (key.priority() != priority || key.rank() != rank)
                    .then(|| (key, key.with_priority_rank(priority, rank)))
            })
            .collect();
        let changed = rekeyed.len();
        for (old, new) in rekeyed {
            self.rekey(old, new);
        }
        changed
    }

    // Keeps `message_id` as the id every queued copy of the segment that
    // carries none of its own is fetched by. Called before the part's id in
    // use moves on, so a copy queued under the old id is still fetched by it.
    pub(crate) fn pin_message_id(
        &mut self,
        segment_id: SegmentId,
        message_id: impl Fn() -> MessageId,
    ) {
        let Some(slots) = self.files.get(&segment_id.file_id) else {
            return;
        };
        let mut keys = Vec::new();
        slots.keys_of(segment_id.segment_number, &mut keys);
        for key in keys {
            let Some(entry) = self.order.get_mut(&key) else {
                continue;
            };
            match entry.extra() {
                Some(index) => {
                    if let Some(extra) = self.extras.slots.get_mut(index).and_then(Option::as_mut)
                        && extra.message_id.is_none()
                    {
                        extra.message_id = Some(message_id());
                    }
                }
                None => {
                    let index = self.extras.insert(Extra {
                        job_id: None,
                        message_id: Some(message_id()),
                        groups: None,
                        counters: None,
                        retry_count: 0,
                        exclude_servers: Vec::new(),
                        avoid_server: None,
                    });
                    entry.meta |= (index + 1) << 1;
                }
            }
        }
    }
}

impl Default for DownloadQueue {
    fn default() -> Self {
        Self::new()
    }
}

// The work dispatch makes of an item, for tests that take items off a queue
// by hand.
#[cfg(test)]
impl QueueItem {
    pub(crate) fn dispatched(self, spec: &JobSpec) -> DownloadWork {
        self.into_work(spec).expect("a queued article of the job")
    }
}

#[cfg(test)]
impl crate::pipeline::Pipeline {
    pub(crate) fn dispatched(&self, item: QueueItem) -> DownloadWork {
        let spec = &self
            .jobs
            .get(&item.segment_id.file_id.job_id)
            .expect("the item's job")
            .spec;
        item.dispatched(spec)
    }

    pub(crate) fn dispatched_all(
        &self,
        items: impl IntoIterator<Item = QueueItem>,
    ) -> Vec<DownloadWork> {
        items
            .into_iter()
            .map(|item| self.dispatched(item))
            .collect()
    }
}

#[cfg(test)]
mod reference;

#[cfg(test)]
mod tests;
