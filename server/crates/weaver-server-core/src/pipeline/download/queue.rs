// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::{BTreeMap, HashMap};

use crate::jobs::ids::{MessageId, NzbFileId, SegmentId};

// A work item representing a segment to download.
#[derive(Clone)]
pub struct DownloadWork {
    // Attached at dispatch; queued articles do not allocate counters.
    pub counters: Option<std::sync::Arc<crate::pipeline::queue::JobCounters>>,
    pub segment_id: SegmentId,
    pub message_id: MessageId,
    // The file's newsgroups, shared by every segment of the file rather than
    // cloned per segment: a job's queue used to carry one `Vec<String>` per
    // article, and every lease and refill cloned it again.
    pub groups: std::sync::Arc<[String]>,
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
    // Whether the work is steered off a server: excluded from one or hinted
    // away from one.
    fn is_steered(&self) -> bool {
        !self.exclude_servers.is_empty() || self.avoid_server.is_some()
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
// Classes 0 and 1 live in the completion-critical map and 2 and 3 in the
// ordinary one, so the split is what `pop` reads; the rank orders the classes
// inside each map ahead of the per-file priority.
const COMPLETION_RANK_CRITICAL: u8 = 0;
const COMPLETION_RANK_PROMOTED_RECOVERY: u8 = 1;
const COMPLETION_RANK_FIRST_ARTICLE: u8 = 2;
const COMPLETION_RANK_ORDINARY: u8 = 3;

fn completion_rank_for(work: &DownloadWork, first_article: bool) -> u8 {
    if !work.completion_critical {
        if first_article {
            COMPLETION_RANK_FIRST_ARTICLE
        } else {
            COMPLETION_RANK_ORDINARY
        }
    } else if work.is_recovery {
        COMPLETION_RANK_PROMOTED_RECOVERY
    } else {
        COMPLETION_RANK_CRITICAL
    }
}

// The dispatch key of one queued item: the queue serves keys in ascending
// order. Lower priority number = higher scheduling priority (downloaded
// first). The sequence is unique within a queue, so no two items share a key.
#[derive(Clone, Copy)]
struct QueueKey {
    // Dispatch class: see [`completion_rank_for`].
    completion_rank: u8,
    priority: u32,
    // Optional intra-priority rank for deterministic dynamic ordering.
    rank: Option<u32>,
    // Tie-breaker: insertion order (lower = earlier).
    sequence: u64,
}

impl PartialEq for QueueKey {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl Eq for QueueKey {}

impl PartialOrd for QueueKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for QueueKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let self_rank = self.rank.unwrap_or(u32::MAX);
        let other_rank = other.rank.unwrap_or(u32::MAX);
        self.completion_rank
            .cmp(&other.completion_rank)
            .then(self.priority.cmp(&other.priority))
            .then(self_rank.cmp(&other_rank))
            .then(self.sequence.cmp(&other.sequence))
    }
}

// One dispatch class, served in ascending key order.
//
// An ordered map rather than a heap so that a matching read can walk the
// class in dispatch order and stop at the first hit, and a removal takes out
// only the matched entry: skipping an ineligible head costs a read, not a
// pop and re-push of everything ahead of the match.
type ClassMap = BTreeMap<QueueKey, DownloadWork>;

// Priority queue for download work items.
pub struct DownloadQueue {
    completion_critical_work: ClassMap,
    ordinary_work: ClassMap,
    next_sequence: u64,
    // Queued items steered off a server: failure exclusions (escalated work
    // that may need a backfill lane) or a rotation hint. Maintained on
    // push/pop; recounted on the rare bulk-removal paths.
    excluded_work: usize,
    // Queued items per file, maintained on every push and removal. The RAR
    // unlock planner asks "does this file still have queued work" once per
    // volume; answering that by scanning the queue made every replan cost
    // files times queued segments.
    queued_by_file: HashMap<NzbFileId, u32>,
    // One article per file — the lowest ordinal each file has. Held here
    // rather than on the work item so that every path that puts an article
    // back, including a requeue after a lane gave one up, lands it in the
    // leading class again without having to know it was a first article.
    first_articles: std::collections::HashSet<SegmentId>,
}

impl DownloadQueue {
    pub fn new() -> Self {
        Self {
            completion_critical_work: ClassMap::new(),
            ordinary_work: ClassMap::new(),
            next_sequence: 0,
            excluded_work: 0,
            queued_by_file: HashMap::new(),
            first_articles: std::collections::HashSet::new(),
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

    pub fn push(&mut self, work: DownloadWork) {
        let priority = work.priority;
        let sequence = self.next_sequence;
        self.next_sequence += 1;
        if work.is_steered() {
            self.excluded_work += 1;
        }
        *self
            .queued_by_file
            .entry(work.segment_id.file_id)
            .or_default() += 1;
        let completion_critical = work.completion_critical;
        let first_article = self.first_articles.contains(&work.segment_id);
        let key = QueueKey {
            completion_rank: completion_rank_for(&work, first_article),
            priority,
            rank: None,
            sequence,
        };
        self.class_mut(completion_critical).insert(key, work);
    }

    pub fn pop(&mut self) -> Option<DownloadWork> {
        if self.completion_critical_work.is_empty() {
            self.pop_from_class(false)
        } else {
            self.pop_from_class(true)
        }
    }

    fn pop_from_class(&mut self, completion_critical: bool) -> Option<DownloadWork> {
        let work = self
            .class_mut(completion_critical)
            .pop_first()
            .map(|(_, work)| work);
        if let Some(work) = &work {
            self.note_removed(work);
        }
        work
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
        // Neither field is part of the key, so the entries are edited in place.
        for work in self
            .completion_critical_work
            .values_mut()
            .chain(self.ordinary_work.values_mut())
        {
            work.exclude_servers.clear();
            work.avoid_server = None;
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
        // Not part of the key, so the entries are edited in place.
        for work in self
            .completion_critical_work
            .values_mut()
            .chain(self.ordinary_work.values_mut())
        {
            if work.avoid_server.take().is_some() {
                cleared += 1;
                if work.exclude_servers.is_empty() {
                    self.excluded_work = self.excluded_work.saturating_sub(1);
                }
            }
        }
        cleared
    }

    fn recount_derived_counts(&mut self) {
        self.excluded_work = self.iter().filter(|work| work.is_steered()).count();
        let mut queued_by_file = HashMap::new();
        for work in self.iter() {
            *queued_by_file.entry(work.segment_id.file_id).or_default() += 1;
        }
        self.queued_by_file = queued_by_file;
    }

    // Queued items for one file, in O(1).
    pub fn queued_count_for_file(&self, file_id: NzbFileId) -> u32 {
        self.queued_by_file.get(&file_id).copied().unwrap_or(0)
    }

    #[cfg(test)]
    pub fn count_matching(&self, mut predicate: impl FnMut(&DownloadWork) -> bool) -> usize {
        self.iter().filter(|work| predicate(work)).count()
    }

    #[cfg(test)]
    pub fn drain_all(&mut self) -> Vec<DownloadWork> {
        self.excluded_work = 0;
        self.queued_by_file.clear();
        std::mem::take(&mut self.completion_critical_work)
            .into_values()
            .chain(std::mem::take(&mut self.ordinary_work).into_values())
            .collect()
    }

    // Every queued item, completion-critical class first, each class in
    // dispatch order.
    fn iter(&self) -> impl Iterator<Item = &DownloadWork> {
        self.completion_critical_work
            .values()
            .chain(self.ordinary_work.values())
    }

    pub fn pop_next_matching(
        &mut self,
        mut matches: impl FnMut(&DownloadWork) -> bool,
    ) -> Option<DownloadWork> {
        let completion_critical = !self.completion_critical_work.is_empty();
        self.class(completion_critical)
            .first_key_value()
            .is_some_and(|(_, work)| matches(work))
            .then(|| self.pop_from_class(completion_critical))?
    }

    pub fn pop_next_matching_in_class(
        &mut self,
        completion_critical: bool,
        mut matches: impl FnMut(&DownloadWork) -> bool,
    ) -> Option<DownloadWork> {
        self.class(completion_critical)
            .first_key_value()
            .is_some_and(|(_, work)| matches(work))
            .then(|| self.pop_from_class(completion_critical))?
    }

    // Removes the highest-priority item matching `matches`, even when another
    // work class currently owns the head. The classes are walked in dispatch
    // order until the first match, so the cost is O(log n) plus one predicate
    // call per item skipped ahead of the match; the skipped items are only
    // read, never moved. With an eligible head it is the same O(log n) as
    // [`Self::pop`].
    // Every dispatch takes this path; the head-only paths above are not
    // used by dispatch.
    pub fn pop_first_matching(
        &mut self,
        mut matches: impl FnMut(&DownloadWork) -> bool,
    ) -> Option<DownloadWork> {
        if let Some(work) =
            Self::remove_first_matching_from_class(&mut self.completion_critical_work, &mut matches)
        {
            self.note_removed(&work);
            return Some(work);
        }
        let work = Self::remove_first_matching_from_class(&mut self.ordinary_work, &mut matches)?;
        self.note_removed(&work);
        Some(work)
    }

    pub fn pop_first_matching_in_class(
        &mut self,
        completion_critical: bool,
        mut matches: impl FnMut(&DownloadWork) -> bool,
    ) -> Option<DownloadWork> {
        let work = Self::remove_first_matching_from_class(
            self.class_mut(completion_critical),
            &mut matches,
        )?;
        self.note_removed(&work);
        Some(work)
    }

    fn remove_first_matching_from_class(
        class: &mut ClassMap,
        matches: &mut impl FnMut(&DownloadWork) -> bool,
    ) -> Option<DownloadWork> {
        let key = class
            .iter()
            .find_map(|(key, work)| matches(work).then_some(*key))?;
        class.remove(&key)
    }

    fn note_removed(&mut self, work: &DownloadWork) {
        if work.is_steered() {
            self.excluded_work = self.excluded_work.saturating_sub(1);
        }
        if let Some(count) = self.queued_by_file.get_mut(&work.segment_id.file_id) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                self.queued_by_file.remove(&work.segment_id.file_id);
            }
        }
    }

    fn class(&self, completion_critical: bool) -> &ClassMap {
        if completion_critical {
            &self.completion_critical_work
        } else {
            &self.ordinary_work
        }
    }

    fn class_mut(&mut self, completion_critical: bool) -> &mut ClassMap {
        if completion_critical {
            &mut self.completion_critical_work
        } else {
            &mut self.ordinary_work
        }
    }

    pub fn peek_next_matching(
        &self,
        mut matches: impl FnMut(&DownloadWork) -> bool,
    ) -> Option<&DownloadWork> {
        let completion_critical = !self.completion_critical_work.is_empty();
        self.class(completion_critical)
            .first_key_value()
            .and_then(|(_, work)| matches(work).then_some(work))
    }

    // Read the same candidate as `pop_first_matching`, including work hidden
    // behind an ineligible head: the classes are walked in dispatch order and
    // the walk stops at the first match, so an eligible head is O(log n).
    pub fn peek_first_matching(
        &self,
        mut matches: impl FnMut(&DownloadWork) -> bool,
    ) -> Option<&DownloadWork> {
        self.iter().find(|work| matches(work))
    }

    // The head of one dispatch class without removing it, in O(log n).
    //
    // For decisions that are about the *shape* of the work rather than the
    // work itself — which newsgroups a connection for this job would have to
    // be opened for, ahead of any lease being cut.
    pub fn peek_in_class(&self, completion_critical: bool) -> Option<&DownloadWork> {
        self.class(completion_critical)
            .first_key_value()
            .map(|(_, work)| work)
    }

    pub fn len(&self) -> usize {
        self.completion_critical_work.len() + self.ordinary_work.len()
    }

    pub fn is_empty(&self) -> bool {
        self.completion_critical_work.is_empty() && self.ordinary_work.is_empty()
    }

    // Queued items in one dispatch class, in O(1).
    //
    // Lease sizing divides the remaining work of the class it is about to
    // lease from, so it must never pay for a queue scan: this is a map
    // length, read once per lease.
    pub fn len_in_class(&self, completion_critical: bool) -> usize {
        self.class(completion_critical).len()
    }

    // Removes and returns every queued item matching the predicate, leaving
    // the rest queued in place.
    pub fn extract_matching(
        &mut self,
        mut predicate: impl FnMut(&DownloadWork) -> bool,
    ) -> Vec<DownloadWork> {
        let mut extracted = Vec::new();
        for class in [&mut self.completion_critical_work, &mut self.ordinary_work] {
            extracted.extend(
                class
                    .extract_if(.., |_, work| predicate(work))
                    .map(|(_, work)| work),
            );
        }
        if !extracted.is_empty() {
            self.recount_derived_counts();
        }
        extracted
    }

    // Adds every queued segment id to `out`.
    //
    // For callers that build work items from a spec rather than from the
    // queue and so must not re-queue an article the queue already owns —
    // pushing a second copy would download it twice.
    pub fn extend_segment_ids(&self, out: &mut std::collections::HashSet<SegmentId>) {
        out.extend(self.iter().map(|work| work.segment_id));
    }

    // Recompute priorities and optional intra-priority ranks for selected queued
    // work. Unranked equal-priority work remains ordered by original insertion.
    pub fn reprioritize_matching_with_rank(
        &mut self,
        mut priority_for: impl FnMut(&DownloadWork) -> Option<(u32, Option<u32>)>,
    ) -> usize {
        let mut changed = 0;
        for class in [&mut self.completion_critical_work, &mut self.ordinary_work] {
            // Only the items whose key changes move; each keeps its sequence,
            // so equal keys still serve in insertion order.
            let rekeyed: Vec<_> = class
                .iter()
                .filter_map(|(key, work)| {
                    let (priority, rank) = priority_for(work)?;
                    (key.priority != priority || key.rank != rank).then_some((
                        *key,
                        QueueKey {
                            priority,
                            rank,
                            ..*key
                        },
                    ))
                })
                .collect();
            changed += rekeyed.len();
            for (old, new) in rekeyed {
                if let Some(mut work) = class.remove(&old) {
                    work.priority = new.priority;
                    class.insert(new, work);
                }
            }
        }
        changed
    }
}

impl Default for DownloadQueue {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests;
