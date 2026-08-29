use std::cmp::Reverse;
use std::collections::BinaryHeap;

use crate::jobs::ids::{MessageId, SegmentId};

/// A work item representing a segment to download.
pub struct DownloadWork {
    pub segment_id: SegmentId,
    pub message_id: MessageId,
    pub groups: Vec<String>,
    pub priority: u32,
    pub byte_estimate: u32,
    pub retry_count: u32,
    /// Whether this segment belongs to a recovery file (PAR2 repair blocks).
    pub is_recovery: bool,
    /// Whether pipeline progress is explicitly waiting for this segment.
    ///
    /// This is deliberately orthogonal to `is_recovery`: PAR2 completion work
    /// and the bounded direct-store identity probe wave both need to lead the
    /// ordinary queue. The flag changes dispatch eligibility only.
    pub completion_critical: bool,
    /// Servers to skip for this download (e.g. after decode failure from that server).
    pub exclude_servers: Vec<usize>,
    /// Transport-rotation hint: the server whose established connection just
    /// failed for this segment. Selection avoids it on the next attempt so the
    /// retry lands elsewhere when an alternative exists — mirroring
    /// NZBGet's per-article `failedServers` — but unlike `exclude_servers` it
    /// never counts toward article-not-found exhaustion, so one transient
    /// timeout can never help declare an article missing. Replaced (not
    /// accumulated) on each transport failure; advisory only, so an index left
    /// stale by a pool rebuild merely skips one server for one attempt.
    pub avoid_server: Option<usize>,
}

/// Wrapper that implements ordering for the priority queue.
/// Lower priority number = higher scheduling priority (downloaded first).
struct PrioritizedWork {
    /// Completion-critical PAR2 work sorts ahead of ordinary queue priority.
    completion_rank: u8,
    priority: u32,
    /// Optional intra-priority rank for deterministic dynamic ordering.
    rank: Option<u32>,
    /// Tie-breaker: insertion order (lower = earlier).
    sequence: u64,
    work: DownloadWork,
}

impl PartialEq for PrioritizedWork {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority
            && self.completion_rank == other.completion_rank
            && self.rank == other.rank
            && self.sequence == other.sequence
    }
}

impl Eq for PrioritizedWork {}

impl PartialOrd for PrioritizedWork {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PrioritizedWork {
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

/// Priority queue for download work items.
pub struct DownloadQueue {
    completion_critical_heap: BinaryHeap<Reverse<PrioritizedWork>>,
    ordinary_heap: BinaryHeap<Reverse<PrioritizedWork>>,
    next_sequence: u64,
    /// Queued items carrying failure exclusions — escalated work that may
    /// need a backfill lane. Maintained on push/pop; recounted on the rare
    /// bulk-removal paths.
    excluded_work: usize,
    /// Queued recovery items. Kept explicitly so scheduler admission checks
    /// stay O(1) even for jobs with very large article queues.
    recovery_work: usize,
}

impl DownloadQueue {
    pub fn new() -> Self {
        Self {
            completion_critical_heap: BinaryHeap::new(),
            ordinary_heap: BinaryHeap::new(),
            next_sequence: 0,
            excluded_work: 0,
            recovery_work: 0,
        }
    }

    pub fn push(&mut self, work: DownloadWork) {
        let priority = work.priority;
        let sequence = self.next_sequence;
        self.next_sequence += 1;
        if !work.exclude_servers.is_empty() {
            self.excluded_work += 1;
        }
        if work.is_recovery {
            self.recovery_work += 1;
        }
        let completion_critical = work.completion_critical;
        let item = Reverse(PrioritizedWork {
            completion_rank: u8::from(!work.completion_critical),
            priority,
            rank: None,
            sequence,
            work,
        });
        if completion_critical {
            self.completion_critical_heap.push(item);
        } else {
            self.ordinary_heap.push(item);
        }
    }

    pub fn pop(&mut self) -> Option<DownloadWork> {
        if self.completion_critical_heap.is_empty() {
            self.pop_from_class(false)
        } else {
            self.pop_from_class(true)
        }
    }

    fn pop_from_class(&mut self, completion_critical: bool) -> Option<DownloadWork> {
        let work = if completion_critical {
            self.completion_critical_heap.pop()
        } else {
            self.ordinary_heap.pop()
        }
        .map(|Reverse(pw)| pw.work);
        if let Some(work) = &work {
            self.note_removed(work);
        }
        work
    }

    /// Number of queued items with failure exclusions.
    pub fn excluded_work_count(&self) -> usize {
        self.excluded_work
    }

    /// Drop failure exclusions from all queued work. Used when the server
    /// config is rebuilt: exclusion indices refer to the old pool layout and
    /// would mis-target (or spuriously exhaust) servers in the new one.
    pub fn clear_exclude_servers(&mut self) {
        if self.excluded_work == 0 {
            return;
        }
        for heap in [&mut self.completion_critical_heap, &mut self.ordinary_heap] {
            let items: Vec<_> = heap.drain().collect();
            for Reverse(mut pw) in items {
                pw.work.exclude_servers.clear();
                pw.work.avoid_server = None;
                heap.push(Reverse(pw));
            }
        }
        self.excluded_work = 0;
    }

    fn recount_derived_counts(&mut self) {
        self.excluded_work = self
            .iter()
            .filter(|item| !item.0.work.exclude_servers.is_empty())
            .count();
        self.recovery_work = self.iter().filter(|item| item.0.work.is_recovery).count();
    }

    fn iter(&self) -> impl Iterator<Item = &Reverse<PrioritizedWork>> {
        self.completion_critical_heap
            .iter()
            .chain(self.ordinary_heap.iter())
    }

    pub fn pop_next_matching(
        &mut self,
        mut matches: impl FnMut(&DownloadWork) -> bool,
    ) -> Option<DownloadWork> {
        let completion_critical = !self.completion_critical_heap.is_empty();
        self.heap_for_class(completion_critical)
            .peek()
            .is_some_and(|Reverse(pw)| matches(&pw.work))
            .then(|| self.pop_from_class(completion_critical))?
    }

    pub fn pop_next_matching_in_class(
        &mut self,
        completion_critical: bool,
        mut matches: impl FnMut(&DownloadWork) -> bool,
    ) -> Option<DownloadWork> {
        self.heap_for_class(completion_critical)
            .peek()
            .is_some_and(|Reverse(pw)| matches(&pw.work))
            .then(|| self.pop_from_class(completion_critical))?
    }

    /// Removes the highest-priority item matching `matches`, even when another
    /// work class currently owns the heap head. This intentionally takes the
    /// slower path and is reserved for class-constrained completion dispatch;
    /// ordinary dispatch continues to use the O(log n) heap-head path above.
    pub fn pop_first_matching(
        &mut self,
        mut matches: impl FnMut(&DownloadWork) -> bool,
    ) -> Option<DownloadWork> {
        if let Some(work) =
            Self::remove_first_matching_from_heap(&mut self.completion_critical_heap, &mut matches)
        {
            self.note_removed(&work);
            return Some(work);
        }
        let work = Self::remove_first_matching_from_heap(&mut self.ordinary_heap, &mut matches)?;
        self.note_removed(&work);
        Some(work)
    }

    pub fn pop_first_matching_in_class(
        &mut self,
        completion_critical: bool,
        mut matches: impl FnMut(&DownloadWork) -> bool,
    ) -> Option<DownloadWork> {
        let work = Self::remove_first_matching_from_heap(
            self.heap_for_class_mut(completion_critical),
            &mut matches,
        )?;
        self.note_removed(&work);
        Some(work)
    }

    fn remove_first_matching_from_heap(
        heap: &mut BinaryHeap<Reverse<PrioritizedWork>>,
        matches: &mut impl FnMut(&DownloadWork) -> bool,
    ) -> Option<DownloadWork> {
        let mut skipped = Vec::new();
        let matched = loop {
            let Some(Reverse(item)) = heap.pop() else {
                break None;
            };
            if matches(&item.work) {
                break Some(item.work);
            }
            skipped.push(Reverse(item));
        };
        heap.extend(skipped);
        matched
    }

    fn note_removed(&mut self, work: &DownloadWork) {
        if !work.exclude_servers.is_empty() {
            self.excluded_work = self.excluded_work.saturating_sub(1);
        }
        if work.is_recovery {
            self.recovery_work = self.recovery_work.saturating_sub(1);
        }
    }

    fn heap_for_class(&self, completion_critical: bool) -> &BinaryHeap<Reverse<PrioritizedWork>> {
        if completion_critical {
            &self.completion_critical_heap
        } else {
            &self.ordinary_heap
        }
    }

    fn heap_for_class_mut(
        &mut self,
        completion_critical: bool,
    ) -> &mut BinaryHeap<Reverse<PrioritizedWork>> {
        if completion_critical {
            &mut self.completion_critical_heap
        } else {
            &mut self.ordinary_heap
        }
    }

    pub fn peek_next_matching(
        &self,
        mut matches: impl FnMut(&DownloadWork) -> bool,
    ) -> Option<&DownloadWork> {
        let completion_critical = !self.completion_critical_heap.is_empty();
        self.heap_for_class(completion_critical)
            .peek()
            .and_then(|Reverse(pw)| matches(&pw.work).then_some(&pw.work))
    }

    pub fn len(&self) -> usize {
        self.completion_critical_heap.len() + self.ordinary_heap.len()
    }

    pub fn is_empty(&self) -> bool {
        self.completion_critical_heap.is_empty() && self.ordinary_heap.is_empty()
    }

    pub fn has_recovery_work(&self) -> bool {
        self.recovery_work > 0
    }

    pub fn count_matching(&self, mut predicate: impl FnMut(&DownloadWork) -> bool) -> usize {
        self.iter().filter(|item| predicate(&item.0.work)).count()
    }

    /// Removes and returns every queued item matching the predicate, leaving
    /// the rest queued in place.
    pub fn extract_matching(
        &mut self,
        mut predicate: impl FnMut(&DownloadWork) -> bool,
    ) -> Vec<DownloadWork> {
        let mut extracted = Vec::new();
        for heap in [&mut self.completion_critical_heap, &mut self.ordinary_heap] {
            let items: Vec<_> = heap.drain().collect();
            for item in items {
                if predicate(&item.0.work) {
                    extracted.push(item.0.work);
                } else {
                    heap.push(item);
                }
            }
        }
        if !extracted.is_empty() {
            self.recount_derived_counts();
        }
        extracted
    }

    /// Adds every queued segment id to `out`.
    ///
    /// For callers that build work items from a spec rather than from the
    /// queue and so must not re-queue an article the queue already owns —
    /// pushing a second copy would download it twice.
    pub fn extend_segment_ids(&self, out: &mut std::collections::HashSet<SegmentId>) {
        out.extend(self.iter().map(|item| item.0.work.segment_id));
    }

    pub fn has_completion_critical_work(&self) -> bool {
        !self.completion_critical_heap.is_empty()
    }

    pub fn has_noncritical_work(&self) -> bool {
        !self.ordinary_heap.is_empty()
    }

    /// Remove and return all queued segments.
    pub fn drain_all(&mut self) -> Vec<DownloadWork> {
        self.excluded_work = 0;
        self.recovery_work = 0;
        self.completion_critical_heap
            .drain()
            .chain(self.ordinary_heap.drain())
            .map(|Reverse(pw)| pw.work)
            .collect()
    }

    /// Recompute priorities for selected queued work while preserving insertion
    /// order for work that ends up with the same priority.
    pub fn reprioritize_matching(
        &mut self,
        mut priority_for: impl FnMut(&DownloadWork) -> Option<u32>,
    ) -> usize {
        self.reprioritize_matching_with_rank(|work| {
            priority_for(work).map(|priority| (priority, None))
        })
    }

    /// Recompute priorities and optional intra-priority ranks for selected queued
    /// work. Unranked equal-priority work remains ordered by original insertion.
    pub fn reprioritize_matching_with_rank(
        &mut self,
        mut priority_for: impl FnMut(&DownloadWork) -> Option<(u32, Option<u32>)>,
    ) -> usize {
        let mut changed = 0;
        for heap in [&mut self.completion_critical_heap, &mut self.ordinary_heap] {
            let items: Vec<_> = heap.drain().collect();
            for Reverse(mut pw) in items {
                if let Some((priority, rank)) = priority_for(&pw.work)
                    && (pw.priority != priority || pw.rank != rank)
                {
                    pw.priority = priority;
                    pw.rank = rank;
                    pw.work.priority = priority;
                    changed += 1;
                }
                heap.push(Reverse(pw));
            }
        }
        changed
    }

    /// Moves selected work into the completion-critical class while applying
    /// its priority and optional intra-priority rank.
    pub fn promote_matching_to_completion_critical_with_rank(
        &mut self,
        mut priority_for: impl FnMut(&DownloadWork) -> Option<(u32, Option<u32>)>,
    ) -> usize {
        let items: Vec<_> = self
            .completion_critical_heap
            .drain()
            .chain(self.ordinary_heap.drain())
            .collect();
        let mut promoted = 0;
        for Reverse(mut pw) in items {
            if let Some((priority, rank)) = priority_for(&pw.work) {
                pw.completion_rank = 0;
                pw.priority = priority;
                pw.rank = rank;
                pw.work.priority = priority;
                pw.work.completion_critical = true;
                promoted += 1;
            }
            if pw.work.completion_critical {
                self.completion_critical_heap.push(Reverse(pw));
            } else {
                self.ordinary_heap.push(Reverse(pw));
            }
        }
        promoted
    }
}

impl Default for DownloadQueue {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests;
