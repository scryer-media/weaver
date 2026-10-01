use std::collections::{BTreeMap, HashMap};

use crate::jobs::ids::{MessageId, NzbFileId, SegmentId};

/// A work item representing a segment to download.
#[derive(Clone)]
pub struct DownloadWork {
    pub segment_id: SegmentId,
    pub message_id: MessageId,
    /// The file's newsgroups, shared by every segment of the file rather than
    /// cloned per segment: a job's queue used to carry one `Vec<String>` per
    /// article, and every lease and refill cloned it again.
    pub groups: std::sync::Arc<[String]>,
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
    /// retry lands elsewhere when an alternative exists. Unlike `exclude_servers`, it
    /// never counts toward article-not-found exhaustion, so one transient
    /// timeout can never help declare an article missing. Replaced (not
    /// accumulated) on each transport failure; advisory only, so an index left
    /// stale by a pool rebuild merely skips one server for one attempt.
    pub avoid_server: Option<usize>,
}

/// Dispatch classes, in the order the queue serves them:
///
/// 0. completion-critical work that is not recovery — PAR2 index bootstrap,
///    metadata discovery, the direct-store identity probe wave;
/// 1. promoted PAR2 recovery blocks, which share the payload's connections and
///    lead it, because a repair cannot start until they land;
/// 2. first articles — one article per file, marked by the owner of the queue.
///    They lead the payload so that the first round trips of a job sample
///    every file in it rather than the first file in it, which is what makes
///    "this post is not there any more" answerable in seconds instead of
///    after a whole file's worth of articles;
/// 3. ordinary payload.
///
/// Classes 0 and 1 live in the completion-critical map and 2 and 3 in the
/// ordinary one, so the split is what `pop` reads; the rank orders the classes
/// inside each map ahead of the per-file priority.
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

/// The dispatch key of one queued item: the queue serves keys in ascending
/// order. Lower priority number = higher scheduling priority (downloaded
/// first). The sequence is unique within a queue, so no two items share a key.
#[derive(Clone, Copy)]
struct QueueKey {
    /// Dispatch class: see [`completion_rank_for`].
    completion_rank: u8,
    priority: u32,
    /// Optional intra-priority rank for deterministic dynamic ordering.
    rank: Option<u32>,
    /// Tie-breaker: insertion order (lower = earlier).
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

/// One dispatch class, served in ascending key order.
///
/// An ordered map rather than a heap so that a matching read can walk the
/// class in dispatch order and stop at the first hit, and a removal takes out
/// only the matched entry: skipping an ineligible head costs a read, not a
/// pop and re-push of everything ahead of the match.
type ClassMap = BTreeMap<QueueKey, DownloadWork>;

/// Priority queue for download work items.
pub struct DownloadQueue {
    completion_critical_work: ClassMap,
    ordinary_work: ClassMap,
    next_sequence: u64,
    /// Queued items carrying failure exclusions — escalated work that may
    /// need a backfill lane. Maintained on push/pop; recounted on the rare
    /// bulk-removal paths.
    excluded_work: usize,
    /// Queued recovery items. Kept explicitly so scheduler admission checks
    /// stay O(1) even for jobs with very large article queues.
    recovery_work: usize,
    /// Queued items per file, maintained on every push and removal. The RAR
    /// unlock planner asks "does this file still have queued work" once per
    /// volume; answering that by scanning the queue made every replan cost
    /// files times queued segments.
    queued_by_file: HashMap<NzbFileId, u32>,
    /// Queued byte estimates per file, as estimate -> how many queued items
    /// carry it, maintained beside `queued_by_file`. Lets a caller ask for a
    /// file's smallest queued article without reading the file's articles.
    queued_estimates_by_file: HashMap<NzbFileId, BTreeMap<u32, u32>>,
    /// The file priority plan currently in force: `(priority, rank)` per
    /// file, applied to every push of an unprotected item and re-applied to
    /// the queue only when the plan itself changes.
    ///
    /// A requeued retry used to lose its rank (a fresh push carries none) and
    /// force a full rebuild to get it back. With the plan held here a
    /// push lands at the right key immediately, so a retry never triggers a
    /// rebuild and an unchanged plan is a no-op.
    file_priority_plan: HashMap<NzbFileId, (u32, Option<u32>)>,
    /// Items at or below this priority are never touched by the file plan.
    file_priority_plan_protected: u32,
    /// Set whenever queued keys are rewritten outside the plan (direct-store
    /// volume binding, chase gating), so that re-installing an unchanged plan
    /// still re-asserts it over those keys, as a rebuild always did.
    file_priority_plan_stale: bool,
    /// One article per file — the lowest ordinal each file has. Held here
    /// rather than on the work item so that every path that puts an article
    /// back, including a requeue after a lane gave one up, lands it in the
    /// leading class again without having to know it was a first article.
    first_articles: std::collections::HashSet<SegmentId>,
}

impl DownloadQueue {
    pub fn new() -> Self {
        Self {
            completion_critical_work: ClassMap::new(),
            ordinary_work: ClassMap::new(),
            next_sequence: 0,
            excluded_work: 0,
            recovery_work: 0,
            queued_by_file: HashMap::new(),
            queued_estimates_by_file: HashMap::new(),
            file_priority_plan: HashMap::new(),
            file_priority_plan_protected: 0,
            file_priority_plan_stale: false,
            first_articles: std::collections::HashSet::new(),
        }
    }

    /// Marks an article as its file's first, which is the class it is served
    /// in from now on, however often it is requeued.
    pub fn note_first_article(&mut self, segment_id: SegmentId) {
        self.first_articles.insert(segment_id);
    }

    pub fn is_first_article(&self, segment_id: SegmentId) -> bool {
        self.first_articles.contains(&segment_id)
    }

    /// Every article this queue holds as a first article, whether or not it is
    /// still queued.
    pub fn first_articles(&self) -> impl Iterator<Item = SegmentId> + '_ {
        self.first_articles.iter().copied()
    }

    pub fn push(&mut self, work: DownloadWork) {
        let mut work = work;
        let mut rank = None;
        if work.priority > self.file_priority_plan_protected
            && let Some((priority, plan_rank)) = self
                .file_priority_plan
                .get(&work.segment_id.file_id)
                .copied()
        {
            work.priority = priority;
            rank = plan_rank;
        }
        let priority = work.priority;
        let sequence = self.next_sequence;
        self.next_sequence += 1;
        if !work.exclude_servers.is_empty() {
            self.excluded_work += 1;
        }
        if work.is_recovery {
            self.recovery_work += 1;
        }
        *self
            .queued_by_file
            .entry(work.segment_id.file_id)
            .or_default() += 1;
        *self
            .queued_estimates_by_file
            .entry(work.segment_id.file_id)
            .or_default()
            .entry(work.byte_estimate)
            .or_default() += 1;
        let completion_critical = work.completion_critical;
        let first_article = self.first_articles.contains(&work.segment_id);
        let key = QueueKey {
            completion_rank: completion_rank_for(&work, first_article),
            priority,
            rank,
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

    fn recount_derived_counts(&mut self) {
        self.excluded_work = self
            .iter()
            .filter(|work| !work.exclude_servers.is_empty())
            .count();
        self.recovery_work = self.iter().filter(|work| work.is_recovery).count();
        let mut queued_by_file = HashMap::new();
        let mut queued_estimates_by_file: HashMap<NzbFileId, BTreeMap<u32, u32>> = HashMap::new();
        for work in self.iter() {
            *queued_by_file.entry(work.segment_id.file_id).or_default() += 1;
            *queued_estimates_by_file
                .entry(work.segment_id.file_id)
                .or_default()
                .entry(work.byte_estimate)
                .or_default() += 1;
        }
        self.queued_by_file = queued_by_file;
        self.queued_estimates_by_file = queued_estimates_by_file;
    }

    /// Queued items for one file, in O(1).
    pub fn queued_count_for_file(&self, file_id: NzbFileId) -> u32 {
        self.queued_by_file.get(&file_id).copied().unwrap_or(0)
    }

    /// The smallest `byte_estimate` among one file's queued items, in
    /// O(log n); `None` when the file has nothing queued.
    pub fn min_queued_byte_estimate_for_file(&self, file_id: NzbFileId) -> Option<u32> {
        self.queued_estimates_by_file
            .get(&file_id)
            .and_then(|estimates| estimates.first_key_value())
            .map(|(estimate, _)| *estimate)
    }

    /// Every queued item, completion-critical class first, each class in
    /// dispatch order.
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

    /// Removes the highest-priority item matching `matches`, even when another
    /// work class currently owns the head. The classes are walked in dispatch
    /// order until the first match, so the cost is O(log n) plus one predicate
    /// call per item skipped ahead of the match; the skipped items are only
    /// read, never moved. With an eligible head it is the same O(log n) as
    /// [`Self::pop`].
    /// Every dispatch takes this path; the head-only paths above are not
    /// used by dispatch.
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
        if !work.exclude_servers.is_empty() {
            self.excluded_work = self.excluded_work.saturating_sub(1);
        }
        if work.is_recovery {
            self.recovery_work = self.recovery_work.saturating_sub(1);
        }
        if let Some(count) = self.queued_by_file.get_mut(&work.segment_id.file_id) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                self.queued_by_file.remove(&work.segment_id.file_id);
            }
        }
        if let Some(estimates) = self
            .queued_estimates_by_file
            .get_mut(&work.segment_id.file_id)
        {
            if let Some(count) = estimates.get_mut(&work.byte_estimate) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    estimates.remove(&work.byte_estimate);
                }
            }
            if estimates.is_empty() {
                self.queued_estimates_by_file
                    .remove(&work.segment_id.file_id);
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

    /// Read the same candidate as `pop_first_matching`, including work hidden
    /// behind an ineligible head: the classes are walked in dispatch order and
    /// the walk stops at the first match, so an eligible head is O(log n).
    pub fn peek_first_matching(
        &self,
        mut matches: impl FnMut(&DownloadWork) -> bool,
    ) -> Option<&DownloadWork> {
        self.iter().find(|work| matches(work))
    }

    /// The **highest-numbered** queued segment of the matching work, ignoring
    /// dispatch priority.
    ///
    /// The one caller is the direct-store header probe over a 7z set, whose map
    /// lives in the last bytes of the last volume. It cannot ask for "the
    /// article covering offset X": a yEnc article's byte range is only known
    /// once it has been decoded, and the NZB's own `bytes=` is an *encoded*
    /// size. Segment order is the only ordering that exists before a byte
    /// lands, and because a landed article leaves the queue, asking for the
    /// highest one still queued walks backwards from the tail on its own.
    ///
    /// Segment order is not dispatch order — a requeued retry or a file's
    /// first article sits elsewhere in the key order than its segment number
    /// says — so this is a scan of every queued item, not a walk from the end
    /// of the dispatch order.
    pub fn peek_last_matching(
        &self,
        mut matches: impl FnMut(&DownloadWork) -> bool,
    ) -> Option<&DownloadWork> {
        self.iter()
            .filter(|work| matches(work))
            .max_by_key(|work| work.segment_id.segment_number)
    }

    /// The head of one dispatch class without removing it, in O(log n).
    ///
    /// For decisions that are about the *shape* of the work rather than the
    /// work itself — which newsgroups a connection for this job would have to
    /// be opened for, ahead of any lease being cut.
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

    /// Queued items in one dispatch class, in O(1).
    ///
    /// Lease sizing divides the remaining work of the class it is about to
    /// lease from, so it must never pay for a queue scan: this is a map
    /// length, read once per lease.
    pub fn len_in_class(&self, completion_critical: bool) -> usize {
        self.class(completion_critical).len()
    }

    pub fn has_recovery_work(&self) -> bool {
        self.recovery_work > 0
    }

    pub fn count_matching(&self, mut predicate: impl FnMut(&DownloadWork) -> bool) -> usize {
        self.iter().filter(|work| predicate(work)).count()
    }

    /// Removes and returns every queued item matching the predicate, leaving
    /// the rest queued in place.
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

    /// Adds every queued segment id to `out`.
    ///
    /// For callers that build work items from a spec rather than from the
    /// queue and so must not re-queue an article the queue already owns —
    /// pushing a second copy would download it twice.
    pub fn extend_segment_ids(&self, out: &mut std::collections::HashSet<SegmentId>) {
        out.extend(self.iter().map(|work| work.segment_id));
    }

    pub fn has_completion_critical_work(&self) -> bool {
        !self.completion_critical_work.is_empty()
    }

    pub fn has_noncritical_work(&self) -> bool {
        !self.ordinary_work.is_empty()
    }

    /// Remove and return all queued segments.
    pub fn drain_all(&mut self) -> Vec<DownloadWork> {
        self.excluded_work = 0;
        self.recovery_work = 0;
        self.queued_by_file.clear();
        self.queued_estimates_by_file.clear();
        std::mem::take(&mut self.completion_critical_work)
            .into_values()
            .chain(std::mem::take(&mut self.ordinary_work).into_values())
            .collect()
    }

    /// Installs a per-file `(priority, rank)` plan and applies it to the queued
    /// work, leaving items at or below `protected` untouched. Returns how many
    /// queued items changed key.
    ///
    /// The plan persists: every later push of an unprotected item for a planned
    /// file lands at the planned key, so a requeued retry keeps its rank without
    /// a rebuild. Re-installing an identical plan is a no-op — queued keys are
    /// only re-keyed when the plan differs from the one in force,
    /// which is what keeps a burst of retry requeues from costing a re-key pass
    /// each.
    pub fn install_file_priority_plan(
        &mut self,
        plan: HashMap<NzbFileId, (u32, Option<u32>)>,
        protected: u32,
    ) -> usize {
        if !self.file_priority_plan_stale
            && plan == self.file_priority_plan
            && protected == self.file_priority_plan_protected
        {
            return 0;
        }
        self.file_priority_plan = plan;
        self.file_priority_plan_protected = protected;
        if self.file_priority_plan.is_empty() {
            // Nothing to apply: queued items keep the keys they have, exactly
            // as a rebuild with an empty plan would leave them.
            return 0;
        }
        let protected = self.file_priority_plan_protected;
        let plan = std::mem::take(&mut self.file_priority_plan);
        let changed = self.reprioritize_matching_with_rank(|work| {
            if work.priority <= protected {
                return None;
            }
            plan.get(&work.segment_id.file_id).copied()
        });
        self.file_priority_plan = plan;
        self.file_priority_plan_stale = false;
        changed
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
        self.file_priority_plan_stale = true;
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

    /// Moves selected work into the completion-critical class while applying
    /// its priority and optional intra-priority rank.
    pub fn promote_matching_to_completion_critical_with_rank(
        &mut self,
        mut priority_for: impl FnMut(&DownloadWork) -> Option<(u32, Option<u32>)>,
    ) -> usize {
        // Every selected item is taken out of whichever class holds it and put
        // back into the completion-critical one under its new key, keeping
        // its sequence; everything else stays where it is.
        let mut promoted = 0;
        for completion_critical in [true, false] {
            let selected: Vec<_> = self
                .class(completion_critical)
                .iter()
                .filter_map(|(key, work)| priority_for(work).map(|pick| (*key, pick)))
                .collect();
            promoted += selected.len();
            for (key, (priority, rank)) in selected {
                let Some(mut work) = self.class_mut(completion_critical).remove(&key) else {
                    continue;
                };
                work.priority = priority;
                work.completion_critical = true;
                let first_article = self.first_articles.contains(&work.segment_id);
                let key = QueueKey {
                    completion_rank: completion_rank_for(&work, first_article),
                    priority,
                    rank,
                    sequence: key.sequence,
                };
                self.completion_critical_work.insert(key, work);
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
