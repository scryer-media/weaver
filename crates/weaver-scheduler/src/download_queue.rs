use std::cmp::Reverse;
use std::collections::BinaryHeap;

use weaver_core::id::{JobId, MessageId, SegmentId};

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
    /// Servers to skip for this download (e.g. after decode failure from that server).
    pub exclude_servers: Vec<usize>,
}

/// Wrapper that implements ordering for the priority queue.
/// Lower priority number = higher scheduling priority (downloaded first).
struct PrioritizedWork {
    priority: u32,
    /// Tie-breaker: insertion order (lower = earlier).
    sequence: u64,
    work: DownloadWork,
}

impl PartialEq for PrioritizedWork {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority && self.sequence == other.sequence
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
        self.priority
            .cmp(&other.priority)
            .then(self.sequence.cmp(&other.sequence))
    }
}

/// Priority queue for download work items.
pub struct DownloadQueue {
    heap: BinaryHeap<Reverse<PrioritizedWork>>,
    next_sequence: u64,
}

impl DownloadQueue {
    pub fn new() -> Self {
        Self {
            heap: BinaryHeap::new(),
            next_sequence: 0,
        }
    }

    pub fn push(&mut self, work: DownloadWork) {
        let priority = work.priority;
        let sequence = self.next_sequence;
        self.next_sequence += 1;
        self.heap.push(Reverse(PrioritizedWork {
            priority,
            sequence,
            work,
        }));
    }

    pub fn pop(&mut self) -> Option<DownloadWork> {
        self.heap.pop().map(|Reverse(pw)| pw.work)
    }

    pub fn len(&self) -> usize {
        self.heap.len()
    }

    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    /// Remove and return all queued segments.
    pub fn drain_all(&mut self) -> Vec<DownloadWork> {
        self.heap.drain().map(|Reverse(pw)| pw.work).collect()
    }

    /// Remove all queued segments for a given job.
    pub fn remove_job(&mut self, job_id: JobId) {
        let items: Vec<_> = self.heap.drain().collect();
        for item in items {
            if item.0.work.segment_id.file_id.job_id != job_id {
                self.heap.push(item);
            }
        }
    }

    /// Remove and return all queued segments for a given job.
    pub fn drain_job(&mut self, job_id: JobId) -> Vec<DownloadWork> {
        let items: Vec<_> = self.heap.drain().collect();
        let mut drained = Vec::new();
        for item in items {
            if item.0.work.segment_id.file_id.job_id == job_id {
                drained.push(item.0.work);
            } else {
                self.heap.push(item);
            }
        }
        drained
    }

    /// Boost priority of all segments for a given job (used when damage detected
    /// to prioritize downloading PAR2 recovery blocks).
    pub fn reprioritize_job(&mut self, job_id: JobId, new_priority_base: u32) {
        let items: Vec<_> = self.heap.drain().collect();
        for Reverse(mut pw) in items {
            if pw.work.segment_id.file_id.job_id == job_id {
                pw.priority = new_priority_base;
                pw.work.priority = new_priority_base;
            }
            self.heap.push(Reverse(pw));
        }
    }
}

impl Default for DownloadQueue {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use weaver_core::id::{JobId, MessageId, NzbFileId, SegmentId};

    use super::*;

    fn make_work(job_id: u64, file_index: u32, seg: u32, priority: u32) -> DownloadWork {
        DownloadWork {
            segment_id: SegmentId {
                file_id: NzbFileId {
                    job_id: JobId(job_id),
                    file_index,
                },
                segment_number: seg,
            },
            message_id: MessageId::new(&format!("msg-{job_id}-{file_index}-{seg}@example.com")),
            groups: vec!["alt.binaries.test".into()],
            priority,
            byte_estimate: 768_000,
            retry_count: 0,
            is_recovery: false,
            exclude_servers: vec![],
        }
    }

    #[test]
    fn empty_queue() {
        let mut q = DownloadQueue::new();
        assert!(q.is_empty());
        assert_eq!(q.len(), 0);
        assert!(q.pop().is_none());
    }

    #[test]
    fn priority_ordering() {
        let mut q = DownloadQueue::new();
        q.push(make_work(1, 0, 0, 100));
        q.push(make_work(1, 0, 1, 10));
        q.push(make_work(1, 0, 2, 50));

        let first = q.pop().unwrap();
        assert_eq!(first.priority, 10);

        let second = q.pop().unwrap();
        assert_eq!(second.priority, 50);

        let third = q.pop().unwrap();
        assert_eq!(third.priority, 100);

        assert!(q.pop().is_none());
    }

    #[test]
    fn reprioritize_job() {
        let mut q = DownloadQueue::new();
        // Job 1 segments at priority 1000 (PAR2 recovery, normally low priority).
        q.push(make_work(1, 0, 0, 1000));
        q.push(make_work(1, 0, 1, 1000));
        // Job 2 segment at priority 10 (RAR volume).
        q.push(make_work(2, 0, 0, 10));

        // Boost job 1 to priority 1 (damage detected, need recovery blocks).
        q.reprioritize_job(JobId(1), 1);

        // Job 1 segments should now come out first.
        let first = q.pop().unwrap();
        assert_eq!(first.segment_id.file_id.job_id, JobId(1));
        assert_eq!(first.priority, 1);

        let second = q.pop().unwrap();
        assert_eq!(second.segment_id.file_id.job_id, JobId(1));
        assert_eq!(second.priority, 1);

        // Job 2 last.
        let third = q.pop().unwrap();
        assert_eq!(third.segment_id.file_id.job_id, JobId(2));
        assert_eq!(third.priority, 10);
    }

    #[test]
    fn mixed_priorities() {
        let mut q = DownloadQueue::new();

        // PAR2 index file: priority 0.
        q.push(make_work(1, 0, 0, 0));
        // First RAR volume: priority 1.
        q.push(make_work(1, 1, 0, 1));
        // Second RAR volume: priority 11.
        q.push(make_work(1, 2, 0, 11));
        // PAR2 recovery: priority 1000.
        q.push(make_work(1, 3, 0, 1000));

        let items: Vec<_> = std::iter::from_fn(|| q.pop()).collect();
        assert_eq!(items.len(), 4);
        assert_eq!(items[0].priority, 0); // PAR2 index
        assert_eq!(items[1].priority, 1); // First RAR
        assert_eq!(items[2].priority, 11); // Second RAR
        assert_eq!(items[3].priority, 1000); // PAR2 recovery
    }

    #[test]
    fn remove_job() {
        let mut q = DownloadQueue::new();
        q.push(make_work(1, 0, 0, 10));
        q.push(make_work(1, 0, 1, 10));
        q.push(make_work(2, 0, 0, 10));
        q.push(make_work(1, 1, 0, 10));
        assert_eq!(q.len(), 4);

        q.remove_job(JobId(1));
        assert_eq!(q.len(), 1);

        let remaining = q.pop().unwrap();
        assert_eq!(remaining.segment_id.file_id.job_id, JobId(2));
        assert!(q.is_empty());
    }
}
