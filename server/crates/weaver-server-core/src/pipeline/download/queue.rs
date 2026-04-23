use std::cmp::Reverse;
use std::collections::BinaryHeap;

use crate::jobs::ids::{JobId, MessageId, SegmentId};

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

    pub fn has_recovery_work(&self) -> bool {
        self.heap.iter().any(|item| item.0.work.is_recovery)
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
mod tests;
