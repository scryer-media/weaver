use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::time::{Duration, Instant};

use super::queue::DownloadWork;

/// Configuration for retry backoff.
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retries before permanent failure.
    pub max_retries: u32,
    /// Base delay for first retry.
    pub base_delay: Duration,
    /// Multiplier applied for each subsequent retry.
    pub multiplier: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            base_delay: Duration::from_secs(1),
            multiplier: 5.0,
        }
    }
}

impl RetryConfig {
    /// Calculate delay for a given attempt number (0-indexed).
    pub fn delay_for_attempt(&self, attempt: u32) -> Duration {
        let secs = self.base_delay.as_secs_f64() * self.multiplier.powi(attempt as i32);
        Duration::from_secs_f64(secs.min(30.0))
    }
}

/// A segment waiting to be retried after backoff.
pub struct RetryEntry {
    pub work: DownloadWork,
    pub retry_count: u32,
    pub ready_at: Instant,
}

impl PartialEq for RetryEntry {
    fn eq(&self, other: &Self) -> bool {
        self.ready_at == other.ready_at
    }
}

impl Eq for RetryEntry {}

impl PartialOrd for RetryEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RetryEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.ready_at.cmp(&other.ready_at)
    }
}

/// Queue of segments waiting for retry with exponential backoff.
///
/// Backed by a min-heap ordered by `ready_at` — the earliest-ready entry
/// is always at the front.
pub struct RetryQueue {
    heap: BinaryHeap<Reverse<RetryEntry>>,
    config: RetryConfig,
}

impl RetryQueue {
    pub fn new(config: RetryConfig) -> Self {
        Self {
            heap: BinaryHeap::new(),
            config,
        }
    }

    /// Schedule a segment for retry. Returns `false` if max retries exceeded.
    pub fn schedule(&mut self, mut work: DownloadWork, current_retry_count: u32) -> bool {
        if current_retry_count >= self.config.max_retries {
            return false;
        }
        let delay = self.config.delay_for_attempt(current_retry_count);
        work.retry_count = current_retry_count + 1;
        let entry = RetryEntry {
            work,
            retry_count: current_retry_count + 1,
            ready_at: Instant::now() + delay,
        };
        self.heap.push(Reverse(entry));
        true
    }

    /// Drain all entries whose `ready_at` has passed.
    pub fn drain_ready(&mut self) -> Vec<RetryEntry> {
        let now = Instant::now();
        let mut ready = Vec::new();
        while let Some(Reverse(entry)) = self.heap.peek() {
            if entry.ready_at <= now {
                let Reverse(entry) = self.heap.pop().unwrap();
                ready.push(entry);
            } else {
                break;
            }
        }
        ready
    }

    /// Time of the next entry becoming ready, or `None` if empty.
    pub fn next_ready_at(&self) -> Option<Instant> {
        self.heap.peek().map(|Reverse(e)| e.ready_at)
    }

    pub fn len(&self) -> usize {
        self.heap.len()
    }

    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }
}

#[cfg(test)]
mod tests;
