use std::collections::{BTreeMap, VecDeque};
use std::time::Duration;

use tokio::time::Instant;

/// Pipeline-owned storage for infrastructure retries.
///
/// Timed work is ordered by deadline without rescanning every parked item on
/// insertion. Work without a deadline remains parked until an explicit wake.
pub(super) struct InfrastructureRetryQueue<T> {
    timed: BTreeMap<Instant, VecDeque<T>>,
    indefinite: VecDeque<T>,
    len: usize,
}

impl<T> Default for InfrastructureRetryQueue<T> {
    fn default() -> Self {
        Self {
            timed: BTreeMap::new(),
            indefinite: VecDeque::new(),
            len: 0,
        }
    }
}

impl<T> InfrastructureRetryQueue<T> {
    pub(super) fn len(&self) -> usize {
        self.len
    }

    pub(super) fn schedule(&mut self, delay: Option<Duration>, work: T) {
        match delay {
            Some(delay) => self
                .timed
                .entry(Instant::now() + delay)
                .or_default()
                .push_back(work),
            None => self.indefinite.push_back(work),
        }
        self.len += 1;
    }

    pub(super) fn next_deadline(&self) -> Option<Instant> {
        self.timed.first_key_value().map(|(deadline, _)| *deadline)
    }

    pub(super) fn take_due(&mut self, now: Instant) -> Vec<T> {
        let mut due = Vec::new();
        while self
            .timed
            .first_key_value()
            .is_some_and(|(deadline, _)| *deadline <= now)
        {
            let (_, batch) = self.timed.pop_first().expect("first deadline was present");
            self.len = self.len.saturating_sub(batch.len());
            due.extend(batch);
        }
        due
    }

    pub(super) fn wake_all(&mut self) -> Vec<T> {
        let mut ready = Vec::with_capacity(self.len);
        for (_, batch) in std::mem::take(&mut self.timed) {
            ready.extend(batch);
        }
        ready.extend(self.indefinite.drain(..));
        self.len = 0;
        ready
    }

    pub(super) fn cancel_where(&mut self, mut predicate: impl FnMut(&T) -> bool) -> usize {
        let previous_len = self.len;
        for batch in self.timed.values_mut() {
            batch.retain(|work| !predicate(work));
        }
        self.timed.retain(|_, batch| !batch.is_empty());
        self.indefinite.retain(|work| !predicate(work));
        self.len = self.timed.values().map(VecDeque::len).sum::<usize>() + self.indefinite.len();
        previous_len.saturating_sub(self.len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deadlines_are_batched_and_indefinite_work_waits_for_wake() {
        let mut queue = InfrastructureRetryQueue::default();
        queue.schedule(Some(Duration::from_secs(2)), 2);
        queue.schedule(None, 3);
        queue.schedule(Some(Duration::ZERO), 1);

        assert_eq!(queue.take_due(Instant::now()), vec![1]);
        assert_eq!(queue.len(), 2);
        assert_eq!(queue.wake_all(), vec![2, 3]);
        assert_eq!(queue.len(), 0);
    }

    #[test]
    fn cancellation_removes_matching_work_from_all_deadlines() {
        let mut queue = InfrastructureRetryQueue::default();
        queue.schedule(Some(Duration::ZERO), (1, 1));
        queue.schedule(Some(Duration::from_secs(1)), (2, 2));
        queue.schedule(None, (1, 3));

        assert_eq!(queue.cancel_where(|(job, _)| *job == 1), 2);
        assert_eq!(queue.wake_all(), vec![(2, 2)]);
    }

    #[test]
    fn ten_thousand_retries_are_cancelled_and_released_in_batches() {
        let mut queue = InfrastructureRetryQueue::default();
        for index in 0..10_000_u32 {
            let delay = (index % 2 == 0).then_some(Duration::ZERO);
            queue.schedule(delay, (index % 4, index));
        }

        assert_eq!(queue.len(), 10_000);
        assert_eq!(queue.cancel_where(|(job, _)| *job == 1), 2_500);

        let due = queue.take_due(Instant::now());
        assert_eq!(due.len(), 5_000);
        assert!(due.iter().all(|(job, _)| *job == 0 || *job == 2));

        let indefinite = queue.wake_all();
        assert_eq!(indefinite.len(), 2_500);
        assert!(indefinite.iter().all(|(job, _)| *job == 3));
        assert_eq!(queue.len(), 0);
    }
}
