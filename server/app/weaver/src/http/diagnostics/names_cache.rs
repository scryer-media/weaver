// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The stand-in table's stored-names read, kept for the MCP diagnostics tools.
//
// That read is the expensive part of a table: every stored NZB it can reach
// in up to thirty seconds. A tool call, and every page of one, used to pay it
// again. Here one read serves every call for five minutes from when it ended,
// complete or not; calls that arrive while it runs wait for it rather than
// start their own. The names held in memory (the queue, servers, categories)
// are not kept: each call gathers them fresh and merges them over the kept
// read, so a job added since still has its names replaced.
//
// One value, in memory only. An expired value is dropped by the next call,
// which reads again. The package download does not use this: it reads its
// own table beside its own collection.

use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::stored_read::ReadState;

// How long a read serves calls, from when it ended.
pub(crate) const STORED_NAMES_LIFETIME: Duration = Duration::from_secs(5 * 60);

type Clock = Arc<dyn Fn() -> Instant + Send + Sync>;

pub(crate) struct StoredNamesCache {
    clock: Clock,
    // The last read and when it ended. The lock is held across a read, so a
    // second call waits for the first one's result. It is this cache's own
    // and nothing else takes it.
    held: tokio::sync::Mutex<Option<(Instant, Arc<ReadState>)>>,
}

impl Default for StoredNamesCache {
    fn default() -> Self {
        Self::with_clock(Arc::new(Instant::now))
    }
}

impl StoredNamesCache {
    pub(crate) fn with_clock(clock: Clock) -> Self {
        Self {
            clock,
            held: tokio::sync::Mutex::new(None),
        }
    }

    // The kept read while it is under its lifetime; otherwise `read` runs
    // once, for this call and every call waiting on it, and is kept.
    pub(super) async fn get<F, Fut>(&self, read: F) -> Arc<ReadState>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = ReadState>,
    {
        let mut held = self.held.lock().await;
        if let Some((ended, state)) = held.as_ref()
            && (self.clock)().saturating_duration_since(*ended) < STORED_NAMES_LIFETIME
        {
            return Arc::clone(state);
        }
        // Expired: let it go before the new read holds its own.
        *held = None;
        let state = Arc::new(read().await);
        *held = Some(((self.clock)(), Arc::clone(&state)));
        state
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::super::stored_read::{MemoryNames, TableNames};
    use weaver_server_core::jobs::diagnostic_names::JobNameRecord;

    // A clock the test moves by hand.
    fn manual_clock() -> (Clock, Arc<Mutex<Instant>>) {
        let now = Arc::new(Mutex::new(Instant::now()));
        let read = Arc::clone(&now);
        (Arc::new(move || *read.lock().unwrap()), now)
    }

    fn counted(reads: &AtomicU64) -> impl Future<Output = ReadState> + '_ {
        reads.fetch_add(1, Ordering::SeqCst);
        std::future::ready(ReadState::default())
    }

    #[tokio::test]
    async fn calls_inside_the_lifetime_share_one_read() {
        let (clock, now) = manual_clock();
        let cache = StoredNamesCache::with_clock(clock);
        let reads = AtomicU64::new(0);

        cache.get(|| counted(&reads)).await;
        *now.lock().unwrap() += STORED_NAMES_LIFETIME - Duration::from_secs(1);
        cache.get(|| counted(&reads)).await;
        assert_eq!(reads.load(Ordering::SeqCst), 1);

        // At the lifetime, the next call reads again.
        *now.lock().unwrap() += Duration::from_secs(1);
        cache.get(|| counted(&reads)).await;
        assert_eq!(reads.load(Ordering::SeqCst), 2);
        // That read starts a lifetime of its own.
        cache.get(|| counted(&reads)).await;
        assert_eq!(reads.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn concurrent_calls_wait_on_one_read() {
        let cache = StoredNamesCache::default();
        let reads = AtomicU64::new(0);
        let (release, released) = tokio::sync::oneshot::channel::<()>();

        // Polled in order on one task: the first call takes the read and
        // waits on `released`, the second waits on it, and only then is the
        // read let go.
        let (first, second, ()) = tokio::join!(
            cache.get(|| async {
                reads.fetch_add(1, Ordering::SeqCst);
                released.await.unwrap();
                ReadState::default()
            }),
            cache.get(|| counted(&reads)),
            async { release.send(()).unwrap() },
        );
        assert_eq!(reads.load(Ordering::SeqCst), 1);
        assert!(Arc::ptr_eq(&first, &second));
    }

    #[tokio::test]
    async fn a_name_only_memory_holds_is_still_named_on_a_kept_read() {
        let cache = StoredNamesCache::default();
        let reads = AtomicU64::new(0);
        cache.get(|| counted(&reads)).await;

        // A job queued after the read: only memory knows it.
        let kept = cache.get(|| counted(&reads)).await;
        assert_eq!(reads.load(Ordering::SeqCst), 1);
        let record = JobNameRecord {
            name: Some("Fixture.Queued.After.The.Read".to_string()),
            ..JobNameRecord::default()
        };
        let memory = MemoryNames {
            queue: vec![(77, record.clone())],
            ..MemoryNames::default()
        };
        let table = TableNames::from_read((*kept).clone(), memory);
        assert_eq!(table.stored.jobs.get(&77), Some(&record));
        // The read did not cover it, so lines naming its other names are guarded.
        assert!(!table.manifest.complete);
    }
}
