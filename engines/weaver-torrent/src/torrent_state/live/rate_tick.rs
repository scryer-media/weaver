// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! The session's rate tick: one timer samples the rate estimators of the
//! runs that move bytes, and no other. A run joins when a peer of it goes
//! live and leaves once it has no live peer and its rates have fallen to
//! nothing, so idle torrents cost no wake at all (libtorrent
//! `session_impl::on_tick` visits only `m_torrent_lists[torrent_want_tick]`).

use std::sync::{
    Arc, Weak,
    atomic::{AtomicU64, Ordering},
};

use parking_lot::Mutex;
use tokio::sync::Notify;

use super::{TorrentStateLive, peer::RATE_SAMPLE_INTERVAL};

#[derive(Default)]
pub(crate) struct RateTick {
    // The runs sampled each tick. A run is on it at most once: its
    // `rate_ticked` flag says whether it is.
    ticking: Mutex<Vec<Weak<TorrentStateLive>>>,
    joined: Notify,
    // How many runs the tick has sampled, for metrics and tests.
    samples: AtomicU64,
}

impl RateTick {
    /// Puts `torrent` on the tick, unless it is there.
    pub(crate) fn join(&self, torrent: &Arc<TorrentStateLive>) {
        if torrent.rate_ticked.swap(true, Ordering::SeqCst) {
            return;
        }
        self.ticking.lock().push(Arc::downgrade(torrent));
        self.joined.notify_one();
    }

    /// How many runs the tick has sampled.
    #[cfg(test)]
    pub(crate) fn samples(&self) -> u64 {
        self.samples.load(Ordering::Relaxed)
    }

    /// How many runs are on the tick.
    #[cfg(test)]
    pub(crate) fn ticking(&self) -> usize {
        self.ticking.lock().len()
    }

    /// Samples every run on the tick each RATE_SAMPLE_INTERVAL, and sleeps
    /// without a timer while none is.
    pub(crate) async fn run(self: Arc<Self>) {
        loop {
            let ticking = std::mem::take(&mut *self.ticking.lock());
            if ticking.is_empty() {
                self.joined.notified().await;
                continue;
            }
            let now = std::time::Instant::now();
            let mut kept = Vec::with_capacity(ticking.len());
            for weak in ticking {
                let Some(torrent) = weak.upgrade() else {
                    continue;
                };
                self.samples.fetch_add(1, Ordering::Relaxed);
                if torrent.sample_rates(now) {
                    kept.push(weak);
                }
            }
            self.ticking.lock().extend(kept);
            tokio::time::sleep(RATE_SAMPLE_INTERVAL).await;
        }
    }
}
