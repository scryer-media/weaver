// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! The torrents' rates under the global limits, laid over their open
//! sessions.

use std::sync::{Arc, Mutex, PoisonError, Weak};
use tokio::time::Instant;

use weaver_torrent::Session;
use weaver_torrent::limits::LimitKind;

use crate::bandwidth::TorrentRates;

/// The download and upload rates torrents may move at, each shared between
/// the open sessions by the same work-conserving predicate that shares the
/// global download limit between usenet and torrents.
#[derive(Default)]
pub struct SessionRates {
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    rates: TorrentRates,
    sessions: Vec<Held>,
}

// An open session and what the split knows of it.
struct Held {
    session: Weak<Session>,
    // When it was last measured, and what it had received and sent by then.
    seen_at: Instant,
    seen: (u64, u64),
    // What it received and sent at between its last two measurements, in
    // bytes per second.
    used: (u64, u64),
    // The lowest rates it was held to between them, the lowest since, and
    // the rates it is held to now.
    given: TorrentRates,
    held: TorrentRates,
    applied: TorrentRates,
}

impl SessionRates {
    /// Put the torrents' rates in force on a session that just opened.
    pub fn add(&self, session: &Arc<Session>) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.sessions.push(Held {
            session: Arc::downgrade(session),
            seen_at: Instant::now(),
            seen: session.transferred(),
            used: (0, 0),
            given: TorrentRates::default(),
            held: TorrentRates::default(),
            applied: TorrentRates::default(),
        });
        state.lay_over();
    }

    /// Measure each open session: what it received and sent since it was
    /// last measured, over the time between. Returns what the open sessions
    /// received at, in bytes per second. The rates laid over the sessions
    /// from here on follow these measurements.
    pub fn sample(&self) -> u64 {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let now = Instant::now();
        state.retain_open();
        let mut received = 0;
        for held in &mut state.sessions {
            let Some(session) = held.session.upgrade() else {
                continue;
            };
            let moved = session.transferred();
            let elapsed = now - held.seen_at;
            held.used = (
                crate::bandwidth::per_second(moved.0.saturating_sub(held.seen.0), elapsed),
                crate::bandwidth::per_second(moved.1.saturating_sub(held.seen.1), elapsed),
            );
            held.seen = moved;
            held.seen_at = now;
            held.given = std::mem::replace(&mut held.held, held.applied);
            received += held.used.0;
        }
        received
    }

    /// Put `rates` in force across the open sessions. `None` is no limit.
    pub fn set(&self, rates: TorrentRates) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.rates = rates;
        state.lay_over();
    }
}

impl State {
    // Keep the sessions still open. A closed session stops its tasks, so
    // what it moved stays frozen while anything, a closing tracker announce
    // say, still holds it: it takes no share.
    fn retain_open(&mut self) {
        self.sessions.retain(|held| {
            held.session
                .upgrade()
                .is_some_and(|session| !session.cancellation_token().is_cancelled())
        });
    }

    fn lay_over(&mut self) {
        self.retain_open();
        let held = &self.sessions;
        let download = session_rates(
            self.rates.download,
            &held
                .iter()
                .map(|held| held.given.download)
                .collect::<Vec<_>>(),
            &held.iter().map(|held| held.used.0).collect::<Vec<_>>(),
        );
        let upload = session_rates(
            self.rates.upload,
            &held
                .iter()
                .map(|held| held.given.upload)
                .collect::<Vec<_>>(),
            &held.iter().map(|held| held.used.1).collect::<Vec<_>>(),
        );
        for ((held, download), upload) in self.sessions.iter_mut().zip(download).zip(upload) {
            let rates = TorrentRates { download, upload };
            held.applied = rates;
            held.held = TorrentRates {
                download: crate::bandwidth::lower(held.held.download, download),
                upload: crate::bandwidth::lower(held.held.upload, upload),
            };
            if let Some(session) = held.session.upgrade() {
                apply_torrent_rates(&session, rates);
            }
        }
    }
}

/// Each session's share of `rate`, given the rate each was held to and what
/// each moved at over the last tick. The sessions start from equal shares;
/// one that leaves its share unused lends it to those that press theirs.
pub fn session_rates(rate: Option<u64>, given: &[Option<u64>], used: &[u64]) -> Vec<Option<u64>> {
    let Some(rate) = rate else {
        return vec![None; used.len()];
    };
    let count = used.len() as u64;
    if count == 0 {
        return Vec::new();
    }
    let shares: Vec<u64> = (0..count)
        .map(|index| rate / count + u64::from(index < rate % count))
        .collect();
    crate::bandwidth::share_out(rate, &shares, given, used)
        .into_iter()
        .map(Some)
        .collect()
}

/// Put `rates` in force on `session`'s downloads and uploads. `None` is no
/// limit, and zero closes that direction: nothing moves until the rate opens
/// again.
///
/// The session's limits are paid on every byte that crosses a peer's
/// socket, so a rate is live on the session as it is: nothing is rebuilt or
/// reattached, and a limit is set only when its rate changes; a change
/// applies to the peers already waiting on it.
pub fn apply_torrent_rates(session: &Session, rates: TorrentRates) {
    let limits = &session.ratelimits;
    let download = limit_kind(rates.download);
    if limits.get_download_bps() != download {
        limits.set_download_bps(download);
    }
    let upload = limit_kind(rates.upload);
    if limits.get_upload_bps() != upload {
        limits.set_upload_bps(upload);
    }
}

/// A rate in bytes per second as a limit: `None` is unlimited and zero
/// closed. A rate past the most a limit counts, `u32::MAX` bytes a second,
/// is held at that most.
pub fn limit_kind(rate: Option<u64>) -> LimitKind {
    LimitKind::from(rate.map(|rate| u32::try_from(rate).unwrap_or(u32::MAX)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn idle_sessions_each_hold_an_equal_share() {
        assert_eq!(
            session_rates(Some(10), &[None; 3], &[0, 0, 0]),
            vec![Some(4), Some(3), Some(3)]
        );
        assert_eq!(session_rates(None, &[None; 2], &[0, 0]), vec![None, None]);
        assert!(session_rates(Some(10), &[], &[]).is_empty());
    }

    #[test]
    fn a_busy_session_takes_what_an_idle_one_leaves_at_once() {
        // The busy one presses its share; the idle one keeps a hundredth of
        // the rate to press when it wakes.
        let rates = session_rates(Some(1_000_000), &[None, None], &[500_000, 0]);
        assert_eq!(rates, vec![Some(990_000), Some(10_000)]);
        let rates = session_rates(Some(1_000_000), &[None, None], &[500_000, 500_000]);
        assert_eq!(rates, vec![Some(500_000), Some(500_000)]);
        // The idle one wakes and presses its probe: its share the tick after.
        let rates = session_rates(
            Some(1_000_000),
            &[Some(990_000), Some(10_000)],
            &[990_000, 10_000],
        );
        assert_eq!(rates, vec![Some(500_000), Some(500_000)]);
    }

    async fn session(dir: &std::path::Path) -> Arc<Session> {
        Session::new_with_opts(
            dir.to_path_buf(),
            weaver_torrent::SessionOptions {
                dht: None,
                disable_trackers: true,
                disable_local_service_discovery: true,
                listen: None,
                persistence: None,
                ..weaver_torrent::SessionOptions::new(std::sync::Arc::new(
                    weaver_torrent::network::KernelSockets::new(None)
                        .expect("the kernel's sockets"),
                ))
            },
        )
        .await
        .unwrap()
    }

    fn limits(session: &Session) -> (Option<u32>, Option<u32>) {
        (
            session.ratelimits.get_download_bps().into(),
            session.ratelimits.get_upload_bps().into(),
        )
    }

    fn both(download: Option<u64>, upload: Option<u64>) -> TorrentRates {
        TorrentRates { download, upload }
    }

    #[tokio::test]
    async fn both_rates_are_shared_over_the_open_sessions() {
        let dir = tempfile::tempdir().unwrap();
        let rates = SessionRates::default();
        let one = session(&dir.path().join("one")).await;
        rates.add(&one);
        assert_eq!(limits(&one), (None, None));

        rates.set(both(Some(1_000_000), Some(400_000)));
        assert_eq!(limits(&one), (Some(1_000_000), Some(400_000)));

        let two = session(&dir.path().join("two")).await;
        rates.add(&two);
        assert_eq!(limits(&one), (Some(500_000), Some(200_000)));
        assert_eq!(limits(&two), (Some(500_000), Some(200_000)));

        // An upload limit of zero closes the uploads alone.
        rates.set(both(None, Some(0)));
        assert_eq!(limits(&one), (None, Some(0)));
        assert_eq!(limits(&two), (None, Some(0)));
        rates.set(both(None, None));
        assert_eq!(limits(&one), (None, None));
        assert_eq!(limits(&two), (None, None));

        one.stop().await;
        two.stop().await;
    }

    #[tokio::test]
    async fn a_closed_session_still_held_takes_no_share() {
        let dir = tempfile::tempdir().unwrap();
        let rates = SessionRates::default();
        let one = session(&dir.path().join("one")).await;
        let two = session(&dir.path().join("two")).await;
        rates.add(&one);
        rates.add(&two);
        rates.set(both(Some(1_000_000), Some(400_000)));
        assert_eq!(limits(&one), (Some(500_000), Some(200_000)));

        // Closed, yet still referenced: the open session gets the whole of
        // both rates, and the closed one is left as it was.
        two.cancellation_token().cancel();
        rates.set(both(Some(800_000), Some(300_000)));
        assert_eq!(limits(&one), (Some(800_000), Some(300_000)));
        assert_eq!(limits(&two), (Some(500_000), Some(200_000)));
        assert_eq!(rates.sample(), 0);

        one.stop().await;
    }
}
