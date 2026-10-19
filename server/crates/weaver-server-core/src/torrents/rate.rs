// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! The torrents' rates under the global limits, laid over their open
//! sessions.

use std::sync::{Arc, Mutex, PoisonError, Weak};

use librqbit::Session;
use librqbit::limits::LimitKind;

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
    sessions: Vec<Weak<Session>>,
}

impl SessionRates {
    /// Put the torrents' rates in force on a session that just opened.
    pub fn add(&self, session: &Arc<Session>) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.sessions.push(Arc::downgrade(session));
        state.lay_over();
    }

    /// What the open sessions downloaded at over the last second, in bytes
    /// per second.
    pub fn download_bps(&self) -> u64 {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state
            .open()
            .iter()
            .map(|session| moved(session).download)
            .sum()
    }

    /// Put `rates` in force across the open sessions. `None` is no limit.
    pub fn set(&self, rates: TorrentRates) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.rates = rates;
        state.lay_over();
    }
}

impl State {
    // The sessions still open. A closed session stops its tasks, the one that
    // measures its speed among them, so what it last moved at stays frozen
    // while anything, a closing tracker announce say, still holds it.
    fn open(&mut self) -> Vec<Arc<Session>> {
        self.sessions.retain(|session| {
            session
                .upgrade()
                .is_some_and(|session| !session.cancellation_token().is_cancelled())
        });
        self.sessions.iter().filter_map(Weak::upgrade).collect()
    }

    fn lay_over(&mut self) {
        let sessions = self.open();
        let used: Vec<Moved> = sessions.iter().map(|session| moved(session)).collect();
        let download = session_rates(
            self.rates.download,
            &used.iter().map(|used| used.download).collect::<Vec<_>>(),
        );
        let upload = session_rates(
            self.rates.upload,
            &used.iter().map(|used| used.upload).collect::<Vec<_>>(),
        );
        for ((session, download), upload) in sessions.iter().zip(download).zip(upload) {
            apply_torrent_rates(session, TorrentRates { download, upload });
        }
    }
}

// What a session moved at over the last second, in bytes per second.
struct Moved {
    download: u64,
    upload: u64,
}

fn moved(session: &Session) -> Moved {
    let stats = session.stats_snapshot();
    Moved {
        download: stats.download_speed.as_bytes(),
        upload: stats.upload_speed.as_bytes(),
    }
}

/// Each session's share of `rate`, given what each moved at. The sessions
/// start from equal shares; one that leaves its share unused lends it to
/// those that want more.
pub fn session_rates(rate: Option<u64>, used: &[u64]) -> Vec<Option<u64>> {
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
    crate::bandwidth::share_out(rate, &shares, used)
        .into_iter()
        .map(Some)
        .collect()
}

/// Put `rates` in force on `session`'s downloads and uploads. `None` is no
/// limit, and zero closes that direction: nothing moves until the rate opens
/// again.
///
/// The session's limiters are read on every block requested and sent, so a
/// rate is live on the session as it is: nothing is rebuilt or reattached,
/// and a limiter is replaced only when its rate changes, since replacing it
/// starts a fresh burst.
pub fn apply_torrent_rates(session: &Session, rates: TorrentRates) {
    let limits = &session.ratelimits;
    let download = limit(rates.download);
    if limits.get_download_bps() != download {
        limits.set_download_bps(download);
    }
    let upload = limit(rates.upload);
    if limits.get_upload_bps() != upload {
        limits.set_upload_bps(upload);
    }
}

fn limit(rate: Option<u64>) -> LimitKind {
    LimitKind::from(rate.map(|rate| u32::try_from(rate).unwrap_or(u32::MAX)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn idle_sessions_each_hold_an_equal_share() {
        assert_eq!(
            session_rates(Some(10), &[0, 0, 0]),
            vec![Some(4), Some(3), Some(3)]
        );
        assert_eq!(session_rates(None, &[0, 0]), vec![None, None]);
        assert!(session_rates(Some(10), &[]).is_empty());
    }

    #[test]
    fn a_busy_session_borrows_what_an_idle_one_leaves() {
        let rates = session_rates(Some(1_000_000), &[500_000, 0]);
        assert_eq!(rates, vec![Some(1_000_000), Some(500_000)]);
        let rates = session_rates(Some(1_000_000), &[500_000, 500_000]);
        assert_eq!(rates, vec![Some(500_000), Some(500_000)]);
    }

    async fn session(dir: &std::path::Path) -> Arc<Session> {
        Session::new_with_opts(
            dir.to_path_buf(),
            librqbit::SessionOptions {
                dht: None,
                disable_trackers: true,
                disable_local_service_discovery: true,
                listen: None,
                persistence: None,
                ..Default::default()
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
        assert_eq!(rates.download_bps(), 0);

        one.stop().await;
    }
}
