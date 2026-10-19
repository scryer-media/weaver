// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! The torrents' download rate under the global download limit, laid over
//! their open sessions.

use std::sync::{Arc, Mutex, PoisonError, Weak};

use librqbit::Session;
use librqbit::limits::LimitKind;

/// The download rate torrents may move at, shared between the open
/// sessions by the same work-conserving predicate that shares the global
/// limit between usenet and torrents.
#[derive(Default)]
pub struct SessionRates {
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    rate: Option<u64>,
    sessions: Vec<Weak<Session>>,
}

impl SessionRates {
    /// Put the torrents' rate in force on a session that just opened.
    pub fn add(&self, session: &Arc<Session>) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.sessions.push(Arc::downgrade(session));
        state.lay_over();
    }

    /// What the open sessions downloaded at over the last second, in bytes
    /// per second.
    pub fn download_bps(&self) -> u64 {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state
            .sessions
            .iter()
            .filter_map(Weak::upgrade)
            .map(|session| download_bps(&session))
            .sum()
    }

    /// Put `rate` in force across the open sessions. `None` is no limit.
    pub fn set(&self, rate: Option<u64>) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.rate = rate;
        state.lay_over();
    }
}

impl State {
    fn lay_over(&mut self) {
        self.sessions.retain(|session| session.strong_count() > 0);
        let sessions: Vec<Arc<Session>> = self.sessions.iter().filter_map(Weak::upgrade).collect();
        let used: Vec<u64> = sessions
            .iter()
            .map(|session| download_bps(session))
            .collect();
        for (session, rate) in sessions.iter().zip(session_rates(self.rate, &used)) {
            apply_torrent_rate(session, rate);
        }
    }
}

fn download_bps(session: &Session) -> u64 {
    session.stats_snapshot().download_speed.as_bytes()
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

/// Put `rate` in force on `session`'s downloads. `None` is no limit, and
/// zero closes them: nothing moves until the rate opens again.
///
/// The session's limiter is read on every block request, so the rate is live
/// on the session as it is: nothing is rebuilt or reattached, and the
/// limiter is replaced only when the rate changes, since replacing it starts
/// a fresh burst.
pub fn apply_torrent_rate(session: &Session, rate: Option<u64>) {
    let kind = LimitKind::from(rate.map(|rate| u32::try_from(rate).unwrap_or(u32::MAX)));
    if session.ratelimits.get_download_bps() != kind {
        session.ratelimits.set_download_bps(kind);
    }
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
}
