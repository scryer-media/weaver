// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! The DHT's send budget, shared by everything it sends (libtorrent
//! `dht_tracker::has_quota` and `send_packet`, dht_tracker.cpp:645-700,
//! v2.0.15). Bytes accrue at [`DHT_UPLOAD_RATE_LIMIT`] a second, up to
//! three seconds' worth; every packet sent spends its size, and is sent
//! even when that takes the budget below zero. While the budget is spent,
//! an incoming query is dropped unanswered (node.cpp:324-328).

use std::time::Duration;

use tokio::time::Instant;

/// libtorrent `settings_pack::dht_upload_rate_limit`'s default
/// (settings_pack.cpp:334, v2.0.15), in bytes a second.
pub(crate) const DHT_UPLOAD_RATE_LIMIT: i64 = 8000;

/// The burst the budget holds: three seconds' worth.
const BURST: Duration = Duration::from_secs(3);

pub(crate) struct SendQuota {
    limit: i64,
    quota: i64,
    last_tick: Instant,
}

impl SendQuota {
    /// A budget of `limit` bytes a second, full for one second at `now`, as
    /// libtorrent starts it (dht_tracker.cpp:108-109).
    pub(crate) fn new(limit: i64, now: Instant) -> Self {
        Self {
            limit,
            quota: limit,
            last_tick: now,
        }
    }

    /// Whether a query arriving at `now` is answered: the budget, topped up
    /// for the time since the last look, is above zero.
    pub(crate) fn has_quota(&mut self, now: Instant) -> bool {
        let delta = now.saturating_duration_since(self.last_tick);
        self.last_tick = now;
        if self.limit <= 0 {
            self.quota = 0;
            return false;
        }
        let max_accrue = self.limit.saturating_mul(3);
        if delta >= BURST {
            self.quota = max_accrue;
            return true;
        }
        let micros = i64::try_from(delta.as_micros()).unwrap_or(i64::MAX);
        let add = self.limit.saturating_mul(micros) / 1_000_000;
        self.quota = self.quota.saturating_add(add).min(max_accrue);
        self.quota > 0
    }

    /// A packet of `bytes` was sent.
    pub(crate) fn spend(&mut self, bytes: usize) {
        self.quota = self
            .quota
            .saturating_sub(i64::try_from(bytes).unwrap_or(i64::MAX));
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::Instant;

    use super::{DHT_UPLOAD_RATE_LIMIT, SendQuota};

    #[test]
    fn a_spent_budget_drops_queries_until_it_refills() {
        let start = Instant::now();
        let mut quota = SendQuota::new(DHT_UPLOAD_RATE_LIMIT, start);
        assert!(quota.has_quota(start));
        // One second's worth sent: the next query at the same instant is
        // dropped.
        quota.spend(8000);
        assert!(!quota.has_quota(start));
        // A tenth of a second later 800 bytes have accrued.
        let later = start + Duration::from_millis(100);
        assert!(quota.has_quota(later));
        quota.spend(800);
        assert!(!quota.has_quota(later));
        // A send is never held back: the budget goes below zero, and takes
        // as long to climb back.
        quota.spend(1600);
        assert!(!quota.has_quota(later + Duration::from_millis(200)));
        assert!(quota.has_quota(later + Duration::from_millis(201)));
    }

    #[test]
    fn the_budget_holds_three_seconds_at_most() {
        let start = Instant::now();
        let mut quota = SendQuota::new(DHT_UPLOAD_RATE_LIMIT, start);
        // Two and a half seconds idle tops it up to the three-second cap,
        // not past it.
        let idle = start + Duration::from_millis(2500);
        assert!(quota.has_quota(idle));
        quota.spend(24_000);
        assert!(!quota.has_quota(idle));
        // A gap of three seconds or more fills it whatever it was.
        let mut quota = SendQuota::new(DHT_UPLOAD_RATE_LIMIT, start);
        quota.spend(1_000_000);
        assert!(quota.has_quota(start + Duration::from_secs(3)));
        quota.spend(23_999);
        assert!(quota.has_quota(start + Duration::from_secs(3)));
    }

    #[test]
    fn no_limit_answers_nothing() {
        let start = Instant::now();
        let mut quota = SendQuota::new(0, start);
        assert!(!quota.has_quota(start + Duration::from_secs(10)));
    }
}
