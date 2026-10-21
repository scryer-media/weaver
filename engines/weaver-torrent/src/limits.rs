// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

//! The download and upload limits over a torrent's peers, or a session's.
//! Each is a channel of quota, filled every round at its rate and shared
//! equally between the peers waiting on it (libtorrent `bandwidth_channel`,
//! `bandwidth_manager`).

use parking_lot::Mutex;
use serde::Deserialize;
use serde::Serialize;
use std::num::NonZeroU32;
use std::time::Duration;
use tokio::sync::{Notify, watch};
use tokio::time::Instant;

/// One direction's limit. Serialized as a number of bytes per second, with
/// `null` for unlimited and `0` for closed, so rows written when the field was
/// an `Option<NonZeroU32>` read back unchanged.
#[derive(Default, Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(from = "Option<u32>", into = "Option<u32>")]
pub enum LimitKind {
    #[default]
    Unlimited,
    /// No bytes are granted until the limit is set to a rate or unlimited.
    Closed,
    Bps(NonZeroU32),
}

impl From<Option<u32>> for LimitKind {
    fn from(bps: Option<u32>) -> Self {
        match bps {
            None => Self::Unlimited,
            Some(bps) => NonZeroU32::new(bps).map_or(Self::Closed, Self::Bps),
        }
    }
}

impl From<LimitKind> for Option<u32> {
    fn from(kind: LimitKind) -> Self {
        match kind {
            LimitKind::Unlimited => None,
            LimitKind::Closed => Some(0),
            LimitKind::Bps(bps) => Some(bps.get()),
        }
    }
}

impl From<Option<NonZeroU32>> for LimitKind {
    fn from(bps: Option<NonZeroU32>) -> Self {
        bps.map_or(Self::Unlimited, Self::Bps)
    }
}

#[derive(Default, Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(default)]
pub struct LimitsConfig {
    pub upload_bps: LimitKind,
    pub download_bps: LimitKind,
}

/// How often the waiting requests are given quota (libtorrent
/// `settings_pack::tick_interval`, which drives
/// `bandwidth_manager::update_quotas`).
const ROUND: Duration = Duration::from_millis(500);

/// The longest gap between two rounds a round fills quota for
/// (`update_quotas` counts at most three seconds).
const MAX_ROUND_GAP: Duration = Duration::from_secs(3);

/// Rounds a request waits before it settles for the part it was assigned
/// (libtorrent `bw_request::ttl`).
const REQUEST_ROUNDS: u32 = 20;

// A peer's request waiting on the channel (libtorrent `bw_request`).
struct Request {
    id: u64,
    want: u64,
    assigned: u64,
    rounds_left: u32,
}

impl Request {
    // Whole, or the rounds it waits are over and it has a part
    // (`bandwidth_manager::update_quotas`).
    fn done(&self) -> bool {
        self.assigned == self.want || (self.rounds_left == 0 && self.assigned > 0)
    }
}

// One direction's quota (libtorrent `bandwidth_channel`) and the requests
// waiting on it (`bandwidth_manager::m_queue`).
struct Budget {
    kind: LimitKind,
    // Bytes the channel may still grant; below zero once more was granted
    // than it was filled with.
    quota_left: i64,
    // The last round, or when the first request of a queue arrived: quota
    // fills only while requests wait (`update_quotas` returns early on an
    // empty queue).
    last_round: Instant,
    queue: Vec<Request>,
    next_id: u64,
}

impl Budget {
    // A request is granted at once only when the quota left covers it and a
    // second's worth besides (`bandwidth_channel::need_queueing`).
    fn grant_at_once(&mut self, want: u64) -> bool {
        match self.kind {
            LimitKind::Unlimited => true,
            LimitKind::Closed => false,
            LimitKind::Bps(limit) => {
                let want = i64::try_from(want).unwrap_or(i64::MAX);
                if self.quota_left.saturating_sub(want) < i64::from(limit.get()) {
                    return false;
                }
                self.quota_left -= want;
                true
            }
        }
    }

    fn enqueue(&mut self, want: u64) -> u64 {
        if self.queue.is_empty() {
            self.last_round = Instant::now();
        }
        let id = self.next_id;
        self.next_id += 1;
        self.queue.push(Request {
            id,
            want,
            assigned: 0,
            rounds_left: REQUEST_ROUNDS,
        });
        id
    }

    // The request `id`, once it is done, taken off the queue with what it
    // was assigned.
    fn take_done(&mut self, id: u64) -> Option<u64> {
        let at = self.queue.iter().position(|r| r.id == id)?;
        self.queue[at]
            .done()
            .then(|| self.queue.remove(at).assigned)
    }

    // A request given up: the quota it was assigned goes back to the
    // channel (`update_quotas` on a disconnecting peer).
    fn cancel(&mut self, id: u64) {
        if let Some(at) = self.queue.iter().position(|r| r.id == id) {
            let request = self.queue.remove(at);
            self.give_back(request.assigned);
        }
    }

    // `bandwidth_channel::return_quota`.
    fn give_back(&mut self, bytes: u64) {
        if let LimitKind::Bps(_) = self.kind {
            self.quota_left = self
                .quota_left
                .saturating_add(i64::try_from(bytes).unwrap_or(i64::MAX));
        }
    }

    // When the next round is due; none while the channel is closed.
    fn next_round(&self) -> Option<Instant> {
        match self.kind {
            LimitKind::Closed => None,
            LimitKind::Unlimited | LimitKind::Bps(_) => Some(self.last_round + ROUND),
        }
    }

    // A round: the channel fills at its rate for the time since the last,
    // to at most three seconds' worth, and each waiting request is assigned
    // an equal share of what it holds, never more than it still wants;
    // what a request leaves is kept for later rounds (libtorrent
    // `bandwidth_channel::update_quota`, `bw_request::assign_bandwidth`).
    // An unlimited channel assigns every request whole.
    fn round(&mut self, now: Instant) {
        let gap = (now - self.last_round).min(MAX_ROUND_GAP);
        self.last_round = now;
        let distribute = match self.kind {
            LimitKind::Closed => return,
            LimitKind::Unlimited => u64::MAX,
            LimitKind::Bps(limit) => {
                let limit = i64::from(limit.get());
                let gap_ms = i64::try_from(gap.as_millis()).unwrap_or(i64::MAX);
                self.quota_left = (self.quota_left + (limit * gap_ms + 500) / 1000).min(3 * limit);
                u64::try_from(self.quota_left).unwrap_or(0)
            }
        };
        let share = distribute / self.queue.len().max(1) as u64;
        for request in &mut self.queue {
            request.rounds_left = request.rounds_left.saturating_sub(1);
            let quota = share.min(request.want - request.assigned);
            request.assigned += quota;
            if let LimitKind::Bps(_) = self.kind {
                self.quota_left -= quota as i64;
            }
        }
    }
}

struct Limit {
    budget: Mutex<Budget>,
    // A round ran or the limit changed: every waiting request looks again.
    changed: Notify,
    // Bytes this limit has granted and not had back.
    granted: watch::Sender<u64>,
}

// Takes a waiting request off the queue when its wait is dropped.
struct Waiting<'a> {
    limit: &'a Limit,
    id: u64,
}

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        self.limit.budget.lock().cancel(self.id);
    }
}

impl Limit {
    fn new(kind: LimitKind) -> Self {
        Self {
            budget: Mutex::new(Budget {
                kind,
                quota_left: 0,
                last_round: Instant::now(),
                queue: Vec::new(),
                next_id: 0,
            }),
            changed: Notify::new(),
            granted: watch::Sender::new(0),
        }
    }

    // Waits for the limit to grant bytes of `want`, and returns how many:
    // all of them at once while it is unlimited, or when the quota left
    // covers them; otherwise what the rounds assign. A closed limit assigns
    // nothing; a change of limit applies to the requests already waiting.
    async fn acquire(&self, want: u64) -> u64 {
        let id = {
            let mut budget = self.budget.lock();
            if budget.grant_at_once(want) {
                drop(budget);
                return self.granted(want);
            }
            budget.enqueue(want)
        };
        let waiting = Waiting { limit: self, id };
        loop {
            let changed = self.changed.notified();
            let mut changed = std::pin::pin!(changed);
            changed.as_mut().enable();
            let next_round = {
                let mut budget = self.budget.lock();
                if let Some(assigned) = budget.take_done(id) {
                    drop(budget);
                    std::mem::forget(waiting);
                    return self.granted(assigned);
                }
                budget.next_round()
            };
            let Some(next_round) = next_round else {
                changed.await;
                continue;
            };
            tokio::select! {
                biased;
                () = changed => {}
                () = tokio::time::sleep_until(next_round) => {
                    // The first request to wake runs the round for all.
                    let mut budget = self.budget.lock();
                    if budget.next_round().is_some_and(|due| due <= Instant::now()) {
                        budget.round(Instant::now());
                        drop(budget);
                        self.changed.notify_waiters();
                    }
                }
            }
        }
    }

    fn granted(&self, bytes: u64) -> u64 {
        self.granted.send_modify(|g| *g += bytes);
        bytes
    }

    fn give_back(&self, bytes: u64) {
        self.budget.lock().give_back(bytes);
        self.granted.send_modify(|g| *g -= bytes);
    }

    // A new limit applies to the requests waiting: an unlimited one assigns
    // them whole at once; a rate fills from now, with the quota left kept,
    // so a change of rate is no fresh burst.
    fn set(&self, kind: LimitKind) {
        let mut budget = self.budget.lock();
        let was = std::mem::replace(&mut budget.kind, kind);
        match kind {
            LimitKind::Unlimited => budget.round(Instant::now()),
            LimitKind::Bps(_) if was == LimitKind::Closed => budget.last_round = Instant::now(),
            LimitKind::Bps(_) | LimitKind::Closed => {}
        }
        drop(budget);
        self.changed.notify_waiters();
    }

    fn get(&self) -> LimitKind {
        self.budget.lock().kind
    }
}

/// A direction of a peer's socket (libtorrent `peer_connection::upload_channel`,
/// `download_channel`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Channel {
    Upload,
    Download,
}

pub struct Limits {
    down: Limit,
    up: Limit,
}

impl Default for Limits {
    fn default() -> Self {
        Self::new(LimitsConfig::default())
    }
}

impl Limits {
    pub fn new(config: LimitsConfig) -> Self {
        Self {
            down: Limit::new(config.download_bps),
            up: Limit::new(config.upload_bps),
        }
    }

    fn limit(&self, channel: Channel) -> &Limit {
        match channel {
            Channel::Upload => &self.up,
            Channel::Download => &self.down,
        }
    }

    /// Waits for `channel` to grant bytes of the `want` a peer asks for, and
    /// returns how many: one or more when `want` is not zero. Every peer
    /// waiting on the channel is assigned an equal share each round. A grant
    /// pays for bytes that cross a peer's socket.
    pub async fn request(&self, channel: Channel, want: u64) -> u64 {
        if want == 0 {
            return 0;
        }
        self.limit(channel).acquire(want).await
    }

    /// `bytes` granted on `channel` that will not cross go back to it
    /// (libtorrent `bandwidth_channel::return_quota`).
    pub fn give_back(&self, channel: Channel, bytes: u64) {
        self.limit(channel).give_back(bytes);
    }

    pub fn set_upload_bps(&self, bps: impl Into<LimitKind>) {
        self.up.set(bps.into());
    }

    pub fn set_download_bps(&self, bps: impl Into<LimitKind>) {
        self.down.set(bps.into());
    }

    pub fn get_upload_bps(&self) -> LimitKind {
        self.up.get()
    }

    pub fn get_download_bps(&self) -> LimitKind {
        self.down.get()
    }

    pub fn get_config(&self) -> LimitsConfig {
        LimitsConfig {
            upload_bps: self.get_upload_bps(),
            download_bps: self.get_download_bps(),
        }
    }

    /// Set both limits. Requests already waiting, including those held by a
    /// closed limit, continue against the new limits.
    pub fn set_config(&self, config: LimitsConfig) {
        self.set_upload_bps(config.upload_bps);
        self.set_download_bps(config.download_bps);
    }

    /// Bytes granted for download and not given back so far; the receiver
    /// sees every later change.
    pub fn watch_download_granted(&self) -> watch::Receiver<u64> {
        self.down.granted.subscribe()
    }

    /// Bytes granted for upload and not given back so far; the receiver
    /// sees every later change.
    pub fn watch_upload_granted(&self) -> watch::Receiver<u64> {
        self.up.granted.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;
    use std::time::Duration;

    use futures::FutureExt;
    use tokio::time::Instant;

    use super::{Channel, LimitKind, Limits, LimitsConfig, ROUND};

    fn bps(v: u32) -> LimitKind {
        LimitKind::Bps(NonZeroU32::new(v).unwrap())
    }

    fn down(kind: LimitKind) -> Limits {
        Limits::new(LimitsConfig {
            upload_bps: LimitKind::Unlimited,
            download_bps: kind,
        })
    }

    // libtorrent `bandwidth_manager::update_quotas`, `bw_request::assign_bandwidth`:
    // each round the quota is split equally between the requests waiting,
    // none taking more than it asked for. Two peers under a cap of 1000 B/s,
    // one asking ten times what the other does, take 250 bytes each per
    // half-second round until the smaller is whole after four rounds; the
    // larger then takes each round whole, and settles for its part when its
    // twenty rounds are over.
    #[tokio::test(start_paused = true)]
    async fn waiting_peers_get_equal_shares_each_round() {
        let limits = down(bps(1000));
        let start = Instant::now();
        let small = async {
            let granted = limits.request(Channel::Download, 1000).await;
            (granted, start.elapsed())
        };
        let large = async {
            let granted = limits.request(Channel::Download, 10_000).await;
            (granted, start.elapsed())
        };
        let (small, large) = tokio::join!(small, large);
        assert_eq!(small, (1000, 4 * ROUND));
        assert_eq!(large, (4 * 250 + 16 * 500, 20 * ROUND));
        assert_eq!(*limits.watch_download_granted().borrow(), 10_000);
    }

    // The limit bounds what passes: a peer asking again as soon as it is
    // granted takes no more than the rate allows, whatever it asks for, and
    // no less than that less the quota left behind, which is under a
    // second's worth and a request beyond what the last round filled.
    #[tokio::test(start_paused = true)]
    async fn the_rate_bounds_what_passes() {
        const RATE: u64 = 1000;
        for ask in [100, 300, 5000] {
            let limits = down(bps(RATE as u32));
            let start = Instant::now();
            let mut total = 0;
            while start.elapsed() < Duration::from_secs(30) {
                total += limits.request(Channel::Download, ask).await;
            }
            let rate_allows = RATE * start.elapsed().as_millis() as u64 / 1000;
            assert!(total <= rate_allows, "{ask}: {total} > {rate_allows}");
            assert!(
                total + RATE + ask + RATE / 2 >= rate_allows,
                "{ask}: {total}"
            );
        }
    }

    // A closed limit holds a request with no error; opening it to unlimited
    // grants it at once, and to a rate grants it as the rounds fill.
    #[tokio::test(start_paused = true)]
    async fn a_closed_limit_holds_until_it_opens() {
        for (open, after) in [
            (LimitKind::Unlimited, Duration::ZERO),
            (bps(1000), 2 * ROUND),
        ] {
            let limits = down(LimitKind::Closed);
            let granted = limits.watch_download_granted();
            let mut waiting = std::pin::pin!(limits.request(Channel::Download, 1000));
            assert!((&mut waiting).now_or_never().is_none());
            tokio::time::advance(Duration::from_secs(60)).await;
            assert!((&mut waiting).now_or_never().is_none());
            assert_eq!(*granted.borrow(), 0);
            // The other direction is untouched.
            assert_eq!(
                limits.request(Channel::Upload, 1000).now_or_never(),
                Some(1000)
            );

            limits.set_download_bps(open);
            let start = Instant::now();
            assert_eq!(waiting.await, 1000);
            assert_eq!(start.elapsed(), after);
            assert_eq!(*granted.borrow(), 1000);
            assert_eq!(limits.get_download_bps(), open);
        }
    }

    // A change of rate applies to the requests already waiting, and is no
    // fresh burst: the quota left is kept, and a request is granted at once
    // only when that covers it and a second at the new rate besides
    // (libtorrent `bandwidth_channel::need_queueing`).
    #[tokio::test(start_paused = true)]
    async fn a_rate_change_applies_to_waiting_requests_without_a_burst() {
        let limits = down(bps(1000));
        let start = Instant::now();
        let mut waiting = std::pin::pin!(limits.request(Channel::Download, 1000));
        assert!((&mut waiting).now_or_never().is_none());
        // One round at 1000 B/s assigns 500; the next, at 4000 B/s, 2000,
        // of which the request takes the 500 it still wants.
        tokio::time::sleep(ROUND).await;
        assert!((&mut waiting).now_or_never().is_none());
        limits.set_download_bps(bps(4000));
        assert_eq!(waiting.await, 1000);
        assert_eq!(start.elapsed(), 2 * ROUND);
        // 1500 left is less than a request of 1000 and a second at 4000.
        assert!(
            limits
                .request(Channel::Download, 1000)
                .now_or_never()
                .is_none()
        );
        // Closing the limit holds new requests, and given back quota is
        // no longer counted as granted.
        limits.set_download_bps(LimitKind::Closed);
        limits.give_back(Channel::Download, 400);
        assert_eq!(*limits.watch_download_granted().borrow(), 600);
        assert!(
            limits
                .request(Channel::Download, 1)
                .now_or_never()
                .is_none()
        );
    }

    #[test]
    fn test_limits_serialize_as_optional_bps() {
        let closed_down = || LimitsConfig {
            upload_bps: LimitKind::Unlimited,
            download_bps: LimitKind::Closed,
        };
        for (config, json) in [
            (
                LimitsConfig::default(),
                r#"{"upload_bps":null,"download_bps":null}"#,
            ),
            (closed_down(), r#"{"upload_bps":null,"download_bps":0}"#),
            (
                LimitsConfig {
                    upload_bps: bps(512),
                    download_bps: bps(1024),
                },
                r#"{"upload_bps":512,"download_bps":1024}"#,
            ),
        ] {
            assert_eq!(serde_json::to_string(&config).unwrap(), json);
            assert_eq!(serde_json::from_str::<LimitsConfig>(json).unwrap(), config);
        }
        // Rows written before the field could be closed: absent or Option<NonZeroU32>.
        assert_eq!(
            serde_json::from_str::<LimitsConfig>("{}").unwrap(),
            LimitsConfig::default()
        );
        assert_eq!(
            serde_json::from_str::<LimitsConfig>(r#"{"download_bps":1024}"#).unwrap(),
            LimitsConfig {
                upload_bps: LimitKind::Unlimited,
                download_bps: bps(1024),
            }
        );
    }
}
