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
use std::pin::Pin;
use std::time::Duration;
use tokio::sync::futures::Notified;
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

impl LimitKind {
    /// The one of two limits that lets less through: closed before any
    /// rate, a lower rate before a higher one, any rate before unlimited.
    pub fn tighter(self, other: Self) -> Self {
        match (self, other) {
            (Self::Closed, _) | (_, Self::Closed) => Self::Closed,
            (Self::Bps(a), Self::Bps(b)) => Self::Bps(a.min(b)),
            (Self::Bps(bps), Self::Unlimited) | (Self::Unlimited, Self::Bps(bps)) => Self::Bps(bps),
            (Self::Unlimited, Self::Unlimited) => Self::Unlimited,
        }
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
    // What the request has taken so far; the channel is charged for it.
    assigned: u64,
    // The channel's share for the request in its last round, not yet taken:
    // a request on several channels takes the least of their shares.
    share: Option<u64>,
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
            share: None,
            rounds_left: REQUEST_ROUNDS,
        });
        id
    }

    fn request(&mut self, id: u64) -> Option<&mut Request> {
        self.queue.iter_mut().find(|r| r.id == id)
    }

    // What the channel offers the request `id` now: all it still wants
    // while the channel is unlimited, nothing while it is closed, and
    // otherwise its share of the last round.
    fn offer(&mut self, id: u64) -> Option<u64> {
        let kind = self.kind;
        let request = self.request(id)?;
        match kind {
            LimitKind::Unlimited => Some(request.want - request.assigned),
            LimitKind::Closed => None,
            LimitKind::Bps(_) => request.share,
        }
    }

    // The request `id` takes `bytes`, and the channel is charged for them
    // (`bw_request::assign_bandwidth` charges every channel the same).
    fn take(&mut self, id: u64, bytes: u64) {
        let kind = self.kind;
        if let Some(request) = self.request(id) {
            request.assigned += bytes;
            request.share = None;
            if let LimitKind::Bps(_) = kind {
                self.quota_left -= i64::try_from(bytes).unwrap_or(i64::MAX);
            }
        }
    }

    fn done(&self, id: u64) -> bool {
        self.queue.iter().any(|r| r.id == id && r.done())
    }

    // The request `id` taken off the queue with what it was assigned.
    fn remove(&mut self, id: u64) -> u64 {
        match self.queue.iter().position(|r| r.id == id) {
            Some(at) => self.queue.remove(at).assigned,
            None => 0,
        }
    }

    // A request given up: the quota it was assigned goes back to the
    // channel (`update_quotas` on a disconnecting peer).
    fn cancel(&mut self, id: u64) {
        let assigned = self.remove(id);
        self.give_back(assigned);
    }

    // `bandwidth_channel::return_quota`.
    fn give_back(&mut self, bytes: u64) {
        if let LimitKind::Bps(_) = self.kind {
            self.quota_left = self
                .quota_left
                .saturating_add(i64::try_from(bytes).unwrap_or(i64::MAX));
        }
    }

    // When the next round is due: only a rate has rounds, since an
    // unlimited channel offers every request whole and a closed one
    // nothing.
    fn next_round(&self) -> Option<Instant> {
        match self.kind {
            LimitKind::Bps(_) => Some(self.last_round + ROUND),
            LimitKind::Unlimited | LimitKind::Closed => None,
        }
    }

    // A round: the channel fills at its rate for the time since the last,
    // to at most three seconds' worth, and each waiting request is offered
    // an equal share of what it holds, never more than it still wants;
    // what a request leaves is kept for later rounds (libtorrent
    // `bandwidth_channel::update_quota`, `bw_request::assign_bandwidth`).
    fn round(&mut self, now: Instant) {
        let gap = (now - self.last_round).min(MAX_ROUND_GAP);
        self.last_round = now;
        let LimitKind::Bps(limit) = self.kind else {
            return;
        };
        let limit = i64::from(limit.get());
        let gap_ms = i64::try_from(gap.as_millis()).unwrap_or(i64::MAX);
        self.quota_left = (self.quota_left + (limit * gap_ms + 500) / 1000).min(3 * limit);
        let share = u64::try_from(self.quota_left).unwrap_or(0) / self.queue.len().max(1) as u64;
        for request in &mut self.queue {
            request.rounds_left = request.rounds_left.saturating_sub(1);
            request.share = Some(share.min(request.want - request.assigned));
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

// A request waiting on one or two channels, the torrent's and the
// session's, each with its id there. Dropped before it is granted, it
// leaves each channel and gives back what it was assigned, so nothing is
// charged for bytes that never cross.
struct Waiting<'a> {
    on: [Option<(&'a Limit, u64)>; 2],
}

impl<'a> Waiting<'a> {
    fn each(&self) -> impl Iterator<Item = (&'a Limit, u64)> + '_ {
        self.on.iter().flatten().copied()
    }
}

// A change on one of a request's channels; never, for a channel it has not.
async fn changed(notified: Pin<&mut Option<Notified<'_>>>) {
    match notified.as_pin_mut() {
        Some(notified) => notified.await,
        None => std::future::pending().await,
    }
}

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        for (limit, id) in self.each() {
            limit.budget.lock().cancel(id);
        }
    }
}

// Waits for every one of `limits` to grant bytes of `want` together, and
// returns how many. One request is queued on each, and each round every
// channel offers it its equal share; it takes the least of them, and each
// channel is charged that alone (libtorrent
// `bandwidth_manager::request_bandwidth` and `bw_request::assign_bandwidth`).
// A request every channel can grant at once is granted whole. A change of
// limit applies to the requests already waiting.
async fn acquire(limits: [Option<&Limit>; 2], want: u64) -> u64 {
    let mut charged = 0;
    for limit in limits.iter().flatten() {
        if !limit.budget.lock().grant_at_once(want) {
            break;
        }
        charged += 1;
    }
    if charged == limits.iter().flatten().count() {
        for limit in limits.iter().flatten() {
            limit.granted(want);
        }
        return want;
    }
    for limit in limits.iter().flatten().take(charged) {
        limit.budget.lock().give_back(want);
    }
    let waiting = Waiting {
        on: limits.map(|limit| limit.map(|limit| (limit, limit.budget.lock().enqueue(want)))),
    };
    loop {
        let [first, second] = waiting
            .on
            .map(|on| on.map(|(limit, _)| limit.changed.notified()));
        let (mut first, mut second) = (std::pin::pin!(first), std::pin::pin!(second));
        for notified in [first.as_mut(), second.as_mut()] {
            if let Some(notified) = notified.as_pin_mut() {
                notified.enable();
            }
        }
        let offer = waiting
            .each()
            .map(|(limit, id)| limit.budget.lock().offer(id))
            .try_fold(u64::MAX, |least, offer| Some(least.min(offer?)));
        if let Some(bytes) = offer {
            for (limit, id) in waiting.each() {
                limit.budget.lock().take(id, bytes);
            }
        }
        if waiting
            .each()
            .any(|(limit, id)| limit.budget.lock().done(id))
        {
            let mut granted = 0;
            for (limit, id) in waiting.each() {
                granted = limit.budget.lock().remove(id);
                limit.granted(granted);
            }
            std::mem::forget(waiting);
            return granted;
        }
        let next_round = waiting
            .each()
            .filter_map(|(limit, _)| limit.budget.lock().next_round())
            .min();
        let next_round = async {
            match next_round {
                Some(due) => tokio::time::sleep_until(due).await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            biased;
            () = changed(first.as_mut()) => {}
            () = changed(second.as_mut()) => {}
            () = next_round => {
                // The first request to wake runs the rounds due for all.
                let now = Instant::now();
                for (limit, _) in waiting.each() {
                    let mut budget = limit.budget.lock();
                    if budget.next_round().is_some_and(|due| due <= now) {
                        budget.round(now);
                        drop(budget);
                        limit.changed.notify_waiters();
                    }
                }
            }
        }
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

    fn granted(&self, bytes: u64) -> u64 {
        self.granted.send_modify(|g| *g += bytes);
        bytes
    }

    fn give_back(&self, bytes: u64) {
        self.budget.lock().give_back(bytes);
        self.granted.send_modify(|g| *g -= bytes);
    }

    // A new limit applies to the requests waiting: an unlimited one offers
    // them whole at once; a rate fills from now, with the quota left kept,
    // so a change of rate is no fresh burst.
    fn set(&self, kind: LimitKind) {
        let mut budget = self.budget.lock();
        let was = std::mem::replace(&mut budget.kind, kind);
        if matches!(kind, LimitKind::Bps(_)) && !matches!(was, LimitKind::Bps(_)) {
            budget.last_round = Instant::now();
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
    /// waiting on the channel is offered an equal share each round. A grant
    /// pays for bytes that cross a peer's socket.
    pub async fn request(&self, channel: Channel, want: u64) -> u64 {
        if want == 0 {
            return 0;
        }
        acquire([Some(self.limit(channel)), None], want).await
    }

    /// `bytes` granted on `channel` that will not cross go back to it
    /// (libtorrent `bandwidth_channel::return_quota`).
    pub fn give_back(&self, channel: Channel, bytes: u64) {
        self.limit(channel).give_back(bytes);
    }

    /// Waits for these limits, a torrent's, and `session`'s together to
    /// grant bytes of the `want` a peer of the torrent asks for, and returns
    /// how many. The request waits on both at once: each round it takes the
    /// lesser of the two channels' shares, so the tighter holds the peer,
    /// and a peer that goes while it waits is charged nothing (libtorrent
    /// `peer_connection::request_bandwidth` queues one request on the
    /// channels of all the peer's classes).
    pub async fn request_within(
        &self,
        session: Option<&Limits>,
        channel: Channel,
        want: u64,
    ) -> u64 {
        if want == 0 {
            return 0;
        }
        let session = session.map(|session| session.limit(channel));
        acquire([Some(self.limit(channel)), session], want).await
    }

    /// Bytes granted by `request_within` that will not cross go back to
    /// these limits and the session's.
    pub fn give_back_within(&self, session: Option<&Limits>, channel: Channel, bytes: u64) {
        self.give_back(channel, bytes);
        if let Some(session) = session {
            session.give_back(channel, bytes);
        }
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

    // A peer of a torrent asking `ask` bytes at a time through its
    // torrent's limits within the session's until `rounds` rounds are over:
    // the bytes of each grant and when it came.
    async fn asking(
        torrent: &Limits,
        session: &Limits,
        channel: Channel,
        ask: u64,
        rounds: u32,
    ) -> Vec<(Duration, u64)> {
        let start = Instant::now();
        let mut grants = Vec::new();
        while start.elapsed() < rounds * ROUND {
            let granted = torrent.request_within(Some(session), channel, ask).await;
            grants.push((start.elapsed(), granted));
        }
        grants
    }

    // libtorrent `torrent::set_download_limit` gives the torrent its own
    // peer class (`torrent::setup_peer_class`), whose channel holds that
    // torrent's peers alone. Two torrents in one unlimited session: the one
    // limited to 1000 B/s is granted 500 bytes each half-second round,
    // while the other's peer is granted whole at once throughout.
    #[tokio::test(start_paused = true)]
    async fn a_torrent_limit_holds_its_own_peers_and_no_other_torrents() {
        let session = Limits::default();
        let held = down(bps(1000));
        let free = Limits::default();
        let grant_free = || {
            free.request_within(Some(&session), Channel::Download, 10_000)
                .now_or_never()
        };
        assert_eq!(grant_free(), Some(10_000));
        let grants = asking(&held, &session, Channel::Download, 500, 10).await;
        assert_eq!(
            grants,
            (1..=10)
                .map(|round| (round * ROUND, 500))
                .collect::<Vec<_>>()
        );
        assert_eq!(grant_free(), Some(10_000));
        assert_eq!(*held.watch_download_granted().borrow(), 5_000);
        assert_eq!(*free.watch_download_granted().borrow(), 20_000);
        assert_eq!(*session.watch_download_granted().borrow(), 25_000);
    }

    // libtorrent `bw_request::assign_bandwidth` assigns a request no more
    // than the tightest channel over it shares out: a session limit tighter
    // than a torrent's holds that torrent's peers to it too, and every
    // other torrent's. Under a session of 1000 B/s, a torrent limited to
    // 4000 B/s and an unlimited one move no more together than the
    // session's rate.
    #[tokio::test(start_paused = true)]
    async fn a_tighter_session_limit_holds_every_torrent() {
        const RATE: u64 = 1000;
        const ASK: u64 = 500;
        let session = down(bps(RATE as u32));
        let limited = down(bps(4000));
        let unlimited = Limits::default();
        let start = Instant::now();
        let (limited_grants, unlimited_grants) = tokio::join!(
            asking(&limited, &session, Channel::Download, ASK, 20),
            asking(&unlimited, &session, Channel::Download, ASK, 20),
        );
        let total = |grants: &[(Duration, u64)]| grants.iter().map(|(_, g)| g).sum::<u64>();
        let (limited_total, unlimited_total) = (total(&limited_grants), total(&unlimited_grants));
        let session_allows = RATE * start.elapsed().as_millis() as u64 / 1000;
        assert!(limited_total > 0 && unlimited_total > 0);
        assert!(
            limited_total + unlimited_total <= session_allows,
            "{limited_total} + {unlimited_total} > {session_allows}"
        );
        assert_eq!(
            *session.watch_download_granted().borrow(),
            limited_total + unlimited_total
        );
        // What the session did not grant went back to the torrent.
        assert_eq!(*limited.watch_download_granted().borrow(), limited_total);
    }

    // Two torrents at different limits under one session tighter than
    // both together, each with a peer asking 1000 bytes at a time. Each
    // round the session offers each waiter its equal share, 500; the slow
    // torrent offers its own 500, the fast one 2000. Each waiter takes the
    // least offer, so both are granted 500 a round: the fast torrent is held
    // to the session's share, not given what the slow one leaves.
    #[tokio::test(start_paused = true)]
    async fn two_torrents_at_different_limits_share_one_session_by_rounds() {
        let session = down(bps(2000));
        let slow = down(bps(1000));
        let fast = down(bps(4000));
        let (slow_grants, fast_grants) = tokio::join!(
            asking(&slow, &session, Channel::Download, 1000, 10),
            asking(&fast, &session, Channel::Download, 1000, 10),
        );
        let each: Vec<_> = (1..=5u32).map(|k| (2 * k * ROUND, 1000)).collect();
        assert_eq!((slow_grants, fast_grants), (each.clone(), each));
    }

    // A peer that goes while it waits on the session leaves nothing
    // charged: its torrent offered it a share but charges only on the
    // grant, and the grant waits for every channel.
    #[tokio::test(start_paused = true)]
    async fn a_peer_dropped_while_the_session_holds_it_costs_its_torrent_nothing() {
        let session = down(LimitKind::Closed);
        let torrent = down(bps(1000));
        let granted = torrent.watch_download_granted();
        {
            let mut waiting =
                std::pin::pin!(torrent.request_within(Some(&session), Channel::Download, 500));
            assert!((&mut waiting).now_or_never().is_none());
            tokio::time::advance(ROUND).await;
            assert!((&mut waiting).now_or_never().is_none());
            // The torrent offered it 500; the closed session holds it, so
            // neither channel has charged anything.
            assert_eq!(*granted.borrow(), 0);
        }
        assert_eq!(*granted.borrow(), 0);
        assert_eq!(*session.watch_download_granted().borrow(), 0);
        // The torrent's quota is whole: the round's 500 it filled with is
        // all left, and no request of the peer stays queued.
        let budget = torrent.limit(Channel::Download).budget.lock();
        assert_eq!(budget.quota_left, 500);
        assert!(budget.queue.is_empty());
        drop(budget);
        assert!(
            session
                .limit(Channel::Download)
                .budget
                .lock()
                .queue
                .is_empty()
        );
    }

    // libtorrent `torrent::set_limit_impl` throttles the torrent's channel
    // in place: a change reaches the peers already waiting on it, with no
    // new connection. A peer waiting under 1000 B/s for 10_000 bytes is
    // granted them whole the moment the torrent's limit is lifted; a closed
    // upload then holds every byte a peer would send, protocol bytes
    // included, while the download goes on.
    #[tokio::test(start_paused = true)]
    async fn a_torrent_limit_set_live_reaches_the_waiting_peers() {
        let session = Limits::default();
        let torrent = down(bps(1000));
        let mut waiting =
            std::pin::pin!(torrent.request_within(Some(&session), Channel::Download, 10_000));
        assert!((&mut waiting).now_or_never().is_none());
        tokio::time::advance(ROUND).await;
        assert!((&mut waiting).now_or_never().is_none());
        torrent.set_download_bps(LimitKind::Unlimited);
        assert_eq!((&mut waiting).now_or_never(), Some(10_000));

        torrent.set_upload_bps(LimitKind::Closed);
        let mut handshake =
            std::pin::pin!(torrent.request_within(Some(&session), Channel::Upload, 68));
        assert!((&mut handshake).now_or_never().is_none());
        tokio::time::advance(Duration::from_secs(60)).await;
        assert!((&mut handshake).now_or_never().is_none());
        assert_eq!(*session.watch_upload_granted().borrow(), 0);
        assert_eq!(
            torrent
                .request_within(Some(&session), Channel::Download, 1000)
                .now_or_never(),
            Some(1000)
        );
        torrent.set_upload_bps(bps(1000));
        assert_eq!(handshake.await, 68);
        assert_eq!(*session.watch_upload_granted().borrow(), 68);
    }

    #[test]
    fn the_tighter_limit_lets_less_through() {
        use LimitKind::{Closed, Unlimited};
        for (a, b, tighter) in [
            (Unlimited, Unlimited, Unlimited),
            (Unlimited, bps(10), bps(10)),
            (bps(5), bps(10), bps(5)),
            (bps(5), Closed, Closed),
            (Unlimited, Closed, Closed),
        ] {
            assert_eq!(a.tighter(b), tighter);
            assert_eq!(b.tighter(a), tighter);
        }
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
