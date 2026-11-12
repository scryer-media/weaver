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
use std::collections::HashMap;
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
pub(crate) const ROUND: Duration = Duration::from_millis(500);

/// The longest gap between two rounds a round fills quota for
/// (`update_quotas` counts at most three seconds).
const MAX_ROUND_GAP: Duration = Duration::from_secs(3);

/// Rounds a request waits before it settles for the part it was assigned
/// (libtorrent `bw_request::ttl`).
const REQUEST_ROUNDS: u32 = 20;

// A peer's request waiting on the channel (libtorrent `bw_request`).
#[derive(Clone, Debug, PartialEq, Eq)]
struct Request {
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
    // Bytes the channel may still grant, kept from round to round up to
    // three seconds' worth; below zero once more was granted than it was
    // filled with.
    quota_left: i64,
    // The last round. Rounds fall every `ROUND` from it while requests
    // wait; quota fills only then (`update_quotas` returns early on an
    // empty queue).
    last_round: Instant,
    // By id, so a waiter finds, settles and leaves its request without
    // walking the others: a round costs each request a constant, not the
    // queue's length. A round offers every request the same share, so the
    // queue has no order to keep.
    queue: HashMap<u64, Request>,
    next_id: u64,
}

impl Budget {
    // What the channel can grant a request of `want` at once, without
    // queueing it: all of it while unlimited, nothing while closed, and
    // under a rate what the channel keeps, up to `want`, while no request
    // waits for its share. libtorrent `bandwidth_channel::need_queueing`
    // grants at once only a whole request, and only while a second's worth
    // stays kept after it; here a request takes what is kept down to
    // nothing, so a lone peer writing between rounds moves all of each
    // round. Either way the most taken at once is what is kept, at most
    // three seconds' worth.
    fn at_once(&self, want: u64) -> u64 {
        match self.kind {
            LimitKind::Unlimited => want,
            LimitKind::Closed => 0,
            LimitKind::Bps(_) if !self.queue.is_empty() => 0,
            LimitKind::Bps(_) => u64::try_from(self.quota_left).unwrap_or(0).min(want),
        }
    }

    // One round's quota at `limit`.
    fn round_quota(limit: NonZeroU32) -> i64 {
        let round_ms = i64::try_from(ROUND.as_millis()).unwrap_or(i64::MAX);
        (i64::from(limit.get()) * round_ms + 500) / 1000
    }

    // A request joins the queue at `now`. Rounds keep the clock of a fixed
    // tick (libtorrent `session_impl::on_tick` every
    // `settings_pack::tick_interval`): a queue that begins waits for the
    // next tick after the last round, not a whole round from now, and the
    // ticks that passed with nothing waiting filled nothing
    // (`update_quotas` returns early on an empty queue). So a peer that
    // writes between rounds loses none of the round to its writing. No
    // timer runs while nothing waits: the tick is reckoned from the last
    // round when a queue begins.
    fn enqueue(&mut self, want: u64, now: Instant) -> u64 {
        if self.queue.is_empty() {
            let since = now.saturating_duration_since(self.last_round);
            let into_tick = since.as_nanos() % ROUND.as_nanos();
            self.last_round = now - Duration::from_nanos(u64::try_from(into_tick).unwrap_or(0));
        }
        let id = self.next_id;
        self.next_id += 1;
        self.queue.insert(
            id,
            Request {
                want,
                assigned: 0,
                share: None,
                rounds_left: REQUEST_ROUNDS,
            },
        );
        id
    }

    fn request(&mut self, id: u64) -> Option<&mut Request> {
        self.queue.get_mut(&id)
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
        self.queue.get(&id).is_some_and(Request::done)
    }

    // The request `id` taken off the queue with what it was assigned.
    fn remove(&mut self, id: u64) -> u64 {
        self.queue.remove(&id).map_or(0, |r| r.assigned)
    }

    // A request given up: the quota it was assigned goes back to the
    // channel (`update_quotas` on a disconnecting peer).
    fn cancel(&mut self, id: u64) {
        let assigned = self.remove(id);
        self.give_back(assigned);
    }

    // Bytes granted without queueing (`bandwidth_channel::use_quota`).
    fn charge(&mut self, bytes: u64) {
        if let LimitKind::Bps(_) = self.kind {
            self.quota_left = self
                .quota_left
                .saturating_sub(i64::try_from(bytes).unwrap_or(i64::MAX));
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
    // and each waiting request is offered an equal share of what it holds,
    // never more than it still wants (libtorrent
    // `bandwidth_channel::update_quota`, `bw_request::assign_bandwidth`).
    // What a round leaves untaken is kept for the next, and a debt carried,
    // up to three seconds' worth of the rate (`update_quota`: `if
    // (m_quota_left / 3 > m_limit) m_quota_left = m_limit * 3`). Quota
    // fills only while requests wait, so what is kept is what waiting
    // requests left: a side of the bandwidth split whose peers waited and
    // took less than the rounds offered can then move up to three seconds'
    // worth past its rate. A change of its rate holds what is kept to a
    // round of the new rate (`Limit::set`).
    fn round(&mut self, now: Instant) {
        let gap = (now - self.last_round).min(MAX_ROUND_GAP);
        self.last_round = now;
        let LimitKind::Bps(limit) = self.kind else {
            return;
        };
        let limit = i64::from(limit.get());
        let gap_ms = i64::try_from(gap.as_millis()).unwrap_or(i64::MAX);
        self.quota_left = self
            .quota_left
            .saturating_add((limit * gap_ms + 500) / 1000);
        if self.quota_left / 3 > limit {
            self.quota_left = limit * 3;
        }
        let share = u64::try_from(self.quota_left).unwrap_or(0) / self.queue.len().max(1) as u64;
        for request in self.queue.values_mut() {
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
// What every channel can grant at once is granted without waiting, and
// charged to each. A change of limit applies to the requests already
// waiting.
async fn acquire(limits: [Option<&Limit>; 2], want: u64) -> u64 {
    let at_once = limits
        .iter()
        .flatten()
        .map(|limit| limit.budget.lock().at_once(want))
        .min()
        .unwrap_or(want);
    if at_once > 0 {
        for limit in limits.iter().flatten() {
            limit.budget.lock().charge(at_once);
            limit.granted(at_once);
        }
        return at_once;
    }
    let waiting = Waiting {
        on: limits.map(|limit| {
            limit.map(|limit| (limit, limit.budget.lock().enqueue(want, Instant::now())))
        }),
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
                queue: HashMap::new(),
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
    // them whole at once; a rate fills from now, with the quota left kept
    // up to a round of the new rate, so a change of rate is no fresh burst
    // and a cut holds at once.
    fn set(&self, kind: LimitKind) {
        let mut budget = self.budget.lock();
        let was = std::mem::replace(&mut budget.kind, kind);
        if let LimitKind::Bps(limit) = kind {
            if !matches!(was, LimitKind::Bps(_)) {
                budget.last_round = Instant::now();
            }
            budget.quota_left = budget.quota_left.min(Budget::round_quota(limit));
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
    // no less than that less a round's worth and a request.
    #[tokio::test(start_paused = true)]
    async fn the_rate_bounds_what_passes() {
        const RATE: u64 = 1000;
        for ask in [100, 300, 5000] {
            let limits = down(bps(u32::try_from(RATE).unwrap()));
            let start = Instant::now();
            let mut total = 0;
            while start.elapsed() < Duration::from_secs(30) {
                total += limits.request(Channel::Download, ask).await;
            }
            let rate_allows = RATE * u64::try_from(start.elapsed().as_millis()).unwrap() / 1000;
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
    // fresh burst: what the last round left is kept only up to a round of
    // the new rate, so a cut holds at once.
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
        // Cut back to 1000 B/s, the 1500 left is held to a round of it.
        limits.set_download_bps(bps(1000));
        assert_eq!(
            limits.request(Channel::Download, 1000).now_or_never(),
            Some(500)
        );
        // Closing the limit holds new requests, and given back quota is
        // no longer counted as granted.
        limits.set_download_bps(LimitKind::Closed);
        limits.give_back(Channel::Download, 400);
        assert_eq!(*limits.watch_download_granted().borrow(), 1100);
        assert!(
            limits
                .request(Channel::Download, 1)
                .now_or_never()
                .is_none()
        );
    }

    // A channel fills only while requests wait (libtorrent
    // `bandwidth_manager::update_quotas` returns early on an empty queue):
    // what a round leaves untaken is granted at once to the next request
    // while none waits, and a minute of idling adds nothing to it.
    #[tokio::test(start_paused = true)]
    async fn an_idle_channel_fills_nothing() {
        let limits = down(bps(1000));
        // A round fills 500; the request takes 100 and 400 are left.
        assert_eq!(limits.request(Channel::Download, 100).await, 100);
        tokio::time::advance(Duration::from_secs(60)).await;
        assert_eq!(
            limits.request(Channel::Download, 10_000).now_or_never(),
            Some(400)
        );
        // Then a round's worth each round. A request that waits takes a
        // part of what the round filled; the next takes the rest at once.
        let start = Instant::now();
        assert_eq!(limits.request(Channel::Download, 10_000).await, 20 * 500);
        assert_eq!(start.elapsed(), 20 * ROUND);
        let start = Instant::now();
        assert_eq!(limits.request(Channel::Download, 300).await, 300);
        assert_eq!(start.elapsed(), ROUND);
        assert_eq!(
            limits.request(Channel::Download, 300).now_or_never(),
            Some(200)
        );
        assert_eq!(limits.request(Channel::Download, 100).now_or_never(), None);
        assert_eq!(*limits.watch_download_granted().borrow(), 11_000);
    }

    fn up(kind: LimitKind) -> Limits {
        Limits::new(LimitsConfig {
            upload_bps: kind,
            download_bps: LimitKind::Unlimited,
        })
    }

    // A peer's writer pays for a batch of 32 blocks at a time, 524,704
    // bytes with their frames.
    const BATCH: u64 = 524_704;

    // libtorrent `bandwidth_channel::update_quota` keeps what a round leaves.
    // Five writers under 5,000,000 B/s: each round fills 2,500,000 and
    // offers each 500,000, short of a batch, so each batch takes two
    // rounds; the second's 2,376,480 left over is kept and offered with
    // the next. From the third round on each writer takes a batch a round,
    // 2,623,520 together, 123,520 more than the round fills, out of what
    // is kept: 2,252,960 after the third round, 153,120 after the
    // twentieth. Over ten seconds twenty rounds fill 50,000,000; the
    // writers took 49,846,880 and 153,120 is kept, none lost.
    #[tokio::test(start_paused = true)]
    async fn waiting_writers_move_the_whole_rate() {
        let limits = up(bps(5_000_000));
        let start = Instant::now();
        let writer = || async {
            while start.elapsed() < Duration::from_secs(10) {
                limits.request(Channel::Upload, BATCH).await;
            }
        };
        tokio::join!(writer(), writer(), writer(), writer(), writer());
        assert_eq!(start.elapsed(), 20 * ROUND);
        let granted = *limits.watch_upload_granted().borrow();
        assert_eq!(granted, 49_846_880);
        let kept = limits.limit(Channel::Upload).budget.lock().quota_left;
        assert_eq!(kept, 153_120);
        assert_eq!(granted + u64::try_from(kept).unwrap(), 20 * 2_500_000);
    }

    // libtorrent's rounds fall on a fixed tick (`session_impl::on_tick`).
    // One writer under 20,000,000 B/s, each grant written at 40,000,000
    // B/s before it asks again: each round fills 10,000,000, the writer
    // takes a batch at the round and the rest at once as it writes, then
    // waits for the next tick. Every half second from the first round
    // moves the round's 10,000,000 whole: 190,000,000 from 0.5 s to 10 s.
    #[tokio::test(start_paused = true)]
    async fn a_lone_writer_moves_the_whole_rate() {
        const LINK: u64 = 40_000_000;
        let limits = up(bps(20_000_000));
        let start = Instant::now();
        let mut by_half_second = [0u64; 21];
        while start.elapsed() < Duration::from_secs(10) {
            let granted = limits.request(Channel::Upload, BATCH).await;
            let at = start.elapsed().as_nanos() / ROUND.as_nanos();
            by_half_second[usize::try_from(at).unwrap()] += granted;
            tokio::time::advance(Duration::from_nanos(granted * 1_000_000_000 / LINK)).await;
        }
        let mut each = vec![10_000_000; 20];
        each[0] = 0;
        assert_eq!(by_half_second[..20], each);
        assert_eq!(by_half_second[..20].iter().sum::<u64>(), 190_000_000);
    }

    // A channel keeps what waiting requests leave, three seconds' worth at
    // most (`bandwidth_channel::update_quota`). A peer of a torrent limited
    // to 1000 B/s waits ten seconds on a closed session, taking nothing of
    // the 500 each round offers: twenty rounds fill 10,000, and the channel
    // keeps 3,000. The peer goes, the channel idles a minute and fills
    // nothing; a request then takes the 3,000 at once and no more, and the
    // next waits for a round.
    #[tokio::test(start_paused = true)]
    async fn a_channel_keeps_three_seconds_at_most() {
        let session = down(LimitKind::Closed);
        let torrent = down(bps(1000));
        {
            let mut waiting =
                std::pin::pin!(torrent.request_within(Some(&session), Channel::Download, 500));
            assert!((&mut waiting).now_or_never().is_none());
            for _ in 0..20 {
                tokio::time::advance(ROUND).await;
                assert!((&mut waiting).now_or_never().is_none());
            }
            assert_eq!(
                torrent.limit(Channel::Download).budget.lock().quota_left,
                3000
            );
        }
        tokio::time::advance(Duration::from_secs(60)).await;
        assert_eq!(
            torrent.request(Channel::Download, 10_000).now_or_never(),
            Some(3000)
        );
        assert_eq!(torrent.request(Channel::Download, 1).now_or_never(), None);
        let start = Instant::now();
        assert_eq!(torrent.request(Channel::Download, 500).await, 500);
        assert_eq!(start.elapsed(), ROUND);
        assert_eq!(*torrent.watch_download_granted().borrow(), 3500);
        assert_eq!(*session.watch_download_granted().borrow(), 0);
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
        let session = down(bps(u32::try_from(RATE).unwrap()));
        let limited = down(bps(4000));
        let unlimited = Limits::default();
        let start = Instant::now();
        let (limited_grants, unlimited_grants) = tokio::join!(
            asking(&limited, &session, Channel::Download, ASK, 20),
            asking(&unlimited, &session, Channel::Download, ASK, 20),
        );
        let total = |grants: &[(Duration, u64)]| grants.iter().map(|(_, g)| g).sum::<u64>();
        let (limited_total, unlimited_total) = (total(&limited_grants), total(&unlimited_grants));
        let session_allows = RATE * u64::try_from(start.elapsed().as_millis()).unwrap() / 1000;
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

    // The queue as a list walked for each request, as the channel kept it
    // before it kept its requests by id: the reference the test below
    // compares the channel with.
    struct ListBudget {
        kind: LimitKind,
        quota_left: i64,
        last_round: Instant,
        queue: Vec<(u64, super::Request)>,
        next_id: u64,
    }

    impl ListBudget {
        fn request(&mut self, id: u64) -> Option<&mut super::Request> {
            self.queue
                .iter_mut()
                .find(|(at, _)| *at == id)
                .map(|(_, r)| r)
        }
        fn enqueue(&mut self, want: u64, now: Instant) -> u64 {
            if self.queue.is_empty() {
                // The last tick at or before `now`, ticks falling every
                // round from the last round.
                while self.last_round + ROUND <= now {
                    self.last_round += ROUND;
                }
            }
            let id = self.next_id;
            self.next_id += 1;
            self.queue.push((
                id,
                super::Request {
                    want,
                    assigned: 0,
                    share: None,
                    rounds_left: super::REQUEST_ROUNDS,
                },
            ));
            id
        }
        fn offer(&mut self, id: u64) -> Option<u64> {
            let kind = self.kind;
            let request = self.request(id)?;
            match kind {
                LimitKind::Unlimited => Some(request.want - request.assigned),
                LimitKind::Closed => None,
                LimitKind::Bps(_) => request.share,
            }
        }
        fn take(&mut self, id: u64, bytes: u64) {
            let kind = self.kind;
            if let Some(request) = self.request(id) {
                request.assigned += bytes;
                request.share = None;
                if let LimitKind::Bps(_) = kind {
                    self.quota_left -= i64::try_from(bytes).unwrap();
                }
            }
        }
        fn done(&self, id: u64) -> bool {
            self.queue.iter().any(|(at, r)| *at == id && r.done())
        }
        fn remove(&mut self, id: u64) -> u64 {
            match self.queue.iter().position(|(at, _)| *at == id) {
                Some(at) => self.queue.remove(at).1.assigned,
                None => 0,
            }
        }
        fn cancel(&mut self, id: u64) {
            let assigned = self.remove(id);
            if let LimitKind::Bps(_) = self.kind {
                self.quota_left += i64::try_from(assigned).unwrap();
            }
        }
        fn round(&mut self, now: Instant) {
            let gap = (now - self.last_round).min(super::MAX_ROUND_GAP);
            self.last_round = now;
            let LimitKind::Bps(limit) = self.kind else {
                return;
            };
            let limit = i64::from(limit.get());
            let gap_ms = i64::try_from(gap.as_millis()).unwrap();
            self.quota_left += (limit * gap_ms + 500) / 1000;
            // Past three seconds' worth and what a third rounds away, back
            // to three seconds' worth.
            if self.quota_left > 3 * limit + 2 {
                self.quota_left = 3 * limit;
            }
            let share =
                u64::try_from(self.quota_left).unwrap_or(0) / self.queue.len().max(1) as u64;
            for (_, request) in &mut self.queue {
                request.rounds_left = request.rounds_left.saturating_sub(1);
                request.share = Some(share.min(request.want - request.assigned));
            }
        }
    }

    // A channel keeping its requests by id assigns every request the same
    // shares, settles the same ones and leaves the same quota as the list
    // walked for each request, over seeded runs of requests arriving,
    // taking their offers, settling, going, rounds passing and time passing
    // between them, at a rate, unlimited and closed. Both keep the same
    // round clock and what the rounds leave, up to three seconds' worth.
    #[test]
    fn requests_kept_by_id_get_the_shares_a_walked_list_gives() {
        let mut rng = 0x6a09_e667_f3bc_c908u64;
        let mut next = || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        for kind in [
            bps(1000),
            bps(37),
            bps(250_000),
            LimitKind::Unlimited,
            LimitKind::Closed,
        ] {
            let mut now = Instant::now();
            let mut by_id = super::Limit::new(kind).budget.into_inner();
            by_id.last_round = now;
            let mut list = ListBudget {
                kind,
                quota_left: 0,
                last_round: now,
                queue: Vec::new(),
                next_id: 0,
            };
            let mut ids: Vec<u64> = Vec::new();
            for step in 0..20_000 {
                let r = next();
                let pick =
                    |ids: &[u64]| (!ids.is_empty()).then(|| ids[(r >> 16) as usize % ids.len()]);
                match r % 7 {
                    0 | 1 => {
                        let want = 1 + (r >> 20) % 4000;
                        let id = by_id.enqueue(want, now);
                        assert_eq!(id, list.enqueue(want, now), "step {step}");
                        ids.push(id);
                    }
                    2 | 3 => {
                        if let Some(id) = pick(&ids) {
                            let offer = by_id.offer(id);
                            assert_eq!(offer, list.offer(id), "step {step}");
                            if let Some(bytes) = offer {
                                by_id.take(id, bytes);
                                list.take(id, bytes);
                            }
                            assert_eq!(by_id.done(id), list.done(id), "step {step}");
                            if by_id.done(id) {
                                assert_eq!(by_id.remove(id), list.remove(id), "step {step}");
                                ids.retain(|at| *at != id);
                            }
                        }
                    }
                    4 => {
                        if let Some(id) = pick(&ids) {
                            by_id.cancel(id);
                            list.cancel(id);
                            ids.retain(|at| *at != id);
                        }
                    }
                    5 => {
                        now += ROUND + Duration::from_millis((r >> 24) % 50);
                        by_id.round(now);
                        list.round(now);
                    }
                    // Time passes with no round: a peer writing what it
                    // was granted, or the channel idle.
                    _ => now += Duration::from_millis((r >> 24) % 1700),
                }
                assert_eq!(by_id.last_round, list.last_round, "step {step}");
                assert_eq!(by_id.quota_left, list.quota_left, "step {step}");
                assert_eq!(by_id.queue.len(), list.queue.len(), "step {step}");
                for (id, request) in &list.queue {
                    assert_eq!(by_id.queue.get(id), Some(request), "step {step}");
                }
            }
        }
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
