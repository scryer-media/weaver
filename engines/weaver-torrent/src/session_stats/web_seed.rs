// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! What a session asked of its torrents' web seeds, counted as each
//! request ends: one relaxed atomic add per request and per chunk of body.
//! Nothing here names a seed or a torrent.

use std::sync::atomic::{AtomicU64, Ordering};

use serde::Serialize;

use super::swarm::{Tally, add, counted, read};

counted! {
    /// How one HTTP request to a web seed ended.
    WebSeedResult {
        /// 200 or 206: the body follows.
        Ok => "ok",
        /// A redirect, followed with a request of its own.
        Redirect => "redirect",
        /// 503: the seed asks to be left for a while.
        Unavailable => "unavailable",
        /// Any other status.
        Rejected => "rejected",
        /// No answer in the time a request is given.
        Timeout => "timeout",
        /// The request could not be sent or its answer not read.
        Failed => "failed",
    }
}

/// The web seed counts of one session.
#[derive(Debug, Default)]
pub struct WebSeedStats {
    requests: Tally<{ WebSeedResult::COUNT }>,
    bytes: AtomicU64,
    waits: AtomicU64,
    failed: AtomicU64,
}

impl WebSeedStats {
    #[inline]
    pub fn requested(&self, result: WebSeedResult) {
        add(&self.requests, result as usize, 1);
    }

    /// `bytes` of a body arrived.
    #[inline]
    pub fn received(&self, bytes: usize) {
        self.bytes.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    /// A seed was left to wait before it is asked again.
    #[inline]
    pub fn waits(&self) {
        self.waits.fetch_add(1, Ordering::Relaxed);
    }

    /// A connection to a seed ended in failure.
    #[inline]
    pub fn failed(&self) {
        self.failed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> WebSeedSnapshot {
        WebSeedSnapshot {
            requests: read(&self.requests),
            bytes: self.bytes.load(Ordering::Relaxed),
            waits: self.waits.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
        }
    }
}

/// [`WebSeedStats`] as read at one moment.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct WebSeedSnapshot {
    /// By [`WebSeedResult`].
    pub requests: [u64; WebSeedResult::COUNT],
    /// Body bytes received.
    pub bytes: u64,
    /// Times a seed was left to wait before being asked again.
    pub waits: u64,
    /// Connections to a seed that ended in failure.
    pub failed: u64,
}
