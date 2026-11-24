// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

pub const DEFAULT_POLL_INTERVAL_SECS: u32 = 900;

// The columns that decide when a feed is next polled, read without the
// rest of the feed so the poller can find the due feeds cheaply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RssFeedSchedule {
    pub id: u32,
    pub enabled: bool,
    pub poll_interval_secs: u32,
    pub last_polled_at: Option<i64>,
}

impl RssFeedSchedule {
    // The unix second at which the feed is next due.
    pub fn next_due_at(&self) -> i64 {
        let interval = if self.poll_interval_secs == 0 {
            DEFAULT_POLL_INTERVAL_SECS
        } else {
            self.poll_interval_secs
        };
        self.last_polled_at
            .unwrap_or(0)
            .saturating_add(i64::from(interval))
    }

    pub fn is_due(&self, now: i64) -> bool {
        now >= self.next_due_at()
    }
}
