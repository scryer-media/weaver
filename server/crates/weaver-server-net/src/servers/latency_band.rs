// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::time::Duration;

// Latency bands. A band picks the starting rung, labels a server in the UI,
// and labels a connection test. It never gates a depth on its own: the depth
// comes from the measured latency/transfer ratio.
pub const LATENCY_BAND_GOOD_MAX: Duration = Duration::from_millis(400);
pub const LATENCY_BAND_MODERATE_MAX: Duration = Duration::from_millis(800);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LatencyBand {
    Good,
    Moderate,
    Slow,
}

impl LatencyBand {
    pub fn from_latency(latency: Duration) -> Self {
        if latency < LATENCY_BAND_GOOD_MAX {
            Self::Good
        } else if latency < LATENCY_BAND_MODERATE_MAX {
            Self::Moderate
        } else {
            Self::Slow
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Good => "good",
            Self::Moderate => "moderate",
            Self::Slow => "slow",
        }
    }

    // Where a server with no measurement of its own starts. A further-away
    // server has more round trip to hide, so it starts deeper. The good band
    // opens deeper from [`OPEN_FOUR_DEEP_MIN_LATENCY`]; see
    // [`guessed_opening_depth`].
    pub fn starting_depth(self) -> u8 {
        match self {
            Self::Good => 2,
            Self::Moderate => 4,
            Self::Slow => 8,
        }
    }
}
