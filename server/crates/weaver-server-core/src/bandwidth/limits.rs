// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// How fast one direction of one egress may move.
//
// Every limit here is an `Option`: `None` is no limit, and `Some(n)` is a
// limit of `n` bytes per second, so `Some(0)` lets nothing through.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Direction {
    Download,
    Upload,
}

// Where an egress stands against its quota in one direction.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuotaState {
    #[default]
    Normal,
    // Past the soft cap: the direction moves at its trickle rate.
    Trickle,
    // At or past the hard cap: the direction moves nothing.
    Stopped,
}

impl QuotaState {
    // The state `used` bytes put a direction in. A cap of `Some(0)` is
    // reached before the first byte.
    pub fn of(used: u64, soft: Option<u64>, hard: Option<u64>) -> Self {
        if hard.is_some_and(|hard| used >= hard) {
            Self::Stopped
        } else if soft.is_some_and(|soft| used >= soft) {
            Self::Trickle
        } else {
            Self::Normal
        }
    }

    // The rate this state allows, given the direction's trickle rate.
    pub const fn rate(self, trickle_bps: u64) -> Option<u64> {
        match self {
            Self::Normal => None,
            Self::Trickle => Some(trickle_bps),
            Self::Stopped => Some(0),
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::Trickle => "trickle",
            Self::Stopped => "stopped",
        }
    }
}

// The limits that bear on one direction of one egress.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RateLimits {
    // The operator's global limit.
    pub global: Option<u64>,
    // The global limit a schedule rule has in force.
    pub schedule: Option<u64>,
    // The egress's own cap.
    pub egress: Option<u64>,
    // The egress's quota state, and the trickle rate it falls to.
    pub quota: QuotaState,
    pub trickle_bps: u64,
}

// The tighter of two limits.
pub const fn tighter(a: Option<u64>, b: Option<u64>) -> Option<u64> {
    match (a, b) {
        (Some(a), Some(b)) => Some(if a < b { a } else { b }),
        (Some(limit), None) | (None, Some(limit)) => Some(limit),
        (None, None) => None,
    }
}

// The rate one direction of one egress may move at: the tightest of every
// limit on it. The quota state is a floor of its own, so a schedule can
// lower the rate under it but never lift it.
pub const fn effective_rate(limits: RateLimits) -> Option<u64> {
    tighter(
        tighter(limits.global, limits.schedule),
        tighter(limits.egress, limits.quota.rate(limits.trickle_bps)),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits() -> RateLimits {
        RateLimits {
            trickle_bps: 2_048,
            ..RateLimits::default()
        }
    }

    #[test]
    fn no_limit_anywhere_is_unlimited() {
        assert_eq!(effective_rate(limits()), None);
    }

    #[test]
    fn the_tightest_limit_wins() {
        let rate = |global, schedule, egress| {
            effective_rate(RateLimits {
                global,
                schedule,
                egress,
                ..limits()
            })
        };
        assert_eq!(rate(Some(500), None, None), Some(500));
        assert_eq!(rate(None, Some(400), None), Some(400));
        assert_eq!(rate(None, None, Some(300)), Some(300));
        assert_eq!(rate(Some(500), Some(400), Some(300)), Some(300));
        assert_eq!(rate(Some(100), Some(400), Some(300)), Some(100));
        // A schedule tighter than the global limit lowers it; a looser one
        // does not lift it.
        assert_eq!(rate(Some(500), Some(900), None), Some(500));
    }

    #[test]
    fn a_zero_limit_is_zero_at_every_level() {
        for limits in [
            RateLimits {
                global: Some(0),
                ..limits()
            },
            RateLimits {
                schedule: Some(0),
                egress: Some(1_000),
                ..limits()
            },
            RateLimits {
                egress: Some(0),
                global: Some(1_000),
                ..limits()
            },
        ] {
            assert_eq!(effective_rate(limits), Some(0));
        }
    }

    #[test]
    fn a_quota_state_is_a_floor_a_schedule_cannot_lift() {
        let trickle = RateLimits {
            quota: QuotaState::Trickle,
            schedule: Some(1_000_000),
            ..limits()
        };
        assert_eq!(effective_rate(trickle), Some(2_048));
        let lower = RateLimits {
            schedule: Some(1_000),
            ..trickle
        };
        assert_eq!(effective_rate(lower), Some(1_000));
        let stopped = RateLimits {
            quota: QuotaState::Stopped,
            global: Some(1_000_000),
            ..limits()
        };
        assert_eq!(effective_rate(stopped), Some(0));
    }

    #[test]
    fn quota_state_follows_usage_through_both_caps() {
        let soft = Some(1_000);
        let hard = Some(2_000);
        assert_eq!(QuotaState::of(0, soft, hard), QuotaState::Normal);
        assert_eq!(QuotaState::of(999, soft, hard), QuotaState::Normal);
        assert_eq!(QuotaState::of(1_000, soft, hard), QuotaState::Trickle);
        assert_eq!(QuotaState::of(1_999, soft, hard), QuotaState::Trickle);
        assert_eq!(QuotaState::of(2_000, soft, hard), QuotaState::Stopped);
        assert_eq!(QuotaState::of(u64::MAX, None, None), QuotaState::Normal);
        // A zero cap is reached at once; no cap is never reached.
        assert_eq!(QuotaState::of(0, None, Some(0)), QuotaState::Stopped);
        assert_eq!(QuotaState::of(0, Some(0), None), QuotaState::Trickle);
    }
}
