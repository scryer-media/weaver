// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use crate::core::constants::CHUNK_SIZE;
use governor::DefaultDirectRateLimiter as RateLimiter;
use governor::Quota;
use serde::Deserialize;
use serde::Serialize;
use std::num::NonZeroU32;
use std::sync::Arc;

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

// The limit and the governor built for it, swapped together so an acquire never
// sees one without the other.
#[derive(Clone)]
struct LimitState {
    kind: LimitKind,
    limiter: Option<Arc<RateLimiter>>,
}

impl LimitState {
    // The burst holds at least one full request (a request never exceeds
    // CHUNK_SIZE), so a rate below that makes an acquire wait, never fail.
    fn new(kind: LimitKind) -> Self {
        let limiter = match kind {
            LimitKind::Bps(bps) => {
                let burst = bps.max(NonZeroU32::new(CHUNK_SIZE).expect("CHUNK_SIZE is not zero"));
                Some(Arc::new(RateLimiter::direct(
                    Quota::per_second(bps).allow_burst(burst),
                )))
            }
            LimitKind::Unlimited | LimitKind::Closed => None,
        };
        Self { kind, limiter }
    }
}

struct Limit {
    // Every acquire watches this, so a change set while acquires wait reaches
    // them at once.
    state: tokio::sync::watch::Sender<LimitState>,
    // Bytes this limit has let through.
    granted: tokio::sync::watch::Sender<u64>,
}

impl Limit {
    fn new(kind: LimitKind) -> Self {
        Self {
            state: tokio::sync::watch::Sender::new(LimitState::new(kind)),
            granted: tokio::sync::watch::Sender::new(0),
        }
    }

    // Waits for the current limit to grant bytes of `want`, at most one
    // request's worth under a rate, and returns how many. A closed limit
    // grants nothing; any change to the limit restarts the wait against the
    // new one.
    async fn acquire(&self, want: NonZeroU32) -> u64 {
        let mut state = self.state.subscribe();
        let size = loop {
            let current = state.borrow_and_update().clone();
            let grant = async {
                if current.kind == LimitKind::Closed {
                    std::future::pending::<()>().await;
                }
                let Some(rl) = current.limiter.as_ref() else {
                    return want;
                };
                let size = want.min(NonZeroU32::new(CHUNK_SIZE).expect("CHUNK_SIZE is not zero"));
                rl.until_n_ready(size)
                    .await
                    .expect("the burst holds a request");
                size
            };
            tokio::select! {
                biased;
                size = grant => break size,
                res = state.changed() => {
                    res.expect("the sender lives as long as the limit");
                }
            }
        };
        let size = u64::from(size.get());
        self.granted.send_modify(|g| *g += size);
        size
    }

    fn set(&self, kind: LimitKind) {
        self.state.send_replace(LimitState::new(kind));
    }

    fn get(&self) -> LimitKind {
        self.state.borrow().kind
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
    /// returns how many: all of them when the channel is unlimited, at least
    /// one when `want` is not zero. A grant pays for bytes that cross a
    /// peer's socket.
    pub async fn request(&self, channel: Channel, want: u64) -> u64 {
        let Some(want) = NonZeroU32::new(u32::try_from(want).unwrap_or(u32::MAX)) else {
            return 0;
        };
        self.limit(channel).acquire(want).await
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

    /// Set both limits. Acquires already waiting, including those held by a
    /// closed limit, continue against the new limits.
    pub fn set_config(&self, config: LimitsConfig) {
        self.set_upload_bps(config.upload_bps);
        self.set_download_bps(config.download_bps);
    }

    /// Bytes let through for download so far; the receiver sees every later grant.
    pub fn watch_download_granted(&self) -> tokio::sync::watch::Receiver<u64> {
        self.down.granted.subscribe()
    }

    /// Bytes let through for upload so far; the receiver sees every later grant.
    pub fn watch_upload_granted(&self) -> tokio::sync::watch::Receiver<u64> {
        self.up.granted.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use crate::core::constants::CHUNK_SIZE;
    use futures::FutureExt;

    use super::{Channel, LimitKind, Limits, LimitsConfig};

    fn bps(v: u32) -> LimitKind {
        LimitKind::Bps(NonZeroU32::new(v).unwrap())
    }

    // Each limiter starts with a full burst of max(rate, CHUNK_SIZE). Whether an
    // acquire is granted on its first poll or left waiting shows which rate it
    // met, with no dependence on the clock beyond a refill of at most a few
    // bytes between two adjacent polls.
    #[test]
    fn test_a_rate_change_applies_to_the_next_acquire() {
        let limits = Limits::new(LimitsConfig {
            upload_bps: LimitKind::Unlimited,
            download_bps: bps(1024),
        });
        let chunk = NonZeroU32::new(CHUNK_SIZE).unwrap();
        let granted = limits.watch_download_granted();

        // The first full request drains the burst; the next one has to wait for a
        // refill at 1024 B/s.
        assert!(matches!(
            limits
                .request(Channel::Download, u64::from(chunk.get()))
                .now_or_never(),
            Some(_)
        ));
        assert!(
            limits
                .request(Channel::Download, u64::from(chunk.get()))
                .now_or_never()
                .is_none()
        );
        assert_eq!(*granted.borrow(), u64::from(CHUNK_SIZE));

        // A higher rate is a new limiter with a full burst: the next acquire is
        // granted at once.
        let four = NonZeroU32::new(4 * CHUNK_SIZE).unwrap();
        limits.set_config(LimitsConfig {
            upload_bps: bps(512),
            download_bps: bps(4 * CHUNK_SIZE),
        });
        assert_eq!(
            limits.get_config(),
            LimitsConfig {
                upload_bps: bps(512),
                download_bps: bps(4 * CHUNK_SIZE),
            }
        );
        assert!(matches!(
            limits
                .request(Channel::Download, u64::from(four.get()))
                .now_or_never(),
            Some(_)
        ));
        // A grant under a rate is one request's worth at most.
        assert_eq!(*granted.borrow(), 2 * u64::from(CHUNK_SIZE));

        // None is unlimited.
        limits.set_config(LimitsConfig::default());
        let big = NonZeroU32::new(u32::MAX).unwrap();
        assert!(matches!(
            limits
                .request(Channel::Upload, u64::from(big.get()))
                .now_or_never(),
            Some(_)
        ));
        assert!(matches!(
            limits
                .request(Channel::Download, u64::from(big.get()))
                .now_or_never(),
            Some(_)
        ));
        assert_eq!(limits.get_config(), LimitsConfig::default());
    }

    // A cap below one request's size admits a full request: it waits for the
    // refill instead of failing with insufficient capacity.
    #[test]
    fn test_a_cap_below_one_request_waits_instead_of_failing() {
        let limits = Limits::new(LimitsConfig {
            upload_bps: bps(1024),
            download_bps: bps(1024),
        });
        let chunk = NonZeroU32::new(CHUNK_SIZE).unwrap();
        for _ in 0..2 {
            assert!(matches!(
                limits
                    .request(Channel::Upload, u64::from(chunk.get()))
                    .now_or_never(),
                Some(_)
            ));
            assert!(matches!(
                limits
                    .request(Channel::Download, u64::from(chunk.get()))
                    .now_or_never(),
                Some(_)
            ));
            // The burst is spent: the next full request is pending, not an error.
            assert!(
                limits
                    .request(Channel::Upload, u64::from(chunk.get()))
                    .now_or_never()
                    .is_none()
            );
            assert!(
                limits
                    .request(Channel::Download, u64::from(chunk.get()))
                    .now_or_never()
                    .is_none()
            );
            // Independent of the clock: the quota has capacity for a full request
            // (Ok, whether ready now or later), never InsufficientCapacity.
            for limit in [&limits.up, &limits.down] {
                let rl = limit.state.borrow().limiter.clone().expect("capped");
                assert!(rl.check_n(chunk).is_ok(), "insufficient capacity");
            }
            // A rate change refills the burst.
            limits.set_config(LimitsConfig {
                upload_bps: bps(1024),
                download_bps: bps(1024),
            });
        }
    }

    fn closed_down() -> LimitsConfig {
        LimitsConfig {
            upload_bps: LimitKind::Unlimited,
            download_bps: LimitKind::Closed,
        }
    }

    // A closed limit holds an acquire with no error; opening it to a rate or to
    // unlimited releases the same waiting acquire.
    #[test]
    fn test_a_closed_limit_holds_until_it_opens() {
        let chunk = NonZeroU32::new(CHUNK_SIZE).unwrap();
        for open in [bps(CHUNK_SIZE), LimitKind::Unlimited] {
            let limits = Limits::new(closed_down());
            let granted = limits.watch_download_granted();
            let mut waiting =
                std::pin::pin!(limits.request(Channel::Download, u64::from(chunk.get())));
            assert!((&mut waiting).now_or_never().is_none());
            assert!((&mut waiting).now_or_never().is_none());
            assert_eq!(*granted.borrow(), 0);
            // The other direction is untouched.
            assert!(matches!(
                limits
                    .request(Channel::Upload, u64::from(chunk.get()))
                    .now_or_never(),
                Some(_)
            ));

            limits.set_download_bps(open);
            assert!(matches!((&mut waiting).now_or_never(), Some(_)));
            assert_eq!(*granted.borrow(), u64::from(CHUNK_SIZE));
            assert_eq!(limits.get_download_bps(), open);
        }
    }

    // Closing a running limit stops the acquires already waiting on its rate;
    // a rate again releases them.
    #[test]
    fn test_closing_a_rate_holds_waiting_acquires_until_it_reopens() {
        let limits = Limits::new(LimitsConfig {
            upload_bps: LimitKind::Unlimited,
            download_bps: bps(1024),
        });
        let chunk = NonZeroU32::new(CHUNK_SIZE).unwrap();
        let granted = limits.watch_download_granted();
        assert!(matches!(
            limits
                .request(Channel::Download, u64::from(chunk.get()))
                .now_or_never(),
            Some(_)
        ));
        // The burst is spent: this one waits inside the governor.
        let mut first = std::pin::pin!(limits.request(Channel::Download, u64::from(chunk.get())));
        assert!((&mut first).now_or_never().is_none());

        limits.set_config(closed_down());
        let mut second = std::pin::pin!(limits.request(Channel::Download, u64::from(chunk.get())));
        assert!((&mut first).now_or_never().is_none());
        assert!((&mut second).now_or_never().is_none());
        assert_eq!(*granted.borrow(), u64::from(CHUNK_SIZE));
        assert_eq!(limits.get_config(), closed_down());

        // A fresh burst of two requests releases both.
        limits.set_download_bps(bps(2 * CHUNK_SIZE));
        assert!(matches!((&mut first).now_or_never(), Some(_)));
        assert!(matches!((&mut second).now_or_never(), Some(_)));
        assert_eq!(*granted.borrow(), 3 * u64::from(CHUNK_SIZE));
    }

    #[test]
    fn test_limits_serialize_as_optional_bps() {
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
