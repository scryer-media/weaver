// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! Whether this run of the server has torrents at all.
//!
//! Torrents take two opt-ins. The first is the environment variable
//! [`TORRENTS_ENV`], read once at start by the app and handed down as a
//! [`TorrentAvailability`]; nothing below the app reads the environment for
//! it. The second is the saved `enabled` setting, which only counts while
//! the first is given.
//!
//! Unavailable, the torrent client is never built: no session, listener,
//! DHT, tracker or peer traffic, no port mapping and no load of the stored
//! torrents. Their rows, settings and files stay as they are, so giving the
//! variable later brings everything back as saved.

/// The environment variable that makes torrents available.
pub const TORRENTS_ENV: &str = "WEAVER_TORRENTS";

/// The refusal of turning torrents on while they are unavailable.
pub const TORRENTS_UNAVAILABLE: &str = "torrents are not available on this server: set the environment variable WEAVER_TORRENTS=1 and restart Weaver, then turn them on";

/// The refusal of a torrent handed to a server that has none: by an upload,
/// a watch folder, a feed or the API. It names the variable, since no switch
/// the operator can see turns them on.
pub const TORRENT_ADD_UNAVAILABLE: &str = "torrents are not available on this server: set the environment variable WEAVER_TORRENTS=1 and restart Weaver";

/// Whether this run of the server has torrents at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TorrentAvailability(bool);

impl TorrentAvailability {
    pub const AVAILABLE: Self = Self(true);
    pub const UNAVAILABLE: Self = Self(false);

    pub const fn of(available: bool) -> Self {
        Self(available)
    }

    /// As the value of [`TORRENTS_ENV`] says: available for `1` or `true`,
    /// unavailable unset or for anything else.
    pub fn of_env_value(value: Option<&str>) -> Self {
        Self(matches!(value, Some("1" | "true")))
    }

    pub const fn available(self) -> bool {
        self.0
    }

    /// Whether the saved setting may be turned on: refused, naming the
    /// variable, while torrents are unavailable.
    pub fn may_turn_on(self) -> Result<(), &'static str> {
        if self.0 {
            Ok(())
        } else {
            Err(TORRENTS_UNAVAILABLE)
        }
    }

    /// Whether torrents are on: available, and turned on by the saved
    /// setting `enabled`.
    pub const fn on(self, enabled: bool) -> bool {
        self.0 && enabled
    }

    /// Whether a test may run a torrent over a route, in a speed test or a
    /// path test: refused, as a speed test's is while the settings turn
    /// torrents off, on a server without torrents.
    pub fn may_test(self) -> Result<(), weaver_server_net::proxies::NetworkRefusal> {
        if self.0 {
            Ok(())
        } else {
            Err(weaver_server_net::proxies::NetworkRefusal::SpeedTestTorrentsOff)
        }
    }

    /// Seat the torrent class of an unavailable server in `queue`: every
    /// intake's torrent is refused by the variable's name, and held or
    /// tried again exactly as one is while the settings turn torrents off.
    /// Nothing is seated while torrents are available; the
    /// client seats itself once it runs.
    pub fn seat(self, queue: &impl SeatUnavailableTorrents) {
        if !self.0 {
            queue.seat_unavailable_torrents();
        }
    }

    /// The torrent client of this run: what `start` builds while torrents
    /// are available. Unavailable, `start` is never called, so nothing of
    /// the client opens, listens or loads, and `queue` refuses a torrent by
    /// the variable's name.
    pub async fn client<T, F>(
        self,
        queue: &impl SeatUnavailableTorrents,
        start: impl FnOnce() -> F,
    ) -> Option<T>
    where
        F: Future<Output = Option<T>>,
    {
        if self.0 {
            start().await
        } else {
            self.seat(queue);
            None
        }
    }
}

/// The queue an unavailable server's torrent class is seated on: the
/// scheduler, which implements it above this crate.
pub trait SeatUnavailableTorrents {
    /// Seat the torrent class that refuses every intake by the variable's
    /// name.
    fn seat_unavailable_torrents(&self);
}
