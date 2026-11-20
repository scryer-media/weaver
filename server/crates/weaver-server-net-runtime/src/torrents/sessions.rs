// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! The routes torrent sessions run on: why one has no session, the leg it
//! dials over and the key of the session it opens. The sessions themselves
//! live in weaver-server-torrents.

pub use weaver_torrent::IpRanges;
use weaver_torrent::SessionLimits;
pub use weaver_torrent::upnp::{PortMapped, PortmapProtocol, PortmapTransport};

use crate::proxies::{
    Consumer, EgressBinding, EgressHealth, EgressInterface, LegPath, NetworkRuntime, ResolvedBy,
    Rung, SYSTEM_EGRESS_ID,
};
use weaver_server_db::persistence::StateError;
pub use weaver_server_net::proxies::session_key::{RouteKey, refused_key};
use weaver_server_net::torrents::net::LegKind;
use weaver_server_settings::settings::model::{TorrentConfig, TorrentLimit};

/// Why a torrent route has no open session.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TorrentRouteReason {
    NotAskedToOpen,
    LegDown,
    LegMissing,
    LegRevoked,
    QuotaSilenced,
    TorrentsDisabled,
    RouteBroken(crate::proxies::TorrentRouteFault),
    LegOpenFailed,
    StateDirectoryFailed,
    ListenPortUnavailable,
    ListenPortBindFailed,
    SessionStateUnreadable,
    SessionBuildFailed,
    /// TCP is turned off, and the route carries no uTP.
    UtpUnavailable,
    BlocklistFailed,
    /// Every session there can be is in use by other routes.
    SessionLimit,
    /// Its session is being opened now.
    Opening,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RouteFailure {
    pub reason: TorrentRouteReason,
    /// The underlying diagnostic for logs and protocol status, never a localized route label.
    pub detail: Option<String>,
}

impl From<TorrentRouteReason> for RouteFailure {
    fn from(reason: TorrentRouteReason) -> Self {
        Self {
            reason,
            detail: None,
        }
    }
}

impl RouteFailure {
    pub fn with_detail(reason: TorrentRouteReason, detail: impl Into<String>) -> Self {
        Self {
            reason,
            detail: Some(detail.into()),
        }
    }

    pub fn leg_down(detail: String) -> Self {
        Self::with_detail(TorrentRouteReason::LegDown, detail)
    }

    /// Text for protocols that carry a status sentence rather than a reason code.
    pub fn message(&self) -> String {
        if let Some(detail) = &self.detail {
            return detail.clone();
        }
        match self.reason {
            TorrentRouteReason::NotAskedToOpen => "not opened: nothing has asked it to open",
            TorrentRouteReason::LegDown => "the route's leg is down",
            TorrentRouteReason::LegMissing => "the route's egress is missing",
            TorrentRouteReason::LegRevoked => "the route's leg was revoked",
            TorrentRouteReason::QuotaSilenced => "quota reached",
            TorrentRouteReason::TorrentsDisabled => {
                weaver_server_torrent_store::torrents::status::TORRENTS_OFF
            }
            TorrentRouteReason::RouteBroken(fault) => fault.message(),
            TorrentRouteReason::LegOpenFailed => "the route's leg could not be opened",
            TorrentRouteReason::StateDirectoryFailed => {
                "the session's state directory could not be created"
            }
            TorrentRouteReason::ListenPortUnavailable => {
                "no listen port is available for this route"
            }
            TorrentRouteReason::ListenPortBindFailed => "the listen port could not be bound",
            TorrentRouteReason::SessionStateUnreadable => {
                "the session's stored ports could not be read"
            }
            TorrentRouteReason::SessionBuildFailed => "the session could not be opened",
            TorrentRouteReason::UtpUnavailable => {
                "TCP is turned off, and this route does not carry uTP"
            }
            TorrentRouteReason::BlocklistFailed => "the blocklist could not be loaded",
            TorrentRouteReason::SessionLimit => "every session is in use by other routes",
            TorrentRouteReason::Opening => "the route's session is opening",
        }
        .to_owned()
    }
}

/// The egress a leg names: its configured one, the system egress, or none.
pub fn leg_egress(network: &NetworkRuntime, egress_id: u32) -> Option<EgressInterface> {
    network
        .egress_configuration(egress_id)
        .or_else(|| (egress_id == SYSTEM_EGRESS_ID).then(EgressInterface::system))
}

/// How a session rides a leg through `egress_id` and `path`.
pub fn leg_kind(network: &NetworkRuntime, egress_id: u32, path: &LegPath) -> LegKind {
    leg_egress(network, egress_id)
        .map_or(LegKind::System, |egress| LegKind::of(&egress.binding, path))
}

/// Why a leg on egress `egress_id` may carry nothing now, when it may not:
/// its egress is down, gone, or out of quota.
pub fn leg_unavailable(network: &NetworkRuntime, egress_id: u32) -> Option<RouteFailure> {
    let Some(egress) = leg_egress(network, egress_id) else {
        return Some(TorrentRouteReason::LegMissing.into());
    };
    match network.interfaces().health(&egress, None) {
        EgressHealth::Down(reason) => Some(RouteFailure::leg_down(reason)),
        _ if network.quota_blocked_egresses().contains(&egress.id) => {
            Some(TorrentRouteReason::QuotaSilenced.into())
        }
        _ => None,
    }
}

/// Who resolves the names a session of `consumer`'s route on `path` looks
/// up, as the route's DNS profile says: through the exit proxy on a ladder,
/// out of the egress on a direct leg. None on a pool exit, whose sessions
/// each name through their own member, or a proxy that is not configured.
pub fn names_on(
    network: &NetworkRuntime,
    consumer: Consumer,
    path: &LegPath,
) -> Option<ResolvedBy> {
    let exit = match path {
        LegPath::Direct => None,
        LegPath::Ladder { rungs, .. } => Some(match rungs.last()? {
            Rung::Proxy { id } => *id,
            Rung::Chain { ids } => *ids.last()?,
            Rung::Pool { .. } => return None,
        }),
    };
    network.resolved_by(consumer, exit)
}

/// The key of the session a route through `egress_id` and `path` opens,
/// whatever DNS profile it names by: each torrent on it names by its own.
pub fn leg_key(network: &NetworkRuntime, egress_id: u32, path: &LegPath) -> RouteKey {
    match leg_egress(network, egress_id) {
        Some(egress) => RouteKey::of(egress.id, &egress.binding, path),
        None => RouteKey::of(egress_id, &EgressBinding::System, path),
    }
}

/// The key of the session a pool route through `egress_id` runs on proxy
/// `member`.
pub fn member_key(network: &NetworkRuntime, egress_id: u32, member: u32) -> RouteKey {
    match leg_egress(network, egress_id) {
        Some(egress) => RouteKey::member(egress.id, &egress.binding, member),
        None => RouteKey::member(egress_id, &EgressBinding::System, member),
    }
}

/// The limits `torrent` holds a session to, no limit as libtorrent counts
/// it: connections within the files that may be open, less twenty kept for
/// files and four in five of the rest (libtorrent `session_impl` sets
/// `connections_limit` to `max(5, (max_files - 20) * 8 / 10)`), so peers
/// never take the descriptors files and the database need, and every peer
/// unchoked (`session_impl::preemptive_unchoke`).
pub fn session_limits(torrent: &TorrentConfig) -> SessionLimits {
    let count = |limit: TorrentLimit, unlimited: usize| {
        limit
            .count()
            .map_or(unlimited, |count| count.get() as usize)
    };
    SessionLimits {
        connections_limit: count(
            torrent.connections_limit,
            connections_within(weaver_server_base::runtime::resource_limits::open_file_limit()),
        ),
        unchoke_slots_limit: count(torrent.unchoke_slots_limit, usize::MAX),
        connection_speed: torrent.connection_speed,
    }
}

// The connections `files` open files leave room for (libtorrent
// `session_impl`: `max(5, (max_files - 20) * 8 / 10)`).
pub fn connections_within(files: usize) -> usize {
    (files.saturating_sub(20) * 8 / 10).max(5)
}

/// Where a listen port is drawn when the settings name none, as qBittorrent
/// draws its own on the first start (`SessionImpl`, a `Port` of -1): above
/// the privileged ports and below the ephemeral range. The SSL ports are
/// drawn from the range above, so no route's slot of one reaches a slot of
/// the other.
pub const LISTEN_PORTS: std::ops::Range<u16> = 10_000..30_000;

/// A port from `ports`, at random.
pub fn random_port(ports: std::ops::Range<u16>) -> Result<u16, StateError> {
    let mut bytes = [0u8; 2];
    getrandom::fill(&mut bytes).map_err(|error| StateError::Database(error.to_string()))?;
    Ok(ports.start + u16::from_le_bytes(bytes) % ports.len() as u16)
}
