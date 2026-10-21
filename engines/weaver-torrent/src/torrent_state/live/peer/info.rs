// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::net::SocketAddr;

use serde::Serialize;

use crate::core::{hash_id::Id20, peer_id::identify_client};
use crate::mse::Encryption;
use crate::stream_connect::ConnectionKind;
use crate::type_aliases::PeerSources;

use super::Peer;

/// What a connection to a peer shows of it and of us (libtorrent
/// `peer_info::flags`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct PeerFlags {
    /// We want pieces the peer has.
    pub interesting: bool,
    /// We choke the peer.
    pub choked: bool,
    /// The peer wants pieces we have.
    pub remote_interested: bool,
    /// The peer chokes us.
    pub remote_choked: bool,
    /// The peer went its request timeout without delivering a block.
    pub snubbed: bool,
    /// The peer holds one of our optimistic upload slots.
    pub optimistic_unchoke: bool,
    /// The peer has every piece.
    pub seed: bool,
    /// The peer only uploads: it said so, or has every piece.
    pub upload_only: bool,
    /// A piece the peer took part in failed, and none has passed since.
    pub on_parole: bool,
    /// A holepunch introduced the peer.
    pub holepunched: bool,
}

/// One connection to a peer of the torrent (libtorrent `peer_info`, as
/// `peer_connection::get_peer_info` fills it).
#[derive(Clone, Debug, Serialize)]
pub struct PeerInfo {
    pub addr: SocketAddr,
    /// The client the peer names in its extended handshake, else the one
    /// its peer id names.
    pub client: String,
    pub peer_id: Id20,
    pub connection: ConnectionKind,
    /// We made the connection (libtorrent `peer_info::outgoing_connection`).
    pub outgoing: bool,
    pub encryption: Encryption,
    pub flags: PeerFlags,
    /// Every source that named the peer (libtorrent `peer_info::source`).
    pub sources: PeerSources,
    /// How many of the torrent's pieces the peer has.
    pub pieces: u32,
    /// The share of the torrent's pieces the peer has, from 0 to 1.
    pub progress: f64,
    /// Payload bytes per second, the peer to us and us to the peer.
    pub download_rate: u64,
    pub upload_rate: u64,
    /// Payload bytes over this connection.
    pub downloaded: u64,
    pub uploaded: u64,
}

impl PeerInfo {
    /// The connection to `peer`, of a torrent of `total_pieces` pieces,
    /// when it is live; `optimistic` when the peer holds an optimistic slot.
    pub(crate) fn of(peer: &Peer, total_pieces: u32, optimistic: bool) -> Option<Self> {
        let live = peer.get_live()?;
        let pieces = u32::try_from(live.bitfield.count_ones()).unwrap_or(u32::MAX);
        let moved = live.this_connection(&peer.stats.counters);
        Some(Self {
            addr: peer.addr,
            client: live
                .client_name
                .clone()
                .or_else(|| identify_client(live.peer_id))
                .unwrap_or_default(),
            peer_id: live.peer_id,
            connection: live.connection_kind,
            outgoing: live.outgoing,
            encryption: live.encryption,
            flags: PeerFlags {
                interesting: live.is_interesting(),
                choked: live.is_choked(),
                remote_interested: live.peer_interested,
                remote_choked: live.choked_by_peer,
                snubbed: live.is_snubbed(),
                optimistic_unchoke: optimistic,
                seed: live.seeder,
                upload_only: live.is_upload_only(),
                on_parole: peer.trust().on_parole(),
                holepunched: peer.holepunched(),
            },
            sources: peer.sources(),
            pieces,
            progress: match total_pieces {
                0 => 0.0,
                total => f64::from(pieces) / f64::from(total),
            },
            download_rate: live.download_rate.bps(),
            upload_rate: live.upload_rate.bps(),
            downloaded: moved.fetched,
            uploaded: moved.uploaded,
        })
    }
}
