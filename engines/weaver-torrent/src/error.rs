// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use crate::peer_protocol::MessageDeserializeError;
use governor::InsufficientCapacity;
use tokio::sync::AcquireError;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("error connecting over TCP: {0:#}")]
    TcpConnect(#[source] std::io::Error),
    #[error("error connecting over uTP: {0:#}")]
    UtpConnect(#[source] weaver_utp::Error),
    #[error("can't connect, all connection methods disabled")]
    ConnectDisabled,
    #[error("error connecting: TCP={tcp:#} uTP={utp:#}")]
    Connect {
        tcp: std::io::Error,
        utp: weaver_utp::Error,
    },
    #[error("uTP disabled")]
    UtpDisabled,
    #[error("TCP connections disabled")]
    TcpDisabled,

    #[error("encryption handshake failed: {0:#}")]
    Mse(
        #[from]
        #[source]
        crate::mse::MseError,
    ),

    #[error("wrong info hash")]
    WrongInfoHash,
    #[error("a hash message from a peer that does not speak v2")]
    HashMessageFromV1Peer,
    #[error("connecting to ourselves")]
    ConnectingToOurselves,

    #[error("error writing handshake: {0:#}")]
    WriteHandshake(#[source] std::io::Error),
    #[error("error reading handshake: {0:#}")]
    ReadHandshake(#[source] std::io::Error),

    #[error("error writing: {0:#}")]
    Write(#[source] std::io::Error),
    #[error("error reading: {0:#}")]
    Read(#[source] std::io::Error),

    /// A step of making the connection, the dial or the TLS handshake on
    /// it, ran out of time.
    #[error("timeout {0}")]
    ConnectTimeout(&'static str),
    /// A step on the open connection ran out of time.
    #[error("timeout {0}")]
    Timeout(&'static str),

    #[error("peer disconnected while reading handshake")]
    PeerDisconnectedReadingHandshake,
    #[error("peer disconnected")]
    PeerDisconnected,

    #[error(transparent)]
    ProtoSerialize(#[from] crate::peer_protocol::SerializeError),

    #[error("error deserializing handshake: {0:#}")]
    DeserializeHandshake(#[source] MessageDeserializeError),
    #[error("error deserializing message: {0:#}")]
    Deserialize(
        #[from]
        #[source]
        MessageDeserializeError,
    ),

    #[error(transparent)]
    Anyhow(anyhow::Error),

    #[error("error reading chunk: {0:#}")]
    ReadChunk(#[source] anyhow::Error),

    #[error("disconnect requested")]
    Disconnect,

    #[error("disconnecting peer, reason: {0:#}")]
    DisconnectWithSource(#[source] anyhow::Error),

    #[error("bug: make_contiguous() called on a contiguous buffer; start={start} len={len}")]
    BugReadBufMakeContiguous { start: u16, len: u16 },

    #[cfg(test)]
    #[error("disconnected to simulate failure in tests")]
    TestDisconnect,

    #[error("torrent is not live")]
    TorrentIsNotLive,

    #[error("peer task is dead")]
    PeerTaskDead,

    #[error("chunk tracker empty, torrent was paused")]
    ChunkTrackerEmpty,

    #[error("rate limiting: insufficient capacity: {0:#}")]
    RateLimitInsufficientCapacity(
        #[from]
        #[source]
        InsufficientCapacity,
    ),

    #[error("semaphore closed")]
    SemaphoreAcquireError(
        #[from]
        #[source]
        AcquireError,
    ),

    #[error("bug: peer not found")]
    BugPeerNotFound,

    #[error("bug: invalid peer state")]
    BugInvalidPeerState,

    #[error("file is None, torrent was probably paused")]
    FsFileIsNone,

    #[error("session is dead")]
    SessionDestroyed,

    #[error(transparent)]
    Core(#[from] crate::core::Error),
}

impl Error {
    /// Our own side turned the connect away before the peer had a say: no
    /// transport was enabled, or each that was refused locally. Such an
    /// error does not count against the peer.
    pub fn is_local_refusal(&self) -> bool {
        match self {
            Error::ConnectDisabled | Error::UtpDisabled | Error::TcpDisabled => true,
            Error::UtpConnect(utp) => utp.is_local_refusal(),
            _ => false,
        }
    }

    /// Whether a connection that ends in this error counts against its
    /// peer: libtorrent `peer_connection::disconnect` sets `m_failed` only
    /// above `normal` severity. A connect that does not complete, timed out
    /// or refused, its TLS handshake included (`ssl_stream::async_connect`),
    /// fails (`connect_failed`, at `failure`), and so does a peer
    /// that breaks the protocol: a wrong info hash or handshake at `failure`,
    /// an encryption handshake it gets wrong at `peer_error`
    /// (`bt_peer_connection::on_receive_impl`). A transport that is open
    /// and that the peer closes, resets or leaves silent does not:
    /// `on_receive_data` and `on_send_data` close at `normal`, as do the
    /// handshake and inactivity timeouts of `second_tick`. Nor does a close
    /// we asked for, a connection to ourselves, or a connect our own side
    /// turned away.
    pub fn is_failure(&self) -> bool {
        match self {
            Error::Disconnect
            | Error::ConnectingToOurselves
            | Error::Timeout(_)
            | Error::ReadHandshake(_)
            | Error::WriteHandshake(_)
            | Error::Read(_)
            | Error::Write(_)
            | Error::PeerDisconnectedReadingHandshake
            | Error::PeerDisconnected => false,
            Error::Mse(e) => e.is_failure(),
            e => !e.is_local_refusal(),
        }
    }
}

pub type Result<T> = core::result::Result<T, Error>;
