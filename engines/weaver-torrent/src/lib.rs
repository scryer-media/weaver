// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

//!
//! This crate provides everything necessary to download [torrents](https://en.wikipedia.org/wiki/BitTorrent).
//!
//! # Overview
//! The main type to start off with is [`Session`].
//!
//! It also proved useful to use the [`Api`] when building the rqbit desktop app, as it provides
//! a facade that works with simple serializable types.
//!

#![warn(clippy::cast_possible_truncation)]

macro_rules! aframe {
    ($e:expr) => {{ $e }};
}

#[macro_use]
mod stat_gen;

pub mod api;
mod api_error;
pub mod bencode;
mod bitv;
mod bitv_factory;
pub mod buffers;
mod chunk_tracker;
pub mod clone_to_owned;
mod connection_slots;
pub mod core;
mod create_torrent_file;
pub mod dht;
mod dht_utils;
mod error;
pub mod file_info;
mod file_ops;
mod identity;
mod ip_ranges;
pub mod limits;
mod listen;
pub mod lsd;
mod merge_streams;
mod meter;
mod mse;
pub mod natpmp;
pub mod network;
mod peer_connection;
mod peer_info_reader;
pub mod peer_protocol;
mod piece_priority;
mod piece_tracker;
mod read_buf;
pub mod route_interface;
mod session;
mod session_persistence;
pub mod session_stats;
pub mod sha1w;
pub mod spawn_utils;
mod ssl;

pub mod storage;
mod stream_connect;
mod torrent_state;
pub mod tracker;
mod type_aliases;
pub mod upnp;
mod v2_hashes;
mod vectored_traits;

pub use error::{Error, Result};

pub use crate::core::spawn_utils::spawn as librqbit_spawn;
pub use crate::tracker::{
    AnnounceError, AnnounceErrorKind, AnnounceOk, LastAnnounce, TrackerAnnounce, TrackerStatus,
};
// Tracker edits name trackers by URL.
pub use api::Api;
pub use api_error::{ApiError, WithStatus, WithStatusError};
pub use create_torrent_file::{CreateTorrentOptions, CreateTorrentResult, create_torrent};
pub use ip_ranges::IpRanges;
pub use listen::{ListenerMode, ListenerOptions};
pub use mse::{Encryption, EncryptionPolicy, MseError};
pub use network::{SocketFactory, StreamListener};
pub use peer_connection::PeerConnectionOptions;
pub use piece_priority::{FilePriority, PiecePriority};
pub use session::{
    AddTorrent, AddTorrentOptions, AddTorrentResponse, DhtSessionConfig, IpFilter,
    ListOnlyResponse, SUPPORTED_SCHEMES, Session, SessionLimits, SessionOptions,
    SessionPersistenceConfig,
};
pub use ssl::SslIdentity;
pub use stream_connect::{ConnectionKind, ConnectionOptions};
pub use torrent_state::{
    ManagedTorrent, ManagedTorrentShared, ManagedTorrentState, TorrentMetadata, TorrentStats,
    TorrentStatsState,
    initializing::Checkpoint,
    live::peer::info::{PeerFlags, PeerInfo},
    live::peers::ResumePeers,
};
pub use type_aliases::{BF, FileInfos, PeerSourceFlag, PeerSources};
pub use url::Url;

pub use crate::buffers::*;
pub use crate::clone_to_owned::CloneToOwned;
pub use crate::core::magnet::*;
pub use crate::core::net::{HttpClients, HttpTarget, for_target};
pub use crate::core::peer_id::*;
pub use crate::core::torrent_metainfo::*;
pub use crate::identity::Identity;

#[cfg(test)]
mod tests;

/// The cargo version of librqbit.
pub const fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

pub const fn client_name_and_version() -> &'static str {
    concat!("Weaver/", env!("CARGO_PKG_VERSION"))
}
