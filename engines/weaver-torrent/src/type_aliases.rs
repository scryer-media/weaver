// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::net::SocketAddr;

use futures::stream::BoxStream;
use tokio::io::AsyncWrite;

use crate::{file_info::FileInfo, storage::TorrentStorage, vectored_traits::AsyncReadVectored};

// NOTE: Msb0 is used because that's what bittorrent protocol uses for bitfield.
// Don't change to Lsb0 even though it might be a bit faster (in theory) on LE architectures.
pub type BS = bitvec::slice::BitSlice<u8, bitvec::order::Msb0>;
pub type BF = bitvec::boxed::BitBox<u8, bitvec::order::Msb0>;

pub type PeerHandle = SocketAddr;
/// Each peer named to a torrent, with who named it.
pub type PeerStream = BoxStream<'static, (SocketAddr, PeerSourceFlag)>;

/// Who named a peer: one of libtorrent's `peer_source_flags_t`
/// (`peer_info::tracker`, `dht`, `pex`, `lsd`, `resume_data`, `incoming`),
/// which `torrent::add_peer` and `peer_list::update_peer` take with each
/// naming.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub enum PeerSourceFlag {
    /// A tracker's announce reply.
    Tracker,
    /// The DHT.
    Dht,
    /// A connected peer's PEX message.
    Pex,
    /// Local service discovery.
    Lsd,
    /// The torrent's own list: the peers it was added with, the operator's
    /// or those a restart saved. The peers a magnet's metadata fetch took
    /// keep the sources that named them.
    ResumeData,
    /// The peer connected to us.
    Incoming,
}

impl PeerSourceFlag {
    const ALL: [Self; 6] = [
        Self::Tracker,
        Self::Dht,
        Self::Pex,
        Self::Lsd,
        Self::ResumeData,
        Self::Incoming,
    ];

    fn bit(self) -> u8 {
        1 << self as u8
    }
}

/// Every source that named a peer (libtorrent `torrent_peer::source`, which
/// `peer_list::add_peer` and `update_peer` OR each naming into).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PeerSources(u8);

impl PeerSources {
    pub(crate) fn insert(&mut self, source: PeerSourceFlag) {
        self.0 |= source.bit();
    }

    pub fn contains(self, source: PeerSourceFlag) -> bool {
        self.0 & source.bit() != 0
    }

    pub fn iter(self) -> impl Iterator<Item = PeerSourceFlag> {
        PeerSourceFlag::ALL
            .into_iter()
            .filter(move |source| self.contains(*source))
    }
}

impl From<PeerSourceFlag> for PeerSources {
    fn from(source: PeerSourceFlag) -> Self {
        Self(source.bit())
    }
}

impl FromIterator<PeerSourceFlag> for PeerSources {
    fn from_iter<I: IntoIterator<Item = PeerSourceFlag>>(sources: I) -> Self {
        let mut all = Self::default();
        sources.into_iter().for_each(|source| all.insert(source));
        all
    }
}

impl serde::Serialize for PeerSources {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(self.iter())
    }
}

/// Where a torrent's live run gets its peers from, and the announce table of
/// its trackers when it has any.
pub(crate) struct PeerSource {
    pub peers: PeerStream,
    pub trackers: Option<crate::tracker::TrackerStatus>,
}
pub type FileInfos = Vec<FileInfo>;
pub(crate) type FileStorage = Box<dyn TorrentStorage>;

pub type BoxAsyncReadVectored = Box<dyn AsyncReadVectored + Unpin + Send + 'static>;
pub type BoxAsyncWrite = Box<dyn AsyncWrite + Unpin + Send + 'static>;
