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
pub type PeerStream = BoxStream<'static, SocketAddr>;

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
