// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::sync::atomic::AtomicU64;

#[derive(Default, Debug)]
pub struct AtomicStats {
    pub have_bytes: AtomicU64,
    pub downloaded_and_checked_bytes: AtomicU64,
    pub downloaded_and_checked_pieces: AtomicU64,
    pub uploaded_bytes: AtomicU64,
    pub fetched_bytes: AtomicU64,
    /// Bytes received and sent that were not block payload (libtorrent
    /// `stat`, the protocol half of each channel).
    pub fetched_protocol_bytes: AtomicU64,
    pub uploaded_protocol_bytes: AtomicU64,
    pub total_piece_download_ms: AtomicU64,
    /// Bytes of the pieces that failed their hash check (libtorrent
    /// `torrent::m_total_failed_bytes`).
    pub failed_bytes: AtomicU64,
    /// Bytes of blocks received and not used (libtorrent
    /// `torrent::m_total_redundant_bytes`).
    pub redundant_bytes: AtomicU64,
    /// The part of `redundant_bytes` that is blocks cut short as their
    /// connection closed: received, but never counted in `fetched_bytes`.
    pub cut_short_bytes: AtomicU64,
    /// Blocks of a piece a peer was handed that were asked of it already,
    /// and so were not asked again.
    pub(crate) asked_already: AtomicU64,
}
