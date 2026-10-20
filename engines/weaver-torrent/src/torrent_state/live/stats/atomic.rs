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
    pub total_piece_download_ms: AtomicU64,
}
