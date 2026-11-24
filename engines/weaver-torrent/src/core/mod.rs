// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

pub mod compact_ip;
pub mod constants;
mod error;
pub mod hash_id;
pub mod lengths;
pub mod magnet;
pub mod merkle;
pub mod net;
pub mod peer_id;
pub mod spawn_utils;
pub mod speed_estimator;
pub mod torrent_metainfo;
#[cfg(test)]
pub(crate) mod v2_test_torrent;
pub mod v2_tree;
pub use hash_id::{Id20, Id32, InfoHashes};

pub use error::Error;
pub type Result<T> = std::result::Result<T, Error>;
