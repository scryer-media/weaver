// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

pub mod model;
pub mod persistence;
pub mod queries;
pub(crate) mod storage;

pub use model::{
    Infohash, TorrentCounters, TorrentFile, TorrentInsert, TorrentRecord, TorrentSource,
    TorrentState, TorrentTracker, TrackerStatus,
};

#[cfg(test)]
mod tests;
