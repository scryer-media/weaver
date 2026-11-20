// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::{KeptIntent, TorrentIntent, TorrentRecord, TorrentSource};
use weaver_server_base::jobs::ids::JobId;

pub fn torrent(job_id: u64, v1: Option<[u8; 20]>, v2: Option<[u8; 32]>) -> TorrentRecord {
    TorrentRecord {
        unpack_in_force: None,
        unpack_over: false,
        unpack_end: None,
        download_ends: 0,
        job_id: JobId(job_id),
        infohash_v1: v1,
        infohash_v2: v2,
        name: format!("torrent {job_id}"),
        source: TorrentSource::Magnet,
        metainfo: None,
        category_id: None,
        added_at_ms: 1_000 + job_id as i64,
        kept: KeptIntent::Intent(TorrentIntent::Resumed),
        forced: false,
        completed_at_ms: None,
        storage_dir: None,
        policy_override: None,
        unpack: None,
        metadata: Vec::new(),
        upload_limit: None,
        download_limit: None,
        sequential: false,
        first_last_piece_first: false,
        save_path: None,
        content_layout: None,
        files_dir: None,
        seed_mode: false,
        move_to: None,
        queue_position: job_id as i64,
        pool_member: None,
        private: false,
    }
}
