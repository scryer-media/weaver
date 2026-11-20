// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

pub mod torrents;

// The history delete and archive of a job's files: here, above the torrents,
// because a history row never takes the files a torrent still lists or holds.
pub mod history {
    pub mod delete_files;
}
