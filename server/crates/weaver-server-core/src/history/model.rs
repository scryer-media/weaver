// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

#[derive(Clone, Debug, Default)]
pub struct HistoryMetadataEquals {
    pub key: String,
    pub value: String,
}

// Which history rows a read sees. A row a client removed with NZBGet's
// non-final `HistoryDelete` stays in the table, as NZBGet keeps it as a hidden
// duplicate, but leaves every listing a user reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HistoryRows {
    // The rows a user's history listing shows.
    Shown,
    // Every row, hidden ones included.
    All,
}

#[derive(Clone, Debug, Default)]
pub struct HistoryFilter {
    pub statuses: Option<Vec<String>>,
    // Exclude rows whose `status` is in this list (exact equality, `NOT IN`).
    //
    // This complements `statuses` for predicates that are more naturally
    // expressed as an exclusion (e.g. the History-page "Failure" bucket, which
    // is every row that is neither `complete` nor `paused`).
    pub status_not_in: Option<Vec<String>>,
    pub item_ids: Option<Vec<u64>>,
    // Exclude rows whose job id is in this list (`NOT IN`). An empty list
    // excludes nothing.
    pub item_ids_not_in: Option<Vec<u64>>,
    pub category: Option<String>,
    pub metadata_has_key: Option<String>,
    pub metadata_equals: Option<HistoryMetadataEquals>,
    // Keep rows whose job carries this tag. The empty tag keeps rows whose
    // job carries none (qBittorrent's rule, as `TagSnapshot::passes`). The
    // tag is trimmed first, as a filter's tag always is.
    pub tag: Option<String>,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
}
