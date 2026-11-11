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

// The column a history listing is ordered by. Ties go to the job id, in the
// same direction.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum HistoryOrderKey {
    #[default]
    CompletedAt,
    TotalBytes,
    Health,
}

// The order a history listing returns its rows in: newest first unless a
// caller asks otherwise.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HistoryOrder {
    pub key: HistoryOrderKey,
    pub descending: bool,
}

impl Default for HistoryOrder {
    fn default() -> Self {
        Self {
            key: HistoryOrderKey::CompletedAt,
            descending: true,
        }
    }
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
    // Keep rows whose category is one of these, a row with no category
    // counting as the empty string. An empty list keeps nothing.
    pub categories_any: Option<Vec<String>>,
    pub metadata_has_key: Option<String>,
    pub metadata_equals: Option<HistoryMetadataEquals>,
    pub order: HistoryOrder,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
}

// The columns a history page is matched and ordered by, without the row's
// other columns. `name` and `metadata` are empty unless the read asked for
// titles.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HistoryKeyRow {
    pub job_id: u64,
    pub name: String,
    pub status: String,
    pub total_bytes: u64,
    pub health: u32,
    pub category: Option<String>,
    pub completed_at: i64,
    pub metadata: Option<String>,
}
