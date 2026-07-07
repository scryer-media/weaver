#[derive(Clone, Debug, Default)]
pub struct HistoryMetadataEquals {
    pub key: String,
    pub value: String,
}

#[derive(Clone, Debug, Default)]
pub struct HistoryFilter {
    pub statuses: Option<Vec<String>>,
    /// Exclude rows whose `status` is in this list (exact equality, `NOT IN`).
    ///
    /// This complements `statuses` for predicates that are more naturally
    /// expressed as an exclusion (e.g. the History-page "Failure" bucket, which
    /// is every row that is neither `complete` nor `paused`).
    pub status_not_in: Option<Vec<String>>,
    pub item_ids: Option<Vec<u64>>,
    pub category: Option<String>,
    pub metadata_has_key: Option<String>,
    pub metadata_equals: Option<HistoryMetadataEquals>,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
}
