// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

pub const CLIENT_REQUEST_ID_ATTRIBUTE_KEY: &str = "__weaver_client_request_id";
pub const VALIDATED_ARCHIVE_PASSWORD_ATTRIBUTE_KEY: &str = "__weaver_validated_archive_password";
// Set on a history row whose job was submitted with a password. The password
// itself leaves with the active row; this keeps only that there was one.
pub const SUBMITTED_PASSWORD_ATTRIBUTE_KEY: &str = "__weaver_submitted_with_password";

// Set on a job reprocessed with no memory limit: a decode window over the
// profile's ceiling is lent from scratch, then RAM, for this job alone. It
// is kept with the job, so a restart or a restore keeps it, and a later
// reprocess sets it afresh or drops it.
pub const NO_MEMORY_LIMIT_ATTRIBUTE_KEY: &str = "__weaver_no_memory_limit";

// Whether a job's metadata asks for no memory limit.
pub fn no_memory_limit(metadata: &[(String, String)]) -> bool {
    metadata
        .iter()
        .any(|(key, value)| key == NO_MEMORY_LIMIT_ATTRIBUTE_KEY && value == "1")
}

// Set on a job that failed because a decode window was over the profile's
// ceiling under a policy that fails it, so a reprocess with no limit could
// finish it. A window over the machine itself would fail again, so it never
// sets this. A reprocess drops it.
pub const FAILED_FOR_MEMORY_ATTRIBUTE_KEY: &str = "__weaver_failed_for_memory";

// Whether a job's metadata says it failed over the profile's ceiling.
pub fn failed_for_memory(metadata: &[(String, String)]) -> bool {
    metadata
        .iter()
        .any(|(key, value)| key == FAILED_FOR_MEMORY_ATTRIBUTE_KEY && value == "1")
}

// The attributes that speak for a job's password. A job started again from
// its history row is told its password afresh, so it never inherits these.
pub fn is_password_attribute_key(key: &str) -> bool {
    key == VALIDATED_ARCHIVE_PASSWORD_ATTRIBUTE_KEY || key == SUBMITTED_PASSWORD_ATTRIBUTE_KEY
}

// Whether a history row's job had a password, submitted or validated.
pub fn history_metadata_has_password(metadata: &[(String, String)]) -> bool {
    metadata
        .iter()
        .any(|(key, _)| is_password_attribute_key(key))
}

// The full release name of a history row: the name the History page shows
// and sorts by. It is stored on the row as `release_name`, so every write of
// a row's name or metadata sets it from this.
pub fn history_release_name(name: &str, metadata: Option<&str>) -> String {
    weaver_server_base::ingest::metadata::original_release_title(
        name,
        &parse_history_metadata(metadata),
    )
}

// The stored columns a history row is sorted and searched by, from its name
// and metadata: `release_name` and `search_key`.
pub fn history_search_columns(name: &str, metadata: Option<&str>) -> (String, String) {
    let release_name = history_release_name(name, metadata);
    let display_title =
        weaver_server_base::ingest::naming::derive_release_name(Some(&release_name), Some(name));
    let mut key = String::new();
    for title in [display_title.as_str(), release_name.as_str(), name] {
        let normalized = normalize_history_search_text(title);
        if !normalized.is_empty() && !key.split('\n').any(|part| part == normalized) {
            if !key.is_empty() {
                key.push('\n');
            }
            key.push_str(&normalized);
        }
    }
    (release_name, key)
}

// A title or a search as the History search compares them: accents
// dropped, letters lowercased, and every run of dots, underscores, dashes
// and white space one space. "The.Movie.2024" and "the movie 2024" are the
// same text, and so are "Été" and "ete". A row keeps its titles one a line
// in `search_key`, and a search never holds a line break, so a match never
// spans two titles.
pub fn normalize_history_search_text(text: &str) -> String {
    use unicode_normalization::UnicodeNormalization;
    use unicode_normalization::char::is_combining_mark;

    let mut out = String::with_capacity(text.len());
    let mut space = false;
    for c in text.nfd().filter(|c| !is_combining_mark(*c)) {
        if c.is_whitespace() || matches!(c, '.' | '_' | '-') {
            space = !out.is_empty();
            continue;
        }
        if space {
            out.push(' ');
            space = false;
        }
        out.extend(c.to_lowercase());
    }
    out
}

pub fn parse_history_metadata(metadata: Option<&str>) -> Vec<(String, String)> {
    metadata
        .and_then(|value| serde_json::from_str::<Vec<(String, String)>>(value).ok())
        .unwrap_or_default()
}

// The parameter name clients use to hand over an archive password. Its
// value is a password, so it is never kept or listed as an attribute.
pub const UNPACK_PASSWORD_PARAMETER: &str = "*unpack:password";

// The keys an RSS job's item link and item id are kept under. A private
// tracker's link carries the passkey, and the id is often the link, so a
// usenet job keeps both encrypted, and only an admin reads them, through
// `Database::rss_link_attributes`. They are never a public attribute.
pub const RSS_LINK_ATTRIBUTE_KEYS: [&str; 2] = ["rss.item_url", "rss.item_id"];

pub fn is_public_history_attribute_key(key: &str) -> bool {
    let key = key.trim().to_ascii_lowercase();
    !key.starts_with("weaver.")
        && !key.starts_with("__weaver_")
        && key != UNPACK_PASSWORD_PARAMETER
        && !RSS_LINK_ATTRIBUTE_KEYS.contains(&key.as_str())
}

// The pairs of `metadata` an RSS item's link and id are kept under, as
// stored: what a view carries to give an admin through
// `Database::rss_link_attributes`, never to show as they are.
pub fn rss_link_pairs(metadata: &[(String, String)]) -> Vec<(String, String)> {
    metadata
        .iter()
        .filter(|(key, _)| RSS_LINK_ATTRIBUTE_KEYS.contains(&key.as_str()))
        .cloned()
        .collect()
}

pub fn public_history_attributes(metadata: &[(String, String)]) -> Vec<(String, String)> {
    metadata
        .iter()
        .filter(|(key, _)| is_public_history_attribute_key(key))
        .cloned()
        .collect()
}

pub fn split_history_metadata(
    metadata: &[(String, String)],
) -> (Option<String>, Vec<(String, String)>) {
    let client_request_id = metadata
        .iter()
        .find(|(key, _)| key == CLIENT_REQUEST_ID_ATTRIBUTE_KEY)
        .map(|(_, value)| value.clone());
    (client_request_id, public_history_attributes(metadata))
}
