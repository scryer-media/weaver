// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

pub const ORIGINAL_TITLE_METADATA_KEY: &str = "weaver.original_title";
// A name the job was given after submission. It replaces the NZB-derived
// name, the release title and the completed folder name, and it survives a
// restart because the NZB is parsed again on restore.
pub const RENAMED_TITLE_METADATA_KEY: &str = "weaver.renamed_title";

pub fn renamed_title(metadata: &[(String, String)]) -> Option<&str> {
    metadata
        .iter()
        .find(|(key, _)| key == RENAMED_TITLE_METADATA_KEY)
        .map(|(_, value)| value.trim())
        .filter(|value| !value.is_empty())
}

pub fn set_renamed_title(metadata: &mut Vec<(String, String)>, name: &str) {
    metadata.retain(|(key, _)| key != RENAMED_TITLE_METADATA_KEY);
    metadata.push((RENAMED_TITLE_METADATA_KEY.to_string(), name.to_string()));
}

// The files Weaver wrote under a name other than their full one: shortened
// past the filesystem's limit, cleaned of characters a filesystem refuses, or
// made unique. Each entry pairs the file's name on disk with its full name,
// which is the posted name until PAR2 or the delivery pass renames the file
// and gives it theirs. Every rename brings the record up to date, so it always
// describes the files as they are. Stored as a JSON list of
// `{name, full_name}`.
pub const FULL_FILE_NAMES_METADATA_KEY: &str = "weaver.full_file_names";
// A pathological NZB could post thousands of long names. The stored record
// never grows past this many bytes; the entries past it are not kept.
const FULL_FILE_NAMES_MAX_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FullFileName {
    // The file's name on disk.
    pub name: String,
    // The name the file should carry.
    pub full_name: String,
}

// A file renamed on disk, and the full name the rename gave it before Weaver
// shortened, cleaned or numbered it into `to`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileRename {
    pub from: String,
    pub to: String,
    pub full_name: String,
}

// Start the record for a job built from its NZB. `files` pairs each posted
// name with the name Weaver writes. A job built again over the files an
// earlier run left, after a restart or to reprocess them, keeps the record
// those files' renames brought up to date.
pub fn append_full_file_names_metadata<'a>(
    mut metadata: Vec<(String, String)>,
    files: impl IntoIterator<Item = (&'a str, &'a str)>,
) -> Vec<(String, String)> {
    if full_file_names_record(&metadata).is_none() {
        let entries = files
            .into_iter()
            .filter_map(|(full_name, name)| full_file_name(name, full_name))
            .collect();
        store_full_file_names(&mut metadata, entries);
    }
    metadata
}

// Bring the record up to date after `renames`. Returns whether it changed.
pub fn record_file_renames(metadata: &mut Vec<(String, String)>, renames: &[FileRename]) -> bool {
    if renames.is_empty() {
        return false;
    }
    let before = full_file_names_record(metadata).map(str::to_string);
    let mut entries = full_file_names(metadata);
    for rename in renames {
        entries.retain(|entry| entry.name != rename.from && entry.name != rename.to);
        entries.extend(full_file_name(&rename.to, &rename.full_name));
    }
    store_full_file_names(metadata, entries);
    full_file_names_record(metadata) != before.as_deref()
}

pub fn full_file_names_record(metadata: &[(String, String)]) -> Option<&str> {
    metadata
        .iter()
        .find(|(key, _)| key == FULL_FILE_NAMES_METADATA_KEY)
        .map(|(_, value)| value.as_str())
}

pub fn parse_full_file_names(record: &str) -> Vec<FullFileName> {
    serde_json::from_str(record).unwrap_or_default()
}

pub fn full_file_names(metadata: &[(String, String)]) -> Vec<FullFileName> {
    full_file_names_record(metadata)
        .map(parse_full_file_names)
        .unwrap_or_default()
}

fn full_file_name(name: &str, full_name: &str) -> Option<FullFileName> {
    let full_name = full_name.trim();
    (!full_name.is_empty() && full_name != name).then(|| FullFileName {
        name: name.to_string(),
        full_name: full_name.to_string(),
    })
}

fn store_full_file_names(metadata: &mut Vec<(String, String)>, entries: Vec<FullFileName>) {
    metadata.retain(|(key, _)| key != FULL_FILE_NAMES_METADATA_KEY);
    // The list's brackets, then each entry with the comma after it.
    let mut stored_bytes = 2usize;
    let kept = entries
        .into_iter()
        .take_while(|entry| {
            let entry_bytes = serde_json::to_string(entry).map_or(usize::MAX, |json| json.len());
            stored_bytes = stored_bytes.saturating_add(entry_bytes.saturating_add(1));
            stored_bytes <= FULL_FILE_NAMES_MAX_BYTES
        })
        .collect::<Vec<_>>();
    if kept.is_empty() {
        return;
    }
    if let Ok(value) = serde_json::to_string(&kept) {
        metadata.push((FULL_FILE_NAMES_METADATA_KEY.to_string(), value));
    }
}

pub fn append_original_title_metadata(
    mut metadata: Vec<(String, String)>,
    primary: Option<&str>,
    secondary: Option<&str>,
) -> Vec<(String, String)> {
    if metadata
        .iter()
        .any(|(key, _)| key == ORIGINAL_TITLE_METADATA_KEY)
    {
        return metadata;
    }

    if let Some(original_title) = [primary, secondary]
        .into_iter()
        .flatten()
        .map(clean_original_title)
        .find(|value| !value.is_empty())
    {
        metadata.push((ORIGINAL_TITLE_METADATA_KEY.to_string(), original_title));
    }

    metadata
}

pub fn original_release_title(job_name: &str, metadata: &[(String, String)]) -> String {
    if let Some(renamed) = renamed_title(metadata) {
        return renamed.to_string();
    }
    metadata
        .iter()
        .find(|(key, _)| key == ORIGINAL_TITLE_METADATA_KEY)
        .map(|(_, value)| value.clone())
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| job_name.to_string())
}

fn clean_original_title(raw: &str) -> String {
    let raw = raw.trim();
    super::strip_nzb_source_suffix(raw)
        .unwrap_or(raw)
        .trim()
        .to_string()
}

#[cfg(test)]
mod tests;
