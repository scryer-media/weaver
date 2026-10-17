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

// The name each file was posted under, for the files Weaver had to write
// under a different name (shortened, cleaned of characters a filesystem
// refuses, or made unique). Stored once, when the job is first built, as a
// JSON list of `{name, posted_name}`.
pub const POSTED_FILE_NAMES_METADATA_KEY: &str = "weaver.posted_file_names";
// A pathological NZB could post thousands of long names; past this many
// bytes the rest are not recorded.
const POSTED_FILE_NAMES_MAX_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PostedFileName {
    // The name the file has on disk.
    pub name: String,
    // The name the poster gave it.
    pub posted_name: String,
}

// Record the posted name of every file whose disk name differs from it.
// `files` pairs each posted name with the name Weaver writes. A job that
// already carries the record keeps it: a restored job is built again from its
// NZB and must report what the original submission saw.
pub fn append_posted_file_names_metadata<'a>(
    mut metadata: Vec<(String, String)>,
    files: impl IntoIterator<Item = (&'a str, &'a str)>,
) -> Vec<(String, String)> {
    if metadata
        .iter()
        .any(|(key, _)| key == POSTED_FILE_NAMES_METADATA_KEY)
    {
        return metadata;
    }

    let mut recorded = Vec::new();
    let mut recorded_bytes = 0usize;
    for (posted_name, name) in files {
        let posted_name = posted_name.trim();
        if posted_name.is_empty() || posted_name == name {
            continue;
        }
        recorded_bytes = recorded_bytes.saturating_add(posted_name.len() + name.len());
        if recorded_bytes > POSTED_FILE_NAMES_MAX_BYTES {
            break;
        }
        recorded.push(PostedFileName {
            name: name.to_string(),
            posted_name: posted_name.to_string(),
        });
    }
    if recorded.is_empty() {
        return metadata;
    }
    if let Ok(value) = serde_json::to_string(&recorded) {
        metadata.push((POSTED_FILE_NAMES_METADATA_KEY.to_string(), value));
    }
    metadata
}

pub fn posted_file_names(metadata: &[(String, String)]) -> Vec<PostedFileName> {
    metadata
        .iter()
        .find(|(key, _)| key == POSTED_FILE_NAMES_METADATA_KEY)
        .and_then(|(_, value)| serde_json::from_str(value).ok())
        .unwrap_or_default()
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
