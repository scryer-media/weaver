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
