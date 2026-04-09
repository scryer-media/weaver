pub const ORIGINAL_TITLE_METADATA_KEY: &str = "weaver.original_title";

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
    metadata
        .iter()
        .find(|(key, _)| key == ORIGINAL_TITLE_METADATA_KEY)
        .map(|(_, value)| value.clone())
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| job_name.to_string())
}

fn clean_original_title(raw: &str) -> String {
    raw.trim()
        .trim_end_matches(".nzb")
        .trim_end_matches(".NZB")
        .trim()
        .to_string()
}

#[cfg(test)]
mod tests;
