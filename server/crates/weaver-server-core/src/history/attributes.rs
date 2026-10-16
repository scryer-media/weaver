// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

pub const CLIENT_REQUEST_ID_ATTRIBUTE_KEY: &str = "__weaver_client_request_id";
pub const VALIDATED_ARCHIVE_PASSWORD_ATTRIBUTE_KEY: &str = "__weaver_validated_archive_password";
// Set on a history row whose job was submitted with a password. The password
// itself leaves with the active row; this keeps only that there was one.
pub const SUBMITTED_PASSWORD_ATTRIBUTE_KEY: &str = "__weaver_submitted_with_password";

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

pub fn parse_history_metadata(metadata: Option<&str>) -> Vec<(String, String)> {
    metadata
        .and_then(|value| serde_json::from_str::<Vec<(String, String)>>(value).ok())
        .unwrap_or_default()
}

pub fn is_public_history_attribute_key(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    !key.starts_with("weaver.") && !key.starts_with("__weaver_")
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn internal_metadata_is_hidden_from_history_attributes() {
        let metadata = vec![
            ("source".into(), "api".into()),
            (
                "weaver.submission.source_url".into(),
                "https://example.invalid/nzb?token=private".into(),
            ),
            (CLIENT_REQUEST_ID_ATTRIBUTE_KEY.into(), "request-1".into()),
        ];
        let (request_id, attributes) = split_history_metadata(&metadata);
        assert_eq!(request_id.as_deref(), Some("request-1"));
        assert_eq!(attributes, vec![("source".into(), "api".into())]);
    }
}
