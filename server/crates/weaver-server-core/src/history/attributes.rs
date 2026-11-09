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

// Whether a failed job's error says a decode window was over the profile's
// ceiling ("needs 4.0 GiB of decode memory, 1.0 GiB allowed"), so a reprocess
// with no limit could finish it. A window over the machine itself ("this
// machine has") would fail again, so it is not one.
pub fn failed_for_memory(error: &str) -> bool {
    error.contains("decode memory") && !error.contains("this machine has")
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

    #[test]
    fn the_no_memory_limit_flag_is_read_from_metadata_and_kept_private() {
        let flagged = vec![(NO_MEMORY_LIMIT_ATTRIBUTE_KEY.into(), "1".into())];
        assert!(no_memory_limit(&flagged));
        assert!(!no_memory_limit(&[]));
        assert!(public_history_attributes(&flagged).is_empty());
    }

    // The efficient ceiling's error offers a retry with no limit; the
    // machine's does not, nor does any other failure.
    #[test]
    fn only_a_failure_over_the_profile_ceiling_is_failed_for_memory() {
        assert!(failed_for_memory(
            "lantern.7z needs 4.0 GiB of decode memory, 512.0 MiB allowed"
        ));
        assert!(!failed_for_memory(
            "lantern.mkv.zst needs 2.0 TiB of decode memory, this machine has 8.0 GiB"
        ));
        assert!(!failed_for_memory(
            "lantern.rar needs a password and none was given"
        ));
    }
}
