pub const CLIENT_REQUEST_ID_ATTRIBUTE_KEY: &str = "__weaver_client_request_id";
pub const DIAGNOSTIC_SOURCE_JOB_ATTRIBUTE_KEY: &str = "__weaver_diagnostic_source_job_id";
pub const DIAGNOSTIC_INCLUDE_SERVER_HOSTNAMES_ATTRIBUTE_KEY: &str =
    "__weaver_diagnostic_include_server_hostnames";

pub fn parse_history_metadata(metadata: Option<&str>) -> Vec<(String, String)> {
    metadata
        .and_then(|value| serde_json::from_str::<Vec<(String, String)>>(value).ok())
        .unwrap_or_default()
}

pub fn is_public_history_attribute_key(key: &str) -> bool {
    !matches!(
        key,
        CLIENT_REQUEST_ID_ATTRIBUTE_KEY
            | DIAGNOSTIC_SOURCE_JOB_ATTRIBUTE_KEY
            | DIAGNOSTIC_INCLUDE_SERVER_HOSTNAMES_ATTRIBUTE_KEY
    )
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
