//! Secret removal for everything the diagnostics package collects.
//!
//! The package is written to be attached to a bug report, so the redaction
//! rule is deliberately blunt: a key whose *name* suggests a credential loses
//! its value, whatever the value happens to look like. Matching on names rather
//! than on value shapes is what keeps a newly added credential field redacted
//! on the day it is added, without anyone having to remember this module
//! exists.

/// The single stand-in every removed value is replaced with.
pub(super) const REDACTED: &str = "<redacted>";

/// Lowercases a key and drops every non-alphanumeric character, so
/// `api_key`, `apiKey` and `API-KEY` all match the same needle.
fn normalize_key(key: &str) -> String {
    key.chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .map(|character| character.to_ascii_lowercase())
        .collect()
}

/// Needles for the GraphQL payloads (servers, settings, and anything else that
/// grows a credential field later).
///
/// `username` is on the list because a provider account name identifies the
/// subscription, and a bug report is not the place for it.
const SENSITIVE_JSON_NEEDLES: [&str; 12] = [
    "password",
    "passwd",
    "passphrase",
    "secret",
    "token",
    "apikey",
    "credential",
    "username",
    "privatekey",
    "encryptionkey",
    "jwt",
    "authorization",
];

/// Needles for on-disk configuration files, where a bare `key` is far more
/// likely to be a credential than a structural field name.
const SENSITIVE_CONFIG_NEEDLES: [&str; 4] = ["password", "secret", "key", "token"];

/// Whether a JSON object key names something that must not leave the host.
pub(super) fn is_sensitive_json_key(key: &str) -> bool {
    let normalized = normalize_key(key);
    SENSITIVE_JSON_NEEDLES
        .iter()
        .any(|needle| normalized.contains(needle))
}

/// Whether a configuration key names something that must not leave the host.
pub(super) fn is_sensitive_config_key(key: &str) -> bool {
    let normalized = normalize_key(key);
    SENSITIVE_CONFIG_NEEDLES
        .iter()
        .any(|needle| normalized.contains(needle))
}

/// Replaces every sensitively-named value in a JSON tree, in place.
///
/// A redacted key keeps its key and its null-ness: a field that was absent
/// stays absent and a field that was `null` stays `null`, so the shape of the
/// payload still tells a reader whether the credential was configured at all.
pub(super) fn redact_json(value: &mut serde_json::Value, is_sensitive: fn(&str) -> bool) {
    match value {
        serde_json::Value::Object(map) => {
            for (key, child) in map.iter_mut() {
                if child.is_null() {
                    continue;
                }
                if is_sensitive(key) {
                    *child = serde_json::Value::String(REDACTED.to_string());
                } else {
                    redact_json(child, is_sensitive);
                }
            }
        }
        serde_json::Value::Array(items) => {
            for item in items.iter_mut() {
                redact_json(item, is_sensitive);
            }
        }
        _ => {}
    }
}

/// Replaces every sensitively-named value in a parsed TOML document, in place.
pub(super) fn redact_toml(value: &mut toml::Value) {
    match value {
        toml::Value::Table(table) => {
            for (key, child) in table.iter_mut() {
                if is_sensitive_config_key(key) {
                    *child = toml::Value::String(REDACTED.to_string());
                } else {
                    redact_toml(child);
                }
            }
        }
        toml::Value::Array(items) => {
            for item in items.iter_mut() {
                redact_toml(item);
            }
        }
        _ => {}
    }
}

/// Reads a TOML document and returns it with every credential value replaced.
///
/// A file that does not parse is never passed through verbatim: an
/// unparseable config could hold a password on any line, and the whole point of
/// this module is that nothing leaves without being understood first.
pub(super) fn redact_toml_document(contents: &str) -> Result<String, toml::de::Error> {
    let mut document: toml::Value = toml::from_str(contents)?;
    redact_toml(&mut document);
    Ok(toml::to_string_pretty(&document).unwrap_or_else(|error| {
        format!("# configuration could not be re-serialized after redaction: {error}\n")
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_keys_are_matched_across_naming_conventions() {
        for key in [
            "password",
            "Password",
            "nntpPassword",
            "api_key",
            "apiKey",
            "API-KEY",
            "jwtSecret",
            "username",
            "encryption_key",
        ] {
            assert!(is_sensitive_json_key(key), "{key}");
        }

        for key in [
            "host",
            "port",
            "connections",
            "keyboardLayout",
            "monkeys",
            "tlsCaCert",
        ] {
            assert!(!is_sensitive_json_key(key), "{key}");
        }
    }

    #[test]
    fn json_redaction_replaces_nested_and_array_values() {
        let mut value = serde_json::json!({
            "servers": [
                {
                    "host": "news.example.invalid",
                    "port": 563,
                    "username": "account-one",
                    "password": "hunter2",
                    "nested": { "apiKey": "abcd", "retentionDays": 3000 }
                }
            ],
            "settings": { "jwtSecret": "s3cr3t", "dataDir": "/var/lib/weaver" }
        });

        redact_json(&mut value, is_sensitive_json_key);

        assert_eq!(value["servers"][0]["host"], "news.example.invalid");
        assert_eq!(value["servers"][0]["port"], 563);
        assert_eq!(value["servers"][0]["username"], REDACTED);
        assert_eq!(value["servers"][0]["password"], REDACTED);
        assert_eq!(value["servers"][0]["nested"]["apiKey"], REDACTED);
        assert_eq!(value["servers"][0]["nested"]["retentionDays"], 3000);
        assert_eq!(value["settings"]["jwtSecret"], REDACTED);
        assert_eq!(value["settings"]["dataDir"], "/var/lib/weaver");
    }

    #[test]
    fn json_redaction_leaves_absent_credentials_absent() {
        let mut value = serde_json::json!({ "password": null, "host": "news.example.invalid" });
        redact_json(&mut value, is_sensitive_json_key);
        assert!(value["password"].is_null());
    }

    #[test]
    fn config_redaction_strips_values_and_keeps_structure() {
        let document = "\
data_dir = \"/var/lib/weaver\"
encryption_key = \"0123456789abcdef\"

[[servers]]
host = \"news.example.invalid\"
port = 563
password = \"hunter2\"
api_token = \"t-1\"
connections = 40
";

        let redacted = redact_toml_document(document).expect("valid document");

        assert!(redacted.contains("/var/lib/weaver"));
        assert!(redacted.contains("news.example.invalid"));
        assert!(redacted.contains("connections = 40"));
        assert!(!redacted.contains("hunter2"));
        assert!(!redacted.contains("0123456789abcdef"));
        assert!(!redacted.contains("t-1"));
        assert_eq!(redacted.matches(REDACTED).count(), 3);
    }

    #[test]
    fn unparseable_config_is_refused_rather_than_passed_through() {
        assert!(redact_toml_document("this is = not = toml").is_err());
    }
}
