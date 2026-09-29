//! NZBGet `manifest.json` v2 parsing and legacy bare-script adapter detection.
//!
//! The manifest supplies a display name, the options schema (including which
//! options are secret), and the NZBGet adapter. Anything without a manifest is a
//! bare script and runs under the SABnzbd contract unless it carries NZBGet's
//! legacy header comment.

use serde::Deserialize;
use serde_json::Value;

use super::model::{
    NzbgetCompatibilityName, NzbgetSection, OptionName, OptionValue, PostProcessingValidationError,
    QueueEvent, ScriptAdapter, ScriptKind, ScriptManifest, ScriptOption, ScriptOptionType,
    ScriptSelectValue, ScriptTaskTime,
};

/// Manifest parse failure without leaking manifest contents.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ManifestError {
    InvalidJson,
    InvalidShape,
    Validation(PostProcessingValidationError),
}

impl std::fmt::Display for ManifestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            Self::InvalidJson => "invalid script manifest JSON",
            Self::InvalidShape => "invalid script manifest shape",
            Self::Validation(error) => return error.fmt(f),
        };
        f.write_str(message)
    }
}

impl std::error::Error for ManifestError {}

impl From<PostProcessingValidationError> for ManifestError {
    fn from(error: PostProcessingValidationError) -> Self {
        Self::Validation(error)
    }
}

/// The NZBGet manifest file name looked for inside a package directory.
pub const NZBGET_MANIFEST_FILE: &str = "manifest.json";

const LEGACY_NZBGET_HEADER: &str = "### NZBGET ";
const MAX_LEGACY_PREAMBLE_LINES: usize = 64;
const MAX_LEGACY_PREAMBLE_BYTES: usize = 8 * 1024;
pub const MAX_LEGACY_METADATA_BYTES: usize = 1024 * 1024;

/// Detect NZBGet kind declarations only in the initial comment preamble.
pub fn detect_bare_script_adapter(script: &str) -> ScriptAdapter {
    if bare_script_kind_header(script).is_some() {
        ScriptAdapter::Nzbget
    } else {
        ScriptAdapter::Sabnzbd
    }
}

fn bare_script_kind_header(script: &str) -> Option<&str> {
    let script = script.strip_prefix('\u{feff}').unwrap_or(script);
    let mut inspected_bytes = 0;
    let mut saw_nonblank = false;
    for line in script.lines().take(MAX_LEGACY_PREAMBLE_LINES) {
        inspected_bytes += line.len() + 1;
        if inspected_bytes > MAX_LEGACY_PREAMBLE_BYTES {
            break;
        }
        let trimmed = line.trim_start();
        if trimmed.is_empty() {
            continue;
        }
        if !saw_nonblank && trimmed.starts_with("#!") {
            saw_nonblank = true;
            continue;
        }
        saw_nonblank = true;
        if !trimmed.starts_with('#') {
            break;
        }
        if let Some(header) = trimmed.strip_prefix(LEGACY_NZBGET_HEADER)
            && let Some((kinds, suffix)) = header.split_once(" SCRIPT")
            && suffix
                .chars()
                .all(|character| character == '#' || character.is_ascii_whitespace())
        {
            return Some(kinds);
        }
    }
    None
}

pub fn apply_bare_script_declarations(manifest: ScriptManifest, script: &str) -> ScriptManifest {
    let Some(kinds) = bare_script_kind_header(script) else {
        return manifest;
    };
    let mut queue_events = "";
    let mut task_times = "";
    let mut bytes = 0;
    for line in script.split_inclusive('\n') {
        bytes += line.len();
        if bytes > MAX_LEGACY_METADATA_BYTES {
            break;
        }
        let line = line.trim();
        if let Some(value) = line.strip_prefix("### QUEUE EVENTS:") {
            queue_events = value.trim().trim_end_matches('#').trim();
        }
        if let Some(value) = line.strip_prefix("### TASK TIME:") {
            task_times = value.trim().trim_end_matches('#').trim();
        }
    }
    apply_declarations(manifest, kinds, queue_events, task_times)
}

fn apply_declarations(
    manifest: ScriptManifest,
    kind: &str,
    queue_events: &str,
    task_times: &str,
) -> ScriptManifest {
    let kinds = ScriptKind::ALL
        .into_iter()
        .filter(|value| kind.contains(value.as_str()))
        .collect::<std::collections::BTreeSet<_>>();
    let mut problems = Vec::new();
    if kinds.is_empty()
        || kind.split('/').any(|member| {
            !ScriptKind::ALL
                .iter()
                .any(|value| member.contains(value.as_str()))
        })
    {
        problems.push("manifest contains an unrecognised script kind".to_string());
    }
    let events = if kinds.contains(&ScriptKind::Queue) {
        // Expand the empty wildcard here: an unknown-only or NZB_NAMED-only
        // declaration must never accidentally subscribe to every raised event.
        QueueEvent::ALL
            .into_iter()
            .filter(|event| queue_events.is_empty() || queue_events.contains(event.as_str()))
            .collect()
    } else {
        Default::default()
    };
    let times = if kinds.contains(&ScriptKind::Scheduler) {
        task_times
            .split([';', ','])
            .map(str::trim)
            .filter(|time| !time.is_empty())
            .filter_map(|time| match time.parse::<ScriptTaskTime>() {
                Ok(time) => Some(time),
                Err(error) => {
                    if !problems.iter().any(|problem| problem == error) {
                        problems.push(error.to_string());
                    }
                    None
                }
            })
            .collect()
    } else {
        Vec::new()
    };
    manifest.with_declarations(kinds, events, times, problems)
}

/// Parses the NZBGet v24+/v2 manifest contract.
pub fn parse_nzbget_manifest(input: &str) -> Result<ScriptManifest, ManifestError> {
    let value: Value = serde_json::from_str(input).map_err(|_| ManifestError::InvalidJson)?;
    if !value.is_object() {
        return Err(ManifestError::InvalidShape);
    }
    let raw: NzbgetManifestRaw =
        serde_json::from_value(value).map_err(|_| ManifestError::InvalidShape)?;
    let compatibility_name = NzbgetCompatibilityName::new(raw.name)?;
    let manifest = ScriptManifest::new(
        ScriptAdapter::Nzbget,
        Some(compatibility_name),
        raw.display_name,
        Some(raw.version),
        raw.main,
        raw.sections
            .into_iter()
            .filter_map(parse_nzbget_section)
            .collect(),
        raw.options
            .into_iter()
            .filter_map(parse_nzbget_option)
            .collect(),
    )?;
    Ok(apply_declarations(
        manifest,
        &raw.kind,
        &raw.queue_events,
        &raw.task_time,
    ))
}

#[derive(Deserialize)]
struct NzbgetManifestRaw {
    main: String,
    name: String,
    #[serde(rename = "displayName")]
    display_name: String,
    version: String,
    kind: String,
    #[serde(rename = "author")]
    _author: String,
    #[serde(rename = "homepage")]
    _homepage: String,
    #[serde(rename = "license")]
    _license: String,
    #[serde(rename = "about")]
    _about: String,
    #[serde(rename = "queueEvents")]
    queue_events: String,
    #[serde(rename = "taskTime")]
    task_time: String,
    #[serde(rename = "description")]
    _description: Vec<Value>,
    #[serde(rename = "requirements")]
    _requirements: Vec<Value>,
    #[serde(rename = "nzbgetMinVersion")]
    _nzbget_min_version: Option<String>,
    #[serde(default)]
    sections: Vec<Value>,
    options: Vec<Value>,
    #[serde(flatten)]
    _metadata: std::collections::BTreeMap<String, Value>,
}

#[derive(Deserialize)]
struct NzbgetSectionRaw {
    name: String,
    prefix: String,
    multi: bool,
}

fn parse_nzbget_section(value: Value) -> Option<NzbgetSection> {
    let name = value.as_object()?.get("name")?.as_str()?;
    if name.eq_ignore_ascii_case("options") {
        return None;
    }
    let raw = serde_json::from_value::<NzbgetSectionRaw>(value).ok()?;
    NzbgetSection::new(raw.name, raw.prefix, raw.multi).ok()
}

#[derive(Deserialize)]
struct NzbgetOptionRaw {
    name: String,
    value: Value,
    #[serde(default)]
    section: Option<String>,
    #[serde(rename = "displayName")]
    display_name: String,
    description: Vec<Value>,
    select: Vec<Value>,
    /// NZBGet has no secret option type; weaver honours an explicit opt-in so
    /// credentials in a manifest package go through the settings encryption
    /// envelope and are masked in the UI.
    #[serde(default)]
    secret: bool,
}

fn parse_nzbget_option(value: Value) -> Option<ScriptOption> {
    let raw = serde_json::from_value::<NzbgetOptionRaw>(value).ok()?;
    let (option_type, default) = if raw.secret {
        (ScriptOptionType::Secret, None)
    } else {
        let (option_type, default) = nzbget_option_value(raw.value).ok()?;
        (option_type, Some(default))
    };
    ScriptOption::new(
        raw.section,
        OptionName::new(raw.name).ok()?,
        option_type,
        default,
        Some(raw.display_name),
        string_values(raw.description),
        select_values(raw.select),
        false,
    )
    .ok()
}

fn nzbget_option_value(value: Value) -> Result<(ScriptOptionType, OptionValue), ManifestError> {
    match value {
        Value::String(value) => Ok((ScriptOptionType::String, OptionValue::String(value))),
        Value::Bool(value) => Ok((ScriptOptionType::Boolean, OptionValue::Boolean(value))),
        Value::Number(value) => match value.as_i64() {
            Some(value) => Ok((ScriptOptionType::Integer, OptionValue::Integer(value))),
            None => Ok((ScriptOptionType::Number, OptionValue::Number(value))),
        },
        _ => Err(ManifestError::InvalidShape),
    }
}

fn select_values(values: Vec<Value>) -> Vec<ScriptSelectValue> {
    values
        .into_iter()
        .filter_map(|value| match value {
            Value::String(value) => Some(ScriptSelectValue::String(value)),
            Value::Number(value) => Some(ScriptSelectValue::Number(value)),
            _ => None,
        })
        .collect()
}

fn string_values(values: Vec<Value>) -> Vec<String> {
    values
        .into_iter()
        .filter_map(|value| match value {
            Value::String(value) => Some(value),
            _ => None,
        })
        .collect()
}
