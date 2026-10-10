// NZBGet `manifest.json` v2 parsing and legacy bare-script adapter detection.
//
// The manifest supplies a display name, the options schema (including which
// options are secret), and the NZBGet adapter. Anything without a manifest is a
// bare script and runs under the SABnzbd contract unless it carries NZBGet's
// legacy header comment. A Go script carries that header in `//` comments.

use serde::Deserialize;
use serde_json::Value;

use super::model::{
    NzbgetCompatibilityName, NzbgetSection, OptionName, OptionValue, PostProcessingValidationError,
    QueueEvent, ScriptAdapter, ScriptKind, ScriptManifest, ScriptOption, ScriptOptionType,
    ScriptSelectValue, ScriptTaskTime,
};

// Manifest parse failure without leaking manifest contents.
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

// The NZBGet manifest file name looked for inside a package directory.
pub const NZBGET_MANIFEST_FILE: &str = "manifest.json";

const LEGACY_NZBGET_HEADER: &str = "### NZBGET ";
const MAX_LEGACY_PREAMBLE_LINES: usize = 64;
const MAX_LEGACY_PREAMBLE_BYTES: usize = 8 * 1024;
pub const MAX_LEGACY_METADATA_BYTES: usize = 1024 * 1024;

// Detect NZBGet kind declarations only in the initial comment preamble.
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

// The legacy header of a Go script, as the `#` comments every other bare
// script carries it in. Go has no `#` comments, so a Go script writes the
// same lines inside its leading `//` comments: `// ### NZBGET SCAN SCRIPT`,
// `// #ApiToken=`. Each of those lines comes back without the `//` and the
// one space after it. A comment that is not a header line, `//go:build`
// among them, comes back blank, and the header ends at the first line of code.
pub fn go_script_header(script: &str) -> String {
    let script = script.strip_prefix('\u{feff}').unwrap_or(script);
    let mut header = String::new();
    for line in script.lines() {
        let line = line.trim();
        if !line.is_empty() {
            let Some(comment) = line.strip_prefix("//") else {
                break;
            };
            let comment = comment.strip_prefix(' ').unwrap_or(comment);
            if comment.starts_with('#') {
                header.push_str(comment);
            }
        }
        header.push('\n');
    }
    header
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

// Words that mark a header option as a credential when one of them is a
// whole word of the option's name. A hint for the form, not a lock: the
// operator may still save the input as plain text.
const SECRET_NAME_HINTS: [&str; 7] = [
    "key",
    "apikey",
    "token",
    "password",
    "passwd",
    "passphrase",
    "secret",
];
// `pass` names a credential only as the last word of the name (`SmtpPass`,
// `DB_PASS`); leading, as in `PassThrough`, it means something else.
const TRAILING_SECRET_NAME_HINT: &str = "pass";
const MAX_HEADER_OPTIONS: usize = 256;

// The words of an option name, lowercased: split on `_`, `-`, `.` and
// whitespace, on lower-to-upper case changes, before the last capital of an
// acronym run (`APIKey` is `api` and `key`), and between letters and digits.
fn option_name_words(name: &str) -> Vec<String> {
    let mut words = Vec::new();
    let mut current = String::new();
    let characters: Vec<char> = name.chars().collect();
    for (index, &character) in characters.iter().enumerate() {
        if !character.is_alphanumeric() {
            if !current.is_empty() {
                words.push(std::mem::take(&mut current));
            }
            continue;
        }
        if let Some(&previous) = index.checked_sub(1).and_then(|at| characters.get(at)) {
            let next = characters.get(index + 1).copied();
            let boundary = (previous.is_lowercase() && character.is_uppercase())
                || (previous.is_alphabetic() && character.is_numeric())
                || (previous.is_numeric() && character.is_alphabetic())
                || (previous.is_uppercase()
                    && character.is_uppercase()
                    && next.is_some_and(char::is_lowercase));
            if boundary && !current.is_empty() {
                words.push(std::mem::take(&mut current));
            }
        }
        current.extend(character.to_lowercase());
    }
    if !current.is_empty() {
        words.push(current);
    }
    words
}

// Whether an option's name says it holds a credential.
pub fn option_name_suggests_secret(name: &str) -> bool {
    let words = option_name_words(name);
    let is_hint = |word: &str| SECRET_NAME_HINTS.contains(&word);
    words.iter().any(|word| is_hint(word))
        || words
            .windows(2)
            .any(|pair| is_hint(&format!("{}{}", pair[0], pair[1])))
        || words
            .last()
            .is_some_and(|word| word == TRAILING_SECRET_NAME_HINT)
}

// The options a bare NZBGet script declares in its header: the `#Name=value`
// lines of the `### OPTIONS ###` section, each with the comment lines above
// it as its description. An option whose name reads as a credential is
// declared secret and loses its default, since a secret never has one.
pub fn bare_script_options(script: &str) -> Vec<ScriptOption> {
    if bare_script_kind_header(script).is_none() {
        return Vec::new();
    }
    let mut options = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    let mut in_options = false;
    let mut description = Vec::<String>::new();
    let mut bytes = 0;
    for line in script.split_inclusive('\n') {
        bytes += line.len();
        if bytes > MAX_LEGACY_METADATA_BYTES || options.len() >= MAX_HEADER_OPTIONS {
            break;
        }
        let line = line.trim();
        // A rule of `#` characters only separates parts of the header.
        if !line.is_empty() && line.chars().all(|character| character == '#') {
            continue;
        }
        if let Some(heading) = line.strip_prefix("###") {
            if in_options {
                break;
            }
            let heading = heading.trim().trim_end_matches('#').trim();
            in_options = heading.eq_ignore_ascii_case("OPTIONS")
                || heading.eq_ignore_ascii_case("OPTIONS SECTION");
            continue;
        }
        if !in_options {
            continue;
        }
        let Some(comment) = line.strip_prefix('#') else {
            if line.is_empty() {
                continue;
            }
            break;
        };
        if comment.is_empty() || comment.starts_with(char::is_whitespace) {
            let text = comment.trim();
            if !text.is_empty() {
                description.push(text.to_string());
            }
            continue;
        }
        let described = std::mem::take(&mut description);
        let Some((name, default)) = comment.split_once('=') else {
            continue;
        };
        let name = name.trim();
        let Ok(option_name) = OptionName::new(name) else {
            continue;
        };
        if !seen.insert(name.to_ascii_uppercase()) {
            continue;
        }
        let (option_type, default) = if option_name_suggests_secret(name) {
            (ScriptOptionType::Secret, None)
        } else {
            (
                ScriptOptionType::String,
                Some(OptionValue::String(default.trim().to_string())),
            )
        };
        let option = ScriptOption::new(
            None,
            option_name.clone(),
            option_type,
            default.clone(),
            None,
            described,
            Vec::new(),
            false,
        )
        .or_else(|_| {
            ScriptOption::new(
                None,
                option_name,
                option_type,
                default,
                None,
                Vec::new(),
                Vec::new(),
                false,
            )
        });
        if let Ok(option) = option {
            options.push(option);
        }
    }
    options
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

// Parses the NZBGet v24+/v2 manifest contract.
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
    // NZBGet has no secret option type; weaver honours an explicit opt-in so
    // credentials in a manifest package go through the settings encryption
    // envelope and are masked in the UI.
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
