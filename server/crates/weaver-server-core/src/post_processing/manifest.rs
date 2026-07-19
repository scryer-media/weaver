use serde::Deserialize;
use serde_json::Value;

use super::model::{
    CommandName, DeclaredExtensionVersion, ExtensionAdapter, ExtensionCommand, ExtensionId,
    ExtensionManifest, ExtensionOption, ExtensionOptionType, ExtensionRevision,
    ExtensionSelectValue, NzbgetCompatibilityName, NzbgetSection, OptionName,
    PostProcessingValidationError, ResolvedOptionValue, VerifiedExtensionDigest,
};

/// Manifest parse failure without leaking manifest secrets.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ManifestError {
    InvalidJson,
    InvalidShape,
    UnsupportedSchema(u16),
    UnsupportedKind,
    ShapeConflict,
    Validation(PostProcessingValidationError),
}

impl std::fmt::Display for ManifestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            Self::InvalidJson => "invalid extension manifest JSON",
            Self::InvalidShape => "invalid extension manifest shape",
            Self::UnsupportedSchema(_) => "unsupported extension manifest schema",
            Self::UnsupportedKind => "unsupported extension manifest kind",
            Self::ShapeConflict => "conflicting native and NZBGet manifest shapes",
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

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum BareScriptAdapter {
    Sabnzbd,
    Nzbget,
}

impl From<BareScriptAdapter> for ExtensionAdapter {
    fn from(adapter: BareScriptAdapter) -> Self {
        match adapter {
            BareScriptAdapter::Sabnzbd => Self::Sabnzbd,
            BareScriptAdapter::Nzbget => Self::Nzbget,
        }
    }
}

const LEGACY_NZBGET_HEADER: &str = "### NZBGET POST-PROCESSING SCRIPT";
const MAX_LEGACY_PREAMBLE_LINES: usize = 64;
const MAX_LEGACY_PREAMBLE_BYTES: usize = 8 * 1024;

/// Detects only the exact NZBGet comment header in an initial blank/shebang/comment preamble.
/// Bare scripts never infer native support.
pub fn detect_bare_script_adapter(
    script: &str,
    caller_default: Option<BareScriptAdapter>,
) -> ExtensionAdapter {
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
        if let Some(suffix) = trimmed.strip_prefix(LEGACY_NZBGET_HEADER)
            && suffix
                .chars()
                .all(|character| character == '#' || character.is_ascii_whitespace())
        {
            return ExtensionAdapter::Nzbget;
        }
    }
    caller_default.unwrap_or(BareScriptAdapter::Sabnzbd).into()
}

/// Parses native schema v1 after package discovery supplied a verified package digest.
pub fn parse_native_manifest(
    input: &str,
    verified_digest: VerifiedExtensionDigest,
) -> Result<ExtensionManifest, ManifestError> {
    let value = parse_json(input)?;
    reject_native_conflicts(&value)?;
    let raw: NativeManifestV1Raw =
        serde_json::from_value(value).map_err(|_| ManifestError::InvalidShape)?;
    if raw.schema_version != 1 {
        return Err(ManifestError::UnsupportedSchema(raw.schema_version));
    }
    let adapter = match raw.kind.as_str() {
        "native" => ExtensionAdapter::Native,
        "webhook" => ExtensionAdapter::Webhook,
        _ => return Err(ManifestError::UnsupportedKind),
    };
    let revision = ExtensionRevision::from_verified(
        ExtensionId::new(raw.id)?,
        DeclaredExtensionVersion::new(raw.version)?,
        verified_digest,
    );
    build_manifest(
        adapter,
        None,
        raw.name,
        revision,
        raw.entrypoint,
        vec![],
        raw.commands
            .into_iter()
            .map(NativeCommandRaw::into_command)
            .collect::<Result<_, _>>()?,
        raw.options
            .into_iter()
            .map(NativeOptionRaw::into_option)
            .collect::<Result<_, _>>()?,
    )
}

/// Parses the NZBGet v24+/v2 manifest contract after package verification supplied its digest.
pub fn parse_nzbget_manifest(
    input: &str,
    verified_digest: VerifiedExtensionDigest,
) -> Result<ExtensionManifest, ManifestError> {
    let value = parse_json(input)?;
    reject_nzbget_conflicts(&value)?;
    let raw: NzbgetManifestRaw =
        serde_json::from_value(value).map_err(|_| ManifestError::InvalidShape)?;
    if !raw
        .kind
        .split('/')
        .any(|kind| kind.trim().eq_ignore_ascii_case("POST-PROCESSING"))
    {
        return Err(ManifestError::UnsupportedKind);
    }
    let compatibility_name = NzbgetCompatibilityName::new(raw.name)?;
    let revision = ExtensionRevision::from_verified(
        compatibility_name.weaver_extension_id()?,
        DeclaredExtensionVersion::new(raw.version)?,
        verified_digest,
    );
    build_manifest(
        ExtensionAdapter::Nzbget,
        Some(compatibility_name),
        raw.display_name,
        revision,
        raw.main,
        raw.sections
            .into_iter()
            .filter_map(parse_nzbget_section)
            .collect(),
        raw.commands
            .into_iter()
            .filter_map(parse_nzbget_command)
            .collect(),
        raw.options
            .into_iter()
            .filter_map(parse_nzbget_option)
            .collect(),
    )
}

fn parse_json(input: &str) -> Result<Value, ManifestError> {
    serde_json::from_str(input).map_err(|_| ManifestError::InvalidJson)
}

fn reject_native_conflicts(value: &Value) -> Result<(), ManifestError> {
    let Some(object) = value.as_object() else {
        return Err(ManifestError::InvalidShape);
    };
    if object.contains_key("main")
        || object.contains_key("displayName")
        || object.contains_key("sections")
        || object.contains_key("approval")
        || object.contains_key("trust")
    {
        return Err(ManifestError::ShapeConflict);
    }
    Ok(())
}

fn reject_nzbget_conflicts(value: &Value) -> Result<(), ManifestError> {
    let Some(object) = value.as_object() else {
        return Err(ManifestError::InvalidShape);
    };
    if object.contains_key("schema_version")
        || object.contains_key("id")
        || object.contains_key("entrypoint")
        || object.contains_key("approval")
        || object.contains_key("trust")
    {
        return Err(ManifestError::ShapeConflict);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn build_manifest(
    adapter: ExtensionAdapter,
    compatibility_name: Option<NzbgetCompatibilityName>,
    display_name: String,
    revision: ExtensionRevision,
    entrypoint: String,
    sections: Vec<NzbgetSection>,
    commands: Vec<ExtensionCommand>,
    options: Vec<ExtensionOption>,
) -> Result<ExtensionManifest, ManifestError> {
    ExtensionManifest::new(
        adapter,
        compatibility_name,
        display_name,
        revision,
        entrypoint,
        sections,
        commands,
        options,
    )
    .map_err(Into::into)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeManifestV1Raw {
    schema_version: u16,
    kind: String,
    id: String,
    name: String,
    version: String,
    entrypoint: String,
    #[serde(default)]
    commands: Vec<NativeCommandRaw>,
    #[serde(default)]
    options: Vec<NativeOptionRaw>,
}

#[derive(Deserialize)]
struct NativeCommandRaw {
    name: String,
    #[serde(default)]
    section: Option<String>,
    #[serde(default)]
    action: Option<String>,
    #[serde(default)]
    display_name: Option<String>,
    #[serde(default)]
    description: TextLines,
}

impl NativeCommandRaw {
    fn into_command(self) -> Result<ExtensionCommand, ManifestError> {
        let action = self.action.unwrap_or_else(|| self.name.clone());
        ExtensionCommand::new(
            self.section,
            CommandName::new(self.name)?,
            action,
            self.display_name,
            self.description.0,
        )
        .map_err(Into::into)
    }
}

#[derive(Deserialize)]
struct NativeOptionRaw {
    name: String,
    #[serde(rename = "type")]
    option_type: ExtensionOptionType,
    #[serde(default)]
    default: Option<Value>,
    #[serde(default)]
    section: Option<String>,
    #[serde(default)]
    display_name: Option<String>,
    #[serde(default)]
    description: TextLines,
    #[serde(default)]
    select: Vec<Value>,
    #[serde(default)]
    required: bool,
}

impl NativeOptionRaw {
    fn into_option(self) -> Result<ExtensionOption, ManifestError> {
        if self.option_type == ExtensionOptionType::Secret && self.default.is_some() {
            return Err(PostProcessingValidationError::InvalidOptionDefault.into());
        }
        let default = self
            .default
            .map(|value| value_to_option_value(value, self.option_type))
            .transpose()?;
        ExtensionOption::new(
            self.section,
            OptionName::new(self.name)?,
            self.option_type,
            default,
            self.display_name,
            self.description.0,
            select_values(self.select),
            self.required,
        )
        .map_err(Into::into)
    }
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
    _queue_events: String,
    #[serde(rename = "taskTime")]
    _task_time: String,
    #[serde(rename = "description")]
    _description: Vec<Value>,
    #[serde(rename = "requirements")]
    _requirements: Vec<Value>,
    #[serde(rename = "nzbgetMinVersion")]
    _nzbget_min_version: Option<String>,
    #[serde(default)]
    sections: Vec<Value>,
    commands: Vec<Value>,
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

impl NzbgetSectionRaw {
    fn into_section(self) -> Result<NzbgetSection, ManifestError> {
        NzbgetSection::new(self.name, self.prefix, self.multi).map_err(Into::into)
    }
}

fn parse_nzbget_section(value: Value) -> Option<NzbgetSection> {
    let name = value.as_object()?.get("name")?.as_str()?;
    if name.eq_ignore_ascii_case("options") {
        return None;
    }
    serde_json::from_value::<NzbgetSectionRaw>(value)
        .ok()?
        .into_section()
        .ok()
}

#[derive(Deserialize)]
struct NzbgetCommandRaw {
    name: String,
    #[serde(default)]
    section: Option<String>,
    #[serde(rename = "displayName")]
    display_name: String,
    action: String,
    description: Vec<Value>,
}

impl NzbgetCommandRaw {
    fn into_command(self) -> Result<ExtensionCommand, ManifestError> {
        ExtensionCommand::new(
            self.section,
            CommandName::new(self.name)?,
            self.action,
            Some(self.display_name),
            string_values(self.description),
        )
        .map_err(Into::into)
    }
}

fn parse_nzbget_command(value: Value) -> Option<ExtensionCommand> {
    serde_json::from_value::<NzbgetCommandRaw>(value)
        .ok()?
        .into_command()
        .ok()
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
}

impl NzbgetOptionRaw {
    fn into_option(self) -> Result<ExtensionOption, ManifestError> {
        let (option_type, default) = nzbget_option_value(self.value)?;
        ExtensionOption::new(
            self.section,
            OptionName::new(self.name)?,
            option_type,
            Some(default),
            Some(self.display_name),
            string_values(self.description),
            select_values(self.select),
            false,
        )
        .map_err(Into::into)
    }
}

fn parse_nzbget_option(value: Value) -> Option<ExtensionOption> {
    serde_json::from_value::<NzbgetOptionRaw>(value)
        .ok()?
        .into_option()
        .ok()
}

fn value_to_option_value(
    value: Value,
    option_type: ExtensionOptionType,
) -> Result<ResolvedOptionValue, ManifestError> {
    match (value, option_type) {
        (Value::String(value), ExtensionOptionType::String) => {
            Ok(ResolvedOptionValue::String(value))
        }
        (Value::Number(value), ExtensionOptionType::Integer) => value
            .as_i64()
            .map(ResolvedOptionValue::Integer)
            .ok_or(ManifestError::InvalidShape),
        (Value::Number(value), ExtensionOptionType::Number) => {
            Ok(ResolvedOptionValue::Number(value))
        }
        (Value::Bool(value), ExtensionOptionType::Boolean) => {
            Ok(ResolvedOptionValue::Boolean(value))
        }
        _ => Err(ManifestError::InvalidShape),
    }
}

fn nzbget_option_value(
    value: Value,
) -> Result<(ExtensionOptionType, ResolvedOptionValue), ManifestError> {
    match value {
        Value::String(value) => Ok((
            ExtensionOptionType::String,
            ResolvedOptionValue::String(value),
        )),
        Value::Bool(value) => Ok((
            ExtensionOptionType::Boolean,
            ResolvedOptionValue::Boolean(value),
        )),
        Value::Number(value) => match value.as_i64() {
            Some(value) => Ok((
                ExtensionOptionType::Integer,
                ResolvedOptionValue::Integer(value),
            )),
            None => Ok((
                ExtensionOptionType::Number,
                ResolvedOptionValue::Number(value),
            )),
        },
        _ => Err(ManifestError::InvalidShape),
    }
}

fn select_values(values: Vec<Value>) -> Vec<ExtensionSelectValue> {
    values
        .into_iter()
        .filter_map(|value| match value {
            Value::String(value) => Some(ExtensionSelectValue::String(value)),
            Value::Number(value) => Some(ExtensionSelectValue::Number(value)),
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

#[derive(Default)]
struct TextLines(Vec<String>);

impl<'de> Deserialize<'de> for TextLines {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum TextLinesWire {
            One(String),
            Many(Vec<String>),
        }
        match Option::<TextLinesWire>::deserialize(deserializer)? {
            None => Ok(Self::default()),
            Some(TextLinesWire::One(value)) => Ok(Self(vec![value])),
            Some(TextLinesWire::Many(values)) => Ok(Self(values)),
        }
    }
}
