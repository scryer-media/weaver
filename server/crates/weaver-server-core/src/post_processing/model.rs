// Typed contracts for post-processing scripts.
//
// A script has no identity beyond its name in the configured scripts directory:
// there are no revisions, digests, or trust states, so nothing here models
// package identity.

use std::collections::{BTreeSet, HashSet};
use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

// Validation failure for a post-processing contract.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum PostProcessingValidationError {
    InvalidName(&'static str),
    InvalidScriptName,
    InvalidEntrypoint,
    InvalidOptionDefault,
    DuplicateOptionName,
    DuplicateSectionName,
    DuplicateScriptName,
    InvalidPolicy,
    InvalidUnacceptableExtension,
}

impl fmt::Display for PostProcessingValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::InvalidName(field) => format!("invalid {field}"),
            Self::InvalidScriptName => "invalid script name".to_string(),
            Self::InvalidEntrypoint => "invalid script entrypoint".to_string(),
            Self::InvalidOptionDefault => "invalid script option default".to_string(),
            Self::DuplicateOptionName => "duplicate script option name".to_string(),
            Self::DuplicateSectionName => "duplicate script section name".to_string(),
            Self::DuplicateScriptName => "duplicate script name in list".to_string(),
            Self::InvalidPolicy => "invalid post-processing policy".to_string(),
            Self::InvalidUnacceptableExtension => {
                "invalid unacceptable extension pattern".to_string()
            }
        };
        f.write_str(&message)
    }
}

impl std::error::Error for PostProcessingValidationError {}

fn validate_member_name(
    value: &str,
    field: &'static str,
) -> Result<(), PostProcessingValidationError> {
    let valid_segment = |segment: &str| {
        !segment.is_empty()
            && segment
                .as_bytes()
                .first()
                .is_some_and(u8::is_ascii_alphabetic)
            && segment
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    };
    let valid = !value.is_empty()
        && value == value.trim()
        && value.len() <= 128
        && value.split('.').all(valid_segment);
    valid
        .then_some(())
        .ok_or(PostProcessingValidationError::InvalidName(field))
}

fn validate_text(value: &str, field: &'static str) -> Result<(), PostProcessingValidationError> {
    (!value.is_empty() && value == value.trim() && !value.contains('\0'))
        .then_some(())
        .ok_or(PostProcessingValidationError::InvalidName(field))
}

fn validate_bounded_metadata(
    value: &str,
    field: &'static str,
) -> Result<(), PostProcessingValidationError> {
    (!value.is_empty()
        && value == value.trim()
        && value.len() <= 128
        && !value.chars().any(char::is_control))
    .then_some(())
    .ok_or(PostProcessingValidationError::InvalidName(field))
}

// Option key declared by a manifest and supplied by the operator.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize)]
#[serde(transparent)]
pub struct OptionName(String);

impl OptionName {
    pub fn new(value: impl Into<String>) -> Result<Self, PostProcessingValidationError> {
        let value = value.into();
        validate_member_name(&value, "script option name")?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for OptionName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

// The name of a file or manifest package directory directly under the configured scripts root.
//
// This is the script's whole identity — the same one SABnzbd and NZBGet use.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize)]
#[serde(transparent)]
pub struct ScriptName(String);

impl ScriptName {
    pub fn new(value: impl Into<String>) -> Result<Self, PostProcessingValidationError> {
        let value = value.into();
        let invalid = value.is_empty()
            || value != value.trim()
            || value.len() > 255
            || value.contains(['/', '\\', ':', '\0'])
            || value.chars().any(char::is_control)
            || value.starts_with('.')
            || value.ends_with(['.', ' '])
            || is_windows_device_component(&value);
        if invalid {
            return Err(PostProcessingValidationError::InvalidScriptName);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ScriptName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for ScriptName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

// Exact NZBGet manifest name, retained because its legacy environment contract uses it verbatim.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize)]
#[serde(transparent)]
pub struct NzbgetCompatibilityName(String);

impl NzbgetCompatibilityName {
    pub fn new(value: impl Into<String>) -> Result<Self, PostProcessingValidationError> {
        let value = value.into();
        if value.is_empty()
            || value != value.trim()
            || value.len() > 128
            || value
                .bytes()
                .any(|byte| byte == 0 || byte.is_ascii_control())
        {
            return Err(PostProcessingValidationError::InvalidName(
                "NZBGet compatibility name",
            ));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for NzbgetCompatibilityName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

// Environment contract a script is executed under.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScriptAdapter {
    Sabnzbd,
    Nzbget,
}

impl ScriptAdapter {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Sabnzbd => "sabnzbd",
            Self::Nzbget => "nzbget",
        }
    }
}

// Typed built-in pipeline failure stage.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PipelineFailureStage {
    Download,
    Verify,
    Repair,
    Extract,
    Move,
}

// Pipeline result, which stays independent of the post-processing result.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "status")]
pub enum PipelineOutcome {
    Succeeded,
    Failed {
        stage: PipelineFailureStage,
        code: String,
        message: String,
    },
}

// Value type accepted by a script option declaration.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScriptOptionType {
    String,
    Integer,
    Number,
    Boolean,
    Secret,
}

// Secret option value that is deliberately impossible to deserialize through generic serde.
#[derive(Clone, Eq, PartialEq)]
pub struct SecretOptionValue(String);

impl SecretOptionValue {
    // Construct a secret at an authenticated administrative input boundary.
    pub fn from_admin_input(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub(crate) fn for_execution(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub(crate) fn expose_for_execution(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretOptionValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SecretOptionValue([REDACTED])")
    }
}

impl Serialize for SecretOptionValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str("[REDACTED]")
    }
}

impl<'de> Deserialize<'de> for SecretOptionValue {
    fn deserialize<D>(_deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Err(serde::de::Error::custom(
            "secret values require an explicit encryption or execution boundary",
        ))
    }
}

// Concrete resolved option value. Decimal JSON numbers retain their original JSON number form.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum OptionValue {
    String(String),
    Integer(i64),
    Number(serde_json::Number),
    Boolean(bool),
    Secret(SecretOptionValue),
}

impl OptionValue {
    pub fn matches_type(&self, option_type: ScriptOptionType) -> bool {
        matches!(
            (self, option_type),
            (Self::String(_), ScriptOptionType::String)
                | (Self::Integer(_), ScriptOptionType::Integer)
                | (Self::Number(_), ScriptOptionType::Number)
                | (Self::Boolean(_), ScriptOptionType::Boolean)
                | (Self::Secret(_), ScriptOptionType::Secret)
        )
    }

    pub fn is_secret(&self) -> bool {
        matches!(self, Self::Secret(_))
    }
}

// NZBGet select entry, preserving documented string or numeric values.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ScriptSelectValue {
    String(String),
    Number(serde_json::Number),
}

// Resolved option with a validated name.
#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
pub struct ResolvedOption {
    name: OptionName,
    value: OptionValue,
}

impl ResolvedOption {
    pub fn new(name: OptionName, value: OptionValue) -> Self {
        Self { name, value }
    }

    pub fn name(&self) -> &OptionName {
        &self.name
    }

    pub fn value(&self) -> &OptionValue {
        &self.value
    }
}

#[derive(Deserialize)]
struct ResolvedOptionWire {
    name: OptionName,
    value: OptionValue,
}

impl<'de> Deserialize<'de> for ResolvedOption {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ResolvedOptionWire::deserialize(deserializer)?;
        Ok(Self::new(wire.name, wire.value))
    }
}

// Named NZBGet section metadata retained from a v2 manifest.
#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
pub struct NzbgetSection {
    name: String,
    prefix: String,
    multi: bool,
}

impl NzbgetSection {
    pub fn new(
        name: String,
        prefix: String,
        multi: bool,
    ) -> Result<Self, PostProcessingValidationError> {
        validate_bounded_metadata(&name, "NZBGet section name")?;
        if name.eq_ignore_ascii_case("options") {
            return Err(PostProcessingValidationError::InvalidName("NZBGet section"));
        }
        validate_bounded_metadata(&prefix, "NZBGet section prefix")?;
        Ok(Self {
            name,
            prefix,
            multi,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    pub fn multi(&self) -> bool {
        self.multi
    }
}

#[derive(Deserialize)]
struct NzbgetSectionWire {
    name: String,
    prefix: String,
    multi: bool,
}

impl<'de> Deserialize<'de> for NzbgetSection {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = NzbgetSectionWire::deserialize(deserializer)?;
        Self::new(wire.name, wire.prefix, wire.multi).map_err(serde::de::Error::custom)
    }
}

// Validated script option declaration.
#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
pub struct ScriptOption {
    section: Option<String>,
    name: OptionName,
    option_type: ScriptOptionType,
    default: Option<OptionValue>,
    display_name: Option<String>,
    description: Vec<String>,
    select: Vec<ScriptSelectValue>,
    required: bool,
}

impl ScriptOption {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        section: Option<String>,
        name: OptionName,
        option_type: ScriptOptionType,
        default: Option<OptionValue>,
        display_name: Option<String>,
        description: Vec<String>,
        select: Vec<ScriptSelectValue>,
        required: bool,
    ) -> Result<Self, PostProcessingValidationError> {
        let section = normalize_section_reference(section)?;
        if option_type == ScriptOptionType::Secret && default.is_some() {
            return Err(PostProcessingValidationError::InvalidOptionDefault);
        }
        if default
            .as_ref()
            .is_some_and(|value| !value.matches_type(option_type))
        {
            return Err(PostProcessingValidationError::InvalidOptionDefault);
        }
        if let Some(display_name) = &display_name {
            validate_text(display_name, "script option display name")?;
        }
        validate_metadata(&description, "script option description")?;
        for value in &select {
            match value {
                ScriptSelectValue::String(value) => {
                    validate_text(value, "script option select value")?;
                }
                ScriptSelectValue::Number(_) => {}
            }
        }
        Ok(Self {
            section,
            name,
            option_type,
            default,
            display_name,
            description,
            select,
            required,
        })
    }

    pub fn section(&self) -> Option<&str> {
        self.section.as_deref()
    }

    pub fn name(&self) -> &OptionName {
        &self.name
    }

    pub fn option_type(&self) -> ScriptOptionType {
        self.option_type
    }

    pub fn default(&self) -> Option<&OptionValue> {
        self.default.as_ref()
    }

    pub fn display_name(&self) -> Option<&str> {
        self.display_name.as_deref()
    }

    pub fn description(&self) -> &[String] {
        &self.description
    }

    pub fn select(&self) -> &[ScriptSelectValue] {
        &self.select
    }

    pub fn required(&self) -> bool {
        self.required
    }

    pub fn is_secret(&self) -> bool {
        self.option_type == ScriptOptionType::Secret
    }
}

#[derive(Deserialize)]
struct ScriptOptionWire {
    section: Option<String>,
    name: OptionName,
    option_type: ScriptOptionType,
    default: Option<OptionValue>,
    display_name: Option<String>,
    #[serde(default)]
    description: Vec<String>,
    #[serde(default)]
    select: Vec<ScriptSelectValue>,
    #[serde(default)]
    required: bool,
}

impl<'de> Deserialize<'de> for ScriptOption {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ScriptOptionWire::deserialize(deserializer)?;
        Self::new(
            wire.section,
            wire.name,
            wire.option_type,
            wire.default,
            wire.display_name,
            wire.description,
            wire.select,
            wire.required,
        )
        .map_err(serde::de::Error::custom)
    }
}

#[path = "script_events.rs"]
mod script_events;
pub use script_events::{QueueEvent, ScriptEventLabel, ScriptKind, ScriptTaskTime};

// Validated internal representation of a discovered script manifest.
//
// A bare executable synthesizes one of these with no options and the file name
// as both display name and entrypoint.
#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
pub struct ScriptManifest {
    adapter: ScriptAdapter,
    kinds: BTreeSet<ScriptKind>,
    queue_events: BTreeSet<QueueEvent>,
    task_times: Vec<ScriptTaskTime>,
    declaration_problems: Vec<String>,
    compatibility_name: Option<NzbgetCompatibilityName>,
    display_name: String,
    version: Option<String>,
    entrypoint: String,
    sections: Vec<NzbgetSection>,
    options: Vec<ScriptOption>,
}

impl ScriptManifest {
    pub fn new(
        adapter: ScriptAdapter,
        compatibility_name: Option<NzbgetCompatibilityName>,
        display_name: String,
        version: Option<String>,
        entrypoint: String,
        sections: Vec<NzbgetSection>,
        options: Vec<ScriptOption>,
    ) -> Result<Self, PostProcessingValidationError> {
        validate_text(&display_name, "script display name")?;
        if let Some(version) = &version {
            validate_bounded_metadata(version, "script version")?;
        }
        validate_relative_entrypoint(&entrypoint)?;
        if compatibility_name.is_some() && adapter != ScriptAdapter::Nzbget {
            return Err(PostProcessingValidationError::InvalidName(
                "script compatibility name",
            ));
        }
        validate_sections(&sections)?;
        validate_unique_option_names(&options)?;
        Ok(Self {
            adapter,
            kinds: BTreeSet::from([ScriptKind::PostProcessing]),
            queue_events: BTreeSet::new(),
            task_times: Vec::new(),
            declaration_problems: Vec::new(),
            compatibility_name,
            display_name,
            version,
            entrypoint,
            sections,
            options,
        })
    }

    pub fn adapter(&self) -> ScriptAdapter {
        self.adapter
    }

    pub(crate) fn with_declarations(
        mut self,
        kinds: BTreeSet<ScriptKind>,
        queue_events: BTreeSet<QueueEvent>,
        task_times: Vec<ScriptTaskTime>,
        declaration_problems: Vec<String>,
    ) -> Self {
        self.kinds = kinds;
        self.queue_events = queue_events;
        self.task_times = task_times;
        self.declaration_problems = declaration_problems;
        self
    }

    // Recognised kinds; unknown-only declarations are listed with a problem and cannot run.
    pub fn kinds(&self) -> &BTreeSet<ScriptKind> {
        &self.kinds
    }

    // Explicit subscriptions. An empty NZBGet declaration is expanded to every event on parse.
    pub fn queue_events(&self) -> &BTreeSet<QueueEvent> {
        &self.queue_events
    }

    pub fn task_times(&self) -> &[ScriptTaskTime] {
        &self.task_times
    }

    pub fn declaration_problems(&self) -> &[String] {
        &self.declaration_problems
    }

    pub fn compatibility_name(&self) -> Option<&NzbgetCompatibilityName> {
        self.compatibility_name.as_ref()
    }

    pub fn display_name(&self) -> &str {
        &self.display_name
    }

    pub fn version(&self) -> Option<&str> {
        self.version.as_deref()
    }

    pub fn entrypoint(&self) -> &str {
        &self.entrypoint
    }

    pub fn sections(&self) -> &[NzbgetSection] {
        &self.sections
    }

    pub fn options(&self) -> &[ScriptOption] {
        &self.options
    }

    // Merge operator-supplied values over manifest defaults, rejecting undeclared or mistyped keys.
    pub fn resolve_options(
        &self,
        supplied: &[ResolvedOption],
    ) -> Result<Vec<ResolvedOption>, PostProcessingValidationError> {
        let mut supplied_by_name = std::collections::HashMap::new();
        for option in supplied {
            let key = option.name().as_str().to_ascii_lowercase();
            if supplied_by_name.insert(key.clone(), option).is_some() {
                return Err(PostProcessingValidationError::DuplicateOptionName);
            }
            let declaration = self
                .options
                .iter()
                .find(|declaration| declaration.name().as_str().to_ascii_lowercase() == key)
                .ok_or(PostProcessingValidationError::InvalidName("script option"))?;
            if !option.value().matches_type(declaration.option_type()) {
                return Err(PostProcessingValidationError::InvalidOptionDefault);
            }
        }

        let mut resolved = Vec::with_capacity(self.options.len());
        for declaration in &self.options {
            let key = declaration.name().as_str().to_ascii_lowercase();
            let value = supplied_by_name
                .get(&key)
                .map(|option| option.value().clone())
                .or_else(|| declaration.default().cloned());
            let Some(value) = value else {
                if declaration.required() {
                    return Err(PostProcessingValidationError::InvalidName(
                        "required script option",
                    ));
                }
                continue;
            };
            resolved.push(ResolvedOption::new(declaration.name().clone(), value));
        }
        Ok(resolved)
    }
}

fn validate_relative_entrypoint(entrypoint: &str) -> Result<(), PostProcessingValidationError> {
    let bytes = entrypoint.as_bytes();
    let windows_drive = bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':';
    let components = entrypoint.split(['/', '\\']).collect::<Vec<_>>();
    let invalid_component = components.iter().any(|component| {
        component.is_empty()
            || matches!(*component, "." | "..")
            || component.contains(':')
            || component.chars().any(char::is_control)
            || component.ends_with(['.', ' '])
            || is_windows_device_component(component)
    });
    if entrypoint.is_empty()
        || entrypoint != entrypoint.trim()
        || entrypoint.contains('\0')
        || entrypoint.starts_with(['/', '\\'])
        || entrypoint.ends_with(['/', '\\'])
        || windows_drive
        || invalid_component
    {
        return Err(PostProcessingValidationError::InvalidEntrypoint);
    }
    Ok(())
}

fn is_windows_device_component(component: &str) -> bool {
    let normalized = component.trim_end_matches(['.', ' ']);
    let base = normalized
        .split('.')
        .next()
        .unwrap_or_default()
        .trim_end_matches(['.', ' '])
        .to_ascii_lowercase();
    matches!(
        base.as_str(),
        "con"
            | "prn"
            | "aux"
            | "nul"
            | "clock$"
            | "conin$"
            | "conout$"
            | "com1"
            | "com2"
            | "com3"
            | "com4"
            | "com5"
            | "com6"
            | "com7"
            | "com8"
            | "com9"
            | "com¹"
            | "com²"
            | "com³"
            | "lpt1"
            | "lpt2"
            | "lpt3"
            | "lpt4"
            | "lpt5"
            | "lpt6"
            | "lpt7"
            | "lpt8"
            | "lpt9"
            | "lpt¹"
            | "lpt²"
            | "lpt³"
    )
}

fn normalize_section_reference(
    section: Option<String>,
) -> Result<Option<String>, PostProcessingValidationError> {
    match section {
        None => Ok(None),
        Some(section) if section.eq_ignore_ascii_case("options") => Ok(None),
        Some(section) => {
            validate_bounded_metadata(&section, "script section")?;
            Ok(Some(section))
        }
    }
}

fn validate_metadata(
    values: &[String],
    field: &'static str,
) -> Result<(), PostProcessingValidationError> {
    values
        .iter()
        .try_for_each(|value| validate_text(value, field))
}

fn normalized_section_key(section: Option<&str>) -> String {
    match section {
        None => String::new(),
        Some(section) if section.eq_ignore_ascii_case("options") => String::new(),
        Some(section) => section.to_ascii_lowercase(),
    }
}

fn validate_unique_option_names(
    options: &[ScriptOption],
) -> Result<(), PostProcessingValidationError> {
    let mut seen = HashSet::new();
    for option in options {
        let key = format!(
            "{}\u{1f}{}",
            normalized_section_key(option.section()),
            option.name().as_str().to_ascii_lowercase()
        );
        if !seen.insert(key) {
            return Err(PostProcessingValidationError::DuplicateOptionName);
        }
    }
    Ok(())
}

fn validate_sections(sections: &[NzbgetSection]) -> Result<(), PostProcessingValidationError> {
    let mut names = HashSet::new();
    if sections
        .iter()
        .all(|section| names.insert(section.name().to_ascii_lowercase()))
    {
        Ok(())
    } else {
        Err(PostProcessingValidationError::DuplicateSectionName)
    }
}

// Whether the instances that run for every category still run for a
// download whose category has instances of its own.
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GlobalScriptsRun {
    // Unscoped instances first, then the category's own.
    #[default]
    Always,
    // A category with instances of its own runs only those.
    OnlyWithoutCategoryScripts,
}

impl GlobalScriptsRun {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Always => "always",
            Self::OnlyWithoutCategoryScripts => "only_without_category_scripts",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct EventScriptSettings {
    pub event_script_timeout_seconds: u64,
    pub file_downloaded_event_interval: i64,
    // The newest runs kept for each download (or each scan, schedule or feed
    // for runs that belong to no download).
    pub script_output_runs_per_job: u32,
    // Failed runs kept beyond `script_output_runs_per_job`, newest first.
    pub script_output_failed_runs_per_job: u32,
}

impl Default for EventScriptSettings {
    fn default() -> Self {
        Self {
            event_script_timeout_seconds: 300,
            file_downloaded_event_interval: 0,
            script_output_runs_per_job: 32,
            script_output_failed_runs_per_job: 8,
        }
    }
}

impl EventScriptSettings {
    pub fn validate(&self) -> Result<(), PostProcessingValidationError> {
        if !(1..=86_400).contains(&self.event_script_timeout_seconds)
            || !(-1..=86_400).contains(&self.file_downloaded_event_interval)
            || !(1..=128).contains(&self.script_output_runs_per_job)
            || self.script_output_failed_runs_per_job > 128
        {
            return Err(PostProcessingValidationError::InvalidPolicy);
        }
        Ok(())
    }
}

pub const DEFAULT_CONCURRENCY: u8 = 32;
pub const MAX_CONCURRENCY: u8 = 128;

// Settings the operator controls. Execution is off until it is explicitly turned on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PostProcessingSettings {
    #[serde(flatten)]
    pub event_scripts: EventScriptSettings,
    pub execution_enabled: bool,
    // Scripts allowed to run at once, of every kind: the ones a job waits for
    // and the ones it does not, queue events, scans, feeds and schedules.
    pub concurrency: u8,
    pub termination_grace_seconds: u64,
    pub python_interpreter: Option<String>,
    pub powershell_interpreter: Option<String>,
    pub batch_interpreter: Option<String>,
    // The `go` command that runs `.go` scripts.
    #[serde(default)]
    pub go_interpreter: Option<String>,
    // Extension-token patterns that reject a job only after Weaver has a
    // trustworthy output name. An empty list disables the policy. A saved
    // document without the field predates the default list and reads it.
    #[serde(default = "default_unacceptable_extensions")]
    pub unacceptable_extensions: Vec<String>,
    #[serde(default)]
    pub global_scripts_run: GlobalScriptsRun,
}

impl Default for PostProcessingSettings {
    fn default() -> Self {
        Self {
            execution_enabled: false,
            event_scripts: EventScriptSettings::default(),
            concurrency: DEFAULT_CONCURRENCY,
            termination_grace_seconds: 10,
            python_interpreter: None,
            powershell_interpreter: None,
            batch_interpreter: None,
            go_interpreter: None,
            unacceptable_extensions: default_unacceptable_extensions(),
            global_scripts_run: GlobalScriptsRun::default(),
        }
    }
}

impl PostProcessingSettings {
    // Return a canonical settings value suitable for persistence.
    pub fn normalized(mut self) -> Result<Self, PostProcessingValidationError> {
        self.unacceptable_extensions = self
            .unacceptable_extensions
            .into_iter()
            .map(|entry| normalize_unacceptable_extension(&entry))
            .collect::<Result<Vec<_>, _>>()?;
        self.unacceptable_extensions.sort();
        self.unacceptable_extensions.dedup();
        if self.unacceptable_extensions.len() > 64 {
            return Err(PostProcessingValidationError::InvalidUnacceptableExtension);
        }
        self.validate()?;
        Ok(self)
    }

    pub fn validate(&self) -> Result<(), PostProcessingValidationError> {
        self.event_scripts.validate()?;
        if !(1..=MAX_CONCURRENCY).contains(&self.concurrency) || self.termination_grace_seconds == 0
        {
            return Err(PostProcessingValidationError::InvalidPolicy);
        }
        if self.unacceptable_extensions.len() > 64
            || self.unacceptable_extensions.iter().any(|entry| {
                normalize_unacceptable_extension(entry).as_deref() != Ok(entry.as_str())
            })
        {
            return Err(PostProcessingValidationError::InvalidUnacceptableExtension);
        }
        Ok(())
    }

    // Return the matching configured extension-token pattern for `filename`.
    // Paths and leading-dot files without a basename are not treated as having
    // an extension, matching SABnzbd's extension semantics.
    pub fn unacceptable_extension_match<'a>(&'a self, filename: &str) -> Option<&'a str> {
        let basename = filename.rsplit(['/', '\\']).next()?;
        let (stem, extension) = basename.rsplit_once('.')?;
        if stem.is_empty() || extension.is_empty() {
            return None;
        }
        self.unacceptable_extensions
            .iter()
            .find(|pattern| crate::runtime::glob::glob_match_ci(pattern, extension))
            .map(String::as_str)
    }
}

// The unwanted extension list a new install starts with, sorted as a saved
// list is normalized. An empty list turns the check off.
pub const DEFAULT_UNACCEPTABLE_EXTENSIONS: [&str; 10] = [
    "bat", "cmd", "com", "exe", "js", "lnk", "msi", "ps1", "scr", "vbs",
];

fn default_unacceptable_extensions() -> Vec<String> {
    DEFAULT_UNACCEPTABLE_EXTENSIONS
        .iter()
        .map(|extension| (*extension).to_string())
        .collect()
}

// The reason a job fails with when the unwanted extension policy refuses
// `relative_path`: the file's extension and the file that carried it, as
// the consumer app and the Jobs screen show it.
pub fn unwanted_extension_reason(relative_path: &str) -> String {
    let relative_path = relative_path.replace('\\', "/");
    let extension = relative_path
        .rsplit('/')
        .next()
        .and_then(|name| name.rsplit_once('.'))
        .map(|(_, extension)| extension.to_ascii_lowercase())
        .unwrap_or_default();
    format!("unwanted extension '.{extension}' in '{relative_path}'")
}

fn normalize_unacceptable_extension(value: &str) -> Result<String, PostProcessingValidationError> {
    let value = value.trim();
    let valid = !value.is_empty()
        && value.len() <= 64
        && !value.starts_with('.')
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'+' | b'*' | b'?')
        });
    valid
        .then(|| value.to_ascii_lowercase())
        .ok_or(PostProcessingValidationError::InvalidUnacceptableExtension)
}

// Job-level rollup of every script that ran.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PostProcessingSummary {
    NotRun,
    // Durable marker that a job entered post-processing. It is only ever
    // observed after a crash, where the startup scan turns it into
    // `Interrupted`; a clean pass always overwrites it with its own rollup.
    Running,
    Succeeded,
    Warning,
    Failed,
    Cancelled,
    Interrupted,
}

impl PostProcessingSummary {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NotRun => "not_run",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Warning => "warning",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Interrupted => "interrupted",
        }
    }

    pub fn from_persisted(value: &str) -> Option<Self> {
        match value {
            "not_run" => Some(Self::NotRun),
            "running" => Some(Self::Running),
            "succeeded" => Some(Self::Succeeded),
            "warning" => Some(Self::Warning),
            "failed" => Some(Self::Failed),
            "cancelled" => Some(Self::Cancelled),
            "interrupted" => Some(Self::Interrupted),
            _ => None,
        }
    }
}

// Worst-of merge: the job rollup reports the most severe script outcome.
pub fn merge_post_processing_summary(
    current: PostProcessingSummary,
    incoming: PostProcessingSummary,
) -> PostProcessingSummary {
    use PostProcessingSummary::{
        Cancelled, Failed, Interrupted, NotRun, Running, Succeeded, Warning,
    };

    match (current, incoming) {
        (Cancelled, _) | (_, Cancelled) => Cancelled,
        (Interrupted, _) | (_, Interrupted) => Interrupted,
        (Failed, _) | (_, Failed) => Failed,
        (Warning, _) | (_, Warning) => Warning,
        (Succeeded, _) | (_, Succeeded) => Succeeded,
        // `Running` is a durability marker, never a script outcome, so it can
        // only appear here as a stale value and must not win.
        (Running, other) | (other, Running) => other,
        (NotRun, NotRun) => NotRun,
    }
}

// Terminal state of one script execution.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScriptStatus {
    Succeeded,
    // NZBGet exit 95 (`NONE`): the script decided it had nothing to do.
    Skipped,
    Warning,
    Failed,
    TimedOut,
    Cancelled,
    // Weaver stopped while the script was running, so how it ended is
    // unknown. It is never run again.
    Interrupted,
}

impl ScriptStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Skipped => "skipped",
            Self::Warning => "warning",
            Self::Failed => "failed",
            Self::TimedOut => "timed_out",
            Self::Cancelled => "cancelled",
            Self::Interrupted => "interrupted",
        }
    }

    pub fn from_persisted(value: &str) -> Option<Self> {
        match value {
            "succeeded" => Some(Self::Succeeded),
            "skipped" => Some(Self::Skipped),
            "warning" => Some(Self::Warning),
            "failed" => Some(Self::Failed),
            "timed_out" => Some(Self::TimedOut),
            "cancelled" => Some(Self::Cancelled),
            "interrupted" => Some(Self::Interrupted),
            _ => None,
        }
    }

    pub fn summary(self) -> PostProcessingSummary {
        match self {
            Self::Succeeded | Self::Skipped => PostProcessingSummary::Succeeded,
            Self::Warning => PostProcessingSummary::Warning,
            Self::Failed | Self::TimedOut => PostProcessingSummary::Failed,
            Self::Cancelled => PostProcessingSummary::Cancelled,
            Self::Interrupted => PostProcessingSummary::Interrupted,
        }
    }
}

// What one script did, appended to the job's events and stored on the job row.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScriptResult {
    pub script: ScriptName,
    // The instance that ran, when the run came from one. Its name is kept as
    // it was at the time, so a result still reads right after a rename.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance_name: Option<String>,
    #[serde(default)]
    pub event: ScriptEventLabel,
    #[serde(default)]
    pub output_id: Option<String>,
    // The run was started without anything waiting for it.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub background: bool,
    pub adapter: ScriptAdapter,
    pub status: ScriptStatus,
    pub exit_code: Option<i32>,
    pub duration_ms: u64,
    #[serde(default)]
    pub output_tail: String,
    #[serde(default)]
    pub output_truncated: bool,
    #[serde(default)]
    pub error_message: Option<String>,
    pub finished_at_epoch_ms: i64,
}

impl ScriptResult {
    // What the run is called wherever it is shown: the instance's name, or
    // the script's for a result recorded before there were instances.
    pub fn label(&self) -> &str {
        self.instance_name
            .as_deref()
            .unwrap_or_else(|| self.script.as_str())
    }
}

// One entry of a job's list that had started when it was last recorded. A
// started entry never runs again for the same job, whatever became of it.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StartedScript {
    pub id: String,
    pub name: String,
    pub script: ScriptName,
    pub waited: bool,
}

// What a job's post-processing scripts are told about how its download
// ended, kept on the job when the pass begins so a resumed pass tells the
// rest of its scripts the same.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PostProcessingFacts {
    pub outcome: PipelineOutcome,
    pub par_status: i32,
    pub unpack_status: i32,
}

impl PostProcessingFacts {
    // The failure the job is finished with after its scripts, when its
    // download failed.
    pub fn primary_failure(&self) -> Option<String> {
        match &self.outcome {
            PipelineOutcome::Failed { message, .. } => Some(message.clone()),
            PipelineOutcome::Succeeded => None,
        }
    }
}

// What a job's interrupted pass left behind: the entries that had started,
// in the order they started, and the results of the ones that finished.
#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct PostProcessingResume {
    pub started: Vec<StartedScript>,
    pub results: Vec<ScriptResult>,
}

impl PostProcessingResume {
    // The results the resumed pass starts from, in the order their entries
    // started. A finished entry keeps its result; a waited entry with none
    // becomes an interrupted row; a background entry was never part of the
    // results.
    pub fn carried_results(&self) -> Vec<ScriptResult> {
        let mut results = Vec::with_capacity(self.started.len());
        for entry in &self.started {
            let kept = self
                .results
                .iter()
                .find(|result| result.instance_id.as_deref() == Some(entry.id.as_str()));
            match kept {
                Some(result) => results.push(result.clone()),
                None if entry.waited => {
                    results.push(ScriptResult {
                        script: entry.script.clone(),
                        instance_id: Some(entry.id.clone()),
                        instance_name: Some(entry.name.clone()),
                        event: Default::default(),
                        output_id: None,
                        background: false,
                        adapter: ScriptAdapter::Sabnzbd,
                        status: ScriptStatus::Interrupted,
                        exit_code: None,
                        duration_ms: 0,
                        output_tail: String::new(),
                        output_truncated: false,
                        error_message: None,
                        finished_at_epoch_ms: chrono::Utc::now().timestamp_millis(),
                    });
                }
                None => {}
            }
        }
        results
    }
}
