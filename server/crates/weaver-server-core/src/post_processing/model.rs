use std::collections::HashSet;
use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Validation failure for a Packet 1A post-processing contract.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum PostProcessingValidationError {
    InvalidIdentifier(&'static str),
    InvalidName(&'static str),
    InvalidDigest,
    InvalidRevisionIdentity,
    InvalidEntrypoint,
    InvalidFilesystemRoot,
    InvalidOptionDefault,
    DuplicateOptionName,
    DuplicateCommandName,
    DuplicateSectionName,
    DuplicateResolvedOptionName,
    InvalidSelection,
    InvalidFrozenPlan,
    InvalidStepOrder,
    InvalidPolicy,
}

impl fmt::Display for PostProcessingValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::InvalidIdentifier(field) => format!("invalid {field}"),
            Self::InvalidName(field) => format!("invalid {field}"),
            Self::InvalidDigest => "invalid extension digest".to_string(),
            Self::InvalidRevisionIdentity => "invalid extension revision identity".to_string(),
            Self::InvalidEntrypoint => "invalid extension entrypoint".to_string(),
            Self::InvalidFilesystemRoot => "invalid approved filesystem root".to_string(),
            Self::InvalidOptionDefault => "invalid extension option default".to_string(),
            Self::DuplicateOptionName => "duplicate extension option name".to_string(),
            Self::DuplicateCommandName => "duplicate extension command name".to_string(),
            Self::DuplicateSectionName => "duplicate extension section name".to_string(),
            Self::DuplicateResolvedOptionName => "duplicate resolved option name".to_string(),
            Self::InvalidSelection => "invalid extension selection".to_string(),
            Self::InvalidFrozenPlan => "invalid frozen post-processing plan".to_string(),
            Self::InvalidStepOrder => "invalid post-processing step order".to_string(),
            Self::InvalidPolicy => "invalid post-processing policy".to_string(),
        };
        f.write_str(&message)
    }
}

impl std::error::Error for PostProcessingValidationError {}

fn validate_stable_identifier(
    value: &str,
    field: &'static str,
    allow_leading_digit: bool,
) -> Result<(), PostProcessingValidationError> {
    let valid = !value.is_empty()
        && value == value.trim()
        && value.len() <= 128
        && value.as_bytes().first().is_some_and(|byte| {
            byte.is_ascii_lowercase() || (allow_leading_digit && byte.is_ascii_digit())
        })
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
        })
        && value
            .split(['.', '_', '-'])
            .all(|segment| !segment.is_empty())
        && !matches!(value, "default" | "latest" | "unknown" | "unnamed");
    valid
        .then_some(())
        .ok_or(PostProcessingValidationError::InvalidIdentifier(field))
}

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

macro_rules! stable_identifier {
    ($name:ident, $doc:literal, $field:literal, $allow_leading_digit:expr) => {
        #[doc = $doc]
        #[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, PostProcessingValidationError> {
                let value = value.into();
                validate_stable_identifier(&value, $field, $allow_leading_digit)?;
                Ok(Self(value))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
            }
        }
    };
}

macro_rules! member_name {
    ($name:ident, $field:literal) => {
        #[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, PostProcessingValidationError> {
                let value = value.into();
                validate_member_name(&value, $field)?;
                Ok(Self(value))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
            }
        }
    };
}

stable_identifier!(
    ExtensionId,
    "Stable Weaver extension identity.",
    "extension identifier",
    false
);
stable_identifier!(
    ExtensionRevisionId,
    "Immutable identity of a verified package revision.",
    "extension revision identifier",
    false
);
stable_identifier!(
    ProfileId,
    "Stable post-processing profile identity.",
    "post-processing profile identifier",
    false
);
stable_identifier!(
    RunId,
    "Stable post-processing run identity.",
    "post-processing run identifier",
    false
);
stable_identifier!(
    AttemptId,
    "Stable post-processing attempt identity.",
    "post-processing attempt identifier",
    false
);
member_name!(OptionName, "extension option name");
member_name!(CommandName, "extension command name");

/// Exact NZBGet compatibility name retained for its legacy environment contract.
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

    /// Produces a collision-safe readable identity with a digest of the exact original bytes.
    pub fn weaver_extension_id(&self) -> Result<ExtensionId, PostProcessingValidationError> {
        let mut normalized = String::new();
        let mut separator = false;
        for character in self.0.chars() {
            if character.is_ascii_alphanumeric() {
                normalized.push(character.to_ascii_lowercase());
                separator = false;
            } else if !separator {
                normalized.push('-');
                separator = true;
            }
        }
        let normalized = normalized.trim_matches('-');
        let readable = if normalized.is_empty() {
            "extension"
        } else {
            &normalized[..normalized.len().min(40)]
        };
        let digest = blake3::hash(self.0.as_bytes()).to_hex();
        ExtensionId::new(format!("nzbget.{readable}-{digest}"))
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

/// A syntactically valid content digest.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize)]
#[serde(transparent)]
pub struct ExtensionDigest(String);

impl ExtensionDigest {
    pub fn new(value: impl Into<String>) -> Result<Self, PostProcessingValidationError> {
        let value = value.into();
        let Some((algorithm, hex)) = value.split_once(':') else {
            return Err(PostProcessingValidationError::InvalidDigest);
        };
        if !matches!(algorithm, "blake3" | "sha256")
            || hex.len() != 64
            || !hex
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        {
            return Err(PostProcessingValidationError::InvalidDigest);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn revision_id(&self) -> ExtensionRevisionId {
        let (algorithm, hex) = self.0.split_once(':').expect("validated digest");
        ExtensionRevisionId::new(format!("{algorithm}-{hex}")).expect("digest-derived revision id")
    }
}

impl<'de> Deserialize<'de> for ExtensionDigest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

/// Opaque proof that package discovery verified a digest over package bytes.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct VerifiedExtensionDigest(ExtensionDigest);

impl VerifiedExtensionDigest {
    /// Package discovery/verification is the only Packet boundary permitted to create this proof.
    #[allow(dead_code)]
    pub(crate) fn from_verified_package_digest(digest: ExtensionDigest) -> Self {
        Self(digest)
    }

    pub fn digest(&self) -> &ExtensionDigest {
        &self.0
    }
}

/// Declared package version, preserved separately from digest-derived revision identity.
#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct DeclaredExtensionVersion(String);

impl DeclaredExtensionVersion {
    pub fn new(value: impl Into<String>) -> Result<Self, PostProcessingValidationError> {
        let value = value.into();
        validate_bounded_metadata(&value, "declared extension version")?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
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

impl<'de> Deserialize<'de> for DeclaredExtensionVersion {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

/// Immutable package revision, bound to a verified digest rather than a manifest version.
#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
pub struct ExtensionRevision {
    extension_id: ExtensionId,
    revision_id: ExtensionRevisionId,
    declared_version: DeclaredExtensionVersion,
    digest: ExtensionDigest,
}

impl ExtensionRevision {
    pub(crate) fn from_verified(
        extension_id: ExtensionId,
        declared_version: DeclaredExtensionVersion,
        verified_digest: VerifiedExtensionDigest,
    ) -> Self {
        let digest = verified_digest.0;
        let revision_id = digest.revision_id();
        Self {
            extension_id,
            revision_id,
            declared_version,
            digest,
        }
    }

    fn from_stored(
        extension_id: ExtensionId,
        revision_id: ExtensionRevisionId,
        declared_version: DeclaredExtensionVersion,
        digest: ExtensionDigest,
    ) -> Result<Self, PostProcessingValidationError> {
        if revision_id != digest.revision_id() {
            return Err(PostProcessingValidationError::InvalidRevisionIdentity);
        }
        Ok(Self {
            extension_id,
            revision_id,
            declared_version,
            digest,
        })
    }

    pub fn extension_id(&self) -> &ExtensionId {
        &self.extension_id
    }

    pub fn revision_id(&self) -> &ExtensionRevisionId {
        &self.revision_id
    }

    pub fn declared_version(&self) -> &DeclaredExtensionVersion {
        &self.declared_version
    }

    pub fn digest(&self) -> &ExtensionDigest {
        &self.digest
    }
}

#[derive(Deserialize)]
struct ExtensionRevisionWire {
    extension_id: ExtensionId,
    revision_id: ExtensionRevisionId,
    declared_version: DeclaredExtensionVersion,
    digest: ExtensionDigest,
}

impl<'de> Deserialize<'de> for ExtensionRevision {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ExtensionRevisionWire::deserialize(deserializer)?;
        Self::from_stored(
            wire.extension_id,
            wire.revision_id,
            wire.declared_version,
            wire.digest,
        )
        .map_err(serde::de::Error::custom)
    }
}

/// Administrator approval state for an immutable extension revision.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrustState {
    Unapproved,
    Approved,
    Revoked,
}

/// Approval state is intentionally separate from manifest input.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExtensionRevisionApproval {
    revision: ExtensionRevision,
    state: TrustState,
}

impl ExtensionRevisionApproval {
    pub fn new(revision: ExtensionRevision, state: TrustState) -> Self {
        Self { revision, state }
    }

    pub fn revision(&self) -> &ExtensionRevision {
        &self.revision
    }

    pub fn state(&self) -> TrustState {
        self.state
    }
}

/// Extension protocol adapter selected by the validated manifest or legacy detection.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExtensionAdapter {
    Native,
    Sabnzbd,
    Nzbget,
    Webhook,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunWhen {
    Always,
    PipelineSuccess,
    PipelineFailure,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnFailure {
    Continue,
    Stop,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutcomeImpact {
    Warning,
    FailJob,
}

/// Typed built-in pipeline failure stage.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PipelineFailureStage {
    Download,
    Verify,
    Repair,
    Extract,
    Move,
}

/// Pipeline result remains independent of post-processing result.
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PostProcessingSettings {
    pub discovery_enabled: bool,
    pub execution_enabled: bool,
    pub concurrency: u8,
    pub termination_grace_seconds: u64,
    pub python_interpreter: Option<String>,
    pub powershell_interpreter: Option<String>,
    pub batch_interpreter: Option<String>,
    #[serde(default)]
    pub webhooks_enabled: bool,
    pub allowed_roots: Vec<String>,
}

impl Default for PostProcessingSettings {
    fn default() -> Self {
        Self {
            discovery_enabled: false,
            execution_enabled: false,
            concurrency: 1,
            termination_grace_seconds: 10,
            python_interpreter: None,
            powershell_interpreter: None,
            batch_interpreter: None,
            webhooks_enabled: false,
            allowed_roots: Vec::new(),
        }
    }
}

impl PostProcessingSettings {
    pub fn validate(&self) -> Result<(), PostProcessingValidationError> {
        if !(1..=8).contains(&self.concurrency) || self.termination_grace_seconds == 0 {
            return Err(PostProcessingValidationError::InvalidPolicy);
        }
        let mut roots = HashSet::new();
        for root in &self.allowed_roots {
            ApprovedFilesystemRoot::new(root.clone())?;
            if !roots.insert(root) {
                return Err(PostProcessingValidationError::InvalidPolicy);
            }
        }
        Ok(())
    }
}

/// Summary of post-processing only; it intentionally never embeds a pipeline outcome.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PostProcessingSummary {
    NotRun,
    Succeeded,
    Warning,
    Failed,
    Cancelled,
    Interrupted,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Queued,
    Starting,
    Running,
    Succeeded,
    Failed,
    Skipped,
    Cancelled,
    Interrupted,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttemptStatus {
    Queued,
    Starting,
    Running,
    Succeeded,
    Failed,
    TimedOut,
    Skipped,
    Cancelled,
    Interrupted,
}

/// Value type accepted by an extension option declaration.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExtensionOptionType {
    String,
    Integer,
    Number,
    Boolean,
    Secret,
}

/// Secret option value that is deliberately impossible to deserialize through generic serde.
#[derive(Clone, Eq, PartialEq)]
pub struct SecretOptionValue(String);

impl SecretOptionValue {
    /// Construct a secret at an authenticated administrative input boundary.
    pub fn from_admin_input(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    #[allow(dead_code)]
    pub(crate) fn for_execution(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    #[allow(dead_code)]
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

/// Concrete resolved option value. Decimal JSON numbers retain their original JSON number form.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum ResolvedOptionValue {
    String(String),
    Integer(i64),
    Number(serde_json::Number),
    Boolean(bool),
    Secret(SecretOptionValue),
}

impl ResolvedOptionValue {
    pub fn matches_type(&self, option_type: ExtensionOptionType) -> bool {
        matches!(
            (self, option_type),
            (Self::String(_), ExtensionOptionType::String)
                | (Self::Integer(_), ExtensionOptionType::Integer)
                | (Self::Number(_), ExtensionOptionType::Number)
                | (Self::Boolean(_), ExtensionOptionType::Boolean)
                | (Self::Secret(_), ExtensionOptionType::Secret)
        )
    }
}

/// NZBGet select entry, preserving documented string or numeric values.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ExtensionSelectValue {
    String(String),
    Number(serde_json::Number),
}

/// Resolved step option with a validated name.
#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
pub struct ResolvedOption {
    name: OptionName,
    value: ResolvedOptionValue,
}

impl ResolvedOption {
    pub fn new(name: OptionName, value: ResolvedOptionValue) -> Self {
        Self { name, value }
    }

    pub fn name(&self) -> &OptionName {
        &self.name
    }

    pub fn value(&self) -> &ResolvedOptionValue {
        &self.value
    }
}

#[derive(Deserialize)]
struct ResolvedOptionWire {
    name: OptionName,
    value: ResolvedOptionValue,
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

/// Named NZBGet section metadata retained from a v24+ manifest.
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

/// Validated extension command declaration.
#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
pub struct ExtensionCommand {
    section: Option<String>,
    name: CommandName,
    action: String,
    display_name: Option<String>,
    description: Vec<String>,
}

impl ExtensionCommand {
    pub fn new(
        section: Option<String>,
        name: CommandName,
        action: String,
        display_name: Option<String>,
        description: Vec<String>,
    ) -> Result<Self, PostProcessingValidationError> {
        let section = normalize_section_reference(section)?;
        validate_text(&action, "extension command action")?;
        if let Some(display_name) = &display_name {
            validate_text(display_name, "extension command display name")?;
        }
        validate_metadata(&description, "extension command description")?;
        Ok(Self {
            section,
            name,
            action,
            display_name,
            description,
        })
    }

    pub fn section(&self) -> Option<&str> {
        self.section.as_deref()
    }

    pub fn name(&self) -> &CommandName {
        &self.name
    }

    pub fn action(&self) -> &str {
        &self.action
    }

    pub fn display_name(&self) -> Option<&str> {
        self.display_name.as_deref()
    }

    pub fn description(&self) -> &[String] {
        &self.description
    }
}

#[derive(Deserialize)]
struct ExtensionCommandWire {
    section: Option<String>,
    name: CommandName,
    action: String,
    display_name: Option<String>,
    #[serde(default)]
    description: Vec<String>,
}

impl<'de> Deserialize<'de> for ExtensionCommand {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ExtensionCommandWire::deserialize(deserializer)?;
        Self::new(
            wire.section,
            wire.name,
            wire.action,
            wire.display_name,
            wire.description,
        )
        .map_err(serde::de::Error::custom)
    }
}

/// Validated extension option declaration.
#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
pub struct ExtensionOption {
    section: Option<String>,
    name: OptionName,
    option_type: ExtensionOptionType,
    default: Option<ResolvedOptionValue>,
    display_name: Option<String>,
    description: Vec<String>,
    select: Vec<ExtensionSelectValue>,
    required: bool,
}

impl ExtensionOption {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        section: Option<String>,
        name: OptionName,
        option_type: ExtensionOptionType,
        default: Option<ResolvedOptionValue>,
        display_name: Option<String>,
        description: Vec<String>,
        select: Vec<ExtensionSelectValue>,
        required: bool,
    ) -> Result<Self, PostProcessingValidationError> {
        let section = normalize_section_reference(section)?;
        if option_type == ExtensionOptionType::Secret && default.is_some() {
            return Err(PostProcessingValidationError::InvalidOptionDefault);
        }
        if default
            .as_ref()
            .is_some_and(|value| !value.matches_type(option_type))
        {
            return Err(PostProcessingValidationError::InvalidOptionDefault);
        }
        if let Some(display_name) = &display_name {
            validate_text(display_name, "extension option display name")?;
        }
        validate_metadata(&description, "extension option description")?;
        for value in &select {
            match value {
                ExtensionSelectValue::String(value) => {
                    validate_text(value, "extension option select value")?;
                }
                ExtensionSelectValue::Number(_) => {}
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

    pub fn option_type(&self) -> ExtensionOptionType {
        self.option_type
    }

    pub fn default(&self) -> Option<&ResolvedOptionValue> {
        self.default.as_ref()
    }

    pub fn display_name(&self) -> Option<&str> {
        self.display_name.as_deref()
    }

    pub fn description(&self) -> &[String] {
        &self.description
    }

    pub fn select(&self) -> &[ExtensionSelectValue] {
        &self.select
    }

    pub fn required(&self) -> bool {
        self.required
    }
}

#[derive(Deserialize)]
struct ExtensionOptionWire {
    section: Option<String>,
    name: OptionName,
    option_type: ExtensionOptionType,
    default: Option<ResolvedOptionValue>,
    display_name: Option<String>,
    #[serde(default)]
    description: Vec<String>,
    #[serde(default)]
    select: Vec<ExtensionSelectValue>,
    #[serde(default)]
    required: bool,
}

impl<'de> Deserialize<'de> for ExtensionOption {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ExtensionOptionWire::deserialize(deserializer)?;
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

/// Validated internal representation of a discovered extension manifest.
#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
pub struct ExtensionManifest {
    adapter: ExtensionAdapter,
    compatibility_name: Option<NzbgetCompatibilityName>,
    display_name: String,
    revision: ExtensionRevision,
    entrypoint: String,
    sections: Vec<NzbgetSection>,
    commands: Vec<ExtensionCommand>,
    options: Vec<ExtensionOption>,
}

impl ExtensionManifest {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        adapter: ExtensionAdapter,
        compatibility_name: Option<NzbgetCompatibilityName>,
        display_name: String,
        revision: ExtensionRevision,
        entrypoint: String,
        sections: Vec<NzbgetSection>,
        commands: Vec<ExtensionCommand>,
        options: Vec<ExtensionOption>,
    ) -> Result<Self, PostProcessingValidationError> {
        validate_text(&display_name, "extension display name")?;
        validate_relative_entrypoint(&entrypoint)?;
        if compatibility_name.is_some() != (adapter == ExtensionAdapter::Nzbget) {
            return Err(PostProcessingValidationError::InvalidName(
                "extension compatibility name",
            ));
        }
        validate_sections(&sections)?;
        validate_unique_qualified_names(
            commands
                .iter()
                .map(|command| (command.section(), command.name().as_str())),
            true,
        )?;
        validate_unique_qualified_names(
            options
                .iter()
                .map(|option| (option.section(), option.name().as_str())),
            false,
        )?;
        Ok(Self {
            adapter,
            compatibility_name,
            display_name,
            revision,
            entrypoint,
            sections,
            commands,
            options,
        })
    }

    pub fn adapter(&self) -> ExtensionAdapter {
        self.adapter
    }

    pub fn compatibility_name(&self) -> Option<&NzbgetCompatibilityName> {
        self.compatibility_name.as_ref()
    }

    pub fn display_name(&self) -> &str {
        &self.display_name
    }

    pub fn revision(&self) -> &ExtensionRevision {
        &self.revision
    }

    pub fn entrypoint(&self) -> &str {
        &self.entrypoint
    }

    pub fn sections(&self) -> &[NzbgetSection] {
        &self.sections
    }

    pub fn commands(&self) -> &[ExtensionCommand] {
        &self.commands
    }

    pub fn options(&self) -> &[ExtensionOption] {
        &self.options
    }
}

#[derive(Deserialize)]
struct ExtensionManifestWire {
    adapter: ExtensionAdapter,
    compatibility_name: Option<NzbgetCompatibilityName>,
    display_name: String,
    revision: ExtensionRevision,
    entrypoint: String,
    #[serde(default)]
    sections: Vec<NzbgetSection>,
    #[serde(default)]
    commands: Vec<ExtensionCommand>,
    #[serde(default)]
    options: Vec<ExtensionOption>,
}

impl<'de> Deserialize<'de> for ExtensionManifest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ExtensionManifestWire::deserialize(deserializer)?;
        Self::new(
            wire.adapter,
            wire.compatibility_name,
            wire.display_name,
            wire.revision,
            wire.entrypoint,
            wire.sections,
            wire.commands,
            wire.options,
        )
        .map_err(serde::de::Error::custom)
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
            validate_bounded_metadata(&section, "extension section")?;
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

fn validate_unique_qualified_names<'a>(
    names: impl Iterator<Item = (Option<&'a str>, &'a str)>,
    commands: bool,
) -> Result<(), PostProcessingValidationError> {
    let mut seen = HashSet::new();
    for (section, name) in names {
        let key = format!(
            "{}\u{1f}{}",
            normalized_section_key(section),
            name.to_ascii_lowercase()
        );
        if !seen.insert(key) {
            return Err(if commands {
                PostProcessingValidationError::DuplicateCommandName
            } else {
                PostProcessingValidationError::DuplicateOptionName
            });
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

/// Revision-selection semantics, distinct from a submission's plan-selection mode.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SelectionMode {
    PinnedRevision,
    LatestApproved,
}

/// Validated request for an extension revision.
#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
pub struct ExtensionSelection {
    extension_id: ExtensionId,
    mode: SelectionMode,
    revision_id: Option<ExtensionRevisionId>,
}

impl ExtensionSelection {
    pub fn pinned(extension_id: ExtensionId, revision_id: ExtensionRevisionId) -> Self {
        Self {
            extension_id,
            mode: SelectionMode::PinnedRevision,
            revision_id: Some(revision_id),
        }
    }

    pub fn latest_approved(extension_id: ExtensionId) -> Self {
        Self {
            extension_id,
            mode: SelectionMode::LatestApproved,
            revision_id: None,
        }
    }

    fn from_wire(
        extension_id: ExtensionId,
        mode: SelectionMode,
        revision_id: Option<ExtensionRevisionId>,
    ) -> Result<Self, PostProcessingValidationError> {
        let valid = matches!(mode, SelectionMode::PinnedRevision) == revision_id.is_some();
        valid
            .then_some(Self {
                extension_id,
                mode,
                revision_id,
            })
            .ok_or(PostProcessingValidationError::InvalidSelection)
    }

    pub fn extension_id(&self) -> &ExtensionId {
        &self.extension_id
    }

    pub fn mode(&self) -> SelectionMode {
        self.mode
    }

    pub fn revision_id(&self) -> Option<&ExtensionRevisionId> {
        self.revision_id.as_ref()
    }
}

#[derive(Deserialize)]
struct ExtensionSelectionWire {
    extension_id: ExtensionId,
    mode: SelectionMode,
    revision_id: Option<ExtensionRevisionId>,
}

impl<'de> Deserialize<'de> for ExtensionSelection {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ExtensionSelectionWire::deserialize(deserializer)?;
        Self::from_wire(wire.extension_id, wire.mode, wire.revision_id)
            .map_err(serde::de::Error::custom)
    }
}

/// Submission-level plan selection, independent of revision selection.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubmissionPlanSelectionMode {
    Inherit,
    Disabled,
    Profile,
    Extensions,
}

/// Validated selection of the post-processing plan submitted with a job.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SubmissionPlanSelection {
    kind: SubmissionPlanSelectionKind,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "mode", content = "value")]
enum SubmissionPlanSelectionKind {
    Inherit,
    Disabled,
    Profile(ProfileId),
    Extensions(Vec<ExtensionSelection>),
}

impl Serialize for SubmissionPlanSelection {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.kind.serialize(serializer)
    }
}

impl SubmissionPlanSelection {
    pub fn inherit() -> Self {
        Self {
            kind: SubmissionPlanSelectionKind::Inherit,
        }
    }

    pub fn disabled() -> Self {
        Self {
            kind: SubmissionPlanSelectionKind::Disabled,
        }
    }

    pub fn profile(profile_id: ProfileId) -> Self {
        Self {
            kind: SubmissionPlanSelectionKind::Profile(profile_id),
        }
    }

    pub fn extensions(
        selections: Vec<ExtensionSelection>,
    ) -> Result<Self, PostProcessingValidationError> {
        (!selections.is_empty())
            .then_some(Self {
                kind: SubmissionPlanSelectionKind::Extensions(selections),
            })
            .ok_or(PostProcessingValidationError::InvalidSelection)
    }

    pub fn mode(&self) -> SubmissionPlanSelectionMode {
        match self.kind {
            SubmissionPlanSelectionKind::Inherit => SubmissionPlanSelectionMode::Inherit,
            SubmissionPlanSelectionKind::Disabled => SubmissionPlanSelectionMode::Disabled,
            SubmissionPlanSelectionKind::Profile(_) => SubmissionPlanSelectionMode::Profile,
            SubmissionPlanSelectionKind::Extensions(_) => SubmissionPlanSelectionMode::Extensions,
        }
    }

    pub fn profile_id(&self) -> Option<&ProfileId> {
        match &self.kind {
            SubmissionPlanSelectionKind::Profile(profile_id) => Some(profile_id),
            _ => None,
        }
    }

    pub fn selected_extensions(&self) -> Option<&[ExtensionSelection]> {
        match &self.kind {
            SubmissionPlanSelectionKind::Extensions(selections) => Some(selections),
            _ => None,
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case", tag = "mode", content = "value")]
enum SubmissionPlanSelectionWire {
    Inherit,
    Disabled,
    Profile(ProfileId),
    Extensions(Vec<ExtensionSelection>),
}

impl<'de> Deserialize<'de> for SubmissionPlanSelection {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        match SubmissionPlanSelectionWire::deserialize(deserializer)? {
            SubmissionPlanSelectionWire::Inherit => Ok(Self::inherit()),
            SubmissionPlanSelectionWire::Disabled => Ok(Self::disabled()),
            SubmissionPlanSelectionWire::Profile(profile_id) => Ok(Self::profile(profile_id)),
            SubmissionPlanSelectionWire::Extensions(selections) => {
                Self::extensions(selections).map_err(serde::de::Error::custom)
            }
        }
    }
}

/// Validated nonzero finite timeout in seconds.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct NonZeroTimeoutSeconds(u64);

impl NonZeroTimeoutSeconds {
    pub fn new(seconds: u64) -> Result<Self, PostProcessingValidationError> {
        (seconds != 0)
            .then_some(Self(seconds))
            .ok_or(PostProcessingValidationError::InvalidName("finite timeout"))
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

impl<'de> Deserialize<'de> for NonZeroTimeoutSeconds {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(u64::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

/// Timeout policy retained by profiles and frozen plans.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeoutPolicy {
    Default24Hours,
    Finite(NonZeroTimeoutSeconds),
    Unlimited,
}

/// Administrator allowlisted filesystem root.
#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct ApprovedFilesystemRoot(String);

impl ApprovedFilesystemRoot {
    pub fn new(value: impl Into<String>) -> Result<Self, PostProcessingValidationError> {
        let value = value.into();
        let bytes = value.as_bytes();
        let windows_normalized = value.replace('/', "\\");
        let windows_namespace = windows_normalized.starts_with(r"\\?\")
            || windows_normalized.starts_with(r"\\.\")
            || windows_normalized.starts_with(r"\??\");
        let windows_drive = bytes.len() >= 3
            && bytes[0].is_ascii_alphabetic()
            && bytes[1] == b':'
            && matches!(bytes[2], b'/' | b'\\');
        let windows_path = windows_drive || value.starts_with("\\\\") || value.starts_with("//");
        let absolute = value.starts_with('/') || value.starts_with("\\\\") || windows_drive;
        let relative_components = value
            .strip_prefix('/')
            .or_else(|| value.strip_prefix("\\\\"))
            .or_else(|| windows_drive.then(|| value.get(3..).unwrap_or_default()))
            .unwrap_or_default();
        let unsafe_component = relative_components
            .split(['/', '\\'])
            .filter(|component| !component.is_empty())
            .any(|component| {
                matches!(component, "." | "..")
                    || component.chars().any(char::is_control)
                    || component.ends_with(['.', ' '])
                    || (windows_path && component.contains(':'))
                    || is_windows_device_component(component)
            });
        if value.is_empty()
            || value != value.trim()
            || value.contains('\0')
            || !absolute
            || windows_namespace
            || unsafe_component
        {
            return Err(PostProcessingValidationError::InvalidFilesystemRoot);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for ApprovedFilesystemRoot {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

/// Immutable collection of administrator-approved roots frozen into a step.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ApprovedFilesystemRoots(Vec<ApprovedFilesystemRoot>);

impl ApprovedFilesystemRoots {
    pub fn new(roots: Vec<ApprovedFilesystemRoot>) -> Self {
        Self(roots)
    }

    pub fn as_slice(&self) -> &[ApprovedFilesystemRoot] {
        &self.0
    }
}

/// Content-aware prerequisite evaluated against files already present in the job output and
/// artifacts emitted by earlier extension steps. Suffixes are filename-only so conditions cannot
/// be used for path traversal.
#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ArtifactCondition {
    required_suffixes: Vec<String>,
    minimum_count: u32,
}

impl ArtifactCondition {
    pub fn new(
        required_suffixes: Vec<String>,
        minimum_count: u32,
    ) -> Result<Self, PostProcessingValidationError> {
        let mut normalized = Vec::with_capacity(required_suffixes.len());
        for suffix in required_suffixes {
            let suffix = suffix.trim();
            if suffix.is_empty() || suffix.contains(['/', '\\', '\0']) || suffix.len() > 1_024 {
                return Err(PostProcessingValidationError::InvalidPolicy);
            }
            if !normalized.iter().any(|existing| existing == suffix) {
                normalized.push(suffix.to_string());
            }
        }
        Ok(Self {
            required_suffixes: normalized,
            minimum_count,
        })
    }

    pub fn required_suffixes(&self) -> &[String] {
        &self.required_suffixes
    }

    pub fn minimum_count(&self) -> u32 {
        self.minimum_count
    }

    pub fn matches<'a>(&self, paths: impl IntoIterator<Item = &'a std::path::Path>) -> bool {
        let names = paths
            .into_iter()
            .filter_map(std::path::Path::file_name)
            .map(|name| name.to_string_lossy())
            .collect::<Vec<_>>();
        names.len() >= usize::try_from(self.minimum_count).unwrap_or(usize::MAX)
            && self
                .required_suffixes
                .iter()
                .all(|suffix| names.iter().any(|name| name.ends_with(suffix)))
    }
}

/// Ordered profile step with policy retained for later plan freezing.
#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
pub struct OrderedStep {
    index: u32,
    selection: ExtensionSelection,
    run_when: RunWhen,
    on_failure: OnFailure,
    outcome_impact: OutcomeImpact,
    timeout_policy: TimeoutPolicy,
    approved_roots: ApprovedFilesystemRoots,
    artifact_condition: ArtifactCondition,
    options: Vec<ResolvedOption>,
}

impl OrderedStep {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        index: u32,
        selection: ExtensionSelection,
        run_when: RunWhen,
        on_failure: OnFailure,
        outcome_impact: OutcomeImpact,
        timeout_policy: TimeoutPolicy,
        approved_roots: ApprovedFilesystemRoots,
        options: Vec<ResolvedOption>,
    ) -> Result<Self, PostProcessingValidationError> {
        validate_resolved_options(&options)?;
        Ok(Self {
            index,
            selection,
            run_when,
            on_failure,
            outcome_impact,
            timeout_policy,
            approved_roots,
            artifact_condition: ArtifactCondition::default(),
            options,
        })
    }

    pub fn index(&self) -> u32 {
        self.index
    }

    pub fn selection(&self) -> &ExtensionSelection {
        &self.selection
    }

    pub fn run_when(&self) -> RunWhen {
        self.run_when
    }

    pub fn on_failure(&self) -> OnFailure {
        self.on_failure
    }

    pub fn outcome_impact(&self) -> OutcomeImpact {
        self.outcome_impact
    }

    pub fn timeout_policy(&self) -> TimeoutPolicy {
        self.timeout_policy
    }

    pub fn approved_roots(&self) -> &ApprovedFilesystemRoots {
        &self.approved_roots
    }

    pub fn artifact_condition(&self) -> &ArtifactCondition {
        &self.artifact_condition
    }

    pub fn with_artifact_condition(mut self, artifact_condition: ArtifactCondition) -> Self {
        self.artifact_condition = artifact_condition;
        self
    }

    pub fn options(&self) -> &[ResolvedOption] {
        &self.options
    }
}

#[derive(Deserialize)]
struct OrderedStepWire {
    index: u32,
    selection: ExtensionSelection,
    run_when: RunWhen,
    on_failure: OnFailure,
    outcome_impact: OutcomeImpact,
    timeout_policy: TimeoutPolicy,
    approved_roots: ApprovedFilesystemRoots,
    #[serde(default)]
    artifact_condition: ArtifactCondition,
    #[serde(default)]
    options: Vec<ResolvedOption>,
}

impl<'de> Deserialize<'de> for OrderedStep {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = OrderedStepWire::deserialize(deserializer)?;
        Ok(Self::new(
            wire.index,
            wire.selection,
            wire.run_when,
            wire.on_failure,
            wire.outcome_impact,
            wire.timeout_policy,
            wire.approved_roots,
            wire.options,
        )
        .map_err(serde::de::Error::custom)?
        .with_artifact_condition(wire.artifact_condition))
    }
}

/// Validated profile with canonical contiguous step ordering.
#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
pub struct Profile {
    id: ProfileId,
    name: String,
    steps: Vec<OrderedStep>,
}

impl Profile {
    pub fn new(
        id: ProfileId,
        name: String,
        mut steps: Vec<OrderedStep>,
    ) -> Result<Self, PostProcessingValidationError> {
        validate_text(&name, "post-processing profile name")?;
        normalize_ordered_steps(&mut steps)?;
        Ok(Self { id, name, steps })
    }

    pub fn id(&self) -> &ProfileId {
        &self.id
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn steps(&self) -> &[OrderedStep] {
        &self.steps
    }
}

#[derive(Deserialize)]
struct ProfileWire {
    id: ProfileId,
    name: String,
    #[serde(default)]
    steps: Vec<OrderedStep>,
}

impl<'de> Deserialize<'de> for Profile {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ProfileWire::deserialize(deserializer)?;
        Self::new(wire.id, wire.name, wire.steps).map_err(serde::de::Error::custom)
    }
}

/// Frozen executable step that pins a verified revision and administrator policy.
#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
pub struct FrozenPlanStep {
    index: u32,
    revision: ExtensionRevision,
    run_when: RunWhen,
    on_failure: OnFailure,
    outcome_impact: OutcomeImpact,
    timeout_policy: TimeoutPolicy,
    approved_roots: ApprovedFilesystemRoots,
    artifact_condition: ArtifactCondition,
    options: Vec<ResolvedOption>,
}

impl FrozenPlanStep {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        index: u32,
        revision: ExtensionRevision,
        run_when: RunWhen,
        on_failure: OnFailure,
        outcome_impact: OutcomeImpact,
        timeout_policy: TimeoutPolicy,
        approved_roots: ApprovedFilesystemRoots,
        options: Vec<ResolvedOption>,
    ) -> Result<Self, PostProcessingValidationError> {
        validate_resolved_options(&options)?;
        Ok(Self {
            index,
            revision,
            run_when,
            on_failure,
            outcome_impact,
            timeout_policy,
            approved_roots,
            artifact_condition: ArtifactCondition::default(),
            options,
        })
    }

    pub fn index(&self) -> u32 {
        self.index
    }

    pub fn revision(&self) -> &ExtensionRevision {
        &self.revision
    }

    pub fn run_when(&self) -> RunWhen {
        self.run_when
    }

    pub fn on_failure(&self) -> OnFailure {
        self.on_failure
    }

    pub fn outcome_impact(&self) -> OutcomeImpact {
        self.outcome_impact
    }

    pub fn timeout_policy(&self) -> TimeoutPolicy {
        self.timeout_policy
    }

    pub fn approved_roots(&self) -> &ApprovedFilesystemRoots {
        &self.approved_roots
    }

    pub fn artifact_condition(&self) -> &ArtifactCondition {
        &self.artifact_condition
    }

    pub fn with_artifact_condition(mut self, artifact_condition: ArtifactCondition) -> Self {
        self.artifact_condition = artifact_condition;
        self
    }

    pub fn options(&self) -> &[ResolvedOption] {
        &self.options
    }
}

#[derive(Deserialize)]
struct FrozenPlanStepWire {
    index: u32,
    revision: ExtensionRevision,
    run_when: RunWhen,
    on_failure: OnFailure,
    outcome_impact: OutcomeImpact,
    timeout_policy: TimeoutPolicy,
    approved_roots: ApprovedFilesystemRoots,
    #[serde(default)]
    artifact_condition: ArtifactCondition,
    #[serde(default)]
    options: Vec<ResolvedOption>,
}

impl<'de> Deserialize<'de> for FrozenPlanStep {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = FrozenPlanStepWire::deserialize(deserializer)?;
        Ok(Self::new(
            wire.index,
            wire.revision,
            wire.run_when,
            wire.on_failure,
            wire.outcome_impact,
            wire.timeout_policy,
            wire.approved_roots,
            wire.options,
        )
        .map_err(serde::de::Error::custom)?
        .with_artifact_condition(wire.artifact_condition))
    }
}

/// Durable explanation of how a concrete plan was selected.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "source")]
pub enum FrozenPlanProvenance {
    Explicit,
    CategoryProfile { profile_id: ProfileId },
    GlobalDefault { profile_id: ProfileId },
    Disabled,
    Empty,
}

/// Canonically ordered resolved plan ready for later persistence and execution.
#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
pub struct FrozenPlan {
    provenance: FrozenPlanProvenance,
    steps: Vec<FrozenPlanStep>,
}

impl FrozenPlan {
    pub fn new(
        provenance: FrozenPlanProvenance,
        mut steps: Vec<FrozenPlanStep>,
    ) -> Result<Self, PostProcessingValidationError> {
        normalize_frozen_steps(&mut steps)?;
        let requires_steps = matches!(
            provenance,
            FrozenPlanProvenance::Explicit
                | FrozenPlanProvenance::CategoryProfile { .. }
                | FrozenPlanProvenance::GlobalDefault { .. }
        );
        if requires_steps == steps.is_empty() {
            return Err(PostProcessingValidationError::InvalidFrozenPlan);
        }
        Ok(Self { provenance, steps })
    }

    pub fn provenance(&self) -> &FrozenPlanProvenance {
        &self.provenance
    }

    pub fn steps(&self) -> &[FrozenPlanStep] {
        &self.steps
    }
}

#[derive(Deserialize)]
struct FrozenPlanWire {
    provenance: FrozenPlanProvenance,
    #[serde(default)]
    steps: Vec<FrozenPlanStep>,
}

impl<'de> Deserialize<'de> for FrozenPlan {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = FrozenPlanWire::deserialize(deserializer)?;
        Self::new(wire.provenance, wire.steps).map_err(serde::de::Error::custom)
    }
}

fn validate_resolved_options(
    options: &[ResolvedOption],
) -> Result<(), PostProcessingValidationError> {
    let mut names = HashSet::new();
    if options
        .iter()
        .all(|option| names.insert(option.name().as_str().to_ascii_lowercase()))
    {
        Ok(())
    } else {
        Err(PostProcessingValidationError::DuplicateResolvedOptionName)
    }
}

fn normalize_ordered_steps(steps: &mut [OrderedStep]) -> Result<(), PostProcessingValidationError> {
    steps.sort_by_key(OrderedStep::index);
    let ordered = steps
        .iter()
        .enumerate()
        .all(|(index, step)| step.index() == index as u32);
    ordered
        .then_some(())
        .ok_or(PostProcessingValidationError::InvalidStepOrder)
}

fn normalize_frozen_steps(
    steps: &mut [FrozenPlanStep],
) -> Result<(), PostProcessingValidationError> {
    steps.sort_by_key(FrozenPlanStep::index);
    let ordered = steps
        .iter()
        .enumerate()
        .all(|(index, step)| step.index() == index as u32);
    ordered
        .then_some(())
        .ok_or(PostProcessingValidationError::InvalidStepOrder)
}
