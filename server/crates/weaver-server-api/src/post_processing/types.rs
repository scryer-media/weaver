use async_graphql::{Enum, InputObject, Json, SimpleObject};
use weaver_server_core::post_processing::manifest::BareScriptAdapter;
use weaver_server_core::post_processing::model::{
    ApprovedFilesystemRoot, ApprovedFilesystemRoots, ArtifactCondition, ExtensionId,
    ExtensionRevisionId, ExtensionSelection, NonZeroTimeoutSeconds, OnFailure, OptionName,
    OrderedStep, OutcomeImpact, PostProcessingSettings, Profile, ProfileId, ResolvedOption,
    ResolvedOptionValue, RunWhen, SecretOptionValue, SubmissionPlanSelection, TimeoutPolicy,
};
use weaver_server_core::post_processing::persistence::{
    ExtensionRevisionRecord, LogStream, PostProcessingArtifactRecord, PostProcessingAttemptRecord,
    PostProcessingLogPage, PostProcessingRunRecord, ProfileRecord,
};

#[derive(Debug, Clone, SimpleObject)]
pub struct PostProcessingSettingsGql {
    pub discovery_enabled: bool,
    pub execution_enabled: bool,
    pub concurrency: u8,
    pub termination_grace_seconds: u64,
    pub python_interpreter: Option<String>,
    pub powershell_interpreter: Option<String>,
    pub batch_interpreter: Option<String>,
    pub webhooks_enabled: bool,
    pub allowed_roots: Vec<String>,
}

impl From<PostProcessingSettings> for PostProcessingSettingsGql {
    fn from(value: PostProcessingSettings) -> Self {
        Self {
            discovery_enabled: value.discovery_enabled,
            execution_enabled: value.execution_enabled,
            concurrency: value.concurrency,
            termination_grace_seconds: value.termination_grace_seconds,
            python_interpreter: value.python_interpreter,
            powershell_interpreter: value.powershell_interpreter,
            batch_interpreter: value.batch_interpreter,
            webhooks_enabled: value.webhooks_enabled,
            allowed_roots: value.allowed_roots,
        }
    }
}

#[derive(Debug, Clone, InputObject)]
pub struct PostProcessingSettingsInput {
    pub discovery_enabled: bool,
    pub execution_enabled: bool,
    pub concurrency: u8,
    pub termination_grace_seconds: u64,
    pub python_interpreter: Option<String>,
    pub powershell_interpreter: Option<String>,
    pub batch_interpreter: Option<String>,
    #[graphql(default)]
    pub webhooks_enabled: bool,
    #[graphql(default)]
    pub allowed_roots: Vec<String>,
}

impl From<PostProcessingSettingsInput> for PostProcessingSettings {
    fn from(value: PostProcessingSettingsInput) -> Self {
        Self {
            discovery_enabled: value.discovery_enabled,
            execution_enabled: value.execution_enabled,
            concurrency: value.concurrency,
            termination_grace_seconds: value.termination_grace_seconds,
            python_interpreter: value.python_interpreter,
            powershell_interpreter: value.powershell_interpreter,
            batch_interpreter: value.batch_interpreter,
            webhooks_enabled: value.webhooks_enabled,
            allowed_roots: value.allowed_roots,
        }
    }
}

#[derive(Debug, Clone, SimpleObject)]
pub struct PostProcessingExtensionRevision {
    pub extension_id: String,
    pub revision_id: String,
    pub declared_version: String,
    pub digest: String,
    pub adapter: String,
    pub display_name: String,
    pub trust_state: String,
    pub managed: bool,
    pub source_path: Option<String>,
    pub discovered_at_epoch_ms: i64,
    pub approved_at_epoch_ms: Option<i64>,
    pub manifest: Json<serde_json::Value>,
}

impl From<ExtensionRevisionRecord> for PostProcessingExtensionRevision {
    fn from(value: ExtensionRevisionRecord) -> Self {
        let revision = value.manifest.revision();
        Self {
            extension_id: revision.extension_id().as_str().to_string(),
            revision_id: revision.revision_id().as_str().to_string(),
            declared_version: revision.declared_version().as_str().to_string(),
            digest: revision.digest().as_str().to_string(),
            adapter: format!("{:?}", value.manifest.adapter()).to_ascii_uppercase(),
            display_name: value.manifest.display_name().to_string(),
            trust_state: format!("{:?}", value.trust_state).to_ascii_uppercase(),
            managed: value.managed_path.is_some(),
            source_path: value.source_path,
            discovered_at_epoch_ms: value.discovered_at_epoch_ms,
            approved_at_epoch_ms: value.approved_at_epoch_ms,
            manifest: Json(serde_json::to_value(value.manifest).unwrap_or(serde_json::Value::Null)),
        }
    }
}

#[derive(Debug, Clone, SimpleObject)]
pub struct PostProcessingProfile {
    pub profile_id: String,
    pub name: String,
    pub enabled: bool,
    pub created_at_epoch_ms: i64,
    pub updated_at_epoch_ms: i64,
    pub definition: Json<serde_json::Value>,
}

impl From<ProfileRecord> for PostProcessingProfile {
    fn from(value: ProfileRecord) -> Self {
        Self {
            profile_id: value.profile.id().as_str().to_string(),
            name: value.profile.name().to_string(),
            enabled: value.enabled,
            created_at_epoch_ms: value.created_at_epoch_ms,
            updated_at_epoch_ms: value.updated_at_epoch_ms,
            definition: Json(
                serde_json::to_value(value.profile).unwrap_or(serde_json::Value::Null),
            ),
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Enum)]
pub enum PostProcessingAdapterInput {
    Sabnzbd,
    Nzbget,
}

impl From<PostProcessingAdapterInput> for BareScriptAdapter {
    fn from(value: PostProcessingAdapterInput) -> Self {
        match value {
            PostProcessingAdapterInput::Sabnzbd => Self::Sabnzbd,
            PostProcessingAdapterInput::Nzbget => Self::Nzbget,
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Enum)]
pub enum PostProcessingRunWhenInput {
    Always,
    Success,
    Failure,
}

impl From<PostProcessingRunWhenInput> for RunWhen {
    fn from(value: PostProcessingRunWhenInput) -> Self {
        match value {
            PostProcessingRunWhenInput::Always => Self::Always,
            PostProcessingRunWhenInput::Success => Self::PipelineSuccess,
            PostProcessingRunWhenInput::Failure => Self::PipelineFailure,
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Enum)]
pub enum PostProcessingOnFailureInput {
    Stop,
    Continue,
}

impl From<PostProcessingOnFailureInput> for OnFailure {
    fn from(value: PostProcessingOnFailureInput) -> Self {
        match value {
            PostProcessingOnFailureInput::Stop => Self::Stop,
            PostProcessingOnFailureInput::Continue => Self::Continue,
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Enum)]
pub enum PostProcessingOutcomeImpactInput {
    Warn,
    FailJob,
}

impl From<PostProcessingOutcomeImpactInput> for OutcomeImpact {
    fn from(value: PostProcessingOutcomeImpactInput) -> Self {
        match value {
            PostProcessingOutcomeImpactInput::Warn => Self::Warning,
            PostProcessingOutcomeImpactInput::FailJob => Self::FailJob,
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Enum)]
pub enum PostProcessingOptionValueKind {
    String,
    Integer,
    Number,
    Boolean,
    Secret,
}

#[derive(Debug, Clone, InputObject)]
pub struct PostProcessingOptionInput {
    pub name: String,
    pub kind: PostProcessingOptionValueKind,
    pub value: String,
}

impl PostProcessingOptionInput {
    pub(crate) fn into_domain(self) -> Result<ResolvedOption, String> {
        let name = OptionName::new(self.name).map_err(|error| error.to_string())?;
        let value = match self.kind {
            PostProcessingOptionValueKind::String => ResolvedOptionValue::String(self.value),
            PostProcessingOptionValueKind::Integer => ResolvedOptionValue::Integer(
                self.value
                    .parse()
                    .map_err(|_| "invalid integer option value".to_string())?,
            ),
            PostProcessingOptionValueKind::Number => ResolvedOptionValue::Number(
                self.value
                    .parse()
                    .map_err(|_| "invalid numeric option value".to_string())?,
            ),
            PostProcessingOptionValueKind::Boolean => ResolvedOptionValue::Boolean(
                self.value
                    .parse()
                    .map_err(|_| "invalid boolean option value".to_string())?,
            ),
            PostProcessingOptionValueKind::Secret => {
                ResolvedOptionValue::Secret(SecretOptionValue::from_admin_input(self.value))
            }
        };
        Ok(ResolvedOption::new(name, value))
    }
}

#[derive(Debug, Clone, InputObject)]
pub struct PostProcessingProfileStepInput {
    pub extension_id: String,
    pub revision_id: Option<String>,
    pub run_when: PostProcessingRunWhenInput,
    pub on_failure: PostProcessingOnFailureInput,
    pub outcome_impact: PostProcessingOutcomeImpactInput,
    pub timeout_seconds: Option<u64>,
    #[graphql(default)]
    pub unlimited_timeout: bool,
    #[graphql(default)]
    pub approved_roots: Vec<String>,
    #[graphql(default)]
    pub required_artifact_suffixes: Vec<String>,
    #[graphql(default)]
    pub minimum_artifact_count: u32,
    #[graphql(default)]
    pub options: Vec<PostProcessingOptionInput>,
}

#[derive(Debug, Clone, InputObject)]
pub struct PostProcessingProfileInput {
    pub profile_id: String,
    pub name: String,
    #[graphql(default = true)]
    pub enabled: bool,
    pub steps: Vec<PostProcessingProfileStepInput>,
}

impl PostProcessingProfileInput {
    pub(crate) fn into_domain(
        self,
        settings: &PostProcessingSettings,
    ) -> Result<(Profile, bool), String> {
        let allowed = &settings.allowed_roots;
        let mut steps = Vec::with_capacity(self.steps.len());
        for (index, step) in self.steps.into_iter().enumerate() {
            if step
                .approved_roots
                .iter()
                .any(|root| !allowed.iter().any(|allowed_root| allowed_root == root))
            {
                return Err(
                    "profile contains a filesystem root outside the administrative allowlist"
                        .to_string(),
                );
            }
            let extension_id =
                ExtensionId::new(step.extension_id).map_err(|error| error.to_string())?;
            let selection = if let Some(revision_id) = step.revision_id {
                ExtensionSelection::pinned(
                    extension_id,
                    ExtensionRevisionId::new(revision_id).map_err(|error| error.to_string())?,
                )
            } else {
                ExtensionSelection::latest_approved(extension_id)
            };
            let timeout_policy = if step.unlimited_timeout {
                TimeoutPolicy::Unlimited
            } else if let Some(seconds) = step.timeout_seconds {
                TimeoutPolicy::Finite(
                    NonZeroTimeoutSeconds::new(seconds).map_err(|error| error.to_string())?,
                )
            } else {
                TimeoutPolicy::Default24Hours
            };
            let roots = step
                .approved_roots
                .into_iter()
                .map(ApprovedFilesystemRoot::new)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| error.to_string())?;
            let options = step
                .options
                .into_iter()
                .map(PostProcessingOptionInput::into_domain)
                .collect::<Result<Vec<_>, _>>()?;
            let artifact_condition = ArtifactCondition::new(
                step.required_artifact_suffixes,
                step.minimum_artifact_count,
            )
            .map_err(|error| error.to_string())?;
            steps.push(
                OrderedStep::new(
                    u32::try_from(index).map_err(|_| "too many profile steps".to_string())?,
                    selection,
                    step.run_when.into(),
                    step.on_failure.into(),
                    step.outcome_impact.into(),
                    timeout_policy,
                    ApprovedFilesystemRoots::new(roots),
                    options,
                )
                .map_err(|error| error.to_string())?
                .with_artifact_condition(artifact_condition),
            );
        }
        let profile = Profile::new(
            ProfileId::new(self.profile_id).map_err(|error| error.to_string())?,
            self.name,
            steps,
        )
        .map_err(|error| error.to_string())?;
        Ok((profile, self.enabled))
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Enum)]
pub enum PostProcessingSelectionModeInput {
    Inherit,
    Disabled,
    Profile,
    Extensions,
}

#[derive(Debug, Clone, PartialEq, Eq, InputObject)]
pub struct PostProcessingSelectionInput {
    pub mode: PostProcessingSelectionModeInput,
    pub profile_id: Option<String>,
    #[graphql(default)]
    pub extension_ids: Vec<String>,
}

impl PostProcessingSelectionInput {
    pub(crate) fn into_domain(self) -> Result<SubmissionPlanSelection, String> {
        match self.mode {
            PostProcessingSelectionModeInput::Inherit => Ok(SubmissionPlanSelection::inherit()),
            PostProcessingSelectionModeInput::Disabled => Ok(SubmissionPlanSelection::disabled()),
            PostProcessingSelectionModeInput::Profile => Ok(SubmissionPlanSelection::profile(
                ProfileId::new(
                    self.profile_id
                        .ok_or_else(|| "PROFILE mode requires profileId".to_string())?,
                )
                .map_err(|error| error.to_string())?,
            )),
            PostProcessingSelectionModeInput::Extensions => SubmissionPlanSelection::extensions(
                self.extension_ids
                    .into_iter()
                    .map(|extension_id| {
                        ExtensionId::new(extension_id).map(ExtensionSelection::latest_approved)
                    })
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string()),
        }
    }
}

#[derive(Debug, Clone, SimpleObject)]
pub struct PostProcessingJobPlan {
    pub job_id: u64,
    pub definition: Json<serde_json::Value>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Enum)]
pub enum PostProcessingRerunModeInput {
    All,
    Selected,
    FailedAndLater,
}

#[derive(Debug, Clone, InputObject)]
pub struct PostProcessingRerunInput {
    pub run_id: String,
    pub mode: PostProcessingRerunModeInput,
    #[graphql(default)]
    pub step_indexes: Vec<u32>,
    #[graphql(default)]
    pub rebind_to_latest_approved: bool,
}

#[derive(Debug, Clone, InputObject)]
pub struct PostProcessingDiagnosticInput {
    pub extension_id: String,
    pub revision_id: String,
    pub command: String,
    #[graphql(default)]
    pub options: Vec<PostProcessingOptionInput>,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct PostProcessingDiagnosticOutputLine {
    pub sequence: u64,
    pub stream: String,
    pub text: String,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct PostProcessingDiagnosticResult {
    pub succeeded: bool,
    pub exit_code: Option<i32>,
    pub error_message: Option<String>,
    pub output_truncated: bool,
    pub output: Vec<PostProcessingDiagnosticOutputLine>,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct PostProcessingRun {
    pub run_id: String,
    pub job_id: u64,
    pub status: String,
    pub pipeline_outcome: Json<serde_json::Value>,
    pub summary: String,
    pub terminal_intent: String,
    pub plan: Json<serde_json::Value>,
    pub rerun_of_run_id: Option<String>,
    pub queued_at_epoch_ms: i64,
    pub queue_position: i64,
    pub started_at_epoch_ms: Option<i64>,
    pub finished_at_epoch_ms: Option<i64>,
}

impl From<PostProcessingRunRecord> for PostProcessingRun {
    fn from(value: PostProcessingRunRecord) -> Self {
        Self {
            run_id: value.run_id.as_str().to_string(),
            job_id: value.job_id,
            status: format!("{:?}", value.status).to_ascii_uppercase(),
            pipeline_outcome: Json(
                serde_json::to_value(value.pipeline_outcome).unwrap_or(serde_json::Value::Null),
            ),
            summary: format!("{:?}", value.summary).to_ascii_uppercase(),
            terminal_intent: format!("{:?}", value.terminal_intent).to_ascii_uppercase(),
            plan: Json(serde_json::to_value(value.plan).unwrap_or(serde_json::Value::Null)),
            rerun_of_run_id: value.rerun_of_run_id.map(|id| id.as_str().to_string()),
            queued_at_epoch_ms: value.queued_at_epoch_ms,
            queue_position: value.queue_position,
            started_at_epoch_ms: value.started_at_epoch_ms,
            finished_at_epoch_ms: value.finished_at_epoch_ms,
        }
    }
}

#[derive(Debug, Clone, SimpleObject)]
pub struct PostProcessingAttempt {
    pub attempt_id: String,
    pub run_id: String,
    pub step_index: u32,
    pub status: String,
    pub extension_id: String,
    pub revision_id: String,
    pub adapter: String,
    pub working_directory: Option<String>,
    pub exit_code: Option<i32>,
    pub error_message: Option<String>,
    pub progress: Option<Json<serde_json::Value>>,
    pub output_truncated: bool,
    pub queued_at_epoch_ms: i64,
    pub started_at_epoch_ms: Option<i64>,
    pub finished_at_epoch_ms: Option<i64>,
}

impl From<PostProcessingAttemptRecord> for PostProcessingAttempt {
    fn from(value: PostProcessingAttemptRecord) -> Self {
        let progress = value.reported_progress();
        Self {
            attempt_id: value.attempt_id.as_str().to_string(),
            run_id: value.run_id.as_str().to_string(),
            step_index: value.step_index,
            status: format!("{:?}", value.status).to_ascii_uppercase(),
            extension_id: value.extension_id.as_str().to_string(),
            revision_id: value.revision_id.as_str().to_string(),
            adapter: format!("{:?}", value.adapter).to_ascii_uppercase(),
            working_directory: value.working_directory,
            exit_code: value.exit_code,
            error_message: value.error_message,
            progress: progress.map(Json),
            output_truncated: value.output_truncated,
            queued_at_epoch_ms: value.queued_at_epoch_ms,
            started_at_epoch_ms: value.started_at_epoch_ms,
            finished_at_epoch_ms: value.finished_at_epoch_ms,
        }
    }
}

#[derive(Debug, Clone, SimpleObject)]
pub struct PostProcessingArtifact {
    pub attempt_id: String,
    pub step_index: u32,
    pub path: String,
    pub exists: bool,
    pub is_file: bool,
    pub is_directory: bool,
    pub is_symlink: bool,
    pub size_bytes: Option<u64>,
}

impl From<PostProcessingArtifactRecord> for PostProcessingArtifact {
    fn from(value: PostProcessingArtifactRecord) -> Self {
        Self {
            attempt_id: value.attempt_id.as_str().to_string(),
            step_index: value.step_index,
            path: value.path.to_string_lossy().into_owned(),
            exists: value.exists,
            is_file: value.is_file,
            is_directory: value.is_directory,
            is_symlink: value.is_symlink,
            size_bytes: value.size_bytes,
        }
    }
}

#[derive(Debug, Clone, SimpleObject)]
pub struct PostProcessingLogChunkGql {
    pub sequence: u64,
    pub stream: String,
    pub text: String,
    pub created_at_epoch_ms: i64,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct PostProcessingLogPageGql {
    pub chunks: Vec<PostProcessingLogChunkGql>,
    pub next_cursor: Option<u64>,
    pub truncated: bool,
}

impl From<PostProcessingLogPage> for PostProcessingLogPageGql {
    fn from(value: PostProcessingLogPage) -> Self {
        Self {
            chunks: value
                .chunks
                .into_iter()
                .map(|chunk| PostProcessingLogChunkGql {
                    sequence: chunk.sequence,
                    stream: match chunk.stream {
                        LogStream::Stdout => "STDOUT",
                        LogStream::Stderr => "STDERR",
                        LogStream::System => "SYSTEM",
                    }
                    .to_string(),
                    text: String::from_utf8_lossy(&chunk.payload).into_owned(),
                    created_at_epoch_ms: chunk.created_at_epoch_ms,
                })
                .collect(),
            next_cursor: value.next_cursor,
            truncated: value.truncated,
        }
    }
}
