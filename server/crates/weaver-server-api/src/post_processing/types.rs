use async_graphql::{Enum, InputObject, MaybeUndefined, SimpleObject};
use chrono::{DateTime, TimeZone, Utc};
use weaver_server_core::post_processing::instances::{
    InstanceInputDraft, InstanceTrigger, ScriptInstance, ScriptInstanceDraft,
};
use weaver_server_core::post_processing::listing::{
    DiscoveredScript, ScriptListing, ScriptProblem,
};
use weaver_server_core::post_processing::model::{
    GlobalScriptsRun, OptionValue, PostProcessingSettings, ScriptAdapter, ScriptName, ScriptOption,
    ScriptOptionType, ScriptResult, ScriptSelectValue,
};
use weaver_server_core::post_processing::output::ScriptRun;
use weaver_server_core::post_processing::preset::ScriptPreset;
use weaver_server_core::post_processing::secrets::{Secret, SecretRef};
use weaver_server_core::post_processing::test_run::ScriptTestSnapshot;

/// Placeholder shown instead of a stored secret. Secrets leave the process only
/// as environment values for the script that declared them.
pub const MASKED_SECRET: &str = "[REDACTED]";

#[derive(Debug, Clone, SimpleObject)]
pub struct PostProcessingSettingsGql {
    pub event_script_concurrency: u8,
    pub event_script_timeout_seconds: u64,
    pub file_downloaded_event_interval: i64,
    pub script_output_ceiling_bytes: u64,
    pub script_output_runs_per_job: u32,
    pub script_output_ring_bytes: u64,
    pub script_output_run_cap_bytes: u64,
    pub script_directory: String,
    pub execution_enabled: bool,
    pub concurrency: u8,
    pub termination_grace_seconds: u64,
    pub python_interpreter: Option<String>,
    pub powershell_interpreter: Option<String>,
    pub batch_interpreter: Option<String>,
    pub unacceptable_extensions: Vec<String>,
    /// True when `WEAVER_STRICT_SECURITY` refuses script execution outright.
    pub strict_security_refuses_execution: bool,
    /// Whether instances for every category also run for a category that has
    /// instances of its own.
    pub global_scripts_run: GlobalScriptsRunGql,
}

/// When instances that are not narrowed to a category run.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Enum)]
#[graphql(name = "GlobalScriptsRun")]
pub enum GlobalScriptsRunGql {
    /// For every download, ahead of the ones for its category.
    Always,
    /// Only for a download whose category has no instances of its own.
    OnlyWithoutCategoryScripts,
}

impl From<GlobalScriptsRun> for GlobalScriptsRunGql {
    fn from(value: GlobalScriptsRun) -> Self {
        match value {
            GlobalScriptsRun::Always => Self::Always,
            GlobalScriptsRun::OnlyWithoutCategoryScripts => Self::OnlyWithoutCategoryScripts,
        }
    }
}

impl From<GlobalScriptsRunGql> for GlobalScriptsRun {
    fn from(value: GlobalScriptsRunGql) -> Self {
        match value {
            GlobalScriptsRunGql::Always => Self::Always,
            GlobalScriptsRunGql::OnlyWithoutCategoryScripts => Self::OnlyWithoutCategoryScripts,
        }
    }
}

impl PostProcessingSettingsGql {
    pub fn from_settings(
        value: PostProcessingSettings,
        script_directory: impl Into<String>,
        strict_security: bool,
    ) -> Self {
        Self {
            script_directory: script_directory.into(),
            event_script_concurrency: value.event_scripts.event_script_concurrency,
            event_script_timeout_seconds: value.event_scripts.event_script_timeout_seconds,
            file_downloaded_event_interval: value.event_scripts.file_downloaded_event_interval,
            script_output_ceiling_bytes: value.event_scripts.script_output_ceiling_bytes,
            script_output_runs_per_job: value.event_scripts.script_output_runs_per_job,
            script_output_ring_bytes: value.event_scripts.script_output_ring_bytes,
            script_output_run_cap_bytes: value.event_scripts.script_output_run_cap_bytes,
            execution_enabled: value.execution_enabled,
            concurrency: value.concurrency,
            termination_grace_seconds: value.termination_grace_seconds,
            python_interpreter: value.python_interpreter,
            powershell_interpreter: value.powershell_interpreter,
            batch_interpreter: value.batch_interpreter,
            unacceptable_extensions: value.unacceptable_extensions,
            strict_security_refuses_execution: strict_security,
            global_scripts_run: value.global_scripts_run.into(),
        }
    }
}

#[derive(Debug, Clone, InputObject)]
pub struct PostProcessingSettingsInput {
    pub event_script_concurrency: Option<u8>,
    pub event_script_timeout_seconds: Option<u64>,
    pub file_downloaded_event_interval: Option<i64>,
    pub script_output_ceiling_bytes: Option<u64>,
    pub script_output_runs_per_job: Option<u32>,
    pub script_output_ring_bytes: Option<u64>,
    pub script_output_run_cap_bytes: Option<u64>,
    pub execution_enabled: bool,
    pub concurrency: u8,
    pub termination_grace_seconds: u64,
    pub python_interpreter: Option<String>,
    pub powershell_interpreter: Option<String>,
    pub batch_interpreter: Option<String>,
    /// Omission preserves the existing policy; a supplied empty list disables
    /// it. `null` is deliberately distinguishable and refused by the mutation.
    pub unacceptable_extensions: MaybeUndefined<Vec<String>>,
    /// Omission keeps the present choice.
    pub global_scripts_run: Option<GlobalScriptsRunGql>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Enum)]
pub enum ScriptAdapterGql {
    Sabnzbd,
    Nzbget,
}

impl From<ScriptAdapter> for ScriptAdapterGql {
    fn from(value: ScriptAdapter) -> Self {
        match value {
            ScriptAdapter::Sabnzbd => Self::Sabnzbd,
            ScriptAdapter::Nzbget => Self::Nzbget,
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Enum)]
pub enum ScriptOptionTypeGql {
    String,
    Integer,
    Number,
    Boolean,
    Secret,
}

impl From<ScriptOptionType> for ScriptOptionTypeGql {
    fn from(value: ScriptOptionType) -> Self {
        match value {
            ScriptOptionType::String => Self::String,
            ScriptOptionType::Integer => Self::Integer,
            ScriptOptionType::Number => Self::Number,
            ScriptOptionType::Boolean => Self::Boolean,
            ScriptOptionType::Secret => Self::Secret,
        }
    }
}

#[derive(Debug, Clone, SimpleObject)]
pub struct ScriptOptionGql {
    pub name: String,
    pub section: Option<String>,
    pub option_type: ScriptOptionTypeGql,
    pub display_name: Option<String>,
    pub description: Vec<String>,
    pub select: Vec<String>,
    pub required: bool,
    /// Manifest default, already masked when the option is secret.
    pub default_value: Option<String>,
}

fn select_text(value: &ScriptSelectValue) -> String {
    match value {
        ScriptSelectValue::String(value) => value.clone(),
        ScriptSelectValue::Number(value) => value.to_string(),
    }
}

pub fn option_value_text(value: &OptionValue) -> String {
    match value {
        OptionValue::String(value) => value.clone(),
        OptionValue::Integer(value) => value.to_string(),
        OptionValue::Number(value) => value.to_string(),
        OptionValue::Boolean(value) => if *value { "yes" } else { "no" }.to_string(),
        OptionValue::Secret(_) => MASKED_SECRET.to_string(),
    }
}

fn script_option_gql(declaration: &ScriptOption) -> ScriptOptionGql {
    ScriptOptionGql {
        name: declaration.name().as_str().to_string(),
        section: declaration.section().map(str::to_string),
        option_type: declaration.option_type().into(),
        display_name: declaration.display_name().map(str::to_string),
        description: declaration.description().to_vec(),
        select: declaration.select().iter().map(select_text).collect(),
        required: declaration.required(),
        default_value: declaration.default().map(option_value_text),
    }
}

#[derive(Debug, Clone, SimpleObject)]
pub struct ScriptGql {
    pub name: String,
    pub display_name: String,
    pub adapter: ScriptAdapterGql,
    pub kinds: Vec<ScriptKindGql>,
    pub queue_events: Vec<QueueEventGql>,
    pub task_times: Vec<String>,
    pub version: Option<String>,
    /// What the header declares about each input, for drawing a form.
    pub options: Vec<ScriptOptionGql>,
    /// What the header offers as a starting point for an instance.
    pub preset: ScriptPresetGql,
}

impl ScriptGql {
    pub fn new(script: &DiscoveredScript) -> Self {
        Self {
            preset: ScriptPreset::of(&script.manifest).into(),
            name: script.name.as_str().to_string(),
            display_name: script.manifest.display_name().to_string(),
            adapter: script.manifest.adapter().into(),
            kinds: script
                .manifest
                .kinds()
                .iter()
                .copied()
                .map(Into::into)
                .collect(),
            queue_events: script
                .manifest
                .queue_events()
                .iter()
                .copied()
                .map(Into::into)
                .collect(),
            task_times: script
                .manifest
                .task_times()
                .iter()
                .map(ToString::to_string)
                .collect(),
            version: script.manifest.version().map(str::to_string),
            options: script
                .manifest
                .options()
                .iter()
                .map(script_option_gql)
                .collect(),
        }
    }
}

#[derive(Debug, Clone, SimpleObject)]
pub struct ScriptProblemGql {
    pub name: String,
    pub message: String,
}

impl From<ScriptProblem> for ScriptProblemGql {
    fn from(value: ScriptProblem) -> Self {
        Self {
            name: value.name,
            message: value.message,
        }
    }
}

#[derive(Debug, Clone, SimpleObject)]
pub struct ScriptListingGql {
    pub scripts: Vec<ScriptGql>,
    /// Entries that look like scripts but could not be listed, so an unparseable
    /// manifest is visible instead of silently absent.
    pub problems: Vec<ScriptProblemGql>,
}

/// One saved input: a value, or a link to a named secret. A secret's value is
/// never read back out.
#[derive(Debug, Clone, SimpleObject)]
#[graphql(name = "ScriptInstanceValue")]
pub struct ScriptInstanceValueGql {
    pub name: String,
    /// Empty when the input is linked to a secret.
    pub value: String,
    /// The secret the input is linked to, or null for a plain value.
    pub secret: Option<SecretRefGql>,
}

/// A named secret an input is linked to.
#[derive(Debug, Clone, SimpleObject)]
#[graphql(name = "SecretRef")]
pub struct SecretRefGql {
    pub id: String,
    pub name: String,
}

impl From<SecretRef> for SecretRefGql {
    fn from(value: SecretRef) -> Self {
        Self {
            id: value.id,
            name: value.name,
        }
    }
}

/// An instance that links a secret.
#[derive(Debug, Clone, SimpleObject)]
#[graphql(name = "ScriptInstanceRef")]
pub struct ScriptInstanceRefGql {
    pub id: String,
    pub name: String,
}

/// A value kept encrypted under its own name, for script inputs to link.
/// The value is write-only.
#[derive(Debug, Clone, SimpleObject)]
#[graphql(name = "Secret")]
pub struct SecretGql {
    pub id: String,
    pub name: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    /// The instances that link it, by name.
    pub used_by: Vec<ScriptInstanceRefGql>,
}

fn ms_to_datetime(ms: i64) -> DateTime<Utc> {
    Utc.timestamp_millis_opt(ms).single().unwrap_or_default()
}

impl From<Secret> for SecretGql {
    fn from(value: Secret) -> Self {
        Self {
            id: value.id,
            name: value.name,
            created_at: ms_to_datetime(value.created_at_ms),
            updated_at: ms_to_datetime(value.updated_at_ms),
            used_by: value
                .used_by
                .into_iter()
                .map(|usage| ScriptInstanceRefGql {
                    id: usage.instance_id,
                    name: usage.instance_name,
                })
                .collect(),
        }
    }
}

/// One input a script's header declares, at its default. A secret input has
/// no value: it is filled by linking a secret.
#[derive(Debug, Clone, SimpleObject)]
#[graphql(name = "ScriptPresetValue")]
pub struct ScriptPresetValueGql {
    pub name: String,
    pub value: String,
    pub secret: bool,
}

/// A trigger a script's header declares.
#[derive(Debug, Clone, SimpleObject)]
#[graphql(name = "ScriptPresetTrigger")]
pub struct ScriptPresetTriggerGql {
    pub trigger: ScriptKindGql,
    /// Set when `trigger` is `QUEUE`.
    pub queue_event: Option<QueueEventGql>,
}

/// What a script's header offers as a starting point. It fills a form and
/// nothing more: a saved instance never follows the header.
#[derive(Debug, Clone, SimpleObject)]
#[graphql(name = "ScriptPreset")]
pub struct ScriptPresetGql {
    /// One per instance the header asks for.
    pub triggers: Vec<ScriptPresetTriggerGql>,
    /// When a schedule instance is meant to run.
    pub task_times: Vec<String>,
    /// Every declared input at its default. A secret has no value.
    pub inputs: Vec<ScriptPresetValueGql>,
}

fn trigger_parts(trigger: InstanceTrigger) -> (ScriptKindGql, Option<QueueEventGql>) {
    let event = match trigger {
        InstanceTrigger::Queue(event) => Some(event.into()),
        _ => None,
    };
    (trigger.kind().into(), event)
}

impl From<ScriptPreset> for ScriptPresetGql {
    fn from(value: ScriptPreset) -> Self {
        Self {
            triggers: value
                .triggers
                .into_iter()
                .map(|trigger| {
                    let (trigger, queue_event) = trigger_parts(trigger);
                    ScriptPresetTriggerGql {
                        trigger,
                        queue_event,
                    }
                })
                .collect(),
            task_times: value.task_times.iter().map(ToString::to_string).collect(),
            inputs: value
                .inputs
                .into_iter()
                .map(|input| ScriptPresetValueGql {
                    value: if input.secret {
                        String::new()
                    } else {
                        input.value
                    },
                    name: input.name,
                    secret: input.secret,
                })
                .collect(),
        }
    }
}

/// A script wired to one trigger, with the inputs and run policy saved for it.
/// What is saved here is what runs.
#[derive(Debug, Clone, SimpleObject)]
#[graphql(name = "ScriptInstance")]
pub struct ScriptInstanceGql {
    pub id: String,
    pub name: String,
    pub script: String,
    pub trigger: ScriptKindGql,
    /// Set when `trigger` is `QUEUE`.
    pub queue_event: Option<QueueEventGql>,
    pub inputs: Vec<ScriptInstanceValueGql>,
    /// Empty runs for every category. Only post-processing and queue instances
    /// can be narrowed.
    pub categories: Vec<String>,
    pub enabled: bool,
    /// Whether whatever raised the trigger waits for the script. One that is
    /// not waited for is started and left to finish on its own, and its result
    /// changes nothing.
    pub blocking: bool,
    /// Null runs under the default timeout of its trigger.
    pub timeout_seconds: Option<u64>,
    pub run_order: i64,
    /// Why the script cannot run as things stand, such as its file having gone
    /// from the scripts directory. Null when it can.
    pub script_problem: Option<String>,
    /// The script's header no longer declares the inputs this instance holds.
    pub header_drift: bool,
}

/// The scripts directory as it is now, for judging saved instances against.
pub(crate) struct ScriptDirectoryView {
    listing: Result<ScriptListing, String>,
}

impl ScriptDirectoryView {
    pub(crate) fn new(listing: Result<ScriptListing, String>) -> Self {
        Self { listing }
    }

    pub(crate) fn instance(&self, instance: ScriptInstance) -> ScriptInstanceGql {
        let (script_problem, header_drift) = match &self.listing {
            Err(error) => (Some(error.clone()), false),
            Ok(listing) => match listing
                .scripts
                .iter()
                .find(|script| script.name == instance.script)
            {
                Some(script) => (
                    None,
                    ScriptPreset::of(&script.manifest).drifted_from(&instance),
                ),
                None => (
                    Some(
                        listing
                            .problems
                            .iter()
                            .find(|problem| problem.name == instance.script.as_str())
                            .map(|problem| problem.message.clone())
                            .unwrap_or_else(|| {
                                "the script is no longer in the scripts directory".to_string()
                            }),
                    ),
                    false,
                ),
            },
        };
        let (trigger, queue_event) = trigger_parts(instance.trigger);
        ScriptInstanceGql {
            id: instance.id,
            name: instance.name,
            script: instance.script.as_str().to_string(),
            trigger,
            queue_event,
            inputs: instance
                .inputs
                .into_iter()
                .map(|input| ScriptInstanceValueGql {
                    name: input.name.as_str().to_string(),
                    value: if input.secret.is_some() {
                        String::new()
                    } else {
                        input.value
                    },
                    secret: input.secret.map(Into::into),
                })
                .collect(),
            categories: instance.categories,
            enabled: instance.enabled,
            blocking: instance.blocking,
            timeout_seconds: instance.timeout_seconds,
            run_order: instance.run_order,
            script_problem,
            header_drift,
        }
    }
}

#[derive(Debug, Clone, InputObject)]
#[graphql(name = "ScriptInstanceValueInput")]
pub struct ScriptInstanceValueInput {
    pub name: String,
    /// A plain value. Give this or `secretId`, not both.
    pub value: Option<String>,
    /// The secret to link. Give this or `value`, not both.
    pub secret_id: Option<String>,
}

#[derive(Debug, Clone, InputObject)]
#[graphql(name = "ScriptInstanceInput")]
pub struct ScriptInstanceInput {
    /// Empty takes the script's name.
    #[graphql(default)]
    pub name: String,
    pub script: String,
    pub trigger: ScriptKindGql,
    /// Required when `trigger` is `QUEUE`, ignored otherwise.
    pub queue_event: Option<QueueEventGql>,
    #[graphql(default)]
    pub inputs: Vec<ScriptInstanceValueInput>,
    /// Empty runs for every category.
    #[graphql(default)]
    pub categories: Vec<String>,
    #[graphql(default = true)]
    pub enabled: bool,
    #[graphql(default = true)]
    pub blocking: bool,
    pub timeout_seconds: Option<u64>,
}

impl ScriptInstanceInput {
    pub(crate) fn into_draft(self) -> Result<ScriptInstanceDraft, String> {
        let trigger = match self.trigger {
            ScriptKindGql::PostProcessing => InstanceTrigger::PostProcessing,
            ScriptKindGql::Queue => InstanceTrigger::Queue(
                self.queue_event
                    .ok_or("a queue instance needs the event it runs on")?
                    .into(),
            ),
            ScriptKindGql::Scan => InstanceTrigger::Scan,
            ScriptKindGql::Scheduler => InstanceTrigger::Schedule,
            ScriptKindGql::Feed => InstanceTrigger::Feed,
        };
        Ok(ScriptInstanceDraft {
            name: self.name,
            script: ScriptName::new(self.script).map_err(|error| error.to_string())?,
            trigger,
            inputs: self
                .inputs
                .into_iter()
                .map(|input| InstanceInputDraft {
                    name: input.name,
                    value: input.value,
                    secret_id: input.secret_id,
                })
                .collect(),
            categories: self.categories,
            enabled: self.enabled,
            blocking: self.blocking,
            timeout_seconds: self.timeout_seconds,
        })
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Enum)]
pub enum ScriptStatusGql {
    Succeeded,
    Skipped,
    Warning,
    Failed,
    TimedOut,
    Cancelled,
}

impl From<weaver_server_core::post_processing::model::ScriptStatus> for ScriptStatusGql {
    fn from(value: weaver_server_core::post_processing::model::ScriptStatus) -> Self {
        use weaver_server_core::post_processing::model::ScriptStatus;
        match value {
            ScriptStatus::Succeeded => Self::Succeeded,
            ScriptStatus::Skipped => Self::Skipped,
            ScriptStatus::Warning => Self::Warning,
            ScriptStatus::Failed => Self::Failed,
            ScriptStatus::TimedOut => Self::TimedOut,
            ScriptStatus::Cancelled => Self::Cancelled,
        }
    }
}

impl From<ScriptStatusGql> for weaver_server_core::post_processing::model::ScriptStatus {
    fn from(value: ScriptStatusGql) -> Self {
        match value {
            ScriptStatusGql::Succeeded => Self::Succeeded,
            ScriptStatusGql::Skipped => Self::Skipped,
            ScriptStatusGql::Warning => Self::Warning,
            ScriptStatusGql::Failed => Self::Failed,
            ScriptStatusGql::TimedOut => Self::TimedOut,
            ScriptStatusGql::Cancelled => Self::Cancelled,
        }
    }
}

#[derive(Debug, Clone, SimpleObject)]
pub struct ScriptResultGql {
    pub output_id: Option<String>,
    pub output_retained: bool,
    pub script: String,
    /// The instance that ran, when the run came from one.
    pub instance_id: Option<String>,
    /// The instance's name as it was when it ran.
    pub instance_name: Option<String>,
    pub event: String,
    /// The run was started without anything waiting for it.
    pub background: bool,
    pub adapter: ScriptAdapterGql,
    pub status: ScriptStatusGql,
    pub exit_code: Option<i32>,
    pub duration_ms: u64,
    pub output_tail: String,
    pub output_truncated: bool,
    pub error_message: Option<String>,
    pub finished_at_epoch_ms: i64,
}

impl From<ScriptResult> for ScriptResultGql {
    fn from(value: ScriptResult) -> Self {
        Self {
            output_id: value.output_id,
            output_retained: false,
            script: value.script.as_str().to_string(),
            instance_id: value.instance_id,
            instance_name: value.instance_name,
            event: value.event.to_string(),
            background: value.background,
            adapter: value.adapter.into(),
            status: value.status.into(),
            exit_code: value.exit_code,
            duration_ms: value.duration_ms,
            output_tail: value.output_tail,
            output_truncated: value.output_truncated,
            error_message: value.error_message,
            finished_at_epoch_ms: value.finished_at_epoch_ms,
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Enum)]
#[graphql(name = "ScriptKind")]
pub enum ScriptKindGql {
    PostProcessing,
    Queue,
    Scan,
    Scheduler,
    Feed,
}

/// One recorded run of a script, whatever started it.
#[derive(Debug, Clone, SimpleObject)]
#[graphql(name = "ScriptRun")]
pub struct ScriptRunGql {
    /// Names the run's kept output for `scriptOutput`, when `outputRetained`.
    pub id: String,
    pub job_id: Option<u64>,
    /// Known once the job has reached history.
    pub job_name: Option<String>,
    pub script: String,
    /// The instance that ran, when the run came from one.
    pub instance_id: Option<String>,
    /// The instance's name as it was when it ran.
    pub instance_name: Option<String>,
    pub event: String,
    pub kind: ScriptKindGql,
    /// The run was started without anything waiting for it.
    pub background: bool,
    pub adapter: ScriptAdapterGql,
    pub status: ScriptStatusGql,
    pub exit_code: Option<i32>,
    pub duration_ms: u64,
    pub output_tail: String,
    pub output_truncated: bool,
    pub output_retained: bool,
    pub error_message: Option<String>,
    pub finished_at_epoch_ms: i64,
}

impl From<ScriptRun> for ScriptRunGql {
    fn from(value: ScriptRun) -> Self {
        let result = value.result;
        Self {
            id: value.id,
            job_id: value.job_id,
            job_name: value.job_name,
            script: result.script.as_str().to_string(),
            instance_id: result.instance_id,
            instance_name: result.instance_name,
            kind: result.event.kind().into(),
            event: result.event.to_string(),
            background: result.background,
            adapter: result.adapter.into(),
            status: result.status.into(),
            exit_code: result.exit_code,
            duration_ms: result.duration_ms,
            output_tail: result.output_tail,
            output_truncated: result.output_truncated,
            output_retained: value.output_retained,
            error_message: result.error_message,
            finished_at_epoch_ms: result.finished_at_epoch_ms,
        }
    }
}

#[derive(Debug, Clone, SimpleObject)]
#[graphql(name = "ScriptRunPage")]
pub struct ScriptRunPageGql {
    /// Latest first.
    pub runs: Vec<ScriptRunGql>,
    /// Pass as `before` for the runs that follow; absent on the last page.
    pub next_before: Option<String>,
    /// How many runs the filter matches across every page.
    pub total: u64,
    /// How the runs the filter matches ended, leaving its own `status` out.
    /// A status no run ended with is absent.
    pub status_counts: Vec<ScriptRunStatusCountGql>,
}

#[derive(Debug, Clone, SimpleObject)]
#[graphql(name = "ScriptRunStatusCount")]
pub struct ScriptRunStatusCountGql {
    pub status: ScriptStatusGql,
    pub count: u64,
}

impl From<ScriptKindGql> for weaver_server_core::post_processing::model::ScriptKind {
    fn from(value: ScriptKindGql) -> Self {
        match value {
            ScriptKindGql::PostProcessing => Self::PostProcessing,
            ScriptKindGql::Queue => Self::Queue,
            ScriptKindGql::Scan => Self::Scan,
            ScriptKindGql::Scheduler => Self::Scheduler,
            ScriptKindGql::Feed => Self::Feed,
        }
    }
}

impl From<weaver_server_core::post_processing::model::ScriptKind> for ScriptKindGql {
    fn from(value: weaver_server_core::post_processing::model::ScriptKind) -> Self {
        use weaver_server_core::post_processing::model::ScriptKind;
        match value {
            ScriptKind::PostProcessing => Self::PostProcessing,
            ScriptKind::Queue => Self::Queue,
            ScriptKind::Scan => Self::Scan,
            ScriptKind::Scheduler => Self::Scheduler,
            ScriptKind::Feed => Self::Feed,
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Enum)]
#[graphql(name = "ScriptQueueEvent")]
pub enum QueueEventGql {
    FileDownloaded,
    UrlCompleted,
    NzbMarked,
    NzbAdded,
    NzbNamed,
    NzbDownloaded,
    NzbDeleted,
}

impl From<weaver_server_core::post_processing::model::QueueEvent> for QueueEventGql {
    fn from(value: weaver_server_core::post_processing::model::QueueEvent) -> Self {
        use weaver_server_core::post_processing::model::QueueEvent;
        match value {
            QueueEvent::FileDownloaded => Self::FileDownloaded,
            QueueEvent::UrlCompleted => Self::UrlCompleted,
            QueueEvent::NzbMarked => Self::NzbMarked,
            QueueEvent::NzbAdded => Self::NzbAdded,
            QueueEvent::NzbNamed => Self::NzbNamed,
            QueueEvent::NzbDownloaded => Self::NzbDownloaded,
            QueueEvent::NzbDeleted => Self::NzbDeleted,
        }
    }
}

impl From<QueueEventGql> for weaver_server_core::post_processing::model::QueueEvent {
    fn from(value: QueueEventGql) -> Self {
        match value {
            QueueEventGql::FileDownloaded => Self::FileDownloaded,
            QueueEventGql::UrlCompleted => Self::UrlCompleted,
            QueueEventGql::NzbMarked => Self::NzbMarked,
            QueueEventGql::NzbAdded => Self::NzbAdded,
            QueueEventGql::NzbNamed => Self::NzbNamed,
            QueueEventGql::NzbDownloaded => Self::NzbDownloaded,
            QueueEventGql::NzbDeleted => Self::NzbDeleted,
        }
    }
}

/// One variable made up for a script under test.
#[derive(Debug, Clone, SimpleObject)]
#[graphql(name = "ScriptTestInput")]
pub struct ScriptTestInputGql {
    pub name: String,
    pub value: String,
}

/// An instance run against made-up inputs, as it stands.
#[derive(Debug, Clone, SimpleObject)]
#[graphql(name = "ScriptTestRun")]
pub struct ScriptTestRunGql {
    pub id: String,
    pub instance_id: String,
    pub instance_name: String,
    pub script: String,
    pub event: String,
    pub kind: ScriptKindGql,
    pub adapter: ScriptAdapterGql,
    pub started_at_epoch_ms: i64,
    /// The run is ended after this long.
    pub timeout_seconds: u64,
    /// False once the script has ended, when `status` is set.
    pub running: bool,
    pub status: Option<ScriptStatusGql>,
    pub exit_code: Option<i32>,
    pub duration_ms: Option<u64>,
    pub error_message: Option<String>,
    /// What the script has printed so far, or all of it once the run has ended.
    pub log: String,
    pub log_truncated: bool,
    /// The variables made up for this run, in name order. The instance's own
    /// inputs are left out.
    pub inputs: Vec<ScriptTestInputGql>,
    /// The arguments made up for this run, in order.
    pub arguments: Vec<String>,
    /// Commands the script issued, in order. A test run applies none of them.
    pub commands: Vec<String>,
    pub commands_truncated: bool,
}

impl From<ScriptTestSnapshot> for ScriptTestRunGql {
    fn from(value: ScriptTestSnapshot) -> Self {
        let outcome = value.outcome;
        Self {
            id: value.id,
            instance_id: value.instance_id,
            instance_name: value.instance_name,
            script: value.script.as_str().to_string(),
            kind: value.event.kind().into(),
            event: value.event.to_string(),
            adapter: value.adapter.into(),
            started_at_epoch_ms: value.started_at_epoch_ms,
            timeout_seconds: value.timeout_seconds,
            running: outcome.is_none(),
            status: outcome.as_ref().map(|outcome| outcome.status.into()),
            exit_code: outcome.as_ref().and_then(|outcome| outcome.exit_code),
            duration_ms: outcome.as_ref().map(|outcome| outcome.duration_ms),
            error_message: outcome.and_then(|outcome| outcome.error_message),
            log: value.log,
            log_truncated: value.log_truncated,
            inputs: value
                .inputs
                .into_iter()
                .map(|(name, value)| ScriptTestInputGql { name, value })
                .collect(),
            arguments: value.arguments,
            commands: value.commands,
            commands_truncated: value.commands_truncated,
        }
    }
}
