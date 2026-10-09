use async_graphql::{Enum, InputObject, MaybeUndefined, SimpleObject};
use weaver_server_core::post_processing::listing::{DiscoveredScript, ScriptProblem};
use weaver_server_core::post_processing::model::{
    OptionName, OptionValue, PostProcessingSettings, ResolvedOption, ScriptAdapter, ScriptList,
    ScriptListEntry, ScriptLists, ScriptName, ScriptOption, ScriptOptionType, ScriptResult,
    ScriptSelectValue, SecretOptionValue,
};
use weaver_server_core::post_processing::output::ScriptRun;
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
    /// Global default list plus every per-category override.
    pub lists: ScriptListsGql,
}

impl PostProcessingSettingsGql {
    pub fn from_settings(
        value: PostProcessingSettings,
        lists: ScriptLists,
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
            lists: lists.into(),
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
    /// Operator-supplied value, masked when the option is secret.
    pub value: Option<String>,
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

fn script_option_gql(declaration: &ScriptOption, stored: &[ResolvedOption]) -> ScriptOptionGql {
    let value = stored
        .iter()
        .find(|option| option.name() == declaration.name())
        .map(|option| option_value_text(option.value()));
    ScriptOptionGql {
        name: declaration.name().as_str().to_string(),
        section: declaration.section().map(str::to_string),
        option_type: declaration.option_type().into(),
        display_name: declaration.display_name().map(str::to_string),
        description: declaration.description().to_vec(),
        select: declaration.select().iter().map(select_text).collect(),
        required: declaration.required(),
        default_value: declaration.default().map(option_value_text),
        value,
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
    pub options: Vec<ScriptOptionGql>,
}

impl ScriptGql {
    pub fn new(script: &DiscoveredScript, stored_options: &[ResolvedOption]) -> Self {
        Self {
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
                .map(|declaration| script_option_gql(declaration, stored_options))
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

#[derive(Debug, Clone, SimpleObject)]
pub struct ScriptListEntryGql {
    pub script: String,
    pub enabled: bool,
    pub timeout_seconds: Option<u64>,
    /// Whether the script is waited for. One that is not is started and left
    /// to finish on its own, and its result changes nothing.
    pub blocking: bool,
}

impl From<&ScriptListEntry> for ScriptListEntryGql {
    fn from(value: &ScriptListEntry) -> Self {
        Self {
            script: value.script.as_str().to_string(),
            enabled: value.enabled,
            timeout_seconds: value.timeout_seconds,
            blocking: value.blocking,
        }
    }
}

#[derive(Debug, Clone, SimpleObject)]
pub struct ScriptCategoryListGql {
    pub category: String,
    pub entries: Vec<ScriptListEntryGql>,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct ScriptListsGql {
    pub global: Vec<ScriptListEntryGql>,
    pub categories: Vec<ScriptCategoryListGql>,
}

impl From<ScriptLists> for ScriptListsGql {
    fn from(value: ScriptLists) -> Self {
        Self {
            global: value.global.entries().iter().map(Into::into).collect(),
            categories: value
                .categories
                .iter()
                .map(|(category, list)| ScriptCategoryListGql {
                    category: category.clone(),
                    entries: list.entries().iter().map(Into::into).collect(),
                })
                .collect(),
        }
    }
}

#[derive(Debug, Clone, InputObject)]
pub struct ScriptListEntryInput {
    pub script: String,
    #[graphql(default = true)]
    pub enabled: bool,
    pub timeout_seconds: Option<u64>,
    /// Whether the script is waited for. One that is not is started and left
    /// to finish on its own, and its result changes nothing.
    #[graphql(default = true)]
    pub blocking: bool,
}

#[derive(Debug, Clone, InputObject)]
pub struct ScriptCategoryListInput {
    pub category: String,
    pub entries: Vec<ScriptListEntryInput>,
}

#[derive(Debug, Clone, InputObject)]
pub struct ScriptListsInput {
    #[graphql(default)]
    pub global: Vec<ScriptListEntryInput>,
    #[graphql(default)]
    pub categories: Vec<ScriptCategoryListInput>,
}

fn script_list(entries: Vec<ScriptListEntryInput>) -> Result<ScriptList, String> {
    let entries = entries
        .into_iter()
        .map(|entry| {
            Ok(ScriptListEntry {
                script: ScriptName::new(entry.script).map_err(|error| error.to_string())?,
                enabled: entry.enabled,
                timeout_seconds: entry.timeout_seconds,
                blocking: entry.blocking,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    ScriptList::new(entries).map_err(|error| error.to_string())
}

impl ScriptListsInput {
    pub(crate) fn into_domain(self) -> Result<ScriptLists, String> {
        let mut categories = std::collections::BTreeMap::new();
        for entry in self.categories {
            let category = entry.category.trim().to_string();
            if category.is_empty() {
                return Err("category name cannot be empty".to_string());
            }
            if categories
                .insert(category, script_list(entry.entries)?)
                .is_some()
            {
                return Err("category appears more than once".to_string());
            }
        }
        Ok(ScriptLists {
            global: script_list(self.global)?,
            categories,
        })
    }
}

#[derive(Debug, Clone, InputObject)]
pub struct ScriptOptionInput {
    pub name: String,
    pub option_type: ScriptOptionTypeGql,
    pub value: String,
}

impl ScriptOptionInput {
    pub(crate) fn into_domain(self) -> Result<ResolvedOption, String> {
        let name = OptionName::new(self.name).map_err(|error| error.to_string())?;
        let value = match self.option_type {
            ScriptOptionTypeGql::String => OptionValue::String(self.value),
            ScriptOptionTypeGql::Integer => OptionValue::Integer(
                self.value
                    .parse()
                    .map_err(|_| "invalid integer option value".to_string())?,
            ),
            ScriptOptionTypeGql::Number => OptionValue::Number(
                self.value
                    .parse()
                    .map_err(|_| "invalid numeric option value".to_string())?,
            ),
            ScriptOptionTypeGql::Boolean => OptionValue::Boolean(
                self.value
                    .parse()
                    .map_err(|_| "invalid boolean option value".to_string())?,
            ),
            ScriptOptionTypeGql::Secret => {
                OptionValue::Secret(SecretOptionValue::from_admin_input(self.value))
            }
        };
        Ok(ResolvedOption::new(name, value))
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

#[derive(Debug, Clone, SimpleObject)]
pub struct ScriptResultGql {
    pub output_id: Option<String>,
    pub output_retained: bool,
    pub script: String,
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

/// A script run against made-up inputs, as it stands.
#[derive(Debug, Clone, SimpleObject)]
#[graphql(name = "ScriptTestRun")]
pub struct ScriptTestRunGql {
    pub id: String,
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
    /// The variables made up for this run, in name order. The script's saved
    /// options are left out.
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
