use async_graphql::{Enum, InputObject, MaybeUndefined, SimpleObject};
use weaver_server_core::post_processing::listing::{DiscoveredScript, ScriptProblem};
use weaver_server_core::post_processing::model::{
    OptionName, OptionValue, PostProcessingSettings, ResolvedOption, ScriptAdapter, ScriptList,
    ScriptListEntry, ScriptLists, ScriptName, ScriptOption, ScriptOptionType, ScriptResult,
    ScriptSelectValue, SecretOptionValue,
};

/// Placeholder shown instead of a stored secret. Secrets leave the process only
/// as environment values for the script that declared them.
pub const MASKED_SECRET: &str = "[REDACTED]";

#[derive(Debug, Clone, SimpleObject)]
pub struct PostProcessingSettingsGql {
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
    pub version: Option<String>,
    pub options: Vec<ScriptOptionGql>,
}

impl ScriptGql {
    pub fn new(script: &DiscoveredScript, stored_options: &[ResolvedOption]) -> Self {
        Self {
            name: script.name.as_str().to_string(),
            display_name: script.manifest.display_name().to_string(),
            adapter: script.manifest.adapter().into(),
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
}

impl From<&ScriptListEntry> for ScriptListEntryGql {
    fn from(value: &ScriptListEntry) -> Self {
        Self {
            script: value.script.as_str().to_string(),
            enabled: value.enabled,
            timeout_seconds: value.timeout_seconds,
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
    pub script: String,
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
            script: value.script.as_str().to_string(),
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
