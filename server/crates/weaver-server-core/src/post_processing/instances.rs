//! Script instances: a script wired to one trigger, with the inputs,
//! categories and run policy the operator saved for it.
//!
//! What is stored here is what runs. A script's own header can fill a form, but
//! nothing is ever read back out of it into a saved instance.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::model::{
    GlobalScriptsRun, OptionName, OptionValue, PostProcessingSettings, QueueEvent, ResolvedOption,
    ScriptEventLabel, ScriptKind, ScriptName, SecretOptionValue,
};
use super::secrets::{SecretRef, secrets_exist_tx};
use crate::persistence::encryption::{decrypt_value, encrypt_value};
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime, SqlTx, is_foreign_key_violation};
use crate::persistence::{Database, StateError};

const MAX_INSTANCE_NAME_BYTES: usize = 128;
const MAX_INPUTS: usize = 256;
const MAX_INPUT_VALUE_BYTES: usize = 64 * 1024;
const MAX_CATEGORIES: usize = 256;
const MAX_TIMEOUT_SECONDS: u64 = 7 * 24 * 60 * 60;

/// The one thing that starts an instance.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
pub enum InstanceTrigger {
    PostProcessing,
    Queue(QueueEvent),
    Scan,
    Schedule,
    Feed,
}

impl InstanceTrigger {
    pub fn kind(self) -> ScriptKind {
        match self {
            Self::PostProcessing => ScriptKind::PostProcessing,
            Self::Queue(_) => ScriptKind::Queue,
            Self::Scan => ScriptKind::Scan,
            Self::Schedule => ScriptKind::Scheduler,
            Self::Feed => ScriptKind::Feed,
        }
    }

    /// Whether `event` is one this trigger starts on.
    pub fn starts_on(self, event: &ScriptEventLabel) -> bool {
        match (self, event) {
            (Self::PostProcessing, ScriptEventLabel::PostProcessing)
            | (Self::Scan, ScriptEventLabel::Scan)
            | (Self::Schedule, ScriptEventLabel::Scheduler(_))
            | (Self::Feed, ScriptEventLabel::Feed(_)) => true,
            (Self::Queue(wanted), ScriptEventLabel::Queue(raised)) => wanted == *raised,
            _ => false,
        }
    }

    /// Only a download has a category, so only the triggers a download raises
    /// can be narrowed to one.
    pub fn category_scoped(self) -> bool {
        matches!(self, Self::PostProcessing | Self::Queue(_))
    }

    fn stored(self) -> (&'static str, &'static str) {
        match self {
            Self::PostProcessing => ("post_processing", ""),
            Self::Queue(event) => ("queue", event.as_str()),
            Self::Scan => ("scan", ""),
            Self::Schedule => ("schedule", ""),
            Self::Feed => ("feed", ""),
        }
    }

    fn from_stored(kind: &str, detail: &str) -> Option<Self> {
        match kind {
            "post_processing" => Some(Self::PostProcessing),
            "queue" => QueueEvent::ALL
                .into_iter()
                .find(|event| event.as_str() == detail)
                .map(Self::Queue),
            "scan" => Some(Self::Scan),
            "schedule" => Some(Self::Schedule),
            "feed" => Some(Self::Feed),
            _ => None,
        }
    }
}

impl fmt::Display for InstanceTrigger {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.stored() {
            (kind, "") => f.write_str(kind),
            (kind, detail) => write!(f, "{kind}:{detail}"),
        }
    }
}

impl std::str::FromStr for InstanceTrigger {
    type Err = ScriptInstanceError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (kind, detail) = value.split_once(':').unwrap_or((value, ""));
        Self::from_stored(kind, detail)
            .ok_or(ScriptInstanceError::Invalid("unknown instance trigger"))
    }
}

impl Serialize for InstanceTrigger {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for InstanceTrigger {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

/// One saved input: a plain value, a link to a named secret, or a secret of
/// the instance's own. A secret's value is never read back out for display.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InstanceInput {
    pub name: OptionName,
    /// Empty for a secret of either kind.
    pub value: String,
    pub secret: Option<SecretRef>,
    /// The input is a secret of the instance's own: its value is kept sealed
    /// in the instance's own row, and no named secret stands behind it.
    #[serde(default)]
    pub sealed: bool,
}

impl InstanceInput {
    /// Whether the input is a secret, linked or the instance's own.
    pub fn is_secret(&self) -> bool {
        self.secret.is_some() || self.sealed
    }
}

/// A script wired to one trigger.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScriptInstance {
    pub id: String,
    pub name: String,
    pub script: ScriptName,
    pub trigger: InstanceTrigger,
    pub inputs: Vec<InstanceInput>,
    /// Empty runs for every category.
    pub categories: Vec<String>,
    pub enabled: bool,
    /// A blocking instance is run in line: the next one waits for it, and so
    /// does whatever its trigger holds. One that is not is started and left to
    /// finish on its own, and its result changes nothing.
    pub blocking: bool,
    /// `None` runs under the default timeout of its trigger.
    pub timeout_seconds: Option<u64>,
    pub run_order: i64,
}

impl ScriptInstance {
    /// How long a run of this instance may last: its own limit, or else the
    /// default of its trigger. A test run is held to the same.
    pub fn time_limit(&self, settings: &PostProcessingSettings) -> std::time::Duration {
        match (self.timeout_seconds, self.trigger) {
            (Some(seconds), _) => std::time::Duration::from_secs(seconds),
            (None, InstanceTrigger::PostProcessing) => super::runner::DEFAULT_TIMEOUT,
            (None, _) => {
                std::time::Duration::from_secs(settings.event_scripts.event_script_timeout_seconds)
            }
        }
    }

    fn runs_for(&self, category: Option<&str>) -> Scope {
        if self.categories.is_empty() {
            return Scope::Global;
        }
        let Some(category) = category.map(str::trim) else {
            return Scope::None;
        };
        if self
            .categories
            .iter()
            .any(|scoped| scoped.eq_ignore_ascii_case(category))
        {
            Scope::Category
        } else {
            Scope::None
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Scope {
    Global,
    Category,
    None,
}

/// The instances `event` starts for a download in `category`, in run order.
///
/// Instances for every category run first, then the ones narrowed to this
/// category. When the setting says so, a category with instances of its own
/// leaves the unscoped ones out. Category names are matched without regard to
/// case, because download clients echo their own casing back.
pub fn resolve_instances(
    all: &[ScriptInstance],
    event: &ScriptEventLabel,
    category: Option<&str>,
    global_scripts_run: GlobalScriptsRun,
) -> Vec<ScriptInstance> {
    let matching = all
        .iter()
        .filter(|instance| instance.enabled && instance.trigger.starts_on(event));
    let (mut global, mut scoped) = (Vec::new(), Vec::new());
    for instance in matching {
        if !instance.trigger.category_scoped() {
            global.push(instance.clone());
            continue;
        }
        match instance.runs_for(category) {
            Scope::Global => global.push(instance.clone()),
            Scope::Category => scoped.push(instance.clone()),
            Scope::None => {}
        }
    }
    global.sort_by_key(|instance| instance.run_order);
    scoped.sort_by_key(|instance| instance.run_order);
    if global_scripts_run == GlobalScriptsRun::OnlyWithoutCategoryScripts && !scoped.is_empty() {
        return scoped;
    }
    global.extend(scoped);
    global
}

#[derive(Debug, thiserror::Error)]
pub enum ScriptInstanceError {
    #[error("{0}")]
    Invalid(&'static str),
    /// An input was sent as a secret of the instance's own with no value, and
    /// the instance holds none under that name to keep.
    #[error("input \"{0}\" is marked secret but was given no value, and none is saved for it")]
    NoOwnSecret(String),
    #[error("script instance does not exist")]
    NotFound,
    #[error(transparent)]
    Storage(#[from] StateError),
}

/// One input as the operator sent it: a plain value, the id of a secret to
/// link, or a secret of the instance's own.
///
/// A secret of the instance's own is `sealed` with a value to seal, or, on an
/// update, `sealed` with nothing else to keep the one the instance already
/// holds under that name.
#[derive(Clone, Eq, PartialEq)]
pub struct InstanceInputDraft {
    pub name: String,
    pub value: Option<String>,
    pub secret_id: Option<String>,
    pub sealed: bool,
}

/// A value on its way to being sealed is not printed.
impl fmt::Debug for InstanceInputDraft {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match &self.value {
            Some(_) if self.sealed => Some("<sealed>"),
            value => value.as_deref(),
        };
        formatter
            .debug_struct("InstanceInputDraft")
            .field("name", &self.name)
            .field("value", &value)
            .field("secret_id", &self.secret_id)
            .field("sealed", &self.sealed)
            .finish()
    }
}

impl InstanceInputDraft {
    pub fn plain(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: Some(value.into()),
            secret_id: None,
            sealed: false,
        }
    }

    pub fn secret(name: impl Into<String>, secret_id: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: None,
            secret_id: Some(secret_id.into()),
            sealed: false,
        }
    }

    /// A secret of the instance's own, to be sealed from `value`.
    pub fn sealed(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: Some(value.into()),
            secret_id: None,
            sealed: true,
        }
    }

    /// The secret of its own the instance already holds under `name`, kept
    /// as it is.
    pub fn kept_sealed(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: None,
            secret_id: None,
            sealed: true,
        }
    }
}

/// An instance as the operator sent it, before it has an id or a place in the
/// order.
#[derive(Debug, Clone)]
pub struct ScriptInstanceDraft {
    /// Empty takes the script's name.
    pub name: String,
    pub script: ScriptName,
    pub trigger: InstanceTrigger,
    pub inputs: Vec<InstanceInputDraft>,
    pub categories: Vec<String>,
    pub enabled: bool,
    pub blocking: bool,
    pub timeout_seconds: Option<u64>,
}

impl ScriptInstanceDraft {
    pub fn new(script: ScriptName, trigger: InstanceTrigger) -> Self {
        Self {
            name: String::new(),
            script,
            trigger,
            inputs: Vec::new(),
            categories: Vec::new(),
            enabled: true,
            blocking: true,
            timeout_seconds: None,
        }
    }

    pub fn named(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    pub fn input(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.inputs.push(InstanceInputDraft::plain(name, value));
        self
    }

    /// An input linked to the secret `secret_id`.
    pub fn secret_input(mut self, name: impl Into<String>, secret_id: impl Into<String>) -> Self {
        self.inputs
            .push(InstanceInputDraft::secret(name, secret_id));
        self
    }

    /// An input that is a secret of the instance's own, sealed from `value`.
    pub fn sealed_input(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.inputs.push(InstanceInputDraft::sealed(name, value));
        self
    }

    pub fn category(mut self, category: impl Into<String>) -> Self {
        self.categories.push(category.into());
        self
    }

    pub fn disabled(mut self) -> Self {
        self.enabled = false;
        self
    }

    pub fn fire_and_forget(mut self) -> Self {
        self.blocking = false;
        self
    }

    pub fn timeout(mut self, seconds: u64) -> Self {
        self.timeout_seconds = Some(seconds);
        self
    }

    /// `instance` as a draft that saves it back unchanged. A secret of its
    /// own is carried as one to keep, so its value is never needed.
    pub fn from_instance(instance: &ScriptInstance) -> Self {
        Self {
            name: instance.name.clone(),
            script: instance.script.clone(),
            trigger: instance.trigger,
            inputs: instance
                .inputs
                .iter()
                .map(|input| match &input.secret {
                    Some(secret) => InstanceInputDraft::secret(input.name.as_str(), &secret.id),
                    None if input.sealed => InstanceInputDraft::kept_sealed(input.name.as_str()),
                    None => InstanceInputDraft::plain(input.name.as_str(), &input.value),
                })
                .collect(),
            categories: instance.categories.clone(),
            enabled: instance.enabled,
            blocking: instance.blocking,
            timeout_seconds: instance.timeout_seconds,
        }
    }
}

/// How an update of an instance came out.
enum Updated {
    Done,
    NotFound,
    MissingSecret,
    /// The named input was to keep a secret of its own that is not there.
    NoOwnSecret(String),
}

const MISSING_SECRET: ScriptInstanceError =
    ScriptInstanceError::Invalid("a linked secret does not exist");

/// A write that lost a race to a secret being deleted hits the foreign key
/// on the input row: that is a missing secret, not a storage failure.
fn missing_secret_race<T>(result: Result<T, StateError>) -> Result<T, ScriptInstanceError> {
    result.map_err(|error| {
        if is_foreign_key_violation(&error) {
            MISSING_SECRET
        } else {
            ScriptInstanceError::Storage(error)
        }
    })
}

/// What an input row is to hold.
#[derive(Debug, Clone)]
enum StoredValue {
    Plain(String),
    /// The id of the named secret it links.
    Secret(String),
    /// A secret of the instance's own, already sealed.
    Sealed(String),
    /// The secret of its own the instance already holds under this name.
    KeepSealed,
}

/// One input row as it is written.
#[derive(Debug, Clone)]
struct InputRow {
    name: String,
    value: String,
    secret_id: Option<String>,
    sealed_value: Option<String>,
}

/// The form an input's name is compared in: names that differ only by ASCII
/// case are one input.
fn input_name_key(name: &str) -> String {
    name.to_ascii_uppercase()
}

#[derive(Debug, Clone)]
struct ValidatedInstance {
    name: String,
    script: String,
    trigger: InstanceTrigger,
    inputs: Vec<(String, StoredValue)>,
    categories: Vec<String>,
    enabled: bool,
    blocking: bool,
    timeout_seconds: Option<i64>,
}

impl ValidatedInstance {
    /// The ids of the secrets its inputs link.
    fn linked_secrets(&self) -> Vec<String> {
        self.inputs
            .iter()
            .filter_map(|(_, stored)| match stored {
                StoredValue::Secret(id) => Some(id.clone()),
                StoredValue::Plain(_) | StoredValue::Sealed(_) | StoredValue::KeepSealed => None,
            })
            .collect()
    }

    /// Whether any input keeps a secret the instance already holds.
    fn keeps_sealed(&self) -> bool {
        self.inputs
            .iter()
            .any(|(_, stored)| matches!(stored, StoredValue::KeepSealed))
    }

    /// The input rows to write. `kept` is the sealed value the instance
    /// already holds under each input name, by compared name. The error is
    /// the name of an input that was to keep one that is not there.
    fn input_rows(&self, kept: &BTreeMap<String, String>) -> Result<Vec<InputRow>, String> {
        self.inputs
            .iter()
            .map(|(name, stored)| {
                let (value, secret_id, sealed_value) = match stored {
                    StoredValue::Plain(value) => (value.clone(), None, None),
                    StoredValue::Secret(secret_id) => {
                        (String::new(), Some(secret_id.clone()), None)
                    }
                    StoredValue::Sealed(sealed) => (String::new(), None, Some(sealed.clone())),
                    StoredValue::KeepSealed => match kept.get(&input_name_key(name)) {
                        Some(sealed) => (String::new(), None, Some(sealed.clone())),
                        None => return Err(name.clone()),
                    },
                };
                Ok(InputRow {
                    name: name.clone(),
                    value,
                    secret_id,
                    sealed_value,
                })
            })
            .collect()
    }
}

impl Database {
    /// Seal the value of a secret of an instance's own. Refused without an
    /// encryption key, as storing a named secret is.
    fn seal_input(&self, value: &str) -> Result<String, ScriptInstanceError> {
        let key = self.encryption_key().ok_or(ScriptInstanceError::Invalid(
            "an encryption key is required to store a secret",
        ))?;
        encrypt_value(key, value).map_err(|error| ScriptInstanceError::Storage(storage(error)))
    }

    fn validate_script_instance(
        &self,
        draft: ScriptInstanceDraft,
    ) -> Result<ValidatedInstance, ScriptInstanceError> {
        let name = match draft.name.trim() {
            "" => draft.script.as_str().to_string(),
            name => name.to_string(),
        };
        if name.len() > MAX_INSTANCE_NAME_BYTES || name.chars().any(char::is_control) {
            return Err(ScriptInstanceError::Invalid(
                "instance name is too long or holds a control character",
            ));
        }
        let timeout_seconds = match draft.timeout_seconds {
            Some(seconds) if !(1..=MAX_TIMEOUT_SECONDS).contains(&seconds) => {
                return Err(ScriptInstanceError::Invalid(
                    "timeout must be between one second and seven days",
                ));
            }
            other => other.map(|seconds| seconds as i64),
        };
        let mut categories = Vec::<String>::new();
        for category in &draft.categories {
            let category = category.trim();
            if category.is_empty() || category.chars().any(char::is_control) {
                return Err(ScriptInstanceError::Invalid("category name is invalid"));
            }
            if !categories
                .iter()
                .any(|seen| seen.eq_ignore_ascii_case(category))
            {
                categories.push(category.to_string());
            }
        }
        if categories.len() > MAX_CATEGORIES {
            return Err(ScriptInstanceError::Invalid("too many categories"));
        }
        if !categories.is_empty() && !draft.trigger.category_scoped() {
            return Err(ScriptInstanceError::Invalid(
                "only post-processing and queue instances can be narrowed to categories",
            ));
        }
        if draft.inputs.len() > MAX_INPUTS {
            return Err(ScriptInstanceError::Invalid("too many inputs"));
        }
        let mut seen = BTreeSet::new();
        let mut inputs = Vec::with_capacity(draft.inputs.len());
        for input in draft.inputs {
            let name = OptionName::new(input.name.trim())
                .map_err(|_| ScriptInstanceError::Invalid("input name is invalid"))?;
            if !seen.insert(input_name_key(name.as_str())) {
                return Err(ScriptInstanceError::Invalid("input names must be unique"));
            }
            let stored = match (input.value, input.secret_id, input.sealed) {
                (Some(value), None, sealed) => {
                    if value.len() > MAX_INPUT_VALUE_BYTES || value.contains('\0') {
                        return Err(ScriptInstanceError::Invalid("input value is invalid"));
                    }
                    if sealed {
                        StoredValue::Sealed(self.seal_input(&value)?)
                    } else {
                        StoredValue::Plain(value)
                    }
                }
                (None, Some(secret_id), false) => StoredValue::Secret(secret_id),
                (None, None, true) => StoredValue::KeepSealed,
                (_, Some(_), true) => {
                    return Err(ScriptInstanceError::Invalid(
                        "an input is a secret of its own or a link to a named secret, not both",
                    ));
                }
                _ => {
                    return Err(ScriptInstanceError::Invalid(
                        "an input holds either a value or a secret",
                    ));
                }
            };
            inputs.push((name.as_str().to_string(), stored));
        }
        // Linked secrets, and secrets of its own that are to be kept, are
        // checked inside the write's transaction.
        Ok(ValidatedInstance {
            name,
            script: draft.script.as_str().to_string(),
            trigger: draft.trigger,
            inputs,
            categories,
            enabled: draft.enabled,
            blocking: draft.blocking,
            timeout_seconds,
        })
    }

    /// Every instance, in run order.
    pub fn script_instances(&self) -> Result<Vec<ScriptInstance>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let instances = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT id, name, script, trigger_kind, trigger_detail, enabled, blocking,
                        timeout_seconds, run_order
                   FROM script_instances ORDER BY run_order, created_at_ms, id",
                &[],
            )
            .await?;
            let inputs = SqlRuntime::fetch_all(
                datastore.read_exec(),
                // A sealed value is never read here, only whether there is one.
                "SELECT i.instance_id, i.name, i.value, i.secret_id, s.name AS secret_name,
                        (i.sealed_value IS NOT NULL) AS sealed
                   FROM script_instance_inputs i
                   LEFT JOIN secrets s ON s.id = i.secret_id
                  ORDER BY i.instance_id, i.position, i.name",
                &[],
            )
            .await?;
            let categories = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT instance_id, category FROM script_instance_categories
                  ORDER BY instance_id, category",
                &[],
            )
            .await?;
            let mut inputs_by_instance = BTreeMap::<String, Vec<InstanceInput>>::new();
            for row in inputs {
                let Ok(name) = OptionName::new(row.text("name")?) else {
                    continue;
                };
                let secret = match row.opt_text("secret_id")? {
                    Some(id) => Some(SecretRef {
                        name: row.opt_text("secret_name")?.unwrap_or_default(),
                        id,
                    }),
                    None => None,
                };
                let sealed = row.bool("sealed")?;
                inputs_by_instance
                    .entry(row.text("instance_id")?)
                    .or_default()
                    .push(InstanceInput {
                        name,
                        value: if secret.is_some() || sealed {
                            String::new()
                        } else {
                            row.text("value")?
                        },
                        secret,
                        sealed,
                    });
            }
            let mut categories_by_instance = BTreeMap::<String, Vec<String>>::new();
            for row in categories {
                categories_by_instance
                    .entry(row.text("instance_id")?)
                    .or_default()
                    .push(row.text("category")?);
            }
            let mut loaded = Vec::with_capacity(instances.len());
            for row in instances {
                let id = row.text("id")?;
                let (Some(trigger), Ok(script)) = (
                    InstanceTrigger::from_stored(
                        &row.text("trigger_kind")?,
                        &row.text("trigger_detail")?,
                    ),
                    ScriptName::new(row.text("script")?),
                ) else {
                    tracing::warn!(instance = %id, "ignoring a script instance that cannot be read");
                    continue;
                };
                loaded.push(ScriptInstance {
                    name: row.text("name")?,
                    script,
                    trigger,
                    inputs: inputs_by_instance.remove(&id).unwrap_or_default(),
                    categories: categories_by_instance.remove(&id).unwrap_or_default(),
                    enabled: row.bool("enabled")?,
                    blocking: row.bool("blocking")?,
                    timeout_seconds: row
                        .opt_i64("timeout_seconds")?
                        .and_then(|seconds| u64::try_from(seconds).ok()),
                    run_order: row.i64("run_order")?,
                    id,
                });
            }
            Ok(loaded)
        })
    }

    pub fn script_instance(&self, id: &str) -> Result<Option<ScriptInstance>, StateError> {
        Ok(self
            .script_instances()?
            .into_iter()
            .find(|instance| instance.id == id))
    }

    /// The instances `event` starts for a download in `category`, in run order.
    pub fn script_instances_for(
        &self,
        event: &ScriptEventLabel,
        category: Option<&str>,
    ) -> Result<Vec<ScriptInstance>, StateError> {
        let mode = self.post_processing_settings()?.global_scripts_run;
        Ok(resolve_instances(
            &self.script_instances()?,
            event,
            category,
            mode,
        ))
    }

    pub fn create_script_instance(
        &self,
        draft: ScriptInstanceDraft,
    ) -> Result<ScriptInstance, ScriptInstanceError> {
        let instance = self.validate_script_instance(draft)?;
        // A new instance holds no secret of its own to keep.
        let rows = instance
            .input_rows(&BTreeMap::new())
            .map_err(ScriptInstanceError::NoOwnSecret)?;
        let mut entropy = [0_u8; 12];
        getrandom::fill(&mut entropy).map_err(storage)?;
        let id = hex::encode(entropy);
        let datastore = self.datastore();
        let now = chrono::Utc::now().timestamp_millis();
        let created = id.clone();
        let result = self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "create_script_instance", |tx| {
                let instance = instance.clone();
                let rows = rows.clone();
                let id = id.clone();
                Box::pin(async move {
                    if !secrets_exist_tx(tx, &instance.linked_secrets()).await? {
                        return Ok(false);
                    }
                    insert_instance_tx(tx, &id, &instance, &rows, now).await?;
                    Ok(true)
                })
            })
            .await
        });
        self.invalidate_queue_script_admission();
        if !missing_secret_race(result)? {
            return Err(MISSING_SECRET);
        }
        self.script_instance(&created)?
            .ok_or(ScriptInstanceError::NotFound)
    }

    pub fn update_script_instance(
        &self,
        id: &str,
        draft: ScriptInstanceDraft,
    ) -> Result<ScriptInstance, ScriptInstanceError> {
        let instance = self.validate_script_instance(draft)?;
        let datastore = self.datastore();
        let now = chrono::Utc::now().timestamp_millis();
        let target = id.to_string();
        let found = self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "update_script_instance", |tx| {
                let instance = instance.clone();
                let id = target.clone();
                Box::pin(async move {
                    if !secrets_exist_tx(tx, &instance.linked_secrets()).await? {
                        return Ok(Updated::MissingSecret);
                    }
                    // Read before the rows are replaced: a secret of its own
                    // that is kept is carried across sealed, as it is stored.
                    let kept = if instance.keeps_sealed() {
                        sealed_inputs_tx(tx, &id).await?
                    } else {
                        BTreeMap::new()
                    };
                    let rows = match instance.input_rows(&kept) {
                        Ok(rows) => rows,
                        Err(name) => {
                            return Ok(if instance_exists_tx(tx, &id).await? {
                                Updated::NoOwnSecret(name)
                            } else {
                                Updated::NotFound
                            });
                        }
                    };
                    let (kind, detail) = instance.trigger.stored();
                    let updated = tx
                        .execute(
                            "UPDATE script_instances
                                SET name = {}, script = {}, trigger_kind = {}, trigger_detail = {},
                                    enabled = {}, blocking = {}, timeout_seconds = {},
                                    updated_at_ms = {}
                              WHERE id = {}",
                            &[
                                SqlArg::Text(instance.name.clone()),
                                SqlArg::Text(instance.script.clone()),
                                SqlArg::Text(kind.into()),
                                SqlArg::Text(detail.into()),
                                SqlArg::Bool(instance.enabled),
                                SqlArg::Bool(instance.blocking),
                                SqlArg::OptI64(instance.timeout_seconds),
                                SqlArg::I64(now),
                                SqlArg::Text(id.clone()),
                            ],
                        )
                        .await?;
                    if updated == 0 {
                        return Ok(Updated::NotFound);
                    }
                    for table in ["script_instance_inputs", "script_instance_categories"] {
                        tx.execute(
                            &format!("DELETE FROM {table} WHERE instance_id = {{}}"),
                            &[SqlArg::Text(id.clone())],
                        )
                        .await?;
                    }
                    if instance.trigger != InstanceTrigger::Feed {
                        tx.execute(
                            "DELETE FROM feed_scripts WHERE instance_id = {}",
                            &[SqlArg::Text(id.clone())],
                        )
                        .await?;
                    }
                    insert_instance_details_tx(tx, &id, &rows, &instance.categories).await?;
                    Ok(Updated::Done)
                })
            })
            .await
        });
        self.invalidate_queue_script_admission();
        match missing_secret_race(found)? {
            Updated::Done => {}
            Updated::NotFound => return Err(ScriptInstanceError::NotFound),
            Updated::MissingSecret => return Err(MISSING_SECRET),
            Updated::NoOwnSecret(name) => return Err(ScriptInstanceError::NoOwnSecret(name)),
        }
        self.script_instance(id)?
            .ok_or(ScriptInstanceError::NotFound)
    }

    /// Remove an instance and everything that hangs off it. Returns whether
    /// there was one.
    pub fn delete_script_instance(&self, id: &str) -> Result<bool, StateError> {
        let datastore = self.datastore();
        let target = id.to_string();
        let result = self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "delete_script_instance", |tx| {
                let id = target.clone();
                Box::pin(async move {
                    for table in [
                        "script_instance_inputs",
                        "script_instance_categories",
                        "feed_scripts",
                    ] {
                        tx.execute(
                            &format!("DELETE FROM {table} WHERE instance_id = {{}}"),
                            &[SqlArg::Text(id.clone())],
                        )
                        .await?;
                    }
                    Ok(tx
                        .execute(
                            "DELETE FROM script_instances WHERE id = {}",
                            &[SqlArg::Text(id)],
                        )
                        .await?
                        > 0)
                })
            })
            .await
        });
        self.invalidate_queue_script_admission();
        result
    }

    /// Put `ids` in this order. Instances that are not named keep their place
    /// after the ones that are.
    pub fn reorder_script_instances(&self, ids: &[String]) -> Result<(), StateError> {
        let datastore = self.datastore();
        let ids = ids.to_vec();
        let result = self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "reorder_script_instances", |tx| {
                let ids = ids.clone();
                Box::pin(async move {
                    let existing = tx
                        .fetch_all(
                            "SELECT id FROM script_instances ORDER BY run_order, created_at_ms, id",
                            &[],
                        )
                        .await?
                        .into_iter()
                        .map(|row| row.text("id"))
                        .collect::<Result<Vec<_>, _>>()?;
                    let named = ids.iter().collect::<BTreeSet<_>>();
                    let order = ids
                        .iter()
                        .filter(|id| existing.contains(id))
                        .chain(existing.iter().filter(|id| !named.contains(id)));
                    for (position, id) in order.enumerate() {
                        tx.execute(
                            "UPDATE script_instances SET run_order = {} WHERE id = {}",
                            &[SqlArg::I64(position as i64), SqlArg::Text(id.clone())],
                        )
                        .await?;
                    }
                    Ok(())
                })
            })
            .await
        });
        self.invalidate_queue_script_admission();
        result
    }

    /// Stop every instance from running without losing what was saved in it.
    pub fn disable_script_instances(&self) -> Result<(), StateError> {
        let datastore = self.datastore();
        let result = self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.read_exec(),
                "UPDATE script_instances SET enabled = {}",
                &[SqlArg::Bool(false)],
            )
            .await?;
            Ok(())
        });
        self.invalidate_queue_script_admission();
        result
    }

    /// An instance's inputs as a run receives them, secrets included: a
    /// secret of its own is handed over exactly as a linked one is. `None`
    /// when the instance is gone.
    pub(crate) fn script_instance_run_inputs(
        &self,
        id: &str,
    ) -> Result<Option<Vec<ResolvedOption>>, StateError> {
        let datastore = self.datastore();
        let target = id.to_string();
        let rows = self.run_sql_blocking_read(async move {
            if SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT id FROM script_instances WHERE id = {}",
                &[SqlArg::Text(target.clone())],
            )
            .await?
            .is_none()
            {
                return Ok(None);
            }
            SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT i.name, i.value, i.secret_id, i.sealed_value, s.value AS linked_value
                   FROM script_instance_inputs i
                   LEFT JOIN secrets s ON s.id = i.secret_id
                  WHERE i.instance_id = {} ORDER BY i.position, i.name",
                &[SqlArg::Text(target)],
            )
            .await?
            .into_iter()
            .map(|row| {
                // `Some` for a secret: the sealed value of the linked secret,
                // or else the one the row holds itself.
                let sealed = if row.opt_text("secret_id")?.is_some() {
                    Some(row.opt_text("linked_value")?)
                } else {
                    row.opt_text("sealed_value")?.map(Some)
                };
                Ok((row.text("name")?, row.text("value")?, sealed))
            })
            .collect::<Result<Vec<_>, StateError>>()
            .map(Some)
        })?;
        let Some(rows) = rows else {
            return Ok(None);
        };
        let key = self.encryption_key();
        let mut inputs = Vec::with_capacity(rows.len());
        for (name, value, sealed) in rows {
            let name = OptionName::new(name).map_err(storage)?;
            let value = match sealed {
                None => OptionValue::String(value),
                Some(sealed) => {
                    let sealed = sealed.ok_or_else(|| {
                        StateError::Database(format!(
                            "the secret linked to input {} is gone",
                            name.as_str()
                        ))
                    })?;
                    let key = key.ok_or_else(|| {
                        StateError::Database("encryption key is required to load secrets".into())
                    })?;
                    OptionValue::Secret(SecretOptionValue::for_execution(
                        decrypt_value(key, &sealed).map_err(storage)?,
                    ))
                }
            };
            inputs.push(ResolvedOption::new(name, value));
        }
        Ok(Some(inputs))
    }

    /// The instances attached to a feed, in the order they run.
    pub fn feed_script_instance_ids(&self, feed_id: u32) -> Result<Vec<String>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT instance_id FROM feed_scripts WHERE feed_id = {}
                  ORDER BY run_order, instance_id",
                &[SqlArg::I64(i64::from(feed_id))],
            )
            .await?
            .into_iter()
            .map(|row| row.text("instance_id"))
            .collect()
        })
    }

    /// Every feed's attached instances, in the order they run.
    pub fn feed_script_instance_ids_by_feed(
        &self,
    ) -> Result<BTreeMap<u32, Vec<String>>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let mut by_feed = BTreeMap::<u32, Vec<String>>::new();
            for row in SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT feed_id, instance_id FROM feed_scripts
                  ORDER BY feed_id, run_order, instance_id",
                &[],
            )
            .await?
            {
                if let Ok(feed_id) = u32::try_from(row.i64("feed_id")?) {
                    by_feed
                        .entry(feed_id)
                        .or_default()
                        .push(row.text("instance_id")?);
                }
            }
            Ok(by_feed)
        })
    }

    /// Replace the instances attached to a feed. Each must be a feed instance.
    pub fn set_feed_script_instances(
        &self,
        feed_id: u32,
        ids: &[String],
    ) -> Result<(), ScriptInstanceError> {
        let instances = self.script_instances()?;
        let mut seen = BTreeSet::new();
        for id in ids {
            let instance = instances
                .iter()
                .find(|instance| &instance.id == id)
                .ok_or(ScriptInstanceError::NotFound)?;
            if instance.trigger != InstanceTrigger::Feed {
                return Err(ScriptInstanceError::Invalid(
                    "only a feed instance can be attached to a feed",
                ));
            }
            if !seen.insert(id) {
                return Err(ScriptInstanceError::Invalid(
                    "an instance can be attached to a feed once",
                ));
            }
        }
        let datastore = self.datastore();
        let ids = ids.to_vec();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "set_feed_script_instances", |tx| {
                let ids = ids.clone();
                Box::pin(async move { set_feed_scripts_tx(tx, feed_id, &ids).await })
            })
            .await
        })?;
        Ok(())
    }

    /// Forget a deleted feed's attachments.
    pub fn delete_feed_script_instances(&self, feed_id: u32) -> Result<(), StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.read_exec(),
                "DELETE FROM feed_scripts WHERE feed_id = {}",
                &[SqlArg::I64(i64::from(feed_id))],
            )
            .await?;
            Ok(())
        })
    }
}

pub(crate) async fn set_feed_scripts_tx(
    tx: &mut SqlTx<'_>,
    feed_id: u32,
    ids: &[String],
) -> Result<(), StateError> {
    tx.execute(
        "DELETE FROM feed_scripts WHERE feed_id = {}",
        &[SqlArg::I64(i64::from(feed_id))],
    )
    .await?;
    for (position, id) in ids.iter().enumerate() {
        tx.execute(
            "INSERT INTO feed_scripts (feed_id, instance_id, run_order) VALUES ({}, {}, {})",
            &[
                SqlArg::I64(i64::from(feed_id)),
                SqlArg::Text(id.clone()),
                SqlArg::I64(position as i64),
            ],
        )
        .await?;
    }
    Ok(())
}

/// The sealed value of each secret of its own an instance holds, by compared
/// input name.
async fn sealed_inputs_tx(
    tx: &mut SqlTx<'_>,
    id: &str,
) -> Result<BTreeMap<String, String>, StateError> {
    let mut sealed = BTreeMap::new();
    for row in tx
        .fetch_all(
            "SELECT name, sealed_value FROM script_instance_inputs
              WHERE instance_id = {} AND sealed_value IS NOT NULL",
            &[SqlArg::Text(id.into())],
        )
        .await?
    {
        sealed.insert(
            input_name_key(&row.text("name")?),
            row.text("sealed_value")?,
        );
    }
    Ok(sealed)
}

async fn instance_exists_tx(tx: &mut SqlTx<'_>, id: &str) -> Result<bool, StateError> {
    Ok(tx
        .fetch_optional(
            "SELECT id FROM script_instances WHERE id = {}",
            &[SqlArg::Text(id.into())],
        )
        .await?
        .is_some())
}

async fn insert_instance_tx(
    tx: &mut SqlTx<'_>,
    id: &str,
    instance: &ValidatedInstance,
    rows: &[InputRow],
    now: i64,
) -> Result<(), StateError> {
    let next = tx
        .fetch_optional(
            "SELECT COALESCE(MAX(run_order), -1) + 1 AS next FROM script_instances",
            &[],
        )
        .await?
        .map(|row| row.i64("next"))
        .transpose()?
        .unwrap_or(0);
    let (kind, detail) = instance.trigger.stored();
    tx.execute(
        "INSERT INTO script_instances
            (id, name, script, trigger_kind, trigger_detail, enabled, blocking,
             timeout_seconds, run_order, created_at_ms, updated_at_ms)
         VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})",
        &[
            SqlArg::Text(id.into()),
            SqlArg::Text(instance.name.clone()),
            SqlArg::Text(instance.script.clone()),
            SqlArg::Text(kind.into()),
            SqlArg::Text(detail.into()),
            SqlArg::Bool(instance.enabled),
            SqlArg::Bool(instance.blocking),
            SqlArg::OptI64(instance.timeout_seconds),
            SqlArg::I64(next),
            SqlArg::I64(now),
            SqlArg::I64(now),
        ],
    )
    .await?;
    insert_instance_details_tx(tx, id, rows, &instance.categories).await
}

async fn insert_instance_details_tx(
    tx: &mut SqlTx<'_>,
    id: &str,
    rows: &[InputRow],
    categories: &[String],
) -> Result<(), StateError> {
    for (position, row) in rows.iter().enumerate() {
        tx.execute(
            "INSERT INTO script_instance_inputs
                (instance_id, name, value, secret_id, sealed_value, position)
             VALUES ({}, {}, {}, {}, {}, {})",
            &[
                SqlArg::Text(id.into()),
                SqlArg::Text(row.name.clone()),
                SqlArg::Text(row.value.clone()),
                SqlArg::OptText(row.secret_id.clone()),
                SqlArg::OptText(row.sealed_value.clone()),
                SqlArg::I64(position as i64),
            ],
        )
        .await?;
    }
    for category in categories {
        tx.execute(
            "INSERT INTO script_instance_categories (instance_id, category) VALUES ({}, {})",
            &[SqlArg::Text(id.into()), SqlArg::Text(category.clone())],
        )
        .await?;
    }
    Ok(())
}

fn storage(error: impl fmt::Display) -> StateError {
    StateError::Database(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn instance(
        id: &str,
        trigger: InstanceTrigger,
        categories: &[&str],
        order: i64,
    ) -> ScriptInstance {
        ScriptInstance {
            id: id.into(),
            name: id.into(),
            script: ScriptName::new("notify.sh").unwrap(),
            trigger,
            inputs: Vec::new(),
            categories: categories.iter().map(|name| name.to_string()).collect(),
            enabled: true,
            blocking: true,
            timeout_seconds: None,
            run_order: order,
        }
    }

    fn ids(instances: &[ScriptInstance]) -> Vec<&str> {
        instances
            .iter()
            .map(|instance| instance.id.as_str())
            .collect()
    }

    #[test]
    fn a_category_adds_its_own_instances_after_the_ones_for_every_category() {
        let pp = InstanceTrigger::PostProcessing;
        let all = [
            instance("tv-late", pp, &["TV"], 0),
            instance("everything", pp, &[], 1),
            instance("movies", pp, &["movies"], 2),
            instance("tv-early", pp, &["tv", "anime"], 3),
            instance("scan", InstanceTrigger::Scan, &[], 4),
        ];
        let event = ScriptEventLabel::PostProcessing;
        let resolve = |category, mode| resolve_instances(&all, &event, category, mode);

        assert_eq!(
            ids(&resolve(Some(" tv "), GlobalScriptsRun::Always)),
            ["everything", "tv-late", "tv-early"]
        );
        assert_eq!(
            ids(&resolve(None, GlobalScriptsRun::Always)),
            ["everything"]
        );
        assert_eq!(
            ids(&resolve(
                Some("tv"),
                GlobalScriptsRun::OnlyWithoutCategoryScripts
            )),
            ["tv-late", "tv-early"]
        );
        assert_eq!(
            ids(&resolve(
                Some("books"),
                GlobalScriptsRun::OnlyWithoutCategoryScripts
            )),
            ["everything"]
        );
    }

    #[test]
    fn an_instance_runs_only_on_its_own_trigger_and_only_while_enabled() {
        let added = InstanceTrigger::Queue(QueueEvent::NzbAdded);
        let mut off = instance("off", added, &[], 0);
        off.enabled = false;
        let all = [
            off,
            instance("added", added, &[], 1),
            instance(
                "deleted",
                InstanceTrigger::Queue(QueueEvent::NzbDeleted),
                &[],
                2,
            ),
            instance("feed", InstanceTrigger::Feed, &[], 3),
        ];
        let resolved = resolve_instances(
            &all,
            &ScriptEventLabel::Queue(QueueEvent::NzbAdded),
            Some("tv"),
            GlobalScriptsRun::Always,
        );
        assert_eq!(ids(&resolved), ["added"]);
        assert_eq!(
            ids(&resolve_instances(
                &all,
                &ScriptEventLabel::Feed(7),
                None,
                GlobalScriptsRun::OnlyWithoutCategoryScripts,
            )),
            ["feed"]
        );
    }

    #[test]
    fn a_trigger_reads_back_as_it_was_written() {
        for trigger in [
            InstanceTrigger::PostProcessing,
            InstanceTrigger::Queue(QueueEvent::FileDownloaded),
            InstanceTrigger::Scan,
            InstanceTrigger::Schedule,
            InstanceTrigger::Feed,
        ] {
            assert_eq!(
                trigger.to_string().parse::<InstanceTrigger>().unwrap(),
                trigger
            );
        }
        assert!("queue:NOPE".parse::<InstanceTrigger>().is_err());
        assert!("cron".parse::<InstanceTrigger>().is_err());
    }
}
