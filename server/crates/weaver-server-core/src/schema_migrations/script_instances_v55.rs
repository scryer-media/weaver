//! Migration 55, upgrade step: carry the script wiring earlier builds saved
//! into instances.
//!
//! Before instances there was an ordered list of script names for every
//! download and one per category, each script's options were kept beside its
//! name, and the script's own header decided which triggers it ran on. This
//! step reads all of that and writes the instances that reproduce it. It is
//! best effort: anything that cannot be carried across becomes a turned-off
//! instance and one warning, so nothing the operator set up is lost without a
//! trace.
//!
//! What it reads and what it writes are spelled out here as they stood at
//! schema 55, so later changes to these tables or to the application's own
//! types do not change what this step does. The one thing it borrows from the
//! running build is the reader for a script's header.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use serde::Deserialize;
use serde_json::{Value, json};

use crate::StateError;
use crate::persistence::sql_runtime::{SqlArg, SqlConn};
use crate::post_processing::listing::resolve_script;
use crate::post_processing::model::{
    GlobalScriptsRun, OptionName, OptionValue, PostProcessingSettings, QueueEvent, ScriptKind,
    ScriptManifest, ScriptName, ScriptTaskTime,
};

pub(crate) const HOOK_ID: &str = "move_script_wiring_to_instances_v55";
/// The schema that first has instances. A backup taken below it carries the
/// earlier wiring instead.
pub(crate) const SCHEMA_VERSION: i64 = 55;
/// The table whose presence in a backup says its wiring is already instances.
pub(crate) const INSTANCES_TABLE: &str = "script_instances";

const LISTS_KEY: &str = "post_processing.script_lists.v1";
const OPTIONS_KEY: &str = "post_processing.script_options.v1";
const DIRECTORY_KEY: &str = "post_processing.script_directory.v1";
const SETTINGS_KEY: &str = "post_processing.settings.v2";
const SCHEDULES_KEY: &str = "schedules";

const MAX_NAME_BYTES: usize = 128;
const MAX_TIMEOUT_SECONDS: u64 = 7 * 24 * 60 * 60;
const MAX_INPUTS: usize = 256;
const MAX_INPUT_VALUE_BYTES: usize = 64 * 1024;

/// Run the step on a connection that is already inside the transaction it
/// belongs to. The instance tables must be empty.
pub(crate) async fn move_script_wiring_to_instances(
    conn: &mut SqlConn<'_>,
) -> Result<(), StateError> {
    let saved = read(conn).await?;
    let plan = plan(&saved)?;
    write(conn, &plan).await?;
    for warning in &plan.warnings {
        tracing::warn!("{warning}");
    }
    Ok(())
}

fn yes() -> bool {
    true
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SavedEntry {
    script: String,
    #[serde(default = "yes")]
    enabled: bool,
    #[serde(default)]
    timeout_seconds: Option<u64>,
    #[serde(default = "yes")]
    blocking: bool,
}

#[derive(Debug, Default, Deserialize)]
struct SavedLists {
    #[serde(default)]
    global: Vec<SavedEntry>,
    #[serde(default)]
    categories: BTreeMap<String, Vec<SavedEntry>>,
}

/// A saved option value: `{"type": "integer", "value": 3}`.
#[derive(Debug, Deserialize)]
struct SavedValue {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    value: Value,
}

impl SavedValue {
    /// The value as the script would have been handed it.
    fn text(&self) -> Option<String> {
        match (self.kind.as_str(), &self.value) {
            ("string", Value::String(text)) => Some(text.clone()),
            ("integer" | "number", Value::Number(number)) => Some(number.to_string()),
            ("boolean", Value::Bool(value)) => Some(yes_no(*value)),
            _ => None,
        }
    }
}

#[derive(Debug, Deserialize)]
struct SavedPlain {
    name: String,
    value: SavedValue,
}

#[derive(Debug, Deserialize)]
struct SavedSecret {
    name: String,
    ciphertext: String,
}

#[derive(Debug, Default, Deserialize)]
struct SavedOptions {
    #[serde(default)]
    plain: Vec<SavedPlain>,
    #[serde(default)]
    secrets: Vec<SavedSecret>,
}

/// Everything the earlier build saved, as it was read.
#[derive(Debug, Default)]
struct Saved {
    /// Where the scripts are, when a directory was ever settled.
    directory: Option<PathBuf>,
    lists: Option<String>,
    options: Option<String>,
    schedules: Option<String>,
    settings: Option<String>,
    /// Each feed with the script names it was given.
    feeds: Vec<(i64, String)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Trigger {
    PostProcessing,
    Queue(QueueEvent),
    Scan,
    Schedule,
    Feed,
}

impl Trigger {
    /// The `trigger_kind` and `trigger_detail` columns.
    fn stored(self) -> (&'static str, &'static str) {
        match self {
            Self::PostProcessing => ("post_processing", ""),
            Self::Queue(event) => ("queue", event.as_str()),
            Self::Scan => ("scan", ""),
            Self::Schedule => ("schedule", ""),
            Self::Feed => ("feed", ""),
        }
    }
}

#[derive(Debug, Clone)]
struct Input {
    name: String,
    /// Plain text, or the ciphertext that was saved for a secret.
    value: String,
    secret: bool,
}

#[derive(Debug, Clone)]
struct Instance {
    id: String,
    name: String,
    script: String,
    trigger: Trigger,
    inputs: Vec<Input>,
    category: Option<String>,
    enabled: bool,
    blocking: bool,
    timeout_seconds: Option<i64>,
}

/// What to write, worked out before anything is written.
#[derive(Debug, Default)]
struct Plan {
    /// In run order.
    instances: Vec<Instance>,
    schedules: Option<String>,
    /// Each feed with the instances attached to it, in run order.
    feeds: Vec<(i64, Vec<String>)>,
    settings: Option<String>,
    warnings: Vec<String>,
}

async fn read(conn: &mut SqlConn<'_>) -> Result<Saved, StateError> {
    let mut saved = Saved::default();
    for row in conn
        .fetch_all(
            "SELECT key, value FROM settings WHERE key IN ({}, {}, {}, {}, {})",
            &[
                SqlArg::Text(LISTS_KEY.into()),
                SqlArg::Text(OPTIONS_KEY.into()),
                SqlArg::Text(DIRECTORY_KEY.into()),
                SqlArg::Text(SETTINGS_KEY.into()),
                SqlArg::Text(SCHEDULES_KEY.into()),
            ],
        )
        .await?
    {
        let value = row.text("value")?;
        match row.text("key")?.as_str() {
            LISTS_KEY => saved.lists = Some(value),
            OPTIONS_KEY => saved.options = Some(value),
            DIRECTORY_KEY => saved.directory = Some(PathBuf::from(value)),
            SETTINGS_KEY => saved.settings = Some(value),
            SCHEDULES_KEY => saved.schedules = Some(value),
            _ => {}
        }
    }
    for row in conn
        .fetch_all("SELECT id, scripts FROM rss_feeds ORDER BY id", &[])
        .await?
    {
        saved.feeds.push((
            i64::from(row.i32("id")?),
            row.opt_text("scripts")?.unwrap_or_default(),
        ));
    }
    Ok(saved)
}

async fn write(conn: &mut SqlConn<'_>, plan: &Plan) -> Result<(), StateError> {
    let now = chrono::Utc::now().timestamp_millis();
    for (run_order, instance) in plan.instances.iter().enumerate() {
        let (kind, detail) = instance.trigger.stored();
        conn.execute(
            "INSERT INTO script_instances
                (id, name, script, trigger_kind, trigger_detail, enabled, blocking,
                 timeout_seconds, run_order, created_at_ms, updated_at_ms)
             VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})",
            &[
                SqlArg::Text(instance.id.clone()),
                SqlArg::Text(instance.name.clone()),
                SqlArg::Text(instance.script.clone()),
                SqlArg::Text(kind.into()),
                SqlArg::Text(detail.into()),
                SqlArg::Bool(instance.enabled),
                SqlArg::Bool(instance.blocking),
                SqlArg::OptI64(instance.timeout_seconds),
                SqlArg::I64(run_order as i64),
                SqlArg::I64(now),
                SqlArg::I64(now),
            ],
        )
        .await?;
        for (position, input) in instance.inputs.iter().enumerate() {
            conn.execute(
                "INSERT INTO script_instance_inputs (instance_id, name, value, secret, position)
                 VALUES ({}, {}, {}, {}, {})",
                &[
                    SqlArg::Text(instance.id.clone()),
                    SqlArg::Text(input.name.clone()),
                    SqlArg::Text(input.value.clone()),
                    SqlArg::Bool(input.secret),
                    SqlArg::I64(position as i64),
                ],
            )
            .await?;
        }
        if let Some(category) = &instance.category {
            conn.execute(
                "INSERT INTO script_instance_categories (instance_id, category) VALUES ({}, {})",
                &[
                    SqlArg::Text(instance.id.clone()),
                    SqlArg::Text(category.clone()),
                ],
            )
            .await?;
        }
    }
    for (feed_id, ids) in &plan.feeds {
        for (run_order, id) in ids.iter().enumerate() {
            conn.execute(
                "INSERT INTO feed_scripts (feed_id, instance_id, run_order) VALUES ({}, {}, {})",
                &[
                    SqlArg::I64(*feed_id),
                    SqlArg::Text(id.clone()),
                    SqlArg::I64(run_order as i64),
                ],
            )
            .await?;
        }
    }
    for (key, value) in [
        (SCHEDULES_KEY, &plan.schedules),
        (SETTINGS_KEY, &plan.settings),
    ] {
        if let Some(value) = value {
            conn.execute(
                "INSERT INTO settings (key, value) VALUES ({}, {})
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                &[SqlArg::Text(key.into()), SqlArg::Text(value.clone())],
            )
            .await?;
        }
    }
    Ok(())
}

fn yes_no(value: bool) -> String {
    if value { "yes" } else { "no" }.to_string()
}

fn new_id() -> Result<String, StateError> {
    let mut entropy = [0_u8; 12];
    getrandom::fill(&mut entropy).map_err(|error| StateError::Database(error.to_string()))?;
    Ok(hex::encode(entropy))
}

/// A script as an earlier build would have found it: its name, and its header
/// when the file is still there to read.
struct Known {
    name: String,
    manifest: Option<ScriptManifest>,
}

impl Known {
    fn declares(&self, kind: ScriptKind) -> bool {
        self.manifest
            .as_ref()
            .is_some_and(|manifest| manifest.kinds().contains(&kind))
    }

    fn queue_events(&self) -> Vec<QueueEvent> {
        self.manifest
            .iter()
            .flat_map(|manifest| manifest.queue_events().iter().copied())
            .collect()
    }
}

/// The run policy an instance is created with.
#[derive(Clone, Copy)]
struct Policy {
    enabled: bool,
    blocking: bool,
    timeout_seconds: Option<u64>,
}

impl Policy {
    fn of(entry: &SavedEntry) -> Self {
        Self {
            enabled: entry.enabled,
            blocking: entry.blocking,
            timeout_seconds: entry.timeout_seconds,
        }
    }

    fn waited_for(self) -> Self {
        Self {
            blocking: true,
            ..self
        }
    }

    fn off(self) -> Self {
        Self {
            enabled: false,
            ..self
        }
    }
}

struct Planner<'a> {
    root: Option<&'a Path>,
    options: BTreeMap<String, SavedOptions>,
    plan: Plan,
}

impl Planner<'_> {
    fn known(&mut self, script: &str) -> Option<Known> {
        let Ok(name) = ScriptName::new(script) else {
            self.plan.warnings.push(format!(
                "saved script wiring names {script:?}, which is not a script name, and was left behind"
            ));
            return None;
        };
        let manifest = self
            .root
            .and_then(|root| resolve_script(root, &name).ok())
            .map(|script| script.manifest);
        Some(Known {
            name: name.as_str().to_string(),
            manifest,
        })
    }

    /// The options saved for a script, as an instance's inputs: every option
    /// its header declares, at the value the operator gave it or the header's
    /// default, then anything else that was saved under its name.
    fn inputs(&self, script: &Known) -> Vec<Input> {
        let saved = self.options.get(&script.name);
        let plain = |name: &str| {
            saved?
                .plain
                .iter()
                .find(|option| option.name.eq_ignore_ascii_case(name))
                .and_then(|option| option.value.text())
        };
        let secret = |name: &str| {
            saved?
                .secrets
                .iter()
                .find(|secret| secret.name.eq_ignore_ascii_case(name))
                .map(|secret| secret.ciphertext.clone())
        };
        let mut seen = BTreeSet::new();
        let mut inputs = Vec::new();
        let mut push = |name: &str, value: String, secret: bool| {
            if inputs.len() < MAX_INPUTS
                && OptionName::new(name).is_ok()
                && value.len() <= MAX_INPUT_VALUE_BYTES
                && !value.contains('\0')
                && seen.insert(name.to_ascii_uppercase())
            {
                inputs.push(Input {
                    name: name.to_string(),
                    value,
                    secret,
                });
            }
        };
        for option in script
            .manifest
            .iter()
            .flat_map(|manifest| manifest.options())
        {
            let name = option.name().as_str();
            if option.is_secret() {
                if let Some(ciphertext) = secret(name) {
                    push(name, ciphertext, true);
                }
                continue;
            }
            // An input with neither a saved value nor a default is carried
            // empty, so the instance holds everything the header declares.
            let value = plain(name)
                .or_else(|| option.default().and_then(default_text))
                .unwrap_or_default();
            push(name, value, false);
        }
        for option in saved.iter().flat_map(|saved| &saved.plain) {
            if let Some(value) = option.value.text() {
                push(&option.name, value, false);
            }
        }
        for secret in saved.iter().flat_map(|saved| &saved.secrets) {
            push(&secret.name, secret.ciphertext.clone(), true);
        }
        inputs
    }

    /// Add an instance and return where it sits in the plan.
    fn add(
        &mut self,
        script: &Known,
        trigger: Trigger,
        category: Option<&str>,
        policy: Policy,
    ) -> Result<usize, StateError> {
        let mut name = script.name.clone();
        while name.len() > MAX_NAME_BYTES {
            name.pop();
        }
        let instance = Instance {
            id: new_id()?,
            name,
            script: script.name.clone(),
            trigger,
            inputs: self.inputs(script),
            category: category.map(str::to_string),
            enabled: policy.enabled,
            blocking: policy.blocking,
            timeout_seconds: policy
                .timeout_seconds
                .filter(|seconds| (1..=MAX_TIMEOUT_SECONDS).contains(seconds))
                .map(|seconds| seconds as i64),
        };
        self.plan.instances.push(instance);
        Ok(self.plan.instances.len() - 1)
    }

    fn id(&self, index: usize) -> String {
        self.plan.instances[index].id.clone()
    }
}

/// A header's default for a plain option, as text.
fn default_text(value: &OptionValue) -> Option<String> {
    match value {
        OptionValue::String(value) => Some(value.clone()),
        OptionValue::Integer(value) => Some(value.to_string()),
        OptionValue::Number(value) => Some(value.to_string()),
        OptionValue::Boolean(value) => Some(yes_no(*value)),
        OptionValue::Secret(_) => None,
    }
}

fn decode<T: for<'de> Deserialize<'de> + Default>(
    raw: Option<&str>,
    what: &str,
    warnings: &mut Vec<String>,
) -> T {
    let Some(raw) = raw else {
        return T::default();
    };
    serde_json::from_str(raw).unwrap_or_else(|error| {
        warnings.push(format!(
            "saved {what} could not be read and were left behind: {error}"
        ));
        T::default()
    })
}

/// A schedule row as it is saved, running `instance_id` at `time`.
fn schedule_row(
    id: String,
    enabled: bool,
    label: String,
    time: String,
    instance_id: String,
) -> Value {
    json!({
        "id": id,
        "enabled": enabled,
        "label": label,
        "days": [],
        "time": time,
        "times": [],
        "every_hour_at_minute": null,
        "action": {
            "type": "run_script",
            "instance_id": instance_id,
            "run_at_startup": false,
        },
    })
}

fn plan(saved: &Saved) -> Result<Plan, StateError> {
    let mut warnings = Vec::new();
    let lists: SavedLists = decode(saved.lists.as_deref(), "script lists", &mut warnings);
    let options = decode(saved.options.as_deref(), "script options", &mut warnings);
    let mut planner = Planner {
        root: saved.directory.as_deref(),
        options,
        plan: Plan {
            warnings,
            ..Plan::default()
        },
    };

    // The list for every download. A script ran on each trigger its header
    // declared, so that is one instance apiece.
    let mut schedule_instances = BTreeMap::<String, usize>::new();
    let mut scan_scripts = BTreeSet::<String>::new();
    let mut feed_instances = Vec::<(String, usize)>::new();
    let mut rows = Vec::<Value>::new();
    for entry in &lists.global {
        let Some(script) = planner.known(&entry.script) else {
            continue;
        };
        let policy = Policy::of(entry);
        let before = planner.plan.instances.len();
        let (kinds, times) = match &script.manifest {
            Some(manifest) => (
                manifest.kinds().iter().copied().collect::<Vec<_>>(),
                manifest.task_times().to_vec(),
            ),
            None => Default::default(),
        };
        for kind in kinds {
            match kind {
                ScriptKind::PostProcessing => {
                    planner.add(&script, Trigger::PostProcessing, None, policy)?;
                }
                ScriptKind::Queue => {
                    for event in script.queue_events() {
                        planner.add(&script, Trigger::Queue(event), None, policy)?;
                    }
                }
                // What a scan or feed script returns was always waited for.
                ScriptKind::Scan => {
                    planner.add(&script, Trigger::Scan, None, policy.waited_for())?;
                    scan_scripts.insert(entry.script.clone());
                }
                ScriptKind::Feed => {
                    let index = planner.add(&script, Trigger::Feed, None, policy.waited_for())?;
                    feed_instances.push((entry.script.clone(), index));
                }
                ScriptKind::Scheduler => {
                    let index = planner.add(&script, Trigger::Schedule, None, policy)?;
                    schedule_instances.insert(entry.script.clone(), index);
                    // The times in the header were a schedule nobody saved.
                    // A start-up time never ran, so it has no row.
                    for time in times
                        .iter()
                        .filter(|time| **time != ScriptTaskTime::Startup)
                    {
                        rows.push(schedule_row(
                            new_id()?,
                            entry.enabled,
                            format!("{} ({time})", entry.script),
                            time.to_string(),
                            planner.id(index),
                        ));
                    }
                }
            }
        }
        if planner.plan.instances.len() == before {
            planner.add(&script, Trigger::PostProcessing, None, policy.off())?;
            planner.plan.warnings.push(format!(
                "script {} could not be read to see what it ran on; it was kept as a turned-off instance",
                entry.script
            ));
        }
    }

    // A category's list ran instead of the list for every download, and only
    // for what a download raises.
    for (category, entries) in &lists.categories {
        let category = category.trim();
        if category.is_empty() || category.chars().any(char::is_control) {
            continue;
        }
        if entries.is_empty() {
            planner.plan.warnings.push(format!(
                "category {category} had an empty script list, which kept scripts from running for it; scripts for every category now run for it"
            ));
        }
        for entry in entries {
            let Some(script) = planner.known(&entry.script) else {
                continue;
            };
            let policy = Policy::of(entry);
            if script.manifest.is_none() {
                planner.add(
                    &script,
                    Trigger::PostProcessing,
                    Some(category),
                    policy.off(),
                )?;
                planner.plan.warnings.push(format!(
                    "script {} in category {category} could not be read to see what it ran on; it was kept as a turned-off instance",
                    entry.script
                ));
                continue;
            }
            if script.declares(ScriptKind::PostProcessing) {
                planner.add(&script, Trigger::PostProcessing, Some(category), policy)?;
            }
            if script.declares(ScriptKind::Queue) {
                for event in script.queue_events() {
                    planner.add(&script, Trigger::Queue(event), Some(category), policy)?;
                }
            }
            // A scan has no category any more, so this one cannot be carried
            // across as it ran.
            if script.declares(ScriptKind::Scan) && scan_scripts.insert(entry.script.clone()) {
                planner.add(&script, Trigger::Scan, None, policy.waited_for().off())?;
                planner.plan.warnings.push(format!(
                    "scan script {} ran only for category {category}; it was kept as a turned-off instance for every scan",
                    entry.script
                ));
            }
        }
    }

    // Saved schedule rows named a script; they name an instance now.
    let mut schedules = match saved
        .schedules
        .as_deref()
        .map(serde_json::from_str::<Value>)
    {
        None => Some(Vec::new()),
        Some(Ok(Value::Array(saved))) => Some(saved),
        Some(_) => {
            planner.plan.warnings.push(
                "saved schedules could not be read, so scheduled scripts were not carried across"
                    .into(),
            );
            None
        }
    };
    if let Some(schedules) = &mut schedules {
        let mut changed = false;
        for row in schedules.iter_mut() {
            let Some(action) = row.get_mut("action").and_then(Value::as_object_mut) else {
                continue;
            };
            if action.get("type").and_then(Value::as_str) != Some("run_script") {
                continue;
            }
            let Some(name) = action
                .get("script")
                .and_then(Value::as_str)
                .map(str::to_string)
            else {
                continue;
            };
            let index = match schedule_instances.get(&name) {
                // A saved row ran its script whether or not the list had it
                // turned on.
                Some(index) => {
                    planner.plan.instances[*index].enabled = true;
                    *index
                }
                None => {
                    let Some(script) = planner.known(&name) else {
                        continue;
                    };
                    let listed = lists.global.iter().find(|entry| entry.script == name);
                    let ran = script.declares(ScriptKind::Scheduler);
                    let policy = Policy {
                        enabled: ran,
                        blocking: listed.is_none_or(|entry| entry.blocking),
                        timeout_seconds: listed.and_then(|entry| entry.timeout_seconds),
                    };
                    if !ran {
                        planner.plan.warnings.push(format!(
                            "a schedule named script {name}, which did not run on a schedule; it was kept as a turned-off instance"
                        ));
                    }
                    let index = planner.add(&script, Trigger::Schedule, None, policy)?;
                    schedule_instances.insert(name, index);
                    index
                }
            };
            action.remove("script");
            action.insert("instance_id".into(), Value::String(planner.id(index)));
            changed = true;
        }
        if !rows.is_empty() {
            schedules.append(&mut rows);
            changed = true;
        }
        if changed {
            planner.plan.schedules = Some(Value::Array(std::mem::take(schedules)).to_string());
        }
    }

    // A feed given scripts of its own ran those; one given none ran every
    // feed script in the list for every download.
    let mut own_feed_instances = BTreeMap::<String, usize>::new();
    for (feed_id, raw) in &saved.feeds {
        let names = serde_json::from_str::<Vec<String>>(raw).unwrap_or_default();
        let mut attached = Vec::<String>::new();
        if names.is_empty() {
            attached.extend(feed_instances.iter().map(|(_, index)| planner.id(*index)));
        }
        for name in names {
            let listed = feed_instances
                .iter()
                .find(|(script, index)| *script == name && planner.plan.instances[*index].enabled)
                .map(|(_, index)| *index);
            let index = match listed.or_else(|| own_feed_instances.get(&name).copied()) {
                Some(index) => index,
                None => {
                    let Some(script) = planner.known(&name) else {
                        continue;
                    };
                    let policy = Policy {
                        enabled: true,
                        blocking: true,
                        timeout_seconds: None,
                    };
                    let index = planner.add(&script, Trigger::Feed, None, policy)?;
                    own_feed_instances.insert(name, index);
                    index
                }
            };
            let id = planner.id(index);
            if !attached.contains(&id) {
                attached.push(id);
            }
        }
        if !attached.is_empty() {
            planner.plan.feeds.push((*feed_id, attached));
        }
    }

    // With a list of its own a category never ran the list for every
    // download, so that is how the new setting starts.
    if !lists.categories.is_empty() {
        let only = GlobalScriptsRun::OnlyWithoutCategoryScripts;
        match saved
            .settings
            .as_deref()
            .map(serde_json::from_str::<Value>)
        {
            Some(Ok(Value::Object(mut settings))) => {
                settings.insert("globalScriptsRun".into(), Value::String(only.as_str().into()));
                planner.plan.settings = Some(Value::Object(settings).to_string());
            }
            Some(_) => planner.plan.warnings.push(
                "saved script settings could not be read, so scripts for every category also run for a category with scripts of its own"
                    .into(),
            ),
            // Nothing was ever saved, so these are the settings as they start.
            None => {
                planner.plan.settings = Some(
                    serde_json::to_string(&PostProcessingSettings {
                        global_scripts_run: only,
                        ..PostProcessingSettings::default()
                    })
                    .map_err(|error| StateError::Database(error.to_string()))?,
                );
            }
        }
    }
    Ok(planner.plan)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use sqlx::Connection;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

    use super::super::{embedded_catalog, embedded_payload_bytes, replay_catalog_into_fresh_db};
    use super::*;
    use crate::Database;
    use crate::bandwidth::ScheduleAction;
    use crate::post_processing::instances::{InstanceTrigger, ScriptInstance};

    /// A script whose header is a line of its own source.
    fn bare(root: &Path, name: &str, header: &str) {
        fs::write(root.join(name), format!("#!/bin/sh\n{header}\n")).unwrap();
    }

    /// A script whose header is a manifest beside it.
    fn package(root: &Path, name: &str, kind: &str, queue_events: &str, options: Value) {
        let package = root.join(name);
        fs::create_dir_all(&package).unwrap();
        fs::write(package.join("main.py"), "#!/usr/bin/env python3\n").unwrap();
        fs::write(
            package.join("manifest.json"),
            json!({
                "main": "main.py",
                "name": name,
                "kind": kind,
                "displayName": name,
                "version": "1.0.0",
                "author": "Author",
                "homepage": "https://example.invalid",
                "license": "GNU",
                "about": "About",
                "description": [],
                "requirements": [],
                "queueEvents": queue_events,
                "taskTime": "",
                "sections": [],
                "commands": [],
                "options": options,
            })
            .to_string(),
        )
        .unwrap();
    }

    fn option(name: &str, value: &str, secret: bool) -> Value {
        json!({
            "name": name,
            "displayName": name,
            "value": value,
            "description": [],
            "select": [],
            "secret": secret,
        })
    }

    fn saved(root: &Path) -> Saved {
        Saved {
            directory: Some(root.to_path_buf()),
            ..Saved::default()
        }
    }

    /// What a planned instance is, without its id.
    fn shape(instance: &Instance) -> (&str, Trigger, Option<&str>, bool, bool) {
        (
            instance.script.as_str(),
            instance.trigger,
            instance.category.as_deref(),
            instance.enabled,
            instance.blocking,
        )
    }

    /// What a stored instance is, without its id.
    fn stored(
        instance: &ScriptInstance,
    ) -> (&str, InstanceTrigger, Vec<&str>, bool, bool, Option<u64>) {
        (
            instance.script.as_str(),
            instance.trigger,
            instance.categories.iter().map(String::as_str).collect(),
            instance.enabled,
            instance.blocking,
            instance.timeout_seconds,
        )
    }

    #[tokio::test]
    async fn an_upgrade_moves_saved_wiring_into_instances_once() {
        let root = tempfile::tempdir().unwrap();
        let scripts = root.path().join("scripts");
        fs::create_dir_all(&scripts).unwrap();
        let scripts = fs::canonicalize(&scripts).unwrap();
        package(
            &scripts,
            "notify",
            "POST-PROCESSING/QUEUE",
            "NZB_ADDED, NZB_DOWNLOADED",
            json!([
                option("Server", "localhost", false),
                option("Token", "", true),
                option("Mode", "fast", false),
            ]),
        );
        bare(&scripts, "scan.sh", "### NZBGET SCAN SCRIPT ###");
        bare(
            &scripts,
            "nightly.sh",
            "### NZBGET SCHEDULER SCRIPT ###\n### TASK TIME: 03:30 ###",
        );
        bare(&scripts, "feed.sh", "### NZBGET FEED SCRIPT ###");

        // A database as the build before instances left it.
        let path = root.path().join("weaver.db");
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(
                SqliteConnectOptions::new()
                    .filename(&path)
                    .create_if_missing(true),
            )
            .await
            .unwrap();
        replay_catalog_into_fresh_db(
            &pool,
            &embedded_catalog().unwrap(),
            &embedded_payload_bytes().unwrap(),
            Some(SCHEMA_VERSION - 1),
            true,
        )
        .await
        .unwrap();
        for (key, value) in [
            (DIRECTORY_KEY, scripts.to_string_lossy().into_owned()),
            (
                LISTS_KEY,
                json!({
                    "global": [
                        {"script": "notify", "enabled": true, "timeoutSeconds": 90, "blocking": false},
                        {"script": "scan.sh"},
                        {"script": "nightly.sh", "enabled": false},
                        {"script": "feed.sh"},
                        {"script": "gone.sh"},
                    ],
                    "categories": {"tv": [{"script": "notify"}]},
                })
                .to_string(),
            ),
            (
                OPTIONS_KEY,
                json!({
                    "notify": {
                        "plain": [
                            {"name": "Server", "value": {"type": "string", "value": "example.test"}},
                            {"name": "Extra", "value": {"type": "integer", "value": 7}},
                        ],
                        "secrets": [{"name": "Token", "ciphertext": "sealed"}],
                    },
                })
                .to_string(),
            ),
            (
                SCHEDULES_KEY,
                json!([
                    {
                        "id": "night",
                        "enabled": true,
                        "label": "night",
                        "days": [],
                        "time": "02:00",
                        "action": {"type": "run_script", "script": "nightly.sh", "run_at_startup": false},
                    },
                    {
                        "id": "quiet",
                        "enabled": true,
                        "label": "quiet",
                        "days": [],
                        "time": "01:00",
                        "action": {"type": "pause"},
                    },
                ])
                .to_string(),
            ),
            (
                SETTINGS_KEY,
                json!({"executionEnabled": true, "concurrency": 2, "terminationGraceSeconds": 10})
                    .to_string(),
            ),
        ] {
            sqlx::query("INSERT INTO settings (key, value) VALUES (?1, ?2)")
                .bind(key)
                .bind(value)
                .execute(&pool)
                .await
                .unwrap();
        }
        for (id, scripts) in [(1, "[]"), (2, r#"["feed.sh"]"#), (3, r#"["other.sh"]"#)] {
            sqlx::query("INSERT INTO rss_feeds (id, name, url, scripts) VALUES (?1, ?2, ?3, ?4)")
                .bind(id)
                .bind(format!("feed {id}"))
                .bind(format!("https://example.invalid/{id}"))
                .bind(scripts)
                .execute(&pool)
                .await
                .unwrap();
        }
        pool.close().await;

        // Opening it is the upgrade.
        let db = Database::open(&path).unwrap();
        let instances = db.script_instances().unwrap();
        use InstanceTrigger::{Feed, PostProcessing, Queue, Scan, Schedule};
        assert_eq!(
            instances.iter().map(stored).collect::<Vec<_>>(),
            [
                ("notify", PostProcessing, vec![], true, false, Some(90)),
                (
                    "notify",
                    Queue(QueueEvent::NzbAdded),
                    vec![],
                    true,
                    false,
                    Some(90)
                ),
                (
                    "notify",
                    Queue(QueueEvent::NzbDownloaded),
                    vec![],
                    true,
                    false,
                    Some(90)
                ),
                ("scan.sh", Scan, vec![], true, true, None),
                // Turned off in the list, but a saved schedule ran it anyway.
                ("nightly.sh", Schedule, vec![], true, true, None),
                ("feed.sh", Feed, vec![], true, true, None),
                // Not there to read, so nothing says what it ran on.
                ("gone.sh", PostProcessing, vec![], false, true, None),
                ("notify", PostProcessing, vec!["tv"], true, true, None),
                (
                    "notify",
                    Queue(QueueEvent::NzbAdded),
                    vec!["tv"],
                    true,
                    true,
                    None
                ),
                (
                    "notify",
                    Queue(QueueEvent::NzbDownloaded),
                    vec!["tv"],
                    true,
                    true,
                    None
                ),
                // Named by a feed and by no list.
                ("other.sh", Feed, vec![], true, true, None),
            ]
        );
        assert_eq!(
            instances
                .iter()
                .map(|instance| instance.run_order)
                .collect::<Vec<_>>(),
            (0..instances.len() as i64).collect::<Vec<_>>()
        );
        assert_eq!(
            instances[0]
                .inputs
                .iter()
                .map(|input| (input.name.as_str(), input.value.as_str(), input.secret))
                .collect::<Vec<_>>(),
            [
                ("Server", "example.test", false),
                ("Token", "", true),
                ("Mode", "fast", false),
                ("Extra", "7", false),
            ]
        );
        assert_eq!(instances[7].inputs, instances[0].inputs);

        let id = |script: &str, trigger: InstanceTrigger| {
            instances
                .iter()
                .find(|instance| instance.script.as_str() == script && instance.trigger == trigger)
                .unwrap()
                .id
                .clone()
        };
        assert_eq!(
            db.feed_script_instance_ids_by_feed().unwrap(),
            BTreeMap::from([
                // A feed given no scripts ran every feed script in the list.
                (1, vec![id("feed.sh", Feed)]),
                (2, vec![id("feed.sh", Feed)]),
                (3, vec![id("other.sh", Feed)]),
            ])
        );

        let nightly = id("nightly.sh", Schedule);
        let schedules = db.list_schedules().unwrap();
        assert_eq!(
            schedules
                .iter()
                .map(|row| (
                    row.label.as_str(),
                    row.time.as_str(),
                    row.enabled,
                    &row.action
                ))
                .collect::<Vec<_>>(),
            [
                (
                    "night",
                    "02:00",
                    true,
                    &ScheduleAction::RunScript {
                        instance_id: nightly.clone(),
                        run_at_startup: false,
                    }
                ),
                ("quiet", "01:00", true, &ScheduleAction::Pause),
                // The time in the header, which nobody had saved.
                (
                    "nightly.sh (03:30)",
                    "03:30",
                    false,
                    &ScheduleAction::RunScript {
                        instance_id: nightly,
                        run_at_startup: false,
                    }
                ),
            ]
        );

        let settings = db.post_processing_settings().unwrap();
        assert_eq!(
            settings.global_scripts_run,
            GlobalScriptsRun::OnlyWithoutCategoryScripts
        );
        assert!(settings.execution_enabled);
        assert_eq!(settings.concurrency, 2);
        db.close().unwrap();

        // The step belongs to migration 55, so a later start does not run it.
        let db = Database::open(&path).unwrap();
        assert_eq!(db.script_instances().unwrap(), instances);
        db.close().unwrap();

        let mut conn =
            sqlx::SqliteConnection::connect_with(&SqliteConnectOptions::new().filename(&path))
                .await
                .unwrap();
        let sealed: Vec<String> = sqlx::query_scalar(
            "SELECT DISTINCT value FROM script_instance_inputs WHERE name = 'Token' AND secret",
        )
        .fetch_all(&mut conn)
        .await
        .unwrap();
        assert_eq!(sealed, ["sealed"]);
        let applied: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM _sqlx_migrations WHERE version = ?1 AND success",
        )
        .bind(SCHEMA_VERSION)
        .fetch_one(&mut conn)
        .await
        .unwrap();
        assert_eq!(applied, 1);
    }

    #[test]
    fn a_new_install_has_nothing_to_move() {
        let db = Database::open_in_memory().unwrap();
        assert!(db.script_instances().unwrap().is_empty());
        let plan = plan(&Saved::default()).unwrap();
        assert!(plan.instances.is_empty() && plan.feeds.is_empty() && plan.warnings.is_empty());
        assert!(plan.schedules.is_none() && plan.settings.is_none());
    }

    #[test]
    fn a_scan_script_that_ran_for_one_category_is_kept_turned_off() {
        let root = tempfile::tempdir().unwrap();
        bare(root.path(), "scan.sh", "### NZBGET SCAN SCRIPT ###");
        let plan = plan(&Saved {
            lists: Some(json!({"categories": {"tv": [{"script": "scan.sh"}]}}).to_string()),
            ..saved(root.path())
        })
        .unwrap();
        assert_eq!(
            plan.instances.iter().map(shape).collect::<Vec<_>>(),
            [("scan.sh", Trigger::Scan, None, false, true)]
        );
        assert_eq!(plan.warnings.len(), 1);
        // Nothing was saved, so the settings start from what a category list meant.
        let settings: PostProcessingSettings =
            serde_json::from_str(plan.settings.as_deref().unwrap()).unwrap();
        assert_eq!(
            settings,
            PostProcessingSettings {
                global_scripts_run: GlobalScriptsRun::OnlyWithoutCategoryScripts,
                ..PostProcessingSettings::default()
            }
        );
    }

    #[test]
    fn a_schedule_for_a_script_that_never_ran_on_one_is_kept_turned_off() {
        let root = tempfile::tempdir().unwrap();
        bare(
            root.path(),
            "post.sh",
            "### NZBGET POST-PROCESSING SCRIPT ###",
        );
        let plan = plan(&Saved {
            schedules: Some(
                json!([{
                    "id": "a",
                    "time": "02:00",
                    "action": {"type": "run_script", "script": "post.sh", "run_at_startup": true},
                }])
                .to_string(),
            ),
            ..saved(root.path())
        })
        .unwrap();
        assert_eq!(
            plan.instances.iter().map(shape).collect::<Vec<_>>(),
            [("post.sh", Trigger::Schedule, None, false, true)]
        );
        assert_eq!(plan.warnings.len(), 1);
        let rows: Value = serde_json::from_str(plan.schedules.as_deref().unwrap()).unwrap();
        assert_eq!(
            rows[0]["action"],
            json!({"type": "run_script", "instance_id": plan.instances[0].id, "run_at_startup": true})
        );
    }

    #[test]
    fn what_cannot_be_read_is_left_behind_with_a_warning() {
        let root = tempfile::tempdir().unwrap();
        bare(
            root.path(),
            "post.sh",
            "### NZBGET POST-PROCESSING SCRIPT ###",
        );
        let plan = plan(&Saved {
            lists: Some(
                json!({
                    "global": [{"script": "post.sh"}, {"script": "../escape.sh"}],
                    "categories": {"tv": []},
                })
                .to_string(),
            ),
            options: Some("not json".into()),
            schedules: Some("{}".into()),
            settings: Some("[]".into()),
            ..saved(root.path())
        })
        .unwrap();
        assert_eq!(
            plan.instances.iter().map(shape).collect::<Vec<_>>(),
            [("post.sh", Trigger::PostProcessing, None, true, true)]
        );
        assert!(plan.schedules.is_none() && plan.settings.is_none());
        // The options, the script name, the empty category list, the schedules
        // and the settings.
        assert_eq!(plan.warnings.len(), 5, "{:?}", plan.warnings);
    }

    #[test]
    fn a_script_with_no_directory_to_read_it_from_is_kept_turned_off() {
        let plan = plan(&Saved {
            lists: Some(json!({"global": [{"script": "post.sh"}]}).to_string()),
            ..Saved::default()
        })
        .unwrap();
        assert_eq!(
            plan.instances.iter().map(shape).collect::<Vec<_>>(),
            [("post.sh", Trigger::PostProcessing, None, false, true)]
        );
    }
}
