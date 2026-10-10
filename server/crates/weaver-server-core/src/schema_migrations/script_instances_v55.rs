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
use crate::bandwidth::Weekday;
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
const SPEED_KEY: &str = "max_download_speed";

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
    /// The download speed limit set in the settings, which a pause or resume
    /// rule used to put back in force.
    configured_speed: u64,
    /// Each feed with the script names it was given.
    feeds: Vec<(i64, String)>,
    /// Secret names already taken, in their compared form.
    secret_names: BTreeSet<String>,
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
    /// Plain text; empty for a secret.
    value: String,
    /// The planned secret this input links.
    secret_id: Option<String>,
}

/// A named secret made from a secret option that was saved for a script. The
/// ciphertext is carried across as it was saved: the step never needs the key.
#[derive(Debug, Clone)]
struct PlannedSecret {
    id: String,
    name: String,
    ciphertext: String,
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
    timing: Timing,
}

/// When a schedule instance runs: the `schedule_days`, `schedule_times` and
/// `run_at_startup` columns.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Timing {
    /// `None` until a time is carried in. Empty is every day.
    days: Option<BTreeSet<Weekday>>,
    /// `HH:MM`, or `*:MM` for every hour.
    times: BTreeSet<String>,
    run_at_startup: bool,
}

impl Timing {
    /// Add the times one saved rule or header ran the script at. The days
    /// are joined, so a rule for other days than the rest runs its times on
    /// those days as well; that is reported.
    fn add(&mut self, days: &[Weekday], times: &[ScriptTaskTime]) -> bool {
        let days = if days.len() >= 7 {
            BTreeSet::new()
        } else {
            days.iter().copied().collect()
        };
        for time in times {
            match time {
                ScriptTaskTime::Startup => self.run_at_startup = true,
                time => {
                    self.times.insert(time.to_string());
                }
            }
        }
        let differs = self.days.as_ref().is_some_and(|saved| *saved != days);
        self.days = Some(match self.days.take() {
            Some(saved) if saved.is_empty() || days.is_empty() => BTreeSet::new(),
            Some(mut saved) => {
                saved.extend(days);
                if saved.len() == 7 {
                    saved.clear();
                }
                saved
            }
            None => days,
        });
        differs
    }

    fn stored_days(&self) -> String {
        self.days
            .iter()
            .flatten()
            .map(|day| day.as_str())
            .collect::<Vec<_>>()
            .join(",")
    }

    fn stored_times(&self) -> String {
        self.times.iter().cloned().collect::<Vec<_>>().join(",")
    }
}

/// What to write, worked out before anything is written.
#[derive(Debug, Default)]
struct Plan {
    /// One per secret option saved for a script, shared by every instance of
    /// that script.
    secrets: Vec<PlannedSecret>,
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
            "SELECT key, value FROM settings WHERE key IN ({}, {}, {}, {}, {}, {})",
            &[
                SqlArg::Text(LISTS_KEY.into()),
                SqlArg::Text(OPTIONS_KEY.into()),
                SqlArg::Text(DIRECTORY_KEY.into()),
                SqlArg::Text(SETTINGS_KEY.into()),
                SqlArg::Text(SCHEDULES_KEY.into()),
                SqlArg::Text(SPEED_KEY.into()),
            ],
        )
        .await?
    {
        let value = row.text("value")?;
        match row.text("key")?.as_str() {
            LISTS_KEY => saved.lists = Some(value),
            OPTIONS_KEY => saved.options = Some(value),
            // A blank value never named a directory.
            DIRECTORY_KEY if value.trim().is_empty() => {}
            DIRECTORY_KEY => saved.directory = Some(PathBuf::from(value)),
            SETTINGS_KEY => saved.settings = Some(value),
            SCHEDULES_KEY => saved.schedules = Some(value),
            SPEED_KEY => saved.configured_speed = value.trim().parse().unwrap_or(0),
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
    for row in conn.fetch_all("SELECT name_key FROM secrets", &[]).await? {
        saved.secret_names.insert(row.text("name_key")?);
    }
    Ok(saved)
}

async fn write(conn: &mut SqlConn<'_>, plan: &Plan) -> Result<(), StateError> {
    let now = chrono::Utc::now().timestamp_millis();
    for secret in &plan.secrets {
        conn.execute(
            "INSERT INTO secrets (id, name, name_key, value, created_at_ms, updated_at_ms)
             VALUES ({}, {}, {}, {}, {}, {})",
            &[
                SqlArg::Text(secret.id.clone()),
                SqlArg::Text(secret.name.clone()),
                SqlArg::Text(name_key(&secret.name)),
                SqlArg::Text(secret.ciphertext.clone()),
                SqlArg::I64(now),
                SqlArg::I64(now),
            ],
        )
        .await?;
    }
    for (run_order, instance) in plan.instances.iter().enumerate() {
        let (kind, detail) = instance.trigger.stored();
        conn.execute(
            "INSERT INTO script_instances
                (id, name, script, trigger_kind, trigger_detail, enabled, blocking,
                 timeout_seconds, schedule_days, schedule_times, run_at_startup, run_order,
                 created_at_ms, updated_at_ms)
             VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})",
            &[
                SqlArg::Text(instance.id.clone()),
                SqlArg::Text(instance.name.clone()),
                SqlArg::Text(instance.script.clone()),
                SqlArg::Text(kind.into()),
                SqlArg::Text(detail.into()),
                SqlArg::Bool(instance.enabled),
                SqlArg::Bool(instance.blocking),
                SqlArg::OptI64(instance.timeout_seconds),
                SqlArg::Text(instance.timing.stored_days()),
                SqlArg::Text(instance.timing.stored_times()),
                SqlArg::Bool(instance.timing.run_at_startup),
                SqlArg::I64(run_order as i64),
                SqlArg::I64(now),
                SqlArg::I64(now),
            ],
        )
        .await?;
        for (position, input) in instance.inputs.iter().enumerate() {
            conn.execute(
                "INSERT INTO script_instance_inputs (instance_id, name, value, secret_id, position)
                 VALUES ({}, {}, {}, {}, {})",
                &[
                    SqlArg::Text(instance.id.clone()),
                    SqlArg::Text(input.name.clone()),
                    SqlArg::Text(input.value.clone()),
                    SqlArg::OptText(input.secret_id.clone()),
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
    // The instances hold all of it now, so nothing reads these again.
    conn.execute(
        "DELETE FROM settings WHERE key IN ({}, {})",
        &[
            SqlArg::Text(LISTS_KEY.into()),
            SqlArg::Text(OPTIONS_KEY.into()),
        ],
    )
    .await?;
    Ok(())
}

fn yes_no(value: bool) -> String {
    if value { "yes" } else { "no" }.to_string()
}

/// The form a secret's name is compared in: names that differ only by case
/// are one name.
fn name_key(name: &str) -> String {
    name.to_lowercase()
}

/// The longest prefix of `text` that fits in `max` bytes, cut on a character
/// boundary.
fn cut_to(text: &str, max: usize) -> &str {
    let mut end = max.min(text.len());
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

/// `<script> <option>`, made unique with a counter. When it does not fit,
/// the script part is cut first so the option's name survives whole.
fn secret_name(script: &str, option: &str, taken: &BTreeSet<String>) -> String {
    for counter in 1_usize.. {
        let suffix = if counter == 1 {
            String::new()
        } else {
            format!(" {counter}")
        };
        let option = cut_to(option.trim(), MAX_NAME_BYTES - suffix.len()).trim_end();
        let room = MAX_NAME_BYTES.saturating_sub(option.len() + suffix.len() + 1);
        let script = cut_to(script.trim(), room).trim_end();
        let base = if script.is_empty() {
            option.to_string()
        } else {
            format!("{script} {option}")
        };
        let name = format!("{}{suffix}", base.trim_end());
        if !taken.contains(&name_key(&name)) {
            return name;
        }
    }
    unreachable!("a counter always finds a free name")
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
    /// The planned secret for each script and upper-cased option name.
    secret_ids: BTreeMap<(String, String), String>,
    /// Secret names in use, in their compared form.
    secret_names: BTreeSet<String>,
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
    /// default, then anything else that was saved under its name. A saved
    /// secret becomes a named secret, one per script and option, which every
    /// instance of that script links.
    fn inputs(&mut self, script: &Known) -> Result<Vec<Input>, StateError> {
        let mut inputs = Vec::new();
        for (name, value, ciphertext) in self.saved_inputs(script) {
            let secret_id = match ciphertext {
                Some(ciphertext) => Some(self.secret_for(&script.name, &name, ciphertext)?),
                None => None,
            };
            inputs.push(Input {
                name,
                value,
                secret_id,
            });
        }
        Ok(inputs)
    }

    fn secret_for(
        &mut self,
        script: &str,
        option: &str,
        ciphertext: String,
    ) -> Result<String, StateError> {
        let key = (script.to_string(), option.to_ascii_uppercase());
        if let Some(id) = self.secret_ids.get(&key) {
            return Ok(id.clone());
        }
        let name = secret_name(script, option, &self.secret_names);
        self.secret_names.insert(name_key(&name));
        let id = new_id()?;
        self.plan.secrets.push(PlannedSecret {
            id: id.clone(),
            name,
            ciphertext,
        });
        self.secret_ids.insert(key, id.clone());
        Ok(id)
    }

    /// Each input as a name, its plain value, and the saved ciphertext when
    /// it is a secret.
    fn saved_inputs(&self, script: &Known) -> Vec<(String, String, Option<String>)> {
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
        let mut push = |name: &str, value: String, ciphertext: Option<String>| {
            let stored = ciphertext.as_deref().unwrap_or(&value);
            if inputs.len() < MAX_INPUTS
                && OptionName::new(name).is_ok()
                && stored.len() <= MAX_INPUT_VALUE_BYTES
                && !stored.contains('\0')
                && seen.insert(name.to_ascii_uppercase())
            {
                inputs.push((name.to_string(), value, ciphertext));
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
                    push(name, String::new(), Some(ciphertext));
                }
                continue;
            }
            // An input with neither a saved value nor a default is carried
            // empty, so the instance holds everything the header declares.
            let value = plain(name)
                .or_else(|| option.default().and_then(default_text))
                .unwrap_or_default();
            push(name, value, None);
        }
        for option in saved.iter().flat_map(|saved| &saved.plain) {
            if let Some(value) = option.value.text() {
                push(&option.name, value, None);
            }
        }
        for secret in saved.iter().flat_map(|saved| &saved.secrets) {
            push(&secret.name, String::new(), Some(secret.ciphertext.clone()));
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
            inputs: self.inputs(script)?,
            category: category.map(str::to_string),
            enabled: policy.enabled,
            blocking: policy.blocking,
            timeout_seconds: policy
                .timeout_seconds
                .filter(|seconds| (1..=MAX_TIMEOUT_SECONDS).contains(seconds))
                .map(|seconds| seconds as i64),
            timing: Timing::default(),
        };
        self.plan.instances.push(instance);
        Ok(self.plan.instances.len() - 1)
    }

    fn id(&self, index: usize) -> String {
        self.plan.instances[index].id.clone()
    }

    /// Move a saved rule that ran a script onto that script's schedule
    /// instance, making the instance when the list had none.
    fn carry_scheduled_run(
        &mut self,
        row: &Value,
        label: &str,
        lists: &SavedLists,
        schedule_instances: &mut BTreeMap<String, usize>,
    ) -> Result<(), StateError> {
        let Some(name) = row
            .pointer("/action/script")
            .and_then(Value::as_str)
            .map(str::to_string)
        else {
            self.plan.warnings.push(format!(
                "schedule rule {label} ran a script it did not name, and was removed"
            ));
            return Ok(());
        };
        let enabled = rule_enabled(row);
        let index = match schedule_instances.get(&name) {
            Some(index) => *index,
            None => {
                let Some(script) = self.known(&name) else {
                    return Ok(());
                };
                let listed = lists.global.iter().find(|entry| entry.script == name);
                let ran = script.declares(ScriptKind::Scheduler);
                let policy = Policy {
                    enabled: false,
                    blocking: listed.is_none_or(|entry| entry.blocking),
                    timeout_seconds: listed.and_then(|entry| entry.timeout_seconds),
                };
                if !ran {
                    self.plan.warnings.push(format!(
                        "a schedule named script {name}, which did not run on a schedule; it was kept as a turned-off instance"
                    ));
                }
                let index = self.add(&script, Trigger::Schedule, None, policy)?;
                schedule_instances.insert(name.clone(), index);
                if !ran {
                    // Kept off, but with the times it would have run at.
                    self.plan.instances[index]
                        .timing
                        .add(&rule_days(row), &rule_times(row));
                    self.plan.warnings.push(format!(
                        "schedule rule {label} ran script {name}; its times are now on that script's schedule job, and the rule was removed"
                    ));
                    return Ok(());
                }
                index
            }
        };
        if !enabled {
            // A rule turned off ran nothing, so its times are not carried.
            self.plan.warnings.push(format!(
                "schedule rule {label} for script {name} was turned off, so its times were not carried to the script's schedule job; the rule was removed"
            ));
            return Ok(());
        }
        // A saved rule ran its script whether or not the list had it turned on.
        let instance = &mut self.plan.instances[index];
        instance.enabled = true;
        let differs = instance.timing.add(&rule_days(row), &rule_times(row));
        self.plan.warnings.push(if differs {
            format!(
                "schedule rule {label} ran script {name} on other days than its other times; its times are now on that script's schedule job on all of those days, and the rule was removed"
            )
        } else {
            format!(
                "schedule rule {label} ran script {name}; its times are now on that script's schedule job, and the rule was removed"
            )
        });
        Ok(())
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

/// What a saved schedule rule is called in a log line.
fn rule_label(row: &Value) -> String {
    [row.get("label"), row.get("id")]
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .find(|text| !text.is_empty())
        .unwrap_or("unnamed")
        .to_string()
}

fn rule_kind(row: &Value) -> &str {
    row.pointer("/action/type")
        .and_then(Value::as_str)
        .unwrap_or_default()
}

fn rule_enabled(row: &Value) -> bool {
    row.get("enabled").and_then(Value::as_bool).unwrap_or(true)
}

/// A rule's days. Empty is every day.
fn rule_days(row: &Value) -> Vec<Weekday> {
    row.get("days")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .filter_map(Weekday::parse)
        .collect()
}

/// Every time a rule that ran a script fired at: its time, which may be a
/// list, its extra times, its minute past every hour and run at startup.
fn rule_times(row: &Value) -> Vec<ScriptTaskTime> {
    let mut declared = Vec::<String>::new();
    if let Some(time) = row.get("time").and_then(Value::as_str) {
        declared.extend(time.split([',', ';']).map(str::to_string));
    }
    declared.extend(
        row.get("times")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .map(str::to_string),
    );
    if let Some(minute) = row.get("every_hour_at_minute").and_then(Value::as_u64) {
        declared.push(format!("*:{minute:02}"));
    }
    if row
        .pointer("/action/run_at_startup")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        declared.push("*".into());
    }
    declared
        .iter()
        .filter_map(|time| time.trim().parse::<ScriptTaskTime>().ok())
        .collect()
}

/// `HH:MM` as minutes past midnight.
fn minute_of_day(time: &str) -> Option<u32> {
    let (hour, minute) = time.trim().split_once(':')?;
    let (hour, minute) = (hour.parse::<u32>().ok()?, minute.parse::<u32>().ok()?);
    (hour < 24 && minute < 60).then_some(hour * 60 + minute)
}

/// Earlier builds held pause, resume and speed rules on one track, so a pause
/// or resume ended a scheduled speed limit and put the configured one back.
/// Each now holds its own track, so for every day a pause or resume followed
/// a speed rule, a speed rule setting the configured limit is added beside
/// it. Returns those days by the rule's position.
fn legacy_speed_resets(rows: &[Value]) -> BTreeMap<usize, Vec<Weekday>> {
    const SHARED: [&str; 4] = ["pause", "resume", "speed_limit", "configured_speed_limit"];
    let on = |row: &Value, day: Weekday| {
        let days = rule_days(row);
        days.is_empty() || days.contains(&day)
    };
    let time = |row: &Value| {
        row.get("time")
            .and_then(Value::as_str)
            .and_then(minute_of_day)
    };
    let mut week = Vec::new();
    for (day_index, day) in Weekday::ALL.into_iter().enumerate() {
        for (index, row) in rows.iter().enumerate() {
            // A rule turned off can be turned on later, so it gets its
            // companion too, turned off the same way.
            if !on(row, day) || !SHARED.contains(&rule_kind(row)) {
                continue;
            }
            if let Some(time) = time(row) {
                week.push((day_index, time, index));
            }
        }
    }
    // A later watch-folder rule in the same minute took the one track.
    week.retain(|&(day, minute, index)| {
        !rows.iter().skip(index + 1).any(|row| {
            rule_enabled(row)
                && on(row, Weekday::ALL[day])
                && time(row) == Some(minute)
                && matches!(
                    rule_kind(row),
                    "pause_watch_folder_scanning" | "resume_watch_folder_scanning"
                )
        })
    });
    week.sort_unstable();
    let mut resets = BTreeMap::<usize, Vec<Weekday>>::new();
    for (position, &(day, minute, index)) in week.iter().enumerate() {
        let previous = (1..=week.len())
            .map(|offset| week[(position + week.len() - offset) % week.len()].2)
            .find(|&candidate| {
                rule_enabled(&rows[candidate]) || rule_kind(&rows[candidate]) == "speed_limit"
            })
            .unwrap_or(index);
        let (next_day, next_minute, next) = week[(position + 1) % week.len()];
        if next_day == day
            && next_minute == minute
            && rule_kind(&rows[next]) == "configured_speed_limit"
        {
            continue;
        }
        if matches!(rule_kind(&rows[index]), "pause" | "resume")
            && rule_kind(&rows[previous]) == "speed_limit"
        {
            resets.entry(index).or_default().push(Weekday::ALL[day]);
        }
    }
    resets
}

/// A speed rule for the global limit alone.
fn global_speed(bytes_per_sec: u64) -> Value {
    json!({
        "type": "speed_limit",
        "limits": [{"target": {"kind": "global"}, "bytes_per_sec": bytes_per_sec}],
    })
}

fn plan(saved: &Saved) -> Result<Plan, StateError> {
    // Without the headers every script would be carried across turned off, so
    // a directory that is there but cannot be read stops the step instead.
    if let Some(directory) = &saved.directory
        && let Err(error) = std::fs::read_dir(directory)
    {
        return Err(StateError::Database(format!(
            "the scripts directory {} could not be read ({error}); make it readable and start again",
            directory.display()
        )));
    }
    let mut warnings = Vec::new();
    let lists: SavedLists = decode(saved.lists.as_deref(), "script lists", &mut warnings);
    let options = decode(saved.options.as_deref(), "script options", &mut warnings);
    let mut planner = Planner {
        root: saved.directory.as_deref(),
        options,
        secret_ids: BTreeMap::new(),
        secret_names: saved.secret_names.clone(),
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
                    // The times in the header ran while the script was turned
                    // on in the list. A start-up time never ran.
                    let header = times
                        .iter()
                        .copied()
                        .filter(|time| *time != ScriptTaskTime::Startup)
                        .collect::<Vec<_>>();
                    if entry.enabled && !header.is_empty() {
                        planner.plan.instances[index].timing.add(&[], &header);
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
        // A category's list kept the list for every download from running for
        // it even when nothing in it ran. Now only a turned-on instance for
        // the trigger at hand does that.
        if entries.is_empty() {
            planner.plan.warnings.push(format!(
                "category {category} had an empty script list, which kept scripts from running for it; scripts for every category now run for it"
            ));
        } else if entries.iter().all(|entry| !entry.enabled) {
            planner.plan.warnings.push(format!(
                "category {category} had every script in its list turned off, which kept scripts from running for it; scripts for every category now run for it"
            ));
        }
        let mut post_processes = false;
        for entry in entries {
            let Some(script) = planner.known(&entry.script) else {
                continue;
            };
            let policy = Policy::of(entry);
            post_processes |= script.declares(ScriptKind::PostProcessing);
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
            if ![
                ScriptKind::PostProcessing,
                ScriptKind::Queue,
                ScriptKind::Scan,
            ]
            .into_iter()
            .any(|kind| script.declares(kind))
            {
                planner.plan.warnings.push(format!(
                    "script {} in category {category} runs only on a schedule or for a feed, neither of which has a category; it was left out of the category",
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
        if !post_processes && entries.iter().any(|entry| entry.enabled) {
            planner.plan.warnings.push(format!(
                "category {category} had no post-processing script in its list, which kept post-processing scripts from running for it; post-processing scripts for every category now run for it"
            ));
        }
    }

    // Saved schedule rules. One that ran a script becomes that script's own
    // run times; one this build no longer offers is removed; a speed rule
    // sets the global limit.
    let schedules = match saved
        .schedules
        .as_deref()
        .map(serde_json::from_str::<Value>)
    {
        None => None,
        Some(Ok(Value::Array(saved))) => Some(saved),
        Some(_) => {
            planner.plan.warnings.push(
                "saved schedules could not be read, so scheduled scripts were not carried across"
                    .into(),
            );
            None
        }
    };
    if let Some(schedules) = schedules {
        let resets = legacy_speed_resets(&schedules);
        let mut ids = schedules
            .iter()
            .filter_map(|row| row.get("id").and_then(Value::as_str))
            .map(str::to_string)
            .collect::<BTreeSet<_>>();
        let mut kept = Vec::with_capacity(schedules.len());
        let mut changed = false;
        for (position, mut row) in schedules.into_iter().enumerate() {
            let label = rule_label(&row);
            match rule_kind(&row) {
                "run_script" => {
                    changed = true;
                    planner.carry_scheduled_run(&row, &label, &lists, &mut schedule_instances)?;
                    continue;
                }
                kind @ ("scan_watch_folder" | "fetch_rss" | "configured_speed_limit") => {
                    changed = true;
                    planner.plan.warnings.push(format!(
                        "schedule rule {label} ({kind}) is not offered any more and was removed"
                    ));
                    continue;
                }
                "speed_limit" => {
                    if let Some(bytes_per_sec) =
                        row.pointer("/action/bytes_per_sec").and_then(Value::as_u64)
                    {
                        row["action"] = global_speed(bytes_per_sec);
                        changed = true;
                    }
                }
                _ => {}
            }
            let reset = resets.get(&position).map(|days| {
                let id = row
                    .get("id")
                    .and_then(Value::as_str)
                    .unwrap_or("rule")
                    .to_string();
                let mut id = format!("{id}-speed-reset");
                while !ids.insert(id.clone()) {
                    id.push_str("-reset");
                }
                json!({
                    "id": id,
                    "enabled": rule_enabled(&row),
                    "label": "Preserve legacy speed reset",
                    "days": if days.len() == 7 {
                        Vec::new()
                    } else {
                        days.iter().map(|day| day.as_str()).collect::<Vec<_>>()
                    },
                    "time": row.get("time").cloned().unwrap_or(Value::Null),
                    "times": [],
                    "every_hour_at_minute": null,
                    "action": global_speed(saved.configured_speed),
                })
            });
            kept.push(row);
            // After its rule, so a later speed rule in the same minute still
            // wins.
            if let Some(reset) = reset {
                kept.push(reset);
                changed = true;
            }
        }
        if changed {
            planner.plan.schedules = Some(Value::Array(kept).to_string());
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
    use crate::bandwidth::{SpeedLimitChange, SpeedTarget};
    use crate::post_processing::instances::{
        InstanceSchedule, InstanceTrigger, ScriptInstance, resolve_instances,
    };
    use crate::post_processing::model::ScriptEventLabel;

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
                .map(|input| (
                    input.name.as_str(),
                    input.value.as_str(),
                    input.secret.as_ref().map(|secret| secret.name.as_str())
                ))
                .collect::<Vec<_>>(),
            [
                ("Server", "example.test", None),
                ("Token", "", Some("notify Token")),
                ("Mode", "fast", None),
                ("Extra", "7", None),
            ]
        );
        // Every instance of the script links the one secret made for it.
        assert_eq!(instances[7].inputs, instances[0].inputs);
        let secrets = db.secrets().unwrap();
        assert_eq!(
            secrets
                .iter()
                .map(|secret| (secret.name.as_str(), secret.used_by.len()))
                .collect::<Vec<_>>(),
            [("notify Token", 6)]
        );

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

        // The saved rule's time is on the job now. The header's time never
        // ran, because the list had the script turned off.
        let nightly = instances
            .iter()
            .find(|instance| instance.id == id("nightly.sh", Schedule))
            .unwrap();
        assert_eq!(
            nightly.schedule,
            InstanceSchedule {
                days: Vec::new(),
                times: vec!["02:00".into()],
                run_at_startup: false,
            }
        );
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
            [("quiet", "01:00", true, &ScheduleAction::Pause)]
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
        // The ciphertext is carried across as it was saved.
        let sealed: Vec<String> = sqlx::query_scalar("SELECT value FROM secrets")
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
        // The scan, and a list with no post-processing script.
        assert_eq!(plan.warnings.len(), 2, "{:?}", plan.warnings);
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
        // The script, and the rule that was removed.
        assert_eq!(plan.warnings.len(), 2, "{:?}", plan.warnings);
        assert_eq!(
            plan.instances[0].timing,
            Timing {
                days: Some(BTreeSet::new()),
                times: BTreeSet::from(["02:00".to_string()]),
                run_at_startup: true,
            }
        );
        assert_eq!(plan.schedules.as_deref(), Some("[]"));
    }

    fn scheduler_script(root: &Path) {
        bare(
            root,
            "tidy.sh",
            "### NZBGET SCHEDULER SCRIPT ###\n### TASK TIME: *;03:30 ###",
        );
    }

    #[test]
    fn script_rules_become_the_jobs_run_times_and_one_turned_off_runs_nothing() {
        let root = tempfile::tempdir().unwrap();
        scheduler_script(root.path());
        let plan = plan(&Saved {
            lists: Some(json!({"global": [{"script": "tidy.sh"}]}).to_string()),
            schedules: Some(
                json!([
                    {"id": "a", "label": "Weekday mornings", "days": ["mon", "fri"], "time": "07:00;*:15",
                     "action": {"type": "run_script", "script": "tidy.sh", "run_at_startup": true}},
                    {"id": "b", "label": "Weekends", "days": ["sat"], "time": "10:00",
                     "action": {"type": "run_script", "script": "tidy.sh", "run_at_startup": false}},
                    {"id": "c", "label": "Off", "enabled": false, "time": "11:00",
                     "action": {"type": "run_script", "script": "tidy.sh", "run_at_startup": false}},
                    {"id": "d", "label": "Keep", "time": "12:00", "action": {"type": "pause"}},
                ])
                .to_string(),
            ),
            ..saved(root.path())
        })
        .unwrap();
        assert_eq!(plan.instances.len(), 1);
        // The header's 03:30 ran every day, so every rule's days join it.
        assert_eq!(
            plan.instances[0].timing,
            Timing {
                days: Some(BTreeSet::new()),
                times: ["*:15", "03:30", "07:00", "10:00"].map(String::from).into(),
                run_at_startup: true,
            }
        );
        // One line per removed rule, naming it.
        for label in ["Weekday mornings", "Weekends", "Off"] {
            assert!(
                plan.warnings.iter().any(|warning| warning.contains(label)),
                "{label}: {:?}",
                plan.warnings
            );
        }
        let rows: Value = serde_json::from_str(plan.schedules.as_deref().unwrap()).unwrap();
        assert_eq!(rows.as_array().unwrap().len(), 1);
        assert_eq!(rows[0]["id"], "d");
    }

    #[test]
    fn rules_for_removed_actions_are_dropped_by_name_and_a_speed_rule_sets_the_global_limit() {
        let plan = plan(&Saved {
            schedules: Some(
                json!([
                    {"id": "a", "label": "Scan now", "time": "01:00", "action": {"type": "scan_watch_folder"}},
                    {"id": "b", "label": "Feeds", "time": "02:00", "action": {"type": "fetch_rss", "feed_id": null}},
                    {"id": "c", "label": "Back to normal", "time": "03:00", "action": {"type": "configured_speed_limit"}},
                    {"id": "d", "label": "Slow", "time": "04:00", "action": {"type": "speed_limit", "bytes_per_sec": 5000}},
                ])
                .to_string(),
            ),
            ..Saved::default()
        })
        .unwrap();
        for label in ["Scan now", "Feeds", "Back to normal"] {
            assert!(
                plan.warnings.iter().any(|warning| warning.contains(label)),
                "{label}: {:?}",
                plan.warnings
            );
        }
        let rows: Vec<crate::bandwidth::ScheduleEntry> =
            serde_json::from_str(plan.schedules.as_deref().unwrap()).unwrap();
        assert_eq!(
            rows.iter()
                .map(|row| (row.id.as_str(), row.action.clone()))
                .collect::<Vec<_>>(),
            [(
                "d",
                ScheduleAction::SpeedLimit {
                    limits: vec![SpeedLimitChange {
                        target: SpeedTarget::Global,
                        bytes_per_sec: 5000,
                    }],
                }
            )]
        );
    }

    #[test]
    fn a_pause_after_a_speed_rule_keeps_putting_the_configured_limit_back() {
        let plan = plan(&Saved {
            configured_speed: 9000,
            schedules: Some(
                json!([
                    {"id": "slow", "time": "08:00", "action": {"type": "speed_limit", "bytes_per_sec": 1000}},
                    {"id": "stop", "days": ["mon", "tue"], "time": "12:00", "action": {"type": "pause"}},
                    {"id": "go", "time": "13:00", "action": {"type": "resume"}},
                ])
                .to_string(),
            ),
            ..Saved::default()
        })
        .unwrap();
        let rows: Vec<crate::bandwidth::ScheduleEntry> =
            serde_json::from_str(plan.schedules.as_deref().unwrap()).unwrap();
        let configured = ScheduleAction::SpeedLimit {
            limits: vec![SpeedLimitChange {
                target: SpeedTarget::Global,
                bytes_per_sec: 9000,
            }],
        };
        assert_eq!(
            rows.iter()
                .map(|row| (row.id.as_str(), row.days.clone(), row.time.as_str()))
                .collect::<Vec<_>>(),
            [
                ("slow", vec![], "08:00"),
                ("stop", vec![Weekday::Mon, Weekday::Tue], "12:00"),
                (
                    "stop-speed-reset",
                    vec![Weekday::Mon, Weekday::Tue],
                    "12:00"
                ),
                ("go", vec![], "13:00"),
                // Only on the days no pause came between.
                (
                    "go-speed-reset",
                    vec![
                        Weekday::Wed,
                        Weekday::Thu,
                        Weekday::Fri,
                        Weekday::Sat,
                        Weekday::Sun
                    ],
                    "13:00"
                ),
            ]
        );
        assert_eq!(rows[2].action, configured);
        assert_eq!(rows[4].action, configured);
        assert_eq!(rows[2].label, "Preserve legacy speed reset");
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
    fn a_saved_secret_option_becomes_one_uniquely_named_secret_per_script() {
        let plan = plan(&Saved {
            lists: Some(
                json!({
                    "global": [{"script": "post.sh"}],
                    "categories": {"tv": [{"script": "post.sh"}]},
                })
                .to_string(),
            ),
            options: Some(
                json!({
                    "post.sh": {
                        "plain": [{"name": "Host", "value": {"type": "string", "value": "h"}}],
                        "secrets": [
                            {"name": "Token", "ciphertext": "sealed-token"},
                            {"name": "Password", "ciphertext": "sealed-password"},
                        ],
                    },
                })
                .to_string(),
            ),
            secret_names: BTreeSet::from(["post.sh token".to_string()]),
            ..Saved::default()
        })
        .unwrap();
        assert_eq!(
            plan.secrets
                .iter()
                .map(|secret| (secret.name.as_str(), secret.ciphertext.as_str()))
                .collect::<Vec<_>>(),
            [
                // The plain name was already taken.
                ("post.sh Token 2", "sealed-token"),
                ("post.sh Password", "sealed-password"),
            ]
        );
        assert_eq!(plan.instances.len(), 2);
        for instance in &plan.instances {
            assert_eq!(
                instance
                    .inputs
                    .iter()
                    .map(|input| (
                        input.name.as_str(),
                        input.value.as_str(),
                        input.secret_id.as_deref()
                    ))
                    .collect::<Vec<_>>(),
                [
                    ("Host", "h", None),
                    ("Token", "", Some(plan.secrets[0].id.as_str())),
                    ("Password", "", Some(plan.secrets[1].id.as_str())),
                ]
            );
        }
        // A long script name is cut, never the option's name, and the cut
        // leaves no trailing space before the option or the counter.
        let long = secret_name(&"x".repeat(200), "Token", &BTreeSet::new());
        assert!(long.len() <= MAX_NAME_BYTES, "{long}");
        assert!(long.ends_with("x Token"), "{long}");
        // The cut for the plain name lands right after the script's space.
        let spaced = format!("{} {}", "x".repeat(MAX_NAME_BYTES - 7), "y".repeat(50));
        let taken = BTreeSet::from([name_key(&secret_name(&spaced, "Token", &BTreeSet::new()))]);
        for name in [
            secret_name(&spaced, "Token", &BTreeSet::new()),
            secret_name(&spaced, "Token", &taken),
        ] {
            assert!(name.len() <= MAX_NAME_BYTES, "{name}");
            assert!(!name.contains("  "), "{name}");
            assert!(!name.ends_with(char::is_whitespace), "{name}");
            assert!(name.contains(" Token"), "{name}");
        }
        assert!(secret_name(&spaced, "Token", &taken).ends_with(" Token 2"));
        // A multi-byte script name is cut on a character boundary.
        let wide = secret_name(&"ü".repeat(100), "Password", &BTreeSet::new());
        assert!(wide.len() <= MAX_NAME_BYTES, "{wide}");
        assert!(wide.ends_with("ü Password"), "{wide}");
        assert!(wide.trim_end_matches(" Password").chars().all(|c| c == 'ü'));
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

    #[test]
    fn a_scripts_directory_that_cannot_be_read_stops_the_upgrade() {
        let root = tempfile::tempdir().unwrap();
        let lists = Some(json!({"global": [{"script": "post.sh"}]}).to_string());
        let file = root.path().join("not-a-directory");
        fs::write(&file, "").unwrap();
        for directory in [root.path().join("missing"), file] {
            let error = plan(&Saved {
                lists: lists.clone(),
                ..saved(&directory)
            })
            .unwrap_err()
            .to_string();
            assert!(
                error.contains(&directory.display().to_string())
                    && error.contains("make it readable and start again"),
                "{error}"
            );
        }

        // One that is there and empty is read, and has nothing in it.
        let empty = root.path().join("empty");
        fs::create_dir(&empty).unwrap();
        let plan = plan(&Saved {
            lists,
            ..saved(&empty)
        })
        .unwrap();
        assert_eq!(
            plan.instances.iter().map(shape).collect::<Vec<_>>(),
            [("post.sh", Trigger::PostProcessing, None, false, true)]
        );
        assert_eq!(plan.warnings.len(), 1, "{:?}", plan.warnings);
    }

    #[test]
    fn a_category_entry_for_a_schedule_or_feed_script_is_left_out_with_a_warning() {
        let root = tempfile::tempdir().unwrap();
        bare(
            root.path(),
            "nightly.sh",
            "### NZBGET SCHEDULER SCRIPT ###\n### TASK TIME: 03:30 ###",
        );
        bare(root.path(), "feed.sh", "### NZBGET FEED SCRIPT ###");
        bare(
            root.path(),
            "post.sh",
            "### NZBGET POST-PROCESSING SCRIPT ###",
        );
        let plan = plan(&Saved {
            lists: Some(
                json!({"categories": {"tv": [
                    {"script": "nightly.sh"},
                    {"script": "feed.sh"},
                    {"script": "post.sh"},
                ]}})
                .to_string(),
            ),
            ..saved(root.path())
        })
        .unwrap();
        assert_eq!(
            plan.instances.iter().map(shape).collect::<Vec<_>>(),
            [("post.sh", Trigger::PostProcessing, Some("tv"), true, true)]
        );
        let left_out = plan
            .warnings
            .iter()
            .filter(|warning| warning.contains("left out of the category"))
            .collect::<Vec<_>>();
        assert_eq!(left_out.len(), 2, "{:?}", plan.warnings);
        assert!(left_out[0].contains("nightly.sh") && left_out[1].contains("feed.sh"));
        assert_eq!(plan.warnings.len(), 2, "{:?}", plan.warnings);
    }

    /// Upgrade a database the build before instances left with `lists` and
    /// the scripts in `scripts`, and return it with the warnings the step
    /// gave.
    async fn upgraded(root: &Path, scripts: &Path, lists: Value) -> (Database, Vec<String>) {
        upgraded_with_options(root, scripts, lists, json!({})).await
    }

    /// [`upgraded`], with `options` saved for the scripts as well.
    async fn upgraded_with_options(
        root: &Path,
        scripts: &Path,
        lists: Value,
        options: Value,
    ) -> (Database, Vec<String>) {
        let path = root.join("weaver.db");
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
        let saved = Saved {
            lists: Some(lists.to_string()),
            options: Some(options.to_string()),
            ..saved(scripts)
        };
        for (key, value) in [
            (DIRECTORY_KEY, scripts.to_string_lossy().into_owned()),
            (LISTS_KEY, saved.lists.clone().unwrap()),
            (OPTIONS_KEY, saved.options.clone().unwrap()),
        ] {
            sqlx::query("INSERT INTO settings (key, value) VALUES (?1, ?2)")
                .bind(key)
                .bind(value)
                .execute(&pool)
                .await
                .unwrap();
        }
        pool.close().await;
        let warnings = plan(&saved).unwrap().warnings;
        let db = Database::open(&path).unwrap();
        // The instances hold the wiring now, so what it was saved as is gone.
        for key in [LISTS_KEY, OPTIONS_KEY] {
            assert_eq!(db.get_setting(key).unwrap(), None, "{key}");
        }
        (db, warnings)
    }

    /// What `event` starts for a download in category tv, as each instance's
    /// script and categories.
    fn runs_for_tv(db: &Database, event: ScriptEventLabel) -> Vec<(String, Vec<String>)> {
        let settings = db.post_processing_settings().unwrap();
        assert_eq!(
            settings.global_scripts_run,
            GlobalScriptsRun::OnlyWithoutCategoryScripts
        );
        resolve_instances(
            &db.script_instances().unwrap(),
            &event,
            Some("tv"),
            settings.global_scripts_run,
        )
        .into_iter()
        .map(|instance| (instance.script.as_str().to_string(), instance.categories))
        .collect()
    }

    /// A scripts directory with a post-processing script and a queue script.
    fn scripts(root: &Path) -> PathBuf {
        let scripts = root.join("scripts");
        fs::create_dir_all(&scripts).unwrap();
        let scripts = fs::canonicalize(&scripts).unwrap();
        bare(&scripts, "post.sh", "### NZBGET POST-PROCESSING SCRIPT ###");
        bare(
            &scripts,
            "tv-post.sh",
            "### NZBGET POST-PROCESSING SCRIPT ###",
        );
        package(&scripts, "notify", "QUEUE", "NZB_ADDED", json!([]));
        scripts
    }

    #[tokio::test]
    async fn scripts_whose_names_differ_only_by_case_get_distinct_secrets_through_open() {
        let root = tempfile::tempdir().unwrap();
        // Neither script is on disk, so the saved options alone say what
        // each instance holds; no case-folding filesystem is involved.
        let scripts = root.path().join("scripts");
        fs::create_dir_all(&scripts).unwrap();
        let scripts = fs::canonicalize(&scripts).unwrap();
        let (db, _) = upgraded_with_options(
            root.path(),
            &scripts,
            json!({"global": [{"script": "Notify.sh"}, {"script": "notify.sh"}]}),
            json!({
                "Notify.sh": {"secrets": [{"name": "Token", "ciphertext": "sealed-upper"}]},
                "notify.sh": {"secrets": [{"name": "Token", "ciphertext": "sealed-lower"}]},
            }),
        )
        .await;

        let secrets = db.secrets().unwrap();
        let mut names = secrets
            .iter()
            .map(|secret| secret.name.as_str())
            .collect::<Vec<_>>();
        names.sort_unstable();
        assert_eq!(names, ["Notify.sh Token", "notify.sh Token 2"]);
        let id_of = |name: &str| {
            secrets
                .iter()
                .find(|secret| secret.name == name)
                .unwrap()
                .id
                .clone()
        };
        let linked = db
            .script_instances()
            .unwrap()
            .iter()
            .map(|instance| {
                (
                    instance.script.as_str().to_string(),
                    instance
                        .inputs
                        .iter()
                        .map(|input| {
                            (
                                input.name.as_str().to_string(),
                                input.secret.as_ref().map(|secret| secret.id.clone()),
                            )
                        })
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            linked,
            [
                (
                    "Notify.sh".to_string(),
                    vec![("Token".to_string(), Some(id_of("Notify.sh Token")))]
                ),
                (
                    "notify.sh".to_string(),
                    vec![("Token".to_string(), Some(id_of("notify.sh Token 2")))]
                ),
            ]
        );
        // Each secret carries its own script's ciphertext across unchanged.
        let datastore = db.datastore();
        let rows = db
            .run_sql_blocking_read(async move {
                crate::persistence::sql_runtime::SqlRuntime::fetch_all(
                    datastore.read_exec(),
                    "SELECT name, value FROM secrets ORDER BY value",
                    &[],
                )
                .await
            })
            .unwrap()
            .into_iter()
            .map(|row| (row.text("name").unwrap(), row.text("value").unwrap()))
            .collect::<Vec<_>>();
        assert_eq!(
            rows,
            [
                ("notify.sh Token 2".to_string(), "sealed-lower".to_string()),
                ("Notify.sh Token".to_string(), "sealed-upper".to_string()),
            ]
        );
    }

    #[tokio::test]
    async fn a_category_list_of_queue_scripts_lets_post_processing_for_every_category_run() {
        let root = tempfile::tempdir().unwrap();
        let scripts = scripts(root.path());
        let (db, warnings) = upgraded(
            root.path(),
            &scripts,
            json!({
                "global": [{"script": "post.sh"}],
                "categories": {"tv": [{"script": "notify"}]},
            }),
        )
        .await;
        use InstanceTrigger::{PostProcessing, Queue};
        assert_eq!(
            db.script_instances()
                .unwrap()
                .iter()
                .map(stored)
                .collect::<Vec<_>>(),
            [
                ("post.sh", PostProcessing, vec![], true, true, None),
                (
                    "notify",
                    Queue(QueueEvent::NzbAdded),
                    vec!["tv"],
                    true,
                    true,
                    None
                ),
            ]
        );
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(warnings[0].contains("no post-processing script"));
        assert_eq!(
            runs_for_tv(&db, ScriptEventLabel::PostProcessing),
            [("post.sh".to_string(), vec![])]
        );
        assert_eq!(
            runs_for_tv(&db, ScriptEventLabel::Queue(QueueEvent::NzbAdded)),
            [("notify".to_string(), vec!["tv".to_string()])]
        );
        db.close().unwrap();
    }

    #[tokio::test]
    async fn a_category_list_with_every_script_turned_off_lets_scripts_for_every_category_run() {
        let root = tempfile::tempdir().unwrap();
        let scripts = scripts(root.path());
        let (db, warnings) = upgraded(
            root.path(),
            &scripts,
            json!({
                "global": [{"script": "post.sh"}],
                "categories": {"tv": [{"script": "tv-post.sh", "enabled": false}]},
            }),
        )
        .await;
        use InstanceTrigger::PostProcessing;
        assert_eq!(
            db.script_instances()
                .unwrap()
                .iter()
                .map(stored)
                .collect::<Vec<_>>(),
            [
                ("post.sh", PostProcessing, vec![], true, true, None),
                ("tv-post.sh", PostProcessing, vec!["tv"], false, true, None),
            ]
        );
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(warnings[0].contains("every script in its list turned off"));
        assert_eq!(
            runs_for_tv(&db, ScriptEventLabel::PostProcessing),
            [("post.sh".to_string(), vec![])]
        );
        db.close().unwrap();
    }

    #[tokio::test]
    async fn an_empty_category_list_lets_scripts_for_every_category_run() {
        let root = tempfile::tempdir().unwrap();
        let scripts = scripts(root.path());
        let (db, warnings) = upgraded(
            root.path(),
            &scripts,
            json!({
                "global": [{"script": "post.sh"}],
                "categories": {"tv": []},
            }),
        )
        .await;
        assert_eq!(
            db.script_instances()
                .unwrap()
                .iter()
                .map(stored)
                .collect::<Vec<_>>(),
            [(
                "post.sh",
                InstanceTrigger::PostProcessing,
                vec![],
                true,
                true,
                None
            )]
        );
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(warnings[0].contains("empty script list"));
        assert_eq!(
            runs_for_tv(&db, ScriptEventLabel::PostProcessing),
            [("post.sh".to_string(), vec![])]
        );
        db.close().unwrap();
    }
}
