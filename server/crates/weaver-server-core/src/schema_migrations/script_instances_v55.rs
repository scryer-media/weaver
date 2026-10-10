// Upgrade the post-processing lists and options shipped in 0.14.7.
// Saved SQL/JSON shapes and emitted defaults are local to this hook. Header
// parsing and script/option validation use the running build; fixture tests
// pin the resulting wiring, including multi-kind headers and sealed options.
// Other triggers did not run in 0.14.7 and are never enabled by this upgrade.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use serde::Deserialize;
use serde_json::{Value, json};

use crate::StateError;
use crate::persistence::sql_runtime::{SqlArg, SqlConn};
use crate::post_processing::listing::resolve_script;
use crate::post_processing::model::{
    OptionName, OptionValue, ScriptKind, ScriptManifest, ScriptName,
};

pub(crate) const HOOK_ID: &str = "move_script_wiring_to_instances_v55";
// The schema that first has instances. A backup taken below it carries the
// earlier wiring instead.
pub(crate) const SCHEMA_VERSION: i64 = 55;
// The table whose presence in a backup says its wiring is already instances.
pub(crate) const INSTANCES_TABLE: &str = "script_instances";

const LISTS_KEY: &str = "post_processing.script_lists.v1";
const OPTIONS_KEY: &str = "post_processing.script_options.v1";
const DIRECTORY_KEY: &str = "post_processing.script_directory.v1";
const SETTINGS_KEY: &str = "post_processing.settings.v2";

const MAX_NAME_BYTES: usize = 128;
const MAX_TIMEOUT_SECONDS: u64 = 7 * 24 * 60 * 60;
const MAX_INPUTS: usize = 256;
const MAX_INPUT_VALUE_BYTES: usize = 64 * 1024;

// Run the step on a connection that is already inside the transaction it
// belongs to. The instance tables must be empty.
pub(crate) async fn move_script_wiring_to_instances(
    conn: &mut SqlConn<'_>,
) -> Result<(), StateError> {
    let saved = read(conn).await?;
    let plan = plan(&saved)?;
    write(conn, &plan).await?;
    super::schedule_tracks_v55::move_schedule_tracks(conn).await?;
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

// A saved option value: `{"type": "integer", "value": 3}`.
#[derive(Debug, Deserialize)]
struct SavedValue {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    value: Value,
}

impl SavedValue {
    // The value as the script would have been handed it.
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

// Everything the earlier build saved, as it was read.
#[derive(Debug, Default)]
struct Saved {
    // Where the scripts are, when a directory was ever settled.
    directory: Option<PathBuf>,
    lists: Option<String>,
    options: Option<String>,
    settings: Option<String>,
    // Secret names already taken, in their compared form.
    secret_names: BTreeSet<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Trigger {
    PostProcessing,
}

impl Trigger {
    fn stored(self) -> (&'static str, &'static str) {
        ("post_processing", "")
    }
}

#[derive(Debug, Clone)]
struct Input {
    name: String,
    // Plain text; empty for a secret.
    value: String,
    // The planned secret this input links.
    secret_id: Option<String>,
}

// A named secret made from a secret option that was saved for a script. The
// ciphertext is carried across as it was saved: the step never needs the key.
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
}

#[derive(Debug, Default)]
struct Plan {
    // One per secret option saved for a script, shared by every instance of
    // that script.
    secrets: Vec<PlannedSecret>,
    // In run order.
    instances: Vec<Instance>,
    settings: Option<String>,
    warnings: Vec<String>,
}

async fn read(conn: &mut SqlConn<'_>) -> Result<Saved, StateError> {
    let mut saved = Saved::default();
    for row in conn
        .fetch_all(
            "SELECT key, value FROM settings WHERE key IN ({}, {}, {}, {})",
            &[
                SqlArg::Text(LISTS_KEY.into()),
                SqlArg::Text(OPTIONS_KEY.into()),
                SqlArg::Text(DIRECTORY_KEY.into()),
                SqlArg::Text(SETTINGS_KEY.into()),
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
            _ => {}
        }
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
                 timeout_seconds, run_order,
                 created_at_ms, updated_at_ms)
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
    for (key, value) in [(SETTINGS_KEY, &plan.settings)] {
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

// The form a secret's name is compared in: names that differ only by case
// are one name.
fn name_key(name: &str) -> String {
    name.to_lowercase()
}

// The longest prefix of `text` that fits in `max` bytes, cut on a character
// boundary.
fn cut_to(text: &str, max: usize) -> &str {
    let mut end = max.min(text.len());
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

// `<script> <option>`, made unique with a counter. When it does not fit,
// the script part is cut first so the option's name survives whole.
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

// A script as an earlier build would have found it: its name, and its header
// when the file is still there to read.
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
}

// The run policy an instance is created with.
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
    // The planned secret for each script and upper-cased option name.
    secret_ids: BTreeMap<(String, String), String>,
    // Secret names in use, in their compared form.
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

    // The options saved for a script, as an instance's inputs: every option
    // its header declares, at the value the operator gave it or the header's
    // default, then anything else that was saved under its name. A saved
    // secret becomes a named secret, one per script and option, which every
    // instance of that script links.
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

    // Each input as a name, its plain value, and the saved ciphertext when
    // it is a secret.
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

    // Add an instance and return where it sits in the plan.
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
        };
        self.plan.instances.push(instance);
        Ok(self.plan.instances.len() - 1)
    }
}

// A header's default for a plain option, as text.
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

    for (category, entries) in std::iter::once((None, &lists.global)).chain(
        lists
            .categories
            .iter()
            .map(|(name, entries)| (Some(name.as_str()), entries)),
    ) {
        let category = category.map(str::trim);
        if category.is_some_and(|name| name.is_empty() || name.chars().any(char::is_control)) {
            continue;
        }
        if let Some(category) = category {
            if entries.is_empty() {
                planner.plan.warnings.push(format!("category {category} had an empty script list; scripts for every category now run for it"));
            } else if entries.iter().all(|entry| !entry.enabled) {
                planner.plan.warnings.push(format!("category {category} had every script in its list turned off; scripts for every category now run for it"));
            }
        }
        for entry in entries {
            let Some(script) = planner.known(&entry.script) else {
                continue;
            };
            let mut policy = Policy::of(entry);
            if !script.declares(ScriptKind::PostProcessing) {
                policy = policy.off();
                planner.plan.warnings.push(format!("script {} could not be read as a post-processing script; it was kept as a turned-off job", entry.script));
            }
            planner.add(&script, Trigger::PostProcessing, category, policy)?;
        }
    }

    // With a list of its own a category never ran the list for every
    // download, so that is how the new setting starts.
    if !lists.categories.is_empty() {
        let only = "only_without_category_scripts";
        match saved
            .settings
            .as_deref()
            .map(serde_json::from_str::<Value>)
        {
            Some(Ok(Value::Object(mut settings))) => {
                settings.insert("globalScriptsRun".into(), Value::String(only.into()));
                planner.plan.settings = Some(Value::Object(settings).to_string());
            }
            Some(_) => planner.plan.warnings.push(
                "saved script settings could not be read, so scripts for every category also run for a category with scripts of its own"
                    .into(),
            ),
            // Nothing was ever saved, so these are the settings as they start.
            None => {
                // Required fields as shipped in 0.14.7; do not serialize the
                // running build's defaults into a frozen upgrade.
                planner.plan.settings = Some(json!({"executionEnabled": false, "concurrency": 4, "terminationGraceSeconds": 10, "globalScriptsRun": only}).to_string());
            }
        }
    }
    Ok(planner.plan)
}

#[cfg(test)]
mod tests {
    use super::super::{embedded_catalog, embedded_payload_bytes, replay_catalog_into_fresh_db};
    use super::*;
    use crate::Database;
    use crate::post_processing::instances::{InstanceTrigger, ScriptInstance, resolve_instances};
    use crate::post_processing::model::{GlobalScriptsRun, ScriptEventLabel};
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use std::fs;
    fn bare(root: &Path, name: &str, header: &str) {
        fs::write(root.join(name), format!("#!/bin/sh\n{header}\n")).unwrap();
    }

    fn saved(root: &Path) -> Saved {
        Saved {
            directory: Some(root.to_path_buf()),
            ..Saved::default()
        }
    }

    fn shape(instance: &Instance) -> (&str, Trigger, Option<&str>, bool, bool) {
        (
            instance.script.as_str(),
            instance.trigger,
            instance.category.as_deref(),
            instance.enabled,
            instance.blocking,
        )
    }

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

    async fn upgraded(root: &Path, scripts: &Path, lists: Value) -> (Database, Vec<String>) {
        upgraded_with_options(root, scripts, lists, json!({})).await
    }

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
            Some(50),
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
        scripts
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

    #[tokio::test]
    async fn shipped_upgrade_preserves_post_processing_without_enabling_new_triggers() {
        let root = tempfile::tempdir().unwrap();
        let scripts = scripts(root.path());
        bare(
            &scripts,
            "multi.sh",
            "### NZBGET POST-PROCESSING/QUEUE/SCAN/SCHEDULER/FEED SCRIPT ###\n### TASK TIME: 03:30 ###",
        );
        let (db, _) = upgraded(
            root.path(),
            &scripts,
            json!({
                "global":[{"script":"multi.sh", "blocking":false, "timeoutSeconds":42}],
                "categories":{"tv":[{"script":"tv-post.sh"}]}
            }),
        )
        .await;
        let instances = db.script_instances().unwrap();
        assert_eq!(
            instances.iter().map(stored).collect::<Vec<_>>(),
            [
                (
                    "multi.sh",
                    InstanceTrigger::PostProcessing,
                    vec![],
                    true,
                    false,
                    Some(42)
                ),
                (
                    "tv-post.sh",
                    InstanceTrigger::PostProcessing,
                    vec!["tv"],
                    true,
                    true,
                    None
                ),
            ]
        );
        assert!(
            instances
                .iter()
                .all(|job| job.schedule.times.is_empty() && !job.schedule.run_at_startup)
        );
        assert_eq!(
            runs_for_tv(&db, ScriptEventLabel::PostProcessing),
            [("tv-post.sh".into(), vec!["tv".into()])]
        );
        db.close().unwrap();
        let reopened = Database::open(&root.path().join("weaver.db")).unwrap();
        assert_eq!(reopened.script_instances().unwrap().len(), 2);
    }

    #[test]
    fn saved_settings_are_preserved_without_serializing_current_defaults() {
        let migrated = plan(&Saved {
            lists: Some(json!({"categories":{"tv":[]}}).to_string()),
            ..Saved::default()
        })
        .unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(migrated.settings.as_deref().unwrap()).unwrap(),
            json!({"executionEnabled":false,"concurrency":4,"terminationGraceSeconds":10,"globalScriptsRun":"only_without_category_scripts"})
        );
        let empty = plan(&Saved::default()).unwrap();
        assert!(empty.instances.is_empty() && empty.settings.is_none());
    }
}
