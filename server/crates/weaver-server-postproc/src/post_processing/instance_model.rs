// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::model::{
    GlobalScriptsRun, OptionName, PostProcessingSettings, QueueEvent, ScriptEventLabel, ScriptKind,
    ScriptName, ScriptTaskTime,
};
use super::secrets::SecretRef;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use weaver_server_db::persistence::StateError;
use weaver_server_net::bandwidth::Weekday;

// The one thing that starts an instance.
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

    // Whether `event` is one this trigger starts on.
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

    // Only a download has a category, so only the triggers a download raises
    // can be narrowed to one.
    pub fn category_scoped(self) -> bool {
        matches!(self, Self::PostProcessing | Self::Queue(_))
    }

    pub fn stored(self) -> (&'static str, &'static str) {
        match self {
            Self::PostProcessing => ("post_processing", ""),
            Self::Queue(event) => ("queue", event.as_str()),
            Self::Scan => ("scan", ""),
            Self::Schedule => ("schedule", ""),
            Self::Feed => ("feed", ""),
        }
    }

    pub fn from_stored(kind: &str, detail: &str) -> Option<Self> {
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

// One saved input: a plain value, a link to a named secret, or a secret of
// the instance's own. A secret's value is never read back out for display.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InstanceInput {
    pub name: OptionName,
    // Empty for a secret of either kind.
    pub value: String,
    pub secret: Option<SecretRef>,
    // The input is a secret of the instance's own: its value is kept sealed
    // in the instance's own row, and no named secret stands behind it.
    #[serde(default)]
    pub sealed: bool,
}

impl InstanceInput {
    // Whether the input is a secret, linked or the instance's own.
    pub fn is_secret(&self) -> bool {
        self.secret.is_some() || self.sealed
    }
}

// When a schedule job runs. Every other trigger leaves it empty.
#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InstanceSchedule {
    // Empty runs on every day.
    pub days: Vec<Weekday>,
    // Each one `HH:MM`, or `*:MM` for that minute of every hour, in order.
    pub times: Vec<String>,
    // Also run once when Weaver starts.
    pub run_at_startup: bool,
}

const MAX_SCHEDULE_TIMES: usize = 96;

impl InstanceSchedule {
    pub fn is_empty(&self) -> bool {
        self.days.is_empty() && self.times.is_empty() && !self.run_at_startup
    }

    // The run times a script header declares: `*` is run at startup, and
    // every other declaration is a time.
    pub fn from_task_times(declared: &[ScriptTaskTime]) -> Self {
        let mut schedule = Self::default();
        for time in declared {
            match time {
                ScriptTaskTime::Startup => schedule.run_at_startup = true,
                time => schedule.times.push(time.to_string()),
            }
        }
        schedule.times.sort();
        schedule.times.dedup();
        schedule
    }

    // Its times, as parsed declarations.
    pub fn task_times(&self) -> impl Iterator<Item = ScriptTaskTime> + '_ {
        self.times
            .iter()
            .filter_map(|time| time.parse::<ScriptTaskTime>().ok())
            .filter(|time| *time != ScriptTaskTime::Startup)
    }

    pub fn validated(self) -> Result<Self, ScriptInstanceError> {
        let mut days = self.days;
        days.sort();
        days.dedup();
        if days.len() == Weekday::ALL.len() {
            days.clear();
        }
        if self.times.len() > MAX_SCHEDULE_TIMES {
            return Err(ScriptInstanceError::Invalid("too many run times"));
        }
        let mut times = Vec::with_capacity(self.times.len());
        for time in &self.times {
            match time.trim().parse::<ScriptTaskTime>() {
                Ok(ScriptTaskTime::Startup) | Err(_) => {
                    return Err(ScriptInstanceError::Invalid(
                        "a run time is HH:MM, or *:MM for every hour",
                    ));
                }
                Ok(time) => times.push(time.to_string()),
            }
        }
        times.sort();
        times.dedup();
        Ok(Self {
            days,
            times,
            run_at_startup: self.run_at_startup,
        })
    }

    pub fn stored_days(&self) -> String {
        self.days
            .iter()
            .map(|day| day.as_str())
            .collect::<Vec<_>>()
            .join(",")
    }

    pub fn from_stored(days: &str, times: &str, run_at_startup: bool) -> Self {
        Self {
            days: days.split(',').filter_map(Weekday::parse).collect(),
            times: times
                .split(',')
                .map(str::trim)
                .filter(|time| !time.is_empty())
                .map(str::to_string)
                .collect(),
            run_at_startup,
        }
    }
}

// A script wired to one trigger.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScriptInstance {
    pub id: String,
    pub name: String,
    pub script: ScriptName,
    pub trigger: InstanceTrigger,
    pub inputs: Vec<InstanceInput>,
    // Empty runs for every category.
    pub categories: Vec<String>,
    pub enabled: bool,
    // A blocking instance is run in line: the next one waits for it, and so
    // does whatever its trigger holds. One that is not is started and left to
    // finish on its own, and its result changes nothing.
    pub blocking: bool,
    // `None` runs under the default timeout of its trigger.
    pub timeout_seconds: Option<u64>,
    pub run_order: i64,
    // When a schedule job runs.
    #[serde(default)]
    pub schedule: InstanceSchedule,
}

impl ScriptInstance {
    // How long a run of this instance may last: its own limit, or else the
    // default of its trigger. A test run is held to the same.
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

// The instances `event` starts for a download in `category`, in run order.
//
// Instances for every category run first, then the ones narrowed to this
// category. When the setting says so, a category with instances of its own
// leaves the unscoped ones out. Category names are matched without regard to
// case, because download clients echo their own casing back.
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
    // An input was sent as a secret of the instance's own with no value, and
    // the instance holds none under that name to keep.
    #[error("input \"{0}\" is marked secret but was given no value, and none is saved for it")]
    NoOwnSecret(String),
    #[error("script job does not exist")]
    NotFound,
    #[error(transparent)]
    Storage(#[from] StateError),
}

pub async fn set_feed_scripts_tx(
    tx: &mut weaver_server_db::persistence::sql_runtime::SqlTx<'_>,
    feed_id: u32,
    ids: &[String],
) -> Result<(), StateError> {
    let mut seen = std::collections::BTreeSet::new();
    for id in ids {
        if !seen.insert(id) {
            return Err(StateError::Conflict(
                "a script job can be attached to a feed once".into(),
            ));
        }
        let row = tx
            .fetch_optional(
                "SELECT trigger_kind FROM script_instances WHERE id = {}",
                &[weaver_server_db::persistence::sql_runtime::SqlArg::Text(
                    id.clone(),
                )],
            )
            .await?
            .ok_or_else(|| StateError::Conflict("script job not found".into()))?;
        if row.text("trigger_kind")? != "feed" {
            return Err(StateError::Conflict(
                "only a feed script job can be attached to a feed".into(),
            ));
        }
    }
    tx.execute(
        "DELETE FROM feed_scripts WHERE feed_id = {}",
        &[weaver_server_db::persistence::sql_runtime::SqlArg::I64(
            i64::from(feed_id),
        )],
    )
    .await?;
    for (position, id) in ids.iter().enumerate() {
        tx.execute(
            "INSERT INTO feed_scripts (feed_id, instance_id, run_order) VALUES ({}, {}, {})",
            &[
                weaver_server_db::persistence::sql_runtime::SqlArg::I64(i64::from(feed_id)),
                weaver_server_db::persistence::sql_runtime::SqlArg::Text(id.clone()),
                weaver_server_db::persistence::sql_runtime::SqlArg::I64(position as i64),
            ],
        )
        .await?;
    }
    Ok(())
}
