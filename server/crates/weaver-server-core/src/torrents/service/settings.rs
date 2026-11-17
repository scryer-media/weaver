// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::live::Live;
use super::{TorrentService, certificate_of, queued, ssl_identity, state_error};
use crate::categories::CategoryConfig;
use crate::jobs::JobId;
use crate::proxies::TorrentScope;
use crate::settings::model::TrackerRule;
use crate::settings::{
    Config, Leaf, Level, Levels, SeedTimeCounting, Seeding, SeedingAction, SeedingGoalMode,
    SeedingPolicy, TorrentConfig, TorrentEncryption, TorrentPeerProtocol,
};
use crate::torrents::TorrentRecord;
use crate::torrents::status::Held;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, PoisonError};
use tokio::sync::watch;

/// The settings torrents are kept under.
#[derive(Debug, Clone)]
pub struct TorrentSettings {
    pub complete_dir: PathBuf,
    pub categories: Vec<CategoryConfig>,
    pub seeding: Option<SeedingPolicy>,
    pub upload_cap: Option<u64>,
    pub torrent: TorrentConfig,
}

/// Where each level keeps one setting a torrent takes over its levels; a
/// level that does not carry it is `None`, and inherits. This is the one
/// place that says which level may set which setting:
///
/// | setting                  | torrent | rule | category | global          |
/// |--------------------------|---------|------|----------|-----------------|
/// | seeding goals, mode, act | yes     | yes  | yes      | yes             |
/// | upload cap               | no (1)  | yes  | yes      | yes             |
/// | unpack                   | yes     | yes  | yes      | yes             |
/// | DHT, PEX, LSD            | no      | yes  | no       | the session's   |
/// | encryption               | no      | yes  | no       | the session's   |
/// | peer protocol            | no      | yes  | no       | the session's   |
/// | route scope              | no      | yes  | yes      | the default     |
///
/// (1) A torrent's own upload limit is not a level of the cap: the tighter
/// of the two holds ([`ratelimits_of`]).
///
/// A tracker rule alone names a client, an SSL identity, whether its
/// torrents start forced, and whether the schedule may pause them; those
/// are read from the rule and have no chain.
pub struct Field<T> {
    torrent: Option<fn(&TorrentRecord) -> Leaf<T>>,
    rule: Option<fn(&TrackerRule) -> Leaf<T>>,
    category: Option<fn(&CategoryConfig) -> Leaf<T>>,
    global: fn(&TorrentSettings) -> Leaf<T>,
}

// A seeding leaf, where every level keeps a policy.
macro_rules! seeding_field {
    ($leaf:ident) => {
        Field {
            torrent: Some(|record| record.policy_override.unwrap_or_default().$leaf),
            rule: Some(|rule| rule.seeding.unwrap_or_default().$leaf),
            category: Some(|category| category.seeding.unwrap_or_default().$leaf),
            global: |settings| settings.seeding.unwrap_or_default().at_global().$leaf,
        }
    };
}

pub const RATIO_GOAL: Field<f32> = seeding_field!(ratio_goal);
pub const SEED_TIME_GOAL: Field<u32> = seeding_field!(seed_time_goal_minutes);
pub const INACTIVE_GOAL: Field<u32> = seeding_field!(inactive_goal_minutes);
pub const SEEDING_MODE: Field<SeedingGoalMode> = seeding_field!(mode);
pub const SEEDING_ACTION: Field<SeedingAction> = seeding_field!(action);
pub const SEED_TIME_COUNTING: Field<SeedTimeCounting> = seeding_field!(seed_time_counting);

pub const UPLOAD_CAP: Field<u64> = Field {
    torrent: None,
    rule: Some(|rule| rule.upload_cap),
    category: Some(|category| category.upload_cap),
    global: |settings| Leaf::limit(settings.upload_cap),
};

pub const UNPACK: Field<bool> = Field {
    torrent: Some(|record| Leaf::of(record.unpack)),
    rule: Some(|rule| Leaf::of(rule.unpack)),
    category: Some(|category| Leaf::of(category.unpack)),
    global: |settings| Leaf::Set(settings.torrent.unpack),
};

// The session runs the DHT, local discovery and peer exchange for every
// torrent it is not turned off for, under its own encryption.
pub const DHT: Field<bool> = Field {
    torrent: None,
    rule: Some(|rule| Leaf::of(rule.dht)),
    category: None,
    global: |_| Leaf::Inherit,
};
pub const PEX: Field<bool> = Field {
    torrent: None,
    rule: Some(|rule| Leaf::of(rule.pex)),
    category: None,
    global: |_| Leaf::Inherit,
};
pub const LSD: Field<bool> = Field {
    torrent: None,
    rule: Some(|rule| Leaf::of(rule.lsd)),
    category: None,
    global: |_| Leaf::Inherit,
};
pub const ENCRYPTION: Field<TorrentEncryption> = Field {
    torrent: None,
    rule: Some(|rule| Leaf::of(rule.encryption)),
    category: None,
    global: |_| Leaf::Inherit,
};
// The transport order a rule's torrents dial peers in over the session's,
// within the transports the session runs.
pub const PEER_PROTOCOL: Field<TorrentPeerProtocol> = Field {
    torrent: None,
    rule: Some(|rule| Leaf::of(rule.peer_protocol)),
    category: None,
    global: |_| Leaf::Inherit,
};

// A rule or category routes its torrents itself once it says so.
pub const SCOPE: Field<TorrentScope> = Field {
    torrent: None,
    rule: Some(|rule| {
        if rule.torrent_route {
            Leaf::Set(TorrentScope::TrackerRule(rule.id))
        } else {
            Leaf::Inherit
        }
    }),
    category: Some(|category| {
        if category.torrent_route {
            Leaf::Set(TorrentScope::Category(category.id))
        } else {
            Leaf::Inherit
        }
    }),
    global: |_| Leaf::Set(TorrentScope::Default),
};

impl TorrentSettings {
    pub fn of(config: &Config) -> Self {
        Self {
            complete_dir: PathBuf::from(config.complete_dir()),
            categories: config.categories.clone(),
            seeding: config.seeding,
            upload_cap: config.upload_cap,
            torrent: config.torrent.clone(),
        }
    }

    pub(super) fn category(&self, id: Option<u32>) -> Option<&CategoryConfig> {
        id.and_then(|id| self.categories.iter().find(|category| category.id == id))
    }

    /// `field` at each level for a torrent: `torrent`, when there is one
    /// yet, in category `category` under tracker rule `rule`.
    pub fn levels<T>(
        &self,
        field: &Field<T>,
        torrent: Option<&TorrentRecord>,
        category: Option<u32>,
        rule: Option<&TrackerRule>,
    ) -> Levels<T> {
        fn at<L, T>(of: Option<fn(&L) -> Leaf<T>>, level: Option<&L>) -> Leaf<T> {
            of.zip(level).map_or(Leaf::Inherit, |(of, level)| of(level))
        }
        Levels {
            torrent: at(field.torrent, torrent),
            rule: at(field.rule, rule),
            category: at(field.category, self.category(category)),
            global: (field.global)(self),
        }
    }

    /// `field` at each level for `record` under `rule`.
    pub fn of_torrent<T>(
        &self,
        field: &Field<T>,
        record: &TorrentRecord,
        rule: Option<&TrackerRule>,
    ) -> Levels<T> {
        self.levels(field, Some(record), record.category_id, rule)
    }

    // The scope whose route a torrent takes: its tracker rule's when the
    // rule routes its torrents itself, else its category's when that does,
    // else the default.
    pub(super) fn scope(&self, id: Option<u32>, rule: Option<&TrackerRule>) -> TorrentScope {
        self.levels(&SCOPE, None, id, rule)
            .resolve()
            .unwrap_or(TorrentScope::Default)
    }

    /// The seeding terms `record` is held to under `rule`, each resolved
    /// over its levels, and the level each comes from.
    pub fn seeding(
        &self,
        record: &TorrentRecord,
        rule: Option<&TrackerRule>,
    ) -> (Seeding, SeedingLevels) {
        let ratio = self.of_torrent(&RATIO_GOAL, record, rule);
        let seed_time = self.of_torrent(&SEED_TIME_GOAL, record, rule);
        let inactive = self.of_torrent(&INACTIVE_GOAL, record, rule);
        let mode = self.of_torrent(&SEEDING_MODE, record, rule);
        let action = self.of_torrent(&SEEDING_ACTION, record, rule);
        let counting = self.of_torrent(&SEED_TIME_COUNTING, record, rule);
        let cap = self.of_torrent(&UPLOAD_CAP, record, rule);
        (
            Seeding {
                ratio_goal: ratio.resolve(),
                seed_time_goal_minutes: seed_time.resolve(),
                inactive_goal_minutes: inactive.resolve(),
                mode: mode.resolve().unwrap_or_default(),
                action: action.resolve().unwrap_or_default(),
                seed_time_counting: counting.resolve().unwrap_or_default(),
            },
            SeedingLevels {
                ratio_goal: ratio.level(),
                seed_time_goal_minutes: seed_time.level(),
                inactive_goal_minutes: inactive.level(),
                mode: mode.level(),
                action: action.level(),
                seed_time_counting: counting.level(),
                upload_cap: cap.level(),
            },
        )
    }

    /// The seeding terms `record` is held to under `rule`.
    pub fn policy(&self, record: &TorrentRecord, rule: Option<&TrackerRule>) -> Seeding {
        self.seeding(record, rule).0
    }

    /// The upload cap `record` takes from its levels under `rule`.
    pub fn upload_cap(&self, record: &TorrentRecord, rule: Option<&TrackerRule>) -> Option<u64> {
        self.of_torrent(&UPLOAD_CAP, record, rule).resolve()
    }

    // Whether the torrent's archives would be extracted were its job built
    // now: its own value, else its tracker rule's, else its category's,
    // else the global one.
    pub(super) fn unpack(&self, record: &TorrentRecord, rule: Option<&TrackerRule>) -> bool {
        self.of_torrent(&UNPACK, record, rule)
            .resolve()
            .unwrap_or(false)
    }
}

/// The level each of a torrent's seeding terms comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SeedingLevels {
    pub ratio_goal: Level,
    pub seed_time_goal_minutes: Level,
    pub inactive_goal_minutes: Level,
    pub mode: Level,
    pub action: Level,
    pub seed_time_counting: Level,
    pub upload_cap: Level,
}

impl Default for SeedingLevels {
    fn default() -> Self {
        Self {
            ratio_goal: Level::Global,
            seed_time_goal_minutes: Level::Global,
            inactive_goal_minutes: Level::Global,
            mode: Level::Global,
            action: Level::Global,
            seed_time_counting: Level::Global,
            upload_cap: Level::Global,
        }
    }
}

/// The torrents the schedule's pause leaves seeding under one tracker rule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduleSeeding {
    pub host_pattern: String,
    pub torrents: u32,
}

/// How torrents that are still running end after torrents are turned off.
/// Turning off with neither stops every torrent at once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindDown {
    /// Every torrent downloads and seeds to its own limits; then torrents
    /// are off.
    Finish,
    /// Every torrent downloads; a torrent seeding stops; then torrents are
    /// off.
    FinishDownloads,
}

impl WindDown {
    fn as_str(self) -> &'static str {
        match self {
            Self::Finish => "finish",
            Self::FinishDownloads => "finish_downloads",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "finish" => Some(Self::Finish),
            "finish_downloads" => Some(Self::FinishDownloads),
            _ => None,
        }
    }
}

// Where a wind-down is kept, so a restart while torrents end keeps ending
// them instead of stopping them or running them on.
pub(super) const WIND_DOWN_KEY: &str = "torrent.wind_down";

/// Whether there are torrents, as last published: any torrent job, and any
/// torrent not paused or stopped.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Presence {
    pub jobs: bool,
    pub running: bool,
}

impl Presence {
    // `torrent` counted: the one place that says what running is.
    pub(super) fn note(&mut self, torrent: &Live) {
        self.jobs = true;
        self.running |= torrent.resumed();
    }

    // Of the live torrents; the walk ends at the first one running.
    fn of(live: &BTreeMap<JobId, Live>) -> Self {
        let mut presence = Self::default();
        for torrent in live.values() {
            presence.note(torrent);
            if presence.running {
                break;
            }
        }
        presence
    }
}

impl TorrentService {
    /// Take the tracker rules as the database holds them now, match every
    /// torrent's trackers against them again, and apply what that changes.
    pub async fn reload_tracker_rules(&self) -> Result<(), String> {
        self.load_rules()?;
        let rules = self.rules();
        {
            let mut live = self.inner.live.lock().await;
            for torrent in live.values_mut() {
                let job_id = torrent.record.job_id;
                torrent.rule_id = self.rule_id(torrent.trackers.iter());
                // An SSL torrent takes its rule's certificate as it stands:
                // its new connections and announces present it.
                let ssl = certificate_of(torrent.rule(&rules));
                if let Some(attached) = torrent.attached.as_mut()
                    && attached.handle.is_ssl()
                    && attached.ssl != ssl
                {
                    if let Err(error) = attached
                        .session
                        .set_ssl_identity(&attached.handle, ssl_identity(ssl.as_ref()))
                    {
                        tracing::warn!(job_id = job_id.0, error = %error, "an SSL torrent kept its certificate");
                    }
                    attached.ssl = ssl;
                }
            }
        }
        self.apply_queue().await;
        self.publish().await;
        Ok(())
    }

    pub(super) fn load_rules(&self) -> Result<(), String> {
        let rules = self.inner.db.list_tracker_rules().map_err(state_error)?;
        *self
            .inner
            .rules
            .write()
            .unwrap_or_else(PoisonError::into_inner) = Arc::from(rules);
        Ok(())
    }

    /// The torrents the schedule's pause leaves seeding, by the tracker
    /// rule that keeps them: none while no such pause is in force.
    pub async fn schedule_seeding(&self) -> Vec<ScheduleSeeding> {
        let settings = self.settings();
        let rules = self.rules();
        let holds = self.holds();
        let now = self.inner.clock.unix_ms();
        let mut counts: BTreeMap<u32, u32> = BTreeMap::new();
        for torrent in self.inner.live.lock().await.values() {
            if torrent.resumed()
                && holds.spares(queued(&settings, &rules, torrent, now).target())
                && let Some(id) = torrent.rule_id
            {
                *counts.entry(id).or_default() += 1;
            }
        }
        rules
            .iter()
            .filter_map(|rule| {
                counts.get(&rule.id).map(|&torrents| ScheduleSeeding {
                    host_pattern: rule.host_pattern.clone(),
                    torrents,
                })
            })
            .collect()
    }

    /// Take the settings as they are now. Turning torrents off closes every
    /// session and leaves each torrent stopped until they are turned on,
    /// unless a wind-down asked for keeps them running until they end.
    pub async fn set_settings(&self, settings: TorrentSettings) {
        let enabled = settings.torrent.enabled;
        self.inner.sessions.set_config(settings.torrent.clone());
        let was = std::mem::replace(
            &mut *self
                .inner
                .settings
                .write()
                .unwrap_or_else(PoisonError::into_inner),
            settings,
        )
        .torrent;
        let turned_on = enabled && !was.enabled;
        {
            let _turning = self.inner.turning.lock().await;
            // Turned on, nothing winds down; off, a wind-down with nothing
            // left running is over before it starts. While on and staying
            // on, one taken for the save that turns them off is kept: the
            // settings handed on before that save are the old ones.
            if turned_on || (!enabled && self.wind_down().is_some() && !self.running().await) {
                self.keep_wind_down(None).await;
            }
            let on = enabled || self.wind_down().is_some();
            if on != self.inner.sessions.on() {
                self.turn(on).await;
            }
        }
        if turned_on {
            self.resume_wound_down().await;
        }
        // The download limit and the seeding levels take hold now, not at
        // the next tick.
        self.apply_queue().await;
        self.publish().await;
        // The routes follow the settings now too: the tick runs at once.
        self.inner.settings_changed.notify_one();
    }

    /// How the torrents still running end when the settings next turn
    /// torrents off: `None` stops them at once. Taken only while torrents
    /// are on or already winding down; turning them on clears it.
    ///
    /// Given before the settings that turn torrents off are saved, so any
    /// caller that hands those settings on finds it. Returns the wind-down
    /// it replaced, for `keep_wind_down_after_failed_save`.
    pub async fn wind_down_when_off(&self, wind_down: Option<WindDown>) -> Option<WindDown> {
        let _turning = self.inner.turning.lock().await;
        let was = self.wind_down();
        if self.settings().torrent.enabled || was.is_some() {
            self.keep_wind_down(wind_down).await;
        }
        was
    }

    /// The wind-down `was` back, when the save that `wind_down_when_off`
    /// went before failed: none while torrents are on.
    pub async fn keep_wind_down_after_failed_save(&self, was: Option<WindDown>) {
        let _turning = self.inner.turning.lock().await;
        let enabled = self.settings().torrent.enabled;
        self.keep_wind_down(if enabled { None } else { was }).await;
    }

    /// How the torrents still running end, while torrents are off and some
    /// still run.
    pub fn wind_down(&self) -> Option<WindDown> {
        *self.inner.wind_down.borrow()
    }

    /// Whether there are torrents, as last published.
    pub fn presence(&self) -> Presence {
        *self.inner.presence.borrow()
    }

    /// The wind-down as it changes: what a test waits on for one to end.
    pub fn wind_downs(&self) -> watch::Receiver<Option<WindDown>> {
        self.inner.wind_down.subscribe()
    }

    // Whether any torrent is to run: not paused or stopped.
    async fn running(&self) -> bool {
        Presence::of(&*self.inner.live.lock().await).running
    }

    // The wind-down, kept where a restart reads it. Unchanged writes
    // nothing.
    async fn keep_wind_down(&self, wind_down: Option<WindDown>) {
        if self.wind_down() == wind_down {
            return;
        }
        let db = self.inner.db.clone();
        let kept = tokio::task::spawn_blocking(move || match wind_down {
            Some(wind_down) => db.set_setting(WIND_DOWN_KEY, wind_down.as_str()),
            None => db.delete_setting(WIND_DOWN_KEY),
        })
        .await;
        match kept {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                tracing::warn!(error = %error, "the torrents' wind-down was not kept for a restart");
            }
            Err(error) => {
                tracing::warn!(error = %error, "the torrents' wind-down was not kept for a restart");
            }
        }
        self.inner.wind_down.send_replace(wind_down);
    }

    /// Open sessions at start as the settings and a kept wind-down say: a
    /// wind-down a restart cut short goes on; one kept while torrents are
    /// on is dropped.
    pub(crate) async fn turn_at_start(&self, enabled: bool) {
        let db = self.inner.db.clone();
        let stored = match tokio::task::spawn_blocking(move || db.get_setting(WIND_DOWN_KEY)).await
        {
            Ok(Ok(stored)) => stored,
            Ok(Err(error)) => {
                tracing::warn!(error = %error, "the torrents' wind-down was not read");
                None
            }
            Err(error) => {
                tracing::warn!(error = %error, "the torrents' wind-down was not read");
                None
            }
        };
        let kept = stored
            .as_deref()
            .and_then(WindDown::parse)
            .filter(|_| !enabled);
        if stored.is_some() && kept.is_none() {
            let db = self.inner.db.clone();
            if let Ok(Err(error)) =
                tokio::task::spawn_blocking(move || db.delete_setting(WIND_DOWN_KEY)).await
            {
                tracing::warn!(error = %error, "a torrents' wind-down no longer in force was kept");
            }
        }
        self.inner.wind_down.send_replace(kept);
        self.inner.sessions.turn(enabled || kept.is_some()).await;
    }

    // A wind-down with nothing left running ends: torrents are off. Rung
    // from the tick, on a task of its own, as turning off ends the tick.
    pub(super) async fn end_wind_down(&self) {
        let _turning = self.inner.turning.lock().await;
        if self.wind_down().is_none() || self.settings().torrent.enabled || self.running().await {
            return;
        }
        self.keep_wind_down(None).await;
        if self.inner.sessions.on() {
            self.turn(false).await;
        }
    }

    // Every torrent is read before its session changes, as `stop` reads
    // before the sessions close.
    async fn turn(&self, on: bool) {
        self.read(self.inner.clock.unix_ms(), true).await;
        self.inner.sessions.turn(on).await;
        self.read_sessions().await;
        self.run(on).await;
    }

    // While torrents are on the tick runs, putting each torrent in its
    // route's session; while they are off every torrent is out of its
    // session, held off.
    pub(super) async fn run(&self, on: bool) {
        if on {
            self.spawn_tick();
        } else {
            self.stop_tick();
            let mut live = self.inner.live.lock().await;
            for torrent in live.values_mut() {
                self.detach(torrent).await;
                torrent.held = self.inner.sessions.off().map(Held::RouteDown);
            }
        }
        self.publish().await;
    }
}
