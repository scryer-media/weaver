// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::StateError;
use crate::jobs::{DuplicateAction, DuplicatePolicy};
use crate::persistence::Database;
use crate::runtime::hardware_profile::HardwareProfile;
use crate::settings::record::SettingRecord;
use crate::settings::{
    BufferPoolOverrides, Config, DeliveryNamingOverrides, DirectStoreOverrides,
    DirectUnpackOverrides, MetricsConfig, PerJobSeries, RetryOverrides, SeedingAction,
    SeedingGoalMode, SeedingPolicy, TorrentConfig, TorrentEncryption,
};
use crate::watch_folder::{WatchFolderConfig, WatchFolderMode};

// The key the chosen hardware profile is stored under. Absent until an
// operator picks one.
pub const HARDWARE_PROFILE_SETTING: &str = "hardware_profile";

// Keys earlier releases stored that nothing reads any more. Loading the
// config removes them, so an old database or a restored backup does not
// carry a setting the product no longer has.
pub(crate) const RETIRED_SETTING_KEYS: &[&str] = &["ip_replacement_trial_extra_connections"];

impl Database {
    // Load a full `Config` from the settings and servers tables.
    pub fn load_config(&self) -> Result<Config, StateError> {
        let settings: std::collections::HashMap<String, String> = self
            .list_setting_records()?
            .into_iter()
            .map(|SettingRecord { key, value }| (key, value))
            .collect();
        for key in RETIRED_SETTING_KEYS {
            if settings.contains_key(*key) {
                self.delete_setting(key)?;
            }
        }
        let servers = self.list_servers()?;
        let categories = self.list_categories()?;

        let data_dir = settings.get("data_dir").cloned().unwrap_or_default();
        let intermediate_dir = settings
            .get("intermediate_dir")
            .cloned()
            .or_else(|| settings.get("output_dir").cloned());
        let complete_dir = settings.get("complete_dir").cloned();
        let bandwidth = self.network_bandwidth()?;
        let propagation_delay_secs = settings
            .get("propagation_delay_secs")
            .and_then(|v| v.parse().ok());
        let cleanup_after_extract = settings
            .get("cleanup_after_extract")
            .and_then(|v| v.parse().ok());
        let watch_folder = WatchFolderConfig {
            mode: settings
                .get("watch_folder.mode")
                .and_then(|v| WatchFolderMode::parse(v))
                .unwrap_or_default(),
            path: settings.get("watch_folder.path").cloned(),
            poll_interval_secs: settings
                .get("watch_folder.poll_interval_secs")
                .and_then(|v| v.parse().ok())
                .unwrap_or(60),
            stability_secs: settings
                .get("watch_folder.stability_secs")
                .and_then(|v| v.parse().ok())
                .unwrap_or(3),
            category_from_subfolders: settings
                .get("watch_folder.category_from_subfolders")
                .and_then(|v| v.parse().ok())
                .unwrap_or(true),
            scanning_paused: settings
                .get("watch_folder.scanning_paused")
                .and_then(|v| v.parse().ok())
                .unwrap_or(false),
        };
        let buffer_pool = {
            let small = settings
                .get("buffer_pool.small_count")
                .and_then(|v| v.parse().ok());
            let medium = settings
                .get("buffer_pool.medium_count")
                .and_then(|v| v.parse().ok());
            let large = settings
                .get("buffer_pool.large_count")
                .and_then(|v| v.parse().ok());
            if small.is_some() || medium.is_some() || large.is_some() {
                Some(BufferPoolOverrides {
                    small_count: small,
                    medium_count: medium,
                    large_count: large,
                })
            } else {
                None
            }
        };

        let retry = {
            let max_retries = settings
                .get("retry.max_retries")
                .and_then(|v| v.parse().ok());
            let base_delay = settings
                .get("retry.base_delay_secs")
                .and_then(|v| v.parse().ok());
            let multiplier = settings
                .get("retry.multiplier")
                .and_then(|v| v.parse().ok());
            if max_retries.is_some() || base_delay.is_some() || multiplier.is_some() {
                Some(RetryOverrides {
                    max_retries,
                    base_delay_secs: base_delay,
                    multiplier,
                })
            } else {
                None
            }
        };

        // Absent keys mean "defaults", and a partially configured table is
        // normal: an operator who only ever flips `enabled` should not have to
        // restate the scratch ceiling.
        let direct_store = {
            let enabled = settings
                .get("direct_store.enabled")
                .and_then(|v| v.parse().ok());
            let bytes_setting = |key: &str| settings.get(key).and_then(|v| v.parse::<u64>().ok());
            let holds_scratch_ceiling_bytes =
                bytes_setting("direct_store.holds_scratch_ceiling_bytes");
            let holds_resident_limit_bytes =
                bytes_setting("direct_store.holds_resident_limit_bytes");
            let holds_scratch_total_bytes = bytes_setting("direct_store.holds_scratch_total_bytes");
            let holds_disk_reserve_bytes = bytes_setting("direct_store.holds_disk_reserve_bytes");
            if enabled.is_some()
                || holds_scratch_ceiling_bytes.is_some()
                || holds_resident_limit_bytes.is_some()
                || holds_scratch_total_bytes.is_some()
                || holds_disk_reserve_bytes.is_some()
            {
                Some(DirectStoreOverrides {
                    enabled,
                    holds_scratch_ceiling_bytes,
                    holds_resident_limit_bytes,
                    holds_scratch_total_bytes,
                    holds_disk_reserve_bytes,
                })
            } else {
                None
            }
        };

        let direct_unpack = {
            let enabled = settings
                .get("direct_unpack.enabled")
                .and_then(|v| v.parse().ok());
            enabled.map(|enabled| DirectUnpackOverrides {
                enabled: Some(enabled),
            })
        };

        let delivery_naming = {
            let deobfuscate_delivered_members = settings
                .get("delivery_naming.deobfuscate_delivered_members")
                .and_then(|v| v.parse().ok());
            let enable_srrdb_lookup = settings
                .get("delivery_naming.enable_srrdb_lookup")
                .and_then(|v| v.parse().ok());
            if deobfuscate_delivered_members.is_some() || enable_srrdb_lookup.is_some() {
                Some(DeliveryNamingOverrides {
                    deobfuscate_delivered_members,
                    enable_srrdb_lookup,
                })
            } else {
                None
            }
        };

        let default_duplicate_policy = DuplicatePolicy::default();
        let duplicate_policy = DuplicatePolicy {
            strict_active_or_success: setting_duplicate_action(
                &settings,
                "duplicate_policy.strict_active_or_success",
            )
            .unwrap_or(default_duplicate_policy.strict_active_or_success),
            strict_failed_or_cancelled: setting_duplicate_action(
                &settings,
                "duplicate_policy.strict_failed_or_cancelled",
            )
            .unwrap_or(default_duplicate_policy.strict_failed_or_cancelled),
            article_layout_active_or_success: setting_duplicate_action(
                &settings,
                "duplicate_policy.article_layout_active_or_success",
            )
            .unwrap_or(default_duplicate_policy.article_layout_active_or_success),
            article_layout_failed_or_cancelled: setting_duplicate_action(
                &settings,
                "duplicate_policy.article_layout_failed_or_cancelled",
            )
            .unwrap_or(default_duplicate_policy.article_layout_failed_or_cancelled),
            article_set: setting_duplicate_action(&settings, "duplicate_policy.article_set")
                .unwrap_or(default_duplicate_policy.article_set),
            normalized_name: setting_duplicate_action(
                &settings,
                "duplicate_policy.normalized_name",
            )
            .unwrap_or(default_duplicate_policy.normalized_name),
        };

        // Absent — and unparseable, from a hand-edited value — both mean "never
        // chosen", which the runtime answers with the machine's recommendation.
        let hardware_profile = settings
            .get(HARDWARE_PROFILE_SETTING)
            .and_then(|value| HardwareProfile::parse(value));

        let metrics = MetricsConfig {
            per_job_series: settings
                .get("metrics.per_job_series")
                .map(|value| PerJobSeries::from_str_or_default(value))
                .unwrap_or_default(),
        };

        let seeding = seeding_policy_from_settings(&settings);
        let torrent = torrent_config_from_settings(&settings);

        Ok(Config {
            data_dir,
            intermediate_dir,
            complete_dir,
            buffer_pool,
            servers,
            categories,
            retry,
            bandwidth,
            cleanup_after_extract,
            propagation_delay_secs,
            watch_folder,
            duplicate_policy,
            direct_store,
            direct_unpack,
            delivery_naming,
            hardware_profile,
            metrics,
            seeding,
            torrent,
            config_path: None,
        })
    }

    // Write the global seeding policy; `None` removes it.
    pub fn save_seeding_policy(&self, policy: Option<&SeedingPolicy>) -> Result<(), StateError> {
        let Some(policy) = policy else {
            for key in SEEDING_KEYS {
                self.delete_setting(key)?;
            }
            return Ok(());
        };
        let optional = [
            (SEEDING_RATIO_GOAL, policy.ratio_goal.map(|v| v.to_string())),
            (
                SEEDING_SEED_TIME_GOAL_MINUTES,
                policy.seed_time_goal_minutes.map(|v| v.to_string()),
            ),
            (
                SEEDING_INACTIVE_GOAL_MINUTES,
                policy.inactive_goal_minutes.map(|v| v.to_string()),
            ),
            (
                SEEDING_MAX_UPLOAD_BPS,
                policy.max_upload_bps.map(|v| v.to_string()),
            ),
        ];
        for (key, value) in optional {
            match value {
                Some(value) => self.set_setting(key, &value)?,
                None => self.delete_setting(key)?,
            }
        }
        self.set_setting(SEEDING_MODE, policy.mode.as_str())?;
        self.set_setting(SEEDING_ACTION, policy.action.as_str())?;
        Ok(())
    }

    pub fn save_torrent_config(&self, torrent: &TorrentConfig) -> Result<(), StateError> {
        self.set_setting("torrent.enabled", &torrent.enabled.to_string())?;
        self.set_setting("torrent.base_port", &torrent.base_port.to_string())?;
        match torrent.announce_port {
            Some(port) => self.set_setting("torrent.announce_port", &port.to_string())?,
            None => self.delete_setting("torrent.announce_port")?,
        }
        self.set_setting("torrent.upnp", &torrent.upnp.to_string())?;
        self.set_setting("torrent.encryption", torrent.encryption.as_str())?;
        self.set_setting(
            "torrent.max_active_downloads",
            &torrent.max_active_downloads.to_string(),
        )?;
        self.set_setting(
            "torrent.max_peers_total",
            &torrent.max_peers_total.to_string(),
        )?;
        match torrent.session_dir.as_deref().and_then(|dir| dir.to_str()) {
            Some(dir) => self.set_setting("torrent.session_dir", dir)?,
            None => self.delete_setting("torrent.session_dir")?,
        }
        self.set_setting("torrent.unpack", &torrent.unpack.to_string())?;
        Ok(())
    }

    // Save a full `Config` to the settings and servers tables.
    pub fn save_config(&self, config: &Config) -> Result<(), StateError> {
        self.set_setting("data_dir", &config.data_dir)?;
        if let Some(ref intermediate_dir) = config.intermediate_dir {
            self.set_setting("intermediate_dir", intermediate_dir)?;
        }
        if let Some(ref complete_dir) = config.complete_dir {
            self.set_setting("complete_dir", complete_dir)?;
        }
        self.set_network_bandwidth(&config.bandwidth)?;
        if let Some(delay) = config.propagation_delay_secs {
            self.set_setting("propagation_delay_secs", &delay.to_string())?;
        }
        if let Some(cleanup) = config.cleanup_after_extract {
            self.set_setting("cleanup_after_extract", &cleanup.to_string())?;
        }
        self.set_setting("watch_folder.mode", config.watch_folder.mode.as_str())?;
        match config.watch_folder.normalized_path() {
            Some(path) => self.set_setting("watch_folder.path", &path)?,
            None => self.delete_setting("watch_folder.path")?,
        }
        self.set_setting(
            "watch_folder.poll_interval_secs",
            &config.watch_folder.poll_interval_secs.to_string(),
        )?;
        self.set_setting(
            "watch_folder.stability_secs",
            &config.watch_folder.stability_secs.to_string(),
        )?;
        self.set_setting(
            "watch_folder.category_from_subfolders",
            &config.watch_folder.category_from_subfolders.to_string(),
        )?;
        self.set_setting(
            "watch_folder.scanning_paused",
            &config.watch_folder.scanning_paused.to_string(),
        )?;
        self.set_setting(
            "duplicate_policy.strict_active_or_success",
            config.duplicate_policy.strict_active_or_success.as_str(),
        )?;
        self.set_setting(
            "duplicate_policy.strict_failed_or_cancelled",
            config.duplicate_policy.strict_failed_or_cancelled.as_str(),
        )?;
        self.set_setting(
            "duplicate_policy.article_layout_active_or_success",
            config
                .duplicate_policy
                .article_layout_active_or_success
                .as_str(),
        )?;
        self.set_setting(
            "duplicate_policy.article_layout_failed_or_cancelled",
            config
                .duplicate_policy
                .article_layout_failed_or_cancelled
                .as_str(),
        )?;
        self.set_setting(
            "duplicate_policy.article_set",
            config.duplicate_policy.article_set.as_str(),
        )?;
        self.set_setting(
            "duplicate_policy.normalized_name",
            config.duplicate_policy.normalized_name.as_str(),
        )?;
        if let Some(ref bp) = config.buffer_pool {
            if let Some(v) = bp.small_count {
                self.set_setting("buffer_pool.small_count", &v.to_string())?;
            }
            if let Some(v) = bp.medium_count {
                self.set_setting("buffer_pool.medium_count", &v.to_string())?;
            }
            if let Some(v) = bp.large_count {
                self.set_setting("buffer_pool.large_count", &v.to_string())?;
            }
        }

        if let Some(ref direct_store) = config.direct_store {
            if let Some(enabled) = direct_store.enabled {
                self.set_setting("direct_store.enabled", &enabled.to_string())?;
            }
            for (key, bytes) in [
                (
                    "direct_store.holds_scratch_ceiling_bytes",
                    direct_store.holds_scratch_ceiling_bytes,
                ),
                (
                    "direct_store.holds_resident_limit_bytes",
                    direct_store.holds_resident_limit_bytes,
                ),
                (
                    "direct_store.holds_scratch_total_bytes",
                    direct_store.holds_scratch_total_bytes,
                ),
                (
                    "direct_store.holds_disk_reserve_bytes",
                    direct_store.holds_disk_reserve_bytes,
                ),
            ] {
                if let Some(bytes) = bytes {
                    self.set_setting(key, &bytes.to_string())?;
                }
            }
        }

        if let Some(ref direct_unpack) = config.direct_unpack
            && let Some(enabled) = direct_unpack.enabled
        {
            self.set_setting("direct_unpack.enabled", &enabled.to_string())?;
        }

        if let Some(ref delivery_naming) = config.delivery_naming {
            if let Some(enabled) = delivery_naming.deobfuscate_delivered_members {
                self.set_setting(
                    "delivery_naming.deobfuscate_delivered_members",
                    &enabled.to_string(),
                )?;
            }
            if let Some(enabled) = delivery_naming.enable_srrdb_lookup {
                self.set_setting("delivery_naming.enable_srrdb_lookup", &enabled.to_string())?;
            }
        }

        if let Some(ref retry) = config.retry {
            if let Some(v) = retry.max_retries {
                self.set_setting("retry.max_retries", &v.to_string())?;
            }
            if let Some(v) = retry.base_delay_secs {
                self.set_setting("retry.base_delay_secs", &v.to_string())?;
            }
            if let Some(v) = retry.multiplier {
                self.set_setting("retry.multiplier", &v.to_string())?;
            }
        }

        // Only a chosen profile is written: an absent key is what tells the
        // settings page the recommendation has not been confirmed yet.
        if let Some(profile) = config.hardware_profile {
            self.set_setting(HARDWARE_PROFILE_SETTING, profile.as_str())?;
        }

        self.set_setting(
            "metrics.per_job_series",
            config.metrics.per_job_series.as_str(),
        )?;

        self.save_seeding_policy(config.seeding.as_ref())?;
        self.save_torrent_config(&config.torrent)?;

        self.replace_servers(&config.servers)?;
        self.replace_categories(&config.categories)?;

        Ok(())
    }
}

const SEEDING_RATIO_GOAL: &str = "seeding.ratio_goal";
const SEEDING_SEED_TIME_GOAL_MINUTES: &str = "seeding.seed_time_goal_minutes";
const SEEDING_INACTIVE_GOAL_MINUTES: &str = "seeding.inactive_goal_minutes";
const SEEDING_MODE: &str = "seeding.mode";
const SEEDING_ACTION: &str = "seeding.action";
const SEEDING_MAX_UPLOAD_BPS: &str = "seeding.max_upload_bps";
const SEEDING_KEYS: [&str; 6] = [
    SEEDING_RATIO_GOAL,
    SEEDING_SEED_TIME_GOAL_MINUTES,
    SEEDING_INACTIVE_GOAL_MINUTES,
    SEEDING_MODE,
    SEEDING_ACTION,
    SEEDING_MAX_UPLOAD_BPS,
];

fn parsed_setting<T: std::str::FromStr>(
    settings: &std::collections::HashMap<String, String>,
    key: &str,
) -> Option<T> {
    settings.get(key).and_then(|v| v.parse().ok())
}

// The global policy exists once any of its keys does; a saved policy always
// writes its mode and action.
fn seeding_policy_from_settings(
    settings: &std::collections::HashMap<String, String>,
) -> Option<SeedingPolicy> {
    if !SEEDING_KEYS.iter().any(|key| settings.contains_key(*key)) {
        return None;
    }
    Some(SeedingPolicy {
        ratio_goal: parsed_setting(settings, SEEDING_RATIO_GOAL),
        seed_time_goal_minutes: parsed_setting(settings, SEEDING_SEED_TIME_GOAL_MINUTES),
        inactive_goal_minutes: parsed_setting(settings, SEEDING_INACTIVE_GOAL_MINUTES),
        mode: settings
            .get(SEEDING_MODE)
            .and_then(|v| SeedingGoalMode::parse(v))
            .unwrap_or_default(),
        action: settings
            .get(SEEDING_ACTION)
            .and_then(|v| SeedingAction::parse(v))
            .unwrap_or_default(),
        max_upload_bps: parsed_setting(settings, SEEDING_MAX_UPLOAD_BPS),
    })
}

fn torrent_config_from_settings(
    settings: &std::collections::HashMap<String, String>,
) -> TorrentConfig {
    let defaults = TorrentConfig::default();
    TorrentConfig {
        enabled: parsed_setting(settings, "torrent.enabled").unwrap_or(defaults.enabled),
        base_port: parsed_setting(settings, "torrent.base_port").unwrap_or(defaults.base_port),
        announce_port: parsed_setting(settings, "torrent.announce_port"),
        upnp: parsed_setting(settings, "torrent.upnp").unwrap_or(defaults.upnp),
        encryption: settings
            .get("torrent.encryption")
            .and_then(|v| TorrentEncryption::parse(v))
            .unwrap_or(defaults.encryption),
        max_active_downloads: parsed_setting(settings, "torrent.max_active_downloads")
            .unwrap_or(defaults.max_active_downloads),
        max_peers_total: parsed_setting(settings, "torrent.max_peers_total")
            .unwrap_or(defaults.max_peers_total),
        session_dir: settings
            .get("torrent.session_dir")
            .map(std::path::PathBuf::from),
        unpack: parsed_setting(settings, "torrent.unpack").unwrap_or(defaults.unpack),
    }
}

fn setting_duplicate_action(
    settings: &std::collections::HashMap<String, String>,
    key: &str,
) -> Option<DuplicateAction> {
    settings
        .get(key)
        .and_then(|value| DuplicateAction::from_persisted(value))
}
