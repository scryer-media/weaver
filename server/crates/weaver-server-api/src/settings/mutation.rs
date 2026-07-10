use std::path::PathBuf;
use std::sync::LazyLock;

use async_graphql::{Context, MaybeUndefined, Object, Result};

use crate::auth::AdminGuard;
use crate::observability::{persist_then_update_config, spawn_blocking_db};
use crate::settings::types::{
    DuplicatePolicySettingsInput, GeneralSettings, GeneralSettingsInput, WatchFolderScanReport,
    WatchFolderSettingsInput,
};
use weaver_server_core::settings::SharedConfig;
use weaver_server_core::watch_folder::{WatchFolderConfig, WatchFolderMode, WatchFolderService};
use weaver_server_core::{Database, SchedulerHandle};

static SETTINGS_MUTATION_GUARD: LazyLock<tokio::sync::Mutex<()>> =
    LazyLock::new(|| tokio::sync::Mutex::new(()));

#[derive(Default)]
pub(crate) struct SettingsMutation;

#[Object]
impl SettingsMutation {
    /// Update general settings.
    #[graphql(guard = "AdminGuard")]
    async fn update_settings(
        &self,
        ctx: &Context<'_>,
        input: GeneralSettingsInput,
    ) -> Result<GeneralSettings> {
        let config = ctx.data::<SharedConfig>()?;
        let handle = ctx.data::<SchedulerHandle>()?;
        let db = ctx.data::<Database>()?;
        let _mutation_guard = SETTINGS_MUTATION_GUARD.lock().await;
        let normalized_intermediate_dir = normalize_settings_path_update(&input.intermediate_dir);
        let normalized_complete_dir = normalize_settings_path_update(&input.complete_dir);
        let cleanup_after_extract = input.cleanup_after_extract;
        let max_download_speed = input.max_download_speed;
        let max_retries = input.max_retries;
        let ip_replacement_trial_extra_connections = input.ip_replacement_trial_extra_connections;
        let isp_bandwidth_cap = input.isp_bandwidth_cap.clone();
        let duplicate_policy_update = input.duplicate_policy.clone();
        let watch_folder_update = input
            .watch_folder
            .clone()
            .map(normalize_watch_folder_update)
            .transpose()?;
        if let Some(ref watch) = watch_folder_update {
            let mut candidate = {
                let cfg = config.read().await;
                cfg.watch_folder.clone()
            };
            apply_watch_folder_update(&mut candidate, watch);
            candidate.validate().map_err(async_graphql::Error::new)?;
        }
        let should_reconcile_watch_folder = watch_folder_update.is_some();
        if ip_replacement_trial_extra_connections.unwrap_or(0) > 1 {
            return Err("ip_replacement_trial_extra_connections must be 0 or 1".into());
        }
        let should_update_paths =
            !normalized_intermediate_dir.is_undefined() || !normalized_complete_dir.is_undefined();

        let persist_input = (
            normalized_intermediate_dir.clone(),
            normalized_complete_dir.clone(),
            cleanup_after_extract,
            max_download_speed,
            max_retries,
            isp_bandwidth_cap.clone(),
            ip_replacement_trial_extra_connections,
            watch_folder_update.clone(),
            duplicate_policy_update.clone(),
        );
        let settings_persist = {
            let db = db.clone();
            async move {
                spawn_blocking_db(
                    "settings.mutation.update_settings.persist",
                    move || -> std::result::Result<(), weaver_server_core::StateError> {
                        match &persist_input.0 {
                            MaybeUndefined::Undefined => {}
                            MaybeUndefined::Null => db.delete_setting("intermediate_dir")?,
                            MaybeUndefined::Value(v) => db.set_setting("intermediate_dir", v)?,
                        }
                        match &persist_input.1 {
                            MaybeUndefined::Undefined => {}
                            MaybeUndefined::Null => db.delete_setting("complete_dir")?,
                            MaybeUndefined::Value(v) => db.set_setting("complete_dir", v)?,
                        }
                        if let Some(v) = persist_input.2 {
                            db.set_setting("cleanup_after_extract", &v.to_string())?;
                        }
                        if let Some(v) = persist_input.3 {
                            db.set_setting("max_download_speed", &v.to_string())?;
                        }
                        if let Some(v) = persist_input.4 {
                            db.set_setting("retry.max_retries", &v.to_string())?;
                        }
                        if let Some(ref cap) = persist_input.5 {
                            db.set_setting("bandwidth_cap.enabled", &cap.enabled.to_string())?;
                            db.set_setting(
                                "bandwidth_cap.period",
                                match cap.period {
                                    crate::settings::types::IspBandwidthCapPeriodGql::Daily => {
                                        "daily"
                                    }
                                    crate::settings::types::IspBandwidthCapPeriodGql::Weekly => {
                                        "weekly"
                                    }
                                    crate::settings::types::IspBandwidthCapPeriodGql::Monthly => {
                                        "monthly"
                                    }
                                },
                            )?;
                            db.set_setting(
                                "bandwidth_cap.limit_bytes",
                                &cap.limit_bytes.to_string(),
                            )?;
                            db.set_setting(
                                "bandwidth_cap.reset_time_minutes_local",
                                &cap.reset_time_minutes_local.to_string(),
                            )?;
                            db.set_setting(
                                "bandwidth_cap.weekly_reset_weekday",
                                match cap.weekly_reset_weekday {
                                    crate::settings::types::IspBandwidthCapWeekdayGql::Mon => "mon",
                                    crate::settings::types::IspBandwidthCapWeekdayGql::Tue => "tue",
                                    crate::settings::types::IspBandwidthCapWeekdayGql::Wed => "wed",
                                    crate::settings::types::IspBandwidthCapWeekdayGql::Thu => "thu",
                                    crate::settings::types::IspBandwidthCapWeekdayGql::Fri => "fri",
                                    crate::settings::types::IspBandwidthCapWeekdayGql::Sat => "sat",
                                    crate::settings::types::IspBandwidthCapWeekdayGql::Sun => "sun",
                                },
                            )?;
                            db.set_setting(
                                "bandwidth_cap.monthly_reset_day",
                                &cap.monthly_reset_day.to_string(),
                            )?;
                        }
                        if let Some(v) = persist_input.6 {
                            db.set_setting(
                                "ip_replacement_trial_extra_connections",
                                &v.to_string(),
                            )?;
                        }
                        if let Some(ref watch) = persist_input.7 {
                            if let Some(mode) = watch.mode {
                                db.set_setting("watch_folder.mode", mode.as_str())?;
                            }
                            match &watch.path {
                                MaybeUndefined::Undefined => {}
                                MaybeUndefined::Null => db.delete_setting("watch_folder.path")?,
                                MaybeUndefined::Value(path) => {
                                    db.set_setting("watch_folder.path", path)?
                                }
                            }
                            if let Some(value) = watch.poll_interval_secs {
                                db.set_setting(
                                    "watch_folder.poll_interval_secs",
                                    &value.to_string(),
                                )?;
                            }
                            if let Some(value) = watch.stability_secs {
                                db.set_setting("watch_folder.stability_secs", &value.to_string())?;
                            }
                            if let Some(value) = watch.category_from_subfolders {
                                db.set_setting(
                                    "watch_folder.category_from_subfolders",
                                    &value.to_string(),
                                )?;
                            }
                            if let Some(value) = watch.scanning_paused {
                                db.set_setting("watch_folder.scanning_paused", &value.to_string())?;
                            }
                        }
                        if let Some(ref duplicate_policy) = persist_input.8 {
                            if let Some(value) = duplicate_policy.strict_active_or_success {
                                db.set_setting(
                                    "duplicate_policy.strict_active_or_success",
                                    weaver_server_core::jobs::DuplicateAction::from(value).as_str(),
                                )?;
                            }
                            if let Some(value) = duplicate_policy.strict_failed_or_cancelled {
                                db.set_setting(
                                    "duplicate_policy.strict_failed_or_cancelled",
                                    weaver_server_core::jobs::DuplicateAction::from(value).as_str(),
                                )?;
                            }
                            if let Some(value) = duplicate_policy.article_layout_active_or_success {
                                db.set_setting(
                                    "duplicate_policy.article_layout_active_or_success",
                                    weaver_server_core::jobs::DuplicateAction::from(value).as_str(),
                                )?;
                            }
                            if let Some(value) = duplicate_policy.article_layout_failed_or_cancelled
                            {
                                db.set_setting(
                                    "duplicate_policy.article_layout_failed_or_cancelled",
                                    weaver_server_core::jobs::DuplicateAction::from(value).as_str(),
                                )?;
                            }
                            if let Some(value) = duplicate_policy.article_set {
                                db.set_setting(
                                    "duplicate_policy.article_set",
                                    weaver_server_core::jobs::DuplicateAction::from(value).as_str(),
                                )?;
                            }
                            if let Some(value) = duplicate_policy.normalized_name {
                                db.set_setting(
                                    "duplicate_policy.normalized_name",
                                    weaver_server_core::jobs::DuplicateAction::from(value).as_str(),
                                )?;
                            }
                        }
                        Ok(())
                    },
                )
                .await
            }
        };

        let (settings, runtime_paths) = persist_then_update_config(
            config,
            "settings.mutation.update_settings.apply",
            settings_persist,
            move |cfg| {
                match &normalized_intermediate_dir {
                    MaybeUndefined::Undefined => {}
                    MaybeUndefined::Null => cfg.intermediate_dir = None,
                    MaybeUndefined::Value(intermediate_dir) => {
                        cfg.intermediate_dir = Some(intermediate_dir.clone());
                    }
                }
                match &normalized_complete_dir {
                    MaybeUndefined::Undefined => {}
                    MaybeUndefined::Null => cfg.complete_dir = None,
                    MaybeUndefined::Value(complete_dir) => {
                        cfg.complete_dir = Some(complete_dir.clone());
                    }
                }
                if let Some(cleanup) = cleanup_after_extract {
                    cfg.cleanup_after_extract = Some(cleanup);
                }
                if let Some(speed) = max_download_speed {
                    cfg.max_download_speed = Some(speed);
                }
                if let Some(retries) = max_retries {
                    let retry =
                        cfg.retry
                            .get_or_insert(weaver_server_core::settings::RetryOverrides {
                                max_retries: None,
                                base_delay_secs: None,
                                multiplier: None,
                            });
                    retry.max_retries = Some(retries);
                }
                if let Some(cap) = isp_bandwidth_cap {
                    cfg.isp_bandwidth_cap = Some(cap.into());
                }
                if let Some(extra) = ip_replacement_trial_extra_connections {
                    cfg.ip_replacement_trial_extra_connections = Some(extra);
                }
                if let Some(ref watch) = watch_folder_update {
                    apply_watch_folder_update(&mut cfg.watch_folder, watch);
                }
                if let Some(ref duplicate_policy) = duplicate_policy_update {
                    apply_duplicate_policy_update(&mut cfg.duplicate_policy, duplicate_policy);
                }

                let runtime_paths = should_update_paths.then(|| {
                    (
                        PathBuf::from(&cfg.data_dir),
                        PathBuf::from(cfg.intermediate_dir()),
                        PathBuf::from(cfg.complete_dir()),
                    )
                });
                let settings = GeneralSettings {
                    data_dir: cfg.data_dir.clone(),
                    intermediate_dir: cfg.intermediate_dir(),
                    complete_dir: cfg.complete_dir(),
                    cleanup_after_extract: cfg.cleanup_after_extract(),
                    max_download_speed: cfg.max_download_speed.unwrap_or(0),
                    max_retries: cfg.retry.as_ref().and_then(|r| r.max_retries).unwrap_or(3),
                    ip_replacement_trial_extra_connections: cfg
                        .ip_replacement_trial_extra_connections(),
                    isp_bandwidth_cap: cfg.isp_bandwidth_cap.as_ref().map(Into::into),
                    watch_folder: (&cfg.watch_folder).into(),
                    duplicate_policy: cfg.duplicate_policy.into(),
                };
                (settings, runtime_paths)
            },
        )
        .await?;

        // Apply speed limit immediately.
        if let Some(speed) = max_download_speed {
            let _ = handle.set_speed_limit(speed).await;
        }
        if let Some(cap) = input.isp_bandwidth_cap {
            let _ = handle.set_bandwidth_cap_policy(Some(cap.into())).await;
        }
        if let Some(extra) = ip_replacement_trial_extra_connections {
            let _ = handle
                .set_ip_replacement_trial_extra_connections(extra)
                .await;
        }

        // Apply directory changes immediately so new jobs use them without restart.
        if let Some((data_dir, intermediate_dir, complete_dir)) = runtime_paths {
            let _ = handle
                .update_runtime_paths(data_dir, intermediate_dir, complete_dir)
                .await;
        }
        if should_reconcile_watch_folder {
            let watch_folder = ctx.data::<WatchFolderService>()?;
            watch_folder
                .reconcile_from_config()
                .await
                .map_err(|error| async_graphql::Error::new(error.to_string()))?;
        }

        Ok(settings)
    }

    #[graphql(guard = "AdminGuard")]
    async fn scan_watch_folder(&self, ctx: &Context<'_>) -> Result<WatchFolderScanReport> {
        let watch_folder = ctx.data::<WatchFolderService>()?;
        let report = watch_folder
            .scan_now()
            .await
            .map_err(|error| async_graphql::Error::new(error.to_string()))?;
        Ok(report.into())
    }

    #[graphql(guard = "AdminGuard")]
    async fn create_schedule(
        &self,
        ctx: &Context<'_>,
        input: crate::settings::types::ScheduleInput,
    ) -> Result<Vec<crate::settings::types::Schedule>> {
        let db = ctx.data::<Database>()?.clone();
        let schedules_state = ctx
            .data::<weaver_server_core::bandwidth::schedule::SharedSchedules>()?
            .clone();
        let entry = input.into_entry();
        let mut entries = tokio::task::spawn_blocking({
            let db = db.clone();
            move || db.list_schedules()
        })
        .await??;
        entries.push(entry);
        let entries_for_save = entries.clone();
        tokio::task::spawn_blocking(move || db.save_schedules(&entries_for_save)).await??;
        *schedules_state.write().await = entries.clone();
        Ok(entries
            .into_iter()
            .map(crate::settings::types::Schedule::from)
            .collect())
    }
    #[graphql(guard = "AdminGuard")]
    async fn update_schedule(
        &self,
        ctx: &Context<'_>,
        id: String,
        input: crate::settings::types::ScheduleInput,
    ) -> Result<Vec<crate::settings::types::Schedule>> {
        let db = ctx.data::<Database>()?.clone();
        let schedules_state = ctx
            .data::<weaver_server_core::bandwidth::schedule::SharedSchedules>()?
            .clone();
        let mut entries = tokio::task::spawn_blocking({
            let db = db.clone();
            move || db.list_schedules()
        })
        .await??;
        if let Some(existing) = entries.iter_mut().find(|e| e.id == id) {
            let updated = input.into_entry();
            existing.enabled = updated.enabled;
            existing.label = updated.label;
            existing.days = updated.days;
            existing.time = updated.time;
            existing.action = updated.action;
        }
        let entries_for_save = entries.clone();
        tokio::task::spawn_blocking(move || db.save_schedules(&entries_for_save)).await??;
        *schedules_state.write().await = entries.clone();
        Ok(entries
            .into_iter()
            .map(crate::settings::types::Schedule::from)
            .collect())
    }
    #[graphql(guard = "AdminGuard")]
    async fn delete_schedule(
        &self,
        ctx: &Context<'_>,
        id: String,
    ) -> Result<Vec<crate::settings::types::Schedule>> {
        let db = ctx.data::<Database>()?.clone();
        let schedules_state = ctx
            .data::<weaver_server_core::bandwidth::schedule::SharedSchedules>()?
            .clone();
        let mut entries = tokio::task::spawn_blocking({
            let db = db.clone();
            move || db.list_schedules()
        })
        .await??;
        entries.retain(|e| e.id != id);
        let entries_for_save = entries.clone();
        tokio::task::spawn_blocking(move || db.save_schedules(&entries_for_save)).await??;
        *schedules_state.write().await = entries.clone();
        Ok(entries
            .into_iter()
            .map(crate::settings::types::Schedule::from)
            .collect())
    }
    #[graphql(guard = "AdminGuard")]
    async fn toggle_schedule(
        &self,
        ctx: &Context<'_>,
        id: String,
        enabled: bool,
    ) -> Result<Vec<crate::settings::types::Schedule>> {
        let db = ctx.data::<Database>()?.clone();
        let schedules_state = ctx
            .data::<weaver_server_core::bandwidth::schedule::SharedSchedules>()?
            .clone();
        let mut entries = tokio::task::spawn_blocking({
            let db = db.clone();
            move || db.list_schedules()
        })
        .await??;
        if let Some(existing) = entries.iter_mut().find(|e| e.id == id) {
            existing.enabled = enabled;
        }
        let entries_for_save = entries.clone();
        tokio::task::spawn_blocking(move || db.save_schedules(&entries_for_save)).await??;
        *schedules_state.write().await = entries.clone();
        Ok(entries
            .into_iter()
            .map(crate::settings::types::Schedule::from)
            .collect())
    }
}

fn normalize_settings_path_update(input: &MaybeUndefined<String>) -> MaybeUndefined<String> {
    match input {
        MaybeUndefined::Undefined => MaybeUndefined::Undefined,
        MaybeUndefined::Null => MaybeUndefined::Null,
        MaybeUndefined::Value(value) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                MaybeUndefined::Null
            } else {
                MaybeUndefined::Value(trimmed.to_string())
            }
        }
    }
}

#[derive(Debug, Clone)]
struct NormalizedWatchFolderSettingsInput {
    mode: Option<WatchFolderMode>,
    path: MaybeUndefined<String>,
    poll_interval_secs: Option<u64>,
    stability_secs: Option<u64>,
    category_from_subfolders: Option<bool>,
    scanning_paused: Option<bool>,
}

fn normalize_watch_folder_update(
    input: WatchFolderSettingsInput,
) -> Result<NormalizedWatchFolderSettingsInput> {
    let mode = input
        .mode
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| {
            WatchFolderMode::parse(value).ok_or_else(|| {
                async_graphql::Error::new("watch_folder.mode must be off, polling, or realtime")
            })
        })
        .transpose()?;
    if input.poll_interval_secs == Some(0) {
        return Err("watch_folder.poll_interval_secs must be greater than 0".into());
    }
    Ok(NormalizedWatchFolderSettingsInput {
        mode,
        path: normalize_settings_path_update(&input.path),
        poll_interval_secs: input.poll_interval_secs,
        stability_secs: input.stability_secs,
        category_from_subfolders: input.category_from_subfolders,
        scanning_paused: input.scanning_paused,
    })
}

fn apply_watch_folder_update(
    config: &mut WatchFolderConfig,
    update: &NormalizedWatchFolderSettingsInput,
) {
    if let Some(mode) = update.mode {
        config.mode = mode;
    }
    match &update.path {
        MaybeUndefined::Undefined => {}
        MaybeUndefined::Null => config.path = None,
        MaybeUndefined::Value(path) => config.path = Some(path.clone()),
    }
    if let Some(value) = update.poll_interval_secs {
        config.poll_interval_secs = value;
    }
    if let Some(value) = update.stability_secs {
        config.stability_secs = value;
    }
    if let Some(value) = update.category_from_subfolders {
        config.category_from_subfolders = value;
    }
    if let Some(value) = update.scanning_paused {
        config.scanning_paused = value;
    }
}

fn apply_duplicate_policy_update(
    policy: &mut weaver_server_core::jobs::DuplicatePolicy,
    update: &DuplicatePolicySettingsInput,
) {
    if let Some(value) = update.strict_active_or_success {
        policy.strict_active_or_success = value.into();
    }
    if let Some(value) = update.strict_failed_or_cancelled {
        policy.strict_failed_or_cancelled = value.into();
    }
    if let Some(value) = update.article_layout_active_or_success {
        policy.article_layout_active_or_success = value.into();
    }
    if let Some(value) = update.article_layout_failed_or_cancelled {
        policy.article_layout_failed_or_cancelled = value.into();
    }
    if let Some(value) = update.article_set {
        policy.article_set = value.into();
    }
    if let Some(value) = update.normalized_name {
        policy.normalized_name = value.into();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::sync::{RwLock, oneshot};
    use tokio::time::timeout;

    use crate::observability::{persist_then_update_config, with_timed_config_read};

    use super::*;

    fn test_config() -> SharedConfig {
        Arc::new(RwLock::new(weaver_server_core::settings::Config {
            data_dir: "/tmp/weaver".to_string(),
            intermediate_dir: None,
            complete_dir: None,
            buffer_pool: None,
            tuner: None,
            servers: vec![],
            categories: vec![],
            retry: None,
            max_download_speed: None,
            cleanup_after_extract: None,
            isp_bandwidth_cap: None,
            ip_replacement_trial_extra_connections: None,
            watch_folder: weaver_server_core::watch_folder::WatchFolderConfig::default(),
            duplicate_policy: weaver_server_core::jobs::DuplicatePolicy::default(),
            config_path: None,
        }))
    }

    #[tokio::test]
    async fn slow_persist_does_not_block_settings_reads() {
        let config = test_config();
        let (release_tx, release_rx) = oneshot::channel();

        let update_task = tokio::spawn({
            let config = config.clone();
            async move {
                persist_then_update_config(
                    &config,
                    "tests.settings.persist_then_update",
                    async move {
                        release_rx.await.expect("release signal should arrive");
                        Ok(())
                    },
                    |cfg| {
                        cfg.max_download_speed = Some(42);
                    },
                )
                .await
                .expect("settings update should succeed");
            }
        });

        tokio::task::yield_now().await;

        let read_result = timeout(
            Duration::from_millis(50),
            with_timed_config_read(&config, "tests.settings.read", |cfg| cfg.max_download_speed),
        )
        .await
        .expect("settings read should not block on slow persist");
        assert_eq!(read_result, None);

        release_tx
            .send(())
            .expect("update task should still be waiting");
        update_task
            .await
            .expect("update task should finish cleanly");
    }
}
