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

/// Refuse a schedule rule this machine could never apply, judged against the
/// live probe as the hardware-profile choice is.
async fn validate_schedule_input(
    ctx: &Context<'_>,
    input: &crate::settings::types::ScheduleInput,
) -> Result<()> {
    let system = ctx.data::<crate::context::SystemRuntimeContext>()?;
    let probe = system
        .profile
        .read()
        .map_err(|_| async_graphql::Error::new("system profile unavailable"))?
        .clone();
    input.validate(&probe).map_err(async_graphql::Error::new)?;
    let Some(instance_id) = input.script_instance_id() else {
        return Ok(());
    };
    // A rule is saved against the instance it runs, so one that names nothing,
    // or something a schedule cannot start, is refused here instead of being
    // skipped every time it comes due.
    let db = ctx.data::<Database>()?.clone();
    let instance = tokio::task::spawn_blocking(move || db.script_instance(&instance_id)).await??;
    match instance {
        Some(instance)
            if instance.trigger
                == weaver_server_core::post_processing::instances::InstanceTrigger::Schedule =>
        {
            Ok(())
        }
        Some(_) => Err(async_graphql::Error::new(
            "that script instance does not run on a schedule",
        )),
        None => Err(async_graphql::Error::new(
            "that script instance does not exist",
        )),
    }
}

async fn schedule_response(
    _ctx: &Context<'_>,
    entries: Vec<weaver_server_core::bandwidth::ScheduleEntry>,
) -> Result<Vec<crate::settings::types::Schedule>> {
    Ok(entries.into_iter().map(Into::into).collect())
}

static SETTINGS_MUTATION_GUARD: LazyLock<tokio::sync::Mutex<()>> =
    LazyLock::new(|| tokio::sync::Mutex::new(()));

#[derive(Default)]
pub(crate) struct SettingsMutation;

#[Object]
impl SettingsMutation {
    /// Choose how hard Weaver leans on this machine.
    ///
    /// A profile the machine cannot honour is refused by name rather than
    /// quietly downgraded, so an operator who asked for more is told what the
    /// hardware is short of.
    #[graphql(guard = "AdminGuard")]
    async fn set_hardware_profile(
        &self,
        ctx: &Context<'_>,
        profile: crate::settings::types::HardwareProfileGql,
    ) -> Result<crate::settings::types::HardwareProfileSettings> {
        use weaver_server_core::runtime::HardwareProfile;
        use weaver_server_core::settings::HARDWARE_PROFILE_SETTING;

        let config = ctx.data::<SharedConfig>()?;
        let db = ctx.data::<Database>()?;
        let handle = ctx.data::<SchedulerHandle>()?;
        let system = ctx.data::<crate::context::SystemRuntimeContext>()?;
        let _mutation_guard = SETTINGS_MUTATION_GUARD.lock().await;

        let detected = system
            .profile
            .read()
            .map_err(|_| async_graphql::Error::new("system profile unavailable"))?
            .clone();
        let chosen: HardwareProfile = profile.into();
        if let Some(requirement) = chosen.unmet_requirement(&detected) {
            return Err(async_graphql::Error::new(requirement));
        }

        let persist = {
            let db = db.clone();
            let stored = chosen.as_str();
            async move {
                spawn_blocking_db(
                    "settings.mutation.set_hardware_profile.persist",
                    move || -> std::result::Result<(), weaver_server_core::StateError> {
                        db.set_setting(HARDWARE_PROFILE_SETTING, stored)
                    },
                )
                .await
            }
        };
        persist_then_update_config(
            config,
            "settings.mutation.set_hardware_profile",
            persist,
            |cfg| {
                cfg.hardware_profile = Some(chosen);
            },
        )
        .await?;

        // Every limit the profile decides, thread pools included, applies to
        // the next download, decode and extraction that starts; work already
        // running finishes under the limits it started with. A schedule rule
        // with a profile in force keeps it until the schedule lets go.
        handle.set_hardware_profile(chosen).await?;

        Ok(
            crate::settings::types::HardwareProfileSettings::resolve(Some(chosen), &detected)
                .with_in_force(handle.hardware_profile_in_force()),
        )
    }

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
        let propagation_delay_secs = input.propagation_delay_secs;
        let enable_srrdb_lookup = input.enable_srrdb_lookup;
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
        let should_update_paths =
            !normalized_intermediate_dir.is_undefined() || !normalized_complete_dir.is_undefined();

        let persist_input = (
            normalized_intermediate_dir.clone(),
            normalized_complete_dir.clone(),
            cleanup_after_extract,
            max_download_speed,
            max_retries,
            isp_bandwidth_cap.clone(),
            watch_folder_update.clone(),
            duplicate_policy_update.clone(),
            enable_srrdb_lookup,
            propagation_delay_secs,
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
                        if let Some(ref watch) = persist_input.6 {
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
                        if let Some(ref duplicate_policy) = persist_input.7 {
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
                        if let Some(seconds) = persist_input.9 {
                            db.set_setting("propagation_delay_secs", &seconds.to_string())?;
                        }
                        if let Some(enabled) = persist_input.8 {
                            db.set_setting(
                                "delivery_naming.enable_srrdb_lookup",
                                &enabled.to_string(),
                            )?;
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
                if let Some(seconds) = propagation_delay_secs {
                    cfg.propagation_delay_secs = Some(seconds);
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
                if let Some(enabled) = enable_srrdb_lookup {
                    cfg.delivery_naming
                        .get_or_insert_with(Default::default)
                        .enable_srrdb_lookup = Some(enabled);
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
                    propagation_delay_secs: cfg.propagation_delay_secs(),
                    max_retries: cfg.retry.as_ref().and_then(|r| r.max_retries).unwrap_or(3),
                    enable_srrdb_lookup: cfg.enable_srrdb_lookup(),
                    isp_bandwidth_cap: cfg.isp_bandwidth_cap.as_ref().map(Into::into),
                    watch_folder: (&cfg.watch_folder).into(),
                    duplicate_policy: cfg.duplicate_policy.into(),
                };
                (settings, runtime_paths)
            },
        )
        .await?;

        if let Some(seconds) = propagation_delay_secs {
            handle.set_propagation_delay(seconds).await?;
        }
        // Apply speed limit immediately.
        if let Some(speed) = max_download_speed {
            let _ = handle.set_speed_limit(speed).await;
        }
        if let Some(cap) = input.isp_bandwidth_cap {
            let _ = handle.set_bandwidth_cap_policy(Some(cap.into())).await;
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

    /// Hold first-run setup open until the wizard finishes it.
    #[graphql(guard = "AdminGuard")]
    async fn begin_first_run_setup(
        &self,
        ctx: &Context<'_>,
    ) -> Result<crate::settings::first_run::FirstRunSetup> {
        let db = ctx.data::<Database>()?;
        let config = ctx.data::<SharedConfig>()?;
        crate::settings::first_run::begin(db, config).await
    }

    /// End first-run setup, finished or skipped, so it never shows again.
    #[graphql(guard = "AdminGuard")]
    async fn finish_first_run_setup(
        &self,
        ctx: &Context<'_>,
    ) -> Result<crate::settings::first_run::FirstRunSetup> {
        let db = ctx.data::<Database>()?;
        crate::settings::first_run::finish(db).await
    }

    /// Record that the access-model notice was read, so it never shows again.
    #[graphql(guard = "AdminGuard")]
    async fn dismiss_security_upgrade_notice(
        &self,
        ctx: &Context<'_>,
    ) -> Result<crate::settings::security_upgrade_notice::SecurityUpgradeNotice> {
        let db = ctx.data::<Database>()?;
        let auth_cache = ctx.data::<crate::auth::LoginAuthCache>()?;
        crate::settings::security_upgrade_notice::dismiss(db, auth_cache).await
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
        validate_schedule_input(ctx, &input).await?;
        let entry = input.into_entry().map_err(async_graphql::Error::new)?;
        let mut schedules_guard = schedules_state.write().await;
        let mut entries = tokio::task::spawn_blocking({
            let db = db.clone();
            move || db.list_schedules()
        })
        .await??;
        entries.push(entry);
        let entries_for_save = entries.clone();
        let entries = tokio::task::spawn_blocking(move || {
            db.save_schedules(&entries_for_save)?;
            db.list_schedules()
        })
        .await??;
        *schedules_guard = entries.clone();
        drop(schedules_guard);
        schedule_response(ctx, entries).await
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
        validate_schedule_input(ctx, &input).await?;
        let mut schedules_guard = schedules_state.write().await;
        let mut entries = tokio::task::spawn_blocking({
            let db = db.clone();
            move || db.list_schedules()
        })
        .await??;
        if let Some(existing) = entries.iter_mut().find(|e| e.id == id) {
            let mut updated = input.into_entry().map_err(async_graphql::Error::new)?;
            updated.id = existing.id.clone();
            *existing = updated;
        }
        let entries_for_save = entries.clone();
        let entries = tokio::task::spawn_blocking(move || {
            db.save_schedules(&entries_for_save)?;
            db.list_schedules()
        })
        .await??;
        *schedules_guard = entries.clone();
        drop(schedules_guard);
        schedule_response(ctx, entries).await
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
        let mut schedules_guard = schedules_state.write().await;
        let mut entries = tokio::task::spawn_blocking({
            let db = db.clone();
            move || db.list_schedules()
        })
        .await??;
        entries.retain(|e| e.id != id);
        let entries_for_save = entries.clone();
        let entries = tokio::task::spawn_blocking(move || {
            db.save_schedules(&entries_for_save)?;
            db.list_schedules()
        })
        .await??;
        *schedules_guard = entries.clone();
        drop(schedules_guard);
        schedule_response(ctx, entries).await
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
        let mut schedules_guard = schedules_state.write().await;
        let mut entries = tokio::task::spawn_blocking({
            let db = db.clone();
            move || db.list_schedules()
        })
        .await??;
        if let Some(existing) = entries.iter_mut().find(|e| e.id == id) {
            existing.enabled = enabled;
        }
        let entries_for_save = entries.clone();
        let entries = tokio::task::spawn_blocking(move || {
            db.save_schedules(&entries_for_save)?;
            db.list_schedules()
        })
        .await??;
        *schedules_guard = entries.clone();
        drop(schedules_guard);
        schedule_response(ctx, entries).await
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

    use tokio::sync::{RwLock, oneshot};

    use crate::observability::{persist_then_update_config, with_timed_config_read};

    use super::*;

    fn test_config() -> SharedConfig {
        Arc::new(RwLock::new(weaver_server_core::settings::Config {
            data_dir: "/tmp/weaver".to_string(),
            hardware_profile: None,
            intermediate_dir: None,
            complete_dir: None,
            buffer_pool: None,
            servers: vec![],
            categories: vec![],
            retry: None,
            max_download_speed: None,
            cleanup_after_extract: None,
            isp_bandwidth_cap: None,
            propagation_delay_secs: None,
            watch_folder: weaver_server_core::watch_folder::WatchFolderConfig::default(),
            duplicate_policy: weaver_server_core::jobs::DuplicatePolicy::default(),
            direct_store: None,
            direct_unpack: None,
            delivery_naming: None,
            metrics: Default::default(),
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

        let read_result =
            with_timed_config_read(&config, "tests.settings.read", |cfg| cfg.max_download_speed)
                .await;
        assert_eq!(read_result, None);

        release_tx
            .send(())
            .expect("update task should still be waiting");
        update_task
            .await
            .expect("update task should finish cleanly");
    }
}
