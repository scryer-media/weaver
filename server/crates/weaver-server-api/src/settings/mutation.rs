// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::path::PathBuf;
use std::sync::LazyLock;
use weaver_server_core::settings::SettingsArchivePasswordsQueries;
use weaver_server_core::settings::SettingsSchedulesQueries;
use weaver_server_core::settings::persistence::SettingsPersistenceQueries;
use weaver_server_core::settings::repository::SettingsRepositoryQueries;
use weaver_server_core::settings::service::SettingsServiceQueries;

use async_graphql::{Context, MaybeUndefined, Object, Result};

use crate::auth::{AdminGuard, graphql_error, invalid_input, not_found};
use crate::observability::{
    persist_then_update_config, spawn_blocking_db, spawn_blocking_with, state_error,
    watch_folder_error,
};
use crate::settings::types::{
    DuplicatePolicySettingsInput, GeneralSettings, GeneralSettingsInput, WatchFolderScanReport,
    WatchFolderSettingsInput,
};
use weaver_server_core::settings::SharedConfig;
use weaver_server_core::settings::tracker_file;
use weaver_server_core::watch_folder::{WatchFolderConfig, WatchFolderMode, WatchFolderService};
use weaver_server_core::{Database, SchedulerHandle};

// Refuse a schedule rule this machine could never apply, judged against the
// live probe as the hardware-profile choice is.
async fn validate_schedule_input(
    ctx: &Context<'_>,
    input: &crate::settings::types::ScheduleInput,
) -> Result<()> {
    let system = ctx.data::<crate::context::SystemRuntimeContext>()?;
    let probe = system
        .profile
        .read()
        .map_err(|_| graphql_error("INTERNAL", "system profile unavailable"))?
        .clone();
    input.validate(&probe).map_err(invalid_input)?;
    Ok(())
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
    #[graphql(guard = "AdminGuard")]
    async fn update_web_appearance(
        &self,
        ctx: &Context<'_>,
        input: crate::settings::appearance::WebAppearanceInput,
    ) -> Result<crate::settings::appearance::WebAppearance> {
        let store = ctx
            .data::<crate::settings::appearance::AppearanceStore>()?
            .clone();
        let db = ctx.data::<Database>()?.clone();
        spawn_blocking_db("settings.mutation.update_web_appearance", move || {
            store.save(&db, input)
        })
        .await
    }

    #[graphql(guard = "AdminGuard")]
    async fn update_archive_password_settings(
        &self,
        ctx: &Context<'_>,
        passwords: Option<Vec<String>>,
        password_file: MaybeUndefined<String>,
    ) -> Result<crate::settings::types::ArchivePasswordSettings> {
        let db = ctx.data::<Database>()?.clone();
        let password_file = match password_file {
            MaybeUndefined::Undefined => None,
            MaybeUndefined::Null => Some(None),
            MaybeUndefined::Value(file) => Some(Some(file)),
        };
        let (has_passwords, password_file) = spawn_blocking_with(
            move || {
                db.save_archive_password_settings(passwords, password_file)?;
                db.archive_password_settings()
            },
            state_error,
        )
        .await?;
        Ok(crate::settings::types::ArchivePasswordSettings {
            has_passwords,
            password_file,
        })
    }

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
            .map_err(|_| graphql_error("INTERNAL", "system profile unavailable"))?
            .clone();
        let chosen: HardwareProfile = profile.into();
        if let Some(requirement) = chosen.unmet_requirement(&detected) {
            return Err(invalid_input(requirement));
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
        handle
            .set_hardware_profile(chosen)
            .await
            .map_err(crate::observability::scheduler_error)?;

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
        let delete_failed_job_files = input.delete_failed_job_files;
        let history_retention_days = input
            .history_retention_days
            .map(|days| {
                u32::try_from(days)
                    .ok()
                    .filter(|days| {
                        *days <= weaver_server_core::settings::MAX_HISTORY_RETENTION_DAYS
                    })
                    .ok_or_else(|| {
                        crate::auth::graphql_error(
                            "INVALID_INPUT",
                            format!(
                                "historyRetentionDays must be between 0 and {}",
                                weaver_server_core::settings::MAX_HISTORY_RETENTION_DAYS
                            ),
                        )
                    })
            })
            .transpose()?;
        let max_retries = input.max_retries;
        let propagation_delay_secs = input.propagation_delay_secs;
        let max_concurrent_nzbs = input
            .max_concurrent_nzbs
            .and_then(std::num::NonZeroU32::new);
        let enable_srrdb_lookup = input.enable_srrdb_lookup;
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
            candidate.validate().map_err(invalid_input)?;
        }
        let seeding_update = match input.seeding {
            MaybeUndefined::Undefined => None,
            MaybeUndefined::Null => Some(None),
            // At the global level nothing is inherited.
            MaybeUndefined::Value(policy) => Some(Some(policy.into_policy()?.at_global())),
        };
        let upload_cap_update = match input.upload_cap {
            MaybeUndefined::Undefined => None,
            MaybeUndefined::Null => Some(None),
            MaybeUndefined::Value(cap) => Some(Some(cap)),
        };
        // Turning torrents off, or choosing again how a wind-down ends: how
        // the torrents still running end, `None` for at once.
        let mut wind_down = None;
        let torrent_update = match &input.torrent {
            Some(update) => {
                // Torrents this run of the server does not have cannot be
                // turned on; the rest of their settings still save.
                if update.enabled == Some(true) {
                    ctx.data::<weaver_server_core::torrents::TorrentAvailability>()?
                        .may_turn_on()
                        .map_err(|refusal| {
                            crate::auth::graphql_error("TORRENTS_UNAVAILABLE", refusal)
                        })?;
                }
                let mut torrent = config.read().await.torrent.clone();
                if update.enabled == Some(false)
                    && (torrent.enabled || update.when_disabled.is_some())
                {
                    wind_down = Some(update.when_disabled.unwrap_or_default().wind_down());
                }
                update.apply(&mut torrent);
                let torrent = torrent
                    .checked()
                    .map_err(|errors| invalid_input(errors.join("; ")))?;
                Some(torrent)
            }
            None => None,
        };
        let should_reconcile_watch_folder = watch_folder_update.is_some();
        let should_update_paths =
            !normalized_intermediate_dir.is_undefined() || !normalized_complete_dir.is_undefined();

        let persist_input = (
            normalized_intermediate_dir.clone(),
            normalized_complete_dir.clone(),
            cleanup_after_extract,
            max_retries,
            watch_folder_update.clone(),
            duplicate_policy_update.clone(),
            enable_srrdb_lookup,
            propagation_delay_secs,
            seeding_update,
            torrent_update.clone(),
            max_concurrent_nzbs,
            upload_cap_update,
            delete_failed_job_files,
            history_retention_days,
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
                            db.set_setting("retry.max_retries", &v.to_string())?;
                        }
                        if let Some(ref watch) = persist_input.4 {
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
                        if let Some(ref duplicate_policy) = persist_input.5 {
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
                        if let Some(limit) = persist_input.10 {
                            db.set_setting("max_concurrent_nzbs", &limit.to_string())?;
                        }
                        if let Some(seconds) = persist_input.7 {
                            db.set_setting("propagation_delay_secs", &seconds.to_string())?;
                        }
                        if let Some(enabled) = persist_input.6 {
                            db.set_setting(
                                "delivery_naming.enable_srrdb_lookup",
                                &enabled.to_string(),
                            )?;
                        }
                        if let Some(policy) = persist_input.8 {
                            db.save_seeding_policy(policy.as_ref())?;
                        }
                        if let Some(cap) = persist_input.11 {
                            db.save_upload_cap(cap)?;
                        }
                        if let Some(ref torrent) = persist_input.9 {
                            db.save_torrent_config(torrent)?;
                        }
                        if let Some(delete) = persist_input.12 {
                            db.set_setting("delete_failed_job_files", &delete.to_string())?;
                        }
                        if let Some(days) = persist_input.13 {
                            db.set_setting("history_retention_days", &days.to_string())?;
                        }
                        Ok(())
                    },
                )
                .await
            }
        };

        // How the torrents still running end is taken before the settings
        // that turn torrents off are saved: any caller that hands the saved
        // settings to the torrents before this mutation does finds it.
        let torrents = ctx.data_opt::<weaver_server_core::torrents::service::TorrentService>();
        let wind_down_was = match (wind_down, torrents) {
            (Some(wind_down), Some(torrents)) => Some(torrents.wind_down_when_off(wind_down).await),
            _ => None,
        };
        let saved = persist_then_update_config(
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
                if let Some(delete) = delete_failed_job_files {
                    cfg.delete_failed_job_files = Some(delete);
                }
                if let Some(days) = history_retention_days {
                    cfg.history_retention_days = Some(days);
                }
                if let Some(limit) = max_concurrent_nzbs {
                    cfg.max_concurrent_nzbs = Some(limit);
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
                if let Some(policy) = seeding_update {
                    cfg.seeding = policy;
                }
                if let Some(cap) = upload_cap_update {
                    cfg.upload_cap = cap;
                }
                if let Some(ref torrent) = torrent_update {
                    cfg.torrent = torrent.clone();
                }

                let runtime_paths = should_update_paths.then(|| {
                    (
                        PathBuf::from(&cfg.data_dir),
                        PathBuf::from(cfg.intermediate_dir()),
                        PathBuf::from(cfg.complete_dir()),
                    )
                });
                let settings = GeneralSettings::from(&*cfg);
                (settings, runtime_paths)
            },
        )
        .await;
        let (settings, runtime_paths) = match saved {
            Ok(saved) => saved,
            Err(error) => {
                if let (Some(was), Some(torrents)) = (wind_down_was, torrents) {
                    torrents.keep_wind_down_after_failed_save(was).await;
                }
                return Err(error);
            }
        };

        if let Some(seconds) = propagation_delay_secs {
            handle
                .set_propagation_delay(seconds)
                .await
                .map_err(crate::observability::scheduler_error)?;
        }
        if let Some(limit) = max_concurrent_nzbs {
            handle
                .set_max_concurrent_nzbs(limit)
                .await
                .map_err(crate::observability::scheduler_error)?;
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
                .map_err(watch_folder_error)?;
        }
        crate::torrents::reconcile(ctx).await?;

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
        let report = watch_folder.scan_now().await.map_err(watch_folder_error)?;
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
        let entry = input.into_entry().map_err(invalid_input)?;
        let mut schedules_guard = schedules_state.write().await;
        let mut entries = spawn_blocking_with(
            {
                let db = db.clone();
                move || db.list_schedules()
            },
            state_error,
        )
        .await?;
        entries.push(entry);
        let entries_for_save = entries.clone();
        let entries = spawn_blocking_with(
            move || {
                db.save_schedules(&entries_for_save)?;
                db.list_schedules()
            },
            state_error,
        )
        .await?;
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
        let mut entries = spawn_blocking_with(
            {
                let db = db.clone();
                move || db.list_schedules()
            },
            state_error,
        )
        .await?;
        if let Some(existing) = entries.iter_mut().find(|e| e.id == id) {
            let mut updated = input.into_entry().map_err(invalid_input)?;
            updated.id = existing.id.clone();
            *existing = updated;
        }
        let entries_for_save = entries.clone();
        let entries = spawn_blocking_with(
            move || {
                db.save_schedules(&entries_for_save)?;
                db.list_schedules()
            },
            state_error,
        )
        .await?;
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
        let mut entries = spawn_blocking_with(
            {
                let db = db.clone();
                move || db.list_schedules()
            },
            state_error,
        )
        .await?;
        entries.retain(|e| e.id != id);
        let entries_for_save = entries.clone();
        let entries = spawn_blocking_with(
            move || {
                db.save_schedules(&entries_for_save)?;
                db.list_schedules()
            },
            state_error,
        )
        .await?;
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
        let mut entries = spawn_blocking_with(
            {
                let db = db.clone();
                move || db.list_schedules()
            },
            state_error,
        )
        .await?;
        if let Some(existing) = entries.iter_mut().find(|e| e.id == id) {
            existing.enabled = enabled;
        }
        let entries_for_save = entries.clone();
        let entries = spawn_blocking_with(
            move || {
                db.save_schedules(&entries_for_save)?;
                db.list_schedules()
            },
            state_error,
        )
        .await?;
        *schedules_guard = entries.clone();
        drop(schedules_guard);
        schedule_response(ctx, entries).await
    }

    /// Add a tracker rule, or replace the one `id` names. Every torrent's
    /// trackers are matched against the rules again.
    #[graphql(guard = "AdminGuard")]
    async fn save_tracker_rule(
        &self,
        ctx: &Context<'_>,
        id: Option<u32>,
        input: crate::settings::types::TrackerRuleInput,
    ) -> Result<Vec<crate::settings::types::TrackerRule>> {
        // An input naming no certificate keeps the rule's.
        let keeps_ssl = input.ssl.is_undefined();
        // Archive passwords come only from a tracker file; the editor may
        // clear them, never set them.
        let keeps_passwords = !input.clear_archive_passwords;
        let rule = input.into_rule(id.unwrap_or(0))?;
        let db = ctx.data::<Database>()?.clone();
        let _guard = SETTINGS_MUTATION_GUARD.lock().await;
        // The rule's transports, resolved over the global ones, leave one
        // on; its preferences are kept as set.
        let global = ctx.data::<SharedConfig>()?.read().await.torrent.transports;
        rule.transports.checked(global).map_err(invalid_input)?;
        let (routed, rules) = spawn_blocking_with(
            move || {
                let routed = routed_rules(&db.list_tracker_rules()?);
                let now = chrono::Utc::now().timestamp_millis();
                let rules = db.save_tracker_rule(id, rule, keeps_ssl, keeps_passwords, now)?;
                Ok::<_, weaver_server_core::StateError>(rules.map(|rules| (routed, rules)))
            },
            state_error,
        )
        .await?
        .map_err(tracker_rule_refusal)?;
        tracker_rules_saved(ctx, &routed, &rules).await?;
        Ok(rules.into_iter().map(Into::into).collect())
    }

    /// Delete a tracker rule. The torrents it matched follow the rule their
    /// trackers match next, or none.
    #[graphql(guard = "AdminGuard")]
    async fn delete_tracker_rule(
        &self,
        ctx: &Context<'_>,
        id: u32,
    ) -> Result<Vec<crate::settings::types::TrackerRule>> {
        let db = ctx.data::<Database>()?.clone();
        let _guard = SETTINGS_MUTATION_GUARD.lock().await;
        let (routed, rules) = spawn_blocking_with(
            move || {
                let routed = routed_rules(&db.list_tracker_rules()?);
                db.delete_tracker_rule(id)?;
                Ok::<_, weaver_server_core::StateError>((routed, db.list_tracker_rules()?))
            },
            state_error,
        )
        .await?;
        tracker_rules_saved(ctx, &routed, &rules).await?;
        Ok(rules.into_iter().map(Into::into).collect())
    }

    /// What importing a tracker file, base64, would do. Nothing is kept.
    #[graphql(guard = "AdminGuard")]
    async fn preview_tracker_file(
        &self,
        ctx: &Context<'_>,
        file: String,
        password: Option<String>,
    ) -> Result<crate::settings::types::TrackerFilePreview> {
        let db = ctx.data::<Database>()?.clone();
        let bytes = decode_tracker_file(&file)?;
        let preview = spawn_blocking_with(
            move || {
                let read = tracker_file::read(&bytes, password.as_deref());
                let file = match read {
                    Ok(file) => file,
                    Err(refusal) => return Ok(Err(refusal)),
                };
                let rules = db.list_tracker_rules()?;
                let rule = match file.rule_in(&rules) {
                    Ok(rule) => rule,
                    Err(refusal) => return Ok(Err(refusal)),
                };
                let fingerprint = tracker_file::fingerprint(&bytes, rule);
                Ok::<_, weaver_server_core::StateError>(Ok(file.preview(rule, fingerprint)))
            },
            state_error,
        )
        .await?
        .map_err(|refusal| graphql_error("CONFLICT", refusal))?;
        Ok(preview.into())
    }

    /// Import a tracker file, base64, as its preview showed it, and return
    /// every rule. A file or rule changed since the preview is refused.
    #[graphql(guard = "AdminGuard")]
    async fn apply_tracker_file(
        &self,
        ctx: &Context<'_>,
        file: String,
        password: Option<String>,
        fingerprint: String,
    ) -> Result<Vec<crate::settings::types::TrackerRule>> {
        let db = ctx.data::<Database>()?.clone();
        let bytes = decode_tracker_file(&file)?;
        let _guard = SETTINGS_MUTATION_GUARD.lock().await;
        let rules = spawn_blocking_with(
            move || {
                let file = match tracker_file::read(&bytes, password.as_deref()) {
                    Ok(file) => file,
                    Err(refusal) => return Ok(Err(refusal)),
                };
                let rules = db.list_tracker_rules()?;
                let rule = match file.rule_in(&rules) {
                    Ok(rule) => rule,
                    Err(refusal) => return Ok(Err(refusal)),
                };
                if tracker_file::fingerprint(&bytes, rule) != fingerprint {
                    return Ok(Err("stale preview".to_owned()));
                }
                let routed = routed_rules(&rules);
                let now = chrono::Utc::now().timestamp_millis();
                match db.apply_tracker_file(&file, now) {
                    Err(weaver_server_core::StateError::Conflict(refusal)) => {
                        return Ok(Err(refusal));
                    }
                    applied => applied?,
                };
                Ok::<_, weaver_server_core::StateError>(Ok((routed, db.list_tracker_rules()?)))
            },
            state_error,
        )
        .await?
        .map_err(|refusal| graphql_error("CONFLICT", refusal))?;
        let (routed, rules) = rules;
        tracker_rules_saved(ctx, &routed, &rules).await?;
        Ok(rules.into_iter().map(Into::into).collect())
    }

    /// Release a rule from the tracker file that filled it, keeping every
    /// value it holds, and return every rule.
    #[graphql(guard = "AdminGuard")]
    async fn unbind_tracker_rule(
        &self,
        ctx: &Context<'_>,
        id: u32,
    ) -> Result<Vec<crate::settings::types::TrackerRule>> {
        let db = ctx.data::<Database>()?.clone();
        let _guard = SETTINGS_MUTATION_GUARD.lock().await;
        let (routed, rules) = spawn_blocking_with(
            move || {
                let routed = routed_rules(&db.list_tracker_rules()?);
                let now = chrono::Utc::now().timestamp_millis();
                if !db.unbind_tracker_rule(id, now)? {
                    return Ok(Err(format!("no tracker rule {id}")));
                }
                Ok::<_, weaver_server_core::StateError>(Ok((routed, db.list_tracker_rules()?)))
            },
            state_error,
        )
        .await?
        .map_err(not_found)?;
        tracker_rules_saved(ctx, &routed, &rules).await?;
        Ok(rules.into_iter().map(Into::into).collect())
    }
}

// A tracker rule not saved, worded as it stands: a locked field or a host
// another rule covers is a conflict, a name in use is taken, and an id of no
// rule is not found.
fn tracker_rule_refusal(
    refusal: weaver_server_core::settings::repository::TrackerRuleRefusal,
) -> async_graphql::Error {
    use weaver_server_core::settings::repository::TrackerRuleRefusal;
    let code = match &refusal {
        TrackerRuleRefusal::Locked(_) | TrackerRuleRefusal::Covered { .. } => "CONFLICT",
        TrackerRuleRefusal::NameTaken(_) => "NAME_TAKEN",
        TrackerRuleRefusal::NotFound(_) => "NOT_FOUND",
    };
    graphql_error(code, refusal.to_string())
}

// The tracker rules that route their torrents themselves: each is a route
// consumer of its own (`torrent_route`), by id.
fn routed_rules(rules: &[weaver_server_core::settings::model::TrackerRule]) -> Vec<u32> {
    rules
        .iter()
        .filter(|rule| rule.torrent_route)
        .map(|rule| rule.id)
        .collect()
}

// After the tracker rules are saved: the routes reload only when the rules
// that route their torrents themselves changed (`routed`, read before the
// save), as no other rule field is read by the routes; then the torrents
// take the rules as saved, whether or not the routes reloaded.
async fn tracker_rules_saved(
    ctx: &Context<'_>,
    routed: &[u32],
    rules: &[weaver_server_core::settings::model::TrackerRule],
) -> Result<()> {
    let routes = if routed_rules(rules) == routed {
        Ok(())
    } else {
        crate::proxies::refresh(ctx).await
    };
    let reloaded = crate::torrents::reload_tracker_rules(ctx).await;
    routes.and(reloaded)
}

// The bytes of an uploaded tracker file, refused past the largest a tracker
// file may be before any of it is decoded.
fn decode_tracker_file(file: &str) -> Result<Vec<u8>> {
    use base64::Engine as _;
    let limit = tracker_file::MAX_UPLOAD_BYTES;
    if file.len() > limit.div_ceil(3) * 4 {
        return Err(graphql_error(
            "UPLOAD_TOO_LARGE",
            format!("The tracker file is larger than {} KiB.", limit / 1024),
        ));
    }
    base64::engine::general_purpose::STANDARD
        .decode(file.trim())
        .map_err(|_| invalid_input("The tracker file is not base64."))
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
            WatchFolderMode::parse(value)
                .ok_or_else(|| invalid_input("watch_folder.mode must be off, polling, or realtime"))
        })
        .transpose()?;
    if input.poll_interval_secs == Some(0) {
        return Err(invalid_input(
            "watch_folder.poll_interval_secs must be greater than 0",
        ));
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

    // A lock or an overlap is a conflict, a taken name is taken, and an
    // unknown id is not found. Each keeps its sentence.
    #[test]
    fn each_tracker_rule_refusal_carries_the_code_of_its_kind() {
        use weaver_server_core::settings::repository::TrackerRuleRefusal;
        let cases = [
            (TrackerRuleRefusal::Locked("locked".into()), "CONFLICT"),
            (TrackerRuleRefusal::NameTaken("Linux".into()), "NAME_TAKEN"),
            (
                TrackerRuleRefusal::Covered {
                    pattern: "*.example.org".into(),
                    by: "Linux".into(),
                },
                "CONFLICT",
            ),
            (TrackerRuleRefusal::NotFound(7), "NOT_FOUND"),
        ];
        for (refusal, code) in cases {
            let message = refusal.to_string();
            let refused = tracker_rule_refusal(refusal);
            assert_eq!(refused.message, message);
            assert_eq!(
                refused.extensions.as_ref().and_then(|ext| ext.get("code")),
                Some(&async_graphql::Value::from(code))
            );
        }
    }

    fn test_config() -> SharedConfig {
        Arc::new(RwLock::new(weaver_server_core::settings::Config {
            seeding: None,
            upload_cap: None,
            torrent: Default::default(),
            data_dir: "/tmp/weaver".to_string(),
            hardware_profile: None,
            intermediate_dir: None,
            complete_dir: None,
            servers: vec![],
            categories: vec![],
            retry: None,
            bandwidth: Default::default(),
            cleanup_after_extract: None,
            delete_failed_job_files: None,
            history_retention_days: None,
            propagation_delay_secs: None,
            max_concurrent_nzbs: None,
            watch_folder: weaver_server_core::watch_folder::WatchFolderConfig::default(),
            duplicate_policy: weaver_server_core::jobs::DuplicatePolicy::default(),
            direct_store: None,
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
                        cfg.propagation_delay_secs = Some(42);
                    },
                )
                .await
                .expect("settings update should succeed");
            }
        });

        tokio::task::yield_now().await;

        let read_result = with_timed_config_read(&config, "tests.settings.read", |cfg| {
            cfg.propagation_delay_secs
        })
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
