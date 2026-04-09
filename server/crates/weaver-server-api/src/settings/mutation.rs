use std::path::PathBuf;

use async_graphql::{Context, MaybeUndefined, Object, Result};

use crate::auth::AdminGuard;
use crate::settings::types::{GeneralSettings, GeneralSettingsInput};
use weaver_server_core::settings::SharedConfig;
use weaver_server_core::{Database, SchedulerHandle};

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
        let normalized_intermediate_dir = normalize_settings_path_update(&input.intermediate_dir);
        let normalized_complete_dir = normalize_settings_path_update(&input.complete_dir);

        let settings = {
            let mut cfg = config.write().await;

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
            if let Some(cleanup) = input.cleanup_after_extract {
                cfg.cleanup_after_extract = Some(cleanup);
            }
            if let Some(speed) = input.max_download_speed {
                cfg.max_download_speed = Some(speed);
            }
            if let Some(retries) = input.max_retries {
                let retry = cfg
                    .retry
                    .get_or_insert(weaver_server_core::settings::RetryOverrides {
                        max_retries: None,
                        base_delay_secs: None,
                        multiplier: None,
                    });
                retry.max_retries = Some(retries);
            }
            if let Some(cap) = input.isp_bandwidth_cap.clone() {
                cfg.isp_bandwidth_cap = Some(cap.into());
            }

            // Persist to SQLite.
            let db = db.clone();
            let input_clone = (
                normalized_intermediate_dir.clone(),
                normalized_complete_dir.clone(),
                input.cleanup_after_extract,
                input.max_download_speed,
                input.max_retries,
                input.isp_bandwidth_cap.clone(),
            );
            tokio::task::spawn_blocking(
                move || -> std::result::Result<(), weaver_server_core::StateError> {
                    match &input_clone.0 {
                        MaybeUndefined::Undefined => {}
                        MaybeUndefined::Null => db.delete_setting("intermediate_dir")?,
                        MaybeUndefined::Value(v) => db.set_setting("intermediate_dir", v)?,
                    }
                    match &input_clone.1 {
                        MaybeUndefined::Undefined => {}
                        MaybeUndefined::Null => db.delete_setting("complete_dir")?,
                        MaybeUndefined::Value(v) => db.set_setting("complete_dir", v)?,
                    }
                    if let Some(v) = input_clone.2 {
                        db.set_setting("cleanup_after_extract", &v.to_string())?;
                    }
                    if let Some(v) = input_clone.3 {
                        db.set_setting("max_download_speed", &v.to_string())?;
                    }
                    if let Some(v) = input_clone.4 {
                        db.set_setting("retry.max_retries", &v.to_string())?;
                    }
                    if let Some(ref cap) = input_clone.5 {
                        db.set_setting("bandwidth_cap.enabled", &cap.enabled.to_string())?;
                        db.set_setting(
                            "bandwidth_cap.period",
                            match cap.period {
                                crate::settings::types::IspBandwidthCapPeriodGql::Daily => "daily",
                                crate::settings::types::IspBandwidthCapPeriodGql::Weekly => {
                                    "weekly"
                                }
                                crate::settings::types::IspBandwidthCapPeriodGql::Monthly => {
                                    "monthly"
                                }
                            },
                        )?;
                        db.set_setting("bandwidth_cap.limit_bytes", &cap.limit_bytes.to_string())?;
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
                    Ok(())
                },
            )
            .await
            .map_err(|e| async_graphql::Error::new(format!("{e}")))?
            .map_err(|e| async_graphql::Error::new(format!("db error: {e}")))?;

            GeneralSettings {
                data_dir: cfg.data_dir.clone(),
                intermediate_dir: cfg.intermediate_dir(),
                complete_dir: cfg.complete_dir(),
                cleanup_after_extract: cfg.cleanup_after_extract(),
                max_download_speed: cfg.max_download_speed.unwrap_or(0),
                max_retries: cfg.retry.as_ref().and_then(|r| r.max_retries).unwrap_or(3),
                isp_bandwidth_cap: cfg.isp_bandwidth_cap.as_ref().map(Into::into),
            }
        };

        // Apply speed limit immediately.
        if let Some(speed) = input.max_download_speed {
            let _ = handle.set_speed_limit(speed).await;
        }
        if let Some(cap) = input.isp_bandwidth_cap {
            let _ = handle.set_bandwidth_cap_policy(Some(cap.into())).await;
        }

        // Apply directory changes immediately so new jobs use them without restart.
        if !normalized_intermediate_dir.is_undefined() || !normalized_complete_dir.is_undefined() {
            let cfg = config.read().await;
            let _ = handle
                .update_runtime_paths(
                    PathBuf::from(&cfg.data_dir),
                    PathBuf::from(cfg.intermediate_dir()),
                    PathBuf::from(cfg.complete_dir()),
                )
                .await;
        }

        Ok(settings)
    }
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
