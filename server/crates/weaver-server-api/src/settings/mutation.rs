use std::path::PathBuf;
use std::sync::LazyLock;

use async_graphql::{Context, MaybeUndefined, Object, Result};

use crate::auth::AdminGuard;
use crate::observability::{persist_then_update_config, spawn_blocking_db};
use crate::settings::types::{GeneralSettings, GeneralSettingsInput};
use weaver_server_core::settings::SharedConfig;
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
        let isp_bandwidth_cap = input.isp_bandwidth_cap.clone();
        let should_update_paths =
            !normalized_intermediate_dir.is_undefined() || !normalized_complete_dir.is_undefined();

        let persist_input = (
            normalized_intermediate_dir.clone(),
            normalized_complete_dir.clone(),
            cleanup_after_extract,
            max_download_speed,
            max_retries,
            isp_bandwidth_cap.clone(),
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
                    isp_bandwidth_cap: cfg.isp_bandwidth_cap.as_ref().map(Into::into),
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

        // Apply directory changes immediately so new jobs use them without restart.
        if let Some((data_dir, intermediate_dir, complete_dir)) = runtime_paths {
            let _ = handle
                .update_runtime_paths(data_dir, intermediate_dir, complete_dir)
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
            diagnostic_upload_url: None,
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
