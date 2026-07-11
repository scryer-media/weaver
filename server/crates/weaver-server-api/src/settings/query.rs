use super::*;
use crate::observability::with_timed_config_read;

#[derive(Default)]
pub(crate) struct SettingsQuery;

#[Object]
impl SettingsQuery {
    /// Get general settings.
    #[graphql(guard = "AdminGuard")]
    async fn settings(&self, ctx: &Context<'_>) -> Result<GeneralSettings> {
        let config = ctx.data::<SharedConfig>()?;
        Ok(
            with_timed_config_read(config, "settings.query.settings", |cfg| GeneralSettings {
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
            })
            .await,
        )
    }
    #[graphql(guard = "AdminGuard")]
    async fn schedules(&self, ctx: &Context<'_>) -> Result<Vec<crate::settings::types::Schedule>> {
        let db = ctx.data::<Database>()?.clone();
        let entries: Vec<weaver_server_core::bandwidth::ScheduleEntry> =
            tokio::task::spawn_blocking(move || db.list_schedules())
                .await
                .map_err(|e| async_graphql::Error::new(e.to_string()))?
                .map_err(|e| async_graphql::Error::new(e.to_string()))?;
        Ok(entries
            .into_iter()
            .map(crate::settings::types::Schedule::from)
            .collect())
    }
}
