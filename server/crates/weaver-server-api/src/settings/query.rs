use super::*;

#[derive(Default)]
pub(crate) struct SettingsQuery;

#[Object]
impl SettingsQuery {
    /// Get general settings.
    #[graphql(guard = "AdminGuard")]
    async fn settings(&self, ctx: &Context<'_>) -> Result<GeneralSettings> {
        let config = ctx.data::<SharedConfig>()?;
        let cfg = config.read().await;
        Ok(GeneralSettings {
            data_dir: cfg.data_dir.clone(),
            intermediate_dir: cfg.intermediate_dir(),
            complete_dir: cfg.complete_dir(),
            cleanup_after_extract: cfg.cleanup_after_extract(),
            max_download_speed: cfg.max_download_speed.unwrap_or(0),
            max_retries: cfg.retry.as_ref().and_then(|r| r.max_retries).unwrap_or(3),
            isp_bandwidth_cap: cfg.isp_bandwidth_cap.as_ref().map(Into::into),
        })
    }
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
