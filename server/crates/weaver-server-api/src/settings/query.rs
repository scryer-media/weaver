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
                propagation_delay_secs: cfg.propagation_delay_secs(),
                max_retries: cfg.retry.as_ref().and_then(|r| r.max_retries).unwrap_or(3),
                ip_replacement_trial_extra_connections: cfg
                    .ip_replacement_trial_extra_connections(),
                enable_srrdb_lookup: cfg.enable_srrdb_lookup(),
                isp_bandwidth_cap: cfg.isp_bandwidth_cap.as_ref().map(Into::into),
                watch_folder: (&cfg.watch_folder).into(),
                duplicate_policy: cfg.duplicate_policy.into(),
            })
            .await,
        )
    }
    /// The hardware profile in force, and the profiles this machine can
    /// honour. Judged against the live probe, so a container that was given
    /// more memory since startup is offered what it has now.
    #[graphql(guard = "AdminGuard")]
    async fn hardware_profile(
        &self,
        ctx: &Context<'_>,
    ) -> Result<crate::settings::types::HardwareProfileSettings> {
        use weaver_server_core::runtime::HardwareProfile;

        let config = ctx.data::<SharedConfig>()?;
        let system = ctx.data::<crate::context::SystemRuntimeContext>()?;
        let profile = system
            .profile
            .read()
            .map_err(|_| async_graphql::Error::new("system profile unavailable"))?
            .clone();

        let selected = with_timed_config_read(config, "settings.query.hardwareProfile", |cfg| {
            cfg.hardware_profile
        })
        .await;

        Ok(crate::settings::types::HardwareProfileSettings {
            selected: selected.map(Into::into),
            recommended: HardwareProfile::recommended(&profile).into(),
            available: HardwareProfile::available(&profile)
                .into_iter()
                .map(Into::into)
                .collect(),
            detected: crate::settings::types::DetectedHardware {
                memory_bytes: HardwareProfile::effective_memory_bytes(&profile),
                cores: HardwareProfile::effective_cores(&profile) as u32,
            },
        })
    }

    /// Whether first-run setup is still owed to this install.
    #[graphql(guard = "AdminGuard")]
    async fn first_run_setup(
        &self,
        ctx: &Context<'_>,
    ) -> Result<crate::settings::first_run::FirstRunSetup> {
        let db = ctx.data::<Database>()?;
        let config = ctx.data::<SharedConfig>()?;
        crate::settings::first_run::status(db, config).await
    }

    /// Whether this install is owed the notice about moving to the 0.12.0
    /// access model, and what it needs to say where.
    #[graphql(guard = "AdminGuard")]
    async fn security_upgrade_notice(
        &self,
        ctx: &Context<'_>,
    ) -> Result<crate::settings::security_upgrade_notice::SecurityUpgradeNotice> {
        let db = ctx.data::<Database>()?;
        let security = ctx.data::<weaver_server_core::security::RuntimeSecurityConfig>()?;
        let auth_cache = ctx.data::<crate::auth::LoginAuthCache>()?;
        crate::settings::security_upgrade_notice::status(db, security, auth_cache).await
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
