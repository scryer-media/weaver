// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::observability::{spawn_blocking_with, state_error};

use super::*;
use crate::observability::with_timed_config_read;
use weaver_server_core::settings::SettingsArchivePasswordsQueries;
use weaver_server_core::settings::SettingsSchedulesQueries;
use weaver_server_core::settings::repository::SettingsRepositoryQueries;
use weaver_server_core::torrents::queries::TorrentsQueries;

#[derive(Default)]
pub(crate) struct SettingsQuery;

#[Object]
impl SettingsQuery {
    #[graphql(guard = "AdminGuard")]
    async fn web_appearance(
        &self,
        ctx: &Context<'_>,
    ) -> Result<crate::settings::appearance::WebAppearance> {
        Ok(ctx
            .data::<crate::settings::appearance::AppearanceStore>()?
            .current())
    }

    #[graphql(guard = "AdminGuard")]
    async fn archive_password_settings(
        &self,
        ctx: &Context<'_>,
    ) -> Result<crate::settings::types::ArchivePasswordSettings> {
        let db = ctx.data::<Database>()?.clone();
        let (has_passwords, password_file) =
            spawn_blocking_with(move || db.archive_password_settings(), state_error).await?;
        Ok(crate::settings::types::ArchivePasswordSettings {
            has_passwords,
            password_file,
        })
    }

    /// Get general settings.
    #[graphql(guard = "AdminGuard")]
    async fn settings(&self, ctx: &Context<'_>) -> Result<GeneralSettings> {
        let config = ctx.data::<SharedConfig>()?;
        Ok(
            with_timed_config_read(config, "settings.query.settings", |cfg| {
                GeneralSettings::from(cfg)
            })
            .await,
        )
    }
    /// The hardware profile in force, and the profiles this machine can
    /// honour. Judged against the machine as probed at startup, the same
    /// probe the running limits were derived from.
    #[graphql(guard = "AdminGuard")]
    async fn hardware_profile(
        &self,
        ctx: &Context<'_>,
    ) -> Result<crate::settings::types::HardwareProfileSettings> {
        let config = ctx.data::<SharedConfig>()?;
        let handle = ctx.data::<SchedulerHandle>()?;
        let system = ctx.data::<crate::context::SystemRuntimeContext>()?;
        let probe = system
            .profile
            .read()
            .map_err(|_| crate::auth::graphql_error("INTERNAL", "system profile unavailable"))?
            .clone();

        let selected = with_timed_config_read(config, "settings.query.hardwareProfile", |cfg| {
            cfg.hardware_profile
        })
        .await;

        Ok(
            crate::settings::types::HardwareProfileSettings::resolve(selected, &probe)
                .with_in_force(handle.hardware_profile_in_force()),
        )
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
    async fn tracker_rules(
        &self,
        ctx: &Context<'_>,
    ) -> Result<Vec<crate::settings::types::TrackerRule>> {
        let db = ctx.data::<Database>()?.clone();
        let rules = spawn_blocking_with(move || db.list_tracker_rules(), state_error).await?;
        Ok(rules.into_iter().map(Into::into).collect())
    }

    /// Every host a torrent has announced to, which a tracker rule's host is
    /// picked from.
    #[graphql(guard = "AdminGuard")]
    async fn tracker_hosts(&self, ctx: &Context<'_>) -> Result<Vec<String>> {
        let db = ctx.data::<Database>()?.clone();
        spawn_blocking_with(move || db.list_tracker_hosts(), state_error).await
    }

    #[graphql(guard = "AdminGuard")]
    async fn schedules(&self, ctx: &Context<'_>) -> Result<Vec<crate::settings::types::Schedule>> {
        let db = ctx.data::<Database>()?.clone();
        let entries: Vec<weaver_server_core::bandwidth::ScheduleEntry> =
            spawn_blocking_with(move || db.list_schedules(), state_error).await?;
        Ok(entries
            .into_iter()
            .map(crate::settings::types::Schedule::from)
            .collect())
    }
}
