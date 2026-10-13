pub use weaver_server_core::operations::{BackupInspectResult, BackupManifest, BackupStatus};

use super::BackupService;
use super::types::{AutoBackupSettingsGql, BackupInfoGql, BackupSettingsGql};
use crate::auth::AdminGuard;
use async_graphql::{Context, Object, Result};

#[derive(Default)]
pub struct BackupQuery;
#[Object]
impl BackupQuery {
    #[graphql(guard = "AdminGuard")]
    async fn backups(&self, ctx: &Context<'_>) -> Result<Vec<BackupInfoGql>> {
        Ok(ctx
            .data::<BackupService>()?
            .backups()
            .await?
            .into_iter()
            .map(Into::into)
            .collect())
    }
    #[graphql(guard = "AdminGuard")]
    async fn backup_settings(&self, ctx: &Context<'_>) -> Result<BackupSettingsGql> {
        Ok(ctx.data::<BackupService>()?.backup_settings().await?.into())
    }
    #[graphql(guard = "AdminGuard")]
    async fn auto_backup_settings(&self, ctx: &Context<'_>) -> Result<AutoBackupSettingsGql> {
        Ok(ctx
            .data::<BackupService>()?
            .auto_backup_settings()
            .await?
            .into())
    }
}
