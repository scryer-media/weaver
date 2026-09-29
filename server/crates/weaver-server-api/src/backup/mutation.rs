pub use weaver_server_core::operations::{
    BackupArtifact, BackupService, BackupServiceError, CategoryRemapInput,
    CategoryRemapRequirement, RestoreOptions, RestoreReport,
};

use super::types::{AutoBackupSettingsGql, AutoBackupSettingsInputGql, BackupSettingsGql};
use crate::auth::FreshAdminGuard;
use async_graphql::{Context, Object, Result};

#[derive(Default)]
pub struct BackupMutation;
#[Object]
impl BackupMutation {
    #[graphql(guard = "FreshAdminGuard")]
    async fn update_backup_settings(
        &self,
        ctx: &Context<'_>,
        custom_backup_path: Option<String>,
    ) -> Result<BackupSettingsGql> {
        Ok(ctx
            .data::<BackupService>()?
            .update_backup_settings(custom_backup_path)
            .await?
            .into())
    }
    #[graphql(guard = "FreshAdminGuard")]
    async fn update_auto_backup_settings(
        &self,
        ctx: &Context<'_>,
        input: AutoBackupSettingsInputGql,
    ) -> Result<AutoBackupSettingsGql> {
        Ok(ctx
            .data::<BackupService>()?
            .update_auto_backup_settings(
                weaver_server_core::operations::backup::AutoBackupSettingsInput {
                    enabled: input.enabled,
                    daily_time_local: input.daily_time_local,
                    set_auto_backup_key: input.set_auto_backup_key,
                    clear_auto_backup_key: input.clear_auto_backup_key,
                },
            )
            .await?
            .into())
    }
    #[graphql(guard = "FreshAdminGuard")]
    async fn delete_backup(&self, ctx: &Context<'_>, filename: String) -> Result<bool> {
        Ok(ctx
            .data::<BackupService>()?
            .delete_backup(&filename)
            .await?)
    }
    #[graphql(guard = "FreshAdminGuard")]
    async fn create_backup_download_token(
        &self,
        ctx: &Context<'_>,
        filename: String,
    ) -> Result<String> {
        Ok(ctx
            .data::<BackupService>()?
            .create_download_token(&filename)
            .await?)
    }
}
