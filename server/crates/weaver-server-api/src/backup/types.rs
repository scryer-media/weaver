pub use weaver_server_core::operations::{
    BackupArtifact, BackupInspectResult, BackupManifest, BackupService, BackupServiceError,
    BackupStatus, CategoryRemapInput, CategoryRemapRequirement, RestoreOptions, RestoreReport,
};

pub fn backup_error_status_code(error: &BackupServiceError) -> axum::http::StatusCode {
    match error {
        BackupServiceError::Busy | BackupServiceError::NotPristine => {
            axum::http::StatusCode::CONFLICT
        }
        BackupServiceError::PasswordRequired
        | BackupServiceError::InvalidPassword
        | BackupServiceError::UnsupportedFormat(_)
        | BackupServiceError::UnsupportedScope(_)
        | BackupServiceError::SchemaMismatch { .. }
        | BackupServiceError::MissingCategoryRemaps(_)
        | BackupServiceError::KeyIncompatible(_)
        | BackupServiceError::Validation(_) => axum::http::StatusCode::BAD_REQUEST,
        BackupServiceError::Io(_) => axum::http::StatusCode::INTERNAL_SERVER_ERROR,
    }
}

#[derive(async_graphql::SimpleObject)]
#[graphql(name = "BackupInfo")]
pub struct BackupInfoGql {
    pub filename: String,
    pub size_bytes: u64,
    pub created_at: String,
    pub format_version: String,
    pub source_weaver_version: String,
    pub source_engine: String,
    pub encrypted: bool,
    pub row_counts: async_graphql::Json<std::collections::BTreeMap<String, u64>>,
    pub trigger: BackupTriggerGql,
    pub status: BackupArtifactStatusGql,
    pub error: Option<String>,
}

#[derive(async_graphql::Enum, Clone, Copy, PartialEq, Eq)]
#[graphql(name = "BackupTrigger")]
pub enum BackupTriggerGql {
    Manual,
    Auto,
}
#[derive(async_graphql::Enum, Clone, Copy, PartialEq, Eq)]
#[graphql(name = "BackupArtifactStatus")]
pub enum BackupArtifactStatusGql {
    Creating,
    Ready,
    Failed,
}

impl From<weaver_server_core::operations::backup::BackupInfo> for BackupInfoGql {
    fn from(info: weaver_server_core::operations::backup::BackupInfo) -> Self {
        use weaver_server_core::operations::backup::{BackupArtifactStatus, BackupTrigger};
        Self {
            filename: info.filename,
            size_bytes: info.size_bytes,
            created_at: info.created_at.to_rfc3339(),
            format_version: info.format_version,
            source_weaver_version: info.source_weaver_version,
            source_engine: info.source_engine,
            encrypted: info.encrypted,
            row_counts: async_graphql::Json(info.row_counts),
            error: info.error,
            trigger: match info.trigger {
                BackupTrigger::Manual => BackupTriggerGql::Manual,
                BackupTrigger::Auto => BackupTriggerGql::Auto,
            },
            status: match info.status {
                BackupArtifactStatus::Creating => BackupArtifactStatusGql::Creating,
                BackupArtifactStatus::Ready => BackupArtifactStatusGql::Ready,
                BackupArtifactStatus::Failed => BackupArtifactStatusGql::Failed,
            },
        }
    }
}

#[derive(async_graphql::SimpleObject)]
#[graphql(name = "BackupSettings")]
pub struct BackupSettingsGql {
    pub custom_backup_path: Option<String>,
    pub backup_path: String,
}
impl From<weaver_server_core::operations::backup::BackupSettings> for BackupSettingsGql {
    fn from(value: weaver_server_core::operations::backup::BackupSettings) -> Self {
        Self {
            custom_backup_path: value.custom_backup_path,
            backup_path: value.backup_path,
        }
    }
}
#[derive(async_graphql::SimpleObject)]
#[graphql(name = "AutoBackupSettings")]
pub struct AutoBackupSettingsGql {
    pub enabled: bool,
    pub daily_time_local: String,
    pub auto_backup_key_present: bool,
    pub next_run_at: Option<String>,
}
impl From<weaver_server_core::operations::backup::AutoBackupSettings> for AutoBackupSettingsGql {
    fn from(value: weaver_server_core::operations::backup::AutoBackupSettings) -> Self {
        Self {
            enabled: value.enabled,
            daily_time_local: value.daily_time_local,
            auto_backup_key_present: value.auto_backup_key_present,
            next_run_at: value.next_run_at,
        }
    }
}
#[derive(async_graphql::InputObject)]
#[graphql(name = "AutoBackupSettingsInput")]
pub struct AutoBackupSettingsInputGql {
    pub enabled: bool,
    pub daily_time_local: String,
    pub set_auto_backup_key: Option<String>,
    #[graphql(default)]
    pub clear_auto_backup_key: bool,
}
