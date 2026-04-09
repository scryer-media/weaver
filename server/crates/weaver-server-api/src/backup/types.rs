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
        | BackupServiceError::Validation(_) => axum::http::StatusCode::BAD_REQUEST,
        BackupServiceError::Io(_) => axum::http::StatusCode::INTERNAL_SERVER_ERROR,
    }
}
