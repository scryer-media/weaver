use std::path::PathBuf;

use axum::Json;
use axum::extract::{Extension, Multipart};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde::Deserialize;

use weaver_server_api::{
    BackupService, BackupStatus, CategoryRemapInput, RestoreOptions, backup_error_status_code,
};
use weaver_server_core::Database;
use weaver_server_core::auth::LoginAuthCache;

#[derive(Debug, Deserialize)]
pub(super) struct BackupExportRequest {
    password: Option<String>,
}

async fn require_admin(
    db: &Database,
    auth_cache: &LoginAuthCache,
    session_token: &str,
    headers: &HeaderMap,
) -> Result<(), StatusCode> {
    let scope = super::auth::resolve_scope(db, auth_cache, session_token, headers).await?;
    if scope.is_admin() {
        Ok(())
    } else {
        Err(StatusCode::FORBIDDEN)
    }
}

pub(super) async fn backup_status_handler(
    Extension(db): Extension<Database>,
    Extension(auth_cache): Extension<LoginAuthCache>,
    Extension(backup): Extension<BackupService>,
    Extension(super::SessionToken(session_token)): Extension<super::SessionToken>,
    headers: HeaderMap,
) -> Result<Json<BackupStatus>, StatusCode> {
    require_admin(&db, &auth_cache, &session_token, &headers).await?;
    let status = backup
        .status()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(status))
}

pub(super) async fn backup_export_handler(
    Extension(db): Extension<Database>,
    Extension(auth_cache): Extension<LoginAuthCache>,
    Extension(backup): Extension<BackupService>,
    Extension(super::SessionToken(session_token)): Extension<super::SessionToken>,
    headers: HeaderMap,
    Json(body): Json<BackupExportRequest>,
) -> Response {
    if let Err(status) = require_admin(&db, &auth_cache, &session_token, &headers).await {
        return status.into_response();
    }
    match backup.create_backup(body.password).await {
        Ok(artifact) => (
            [
                (header::CONTENT_TYPE, "application/octet-stream".to_string()),
                (
                    header::CONTENT_DISPOSITION,
                    format!("attachment; filename=\"{}\"", artifact.filename),
                ),
            ],
            artifact.bytes,
        )
            .into_response(),
        Err(error) => super::error_response(backup_error_status_code(&error), &error.to_string()),
    }
}

pub(super) async fn backup_inspect_handler(
    Extension(db): Extension<Database>,
    Extension(auth_cache): Extension<LoginAuthCache>,
    Extension(backup): Extension<BackupService>,
    Extension(super::SessionToken(session_token)): Extension<super::SessionToken>,
    headers: HeaderMap,
    multipart: Multipart,
) -> Response {
    if let Err(status) = require_admin(&db, &auth_cache, &session_token, &headers).await {
        return status.into_response();
    }
    match parse_backup_upload(multipart).await {
        Ok(upload) => match backup
            .inspect_backup(&upload.file_path, upload.password)
            .await
        {
            Ok(result) => Json(result).into_response(),
            Err(error) => {
                super::error_response(backup_error_status_code(&error), &error.to_string())
            }
        },
        Err((status, message)) => super::error_response(status, &message),
    }
}

pub(super) async fn backup_restore_handler(
    Extension(db): Extension<Database>,
    Extension(auth_cache): Extension<LoginAuthCache>,
    Extension(backup): Extension<BackupService>,
    Extension(super::SessionToken(session_token)): Extension<super::SessionToken>,
    headers: HeaderMap,
    multipart: Multipart,
) -> Response {
    if let Err(status) = require_admin(&db, &auth_cache, &session_token, &headers).await {
        return status.into_response();
    }
    match parse_backup_upload(multipart).await {
        Ok(upload) => {
            let options = RestoreOptions {
                data_dir: upload.data_dir.unwrap_or_default(),
                intermediate_dir: upload.intermediate_dir,
                complete_dir: upload.complete_dir,
                category_remaps: upload.category_remaps,
            };
            if options.data_dir.trim().is_empty() {
                return super::error_response(StatusCode::BAD_REQUEST, "data_dir is required");
            }
            match backup
                .restore_backup(&upload.file_path, upload.password, options)
                .await
            {
                Ok(report) => Json(report).into_response(),
                Err(error) => {
                    super::error_response(backup_error_status_code(&error), &error.to_string())
                }
            }
        }
        Err((status, message)) => super::error_response(status, &message),
    }
}

struct ParsedBackupUpload {
    _temp_dir: tempfile::TempDir,
    file_path: PathBuf,
    password: Option<String>,
    data_dir: Option<String>,
    intermediate_dir: Option<String>,
    complete_dir: Option<String>,
    category_remaps: Vec<CategoryRemapInput>,
}

async fn parse_backup_upload(
    mut multipart: Multipart,
) -> Result<ParsedBackupUpload, (StatusCode, String)> {
    use tokio::io::AsyncWriteExt;

    let temp_dir = tempfile::tempdir().map_err(super::internal_upload_err)?;
    let file_path = temp_dir.path().join("upload.bin");
    let mut saw_file = false;
    let mut password = None;
    let mut data_dir = None;
    let mut intermediate_dir = None;
    let mut complete_dir = None;
    let mut category_remaps = Vec::new();

    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?
    {
        let name = field.name().unwrap_or_default().to_string();
        match name.as_str() {
            "file" => {
                saw_file = true;
                let mut dest = tokio::fs::File::create(&file_path)
                    .await
                    .map_err(super::internal_upload_err)?;
                let mut field = field;
                while let Some(chunk) = field
                    .chunk()
                    .await
                    .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?
                {
                    dest.write_all(&chunk)
                        .await
                        .map_err(super::internal_upload_err)?;
                }
                dest.flush().await.map_err(super::internal_upload_err)?;
            }
            "password" => {
                password = Some(
                    field
                        .text()
                        .await
                        .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?,
                );
            }
            "data_dir" => {
                data_dir = Some(
                    field
                        .text()
                        .await
                        .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?,
                );
            }
            "intermediate_dir" => {
                intermediate_dir = Some(
                    field
                        .text()
                        .await
                        .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?,
                );
            }
            "complete_dir" => {
                complete_dir = Some(
                    field
                        .text()
                        .await
                        .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?,
                );
            }
            "category_remaps" => {
                let raw = field
                    .text()
                    .await
                    .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
                category_remaps = serde_json::from_str(&raw)
                    .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
            }
            _ => {}
        }
    }

    if !saw_file {
        return Err((StatusCode::BAD_REQUEST, "file is required".into()));
    }

    Ok(ParsedBackupUpload {
        _temp_dir: temp_dir,
        file_path,
        password,
        data_dir,
        intermediate_dir,
        complete_dir,
        category_remaps,
    })
}
