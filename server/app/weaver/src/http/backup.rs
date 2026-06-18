use std::path::PathBuf;

use axum::Json;
use axum::extract::{
    Extension, Multipart,
    multipart::{Field, MultipartError},
};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde::Deserialize;

use weaver_server_api::{
    BackupService, BackupStatus, CategoryRemapInput, RestoreOptions, backup_error_status_code,
};
use weaver_server_core::Database;
use weaver_server_core::auth::{ApiKeyCache, LoginAuthCache};
use weaver_server_core::security::RuntimeSecurityConfig;

#[derive(Debug, Deserialize)]
pub(super) struct BackupExportRequest {
    password: Option<String>,
}

async fn require_admin(
    db: &Database,
    auth_cache: &LoginAuthCache,
    api_key_cache: &ApiKeyCache,
    session_token: &str,
    headers: &HeaderMap,
) -> Result<(), StatusCode> {
    let scope =
        super::auth::resolve_scope(db, auth_cache, api_key_cache, session_token, headers).await?;
    if scope.is_admin() {
        Ok(())
    } else {
        Err(StatusCode::FORBIDDEN)
    }
}

pub(super) async fn backup_status_handler(
    Extension(db): Extension<Database>,
    Extension(auth_cache): Extension<LoginAuthCache>,
    Extension(api_key_cache): Extension<ApiKeyCache>,
    Extension(backup): Extension<BackupService>,
    Extension(super::SessionToken(session_token)): Extension<super::SessionToken>,
    headers: HeaderMap,
) -> Result<Json<BackupStatus>, StatusCode> {
    require_admin(&db, &auth_cache, &api_key_cache, &session_token, &headers).await?;
    let status = backup
        .status()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(status))
}

pub(super) async fn backup_export_handler(
    Extension(db): Extension<Database>,
    Extension(auth_cache): Extension<LoginAuthCache>,
    Extension(api_key_cache): Extension<ApiKeyCache>,
    Extension(backup): Extension<BackupService>,
    Extension(super::SessionToken(session_token)): Extension<super::SessionToken>,
    headers: HeaderMap,
    Json(body): Json<BackupExportRequest>,
) -> Response {
    if let Err(status) =
        require_admin(&db, &auth_cache, &api_key_cache, &session_token, &headers).await
    {
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
    Extension(api_key_cache): Extension<ApiKeyCache>,
    Extension(backup): Extension<BackupService>,
    Extension(super::SessionToken(session_token)): Extension<super::SessionToken>,
    Extension(security): Extension<RuntimeSecurityConfig>,
    headers: HeaderMap,
    multipart: Multipart,
) -> Response {
    if let Err(status) =
        require_admin(&db, &auth_cache, &api_key_cache, &session_token, &headers).await
    {
        return status.into_response();
    }
    match parse_backup_upload(multipart, security.backup_upload_limit_bytes).await {
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
    Extension(api_key_cache): Extension<ApiKeyCache>,
    Extension(backup): Extension<BackupService>,
    Extension(super::SessionToken(session_token)): Extension<super::SessionToken>,
    Extension(security): Extension<RuntimeSecurityConfig>,
    headers: HeaderMap,
    multipart: Multipart,
) -> Response {
    if let Err(status) =
        require_admin(&db, &auth_cache, &api_key_cache, &session_token, &headers).await
    {
        return status.into_response();
    }
    match parse_backup_upload(multipart, security.backup_upload_limit_bytes).await {
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
                Ok(report) => {
                    if let Err(status) =
                        super::auth::refresh_auth_caches(&db, &auth_cache, &api_key_cache).await
                    {
                        return status.into_response();
                    }
                    Json(report).into_response()
                }
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

fn multipart_error_response(error: MultipartError) -> (StatusCode, String) {
    if error.status() == StatusCode::INTERNAL_SERVER_ERROR
        && error_source_contains(&error, "length limit exceeded")
    {
        return (
            StatusCode::PAYLOAD_TOO_LARGE,
            "Request payload is too large".to_string(),
        );
    }
    (error.status(), error.body_text())
}

fn error_source_contains(error: &dyn std::error::Error, needle: &str) -> bool {
    let mut source = error.source();
    while let Some(error) = source {
        if error.to_string().contains(needle) {
            return true;
        }
        source = error.source();
    }
    false
}

struct MultipartByteLimiter {
    consumed: u64,
    limit: u64,
}

impl MultipartByteLimiter {
    fn new(limit: u64) -> Self {
        Self { consumed: 0, limit }
    }

    fn consume(&mut self, len: usize) -> Result<(), (StatusCode, String)> {
        self.consumed = self.consumed.saturating_add(len as u64);
        if self.consumed > self.limit {
            return Err((
                StatusCode::PAYLOAD_TOO_LARGE,
                format!("backup upload exceeds {} bytes", self.limit),
            ));
        }
        Ok(())
    }
}

async fn read_field_bytes_limited(
    mut field: Field<'_>,
    limiter: &mut MultipartByteLimiter,
) -> Result<Vec<u8>, (StatusCode, String)> {
    let mut bytes = Vec::new();
    while let Some(chunk) = field.chunk().await.map_err(multipart_error_response)? {
        limiter.consume(chunk.len())?;
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

async fn read_text_field_limited(
    field: Field<'_>,
    limiter: &mut MultipartByteLimiter,
) -> Result<String, (StatusCode, String)> {
    let bytes = read_field_bytes_limited(field, limiter).await?;
    String::from_utf8(bytes).map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))
}

async fn drain_field_limited(
    mut field: Field<'_>,
    limiter: &mut MultipartByteLimiter,
) -> Result<(), (StatusCode, String)> {
    while let Some(chunk) = field.chunk().await.map_err(multipart_error_response)? {
        limiter.consume(chunk.len())?;
    }
    Ok(())
}

async fn parse_backup_upload(
    mut multipart: Multipart,
    upload_limit_bytes: u64,
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
    let mut limiter = MultipartByteLimiter::new(upload_limit_bytes);

    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(multipart_error_response)?
    {
        let name = field.name().unwrap_or_default().to_string();
        match name.as_str() {
            "file" => {
                if saw_file {
                    return Err((
                        StatusCode::BAD_REQUEST,
                        "file may only be provided once".into(),
                    ));
                }
                saw_file = true;
                let mut dest = tokio::fs::File::create(&file_path)
                    .await
                    .map_err(super::internal_upload_err)?;
                let mut field = field;
                while let Some(chunk) = field.chunk().await.map_err(multipart_error_response)? {
                    limiter.consume(chunk.len())?;
                    dest.write_all(&chunk)
                        .await
                        .map_err(super::internal_upload_err)?;
                }
                dest.flush().await.map_err(super::internal_upload_err)?;
            }
            "password" => {
                password = Some(read_text_field_limited(field, &mut limiter).await?);
            }
            "data_dir" => {
                data_dir = Some(read_text_field_limited(field, &mut limiter).await?);
            }
            "intermediate_dir" => {
                intermediate_dir = Some(read_text_field_limited(field, &mut limiter).await?);
            }
            "complete_dir" => {
                complete_dir = Some(read_text_field_limited(field, &mut limiter).await?);
            }
            "category_remaps" => {
                let raw = read_text_field_limited(field, &mut limiter).await?;
                category_remaps = serde_json::from_str(&raw)
                    .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
            }
            _ => drain_field_limited(field, &mut limiter).await?,
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::extract::FromRequest;
    use axum::http::{Request, header};
    use axum::routing::post;
    use tower::ServiceExt;
    use tower_http::limit::RequestBodyLimitLayer;

    fn push_part(body: &mut Vec<u8>, boundary: &str, name: &str, bytes: &[u8]) {
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(
            format!("Content-Disposition: form-data; name=\"{name}\"\r\n\r\n").as_bytes(),
        );
        body.extend_from_slice(bytes);
        body.extend_from_slice(b"\r\n");
    }

    fn multipart_request_from_parts(parts: &[(&str, &[u8])]) -> Request<Body> {
        let boundary = "weaver-test-boundary";
        let mut body = Vec::new();
        for (name, bytes) in parts {
            push_part(&mut body, boundary, name, bytes);
        }
        body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
        Request::builder()
            .method("POST")
            .uri("/backup")
            .header(
                header::CONTENT_TYPE,
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(Body::from(body))
            .unwrap()
    }

    async fn multipart_from_parts(parts: &[(&str, &[u8])]) -> Multipart {
        let request = multipart_request_from_parts(parts);
        Multipart::from_request(request, &()).await.unwrap()
    }

    async fn parse_upload_err(
        multipart: Multipart,
        upload_limit_bytes: u64,
    ) -> (StatusCode, String) {
        match parse_backup_upload(multipart, upload_limit_bytes).await {
            Ok(_) => panic!("backup upload unexpectedly succeeded"),
            Err(error) => error,
        }
    }

    #[tokio::test]
    async fn backup_upload_limit_applies_to_non_file_fields() {
        let multipart = multipart_from_parts(&[("password", b"too-large")]).await;

        let (status, message) = parse_upload_err(multipart, 4).await;

        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
        assert!(message.contains("backup upload exceeds 4 bytes"));
    }

    #[tokio::test]
    async fn backup_upload_limit_counts_combined_fields() {
        let multipart = multipart_from_parts(&[("file", b"abc"), ("password", b"def")]).await;

        let (status, message) = parse_upload_err(multipart, 5).await;

        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
        assert!(message.contains("backup upload exceeds 5 bytes"));
    }

    #[tokio::test]
    async fn backup_upload_rejects_duplicate_file_parts() {
        let multipart = multipart_from_parts(&[("file", b"one"), ("file", b"two")]).await;

        let (status, message) = parse_upload_err(multipart, 64).await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(message.contains("file may only be provided once"));
    }

    #[tokio::test]
    async fn backup_upload_route_body_limit_rejects_before_multipart_parse() {
        async fn parses_multipart(multipart: Multipart) -> StatusCode {
            match parse_backup_upload(multipart, 1024).await {
                Ok(_) => StatusCode::OK,
                Err((status, _)) => status,
            }
        }

        let app = axum::Router::new()
            .route("/backup", post(parses_multipart))
            .route_layer(RequestBodyLimitLayer::new(8));
        let response = app
            .oneshot(multipart_request_from_parts(&[("file", b"abc")]))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }
}
