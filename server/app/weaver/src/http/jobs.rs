use std::io::Read;
use std::path::{Path as FsPath, PathBuf};

use axum::Form;
use axum::body::Body;
use axum::extract::{Extension, Path};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use tokio_util::io::ReaderStream;

use weaver_server_core::Database;
use weaver_server_core::SchedulerHandle;
use weaver_server_core::auth::{ApiKeyCache, LoginAuthCache};
use weaver_server_core::ingest::{open_persisted_nzb_reader, original_release_title};
use weaver_server_core::jobs::ids::JobId;

const INTERNAL_OUTPUT_MARKER_NAME: &str = ".weaver-job-dir";

async fn require_read(
    db: &Database,
    auth_cache: &LoginAuthCache,
    api_key_cache: &ApiKeyCache,
    session_token: &str,
    headers: &HeaderMap,
) -> Result<(), StatusCode> {
    let scope =
        super::auth::resolve_scope(db, auth_cache, api_key_cache, session_token, headers).await?;
    if scope.can_read() {
        Ok(())
    } else {
        Err(StatusCode::FORBIDDEN)
    }
}

struct JobNzbDownload {
    path: PathBuf,
    title: String,
}

#[derive(Debug, Deserialize)]
pub(super) struct JobOutputFileDownloadRequest {
    path: String,
    token: Option<String>,
}

pub(super) async fn job_nzb_download_handler(
    Path(job_id): Path<u64>,
    Extension(db): Extension<Database>,
    Extension(handle): Extension<SchedulerHandle>,
    Extension(auth_cache): Extension<LoginAuthCache>,
    Extension(api_key_cache): Extension<ApiKeyCache>,
    Extension(super::SessionToken(session_token)): Extension<super::SessionToken>,
    headers: HeaderMap,
) -> Response {
    if let Err(status) =
        require_read(&db, &auth_cache, &api_key_cache, &session_token, &headers).await
    {
        return status.into_response();
    }

    let job = match load_job_nzb_download(&db, &handle, job_id).await {
        Ok(Some(job)) => job,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(status) => return status.into_response(),
    };

    match load_uncompressed_nzb_bytes(job.path).await {
        Ok(bytes) => (
            [
                (header::CONTENT_TYPE, "application/x-nzb".to_string()),
                (
                    header::CONTENT_DISPOSITION,
                    format!("attachment; filename=\"{}\"", download_filename(&job.title)),
                ),
            ],
            bytes,
        )
            .into_response(),
        Err(status) => status.into_response(),
    }
}

pub(super) async fn job_output_file_download_handler(
    Path(job_id): Path<u64>,
    Extension(handle): Extension<SchedulerHandle>,
    Extension(request_auth): Extension<super::RequestAuthContext>,
    headers: HeaderMap,
    Form(request): Form<JobOutputFileDownloadRequest>,
) -> Response {
    if let Err(status) = require_read_with_optional_token(
        &request_auth.db,
        &request_auth.auth_cache,
        &request_auth.api_key_cache,
        request_auth.session_token.0.as_str(),
        &headers,
        request.token.as_deref(),
    )
    .await
    {
        return status.into_response();
    }

    let output_dir = match load_job_output_dir(&request_auth.db, &handle, job_id).await {
        Ok(Some(path)) => path,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(status) => return status.into_response(),
    };

    let resolved_path = match resolve_output_file_path(&output_dir, &request.path).await {
        Ok(path) => path,
        Err(status) => return status.into_response(),
    };

    match stream_output_file(resolved_path).await {
        Ok(response) => response,
        Err(status) => status.into_response(),
    }
}

async fn require_read_with_optional_token(
    db: &Database,
    auth_cache: &LoginAuthCache,
    api_key_cache: &ApiKeyCache,
    session_token: &str,
    headers: &HeaderMap,
    token: Option<&str>,
) -> Result<(), StatusCode> {
    let mut effective_headers = headers.clone();
    if effective_headers.get(header::AUTHORIZATION).is_none()
        && let Some(token) = token.filter(|value| !value.trim().is_empty())
    {
        let value = format!("Bearer {}", token.trim());
        let header_value =
            axum::http::HeaderValue::from_str(&value).map_err(|_| StatusCode::BAD_REQUEST)?;
        effective_headers.insert(header::AUTHORIZATION, header_value);
    }

    require_read(
        db,
        auth_cache,
        api_key_cache,
        session_token,
        &effective_headers,
    )
    .await
}

async fn load_job_nzb_download(
    db: &Database,
    handle: &SchedulerHandle,
    job_id: u64,
) -> Result<Option<JobNzbDownload>, StatusCode> {
    if let Ok(info) = handle.get_job(JobId(job_id)) {
        let db = db.clone();
        let recovered = tokio::task::spawn_blocking(move || db.load_active_jobs())
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        if let Some(active) = recovered.get(&JobId(job_id)) {
            return Ok(Some(JobNzbDownload {
                path: active.nzb_path.clone(),
                title: original_release_title(&info.name, &active.metadata),
            }));
        }
    }

    let db = db.clone();
    let history = tokio::task::spawn_blocking(move || db.get_job_history(job_id))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let Some(row) = history else {
        return Ok(None);
    };
    let Some(path) = row.nzb_path else {
        return Ok(None);
    };
    let metadata = row
        .metadata
        .as_deref()
        .and_then(|value| serde_json::from_str::<Vec<(String, String)>>(value).ok())
        .unwrap_or_default();

    Ok(Some(JobNzbDownload {
        path: PathBuf::from(path),
        title: original_release_title(&row.name, &metadata),
    }))
}

async fn load_job_output_dir(
    db: &Database,
    handle: &SchedulerHandle,
    job_id: u64,
) -> Result<Option<PathBuf>, StatusCode> {
    if let Ok(info) = handle.get_job(JobId(job_id))
        && let Some(output_dir) = info.output_dir
    {
        return Ok(Some(PathBuf::from(output_dir)));
    }

    let db = db.clone();
    let history = tokio::task::spawn_blocking(move || db.get_job_history(job_id))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(history.and_then(|row| row.output_dir).map(PathBuf::from))
}

async fn resolve_output_file_path(
    output_dir: &FsPath,
    requested_path: &str,
) -> Result<PathBuf, StatusCode> {
    let requested_path = requested_path.trim();
    if requested_path.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }

    let canonical_output_dir = tokio::fs::canonicalize(output_dir)
        .await
        .map_err(io_status)?;
    let requested_path = PathBuf::from(requested_path);
    let candidate = if requested_path.is_absolute() {
        requested_path
    } else {
        canonical_output_dir.join(requested_path)
    };
    let canonical_candidate = tokio::fs::canonicalize(candidate)
        .await
        .map_err(io_status)?;

    if !canonical_candidate.starts_with(&canonical_output_dir) {
        return Err(StatusCode::NOT_FOUND);
    }

    let Some(file_name) = canonical_candidate
        .file_name()
        .and_then(|value| value.to_str())
    else {
        return Err(StatusCode::NOT_FOUND);
    };
    if file_name == INTERNAL_OUTPUT_MARKER_NAME {
        return Err(StatusCode::NOT_FOUND);
    }

    let metadata = tokio::fs::metadata(&canonical_candidate)
        .await
        .map_err(io_status)?;
    if !metadata.is_file() {
        return Err(StatusCode::NOT_FOUND);
    }

    Ok(canonical_candidate)
}

async fn stream_output_file(path: PathBuf) -> Result<Response, StatusCode> {
    let file = tokio::fs::File::open(&path).await.map_err(io_status)?;
    let metadata = file.metadata().await.map_err(io_status)?;
    let content_type = mime_guess::from_path(&path)
        .first_or_octet_stream()
        .essence_str()
        .to_string();
    let filename = download_filename_for_path(&path);
    let body = Body::from_stream(ReaderStream::new(file));

    Ok((
        [
            (header::CONTENT_TYPE, content_type),
            (
                header::CONTENT_DISPOSITION,
                format!("attachment; filename=\"{filename}\""),
            ),
            (header::CONTENT_LENGTH, metadata.len().to_string()),
        ],
        body,
    )
        .into_response())
}

async fn load_uncompressed_nzb_bytes(path: PathBuf) -> Result<Vec<u8>, StatusCode> {
    tokio::task::spawn_blocking(move || {
        let mut reader = open_persisted_nzb_reader(&path).map_err(io_status)?;
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).map_err(io_status)?;
        Ok::<_, StatusCode>(bytes)
    })
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
}

fn io_status(error: std::io::Error) -> StatusCode {
    if error.kind() == std::io::ErrorKind::NotFound {
        StatusCode::NOT_FOUND
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    }
}

fn download_filename(title: &str) -> String {
    let trimmed = title
        .trim()
        .trim_end_matches(".nzb")
        .trim_end_matches(".NZB");
    let safe = trimmed
        .chars()
        .map(|ch| match ch {
            '"' | '\\' | '/' | ':' | '*' | '?' | '<' | '>' | '|' | '\r' | '\n' => '_',
            _ => ch,
        })
        .collect::<String>();
    let base = safe.trim();
    if base.is_empty() {
        "job.nzb".to_string()
    } else {
        format!("{base}.nzb")
    }
}

fn download_filename_for_path(path: &FsPath) -> String {
    let name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("download.bin");
    let safe = name
        .chars()
        .map(|ch| match ch {
            '"' | '\\' | '/' | ':' | '*' | '?' | '<' | '>' | '|' | '\r' | '\n' => '_',
            _ => ch,
        })
        .collect::<String>();
    let trimmed = safe.trim();
    if trimmed.is_empty() {
        "download.bin".to_string()
    } else {
        trimmed.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn download_filename_preserves_release_name_and_appends_suffix() {
        assert_eq!(
            download_filename("Friends.S05.720p.BluRay"),
            "Friends.S05.720p.BluRay.nzb"
        );
        assert_eq!(download_filename("already.nzb"), "already.nzb");
        assert_eq!(download_filename("  "), "job.nzb");
    }

    #[test]
    fn download_filename_for_path_uses_basename() {
        assert_eq!(
            download_filename_for_path(FsPath::new("/downloads/episode-01.mkv")),
            "episode-01.mkv"
        );
    }
}
