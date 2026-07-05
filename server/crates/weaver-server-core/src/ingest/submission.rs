use std::future::Future;
use std::io::Read;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use tracing::{info, warn};

use crate::Database;
use crate::SchedulerHandle;
use crate::jobs::JobSpec;
use crate::jobs::ids::JobId;
use crate::security::{ResolvedFetchTarget, RuntimeSecurityConfig, resolve_fetch_target};
use crate::settings::SharedConfig;
use weaver_nzb::Nzb;

use super::persisted_nzb;
use crate::ingest::{append_original_title_metadata, derive_release_name, nzb_password_candidates};
use crate::jobs::{FileSpec, SegmentSpec};
use weaver_model::files::{FileRole, unique_download_filenames};

static NEXT_API_JOB_ID: AtomicU64 = AtomicU64::new(10_000);
const MAX_FETCH_REDIRECTS: usize = 10;

#[derive(Clone)]
pub struct SubmittedJob {
    pub job_id: JobId,
    pub job_hash: [u8; 32],
    pub spec: JobSpec,
    pub created_at_epoch_ms: f64,
}

struct PreparedSubmission {
    nzb_zstd: Vec<u8>,
    filename: Option<String>,
    password: Option<String>,
    category: Option<String>,
    metadata: Vec<(String, String)>,
    submit_started: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CategoryResolutionMode {
    ResolveConfigured,
    PreserveSubmitted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubmissionOptions {
    pub category_resolution: CategoryResolutionMode,
    pub add_paused: bool,
}

impl Default for SubmissionOptions {
    fn default() -> Self {
        Self {
            category_resolution: CategoryResolutionMode::ResolveConfigured,
            add_paused: false,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SubmitNzbError {
    #[error("NZB parse error: {0}")]
    Parse(#[from] weaver_nzb::NzbError),
    #[error("NZB contains no files")]
    Empty,
    #[error("failed to save NZB: {0}")]
    Save(std::io::Error),
    #[error("failed to read uploaded NZB: {0}")]
    Upload(std::io::Error),
    #[error("scheduler error: {0}")]
    Scheduler(#[from] crate::SchedulerError),
    #[error("state error: {0}")]
    State(#[from] crate::StateError),
    #[error("NZB fetch failed: {0}")]
    Fetch(String),
    #[error("NZB response was not valid XML")]
    NotXml,
}

pub fn init_job_counter(start: u64) {
    NEXT_API_JOB_ID.store(start.max(10_000), Ordering::Relaxed);
}

pub fn next_submission_job_id() -> JobId {
    JobId(NEXT_API_JOB_ID.fetch_add(1, Ordering::Relaxed))
}

pub fn nzb_to_submission_spec(
    nzb: &Nzb,
    filename: Option<&str>,
    password: Option<String>,
    category: Option<String>,
    metadata: Vec<(String, String)>,
) -> JobSpec {
    let metadata = append_original_title_metadata(
        metadata,
        filename.and_then(|value| value.strip_suffix(".nzb")),
        nzb.meta.title.as_deref(),
    );

    let name = derive_release_name(
        filename.and_then(|value| value.strip_suffix(".nzb")),
        nzb.meta.title.as_deref(),
    );

    let password_path = filename
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("upload.nzb"));
    let password = nzb_password_candidates(nzb, &password_path, password.as_deref())
        .into_iter()
        .next()
        .map(|candidate| candidate.value().to_string());

    let mut files = Vec::with_capacity(nzb.files.len());
    let mut total_bytes: u64 = 0;
    let filename_candidates = nzb
        .files
        .iter()
        .map(|nzb_file| {
            // Prefer the enhanced extraction (handles PRiVATE format, higher confidence).
            // If the extracted name is obfuscated, keep it - PAR2 deobfuscation will
            // rename it post-repair using 16KB hash matching.
            nzb_file
                .extract_filename()
                .map(|(name, _confidence)| name)
                .or_else(|| nzb_file.filename().map(str::to_string))
                .unwrap_or_else(|| "unknown".to_string())
        })
        .collect::<Vec<_>>();
    let filenames = unique_download_filenames(filename_candidates.iter().map(String::as_str));

    for (nzb_file, filename) in nzb.files.iter().zip(filenames) {
        let role = FileRole::from_filename(&filename);

        let segments: Vec<SegmentSpec> = nzb_file
            .segments
            .iter()
            .enumerate()
            .map(|(ordinal, segment)| SegmentSpec {
                ordinal: ordinal as u32,
                article_number: segment.number,
                bytes: segment.bytes,
                message_id: segment.message_id.clone(),
            })
            .collect();

        let file_bytes: u64 = nzb_file.total_bytes();
        total_bytes += file_bytes;

        files.push(FileSpec {
            filename,
            role,
            groups: nzb_file.groups.clone(),
            posted_at_epoch: (nzb_file.date > 0).then_some(nzb_file.date),
            segments,
        });
    }

    JobSpec {
        name,
        password,
        files,
        total_bytes,
        category,
        metadata,
    }
}

pub async fn resolve_submission_category(
    config: &SharedConfig,
    category: Option<&str>,
) -> Option<String> {
    resolve_submission_category_with_mode(
        config,
        category,
        CategoryResolutionMode::ResolveConfigured,
    )
    .await
}

pub async fn resolve_submission_category_with_mode(
    config: &SharedConfig,
    category: Option<&str>,
    mode: CategoryResolutionMode,
) -> Option<String> {
    let category = category?;
    if matches!(mode, CategoryResolutionMode::PreserveSubmitted) {
        return Some(category.to_string());
    }

    let cfg = config.read().await;
    if cfg.categories.is_empty() {
        return Some(category.to_string());
    }

    match crate::categories::resolve_category(&cfg.categories, category) {
        Some(canonical) => Some(canonical),
        None => {
            warn!(category = %category, "unknown category, submitting without category");
            None
        }
    }
}

async fn submit_prepared_nzb(
    db: &Database,
    handle: &SchedulerHandle,
    config: &SharedConfig,
    nzb: Nzb,
    prepared: PreparedSubmission,
    options: SubmissionOptions,
) -> Result<SubmittedJob, SubmitNzbError> {
    if nzb.files.is_empty() {
        return Err(SubmitNzbError::Empty);
    }

    let PreparedSubmission {
        nzb_zstd,
        filename,
        password,
        category,
        metadata,
        submit_started,
    } = prepared;

    let resolved_category = resolve_submission_category_with_mode(
        config,
        category.as_deref(),
        options.category_resolution,
    )
    .await;
    let job_id = db.reserve_next_job_id()?;
    let job_hash = persisted_nzb::hash_persisted_nzb_bytes(&nzb_zstd);
    let spec = nzb_to_submission_spec(
        &nzb,
        filename.as_deref(),
        password,
        resolved_category,
        metadata,
    );
    let nzb_path = PathBuf::from(
        filename
            .clone()
            .unwrap_or_else(|| format!("job-{}.nzb", job_id.0)),
    );

    if let Err(error) = handle
        .add_job_with_options(
            job_id,
            spec.clone(),
            nzb_path.clone(),
            nzb_zstd,
            crate::jobs::AddJobOptions {
                initially_paused: options.add_paused,
            },
        )
        .await
    {
        return Err(error.into());
    }

    info!(
        job_id = job_id.0,
        name = %spec.name,
        category = spec.category,
        metadata_len = spec.metadata.len(),
        has_password = spec.password.is_some(),
        elapsed_ms = submit_started.elapsed().as_millis() as u64,
        "submitted NZB job"
    );

    Ok(SubmittedJob {
        job_id,
        job_hash,
        spec,
        created_at_epoch_ms: crate::jobs::model::epoch_ms_now(),
    })
}

#[allow(clippy::too_many_arguments)]
pub async fn submit_nzb_bytes(
    db: &Database,
    handle: &SchedulerHandle,
    config: &SharedConfig,
    nzb_bytes: &[u8],
    filename: Option<String>,
    password: Option<String>,
    category: Option<String>,
    metadata: Vec<(String, String)>,
) -> Result<SubmittedJob, SubmitNzbError> {
    submit_nzb_bytes_with_options(
        db,
        handle,
        config,
        nzb_bytes,
        filename,
        password,
        category,
        metadata,
        SubmissionOptions::default(),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn submit_nzb_bytes_with_options(
    db: &Database,
    handle: &SchedulerHandle,
    config: &SharedConfig,
    nzb_bytes: &[u8],
    filename: Option<String>,
    password: Option<String>,
    category: Option<String>,
    metadata: Vec<(String, String)>,
    options: SubmissionOptions,
) -> Result<SubmittedJob, SubmitNzbError> {
    let submit_started = Instant::now();
    let nzb = weaver_nzb::parse_nzb(nzb_bytes)?;
    let nzb_zstd = persisted_nzb::compress_nzb_bytes(nzb_bytes).map_err(SubmitNzbError::Save)?;
    submit_prepared_nzb(
        db,
        handle,
        config,
        nzb,
        PreparedSubmission {
            nzb_zstd,
            filename,
            password,
            category,
            metadata,
            submit_started,
        },
        options,
    )
    .await
}

pub async fn fetch_nzb_from_url(
    _client: &reqwest::Client,
    url: &str,
) -> Result<(Vec<u8>, Option<String>), SubmitNzbError> {
    fetch_nzb_from_url_with_resolver(url, |current_url| async move {
        resolve_fetch_target(&current_url, false).await
    })
    .await
}

async fn fetch_nzb_from_url_with_resolver<R, Fut>(
    url: &str,
    mut resolve_target: R,
) -> Result<(Vec<u8>, Option<String>), SubmitNzbError>
where
    R: FnMut(reqwest::Url) -> Fut,
    Fut: Future<Output = Result<ResolvedFetchTarget, String>>,
{
    let limits = RuntimeSecurityConfig::from_env_or_default_for_tests();
    let mut current_url =
        reqwest::Url::parse(url).map_err(|error| SubmitNzbError::Fetch(error.to_string()))?;

    for redirect_count in 0..=MAX_FETCH_REDIRECTS {
        let target = resolve_target(current_url.clone())
            .await
            .map_err(SubmitNzbError::Fetch)?;
        let client = target
            .apply_dns_override(
                reqwest::Client::builder()
                    .redirect(reqwest::redirect::Policy::none())
                    .timeout(std::time::Duration::from_secs(60))
                    .user_agent("weaver/0.1")
                    .gzip(true)
                    .brotli(true)
                    .deflate(true)
                    .zstd(true),
            )
            .build()
            .map_err(|error| SubmitNzbError::Fetch(format!("client build failed: {error}")))?;

        let response = client
            .get(target.url.clone())
            .send()
            .await
            .map_err(|error| SubmitNzbError::Fetch(format!("request failed: {error}")))?;

        if response.status().is_redirection() {
            if redirect_count == MAX_FETCH_REDIRECTS {
                return Err(SubmitNzbError::Fetch("too many redirects".to_string()));
            }
            let location = response
                .headers()
                .get(reqwest::header::LOCATION)
                .and_then(|value| value.to_str().ok())
                .ok_or_else(|| SubmitNzbError::Fetch("redirect missing Location".to_string()))?;
            current_url = current_url
                .join(location)
                .map_err(|error| SubmitNzbError::Fetch(format!("invalid redirect: {error}")))?;
            continue;
        }

        if !response.status().is_success() {
            return Err(SubmitNzbError::Fetch(format!("HTTP {}", response.status())));
        }

        let filename = extract_filename_from_response(&response, current_url.as_str());
        let nzb_bytes =
            read_response_with_limit(response, limits.nzb_decompressed_limit_bytes).await?;

        if nzb_bytes.is_empty() {
            return Err(SubmitNzbError::Fetch("empty response body".into()));
        }

        let nzb = weaver_nzb::parse_nzb(&nzb_bytes)?;
        if nzb.files.is_empty() {
            return Err(SubmitNzbError::Empty);
        }

        return Ok((nzb_bytes, filename));
    }

    Err(SubmitNzbError::Fetch("too many redirects".to_string()))
}

async fn read_response_with_limit(
    mut response: reqwest::Response,
    limit: u64,
) -> Result<Vec<u8>, SubmitNzbError> {
    if response
        .content_length()
        .is_some_and(|length| length > limit)
    {
        return Err(SubmitNzbError::Fetch(format!(
            "response exceeds {limit} bytes"
        )));
    }

    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| SubmitNzbError::Fetch(format!("body read failed: {error}")))?
    {
        let next_len = body.len().saturating_add(chunk.len());
        if next_len as u64 > limit {
            return Err(SubmitNzbError::Fetch(format!(
                "response exceeds {limit} bytes"
            )));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

#[allow(clippy::too_many_arguments)]
pub async fn submit_uploaded_nzb_reader<R>(
    db: &Database,
    handle: &SchedulerHandle,
    config: &SharedConfig,
    source: R,
    filename: Option<String>,
    password: Option<String>,
    category: Option<String>,
    metadata: Vec<(String, String)>,
) -> Result<SubmittedJob, SubmitNzbError>
where
    R: Read + Send + 'static,
{
    let submit_started = Instant::now();
    let persist_result = tokio::task::spawn_blocking(move || {
        let mut source = source;
        super::persisted_nzb::persist_decoded_nzb_reader_to_zstd(&mut source)
    })
    .await
    .map_err(|error| SubmitNzbError::Upload(std::io::Error::other(error.to_string())))?;
    let (nzb_zstd, nzb) = match persist_result {
        Ok(values) => values,
        Err(super::persisted_nzb::PersistedNzbError::Io(error)) => {
            return Err(SubmitNzbError::Save(error));
        }
        Err(super::persisted_nzb::PersistedNzbError::Parse(error)) => {
            return Err(SubmitNzbError::Parse(error));
        }
    };
    submit_prepared_nzb(
        db,
        handle,
        config,
        nzb,
        PreparedSubmission {
            nzb_zstd,
            filename,
            password,
            category,
            metadata,
            submit_started,
        },
        SubmissionOptions::default(),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn submit_staged_nzb_zstd(
    db: &Database,
    handle: &SchedulerHandle,
    config: &SharedConfig,
    nzb_zstd: Vec<u8>,
    filename: Option<String>,
    password: Option<String>,
    category: Option<String>,
    metadata: Vec<(String, String)>,
) -> Result<SubmittedJob, SubmitNzbError> {
    let submit_started = Instant::now();
    let nzb = match persisted_nzb::parse_persisted_nzb_bytes(&nzb_zstd) {
        Ok(nzb) => nzb,
        Err(persisted_nzb::PersistedNzbError::Io(error)) => {
            return Err(SubmitNzbError::Save(error));
        }
        Err(persisted_nzb::PersistedNzbError::Parse(error)) => {
            return Err(SubmitNzbError::Parse(error));
        }
    };
    submit_prepared_nzb(
        db,
        handle,
        config,
        nzb,
        PreparedSubmission {
            nzb_zstd,
            filename,
            password,
            category,
            metadata,
            submit_started,
        },
        SubmissionOptions::default(),
    )
    .await
}

fn extract_filename_from_response(response: &reqwest::Response, url: &str) -> Option<String> {
    if let Some(content_disposition) = response.headers().get(reqwest::header::CONTENT_DISPOSITION)
        && let Ok(value) = content_disposition.to_str()
        && let Some(name) = parse_content_disposition_filename(value)
    {
        return Some(name);
    }

    let path = url.split('?').next().unwrap_or(url);
    let segment = path
        .rsplit('/')
        .next()
        .filter(|segment| !segment.is_empty() && segment.contains('.'))?;
    Some(segment.to_string())
}

fn parse_content_disposition_filename(header: &str) -> Option<String> {
    let lower = header.to_ascii_lowercase();
    let idx = lower.find("filename=")?;
    let rest = &header[idx + "filename=".len()..];
    let rest = rest.trim_start();
    if let Some(stripped) = rest.strip_prefix('"') {
        let end = stripped.find('"')?;
        let name = &stripped[..end];
        if name.is_empty() {
            return None;
        }
        Some(name.to_string())
    } else {
        let name = rest.split(';').next()?.trim();
        if name.is_empty() {
            return None;
        }
        Some(name.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn minimal_nzb(name: &str) -> String {
        format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
  <file poster="test@test.com" date="1234567890" subject="{name} - &quot;file.rar&quot; yEnc (1/1)">
    <groups><group>alt.binaries.test</group></groups>
    <segments><segment bytes="500000" number="1">{name}-seg1@test.com</segment></segments>
  </file>
</nzb>"#
        )
    }

    async fn read_http_request(stream: &mut tokio::net::TcpStream) -> String {
        let mut request = Vec::new();
        let mut buf = [0_u8; 1024];
        loop {
            let read = stream.read(&mut buf).await.unwrap();
            if read == 0 {
                break;
            }
            request.extend_from_slice(&buf[..read]);
            if request.windows(4).any(|window| window == b"\r\n\r\n") {
                break;
            }
        }
        String::from_utf8_lossy(&request).into_owned()
    }

    fn pinned_target(url: reqwest::Url, addr: SocketAddr) -> ResolvedFetchTarget {
        let host = url.host_str().unwrap().to_string();
        ResolvedFetchTarget {
            url,
            host,
            addrs: vec![addr],
        }
    }

    #[tokio::test]
    async fn fetch_nzb_uses_pinned_hostname_resolution() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let xml = minimal_nzb("Pinned.Release");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read_http_request(&mut stream).await;
            let response = format!(
                "HTTP/1.1 200 OK\r\n\
                 Content-Type: application/x-nzb\r\n\
                 Content-Disposition: attachment; filename=\"Pinned.Release.nzb\"\r\n\
                 Content-Length: {}\r\n\
                 Connection: close\r\n\
                 \r\n\
                 {}",
                xml.len(),
                xml
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            request
        });

        let url = format!("http://public.test:{}/download.nzb", addr.port());
        let (bytes, filename) = fetch_nzb_from_url_with_resolver(&url, move |url| async move {
            Ok(pinned_target(url, addr))
        })
        .await
        .unwrap();
        let request = server.await.unwrap();

        assert_eq!(filename.as_deref(), Some("Pinned.Release.nzb"));
        assert!(!bytes.is_empty());
        assert!(request.starts_with("GET /download.nzb HTTP/1.1"));
        assert!(request.contains(&format!("host: public.test:{}", addr.port())));
    }

    #[tokio::test]
    async fn fetch_nzb_rejects_redirect_to_private_before_request() {
        let redirect_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let redirect_addr = redirect_listener.local_addr().unwrap();
        let private_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let private_addr = private_listener.local_addr().unwrap();
        let redirect_server = tokio::spawn(async move {
            let (mut stream, _) = redirect_listener.accept().await.unwrap();
            let _request = read_http_request(&mut stream).await;
            let response = format!(
                "HTTP/1.1 302 Found\r\n\
                 Location: http://127.0.0.1:{}/private.nzb\r\n\
                 Content-Length: 0\r\n\
                 Connection: close\r\n\
                 \r\n",
                private_addr.port()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
        });
        let private_accept = tokio::spawn(async move {
            tokio::time::timeout(Duration::from_millis(200), private_listener.accept())
                .await
                .is_ok()
        });

        let url = format!("http://public.test:{}/redirect.nzb", redirect_addr.port());
        let error = fetch_nzb_from_url_with_resolver(&url, move |url| async move {
            if url.host_str() == Some("public.test") {
                Ok(pinned_target(url, redirect_addr))
            } else {
                resolve_fetch_target(&url, false).await
            }
        })
        .await
        .unwrap_err();
        redirect_server.await.unwrap();
        let private_was_hit = private_accept.await.unwrap();

        assert!(error.to_string().contains("not allowed"));
        assert!(!private_was_hit);
    }

    #[test]
    fn pinned_target_preserves_original_host_for_dns_override() {
        let url = reqwest::Url::parse("http://public.test:8080/file.nzb").unwrap();
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080);
        let target = pinned_target(url, addr);

        assert_eq!(target.host, "public.test");
        assert_eq!(target.addrs, vec![addr]);
    }
}
