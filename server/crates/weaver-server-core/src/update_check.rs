//! Background release checker.
//!
//! Polls the project's public GitHub releases API on a conservative cadence and
//! publishes the result through a [`tokio::sync::watch`] channel so the GraphQL
//! query and subscription can both read the same state without re-fetching.
//!
//! Design rules this module holds to:
//!
//! * The loop is hours-scale with a jittered startup delay. A deployment fleet
//!   that all restarts at once must not turn into a synchronised burst against
//!   the API, and a failing check must never degrade into a tight retry loop.
//! * Conditional requests (`If-None-Match`) and `Retry-After` are honoured, so
//!   the steady state costs an unmetered `304` rather than a rate-limit unit.
//! * Errors never clear the last good result. A network blip sets `last_error`
//!   and leaves the previously discovered release in place.
//! * Only stable releases count. Drafts, prereleases, and semver-prerelease
//!   tags are ignored, and `update_available` is set only when the discovered
//!   version is strictly greater than the running one.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{debug, warn};

use crate::persistence::Database;

/// Releases API for this project's public repository.
///
/// `.../releases/latest` is GitHub's "latest stable" resource: it already
/// excludes drafts and prereleases. The draft/prerelease and semver-prerelease
/// filters below are kept anyway so a hand-published release cannot surprise
/// users of a stable build.
const GITHUB_LATEST_RELEASE_ENDPOINT: &str =
    "https://api.github.com/repos/scryer-media/weaver/releases/latest";

/// Release tags are cut as `weaver-v{version}` by `cargo xtask release`.
const RELEASE_TAG_PREFIX: &str = "weaver-v";

/// Settings key holding the JSON-encoded [`PersistedUpdateState`].
const UPDATE_CHECK_SETTING_KEY: &str = "update_check_state";

/// Steady-state gap between checks.
const CHECK_INTERVAL: Duration = Duration::from_secs(6 * 60 * 60);

/// Floor applied to every computed wait. Guarantees that no combination of
/// `Retry-After`, clock movement, or arithmetic can produce a tight loop.
const MIN_CHECK_INTERVAL: Duration = Duration::from_secs(15 * 60);

/// Ceiling applied to a server-provided `Retry-After`.
const MAX_RETRY_AFTER: Duration = Duration::from_secs(24 * 60 * 60);

/// Delay before the first check, so startup is never blocked on the network and
/// a fleet restart does not stampede the API.
const STARTUP_DELAY: Duration = Duration::from_secs(30);

/// Upper bound of the random jitter added to the startup delay.
const STARTUP_JITTER: Duration = Duration::from_secs(4 * 60);

/// Per-request timeout.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);

/// Cap on the response body we are willing to buffer from the API.
const MAX_RESPONSE_BYTES: usize = 1024 * 1024;

/// Snapshot of what the checker currently knows about available releases.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct UpdateStatus {
    pub current_version: String,
    pub latest_version: Option<String>,
    pub update_available: bool,
    pub release_url: Option<String>,
    pub published_at_epoch_ms: Option<i64>,
    pub checking: bool,
    pub last_checked_at_epoch_ms: Option<i64>,
    pub last_successful_check_at_epoch_ms: Option<i64>,
    pub last_error: Option<String>,
}

impl UpdateStatus {
    fn initial(current_version: String) -> Self {
        Self {
            current_version,
            latest_version: None,
            update_available: false,
            release_url: None,
            published_at_epoch_ms: None,
            checking: false,
            last_checked_at_epoch_ms: None,
            last_successful_check_at_epoch_ms: None,
            last_error: None,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum UpdateCheckError {
    #[error("failed to build update-check HTTP client: {0}")]
    HttpClient(String),
}

/// The subset of [`UpdateStatus`] that survives a restart.
///
/// Transient fields (`checking`, `last_error`) are deliberately excluded, and
/// `update_available` is recomputed against the running binary on load so an
/// upgraded install does not come back up still advertising the version it now
/// runs. The conditional-request validator rides along so a restart revalidates
/// with a `304` instead of spending a fresh rate-limit unit.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
struct PersistedUpdateState {
    #[serde(default)]
    latest_version: Option<String>,
    #[serde(default)]
    release_url: Option<String>,
    #[serde(default)]
    published_at_epoch_ms: Option<i64>,
    #[serde(default)]
    last_successful_check_at_epoch_ms: Option<i64>,
    #[serde(default)]
    etag: Option<String>,
}

/// What a single fetch attempt produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FetchOutcome {
    /// The cached validator still matches; nothing about the release changed.
    NotModified,
    /// A fresh representation. `release` is `None` when the repository has no
    /// stable release yet, or the newest one is a draft/prerelease.
    Fetched {
        release: Option<ReleaseInfo>,
        etag: Option<String>,
    },
    /// The API asked us to back off.
    RateLimited { retry_after: Option<Duration> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReleaseInfo {
    pub(crate) version: String,
    pub(crate) url: Option<String>,
    pub(crate) published_at_epoch_ms: Option<i64>,
}

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Indirection over the network call so tests can drive the state machine
/// deterministically under a paused clock without touching the network.
pub(crate) trait ReleaseFetcher: Send + Sync + 'static {
    fn fetch_latest<'a>(
        &'a self,
        etag: Option<&'a str>,
    ) -> BoxFuture<'a, Result<FetchOutcome, String>>;
}

#[derive(Clone)]
pub struct UpdateCheckService {
    inner: Arc<UpdateCheckInner>,
}

struct UpdateCheckInner {
    db: Database,
    fetcher: Arc<dyn ReleaseFetcher>,
    status: watch::Sender<UpdateStatus>,
    /// Conditional-request validator from the last successful fetch.
    etag: std::sync::Mutex<Option<String>>,
}

impl UpdateCheckService {
    pub fn new(db: Database) -> Result<Self, UpdateCheckError> {
        let fetcher = GithubReleaseFetcher::new()?;
        Ok(Self::with_fetcher(db, Arc::new(fetcher)))
    }

    pub(crate) fn with_fetcher(db: Database, fetcher: Arc<dyn ReleaseFetcher>) -> Self {
        let current_version = env!("CARGO_PKG_VERSION").to_string();
        let persisted = load_persisted_state(&db);
        let mut status = UpdateStatus::initial(current_version);
        let mut etag = None;

        if let Some(persisted) = persisted {
            etag = persisted.etag;
            status.release_url = persisted.release_url;
            status.published_at_epoch_ms = persisted.published_at_epoch_ms;
            status.last_successful_check_at_epoch_ms = persisted.last_successful_check_at_epoch_ms;
            // Recomputed rather than restored: the running binary may now be
            // newer than whatever the last session recorded.
            status.update_available =
                is_newer_than_current(persisted.latest_version.as_deref(), &status.current_version);
            status.latest_version = persisted.latest_version;
        }

        let (tx, _rx) = watch::channel(status);
        Self {
            inner: Arc::new(UpdateCheckInner {
                db,
                fetcher,
                status: tx,
                etag: std::sync::Mutex::new(etag),
            }),
        }
    }

    /// Current snapshot, as served by the `updateStatus` query.
    pub fn status(&self) -> UpdateStatus {
        self.inner.status.borrow().clone()
    }

    /// Receiver seeded with the current snapshot, so a new subscriber sees the
    /// state immediately instead of waiting for the next transition.
    pub fn subscribe(&self) -> watch::Receiver<UpdateStatus> {
        self.inner.status.subscribe()
    }

    pub fn start_background_loop(&self) -> JoinHandle<()> {
        let service = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(startup_delay()).await;
            loop {
                let wait = match service.run_check().await {
                    Ok(retry_after) => retry_after.unwrap_or(CHECK_INTERVAL),
                    Err(error) => {
                        debug!(error = %error, "release check failed");
                        CHECK_INTERVAL
                    }
                };
                tokio::time::sleep(wait.clamp(MIN_CHECK_INTERVAL, MAX_RETRY_AFTER)).await;
            }
        })
    }

    /// Run one check. Returns the server-requested backoff, if any.
    ///
    /// The `Err` arm reports the fetch failure to the caller for logging; the
    /// failure has already been folded into the published status by then.
    pub(crate) async fn run_check(&self) -> Result<Option<Duration>, String> {
        self.publish(|status| status.checking = true);

        let etag = self
            .inner
            .etag
            .lock()
            .expect("update-check etag poisoned")
            .clone();
        let result = self.inner.fetcher.fetch_latest(etag.as_deref()).await;
        let now = epoch_ms_now();

        match result {
            Ok(FetchOutcome::NotModified) => {
                self.publish(|status| {
                    status.checking = false;
                    status.last_checked_at_epoch_ms = Some(now);
                    status.last_successful_check_at_epoch_ms = Some(now);
                    status.last_error = None;
                });
                self.persist();
                Ok(None)
            }
            Ok(FetchOutcome::Fetched { release, etag }) => {
                if let Some(etag) = etag {
                    *self.inner.etag.lock().expect("update-check etag poisoned") = Some(etag);
                }
                self.publish(|status| {
                    status.checking = false;
                    status.last_checked_at_epoch_ms = Some(now);
                    status.last_successful_check_at_epoch_ms = Some(now);
                    status.last_error = None;
                    match &release {
                        Some(release) => {
                            status.update_available = is_newer_than_current(
                                Some(&release.version),
                                &status.current_version,
                            );
                            status.latest_version = Some(release.version.clone());
                            status.release_url = release.url.clone();
                            status.published_at_epoch_ms = release.published_at_epoch_ms;
                        }
                        None => {
                            // A repository with no stable release yet. Report
                            // "nothing newer" without inventing a version.
                            status.update_available = false;
                            status.latest_version = None;
                            status.release_url = None;
                            status.published_at_epoch_ms = None;
                        }
                    }
                });
                self.persist();
                Ok(None)
            }
            Ok(FetchOutcome::RateLimited { retry_after }) => {
                // Rate limiting is not a state-invalidating error: the last good
                // release survives, we simply record when we tried and back off.
                self.publish(|status| {
                    status.checking = false;
                    status.last_checked_at_epoch_ms = Some(now);
                    status.last_error = Some("release API rate limit reached".to_string());
                });
                Ok(retry_after)
            }
            Err(error) => {
                // Everything discovered previously stays exactly as it was.
                self.publish(|status| {
                    status.checking = false;
                    status.last_checked_at_epoch_ms = Some(now);
                    status.last_error = Some(error.clone());
                });
                Err(error)
            }
        }
    }

    /// Apply a transition and publish it to every subscriber.
    fn publish(&self, mutate: impl FnOnce(&mut UpdateStatus)) {
        self.inner.status.send_modify(mutate);
    }

    fn persist(&self) {
        let status = self.inner.status.borrow().clone();
        let state = PersistedUpdateState {
            latest_version: status.latest_version,
            release_url: status.release_url,
            published_at_epoch_ms: status.published_at_epoch_ms,
            last_successful_check_at_epoch_ms: status.last_successful_check_at_epoch_ms,
            etag: self
                .inner
                .etag
                .lock()
                .expect("update-check etag poisoned")
                .clone(),
        };
        let Ok(encoded) = serde_json::to_string(&state) else {
            return;
        };
        // Best-effort: a settings-write failure must not take down the loop or
        // discard the in-memory result.
        if let Err(error) = self
            .inner
            .db
            .set_setting(UPDATE_CHECK_SETTING_KEY, &encoded)
        {
            warn!(error = %error, "failed to persist release-check state");
        }
    }
}

fn load_persisted_state(db: &Database) -> Option<PersistedUpdateState> {
    match db.get_setting(UPDATE_CHECK_SETTING_KEY) {
        Ok(Some(raw)) => match serde_json::from_str(&raw) {
            Ok(state) => Some(state),
            Err(error) => {
                warn!(error = %error, "ignoring unreadable persisted release-check state");
                None
            }
        },
        Ok(None) => None,
        Err(error) => {
            warn!(error = %error, "failed to read persisted release-check state");
            None
        }
    }
}

/// Startup delay with jitter derived from the wall clock, so co-starting
/// instances spread their first request instead of firing together.
fn startup_delay() -> Duration {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    let jitter = u64::from(nanos) % STARTUP_JITTER.as_secs().max(1);
    STARTUP_DELAY + Duration::from_secs(jitter)
}

fn epoch_ms_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// Parse a release tag into a comparable stable version.
///
/// Returns `None` for tags that are not semver, and for semver prereleases
/// (`0.9.0-rc.1`) — a stable build must never be nudged onto a prerelease.
pub(crate) fn parse_stable_version(tag: &str) -> Option<semver::Version> {
    let trimmed = tag.trim();
    let stripped = trimmed
        .strip_prefix(RELEASE_TAG_PREFIX)
        .or_else(|| trimmed.strip_prefix('v'))
        .unwrap_or(trimmed);
    let version = semver::Version::parse(stripped).ok()?;
    if version.pre.is_empty() {
        Some(version)
    } else {
        None
    }
}

/// `true` only when `candidate` parses as a stable release strictly newer than
/// the running build. An unparseable candidate is never "newer".
pub(crate) fn is_newer_than_current(candidate: Option<&str>, current_version: &str) -> bool {
    let Some(candidate) = candidate.and_then(parse_stable_version) else {
        return false;
    };
    let Some(current) = parse_stable_version(current_version) else {
        return false;
    };
    candidate > current
}

/// Reqwest-backed [`ReleaseFetcher`] pointed at the public GitHub API.
struct GithubReleaseFetcher {
    client: reqwest::Client,
    endpoint: String,
}

impl GithubReleaseFetcher {
    fn new() -> Result<Self, UpdateCheckError> {
        let client = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .user_agent(concat!("weaver/", env!("CARGO_PKG_VERSION")))
            .gzip(true)
            .redirect(reqwest::redirect::Policy::limited(5))
            .build()
            .map_err(|error| UpdateCheckError::HttpClient(error.to_string()))?;
        Ok(Self {
            client,
            endpoint: GITHUB_LATEST_RELEASE_ENDPOINT.to_string(),
        })
    }
}

impl ReleaseFetcher for GithubReleaseFetcher {
    fn fetch_latest<'a>(
        &'a self,
        etag: Option<&'a str>,
    ) -> BoxFuture<'a, Result<FetchOutcome, String>> {
        Box::pin(async move {
            let mut request = self
                .client
                .get(&self.endpoint)
                .header(reqwest::header::ACCEPT, "application/vnd.github+json")
                .header("X-GitHub-Api-Version", "2022-11-28");
            if let Some(etag) = etag {
                request = request.header(reqwest::header::IF_NONE_MATCH, etag);
            }

            let response = request.send().await.map_err(|error| error.to_string())?;
            let status = response.status();

            if status == reqwest::StatusCode::NOT_MODIFIED {
                return Ok(FetchOutcome::NotModified);
            }
            if is_rate_limited(&response) {
                return Ok(FetchOutcome::RateLimited {
                    retry_after: retry_after_from_headers(response.headers()),
                });
            }
            if !status.is_success() {
                return Err(format!("release API returned HTTP {status}"));
            }

            let etag = response
                .headers()
                .get(reqwest::header::ETAG)
                .and_then(|value| value.to_str().ok())
                .map(str::to_string);
            let body = read_body_with_limit(response).await?;
            let release = parse_release_payload(&body)?;
            Ok(FetchOutcome::Fetched { release, etag })
        })
    }
}

/// GitHub signals an exhausted quota as `403`/`429` with `x-ratelimit-remaining: 0`.
fn is_rate_limited(response: &reqwest::Response) -> bool {
    if response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
        return true;
    }
    response.status() == reqwest::StatusCode::FORBIDDEN
        && response
            .headers()
            .get("x-ratelimit-remaining")
            .and_then(|value| value.to_str().ok())
            .map(|value| value.trim() == "0")
            .unwrap_or(false)
}

/// `Retry-After` is either delta-seconds or an HTTP-date; both forms are
/// accepted, and GitHub's `x-ratelimit-reset` epoch is used as a fallback.
pub(crate) fn retry_after_from_headers(headers: &reqwest::header::HeaderMap) -> Option<Duration> {
    if let Some(value) = headers
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
    {
        let value = value.trim();
        if let Ok(seconds) = value.parse::<u64>() {
            return Some(Duration::from_secs(seconds).min(MAX_RETRY_AFTER));
        }
        if let Ok(when) = httpdate::parse_http_date(value) {
            return Some(
                when.duration_since(SystemTime::now())
                    .unwrap_or_default()
                    .min(MAX_RETRY_AFTER),
            );
        }
    }

    let reset = headers
        .get("x-ratelimit-reset")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<u64>().ok())?;
    let reset_at = UNIX_EPOCH + Duration::from_secs(reset);
    Some(
        reset_at
            .duration_since(SystemTime::now())
            .unwrap_or_default()
            .min(MAX_RETRY_AFTER),
    )
}

async fn read_body_with_limit(mut response: reqwest::Response) -> Result<Vec<u8>, String> {
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| format!("release API body read failed: {error}"))?
    {
        if body.len().saturating_add(chunk.len()) > MAX_RESPONSE_BYTES {
            return Err(format!(
                "release API response exceeds {MAX_RESPONSE_BYTES} bytes"
            ));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

/// Extract the stable release from a `releases/latest` payload.
///
/// `Ok(None)` means "the payload is valid but describes nothing we would offer"
/// — a draft, a prerelease, or a tag that is not stable semver.
pub(crate) fn parse_release_payload(body: &[u8]) -> Result<Option<ReleaseInfo>, String> {
    let payload: serde_json::Value = serde_json::from_slice(body)
        .map_err(|error| format!("invalid release API response: {error}"))?;

    if payload
        .get("draft")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
        || payload
            .get("prerelease")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
    {
        return Ok(None);
    }

    let Some(tag) = payload.get("tag_name").and_then(serde_json::Value::as_str) else {
        return Ok(None);
    };
    let Some(version) = parse_stable_version(tag) else {
        return Ok(None);
    };

    let url = payload
        .get("html_url")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string);
    let published_at_epoch_ms = payload
        .get("published_at")
        .and_then(serde_json::Value::as_str)
        .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
        .map(|value| value.timestamp_millis());

    Ok(Some(ReleaseInfo {
        version: version.to_string(),
        url,
        published_at_epoch_ms,
    }))
}

#[cfg(test)]
mod tests;
