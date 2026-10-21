// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! The torrent client compatibility facade: the WebUI API v2 shape that
//! torrent automation drives, served beside the first-party GraphQL API.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use axum::Router;
use axum::extract::{ConnectInfo, Extension, FromRequest, Multipart, Query, Request};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use serde_json::{Map, Value, json};

use weaver_server_core::auth::{
    ApiKeyCache, CallerScope, LoginAuthCache, generate_api_key, hash_api_key,
};
use weaver_server_core::categories::{
    CategoryConfig, completion_parent, validate_category_path_component,
};
use weaver_server_core::ingest::{Kind, classify, fetch_torrent_url};
use weaver_server_core::proxies::{Bandwidth, TorrentScope};
use weaver_server_core::security::RuntimeSecurityConfig;
use weaver_server_core::settings::model::{Config, SharedConfig};
use weaver_server_core::settings::{
    SeedingAction, SeedingPolicy, TorrentConfig, TorrentEncryption, TorrentLimit,
    TorrentPeerProtocol, resolve_seeding_policy,
};
use weaver_server_core::torrents::model::{file_priority_number, file_priority_of};
use weaver_server_core::torrents::service::{
    ActionError, AddError, AddRequest, SeedingSource, TorrentService, TorrentSettings, TorrentView,
};
use weaver_server_core::torrents::{
    ConnectionKind, Encryption, FilePriority, PeerInfo, PeerSourceFlag, TorrentState,
    TorrentTracker, TrackerStatus, status,
};
use weaver_server_core::watch_folder::WatchFolderService;
use weaver_server_core::{Database, SchedulerHandle};

use super::auth::{LoginRateLimiter, login_client_id};

#[cfg(test)]
mod tests;

pub(super) const WEBAPI_VERSION: &str = "2.11.0";
const BODY_LIMIT_BYTES: usize = 32 * 1024 * 1024;
const SESSION_CAP: usize = 1024;
// The client's "unknown" ETA: 100 days.
const ETA_UNKNOWN: u64 = 8_640_000;

#[derive(Clone)]
pub(super) struct TorrentFacadeContext {
    inner: Arc<Inner>,
}

struct Inner {
    db: Database,
    handle: SchedulerHandle,
    config: SharedConfig,
    auth_cache: LoginAuthCache,
    api_key_cache: ApiKeyCache,
    session_token: super::SessionToken,
    security: RuntimeSecurityConfig,
    watch_folder: WatchFolderService,
    torrents: Option<TorrentService>,
    // Login cookie -> the hash of the API key it was issued for, looked up
    // again on every request so a revoked key ends its sessions.
    sessions: Mutex<SessionTable>,
    rid: AtomicU64,
    categories: tokio::sync::Mutex<()>,
}

#[derive(Default)]
struct SessionTable {
    keys: HashMap<String, (u64, [u8; 32])>,
    issued: u64,
}

impl SessionTable {
    fn issue(&mut self, key: [u8; 32]) -> String {
        if self.keys.len() >= SESSION_CAP
            && let Some(oldest) = self
                .keys
                .iter()
                .min_by_key(|(_, (issued, _))| *issued)
                .map(|(sid, _)| sid.clone())
        {
            self.keys.remove(&oldest);
        }
        self.issued += 1;
        let sid = generate_api_key();
        self.keys.insert(sid.clone(), (self.issued, key));
        sid
    }
}

impl TorrentFacadeContext {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        db: Database,
        handle: SchedulerHandle,
        config: SharedConfig,
        auth_cache: LoginAuthCache,
        api_key_cache: ApiKeyCache,
        session_token: super::SessionToken,
        security: RuntimeSecurityConfig,
        watch_folder: WatchFolderService,
        torrents: Option<TorrentService>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                db,
                handle,
                config,
                auth_cache,
                api_key_cache,
                session_token,
                security,
                watch_folder,
                torrents,
                sessions: Mutex::new(SessionTable::default()),
                rid: AtomicU64::new(0),
                categories: tokio::sync::Mutex::new(()),
            }),
        }
    }

    fn sessions(&self) -> std::sync::MutexGuard<'_, SessionTable> {
        self.inner
            .sessions
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    // The download limit torrents are held to this tick.
    fn torrent_download_limit(&self) -> Option<u64> {
        self.inner.handle.state().bandwidth_split().rates.torrent
    }

    // Every torrent with its files, which its paths follow.
    async fn views(&self) -> Vec<TorrentView> {
        let Some(torrents) = &self.inner.torrents else {
            return Vec::new();
        };
        let mut views = Vec::new();
        for view in torrents.torrents(None).await {
            views.extend(torrents.torrent(view.job_id).await);
        }
        views
    }

    async fn by_hash(&self, hash: &str) -> Option<TorrentView> {
        let torrents = self.inner.torrents.as_ref()?;
        let job_id = torrents
            .torrents(None)
            .await
            .into_iter()
            .find(|view| view.infohash.eq_ignore_ascii_case(hash.trim()))?
            .job_id;
        torrents.torrent(job_id).await
    }

    // The torrents a `hashes` parameter names: `|`-separated, or `all`.
    async fn targets(&self, params: &Params) -> Vec<TorrentView> {
        let wanted = params.get("hashes").unwrap_or_default();
        let views = self.views().await;
        if names_all(params) {
            return views;
        }
        let wanted: Vec<&str> = wanted.split('|').map(str::trim).collect();
        views
            .into_iter()
            .filter(|view| {
                wanted
                    .iter()
                    .any(|hash| view.infohash.eq_ignore_ascii_case(hash))
            })
            .collect()
    }

    async fn resolve(&self, key: &str) -> Result<CallerScope, StatusCode> {
        let mut headers = HeaderMap::new();
        let value =
            HeaderValue::from_str(&format!("Bearer {key}")).map_err(|_| StatusCode::FORBIDDEN)?;
        headers.insert(header::AUTHORIZATION, value);
        self.resolve_headers(&headers).await
    }

    async fn resolve_headers(&self, headers: &HeaderMap) -> Result<CallerScope, StatusCode> {
        let inner = &self.inner;
        super::auth::resolve_caller(
            &inner.db,
            &inner.auth_cache,
            &inner.api_key_cache,
            &inner.session_token.0,
            &inner.security,
            super::auth::BrowserSessionPolicy::Denied,
            headers,
        )
        .await
        .map(|caller| caller.scope)
    }

    // A session's key, by its hash, as the API key check finds it.
    async fn resolve_key_hash(
        &self,
        key_hash: [u8; 32],
    ) -> Result<Option<CallerScope>, StatusCode> {
        let inner = &self.inner;
        let row =
            super::auth::lookup_api_key_auth(&inner.db, &inner.api_key_cache, key_hash).await?;
        Ok(row.map(|row| {
            super::auth::queue_touch_api_key_last_used(&inner.db, row.id);
            super::auth::caller_scope_from_api_key_scope(&row.scope)
        }))
    }

    // The caller: the login cookie, else an API key sent with the request
    // (bearer, `X-Api-Key`, or the password of basic auth). A cookie this
    // facade does not know leaves the request to its other credentials.
    async fn caller(&self, headers: &HeaderMap) -> Result<CallerScope, StatusCode> {
        let session = cookie(headers, "SID").and_then(|sid| {
            let key_hash = self
                .sessions()
                .keys
                .get(&sid)
                .map(|(_, key_hash)| *key_hash)?;
            Some((sid, key_hash))
        });
        if let Some((sid, key_hash)) = session {
            match self.resolve_key_hash(key_hash).await? {
                Some(scope) => return Ok(scope),
                None => {
                    self.sessions().keys.remove(&sid);
                }
            }
        }
        if let Some(password) = basic_password(headers) {
            return self.resolve(&password).await;
        }
        let mut headers = headers.clone();
        headers.remove(header::COOKIE);
        self.resolve_headers(&headers).await
    }
}

pub(super) fn build_torrent_facade_routes(context: TorrentFacadeContext) -> Router {
    Router::new()
        .route("/api/v2/{group}/{action}", any(handle))
        .route_layer(axum::extract::DefaultBodyLimit::max(BODY_LIMIT_BYTES))
        .layer(Extension(context))
}

fn cookie(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(';'))
        .filter_map(|pair| pair.trim().split_once('='))
        .find(|(key, _)| *key == name)
        .map(|(_, value)| value.to_owned())
        .filter(|value| !value.is_empty())
}

fn basic_password(headers: &HeaderMap) -> Option<String> {
    let value = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    let (scheme, encoded) = value.trim_start().split_once(char::is_whitespace)?;
    if !scheme.eq_ignore_ascii_case("basic") {
        return None;
    }
    let decoded = String::from_utf8(BASE64_STANDARD.decode(encoded.trim()).ok()?).ok()?;
    let (_, password) = decoded.split_once(':')?;
    Some(password.trim().to_owned()).filter(|password| !password.is_empty())
}

// A request's parameters: the query string, then the form body over it.
#[derive(Default)]
struct Params {
    fields: HashMap<String, String>,
    files: Vec<Vec<u8>>,
}

impl Params {
    fn get(&self, name: &str) -> Option<&str> {
        self.fields.get(name).map(String::as_str)
    }

    fn flag(&self, name: &str) -> Option<bool> {
        self.get(name)
            .map(|value| value.trim().eq_ignore_ascii_case("true"))
    }

    async fn read(request: Request) -> Result<Self, Response> {
        let mut params = Params::default();
        if let Ok(Query(query)) = Query::<HashMap<String, String>>::try_from_uri(request.uri()) {
            params.fields.extend(query);
        }
        if request.method() == Method::GET {
            return Ok(params);
        }
        let content_type = request
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_ascii_lowercase();
        if content_type.starts_with("multipart/form-data") {
            let mut multipart = Multipart::from_request(request, &())
                .await
                .map_err(IntoResponse::into_response)?;
            while let Some(field) = multipart
                .next_field()
                .await
                .map_err(IntoResponse::into_response)?
            {
                let name = field.name().unwrap_or_default().to_owned();
                let is_file = field.file_name().is_some();
                let bytes = field.bytes().await.map_err(IntoResponse::into_response)?;
                if name == "torrents" && is_file {
                    params.files.push(bytes.to_vec());
                } else {
                    params
                        .fields
                        .insert(name, String::from_utf8_lossy(&bytes).into_owned());
                }
            }
        } else if content_type.starts_with("application/x-www-form-urlencoded") {
            let axum::Form(form) =
                axum::Form::<HashMap<String, String>>::from_request(request, &())
                    .await
                    .map_err(IntoResponse::into_response)?;
            params.fields.extend(form);
        }
        Ok(params)
    }
}

fn text(status: StatusCode, body: impl Into<String>) -> Response {
    (
        status,
        [(header::CONTENT_TYPE, "text/plain; charset=UTF-8")],
        body.into(),
    )
        .into_response()
}

fn ok() -> Response {
    text(StatusCode::OK, "")
}

fn forbidden() -> Response {
    text(StatusCode::FORBIDDEN, "Forbidden")
}

// A failure inside weaver: logged, never shown to the caller.
// Whether a `hashes` parameter names every torrent.
fn names_all(params: &Params) -> bool {
    params.get("hashes").unwrap_or_default().trim() == "all"
}

fn held(result: Result<(), weaver_server_core::SchedulerError>) -> Response {
    match result {
        Ok(()) => ok(),
        Err(error) => internal(error),
    }
}

fn internal(error: impl std::fmt::Display) -> Response {
    tracing::warn!(%error, "torrent client compatibility request failed");
    text(StatusCode::INTERNAL_SERVER_ERROR, "internal error")
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Need {
    Read,
    Control,
}

fn need(group: &str, action: &str) -> Option<Need> {
    Some(match (group, action) {
        ("app", "version" | "webapiVersion" | "buildInfo" | "preferences" | "defaultSavePath")
        | (
            "torrents",
            "info" | "properties" | "files" | "trackers" | "categories" | "tags" | "uploadLimit"
            | "downloadLimit",
        )
        | ("sync", "maindata" | "torrentPeers")
        | ("transfer", "info")
        | ("log", "main") => Need::Read,
        ("transfer", "banPeers") => Need::Control,
        ("app", "setPreferences")
        | (
            "torrents",
            "add" | "delete" | "pause" | "stop" | "resume" | "start" | "setCategory"
            | "setForceStart" | "createCategory" | "setLocation" | "setShareLimits" | "topPrio"
            | "addTags" | "removeTags" | "recheck" | "reannounce" | "setUploadLimit"
            | "setDownloadLimit",
        )
        | ("torrents", "filePrio" | "toggleSequentialDownload" | "toggleFirstLastPiecePrio") => {
            Need::Control
        }
        _ => return None,
    })
}

async fn handle(
    Extension(ctx): Extension<TorrentFacadeContext>,
    Extension(login_limiter): Extension<LoginRateLimiter>,
    peer: Option<Extension<ConnectInfo<SocketAddr>>>,
    axum::extract::Path((group, action)): axum::extract::Path<(String, String)>,
    request: Request,
) -> Response {
    match (group.as_str(), action.as_str()) {
        ("auth", "login") => {
            let peer = peer.map(|Extension(ConnectInfo(peer))| peer);
            let client_id = login_client_id(&ctx.inner.security, request.headers(), peer);
            return match Params::read(request).await {
                Ok(params) => login(&ctx, &login_limiter, &client_id, &params).await,
                Err(response) => response,
            };
        }
        ("auth", "logout") => {
            if let Some(sid) = cookie(request.headers(), "SID") {
                ctx.sessions().keys.remove(&sid);
            }
            return ok();
        }
        _ => {}
    }
    // The caller is known before any of the body is read.
    let Ok(scope) = ctx.caller(request.headers()).await else {
        return forbidden();
    };
    let Some(need) = need(&group, &action) else {
        return text(StatusCode::NOT_FOUND, "Not Found");
    };
    let allowed = match need {
        Need::Read => scope.can_read(),
        Need::Control => scope.can_control(),
    };
    if !allowed {
        return forbidden();
    }
    let params = match Params::read(request).await {
        Ok(params) => params,
        Err(response) => return response,
    };
    match (group.as_str(), action.as_str()) {
        ("app", "version") => text(StatusCode::OK, format!("v{}", env!("CARGO_PKG_VERSION"))),
        ("app", "webapiVersion") => text(StatusCode::OK, WEBAPI_VERSION),
        ("app", "buildInfo") => axum::Json(json!({
            "qt": "", "libtorrent": "", "boost": "", "openssl": "", "zlib": "",
            "bitness": 64,
        }))
        .into_response(),
        ("app", "preferences") => {
            let config = ctx.inner.config.read().await;
            let mut dht = false;
            if let Some(torrents) = ctx.inner.torrents.as_ref() {
                for scope in std::iter::once(TorrentScope::Default).chain(
                    config
                        .categories
                        .iter()
                        .map(|c| TorrentScope::Category(c.id)),
                ) {
                    if torrents.carries_dht(scope).await {
                        dht = true;
                        break;
                    }
                }
            }
            axum::Json(preferences(&config, dht)).into_response()
        }
        ("app", "setPreferences") => set_preferences(&ctx, &params).await,
        ("app", "defaultSavePath") => {
            text(StatusCode::OK, ctx.inner.config.read().await.complete_dir())
        }
        ("torrents", "info") => torrents_info(&ctx, &params).await,
        ("torrents", "properties") => properties(&ctx, &params).await,
        ("torrents", "files") => files(&ctx, &params).await,
        ("torrents", "trackers") => trackers(&ctx, &params).await,
        ("torrents", "add") => add(&ctx, params).await,
        ("torrents", "delete") => delete(&ctx, &params).await,
        // Every torrent at once is one hold, which leaves each torrent's
        // own pause or resume standing; named torrents are each paused.
        ("torrents", "pause" | "stop") if names_all(&params) => {
            held(ctx.inner.handle.pause_torrents().await)
        }
        ("torrents", "resume" | "start") if names_all(&params) => {
            held(ctx.inner.handle.resume_torrents().await)
        }
        ("torrents", "pause" | "stop") => {
            each(&ctx, &params, |torrents, view| async move {
                torrents.pause(view.job_id).await
            })
            .await
        }
        ("torrents", "resume" | "start") => {
            each(&ctx, &params, |torrents, view| async move {
                torrents.resume(view.job_id).await
            })
            .await
        }
        ("torrents", "setForceStart") => {
            let forced = params.flag("value").unwrap_or(false);
            each(&ctx, &params, move |torrents, view| async move {
                torrents.set_forced(view.job_id, forced).await
            })
            .await
        }
        ("torrents", "filePrio") => file_prio(&ctx, &params).await,
        ("torrents", "toggleSequentialDownload") => {
            each(&ctx, &params, |torrents, view| async move {
                torrents.set_sequential(view.job_id, !view.sequential).await
            })
            .await
        }
        ("torrents", "toggleFirstLastPiecePrio") => {
            each(&ctx, &params, |torrents, view| async move {
                torrents
                    .set_first_last_piece_first(view.job_id, !view.first_last_piece_first)
                    .await
            })
            .await
        }
        ("torrents", "setCategory") => {
            let category = params
                .get("category")
                .map(str::trim)
                .filter(|name| !name.is_empty())
                .map(str::to_owned);
            each(&ctx, &params, move |torrents, view| {
                let category = category.clone();
                async move { torrents.set_category(view.job_id, category).await }
            })
            .await
        }
        ("torrents", "recheck") => {
            each(&ctx, &params, |torrents, view| async move {
                torrents.recheck(view.job_id).await
            })
            .await
        }
        ("torrents", "categories") => {
            axum::Json(categories(&*ctx.inner.config.read().await)).into_response()
        }
        ("torrents", "createCategory") => create_category(&ctx, &params).await,
        ("torrents", "setShareLimits") => set_share_limits(&ctx, &params).await,
        ("torrents", "uploadLimit") => rate_limits(&ctx, &params, |view| view.upload_limit).await,
        ("torrents", "downloadLimit") => {
            rate_limits(&ctx, &params, |view| view.download_limit).await
        }
        ("torrents", "setUploadLimit") => set_rate_limit(&ctx, &params, Direction::Upload).await,
        ("torrents", "setDownloadLimit") => {
            set_rate_limit(&ctx, &params, Direction::Download).await
        }
        ("torrents", "topPrio") => text(StatusCode::CONFLICT, "queueing is off"),
        ("torrents", "setLocation") => {
            let Some(location) = params
                .get("location")
                .map(str::trim)
                .filter(|path| !path.is_empty())
                .map(str::to_owned)
            else {
                return text(StatusCode::BAD_REQUEST, "Save path cannot be empty");
            };
            each(&ctx, &params, move |torrents, view| {
                let location = location.clone();
                async move { torrents.set_location(view.job_id, location).await }
            })
            .await
        }
        // Tags are not kept; reannounce has no service action.
        ("torrents", "addTags" | "removeTags" | "reannounce") => ok(),
        ("torrents", "tags") | ("log", "main") => axum::Json(json!([])).into_response(),
        ("sync", "maindata") => maindata(&ctx).await,
        ("sync", "torrentPeers") => torrent_peers(&ctx, &params).await,
        ("transfer", "banPeers") => ban_peers(&ctx, &params).await,
        ("transfer", "info") => {
            let views = ctx.views().await;
            let upload = ctx.inner.config.read().await.bandwidth.upload_bps;
            axum::Json(transfer(&views, ctx.torrent_download_limit(), upload)).into_response()
        }
        _ => text(StatusCode::NOT_FOUND, "Not Found"),
    }
}

async fn login(
    ctx: &TorrentFacadeContext,
    login_limiter: &LoginRateLimiter,
    client_id: &str,
    params: &Params,
) -> Response {
    let username = params.get("username").unwrap_or_default();
    if login_limiter.too_many_failures(username, client_id) {
        return super::error_response(StatusCode::TOO_MANY_REQUESTS, "too many login attempts");
    }
    let password = params.get("password").unwrap_or_default().trim();
    if password.is_empty() || ctx.resolve(password).await.is_err() {
        login_limiter.record_failure(username, client_id);
        return text(StatusCode::OK, "Fails.");
    }
    login_limiter.record_success(username, client_id);
    let sid = ctx.sessions().issue(hash_api_key(password));
    let mut response = text(StatusCode::OK, "Ok.");
    let secure = if ctx.inner.security.secure_cookies {
        "; Secure"
    } else {
        ""
    };
    if let Ok(value) = HeaderValue::from_str(&format!(
        "SID={sid}; HttpOnly; SameSite=Strict; path=/{secure}"
    )) {
        response.headers_mut().insert(header::SET_COOKIE, value);
    }
    response
}

/// The client state a torrent's weaver state reads as.
pub(super) fn state(view: &TorrentView) -> &'static str {
    let complete = view.completed_at_ms.is_some();
    let side = |download: &'static str, upload: &'static str| {
        if complete { upload } else { download }
    };
    match view.state {
        // The client has no hold on its queue: a held torrent is stopped.
        TorrentState::Queued if view.paused_by.is_some() => side("stoppedDL", "stoppedUP"),
        TorrentState::Queued => side("queuedDL", "queuedUP"),
        TorrentState::FetchingMetadata => "metaDL",
        TorrentState::CheckingFiles => side("checkingDL", "checkingUP"),
        TorrentState::Downloading if view.forced => "forcedDL",
        TorrentState::Downloading => "downloading",
        TorrentState::Stalled => side("stalledDL", "stalledUP"),
        TorrentState::Seeding if view.forced => "forcedUP",
        TorrentState::Seeding if view.upload_bps > 0 => "uploading",
        TorrentState::Seeding => "stalledUP",
        TorrentState::Paused => side("stoppedDL", "stoppedUP"),
        TorrentState::Moving => "moving",
        // A stop with any reason but its met goal is a failure.
        TorrentState::Stopped => match view.detail.as_deref() {
            None | Some(status::GOAL_MET) => side("stoppedDL", "stoppedUP"),
            Some(_) => "error",
        },
    }
}

fn category_dir(config: &Config, category: Option<&str>) -> PathBuf {
    let complete = PathBuf::from(config.complete_dir());
    completion_parent(&complete, &config.categories, category).unwrap_or(complete)
}

fn secs(ms: i64) -> i64 {
    ms.div_euclid(1000)
}

fn eta(view: &TorrentView) -> u64 {
    if view.completed_at_ms.is_some() {
        return ETA_UNKNOWN;
    }
    view.eta_seconds.unwrap_or(ETA_UNKNOWN)
}

// The torrent's share limits (`-2` where it inherits the global policy the
// preferences report) and the limits in force; `-1` is no goal.
fn share_limits(view: &TorrentView) -> [(&'static str, Value); 6] {
    let policy = &view.seeding_policy;
    let minutes = |value: Option<u32>| json!(value.map_or(-1, i64::from));
    let (ratio, seeding, inactive) = (
        policy
            .ratio_goal
            .map_or(json!(-1), |ratio| json!(f64::from(ratio))),
        minutes(policy.seed_time_goal_minutes),
        minutes(policy.inactive_goal_minutes),
    );
    let own = |value: &Value| match view.seeding_source {
        SeedingSource::Global => json!(-2),
        SeedingSource::Torrent | SeedingSource::TrackerRule(_) | SeedingSource::Category => {
            value.clone()
        }
    };
    [
        ("ratio_limit", own(&ratio)),
        ("seeding_time_limit", own(&seeding)),
        ("inactive_seeding_time_limit", own(&inactive)),
        ("max_ratio", ratio),
        ("max_seeding_time", seeding),
        ("max_inactive_seeding_time", inactive),
    ]
}

pub(super) fn info(view: &TorrentView) -> Value {
    let (save_path, content_path) = view.paths();
    let mut fields = object([
        ("hash", json!(view.infohash.to_ascii_lowercase())),
        (
            "infohash_v1",
            json!(view.infohash_v1.clone().unwrap_or_default()),
        ),
        (
            "infohash_v2",
            json!(view.infohash_v2.clone().unwrap_or_default()),
        ),
        ("name", json!(view.name)),
        ("size", json!(view.total_bytes)),
        ("total_size", json!(view.total_bytes)),
        ("progress", json!(view.progress.clamp(0.0, 1.0))),
        ("dlspeed", json!(view.download_bps)),
        ("upspeed", json!(view.upload_bps)),
        ("priority", json!(0)),
        ("num_seeds", json!(view.seeds)),
        ("num_leechs", json!(view.leechers)),
        ("num_complete", json!(view.seeds)),
        ("num_incomplete", json!(view.leechers)),
        ("ratio", json!(view.ratio)),
        ("eta", json!(eta(view))),
        ("state", json!(state(view))),
        ("category", json!(view.category.clone().unwrap_or_default())),
        ("tags", json!("")),
        ("added_on", json!(secs(view.added_at_ms))),
        (
            "completion_on",
            json!(view.completed_at_ms.map_or(-1, secs)),
        ),
        ("save_path", json!(save_path)),
        ("content_path", json!(content_path)),
        ("download_path", json!("")),
        ("downloaded", json!(view.downloaded_bytes)),
        ("uploaded", json!(view.uploaded_bytes)),
        ("completed", json!(view.have_bytes)),
        (
            "amount_left",
            json!(view.total_bytes.saturating_sub(view.have_bytes)),
        ),
        ("force_start", json!(view.forced)),
        ("private", json!(view.private)),
        ("auto_tmm", json!(false)),
        ("super_seeding", json!(false)),
        ("seq_dl", json!(view.sequential)),
        ("f_l_piece_prio", json!(view.first_last_piece_first)),
        ("tracker", json!("")),
        ("magnet_uri", json!(view.magnet)),
    ]);
    // The limits as the client keeps them, 0 for none.
    fields = with_limits(
        fields,
        0,
        [
            ("dl_limit", view.download_limit),
            ("up_limit", view.upload_limit),
        ],
    );
    if let Some(fields) = fields.as_object_mut() {
        fields.extend(
            share_limits(view)
                .into_iter()
                .map(|(name, value)| (name.to_owned(), value)),
        );
    }
    fields
}

fn wanted(view: &TorrentView, filter: &str) -> bool {
    let state = state(view);
    let complete = view.completed_at_ms.is_some();
    let active = view.download_bps > 0 || view.upload_bps > 0;
    match filter {
        "" | "all" => true,
        "downloading" => !complete && !matches!(state, "stoppedDL" | "error"),
        "seeding" => matches!(
            state,
            "uploading" | "stalledUP" | "forcedUP" | "queuedUP" | "checkingUP"
        ),
        "completed" => complete,
        "paused" | "stopped" => matches!(state, "stoppedDL" | "stoppedUP"),
        "resumed" | "running" => !matches!(state, "stoppedDL" | "stoppedUP"),
        "active" => active,
        "inactive" => !active,
        "stalled" => matches!(state, "stalledDL" | "stalledUP"),
        "stalled_uploading" => state == "stalledUP",
        "stalled_downloading" => state == "stalledDL",
        "checking" => matches!(state, "checkingDL" | "checkingUP"),
        "errored" => state == "error",
        _ => false,
    }
}

async fn torrents_info(ctx: &TorrentFacadeContext, params: &Params) -> Response {
    let views = ctx.views().await;
    let hashes: Option<Vec<&str>> = params
        .get("hashes")
        .map(|hashes| hashes.split('|').map(str::trim).collect());
    let filter = params.get("filter").unwrap_or_default();
    let list: Vec<Value> = views
        .iter()
        .filter(|view| match params.get("category") {
            None => true,
            Some(name) => view
                .category
                .as_deref()
                .unwrap_or_default()
                .eq_ignore_ascii_case(name),
        })
        .filter(|view| {
            hashes.as_ref().is_none_or(|hashes| {
                hashes
                    .iter()
                    .any(|hash| view.infohash.eq_ignore_ascii_case(hash))
            })
        })
        .filter(|view| wanted(view, filter))
        .map(info)
        .collect();
    axum::Json(list).into_response()
}

fn object<const N: usize>(fields: [(&str, Value); N]) -> Value {
    Value::Object(
        fields
            .into_iter()
            .map(|(name, value)| (name.to_owned(), value))
            .collect(),
    )
}

fn not_found() -> Response {
    text(StatusCode::NOT_FOUND, "Torrent hash was not found")
}

async fn properties(ctx: &TorrentFacadeContext, params: &Params) -> Response {
    let Some(view) = ctx.by_hash(params.get("hash").unwrap_or_default()).await else {
        return not_found();
    };
    let (save_path, content_path) = view.paths();
    let fields = object([
        ("hash", json!(view.infohash.to_ascii_lowercase())),
        (
            "infohash_v1",
            json!(view.infohash_v1.clone().unwrap_or_default()),
        ),
        (
            "infohash_v2",
            json!(view.infohash_v2.clone().unwrap_or_default()),
        ),
        ("name", json!(view.name)),
        ("save_path", json!(save_path)),
        ("content_path", json!(content_path)),
        ("download_path", json!("")),
        ("is_private", json!(view.private)),
        ("comment", json!(view.comment.clone().unwrap_or_default())),
        (
            "created_by",
            json!(view.created_by.clone().unwrap_or_default()),
        ),
        ("creation_date", json!(view.created_at_ms.map_or(-1, secs))),
        (
            "piece_size",
            json!(view.pieces.map_or(-1, |pieces| pieces.size as i64)),
        ),
        (
            "pieces_have",
            json!(view.pieces.map_or(-1, |pieces| i64::from(pieces.have))),
        ),
        (
            "pieces_num",
            json!(view.pieces.map_or(-1, |pieces| i64::from(pieces.count))),
        ),
        ("total_size", json!(view.total_bytes)),
        ("total_wasted", json!(view.wasted_bytes)),
        ("total_uploaded", json!(view.uploaded_bytes)),
        ("total_uploaded_session", json!(view.uploaded_bytes)),
        ("total_downloaded", json!(view.downloaded_bytes)),
        ("total_downloaded_session", json!(view.downloaded_bytes)),
        ("time_elapsed", json!(view.active_seconds)),
        ("nb_connections", json!(view.peers)),
        ("nb_connections_limit", json!(view.max_peers)),
        ("share_ratio", json!(view.ratio)),
        ("addition_date", json!(secs(view.added_at_ms))),
        (
            "completion_date",
            json!(view.completed_at_ms.map_or(-1, secs)),
        ),
        (
            "last_seen",
            json!(view.last_seen_complete_ms.map_or(-1, secs)),
        ),
        ("dl_speed", json!(view.download_bps)),
        ("dl_speed_avg", json!(view.download_avg_bps)),
        ("up_speed", json!(view.upload_bps)),
        ("up_speed_avg", json!(view.upload_avg_bps)),
        ("eta", json!(eta(&view))),
        ("peers", json!(view.leechers)),
        (
            "peers_total",
            json!(view.swarm_leechers.unwrap_or(view.leechers)),
        ),
        ("seeds", json!(view.seeds)),
        ("seeds_total", json!(view.swarm_seeds.unwrap_or(view.seeds))),
        ("reannounce", json!(view.reannounce_seconds.unwrap_or(0))),
        ("seq_dl", json!(view.sequential)),
        ("f_l_piece_prio", json!(view.first_last_piece_first)),
    ]);
    // The properties read no limit as -1, where the list reads it as 0.
    let fields = with_limits(
        fields,
        -1,
        [
            ("up_limit", view.upload_limit),
            ("dl_limit", view.download_limit),
        ],
    );
    axum::Json(fields).into_response()
}

async fn files(ctx: &TorrentFacadeContext, params: &Params) -> Response {
    let Some(view) = ctx.by_hash(params.get("hash").unwrap_or_default()).await else {
        return not_found();
    };
    let list: Vec<Value> = view
        .files
        .iter()
        .map(|file| {
            let progress = view.file_progress(file);
            json!({
                "index": file.index,
                "name": file.path,
                "size": file.length,
                "progress": progress,
                "priority": qbt_priority(file.priority),
                "is_seed": view.completed_at_ms.is_some(),
                "piece_range": [0, 0],
                "availability": -1,
            })
        })
        .collect();
    axum::Json(list).into_response()
}

// A file's level as the client numbers it.
fn qbt_priority(priority: FilePriority) -> u8 {
    file_priority_number(priority)
}

// The client's number as a level; the four it defines and nothing else.
fn level_of(priority: &str) -> Option<FilePriority> {
    file_priority_of(priority.trim().parse().ok()?)
}

// Set the level of the files `id` names, `|`-separated, on the torrent
// `hash` names (qBittorrent `TorrentsController::filePrioAction`): a missing
// parameter, a level the client does not define or an id that is no integer
// is 400, an unknown torrent 404, and an id the torrent has no file at,
// negative or past its last, 409.
async fn file_prio(ctx: &TorrentFacadeContext, params: &Params) -> Response {
    let (Some(hash), Some(ids), Some(priority)) =
        (params.get("hash"), params.get("id"), params.get("priority"))
    else {
        return text(StatusCode::BAD_REQUEST, "Missing required parameters");
    };
    let Some(level) = level_of(priority) else {
        return text(StatusCode::BAD_REQUEST, "Priority is not valid");
    };
    let Ok(ids) = ids
        .split('|')
        .map(|id| id.trim().parse::<i64>())
        .collect::<Result<Vec<_>, _>>()
    else {
        return text(StatusCode::BAD_REQUEST, "File IDs must be integers");
    };
    let Some(view) = ctx.by_hash(hash).await else {
        return not_found();
    };
    let Some(torrents) = &ctx.inner.torrents else {
        return not_found();
    };
    let Ok(priorities) = ids
        .into_iter()
        .map(|id| u32::try_from(id).map(|id| (id, level)))
        .collect::<Result<Vec<_>, _>>()
    else {
        return text(StatusCode::CONFLICT, "File ID is not valid");
    };
    match torrents.set_file_priorities(view.job_id, &priorities).await {
        Ok(()) => ok(),
        Err(ActionError::NotFound(_)) => not_found(),
        Err(error @ ActionError::Invalid(_)) => text(StatusCode::CONFLICT, error.to_string()),
        Err(error) => internal(error),
    }
}

fn tracker_status(status: TrackerStatus) -> u8 {
    match status {
        TrackerStatus::Disabled => 0,
        TrackerStatus::NotContacted => 1,
        TrackerStatus::Working => 2,
        TrackerStatus::Updating => 3,
        TrackerStatus::NotWorking => 4,
    }
}

async fn trackers(ctx: &TorrentFacadeContext, params: &Params) -> Response {
    let Some(view) = ctx.by_hash(params.get("hash").unwrap_or_default()).await else {
        return not_found();
    };
    let list: Vec<Value> = view.trackers.iter().map(tracker_json).collect();
    axum::Json(list).into_response()
}

fn tracker_json(tracker: &TorrentTracker) -> Value {
    let count = |value: Option<u32>| value.map_or(-1, i64::from);
    json!({
        "url": tracker.url,
        "status": tracker_status(tracker.status),
        "tier": tracker.tier,
        "num_peers": count(tracker.leechers.zip(tracker.seeders).map(|(l, s)| l + s)),
        "num_seeds": count(tracker.seeders),
        "num_leeches": count(tracker.leechers),
        "num_downloaded": -1,
        "msg": tracker.message.clone().unwrap_or_default(),
    })
}

async fn add(ctx: &TorrentFacadeContext, params: Params) -> Response {
    let Some(torrents) = &ctx.inner.torrents else {
        return text(StatusCode::CONFLICT, "torrents are not enabled");
    };
    let category = params
        .get("category")
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(str::to_owned);
    let policy_override = {
        let config = ctx.inner.config.read().await;
        match share_policy(&params, inherited(&config, category.as_deref()), None) {
            Ok(policy) => policy,
            Err(response) => return response,
        }
    };
    // Each file, and each url's text, is read for what it is. A url is
    // fetched on the route its torrent takes, with the client's cookie.
    let mut bodies = params.files.clone();
    for line in params
        .get("urls")
        .unwrap_or_default()
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
    {
        let Kind::Url(url) = classify(line.as_bytes()) else {
            bodies.push(line.as_bytes().to_vec());
            continue;
        };
        let Some(runtime) = ctx.inner.handle.proxy_runtime() else {
            return text(StatusCode::CONFLICT, "torrent request has no route");
        };
        let scope = torrents.scope_of(category.as_deref());
        match fetch_torrent_url(&runtime, scope, &url, params.get("cookie")).await {
            Ok(body) => bodies.push(body),
            Err(error) => return text(StatusCode::CONFLICT, error),
        }
    }
    let mut sources = Vec::with_capacity(bodies.len());
    for body in bodies {
        match classify(&body).torrent(body) {
            Ok(source) => sources.push(source),
            Err(_) => return text(StatusCode::UNSUPPORTED_MEDIA_TYPE, "not a torrent"),
        }
    }
    if sources.is_empty() {
        return text(StatusCode::BAD_REQUEST, "no torrents or urls");
    }
    let paused = params
        .flag("stopped")
        .or_else(|| params.flag("paused"))
        .unwrap_or(false);
    let mut added = 0usize;
    let mut refusal: Option<Response> = None;
    for source in sources {
        let request = AddRequest {
            source,
            category: category.clone(),
            policy_override,
            paused,
            forced: false,
            unpack: None,
            metadata: Vec::new(),
        };
        match torrents.add(request).await {
            Ok(_) => added += 1,
            Err(AddError::Duplicate(_)) => {}
            Err(error) => {
                refusal.get_or_insert_with(|| match error {
                    AddError::Invalid(message) => text(StatusCode::UNSUPPORTED_MEDIA_TYPE, message),
                    AddError::State(error) => internal(error),
                    refused => text(StatusCode::CONFLICT, refused.to_string()),
                });
            }
        }
    }
    match refusal {
        Some(response) => response,
        None if added == 0 => text(StatusCode::OK, "Fails."),
        None => text(StatusCode::OK, "Ok."),
    }
}

// The seeding policy a torrent added to `category` inherits before its
// trackers are known; the tracker rule it matches is applied as it is added.
fn inherited(config: &Config, category: Option<&str>) -> Option<SeedingPolicy> {
    let category = category.and_then(|name| {
        config
            .categories
            .iter()
            .find(|category| category.name.eq_ignore_ascii_case(name))
    });
    resolve_seeding_policy(
        None,
        None,
        category.and_then(|category| category.seeding.as_ref()),
        config.seeding.as_ref(),
    )
}

// One share limit: absent leaves it as it is, `-2` inherits, `-1` is no
// goal, and a value from 0 up is the goal. A torrent's override cannot
// lift a goal it inherits, so `-1` over one is refused.
fn share_limit(
    params: &Params,
    name: &str,
    current: Option<f64>,
    inherited: Option<f64>,
) -> Result<Option<f64>, Response> {
    let Some(value) = params.get(name) else {
        return Ok(current);
    };
    match value.trim().parse::<f64>() {
        Ok(value) if value >= 0.0 => Ok(Some(value)),
        Ok(value) if value == -2.0 => Ok(None),
        Ok(value) if value == -1.0 && inherited.is_none() => Ok(None),
        Ok(value) if value == -1.0 => Err(text(
            StatusCode::CONFLICT,
            format!("{name}: a torrent cannot drop the goal it inherits"),
        )),
        _ => Err(text(
            StatusCode::BAD_REQUEST,
            format!("{name} is not a limit"),
        )),
    }
}

// A torrent's seeding override from the client's share limits, over its
// current override. Mode and action stay those of the policy in force.
fn share_policy(
    params: &Params,
    inherited: Option<SeedingPolicy>,
    current: Option<SeedingPolicy>,
) -> Result<Option<SeedingPolicy>, Response> {
    let field = |pick: fn(&SeedingPolicy) -> Option<f64>| {
        (
            current.as_ref().and_then(pick),
            inherited.as_ref().and_then(pick),
        )
    };
    let (now, from) = field(|policy| policy.ratio_goal.map(f64::from));
    let ratio = share_limit(params, "ratioLimit", now, from)?;
    let (now, from) = field(|policy| policy.seed_time_goal_minutes.map(f64::from));
    let seeding = share_limit(params, "seedingTimeLimit", now, from)?;
    let (now, from) = field(|policy| policy.inactive_goal_minutes.map(f64::from));
    let inactive = share_limit(params, "inactiveSeedingTimeLimit", now, from)?;
    let max_upload_bps = current.as_ref().and_then(|policy| policy.max_upload_bps);
    if ratio.is_none() && seeding.is_none() && inactive.is_none() && max_upload_bps.is_none() {
        return Ok(None);
    }
    let minutes =
        |value: Option<f64>| value.map(|value| value.round().min(f64::from(u32::MAX)) as u32);
    let base = current.or(inherited).unwrap_or_default();
    Ok(Some(SeedingPolicy {
        ratio_goal: ratio.map(|ratio| ratio as f32),
        seed_time_goal_minutes: minutes(seeding),
        inactive_goal_minutes: minutes(inactive),
        max_upload_bps,
        ..base
    }))
}

async fn set_share_limits(ctx: &TorrentFacadeContext, params: &Params) -> Response {
    let Some(torrents) = &ctx.inner.torrents else {
        return ok();
    };
    for view in ctx.targets(params).await {
        let policy = match share_policy(params, view.seeding_inherited, view.seeding_override) {
            Ok(policy) => policy,
            Err(response) => return response,
        };
        match torrents.set_policy_override(view.job_id, policy).await {
            Ok(()) | Err(ActionError::NotFound(_)) => {}
            Err(error @ ActionError::Invalid(_)) => {
                return text(StatusCode::CONFLICT, error.to_string());
            }
            Err(error) => return internal(error),
        }
    }
    ok()
}

async fn delete(ctx: &TorrentFacadeContext, params: &Params) -> Response {
    let Some(torrents) = &ctx.inner.torrents else {
        return ok();
    };
    let delete_files = params.flag("deleteFiles").unwrap_or(false);
    let confirm = params.flag("confirmUnmetGoal").unwrap_or(false);
    let mut refused = Vec::new();
    for view in ctx.targets(params).await {
        match torrents.remove(view.job_id, delete_files, confirm).await {
            Ok(()) | Err(ActionError::NotFound(_)) => {}
            Err(error @ ActionError::UnmetGoal(_)) => refused.push(error.to_string()),
            Err(error) => return internal(error),
        }
    }
    if refused.is_empty() {
        ok()
    } else {
        text(StatusCode::CONFLICT, refused.join("\n"))
    }
}

// Runs one action on every torrent `hashes` names. Unknown hashes are
// skipped, as the client does.
async fn each<F, Fut>(ctx: &TorrentFacadeContext, params: &Params, action: F) -> Response
where
    F: Fn(TorrentService, TorrentView) -> Fut,
    Fut: std::future::Future<Output = Result<(), ActionError>>,
{
    let Some(torrents) = &ctx.inner.torrents else {
        return ok();
    };
    for view in ctx.targets(params).await {
        match action(torrents.clone(), view).await {
            Ok(()) | Err(ActionError::NotFound(_)) => {}
            Err(error @ ActionError::Invalid(_)) => {
                return text(StatusCode::CONFLICT, error.to_string());
            }
            Err(error) => return internal(error),
        }
    }
    ok()
}

// The limits as the client reads them, where `unlimited` is no limit. A cap
// that lets nothing through has no such number, so its field is left out.
fn with_limits(mut fields: Value, unlimited: i64, limits: [(&str, Option<u64>); 2]) -> Value {
    if let Some(fields) = fields.as_object_mut() {
        for (name, bps) in limits {
            if let Some(limit) = client_limit(bps, unlimited) {
                fields.insert(name.to_owned(), limit);
            }
        }
    }
    fields
}

// A limit as the client reads it: `None` is `unlimited`, and a closed limit
// has no reading.
fn client_limit(bps: Option<u64>, unlimited: i64) -> Option<Value> {
    match bps {
        None => Some(json!(unlimited)),
        Some(0) => None,
        Some(bps) => Some(json!(bps)),
    }
}

// One limit of each torrent `hashes` names, keyed by the hash as it was
// sent: 0 is no limit and -1 a hash no torrent has. A closed limit has no reading and is
// left out, as in the rows.
async fn rate_limits(
    ctx: &TorrentFacadeContext,
    params: &Params,
    limit: impl Fn(&TorrentView) -> Option<u64>,
) -> Response {
    let views = ctx.views().await;
    let reading = |view: &TorrentView| client_limit(limit(view), 0);
    let map: Map<String, Value> = if names_all(params) {
        views
            .iter()
            .filter_map(|view| Some((view.infohash.to_ascii_lowercase(), reading(view)?)))
            .collect()
    } else {
        params
            .get("hashes")
            .unwrap_or_default()
            .split('|')
            .filter_map(|hash| {
                let reading = match views
                    .iter()
                    .find(|view| view.infohash.eq_ignore_ascii_case(hash.trim()))
                {
                    Some(view) => reading(view)?,
                    None => json!(-1),
                };
                Some((hash.to_owned(), reading))
            })
            .collect()
    };
    axum::Json(map).into_response()
}

#[derive(Clone, Copy)]
enum Direction {
    Upload,
    Download,
}

// Sets one limit on each torrent `hashes` names; the other is left as it
// is. The client's 0 and any negative number are no limit.
async fn set_rate_limit(
    ctx: &TorrentFacadeContext,
    params: &Params,
    direction: Direction,
) -> Response {
    let Some(Ok(limit)) = params.get("limit").map(|limit| limit.trim().parse::<i64>()) else {
        return text(StatusCode::BAD_REQUEST, "limit is not a number");
    };
    let limit = Some(u64::try_from(limit).ok().filter(|limit| *limit > 0));
    let (upload, download) = match direction {
        Direction::Upload => (limit, None),
        Direction::Download => (None, limit),
    };
    each(ctx, params, move |torrents, view| async move {
        torrents
            .set_rate_limits(view.job_id, upload, download)
            .await
    })
    .await
}

// `dht` is whether any torrent route carries it: a client reads it before
// handing over a magnet with no trackers, and the route refuses one it
// cannot serve.
pub(super) fn preferences(config: &Config, dht: bool) -> Value {
    let torrent = &config.torrent;
    let seeding = config.seeding.as_ref();
    let minutes = |value: Option<u32>| value.map_or(-1, i64::from);
    let bandwidth = config.bandwidth;
    let fields = json!({
        "dht": dht,
        "save_path": config.complete_dir(),
        "temp_path_enabled": false,
        "temp_path": "",
        "auto_tmm_enabled": false,
        "queueing_enabled": false,
        "max_active_downloads": torrent.max_active_downloads,
        "max_active_uploads": -1,
        "max_active_torrents": -1,
        "listen_port": torrent.base_port,
        "upnp": torrent.upnp,
        "pex": false,
        "lsd": false,
        "encryption": match torrent.encryption {
            TorrentEncryption::Prefer => 0,
            TorrentEncryption::Require => 1,
        },
        "max_connec_per_torrent": torrent.max_peers_total,
        "max_connec": limit(torrent.connections_limit),
        "max_uploads": limit(torrent.unchoke_slots_limit),
        "connection_speed": torrent.connection_speed,
        "enable_multi_connections_from_same_ip": torrent.allow_multiple_connections_per_ip,
        "bittorrent_protocol": match torrent.peer_protocol {
            TorrentPeerProtocol::Both => 0,
            TorrentPeerProtocol::Tcp => 1,
            TorrentPeerProtocol::Utp => 2,
        },
        "dht_bootstrap_nodes": torrent.dht_bootstrap_nodes,
        "ip_filter_enabled": torrent.blocklist_enabled,
        "ip_filter_path": torrent.blocklist.as_deref().unwrap_or_default(),
        "stop_tracker_timeout": torrent.stop_tracker_timeout_secs,
        "max_ratio_enabled": seeding.is_some_and(|policy| policy.ratio_goal.is_some()),
        "max_ratio": seeding.and_then(|policy| policy.ratio_goal).map_or(-1.0, f64::from),
        "max_seeding_time_enabled": seeding.is_some_and(|policy| policy.seed_time_goal_minutes.is_some()),
        "max_seeding_time": minutes(seeding.and_then(|policy| policy.seed_time_goal_minutes)),
        "max_inactive_seeding_time_enabled": seeding.is_some_and(|policy| policy.inactive_goal_minutes.is_some()),
        "max_inactive_seeding_time": minutes(seeding.and_then(|policy| policy.inactive_goal_minutes)),
        "max_ratio_act": match seeding.map_or(SeedingAction::Stop, |policy| policy.action) {
            SeedingAction::Stop => 0,
            SeedingAction::Remove => 1,
            SeedingAction::RemoveWithData => 3,
        },
    });
    with_limits(
        fields,
        0,
        [
            ("dl_limit", bandwidth.download_bps),
            ("up_limit", bandwidth.upload_bps),
        ],
    )
}

// A limit as qBittorrent reports one, -1 for none.
fn limit(limit: TorrentLimit) -> i64 {
    limit.count().map_or(-1, |count| i64::from(count.get()))
}

// The torrent settings `changes` asks for over `current`, the keys as
// qBittorrent's `AppController::setPreferencesAction` reads them. Every
// count is one at least; for the connection and upload slot limits,
// qBittorrent's zero or less is no limit (`SessionImpl::setMaxConnections`,
// `setMaxUploads`), and a negative stop tracker timeout is none, as
// libtorrent takes it (`torrent::announce_with_tracker`). Any other value is
// refused, and with it the whole request.
fn torrent_preferences(
    current: &TorrentConfig,
    changes: &Map<String, Value>,
) -> Result<TorrentConfig, String> {
    let mut torrent = current.clone();
    let out_of_range = |name: &str| format!("{name} is out of range");
    let number = |name: &str| -> Result<Option<i64>, String> {
        changes
            .get(name)
            .map(|value| value.as_i64().ok_or_else(|| out_of_range(name)))
            .transpose()
    };
    let positive = |name: &str, value: i64| {
        u32::try_from(value)
            .ok()
            .and_then(NonZeroU32::new)
            .ok_or_else(|| out_of_range(name))
    };
    let count = |name: &str| -> Result<Option<NonZeroU32>, String> {
        number(name)?.map(|value| positive(name, value)).transpose()
    };
    let limit = |name: &str| -> Result<Option<TorrentLimit>, String> {
        number(name)?
            .map(|value| match value {
                ..=0 => Ok(TorrentLimit::Unlimited),
                value => positive(name, value).map(TorrentLimit::Count),
            })
            .transpose()
    };
    let flag = |name: &str| changes.get(name).and_then(Value::as_bool);
    // Weaver has no seed queue, so only the download limit has a home.
    if let Some(downloads) = count("max_active_downloads")? {
        torrent.max_active_downloads = downloads.get();
    }
    if let Some(peers) = count("max_connec_per_torrent")? {
        torrent.max_peers_total = peers.get();
    }
    if let Some(limit) = limit("max_connec")? {
        torrent.connections_limit = limit;
    }
    if let Some(slots) = limit("max_uploads")? {
        torrent.unchoke_slots_limit = slots;
    }
    if let Some(speed) = count("connection_speed")? {
        torrent.connection_speed = speed;
    }
    if let Some(secs) = number("stop_tracker_timeout")? {
        torrent.stop_tracker_timeout_secs =
            u32::try_from(secs.max(0)).map_err(|_| out_of_range("stop_tracker_timeout"))?;
    }
    if let Some(allow) = flag("enable_multi_connections_from_same_ip") {
        torrent.allow_multiple_connections_per_ip = allow;
    }
    if let Some(protocol) = changes.get("bittorrent_protocol") {
        torrent.peer_protocol = match protocol.as_i64() {
            Some(0) => TorrentPeerProtocol::Both,
            Some(1) => TorrentPeerProtocol::Tcp,
            Some(2) => TorrentPeerProtocol::Utp,
            _ => return Err(out_of_range("bittorrent_protocol")),
        };
    }
    if let Some(nodes) = changes.get("dht_bootstrap_nodes").and_then(Value::as_str) {
        nodes.clone_into(&mut torrent.dht_bootstrap_nodes);
    }
    // The switch and the list are two settings, as qBittorrent keeps them
    // (`SessionImpl::setIPFilteringEnabled`, `setIPFilterFile`).
    if let Some(enabled) = flag("ip_filter_enabled") {
        torrent.blocklist_enabled = enabled;
    }
    if let Some(path) = changes.get("ip_filter_path").and_then(Value::as_str) {
        torrent.blocklist = Some(path.to_owned());
    }
    torrent.checked().map_err(|errors| errors.join("\n"))
}

async fn set_preferences(ctx: &TorrentFacadeContext, params: &Params) -> Response {
    let Some(Ok(Value::Object(changes))) = params.get("json").map(serde_json::from_str::<Value>)
    else {
        return text(StatusCode::BAD_REQUEST, "json is not an object");
    };
    let rate = |name: &str| {
        changes
            .get(name)
            .and_then(Value::as_i64)
            .map(|value| u64::try_from(value).ok().filter(|value| *value > 0))
    };
    let inner = &ctx.inner;
    let current = inner.config.read().await.torrent.clone();
    let torrent = match torrent_preferences(&current, &changes) {
        Ok(torrent) => torrent,
        Err(error) => return text(StatusCode::BAD_REQUEST, error),
    };
    let (download, upload) = (rate("dl_limit"), rate("up_limit"));
    if download.is_some() || upload.is_some() {
        let current = inner.config.read().await.bandwidth;
        let bandwidth = Bandwidth {
            download_bps: download.unwrap_or(current.download_bps),
            upload_bps: upload.unwrap_or(current.upload_bps),
            ..current
        };
        if bandwidth.validate().is_err() {
            return text(StatusCode::BAD_REQUEST, "limit too large");
        }
        if let Err(error) = inner
            .handle
            .save_bandwidth(&inner.db, &inner.config, bandwidth)
            .await
        {
            return internal(error);
        }
    }
    if torrent != current {
        let db = inner.db.clone();
        let saved = torrent.clone();
        match tokio::task::spawn_blocking(move || db.save_torrent_config(&saved)).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => return internal(error),
            Err(error) => return internal(error),
        }
        inner.config.write().await.torrent = torrent;
        ctx.refresh_settings().await;
    }
    ok()
}

impl TorrentFacadeContext {
    async fn refresh_settings(&self) {
        if let Some(torrents) = &self.inner.torrents {
            let settings = TorrentSettings::of(&*self.inner.config.read().await);
            torrents.set_settings(settings).await;
        }
    }
}

pub(super) fn categories(config: &Config) -> Value {
    let map: Map<String, Value> = config
        .categories
        .iter()
        .map(|category| {
            (
                category.name.clone(),
                json!({
                    "name": category.name,
                    "savePath": category_dir(config, Some(&category.name)).to_string_lossy(),
                    "download_path": Value::Null,
                }),
            )
        })
        .collect();
    Value::Object(map)
}

async fn create_category(ctx: &TorrentFacadeContext, params: &Params) -> Response {
    let Ok(name) = validate_category_path_component(params.get("category").unwrap_or_default())
    else {
        return text(StatusCode::BAD_REQUEST, "Invalid category name");
    };
    let inner = &ctx.inner;
    let _guard = inner.categories.lock().await;
    if inner
        .config
        .read()
        .await
        .categories
        .iter()
        .any(|category| category.name.eq_ignore_ascii_case(&name))
    {
        return text(StatusCode::CONFLICT, "Unable to create category");
    }
    let db = inner.db.clone();
    let candidate = name.clone();
    let created = tokio::task::spawn_blocking(move || {
        let category = CategoryConfig {
            id: db.next_category_id()?,
            name: candidate,
            dest_dir: None,
            aliases: String::new(),
            seeding: None,
            torrent_route: false,
            unpack: None,
        };
        db.insert_category(&category).map(|()| category)
    })
    .await;
    let category = match created {
        Ok(Ok(category)) => category,
        Ok(Err(error)) => return internal(error),
        Err(error) => return internal(error),
    };
    inner.config.write().await.categories.push(category);
    ctx.refresh_settings().await;
    if let Err(error) = inner.watch_folder.reconcile_from_config().await {
        tracing::warn!(%error, "watch folder did not take the new category");
    }
    ok()
}

// The download limit is what torrents are held to this tick: their share of
// the global limit, not the limit itself.
fn transfer(views: &[TorrentView], download: Option<u64>, upload: Option<u64>) -> Value {
    let fields = json!({
        "dl_info_speed": views.iter().map(|view| view.download_bps).sum::<u64>(),
        "dl_info_data": views.iter().map(|view| view.downloaded_bytes).sum::<u64>(),
        "up_info_speed": views.iter().map(|view| view.upload_bps).sum::<u64>(),
        "up_info_data": views.iter().map(|view| view.uploaded_bytes).sum::<u64>(),
        "dht_nodes": 0,
        "connection_status": "connected",
    });
    with_limits(
        fields,
        0,
        [("dl_rate_limit", download), ("up_rate_limit", upload)],
    )
}

async fn maindata(ctx: &TorrentFacadeContext) -> Response {
    let rid = ctx.inner.rid.fetch_add(1, Ordering::Relaxed) + 1;
    let views = ctx.views().await;
    let config = ctx.inner.config.read().await;
    let torrents: Map<String, Value> = views
        .iter()
        .map(|view| {
            let mut entry = info(view);
            let hash = entry
                .as_object_mut()
                .and_then(|entry| entry.remove("hash"))
                .and_then(|hash| hash.as_str().map(str::to_owned))
                .unwrap_or_default();
            (hash, entry)
        })
        .collect();
    let mut server_state = transfer(
        &views,
        ctx.torrent_download_limit(),
        config.bandwidth.upload_bps,
    );
    if let Some(state) = server_state.as_object_mut() {
        state.insert("queueing".into(), Value::Bool(false));
        state.insert("use_alt_speed_limits".into(), Value::Bool(false));
        state.insert("refresh_interval".into(), json!(1500));
    }
    axum::Json(json!({
        "rid": rid,
        "full_update": true,
        "torrents": torrents,
        "categories": categories(&config),
        "tags": [],
        "server_state": server_state,
    }))
    .into_response()
}

// The client's name for a peer from the first eight bytes of its peer id
// (qBittorrent `PeerInfo::peerIdClient`): empty while the id is all zeros,
// "Unknown" when those bytes are not printable.
fn peer_id_client(peer_id: &[u8; 20]) -> String {
    if peer_id.iter().all(|byte| *byte == 0) {
        return String::new();
    }
    let head = &peer_id[..8];
    if head.iter().all(|byte| (32..=126).contains(byte)) {
        head.iter().map(|byte| char::from(*byte)).collect()
    } else {
        "Unknown".to_owned()
    }
}

// A peer's flags as the client's letters, each with what it stands for
// (qBittorrent `PeerInfo::determineFlags`).
fn peer_flags(peer: &PeerInfo) -> (String, String) {
    let flags = peer.flags;
    let letters = [
        (
            flags.interesting && flags.remote_choked,
            'd',
            "interested, choked by the peer",
        ),
        (
            flags.interesting && !flags.remote_choked,
            'D',
            "interested, unchoked by the peer",
        ),
        (
            flags.remote_interested && flags.choked,
            'u',
            "peer interested, choked by us",
        ),
        (
            flags.remote_interested && !flags.choked,
            'U',
            "peer interested, unchoked by us",
        ),
        (
            !flags.remote_choked && !flags.interesting,
            'K',
            "not interested, unchoked by the peer",
        ),
        (
            !flags.choked && !flags.remote_interested,
            '?',
            "peer not interested, unchoked by us",
        ),
        (flags.optimistic_unchoke, 'O', "optimistic unchoke"),
        (flags.snubbed, 'S', "snubbed"),
        (!peer.outgoing, 'I', "incoming"),
        (peer.sources.contains(PeerSourceFlag::Dht), 'H', "from DHT"),
        (peer.sources.contains(PeerSourceFlag::Pex), 'X', "from PEX"),
        (peer.sources.contains(PeerSourceFlag::Lsd), 'L', "from LSD"),
        (peer.encryption == Encryption::Rc4, 'E', "encrypted"),
        (
            peer.encryption == Encryption::HeaderOnly,
            'e',
            "encrypted handshake",
        ),
        (matches!(peer.connection, ConnectionKind::Utp), 'P', "μTP"),
        (flags.holepunched, 'h', "holepunched"),
    ];
    let set: Vec<_> = letters.iter().filter(|(on, ..)| *on).collect();
    let letters = set
        .iter()
        .map(|(_, letter, _)| letter.to_string())
        .collect::<Vec<_>>()
        .join(" ");
    let described = set
        .iter()
        .map(|(_, letter, meaning)| format!("{letter} = {meaning}"))
        .collect::<Vec<_>>()
        .join("\n");
    (letters, described)
}

// One peer as `sync/torrentPeers` lists it (qBittorrent
// `SyncController::torrentPeersAction`).
fn peer_json(peer: &PeerInfo, total_bytes: u64) -> Value {
    let (flags, flags_desc) = peer_flags(peer);
    // The share of what the peer has that came from us.
    let contribution = if peer.uploaded == 0 {
        0.0
    } else {
        let total = if total_bytes == 0 {
            peer.uploaded
        } else {
            total_bytes
        } as f64;
        let have = peer.progress * total;
        peer.uploaded as f64 / if have <= 0.0 { total } else { have }
    };
    object([
        ("client", json!(peer.client)),
        ("peer_id_client", json!(peer_id_client(&peer.peer_id.0))),
        ("progress", json!(peer.progress)),
        ("dl_speed", json!(peer.download_rate)),
        ("up_speed", json!(peer.upload_rate)),
        ("downloaded", json!(peer.downloaded)),
        ("uploaded", json!(peer.uploaded)),
        (
            "connection",
            json!(match peer.connection {
                ConnectionKind::Tcp => "BT",
                ConnectionKind::Utp => "μTP",
            }),
        ),
        ("flags", json!(flags)),
        ("flags_desc", json!(flags_desc)),
        ("contribution", json!(contribution)),
        ("ip", json!(peer.addr.ip().to_string())),
        ("port", json!(peer.addr.port())),
        ("host_name", json!("")),
        ("country_code", Value::Null),
        ("country", Value::Null),
    ])
}

async fn torrent_peers(ctx: &TorrentFacadeContext, params: &Params) -> Response {
    let Some(torrents) = &ctx.inner.torrents else {
        return not_found();
    };
    let Some(view) = ctx.by_hash(params.get("hash").unwrap_or_default()).await else {
        return not_found();
    };
    let peers = match torrents.peers(view.job_id).await {
        Ok(peers) => peers,
        Err(ActionError::NotFound(_)) => return not_found(),
        Err(error) => return internal(error),
    };
    let rid = ctx.inner.rid.fetch_add(1, Ordering::Relaxed) + 1;
    let peers: Map<String, Value> = peers
        .iter()
        .map(|peer| (peer.addr.to_string(), peer_json(peer, view.total_bytes)))
        .collect();
    axum::Json(json!({
        "rid": rid,
        "full_update": true,
        "show_flags": false,
        "peers": peers,
    }))
    .into_response()
}

// `transfer/banPeers` (qBittorrent `TransferController::banPeersAction`):
// `|`-separated `ip:port`s, each IP banned from every torrent. qBittorrent's
// `SessionImpl::banIP` bans session-wide; the bans here go on each
// torrent's own peer list (libtorrent `torrent::ban_peer`), saved once per
// torrent. A torrent that fails to save does not stop the others.
async fn ban_peers(ctx: &TorrentFacadeContext, params: &Params) -> Response {
    let Some(peers) = params.get("peers") else {
        return text(StatusCode::BAD_REQUEST, "Missing parameter: peers");
    };
    let Some(torrents) = &ctx.inner.torrents else {
        return ok();
    };
    let ips: Vec<_> = peers
        .split('|')
        .filter_map(|peer| peer.trim().parse::<SocketAddr>().ok())
        .map(|addr| addr.ip())
        .collect();
    if ips.is_empty() {
        return ok();
    }
    let mut failed = None;
    for view in torrents.torrents(None).await {
        match torrents.ban_peers(view.job_id, &ips).await {
            Ok(()) | Err(ActionError::NotFound(_)) => {}
            Err(error) => failed = Some(internal(error)),
        }
    }
    failed.unwrap_or_else(ok)
}
