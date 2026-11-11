// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! The torrent client compatibility facade: the WebUI API v2 shape that
//! torrent automation drives, served beside the first-party GraphQL API.

use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::net::{IpAddr, SocketAddr};
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
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
use weaver_server_core::ingest::{Kind, classify};
use weaver_server_core::jobs::ids::JobId;
use weaver_server_core::jobs::{HoldOrigin, HoldOwner, QueueMoveTarget};
use weaver_server_core::operations::probe_disk_space;
use weaver_server_core::proxies::{Bandwidth, ResolvedBy, TorrentScope};
use weaver_server_core::proxies::{DiscoveredInterface, InterfaceSource, SystemInterfaceSource};
use weaver_server_core::rss::RssService;
use weaver_server_core::security::RuntimeSecurityConfig;
use weaver_server_core::settings::model::{Config, SharedConfig};
use weaver_server_core::settings::{
    Leaf, SeedTimeCounting, SeedingAction, SeedingGoalMode, SeedingPolicy, TorrentConfig,
    TorrentEncryption,
    TorrentLimit, TorrentPeerProtocol,
};
use weaver_server_core::tags::{TagSnapshot, split_tags};
use weaver_server_core::torrents::model::{file_priority_number, file_priority_of};
use weaver_server_core::torrents::service::fingerprint::ViewParts;
use weaver_server_core::torrents::service::{
    ActionError, AddError, AddRequest, Placement, TorrentService, TorrentSettings, TorrentView,
};
use weaver_server_core::torrents::{
    ConnectionKind, ContentLayout, Encryption, FilePriority, NamedBy, PeerInfo, PeerSourceFlag,
    TorrentState, TorrentTracker, TrackerStatus, redacted_tracker_url, status,
};
use weaver_server_core::watch_folder::WatchFolderService;
use weaver_server_core::{Database, RssFeedRow, SchedulerHandle};

use super::auth::{LoginRateLimiter, login_client_id, same_origin_request};

mod api;
mod sync;
#[cfg(test)]
mod tests;

pub(super) use api::Api;
use sync::{CLOCK_KEYS, Fields, Poll, Row, SyncClient, SyncTable};

const BODY_LIMIT_BYTES: usize = 32 * 1024 * 1024;
const SESSION_CAP: usize = 1024;
// The client's "unknown" ETA: 100 days.
const ETA_UNKNOWN: u64 = 8_640_000;
// `app/version`: qBittorrent 5.3.0's own string, whose WebAPI is 2.16.2.
const QBT_VERSION: &str = "v5.3.0";

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
    // Each client's `sync/maindata` ids and the state they stand for.
    sync: Mutex<SyncTable>,
    // The torrent rows `sync/maindata` built, counted under test so a test
    // pins that a poll builds only the rows whose view moved.
    #[cfg(test)]
    maindata_rows: AtomicU64,
    // `sync/torrentPeers` ids.
    rid: AtomicU64,
    categories: tokio::sync::Mutex<()>,
    rss: RssService,
    // The API version the facade answers as.
    api: Api,
    // When this listener started, in seconds since the epoch.
    launched: i64,
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
        rss: RssService,
        api: Api,
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
                rss,
                api,
                sessions: Mutex::new(SessionTable::default()),
                sync: Mutex::new(SyncTable::default()),
                #[cfg(test)]
                maindata_rows: AtomicU64::new(0),
                rid: AtomicU64::new(0),
                categories: tokio::sync::Mutex::new(()),
                launched: now_secs(),
            }),
        }
    }

    fn api(&self) -> Api {
        self.inner.api
    }

    async fn count(&self) -> usize {
        match &self.inner.torrents {
            Some(torrents) => torrents.torrents(None).await.len(),
            None => 0,
        }
    }

    fn sessions(&self) -> std::sync::MutexGuard<'_, SessionTable> {
        self.inner
            .sessions
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn sync(&self) -> std::sync::MutexGuard<'_, SyncTable> {
        self.inner
            .sync
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    // Ends a login session, and the `sync/maindata` state kept for it.
    fn end_session(&self, sid: &str) {
        self.sessions().keys.remove(sid);
        self.sync().forget(&SyncClient::Session(sid.to_owned()));
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
        torrents.torrents_with_files().await
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
        self.named(params.get("hashes").unwrap_or_default()).await
    }

    // The torrents `wanted` names: `|`-separated hashes, or `all`
    // (qBittorrent `applyToTorrents`).
    async fn named(&self, wanted: &str) -> Vec<TorrentView> {
        let views = self.views().await;
        if wanted.trim() == "all" {
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
    async fn caller(&self, headers: &HeaderMap) -> Result<Caller, StatusCode> {
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
                Some(scope) => {
                    return Ok(Caller {
                        scope,
                        by_cookie: true,
                        client: SyncClient::Session(sid),
                    });
                }
                None => self.end_session(&sid),
            }
        }
        let (scope, key) = match basic_password(headers) {
            Some(password) => (self.resolve(&password).await?, Some(password)),
            None => {
                let mut headers = headers.clone();
                headers.remove(header::COOKIE);
                (self.resolve_headers(&headers).await?, sent_key(&headers))
            }
        };
        Ok(Caller {
            scope,
            by_cookie: false,
            client: key.map_or(SyncClient::Anonymous, |key| {
                SyncClient::Key(hash_api_key(&key))
            }),
        })
    }
}

// Who a request is from, and whether the login cookie said so.
struct Caller {
    scope: CallerScope,
    by_cookie: bool,
    // Whose `sync/maindata` ids the request carries.
    client: SyncClient,
}

impl Caller {
    // Whether the request may change anything: its key's scope allows it,
    // and a request the browser sent with the login cookie came from a page
    // of this listener, whatever its method.
    fn may_change(&self, headers: &HeaderMap) -> bool {
        self.scope.can_control() && (!self.by_cookie || same_origin_request(headers))
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

// The API key a request sends as a bearer token or in `X-Api-Key`.
fn sent_key(headers: &HeaderMap) -> Option<String> {
    let bearer = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    bearer
        .or_else(|| {
            headers
                .get("x-api-key")
                .and_then(|value| value.to_str().ok())
        })
        .map(str::trim)
        .filter(|key| !key.is_empty())
        .map(str::to_owned)
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

// A request's parameters: the query string, then the form body over it,
// and each file the body uploads with its name.
#[derive(Default)]
struct Params {
    fields: HashMap<String, String>,
    files: Vec<(String, Vec<u8>)>,
}

impl Params {
    fn get(&self, name: &str) -> Option<&str> {
        self.fields.get(name).map(String::as_str)
    }

    fn flag(&self, name: &str) -> Option<bool> {
        self.get(name)
            .map(|value| value.trim().eq_ignore_ascii_case("true"))
    }

    // A flag that is only read when it is `true` or `false`
    // (qBittorrent `parseBool`).
    fn flag_value(&self, name: &str) -> Option<bool> {
        match self.get(name)?.trim() {
            value if value.eq_ignore_ascii_case("true") => Some(true),
            value if value.eq_ignore_ascii_case("false") => Some(false),
            _ => None,
        }
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
                let file_name = field.file_name().map(str::to_owned);
                let bytes = field.bytes().await.map_err(IntoResponse::into_response)?;
                if let Some(file_name) = file_name {
                    params.files.push((file_name, bytes.to_vec()));
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

// A feature of the client Weaver does not have.
fn unavailable(what: &str) -> Response {
    text(
        StatusCode::CONFLICT,
        format!("{what} is not available in Weaver"),
    )
}

fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| {
            i64::try_from(since.as_secs()).unwrap_or(i64::MAX)
        })
}

fn forbidden() -> Response {
    text(StatusCode::FORBIDDEN, "Forbidden")
}

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

// A failure inside weaver: logged, never shown to the caller.
fn internal(error: impl std::fmt::Display) -> Response {
    tracing::warn!(%error, "torrent client compatibility request failed");
    text(StatusCode::INTERNAL_SERVER_ERROR, "internal error")
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Need {
    Read,
    Control,
    // What the GraphQL API keeps to administrators: the RSS feeds and rules,
    // and the server's directories.
    Admin,
}

// The groups qBittorrent 5.3.0 registers (`WebApplication::sessionStart`).
const GROUPS: [&str; 10] = [
    "app",
    "auth",
    "clientdata",
    "log",
    "rss",
    "search",
    "sync",
    "torrentcreator",
    "torrents",
    "transfer",
];

// Every action of qBittorrent 5.3.0 outside `auth`, by what it needs.
fn need(group: &str, action: &str) -> Option<Need> {
    use Need::{Admin, Control, Read};
    Some(match (group, action) {
        (
            "app",
            "webapiVersion"
            | "version"
            | "buildInfo"
            | "processInfo"
            | "preferences"
            | "defaultSavePath"
            | "getFreeSpaceAtPath"
            | "cookies"
            | "networkInterfaceList"
            | "networkInterfaceAddressList",
        ) => Read,
        ("app", "getDirectoryContent") => Admin,
        (
            "app",
            "shutdown" | "setPreferences" | "sendTestEmail" | "setCookies" | "rotateAPIKey"
            | "deleteAPIKey",
        ) => Control,
        ("clientdata", "load") => Read,
        ("clientdata", "store") => Control,
        ("log", "main" | "peers") => Read,
        ("rss", "items" | "rules" | "matchingArticles" | "exportRules") => Read,
        (
            "rss",
            "addFolder"
            | "addFeed"
            | "setFeedURL"
            | "setFeedRefreshInterval"
            | "removeItem"
            | "moveItem"
            | "markAsRead"
            | "refreshItem"
            | "setRule"
            | "importRules"
            | "renameRule"
            | "cloneRule"
            | "removeRule",
        ) => Admin,
        ("search", "status" | "results" | "plugins") => Read,
        (
            "search",
            "start" | "stop" | "delete" | "downloadTorrent" | "installPlugin" | "uninstallPlugin"
            | "enablePlugin" | "updatePlugins",
        ) => Control,
        ("sync", "maindata" | "torrentPeers") => Read,
        ("torrentcreator", "status" | "torrentFile") => Read,
        ("torrentcreator", "addTask" | "deleteTask") => Control,
        (
            "torrents",
            "count" | "info" | "properties" | "trackers" | "webseeds" | "files" | "pieceHashes"
            | "pieceStates" | "pieceAvailability" | "uploadLimit" | "downloadLimit" | "categories"
            | "tags" | "export" | "SSLParameters" | "downloadFile",
        ) => Read,
        (
            "torrents",
            "add"
            | "addWebSeeds"
            | "editWebSeed"
            | "removeWebSeeds"
            | "addTrackers"
            | "editTracker"
            | "removeTrackers"
            | "addPeers"
            | "stop"
            | "start"
            | "filePrio"
            | "setUploadLimit"
            | "setDownloadLimit"
            | "setShareLimits"
            | "toggleSequentialDownload"
            | "toggleFirstLastPiecePrio"
            | "setSuperSeeding"
            | "setForceStart"
            | "delete"
            | "increasePrio"
            | "decreasePrio"
            | "topPrio"
            | "bottomPrio"
            | "setLocation"
            | "setSavePath"
            | "setDownloadPath"
            | "rename"
            | "setComment"
            | "setAutoManagement"
            | "recheck"
            | "reannounce"
            | "setCategory"
            | "createCategory"
            | "editCategory"
            | "removeCategories"
            | "addTags"
            | "setTags"
            | "removeTags"
            | "createTags"
            | "deleteTags"
            | "renameFile"
            | "renameFolder"
            | "setSSLParameters"
            | "fetchMetadata"
            | "parseMetadata"
            | "saveMetadata",
        ) => Control,
        (
            "transfer",
            "info" | "uploadLimit" | "downloadLimit" | "getSpeedLimits" | "speedLimitsMode",
        ) => Read,
        (
            "transfer",
            "setUploadLimit"
            | "setDownloadLimit"
            | "setSpeedLimits"
            | "toggleSpeedLimitsMode"
            | "setSpeedLimitsMode"
            | "banPeers"
            | "pauseSession"
            | "resumeSession",
        ) => Control,
        _ => return None,
    })
}

// An action qBittorrent does not have: an unknown group is the server's
// 404, an unknown action in a known group the API's.
fn no_endpoint(api: Api, group: &str) -> Response {
    if GROUPS.contains(&group) {
        api.no_action()
    } else {
        text(StatusCode::NOT_FOUND, "Not Found")
    }
}

async fn handle(
    Extension(ctx): Extension<TorrentFacadeContext>,
    Extension(login_limiter): Extension<LoginRateLimiter>,
    peer: Option<Extension<ConnectInfo<SocketAddr>>>,
    axum::extract::Path((group, action)): axum::extract::Path<(String, String)>,
    request: Request,
) -> Response {
    if group == "auth" {
        // A request sent with an API key may not log in or out (qBittorrent
        // `WebApplication::processRequest`).
        if bearer(request.headers()) {
            return forbidden();
        }
        return match action.as_str() {
            "login" => {
                let peer = peer.map(|Extension(ConnectInfo(peer))| peer);
                let client_id = login_client_id(&ctx.inner.security, request.headers(), peer);
                match Params::read(request).await {
                    Ok(params) => login(&ctx, &login_limiter, &client_id, &params).await,
                    Err(response) => response,
                }
            }
            "logout" => {
                if let Some(sid) = cookie(request.headers(), "SID") {
                    if !same_origin_request(request.headers()) {
                        return forbidden();
                    }
                    ctx.end_session(&sid);
                }
                ok()
            }
            _ => no_endpoint(ctx.api(), &group),
        };
    }
    // The caller is known before any of the body is read.
    let Ok(caller) = ctx.caller(request.headers()).await else {
        return forbidden();
    };
    let Some(need) = need(&group, &action) else {
        return no_endpoint(ctx.api(), &group);
    };
    let may_change = caller.may_change(request.headers());
    let Caller { scope, client, .. } = caller;
    let allowed = match need {
        Need::Read => scope.can_read(),
        Need::Control => may_change,
        Need::Admin => scope.is_admin() && may_change,
    };
    if !allowed {
        return forbidden();
    }
    let params = match Params::read(request).await {
        Ok(params) => params,
        Err(response) => return response,
    };
    match group.as_str() {
        "app" => app(&ctx, &action, &params).await,
        "torrents" => torrents(&ctx, &action, params, scope).await,
        "transfer" => transfer_action(&ctx, &action, &params).await,
        "sync" if action == "maindata" => maindata(&ctx, scope, client, &params).await,
        "sync" => torrent_peers(&ctx, &params).await,
        "rss" => rss(&ctx, &action, &params).await,
        // Weaver keeps no log the client's shape fits.
        "log" => axum::Json(json!([])).into_response(),
        // Nothing of the client's WebUI is kept.
        "clientdata" if action == "load" => axum::Json(json!({})).into_response(),
        "clientdata" => unavailable("Client data"),
        "search" => unavailable("Search"),
        _ => unavailable("Torrent creation"),
    }
}

// Whether the request carries an API key as a bearer token.
fn bearer(headers: &HeaderMap) -> bool {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split_whitespace().next())
        .is_some_and(|scheme| scheme.eq_ignore_ascii_case("bearer"))
}

// The parameters an action cannot do without (qBittorrent
// `APIController::requireParams`).
fn require(params: &Params, names: &[&str]) -> Result<(), Box<Response>> {
    let missing: Vec<&str> = names
        .iter()
        .copied()
        .filter(|name| params.get(name).is_none())
        .collect();
    if missing.is_empty() {
        return Ok(());
    }
    Err(Box::new(text(
        StatusCode::BAD_REQUEST,
        format!("Missing required parameters: {}", missing.join(", ")),
    )))
}

// qBittorrent `AuthController::loginAction`: a session for the key the
// password holds, with its cookie.
async fn login(
    ctx: &TorrentFacadeContext,
    login_limiter: &LoginRateLimiter,
    client_id: &str,
    params: &Params,
) -> Response {
    let username = params.get("username").unwrap_or_default();
    if login_limiter.too_many_failures(username, client_id) {
        return text(
            StatusCode::FORBIDDEN,
            "Your IP address has been banned after too many failed authentication attempts.",
        );
    }
    let password = params.get("password").unwrap_or_default().trim();
    if password.is_empty() || ctx.resolve(password).await.is_err() {
        login_limiter.record_failure(username, client_id);
        return ctx.api().login_refused();
    }
    login_limiter.record_success(username, client_id);
    let sid = ctx.sessions().issue(hash_api_key(password));
    let mut response = ctx.api().logged_in();
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

// The `app` group (qBittorrent `AppController`).
async fn app(ctx: &TorrentFacadeContext, action: &str, params: &Params) -> Response {
    match action {
        "version" => text(StatusCode::OK, QBT_VERSION),
        "webapiVersion" => text(StatusCode::OK, ctx.api().version()),
        "buildInfo" => axum::Json(json!({
            "qt": "", "libtorrent": "", "boost": "", "openssl": "", "zlib": "",
            "bitness": 64,
            "platform": platform(),
        }))
        .into_response(),
        "processInfo" => axum::Json(json!({ "launch_time": ctx.inner.launched })).into_response(),
        "preferences" => {
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
        "setPreferences" => set_preferences(ctx, params).await,
        "defaultSavePath" => text(StatusCode::OK, ctx.inner.config.read().await.complete_dir()),
        "getFreeSpaceAtPath" => free_space(params).await,
        "getDirectoryContent" => directory_content(params).await,
        // Weaver keeps no cookies: a url is fetched with the cookie its add
        // names.
        "cookies" => axum::Json(json!([])).into_response(),
        "setCookies" => set_cookies(params),
        "networkInterfaceList" => interface_list().await,
        "networkInterfaceAddressList" => interface_addresses(params).await,
        // Weaver's process and its API keys are its own to manage.
        "shutdown" => unavailable("Shutting down"),
        "sendTestEmail" => unavailable("Email"),
        "rotateAPIKey" | "deleteAPIKey" => unavailable("The client's API key"),
        _ => no_endpoint(ctx.api(), "app"),
    }
}

fn platform() -> &'static str {
    match std::env::consts::OS {
        os @ ("linux" | "macos" | "windows") => os,
        _ => "unknown",
    }
}

// The parameters each `torrents` action requires (qBittorrent
// `TorrentsController`). `setShareLimits` takes mode and action as
// optional, as the clients that predate them send none.
fn torrents_required(action: &str) -> &'static [&'static str] {
    match action {
        "properties" | "trackers" | "webseeds" | "files" | "export" => &["hash"],
        "addTrackers" | "removeTrackers" => &["hash", "urls"],
        "filePrio" => &["hash", "id", "priority"],
        "stop"
        | "start"
        | "uploadLimit"
        | "downloadLimit"
        | "toggleSequentialDownload"
        | "toggleFirstLastPiecePrio"
        | "increasePrio"
        | "decreasePrio"
        | "topPrio"
        | "bottomPrio"
        | "recheck"
        | "reannounce"
        | "removeTags" => &["hashes"],
        "setUploadLimit" | "setDownloadLimit" => &["hashes", "limit"],
        "setShareLimits" => &[
            "hashes",
            "ratioLimit",
            "seedingTimeLimit",
            "inactiveSeedingTimeLimit",
        ],
        "setSuperSeeding" | "setForceStart" => &["hashes", "value"],
        "delete" => &["hashes", "deleteFiles"],
        "setLocation" => &["hashes", "location"],
        "setSavePath" | "setDownloadPath" => &["id", "path"],
        "setAutoManagement" => &["hashes", "enable"],
        "setCategory" => &["hashes", "category"],
        "createCategory" | "editCategory" => &["category"],
        "removeCategories" => &["categories"],
        "addTags" | "setTags" => &["hashes", "tags"],
        "createTags" | "deleteTags" => &["tags"],
        _ => &[],
    }
}

// The `torrents` group (qBittorrent `TorrentsController`).
async fn torrents(
    ctx: &TorrentFacadeContext,
    action: &str,
    params: Params,
    scope: CallerScope,
) -> Response {
    if let Err(response) = require(&params, torrents_required(action)) {
        return *response;
    }
    if action == "add" {
        return add(ctx, params).await;
    }
    let params = &params;
    match action {
        "count" => text(StatusCode::OK, ctx.count().await.to_string()),
        "info" => torrents_info(ctx, params, scope).await,
        "properties" => properties(ctx, params).await,
        "trackers" => trackers(ctx, params, scope).await,
        // Weaver fetches from no web seed.
        "webseeds" => match ctx.by_hash(params.get("hash").unwrap_or_default()).await {
            Some(_) => axum::Json(json!([])).into_response(),
            None => not_found(),
        },
        "files" => files(ctx, params).await,
        "export" => export(ctx, params).await,
        "delete" => delete(ctx, params).await,
        // Every torrent at once is one hold, which leaves each torrent's
        // own stop or start standing; named torrents are each stopped.
        "stop" if names_all(params) => held(ctx.inner.handle.pause_torrents().await),
        "start" if names_all(params) => held(ctx.inner.handle.resume_torrents().await),
        "stop" => {
            each(ctx, params, |torrents, view| async move {
                torrents.pause(view.job_id).await
            })
            .await
        }
        "start" => {
            each(ctx, params, |torrents, view| async move {
                torrents.resume(view.job_id).await
            })
            .await
        }
        "setForceStart" => {
            let forced = params.flag("value").unwrap_or(false);
            each(ctx, params, move |torrents, view| async move {
                torrents.set_forced(view.job_id, forced).await
            })
            .await
        }
        "filePrio" => file_prio(ctx, params).await,
        "toggleSequentialDownload" => {
            each(ctx, params, |torrents, view| async move {
                torrents.set_sequential(view.job_id, !view.sequential).await
            })
            .await
        }
        "toggleFirstLastPiecePrio" => {
            each(ctx, params, |torrents, view| async move {
                torrents
                    .set_first_last_piece_first(view.job_id, !view.first_last_piece_first)
                    .await
            })
            .await
        }
        "setCategory" => {
            let category = params
                .get("category")
                .map(str::trim)
                .filter(|name| !name.is_empty())
                .map(str::to_owned);
            each(ctx, params, move |torrents, view| {
                let category = category.clone();
                async move { torrents.set_category(view.job_id, category).await }
            })
            .await
        }
        "recheck" => {
            each(ctx, params, |torrents, view| async move {
                torrents.recheck(view.job_id).await
            })
            .await
        }
        "reannounce" => reannounce(ctx, params).await,
        "categories" => axum::Json(categories(&*ctx.inner.config.read().await)).into_response(),
        "createCategory" => create_category(ctx, params, scope).await,
        "editCategory" => edit_category(ctx, params, scope).await,
        // A category is removed in Weaver's settings, beside what it routes.
        "removeCategories" => unavailable("Removing a category"),
        "setShareLimits" => set_share_limits(ctx, params).await,
        "uploadLimit" => rate_limits(ctx, params, |view| view.upload_limit).await,
        "downloadLimit" => rate_limits(ctx, params, |view| view.download_limit).await,
        "setUploadLimit" => set_rate_limit(ctx, params, Direction::Upload).await,
        "setDownloadLimit" => set_rate_limit(ctx, params, Direction::Download).await,
        // qBittorrent `increasePrioAction` and the others: up is toward the
        // front of the download queue.
        "increasePrio" => move_in_queue(ctx, params, QueueMoveTarget::Offset(-1)).await,
        "decreasePrio" => move_in_queue(ctx, params, QueueMoveTarget::Offset(1)).await,
        "topPrio" => move_in_queue(ctx, params, QueueMoveTarget::Top).await,
        "bottomPrio" => move_in_queue(ctx, params, QueueMoveTarget::Bottom).await,
        "setLocation" => set_location(ctx, params, "hashes", "location").await,
        "setSavePath" => set_location(ctx, params, "id", "path").await,
        // A torrent is written where it is saved: no download path is
        // all a client can ask.
        "setDownloadPath" if params.get("path").unwrap_or_default().is_empty() => ok(),
        "setDownloadPath" => unavailable("A download path"),
        "setAutoManagement" if params.flag("enable") == Some(true) => {
            unavailable("Automatic torrent management")
        }
        "setSuperSeeding" if params.flag("value") == Some(true) => unavailable("Super seeding"),
        "setAutoManagement" | "setSuperSeeding" => ok(),
        "addTrackers" => ctx.api().without_result(add_trackers(ctx, params).await),
        "editTracker" => ctx.api().edited_tracker(edit_tracker(ctx, params).await),
        "removeTrackers" => ctx.api().without_result(remove_trackers(ctx, params).await),
        "addTags" | "setTags" | "removeTags" => tag_torrents(ctx, action, params).await,
        "createTags" | "deleteTags" => make_tags(ctx, action, params).await,
        "tags" => match tag_snapshot(ctx).await {
            Ok(tags) => axum::Json(json!(tags.made())).into_response(),
            Err(error) => internal(error),
        },
        "addWebSeeds" | "editWebSeed" | "removeWebSeeds" => unavailable("Web seeds"),
        "addPeers" => unavailable("Adding peers"),
        "pieceHashes" | "pieceStates" | "pieceAvailability" => unavailable("Piece detail"),
        // A torrent keeps the names its metainfo gives it and its files.
        "rename" | "renameFile" | "renameFolder" => unavailable("Renaming"),
        "setComment" => unavailable("Editing a comment"),
        "SSLParameters" | "setSSLParameters" => unavailable("SSL torrents"),
        "fetchMetadata" | "parseMetadata" | "saveMetadata" => {
            unavailable("Reading metadata before adding")
        }
        "downloadFile" => unavailable("Downloading one file"),
        _ => no_endpoint(ctx.api(), "torrents"),
    }
}

// The `transfer` group (qBittorrent `TransferController`). Weaver has one
// set of global limits: the alternative limits are 0, none, and stay off.
async fn transfer_action(ctx: &TorrentFacadeContext, action: &str, params: &Params) -> Response {
    let required: &[&str] = match action {
        "setUploadLimit" | "setDownloadLimit" => &["limit"],
        "setSpeedLimits" => &["up_limit", "dl_limit", "alt_up_limit", "alt_dl_limit"],
        "setSpeedLimitsMode" => &["mode"],
        "banPeers" => &["peers"],
        _ => &[],
    };
    if let Err(response) = require(params, required) {
        return *response;
    }
    let bandwidth = ctx.inner.config.read().await.bandwidth;
    // A global limit as qBittorrent reads one: 0 is none.
    let reading = |bps: Option<u64>| bps.unwrap_or(0);
    // A limit as qBittorrent takes one: 0 or less is none.
    let taken = |name: &str| {
        let limit = params
            .get(name)
            .unwrap_or_default()
            .trim()
            .parse::<i64>()
            .unwrap_or(0);
        u64::try_from(limit).ok().filter(|limit| *limit > 0)
    };
    match action {
        "info" => {
            let views = ctx.views().await;
            axum::Json(transfer(
                &views,
                ctx.torrent_download_limit(),
                bandwidth.upload_bps,
            ))
            .into_response()
        }
        "uploadLimit" => text(StatusCode::OK, reading(bandwidth.upload_bps).to_string()),
        "downloadLimit" => text(StatusCode::OK, reading(bandwidth.download_bps).to_string()),
        "setUploadLimit" => save_rates(ctx, None, Some(taken("limit"))).await,
        "setDownloadLimit" => save_rates(ctx, Some(taken("limit")), None).await,
        "getSpeedLimits" => axum::Json(json!({
            "up_limit": reading(bandwidth.upload_bps),
            "dl_limit": reading(bandwidth.download_bps),
            "alt_up_limit": 0,
            "alt_dl_limit": 0,
        }))
        .into_response(),
        "setSpeedLimits" if taken("alt_up_limit").is_some() || taken("alt_dl_limit").is_some() => {
            unavailable("Alternative speed limits")
        }
        "setSpeedLimits" => ctx.api().without_result(
            save_rates(ctx, Some(taken("dl_limit")), Some(taken("up_limit"))).await,
        ),
        "speedLimitsMode" => text(StatusCode::OK, "0"),
        "setSpeedLimitsMode" => {
            match params.get("mode").unwrap_or_default().trim().parse::<i64>() {
                Ok(0) => ok(),
                Ok(_) => unavailable("Alternative speed limits"),
                Err(_) => text(StatusCode::BAD_REQUEST, "'mode': invalid argument"),
            }
        }
        "toggleSpeedLimitsMode" => unavailable("Alternative speed limits"),
        "banPeers" => ban_peers(ctx, params).await,
        "pauseSession" => ctx
            .api()
            .without_result(held(ctx.inner.handle.pause_torrents().await)),
        "resumeSession" => ctx
            .api()
            .without_result(held(ctx.inner.handle.resume_torrents().await)),
        _ => no_endpoint(ctx.api(), "transfer"),
    }
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
        // qBittorrent serialize_torrent.cpp:80-81.
        TorrentState::CheckingResumeData => "checkingResumeData",
        TorrentState::CheckingFiles => side("checkingDL", "checkingUP"),
        TorrentState::Downloading if view.forced => "forcedDL",
        TorrentState::Downloading => "downloading",
        TorrentState::Stalled | TorrentState::NeedsCertificate => side("stalledDL", "stalledUP"),
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

// Seconds to go (qBittorrent `TorrentImpl::eta`, torrentimpl.cpp:1403-1456):
// none for a stopped torrent; while downloading, the wanted bytes left at
// the download speed; once complete, the time to its seeding goal, the
// soonest goal under `MatchAny` and the last under `MatchAll`, each goal
// counted as Weaver meets it. Unknown is `ETA_UNKNOWN` (`MAX_ETA`).
fn eta(view: &TorrentView, now: i64) -> u64 {
    let stopped = view.paused_by.is_some()
        || matches!(view.state, TorrentState::Paused | TorrentState::Stopped);
    if stopped {
        return ETA_UNKNOWN;
    }
    if view.completed_at_ms.is_none() {
        let left = view.wanted_bytes.saturating_sub(view.wanted_have_bytes);
        return left.checked_div(view.download_bps).unwrap_or(ETA_UNKNOWN);
    }
    let policy = &view.seeding_policy;
    let mut etas = Vec::with_capacity(3);
    if let Some(goal) = policy.ratio_goal {
        let over = status::ratio_base(view.downloaded_bytes, view.total_bytes);
        let to_upload = over as f64 * f64::from(goal) - view.uploaded_bytes as f64;
        etas.push(if to_upload <= 0.0 {
            0
        } else if view.upload_bps > 0 {
            (to_upload / view.upload_bps as f64) as u64
        } else {
            ETA_UNKNOWN
        });
    }
    if let Some(minutes) = policy.seed_time_goal_minutes {
        etas.push((u64::from(minutes) * 60).saturating_sub(seeding_time(view)));
    }
    if let Some(minutes) = policy.inactive_goal_minutes {
        // Weaver counts no idle time for a torrent that never moved data.
        etas.push(view.last_activity_ms.map_or(ETA_UNKNOWN, |at| {
            let idle = u64::try_from(now - secs(at)).unwrap_or(0);
            (u64::from(minutes) * 60).saturating_sub(idle)
        }));
    }
    let eta = match policy.mode {
        SeedingGoalMode::Any => etas.into_iter().min(),
        SeedingGoalMode::All => etas.into_iter().max(),
    };
    eta.unwrap_or(ETA_UNKNOWN).min(ETA_UNKNOWN)
}

// The torrent's own share limits, each `-2` where it inherits, `-1` where
// it has no goal of its own, else its goal; and the limits in force, `-1`
// for no goal. Mode and action read `Default` while the torrent inherits
// them.
fn share_limits(view: &TorrentView) -> [(&'static str, Value); 8] {
    let policy = &view.seeding_policy;
    let own = view.seeding_override.unwrap_or_default();
    let minutes = |value: Option<u32>| json!(value.map_or(-1, i64::from));
    let ratio = |value: Option<f32>| value.map_or(json!(-1), |ratio| json!(ratio_number(ratio)));
    let leaf = |leaf: Leaf<Value>| match leaf {
        Leaf::Inherit => json!(-2),
        Leaf::Off => json!(-1),
        Leaf::Set(value) => value,
    };
    [
        (
            "ratio_limit",
            leaf(own.ratio_goal.map(|value| ratio(Some(value)))),
        ),
        (
            "seeding_time_limit",
            leaf(own.seed_time_goal_minutes.map(|value| minutes(Some(value)))),
        ),
        (
            "inactive_seeding_time_limit",
            leaf(own.inactive_goal_minutes.map(|value| minutes(Some(value)))),
        ),
        (
            "share_limits_mode",
            json!(own.mode.set().map_or("Default", limits_mode_name)),
        ),
        (
            "share_limit_action",
            json!(own.action.set().map_or("Default", limit_action_name)),
        ),
        ("max_ratio", ratio(policy.ratio_goal)),
        ("max_seeding_time", minutes(policy.seed_time_goal_minutes)),
        (
            "max_inactive_seeding_time",
            minutes(policy.inactive_goal_minutes),
        ),
    ]
}

// A ratio goal as the number it was given: Weaver keeps it as an `f32`, and
// 1.1 widened straight to an `f64` would read 1.100000023841858.
fn ratio_number(ratio: f32) -> f64 {
    ratio.to_string().parse().unwrap_or(f64::from(ratio))
}

// qBittorrent's `ShareLimitsMode` and `ShareLimitAction` names
// (`Utils::String::fromEnum`).
fn limits_mode_name(mode: SeedingGoalMode) -> &'static str {
    match mode {
        SeedingGoalMode::Any => "MatchAny",
        SeedingGoalMode::All => "MatchAll",
    }
}

fn limit_action_name(action: SeedingAction) -> &'static str {
    match action {
        SeedingAction::Stop => "Stop",
        SeedingAction::Remove => "Remove",
        SeedingAction::RemoveWithData => "RemoveWithContent",
    }
}

// A time the torrent may not have, in seconds since the epoch, -1 without
// one (qBittorrent `Utils::DateTime::toSecsSinceEpoch`).
fn when(ms: Option<i64>) -> i64 {
    ms.map_or(-1, secs)
}

// The bytes of the files it fetches, and of those the bytes it lacks
// (qBittorrent `wantedSize`, `remainingSize`).
fn wanted_and_left(view: &TorrentView) -> (u64, u64) {
    if view.files.is_empty() {
        return (
            view.total_bytes,
            view.total_bytes.saturating_sub(view.have_bytes),
        );
    }
    view.files
        .iter()
        .filter(|file| file.selected())
        .fold((0, 0), |(wanted, left), file| {
            let have = view
                .file_have
                .get(file.index as usize)
                .copied()
                .unwrap_or(0);
            (
                wanted + file.length,
                left + file.length.saturating_sub(have),
            )
        })
}

// The ratio per month active (qBittorrent `TorrentImpl::popularity`).
fn popularity(view: &TorrentView) -> f64 {
    const MONTH_SECS: f64 = 2_629_746.0;
    let months = view.active_seconds as f64 / MONTH_SECS;
    if months > 0.0 {
        view.ratio / months
    } else {
        0.0
    }
}

// Seconds seeding, as its seed-time goal counts them: every second seeded,
// or those of the current unbroken run. qBittorrent sends libtorrent's
// `finished_duration`, the time spent complete and not paused
// (serialize_torrent.cpp:181, torrentimpl.cpp:1030-1033).
fn seeding_time(view: &TorrentView) -> u64 {
    match view.seeding_policy.seed_time_counting {
        SeedTimeCounting::Cumulative => view.seeding_seconds,
        SeedTimeCounting::Consecutive => view.consecutive_seeding_seconds,
    }
}

// When it last sent or received payload, in seconds since the epoch, else
// when it was added (qBittorrent serialize_torrent.cpp:102-108).
fn last_activity(view: &TorrentView) -> i64 {
    secs(view.last_activity_ms.unwrap_or(view.added_at_ms))
}

// The folder holding a torrent of many files, empty for one file
// (qBittorrent `TorrentImpl::rootPath`).
fn root_path(view: &TorrentView) -> String {
    let (_, content) = view.paths();
    let single = matches!(view.files.as_slice(), [file] if file.path == view.name);
    if single { String::new() } else { content }
}

// A torrent as `torrents/info` lists it (qBittorrent `serialize`).
pub(super) fn info(view: &TorrentView, scope: CallerScope) -> Value {
    Value::Object(
        steady_fields(view, scope)
            .into_iter()
            .chain(clock_fields(view, now_secs()))
            .map(|(name, value)| (name.to_owned(), value))
            .collect(),
    )
}

// The fields of a torrent's row that move with the clock alone, read from
// the view's fields that do (`ViewParts`): the time active and seeding, when
// it last moved data, the countdowns, the ratio per month active, and when
// it was last seen complete.
fn clock_fields(view: &TorrentView, now: i64) -> [(&'static str, Value); CLOCK_KEYS] {
    [
        ("eta", json!(eta(view, now))),
        ("popularity", json!(popularity(view))),
        ("seen_complete", json!(when(view.last_seen_complete_ms))),
        ("time_active", json!(view.active_seconds)),
        ("seeding_time", json!(seeding_time(view))),
        ("last_activity", json!(last_activity(view))),
        ("reannounce", json!(view.reannounce_seconds.unwrap_or(0))),
    ]
}

// Every other field of a torrent's row, which moves only when the view does.
fn steady_fields(view: &TorrentView, scope: CallerScope) -> Vec<(&'static str, Value)> {
    let (save_path, content_path) = view.paths();
    let (wanted, left) = wanted_and_left(view);
    let has_metadata = view.pieces.is_some();
    let mut fields = Vec::from([
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
        ("has_metadata", json!(has_metadata)),
        (
            "created_by",
            json!(view.created_by.clone().unwrap_or_default()),
        ),
        ("creation_date", json!(when(view.created_at_ms))),
        (
            "private",
            if has_metadata {
                json!(view.private)
            } else {
                Value::Null
            },
        ),
        ("total_size", json!(view.total_bytes)),
        (
            "pieces_num",
            json!(view.pieces.map_or(-1, |pieces| i64::from(pieces.count))),
        ),
        (
            "piece_size",
            json!(view.pieces.map_or(-1, |pieces| pieces.size as i64)),
        ),
        ("magnet_uri", json!(view.magnet)),
        ("size", json!(wanted)),
        ("progress", json!(view.progress.clamp(0.0, 1.0))),
        ("total_wasted", json!(view.wasted_bytes)),
        (
            "pieces_have",
            json!(view.pieces.map_or(0, |pieces| pieces.have)),
        ),
        ("dlspeed", json!(view.download_bps)),
        ("upspeed", json!(view.upload_bps)),
        // Its place in the download queue from 1, and 0 for a seed, which
        // has none (qBittorrent `serialize_torrent.cpp`, `adjustQueuePosition`).
        ("priority", json!(view.queue_position.unwrap_or(0))),
        ("num_seeds", json!(view.seeds)),
        (
            "num_complete",
            json!(view.swarm_seeds.unwrap_or(view.seeds)),
        ),
        ("num_leechs", json!(view.leechers)),
        (
            "num_incomplete",
            json!(view.swarm_leechers.unwrap_or(view.leechers)),
        ),
        ("state", json!(state(view))),
        ("seq_dl", json!(view.sequential)),
        ("f_l_piece_prio", json!(view.first_last_piece_first)),
        ("category", json!(view.category.clone().unwrap_or_default())),
        // qBittorrent `serialize_torrent.cpp` joins them with ", ".
        ("tags", json!(view.tags.join(", "))),
        ("super_seeding", json!(false)),
        ("force_start", json!(view.forced)),
        ("save_path", json!(save_path)),
        ("download_path", json!("")),
        ("content_path", json!(content_path)),
        ("root_path", json!(root_path(view))),
        ("added_on", json!(secs(view.added_at_ms))),
        ("completion_on", json!(when(view.completed_at_ms))),
        ("tracker", json!(current_tracker(view, scope))),
        ("trackers_count", json!(view.trackers.len())),
        // All time, and this run alone, which a pause resets (qBittorrent
        // `serialize_torrent.cpp`: `all_time_download` and
        // `total_payload_download`).
        ("downloaded", json!(view.downloaded_bytes)),
        ("uploaded", json!(view.uploaded_bytes)),
        ("downloaded_session", json!(view.run_downloaded_bytes)),
        ("uploaded_session", json!(view.run_uploaded_bytes)),
        ("amount_left", json!(left)),
        ("completed", json!(view.have_bytes)),
        ("connections_count", json!(view.peers)),
        ("connections_limit", json!(view.max_peers)),
        ("ratio", json!(view.ratio)),
        ("auto_tmm", json!(false)),
        // Weaver does not count the copies the swarm holds.
        ("availability", json!(-1)),
        ("comment", json!(view.comment.clone().unwrap_or_default())),
    ]);
    // The limits as the client keeps them, 0 for none; a closed one has no
    // reading.
    for (name, bps) in [
        ("dl_limit", view.download_limit),
        ("up_limit", view.upload_limit),
    ] {
        if let Some(limit) = client_limit(bps, 0) {
            fields.push((name, limit));
        }
    }
    fields.extend(share_limits(view));
    fields
}

// What `sync/maindata` adds to a torrent's row: whether a tracker answered
// with a warning, failed, or could not be reached (qBittorrent
// `addAnnounceStats`).
fn announce_stats(view: &TorrentView) -> [(&'static str, Value); 3] {
    let any = |pick: fn(&TorrentTracker) -> bool| json!(view.trackers.iter().any(pick));
    [
        (
            "has_tracker_warning",
            any(|tracker| {
                tracker.status == TrackerStatus::Working
                    && tracker
                        .message
                        .as_deref()
                        .is_some_and(|message| !message.is_empty())
            }),
        ),
        (
            "has_tracker_error",
            any(|tracker| tracker.status == TrackerStatus::TrackerError),
        ),
        (
            "has_other_announce_error",
            any(|tracker| {
                matches!(
                    tracker.status,
                    TrackerStatus::NotWorking | TrackerStatus::Unreachable
                )
            }),
        ),
    ]
}

// Whether a torrent passes a `torrents/info` status filter (qBittorrent
// `TorrentFilter::matchStatus`); a filter the client does not define is
// all of them.
fn wanted(view: &TorrentView, filter: &str) -> bool {
    let state = state(view);
    let active = view.download_bps > 0 || view.upload_bps > 0;
    let stopped = matches!(state, "stoppedDL" | "stoppedUP");
    match filter {
        "downloading" => matches!(
            state,
            "downloading"
                | "metaDL"
                | "forcedMetaDL"
                | "stalledDL"
                | "checkingDL"
                | "stoppedDL"
                | "queuedDL"
                | "forcedDL"
        ),
        "seeding" => matches!(
            state,
            "uploading" | "stalledUP" | "checkingUP" | "queuedUP" | "forcedUP"
        ),
        "completed" => matches!(
            state,
            "uploading" | "stalledUP" | "checkingUP" | "stoppedUP" | "queuedUP" | "forcedUP"
        ),
        "stopped" => stopped,
        "running" => !stopped,
        "active" => active,
        "inactive" => !active,
        "stalled" => matches!(state, "stalledDL" | "stalledUP"),
        "stalled_uploading" => state == "stalledUP",
        "stalled_downloading" => state == "stalledDL",
        "checking" => matches!(state, "checkingDL" | "checkingUP" | "checkingResumeData"),
        "moving" => state == "moving",
        "errored" => matches!(state, "error" | "missingFiles"),
        _ => true,
    }
}

// qBittorrent's order of two values of a column: by number, text or flag,
// and none between values of different kinds.
fn less(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Number(left), Value::Number(right)) => {
            left.as_f64().unwrap_or(0.0) < right.as_f64().unwrap_or(0.0)
        }
        (Value::String(left), Value::String(right)) => left < right,
        (Value::Bool(left), Value::Bool(right)) => left < right,
        _ => false,
    }
}

// qBittorrent `infoAction`: the torrents the filters pass, sorted by a
// column and paged when asked.
async fn torrents_info(
    ctx: &TorrentFacadeContext,
    params: &Params,
    scope: CallerScope,
) -> Response {
    let hashes: Vec<&str> = params
        .get("hashes")
        .unwrap_or_default()
        .split('|')
        .map(str::trim)
        .filter(|hash| !hash.is_empty())
        .collect();
    let filter = params.get("filter").unwrap_or_default();
    let private = params.flag_value("private");
    // A tag the torrent carries; the empty tag passes the untagged ones
    // (qBittorrent `TorrentFilter::matchTag`).
    let tag = params.get("tag").map(str::trim);
    let with_files = params.flag("includeFiles").unwrap_or(false);
    let with_trackers = params.flag("includeTrackers").unwrap_or(false);
    let mut list: Vec<Value> = Vec::new();
    for view in ctx.views().await {
        let passes = tag.is_none_or(|tag| {
            if tag.is_empty() {
                view.tags.is_empty()
            } else {
                view.tags.iter().any(|carried| carried == tag)
            }
        }) && wanted(&view, filter)
            && (hashes.is_empty()
                || hashes
                    .iter()
                    .any(|hash| view.infohash.eq_ignore_ascii_case(hash)))
            && params.get("category").is_none_or(|name| {
                let category = view.category.as_deref().unwrap_or_default();
                category.eq_ignore_ascii_case(name)
                    || category
                        .get(..name.len() + 1)
                        .is_some_and(|parent| parent.eq_ignore_ascii_case(&format!("{name}/")))
            })
            && private.is_none_or(|private| view.private == private);
        if !passes {
            continue;
        }
        let mut row = info(&view, scope);
        if with_files && view.pieces.is_some() {
            row["files"] = json!(file_list(&view, 0..view.files.len()));
        }
        if with_trackers {
            row["trackers"] = json!(
                view.trackers
                    .iter()
                    .map(|tracker| tracker_json(tracker, scope, ctx.api()))
                    .collect::<Vec<_>>()
            );
        }
        list.push(row);
    }
    if let Some(column) = params.get("sort").filter(|column| !column.is_empty()) {
        if list.first().is_some_and(|row| row.get(column).is_none()) {
            return text(StatusCode::BAD_REQUEST, "'sort' parameter is invalid");
        }
        let reverse = params.flag("reverse").unwrap_or(false);
        list.sort_by(|left, right| {
            let (left, right) = (&left[column], &right[column]);
            let (left, right) = if reverse {
                (right, left)
            } else {
                (left, right)
            };
            if less(left, right) {
                std::cmp::Ordering::Less
            } else if less(right, left) {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Equal
            }
        });
    }
    let number = |name: &str| {
        params
            .get(name)
            .and_then(|value| value.trim().parse::<i64>().ok())
            .unwrap_or(0)
    };
    let (limit, mut offset) = (number("limit"), number("offset"));
    let size = i64::try_from(list.len()).unwrap_or(i64::MAX);
    // A negative offset counts back from the end; a limit of 0 or less is
    // none.
    if offset < 0 {
        offset += size;
    }
    if limit > 0 || offset > 0 {
        let start = offset.clamp(0, size);
        let end = if limit > 0 {
            start.saturating_add(limit).min(size)
        } else {
            size
        };
        list = list
            .drain(usize::try_from(start).unwrap_or(0)..usize::try_from(end).unwrap_or(0))
            .collect();
    }
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
    text(StatusCode::NOT_FOUND, "Not Found")
}

// A ratio as qBittorrent reports one: -1 from its cap up
// (`Torrent::MAX_RATIO`).
fn capped(ratio: f64) -> f64 {
    const MAX_RATIO: f64 = 9999.0;
    if ratio >= MAX_RATIO { -1.0 } else { ratio }
}

// qBittorrent `propertiesAction`.
async fn properties(ctx: &TorrentFacadeContext, params: &Params) -> Response {
    let Some(view) = ctx.by_hash(params.get("hash").unwrap_or_default()).await else {
        return not_found();
    };
    let now = now_secs();
    let (save_path, _) = view.paths();
    let has_metadata = view.pieces.is_some();
    // The average rates over the time spent downloading, the time active
    // less every second complete, and over the time active
    // (torrentscontroller.cpp:819-820).
    let average = |bytes: u64, secs: u64| {
        bytes
            .checked_div(secs)
            .map_or(json!(-1), |rate| json!(rate))
    };
    let fields = object([
        (
            "infohash_v1",
            json!(view.infohash_v1.clone().unwrap_or_default()),
        ),
        (
            "infohash_v2",
            json!(view.infohash_v2.clone().unwrap_or_default()),
        ),
        ("name", json!(view.name)),
        ("hash", json!(view.infohash.to_ascii_lowercase())),
        ("time_elapsed", json!(view.active_seconds)),
        ("seeding_time", json!(seeding_time(&view))),
        ("eta", json!(eta(&view, now))),
        ("nb_connections", json!(view.peers)),
        ("nb_connections_limit", json!(view.max_peers)),
        ("total_downloaded", json!(view.downloaded_bytes)),
        ("total_downloaded_session", json!(view.run_downloaded_bytes)),
        ("total_uploaded", json!(view.uploaded_bytes)),
        ("total_uploaded_session", json!(view.run_uploaded_bytes)),
        ("dl_speed", json!(view.download_bps)),
        (
            "dl_speed_avg",
            average(
                view.downloaded_bytes,
                view.active_seconds.saturating_sub(view.seeding_seconds),
            ),
        ),
        ("up_speed", json!(view.upload_bps)),
        (
            "up_speed_avg",
            average(view.uploaded_bytes, view.active_seconds),
        ),
        ("total_wasted", json!(view.wasted_bytes)),
        ("seeds", json!(view.seeds)),
        ("seeds_total", json!(view.swarm_seeds.unwrap_or(view.seeds))),
        ("peers", json!(view.leechers)),
        (
            "peers_total",
            json!(view.swarm_leechers.unwrap_or(view.leechers)),
        ),
        ("share_ratio", json!(capped(view.ratio))),
        ("popularity", json!(capped(popularity(&view)))),
        // Weaver does not count the copies the swarm holds.
        ("availability", json!(-1)),
        ("reannounce", json!(view.reannounce_seconds.unwrap_or(0))),
        ("total_size", json!(view.total_bytes)),
        (
            "pieces_num",
            json!(view.pieces.map_or(-1, |pieces| i64::from(pieces.count))),
        ),
        (
            "piece_size",
            json!(view.pieces.map_or(-1, |pieces| pieces.size as i64)),
        ),
        (
            "pieces_have",
            json!(view.pieces.map_or(-1, |pieces| i64::from(pieces.have))),
        ),
        (
            "created_by",
            json!(view.created_by.clone().unwrap_or_default()),
        ),
        ("is_private", json!(view.private)),
        (
            "private",
            if has_metadata {
                json!(view.private)
            } else {
                Value::Null
            },
        ),
        ("addition_date", json!(secs(view.added_at_ms))),
        ("last_seen", json!(when(view.last_seen_complete_ms))),
        ("completion_date", json!(when(view.completed_at_ms))),
        ("creation_date", json!(when(view.created_at_ms))),
        ("save_path", json!(save_path)),
        ("download_path", json!("")),
        ("comment", json!(view.comment.clone().unwrap_or_default())),
        ("has_metadata", json!(has_metadata)),
        ("progress", json!(view.progress.clamp(0.0, 1.0))),
    ]);
    // The properties read no limit as -1, where the list reads it as 0.
    let fields = with_limits(
        fields,
        -1,
        [
            ("dl_limit", view.download_limit),
            ("up_limit", view.upload_limit),
        ],
    );
    axum::Json(fields).into_response()
}

// qBittorrent `filesAction`: the torrent's files, or those `indexes`
// names, `|`-separated.
async fn files(ctx: &TorrentFacadeContext, params: &Params) -> Response {
    let Some(view) = ctx.by_hash(params.get("hash").unwrap_or_default()).await else {
        return not_found();
    };
    if view.pieces.is_none() {
        return axum::Json(json!([])).into_response();
    }
    let Some(wanted) = params.get("indexes") else {
        return axum::Json(file_list(&view, 0..view.files.len())).into_response();
    };
    let mut indexes = Vec::new();
    for index in wanted.split('|') {
        match index.trim().parse::<usize>() {
            Ok(index) if index < view.files.len() => indexes.push(index),
            Ok(_) => {
                return text(
                    StatusCode::CONFLICT,
                    format!("Index {index} is out of bounds."),
                );
            }
            Err(_) => {
                return text(
                    StatusCode::CONFLICT,
                    format!("\"{index}\" is not a valid file index."),
                );
            }
        }
    }
    axum::Json(file_list(&view, indexes)).into_response()
}

// The files at `indexes` (qBittorrent `getFiles`), the first saying
// whether the torrent is done.
fn file_list(view: &TorrentView, indexes: impl IntoIterator<Item = usize>) -> Vec<Value> {
    // The pieces each file spans, from where it starts in the torrent.
    let piece_size = view.pieces.map_or(1, |pieces| pieces.size.max(1));
    let mut start = 0u64;
    let ranges: Vec<[u64; 2]> = view
        .files
        .iter()
        .map(|file| {
            let first = start / piece_size;
            let last = (start + file.length.max(1) - 1) / piece_size;
            start += file.length;
            [first, last]
        })
        .collect();
    let mut list: Vec<Value> = indexes
        .into_iter()
        .map(|index| {
            let file = &view.files[index];
            json!({
                "index": file.index,
                "name": file.path,
                "size": file.length,
                "progress": view.file_progress(file),
                "priority": qbt_priority(file.priority),
                "piece_range": ranges[index],
                // Weaver does not count the copies the swarm holds.
                "availability": -1,
            })
        })
        .collect();
    if let Some(first) = list.first_mut() {
        first["is_seed"] = json!(view.completed_at_ms.is_some());
    }
    list
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

// qBittorrent `TorrentImpl::currentTracker`, as `TorrentView::current_tracker`
// says it. The URL is redacted for a caller that only reads, as in
// `torrents/trackers`.
fn current_tracker(view: &TorrentView, scope: CallerScope) -> String {
    match view.current_tracker() {
        None => String::new(),
        Some(tracker) if scope.can_control() => tracker.url.clone(),
        Some(tracker) => redacted_tracker_url(&tracker.url),
    }
}

// `torrents/trackers` (qBittorrent `TorrentsController::trackersAction`):
// the DHT, PeX and LSD rows, then the torrent's trackers.
async fn trackers(ctx: &TorrentFacadeContext, params: &Params, scope: CallerScope) -> Response {
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
    let dht = torrents.carries_dht(view.scope).await;
    let mut list = sticky_trackers(
        view.private,
        &peers,
        [
            ("** [DHT] **", PeerSourceFlag::Dht, dht),
            ("** [PeX] **", PeerSourceFlag::Pex, PEX),
            ("** [LSD] **", PeerSourceFlag::Lsd, LSD),
        ],
    );
    list.extend(
        view.trackers
            .iter()
            .map(|tracker| tracker_json(tracker, scope, ctx.api())),
    );
    axum::Json(list).into_response()
}

// qBittorrent `getStickyTrackers`: a row for each source of peers, working
// while it is on and the torrent is not private, with the seeds and leeches
// it brought among the connected peers.
fn sticky_trackers(
    private: bool,
    peers: &[PeerInfo],
    sources: [(&str, PeerSourceFlag, bool); 3],
) -> Vec<Value> {
    sources
        .into_iter()
        .map(|(url, source, on)| {
            let (seeds, leeches) = peers
                .iter()
                .filter(|peer| peer.sources.contains(source))
                .fold((0, 0), |(seeds, leeches), peer| {
                    if peer.flags.seed {
                        (seeds + 1, leeches)
                    } else {
                        (seeds, leeches + 1)
                    }
                });
            json!({
                "url": url,
                "tier": -1,
                "msg": if private { "This torrent is private" } else { "" },
                "status": if on && !private { 2 } else { 0 },
                "num_peers": 0,
                "num_downloaded": 0,
                "num_seeds": seeds,
                "num_leeches": leeches,
            })
        })
        .collect()
}

// qBittorrent `getTrackers`, each time in seconds since the epoch and -1
// for a count the tracker has not given. A caller that may change trackers
// sees the whole URL; one that only reads, its scheme, host and port.
fn tracker_json(tracker: &TorrentTracker, scope: CallerScope, api: Api) -> Value {
    let count = |value: Option<u32>| value.map_or(-1, i64::from);
    let seconds = |ms: Option<i64>| ms.map_or(0, |ms| ms / 1000);
    let url = if scope.can_control() {
        tracker.url.clone()
    } else {
        redacted_tracker_url(&tracker.url)
    };
    json!({
        "url": url,
        "tier": tracker.tier,
        "source": match tracker.named_by {
            NamedBy::Torrent => "TORRENT",
            NamedBy::Operator => "OPERATOR",
        },
        "updating": tracker.updating,
        "status": api.tracker_status(tracker.status),
        "msg": tracker.message.clone().unwrap_or_default(),
        "num_peers": count(tracker.peers),
        "num_seeds": count(tracker.seeders),
        "num_leeches": count(tracker.leechers),
        "num_downloaded": count(tracker.downloaded),
        "next_announce": seconds(tracker.next_announce_ms),
        "min_announce": seconds(tracker.min_announce_ms),
        "endpoints": [],
        "resolved_by": tracker.resolved_by.map(resolved_by_name),
    })
}

// Who resolves a tracker's host, as Weaver names it.
fn resolved_by_name(by: ResolvedBy) -> &'static str {
    match by {
        ResolvedBy::Literal => "LITERAL",
        ResolvedBy::Host => "HOST",
        ResolvedBy::Servers => "SERVERS",
        ResolvedBy::Proxy => "PROXY",
        ResolvedBy::ProxyServers => "PROXY_SERVERS",
    }
}

// A `|`-separated list whose parts a client may percent-encode, as
// qBittorrent reads `urls` (`QUrl::fromPercentEncoding`).
fn url_list(list: &str) -> Vec<String> {
    list.split('|')
        .filter(|part| !part.is_empty())
        .map(percent_decoded)
        .collect()
}

fn percent_decoded(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut at = 0;
    while at < bytes.len() {
        let hex = bytes
            .get(at + 1..at + 3)
            .and_then(|hex| std::str::from_utf8(hex).ok())
            .and_then(|hex| u8::from_str_radix(hex, 16).ok());
        match (bytes[at], hex) {
            (b'%', Some(byte)) => {
                out.push(byte);
                at += 3;
            }
            (byte, _) => {
                out.push(byte);
                at += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

// `torrents/addTrackers` (qBittorrent `addTrackersAction`): one tracker a
// line, each blank line starting the next tier (`parseTrackerEntries`).
// The tiers go after each torrent's last.
async fn add_trackers(ctx: &TorrentFacadeContext, params: &Params) -> Response {
    let (Some(hash), Some(urls)) = (params.get("hash"), params.get("urls")) else {
        return text(StatusCode::BAD_REQUEST, "Missing parameter: hash, urls");
    };
    let mut tiers: Vec<Vec<String>> = vec![Vec::new()];
    for line in urls.split('\n').map(str::trim) {
        match tiers.last_mut() {
            Some(tier) if !line.is_empty() => tier.push(line.to_owned()),
            _ => tiers.push(Vec::new()),
        }
    }
    each_of(ctx, ctx.named(hash).await, move |torrents, view| {
        let tiers = tiers.clone();
        async move { torrents.add_trackers(view.job_id, tiers).await }
    })
    .await
}

// `torrents/editTracker` (qBittorrent `editTrackerAction`): a new URL or
// tier for one tracker, then an announce to every tracker.
async fn edit_tracker(ctx: &TorrentFacadeContext, params: &Params) -> Response {
    // The tracker is `url` from 2.13.0, `origUrl` before it.
    let url = params.get("url").or_else(|| params.get("origUrl"));
    let (Some(hash), Some(url)) = (params.get("hash"), url) else {
        return text(
            StatusCode::BAD_REQUEST,
            "Missing required parameters: hash, url",
        );
    };
    let url = url.trim();
    let Some(torrents) = &ctx.inner.torrents else {
        return not_found();
    };
    let Some(view) = ctx.by_hash(hash).await else {
        return not_found();
    };
    let (new_url, new_tier) = (params.get("newUrl"), params.get("tier"));
    if new_url.is_none() && new_tier.is_none() {
        return text(
            StatusCode::BAD_REQUEST,
            "Must specify at least one of [newUrl, tier]",
        );
    }
    let new_tier = match new_tier.map(|tier| tier.trim().parse::<i64>()) {
        None => None,
        Some(Ok(tier @ 0..=255)) => Some(tier as u32),
        Some(Ok(_)) => return text(StatusCode::BAD_REQUEST, "tier must be between 0 and 255"),
        Some(Err(_)) => return text(StatusCode::BAD_REQUEST, "tier must be an integer"),
    };
    let new_url = new_url
        .map(str::trim)
        .filter(|new| *new != url)
        .map(str::to_owned);
    if new_url.is_none() && new_tier.is_none() {
        return ok();
    }
    let mut found = false;
    let mut entries = Vec::with_capacity(view.trackers.len());
    for tracker in &view.trackers {
        if new_url.as_deref() == Some(tracker.url.as_str()) {
            return text(StatusCode::CONFLICT, "New tracker URL already exists");
        }
        let mut entry = (tracker.tier, tracker.url.clone());
        if tracker.url == url {
            if new_url.is_none() && new_tier == Some(tracker.tier) {
                return ok();
            }
            found = true;
            entry.1 = new_url.clone().unwrap_or(entry.1);
            entry.0 = new_tier.unwrap_or(entry.0);
        }
        entries.push(entry);
    }
    if !found {
        return text(StatusCode::CONFLICT, "Tracker not found");
    }
    let edited = match torrents.replace_trackers(view.job_id, entries).await {
        Ok(()) => torrents.reannounce(view.job_id).await,
        Err(error) => Err(error),
    };
    // The trackers kept are the torrent's own: only the new URL can be one
    // the service refuses.
    match edited {
        Ok(()) => ok(),
        Err(ActionError::NotFound(_)) => not_found(),
        Err(ActionError::Invalid(_)) => text(StatusCode::BAD_REQUEST, "New tracker URL is invalid"),
        Err(error) => internal(error),
    }
}

// `torrents/removeTrackers` (qBittorrent `removeTrackersAction`): `*`
// names every torrent.
async fn remove_trackers(ctx: &TorrentFacadeContext, params: &Params) -> Response {
    let (Some(hash), Some(urls)) = (params.get("hash"), params.get("urls")) else {
        return text(StatusCode::BAD_REQUEST, "Missing parameter: hash, urls");
    };
    let hash = if hash.trim() == "*" { "all" } else { hash };
    let urls = url_list(urls);
    each_of(ctx, ctx.named(hash).await, move |torrents, view| {
        let urls = urls.clone();
        async move { torrents.remove_trackers(view.job_id, &urls).await }
    })
    .await
}

// `torrents/reannounce` (qBittorrent `reannounceAction`): the trackers
// `urls` names, else every tracker and the DHT.
async fn reannounce(ctx: &TorrentFacadeContext, params: &Params) -> Response {
    let urls = url_list(params.get("urls").unwrap_or_default());
    each_of(ctx, ctx.targets(params).await, move |torrents, view| {
        let urls = urls.clone();
        async move {
            if urls.is_empty() {
                torrents.reannounce(view.job_id).await?;
                return torrents.dht_announce(view.job_id).await;
            }
            for tracker in view.trackers.iter().filter(|t| urls.contains(&t.url)) {
                torrents
                    .reannounce_tracker(view.job_id, &tracker.url)
                    .await?;
            }
            Ok(())
        }
    })
    .await
}

// qBittorrent `addAction`: every file and url added with the same terms.
// A url is fetched when it is added, over the one leg of the route its
// torrent takes, with the client's cookie; one that cannot be fetched, or
// a torrent already added, is a failure.
async fn add(ctx: &TorrentFacadeContext, params: Params) -> Response {
    let Some(torrents) = &ctx.inner.torrents else {
        return text(StatusCode::CONFLICT, "torrents are not enabled");
    };
    let urls: Vec<&str> = params
        .get("urls")
        .unwrap_or_default()
        .split('\n')
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect();
    // Weaver reads no metadata before a torrent is added, so no file has a
    // level before it is.
    if !params
        .get("filePriorities")
        .unwrap_or_default()
        .trim()
        .is_empty()
    {
        let refusal = if urls.len() > 1 {
            "Cannot specify filePriorities when adding multiple torrents"
        } else if !params.files.is_empty() {
            "Cannot specify filePriorities when uploading torrent files"
        } else {
            "`filePriorities` may only be specified when metadata has already been fetched"
        };
        return text(StatusCode::BAD_REQUEST, refusal);
    }
    if !params.get("rename").unwrap_or_default().trim().is_empty() {
        return unavailable("Renaming");
    }
    let category = params
        .get("category")
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(str::to_owned);
    let policy_override = match share_policy(&params, None, false) {
        Ok(policy) => policy,
        Err(response) => return *response,
    };
    let (upload_limit, download_limit) =
        match (add_limit(&params, "upLimit"), add_limit(&params, "dlLimit")) {
            (Ok(upload), Ok(download)) => (upload, download),
            (Err(response), _) | (_, Err(response)) => return *response,
        };
    let mut sources = Vec::with_capacity(params.files.len() + urls.len());
    for (name, body) in &params.files {
        match classify(body).torrent(body.clone()) {
            Ok(source) => sources.push(Ok(source)),
            Err(_) => {
                return text(
                    StatusCode::UNSUPPORTED_MEDIA_TYPE,
                    format!("Error: '{name}' is not a valid torrent file."),
                );
            }
        }
    }
    for line in urls {
        let body = match classify(line.as_bytes()) {
            Kind::Url(url) => match ctx.inner.handle.proxy_runtime() {
                Some(runtime) => torrents
                    .fetch_url(&runtime, category.as_deref(), &url, params.get("cookie"))
                    .await
                    .map_err(|error| error.to_string()),
                None => Err("torrent request has no route".to_owned()),
            },
            _ => Ok(line.as_bytes().to_vec()),
        };
        sources.push(body.and_then(|body| {
            classify(&body)
                .torrent(body)
                .map_err(|_| format!("{line} is not a torrent"))
        }));
    }
    let stopped = params
        .flag("stopped")
        .or_else(|| params.flag("paused"))
        .unwrap_or(false);
    let sequential = params.flag("sequentialDownload").unwrap_or(false);
    let first_last = params.flag("firstLastPiecePrio").unwrap_or(false);
    // Off unless asked, as qBittorrent's `isAddTorrentToQueueTop` is.
    let to_top = params.flag_value("addToTopOfQueue").unwrap_or(false);
    let placement = placement(&params);
    // A comma list; empty parts are skipped (qBittorrent `torrents/add`).
    let tags = params.get("tags").map(split_tags).unwrap_or_default();
    let mut ids = Vec::new();
    let mut failed = 0usize;
    let mut refusal: Option<String> = None;
    for source in sources {
        let added = match source {
            Ok(source) => torrents
                .add(AddRequest {
                    source,
                    category: category.clone(),
                    policy_override,
                    paused: stopped,
                    forced: params.flag("forced").unwrap_or(false),
                    unpack: None,
                    metadata: Vec::new(),
                    upload_limit,
                    download_limit,
                    placement: placement.clone(),
                    tags: tags.clone(),
                })
                .await
                .map_err(|error| match error {
                    AddError::State(error) => Err(error),
                    refused => Ok(refused.to_string()),
                }),
            Err(error) => Err(Ok(error)),
        };
        match added {
            Ok(job_id) => {
                if sequential && let Err(error) = torrents.set_sequential(job_id, true).await {
                    tracing::warn!(%error, "added torrent did not take sequential download");
                }
                if first_last
                    && let Err(error) = torrents.set_first_last_piece_first(job_id, true).await
                {
                    tracing::warn!(%error, "added torrent did not take first and last pieces first");
                }
                // Once it is in the queue, as qBittorrent calls
                // `queue_position_top` once the torrent is added.
                if to_top
                    && let Err(error) = torrents.reorder(&[job_id], QueueMoveTarget::Top).await
                {
                    tracing::warn!(%error, "added torrent did not take the top of the queue");
                }
                if let Some(view) = torrents.torrent(job_id).await {
                    ids.push(view.infohash.to_ascii_lowercase());
                }
            }
            Err(Ok(why)) => {
                failed += 1;
                refusal.get_or_insert(why);
            }
            Err(Err(error)) => return internal(error),
        }
    }
    if ids.is_empty() && failed == 0 {
        return text(StatusCode::BAD_REQUEST, "no torrents or urls");
    }
    ctx.api().added(&ids, failed, refusal)
}

// Where an added torrent's files go, as qBittorrent `addAction` and
// `SessionImpl::addTorrent` read it. A save path is taken unless `autoTMM`
// asks for the category's; with no `autoTMM` qBittorrent manages a torrent
// by hand. A download path is taken unless `useDownloadPath` turns it off.
// `seedMode` seeds the files as they are; `skip_checking`, the name the
// qBittorrent API documents for it and many clients send, means the same. A layout
// qBittorrent does not name is its default, `Original`, and with no
// `contentLayout` the older `root_folder` picks one.
fn placement(params: &Params) -> Placement {
    let managed = params.flag_value("autoTMM").unwrap_or(false);
    let path = |name: &str| {
        params
            .get(name)
            .map(str::trim)
            .filter(|path| !path.is_empty() && !managed)
            .map(str::to_owned)
    };
    let layout = match params
        .get("contentLayout")
        .filter(|value| !value.is_empty())
    {
        Some(value) => Some(ContentLayout::parse(value).unwrap_or(ContentLayout::Original)),
        None => params.flag_value("root_folder").map(|folder| {
            if folder {
                ContentLayout::Subfolder
            } else {
                ContentLayout::NoSubfolder
            }
        }),
    };
    Placement {
        save_path: path("savepath"),
        layout,
        download_path: path("downloadPath")
            .filter(|_| params.flag_value("useDownloadPath").unwrap_or(true)),
        seed_mode: params.flag("seedMode").unwrap_or(false)
            || params.flag("skip_checking").unwrap_or(false),
    }
}

// One share limit over the level's current one: absent leaves it as it
// is, `-2` inherits, `-1` is no goal at this level whatever the levels
// above set, and a value from 0 up is the goal.
fn share_limit(
    params: &Params,
    name: &str,
    current: Leaf<f64>,
    strict: bool,
) -> Result<Leaf<f64>, Box<Response>> {
    let Some(value) = params.get(name) else {
        return Ok(current);
    };
    match value.trim().parse::<f64>() {
        Ok(value) if value >= 0.0 => Ok(Leaf::Set(value)),
        Ok(-2.0) => Ok(Leaf::Inherit),
        Ok(-1.0) => Ok(Leaf::Off),
        // Outside a category, any other negative limit is none
        // (`TorrentImpl::setShareLimits`), and a value that is not a number
        // leaves the limit as it is (`addAction` reads it as the default).
        Ok(value) if !strict && value < 0.0 => Ok(Leaf::Off),
        Err(_) if !strict => Ok(current),
        _ => Err(Box::new(text(
            StatusCode::BAD_REQUEST,
            format!("{name} is not a limit"),
        ))),
    }
}

// A share-limit mode or action by its qBittorrent name: absent leaves it as
// it is, and `Default` takes the one inherited. A name the client does not
// define is refused where `strict` (`applyShareLimitsParams`), else read
// as `Default` (`setShareLimitsAction`).
fn share_named<T>(
    params: &Params,
    name: &str,
    of: fn(&str) -> Option<Option<T>>,
    strict: bool,
) -> Result<Option<Option<T>>, Box<Response>> {
    let Some(value) = params.get(name) else {
        return Ok(None);
    };
    match of(value.trim()) {
        Some(named) => Ok(Some(named)),
        None if strict => Err(Box::new(text(
            StatusCode::BAD_REQUEST,
            format!("'{name}' parameter has invalid value"),
        ))),
        None => Ok(Some(None)),
    }
}

fn limits_mode_of(name: &str) -> Option<Option<SeedingGoalMode>> {
    match name {
        "Default" => Some(None),
        "MatchAny" => Some(Some(SeedingGoalMode::Any)),
        "MatchAll" => Some(Some(SeedingGoalMode::All)),
        _ => None,
    }
}

fn limit_action_of(name: &str) -> Option<Option<SeedingAction>> {
    match name {
        "Default" => Some(None),
        "Stop" => Some(Some(SeedingAction::Stop)),
        "Remove" => Some(Some(SeedingAction::Remove)),
        "RemoveWithContent" => Some(Some(SeedingAction::RemoveWithData)),
        _ => None,
    }
}

// A level's seeding terms from the client's share limits, over its current
// ones: a torrent's, or a category's. Terms that set nothing are none.
fn share_policy(
    params: &Params,
    current: Option<SeedingPolicy>,
    strict: bool,
) -> Result<Option<SeedingPolicy>, Box<Response>> {
    if params.get("shareLimitAction").map(str::trim) == Some("EnableSuperSeeding") {
        return Err(Box::new(unavailable("Super seeding")));
    }
    let current = current.unwrap_or_default();
    fn named<T>(named: Option<Option<T>>, current: Leaf<T>) -> Leaf<T> {
        match named {
            None => current,
            Some(None) => Leaf::Inherit,
            Some(Some(value)) => Leaf::Set(value),
        }
    }
    let mode = named(
        share_named(params, "shareLimitsMode", limits_mode_of, strict)?,
        current.mode,
    );
    let action = named(
        share_named(params, "shareLimitAction", limit_action_of, strict)?,
        current.action,
    );
    let minutes = |value: f64| value.round().min(f64::from(u32::MAX)) as u32;
    let ratio = share_limit(
        params,
        "ratioLimit",
        current.ratio_goal.map(f64::from),
        strict,
    )?;
    let seeding = share_limit(
        params,
        "seedingTimeLimit",
        current.seed_time_goal_minutes.map(f64::from),
        strict,
    )?;
    let inactive = share_limit(
        params,
        "inactiveSeedingTimeLimit",
        current.inactive_goal_minutes.map(f64::from),
        strict,
    )?;
    Ok(SeedingPolicy::kept(Some(SeedingPolicy {
        ratio_goal: ratio.map(|ratio| ratio as f32),
        seed_time_goal_minutes: seeding.map(minutes),
        inactive_goal_minutes: inactive.map(minutes),
        mode,
        action,
        seed_time_counting: current.seed_time_counting,
    })))
}

async fn set_share_limits(ctx: &TorrentFacadeContext, params: &Params) -> Response {
    let Some(torrents) = &ctx.inner.torrents else {
        return ok();
    };
    for view in ctx.targets(params).await {
        let policy = match share_policy(params, view.seeding_override, false) {
            Ok(policy) => policy,
            Err(response) => return *response,
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

// The tags as stored: from memory when nothing changed them since the last
// read, else read off the async workers.
async fn tag_snapshot(
    ctx: &TorrentFacadeContext,
) -> Result<Arc<TagSnapshot>, weaver_server_core::StateError> {
    if let Some(tags) = ctx.inner.db.cached_tag_snapshot() {
        return Ok(tags);
    }
    let db = ctx.inner.db.clone();
    tokio::task::spawn_blocking(move || db.tag_snapshot())
        .await
        .map_err(|error| weaver_server_core::StateError::Database(error.to_string()))?
}

// One change to the tags, run off the async workers.
async fn change_tags(
    ctx: &TorrentFacadeContext,
    change: impl FnOnce(&Database) -> Result<(), weaver_server_core::StateError> + Send + 'static,
) -> Result<(), Response> {
    let db = ctx.inner.db.clone();
    match tokio::task::spawn_blocking(move || change(&db)).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(internal(error)),
        Err(error) => Err(internal(error)),
    }
}

// qBittorrent `addTagsAction`, `setTagsAction` and `removeTagsAction` on the
// torrents `hashes` names, unknown ones skipped. Adding or setting a tag
// makes it; removing no tag takes every tag off.
async fn tag_torrents(ctx: &TorrentFacadeContext, action: &str, params: &Params) -> Response {
    let named = params.get("tags").unwrap_or_default();
    let tags = split_tags(named);
    let any_named = named.split(',').any(|part| !part.is_empty());
    let jobs: Vec<JobId> = ctx
        .targets(params)
        .await
        .iter()
        .map(|view| view.job_id)
        .collect();
    let changed = match action {
        "addTags" => {
            change_tags(ctx, move |db| {
                db.add_job_tags(&jobs, tags.iter().map(String::as_str))
            })
            .await
        }
        "setTags" => {
            change_tags(ctx, move |db| {
                db.set_job_tags(&jobs, tags.iter().map(String::as_str))
            })
            .await
        }
        // A list that names only tags that are not tags removes nothing.
        _ if any_named && tags.is_empty() => Ok(()),
        _ => {
            change_tags(ctx, move |db| {
                db.remove_job_tags(&jobs, tags.iter().map(String::as_str))
            })
            .await
        }
    };
    match changed {
        Ok(()) if action == "setTags" => ctx.api().no_result(),
        Ok(()) => ok(),
        Err(response) => response,
    }
}

// qBittorrent `createTagsAction` and `deleteTagsAction`.
async fn make_tags(ctx: &TorrentFacadeContext, action: &str, params: &Params) -> Response {
    let tags = split_tags(params.get("tags").unwrap_or_default());
    let changed = if action == "createTags" {
        change_tags(ctx, move |db| {
            db.create_tags(tags.iter().map(String::as_str))
        })
        .await
    } else {
        change_tags(ctx, move |db| {
            db.delete_tags(tags.iter().map(String::as_str))
        })
        .await
    };
    match changed {
        Ok(()) => ok(),
        Err(response) => response,
    }
}

// Runs one action on every torrent `hashes` names. Unknown hashes are
// skipped, as the client does.
async fn each<F, Fut>(ctx: &TorrentFacadeContext, params: &Params, action: F) -> Response
where
    F: Fn(TorrentService, TorrentView) -> Fut,
    Fut: std::future::Future<Output = Result<(), ActionError>>,
{
    each_of(ctx, ctx.targets(params).await, action).await
}

// The torrents `hashes` names moved in the download queue together, as
// qBittorrent moves them (see `TorrentService::reorder`).
async fn move_in_queue(
    ctx: &TorrentFacadeContext,
    params: &Params,
    target: QueueMoveTarget,
) -> Response {
    let Some(torrents) = &ctx.inner.torrents else {
        return ok();
    };
    let job_ids: Vec<JobId> = ctx
        .targets(params)
        .await
        .iter()
        .map(|view| view.job_id)
        .collect();
    match torrents.reorder(&job_ids, target).await {
        Ok(()) | Err(ActionError::NotFound(_)) => ok(),
        Err(error @ ActionError::Invalid(_)) => text(StatusCode::CONFLICT, error.to_string()),
        Err(error) => internal(error),
    }
}

// `action` on each of `views`, as `each` does it.
async fn each_of<F, Fut>(ctx: &TorrentFacadeContext, views: Vec<TorrentView>, action: F) -> Response
where
    F: Fn(TorrentService, TorrentView) -> Fut,
    Fut: std::future::Future<Output = Result<(), ActionError>>,
{
    let Some(torrents) = &ctx.inner.torrents else {
        return ok();
    };
    for view in views {
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

// A limit in bytes a second as the client names it under `key`: its 0 and
// any negative number are no limit.
fn rate_limit_of(params: &Params, key: &str) -> Option<Result<Option<u64>, Box<Response>>> {
    params
        .get(key)
        .map(|limit| match limit.trim().parse::<i64>() {
            Ok(limit) => Ok(u64::try_from(limit).ok().filter(|limit| *limit > 0)),
            Err(_) => Err(Box::new(text(
                StatusCode::BAD_REQUEST,
                format!("{key} is not a number"),
            ))),
        })
}

// A limit an added torrent takes, none when the client names none.
fn add_limit(params: &Params, key: &str) -> Result<Option<u64>, Box<Response>> {
    rate_limit_of(params, key).unwrap_or(Ok(None))
}

// Sets one limit on each torrent `hashes` names; the other is left as it
// is.
async fn set_rate_limit(
    ctx: &TorrentFacadeContext,
    params: &Params,
    direction: Direction,
) -> Response {
    let limit = match rate_limit_of(params, "limit") {
        Some(Ok(limit)) => Some(limit),
        Some(Err(response)) => return *response,
        None => return text(StatusCode::BAD_REQUEST, "limit is not a number"),
    };
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

// Peer exchange and local discovery are off on every session.
const PEX: bool = false;
const LSD: bool = false;

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
        "queueing_enabled": true,
        // An add without `addToTopOfQueue` goes to the bottom.
        "add_to_top_of_queue": false,
        "max_active_downloads": torrent.max_active_downloads,
        "max_active_uploads": -1,
        "max_active_torrents": -1,
        "listen_port": torrent.base_port,
        "upnp": torrent.upnp,
        "pex": PEX,
        "lsd": LSD,
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
        "ip_filter_trackers": torrent.filter_trackers,
        "banned_IPs": torrent.banned_ips.join("\n"),
        "block_peers_on_privileged_ports": torrent.block_privileged_ports,
        "stop_tracker_timeout": torrent.stop_tracker_timeout_secs,
        "max_ratio_enabled": seeding.is_some_and(|policy| policy.ratio_goal.set().is_some()),
        "max_ratio": seeding.and_then(|policy| policy.ratio_goal.set()).map_or(-1.0, ratio_number),
        "max_seeding_time_enabled": seeding.is_some_and(|policy| policy.seed_time_goal_minutes.set().is_some()),
        "max_seeding_time": minutes(seeding.and_then(|policy| policy.seed_time_goal_minutes.set())),
        "max_inactive_seeding_time_enabled": seeding.is_some_and(|policy| policy.inactive_goal_minutes.set().is_some()),
        "max_inactive_seeding_time": minutes(seeding.and_then(|policy| policy.inactive_goal_minutes.set())),
        "max_ratio_act": match seeding.and_then(|policy| policy.action.set()).unwrap_or_default() {
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
    if let Some(filter) = flag("ip_filter_trackers") {
        torrent.filter_trackers = filter;
    }
    // One entry a line, blank lines skipped (`SessionImpl::setBannedIPs`).
    if let Some(banned) = changes.get("banned_IPs").and_then(Value::as_str) {
        torrent.banned_ips = banned
            .split('\n')
            .filter(|entry| !entry.is_empty())
            .map(str::to_owned)
            .collect();
    }
    if let Some(block) = flag("block_peers_on_privileged_ports") {
        torrent.block_privileged_ports = block;
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
    let saved = save_rates(ctx, rate("dl_limit"), rate("up_limit")).await;
    if saved.status() != StatusCode::OK {
        return saved;
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

// Add `ips` to the banned addresses, saved and laid over the sessions. The
// settings stay locked from read to save, so a ban made beside another
// change loses neither.
async fn ban_session_wide(ctx: &TorrentFacadeContext, ips: &[IpAddr]) -> Result<(), String> {
    let inner = &ctx.inner;
    let mut config = inner.config.write().await;
    let mut torrent = config.torrent.clone();
    torrent
        .banned_ips
        .extend(ips.iter().map(ToString::to_string));
    let torrent = torrent.checked().map_err(|errors| errors.join("\n"))?;
    if torrent == config.torrent {
        return Ok(());
    }
    let db = inner.db.clone();
    let saved = torrent.clone();
    tokio::task::spawn_blocking(move || db.save_torrent_config(&saved))
        .await
        .map_err(|error| error.to_string())?
        .map_err(|error| error.to_string())?;
    config.torrent = torrent;
    drop(config);
    ctx.refresh_settings().await;
    Ok(())
}

impl TorrentFacadeContext {
    async fn refresh_settings(&self) {
        if let Some(torrents) = &self.inner.torrents {
            let settings = TorrentSettings::of(&*self.inner.config.read().await);
            torrents.set_settings(settings).await;
        }
    }

    // What follows a category added or changed here.
    async fn categories_changed(&self) {
        self.refresh_settings().await;
        if let Err(error) = self.inner.watch_folder.reconcile_from_config().await {
            tracing::warn!(%error, "watch folder did not take the category");
        }
    }
}

// Each category with its folder and share limits (qBittorrent
// `categoriesAction`, `CategoryOptions::toJSON`): `-2` where a limit is
// inherited, `-1` where the category sets no goal, and `Default` for a mode
// and action the category does not set.
pub(super) fn categories(config: &Config) -> Value {
    let map: Map<String, Value> = config
        .categories
        .iter()
        .map(|category| {
            let seeding = category.seeding.unwrap_or_default();
            let leaf = |leaf: Leaf<Value>| match leaf {
                Leaf::Inherit => json!(-2),
                Leaf::Off => json!(-1),
                Leaf::Set(value) => value,
            };
            let ratio = leaf(seeding.ratio_goal.map(|ratio| json!(ratio_number(ratio))));
            let minutes = |value: Leaf<u32>| leaf(value.map(|value| json!(i64::from(value))));
            (
                category.name.clone(),
                json!({
                    "name": category.name,
                    "savePath": category_dir(config, Some(&category.name)).to_string_lossy(),
                    "download_path": Value::Null,
                    "ratio_limit": ratio,
                    "seeding_time_limit": minutes(seeding.seed_time_goal_minutes),
                    "inactive_seeding_time_limit": minutes(seeding.inactive_goal_minutes),
                    "share_limit_action": seeding.action.set().map_or("Default", limit_action_name),
                    "share_limits_mode": seeding.mode.set().map_or("Default", limits_mode_name),
                }),
            )
        })
        .collect();
    Value::Object(map)
}

// The parameters of `createCategory` and `editCategory` past the name. In
// Weaver a category's folder and goals are an administrator's to set.
const CATEGORY_OPTIONS: [&str; 7] = [
    "savePath",
    "downloadPathEnabled",
    "ratioLimit",
    "seedingTimeLimit",
    "inactiveSeedingTimeLimit",
    "shareLimitsMode",
    "shareLimitAction",
];

// A category's folder and seeding goals with the options the request sets
// applied: an empty `savePath` is the category's default folder, a
// relative one is under the complete folder.
fn category_options(
    config: &Config,
    params: &Params,
    scope: CallerScope,
    current: Option<&CategoryConfig>,
) -> Result<(Option<String>, Option<SeedingPolicy>), Box<Response>> {
    if CATEGORY_OPTIONS
        .iter()
        .any(|name| params.get(name).is_some())
        && !scope.is_admin()
    {
        return Err(Box::new(forbidden()));
    }
    if params.flag("downloadPathEnabled") == Some(true) {
        return Err(Box::new(unavailable("A download path")));
    }
    let dest_dir = match params.get("savePath").map(str::trim) {
        None => current.and_then(|category| category.dest_dir.clone()),
        Some("") => None,
        Some(path) if Path::new(path).is_absolute() => Some(path.to_owned()),
        Some(path) => Some(
            Path::new(&config.complete_dir())
                .join(path)
                .to_string_lossy()
                .into_owned(),
        ),
    };
    let seeding = share_policy(params, current.and_then(|category| category.seeding), true)?;
    Ok((dest_dir, seeding))
}

// qBittorrent `createCategoryAction`.
async fn create_category(
    ctx: &TorrentFacadeContext,
    params: &Params,
    scope: CallerScope,
) -> Response {
    let name = params.get("category").unwrap_or_default();
    if name.is_empty() {
        return text(StatusCode::BAD_REQUEST, "Category cannot be empty");
    }
    let Ok(name) = validate_category_path_component(name) else {
        return text(StatusCode::CONFLICT, "Incorrect category name");
    };
    let inner = &ctx.inner;
    let _guard = inner.categories.lock().await;
    let (dest_dir, seeding) = {
        let config = inner.config.read().await;
        if config
            .categories
            .iter()
            .any(|category| category.name.eq_ignore_ascii_case(&name))
        {
            return text(StatusCode::CONFLICT, "Unable to create category");
        }
        match category_options(&config, params, scope, None) {
            Ok(options) => options,
            Err(response) => return *response,
        }
    };
    let db = inner.db.clone();
    let candidate = name.clone();
    let created = tokio::task::spawn_blocking(move || {
        let category = CategoryConfig {
            upload_cap: Default::default(),
            id: db.next_category_id()?,
            name: candidate,
            dest_dir,
            aliases: String::new(),
            seeding,
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
    ctx.categories_changed().await;
    ok()
}

// qBittorrent `editCategoryAction`: the category's folder and goals.
async fn edit_category(
    ctx: &TorrentFacadeContext,
    params: &Params,
    scope: CallerScope,
) -> Response {
    let name = params.get("category").unwrap_or_default();
    if name.is_empty() {
        return text(StatusCode::BAD_REQUEST, "Category cannot be empty");
    }
    let inner = &ctx.inner;
    let _guard = inner.categories.lock().await;
    let edited = {
        let config = inner.config.read().await;
        let Some(current) = config
            .categories
            .iter()
            .find(|category| category.name == name)
        else {
            return text(StatusCode::NOT_FOUND, "Category does not exist");
        };
        match category_options(&config, params, scope, Some(current)) {
            Ok((dest_dir, seeding)) => CategoryConfig {
                dest_dir,
                seeding,
                ..current.clone()
            },
            Err(response) => return *response,
        }
    };
    let db = inner.db.clone();
    let persisted = edited.clone();
    match tokio::task::spawn_blocking(move || db.update_category(&persisted)).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => return internal(error),
        Err(error) => return internal(error),
    }
    if let Some(category) = inner
        .config
        .write()
        .await
        .categories
        .iter_mut()
        .find(|category| category.id == edited.id)
    {
        *category = edited;
    }
    ctx.categories_changed().await;
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
        // Weaver does not learn its external address from peers.
        "last_external_address_v4": "",
        "last_external_address_v6": "",
        "dht_nodes": 0,
        "connection_status": "connected",
    });
    with_limits(
        fields,
        0,
        [("dl_rate_limit", download), ("up_rate_limit", upload)],
    )
}

// `sync/maindata`: the whole state, or what changed since the response
// `rid` names (qBittorrent `SyncController::maindataAction`).
async fn maindata(
    ctx: &TorrentFacadeContext,
    scope: CallerScope,
    client: SyncClient,
    params: &Params,
) -> Response {
    // qBittorrent reads an id it cannot parse as 0.
    let rid = params
        .get("rid")
        .and_then(|rid| rid.trim().parse::<u64>().ok())
        .unwrap_or(0);
    let views = ctx.views().await;
    let tags = match tag_snapshot(ctx).await {
        Ok(tags) => tags.made().to_vec(),
        Err(error) => return internal(error),
    };
    let config = ctx.inner.config.read().await;
    // The view's fingerprint leaves out the fields that move with the clock
    // alone, which `clock_fields` reads; whether the caller may control
    // torrents shapes the row too (`current_tracker`).
    let torrents = views
        .iter()
        .map(|view| Row {
            hash: view.infohash.to_ascii_lowercase(),
            fingerprint: {
                let mut hasher = DefaultHasher::new();
                (view.fingerprint(ViewParts::Steady), scope.can_control()).hash(&mut hasher);
                hasher.finish()
            },
            closed: u8::from(view.download_limit == Some(0))
                | u8::from(view.upload_limit == Some(0)) << 1,
            view,
        })
        .collect();
    let (download, upload) = (ctx.torrent_download_limit(), config.bandwidth.upload_bps);
    let server_closed = u8::from(download == Some(0)) | u8::from(upload == Some(0)) << 1;
    let mut server_state = transfer(&views, download, upload);
    let save_path = PathBuf::from(config.complete_dir());
    let free = tokio::task::spawn_blocking(move || probe_disk_space(&save_path))
        .await
        .ok()
        .and_then(Result::ok)
        .map_or(-1, |space| {
            i64::try_from(space.available_bytes).unwrap_or(i64::MAX)
        });
    // Weaver keeps no totals past its torrents: the all-time figures are
    // those of the torrents it has.
    let downloaded = views.iter().map(|view| view.downloaded_bytes).sum::<u64>();
    let uploaded = views.iter().map(|view| view.uploaded_bytes).sum::<u64>();
    let ratio = if downloaded > 0 && uploaded > 0 {
        format!("{:.2}", uploaded as f64 / downloaded as f64)
    } else {
        "-".to_owned()
    };
    let holds = ctx.inner.handle.state().holds();
    let session_paused = [HoldOrigin::Manual, HoldOrigin::Scheduled]
        .into_iter()
        .any(|origin| holds.held_by(origin, HoldOwner::Torrents));
    if let Some(state) = server_state.as_object_mut() {
        state.extend(
            [
                ("free_space_on_disk", json!(free)),
                ("alltime_dl", json!(downloaded)),
                ("alltime_ul", json!(uploaded)),
                (
                    "total_wasted_session",
                    json!(views.iter().map(|view| view.wasted_bytes).sum::<u64>()),
                ),
                ("global_ratio", json!(ratio)),
                (
                    "total_peer_connections",
                    json!(views.iter().map(|view| u64::from(view.peers)).sum::<u64>()),
                ),
                // Weaver has no disk cache or announce queue to report.
                ("read_cache_hits", json!("0")),
                ("read_cache_overload", json!("0")),
                ("write_cache_overload", json!("0")),
                ("total_buffers_size", json!(0)),
                ("queued_io_jobs", json!(0)),
                ("average_time_queue", json!(0)),
                ("total_queued_size", json!(0)),
                ("request_latency", json!(0)),
                ("queued_tracker_announces", json!(0)),
                ("queueing", json!(false)),
                ("use_alt_speed_limits", json!(false)),
                // Gone from 2.15.0; a category name holds no `/`.
                ("use_subcategories", json!(false)),
                ("refresh_interval", json!(1500)),
                ("session_state", json!(session_paused)),
            ]
            .map(|(key, value)| (key.to_owned(), value)),
        );
    }
    let now = Poll {
        torrents,
        categories: match categories(&config) {
            Value::Object(categories) => categories
                .into_iter()
                .filter_map(|(name, category)| match category {
                    Value::Object(category) => Some((name, category)),
                    _ => None,
                })
                .collect(),
            _ => Vec::new(),
        },
        tags,
        server_state: match server_state {
            Value::Object(state) => state,
            _ => Map::new(),
        },
        server_closed,
    };
    drop(config);
    let at = now_secs();
    let fields = Fields {
        // `torrents/info`'s row with the announce flags, keyed by its hash.
        steady: |view: &TorrentView| {
            #[cfg(test)]
            ctx.inner.maindata_rows.fetch_add(1, Ordering::Relaxed);
            let mut row = steady_fields(view, scope);
            row.retain(|(name, _)| *name != "hash");
            row.extend(announce_stats(view));
            row
        },
        clock: |view: &TorrentView| clock_fields(view, at),
    };
    // The lock is held for the bookkeeping alone, not while the response is
    // built.
    let start = ctx.sync().begin(&client, rid);
    let (body, kept) = sync::answer(&start, now, &fields);
    ctx.sync().finish(&client, start.id(), kept);
    axum::Json(body).into_response()
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
// `|`-separated `ip:port`s, each IP banned from every torrent. Each joins
// the banned addresses every session filters, as qBittorrent's
// `SessionImpl::banIP` adds it to `BannedIPs`, and goes on each torrent's
// own peer list too (libtorrent `torrent::ban_peer`), saved once per
// torrent. A torrent that fails to save does not stop the others.
async fn ban_peers(ctx: &TorrentFacadeContext, params: &Params) -> Response {
    let Some(peers) = params.get("peers") else {
        return text(StatusCode::BAD_REQUEST, "Missing parameter: peers");
    };
    let ips: Vec<_> = peers
        .split('|')
        .filter_map(|peer| peer.trim().parse::<SocketAddr>().ok())
        .map(|addr| addr.ip())
        .collect();
    if ips.is_empty() {
        return ok();
    }
    if let Err(error) = ban_session_wide(ctx, &ips).await {
        return internal(error);
    }
    let Some(torrents) = &ctx.inner.torrents else {
        return ok();
    };
    let mut failed = None;
    for view in torrents.torrents(None).await {
        match torrents.ban_peers(view.job_id, &ips).await {
            Ok(()) | Err(ActionError::NotFound(_)) => {}
            Err(error) => failed = Some(internal(error)),
        }
    }
    failed.unwrap_or_else(ok)
}

// qBittorrent `getFreeSpaceAtPathAction`: the bytes free on the filesystem
// holding `path`, -1 where that cannot be read.
async fn free_space(params: &Params) -> Response {
    if let Err(response) = require(params, &["path"]) {
        return *response;
    }
    let path = PathBuf::from(params.get("path").unwrap_or_default());
    let free = tokio::task::spawn_blocking(move || probe_disk_space(&path))
        .await
        .ok()
        .and_then(Result::ok)
        .map_or(-1, |space| {
            i64::try_from(space.available_bytes).unwrap_or(i64::MAX)
        });
    text(StatusCode::OK, free.to_string())
}

// qBittorrent `getDirectoryContentAction`: what an absolute directory
// holds, hidden entries left out, as paths or, with `withMetadata`, as
// entries.
async fn directory_content(params: &Params) -> Response {
    if let Err(response) = require(params, &["dirPath"]) {
        return *response;
    }
    let dir = params.get("dirPath").unwrap_or_default();
    if dir.is_empty() || dir.starts_with(':') || !Path::new(dir).is_absolute() {
        return text(StatusCode::BAD_REQUEST, "Invalid directory path");
    }
    let dir = PathBuf::from(dir);
    let mode = params.get("mode").unwrap_or("all").to_owned();
    let with_metadata = params.flag("withMetadata").unwrap_or(false);
    tokio::task::spawn_blocking(move || list_directory(&dir, &mode, with_metadata))
        .await
        .unwrap_or_else(internal)
}

fn list_directory(dir: &Path, mode: &str, with_metadata: bool) -> Response {
    if !dir.is_dir() {
        return text(StatusCode::NOT_FOUND, "Directory does not exist");
    }
    let (dirs, files) = match mode {
        "all" => (true, true),
        "dirs" => (true, false),
        "files" => (false, true),
        _ => {
            return text(
                StatusCode::BAD_REQUEST,
                "Invalid mode, allowed values: all, dirs, files",
            );
        }
    };
    let since_epoch = |at: std::io::Result<std::time::SystemTime>| {
        at.ok()
            .and_then(|at| at.duration_since(std::time::UNIX_EPOCH).ok())
            .map_or(-1, |since| {
                i64::try_from(since.as_secs()).unwrap_or(i64::MAX)
            })
    };
    let mut listed: Vec<(String, Value)> = Vec::new();
    for entry in std::fs::read_dir(dir).into_iter().flatten().flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with('.') {
            continue;
        }
        // A link is listed as what it leads to; one that leads nowhere is
        // left out.
        let Ok(metadata) = std::fs::metadata(entry.path()) else {
            continue;
        };
        let is_dir = metadata.is_dir();
        if !((is_dir && dirs) || (metadata.is_file() && files)) {
            continue;
        }
        let value = if with_metadata {
            let mut fields = object([
                ("name", json!(name)),
                ("type", json!(if is_dir { "dir" } else { "file" })),
                ("creation_date", json!(since_epoch(metadata.created()))),
                ("last_access_date", json!(since_epoch(metadata.accessed()))),
                (
                    "last_modification_date",
                    json!(since_epoch(metadata.modified())),
                ),
            ]);
            if !is_dir {
                fields["size"] = json!(metadata.len());
            }
            fields
        } else {
            json!(entry.path().to_string_lossy())
        };
        listed.push((name, value));
    }
    listed.sort_by(|(left, _), (right, _)| left.cmp(right));
    let listed: Vec<Value> = listed.into_iter().map(|(_, value)| value).collect();
    axum::Json(listed).into_response()
}

// qBittorrent `setCookiesAction`. Weaver keeps no cookies of its own, so
// an empty jar is all a client can set.
fn set_cookies(params: &Params) -> Response {
    if let Err(response) = require(params, &["cookies"]) {
        return *response;
    }
    match serde_json::from_str::<Value>(params.get("cookies").unwrap_or_default().trim()) {
        Err(error) => text(StatusCode::BAD_REQUEST, error.to_string()),
        Ok(Value::Array(cookies)) if cookies.is_empty() => ok(),
        Ok(Value::Array(_)) => unavailable("A cookie jar"),
        Ok(_) => text(StatusCode::BAD_REQUEST, "cookies must be array"),
    }
}

async fn interfaces() -> Result<Vec<DiscoveredInterface>, Response> {
    tokio::task::spawn_blocking(|| SystemInterfaceSource.interfaces())
        .await
        .map_err(internal)?
        .map_err(internal)
}

// qBittorrent `networkInterfaceListAction`: each interface with an address.
async fn interface_list() -> Response {
    match interfaces().await {
        Ok(found) => {
            let list: Vec<Value> = found
                .iter()
                .filter(|iface| !iface.addresses.is_empty())
                .map(|iface| json!({ "name": iface.name, "value": iface.name }))
                .collect();
            axum::Json(list).into_response()
        }
        Err(response) => response,
    }
}

// qBittorrent `networkInterfaceAddressListAction`: the addresses of the
// interface `iface` names, of every interface where it names none.
async fn interface_addresses(params: &Params) -> Response {
    if let Err(response) = require(params, &["iface"]) {
        return *response;
    }
    let wanted = params.get("iface").unwrap_or_default();
    match interfaces().await {
        Ok(found) => {
            let list: Vec<String> = found
                .iter()
                .filter(|iface| wanted.is_empty() || iface.name == wanted)
                .flat_map(|iface| iface.addresses.iter())
                .map(|address| address.address.to_string())
                .collect();
            axum::Json(list).into_response()
        }
        Err(response) => response,
    }
}

// Move each torrent the `ids` parameter names to the folder `path` names
// (qBittorrent `setLocationAction`, `setSavePathAction`).
async fn set_location(
    ctx: &TorrentFacadeContext,
    params: &Params,
    ids: &str,
    path: &str,
) -> Response {
    let Some(location) = params
        .get(path)
        .map(str::trim)
        .filter(|path| !path.is_empty())
        .map(str::to_owned)
    else {
        return text(StatusCode::BAD_REQUEST, "Save path cannot be empty");
    };
    let views = ctx.named(params.get(ids).unwrap_or_default()).await;
    each_of(ctx, views, move |torrents, view| {
        let location = location.clone();
        async move { torrents.set_location(view.job_id, location).await }
    })
    .await
}

// qBittorrent `exportAction`: the torrent's metainfo as it was added. A
// torrent added by magnet whose metadata has not come has none.
async fn export(ctx: &TorrentFacadeContext, params: &Params) -> Response {
    let Some(torrents) = &ctx.inner.torrents else {
        return text(StatusCode::NOT_FOUND, "Not Found");
    };
    let hash = params.get("hash").unwrap_or_default();
    let Some(view) = ctx.by_hash(hash).await else {
        return text(StatusCode::NOT_FOUND, "Not Found");
    };
    let Some(metainfo) = torrents
        .support(view.job_id)
        .await
        .and_then(|support| support.metainfo)
    else {
        return text(
            StatusCode::CONFLICT,
            "Unable to export torrent file. Error: the torrent has no metadata yet",
        );
    };
    let name: String = view
        .name
        .chars()
        .filter(|c| {
            !matches!(c, '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|') && !c.is_control()
        })
        .collect();
    let name = if name.trim().is_empty() {
        view.infohash.to_ascii_lowercase()
    } else {
        name
    };
    let disposition = format!("attachment; filename=\"{}.torrent\"", name.replace('"', ""));
    let mut response = (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/x-bittorrent")],
        metainfo,
    )
        .into_response();
    if let Ok(value) = HeaderValue::from_str(&disposition) {
        response
            .headers_mut()
            .insert(header::CONTENT_DISPOSITION, value);
    }
    response
}

// Save the global limits: `None` leaves one as it is, `Some(None)` lifts
// it.
async fn save_rates(
    ctx: &TorrentFacadeContext,
    download: Option<Option<u64>>,
    upload: Option<Option<u64>>,
) -> Response {
    if download.is_none() && upload.is_none() {
        return ok();
    }
    let inner = &ctx.inner;
    let current = inner.config.read().await.bandwidth;
    let bandwidth = Bandwidth {
        download_bps: download.unwrap_or(current.download_bps),
        upload_bps: upload.unwrap_or(current.upload_bps),
        ..current
    };
    if bandwidth.validate().is_err() {
        return text(StatusCode::BAD_REQUEST, "limit too large");
    }
    match inner
        .handle
        .save_bandwidth(&inner.db, &inner.config, bandwidth)
        .await
    {
        Ok(_) => ok(),
        Err(error) => internal(error),
    }
}

// The articles of a feed the client is shown, as many as qBittorrent keeps
// by default (`RSS::Session::maxArticlesPerFeed`).
const RSS_ARTICLES: u32 = 50;
// The refresh interval of a feed added without one.
const RSS_REFRESH_SECS: u32 = 900;

// The `rss` group (qBittorrent `RSSController`). Weaver's feeds are one
// flat list: a feed's path is its name, and there are no folders. A feed's
// articles are the items Weaver has seen, each already acted on, so each
// reads as read.
async fn rss(ctx: &TorrentFacadeContext, action: &str, params: &Params) -> Response {
    let required: &[&str] = match action {
        "addFolder" | "removeItem" => &["path"],
        "addFeed" | "setFeedURL" => &["url", "path"],
        "setFeedRefreshInterval" => &["path", "refreshInterval"],
        "moveItem" => &["itemPath", "destPath"],
        "markAsRead" | "refreshItem" => &["itemPath"],
        _ => &[],
    };
    if let Err(response) = require(params, required) {
        return *response;
    }
    let get = |name: &str| params.get(name).unwrap_or_default();
    match action {
        "items" => rss_items(ctx, params.flag("withData").unwrap_or(false)).await,
        "addFolder" => unavailable("RSS folders"),
        "addFeed" => add_feed(ctx, get("url"), get("path"), get("refreshInterval")).await,
        "setFeedURL" => {
            let url = get("url").trim().to_owned();
            edit_feed(ctx, get("path"), move |feed, feeds| {
                feed_url(&url)?;
                if feeds
                    .iter()
                    .any(|other| other.id != feed.id && other.url == url)
                {
                    return Err(format!("RSS feed with given URL already exists: {url}."));
                }
                feed.url = url;
                Ok(())
            })
            .await
        }
        "setFeedRefreshInterval" => match get("refreshInterval").trim().parse::<i64>() {
            Ok(secs) if secs >= 0 => {
                let secs = refresh_secs(secs);
                ctx.api().without_result(
                    edit_feed(ctx, get("path"), move |feed, _| {
                        feed.poll_interval_secs = secs;
                        Ok(())
                    })
                    .await,
                )
            }
            _ => text(StatusCode::BAD_REQUEST, "Invalid 'refreshInterval' value"),
        },
        "removeItem" => remove_feed(ctx, get("path")).await,
        "moveItem" => move_feed(ctx, get("itemPath"), get("destPath")).await,
        "markAsRead" => ok(),
        "refreshItem" => {
            let path = get("itemPath");
            if path.is_empty() {
                ctx.inner.rss.request_background_sync();
            } else if let Ok(Some(feed)) = feed_by_name(ctx, path).await {
                let rss = ctx.inner.rss.clone();
                tokio::spawn(async move {
                    if let Err(error) = rss.run_feed_sync(feed.id).await {
                        tracing::warn!(%error, feed = feed.id, "RSS feed refresh failed");
                    }
                });
            }
            ok()
        }
        // Weaver's rules belong to one feed each and match by pattern; the
        // client's are named and span feeds.
        "rules" | "setRule" | "renameRule" | "cloneRule" | "removeRule" | "matchingArticles"
        | "exportRules" | "importRules" => unavailable("The client's RSS download rules"),
        _ => no_endpoint(ctx.api(), "rss"),
    }
}

// A refresh interval as the client gives one: 0 is the default.
fn refresh_secs(secs: i64) -> u32 {
    match u32::try_from(secs) {
        Ok(0) => RSS_REFRESH_SECS,
        Ok(secs) => secs,
        Err(_) => u32::MAX,
    }
}

// A feed's url, which Weaver fetches over http or https.
fn feed_url(url: &str) -> Result<(), String> {
    match reqwest::Url::parse(url) {
        Ok(parsed) if matches!(parsed.scheme(), "http" | "https") => Ok(()),
        _ => Err(format!("Invalid RSS feed URL: {url}.")),
    }
}

// A feed's id as the client's item uid.
fn feed_uid(id: u32) -> String {
    format!("{{00000000-0000-0000-0000-{id:012x}}}")
}

async fn feeds(ctx: &TorrentFacadeContext) -> Result<Vec<RssFeedRow>, Response> {
    let db = ctx.inner.db.clone();
    match tokio::task::spawn_blocking(move || db.list_rss_feeds()).await {
        Ok(Ok(feeds)) => Ok(feeds),
        Ok(Err(error)) => Err(internal(error)),
        Err(error) => Err(internal(error)),
    }
}

async fn feed_by_name(
    ctx: &TorrentFacadeContext,
    name: &str,
) -> Result<Option<RssFeedRow>, Response> {
    Ok(feeds(ctx).await?.into_iter().find(|feed| feed.name == name))
}

// qBittorrent `itemsAction`: the root folder, each feed by its name.
async fn rss_items(ctx: &TorrentFacadeContext, with_data: bool) -> Response {
    let feeds = match feeds(ctx).await {
        Ok(feeds) => feeds,
        Err(response) => return response,
    };
    let mut root = Map::new();
    for feed in feeds {
        let mut item = Map::new();
        item.insert("uid".to_owned(), json!(feed_uid(feed.id)));
        item.insert("url".to_owned(), json!(feed.url));
        item.insert("refreshInterval".to_owned(), json!(feed.poll_interval_secs));
        if with_data {
            let db = ctx.inner.db.clone();
            let id = feed.id;
            let seen = match tokio::task::spawn_blocking(move || {
                db.list_rss_seen_items(Some(id), Some(RSS_ARTICLES))
            })
            .await
            {
                Ok(Ok(seen)) => seen,
                Ok(Err(error)) => return internal(error),
                Err(error) => return internal(error),
            };
            let articles: Vec<Value> = seen
                .iter()
                .map(|article| {
                    let date = chrono::DateTime::from_timestamp(
                        article.published_at.unwrap_or(article.seen_at),
                        0,
                    )
                    .map(|date| date.to_rfc2822())
                    .unwrap_or_default();
                    let url = article.item_url.clone().unwrap_or_default();
                    json!({
                        "id": article.item_id,
                        "date": date,
                        "title": article.item_title,
                        "author": "",
                        "description": "",
                        "torrentURL": url,
                        "link": url,
                        "isRead": true,
                    })
                })
                .collect();
            item.insert("title".to_owned(), json!(feed.name));
            item.insert("lastBuildDate".to_owned(), json!(""));
            item.insert("isLoading".to_owned(), json!(false));
            item.insert("hasError".to_owned(), json!(feed.last_error.is_some()));
            item.insert("articles".to_owned(), json!(articles));
        }
        root.insert(feed.name, Value::Object(item));
    }
    axum::Json(Value::Object(root)).into_response()
}

// Change the feeds under the network runtime's lock, which a feed's route
// is read through, and have the runtime read them again.
async fn feeds_changed<T>(
    ctx: &TorrentFacadeContext,
    change: impl FnOnce(&Database) -> Result<Result<T, String>, weaver_server_core::StateError>
    + Send
    + 'static,
) -> Result<T, Response>
where
    T: Send + 'static,
{
    let runtime = ctx.inner.handle.proxy_runtime();
    let _guard = match &runtime {
        Some(runtime) => Some(runtime.mutations.lock().await),
        None => None,
    };
    let db = ctx.inner.db.clone();
    let changed = match tokio::task::spawn_blocking(move || change(&db)).await {
        Ok(Ok(Ok(changed))) => changed,
        Ok(Ok(Err(refusal))) => return Err(text(StatusCode::CONFLICT, refusal)),
        Ok(Err(error)) => return Err(internal(error)),
        Err(error) => return Err(internal(error)),
    };
    if let Some(runtime) = &runtime
        && let Err(error) = runtime.reload().await
    {
        tracing::warn!(%error, "network runtime did not take the RSS feed change");
    }
    Ok(changed)
}

// qBittorrent `addFeedAction`: a feed named by its path, else its url.
async fn add_feed(ctx: &TorrentFacadeContext, url: &str, path: &str, every: &str) -> Response {
    let url = url.trim().to_owned();
    let name = if path.is_empty() {
        url.clone()
    } else {
        path.to_owned()
    };
    if name.contains('\\') {
        return unavailable("RSS folders");
    }
    let every = refresh_secs(every.trim().parse::<i64>().unwrap_or(0).max(0));
    let added = feeds_changed(ctx, move |db| {
        if let Err(refusal) = feed_url(&url) {
            return Ok(Err(refusal));
        }
        let feeds = db.list_rss_feeds()?;
        if feeds.iter().any(|feed| feed.url == url) {
            return Ok(Err(format!(
                "RSS feed with given URL already exists: {url}."
            )));
        }
        if feeds.iter().any(|feed| feed.name == name) {
            return Ok(Err(format!(
                "RSS item with given path already exists: {name}."
            )));
        }
        let row = RssFeedRow {
            scripts: Vec::new(),
            id: db.next_rss_feed_id()?,
            name,
            url,
            enabled: true,
            poll_interval_secs: every,
            username: None,
            password: None,
            default_category: None,
            default_metadata: Vec::new(),
            etag: None,
            last_modified: None,
            last_polled_at: None,
            last_success_at: None,
            last_error: None,
            consecutive_failures: 0,
        };
        db.insert_rss_feed_with_routing(&row, None).map(Ok)
    })
    .await;
    match added {
        Ok(()) => ok(),
        Err(response) => response,
    }
}

// Change the feed `path` names; the change sees every feed.
async fn edit_feed(
    ctx: &TorrentFacadeContext,
    path: &str,
    edit: impl FnOnce(&mut RssFeedRow, &[RssFeedRow]) -> Result<(), String> + Send + 'static,
) -> Response {
    let path = path.to_owned();
    let edited = feeds_changed(ctx, move |db| {
        let feeds = db.list_rss_feeds()?;
        let Some(mut feed) = feeds.iter().find(|feed| feed.name == path).cloned() else {
            return Ok(Err(format!("Feed doesn't exist: {path}.")));
        };
        if let Err(refusal) = edit(&mut feed, &feeds) {
            return Ok(Err(refusal));
        }
        db.update_rss_feed_with_routing(&feed, None).map(Ok)
    })
    .await;
    match edited {
        Ok(()) => ok(),
        Err(response) => response,
    }
}

// qBittorrent `removeItemAction`.
async fn remove_feed(ctx: &TorrentFacadeContext, path: &str) -> Response {
    if path.is_empty() {
        return text(StatusCode::CONFLICT, "Cannot delete root folder.");
    }
    let path = path.to_owned();
    let removed = feeds_changed(ctx, move |db| {
        let Some(feed) = db
            .list_rss_feeds()?
            .into_iter()
            .find(|feed| feed.name == path)
        else {
            return Ok(Err(format!("Item doesn't exist: {path}.")));
        };
        db.delete_rss_feed(feed.id).map(|_| Ok(()))
    })
    .await;
    match removed {
        Ok(()) => ok(),
        Err(response) => response,
    }
}

// qBittorrent `moveItemAction`: with no folders, a move is a rename.
async fn move_feed(ctx: &TorrentFacadeContext, from: &str, to: &str) -> Response {
    if from.is_empty() {
        return text(StatusCode::CONFLICT, "Cannot move root folder.");
    }
    if to.contains('\\') {
        return unavailable("RSS folders");
    }
    if to.is_empty() {
        return text(
            StatusCode::CONFLICT,
            format!("Incorrect RSS Item path: {to}."),
        );
    }
    let (from, to) = (from.to_owned(), to.to_owned());
    let moved = feeds_changed(ctx, move |db| {
        let feeds = db.list_rss_feeds()?;
        let Some(mut feed) = feeds.iter().find(|feed| feed.name == from).cloned() else {
            return Ok(Err(format!("Item doesn't exist: {from}.")));
        };
        if feeds
            .iter()
            .any(|other| other.id != feed.id && other.name == to)
        {
            return Ok(Err(format!(
                "RSS item with given path already exists: {to}."
            )));
        }
        feed.name = to;
        db.update_rss_feed_with_routing(&feed, None).map(Ok)
    })
    .await;
    match moved {
        Ok(()) => ok(),
        Err(response) => response,
    }
}
