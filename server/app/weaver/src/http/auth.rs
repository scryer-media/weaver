use axum::Json;
use axum::extract::{ConnectInfo, Extension};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
use weaver_server_api::auth::CallerIdentity;

use weaver_server_core::Database;
use weaver_server_core::auth::{self as jwt, JWT_TTL_SECS};
use weaver_server_core::auth::{
    ApiKeyAuthRow, ApiKeyCache, BrowserSession, CallerScope, LoginAuthCache,
    derive_browser_csrf_token, generate_browser_session_secret, hash_api_key, verify_password,
};
use weaver_server_core::security::RuntimeSecurityConfig;

pub(super) const JWT_COOKIE_NAME: &str = "weaver_jwt";
pub(super) const SESSION_COOKIE_NAME: &str = "weaver_session";
pub(super) const LOGIN_MAX_FAILURES: usize = 5;
const LOGIN_FAILURE_WINDOW: Duration = Duration::from_secs(60);
const LOGIN_LIMITER_MAX_KEYS: usize = 1024;

pub(super) async fn verify_password_bounded(
    password: String,
    hash: String,
) -> Result<bool, StatusCode> {
    let permit = weaver_server_core::auth::service::password_work_permit()
        .map_err(|_| StatusCode::TOO_MANY_REQUESTS)?;
    tokio::task::spawn_blocking(move || {
        // A cancelled HTTP request must not free the slot while Argon2 is running.
        let _permit = permit;
        verify_password(&password, &hash)
    })
    .await
    .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)
}

#[derive(Clone, Default)]
pub(super) struct LoginRateLimiter {
    inner: Arc<Mutex<HashMap<String, LoginFailureWindow>>>,
}

#[derive(Clone)]
struct LoginFailureWindow {
    failures: usize,
    first_failure: Instant,
}

impl LoginRateLimiter {
    fn too_many_failures(&self, username: &str, client_id: &str) -> bool {
        let now = Instant::now();
        let keys = Self::limiter_keys(username, client_id);
        let mut attempts = self.inner.lock().unwrap();
        Self::prune_expired(&mut attempts, now);
        keys.iter().any(|key| {
            attempts
                .get(key)
                .is_some_and(|window| window.failures >= LOGIN_MAX_FAILURES)
        }) || (attempts.len() >= LOGIN_LIMITER_MAX_KEYS
            && keys.iter().any(|key| !attempts.contains_key(key)))
    }

    fn record_failure(&self, username: &str, client_id: &str) {
        let now = Instant::now();
        let keys = Self::limiter_keys(username, client_id);
        let mut attempts = self.inner.lock().unwrap();
        Self::prune_expired(&mut attempts, now);
        for key in keys {
            if !attempts.contains_key(&key) && attempts.len() >= LOGIN_LIMITER_MAX_KEYS {
                // Preserve existing lockouts at capacity. Dropping a new
                // bucket is conservative and cannot let an attacker flush all
                // active limits by cycling spoofed identities.
                continue;
            }
            let window = attempts.entry(key).or_insert(LoginFailureWindow {
                failures: 0,
                first_failure: now,
            });
            window.failures = window.failures.saturating_add(1);
        }
    }

    fn record_success(&self, username: &str, client_id: &str) {
        let keys = Self::limiter_keys(username, client_id);
        let mut attempts = self.inner.lock().unwrap();
        for key in keys {
            attempts.remove(&key);
        }
    }

    fn prune_expired(attempts: &mut HashMap<String, LoginFailureWindow>, now: Instant) {
        attempts
            .retain(|_, window| now.duration_since(window.first_failure) < LOGIN_FAILURE_WINDOW);
    }

    fn limiter_keys(username: &str, client_id: &str) -> [String; 2] {
        [Self::account_key(username), format!("client:{client_id}")]
    }

    fn account_key(username: &str) -> String {
        format!("account:{}", username.trim().to_ascii_lowercase())
    }
}

/// The address login attempts are metered against.
///
/// Resolved the same way trust is, so a deployment behind a configured proxy
/// meters each browser separately instead of pooling every attempt under the
/// proxy's own address. Headers are believed only from a configured proxy; a
/// direct peer is metered on its socket address exactly as before.
fn login_client_id(
    security: &RuntimeSecurityConfig,
    headers: &HeaderMap,
    peer_addr: Option<SocketAddr>,
) -> String {
    if let Some(ip) = security.resolve_client_ip(peer_addr, headers) {
        return ip.to_string();
    }
    if security.authenticated_access_mode() {
        return "unresolved".to_string();
    }
    for name in ["x-forwarded-for", "x-real-ip"] {
        if let Some(value) = headers.get(name).and_then(|value| value.to_str().ok()) {
            let candidate = value
                .split(',')
                .next()
                .map(str::trim)
                .filter(|value| !value.is_empty());
            if let Some(candidate) = candidate {
                return candidate.to_string();
            }
        }
    }
    "unknown".to_string()
}

fn canonical_browser_origin(headers: &HeaderMap) -> Result<String, StatusCode> {
    let mut values = headers
        .get_all(header::ORIGIN)
        .iter()
        .map(|value| value.to_str().map_err(|_| StatusCode::FORBIDDEN));
    let Some(value) = values.next() else {
        return Err(StatusCode::FORBIDDEN);
    };
    if values.next().is_some() {
        return Err(StatusCode::FORBIDDEN);
    }
    let value = value?;
    if value == "null" {
        return Err(StatusCode::FORBIDDEN);
    }
    let url = reqwest::Url::parse(value).map_err(|_| StatusCode::FORBIDDEN)?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(StatusCode::FORBIDDEN);
    }
    Ok(url.origin().ascii_serialization())
}

fn epoch_seconds() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn hash_to_hex(hash: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(64);
    for byte in hash {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

/// Extract the `weaver_jwt` cookie value from request headers.
pub(super) fn extract_jwt_cookie(headers: &HeaderMap) -> Option<String> {
    extract_cookie(headers, JWT_COOKIE_NAME)
}

pub(super) fn extract_session_cookie(headers: &HeaderMap) -> Option<String> {
    extract_cookie(headers, SESSION_COOKIE_NAME)
}

fn extract_cookie(headers: &HeaderMap, name: &str) -> Option<String> {
    let prefix = format!("{name}=");
    headers
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(';'))
        .map(str::trim)
        .find_map(|cookie| cookie.strip_prefix(&prefix).map(|value| value.to_string()))
}

fn explicit_api_key(headers: &HeaderMap) -> Result<Option<String>, StatusCode> {
    let bearer = match headers.get(header::AUTHORIZATION) {
        Some(value) => {
            let value = value.to_str().map_err(|_| StatusCode::UNAUTHORIZED)?;
            let value = value
                .strip_prefix("Bearer ")
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or(StatusCode::UNAUTHORIZED)?;
            Some(value.to_owned())
        }
        None => None,
    };
    let api_key = match headers.get("x-api-key") {
        Some(value) => {
            let value = value.to_str().map_err(|_| StatusCode::UNAUTHORIZED)?;
            (!value.is_empty())
                .then(|| value.to_owned())
                .ok_or(StatusCode::UNAUTHORIZED)
                .map(Some)?
        }
        None => None,
    };

    match (bearer, api_key) {
        (Some(bearer), Some(api_key)) if bearer != api_key => Err(StatusCode::UNAUTHORIZED),
        (Some(key), _) | (_, Some(key)) => Ok(Some(key)),
        (None, None) => Ok(None),
    }
}

pub(super) fn caller_scope_from_api_key_scope(scope: &str) -> CallerScope {
    match scope {
        "admin" => CallerScope::Admin,
        "read" => CallerScope::Read,
        "control" | "integration" => CallerScope::Control,
        _ => CallerScope::Control,
    }
}

pub(super) async fn lookup_api_key_auth(
    db: &Database,
    api_key_cache: &ApiKeyCache,
    key_hash: [u8; 32],
) -> Result<Option<ApiKeyAuthRow>, StatusCode> {
    if let Some(row) = api_key_cache.get(&key_hash) {
        return Ok(Some(row));
    }

    let db_clone = db.clone();
    let row = tokio::task::spawn_blocking(move || db_clone.lookup_api_key(&key_hash))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let Some(row) = row else {
        return Ok(None);
    };

    let cached = ApiKeyAuthRow {
        key_hash,
        id: row.id,
        scope: row.scope,
    };
    api_key_cache.upsert(cached.clone());
    Ok(Some(cached))
}

/// Debounce interval for `api_keys.last_used_at` writes. *arr pollers hit the
/// API every few seconds; persisting a timestamp that granular is pointless and
/// on Postgres it is a write round-trip + WAL flush per request.
const API_KEY_TOUCH_MIN_INTERVAL_MS: i64 = 60_000;
const API_KEY_TOUCH_MAX_KEYS: usize = 4096;

fn api_key_touch_throttle() -> &'static Mutex<HashMap<i64, i64>> {
    static THROTTLE: OnceLock<Mutex<HashMap<i64, i64>>> = OnceLock::new();
    THROTTLE.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(super) fn queue_touch_api_key_last_used(db: &Database, id: i64) {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;

    {
        let mut throttle = api_key_touch_throttle()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(&last) = throttle.get(&id)
            && now.saturating_sub(last) < API_KEY_TOUCH_MIN_INTERVAL_MS
        {
            return;
        }
        // The set of key ids is tiny in practice; this cap only guards against
        // pathological churn.
        if throttle.len() >= API_KEY_TOUCH_MAX_KEYS && !throttle.contains_key(&id) {
            throttle.clear();
        }
        throttle.insert(id, now);
    }

    let db_touch = db.clone();
    tokio::task::spawn_blocking(move || {
        let _ = db_touch.touch_api_key_last_used(id, now);
    });
}

#[derive(Clone)]
pub(super) struct ResolvedCaller {
    pub(super) scope: CallerScope,
    pub(super) identity: CallerIdentity,
}

/// Browser session cookies are accepted only on browser-facing routes whose
/// immediate socket peer has been explicitly trusted by the operator.
#[derive(Clone, Copy)]
pub(super) enum BrowserSessionPolicy {
    TrustedPeer(Option<SocketAddr>),
    Denied,
}

/// Resolve the caller scope and stable request identity from persistent API
/// key headers, a login JWT cookie, or a trusted-peer browser session cookie.
pub(super) async fn resolve_caller(
    db: &Database,
    auth_cache: &LoginAuthCache,
    api_key_cache: &ApiKeyCache,
    session_token: &str,
    security: &RuntimeSecurityConfig,
    browser_session: BrowserSessionPolicy,
    headers: &HeaderMap,
) -> Result<ResolvedCaller, StatusCode> {
    // An explicit machine credential must be a persistent API key. In
    // particular, never fall back to browser cookies after an invalid header.
    if let Some(raw_key) = explicit_api_key(headers)? {
        let key_hash = hash_api_key(&raw_key);
        if let Some(row) = lookup_api_key_auth(db, api_key_cache, key_hash).await? {
            queue_touch_api_key_last_used(db, row.id);
            return Ok(ResolvedCaller {
                scope: caller_scope_from_api_key_scope(&row.scope),
                identity: CallerIdentity::ApiKey(row.key_hash),
            });
        }
        return Err(StatusCode::UNAUTHORIZED);
    }

    // The authenticated policy uses an opaque, persisted browser credential;
    // never accept the legacy JWT as a compatibility fallback in this mode.
    if security.authenticated_access_mode()
        && matches!(browser_session, BrowserSessionPolicy::TrustedPeer(_))
        && let Some(cookie) = extract_session_cookie(headers)
    {
        let token_hash = hash_to_hex(hash_api_key(&cookie));
        let db = db.clone();
        let now = epoch_seconds();
        if let Some(session) =
            tokio::task::spawn_blocking(move || db.get_active_browser_session(&token_hash, now))
                .await
                .ok()
                .and_then(Result::ok)
                .flatten()
        {
            if session.remembered
                && !matches!(browser_session, BrowserSessionPolicy::TrustedPeer(peer)
                    if security.remembered_client_allowed(peer, headers))
            {
                return Err(StatusCode::UNAUTHORIZED);
            }
            // Navigation GETs legitimately omit Origin. If a browser does send
            // one, it must remain bound to the origin established at login.
            if headers.contains_key(header::ORIGIN)
                && canonical_browser_origin(headers).ok().as_deref()
                    != Some(session.origin.as_str())
            {
                return Err(StatusCode::FORBIDDEN);
            }
            return Ok(ResolvedCaller {
                scope: CallerScope::Admin,
                identity: CallerIdentity::Jwt(hash_api_key(&cookie)),
            });
        }
        return Err(StatusCode::UNAUTHORIZED);
    }

    if security.authenticated_access_mode() {
        return Err(StatusCode::UNAUTHORIZED);
    }

    // 2. JWT cookie (when login auth is enabled).
    let cached_auth = auth_cache.snapshot();
    if let Some(token) = extract_jwt_cookie(headers)
        && let Some(auth) = cached_auth.as_ref()
        && jwt::verify_jwt(&token, &auth.jwt_secret).is_ok()
    {
        return Ok(ResolvedCaller {
            scope: CallerScope::Admin,
            identity: CallerIdentity::Jwt(hash_api_key(&token)),
        });
    }

    // A browser cookie is process-bound *and* peer-bound. Once login is
    // enabled, credentials always take precedence over trusted-network access.
    if let BrowserSessionPolicy::TrustedPeer(peer) = browser_session
        && !security.authenticated_access_mode()
        && cached_auth.is_none()
        && security.is_trusted_client(peer, headers)
        && let Some(cookie) = extract_session_cookie(headers)
        && cookie == session_token
    {
        return Ok(ResolvedCaller {
            scope: CallerScope::Local,
            identity: CallerIdentity::Local(hash_api_key(&cookie)),
        });
    }

    Err(StatusCode::UNAUTHORIZED)
}

/// Resolve the caller scope with an explicit browser-session policy.
pub(super) async fn resolve_scope(
    db: &Database,
    auth_cache: &LoginAuthCache,
    api_key_cache: &ApiKeyCache,
    session_token: &str,
    security: &RuntimeSecurityConfig,
    browser_session: BrowserSessionPolicy,
    headers: &HeaderMap,
) -> Result<CallerScope, StatusCode> {
    Ok(resolve_caller(
        db,
        auth_cache,
        api_key_cache,
        session_token,
        security,
        browser_session,
        headers,
    )
    .await?
    .scope)
}

pub(super) async fn enforce_browser_csrf(
    Extension(db): Extension<Database>,
    Extension(security): Extension<RuntimeSecurityConfig>,
    Extension(base_url): Extension<super::assets::BaseUrl>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let path = request
        .uri()
        .path()
        .strip_prefix(base_url.0.as_str())
        .unwrap_or(request.uri().path());
    if !request.method().is_safe()
        && !matches!(path, "/api/login" | "/api/auth/setup")
        && !matches!(explicit_api_key(request.headers()), Ok(Some(_)))
        && let Err(status) = validate_browser_csrf(&db, &security, request.headers()).await
    {
        return super::error_response(status, "browser verification required");
    }
    next.run(request).await
}

/// Validate the browser binding for a state-changing cookie request. API-key
/// callers do not carry the browser session cookie and bypass this adapter.
pub(super) async fn validate_browser_csrf(
    db: &Database,
    security: &RuntimeSecurityConfig,
    headers: &HeaderMap,
) -> Result<(), StatusCode> {
    if !security.authenticated_access_mode() || extract_session_cookie(headers).is_none() {
        return Ok(());
    }
    let token = extract_session_cookie(headers).ok_or(StatusCode::FORBIDDEN)?;
    let mut csrf_headers = headers.get_all("x-weaver-csrf").iter();
    let csrf = csrf_headers
        .next()
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .ok_or(StatusCode::FORBIDDEN)?;
    if csrf_headers.next().is_some() {
        return Err(StatusCode::FORBIDDEN);
    }
    let hash = hash_to_hex(hash_api_key(&token));
    let db = db.clone();
    let session =
        tokio::task::spawn_blocking(move || db.get_active_browser_session(&hash, epoch_seconds()))
            .await
            .ok()
            .and_then(Result::ok)
            .flatten()
            .ok_or(StatusCode::FORBIDDEN)?;
    if canonical_browser_origin(headers).ok().as_deref() != Some(session.origin.as_str())
        || !weaver_server_core::auth::service::verify_browser_csrf_token(
            csrf,
            &session.csrf_verifier,
        )
    {
        return Err(StatusCode::FORBIDDEN);
    }
    Ok(())
}

#[derive(Deserialize)]
pub(super) struct LoginRequest {
    username: String,
    password: String,
    #[serde(default)]
    remember: bool,
}

#[expect(
    clippy::too_many_arguments,
    reason = "Axum extracts independent request and application state"
)]
pub(super) async fn login_handler(
    ConnectInfo(peer_addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Extension(db): Extension<Database>,
    Extension(auth_cache): Extension<LoginAuthCache>,
    Extension(login_limiter): Extension<LoginRateLimiter>,
    Extension(security): Extension<RuntimeSecurityConfig>,
    Extension(base_url): Extension<super::assets::BaseUrl>,
    Json(body): Json<LoginRequest>,
) -> Response {
    let browser_binding = if security.authenticated_access_mode() {
        let origin = match canonical_browser_origin(&headers) {
            Ok(origin) => origin,
            Err(status) => {
                return super::error_response(status, "a single valid Origin is required");
            }
        };
        let secure = match browser_cookie_secure(&security, peer_addr, &headers, &origin) {
            Ok(secure) => secure,
            Err(status) => return super::error_response(status, "invalid proxy protocol header"),
        };
        Some((origin, secure))
    } else {
        None
    };
    let creds = match auth_cache.snapshot() {
        Some(creds) => creds,
        None => {
            return super::error_response(StatusCode::BAD_REQUEST, "login is not enabled");
        }
    };
    let client_id = login_client_id(&security, &headers, Some(peer_addr));

    if login_limiter.too_many_failures(&body.username, &client_id) {
        return super::error_response(StatusCode::TOO_MANY_REQUESTS, "too many login attempts");
    }

    let username_matches = body.username == creds.username;
    let hash = creds.password_hash.clone();
    let password = body.password.clone();
    let password_valid = match verify_password_bounded(password, hash).await {
        Ok(valid) => valid,
        Err(status) => {
            return super::error_response(
                status,
                "Password verification is busy. Try again shortly.",
            );
        }
    };

    if !username_matches || !password_valid {
        login_limiter.record_failure(&body.username, &client_id);
        return super::error_response(StatusCode::UNAUTHORIZED, "invalid credentials");
    }

    login_limiter.record_success(&body.username, &client_id);
    if let Some((origin, secure)) = browser_binding {
        let token = generate_browser_session_secret();
        let csrf = derive_browser_csrf_token(&token, &creds.jwt_secret);
        let now = epoch_seconds();
        let remembered =
            body.remember && security.remembered_client_allowed(Some(peer_addr), &headers);
        let session = BrowserSession {
            token_hash: hash_to_hex(hash_api_key(&token)),
            csrf_verifier: hash_to_hex(hash_api_key(&csrf)),
            origin,
            client_ip: security
                .resolve_client_ip(Some(peer_addr), &headers)
                .map(|ip| ip.to_string()),
            remembered,
            created_at: now,
            expires_at: now + JWT_TTL_SECS as i64,
            revoked_at: None,
        };
        let write = db.clone();
        if tokio::task::spawn_blocking(move || write.create_browser_session(&session))
            .await
            .ok()
            .and_then(Result::ok)
            .is_none()
        {
            return super::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "could not create browser session",
            );
        }
        let cookie = browser_session_cookie(&token, &base_url.0, secure, remembered);
        return (
            StatusCode::OK,
            [(header::SET_COOKIE, cookie)],
            Json(serde_json::json!({ "ok": true, "csrfToken": csrf, "remembered": remembered })),
        )
            .into_response();
    }
    let effective_auth = creds.clone();

    let token = jwt::create_jwt(
        &effective_auth.username,
        &effective_auth.jwt_secret,
        JWT_TTL_SECS,
    );
    let cookie = format!(
        "{JWT_COOKIE_NAME}={token}; Path=/; HttpOnly; SameSite=Strict; Max-Age={JWT_TTL_SECS}{}",
        secure_cookie_suffix(&security)
    );

    (
        StatusCode::OK,
        [(header::SET_COOKIE, cookie)],
        Json(serde_json::json!({ "ok": true })),
    )
        .into_response()
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct SetupRequest {
    mode: String,
    #[serde(default)]
    setup_code: Option<String>,
    #[serde(default)]
    username: Option<String>,
    #[serde(default)]
    password: Option<String>,
    #[serde(default)]
    bind_address: Option<String>,
    #[serde(default)]
    trusted_networks: Option<Vec<String>>,
}

/// Complete first-run setup from the browser: pick an access mode, optionally
/// create the login, optionally widen the binding.
///
/// This is the wizard's endpoint, and its whole reason to exist is that every
/// peer product does setup in the browser while Weaver used to demand
/// environment variables. It is callable exactly once — while no credentials
/// are stored — and only from loopback or an already-trusted peer, which is
/// the same trust argument the loopback bind default rests on: the first
/// browser to reach a fresh instance from the machine itself is the operator.
#[expect(
    clippy::too_many_arguments,
    reason = "Axum extracts independent request and application state"
)]
pub(super) async fn setup_handler(
    ConnectInfo(peer_addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Extension(db): Extension<Database>,
    Extension(auth_cache): Extension<LoginAuthCache>,
    Extension(security): Extension<RuntimeSecurityConfig>,
    Extension(session_token): Extension<super::SessionToken>,
    Extension(base_url): Extension<super::assets::BaseUrl>,
    challenge: Option<Extension<super::setup_code::SetupChallenge>>,
    Json(body): Json<SetupRequest>,
) -> Response {
    use weaver_server_core::security::{
        AUTHENTICATED_POLICY_REVISION, AccessMode, LOCAL_NETWORK_PRESETS, LOOPBACK_NETWORKS,
        SETTING_ACCESS_MODE, SETTING_HTTP_BIND_ADDRESS, SETTING_SECURITY_POLICY_REVISION,
        SETTING_TRUSTED_NETWORKS, ip_is_loopback, resolve_bind_address,
    };

    if auth_cache.snapshot().is_some() {
        return super::error_response(StatusCode::CONFLICT, "setup is already complete");
    }
    if security.authenticated_access_mode() {
        use super::setup_code::SetupCodeError;
        use weaver_server_core::auth::repository::{
            InitialAuthenticatedSetup, InitialSetupOutcome,
        };
        let failure = |status, code: &str, message: &str| {
            (
                status,
                Json(serde_json::json!({"code": code, "error": message})),
            )
                .into_response()
        };
        let Some(Extension(challenge)) = challenge else {
            return failure(
                StatusCode::CONFLICT,
                "SETUP_UNAVAILABLE",
                "Setup is unavailable. Use the documented administrator recovery procedure.",
            );
        };
        if let Err(error) = challenge.verify(body.setup_code.as_deref()) {
            return match error {
                SetupCodeError::RateLimited => failure(
                    StatusCode::TOO_MANY_REQUESTS,
                    "SETUP_RATE_LIMITED",
                    "Too many setup code attempts. Wait one minute and try again.",
                ),
                SetupCodeError::Consumed => failure(
                    StatusCode::CONFLICT,
                    "SETUP_COMPLETE",
                    "Setup is already complete. Sign in.",
                ),
                SetupCodeError::Missing | SetupCodeError::Invalid => failure(
                    StatusCode::FORBIDDEN,
                    "SETUP_CODE_REQUIRED",
                    "Enter the current one-time code from Weaver's startup output.",
                ),
            };
        }
        let origin = match canonical_browser_origin(&headers) {
            Ok(origin) => origin,
            Err(status) => {
                return failure(
                    status,
                    "INVALID_BROWSER_REQUEST",
                    "A single valid Origin is required.",
                );
            }
        };
        let secure = match browser_cookie_secure(&security, peer_addr, &headers, &origin) {
            Ok(secure) => secure,
            Err(status) => {
                return failure(
                    status,
                    "INVALID_PROXY_PROTOCOL",
                    "The trusted proxy supplied an invalid protocol header.",
                );
            }
        };
        let username = body
            .username
            .as_deref()
            .unwrap_or_default()
            .trim()
            .to_string();
        let password = body.password.clone().unwrap_or_default();
        if body.mode != "login_required" || username.is_empty() || password.is_empty() {
            return failure(
                StatusCode::BAD_REQUEST,
                "INVALID_SETUP",
                "Administrator username and password are required.",
            );
        }
        let bind_pinned = !security.bind_address_source.is_editable();
        let bind_address = body
            .bind_address
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let bind_address = if bind_pinned {
            None
        } else {
            bind_address.map(str::to_string)
        };
        if let Some(value) = bind_address.as_deref()
            && let Err(error) = resolve_bind_address(None, Some(value))
        {
            return failure(
                StatusCode::BAD_REQUEST,
                "INVALID_BIND_ADDRESS",
                &error.to_string(),
            );
        }
        let networks_json =
            serde_json::to_string(body.trusted_networks.as_deref().unwrap_or_default())
                .expect("network strings serialize");
        let parsed_networks =
            match weaver_server_core::security::parse_trusted_networks_json(&networks_json) {
                Ok(networks) => networks,
                Err(error) => {
                    return failure(
                        StatusCode::BAD_REQUEST,
                        "INVALID_TRUSTED_NETWORKS",
                        &error.to_string(),
                    );
                }
            };
        let password_hash =
            match tokio::task::spawn_blocking(move || jwt::hash_password(&password)).await {
                Ok(Ok(hash)) => hash,
                _ => {
                    return failure(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "SETUP_HASH_FAILED",
                        "Could not protect the password. Setup was not changed; try again.",
                    );
                }
            };
        let secret = jwt::generate_jwt_secret();
        let token = generate_browser_session_secret();
        let csrf = derive_browser_csrf_token(&token, &secret);
        let now = epoch_seconds();
        let restart_required = bind_address
            .as_deref()
            .is_some_and(|value| value != security.http_bind_address.to_string());
        let setup = InitialAuthenticatedSetup {
            username: username.clone(),
            password_hash: password_hash.clone(),
            jwt_secret: secret,
            completed_at: now,
            bind_address,
            trusted_networks: (!security.trust_env_pinned).then_some(networks_json),
            browser_session: BrowserSession {
                token_hash: hash_to_hex(hash_api_key(&token)),
                csrf_verifier: hash_to_hex(hash_api_key(&csrf)),
                origin,
                client_ip: security
                    .resolve_client_ip(Some(peer_addr), &headers)
                    .map(|ip| ip.to_string()),
                remembered: false,
                created_at: now,
                expires_at: now + JWT_TTL_SECS as i64,
                revoked_at: None,
            },
        };
        let write = db.clone();
        match tokio::task::spawn_blocking(move || {
            write.complete_initial_authenticated_setup(&setup)
        })
        .await
        {
            Ok(Ok(InitialSetupOutcome::Created)) => {}
            Ok(Ok(InitialSetupOutcome::AlreadyCompleted)) => {
                return failure(
                    StatusCode::CONFLICT,
                    "SETUP_COMPLETE",
                    "Setup is no longer available. Sign in.",
                );
            }
            _ => {
                return failure(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "SETUP_SAVE_FAILED",
                    "Could not save setup. No changes were committed; check storage and try again.",
                );
            }
        }
        challenge.consume();
        auth_cache.replace(Some(weaver_server_core::auth::CachedLoginAuth::new(
            username,
            password_hash,
            secret,
        )));
        if !security.trust_env_pinned {
            security.set_trusted_cidrs(parsed_networks);
        }
        security.mark_security_configured();
        let capability = weaver_server_core::runtime::restart::current_restart_capability();
        return (
            StatusCode::OK,
            [
                (
                    header::SET_COOKIE,
                    browser_session_cookie(&token, &base_url.0, secure, false),
                ),
                (header::CACHE_CONTROL, "no-store".to_string()),
            ],
            Json(serde_json::json!({
                "ok": true, "csrfToken": csrf,
                "restartRequiredForBind": restart_required,
                "bindIgnoredBecauseEnvPinned": bind_pinned && body.bind_address.is_some(),
                "accessPolicyIgnoredBecauseEnvPinned": security.trust_env_pinned,
                "restartSupported": capability.supported,
                "restartUnsupportedReason": capability.reason,
            })),
        )
            .into_response();
    }
    if !legacy_setup_available(&security) {
        return super::error_response(
            StatusCode::CONFLICT,
            "Login recovery requires an explicit WEAVER_RESET_LOGIN override",
        );
    }
    // Validate the browser binding before password hashing or any durable
    // setup write. A malformed cross-origin request must leave setup retryable.
    let browser_binding = if security.authenticated_access_mode() {
        let origin = match canonical_browser_origin(&headers) {
            Ok(origin) => origin,
            Err(status) => {
                return super::error_response(status, "a single valid Origin is required");
            }
        };
        let secure = match browser_cookie_secure(&security, peer_addr, &headers, &origin) {
            Ok(secure) => secure,
            Err(status) => return super::error_response(status, "invalid proxy protocol header"),
        };
        Some((origin, secure))
    } else {
        None
    };
    // Judged on the resolved client, not the socket peer, so a machine-local
    // browser reaching Weaver through a configured reverse proxy is still the
    // machine's own browser. Canonical loopback: on a dual-stack listener that
    // browser arrives as `::ffff:127.0.0.1`, which must not read as remote.
    let client_ip = security.resolve_client_ip(Some(peer_addr), &headers);
    if !client_ip.is_some_and(ip_is_loopback)
        && !security.is_trusted_client(Some(peer_addr), &headers)
    {
        return super::error_response(
            StatusCode::FORBIDDEN,
            "setup must be completed from the machine Weaver runs on",
        );
    }

    let Some(mode) = AccessMode::parse_setting_value(&body.mode) else {
        return super::error_response(StatusCode::BAD_REQUEST, "unknown access mode");
    };
    // With the trust list pinned by the environment, the wizard's only real
    // effect is creating credentials. No-login creates none and stores
    // nothing, which would complete setup as a total no-op and leave this
    // browser exactly where it started — refuse it instead of pretending.
    if security.trust_env_pinned && matches!(mode, AccessMode::NoLogin) {
        return super::error_response(
            StatusCode::BAD_REQUEST,
            "WEAVER_TRUSTED_CIDRS pins the browser-access policy in this deployment's \
             environment; choose a login mode",
        );
    }
    if security.authenticated_access_mode() && matches!(mode, AccessMode::NoLogin) {
        return super::error_response(
            StatusCode::BAD_REQUEST,
            "authenticated access requires administrator credentials",
        );
    }

    // Credentials: required for the two login modes, refused for no-login so a
    // password can never be silently collected and ignored.
    let credentials = match mode {
        AccessMode::LoginRequired | AccessMode::LoginExceptLocal => {
            let username = body.username.as_deref().unwrap_or("").trim().to_string();
            let password = body.password.clone().unwrap_or_default();
            if username.is_empty() || password.is_empty() {
                return super::error_response(
                    StatusCode::BAD_REQUEST,
                    "username and password are required for this access mode",
                );
            }
            Some((username, password))
        }
        AccessMode::NoLogin => {
            if body.username.is_some() || body.password.is_some() {
                return super::error_response(
                    StatusCode::BAD_REQUEST,
                    "no-login mode does not take credentials",
                );
            }
            None
        }
    };

    // Trusted networks: only meaningful for except-local; the preset when the
    // wizard sends nothing. Validated all-or-nothing through the same parser
    // startup settles with.
    let trusted_networks: Vec<String> = match (&mode, &body.trusted_networks) {
        (AccessMode::LoginExceptLocal, Some(entries)) => {
            let cleaned: Vec<String> = entries
                .iter()
                .map(|entry| entry.trim().to_string())
                .filter(|entry| !entry.is_empty())
                .collect();
            if cleaned.is_empty() {
                return super::error_response(
                    StatusCode::BAD_REQUEST,
                    "trusted networks must not be empty for this access mode",
                );
            }
            cleaned
        }
        (AccessMode::LoginExceptLocal, None) => LOCAL_NETWORK_PRESETS
            .iter()
            .map(|s| s.to_string())
            .collect(),
        (AccessMode::NoLogin, _) => LOOPBACK_NETWORKS.iter().map(|s| s.to_string()).collect(),
        (AccessMode::LoginRequired, _) => Vec::new(),
    };
    let networks_json = serde_json::to_string(&trusted_networks).unwrap_or_else(|_| "[]".into());
    let parsed_networks =
        match weaver_server_core::security::parse_trusted_networks_json(&networks_json) {
            Ok(parsed) => parsed,
            Err(error) => {
                return super::error_response(StatusCode::BAD_REQUEST, &error.to_string());
            }
        };

    // Bind address: validated exactly as startup resolves it; ignored with a
    // note when the environment pins it, because storing a value the process
    // will never read is a lie waiting to be discovered.
    let bind_pinned_by_env = !security.bind_address_source.is_editable();
    let bind_to_store = match body
        .bind_address
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        None => None,
        Some(_) if bind_pinned_by_env => None,
        Some(value) => match resolve_bind_address(None, Some(value)) {
            Ok(_) => Some(value.to_string()),
            Err(error) => {
                return super::error_response(StatusCode::BAD_REQUEST, &error.to_string());
            }
        },
    };

    // Persist everything, then apply the live effects.
    let hashed = match credentials.clone() {
        Some((username, password)) => {
            let hash_result =
                tokio::task::spawn_blocking(move || jwt::hash_password(&password)).await;
            match hash_result {
                Ok(Ok(hash)) => Some((username, hash)),
                Ok(Err(error)) => {
                    return super::error_response(StatusCode::INTERNAL_SERVER_ERROR, &error);
                }
                Err(error) => {
                    return super::error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        &error.to_string(),
                    );
                }
            }
        }
        None => None,
    };

    // The environment pins the browser-access policy exactly as it pins the
    // bind address: the wizard's answer is neither stored nor applied live,
    // so a running env-managed instance can never have its trust list swapped
    // out from under the deployment by a loopback browser. Credentials are
    // still created — they are the half the environment did not answer.
    let trust_pinned_by_env = security.trust_env_pinned;
    let authenticated_access_mode = security.authenticated_access_mode();
    let db_for_write = db.clone();
    let mode_value = mode.as_setting_value().to_string();
    let networks_json = networks_json.clone();
    let bind_for_write = bind_to_store.clone();
    let hashed_for_write = hashed.clone();
    let store_trusted = matches!(mode, AccessMode::LoginExceptLocal) && !trust_pinned_by_env;
    let write_result = tokio::task::spawn_blocking(move || {
        if !trust_pinned_by_env {
            db_for_write.set_setting(SETTING_ACCESS_MODE, &mode_value)?;
        }
        if authenticated_access_mode {
            db_for_write.set_setting(
                SETTING_SECURITY_POLICY_REVISION,
                AUTHENTICATED_POLICY_REVISION,
            )?;
        }
        if store_trusted {
            db_for_write.set_setting(SETTING_TRUSTED_NETWORKS, &networks_json)?;
        }
        if let Some(address) = bind_for_write.as_deref() {
            db_for_write.set_setting(SETTING_HTTP_BIND_ADDRESS, address)?;
        }
        let jwt_secret = match hashed_for_write {
            Some((username, hash)) => {
                db_for_write.set_auth_credentials(&username, &hash)?;
                Some(db_for_write.rotate_jwt_signing_secret()?)
            }
            None => None,
        };
        Ok::<_, weaver_server_core::StateError>(jwt_secret)
    })
    .await;
    let jwt_secret = match write_result {
        Ok(Ok(secret)) => secret,
        Ok(Err(error)) => {
            return super::error_response(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string());
        }
        Err(error) => {
            return super::error_response(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string());
        }
    };

    // Live effects: trust applies immediately (the shared list every clone
    // reads), credentials swap into the cache, and the wizard's own browser is
    // signed in so completing setup lands in the app rather than at a login
    // form. Only the bind address waits for a restart.
    if !trust_pinned_by_env {
        security.set_trusted_cidrs(parsed_networks);
        // The policy is settled from this instant, which is what stops a
        // no-login install re-offering this wizard to every browser it does
        // not trust. (An env-pinned deployment was settled at startup.)
        security.mark_security_configured();
    }

    let mut response_headers: Vec<(header::HeaderName, String)> = Vec::new();
    // A no-login install that now trusts this browser's own machine gets the
    // browser session cookie the next page load would hand it anyway. The
    // wizard's remaining calls are made from THIS page, which never reloads
    // when the operator restarts Weaver from it.
    if credentials.is_none() && security.is_trusted_client(Some(peer_addr), &headers) {
        response_headers.push((
            header::SET_COOKIE,
            session_cookie_value(session_token.0.as_str(), &security),
        ));
    }
    if let (Some((username, hash)), Some(secret)) = (hashed, jwt_secret) {
        let cached = weaver_server_core::auth::CachedLoginAuth::new(username, hash, secret);
        auth_cache.replace(Some(cached.clone()));
        if let Some((origin, secure)) = browser_binding {
            let token = generate_browser_session_secret();
            let csrf = derive_browser_csrf_token(&token, &cached.jwt_secret);
            let now = epoch_seconds();
            let session = BrowserSession {
                token_hash: hash_to_hex(hash_api_key(&token)),
                csrf_verifier: hash_to_hex(hash_api_key(&csrf)),
                origin,
                client_ip: security
                    .resolve_client_ip(Some(peer_addr), &headers)
                    .map(|ip| ip.to_string()),
                remembered: false,
                created_at: now,
                expires_at: now + JWT_TTL_SECS as i64,
                revoked_at: None,
            };
            let db = db.clone();
            if tokio::task::spawn_blocking(move || db.create_browser_session(&session))
                .await
                .ok()
                .and_then(Result::ok)
                .is_none()
            {
                return super::error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "could not create browser session",
                );
            }
            response_headers.push((
                header::SET_COOKIE,
                browser_session_cookie(&token, &base_url.0, secure, false),
            ));
        } else {
            let token = jwt::create_jwt(&cached.username, &cached.jwt_secret, JWT_TTL_SECS);
            response_headers.push((
                header::SET_COOKIE,
                format!(
                    "{JWT_COOKIE_NAME}={token}; Path=/; HttpOnly; SameSite=Strict; Max-Age={JWT_TTL_SECS}{}",
                    secure_cookie_suffix(&security)
                ),
            ));
        }
    }

    let restart_required_for_bind = bind_to_store.is_some();
    tracing::info!(
        mode = mode.as_setting_value(),
        login_created = credentials.is_some(),
        bind_stored = restart_required_for_bind,
        bind_ignored_env_pinned = bind_pinned_by_env
            && body
                .bind_address
                .as_deref()
                .is_some_and(|v| !v.trim().is_empty()),
        "first-run setup completed from the wizard"
    );

    // Whether the browser may offer to do the restart itself, answered from the
    // same rule the restart endpoint enforces.
    let restart_capability = weaver_server_core::runtime::restart::current_restart_capability();
    let mut response = (
        StatusCode::OK,
        Json(serde_json::json!({
            "ok": true,
            "restartRequiredForBind": restart_required_for_bind,
            "bindIgnoredBecauseEnvPinned": bind_pinned_by_env
                && body.bind_address.as_deref().is_some_and(|v| !v.trim().is_empty()),
            "accessPolicyIgnoredBecauseEnvPinned": trust_pinned_by_env,
            "restartSupported": restart_capability.supported,
            "restartUnsupportedReason": restart_capability.reason,
        })),
    )
        .into_response();
    for (name, value) in response_headers {
        if let Ok(value) = value.parse() {
            response.headers_mut().append(name, value);
        }
    }
    response
}

pub(super) async fn logout_handler(
    Extension(db): Extension<Database>,
    Extension(security): Extension<RuntimeSecurityConfig>,
    Extension(base_url): Extension<super::assets::BaseUrl>,
    headers: HeaderMap,
) -> Response {
    if let Err(status) = validate_browser_csrf(&db, &security, &headers).await {
        return super::error_response(status, "browser verification required");
    }
    let mut revoked = true;
    if security.authenticated_access_mode()
        && let Some(token) = extract_session_cookie(&headers)
    {
        let token_hash = hash_to_hex(hash_api_key(&token));
        revoked = matches!(
            tokio::task::spawn_blocking(move || {
                db.revoke_browser_session(&token_hash, epoch_seconds())
            })
            .await,
            Ok(Ok(()))
        );
    }
    let secure = if security.secure_cookies
        || canonical_browser_origin(&headers).is_ok_and(|origin| origin.starts_with("https://"))
    {
        "; Secure"
    } else {
        ""
    };
    let path = if security.authenticated_access_mode() {
        browser_cookie_path(&base_url.0)
    } else {
        "/"
    };
    let jwt_cookie =
        format!("{JWT_COOKIE_NAME}=; Path=/; HttpOnly; SameSite=Strict; Max-Age=0{secure}");
    let session_cookie = format!(
        "{SESSION_COOKIE_NAME}=; Path={path}; HttpOnly; SameSite=Strict; Max-Age=0{secure}"
    );
    let mut response = if revoked {
        (StatusCode::OK, Json(serde_json::json!({ "ok": true }))).into_response()
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({
            "ok": false,
            "code": "SESSION_REVOCATION_FAILED",
            "error": "This browser was signed out, but server session revocation failed. Retry after storage is available."
        }))).into_response()
    };
    response.headers_mut().append(
        header::SET_COOKIE,
        jwt_cookie.parse().expect("JWT expiry cookie is valid"),
    );
    response.headers_mut().append(
        header::SET_COOKIE,
        session_cookie
            .parse()
            .expect("session expiry cookie is valid"),
    );
    response
}

const FRESH_ADMIN_TTL_SECS: i64 = 15 * 60;

/// Resolves the current caller and requires a recent password check for a
/// durable browser session. Persistent administrator API keys deliberately
/// retain their documented machine-to-machine capability.
pub(super) async fn require_fresh_admin(
    request_auth: &super::RequestAuthContext,
    peer: Option<SocketAddr>,
    headers: &HeaderMap,
) -> Result<ResolvedCaller, StatusCode> {
    let caller = resolve_caller(
        &request_auth.db,
        &request_auth.auth_cache,
        &request_auth.api_key_cache,
        request_auth.session_token.0.as_str(),
        &request_auth.security,
        BrowserSessionPolicy::TrustedPeer(peer),
        headers,
    )
    .await?;
    if !caller.scope.is_admin() {
        return Err(StatusCode::FORBIDDEN);
    }
    if !request_auth.security.authenticated_access_mode()
        || matches!(&caller.identity, CallerIdentity::ApiKey(_))
    {
        return Ok(caller);
    }
    let Some(token) = extract_session_cookie(headers) else {
        return Err(StatusCode::UNAUTHORIZED);
    };
    if caller.identity != CallerIdentity::Jwt(hash_api_key(&token)) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let token_hash = hash_to_hex(hash_api_key(&token));
    let db = request_auth.db.clone();
    let now = epoch_seconds();
    let verified_at = tokio::task::spawn_blocking(move || {
        db.browser_session_password_verified_at(&token_hash, now)
    })
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    if verified_at
        .is_none_or(|verified_at| verified_at > now || now - verified_at >= FRESH_ADMIN_TTL_SECS)
    {
        return Err(StatusCode::PRECONDITION_REQUIRED);
    }
    Ok(caller)
}

#[derive(Deserialize)]
pub(super) struct VerifyPasswordRequest {
    password: String,
}

pub(super) async fn verify_password_handler(
    ConnectInfo(peer_addr): ConnectInfo<SocketAddr>,
    Extension(request_auth): Extension<super::RequestAuthContext>,
    Extension(login_limiter): Extension<LoginRateLimiter>,
    headers: HeaderMap,
    Json(body): Json<VerifyPasswordRequest>,
) -> Response {
    if validate_browser_csrf(&request_auth.db, &request_auth.security, &headers)
        .await
        .is_err()
    {
        return super::error_response(StatusCode::FORBIDDEN, "browser verification required");
    }
    let caller = match resolve_caller(
        &request_auth.db,
        &request_auth.auth_cache,
        &request_auth.api_key_cache,
        request_auth.session_token.0.as_str(),
        &request_auth.security,
        BrowserSessionPolicy::TrustedPeer(Some(peer_addr)),
        &headers,
    )
    .await
    {
        Ok(caller) => caller,
        Err(status) => return status.into_response(),
    };
    let CallerIdentity::Jwt(_) = caller.identity else {
        return super::error_response(StatusCode::FORBIDDEN, "browser session required");
    };
    let Some(auth) = request_auth.auth_cache.snapshot() else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    let client_id = login_client_id(&request_auth.security, &headers, Some(peer_addr));
    if login_limiter.too_many_failures(&auth.username, &client_id) {
        return super::error_response(
            StatusCode::TOO_MANY_REQUESTS,
            "too many verification attempts",
        );
    }
    let username = auth.username.clone();
    let valid = match verify_password_bounded(body.password, auth.password_hash).await {
        Ok(valid) => valid,
        Err(status) => return status.into_response(),
    };
    if !valid {
        login_limiter.record_failure(&username, &client_id);
        return super::error_response(StatusCode::UNAUTHORIZED, "invalid password");
    }
    login_limiter.record_success(&username, &client_id);
    let Some(token) = extract_session_cookie(&headers) else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    let token_hash = hash_to_hex(hash_api_key(&token));
    let db = request_auth.db.clone();
    if tokio::task::spawn_blocking(move || {
        db.verify_browser_session_password(&token_hash, epoch_seconds())
    })
    .await
    .ok()
    .and_then(Result::ok)
    .is_none()
    {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    Json(serde_json::json!({ "ok": true })).into_response()
}

pub(super) async fn sign_out_all_handler(
    Extension(request_auth): Extension<super::RequestAuthContext>,
    peer: Option<Extension<ConnectInfo<SocketAddr>>>,
    headers: HeaderMap,
) -> Response {
    let peer = peer.map(|Extension(ConnectInfo(peer))| peer);
    if let Err(status) = require_fresh_admin(&request_auth, peer, &headers).await {
        return super::error_response(status, "recent password verification required");
    }
    let db = request_auth.db.clone();
    let now = epoch_seconds();
    if tokio::task::spawn_blocking(move || db.revoke_all_browser_sessions(now))
        .await
        .ok()
        .and_then(Result::ok)
        .is_none()
    {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    Json(serde_json::json!({ "ok": true })).into_response()
}

pub(super) async fn csrf_handler(
    ConnectInfo(peer_addr): ConnectInfo<SocketAddr>,
    Extension(db): Extension<Database>,
    Extension(auth_cache): Extension<LoginAuthCache>,
    Extension(security): Extension<RuntimeSecurityConfig>,
    headers: HeaderMap,
) -> Response {
    if !security.authenticated_access_mode() {
        return super::error_response(StatusCode::NOT_FOUND, "not available");
    }
    let Some(token) = extract_session_cookie(&headers) else {
        return super::error_response(StatusCode::UNAUTHORIZED, "authentication required");
    };
    let token_hash = hash_to_hex(hash_api_key(&token));
    let session = tokio::task::spawn_blocking(move || {
        db.get_active_browser_session(&token_hash, epoch_seconds())
    })
    .await
    .ok()
    .and_then(Result::ok)
    .flatten();
    let Some(session) = session else {
        return super::error_response(StatusCode::UNAUTHORIZED, "authentication required");
    };
    if (headers.contains_key(header::ORIGIN)
        && canonical_browser_origin(&headers).ok().as_deref() != Some(session.origin.as_str()))
        || (session.remembered && !security.remembered_client_allowed(Some(peer_addr), &headers))
    {
        return super::error_response(StatusCode::FORBIDDEN, "browser verification required");
    }
    let Some(auth) = auth_cache.snapshot() else {
        return super::error_response(StatusCode::UNAUTHORIZED, "authentication required");
    };
    let csrf = derive_browser_csrf_token(&token, &auth.jwt_secret);
    if !weaver_server_core::auth::service::verify_browser_csrf_token(&csrf, &session.csrf_verifier)
    {
        return super::error_response(StatusCode::UNAUTHORIZED, "sign in again");
    }
    (
        [(header::CACHE_CONTROL, "no-store")],
        Json(serde_json::json!({ "csrfToken": csrf })),
    )
        .into_response()
}

pub(super) fn session_cookie_value(
    session_token: &str,
    security: &RuntimeSecurityConfig,
) -> String {
    format!(
        "{SESSION_COOKIE_NAME}={session_token}; Path=/; HttpOnly; SameSite=Strict{}",
        secure_cookie_suffix(security)
    )
}

fn secure_cookie_suffix(security: &RuntimeSecurityConfig) -> &'static str {
    if security.secure_cookies {
        "; Secure"
    } else {
        ""
    }
}

fn browser_cookie_secure(
    security: &RuntimeSecurityConfig,
    peer: SocketAddr,
    headers: &HeaderMap,
    origin: &str,
) -> Result<bool, StatusCode> {
    let mut forwarded_https = false;
    if security.is_trusted_proxy(peer.ip()) {
        let mut protocols = headers.get_all("x-forwarded-proto").iter();
        if let Some(value) = protocols.next() {
            if protocols.next().is_some() {
                return Err(StatusCode::FORBIDDEN);
            }
            forwarded_https = match value.to_str() {
                Ok("https") => true,
                Ok("http") => false,
                _ => return Err(StatusCode::FORBIDDEN),
            };
        }
    }
    Ok(security.secure_cookies || forwarded_https || origin.starts_with("https://"))
}

fn browser_cookie_path(base_url: &str) -> &str {
    if base_url.is_empty() { "/" } else { base_url }
}

fn browser_session_cookie(token: &str, base_url: &str, secure: bool, remember: bool) -> String {
    let path = browser_cookie_path(base_url);
    let secure = if secure { "; Secure" } else { "" };
    let lifetime = if remember {
        format!("; Max-Age={JWT_TTL_SECS}")
    } else {
        String::new()
    };
    format!(
        "{SESSION_COOKIE_NAME}={token}; Path={path}; HttpOnly; SameSite=Strict{lifetime}{secure}"
    )
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use super::*;

    fn authenticated_request_context(
        db: Database,
        security: Arc<RuntimeSecurityConfig>,
    ) -> super::super::RequestAuthContext {
        super::super::RequestAuthContext {
            db,
            auth_cache: LoginAuthCache::default(),
            api_key_cache: ApiKeyCache::default(),
            session_token: super::super::SessionToken(Arc::new("legacy-session".to_string())),
            security,
        }
    }

    #[tokio::test]
    async fn fresh_admin_rejects_stale_browser_session_but_accepts_admin_api_key() {
        let db = Database::open_in_memory().unwrap();
        let security = Arc::new(RuntimeSecurityConfig::default());
        security.apply_stored_access_policy_revision(None, None, false);
        let now = epoch_seconds();
        let browser_token = "stale-browser-session";
        db.create_browser_session(&BrowserSession {
            token_hash: hash_to_hex(hash_api_key(browser_token)),
            csrf_verifier: "csrf-verifier".to_string(),
            origin: "http://localhost".to_string(),
            client_ip: Some("127.0.0.1".to_string()),
            remembered: false,
            created_at: now - FRESH_ADMIN_TTL_SECS,
            expires_at: now + 3_600,
            revoked_at: None,
        })
        .unwrap();
        let request_auth = authenticated_request_context(db.clone(), security.clone());
        let mut browser_headers = HeaderMap::new();
        browser_headers.insert(
            header::COOKIE,
            format!("{SESSION_COOKIE_NAME}={browser_token}")
                .parse()
                .unwrap(),
        );
        assert_eq!(
            require_fresh_admin(&request_auth, None, &browser_headers)
                .await
                .err(),
            Some(StatusCode::PRECONDITION_REQUIRED)
        );

        let api_key = "fresh-admin-api-key";
        db.insert_api_key("fresh-admin", &hash_api_key(api_key), "admin")
            .unwrap();
        let mut api_headers = HeaderMap::new();
        api_headers.insert(
            header::AUTHORIZATION,
            format!("Bearer {api_key}").parse().unwrap(),
        );
        assert!(
            require_fresh_admin(&request_auth, None, &api_headers)
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn authenticated_policy_rejects_legacy_jwt_and_shared_cookie() {
        let db = Database::open_in_memory().unwrap();
        let cache = LoginAuthCache::default();
        let secret = [7; 32];
        cache.replace(Some(weaver_server_core::auth::CachedLoginAuth::new(
            "admin", "unused", secret,
        )));
        let security = RuntimeSecurityConfig::default();
        security.apply_stored_access_policy_revision(None, None, false);
        security.set_trusted_cidrs(vec!["127.0.0.0/8".parse().unwrap()]);
        let legacy_jwt = jwt::create_jwt("admin", &secret, JWT_TTL_SECS);
        for cookie in [
            format!("{JWT_COOKIE_NAME}={legacy_jwt}"),
            format!("{SESSION_COOKIE_NAME}=shared-token"),
            String::new(),
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(header::COOKIE, cookie.parse().unwrap());
            let result = resolve_caller(
                &db,
                &cache,
                &ApiKeyCache::default(),
                "shared-token",
                &security,
                BrowserSessionPolicy::TrustedPeer(Some("127.0.0.1:54321".parse().unwrap())),
                &headers,
            )
            .await;
            assert!(matches!(result, Err(StatusCode::UNAUTHORIZED)));
        }
    }

    #[tokio::test]
    async fn logout_requires_csrf_and_revokes_only_the_current_browser() {
        let db = Database::open_in_memory().unwrap();
        let security = RuntimeSecurityConfig::default();
        security.apply_stored_access_policy_revision(None, None, false);
        let mut headers = HeaderMap::new();
        headers.insert(
            header::COOKIE,
            "weaver_session=browser-one".parse().unwrap(),
        );
        headers.insert(header::ORIGIN, "https://media.test".parse().unwrap());
        let now = epoch_seconds();
        for token in ["browser-one", "browser-two"] {
            db.create_browser_session(&BrowserSession {
                token_hash: hash_to_hex(hash_api_key(token)),
                csrf_verifier: hash_to_hex(hash_api_key("csrf-one")),
                origin: "https://media.test".into(),
                client_ip: None,
                remembered: false,
                created_at: now,
                expires_at: now + 600,
                revoked_at: None,
            })
            .unwrap();
        }
        let base = super::super::assets::BaseUrl(Arc::new("/weaver".into()));
        let rejected = logout_handler(
            Extension(db.clone()),
            Extension(security.clone()),
            Extension(base.clone()),
            headers.clone(),
        )
        .await;
        assert_eq!(rejected.status(), StatusCode::FORBIDDEN);
        assert!(
            db.get_active_browser_session(&hash_to_hex(hash_api_key("browser-one")), now)
                .unwrap()
                .is_some()
        );
        headers.insert("x-weaver-csrf", "csrf-one".parse().unwrap());
        let response = logout_handler(
            Extension(db.clone()),
            Extension(security),
            Extension(base),
            headers,
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            response
                .headers()
                .get_all(header::SET_COOKIE)
                .iter()
                .any(|cookie| cookie
                    .to_str()
                    .unwrap()
                    .starts_with("weaver_session=; Path=/weaver;"))
        );
        assert!(
            db.get_active_browser_session(&hash_to_hex(hash_api_key("browser-one")), now)
                .unwrap()
                .is_none()
        );
        assert!(
            db.get_active_browser_session(&hash_to_hex(hash_api_key("browser-two")), now)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn https_origin_strengthens_cookie_without_trusting_forwarded_headers() {
        let security = RuntimeSecurityConfig::default();
        let peer = "203.0.113.7:54321".parse().unwrap();
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-proto", "https".parse().unwrap());
        assert!(!browser_cookie_secure(&security, peer, &headers, "http://media.test").unwrap());
        assert!(browser_cookie_secure(&security, peer, &headers, "https://media.test").unwrap());
    }

    #[test]
    fn trusted_proxy_protocol_is_single_and_unambiguous() {
        let mut security = RuntimeSecurityConfig::default();
        security.trusted_proxies = vec!["10.0.0.3/32".parse().unwrap()];
        let peer = "10.0.0.3:54321".parse().unwrap();
        for invalid in ["https,http", "https, https", "invalid", ""] {
            let mut headers = HeaderMap::new();
            headers.insert("x-forwarded-proto", invalid.parse().unwrap());
            assert!(
                browser_cookie_secure(&security, peer, &headers, "https://media.test").is_err()
            );
        }
        let mut headers = HeaderMap::new();
        headers.append("x-forwarded-proto", "https".parse().unwrap());
        headers.append("x-forwarded-proto", "https".parse().unwrap());
        assert!(browser_cookie_secure(&security, peer, &headers, "https://media.test").is_err());
        headers.remove("x-forwarded-proto");
        headers.insert("x-forwarded-proto", "https".parse().unwrap());
        assert!(browser_cookie_secure(&security, peer, &headers, "http://media.test").unwrap());
    }

    #[test]
    fn browser_cookie_lifetime_and_path_follow_login_choice() {
        let session = browser_session_cookie("test-token", "/weaver", true, false);
        assert!(session.contains("Path=/weaver;"));
        assert!(session.contains("; Secure"));
        assert!(!session.contains("Max-Age"));
        assert!(!session.contains("Domain="));
        let remembered = browser_session_cookie("test-token", "", false, true);
        assert!(remembered.contains("Path=/;"));
        assert!(remembered.contains(&format!("Max-Age={JWT_TTL_SECS}")));
    }

    #[test]
    fn login_limiter_blocks_account_across_clients() {
        let limiter = LoginRateLimiter::default();
        for idx in 0..LOGIN_MAX_FAILURES {
            limiter.record_failure("Admin", &format!("10.0.0.{idx}"));
        }

        assert!(limiter.too_many_failures("admin", "192.0.2.1"));
        assert!(!limiter.too_many_failures("other", "192.0.2.1"));
    }

    #[test]
    fn login_limiter_blocks_client_across_accounts() {
        let limiter = LoginRateLimiter::default();
        for idx in 0..LOGIN_MAX_FAILURES {
            limiter.record_failure(&format!("user{idx}"), "198.51.100.7");
        }

        assert!(limiter.too_many_failures("new-user", "198.51.100.7"));
        assert!(!limiter.too_many_failures("new-user", "198.51.100.8"));
    }

    #[test]
    fn login_limiter_success_clears_matching_account_and_client() {
        let limiter = LoginRateLimiter::default();
        for _ in 0..LOGIN_MAX_FAILURES {
            limiter.record_failure("admin", "203.0.113.5");
        }
        assert!(limiter.too_many_failures("admin", "203.0.113.5"));
        assert!(limiter.too_many_failures("other", "203.0.113.5"));

        limiter.record_success("admin", "203.0.113.5");

        assert!(!limiter.too_many_failures("admin", "203.0.113.6"));
        assert!(!limiter.too_many_failures("other", "203.0.113.5"));
    }

    #[test]
    fn login_limiter_fails_closed_for_unseen_identity_at_capacity() {
        let limiter = LoginRateLimiter::default();
        for idx in 0..(LOGIN_LIMITER_MAX_KEYS / 2) {
            limiter.record_failure(
                &format!("account-{idx}"),
                &format!("198.51.100.{}", idx + 1),
            );
        }

        assert!(limiter.too_many_failures("new-account", "203.0.113.10"));
    }

    #[test]
    fn login_client_id_prefers_peer_address_then_forwarded_headers() {
        let security = RuntimeSecurityConfig::default();
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-forwarded-for",
            "198.51.100.10, 198.51.100.11".parse().unwrap(),
        );
        headers.insert("x-real-ip", "198.51.100.12".parse().unwrap());

        let peer: SocketAddr = "203.0.113.9:51234".parse().unwrap();
        assert_eq!(
            login_client_id(&security, &headers, Some(peer)),
            "203.0.113.9"
        );
        assert_eq!(login_client_id(&security, &headers, None), "198.51.100.10");
    }

    /// Metering follows the same resolution trust does: a browser behind a
    /// configured proxy is rate-limited on its own address, so one attacker
    /// cannot lock out every other browser sharing that proxy.
    #[test]
    fn login_client_id_meters_the_client_behind_a_configured_proxy() {
        let mut security = RuntimeSecurityConfig::default();
        security.trusted_proxies = vec!["10.8.0.2/32".parse().unwrap()];
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-for", "198.51.100.10".parse().unwrap());

        let proxy: SocketAddr = "10.8.0.2:51234".parse().unwrap();
        let stranger: SocketAddr = "203.0.113.9:51234".parse().unwrap();
        assert_eq!(
            login_client_id(&security, &headers, Some(proxy)),
            "198.51.100.10"
        );
        assert_eq!(
            login_client_id(&security, &headers, Some(stranger)),
            "203.0.113.9"
        );
    }

    #[tokio::test]
    async fn logout_expires_both_auth_cookies() {
        let response = logout_handler(
            Extension(Database::open_in_memory().unwrap()),
            Extension(RuntimeSecurityConfig::default()),
            Extension(super::super::assets::BaseUrl(Arc::new(String::new()))),
            HeaderMap::new(),
        )
        .await;
        let cookies = response
            .headers()
            .get_all(header::SET_COOKIE)
            .iter()
            .map(|value| value.to_str().unwrap())
            .collect::<Vec<_>>();

        assert_eq!(cookies.len(), 2);
        assert!(
            cookies
                .iter()
                .any(|cookie| cookie.starts_with("weaver_jwt=;"))
        );
        assert!(
            cookies
                .iter()
                .any(|cookie| cookie.starts_with("weaver_session=;"))
        );
        assert!(cookies.iter().all(|cookie| cookie.contains("Max-Age=0")));
    }
}

pub(super) fn legacy_setup_available(security: &RuntimeSecurityConfig) -> bool {
    !security.security_configured()
        || std::env::var("WEAVER_RESET_LOGIN")
            .is_ok_and(|value| value == "1" || value.eq_ignore_ascii_case("true"))
}

pub(super) async fn auth_status_handler(
    Extension(db): Extension<Database>,
    Extension(api_key_cache): Extension<ApiKeyCache>,
    Extension(auth_cache): Extension<LoginAuthCache>,
    Extension(security): Extension<RuntimeSecurityConfig>,
    challenge: Option<Extension<super::setup_code::SetupChallenge>>,
    peer: Option<Extension<ConnectInfo<SocketAddr>>>,
    headers: HeaderMap,
) -> Response {
    let creds = auth_cache.snapshot();
    let peer = peer.map(|Extension(ConnectInfo(peer))| peer);
    let login_authenticated = if security.authenticated_access_mode() {
        resolve_caller(
            &db,
            &auth_cache,
            &api_key_cache,
            "",
            &security,
            BrowserSessionPolicy::TrustedPeer(peer),
            &headers,
        )
        .await
        .is_ok()
    } else if let Some(creds) = creds.as_ref() {
        if let Some(token) = extract_jwt_cookie(&headers) {
            jwt::verify_jwt(&token, &creds.jwt_secret).is_ok()
        } else {
            false
        }
    } else {
        false
    };
    let trusted_peer =
        !security.authenticated_access_mode() && security.is_trusted_client(peer, &headers);
    // Setup is offered to exactly the browsers that could complete it:
    // `setup_handler` admits loopback-or-trusted, and a trusted peer skips
    // the wizard entirely (it is already admitted), which leaves loopback.
    // The loopback term is what stops the two permanent loops — a CONFIGURED
    // no-login instance looks like "no credentials, not trusted" to every
    // outside browser, and inside a container NO outside browser is ever
    // loopback — while still reopening the wizard for the machine's own
    // browser after WEAVER_RESET_LOGIN clears the credentials on an
    // already-configured install.
    let setup_required = creds.is_none()
        && if security.authenticated_access_mode() {
            challenge.is_some_and(|Extension(challenge)| challenge.is_available())
        } else {
            legacy_setup_available(&security)
                && !trusted_peer
                && security
                    .resolve_client_ip(peer, &headers)
                    .is_some_and(weaver_server_core::security::ip_is_loopback)
        };

    let mut status = serde_json::json!({
        "enabled": creds.is_some(),
        "authenticatedAccess": security.authenticated_access_mode(),
        "authenticated": login_authenticated || trusted_peer,
        "setupRequired": setup_required,
    });
    // Only a browser about to run the first-run wizard is told how this
    // deployment is packaged. This endpoint is unauthenticated, so a
    // configured install must not describe itself to anyone who asks.
    if setup_required {
        let environment = weaver_server_core::runtime::environment::detect_runtime_environment();
        status["setup"] = serde_json::json!({
            "bindEditable": security.bind_address_source.is_editable(),
            "codeRequired": security.authenticated_access_mode(),
            "deployment": environment.deployment.as_str(),
        });
    }
    (
        [
            (header::CACHE_CONTROL, "no-store"),
            (header::VARY, "Cookie, Authorization"),
        ],
        Json(status),
    )
        .into_response()
}
