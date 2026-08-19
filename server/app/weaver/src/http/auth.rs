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
    ApiKeyAuthRow, ApiKeyCache, CallerScope, LoginAuthCache, hash_api_key, verify_password,
};
use weaver_server_core::security::RuntimeSecurityConfig;

pub(super) const JWT_COOKIE_NAME: &str = "weaver_jwt";
pub(super) const SESSION_COOKIE_NAME: &str = "weaver_session";
pub(super) const LOGIN_MAX_FAILURES: usize = 5;
const LOGIN_FAILURE_WINDOW: Duration = Duration::from_secs(60);
const LOGIN_LIMITER_MAX_KEYS: usize = 1024;

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
        })
    }

    fn record_failure(&self, username: &str, client_id: &str) {
        let now = Instant::now();
        let keys = Self::limiter_keys(username, client_id);
        let mut attempts = self.inner.lock().unwrap();
        Self::prune_expired(&mut attempts, now);
        for key in keys {
            if !attempts.contains_key(&key) && attempts.len() >= LOGIN_LIMITER_MAX_KEYS {
                attempts.clear();
            }
            let window = attempts.entry(key).or_insert(LoginFailureWindow {
                failures: 0,
                first_failure: now,
            });
            window.failures = window.failures.saturating_add(1);
        }
    }

    fn record_success(&self, username: &str, _client_id: &str) {
        let key = Self::account_key(username);
        let mut attempts = self.inner.lock().unwrap();
        attempts.remove(&key);
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

fn login_client_id(headers: &HeaderMap, peer_addr: Option<SocketAddr>) -> String {
    if let Some(addr) = peer_addr {
        return addr.ip().to_string();
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
        && cached_auth.is_none()
        && security.is_trusted_peer(peer)
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

#[derive(Deserialize)]
pub(super) struct LoginRequest {
    username: String,
    password: String,
}

pub(super) async fn login_handler(
    ConnectInfo(peer_addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Extension(_db): Extension<Database>,
    Extension(auth_cache): Extension<LoginAuthCache>,
    Extension(login_limiter): Extension<LoginRateLimiter>,
    Extension(security): Extension<RuntimeSecurityConfig>,
    Json(body): Json<LoginRequest>,
) -> Response {
    let creds = match auth_cache.snapshot() {
        Some(creds) => creds,
        None => {
            return super::error_response(StatusCode::BAD_REQUEST, "login is not enabled");
        }
    };
    let client_id = login_client_id(&headers, Some(peer_addr));

    if login_limiter.too_many_failures(&body.username, &client_id) {
        return super::error_response(StatusCode::TOO_MANY_REQUESTS, "too many login attempts");
    }

    let username_matches = body.username == creds.username;
    let hash = creds.password_hash.clone();
    let password = body.password.clone();
    let password_valid = tokio::task::spawn_blocking(move || verify_password(&password, &hash))
        .await
        .unwrap_or(false);

    if !username_matches || !password_valid {
        login_limiter.record_failure(&body.username, &client_id);
        return super::error_response(StatusCode::UNAUTHORIZED, "invalid credentials");
    }

    login_limiter.record_success(&body.username, &client_id);
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
pub(super) async fn setup_handler(
    ConnectInfo(peer_addr): ConnectInfo<SocketAddr>,
    Extension(db): Extension<Database>,
    Extension(auth_cache): Extension<LoginAuthCache>,
    Extension(security): Extension<RuntimeSecurityConfig>,
    Json(body): Json<SetupRequest>,
) -> Response {
    use weaver_server_core::security::{
        AccessMode, LOCAL_NETWORK_PRESETS, LOOPBACK_NETWORKS, SETTING_ACCESS_MODE,
        SETTING_HTTP_BIND_ADDRESS, SETTING_TRUSTED_NETWORKS, ip_is_loopback, resolve_bind_address,
    };

    if auth_cache.snapshot().is_some() {
        return super::error_response(StatusCode::CONFLICT, "setup is already complete");
    }
    // Canonical loopback: on a dual-stack listener the machine's own browser
    // arrives as `::ffff:127.0.0.1`, which must not be refused as remote.
    if !ip_is_loopback(peer_addr.ip()) && !security.is_trusted_peer(Some(peer_addr)) {
        return super::error_response(
            StatusCode::FORBIDDEN,
            "setup must be completed from the machine Weaver runs on",
        );
    }

    let Some(mode) = AccessMode::parse_setting_value(&body.mode) else {
        return super::error_response(StatusCode::BAD_REQUEST, "unknown access mode");
    };

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

    let db_for_write = db.clone();
    let mode_value = mode.as_setting_value().to_string();
    let networks_json = networks_json.clone();
    let bind_for_write = bind_to_store.clone();
    let hashed_for_write = hashed.clone();
    let store_trusted = matches!(mode, AccessMode::LoginExceptLocal);
    let write_result = tokio::task::spawn_blocking(move || {
        db_for_write.set_setting(SETTING_ACCESS_MODE, &mode_value)?;
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
    security.set_trusted_cidrs(parsed_networks);

    let mut response_headers: Vec<(header::HeaderName, String)> = Vec::new();
    if let (Some((username, hash)), Some(secret)) = (hashed, jwt_secret) {
        let cached = weaver_server_core::auth::CachedLoginAuth::new(username, hash, secret);
        let token = jwt::create_jwt(&cached.username, &cached.jwt_secret, JWT_TTL_SECS);
        auth_cache.replace(Some(cached));
        response_headers.push((
            header::SET_COOKIE,
            format!(
                "{JWT_COOKIE_NAME}={token}; Path=/; HttpOnly; SameSite=Strict; Max-Age={JWT_TTL_SECS}{}",
                secure_cookie_suffix(&security)
            ),
        ));
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

    let mut response = (
        StatusCode::OK,
        Json(serde_json::json!({
            "ok": true,
            "restartRequiredForBind": restart_required_for_bind,
            "bindIgnoredBecauseEnvPinned": bind_pinned_by_env
                && body.bind_address.as_deref().is_some_and(|v| !v.trim().is_empty()),
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
    Extension(security): Extension<RuntimeSecurityConfig>,
) -> Response {
    let secure = secure_cookie_suffix(&security);
    let jwt_cookie =
        format!("{JWT_COOKIE_NAME}=; Path=/; HttpOnly; SameSite=Strict; Max-Age=0{secure}");
    let session_cookie =
        format!("{SESSION_COOKIE_NAME}=; Path=/; HttpOnly; SameSite=Strict; Max-Age=0{secure}");
    let mut response = (StatusCode::OK, Json(serde_json::json!({ "ok": true }))).into_response();
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

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use super::*;

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
    fn login_limiter_success_clears_matching_account_but_not_client() {
        let limiter = LoginRateLimiter::default();
        for _ in 0..LOGIN_MAX_FAILURES {
            limiter.record_failure("admin", "203.0.113.5");
        }
        assert!(limiter.too_many_failures("admin", "203.0.113.5"));
        assert!(limiter.too_many_failures("other", "203.0.113.5"));

        limiter.record_success("admin", "203.0.113.5");

        assert!(!limiter.too_many_failures("admin", "203.0.113.6"));
        assert!(limiter.too_many_failures("other", "203.0.113.5"));
    }

    #[test]
    fn login_client_id_prefers_peer_address_then_forwarded_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-forwarded-for",
            "198.51.100.10, 198.51.100.11".parse().unwrap(),
        );
        headers.insert("x-real-ip", "198.51.100.12".parse().unwrap());

        let peer: SocketAddr = "203.0.113.9:51234".parse().unwrap();
        assert_eq!(login_client_id(&headers, Some(peer)), "203.0.113.9");
        assert_eq!(login_client_id(&headers, None), "198.51.100.10");
    }

    #[tokio::test]
    async fn logout_expires_both_auth_cookies() {
        let response = logout_handler(Extension(RuntimeSecurityConfig::default())).await;
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

pub(super) async fn auth_status_handler(
    Extension(auth_cache): Extension<LoginAuthCache>,
    Extension(security): Extension<RuntimeSecurityConfig>,
    peer: Option<Extension<ConnectInfo<SocketAddr>>>,
    headers: HeaderMap,
) -> Json<serde_json::Value> {
    let creds = auth_cache.snapshot();
    let login_authenticated = if let Some(creds) = creds.as_ref() {
        if let Some(token) = extract_jwt_cookie(&headers) {
            jwt::verify_jwt(&token, &creds.jwt_secret).is_ok()
        } else {
            false
        }
    } else {
        false
    };
    let trusted_peer = security.is_trusted_peer(peer.map(|Extension(ConnectInfo(peer))| peer));

    Json(serde_json::json!({
        "enabled": creds.is_some(),
        "authenticated": login_authenticated || trusted_peer,
        "setupRequired": creds.is_none() && !trusted_peer,
    }))
}
