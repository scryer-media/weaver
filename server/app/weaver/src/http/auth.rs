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

fn extract_bearer_token(headers: &HeaderMap) -> Option<String> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

/// Check if login auth is enabled and return the cached JWT secret if so.
pub(super) fn jwt_secret_if_auth_enabled(auth_cache: &LoginAuthCache) -> Option<[u8; 32]> {
    auth_cache.snapshot().map(|auth| auth.jwt_secret)
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

/// Resolve the caller scope and stable request identity from API key header, JWT cookie, or session token.
pub(super) async fn resolve_caller(
    db: &Database,
    auth_cache: &LoginAuthCache,
    api_key_cache: &ApiKeyCache,
    session_token: &str,
    headers: &HeaderMap,
) -> Result<ResolvedCaller, StatusCode> {
    let api_key_header = headers.get("x-api-key");
    let bearer_token = extract_bearer_token(headers);
    let presented_token = bearer_token
        .as_deref()
        .or_else(|| api_key_header.and_then(|value| value.to_str().ok()));

    // 1. Bearer token or API key header (session token or stored key).
    if let Some(raw_key) = presented_token {
        if raw_key == session_token {
            return Ok(ResolvedCaller {
                scope: CallerScope::Local,
                identity: CallerIdentity::Local(hash_api_key(raw_key)),
            });
        }
        let key_hash = hash_api_key(raw_key);
        if let Some(row) = lookup_api_key_auth(db, api_key_cache, key_hash).await? {
            queue_touch_api_key_last_used(db, row.id);
            return Ok(ResolvedCaller {
                scope: caller_scope_from_api_key_scope(&row.scope),
                identity: CallerIdentity::ApiKey(row.key_hash),
            });
        }
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

    // 3. No login auth enabled: accept the browser-only session cookie issued
    // by the SPA index response.
    if cached_auth.is_none()
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

/// Resolve the caller scope from API key header, JWT cookie, or session token.
pub(super) async fn resolve_scope(
    db: &Database,
    auth_cache: &LoginAuthCache,
    api_key_cache: &ApiKeyCache,
    session_token: &str,
    headers: &HeaderMap,
) -> Result<CallerScope, StatusCode> {
    Ok(
        resolve_caller(db, auth_cache, api_key_cache, session_token, headers)
            .await?
            .scope,
    )
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
    headers: HeaderMap,
) -> Json<serde_json::Value> {
    let creds = auth_cache.snapshot();
    let authenticated = if let Some(creds) = creds.as_ref() {
        if let Some(token) = extract_jwt_cookie(&headers) {
            jwt::verify_jwt(&token, &creds.jwt_secret).is_ok()
        } else {
            false
        }
    } else {
        true // auth not enabled -> everyone is "authenticated"
    };

    Json(serde_json::json!({
        "enabled": creds.is_some(),
        "authenticated": authenticated,
    }))
}
