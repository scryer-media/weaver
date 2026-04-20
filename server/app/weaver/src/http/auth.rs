use axum::Json;
use axum::extract::Extension;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde::Deserialize;

use weaver_server_core::Database;
use weaver_server_core::auth::{self as jwt, JWT_TTL_SECS};
use weaver_server_core::auth::{
    ApiKeyAuthRow, ApiKeyCache, CachedLoginAuth, CallerScope, LoginAuthCache, hash_api_key,
    hash_password, needs_rehash, verify_password,
};

/// Extract the `weaver_jwt` cookie value from request headers.
pub(super) fn extract_jwt_cookie(headers: &HeaderMap) -> Option<String> {
    headers
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(';'))
        .map(str::trim)
        .find_map(|cookie| {
            cookie
                .strip_prefix("weaver_jwt=")
                .map(|value| value.to_string())
        })
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

pub(super) fn queue_touch_api_key_last_used(db: &Database, id: i64) {
    let db_touch = db.clone();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;
    tokio::task::spawn_blocking(move || {
        let _ = db_touch.touch_api_key_last_used(id, now);
    });
}

pub(super) async fn refresh_auth_caches(
    db: &Database,
    auth_cache: &LoginAuthCache,
    api_key_cache: &ApiKeyCache,
) -> Result<(), StatusCode> {
    let db_clone = db.clone();
    let (credentials, api_keys) = tokio::task::spawn_blocking(move || {
        let credentials = db_clone.get_auth_credentials().map_err(|_| ())?;
        let api_keys = db_clone.list_api_key_auth_rows().map_err(|_| ())?;
        Ok::<_, ()>((credentials, api_keys))
    })
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    auth_cache.replace_credentials(credentials);
    api_key_cache.replace_rows(api_keys);
    Ok(())
}

/// Resolve the caller scope from API key header, JWT cookie, or session token.
pub(super) async fn resolve_scope(
    db: &Database,
    auth_cache: &LoginAuthCache,
    api_key_cache: &ApiKeyCache,
    session_token: &str,
    headers: &HeaderMap,
) -> Result<CallerScope, StatusCode> {
    let api_key_header = headers.get("x-api-key");
    let bearer_token = extract_bearer_token(headers);
    let presented_token = bearer_token
        .as_deref()
        .or_else(|| api_key_header.and_then(|value| value.to_str().ok()));

    // 1. Bearer token or API key header (session token or stored key).
    if let Some(raw_key) = presented_token {
        if raw_key == session_token {
            return Ok(CallerScope::Local);
        }
        let key_hash = hash_api_key(raw_key);
        if let Some(row) = lookup_api_key_auth(db, api_key_cache, key_hash).await? {
            queue_touch_api_key_last_used(db, row.id);
            return Ok(caller_scope_from_api_key_scope(&row.scope));
        }
    }

    // 2. JWT cookie (when login auth is enabled).
    let cached_auth = auth_cache.snapshot();
    if let Some(token) = extract_jwt_cookie(headers)
        && let Some(auth) = cached_auth.as_ref()
        && jwt::verify_jwt(&token, &auth.jwt_secret).is_ok()
    {
        return Ok(CallerScope::Admin);
    }

    // 3. No login auth enabled: require an explicit session token or API key.
    if cached_auth.is_none() {
        return Err(StatusCode::UNAUTHORIZED);
    }

    Err(StatusCode::UNAUTHORIZED)
}

#[derive(Deserialize)]
pub(super) struct LoginRequest {
    username: String,
    password: String,
}

pub(super) async fn login_handler(
    Extension(db): Extension<Database>,
    Extension(auth_cache): Extension<LoginAuthCache>,
    Json(body): Json<LoginRequest>,
) -> Response {
    let creds = match auth_cache.snapshot() {
        Some(creds) => creds,
        None => {
            return super::error_response(StatusCode::BAD_REQUEST, "login is not enabled");
        }
    };

    if body.username != creds.username {
        return super::error_response(StatusCode::UNAUTHORIZED, "invalid credentials");
    }

    let hash = creds.password_hash.clone();
    let password = body.password.clone();
    let valid = tokio::task::spawn_blocking(move || verify_password(&password, &hash))
        .await
        .unwrap_or(false);

    if !valid {
        return super::error_response(StatusCode::UNAUTHORIZED, "invalid credentials");
    }

    // Transparent rehash: upgrade legacy scrypt hashes to argon2id on successful login.
    let effective_auth = if needs_rehash(&creds.password_hash) {
        let password = body.password.clone();
        let db_rehash = db.clone();
        let username = creds.username.clone();
        if let Ok(new_hash) = tokio::task::spawn_blocking(move || {
            hash_password(&password).and_then(|hash| {
                db_rehash
                    .set_auth_credentials(&username, &hash)
                    .map_err(|error| format!("db update failed: {error}"))?;
                Ok(hash)
            })
        })
        .await
        .unwrap_or(Err("task failed".into()))
        {
            let auth = CachedLoginAuth::new(creds.username.clone(), new_hash);
            auth_cache.replace(Some(auth.clone()));
            auth
        } else {
            creds.clone()
        }
    } else {
        creds.clone()
    };

    let token = jwt::create_jwt(
        &effective_auth.username,
        &effective_auth.jwt_secret,
        JWT_TTL_SECS,
    );
    let cookie =
        format!("weaver_jwt={token}; Path=/; HttpOnly; SameSite=Strict; Max-Age={JWT_TTL_SECS}");

    (
        StatusCode::OK,
        [(header::SET_COOKIE, cookie)],
        Json(serde_json::json!({ "ok": true })),
    )
        .into_response()
}

pub(super) async fn logout_handler() -> Response {
    let cookie = "weaver_jwt=; Path=/; HttpOnly; SameSite=Strict; Max-Age=0";
    (
        StatusCode::OK,
        [(header::SET_COOKIE, cookie.to_string())],
        Json(serde_json::json!({ "ok": true })),
    )
        .into_response()
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
