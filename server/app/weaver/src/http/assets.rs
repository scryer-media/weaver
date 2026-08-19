use std::sync::Arc;

use axum::extract::{ConnectInfo, Extension};
use axum::http::{HeaderMap, HeaderValue, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use rust_embed::Embed;
use std::net::SocketAddr;

use weaver_server_core::auth as jwt;
use weaver_server_core::auth::LoginAuthCache;
use weaver_server_core::security::RuntimeSecurityConfig;

#[derive(Embed)]
#[folder = "../../../apps/weaver-web/dist/"]
struct FrontendAssets;

#[derive(Clone)]
pub(super) struct BaseUrl(pub(super) Arc<String>);

/// Rewrite `index.html` to inject the optional base URL.
///
/// When `base_url` is non-empty (e.g. "/weaver"):
/// 1. Replaces `<base href="/">` with `<base href="/weaver/">`
/// 2. Injects `window.__WEAVER_BASE__` so the frontend knows its prefix
fn rewrite_index_html(raw: &[u8], base_url: &str) -> Vec<u8> {
    let html = String::from_utf8_lossy(raw);
    let html = if base_url.is_empty() {
        html.into_owned()
    } else {
        let html = html.replace(
            "<base href=\"/\" />",
            &format!("<base href=\"{base_url}/\" />"),
        );
        html.replace(
            "</head>",
            &format!(
                "<script>window.__WEAVER_BASE__={}</script>\n  </head>",
                serde_json::to_string(base_url).unwrap_or_default()
            ),
        )
    };
    html.into_bytes()
}

fn accepts_gzip(headers: &HeaderMap) -> bool {
    headers
        .get(header::ACCEPT_ENCODING)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.contains("gzip"))
}

pub(super) async fn static_handler(
    uri: Uri,
    headers: HeaderMap,
    Extension(BaseUrl(base_url)): Extension<BaseUrl>,
    Extension(super::SessionToken(session_token)): Extension<super::SessionToken>,
    Extension(auth_cache): Extension<LoginAuthCache>,
    Extension(security): Extension<RuntimeSecurityConfig>,
    peer: Option<Extension<ConnectInfo<SocketAddr>>>,
) -> impl IntoResponse {
    let path = uri.path().trim_start_matches('/');

    // Static assets remain public so that both login and setup-required pages
    // can load. The SPA entry itself is always handled below, including the
    // exact /index.html path.
    if !path.is_empty()
        && path != "index.html"
        && let Some(file) = FrontendAssets::get(path)
    {
        let mime = mime_guess::from_path(path).first_or_octet_stream();

        // Hashed assets (Vite output in assets/) are immutable - cache for 1 year.
        // Everything else (sw.js, manifest, etc.) gets no-cache.
        let cache_control = if path.starts_with("assets/") {
            "public, max-age=31536000, immutable"
        } else {
            "no-cache"
        };

        // Serve pre-compressed .gz variant if client accepts gzip.
        if accepts_gzip(&headers) {
            let gz_path = format!("{path}.gz");
            if let Some(gz_file) = FrontendAssets::get(&gz_path) {
                return (
                    StatusCode::OK,
                    [
                        (header::CONTENT_TYPE, mime.as_ref().to_string()),
                        (header::CONTENT_ENCODING, "gzip".to_string()),
                        (header::CACHE_CONTROL, cache_control.to_string()),
                    ],
                    gz_file.data,
                )
                    .into_response();
            }
        }

        return (
            StatusCode::OK,
            [
                (header::CONTENT_TYPE, mime.as_ref().to_string()),
                (header::CACHE_CONTROL, cache_control.to_string()),
            ],
            file.data,
        )
            .into_response();
    }

    let peer = peer.map(|Extension(ConnectInfo(peer))| peer);
    let cached_auth = auth_cache.snapshot();
    // Trusted-network sessions only apply while login protection is disabled.
    // When login is enabled, returning the SPA shell would make it repeatedly
    // receive GraphQL 401s instead of serving the sign-in page.
    let trusted_peer = cached_auth.is_none() && security.is_trusted_peer(peer);
    let has_valid_jwt = cached_auth.as_ref().is_some_and(|auth| {
        super::auth::extract_jwt_cookie(&headers)
            .is_some_and(|token| jwt::verify_jwt(&token, &auth.jwt_secret).is_ok())
    });
    // A fresh install — no credentials stored — serves the SPA so the browser
    // can run the first-run wizard. That is the whole point of the loopback
    // default: the machine's own browser is the operator, and setup happens in
    // the UI like every peer product, not in environment variables. The wizard
    // endpoint itself enforces loopback-or-trusted, so a remote visitor who
    // somehow reaches a pre-setup instance sees the wizard but cannot submit
    // it. Once credentials exist, an unauthenticated browser gets the login
    // page exactly as before.
    let setup_pending = cached_auth.is_none() && !trusted_peer;
    if !trusted_peer && !has_valid_jwt && !setup_pending {
        return login_page_response();
    }

    if let Some(index) = FrontendAssets::get("index.html") {
        let mime = mime_guess::from_path("index.html").first_or_octet_stream();
        let body = rewrite_index_html(&index.data, &base_url);
        let mut response = (
            StatusCode::OK,
            [(header::CONTENT_TYPE, mime.as_ref().to_string())],
            body,
        )
            .into_response();
        if trusted_peer {
            let cookie = super::auth::session_cookie_value(&session_token, &security);
            if let Ok(value) = HeaderValue::from_str(&cookie) {
                response.headers_mut().append(header::SET_COOKIE, value);
            }
        }
        response
    } else {
        StatusCode::NOT_FOUND.into_response()
    }
}

const LOGIN_PAGE_HTML: &str = r#"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Weaver - Login</title>
<style>
  *{box-sizing:border-box;margin:0;padding:0}
  body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;
    background:#0a0e1a;color:#e2e8f0;display:flex;align-items:center;
    justify-content:center;min-height:100vh}
  .card{background:#111827;border:1px solid rgba(255,255,255,.08);
    border-radius:20px;padding:40px;width:100%;max-width:380px;
    box-shadow:0 20px 60px rgba(0,0,0,.4)}
  h1{font-size:1.5rem;font-weight:600;margin-bottom:8px;letter-spacing:-.02em}
  .subtitle{font-size:.8rem;color:#64748b;text-transform:uppercase;
    letter-spacing:.2em;margin-bottom:32px}
  label{display:block;font-size:.85rem;color:#94a3b8;margin-bottom:6px}
  input{width:100%;padding:10px 14px;border:1px solid rgba(255,255,255,.1);
    border-radius:10px;background:#0f172a;color:#e2e8f0;font-size:.95rem;
    margin-bottom:16px;outline:none;transition:border .2s}
  input:focus{border-color:#6366f1}
  button{width:100%;padding:11px;border:none;border-radius:10px;
    background:#6366f1;color:#fff;font-size:.95rem;font-weight:500;
    cursor:pointer;transition:background .2s}
  button:hover{background:#4f46e5}
  button:disabled{opacity:.5;cursor:not-allowed}
  .error{color:#f87171;font-size:.85rem;margin-bottom:12px;display:none}
  .forgot{margin-top:16px;text-align:center}
  .forgot a{color:#6366f1;font-size:.85rem;text-decoration:none;cursor:pointer}
  .forgot a:hover{text-decoration:underline}
  .reset-help{display:none;margin-top:12px;padding:12px;border-radius:10px;
    background:#0f172a;border:1px solid rgba(255,255,255,.08);font-size:.8rem;
    color:#94a3b8;line-height:1.5}
  .reset-help code{background:#1e293b;padding:2px 6px;border-radius:4px;
    color:#e2e8f0;font-size:.8rem}
</style>
</head>
<body>
<div class="card">
  <h1>Weaver</h1>
  <div class="subtitle">Sign In</div>
  <form id="form">
    <label for="username">Username</label>
    <input id="username" name="username" type="text" autocomplete="username" required autofocus/>
    <label for="password">Password</label>
    <input id="password" name="password" type="password" autocomplete="current-password" required/>
    <div class="error" id="error"></div>
    <button type="submit" id="btn">Sign In</button>
  </form>
  <div class="forgot">
    <a id="forgot-link">Forgot password?</a>
    <div class="reset-help" id="reset-help">
      Stop Weaver, then restart with reset plus bootstrap credentials. Prefer a password file.<br/><br/>
      <strong>Docker:</strong><br/>
      <code>docker run -e WEAVER_RESET_LOGIN=1 -e WEAVER_BOOTSTRAP_LOGIN_USERNAME=admin -e WEAVER_BOOTSTRAP_LOGIN_PASSWORD_FILE=/run/secrets/weaver-login -v /host/password:/run/secrets/weaver-login:ro ...</code><br/><br/>
      <strong>Bare metal:</strong><br/>
      <code>WEAVER_RESET_LOGIN=1 WEAVER_BOOTSTRAP_LOGIN_USERNAME=admin WEAVER_BOOTSTRAP_LOGIN_PASSWORD_FILE=/path/to/password weaver serve</code><br/><br/>
      Alternatively, configure explicit <code>WEAVER_TRUSTED_CIDRS</code> for loginless full-administrator browser access.
    </div>
  </div>
</div>
<script>
const form=document.getElementById("form"),
  err=document.getElementById("error"),
  btn=document.getElementById("btn");
form.addEventListener("submit",async e=>{
  e.preventDefault();
  err.style.display="none";
  btn.disabled=true;
  btn.textContent="Signing in\u2026";
  try{
    const r=await fetch("/api/login",{
      method:"POST",
      headers:{"Content-Type":"application/json"},
      body:JSON.stringify({
        username:form.username.value,
        password:form.password.value
      })
    });
    if(r.ok){window.location.href="/";return}
    const d=await r.json().catch(()=>({}));
    err.textContent=d.error||"Login failed";
    err.style.display="block";
  }catch{
    err.textContent="Connection error";
    err.style.display="block";
  }
  btn.disabled=false;
  btn.textContent="Sign In";
});
document.getElementById("forgot-link").addEventListener("click",()=>{
  const el=document.getElementById("reset-help");
  el.style.display=el.style.display==="block"?"none":"block";
});
</script>
</body>
</html>"#;

fn login_page_response() -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/html; charset=utf-8".to_string())],
        LOGIN_PAGE_HTML,
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn entry_response(
        path: &'static str,
        security: RuntimeSecurityConfig,
        peer: Option<SocketAddr>,
    ) -> Response {
        static_handler(
            Uri::from_static(path),
            HeaderMap::new(),
            Extension(BaseUrl(Arc::new(String::new()))),
            Extension(super::super::SessionToken(Arc::new(
                "browser-token".to_string(),
            ))),
            Extension(LoginAuthCache::default()),
            Extension(security),
            peer.map(|peer| Extension(ConnectInfo(peer))),
        )
        .await
        .into_response()
    }

    #[tokio::test]
    async fn untrusted_entry_without_login_serves_the_setup_wizard_spa() {
        // A fresh install serves the SPA so the browser can run the first-run
        // wizard — the env-instructions 503 is gone deliberately. No session
        // cookie: the peer is not trusted, and the wizard endpoint does its
        // own loopback-or-trusted enforcement on submit.
        let response = entry_response("/", RuntimeSecurityConfig::default(), None).await;

        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().get(header::SET_COOKIE).is_none());
    }

    #[tokio::test]
    async fn trusted_root_and_index_issue_peer_bound_browser_cookie() {
        let security = {
            let security = weaver_server_core::security::RuntimeSecurityConfig::default();
            security.set_trusted_cidrs(vec!["127.0.0.0/8".parse().unwrap()]);
            security
        };
        let peer = "127.0.0.1:49152".parse().unwrap();

        for path in ["/", "/index.html"] {
            let response = entry_response(path, security.clone(), Some(peer)).await;
            assert_eq!(response.status(), StatusCode::OK, "{path}");
            let cookie = response
                .headers()
                .get(header::SET_COOKIE)
                .and_then(|value| value.to_str().ok())
                .unwrap();
            assert!(cookie.starts_with("weaver_session=browser-token;"));
            assert!(cookie.contains("HttpOnly"));
            assert!(cookie.contains("SameSite=Strict"));
        }
    }
}
