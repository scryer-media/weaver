use std::sync::Arc;

use axum::extract::Extension;
use axum::http::{HeaderMap, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use rust_embed::Embed;

use weaver_server_core::auth as jwt;
use weaver_server_core::auth::LoginAuthCache;

#[derive(Embed)]
#[folder = "../../../apps/weaver-web/dist/"]
struct FrontendAssets;

#[derive(Clone)]
pub(super) struct BaseUrl(pub(super) Arc<String>);

/// Rewrite `index.html` to inject the session token and (optionally) base URL.
///
/// Always injects `window.__WEAVER_SESSION__` so the frontend can authenticate.
/// When `base_url` is non-empty (e.g. "/weaver"), also:
/// 1. Replaces `<base href="/">` with `<base href="/weaver/">`
/// 2. Injects `window.__WEAVER_BASE__` so the frontend knows its prefix
fn rewrite_index_html(raw: &[u8], base_url: &str, session_token: &str) -> Vec<u8> {
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
    let html = html.replace(
        "</head>",
        &format!(
            "<script>window.__WEAVER_SESSION__={}</script>\n  </head>",
            serde_json::to_string(session_token).unwrap_or_default()
        ),
    );
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
) -> impl IntoResponse {
    let path = uri.path().trim_start_matches('/');

    // Try the exact path first, then fall back to index.html for SPA routing.
    if let Some(file) = FrontendAssets::get(path) {
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

        (
            StatusCode::OK,
            [
                (header::CONTENT_TYPE, mime.as_ref().to_string()),
                (header::CACHE_CONTROL, cache_control.to_string()),
            ],
            file.data,
        )
            .into_response()
    } else {
        // SPA fallback: serve index.html (or login page if auth is enabled).
        // When auth is enabled and the user doesn't have a valid JWT cookie,
        // serve the standalone login page instead of the React app.
        if let Some(secret) = super::auth::jwt_secret_if_auth_enabled(&auth_cache) {
            let has_valid_jwt = super::auth::extract_jwt_cookie(&headers)
                .is_some_and(|token| jwt::verify_jwt(&token, &secret).is_ok());
            if !has_valid_jwt {
                return login_page_response();
            }
        }

        if let Some(index) = FrontendAssets::get("index.html") {
            let mime = mime_guess::from_path("index.html").first_or_octet_stream();
            let body = rewrite_index_html(&index.data, &base_url, &session_token);
            (
                StatusCode::OK,
                [(header::CONTENT_TYPE, mime.as_ref().to_string())],
                body,
            )
                .into_response()
        } else {
            StatusCode::NOT_FOUND.into_response()
        }
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
      Stop weaver, then restart with:<br/><br/>
      <code>WEAVER_RESET_LOGIN=1</code><br/><br/>
      This disables login protection on startup. You can then set a new password in Settings &rarr; Security.
      <br/><br/>
      <strong>Docker:</strong><br/>
      <code>docker run -e WEAVER_RESET_LOGIN=1 ...</code><br/><br/>
      <strong>Bare metal:</strong><br/>
      <code>WEAVER_RESET_LOGIN=1 weaver serve</code>
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
