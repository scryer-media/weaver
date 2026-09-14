//! A holding page while a database upgrade runs.
//!
//! Migrations finish before the real server binds, so a long upgrade used to
//! look like a Weaver that would not start: the browser got "connection
//! refused" and nothing said why. This watches for an upgrade and, only once
//! one is actually running, answers on Weaver's own address with the turning W
//! and how far the upgrade has got. The page polls, and reloads into Weaver
//! when something other than this page answers.
//!
//! Nothing here is authenticated because nothing here is private: the page
//! shows a count of migrations. Every non-page request is refused with 503, so
//! an integration retries rather than mistaking the holding page for Weaver.

use std::borrow::Cow;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;
use tracing::{info, warn};

use weaver_server_core::schema_upgrade::{self, SchemaUpgrade};
use weaver_server_core::security::{
    DEFAULT_HTTP_BIND_ADDRESS, ENV_HTTP_BIND_ADDRESS, SETTING_HTTP_BIND_ADDRESS,
    resolve_bind_address,
};

/// Where the page's own requests go, under the base URL. Namespaced so it
/// cannot collide with a route of the real server.
const SPLASH_PATH: &str = "/__weaver/upgrade";
/// How long a stopping page may take to close its connections before it is
/// cut off: the real server is waiting for the port.
const STOP_GRACE: Duration = Duration::from_secs(2);
/// A stored address that cannot be read quickly is not worth delaying the
/// page for; it falls back the same way an unreadable one does.
const PEEK_TIMEOUT: Duration = Duration::from_secs(2);

/// The watcher, and the page once it is up. Stop it before the real server
/// binds.
pub(crate) struct UpgradeSplash {
    stop: oneshot::Sender<()>,
    task: JoinHandle<()>,
}

impl UpgradeSplash {
    /// Watch this process's schema upgrade and serve the page on `port` while
    /// one runs.
    pub(crate) fn watch(config_path: PathBuf, port: u16, base_url: &str) -> Self {
        let (stop, mut stopped) = oneshot::channel();
        let base_url = normalize_base_url(base_url);
        let task = tokio::spawn(async move {
            let mut progress = schema_upgrade::subscribe();
            loop {
                if matches!(*progress.borrow_and_update(), SchemaUpgrade::Running { .. }) {
                    break;
                }
                tokio::select! {
                    _ = &mut stopped => return,
                    changed = progress.changed() => {
                        if changed.is_err() {
                            return;
                        }
                    }
                }
            }
            let Some(listener) = bind(&config_path, port).await else {
                return;
            };
            if let Ok(addr) = listener.local_addr() {
                info!(%addr, "serving the upgrade page while the database is upgraded");
            }
            let app = router(SplashState::new(progress, base_url));
            let served = axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = stopped.await;
                })
                .await;
            if let Err(error) = served {
                warn!(%error, "upgrade page stopped unexpectedly");
            }
        });
        Self { stop, task }
    }

    /// Take the page down and release the port.
    pub(crate) async fn stop(self) {
        let Self { stop, mut task } = self;
        let _ = stop.send(());
        if tokio::time::timeout(STOP_GRACE, &mut task).await.is_err() {
            task.abort();
            let _ = task.await;
        }
    }
}

/// The address the real server will try, with the same loopback fallback.
async fn bind(config_path: &std::path::Path, port: u16) -> Option<tokio::net::TcpListener> {
    let env_value = std::env::var(ENV_HTTP_BIND_ADDRESS).ok();
    let stored = if env_value
        .as_deref()
        .is_some_and(|value| !value.trim().is_empty())
    {
        None
    } else {
        tokio::time::timeout(
            PEEK_TIMEOUT,
            weaver_server_core::persistence::setup::peek_setting(
                config_path,
                SETTING_HTTP_BIND_ADDRESS,
            ),
        )
        .await
        .ok()
        .flatten()
    };
    let address = resolve_bind_address(env_value.as_deref(), stored.as_deref())
        .map(|(address, _)| address)
        .unwrap_or(DEFAULT_HTTP_BIND_ADDRESS);
    bind_with_fallback(address, port).await
}

async fn bind_with_fallback(address: IpAddr, port: u16) -> Option<tokio::net::TcpListener> {
    match tokio::net::TcpListener::bind(SocketAddr::new(address, port)).await {
        Ok(listener) => Some(listener),
        Err(_) if !address.is_loopback() => {
            tokio::net::TcpListener::bind(SocketAddr::new(DEFAULT_HTTP_BIND_ADDRESS, port))
                .await
                .ok()
        }
        // The port is taken. The real server will say so; the page is not
        // worth a second report.
        Err(_) => None,
    }
}

fn normalize_base_url(base_url: &str) -> String {
    let trimmed = base_url.trim_matches('/');
    if trimmed.is_empty() {
        String::new()
    } else {
        format!("/{trimmed}")
    }
}

#[derive(Clone)]
struct SplashState {
    progress: watch::Receiver<SchemaUpgrade>,
    base_url: Arc<str>,
    page: Arc<str>,
    mark: Option<Cow<'static, [u8]>>,
    mark_still: Option<Cow<'static, [u8]>>,
    title_font: Option<Cow<'static, [u8]>>,
    ui_font: Option<Cow<'static, [u8]>>,
}

impl SplashState {
    fn new(progress: watch::Receiver<SchemaUpgrade>, base_url: String) -> Self {
        Self {
            progress,
            page: render_page(&base_url).into(),
            base_url: base_url.into(),
            mark: super::assets::built_asset("weaver-loading", "webp"),
            mark_still: super::assets::built_asset("weaver-loading-still", "webp"),
            title_font: super::assets::built_asset("sora-latin-wght-normal", "woff2"),
            ui_font: super::assets::built_asset("fira-code-latin-wght-normal", "woff2"),
        }
    }
}

fn router(state: SplashState) -> Router {
    Router::new().fallback(handle).with_state(state)
}

async fn handle(
    State(state): State<SplashState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
) -> Response {
    let path = uri
        .path()
        .strip_prefix(&*state.base_url)
        .filter(|rest| rest.is_empty() || rest.starts_with('/'))
        .unwrap_or(uri.path());
    let mut response = match (method == Method::GET || method == Method::HEAD, path) {
        (true, "/healthz") => (StatusCode::OK, "ok").into_response(),
        (true, path) if path.starts_with(SPLASH_PATH) => {
            splash_resource(&state, &path[SPLASH_PATH.len()..])
        }
        (true, _) if accepts_html(&headers) => {
            unavailable("text/html; charset=utf-8", state.page.as_bytes().to_vec())
        }
        _ => unavailable(
            "application/json",
            br#"{"error":"Weaver is upgrading its database"}"#.to_vec(),
        ),
    };
    let headers = response.headers_mut();
    for (name, value) in [
        (header::CACHE_CONTROL, "no-store"),
        (header::CONTENT_SECURITY_POLICY, "frame-ancestors 'none'"),
        (header::X_FRAME_OPTIONS, "DENY"),
        (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
        (header::REFERRER_POLICY, "same-origin"),
    ] {
        headers.insert(name, HeaderValue::from_static(value));
    }
    response
}

fn splash_resource(state: &SplashState, resource: &str) -> Response {
    let asset = |bytes: &Option<Cow<'static, [u8]>>, mime: &'static str| match bytes {
        Some(bytes) => ([(header::CONTENT_TYPE, mime)], bytes.to_vec()).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    };
    match resource {
        "/status" => {
            let body = match *state.progress.borrow() {
                SchemaUpgrade::Running { applied, total } => serde_json::json!({
                    "state": "upgrading",
                    "applied": applied,
                    "total": total,
                }),
                // The migrations are in and the rest of startup is running.
                SchemaUpgrade::Idle => serde_json::json!({ "state": "starting" }),
            };
            axum::Json(body).into_response()
        }
        "/mark.webp" => asset(&state.mark, "image/webp"),
        "/mark-still.webp" => asset(&state.mark_still, "image/webp"),
        "/title.woff2" => asset(&state.title_font, "font/woff2"),
        "/ui.woff2" => asset(&state.ui_font, "font/woff2"),
        _ => StatusCode::NOT_FOUND.into_response(),
    }
}

fn unavailable(content_type: &'static str, body: Vec<u8>) -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        [
            (header::CONTENT_TYPE, content_type),
            (header::RETRY_AFTER, "5"),
        ],
        body,
    )
        .into_response()
}

fn accepts_html(headers: &HeaderMap) -> bool {
    headers
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|accept| accept.contains("text/html"))
}

fn render_page(base_url: &str) -> String {
    let root = format!("{base_url}{SPLASH_PATH}");
    let attribute_root = root
        .replace('&', "&amp;")
        .replace('"', "&quot;")
        .replace('<', "&lt;")
        .replace('>', "&gt;");
    // `</` cannot end the script early once the slash is escaped.
    let script_root = serde_json::to_string(&root)
        .unwrap_or_else(|_| "\"\"".to_string())
        .replace("</", "<\\/");
    PAGE_TEMPLATE
        .replace("{{root}}", &attribute_root)
        .replace("{{script_root}}", &script_root)
}

const PAGE_TEMPLATE: &str = r##"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="color-scheme" content="dark">
<title>Upgrading Weaver</title>
<style>
@font-face { font-family: "Sora Variable"; src: url("{{root}}/title.woff2") format("woff2"); font-weight: 100 800; font-display: swap; }
@font-face { font-family: "Fira Code Variable"; src: url("{{root}}/ui.woff2") format("woff2"); font-weight: 300 700; font-display: swap; }
* { box-sizing: border-box; }
html, body { height: 100%; margin: 0; }
body {
  display: flex; align-items: center; justify-content: center; padding: 24px;
  background: #1a1b1e; color: #ecebe6;
  font: 13px/1.6 "Fira Code Variable", ui-monospace, "SFMono-Regular", Menlo, monospace;
}
main { display: flex; width: 100%; max-width: 420px; flex-direction: column; align-items: center; text-align: center; }
img { display: block; width: auto; height: 56px; user-select: none; }
h1 { margin: 28px 0 0; font: 600 22px/1.3 "Sora Variable", ui-sans-serif, system-ui, sans-serif; letter-spacing: -0.01em; }
p { margin: 8px 0 0; color: #a09d96; }
.meter { width: 100%; height: 2px; margin-top: 24px; background: #33343a; }
.meter span { display: block; width: 0; height: 100%; background: #3fb39c; transition: width 400ms ease; }
.note { margin-top: 20px; font-size: 12px; color: #84817a; }
</style>
</head>
<body>
<main>
  <picture>
    <source media="(prefers-reduced-motion: reduce)" srcset="{{root}}/mark-still.webp">
    <img src="{{root}}/mark.webp" width="304" height="209" alt="" aria-hidden="true" draggable="false">
  </picture>
  <h1>Upgrading the database</h1>
  <p id="progress" role="status" aria-live="polite">Applying changes</p>
  <div class="meter" aria-hidden="true"><span id="meter"></span></div>
  <p class="note">Leave Weaver running. This page opens it when the upgrade is done.</p>
</main>
<script>
(function () {
  var root = {{script_root}};
  var progress = document.getElementById("progress");
  var meter = document.getElementById("meter");
  function starting() {
    progress.textContent = "Starting Weaver";
    meter.style.width = "100%";
  }
  function poll() {
    fetch(root + "/status", { cache: "no-store", credentials: "same-origin" })
      .then(function (response) {
        return response.ok ? response.json().catch(function () { return null; }) : null;
      })
      .then(function (body) {
        if (body && body.state === "upgrading") {
          progress.textContent = "Applying change " + Math.min(body.applied + 1, body.total) + " of " + body.total;
          meter.style.width = (body.total ? (body.applied / body.total) * 100 : 0) + "%";
        } else if (body && body.state === "starting") {
          starting();
        } else {
          // Something other than this page answered: Weaver is up.
          window.location.reload();
          return;
        }
        window.setTimeout(poll, 1000);
      }, function () {
        // Nothing is listening between this page and Weaver taking over.
        starting();
        window.setTimeout(poll, 1000);
      });
  }
  poll();
})();
</script>
</body>
</html>
"##;

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    fn state(upgrade: SchemaUpgrade, base_url: &str) -> (watch::Sender<SchemaUpgrade>, Router) {
        let (sender, receiver) = watch::channel(upgrade);
        let app = router(SplashState::new(receiver, normalize_base_url(base_url)));
        (sender, app)
    }

    async fn get(app: &Router, path: &str, accept: &str) -> (StatusCode, HeaderMap, String) {
        let response = app
            .clone()
            .oneshot(
                Request::get(path)
                    .header(header::ACCEPT, accept)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let headers = response.headers().clone();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        (
            status,
            headers,
            String::from_utf8_lossy(&bytes).into_owned(),
        )
    }

    #[tokio::test]
    async fn a_browser_gets_the_page_and_everything_else_is_told_to_retry() {
        let (_sender, app) = state(
            SchemaUpgrade::Running {
                applied: 1,
                total: 4,
            },
            "/weaver/",
        );

        let (status, headers, body) = get(&app, "/weaver/queue", "text/html").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(headers[header::RETRY_AFTER], "5");
        assert_eq!(headers[header::CACHE_CONTROL], "no-store");
        assert_eq!(headers[header::X_FRAME_OPTIONS], "DENY");
        assert!(body.contains("Upgrading the database"));
        assert!(body.contains(r#"src="/weaver/__weaver/upgrade/mark.webp""#));

        let (status, headers, body) = get(&app, "/weaver/graphql", "application/json").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(headers[header::CONTENT_TYPE], "application/json");
        assert!(body.contains("upgrading"));
    }

    #[tokio::test]
    async fn status_follows_the_upgrade_into_startup() {
        let (sender, app) = state(
            SchemaUpgrade::Running {
                applied: 1,
                total: 4,
            },
            "",
        );

        let (status, _, body) = get(&app, "/__weaver/upgrade/status", "*/*").await;
        assert_eq!(status, StatusCode::OK);
        let body: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(
            body,
            serde_json::json!({ "state": "upgrading", "applied": 1, "total": 4 })
        );

        sender.send_replace(SchemaUpgrade::Idle);
        let (_, _, body) = get(&app, "/__weaver/upgrade/status", "*/*").await;
        assert_eq!(body, r#"{"state":"starting"}"#);
    }

    #[tokio::test]
    async fn liveness_holds_while_readiness_waits() {
        let (_sender, app) = state(
            SchemaUpgrade::Running {
                applied: 0,
                total: 1,
            },
            "/weaver",
        );
        assert_eq!(get(&app, "/weaver/healthz", "*/*").await.0, StatusCode::OK);
        assert_eq!(get(&app, "/healthz", "*/*").await.0, StatusCode::OK);
        assert_eq!(
            get(&app, "/weaver/readyz", "*/*").await.0,
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[test]
    fn a_hostile_base_url_cannot_break_out_of_the_page() {
        let page = render_page("/a\"></script><script>x()</script>");
        assert!(!page.contains("</script><script>x()"));
        assert!(page.contains("&quot;&gt;"));
    }

    #[tokio::test]
    async fn stopping_releases_the_port_for_the_real_server() {
        let listener = bind_with_fallback(DEFAULT_HTTP_BIND_ADDRESS, 0)
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();
        let (stop, stopped) = oneshot::channel::<()>();
        let (_sender, receiver) = watch::channel(SchemaUpgrade::Running {
            applied: 0,
            total: 1,
        });
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, router(SplashState::new(receiver, String::new())))
                .with_graceful_shutdown(async move {
                    let _ = stopped.await;
                })
                .await;
        });
        UpgradeSplash { stop, task }.stop().await;
        tokio::net::TcpListener::bind(addr).await.unwrap();
    }
}
