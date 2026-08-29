use std::sync::Arc;

use axum::extract::{ConnectInfo, Extension};
use axum::http::{HeaderMap, HeaderValue, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use rust_embed::Embed;
use std::net::SocketAddr;

use weaver_server_core::auth as jwt;
use weaver_server_core::auth::LoginAuthCache;
use weaver_server_core::runtime::environment::DeploymentEnvironment;
use weaver_server_core::security::{RuntimeSecurityConfig, ip_is_loopback};

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
    // How this deployment is packaged decides which "you cannot finish setup
    // from here" page an untrusted visitor gets. Detected inline rather than
    // resolved at boot: this is the entry path — one page load, not one call
    // per asset — and the answer must not go stale against a marker the
    // operator adds to a running deployment's environment on the next start.
    let deployment =
        weaver_server_core::runtime::environment::detect_runtime_environment().deployment;
    entry_response(
        &headers,
        &base_url,
        &session_token,
        &auth_cache,
        &security,
        peer,
        deployment,
    )
}

/// The entry decision, split out from [`static_handler`] so the deployment
/// shape can be supplied rather than detected: a test that asserts the native
/// page would otherwise pass or fail depending on whether the suite itself is
/// running inside a container.
fn entry_response(
    headers: &HeaderMap,
    base_url: &str,
    session_token: &str,
    auth_cache: &LoginAuthCache,
    security: &RuntimeSecurityConfig,
    peer: Option<SocketAddr>,
    deployment: DeploymentEnvironment,
) -> Response {
    let cached_auth = auth_cache.snapshot();
    // Trusted-network sessions only apply while login protection is disabled.
    // When login is enabled, returning the SPA shell would make it repeatedly
    // receive GraphQL 401s instead of serving the sign-in page.
    let trusted_peer = cached_auth.is_none() && security.is_trusted_peer(peer);
    let has_valid_jwt = cached_auth.as_ref().is_some_and(|auth| {
        super::auth::extract_jwt_cookie(headers)
            .is_some_and(|token| jwt::verify_jwt(&token, &auth.jwt_secret).is_ok())
    });

    if cached_auth.is_some() {
        // Credentials exist: an unauthenticated browser gets the login page,
        // exactly as before.
        if !has_valid_jwt {
            return login_page_response();
        }
    } else if !trusted_peer {
        // No credentials and a peer outside the trust list. Loopback is
        // admitted to the wizard FIRST, before the configured check: the
        // machine's own browser must be able to run setup whenever no
        // credentials exist — including after WEAVER_RESET_LOGIN cleared them
        // on an already-configured install, which is exactly the lockout that
        // browser has to be able to repair. (Loopback is not in the trust
        // list on a fresh install — the list is what the wizard writes — so
        // `trusted_peer` alone would turn the operator away. This mirrors
        // `setup_handler`'s admission rule: the form goes to exactly the
        // browsers that could submit it.)
        if !peer.is_some_and(|peer| ip_is_loopback(peer.ip())) {
            // A browser the wizard endpoint would refuse must not be handed
            // the form on every visit forever. Three situations share this
            // shape; each gets the page that tells it the truth.
            if security.security_configured() {
                // The operator already answered — a no-login install
                // answering network-wide is the common case. Nothing is
                // pending; this browser is simply not admitted.
                return BROWSER_ACCESS_RESTRICTED_PAGE.response();
            }
            return if matches!(
                deployment,
                DeploymentEnvironment::Docker | DeploymentEnvironment::Container
            ) {
                // Inside a container namespace no outside browser is ever
                // loopback, so the wizard is unreachable by construction and
                // first-run setup belongs to the deployment.
                CONTAINER_SETUP_PAGE.response()
            } else {
                // Native: the wizard exists and works, just not from here.
                COMPLETE_SETUP_ON_MACHINE_PAGE.response()
            };
        }
        // Machine's own browser, no credentials: the first-run wizard. No
        // session cookie — loopback earns trust by finishing setup, not by
        // arriving.
    }

    if let Some(index) = FrontendAssets::get("index.html") {
        let mime = mime_guess::from_path("index.html").first_or_octet_stream();
        let body = rewrite_index_html(&index.data, base_url);
        let mut response = (
            StatusCode::OK,
            [(header::CONTENT_TYPE, mime.as_ref().to_string())],
            body,
        )
            .into_response();
        if trusted_peer {
            let cookie = super::auth::session_cookie_value(session_token, security);
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
    html_page_response(LOGIN_PAGE_HTML)
}

/// Shared styling for the three static pages an untrusted, credential-less
/// browser can land on. Kept as one constant rather than repeated per page so
/// they cannot drift apart from each other or from the login page's look.
const NOTICE_PAGE_STYLE: &str = r#"<style>
  *{box-sizing:border-box;margin:0;padding:0}
  body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;
    background:#0a0e1a;color:#e2e8f0;display:flex;align-items:center;
    justify-content:center;min-height:100vh;padding:24px}
  .card{background:#111827;border:1px solid rgba(255,255,255,.08);
    border-radius:20px;padding:40px;width:100%;max-width:560px;
    box-shadow:0 20px 60px rgba(0,0,0,.4)}
  h1{font-size:1.5rem;font-weight:600;margin-bottom:8px;letter-spacing:-.02em}
  .subtitle{font-size:.8rem;color:#64748b;text-transform:uppercase;
    letter-spacing:.2em;margin-bottom:32px}
  p{font-size:.9rem;color:#94a3b8;line-height:1.6;margin-bottom:16px}
  p:last-child{margin-bottom:0}
  h2{font-size:.95rem;font-weight:600;color:#e2e8f0;margin:24px 0 8px}
  code{display:block;background:#0f172a;border:1px solid rgba(255,255,255,.08);
    border-radius:10px;padding:12px 14px;color:#e2e8f0;font-size:.8rem;
    line-height:1.6;overflow-x:auto;white-space:pre;margin-bottom:8px}
  .inline-code{background:#1e293b;padding:2px 6px;border-radius:4px;
    color:#e2e8f0;font-family:ui-monospace,SFMono-Regular,Menlo,monospace}
</style>"#;

/// Case (b): a container install with no provisioning. The wizard cannot be
/// the answer here — inside a container namespace an outside browser arrives
/// from the bridge or the gateway, never loopback — so the page names the two
/// deployment-level ways in instead of showing a form nobody can submit.
const CONTAINER_SETUP_PAGE: NoticePage = NoticePage {
    title: "Set up Weaver",
    subtitle: "First Run",
    body: r#"  <p>
    Weaver has not been set up yet. Inside a container no browser counts as
    the machine itself, so first-run setup happens in the deployment rather
    than on this page. Add one of these to the container's environment.
  </p>
  <h2>Trusted networks</h2>
  <code>WEAVER_TRUSTED_CIDRS=192.168.0.0/16</code>
  <p>Browsers in these networks get full access without a login.</p>
  <h2>Bootstrap login</h2>
  <code>WEAVER_BOOTSTRAP_LOGIN_USERNAME=admin
WEAVER_BOOTSTRAP_LOGIN_PASSWORD_FILE=/run/secrets/weaver-login</code>
  <p>Then sign in normally.</p>
  <p>Restart the container with either one set and this page goes away.</p>"#,
};

/// Case (c): a native install reached from somewhere other than its own
/// machine. The wizard works — it just refuses this peer on submit — so the
/// page says where to run it rather than leaving a form that always 403s.
const COMPLETE_SETUP_ON_MACHINE_PAGE: NoticePage = NoticePage {
    title: "Set up Weaver",
    subtitle: "First Run",
    body: r#"  <p>
    Weaver has not been set up yet, and setup can only be completed from the
    machine it runs on.
  </p>
  <p>
    Open Weaver in a browser on that machine &mdash; or through an SSH
    port-forward to <span class="inline-code">127.0.0.1</span> &mdash; to run
    the first-run wizard.
  </p>"#,
};

/// Case (d): a configured no-login install answering network-wide. Nothing is
/// pending and nothing is broken — this browser is simply outside the policy
/// the operator chose — so the page says so and names where to change it.
const BROWSER_ACCESS_RESTRICTED_PAGE: NoticePage = NoticePage {
    title: "Weaver",
    subtitle: "Browser Access Restricted",
    body: r#"  <p>
    This Weaver allows browser access only from its own machine or its trusted
    networks. API clients with keys are unaffected.
  </p>
  <p>
    To allow more browsers, adjust Browser access in Settings &rarr; Security
    from a trusted machine.
  </p>"#,
};

/// One of the static pages an untrusted, credential-less browser can land on.
///
/// Assembled at response time rather than stored as three finished constants
/// only because [`NOTICE_PAGE_STYLE`] cannot be spliced into a `const` — these
/// are cold paths (one page load for a visitor who is being turned away), and
/// one shared style is worth more than three copies that drift.
struct NoticePage {
    title: &'static str,
    subtitle: &'static str,
    body: &'static str,
}

impl NoticePage {
    fn render(&self) -> String {
        let Self {
            title,
            subtitle,
            body,
        } = self;
        format!(
            r#"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>{title}</title>
{NOTICE_PAGE_STYLE}
</head>
<body>
<div class="card">
  <h1>{title}</h1>
  <div class="subtitle">{subtitle}</div>
{body}
</div>
</body>
</html>"#
        )
    }

    fn response(&self) -> Response {
        html_page_response(self.render())
    }
}

fn html_page_response(html: impl Into<axum::body::Body>) -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/html; charset=utf-8".to_string())],
        html.into(),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn entry_page(
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

    /// The entry decision with the deployment supplied instead of detected, so
    /// the native cases do not depend on whether the test suite itself happens
    /// to be running inside a container.
    fn untrusted_entry(
        security: &RuntimeSecurityConfig,
        deployment: DeploymentEnvironment,
    ) -> Response {
        super::entry_response(
            &HeaderMap::new(),
            "",
            "browser-token",
            &LoginAuthCache::default(),
            security,
            Some("192.168.1.20:49152".parse().unwrap()),
            deployment,
        )
    }

    async fn body_text(response: Response) -> String {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("entry page body is readable");
        String::from_utf8(bytes.to_vec()).expect("entry page body is UTF-8")
    }

    fn no_login_configured() -> RuntimeSecurityConfig {
        // What a completed no-login setup leaves behind: loopback trust, no
        // credentials, and a settled policy.
        let security = RuntimeSecurityConfig::default();
        security.apply_stored_trust(Some("no_login"), None);
        security
    }

    #[tokio::test]
    async fn a_fresh_install_still_serves_the_wizard_to_the_machines_own_browser() {
        // The trap in gating on `trusted_peer`: a fresh install trusts NOTHING
        // — the trust list is what the wizard writes — so loopback is not a
        // trusted peer, and turning it away here would break first run for
        // every native install while "fixing" the two remote loops.
        for deployment in [DeploymentEnvironment::Native, DeploymentEnvironment::Docker] {
            let security = RuntimeSecurityConfig::default();
            assert!(!security.is_trusted_peer(Some("127.0.0.1:49152".parse().unwrap())));

            let response = super::entry_response(
                &HeaderMap::new(),
                "",
                "browser-token",
                &LoginAuthCache::default(),
                &security,
                Some("127.0.0.1:49152".parse().unwrap()),
                deployment,
            );

            assert_eq!(response.status(), StatusCode::OK, "{deployment:?}");
            // The SPA, not a notice page — and no session cookie, because
            // loopback earns trust by finishing setup, not by arriving.
            assert!(
                response.headers().get(header::SET_COOKIE).is_none(),
                "{deployment:?}"
            );
            let body = body_text(response).await;
            assert!(
                !body.contains("<title>Set up Weaver</title>"),
                "the machine's own browser must get the wizard SPA: {body}"
            );
        }
    }

    #[tokio::test]
    async fn an_unconfigured_native_install_tells_an_outside_browser_where_setup_runs() {
        // Not the SPA: the wizard endpoint enforces loopback-or-trusted on
        // submit, so serving the form here is a page that can only ever 403.
        let response = untrusted_entry(
            &RuntimeSecurityConfig::default(),
            DeploymentEnvironment::Native,
        );

        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().get(header::SET_COOKIE).is_none());
        let body = body_text(response).await;
        assert!(body.contains("<title>Set up Weaver</title>"), "{body}");
        assert!(body.contains("machine it runs on"), "{body}");
        assert!(body.contains("first-run wizard"), "{body}");
    }

    #[tokio::test]
    async fn an_unconfigured_container_install_names_the_provisioning_variables() {
        // Loop 2: inside a container namespace no outside browser is ever
        // loopback, so the wizard is uncompletable by construction and the
        // page has to point at the deployment instead.
        for deployment in [
            DeploymentEnvironment::Docker,
            DeploymentEnvironment::Container,
        ] {
            let response = untrusted_entry(&RuntimeSecurityConfig::default(), deployment);

            assert_eq!(response.status(), StatusCode::OK, "{deployment:?}");
            let body = body_text(response).await;
            assert!(body.contains("<title>Set up Weaver</title>"), "{body}");
            assert!(body.contains("WEAVER_TRUSTED_CIDRS"), "{body}");
            assert!(body.contains("WEAVER_BOOTSTRAP_LOGIN_USERNAME"), "{body}");
            assert!(
                body.contains("WEAVER_BOOTSTRAP_LOGIN_PASSWORD_FILE"),
                "{body}"
            );
        }
    }

    #[tokio::test]
    async fn a_configured_no_login_install_turns_outside_browsers_away_without_a_wizard() {
        // Loop 1: this instance has no credentials and does not trust this
        // peer — the same shape as a fresh install — but the operator already
        // answered, so the wizard must never come back.
        for deployment in [DeploymentEnvironment::Native, DeploymentEnvironment::Docker] {
            let response = untrusted_entry(&no_login_configured(), deployment);

            assert_eq!(response.status(), StatusCode::OK, "{deployment:?}");
            let body = body_text(response).await;
            assert!(body.contains("<title>Weaver</title>"), "{body}");
            assert!(body.contains("only from its own machine"), "{body}");
            assert!(
                !body.contains("WEAVER_TRUSTED_CIDRS") && !body.contains("first-run wizard"),
                "a configured install must not describe setup: {body}"
            );
        }
    }

    #[tokio::test]
    async fn a_credential_reset_reopens_the_wizard_for_the_machines_own_browser() {
        // WEAVER_RESET_LOGIN clears the credentials but leaves the stored
        // access mode, so the instance reads as configured while trusting
        // nothing. The machine's own browser must still get the wizard —
        // it is the only thing that can repair this state from the UI — so
        // the configured check must never outrank loopback admission.
        let security = RuntimeSecurityConfig::default();
        security.apply_stored_trust(Some("login_required"), None);
        assert!(security.security_configured());
        assert!(!security.is_trusted_peer(Some("127.0.0.1:49152".parse().unwrap())));

        let response = super::entry_response(
            &HeaderMap::new(),
            "",
            "browser-token",
            &LoginAuthCache::default(),
            &security,
            Some("127.0.0.1:49152".parse().unwrap()),
            DeploymentEnvironment::Native,
        );

        assert_eq!(response.status(), StatusCode::OK);
        let body = body_text(response).await;
        assert!(
            !body.contains("Browser Access Restricted")
                && !body.contains("<title>Set up Weaver</title>"),
            "the reset operator's own browser must get the wizard SPA, not a notice page: {body}"
        );
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
            let response = entry_page(path, security.clone(), Some(peer)).await;
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
