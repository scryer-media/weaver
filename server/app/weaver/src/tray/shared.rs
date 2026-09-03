//! The parts of the Weaver desktop wrapper that are the same on every platform.
//!
//! Both wrappers supervise the same `weaver` server process, poll the same
//! readiness surface, and answer the same question about which links belong
//! inside the app window. Keeping those rules here is what keeps the
//! platform modules down to window and menu plumbing, and it is the only way
//! the two platforms can be relied on to behave identically.
#![allow(
    dead_code,
    reason = "the Windows and macOS wrappers each use a subset of this module, and neither is compiled on other platforms"
)]

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::thread;
use std::time::{Duration, Instant};

/// The port `weaver serve` is started on, and the port the wrapper expects the
/// UI to answer on. Users who want a different port run the server themselves;
/// the wrapper owns this one.
pub(crate) const DEFAULT_PORT: u16 = 9090;

/// How long a start or restart waits for the server to answer before the
/// wrapper reports the failure to the user.
pub(crate) const SERVER_READY_TIMEOUT: Duration = Duration::from_secs(30);

/// How long a Unix stop waits after `SIGTERM` before escalating to `SIGKILL`.
/// The server's own graceful teardown is what needs the time here; a process
/// that ignores the signal must not be able to strand the wrapper.
#[cfg(unix)]
const GRACEFUL_STOP_TIMEOUT: Duration = Duration::from_secs(10);

/// The names the wrapper and the server are installed under. Both binaries
/// ship in the same directory, which is what lets the wrapper find the server
/// without a configured path.
#[cfg(windows)]
const SERVER_EXECUTABLE: &str = "weaver.exe";
#[cfg(windows)]
const WRAPPER_EXECUTABLE: &str = "weaver-tray.exe";
#[cfg(not(windows))]
const SERVER_EXECUTABLE: &str = "weaver";
#[cfg(not(windows))]
const WRAPPER_EXECUTABLE: &str = "weaver-tray";

/// The origin the app window is allowed to stay inside.
pub(crate) fn app_origin(port: u16) -> String {
    format!("http://127.0.0.1:{port}")
}

/// The document the app window opens.
pub(crate) fn app_url(port: u16) -> String {
    format!("http://127.0.0.1:{port}/")
}

/// Where the desktop wrapper keeps the server's configuration, database and
/// logs. This is deliberately not the portable layout the tarball uses: a
/// wrapper install writes to per-user application data, and a portable install
/// writes beside itself, and the two must never collide.
#[cfg(windows)]
pub(crate) fn desktop_profile_dir() -> Result<PathBuf, String> {
    let local_app_data = std::env::var_os("LOCALAPPDATA")
        .ok_or_else(|| "LOCALAPPDATA is not set; cannot locate Weaver desktop data".to_string())?;
    Ok(desktop_profile_dir_from(Path::new(&local_app_data)))
}

#[cfg(target_os = "macos")]
pub(crate) fn desktop_profile_dir() -> Result<PathBuf, String> {
    let home = std::env::var_os("HOME")
        .ok_or_else(|| "HOME is not set; cannot locate Weaver desktop data".to_string())?;
    Ok(desktop_profile_dir_from(
        &Path::new(&home).join("Library").join("Application Support"),
    ))
}

/// The vendor/product suffix both platforms append to their per-user data
/// root, split out so the layout can be asserted without an environment.
pub(crate) fn desktop_profile_dir_from(application_data: &Path) -> PathBuf {
    application_data.join("ScryerMedia").join("Weaver")
}

/// Whether a navigation the app window is about to perform belongs in the
/// user's browser instead.
///
/// Only absolute `http`/`https` navigations are redirected. Everything else —
/// `about:blank` while a webview initializes, `data:` and `blob:` documents
/// the app itself creates, and schemes the platform webview already refuses —
/// is left to the webview, because handing those to the shell would either do
/// nothing or hand the user's shell a document the app generated.
pub(crate) fn opens_in_external_browser(app_origin: &str, url: &str) -> bool {
    match http_origin(url) {
        Some(origin) => !origin.eq_ignore_ascii_case(app_origin),
        None => false,
    }
}

/// The `scheme://authority` prefix of an absolute `http`/`https` URL.
///
/// This is deliberately a prefix slice rather than a parsed origin: the
/// comparison above is against a string this process built, so anything that
/// is not byte-for-byte (case-insensitively) the same origin — a different
/// port, a host alias, userinfo smuggled into the authority — is correctly
/// treated as somewhere else.
fn http_origin(url: &str) -> Option<&str> {
    let (scheme, rest) = url.split_once("://")?;
    if !scheme.eq_ignore_ascii_case("http") && !scheme.eq_ignore_ascii_case("https") {
        return None;
    }
    let authority_len = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    Some(&url[..scheme.len() + "://".len() + authority_len])
}

/// Poll until the server answers, or the timeout expires.
pub(crate) fn wait_for_server(port: u16, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if server_ready(port) {
            return true;
        }
        thread::sleep(Duration::from_millis(250));
    }
    false
}

/// Whether the server is serving the UI yet.
///
/// A connected socket is not enough: the listener binds before the SPA is
/// mounted, so the wrapper would open a window on an error page. Asking for
/// the document itself is the only readiness signal that means what the user
/// is about to see is there.
///
/// A `200` is not enough either. The wrapper owns a fixed port, and anything
/// else on the machine can be listening on it; accepting whatever answers
/// would skip starting the bundled server and load a stranger's page into the
/// window. The document has to be one of Weaver's.
pub(crate) fn server_ready(port: u16) -> bool {
    probe_port(port) == PortProbe::Weaver
}

/// What one probe of the wrapper's port found.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PortProbe {
    /// Nothing answered: the port is free, or a server is still starting. The
    /// server binds its listener before it serves, so a connection accepted in
    /// that window simply gets no response and lands here too.
    NotAnswering,
    /// Something answered, and it was not Weaver's entry page.
    Foreign,
    /// Weaver's entry page came back.
    Weaver,
}

pub(crate) fn probe_port(port: u16) -> PortProbe {
    let request = format!(
        "GET / HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nAccept: text/html\r\nConnection: close\r\n\r\n"
    );
    match http_exchange(port, request.as_bytes(), READY_PROBE_TIMEOUT) {
        None => PortProbe::NotAnswering,
        Some(response) if is_weaver_document(&response) => PortProbe::Weaver,
        Some(_) => PortProbe::Foreign,
    }
}

/// How long one readiness probe waits on the socket. Short: the probe runs on
/// a timer while the splash screen is up, and a server that takes longer than
/// this to hand over its entry page is not ready.
const READY_PROBE_TIMEOUT: Duration = Duration::from_millis(500);

/// Whether a response to `GET /` is Weaver's entry page rather than some other
/// program's.
///
/// Every document the server answers `/` with — the SPA shell, the login page,
/// the setup wizard, and the pages that tell an unadmitted browser why it is
/// not getting in — is titled with the product name, and the server's own
/// tests pin those titles. The title is the one thing all of them share that a
/// listener which merely happens to be on the port would not produce.
pub(crate) fn is_weaver_document(response: &HttpResponse) -> bool {
    if response.status != 200 {
        return false;
    }
    let Ok(body) = std::str::from_utf8(&response.body) else {
        return false;
    };
    html_title(body).is_some_and(|title| title.contains("Weaver"))
}

/// The text of the first `<title>` element, if the document has one.
fn html_title(html: &str) -> Option<&str> {
    let lower = html.to_ascii_lowercase();
    let start = lower.find("<title>")? + "<title>".len();
    let end = lower[start..].find("</title>")? + start;
    Some(html[start..end].trim())
}

// ---------------------------------------------------------------------------
// The queue snapshot the menu-bar popover shows
// ---------------------------------------------------------------------------

/// The name of the browser session cookie the server hands a trusted loopback
/// peer (`http::auth::SESSION_COOKIE_NAME`). The wrapper is a browser as far as
/// the server is concerned, so it earns its access the same way.
const SESSION_COOKIE: &str = "weaver_session";

/// How many queue items the popover shows. The popover is a glance, not the
/// queue page.
pub(crate) const POPOVER_ROWS: usize = 5;

/// The width of the popover, in points.
pub(crate) const POPOVER_WIDTH: f64 = 300.0;

/// How long a popover fetch waits on the server before it gives up. Short: a
/// hover that has already ended must not leave a socket open behind it.
const QUEUE_FETCH_TIMEOUT: Duration = Duration::from_millis(1500);

/// A response body larger than this is a server the wrapper does not
/// understand, not a queue snapshot.
const QUEUE_RESPONSE_LIMIT: usize = 1 << 20;

/// The query the popover asks. Written as one line so the request body needs no
/// escaping beyond the quoting below.
const QUEUE_QUERY: &str = "{ queueSnapshot { items { id displayTitle name state progressPercent phaseProgress { phase rateBps } } globalState { isPaused } } }";

/// One queue item, reduced to what a menu-bar row can show.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct QueueRow {
    pub(crate) name: String,
    /// The item state, in the casing a person reads rather than the casing the
    /// schema uses.
    pub(crate) state: String,
    pub(crate) progress_percent: f64,
}

/// Everything the popover draws.
///
/// `status` is absent exactly when there is no queue to describe — the server
/// did not answer, or it refused the wrapper — because in those states a status
/// line would be inventing one.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct PopoverContent {
    pub(crate) status: Option<String>,
    pub(crate) rows: Vec<QueueRow>,
    /// Shown instead of rows when there are none.
    pub(crate) message: Option<String>,
}

impl PopoverContent {
    fn message_only(message: &str) -> Self {
        Self {
            status: None,
            rows: Vec::new(),
            message: Some(message.to_string()),
        }
    }

    /// The server is not answering on the port at all.
    pub(crate) fn offline() -> Self {
        Self::message_only("Weaver isn't running")
    }

    /// Something is answering the port, but it did not give up a queue: this
    /// peer is not one the install hands a session to — a fresh install that
    /// trusts nobody yet, or a login-protected one — or the answer was not a
    /// snapshot at all. Either way the window is where the user finds out.
    fn unavailable() -> Self {
        Self::message_only("Queue unavailable — open Weaver")
    }
}

/// Ask the running server for the queue, reusing a session cookie across calls.
///
/// The cookie is refreshed at most once per call: a second 401 means the server
/// is never going to hand this peer a session, and retrying it on every hover
/// would be a login attempt loop the user cannot see.
pub(crate) fn fetch_popover_content(port: u16, cookie: &mut Option<String>) -> PopoverContent {
    if cookie.is_none() {
        *cookie = fetch_session_cookie(port);
    }
    let mut response = post_graphql(port, cookie.as_deref());
    if response.as_ref().is_some_and(|it| it.status == 401) {
        *cookie = fetch_session_cookie(port);
        if cookie.is_none() {
            return PopoverContent::unavailable();
        }
        response = post_graphql(port, cookie.as_deref());
    }
    // Only a connection that never produced a response means the server is
    // gone; anything that answered is running, whatever it answered with.
    let Some(response) = response else {
        return PopoverContent::offline();
    };
    if response.status != 200 {
        return PopoverContent::unavailable();
    }
    std::str::from_utf8(&response.body)
        .ok()
        .and_then(popover_content_from_graphql)
        .unwrap_or_else(PopoverContent::unavailable)
}

/// Fetch the document the way a browser does, for the session cookie it sets.
fn fetch_session_cookie(port: u16) -> Option<String> {
    let request = format!(
        "GET / HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nAccept: text/html\r\nConnection: close\r\n\r\n"
    );
    let response = http_exchange(port, request.as_bytes(), QUEUE_FETCH_TIMEOUT)?;
    set_cookie_value(&response.headers, SESSION_COOKIE)
}

fn post_graphql(port: u16, cookie: Option<&str>) -> Option<HttpResponse> {
    let body = format!(
        "{{\"query\":{}}}",
        json_string_literal(QUEUE_QUERY.trim_end())
    );
    let mut request = format!(
        "POST /graphql HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n\
         Content-Type: application/json\r\nAccept: application/json\r\n\
         Content-Length: {}\r\nConnection: close\r\n",
        body.len()
    );
    if let Some(cookie) = cookie {
        request.push_str(&format!("Cookie: {SESSION_COOKIE}={cookie}\r\n"));
    }
    request.push_str("\r\n");
    request.push_str(&body);
    http_exchange(port, request.as_bytes(), QUEUE_FETCH_TIMEOUT)
}

/// One request and one response on a connection the server closes.
///
/// `Connection: close` is what makes reading to EOF a complete response, so
/// this needs no keep-alive framing of its own.
fn http_exchange(port: u16, request: &[u8], timeout: Duration) -> Option<HttpResponse> {
    let address = SocketAddr::from(([127, 0, 0, 1], port));
    let mut stream = TcpStream::connect_timeout(&address, timeout).ok()?;
    stream.set_read_timeout(Some(timeout)).ok()?;
    stream.set_write_timeout(Some(timeout)).ok()?;
    stream.write_all(request).ok()?;

    let mut raw = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        let read = stream.read(&mut chunk).ok()?;
        if read == 0 {
            break;
        }
        raw.extend_from_slice(&chunk[..read]);
        if raw.len() > QUEUE_RESPONSE_LIMIT {
            return None;
        }
    }
    parse_http_response(&raw)
}

/// A response split into the three parts the wrapper reads.
#[derive(Debug, PartialEq)]
pub(crate) struct HttpResponse {
    pub(crate) status: u16,
    /// Field names as received; every lookup here is case-insensitive.
    pub(crate) headers: Vec<(String, String)>,
    pub(crate) body: Vec<u8>,
}

/// Split a raw response, decoding a chunked body if that is how it arrived.
///
/// Only the two framings axum produces are handled — a declared length and
/// chunked — because a response with neither is one this wrapper did not ask
/// for.
pub(crate) fn parse_http_response(raw: &[u8]) -> Option<HttpResponse> {
    let split = raw.windows(4).position(|window| window == b"\r\n\r\n")?;
    let head = std::str::from_utf8(&raw[..split]).ok()?;
    let body = &raw[split + 4..];

    let mut lines = head.split("\r\n");
    let status = lines
        .next()?
        .split(' ')
        .nth(1)
        .and_then(|code| code.parse::<u16>().ok())?;
    let headers: Vec<(String, String)> = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.trim().to_string(), value.trim().to_string()))
        .collect();

    let body = if header_value(&headers, "transfer-encoding")
        .is_some_and(|value| value.to_ascii_lowercase().contains("chunked"))
    {
        decode_chunked(body)?
    } else {
        body.to_vec()
    };
    Some(HttpResponse {
        status,
        headers,
        body,
    })
}

/// The first value for a header name, matched case-insensitively.
fn header_value<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(field, _)| field.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}

/// The value of one named cookie across every `Set-Cookie` header.
pub(crate) fn set_cookie_value(headers: &[(String, String)], name: &str) -> Option<String> {
    headers
        .iter()
        .filter(|(field, _)| field.eq_ignore_ascii_case("set-cookie"))
        .find_map(|(_, value)| {
            let pair = value.split(';').next()?;
            let (field, cookie) = pair.split_once('=')?;
            (field.trim() == name && !cookie.trim().is_empty()).then(|| cookie.trim().to_string())
        })
}

/// Reassemble a `Transfer-Encoding: chunked` body.
pub(crate) fn decode_chunked(body: &[u8]) -> Option<Vec<u8>> {
    let mut decoded = Vec::new();
    let mut rest = body;
    loop {
        let end = rest.windows(2).position(|window| window == b"\r\n")?;
        let header = std::str::from_utf8(&rest[..end]).ok()?;
        // A chunk size may carry extensions after a semicolon; nothing here
        // uses them, but they are legal and must not be parsed as digits.
        let size = usize::from_str_radix(header.split(';').next()?.trim(), 16).ok()?;
        rest = rest.get(end + 2..)?;
        if size == 0 {
            return Some(decoded);
        }
        decoded.extend_from_slice(rest.get(..size)?);
        // The CRLF that terminates the chunk data.
        rest = rest.get(size + 2..)?;
    }
}

/// A string as a JSON literal. The query is a program constant, so this only
/// has to be correct, not fast.
fn json_string_literal(value: &str) -> String {
    let mut literal = String::with_capacity(value.len() + 2);
    literal.push('"');
    for character in value.chars() {
        match character {
            '"' => literal.push_str("\\\""),
            '\\' => literal.push_str("\\\\"),
            '\n' => literal.push_str("\\n"),
            '\r' => literal.push_str("\\r"),
            '\t' => literal.push_str("\\t"),
            control if (control as u32) < 0x20 => {
                literal.push_str(&format!("\\u{:04x}", control as u32));
            }
            other => literal.push(other),
        }
    }
    literal.push('"');
    literal
}

/// Map a `queueSnapshot` response onto what the popover draws.
///
/// Returns `None` only when the payload is not a queue snapshot at all. Unknown
/// item states and missing optional fields are carried through: a state this
/// build has never heard of must still show up as a row.
pub(crate) fn popover_content_from_graphql(body: &str) -> Option<PopoverContent> {
    #[derive(serde::Deserialize)]
    struct Envelope {
        data: Data,
    }
    #[derive(serde::Deserialize)]
    struct Data {
        #[serde(rename = "queueSnapshot")]
        queue_snapshot: Snapshot,
    }
    #[derive(serde::Deserialize)]
    struct Snapshot {
        #[serde(default)]
        items: Vec<Item>,
        #[serde(rename = "globalState")]
        global_state: Option<GlobalState>,
    }
    #[derive(serde::Deserialize)]
    struct GlobalState {
        #[serde(rename = "isPaused", default)]
        is_paused: bool,
    }
    #[derive(serde::Deserialize)]
    struct Item {
        #[serde(rename = "displayTitle", default)]
        display_title: String,
        #[serde(default)]
        name: String,
        #[serde(default)]
        state: String,
        #[serde(rename = "progressPercent", default)]
        progress_percent: f64,
        #[serde(rename = "phaseProgress", default)]
        phase_progress: Vec<Phase>,
    }
    #[derive(serde::Deserialize)]
    struct Phase {
        #[serde(rename = "rateBps", default)]
        rate_bps: Option<f64>,
    }

    let envelope: Envelope = serde_json::from_str(body).ok()?;
    let snapshot = envelope.data.queue_snapshot;
    let is_paused = snapshot
        .global_state
        .is_some_and(|global_state| global_state.is_paused);

    let speed: f64 = snapshot
        .items
        .iter()
        .filter(|item| is_downloading_state(&item.state))
        .flat_map(|item| item.phase_progress.iter())
        .filter_map(|phase| phase.rate_bps)
        .filter(|rate| rate.is_finite() && *rate > 0.0)
        .sum();
    let status = queue_status_line(
        is_paused,
        snapshot.items.iter().map(|item| item.state.as_str()),
        speed,
    );

    let rows: Vec<QueueRow> = snapshot
        .items
        .iter()
        .take(POPOVER_ROWS)
        .map(|item| QueueRow {
            name: row_name(&item.display_title, &item.name),
            state: humanize_state(&item.state),
            progress_percent: clamp_percent(item.progress_percent),
        })
        .collect();
    let message = rows.is_empty().then(|| "Queue is empty".to_string());
    Some(PopoverContent {
        status: Some(status),
        rows,
        message,
    })
}

/// The states that mean bytes are moving. `FINALIZING_DOWNLOAD` counts: the
/// last articles of a job are still arriving while it is set.
fn is_downloading_state(state: &str) -> bool {
    ["DOWNLOADING", "FETCHING_REPAIR_DATA", "FINALIZING_DOWNLOAD"]
        .iter()
        .any(|known| state.eq_ignore_ascii_case(known))
}

/// The one line above the rows.
fn queue_status_line<'a>(
    is_paused: bool,
    states: impl Iterator<Item = &'a str>,
    speed_bytes_per_sec: f64,
) -> String {
    let mut any = false;
    let mut downloading = false;
    for state in states {
        any = true;
        downloading |= is_downloading_state(state);
    }
    if is_paused {
        "Paused".to_string()
    } else if downloading {
        format!("Downloading — {}", format_speed(speed_bytes_per_sec))
    } else if any {
        "Queued".to_string()
    } else {
        "Idle".to_string()
    }
}

/// `displayTitle` is what the queue page shows; `name` is the fallback for a
/// job whose release has not been parsed yet.
fn row_name(display_title: &str, name: &str) -> String {
    for candidate in [display_title, name] {
        let candidate = candidate.trim();
        if !candidate.is_empty() {
            return candidate.to_string();
        }
    }
    "Untitled".to_string()
}

/// `FETCHING_REPAIR_DATA` is not a label. Unknown states go through the same
/// transformation rather than being dropped.
fn humanize_state(state: &str) -> String {
    let words = state.trim().replace('_', " ").to_ascii_lowercase();
    let mut characters = words.chars();
    match characters.next() {
        Some(first) => first.to_uppercase().collect::<String>() + characters.as_str(),
        None => "Unknown".to_string(),
    }
}

fn clamp_percent(percent: f64) -> f64 {
    if percent.is_finite() {
        percent.clamp(0.0, 100.0)
    } else {
        0.0
    }
}

/// The byte scale the web UI uses (`SpeedDisplay.tsx`), so a speed read in the
/// menu bar and the same speed read in the window agree.
pub(crate) fn format_bytes(bytes: f64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    if !bytes.is_finite() || bytes <= 0.0 {
        return "0 B".to_string();
    }
    #[allow(
        clippy::cast_possible_truncation,
        reason = "the exponent is clamped to the unit table before it is used"
    )]
    let index = (bytes.log(1024.0).floor() as i32).clamp(0, UNITS.len() as i32 - 1);
    let value = bytes / 1024f64.powi(index);
    if index == 0 {
        format!("{value:.0} {}", UNITS[0])
    } else {
        format!("{value:.1} {}", UNITS[index as usize])
    }
}

pub(crate) fn format_speed(bytes_per_sec: f64) -> String {
    format!("{}/s", format_bytes(bytes_per_sec))
}

/// The secondary line under a row's progress bar.
pub(crate) fn row_detail(row: &QueueRow) -> String {
    format!("{} · {}%", row.state, row.progress_percent.round())
}

/// The `weaver` server process the wrapper owns.
///
/// The wrapper is the parent of the server it started, so this is also what
/// guarantees the server goes away when the user quits the wrapper. It
/// deliberately tolerates a server it did not start: a user who already has
/// `weaver serve` running on the port gets the same window, and the wrapper
/// simply has no child to supervise.
pub(crate) struct ServerSupervisor {
    profile_dir: PathBuf,
    port: u16,
    server: Option<Child>,
}

impl ServerSupervisor {
    pub(crate) fn new(profile_dir: PathBuf, port: u16) -> Self {
        Self {
            profile_dir,
            port,
            server: None,
        }
    }

    pub(crate) fn profile_dir(&self) -> &Path {
        &self.profile_dir
    }

    pub(crate) fn logs_dir(&self) -> PathBuf {
        self.profile_dir.join("logs")
    }

    pub(crate) fn port(&self) -> u16 {
        self.port
    }

    /// Create the profile the server is about to be pointed at. The server
    /// creates its own subdirectories, but it is started with an explicit log
    /// file path, and it cannot create the directory that path lives in.
    pub(crate) fn ensure_profile_dirs(&self) -> Result<(), String> {
        std::fs::create_dir_all(self.logs_dir()).map_err(|error| {
            format!(
                "failed to create Weaver desktop profile at {}: {error}",
                self.profile_dir.display()
            )
        })
    }

    /// Start the server unless something is already serving the port.
    ///
    /// Returning early on a live port is what makes this idempotent for every
    /// caller — menu item, window open, and login start all funnel through
    /// here — but it is also why a caller that has just torn the server down
    /// must wait for the old process to disappear first.
    pub(crate) fn start(&mut self) -> Result<(), String> {
        match probe_port(self.port) {
            PortProbe::Weaver => return Ok(()),
            // Spawning a server here would only make it fail to bind, and the
            // user would be told Weaver timed out rather than what is wrong.
            PortProbe::Foreign => {
                return Err(format!(
                    "port {} is already in use by a program that is not Weaver; stop it, or run `weaver serve --port {}` yourself",
                    self.port, self.port
                ));
            }
            PortProbe::NotAnswering => {}
        }
        if let Some(child) = self.server.as_mut() {
            match child.try_wait() {
                Ok(None) => return Ok(()),
                Ok(Some(_)) => self.server = None,
                Err(error) => {
                    return Err(format!("failed to check Weaver server status: {error}"));
                }
            }
        }

        let server_executable = self.server_executable()?;
        let log_file = self.logs_dir().join("weaver.log");
        let mut command = Command::new(&server_executable);
        command
            .arg("--config")
            .arg(&self.profile_dir)
            .arg("--log-file")
            .arg(&log_file)
            .args(["serve", "--port", &self.port.to_string()]);
        configure_server_command(&mut command);
        let child = command.spawn().map_err(|error| {
            format!(
                "failed to start Weaver from {}: {error}",
                server_executable.display()
            )
        })?;
        self.server = Some(child);
        Ok(())
    }

    /// Stop the server this wrapper started. A server it did not start is left
    /// alone: it belongs to whoever ran it.
    pub(crate) fn stop(&mut self) -> Result<(), String> {
        let Some(mut child) = self.server.take() else {
            return Ok(());
        };
        if child
            .try_wait()
            .map_err(|error| format!("failed to check Weaver server status: {error}"))?
            .is_none()
        {
            #[cfg(unix)]
            if request_graceful_stop(&mut child)? {
                return Ok(());
            }
            child
                .kill()
                .map_err(|error| format!("failed to stop Weaver server: {error}"))?;
            child
                .wait()
                .map_err(|error| format!("failed to wait for Weaver server exit: {error}"))?;
        }
        Ok(())
    }

    /// Stop and start again, from a user action.
    pub(crate) fn restart(&mut self) -> Result<(), String> {
        self.stop()?;
        self.start()?;
        self.wait_until_ready()
    }

    pub(crate) fn wait_until_ready(&self) -> Result<(), String> {
        if wait_for_server(self.port, SERVER_READY_TIMEOUT) {
            Ok(())
        } else {
            Err("timed out waiting for Weaver after restart".to_string())
        }
    }

    /// Wait for the running server to disappear, bounded so a process that
    /// never exits cannot strand the wrapper. The owned child is the reliable
    /// signal; when the wrapper does not own one, the port answering is the
    /// only evidence left. Falls back to the kill path on timeout.
    pub(crate) fn wait_for_exit(&mut self, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            match self.server.as_mut() {
                Some(child) => match child.try_wait() {
                    Ok(Some(_)) => {
                        self.server = None;
                        return;
                    }
                    Ok(None) => {}
                    Err(_) => break,
                },
                None => {
                    if !server_ready(self.port) {
                        return;
                    }
                }
            }
            thread::sleep(Duration::from_millis(250));
        }
        let _ = self.stop();
    }

    /// The server ships beside the wrapper, so its path is derived rather than
    /// searched: picking up a `weaver` from `PATH` would silently run a
    /// different install than the one the user launched.
    fn server_executable(&self) -> Result<PathBuf, String> {
        let wrapper = std::env::current_exe()
            .map_err(|error| format!("failed to resolve {WRAPPER_EXECUTABLE} path: {error}"))?;
        let server = wrapper.with_file_name(SERVER_EXECUTABLE);
        if !server.is_file() {
            return Err(format!(
                "{SERVER_EXECUTABLE} was not found beside {WRAPPER_EXECUTABLE} at {}",
                server.display()
            ));
        }
        Ok(server)
    }
}

/// Keep the server out of the user's face. On Windows a console subsystem
/// child would flash a window on every start; on macOS the child inherits the
/// wrapper's already-windowless session and needs nothing.
#[cfg(windows)]
fn configure_server_command(command: &mut Command) {
    use std::os::windows::process::CommandExt;
    use windows_sys::Win32::System::Threading::CREATE_NO_WINDOW;

    command.creation_flags(CREATE_NO_WINDOW);
}

#[cfg(not(windows))]
fn configure_server_command(_command: &mut Command) {}

/// Ask the server to shut down cleanly, and report whether it did.
///
/// The server writes a database on every job; killing it outright is safe but
/// throws away the flush it would otherwise do, so the signal comes first and
/// the caller only escalates when this returns false.
#[cfg(unix)]
fn request_graceful_stop(child: &mut Child) -> Result<bool, String> {
    // SAFETY: `child` is a live process this wrapper started, so its PID is
    // still ours to signal and cannot have been recycled.
    unsafe { libc::kill(child.id() as libc::pid_t, libc::SIGTERM) };

    let deadline = Instant::now() + GRACEFUL_STOP_TIMEOUT;
    while Instant::now() < deadline {
        match child.try_wait() {
            Ok(Some(_)) => return Ok(true),
            Ok(None) => {}
            Err(error) => return Err(format!("failed to check Weaver server status: {error}")),
        }
        thread::sleep(Duration::from_millis(100));
    }
    Ok(false)
}

/// A single-page HTTP server used only by `--webview-smoke`.
///
/// The smoke test has to prove the webview stack renders a real network
/// document, and it has to do that on a machine where no Weaver server is
/// installed or running — so it serves its own.
pub(crate) fn start_smoke_server() -> Result<u16, String> {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .map_err(|error| format!("failed to bind the smoke test HTTP server: {error}"))?;
    let port = listener
        .local_addr()
        .map_err(|error| format!("failed to read the smoke test HTTP server port: {error}"))?
        .port();

    thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
            let _ = stream.set_write_timeout(Some(Duration::from_secs(5)));
            // The request is drained rather than parsed: every request gets the
            // same document, and the only thing that matters is that the socket
            // is not closed before the client finished writing.
            let mut request = [0u8; 1024];
            let _ = stream.read(&mut request);
            let _ = stream.write_all(SMOKE_RESPONSE.as_bytes());
            let _ = stream.flush();
        }
    });

    Ok(port)
}

const SMOKE_BODY: &str = "<!doctype html><title>Weaver webview smoke</title><p>ok</p>";

/// Written verbatim, including the length, so the smoke server needs no HTTP
/// implementation at all.
const SMOKE_RESPONSE: &str = concat!(
    "HTTP/1.1 200 OK\r\n",
    "Content-Type: text/html; charset=utf-8\r\n",
    "Content-Length: 59\r\n",
    "Connection: close\r\n",
    "\r\n",
    "<!doctype html><title>Weaver webview smoke</title><p>ok</p>",
);

/// How long the smoke test waits for the webview to report a result before it
/// declares the wiring broken. Generous, because a cold WebView2 or WebKit
/// process launch on a loaded CI machine is slow.
pub(crate) const SMOKE_TIMEOUT: Duration = Duration::from_secs(90);

/// The line CI greps for. Printed to stdout only on success.
pub(crate) const SMOKE_SUCCESS_LINE: &str = "webview-smoke: ok";

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::{
        HttpResponse, PopoverContent, QueueRow, SMOKE_BODY, SMOKE_RESPONSE, app_origin, app_url,
        decode_chunked, desktop_profile_dir_from, format_bytes, format_speed, http_origin,
        is_weaver_document, opens_in_external_browser, parse_http_response,
        popover_content_from_graphql, row_detail, set_cookie_value,
    };

    #[test]
    fn desktop_profile_is_isolated_from_legacy_portable_state() {
        let local_app_data = Path::new(r"C:\\")
            .join("Users")
            .join("example")
            .join("AppData")
            .join("Local");

        assert_eq!(
            desktop_profile_dir_from(&local_app_data),
            local_app_data.join("ScryerMedia").join("Weaver")
        );
    }

    #[test]
    fn the_app_window_stays_on_the_local_server() {
        let origin = app_origin(9090);
        assert_eq!(origin, "http://127.0.0.1:9090");
        assert_eq!(app_url(9090), "http://127.0.0.1:9090/");

        for inside in [
            "http://127.0.0.1:9090",
            "http://127.0.0.1:9090/",
            "http://127.0.0.1:9090/queue",
            "http://127.0.0.1:9090/graphql?query=1",
            "http://127.0.0.1:9090/#/settings",
            "HTTP://127.0.0.1:9090/queue",
        ] {
            assert!(
                !opens_in_external_browser(&origin, inside),
                "{inside} should stay in the app window"
            );
        }
    }

    #[test]
    fn links_off_the_local_server_go_to_the_browser() {
        let origin = app_origin(9090);

        for outside in [
            "https://example.invalid/docs",
            // Same host and port, different scheme: not the app.
            "https://127.0.0.1:9090/",
            // Same host, different port: somebody else's server.
            "http://127.0.0.1:9091/",
            // A host alias is still a different origin, and the app never
            // links to itself that way.
            "http://localhost:9090/",
            // Userinfo cannot be used to smuggle the origin check.
            "http://127.0.0.1:9090@example.invalid/",
        ] {
            assert!(
                opens_in_external_browser(&origin, outside),
                "{outside} should open in the browser"
            );
        }
    }

    #[test]
    fn non_http_documents_are_left_to_the_webview() {
        let origin = app_origin(9090);

        for internal in [
            "about:blank",
            "data:text/html,<p>hi</p>",
            "blob:http://127.0.0.1:9090/9d1f",
            "javascript:void(0)",
            "",
        ] {
            assert!(
                !opens_in_external_browser(&origin, internal),
                "{internal} should be left to the webview"
            );
            assert_eq!(http_origin(internal), None);
        }
    }

    #[test]
    fn the_smoke_response_declares_its_own_length() {
        let (headers, body) = SMOKE_RESPONSE
            .split_once("\r\n\r\n")
            .expect("smoke response has a header block");
        assert_eq!(body, SMOKE_BODY);
        assert!(
            headers.contains(&format!("Content-Length: {}", SMOKE_BODY.len())),
            "declared length must match the body: {headers}"
        );
    }

    fn document(status: u16, body: &str) -> HttpResponse {
        HttpResponse {
            status,
            headers: vec![("content-type".to_string(), "text/html".to_string())],
            body: body.as_bytes().to_vec(),
        }
    }

    #[test]
    fn every_page_the_server_answers_the_root_with_counts_as_ready() {
        for body in [
            "<!doctype html><html><head><title>Weaver</title></head><body></body></html>",
            "<!doctype html><html><head>\n<title>Weaver - Login</title>\n</head></html>",
            "<html><head><TITLE>Set up Weaver</TITLE></head></html>",
        ] {
            assert!(is_weaver_document(&document(200, body)), "{body}");
        }
    }

    #[test]
    fn a_stranger_on_the_port_is_not_a_ready_server() {
        // Another local program answering 200 must not be mistaken for Weaver,
        // or the wrapper would skip starting its own server and show its page.
        assert!(!is_weaver_document(&document(
            200,
            "<html><head><title>Grafana</title></head><body></body></html>"
        )));
        assert!(!is_weaver_document(&document(200, "{\"status\":\"ok\"}")));
        assert!(!is_weaver_document(&document(200, "")));
    }

    #[test]
    fn a_weaver_page_that_is_not_a_200_is_not_ready_yet() {
        assert!(!is_weaver_document(&document(
            503,
            "<html><head><title>Weaver</title></head></html>"
        )));
    }

    #[test]
    fn the_smoke_document_is_a_weaver_page() {
        let response = parse_http_response(SMOKE_RESPONSE.as_bytes()).unwrap();
        assert!(is_weaver_document(&response));
    }

    // -- the queue snapshot the popover shows --------------------------------

    fn snapshot(items: &str, is_paused: bool) -> String {
        format!(
            "{{\"data\":{{\"queueSnapshot\":{{\"items\":[{items}],\
             \"globalState\":{{\"isPaused\":{is_paused}}}}}}}}}"
        )
    }

    fn item(title: &str, state: &str, percent: f64, rates: &str) -> String {
        format!(
            "{{\"id\":1,\"displayTitle\":\"{title}\",\"name\":\"{title}.nzb\",\
             \"state\":\"{state}\",\"progressPercent\":{percent},\
             \"phaseProgress\":[{rates}]}}"
        )
    }

    fn phase(phase: &str, rate: &str) -> String {
        format!("{{\"phase\":\"{phase}\",\"rateBps\":{rate}}}")
    }

    #[test]
    fn a_declared_length_response_splits_into_status_headers_and_body() {
        let raw = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
                    Content-Length: 2\r\n\r\n{}";
        let response = parse_http_response(raw).expect("a well-formed response parses");

        assert_eq!(response.status, 200);
        assert_eq!(response.body, b"{}");
        assert_eq!(
            response
                .headers
                .iter()
                .find(|(field, _)| field == "Content-Type")
                .map(|(_, value)| value.as_str()),
            Some("application/json")
        );
    }

    #[test]
    fn a_chunked_response_is_reassembled() {
        let raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n\
                    5\r\nhello\r\n6;ext=1\r\n world\r\n0\r\n\r\n";
        let response = parse_http_response(raw).expect("a chunked response parses");

        assert_eq!(response.status, 200);
        assert_eq!(response.body, b"hello world");
    }

    #[test]
    fn a_truncated_chunked_body_is_rejected_rather_than_guessed() {
        assert_eq!(decode_chunked(b"5\r\nhel"), None);
        assert_eq!(decode_chunked(b"zz\r\n"), None);
        // No terminating zero chunk: the response never finished arriving.
        assert_eq!(decode_chunked(b"5\r\nhello\r\n"), None);
    }

    #[test]
    fn a_response_without_a_header_block_is_not_a_response() {
        assert_eq!(parse_http_response(b"HTTP/1.1 200 OK\r\n"), None);
        assert_eq!(parse_http_response(b""), None);
    }

    #[test]
    fn the_session_cookie_is_read_out_of_its_own_set_cookie_header() {
        let headers = vec![
            ("Set-Cookie".to_string(), "other=nope; Path=/".to_string()),
            (
                "set-cookie".to_string(),
                "weaver_session=abc123; Path=/; HttpOnly; SameSite=Strict".to_string(),
            ),
        ];
        assert_eq!(
            set_cookie_value(&headers, "weaver_session"),
            Some("abc123".to_string())
        );
        assert_eq!(set_cookie_value(&headers, "weaver_jwt"), None);
    }

    #[test]
    fn a_cleared_cookie_is_not_a_session() {
        let headers = vec![(
            "Set-Cookie".to_string(),
            "weaver_session=; Path=/; Max-Age=0".to_string(),
        )];
        assert_eq!(set_cookie_value(&headers, "weaver_session"), None);
    }

    #[test]
    fn a_downloading_queue_reports_the_summed_rate() {
        let items = [
            item(
                "Silver Horizon",
                "DOWNLOADING",
                42.5,
                &format!("{},{}", phase("DOWNLOAD", "1048576"), phase("PAR2", "null")),
            ),
            item(
                "Amber Tide",
                "FETCHING_REPAIR_DATA",
                90.0,
                &phase("DOWNLOAD", "1048576"),
            ),
            // Not downloading: its rate must not be counted.
            item(
                "Copper Lane",
                "EXTRACTING",
                10.0,
                &phase("EXTRACT", "999999"),
            ),
        ]
        .join(",");
        let content =
            popover_content_from_graphql(&snapshot(&items, false)).expect("a snapshot maps");

        assert_eq!(content.status.as_deref(), Some("Downloading — 2.0 MB/s"));
        assert_eq!(content.message, None);
        assert_eq!(content.rows.len(), 3);
        assert_eq!(content.rows[0].name, "Silver Horizon");
        assert_eq!(content.rows[0].state, "Downloading");
        assert_eq!(content.rows[1].state, "Fetching repair data");
    }

    #[test]
    fn a_paused_queue_says_so_whatever_its_items_are_doing() {
        let items = item(
            "Silver Horizon",
            "DOWNLOADING",
            1.0,
            &phase("DOWNLOAD", "5000"),
        );
        let content =
            popover_content_from_graphql(&snapshot(&items, true)).expect("a snapshot maps");

        assert_eq!(content.status.as_deref(), Some("Paused"));
    }

    #[test]
    fn a_queue_with_nothing_moving_is_queued_and_an_empty_one_is_idle() {
        let waiting = item("Silver Horizon", "QUEUED", 0.0, "");
        let content =
            popover_content_from_graphql(&snapshot(&waiting, false)).expect("a snapshot maps");
        assert_eq!(content.status.as_deref(), Some("Queued"));
        assert_eq!(content.message, None);

        let content = popover_content_from_graphql(&snapshot("", false)).expect("a snapshot maps");
        assert_eq!(content.status.as_deref(), Some("Idle"));
        assert_eq!(content.message.as_deref(), Some("Queue is empty"));
        assert!(content.rows.is_empty());
    }

    #[test]
    fn the_popover_shows_at_most_five_rows() {
        let items = (0..9)
            .map(|index| item(&format!("Item {index}"), "QUEUED", 0.0, ""))
            .collect::<Vec<_>>()
            .join(",");
        let content =
            popover_content_from_graphql(&snapshot(&items, false)).expect("a snapshot maps");

        assert_eq!(content.rows.len(), 5);
        assert_eq!(content.rows[0].name, "Item 0");
        assert_eq!(content.rows[4].name, "Item 4");
    }

    #[test]
    fn an_unknown_state_still_produces_a_row() {
        let items = item("Silver Horizon", "TELEPORTING", 55.4, "");
        let content =
            popover_content_from_graphql(&snapshot(&items, false)).expect("a snapshot maps");

        assert_eq!(content.rows.len(), 1);
        assert_eq!(content.rows[0].state, "Teleporting");
        // Unknown states are not downloading states, so no speed is claimed.
        assert_eq!(content.status.as_deref(), Some("Queued"));
    }

    #[test]
    fn an_out_of_range_progress_is_pulled_back_onto_the_bar() {
        let items = [
            item("Over", "QUEUED", 140.0, ""),
            item("Under", "QUEUED", -3.0, ""),
        ]
        .join(",");
        let content =
            popover_content_from_graphql(&snapshot(&items, false)).expect("a snapshot maps");

        assert_eq!(content.rows[0].progress_percent, 100.0);
        assert_eq!(content.rows[1].progress_percent, 0.0);
    }

    #[test]
    fn an_item_without_a_display_title_falls_back_to_its_name() {
        let items = "{\"id\":1,\"displayTitle\":\"  \",\"name\":\"amber-tide.nzb\",\
                     \"state\":\"QUEUED\",\"progressPercent\":0,\"phaseProgress\":[]}";
        let content =
            popover_content_from_graphql(&snapshot(items, false)).expect("a snapshot maps");

        assert_eq!(content.rows[0].name, "amber-tide.nzb");
    }

    #[test]
    fn a_server_that_answers_with_something_else_is_running_but_unavailable() {
        // The offline message is reserved for a port nobody answered; anything
        // that replied is running, whatever it replied with.
        assert_ne!(PopoverContent::unavailable(), PopoverContent::offline());
    }

    #[test]
    fn a_body_that_is_not_a_queue_snapshot_maps_to_nothing() {
        for body in [
            "{}",
            "not json",
            "{\"errors\":[{\"message\":\"unauthorized\"}]}",
            "{\"data\":{}}",
        ] {
            assert_eq!(popover_content_from_graphql(body), None, "{body}");
        }
    }

    #[test]
    fn the_offline_and_refused_states_carry_no_status_line() {
        let offline = PopoverContent::offline();
        assert_eq!(offline.status, None);
        assert_eq!(offline.message.as_deref(), Some("Weaver isn't running"));
        assert!(offline.rows.is_empty());

        let refused = PopoverContent::unavailable();
        assert_eq!(refused.status, None);
        assert_eq!(
            refused.message.as_deref(),
            Some("Queue unavailable — open Weaver")
        );
    }

    #[test]
    fn byte_scales_match_the_web_ui() {
        assert_eq!(format_bytes(0.0), "0 B");
        assert_eq!(format_bytes(-1.0), "0 B");
        assert_eq!(format_bytes(f64::NAN), "0 B");
        assert_eq!(format_bytes(512.0), "512 B");
        assert_eq!(format_bytes(1024.0), "1.0 KB");
        assert_eq!(format_bytes(1024.0 * 1024.0 * 1.5), "1.5 MB");
        assert_eq!(format_bytes(1024f64.powi(4) * 3.0), "3.0 TB");
        // Past the last unit the scale stops rather than running off the table.
        assert_eq!(format_bytes(1024f64.powi(5)), "1024.0 TB");
        assert_eq!(format_speed(1024.0), "1.0 KB/s");
    }

    #[test]
    fn a_row_detail_reads_state_then_percent() {
        let row = QueueRow {
            name: "Silver Horizon".to_string(),
            state: "Downloading".to_string(),
            progress_percent: 42.5,
        };
        assert_eq!(row_detail(&row), "Downloading · 43%");
    }
}
