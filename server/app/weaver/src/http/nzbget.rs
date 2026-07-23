use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::body::Bytes;
use axum::extract::Extension;
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use serde::Deserialize;
use serde_json::{Value, json};
use weaver_server_api::{
    Attribute, AttributeInput, CLIENT_REQUEST_ID_ATTRIBUTE_KEY, CategoryResolutionMode,
    PRIORITY_ATTRIBUTE_KEY, QueueItem, QueueItemState, QueuePhase, SubmissionOptions,
    SubmitNzbError, fetch_nzb_from_url, history_item_from_row, normalize_priority_value,
    queue_item_from_job, submit_metadata, submit_nzb_bytes_with_options,
};
use weaver_server_core::auth::{ApiKeyCache, CallerScope, LoginAuthCache};
use weaver_server_core::settings::model::SharedConfig;
use weaver_server_core::{
    Database, FieldUpdate, HistoryFilter, JobId, JobStatus, JobUpdate, SchedulerError,
    SchedulerHandle,
};

#[derive(Clone)]
pub(super) struct NzbgetFacadeContext {
    db: Database,
    handle: SchedulerHandle,
    config: SharedConfig,
    auth_cache: LoginAuthCache,
    api_key_cache: ApiKeyCache,
    session_token: super::SessionToken,
    http_client: reqwest::Client,
    started_at: Instant,
    scheduled_resume: weaver_server_api::ScheduledResumeCoordinator,
    rss: weaver_server_api::RssService,
    watch_folder: weaver_server_core::watch_folder::WatchFolderService,
    /// TTL-cached free-disk-space reading: `(epoch_secs, available_bytes)`.
    /// `statvfs`/`GetDiskFreeSpaceExW` is a blocking syscall that can stall
    /// for seconds against an unhealthy NAS/NFS mount; `status()` reuses this
    /// for a few seconds instead of calling it on every poll.
    disk_cache: Arc<tokio::sync::Mutex<Option<(u64, u64)>>>,
    /// Memoized PARSE of DB-row (immutable) history, keyed by job id ->
    /// `(completed_at, HistoryItem)`. A history row's `completed_at` never
    /// changes once written, so a cache hit reuses the parsed item instead of
    /// re-running `history_item_from_row` (metadata parse + release-name parse)
    /// for up to 1000 rows every poll. Only the PARSE is cached — the final
    /// NZBGet entry (which folds in per-poll stage-timing fields derived from
    /// `job_events`, a separate mutable source) is rebuilt fresh each call, so
    /// a reprocess that appends new stage events is reflected immediately.
    history_cache: Arc<
        tokio::sync::Mutex<std::collections::HashMap<u64, (i64, weaver_server_api::HistoryItem)>>,
    >,
}

impl NzbgetFacadeContext {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        db: Database,
        handle: SchedulerHandle,
        config: SharedConfig,
        auth_cache: LoginAuthCache,
        api_key_cache: ApiKeyCache,
        session_token: super::SessionToken,
        rss: weaver_server_api::RssService,
        watch_folder: weaver_server_core::watch_folder::WatchFolderService,
        scheduled_resume: weaver_server_api::ScheduledResumeCoordinator,
    ) -> Self {
        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(60))
            .redirect(reqwest::redirect::Policy::limited(10))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());

        Self {
            db,
            handle,
            config,
            auth_cache,
            api_key_cache,
            session_token,
            http_client,
            started_at: Instant::now(),
            scheduled_resume,
            rss,
            watch_folder,
            disk_cache: Arc::new(tokio::sync::Mutex::new(None)),
            history_cache: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
        }
    }

    /// Cached free-disk-space lookup used by `status()`. The actual syscall
    /// runs off the tokio runtime thread (`spawn_blocking`) and the result is
    /// reused for up to `DISK_CACHE_TTL_SECS` seconds, so a stalled NAS/NFS
    /// mount can neither pin a worker thread nor (since callers read this
    /// only after releasing the config guard) convoy config readers behind
    /// it on every `status` poll.
    async fn free_disk_space_bytes(&self, complete_dir: String) -> u64 {
        const DISK_CACHE_TTL_SECS: u64 = 5;
        let now = unix_now_secs();
        if let Some((epoch_secs, bytes)) = *self.disk_cache.lock().await
            && now.saturating_sub(epoch_secs) < DISK_CACHE_TTL_SECS
        {
            return bytes;
        }
        let available = tokio::task::spawn_blocking(move || {
            weaver_server_core::operations::disk_space(Path::new(&complete_dir))
                .map(|space| space.available_bytes)
        })
        .await
        .ok()
        .flatten();
        match available {
            // Only cache a real reading, so a transient statvfs failure (a
            // briefly-unavailable mount, a join error) reports 0 for THIS poll
            // without poisoning the cache with 0 for the next 5s.
            Some(bytes) => {
                *self.disk_cache.lock().await = Some((now, bytes));
                bytes
            }
            None => 0,
        }
    }
}

fn unix_now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or(0)
}

#[derive(Debug, Deserialize)]
struct RpcRequest {
    method: String,
    #[serde(default)]
    params: Option<Value>,
    #[serde(default)]
    id: Option<Value>,
}

#[derive(Debug)]
struct RpcError {
    code: i64,
    message: String,
}

impl RpcError {
    fn invalid_procedure(method: &str) -> Self {
        Self {
            code: 1,
            message: format!("Invalid procedure ({method})"),
        }
    }

    fn invalid_parameter(message: impl Into<String>) -> Self {
        Self {
            code: 2,
            message: format!("Invalid parameter ({})", message.into()),
        }
    }

    fn invalid_action(action: &str) -> Self {
        Self {
            code: 3,
            message: format!("Invalid action ({action})"),
        }
    }

    fn access_denied() -> Self {
        Self {
            code: 401,
            message: "Access denied".to_string(),
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct NzbgetCallerScope(pub(super) CallerScope);

pub(super) async fn jsonrpc_handler(
    Extension(ctx): Extension<NzbgetFacadeContext>,
    Extension(NzbgetCallerScope(scope)): Extension<NzbgetCallerScope>,
    body: Bytes,
) -> Response {
    let request = match serde_json::from_slice::<RpcRequest>(&body) {
        Ok(request) => request,
        Err(error) => {
            return rpc_error_response(
                StatusCode::OK,
                Value::Null,
                RpcError::invalid_parameter(format!("invalid JSON-RPC request: {error}")),
            );
        }
    };

    let id = request.id.clone().unwrap_or(Value::Null);
    let method = request.method.to_ascii_lowercase();
    let required = method_required_scope(&method);
    if required.is_none() {
        return rpc_error_response(
            StatusCode::OK,
            id,
            RpcError::invalid_procedure(&request.method),
        );
    }
    if required == Some(RequiredScope::Control) && !scope.can_control() {
        return rpc_error_response(StatusCode::FORBIDDEN, id, RpcError::access_denied());
    }
    if required == Some(RequiredScope::Admin) && !scope.is_admin() {
        return rpc_error_response(StatusCode::FORBIDDEN, id, RpcError::access_denied());
    }

    match dispatch_method(&ctx, &method, request.params).await {
        Ok(result) => rpc_success_response(id, result),
        Err(error) => rpc_error_response(StatusCode::OK, id, error),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RequiredScope {
    Read,
    Control,
    Admin,
}

fn method_required_scope(method: &str) -> Option<RequiredScope> {
    match method {
        "version" | "status" | "listgroups" | "listfiles" | "history" | "config" | "loadconfig"
        | "log" | "loadlog" | "postqueue" | "urlqueue" | "servervolumes" => {
            Some(RequiredScope::Read)
        }
        // viewfeed/previewfeed return RSS item download URLs, which routinely
        // embed indexer API keys — privileged, not read-only.
        "append" | "appendurl" | "editqueue" | "pausedownload" | "resumedownload"
        | "pausedownload2" | "resumedownload2" | "pausepost" | "resumepost" | "pausescan"
        | "resumescan" | "scheduleresume" | "rate" | "writelog" | "fetchfeeds" | "viewfeed"
        | "previewfeed" => Some(RequiredScope::Control),
        "loadextensions" => Some(RequiredScope::Admin),
        _ => None,
    }
}

async fn dispatch_method(
    ctx: &NzbgetFacadeContext,
    method: &str,
    params: Option<Value>,
) -> Result<Value, RpcError> {
    match method {
        "version" => Ok(json!("16.0-weaver")),
        "append" => append(ctx, params).await,
        "appendurl" => append_url(ctx, params).await,
        "status" => status(ctx).await,
        "listgroups" => listgroups(ctx).await,
        "listfiles" => listfiles(ctx, params).await,
        "history" => history(ctx).await,
        // NZBGet distinguishes current values (`config`) from on-disk values
        // (`loadconfig`); weaver has a single live config. nzb360 reads its
        // category list from `loadconfig`.
        "config" | "loadconfig" => config(ctx).await,
        "loadextensions" => loadextensions(ctx).await,
        "editqueue" => editqueue(ctx, params).await,
        "log" => log_entries(ctx, params).await,
        "loadlog" => loadlog(ctx, params).await,
        "postqueue" => postqueue(ctx).await,
        "urlqueue" => Ok(Value::Array(Vec::new())),
        "servervolumes" => servervolumes(ctx).await,
        "viewfeed" | "previewfeed" => viewfeed(ctx, params).await,
        "fetchfeeds" => fetch_feeds(ctx),
        "pausedownload" | "pausedownload2" => pause_download(ctx).await,
        "resumedownload" | "resumedownload2" => resume_download(ctx).await,
        "pausepost" => pause_post_processing(ctx).await,
        "resumepost" => resume_post_processing(ctx).await,
        "pausescan" => set_scan_paused(ctx, true).await,
        "resumescan" => set_scan_paused(ctx, false).await,
        "scheduleresume" => scheduleresume(ctx, params).await,
        "rate" => rate(ctx, params).await,
        "writelog" => writelog(params),
        _ => Err(RpcError::invalid_procedure(method)),
    }
}

pub(super) async fn resolve_scope_for_facade(
    ctx: &NzbgetFacadeContext,
    headers: &HeaderMap,
) -> Result<CallerScope, StatusCode> {
    let headers = normalize_nzbget_auth_headers(headers)?;
    super::auth::resolve_caller(
        &ctx.db,
        &ctx.auth_cache,
        &ctx.api_key_cache,
        &ctx.session_token.0,
        &headers,
    )
    .await
    .map(|caller| caller.scope)
}

fn normalize_nzbget_auth_headers(headers: &HeaderMap) -> Result<HeaderMap, StatusCode> {
    let mut normalized = headers.clone();
    if let Some(token) = basic_password_token(headers.get(header::AUTHORIZATION))? {
        insert_bearer(&mut normalized, token)?;
        return Ok(normalized);
    }
    if let Some(token) = basic_password_token(headers.get("x-authorization"))? {
        insert_bearer(&mut normalized, token)?;
    }
    Ok(normalized)
}

fn basic_password_token(value: Option<&HeaderValue>) -> Result<Option<String>, StatusCode> {
    let Some(value) = value else {
        return Ok(None);
    };
    let value = value.to_str().map_err(|_| StatusCode::UNAUTHORIZED)?;
    let Some(encoded) = value
        .trim_start()
        .strip_prefix("Basic ")
        .or_else(|| value.trim_start().strip_prefix("basic "))
    else {
        return Ok(None);
    };
    let decoded = BASE64_STANDARD
        .decode(encoded.trim())
        .map_err(|_| StatusCode::UNAUTHORIZED)?;
    let decoded = String::from_utf8(decoded).map_err(|_| StatusCode::UNAUTHORIZED)?;
    let Some((_username, password)) = decoded.split_once(':') else {
        return Err(StatusCode::UNAUTHORIZED);
    };
    let password = password.trim();
    if password.is_empty() {
        return Err(StatusCode::UNAUTHORIZED);
    }
    Ok(Some(password.to_string()))
}

fn insert_bearer(headers: &mut HeaderMap, token: String) -> Result<(), StatusCode> {
    let value =
        HeaderValue::from_str(&format!("Bearer {token}")).map_err(|_| StatusCode::UNAUTHORIZED)?;
    headers.insert(header::AUTHORIZATION, value);
    Ok(())
}

fn rpc_success_response(id: Value, result: Value) -> Response {
    axum::Json(json!({
        "version": "1.1",
        "id": id,
        "result": result,
    }))
    .into_response()
}

fn rpc_error_response(status: StatusCode, id: Value, error: RpcError) -> Response {
    (
        status,
        axum::Json(json!({
            "version": "1.1",
            "id": id,
            "error": {
                "name": "JSONRPCError",
                "code": error.code,
                "message": error.message,
            },
        })),
    )
        .into_response()
}

// ---- XML-RPC transport ----------------------------------------------------
//
// nzb360 (the dominant mobile NZBGet client) speaks XML-RPC exclusively:
// POST /xmlrpc with a <methodCall> body and HTTP Basic auth. The calls are
// decoded into the same serde_json values the JSON-RPC dispatcher consumes,
// so every facade method is reachable over both transports.

const XMLRPC_MAX_DEPTH: usize = 32;
const XMLRPC_MAX_PARAMS: usize = 64;

struct XmlRpcCall {
    method: String,
    params: Vec<Value>,
}

pub(super) async fn xmlrpc_handler(
    Extension(ctx): Extension<NzbgetFacadeContext>,
    Extension(NzbgetCallerScope(scope)): Extension<NzbgetCallerScope>,
    body: Bytes,
) -> Response {
    let call = match parse_xmlrpc_call(&body) {
        Ok(call) => call,
        Err(error) => {
            return xmlrpc_fault_response(
                StatusCode::OK,
                RpcError::invalid_parameter(format!("invalid XML-RPC request: {error}")),
            );
        }
    };

    let method = call.method.to_ascii_lowercase();
    let Some(required) = method_required_scope(&method) else {
        return xmlrpc_fault_response(StatusCode::OK, RpcError::invalid_procedure(&call.method));
    };
    if required == RequiredScope::Control && !scope.can_control() {
        return xmlrpc_fault_response(StatusCode::FORBIDDEN, RpcError::access_denied());
    }
    if required == RequiredScope::Admin && !scope.is_admin() {
        return xmlrpc_fault_response(StatusCode::FORBIDDEN, RpcError::access_denied());
    }

    let params = if call.params.is_empty() {
        None
    } else {
        Some(Value::Array(call.params))
    };
    match dispatch_method(&ctx, &method, params).await {
        Ok(result) => xmlrpc_success_response(&result),
        Err(error) => xmlrpc_fault_response(StatusCode::OK, error),
    }
}

pub(super) fn authentication_error_response(path: &str, status: StatusCode) -> Response {
    if path.ends_with("/xmlrpc") {
        xmlrpc_fault_response(status, RpcError::access_denied())
    } else {
        rpc_error_response(status, Value::Null, RpcError::access_denied())
    }
}

fn xml_local_name(name: &[u8]) -> &[u8] {
    match name.iter().position(|byte| *byte == b':') {
        Some(index) => &name[index + 1..],
        None => name,
    }
}

fn xml_decode_text(event: &quick_xml::events::BytesText<'_>) -> Result<String, String> {
    let decoded = event.decode().map_err(|error| error.to_string())?;
    Ok(quick_xml::escape::unescape(&decoded)
        .map(|text| text.into_owned())
        .unwrap_or_else(|_| decoded.into_owned()))
}

fn parse_xmlrpc_call(body: &[u8]) -> Result<XmlRpcCall, String> {
    let mut reader = quick_xml::Reader::from_reader(body);
    // Do NOT trim text globally: it strips significant whitespace from string
    // params (e.g. an unpack password "* secret" or a whitespace-only password),
    // silently corrupting them. The typed int/bool/double parsers and the method
    // name trim themselves; struct member names are trimmed explicitly below.
    reader.config_mut().trim_text(false);
    let mut buf = Vec::new();
    let mut method: Option<String> = None;
    let mut params = Vec::new();
    let mut saw_method_call = false;

    loop {
        match reader
            .read_event_into(&mut buf)
            .map_err(|error| error.to_string())?
        {
            quick_xml::events::Event::Eof => break,
            quick_xml::events::Event::Start(event) => {
                let name = xml_local_name(event.name().as_ref()).to_vec();
                match name.as_slice() {
                    b"methodCall" => saw_method_call = true,
                    b"methodName" => {
                        method = Some(read_xml_text_until_end(&mut reader, b"methodName")?);
                    }
                    b"value" => {
                        if params.len() >= XMLRPC_MAX_PARAMS {
                            return Err(format!("more than {XMLRPC_MAX_PARAMS} parameters"));
                        }
                        params.push(parse_xmlrpc_value(&mut reader, 0)?);
                    }
                    // methodCall/params/param are structural containers.
                    _ => {}
                }
            }
            // A self-closing <value/> is an empty-string param. Without this arm
            // it falls through and is dropped, shifting every later positional
            // argument left (parse_xmlrpc_array already handles it this way).
            quick_xml::events::Event::Empty(event)
                if xml_local_name(event.name().as_ref()) == b"value" =>
            {
                if params.len() >= XMLRPC_MAX_PARAMS {
                    return Err(format!("more than {XMLRPC_MAX_PARAMS} parameters"));
                }
                params.push(Value::String(String::new()));
            }
            _ => {}
        }
        buf.clear();
    }

    if !saw_method_call {
        return Err("missing methodCall element".to_string());
    }
    let method = method
        .map(|name| name.trim().to_string())
        .filter(|name| !name.is_empty())
        .ok_or_else(|| "missing methodName".to_string())?;
    Ok(XmlRpcCall { method, params })
}

fn read_xml_text_until_end(
    reader: &mut quick_xml::Reader<&[u8]>,
    end: &[u8],
) -> Result<String, String> {
    let mut buf = Vec::new();
    let mut text = String::new();
    loop {
        match reader
            .read_event_into(&mut buf)
            .map_err(|error| error.to_string())?
        {
            quick_xml::events::Event::Text(event) => text.push_str(&xml_decode_text(&event)?),
            quick_xml::events::Event::CData(event) => {
                text.push_str(&String::from_utf8_lossy(event.as_ref()));
            }
            // quick-xml 0.41 tokenizes each entity reference (&amp; &lt; &#65; …)
            // as its own event; without this arm the catch-all below faults the
            // whole call on any typed value containing an entity — real indexer
            // URLs (`&`), filenames, and passwords with & < > ".
            quick_xml::events::Event::GeneralRef(event) => {
                let raw = format!("&{};", event.decode().map_err(|error| error.to_string())?);
                text.push_str(
                    &quick_xml::escape::unescape(&raw).map_err(|error| error.to_string())?,
                );
            }
            quick_xml::events::Event::End(event)
                if xml_local_name(event.name().as_ref()) == end =>
            {
                return Ok(text);
            }
            quick_xml::events::Event::Eof => {
                return Err(format!(
                    "unexpected EOF inside <{}>",
                    String::from_utf8_lossy(end)
                ));
            }
            _ => {
                return Err(format!(
                    "unexpected markup inside <{}>",
                    String::from_utf8_lossy(end)
                ));
            }
        }
        buf.clear();
    }
}

/// Parse one XML-RPC `<value>`; the opening tag has already been consumed.
fn parse_xmlrpc_value(
    reader: &mut quick_xml::Reader<&[u8]>,
    depth: usize,
) -> Result<Value, String> {
    if depth > XMLRPC_MAX_DEPTH {
        return Err("value nesting too deep".to_string());
    }
    let mut buf = Vec::new();
    let mut text = String::new();
    let mut typed: Option<Value> = None;

    loop {
        match reader
            .read_event_into(&mut buf)
            .map_err(|error| error.to_string())?
        {
            quick_xml::events::Event::Text(event) => text.push_str(&xml_decode_text(&event)?),
            quick_xml::events::Event::CData(event) => {
                text.push_str(&String::from_utf8_lossy(event.as_ref()));
            }
            // Resolve entity refs (0.41 emits them as their own event) into the
            // text buffer, so an untyped <value>a&amp;b</value> becomes "a&b"
            // instead of silently dropping the entity.
            quick_xml::events::Event::GeneralRef(event) => {
                let raw = format!("&{};", event.decode().map_err(|error| error.to_string())?);
                text.push_str(
                    &quick_xml::escape::unescape(&raw).map_err(|error| error.to_string())?,
                );
            }
            quick_xml::events::Event::Start(event) => {
                let name = xml_local_name(event.name().as_ref()).to_vec();
                let value = match name.as_slice() {
                    // Base64 payloads stay textual: the append handler decodes
                    // base64 content itself, matching NZBGet's string usage.
                    b"string" | b"base64" | b"dateTime.iso8601" => {
                        Value::String(read_xml_text_until_end(reader, &name)?)
                    }
                    b"i4" | b"int" | b"i8" => {
                        let text = read_xml_text_until_end(reader, &name)?;
                        let parsed = text
                            .trim()
                            .parse::<i64>()
                            .map_err(|_| format!("invalid integer '{}'", text.trim()))?;
                        Value::from(parsed)
                    }
                    b"boolean" => {
                        let text = read_xml_text_until_end(reader, &name)?;
                        Value::Bool(matches!(text.trim(), "1" | "true" | "TRUE" | "True"))
                    }
                    b"double" => {
                        let text = read_xml_text_until_end(reader, &name)?;
                        let parsed = text
                            .trim()
                            .parse::<f64>()
                            .map_err(|_| format!("invalid double '{}'", text.trim()))?;
                        json!(parsed)
                    }
                    b"nil" => {
                        read_xml_text_until_end(reader, &name)?;
                        Value::Null
                    }
                    b"array" => parse_xmlrpc_array(reader, depth + 1)?,
                    b"struct" => parse_xmlrpc_struct(reader, depth + 1)?,
                    other => {
                        return Err(format!(
                            "unsupported XML-RPC value type '{}'",
                            String::from_utf8_lossy(other)
                        ));
                    }
                };
                typed = Some(value);
            }
            quick_xml::events::Event::Empty(event) => {
                let name = xml_local_name(event.name().as_ref()).to_vec();
                typed = Some(match name.as_slice() {
                    b"nil" => Value::Null,
                    b"array" => Value::Array(Vec::new()),
                    b"struct" => Value::Object(serde_json::Map::new()),
                    _ => Value::String(String::new()),
                });
            }
            quick_xml::events::Event::End(event)
                if xml_local_name(event.name().as_ref()) == b"value" =>
            {
                // Untyped <value>text</value> is a string per the XML-RPC spec.
                // Trim only the untyped form (structural whitespace now reaches
                // us since trim_text is off); a typed <string> keeps its bytes.
                return Ok(typed.unwrap_or_else(|| {
                    Value::String(std::mem::take(&mut text).trim().to_string())
                }));
            }
            quick_xml::events::Event::Eof => return Err("unexpected EOF inside value".to_string()),
            _ => {}
        }
        buf.clear();
    }
}

fn parse_xmlrpc_array(
    reader: &mut quick_xml::Reader<&[u8]>,
    depth: usize,
) -> Result<Value, String> {
    let mut buf = Vec::new();
    let mut items = Vec::new();
    loop {
        match reader
            .read_event_into(&mut buf)
            .map_err(|error| error.to_string())?
        {
            quick_xml::events::Event::Start(event) => {
                let name = xml_local_name(event.name().as_ref()).to_vec();
                match name.as_slice() {
                    b"data" => {}
                    b"value" => items.push(parse_xmlrpc_value(reader, depth + 1)?),
                    other => {
                        return Err(format!(
                            "unexpected element '{}' in array",
                            String::from_utf8_lossy(other)
                        ));
                    }
                }
            }
            quick_xml::events::Event::Empty(event)
                if xml_local_name(event.name().as_ref()) == b"value" =>
            {
                items.push(Value::String(String::new()));
            }
            quick_xml::events::Event::End(event)
                if xml_local_name(event.name().as_ref()) == b"array" =>
            {
                return Ok(Value::Array(items));
            }
            quick_xml::events::Event::Eof => return Err("unexpected EOF inside array".to_string()),
            _ => {}
        }
        buf.clear();
    }
}

fn parse_xmlrpc_struct(
    reader: &mut quick_xml::Reader<&[u8]>,
    depth: usize,
) -> Result<Value, String> {
    let mut buf = Vec::new();
    let mut map = serde_json::Map::new();
    let mut member_name: Option<String> = None;
    loop {
        match reader
            .read_event_into(&mut buf)
            .map_err(|error| error.to_string())?
        {
            quick_xml::events::Event::Start(event) => {
                let name = xml_local_name(event.name().as_ref()).to_vec();
                match name.as_slice() {
                    b"member" => member_name = None,
                    b"name" => {
                        // Keys are identifiers; trim explicitly since trim_text is off.
                        member_name =
                            Some(read_xml_text_until_end(reader, b"name")?.trim().to_string());
                    }
                    b"value" => {
                        let value = parse_xmlrpc_value(reader, depth + 1)?;
                        let key = member_name
                            .take()
                            .ok_or_else(|| "struct value without name".to_string())?;
                        map.insert(key, value);
                    }
                    other => {
                        return Err(format!(
                            "unexpected element '{}' in struct",
                            String::from_utf8_lossy(other)
                        ));
                    }
                }
            }
            // Self-closing <value/> for a member is an empty string, matching the
            // <value/> handling in parse_xmlrpc_call/array; without it the member
            // is silently dropped (and even bypasses the unexpected-element guard).
            quick_xml::events::Event::Empty(event)
                if xml_local_name(event.name().as_ref()) == b"value" =>
            {
                let key = member_name
                    .take()
                    .ok_or_else(|| "struct value without name".to_string())?;
                map.insert(key, Value::String(String::new()));
            }
            quick_xml::events::Event::End(event)
                if xml_local_name(event.name().as_ref()) == b"struct" =>
            {
                return Ok(Value::Object(map));
            }
            quick_xml::events::Event::Eof => return Err("unexpected EOF inside struct".to_string()),
            _ => {}
        }
        buf.clear();
    }
}

fn xmlrpc_success_response(result: &Value) -> Response {
    let mut body = String::with_capacity(1024);
    body.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    body.push_str("<methodResponse><params><param>");
    write_xmlrpc_value(&mut body, result);
    body.push_str("</param></params></methodResponse>");
    (
        [(header::CONTENT_TYPE, HeaderValue::from_static("text/xml"))],
        body,
    )
        .into_response()
}

fn xmlrpc_fault_response(status: StatusCode, error: RpcError) -> Response {
    let mut body = String::with_capacity(256);
    body.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    body.push_str("<methodResponse><fault>");
    write_xmlrpc_value(
        &mut body,
        &json!({
            "faultCode": error.code,
            "faultString": error.message,
        }),
    );
    body.push_str("</fault></methodResponse>");
    (
        status,
        [(header::CONTENT_TYPE, HeaderValue::from_static("text/xml"))],
        body,
    )
        .into_response()
}

fn write_xmlrpc_value(out: &mut String, value: &Value) {
    use std::fmt::Write as _;

    out.push_str("<value>");
    match value {
        Value::Null => out.push_str("<nil/>"),
        Value::Bool(value) => {
            out.push_str("<boolean>");
            out.push(if *value { '1' } else { '0' });
            out.push_str("</boolean>");
        }
        Value::Number(value) => {
            // NZBGet emits unsigned 32-bit Lo/Hi counters as <i4> beyond the
            // signed 32-bit range; established clients parse them as 64-bit,
            // so mirroring that keeps large sizes intact.
            if let Some(int) = value.as_i64() {
                let _ = write!(out, "<i4>{int}</i4>");
            } else if let Some(int) = value.as_u64() {
                let _ = write!(out, "<i4>{int}</i4>");
            } else {
                let _ = write!(out, "<double>{}</double>", value.as_f64().unwrap_or(0.0));
            }
        }
        Value::String(value) => {
            out.push_str("<string>");
            push_xml_escaped(out, value);
            out.push_str("</string>");
        }
        Value::Array(items) => {
            out.push_str("<array><data>");
            for item in items {
                write_xmlrpc_value(out, item);
            }
            out.push_str("</data></array>");
        }
        Value::Object(map) => {
            out.push_str("<struct>");
            for (key, item) in map {
                out.push_str("<member><name>");
                push_xml_escaped(out, key);
                out.push_str("</name>");
                write_xmlrpc_value(out, item);
                out.push_str("</member>");
            }
            out.push_str("</struct>");
        }
    }
    out.push_str("</value>");
}

fn push_xml_escaped(out: &mut String, text: &str) {
    for ch in text.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            _ => out.push(ch),
        }
    }
}

struct AppendRequest {
    filename: Option<String>,
    content_or_url: String,
    category: Option<String>,
    priority: i64,
    add_paused: bool,
    dupe_key: Option<String>,
    dupe_score: Option<i64>,
    dupe_mode: Option<String>,
    parameters: Vec<(String, String)>,
}

async fn append(ctx: &NzbgetFacadeContext, params: Option<Value>) -> Result<Value, RpcError> {
    let request = parse_append_params(params)?;
    let frozen_post_processing_plan =
        resolve_nzbget_append_post_processing_plan(ctx, &request).await?;
    let (nzb_bytes, fetched_filename) = if is_http_url(&request.content_or_url) {
        match fetch_nzb_from_url(&ctx.http_client, &request.content_or_url).await {
            Ok(fetched) => fetched,
            Err(error) => return append_rejection_result(error),
        }
    } else {
        // Strip ASCII whitespace before decoding: with trim_text off, typed
        // <base64>/<string> content now reaches us verbatim, and base64 is
        // canonically line-wrapped — the STANDARD engine rejects embedded
        // whitespace as an invalid byte. Base64 is pure ASCII, so filter at
        // the byte level into a pre-sized buffer instead of `.chars()`,
        // which would pay a UTF-8 decode per byte for content that can run
        // to hundreds of megabytes.
        let source = request.content_or_url.as_bytes();
        let mut cleaned = Vec::with_capacity(source.len());
        cleaned.extend(
            source
                .iter()
                .copied()
                .filter(|byte| !byte.is_ascii_whitespace()),
        );
        let bytes = BASE64_STANDARD
            .decode(&cleaned)
            .map_err(|error| RpcError::invalid_parameter(format!("invalid base64: {error}")))?;
        (bytes, None)
    };

    let client_request_id = request
        .parameters
        .iter()
        .rev()
        .find_map(|(name, value)| (name == "drone").then(|| value.clone()));
    let mut attributes = vec![AttributeInput {
        key: PRIORITY_ATTRIBUTE_KEY.to_string(),
        value: priority_label(request.priority).to_string(),
    }];
    let dupe_key = request
        .dupe_key
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let dupe_mode = request
        .dupe_mode
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    if let Some(value) = dupe_key.clone() {
        attributes.push(AttributeInput {
            key: "nzbget.dupe_key".to_string(),
            value,
        });
    }
    if let Some(value) = request.dupe_score {
        attributes.push(AttributeInput {
            key: "nzbget.dupe_score".to_string(),
            value: value.to_string(),
        });
    }
    if let Some(value) = dupe_mode.clone() {
        attributes.push(AttributeInput {
            key: "nzbget.dupe_mode".to_string(),
            value,
        });
    }

    let metadata = submit_metadata(Some(attributes), client_request_id)
        .map_err(RpcError::invalid_parameter)?;
    let submitted = submit_nzb_bytes_with_options(
        &ctx.db,
        &ctx.handle,
        &ctx.config,
        &nzb_bytes,
        request.filename.or(fetched_filename),
        None,
        request.category.clone(),
        metadata,
        SubmissionOptions {
            category_resolution: CategoryResolutionMode::ResolveConfigured,
            add_paused: request.add_paused,
            duplicate_mode: nzbget_duplicate_mode(
                dupe_mode.as_deref(),
                dupe_key.is_some(),
                request.dupe_score.is_some(),
            ),
            semantic_duplicate: dupe_key.as_deref().and_then(|key| {
                weaver_server_core::SemanticDuplicate::from_source(
                    key,
                    request.dupe_score.unwrap_or_default(),
                )
            }),
            origin: weaver_server_core::SubmissionOrigin::NzbGet,
            frozen_post_processing_plan,
            ..SubmissionOptions::default()
        },
    )
    .await;

    match submitted {
        Ok(submitted) => Ok(json!(submitted.job_id.0)),
        Err(error) => append_rejection_result(error),
    }
}

async fn resolve_nzbget_append_post_processing_plan(
    ctx: &NzbgetFacadeContext,
    request: &AppendRequest,
) -> Result<Option<weaver_server_core::post_processing::model::FrozenPlan>, RpcError> {
    let db = ctx.db.clone();
    let parameters = request.parameters.clone();
    tokio::task::spawn_blocking(move || resolve_nzbget_post_processing_plan(&db, &parameters))
        .await
        .map_err(|error| RpcError::invalid_parameter(format!("append failed: {error}")))?
        .map_err(|error| RpcError::invalid_parameter(format!("append failed: {error}")))
}

fn resolve_nzbget_post_processing_plan(
    db: &Database,
    parameters: &[(String, String)],
) -> Result<
    Option<weaver_server_core::post_processing::model::FrozenPlan>,
    weaver_server_core::StateError,
> {
    let script_flags = parameters
        .iter()
        .filter_map(|(name, value)| name.strip_suffix(':').map(|name| (name, value)))
        .collect::<Vec<_>>();
    if script_flags.is_empty() {
        return Ok(None);
    }

    let revisions = db.list_extension_revisions()?;
    let mut recognized = false;
    let mut selections = Vec::new();
    for (name, value) in script_flags {
        let enabled = matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "yes" | "on" | "1"
        );
        let record = revisions.iter().find(|record| {
            record.trust_state == weaver_server_core::post_processing::model::TrustState::Approved
                && record
                    .manifest
                    .compatibility_name()
                    .is_some_and(|compatibility| compatibility.as_str() == name)
        });
        let Some(record) = record else {
            if enabled {
                return Err(weaver_server_core::StateError::Database(format!(
                    "approved post-processing extension '{name}' was not found"
                )));
            }
            continue;
        };
        recognized = true;
        if enabled {
            selections.push(
                weaver_server_core::post_processing::model::ExtensionSelection::pinned(
                    record.manifest.revision().extension_id().clone(),
                    record.manifest.revision().revision_id().clone(),
                ),
            );
        }
    }

    if !recognized {
        return Ok(None);
    }
    let selection = if selections.is_empty() {
        weaver_server_core::post_processing::model::SubmissionPlanSelection::disabled()
    } else {
        weaver_server_core::post_processing::model::SubmissionPlanSelection::extensions(selections)
            .map_err(|error| weaver_server_core::StateError::Database(error.to_string()))?
    };
    db.freeze_submission_post_processing_plan(Some(&selection))
}

fn append_rejection_result(error: SubmitNzbError) -> Result<Value, RpcError> {
    match error {
        SubmitNzbError::Parse(_) | SubmitNzbError::Empty | SubmitNzbError::NotXml => Ok(json!(0)),
        SubmitNzbError::DuplicateBlocked { .. } => Ok(json!(0)),
        error => Err(RpcError::invalid_parameter(error.to_string())),
    }
}

fn nzbget_duplicate_mode(
    requested_mode: Option<&str>,
    _has_dupe_key: bool,
    _has_dupe_score: bool,
) -> weaver_server_core::DuplicateMode {
    requested_mode
        .and_then(weaver_server_core::DuplicateMode::from_persisted)
        // NZBGet defaults every append to SCORE. Without a non-empty key this
        // only leaves semantic grouping absent; fingerprint policy still runs.
        .unwrap_or(weaver_server_core::DuplicateMode::Score)
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod append_rejection_tests {
    use super::*;

    #[test]
    fn parse_xmlrpc_call_resolves_entity_references() {
        // quick-xml 0.41 tokenizes each XML entity (&amp; &lt; &#65; …) as its own
        // event; the parser must resolve them, not fault. Real payloads this
        // guards: indexer URLs (with `&`), filenames, and unpack passwords with
        // & < > ". Covers a typed <string> and the untyped <value> form.
        let body = concat!(
            r#"<?xml version="1.0"?><methodCall><methodName>editqueue</methodName><params>"#,
            r#"<param><value><string>GroupSetParameter</string></value></param>"#,
            r#"<param><value><string>*Unpack:Password=p@ss&amp;w0rd &quot;x&quot; &lt;y&gt; &#65;</string></value></param>"#,
            r#"<param><value>plain &amp; untyped</value></param>"#,
            r#"</params></methodCall>"#,
        );
        let call = parse_xmlrpc_call(body.as_bytes()).expect("entity-bearing call must parse");
        assert_eq!(call.method, "editqueue");
        assert_eq!(
            call.params[1].as_str().unwrap(),
            "*Unpack:Password=p@ss&w0rd \"x\" <y> A"
        );
        assert_eq!(call.params[2].as_str().unwrap(), "plain & untyped");
    }

    #[test]
    fn append_rejection_result_returns_zero_for_parse_empty_and_notxml() {
        let parse_error = weaver_nzb::parse_nzb(b"not an nzb").unwrap_err();
        assert_eq!(
            append_rejection_result(SubmitNzbError::Parse(parse_error)).unwrap(),
            json!(0)
        );
        assert_eq!(
            append_rejection_result(SubmitNzbError::Empty).unwrap(),
            json!(0)
        );
        assert_eq!(
            append_rejection_result(SubmitNzbError::NotXml).unwrap(),
            json!(0)
        );

        let error =
            append_rejection_result(SubmitNzbError::Fetch("not allowed".into())).unwrap_err();
        assert_eq!(error.code, 2);
        assert!(error.message.contains("not allowed"));
    }

    #[test]
    fn nzbget_dupe_metadata_defaults_to_score_and_modes_are_case_insensitive() {
        use weaver_server_core::DuplicateMode;

        assert_eq!(
            nzbget_duplicate_mode(None, true, false),
            DuplicateMode::Score
        );
        assert_eq!(
            nzbget_duplicate_mode(None, false, true),
            DuplicateMode::Score
        );
        assert_eq!(
            nzbget_duplicate_mode(Some("aLl"), true, true),
            DuplicateMode::All
        );
        assert_eq!(
            nzbget_duplicate_mode(Some("FoRcE"), true, true),
            DuplicateMode::Force
        );
        assert_eq!(
            nzbget_duplicate_mode(None, false, false),
            DuplicateMode::Score
        );
    }

    #[test]
    fn append_parses_legacy_and_current_pp_parameter_layouts_in_last_value_order() {
        let legacy = parse_append_params(Some(json!([
            "legacy.nzb",
            "payload",
            "",
            0,
            false,
            false,
            "",
            0,
            "all",
            ["First:", "yes", "Second:", "on", "First:", "off"]
        ])))
        .unwrap();
        assert_eq!(
            legacy.parameters,
            vec![
                ("Second:".to_string(), "on".to_string()),
                ("First:".to_string(), "off".to_string())
            ]
        );

        let current = parse_append_params(Some(json!([
            "current.nzb",
            "payload",
            "",
            0,
            false,
            false,
            "",
            0,
            "all",
            true,
            ["First:", "1"]
        ])))
        .unwrap();
        assert_eq!(
            current.parameters,
            vec![("First:".to_string(), "1".to_string())]
        );
    }

    #[test]
    fn nzbget_append_scripts_resolve_to_approved_pinned_revisions() {
        use weaver_server_core::post_processing::discovery::{
            DiscoveryOptions, discover_and_record_extensions,
        };
        use weaver_server_core::post_processing::model::FrozenPlanProvenance;

        let db = Database::open_in_memory().unwrap();
        let data_dir = tempfile::tempdir().unwrap();
        let package = data_dir.path().join("scripts/email");
        std::fs::create_dir_all(&package).unwrap();
        std::fs::write(
            package.join("manifest.json"),
            include_str!(
                "../../../../crates/weaver-server-core/src/post_processing/fixtures/nzbget-v2-post-processing-manifest.json"
            ),
        )
        .unwrap();
        std::fs::write(package.join("email.py"), "#!/usr/bin/env python3\n").unwrap();
        let discovered = discover_and_record_extensions(
            &db,
            data_dir.path(),
            DiscoveryOptions {
                enabled: true,
                bare_script_adapter: None,
            },
            10,
        )
        .unwrap();
        let manifest = &discovered[0].manifest;
        db.approve_extension_revision(
            manifest.revision().extension_id(),
            manifest.revision().revision_id(),
            "/managed/example",
            20,
        )
        .unwrap();

        let compatibility_name = manifest.compatibility_name().unwrap().as_str();
        let flag = format!("{compatibility_name}:");
        let plan = resolve_nzbget_post_processing_plan(&db, &[(flag.clone(), "yes".to_string())])
            .unwrap()
            .unwrap();
        assert_eq!(plan.steps().len(), 1);
        assert_eq!(plan.steps()[0].revision(), manifest.revision());
        assert!(matches!(plan.provenance(), FrozenPlanProvenance::Explicit));

        let disabled = resolve_nzbget_post_processing_plan(&db, &[(flag, "no".to_string())])
            .unwrap()
            .unwrap();
        assert!(disabled.steps().is_empty());
        assert!(matches!(
            disabled.provenance(),
            FrozenPlanProvenance::Disabled
        ));
        assert!(
            resolve_nzbget_post_processing_plan(
                &db,
                &[("missing-script:".to_string(), "on".to_string())],
            )
            .is_err()
        );
        assert_eq!(
            resolve_nzbget_post_processing_plan(
                &db,
                &[("missing-script:".to_string(), "off".to_string())],
            )
            .unwrap(),
            None
        );
    }
}

fn parse_append_params(params: Option<Value>) -> Result<AppendRequest, RpcError> {
    let params = positional_params(params)?;
    if params.len() < 2 {
        return Err(RpcError::invalid_parameter(
            "append requires at least filename and content",
        ));
    }

    Ok(AppendRequest {
        filename: optional_string_param(&params, 0)?,
        content_or_url: required_string_param(&params, 1)?,
        category: optional_string_param(&params, 2)?,
        priority: optional_i64_param(&params, 3)?.unwrap_or(0),
        add_paused: optional_bool_param(&params, 5)?.unwrap_or(false),
        dupe_key: optional_string_param(&params, 6)?,
        dupe_score: optional_i64_param(&params, 7)?,
        dupe_mode: optional_string_param(&params, 8)?,
        parameters: parse_parameter_pairs(params.get(append_parameter_index(&params)))?,
    })
}

fn append_parameter_index(params: &[Value]) -> usize {
    match params.get(9) {
        Some(Value::Bool(_) | Value::Number(_)) => 10,
        Some(Value::String(value))
            if matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "true" | "false" | "yes" | "no" | "on" | "off" | "1" | "0"
            ) =>
        {
            10
        }
        _ => 9,
    }
}

/// Legacy (pre-v13) `appendurl(NZBFilename, Category, Priority, AddToTop,
/// URL)` returning bool. nzb360 still submits URL adds through this shape;
/// note the category/priority positions differ from `append` and the URL sits
/// last. Clients that instead mirror append's ordering (URL in the content
/// slot) are handled by delegating verbatim.
async fn append_url(ctx: &NzbgetFacadeContext, params: Option<Value>) -> Result<Value, RpcError> {
    let params = positional_params(params)?;
    let url_at = |index: usize| {
        optional_string_param(&params, index)
            .ok()
            .flatten()
            .filter(|value| is_http_url(value))
    };

    let append_shaped = if let Some(url) = url_at(4) {
        vec![
            params.first().cloned().unwrap_or(Value::Null),
            Value::String(url),
            params.get(1).cloned().unwrap_or(Value::Null),
            params.get(2).cloned().unwrap_or(Value::Null),
        ]
    } else if url_at(1).is_some() {
        params
    } else {
        return Err(RpcError::invalid_parameter(
            "appendurl requires an http(s) URL",
        ));
    };
    let result = append(ctx, Some(Value::Array(append_shaped))).await?;
    // appendurl reports success as a bool, not the created NZBID.
    let succeeded = result.as_i64().unwrap_or(0) > 0;
    Ok(json!(succeeded))
}

async fn status(ctx: &NzbgetFacadeContext) -> Result<Value, RpcError> {
    // Iterate `JobInfo` directly instead of mapping every job through
    // `queue_item_from_job` (which deep-clones name/metadata/phase_progress)
    // just to sum bytes and count states. Classify via
    // `queue_item_state_from_job_info` — the SAME projection `queue_item_from_job`
    // uses — not `QueueItemState::from(&status)`: the pipeline force-projects
    // `download_state = Downloading` for a post-processing job with pending
    // download work, so classifying off `status` alone would flip `ServerStandBy`
    // and the post/par counts for those jobs.
    let jobs = ctx.handle.list_jobs();
    let states: Vec<(QueueItemState, u64, u64)> = jobs
        .iter()
        .map(|job| {
            (
                weaver_server_api::queue_item_state_from_job_info(job),
                job.total_bytes,
                job.downloaded_bytes,
            )
        })
        .collect();
    let remaining = states
        .iter()
        .filter(|(state, _, _)| {
            !matches!(state, QueueItemState::Completed | QueueItemState::Failed)
        })
        .map(|(_, total, downloaded)| total.saturating_sub(*downloaded))
        .sum::<u64>();
    let forced = 0u64;
    let post_job_count = jobs
        .iter()
        .zip(&states)
        .filter(|(job, (state, _, _))| {
            matches!(job.status, JobStatus::QueuedPostProcessing)
                || queue_item_state_in_post_processing(*state)
        })
        .count();
    let any_active = states
        .iter()
        .any(|(state, _, _)| *state == QueueItemState::Downloading);
    let metrics = ctx.handle.get_metrics();
    let download_rate = metrics.current_download_speed;
    let arr_download_rate = download_rate.min(i32::MAX as u64);
    let uptime_secs = ctx.started_at.elapsed().as_secs();
    // NZBGet's DownloadedSize/AverageDownloadRate are since-server-start. Use the
    // in-memory session counter alone; folding in persisted queue progress (a
    // recovered job's lifetime bytes) inflated both to garbage right after boot.
    let session_downloaded = metrics.bytes_downloaded;
    let average_rate = session_downloaded / uptime_secs.max(1);
    let arr_average_rate = average_rate.min(i32::MAX as u64);
    let download_paused = ctx.handle.is_globally_paused();
    let download_block = ctx.handle.get_download_block();
    let quota_reached = matches!(
        download_block.kind,
        weaver_server_core::DownloadBlockKind::IspCap
            | weaver_server_core::DownloadBlockKind::ServerQuota
    );
    let config = ctx.config.read().await;
    let download_limit = config.max_download_speed.unwrap_or(0);
    let arr_download_limit = download_limit.min(i32::MAX as u64);
    let scan_paused = config.watch_folder.scanning_paused;
    let complete_dir = config.complete_dir();
    let news_servers = config
        .servers
        .iter()
        .map(|server| {
            json!({
                "ID": server.id,
                "Active": server.active,
            })
        })
        .collect::<Vec<_>>();
    drop(config);
    // Query free disk space only after the config guard is released and off
    // the async runtime thread: `disk_space` calls `statvfs` (or
    // `GetDiskFreeSpaceExW`), a blocking syscall that can stall for seconds
    // against an unhealthy NAS/NFS mount. Doing that while holding
    // `config.read()` would pin a tokio worker AND -- this being a
    // write-preferring `RwLock` -- convoy every other config reader behind
    // the stall. `free_disk_space_bytes` additionally caches the result for a
    // few seconds so a client polling `status` frequently doesn't hit the
    // syscall on every request.
    let free_disk = ctx.free_disk_space_bytes(complete_dir).await;
    // "Article cache" maps to weaver's in-flight decoded/buffered article
    // bytes: queued for decode, being decoded, and buffered for write.
    let article_cache = metrics
        .decode_pending_bytes
        .saturating_add(metrics.decode_active_bytes)
        .saturating_add(metrics.write_buffered_bytes);
    let thread_count = metrics.active_downloads;
    let (remaining_lo, remaining_hi) = size_parts(remaining);
    let (forced_lo, forced_hi) = size_parts(forced);
    let (downloaded_lo, downloaded_hi) = size_parts(session_downloaded);
    let (rate_lo, rate_hi) = size_parts(download_rate);
    let (average_lo, average_hi) = size_parts(average_rate);
    let (free_disk_lo, free_disk_hi) = size_parts(free_disk);
    let (article_cache_lo, article_cache_hi) = size_parts(article_cache);
    let resume_time = ctx.scheduled_resume.resume_at().await;

    Ok(json!({
        "RemainingSizeLo": remaining_lo,
        "RemainingSizeHi": remaining_hi,
        "RemainingSizeMB": bytes_to_mib(remaining),
        "ForcedSizeLo": forced_lo,
        "ForcedSizeHi": forced_hi,
        "ForcedSizeMB": bytes_to_mib(forced),
        "DownloadedSizeLo": downloaded_lo,
        "DownloadedSizeHi": downloaded_hi,
        "DownloadedSizeMB": bytes_to_mib(session_downloaded),
        "ArticleCacheLo": article_cache_lo,
        "ArticleCacheHi": article_cache_hi,
        "ArticleCacheMB": bytes_to_mib(article_cache),
        "DownloadRate": arr_download_rate,
        "DownloadRateLo": rate_lo,
        "DownloadRateHi": rate_hi,
        "AverageDownloadRate": arr_average_rate,
        "AverageDownloadRateLo": average_lo,
        "AverageDownloadRateHi": average_hi,
        "DownloadLimit": arr_download_limit,
        "ThreadCount": thread_count,
        "ParJobCount": post_job_count,
        "PostJobCount": post_job_count,
        "UrlCount": 0,
        "UpTimeSec": uptime_secs,
        "DownloadTimeSec": uptime_secs,
        "ServerTime": unix_now_secs(),
        "ResumeTime": resume_time,
        "ServerPaused": download_paused,
        "DownloadPaused": download_paused,
        "Download2Paused": download_paused,
        "ServerStandBy": !any_active,
        "PostPaused": ctx.handle.is_post_processing_paused(),
        "ScanPaused": scan_paused,
        "QuotaReached": quota_reached,
        "FeedActive": false,
        "QueueScriptCount": 0,
        "NewsServers": news_servers,
        "FreeDiskSpaceLo": free_disk_lo,
        "FreeDiskSpaceHi": free_disk_hi,
        "FreeDiskSpaceMB": bytes_to_mib(free_disk),
    }))
}

/// `status()` iterates `SchedulerHandle::list_jobs()` directly (avoiding a
/// per-job `QueueItem` deep-clone) and classifies each job with
/// `queue_item_state_from_job_info`, so it needs the check on a bare state.
fn queue_item_state_in_post_processing(state: QueueItemState) -> bool {
    matches!(
        state,
        QueueItemState::Checking
            | QueueItemState::Verifying
            | QueueItemState::Repairing
            | QueueItemState::Extracting
            | QueueItemState::Finalizing
            | QueueItemState::PostProcessing
    )
}

async fn listgroups(ctx: &NzbgetFacadeContext) -> Result<Value, RpcError> {
    let groups = ctx
        .handle
        .list_jobs()
        .iter()
        .map(queue_item_from_job)
        .filter(|item| {
            !matches!(
                item.state,
                QueueItemState::Completed | QueueItemState::Failed
            )
        })
        .map(|item| nzbget_group(&item))
        .collect::<Vec<_>>();
    Ok(Value::Array(groups))
}

fn nzbget_group(item: &QueueItem) -> Value {
    let total = item.total_bytes;
    let downloaded = item.downloaded_bytes;
    let remaining = total.saturating_sub(downloaded);
    let paused = if item.state == QueueItemState::Paused {
        remaining
    } else {
        0
    };
    let (file_lo, file_hi) = size_parts(total);
    let (remaining_lo, remaining_hi) = size_parts(remaining);
    let (paused_lo, paused_hi) = size_parts(paused);
    let (downloaded_lo, downloaded_hi) = size_parts(downloaded);
    let priority = nzbget_priority(&item.attributes);
    let output_dir = item.output_dir.clone().unwrap_or_default();
    let post_info = post_progress_info(item);
    let critical_health = critical_health(item);

    let mut group = json!({
        "FirstID": item.id,
        "LastID": item.id,
        "NZBID": item.id,
        "Name": item.name,
        "NZBName": item.name,
        "NZBNicename": item.display_title,
        "Kind": "NZB",
        "URL": "",
        "NZBFilename": item.name,
        "DestDir": output_dir,
        "FinalDir": output_dir,
        "Category": item.category.clone().unwrap_or_default(),
        "FileSizeLo": file_lo,
        "FileSizeHi": file_hi,
        "FileSizeMB": bytes_to_mib(total),
        "RemainingSizeLo": remaining_lo,
        "RemainingSizeHi": remaining_hi,
        "RemainingSizeMB": bytes_to_mib(remaining),
        "PausedSizeLo": paused_lo,
        "PausedSizeHi": paused_hi,
        "PausedSizeMB": bytes_to_mib(paused),
        "DownloadedSizeLo": downloaded_lo,
        "DownloadedSizeHi": downloaded_hi,
        "DownloadedSizeMB": bytes_to_mib(downloaded),
    });
    let extension = json!({
        "MinPriority": priority,
        "MaxPriority": priority,
        "MinPostTime": 0,
        "MaxPostTime": 0,
        "ActiveDownloads": active_downloads(item),
        "Status": nzbget_group_status(item),
        "FileCount": item.file_count,
        "RemainingFileCount": item.remaining_file_count,
        "RemainingParCount": item.remaining_par_count,
        "Health": item.health,
        "CriticalHealth": critical_health,
        "DupeKey": attribute_value(&item.attributes, "nzbget.dupe_key").unwrap_or_default(),
        "DupeScore": attribute_value(&item.attributes, "nzbget.dupe_score")
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(0),
        "DupeMode": attribute_value(&item.attributes, "nzbget.dupe_mode")
            .unwrap_or_else(|| "SCORE".to_string()),
        "PostInfoText": post_info.text,
        "PostStageProgress": post_info.stage_progress,
        "PostTotalTimeSec": post_info.total_time_sec,
        "PostStageTimeSec": post_info.stage_time_sec,
        "Parameters": response_parameters(item.client_request_id.as_deref(), &item.attributes),
        "ScriptStatuses": [],
        "ServerStats": [],
        "MessageCount": 0,
    });
    merge_json_objects(&mut group, extension);
    group
}

/// Merge `extension`'s top-level keys into `base`. Both arguments come from
/// `json!` object literals split to stay under the macro recursion limit.
fn merge_json_objects(base: &mut Value, extension: Value) {
    if let (Value::Object(base), Value::Object(extension)) = (base, extension) {
        base.extend(extension);
    }
}

struct PostProgressInfo {
    text: String,
    /// 0..1000 like NZBGet's PostStageProgress.
    stage_progress: u64,
    total_time_sec: u64,
    stage_time_sec: u64,
}

/// Derive NZBGet-style post-processing progress from weaver's phase progress
/// entries. Returns "NONE" text when the job is not in a post stage.
fn post_progress_info(item: &QueueItem) -> PostProgressInfo {
    let phase = match item.state {
        QueueItemState::Repairing => Some(QueuePhase::Repairing),
        QueueItemState::Extracting => Some(QueuePhase::Extracting),
        QueueItemState::Finalizing => Some(QueuePhase::Moving),
        _ => None,
    };
    let label = match item.state {
        QueueItemState::Checking | QueueItemState::Verifying => Some("Verifying"),
        QueueItemState::Repairing => Some("Repairing"),
        QueueItemState::Extracting => Some("Unpacking"),
        QueueItemState::Finalizing => Some("Moving"),
        _ => None,
    };
    let Some(label) = label else {
        return PostProgressInfo {
            text: "NONE".to_string(),
            stage_progress: 0,
            total_time_sec: 0,
            stage_time_sec: 0,
        };
    };
    let progress = phase.and_then(|phase| {
        item.phase_progress
            .iter()
            .find(|entry| entry.phase == phase)
    });
    let stage_progress = progress
        .map(|entry| (entry.progress_percent.clamp(0.0, 100.0) * 10.0) as u64)
        .unwrap_or(0);
    let stage_time_sec = progress
        .map(|entry| {
            ((entry.updated_at_epoch_ms - entry.started_at_epoch_ms) / 1000.0).max(0.0) as u64
        })
        .unwrap_or(0);
    let text = match progress {
        Some(entry) if entry.progress_percent > 0.0 => {
            format!("{label} ({:.0}%)", entry.progress_percent.clamp(0.0, 100.0))
        }
        _ => label.to_string(),
    };
    PostProgressInfo {
        text,
        stage_progress,
        total_time_sec: stage_time_sec,
        stage_time_sec,
    }
}

/// NZBGet's CriticalHealth: the health floor below which the download cannot
/// be repaired. Approximated from the recovery volume share of the job.
fn critical_health(item: &QueueItem) -> u64 {
    if item.total_bytes == 0 {
        return 1000;
    }
    let payload = item
        .total_bytes
        .saturating_sub(item.optional_recovery_bytes);
    (payload * 1000) / item.total_bytes
}

fn attribute_value(attributes: &[Attribute], key: &str) -> Option<String> {
    attributes
        .iter()
        .find(|attribute| attribute.key.eq_ignore_ascii_case(key))
        .map(|attribute| attribute.value.clone())
}

async fn history(ctx: &NzbgetFacadeContext) -> Result<Value, RpcError> {
    let db = ctx.db.clone();
    let rows = tokio::task::spawn_blocking(move || {
        db.list_job_history(&HistoryFilter {
            limit: Some(1000),
            ..HistoryFilter::default()
        })
    })
    .await
    .map_err(|error| RpcError::invalid_parameter(format!("history unavailable: {error}")))?
    .map_err(|error| RpcError::invalid_parameter(format!("history unavailable: {error}")))?;
    let db = ctx.db.clone();
    let post_processing_runs = tokio::task::spawn_blocking(move || {
        let mut latest = HashMap::new();
        for run in db.list_post_processing_runs(None, 1000)? {
            latest.entry(run.job_id).or_insert(run);
        }
        Ok::<_, weaver_server_core::StateError>(latest)
    })
    .await
    .map_err(|error| RpcError::invalid_parameter(format!("history unavailable: {error}")))?
    .map_err(|error| RpcError::invalid_parameter(format!("history unavailable: {error}")))?;
    let terminal_items = ctx
        .handle
        .list_jobs()
        .iter()
        .map(queue_item_from_job)
        .filter(|item| {
            matches!(
                item.state,
                QueueItemState::Completed | QueueItemState::Failed
            )
        })
        .collect::<Vec<_>>();

    let ids: Vec<u64> = terminal_items
        .iter()
        .map(|item| item.id)
        .chain(rows.iter().map(|row| row.job_id))
        .collect();
    let db = ctx.db.clone();
    let stage_bounds = tokio::task::spawn_blocking(move || {
        db.get_job_event_stage_bounds(&ids, HISTORY_STAGE_KINDS)
    })
    .await
    .map_err(|error| RpcError::invalid_parameter(format!("history unavailable: {error}")))?
    .unwrap_or_default();

    let persisted_ids = rows.iter().map(|row| row.job_id).collect::<HashSet<_>>();
    let mut seen_terminal_ids = HashSet::with_capacity(terminal_items.len());
    let mut items = terminal_items
        .iter()
        .filter(|item| !persisted_ids.contains(&item.id) && seen_terminal_ids.insert(item.id))
        .map(|item| nzbget_history_queue_item(item, history_timings(stage_bounds.get(&item.id))))
        .collect::<Vec<_>>();

    // DB-row-derived entries are the immutable-parse path: a history row's
    // `completed_at` never changes once written, so memoize the PARSED
    // `HistoryItem` by job id and reuse it on a hit instead of re-running
    // `history_item_from_row` (metadata + release-name parse) for up to 1000
    // rows every poll. The final entry still merges FRESH per-poll stage
    // timings (from `job_events`, which a reprocess can append to without
    // touching `completed_at`), so it is never served stale. The in-memory
    // terminal QueueItem fallbacks above are emitted only until their durable
    // row is visible and are rebuilt every call because they are not immutable.
    {
        let mut cache = ctx.history_cache.lock().await;
        // Simple bound: a long-running server accumulates one entry per
        // historical job id ever polled, so drop everything once it grows
        // past a generous ceiling rather than tracking per-entry recency.
        if cache.len() > 4000 {
            cache.clear();
        }
        for row in &rows {
            let cached = cache
                .get(&row.job_id)
                .filter(|(completed_at, _)| *completed_at == row.completed_at)
                .map(|(_, item)| item.clone());
            let item = cached.unwrap_or_else(|| {
                let item = history_item_from_row(row, None);
                cache.insert(row.job_id, (row.completed_at, item.clone()));
                item
            });
            items.push(nzbget_history_item(
                &item,
                history_timings(stage_bounds.get(&row.job_id)),
            ));
        }
    }
    for item in &mut items {
        let Some(job_id) = item.get("NZBID").and_then(Value::as_u64) else {
            continue;
        };
        let Some(run) = post_processing_runs.get(&job_id) else {
            continue;
        };
        let script_status = match run.summary {
            weaver_server_core::post_processing::model::PostProcessingSummary::NotRun => "NONE",
            weaver_server_core::post_processing::model::PostProcessingSummary::Succeeded
            | weaver_server_core::post_processing::model::PostProcessingSummary::Warning => {
                "SUCCESS"
            }
            weaver_server_core::post_processing::model::PostProcessingSummary::Failed
            | weaver_server_core::post_processing::model::PostProcessingSummary::Cancelled
            | weaver_server_core::post_processing::model::PostProcessingSummary::Interrupted => {
                "FAILURE"
            }
        };
        if let Some(object) = item.as_object_mut() {
            object.insert("ScriptStatus".to_string(), json!(script_status));
            object.insert(
                "ScriptStatuses".to_string(),
                json!([{
                    "Name": format!("weaver:{}", run.run_id.as_str()),
                    "Status": script_status,
                }]),
            );
        }
    }
    Ok(Value::Array(items))
}

/// Stage-boundary event kinds consulted for history duration fields.
const HISTORY_STAGE_KINDS: &[&str] = &[
    "DownloadStarted",
    "DownloadFinished",
    "JobVerificationStarted",
    "JobVerificationComplete",
    "VerificationStarted",
    "VerificationComplete",
    "RepairStarted",
    "RepairComplete",
    "RepairFailed",
    "ExtractionReady",
    "ExtractionComplete",
    "ExtractionFailed",
    "MoveToCompleteStarted",
    "MoveToCompleteFinished",
];

#[derive(Debug, Clone, Copy, Default)]
struct HistoryTimings {
    download_sec: u64,
    par_sec: u64,
    repair_sec: u64,
    unpack_sec: u64,
    post_total_sec: u64,
}

/// Derive NZBGet-style stage durations from per-kind (min,max) event
/// timestamp bounds. Bounds are epoch milliseconds; results are seconds.
fn history_timings(bounds: Option<&Vec<(String, i64, i64)>>) -> HistoryTimings {
    let Some(bounds) = bounds else {
        return HistoryTimings::default();
    };
    let first = |kinds: &[&str]| {
        bounds
            .iter()
            .filter(|(kind, _, _)| kinds.contains(&kind.as_str()))
            .map(|(_, first_ts, _)| *first_ts)
            .min()
    };
    let last = |kinds: &[&str]| {
        bounds
            .iter()
            .filter(|(kind, _, _)| kinds.contains(&kind.as_str()))
            .map(|(_, _, last_ts)| *last_ts)
            .max()
    };
    let span = |starts: &[&str], ends: &[&str]| -> u64 {
        match (first(starts), last(ends)) {
            (Some(start), Some(end)) if end > start => (end - start) as u64 / 1000,
            _ => 0,
        }
    };
    let download_sec = span(&["DownloadStarted"], &["DownloadFinished"]);
    let par_sec = span(
        &["JobVerificationStarted", "VerificationStarted"],
        &["JobVerificationComplete", "VerificationComplete"],
    );
    let repair_sec = span(&["RepairStarted"], &["RepairComplete", "RepairFailed"]);
    let unpack_sec = span(
        &["ExtractionReady"],
        &["ExtractionComplete", "ExtractionFailed"],
    );
    let move_sec = span(&["MoveToCompleteStarted"], &["MoveToCompleteFinished"]);
    HistoryTimings {
        download_sec,
        par_sec,
        repair_sec,
        unpack_sec,
        post_total_sec: par_sec + repair_sec + unpack_sec + move_sec,
    }
}

struct HistoryStatuses {
    par: &'static str,
    unpack: &'static str,
    mv: &'static str,
    script: &'static str,
    delete: &'static str,
    /// Compound NZBGet v13+ status like "SUCCESS/ALL" or "DELETED/MANUAL".
    /// Sonarr/Radarr ignore this and use the granular fields; nzb360 renders
    /// the compound form.
    status: &'static str,
}

fn history_statuses(cancelled: bool, failed: bool) -> HistoryStatuses {
    if cancelled {
        return HistoryStatuses {
            par: "NONE",
            unpack: "NONE",
            mv: "NONE",
            script: "NONE",
            delete: "MANUAL",
            status: "DELETED/MANUAL",
        };
    }
    if failed {
        // Weaver exposes no per-stage failure cause here, so we must not assert a
        // PAR failure (the old "FAILURE/PAR" + ParStatus=FAILURE steered clients
        // and Sonarr to par/repair diagnostics for password/unpack/health/move
        // failures alike). But Sonarr/Radarr decide failure ONLY from the granular
        // status fields, never the compound `Status` string — all-NONE would be
        // read as success and the failed release silently imported. So signal the
        // failure through `DeleteStatus = "HEALTH"`, which is in both clients'
        // delete-failed set and matches NZBGet's generic health-failure semantics,
        // while leaving par/unpack/move/script NONE (no false stage claim).
        return HistoryStatuses {
            par: "NONE",
            unpack: "NONE",
            mv: "NONE",
            script: "NONE",
            delete: "HEALTH",
            status: "FAILURE/HEALTH",
        };
    }
    HistoryStatuses {
        par: "SUCCESS",
        unpack: "SUCCESS",
        mv: "SUCCESS",
        script: "SUCCESS",
        delete: "NONE",
        status: "SUCCESS/ALL",
    }
}

#[allow(clippy::too_many_arguments)]
fn nzbget_history_entry(
    id: u64,
    name: &str,
    category: Option<&str>,
    total_bytes: u64,
    output_dir: &str,
    history_time: i64,
    statuses: HistoryStatuses,
    timings: HistoryTimings,
    health: u32,
    message: &str,
    client_request_id: Option<&str>,
    attributes: &[Attribute],
) -> Value {
    let (file_lo, file_hi) = size_parts(total_bytes);
    json!({
        "ID": id,
        "NZBID": id,
        "Kind": "NZB",
        "URL": "",
        "Name": name,
        "NZBName": name,
        "NZBNicename": name,
        "NZBFilename": name,
        "Category": category.unwrap_or_default(),
        "FileSizeLo": file_lo,
        "FileSizeHi": file_hi,
        "FileSizeMB": bytes_to_mib(total_bytes),
        "DestDir": output_dir,
        "FinalDir": output_dir,
        "HistoryTime": history_time,
        "DownloadTimeSec": timings.download_sec,
        "ParTimeSec": timings.par_sec,
        "RepairTimeSec": timings.repair_sec,
        "UnpackTimeSec": timings.unpack_sec,
        "PostTotalTimeSec": timings.post_total_sec,
        "ParStatus": statuses.par,
        "UnpackStatus": statuses.unpack,
        "MoveStatus": statuses.mv,
        "ScriptStatus": statuses.script,
        "DeleteStatus": statuses.delete,
        "MarkStatus": "NONE",
        "UrlStatus": "NONE",
        "Status": statuses.status,
        "Health": health,
        "CriticalHealth": 1000,
        "Deleted": statuses.delete != "NONE",
        "RetryData": false,
        "Message": message,
        "MessageCount": 0,
        "Parameters": response_parameters(client_request_id, attributes),
        "ScriptStatuses": [],
        "ServerStats": [],
    })
}

fn nzbget_history_item(item: &weaver_server_api::HistoryItem, timings: HistoryTimings) -> Value {
    let cancelled = item
        .attention
        .as_ref()
        .is_some_and(|attention| attention.code == "CANCELLED");
    let failed = item.state == QueueItemState::Failed && !cancelled;
    nzbget_history_entry(
        item.id,
        &item.name,
        item.category.as_deref(),
        item.total_bytes,
        &item.output_dir.clone().unwrap_or_default(),
        item.completed_at.timestamp(),
        history_statuses(cancelled, failed),
        timings,
        item.health,
        &item.error.clone().unwrap_or_default(),
        item.client_request_id.as_deref(),
        &item.attributes,
    )
}

fn nzbget_history_queue_item(item: &QueueItem, timings: HistoryTimings) -> Value {
    let failed = item.state == QueueItemState::Failed;
    nzbget_history_entry(
        item.id,
        &item.name,
        item.category.as_deref(),
        item.total_bytes,
        &item.output_dir.clone().unwrap_or_default(),
        // Stable per-poll timestamp. Using unix_now_secs() here made HistoryTime
        // advance on every poll (perpetually "newest", re-firing change
        // detection) and then jump backward once the row moved to DB history.
        // created_at is stable and never later than the eventual completed_at.
        item.created_at.timestamp(),
        history_statuses(false, failed),
        timings,
        item.health,
        &item.error.clone().unwrap_or_default(),
        item.client_request_id.as_deref(),
        &item.attributes,
    )
}

async fn config(ctx: &NzbgetFacadeContext) -> Result<Value, RpcError> {
    let config = ctx.config.read().await;
    let main_dir = config.data_dir.clone();
    let dest_dir = config.complete_dir();
    let script_dir = Path::new(&main_dir)
        .join("scripts")
        .to_string_lossy()
        .into_owned();
    let mut entries = vec![
        config_entry("KeepHistory", "7"),
        config_entry("MainDir", &main_dir),
        config_entry("DestDir", &dest_dir),
        config_entry("ScriptDir", &script_dir),
        config_entry("AppendCategoryDir", "yes"),
    ];

    let raw_categories = if config.categories.is_empty() {
        ["tv", "Movies", "Music", "Books", "Prowlarr"]
            .into_iter()
            .map(|name| {
                (
                    name.to_string(),
                    category_dest_dir(&dest_dir, name),
                    String::new(),
                )
            })
            .collect::<Vec<_>>()
    } else {
        config
            .categories
            .iter()
            .map(|category| {
                (
                    category.name.clone(),
                    category
                        .dest_dir
                        .clone()
                        .unwrap_or_else(|| category_dest_dir(&dest_dir, &category.name)),
                    category.aliases.clone(),
                )
            })
            .collect::<Vec<_>>()
    };
    let categories = nzbget_config_categories(raw_categories);
    let category_names = categories
        .iter()
        .map(|(name, _, _)| name.clone())
        .collect::<Vec<_>>();
    let db = ctx.db.clone();
    let category_default_scripts = tokio::task::spawn_blocking(move || {
        let settings = db.post_processing_settings()?;
        let mut defaults = HashMap::new();
        if settings.execution_enabled {
            for category in category_names {
                let plan = db.resolve_post_processing_plan(None, Some(&category))?;
                let mut scripts = Vec::with_capacity(plan.steps().len());
                for step in plan.steps() {
                    let record = db
                        .extension_revision(
                            step.revision().extension_id(),
                            step.revision().revision_id(),
                        )?
                        .ok_or_else(|| {
                            weaver_server_core::StateError::Database(
                                "frozen category extension revision disappeared".to_string(),
                            )
                        })?;
                    scripts.push(
                        record
                            .manifest
                            .compatibility_name()
                            .map(|name| name.as_str())
                            .unwrap_or_else(|| record.manifest.entrypoint())
                            .to_string(),
                    );
                }
                defaults.insert(category, scripts.join(","));
            }
        }
        Ok::<_, weaver_server_core::StateError>(defaults)
    })
    .await
    .map_err(|error| RpcError::invalid_parameter(format!("config unavailable: {error}")))?
    .map_err(|error| RpcError::invalid_parameter(format!("config unavailable: {error}")))?;

    for (index, (name, dest_dir, aliases)) in categories.iter().enumerate() {
        let prefix = format!("Category{}.", index + 1);
        entries.push(config_entry(&format!("{prefix}Name"), name));
        entries.push(config_entry(&format!("{prefix}DestDir"), dest_dir));
        entries.push(config_entry(&format!("{prefix}Unpack"), "yes"));
        entries.push(config_entry(
            &format!("{prefix}DefScript"),
            category_default_scripts
                .get(name)
                .map(String::as_str)
                .unwrap_or_default(),
        ));
        entries.push(config_entry(&format!("{prefix}Aliases"), aliases));
    }

    // Expose weaver's RSS feeds as sequential NZBGet FeedN entries (Feed1..N).
    // NZBGet numbers feed sections contiguously; weaver's db ids are not
    // contiguous after a feed is deleted, so a client enumerating Feed1, Feed2…
    // would stop at the first gap and never see later feeds. viewfeed resolves
    // the same 1-based number back to the db id. The feed URL is deliberately
    // NOT emitted: it embeds the indexer API key and config/loadconfig are
    // reachable with a read-scoped key.
    let db = ctx.db.clone();
    let mut feeds = tokio::task::spawn_blocking(move || db.list_rss_feeds())
        .await
        .map_err(|error| RpcError::invalid_parameter(format!("config unavailable: {error}")))?
        .unwrap_or_default();
    feeds.sort_by_key(|feed| feed.id);
    for (index, feed) in feeds.iter().enumerate() {
        let prefix = format!("Feed{}.", index + 1);
        entries.push(config_entry(&format!("{prefix}Name"), &feed.name));
        entries.push(config_entry(
            &format!("{prefix}Interval"),
            &(feed.poll_interval_secs / 60).to_string(),
        ));
    }

    Ok(Value::Array(entries))
}

fn nzbget_config_categories(
    raw_categories: Vec<(String, String, String)>,
) -> Vec<(String, String, String)> {
    let mut categories = Vec::new();
    let mut seen_names = HashSet::new();

    for (name, dest_dir, aliases) in raw_categories {
        push_nzbget_config_category(
            &mut categories,
            &mut seen_names,
            name.clone(),
            dest_dir.clone(),
            aliases.clone(),
        );

        let lowercase_name = name.to_ascii_lowercase();
        if lowercase_name != name {
            push_nzbget_config_category(
                &mut categories,
                &mut seen_names,
                lowercase_name,
                dest_dir.clone(),
                String::new(),
            );
        }

        for alias in aliases.split(',').map(str::trim) {
            if alias.is_empty() || alias.contains('*') || alias.contains('?') {
                continue;
            }
            push_nzbget_config_category(
                &mut categories,
                &mut seen_names,
                alias.to_string(),
                dest_dir.clone(),
                String::new(),
            );
        }
    }

    categories
}

fn push_nzbget_config_category(
    categories: &mut Vec<(String, String, String)>,
    seen_names: &mut HashSet<String>,
    name: String,
    dest_dir: String,
    aliases: String,
) {
    if seen_names.insert(name.clone()) {
        categories.push((name, dest_dir, aliases));
    }
}

struct EditQueueRequest {
    command: String,
    param: String,
    /// Legacy 4-arg shape's integer Offset argument (nzb360 puts the
    /// GroupMoveOffset delta here); 0 when absent.
    offset: i64,
    ids: Vec<JobId>,
}

/// Parse both editqueue signatures:
/// - pre-v13 (Sonarr/Radarr): `[Command, Offset, Param, ID, ID, ...]`
/// - v13+ (nzb360 and modern clients): `[Command, Param, [IDs]]`
///
/// IDs may arrive as trailing scalars, one array, or a mix; all are flattened.
fn parse_editqueue_params(params: Option<Value>) -> Result<EditQueueRequest, RpcError> {
    let params = positional_params(params)?;
    if params.len() < 2 {
        return Err(RpcError::invalid_parameter(
            "editqueue requires a command and at least one id",
        ));
    }
    let command = required_string_param(&params, 0)?;

    let legacy_shape = params.len() >= 4 && params[1].is_number() && params[2].is_string();
    let offset = if legacy_shape {
        params[1].as_i64().unwrap_or(0)
    } else {
        0
    };
    let (param, id_values) = if legacy_shape {
        (
            params[2].as_str().unwrap_or_default().to_string(),
            &params[3..],
        )
    } else if params.len() == 2 {
        // `[Command, [IDs]]` — param omitted entirely.
        (String::new(), &params[1..])
    } else {
        let param = match &params[1] {
            Value::Null => String::new(),
            Value::String(value) => value.clone(),
            Value::Number(value) => value.to_string(),
            other => {
                return Err(RpcError::invalid_parameter(format!(
                    "unsupported editqueue param value: {other}"
                )));
            }
        };
        (param, &params[2..])
    };

    let mut ids = Vec::new();
    for value in id_values {
        match value {
            Value::Array(values) => {
                for value in values {
                    ids.push(parse_job_id_value(value)?);
                }
            }
            other => ids.push(parse_job_id_value(other)?),
        }
    }

    // A single call with an enormous id list (e.g. 100k ids) would otherwise
    // monopolize the orchestrator command loop -- for the per-id commands
    // (pause/resume/delete) every id is a scheduler round-trip. The cap has to
    // clear a legitimate "select all" on a large hoarder queue, so it sits well
    // above any real queue size while still bounding abuse two orders of
    // magnitude below the 100k pathological case. (Reorder is exempt from the
    // round-trip cost regardless: it batches into one `ReorderJobs` command.)
    const MAX_EDITQUEUE_IDS: usize = 10_000;
    if ids.len() > MAX_EDITQUEUE_IDS {
        return Err(RpcError::invalid_parameter(format!(
            "too many ids ({}), max {MAX_EDITQUEUE_IDS}",
            ids.len()
        )));
    }

    Ok(EditQueueRequest {
        command,
        param,
        offset,
        ids,
    })
}

/// Commands weaver recognizes but has no backing capability for. NZBGet's
/// editqueue reports per-call success as a bool; answering `false` (instead of
/// an "Invalid action" fault) tells clients the action failed without breaking
/// their RPC plumbing.
fn is_unsupported_editqueue_command(command: &str) -> bool {
    matches!(
        command,
        // Anchor-relative moves and sorting have no weaver equivalent; the
        // absolute moves (top/bottom/offset) map to the manual queue order.
        "groupmovebefore" | "groupmoveafter" | "groupsort"
        // Job renaming is not supported; names come from the NZB.
        | "groupsetname" | "historysetname"
        // Weaver has no per-file scheduling surface.
        | "filepause" | "fileresume" | "filedelete" | "filesetpriority" | "filemoveoffset"
        | "filemovetop" | "filemovebottom" | "filepauseallpars" | "filepauseextrapars"
        // History rows are immutable snapshots in weaver.
        | "historysetcategory" | "historysetparameter" | "historysetdupekey"
        | "historysetdupescore" | "historysetdupemode"
        // Weaver's duplicate arbitration handles bad-marking through its own
        // UI flow (candidate promotion), which this facade does not drive.
        | "historymarkbad" | "historymarksuccess"
    )
}

async fn editqueue(ctx: &NzbgetFacadeContext, params: Option<Value>) -> Result<Value, RpcError> {
    let request = parse_editqueue_params(params)?;
    let command = request.command.to_ascii_lowercase();
    if is_unsupported_editqueue_command(&command) {
        return Ok(json!(false));
    }
    if request.ids.is_empty() {
        return Ok(json!(false));
    }

    // Commands with whole-request semantics are answered before the per-id
    // loop below.
    match command.as_str() {
        // Weaver fetches recovery volumes on demand, so "don't download
        // unneeded pars" already holds for every job.
        "grouppauseallpars" | "grouppauseextrapars" => return Ok(json!(true)),
        "groupmovetop" | "groupmovebottom" | "groupmoveoffset" => {
            return reorder_groups(ctx, &request, &command).await;
        }
        _ => {}
    }

    for job_id in request.ids {
        let result = match command.as_str() {
            "grouppause" => ctx.handle.pause_job(job_id).await,
            "groupresume" => ctx.handle.resume_job(job_id).await,
            "groupdelete" | "groupparkdelete" | "groupdupedelete" => {
                ctx.handle.cancel_job(job_id).await
            }
            "groupfinaldelete" => group_final_delete(ctx, job_id).await,
            "groupsetcategory" | "groupapplycategory" => {
                set_job_category(ctx, job_id, &request.param).await
            }
            "groupsetpriority" => set_job_priority(ctx, job_id, &request.param).await,
            "groupsetparameter" => {
                let (name, value) = request.param.split_once('=').ok_or_else(|| {
                    RpcError::invalid_parameter("GroupSetParameter expects Name=Value")
                })?;
                // nzb360 sets unpack passwords through this parameter. It maps
                // to weaver's durable password override and stays out of the
                // visible metadata/Parameters listings.
                if name.trim().eq_ignore_ascii_case("*Unpack:Password") {
                    set_job_password(ctx, job_id, value).await
                } else {
                    set_job_parameter(ctx, job_id, name, value).await
                }
            }
            "groupsetdupekey" => {
                set_job_parameter(ctx, job_id, "nzbget.dupe_key", &request.param).await
            }
            "groupsetdupescore" => {
                set_job_parameter(ctx, job_id, "nzbget.dupe_score", &request.param).await
            }
            "groupsetdupemode" => {
                set_job_parameter(ctx, job_id, "nzbget.dupe_mode", &request.param).await
            }
            "historydelete" | "historyfinaldelete" => {
                ctx.handle.delete_history(job_id, false).await
            }
            "historyreturn" | "historyredownload" => ctx.handle.redownload_job(job_id).await,
            "historyprocess" => ctx.handle.reprocess_job(job_id).await,
            "historymarkgood" => mark_history_good(ctx, job_id).await,
            other => return Err(RpcError::invalid_action(other)),
        };
        match result {
            Ok(()) => {}
            Err(SchedulerError::JobNotFound(_)) => return Ok(json!(false)),
            Err(error) => {
                return Err(RpcError::invalid_parameter(format!(
                    "{} failed: {error}",
                    request.command
                )));
            }
        }
    }

    Ok(json!(true))
}

async fn group_final_delete(
    ctx: &NzbgetFacadeContext,
    job_id: JobId,
) -> Result<(), SchedulerError> {
    ctx.handle.cancel_job(job_id).await?;
    ctx.handle.delete_history(job_id, false).await
}

/// GroupMoveTop / GroupMoveBottom / GroupMoveOffset. The move delta arrives in
/// the legacy Offset argument (nzb360) or as the v13+ Param string.
async fn reorder_groups(
    ctx: &NzbgetFacadeContext,
    request: &EditQueueRequest,
    command: &str,
) -> Result<Value, RpcError> {
    let target = match command {
        "groupmovetop" => weaver_server_core::QueueMoveTarget::Top,
        "groupmovebottom" => weaver_server_core::QueueMoveTarget::Bottom,
        _ => {
            let offset = if request.offset != 0 {
                request.offset
            } else {
                request.param.trim().parse::<i64>().unwrap_or(0)
            };
            weaver_server_core::QueueMoveTarget::Offset(offset)
        }
    };
    // Preserve the moved set's relative order: for MoveTop the first listed id
    // must end topmost, so moves apply in reverse.
    let ids: Vec<JobId> = if matches!(target, weaver_server_core::QueueMoveTarget::Top) {
        request.ids.iter().rev().copied().collect()
    } else {
        request.ids.clone()
    };
    // One batched round-trip: `reorder_jobs` applies every move and then
    // persists+publishes once, instead of one scheduler round-trip (and one
    // persist+publish) per id.
    let moves = ids.into_iter().map(|job_id| (job_id, target)).collect();
    match ctx.handle.reorder_jobs(moves).await {
        Ok(()) => Ok(json!(true)),
        Err(SchedulerError::JobNotFound(_)) => Ok(json!(false)),
        Err(error) => Err(RpcError::invalid_parameter(format!(
            "{command} failed: {error}"
        ))),
    }
}

async fn set_job_category(
    ctx: &NzbgetFacadeContext,
    job_id: JobId,
    category: &str,
) -> Result<(), SchedulerError> {
    let category = category.trim();
    let update = JobUpdate {
        category: if category.is_empty() {
            FieldUpdate::Clear
        } else {
            FieldUpdate::Set(category.to_string())
        },
        ..JobUpdate::default()
    };
    ctx.handle.update_job(job_id, update).await
}

async fn set_job_password(
    ctx: &NzbgetFacadeContext,
    job_id: JobId,
    password: &str,
) -> Result<(), SchedulerError> {
    let update = JobUpdate {
        password: if password.is_empty() {
            FieldUpdate::Clear
        } else {
            FieldUpdate::Set(password.to_string())
        },
        ..JobUpdate::default()
    };
    ctx.handle.update_job(job_id, update).await
}

async fn set_job_priority(
    ctx: &NzbgetFacadeContext,
    job_id: JobId,
    param: &str,
) -> Result<(), SchedulerError> {
    // NZBGet priorities are numbers (-100 very low .. 900 force); weaver keeps
    // the three-level label, which genuinely biases dispatch. Some clients
    // send the level name instead of a number; FORCE collapses to HIGH.
    let trimmed = param.trim();
    let label = if let Ok(numeric) = trimmed.parse::<i64>() {
        priority_label(numeric)
    } else if trimmed.eq_ignore_ascii_case("force") || trimmed.eq_ignore_ascii_case("high") {
        "HIGH"
    } else if trimmed.eq_ignore_ascii_case("low") {
        "LOW"
    } else {
        "NORMAL"
    };
    upsert_job_metadata(ctx, job_id, PRIORITY_ATTRIBUTE_KEY, Some(label.to_string())).await
}

async fn set_job_parameter(
    ctx: &NzbgetFacadeContext,
    job_id: JobId,
    name: &str,
    value: &str,
) -> Result<(), SchedulerError> {
    let name = name.trim();
    if name.is_empty() {
        return Ok(());
    }
    // NZBGet exposes the drone id as a plain post-processing parameter, but
    // weaver stores it under the canonical client-request-id metadata key.
    let key = if name.eq_ignore_ascii_case("drone") {
        CLIENT_REQUEST_ID_ATTRIBUTE_KEY
    } else {
        name
    };
    let value = if name.eq_ignore_ascii_case(PRIORITY_ATTRIBUTE_KEY) {
        normalize_priority_value(value).unwrap_or_else(|_| value.to_string())
    } else {
        value.to_string()
    };
    let update = if value.is_empty() { None } else { Some(value) };
    upsert_job_metadata(ctx, job_id, key, update).await
}

/// Replace (or remove, when `value` is None) one metadata entry on a job,
/// preserving the rest. Mirrors the GraphQL update_jobs upsert semantics.
async fn upsert_job_metadata(
    ctx: &NzbgetFacadeContext,
    job_id: JobId,
    key: &str,
    value: Option<String>,
) -> Result<(), SchedulerError> {
    let mut metadata = ctx.handle.get_job(job_id)?.metadata;
    metadata.retain(|(existing, _)| !existing.eq_ignore_ascii_case(key));
    if let Some(value) = value {
        metadata.push((key.to_string(), value));
    }
    let update = JobUpdate {
        metadata: FieldUpdate::Set(metadata),
        ..JobUpdate::default()
    };
    ctx.handle.update_job(job_id, update).await
}

async fn mark_history_good(ctx: &NzbgetFacadeContext, job_id: JobId) -> Result<(), SchedulerError> {
    let db = ctx.db.clone();
    // A missing semantic candidate is not an error: NZBGet lets any history
    // item be marked good, and for ordinary items there is nothing to record.
    tokio::task::spawn_blocking(move || db.mark_semantic_candidate_good(job_id))
        .await
        .map_err(|error| SchedulerError::InvalidInput(error.to_string()))?
        .map_err(SchedulerError::from)?;
    Ok(())
}

/// Synthetic per-file IDs live above the NZBID range so they never collide
/// with job ids in clients that mix both in one numeric space.
const LISTFILES_ID_STRIDE: u64 = 100_000;

/// Build a synthetic listfiles file ID that always fits a positive `i32`.
///
/// The raw `job_id * STRIDE + index` scheme overflows `i32::MAX` from job 21_475
/// onward (job ids only ever grow), yet weaver emits it as XML-RPC `<i4>`, which
/// strict clients parse as signed 32-bit and then choke on. We fold the value
/// into the top quarter of the positive i32 range: still above any realistic
/// NZBID, still distinct per file within a job, and always parseable. These IDs
/// are display-only — weaver exposes no File* operations — so the cross-job
/// aliasing the fold can introduce is harmless.
fn listfiles_file_id(job_id: JobId, index: usize) -> u64 {
    let raw = job_id
        .0
        .wrapping_mul(LISTFILES_ID_STRIDE)
        .wrapping_add(index as u64 + 1);
    0x4000_0000 | (raw & 0x3FFF_FFFF)
}

async fn listfiles(ctx: &NzbgetFacadeContext, params: Option<Value>) -> Result<Value, RpcError> {
    let params = positional_params(params)?;
    // v13+ signature is (IDFrom, IDTo, NZBID); older clients send (IDFrom,
    // IDTo) or a bare id. Use the last non-zero value as the job filter.
    let nzb_id = params
        .iter()
        .rev()
        .filter_map(|value| value.as_i64().or_else(|| value.as_str()?.parse().ok()))
        .find(|id| *id > 0)
        .map(|id| JobId(id as u64));

    // Require a job filter. A bare listfiles() (nzb_id = None) would load,
    // zstd-decompress, and XML-parse EVERY queued NZB serially on each poll —
    // unbounded CPU at read scope — and weaver exposes no File* operations that
    // consume an all-jobs file listing, so return an empty listing instead.
    let Some(job_id) = nzb_id else {
        return Ok(Value::Array(Vec::new()));
    };
    let targets: Vec<_> = ctx
        .handle
        .list_jobs()
        .into_iter()
        .filter(|job| job.job_id == job_id)
        .collect();
    if targets.is_empty() {
        return Err(RpcError::invalid_parameter("nzb id not found in queue"));
    }

    let mut files = Vec::new();
    for info in targets {
        let job_id = info.job_id;
        let db = ctx.db.clone();
        let loaded = tokio::task::spawn_blocking(move || {
            let nzb = db.load_active_job_persisted_nzb(job_id)?;
            let runtime = db.load_active_file_runtime(job_id)?;
            Ok::<_, weaver_server_core::StateError>((nzb, runtime))
        })
        .await
        .map_err(|error| RpcError::invalid_parameter(format!("listfiles failed: {error}")))?
        .map_err(|error| RpcError::invalid_parameter(format!("listfiles failed: {error}")))?;
        let (persisted, (progress, complete)) = loaded;
        let Some((_, Some(nzb_zstd))) = persisted else {
            continue;
        };
        let Ok(nzb_bytes) = weaver_server_core::ingest::decode_persisted_nzb_bytes(&nzb_zstd)
        else {
            continue;
        };
        let Ok(nzb) = weaver_nzb::parse_nzb(&nzb_bytes) else {
            continue;
        };

        let item = queue_item_from_job(&info);
        let job_done = matches!(item.state, QueueItemState::Completed);
        let paused = item.state == QueueItemState::Paused;
        let output_dir = item.output_dir.clone().unwrap_or_default();
        for (index, file) in nzb.files.iter().enumerate() {
            let index_u32 = index as u32;
            let total: u64 = file
                .segments
                .iter()
                .map(|segment| segment.bytes as u64)
                .sum();
            let remaining = if job_done || complete.contains(&index_u32) {
                0
            } else {
                total.saturating_sub(progress.get(&index_u32).copied().unwrap_or(0))
            };
            let (file_lo, file_hi) = size_parts(total);
            let (remaining_lo, remaining_hi) = size_parts(remaining);
            let filename = file
                .filename()
                .map(str::to_string)
                .unwrap_or_else(|| file.subject.clone());
            files.push(json!({
                "ID": listfiles_file_id(job_id, index),
                "NZBID": job_id.0,
                "NZBFilename": item.name,
                "NZBName": item.name,
                "NZBNicename": item.display_title,
                "Subject": file.subject,
                "Filename": filename,
                "DestDir": output_dir,
                "FileSizeLo": file_lo,
                "FileSizeHi": file_hi,
                "RemainingSizeLo": remaining_lo,
                "RemainingSizeHi": remaining_hi,
                "PostTime": file.date,
                "FilenameConfirmed": true,
                "Paused": paused,
                "ActiveDownloads": 0,
                "Progress": (total.saturating_sub(remaining))
                    .saturating_mul(1000)
                    .checked_div(total)
                    .unwrap_or(1000),
                "Priority": nzbget_priority(&item.attributes),
                "Category": item.category.clone().unwrap_or_default(),
            }));
        }
    }
    Ok(Value::Array(files))
}

async fn postqueue(ctx: &NzbgetFacadeContext) -> Result<Value, RpcError> {
    let db = ctx.db.clone();
    let run_details = tokio::task::spawn_blocking(move || {
        let mut details = HashMap::new();
        for run in db.list_post_processing_runs(None, 500)? {
            if details.contains_key(&run.job_id) {
                continue;
            }
            let attempt = db.post_processing_attempts(&run.run_id)?.pop();
            details.insert(run.job_id, (run, attempt));
        }
        Ok::<_, weaver_server_core::StateError>(details)
    })
    .await
    .map_err(|error| RpcError::invalid_parameter(format!("postqueue unavailable: {error}")))?
    .map_err(|error| RpcError::invalid_parameter(format!("postqueue unavailable: {error}")))?;
    let entries = ctx
        .handle
        .list_jobs()
        .iter()
        .filter(|job| {
            matches!(job.status, JobStatus::QueuedPostProcessing)
                || queue_item_state_in_post_processing(
                    weaver_server_api::queue_item_state_from_job_info(job),
                )
        })
        .map(queue_item_from_job)
        .map(|item| {
            let info = post_progress_info(&item);
            let stage = match item.state {
                QueueItemState::Checking | QueueItemState::Verifying => "VERIFYING_SOURCES",
                QueueItemState::Repairing => "REPAIRING",
                QueueItemState::Extracting => "UNPACKING",
                QueueItemState::Finalizing => "MOVING",
                QueueItemState::PostProcessing => "EXECUTING_SCRIPT",
                _ => "QUEUED",
            };
            let (script_name, script_status, script_progress) = run_details
                .get(&item.id)
                .and_then(|(_, attempt)| attempt.as_ref())
                .map(|attempt| {
                    (
                        attempt.extension_id.as_str().to_string(),
                        format!("{:?}", attempt.status).to_ascii_uppercase(),
                        attempt.progress.clone().unwrap_or(serde_json::Value::Null),
                    )
                })
                .unwrap_or_else(|| (String::new(), "QUEUED".to_string(), Value::Null));
            json!({
                "ID": item.id,
                "NZBID": item.id,
                "NZBName": item.name,
                "NZBNicename": item.display_title,
                "InfoName": item.name,
                "Stage": stage,
                "ProgressLabel": info.text,
                "FileProgress": (item.progress_percent.clamp(0.0, 100.0) * 10.0) as u64,
                "StageProgress": info.stage_progress,
                "TotalTimeSec": info.total_time_sec,
                "StageTimeSec": info.stage_time_sec,
                "ScriptName": script_name,
                "ScriptStatus": script_status,
                "ScriptProgress": script_progress,
                "Log": [],
            })
        })
        .collect::<Vec<_>>();
    Ok(Value::Array(entries))
}

/// NZBGet feed listing backed by weaver's RSS seen-item store. Items weaver's
/// filter rules grabbed report FETCHED; everything else the poller has
/// already evaluated is BACKLOG.
async fn viewfeed(ctx: &NzbgetFacadeContext, params: Option<Value>) -> Result<Value, RpcError> {
    let params = positional_params(params)?;
    let feed_number = params
        .iter()
        .find_map(|value| value.as_i64().or_else(|| value.as_str()?.parse().ok()))
        .filter(|id| *id > 0)
        .ok_or_else(|| RpcError::invalid_parameter("viewfeed requires a feed id"))?;

    // config advertises feeds as sequential Feed1..N (sorted by id); resolve that
    // 1-based number back to the real db feed id so the two surfaces agree.
    let db = ctx.db.clone();
    let mut feeds = tokio::task::spawn_blocking(move || db.list_rss_feeds())
        .await
        .map_err(|error| RpcError::invalid_parameter(format!("viewfeed failed: {error}")))?
        .unwrap_or_default();
    feeds.sort_by_key(|feed| feed.id);
    let feed_id = feeds
        .get((feed_number as usize) - 1)
        .map(|feed| feed.id)
        .ok_or_else(|| RpcError::invalid_parameter("nzb feed not found"))?;

    let db = ctx.db.clone();
    let items =
        tokio::task::spawn_blocking(move || db.list_rss_seen_items(Some(feed_id), Some(500)))
            .await
            .map_err(|error| RpcError::invalid_parameter(format!("viewfeed failed: {error}")))?
            .map_err(|error| RpcError::invalid_parameter(format!("viewfeed failed: {error}")))?;

    let entries = items
        .iter()
        .enumerate()
        .map(|(index, item)| {
            let size = item.size_bytes.unwrap_or(0);
            let (size_lo, size_hi) = size_parts(size);
            let fetched = item.decision == "submitted" && item.job_id.is_some();
            let match_status = match item.decision.as_str() {
                "submitted" => "ACCEPTED",
                "rejected" => "REJECTED",
                _ => "IGNORED",
            };
            json!({
                "ID": index as u64 + 1,
                "Title": item.item_title,
                "Filename": item.item_title,
                "URL": item.item_url.clone().unwrap_or_default(),
                "SizeLo": size_lo,
                "SizeHi": size_hi,
                "SizeMB": bytes_to_mib(size),
                "Category": "",
                "AddCategory": "",
                "PauseNzb": false,
                "Priority": 0,
                "Time": item.published_at.unwrap_or(item.seen_at),
                "Match": match_status,
                "MatchStatus": match_status,
                "MatchRule": 0,
                "DupeKey": "",
                "DupeScore": 0,
                "DupeMode": "SCORE",
                "Status": if fetched { "FETCHED" } else { "BACKLOG" },
            })
        })
        .collect::<Vec<_>>();
    Ok(Value::Array(entries))
}

/// Kick a refresh of every enabled RSS feed. NZBGet's fetchfeeds returns
/// immediately; the sync itself runs in the background. Coalesced: a poll
/// storm of fetchfeeds calls collapses into a single in-flight sync instead
/// of spawning a new one (and a new local `tokio::spawn`) per call.
fn fetch_feeds(ctx: &NzbgetFacadeContext) -> Result<Value, RpcError> {
    ctx.rss.request_background_sync();
    Ok(json!(true))
}

/// Per-server volume statistics backed by weaver's download-quota usage
/// tracking. TotalSize is lifetime bytes per server; CustomSize is the
/// current quota window (CustomTime = window start), matching how NZBGet
/// clients use the custom counter. Rolling per-second/minute/hour histograms
/// are not tracked and are reported as zeroed series.
async fn servervolumes(ctx: &NzbgetFacadeContext) -> Result<Value, RpcError> {
    let db = ctx.db.clone();
    let usage = tokio::task::spawn_blocking(move || db.list_server_download_usage())
        .await
        .map_err(|error| RpcError::invalid_parameter(format!("servervolumes failed: {error}")))?
        .map_err(|error| RpcError::invalid_parameter(format!("servervolumes failed: {error}")))?;

    let window_bytes = |entry: &weaver_server_core::servers::ServerDownloadUsage| {
        entry
            .lifetime_bytes
            .saturating_sub(entry.quota_baseline_bytes)
    };
    let total_lifetime: u64 = usage.iter().map(|entry| entry.lifetime_bytes).sum();
    let total_window: u64 = usage.iter().map(window_bytes).sum();

    // Entry 0 is NZBGet's all-servers aggregate.
    let mut entries = Vec::with_capacity(usage.len() + 1);
    entries.push(server_volume_entry(
        0,
        total_lifetime,
        total_window,
        0,
        unix_now_secs() as i64,
    ));
    for entry in &usage {
        entries.push(server_volume_entry(
            entry.server_id,
            entry.lifetime_bytes,
            window_bytes(entry),
            entry
                .window_start
                .map(|start| start.timestamp())
                .unwrap_or(0),
            entry.updated_at.timestamp(),
        ));
    }
    Ok(Value::Array(entries))
}

fn server_volume_entry(
    server_id: u32,
    total_bytes: u64,
    custom_bytes: u64,
    custom_time: i64,
    data_time: i64,
) -> Value {
    let (total_lo, total_hi) = size_parts(total_bytes);
    let (custom_lo, custom_hi) = size_parts(custom_bytes);
    json!({
        "ServerID": server_id,
        "DataTime": data_time,
        "FirstDay": 0,
        "TotalSizeLo": total_lo,
        "TotalSizeHi": total_hi,
        "TotalSizeMB": bytes_to_mib(total_bytes),
        "CustomSizeLo": custom_lo,
        "CustomSizeHi": custom_hi,
        "CustomSizeMB": bytes_to_mib(custom_bytes),
        "CustomTime": custom_time,
        // Weaver tracks no rolling download-rate series, so these are always
        // zero — but NZBGet's wire contract is FIXED-length windows (60 secs,
        // 60 mins, 24 hours), and a strict client may index a fixed offset
        // (e.g. BytesPerSeconds[59]) or chart a fixed-length series. Emit the
        // zero-filled fixed lengths; servervolumes is a cold endpoint, so the
        // 144-element alloc per server is immaterial.
        "BytesPerSeconds": vec![0u32; 60],
        "BytesPerMinutes": vec![0u32; 60],
        "BytesPerHours": vec![0u32; 24],
        "BytesPerDays": [],
        "SecSlot": 0,
        "MinSlot": 0,
        "HourSlot": 0,
        "DaySlot": 0,
    })
}

fn log_entry_kind(kind: &str) -> &'static str {
    let lowered = kind.to_ascii_lowercase();
    if lowered.contains("fail") || lowered.contains("error") {
        "ERROR"
    } else if lowered.contains("warn") || lowered.contains("attention") {
        "WARNING"
    } else {
        "INFO"
    }
}

fn job_events_to_log_entries(
    events: Vec<weaver_server_core::history::timeline::JobEventRecord>,
    id_from: u64,
    limit: usize,
    label: impl Fn(&weaver_server_core::history::timeline::JobEvent) -> String,
) -> Vec<Value> {
    let mut entries = events
        .into_iter()
        .map(|event| {
            // Job events carry epoch milliseconds; NZBGet log times are
            // epoch seconds.
            let timestamp = event.timestamp.max(0) as u64 / 1000;
            json!({
                "ID": event.id,
                "Kind": log_entry_kind(&event.kind),
                "Time": timestamp,
                "Text": label(&event.event),
            })
        })
        .filter(|entry| id_from == 0 || entry["ID"].as_u64().unwrap_or(0) >= id_from)
        .collect::<Vec<_>>();
    entries.sort_by_key(|entry| entry["ID"].as_u64().unwrap_or(0));
    if limit > 0 && entries.len() > limit {
        let excess = entries.len() - limit;
        entries.drain(..excess);
    }
    entries
}

/// Global message log assembled from the event tails of current queue jobs.
async fn log_entries(ctx: &NzbgetFacadeContext, params: Option<Value>) -> Result<Value, RpcError> {
    let params = positional_params(params)?;
    let id_from = optional_i64_param(&params, 0)?.unwrap_or(0).max(0) as u64;
    let limit = optional_i64_param(&params, 1)?.unwrap_or(0).max(0) as usize;

    let jobs = ctx.handle.list_jobs();
    let names: std::collections::HashMap<u64, String> = jobs
        .iter()
        .map(|job| (job.job_id.0, job.name.clone()))
        .collect();
    let ids: Vec<u64> = jobs.iter().map(|job| job.job_id.0).collect();
    let db = ctx.db.clone();
    // `limit == 0` means "no cap" everywhere else in this RPC surface, but an
    // unbounded fetch across every queued job in one query is not a
    // reasonable default, so fall back to a sane cap.
    let query_limit = if limit == 0 { 100 } else { limit };
    let events = tokio::task::spawn_blocking(move || {
        db.get_recent_job_events_multi(&ids, id_from, query_limit)
    })
    .await
    .map_err(|error| RpcError::invalid_parameter(format!("log unavailable: {error}")))?
    .map_err(|error| RpcError::invalid_parameter(format!("log unavailable: {error}")))?;

    Ok(Value::Array(job_events_to_log_entries(
        events,
        id_from,
        limit,
        |event| match names.get(&event.job_id) {
            Some(name) => format!("[{name}] {}", event.message),
            None => event.message.clone(),
        },
    )))
}

/// Per-download log (NZBGet keeps one per NZB; weaver's job event timeline is
/// the equivalent).
async fn loadlog(ctx: &NzbgetFacadeContext, params: Option<Value>) -> Result<Value, RpcError> {
    let params = positional_params(params)?;
    if params.is_empty() {
        return Err(RpcError::invalid_parameter("loadlog requires an nzb id"));
    }
    let job_id = parse_job_id_value(&params[0])?;
    let id_from = optional_i64_param(&params, 1)?.unwrap_or(0).max(0) as u64;
    let limit = optional_i64_param(&params, 2)?.unwrap_or(0).max(0) as usize;

    let db = ctx.db.clone();
    let events =
        tokio::task::spawn_blocking(move || db.get_job_event_records_latest(job_id.0, 1000))
            .await
            .map_err(|error| RpcError::invalid_parameter(format!("loadlog unavailable: {error}")))?
            .map_err(|error| {
                RpcError::invalid_parameter(format!("loadlog unavailable: {error}"))
            })?;

    Ok(Value::Array(job_events_to_log_entries(
        events,
        id_from,
        limit,
        |event| event.message.clone(),
    )))
}

fn writelog(params: Option<Value>) -> Result<Value, RpcError> {
    let params = positional_params(params.clone()).unwrap_or_default();
    let kind = params
        .first()
        .and_then(Value::as_str)
        .unwrap_or("INFO")
        .to_string();
    let text = params
        .get(1)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    match kind.to_ascii_uppercase().as_str() {
        "ERROR" => tracing::error!(target: "weaver::nzbget_facade", "{text}"),
        "WARNING" => tracing::warn!(target: "weaver::nzbget_facade", "{text}"),
        _ => tracing::info!(target: "weaver::nzbget_facade", "{text}"),
    }
    Ok(json!(true))
}

fn scheduler_rpc_error(command: &str, error: SchedulerError) -> RpcError {
    RpcError::invalid_parameter(format!("{command} failed: {error}"))
}

async fn pause_download(ctx: &NzbgetFacadeContext) -> Result<Value, RpcError> {
    ctx.scheduled_resume
        .pause_all()
        .await
        .map_err(|error| RpcError::invalid_parameter(format!("pausedownload failed: {error}")))?;
    Ok(json!(true))
}

async fn resume_download(ctx: &NzbgetFacadeContext) -> Result<Value, RpcError> {
    ctx.scheduled_resume
        .resume_all()
        .await
        .map_err(|error| RpcError::invalid_parameter(format!("resumedownload failed: {error}")))?;
    Ok(json!(true))
}

async fn pause_post_processing(ctx: &NzbgetFacadeContext) -> Result<Value, RpcError> {
    ctx.handle
        .pause_post_processing()
        .await
        .map_err(|error| RpcError::invalid_parameter(format!("pausepost failed: {error}")))?;
    Ok(json!(true))
}

async fn resume_post_processing(ctx: &NzbgetFacadeContext) -> Result<Value, RpcError> {
    ctx.handle
        .resume_post_processing()
        .await
        .map_err(|error| RpcError::invalid_parameter(format!("resumepost failed: {error}")))?;
    Ok(json!(true))
}

async fn loadextensions(ctx: &NzbgetFacadeContext) -> Result<Value, RpcError> {
    let db = ctx.db.clone();
    let settings = tokio::task::spawn_blocking(move || db.post_processing_settings())
        .await
        .map_err(|error| RpcError::invalid_parameter(format!("loadextensions failed: {error}")))?
        .map_err(|error| RpcError::invalid_parameter(format!("loadextensions failed: {error}")))?;
    if !settings.discovery_enabled {
        return Err(RpcError::invalid_parameter(
            "post-processing extension discovery is disabled",
        ));
    }
    let data_dir = std::path::PathBuf::from(ctx.config.read().await.data_dir.clone());
    let db = ctx.db.clone();
    let discovered = tokio::task::spawn_blocking(move || {
        weaver_server_core::post_processing::discovery::discover_and_record_extensions(
            &db,
            &data_dir,
            weaver_server_core::post_processing::discovery::DiscoveryOptions {
                enabled: true,
                bare_script_adapter: None,
            },
            chrono::Utc::now().timestamp_millis(),
        )
    })
    .await
    .map_err(|error| RpcError::invalid_parameter(format!("loadextensions failed: {error}")))?
    .map_err(|error| RpcError::invalid_parameter(format!("loadextensions failed: {error}")))?;
    tracing::info!(
        count = discovered.len(),
        "NZBGet facade refreshed extension discovery"
    );
    Ok(json!(true))
}

async fn set_scan_paused(ctx: &NzbgetFacadeContext, paused: bool) -> Result<Value, RpcError> {
    // Delegate to the live WatchFolderService so the flag actually reconciles the
    // running scanner (stops/starts the poll + realtime tasks). Writing the
    // setting and flipping the shared-config flag by hand — as this did before —
    // left the already-spawned poller looping on its start-time snapshot, so
    // pausescan reported ScanPaused=true while imports kept happening (and
    // resumescan could not restart a properly-paused task).
    ctx.watch_folder
        .set_scanning_paused(paused)
        .await
        .map_err(|error| RpcError::invalid_parameter(format!("pausescan failed: {error}")))?;
    Ok(json!(true))
}

/// Longest accepted `scheduleresume` delay. NZBGet clamps similarly; this
/// keeps hostile inputs from arming year-long timers.
const MAX_SCHEDULE_RESUME_SECS: i64 = 60 * 60 * 24 * 30;

async fn scheduleresume(
    ctx: &NzbgetFacadeContext,
    params: Option<Value>,
) -> Result<Value, RpcError> {
    let params = positional_params(params)?;
    let seconds = optional_i64_param(&params, 0)?.ok_or_else(|| {
        RpcError::invalid_parameter("scheduleresume requires seconds until resume")
    })?;
    if seconds <= 0 || seconds > MAX_SCHEDULE_RESUME_SECS {
        return Err(RpcError::invalid_parameter(
            "scheduleresume seconds must be between 1 and 2592000",
        ));
    }

    let resume_at = unix_now_secs() + seconds as u64;
    ctx.scheduled_resume
        .schedule_resume(resume_at)
        .await
        .map_err(|error| RpcError::invalid_parameter(format!("scheduleresume failed: {error}")))?;
    Ok(json!(true))
}

async fn rate(ctx: &NzbgetFacadeContext, params: Option<Value>) -> Result<Value, RpcError> {
    let params = positional_params(params)?;
    let kib_per_sec = optional_i64_param(&params, 0)?
        .ok_or_else(|| RpcError::invalid_parameter("rate requires a limit in KB/s"))?;
    let bytes_per_sec = u64::try_from(kib_per_sec.max(0))
        .unwrap_or(0)
        .saturating_mul(1024);

    // Mirror the settings mutation: persist, update shared config, then apply
    // to the live limiter, so `status` and the weaver UI agree with the value.
    let db = ctx.db.clone();
    tokio::task::spawn_blocking(move || {
        db.set_setting("max_download_speed", &bytes_per_sec.to_string())
    })
    .await
    .map_err(|error| RpcError::invalid_parameter(format!("rate failed: {error}")))?
    .map_err(|error| RpcError::invalid_parameter(format!("rate failed: {error}")))?;
    {
        let mut config = ctx.config.write().await;
        config.max_download_speed = Some(bytes_per_sec);
    }
    ctx.handle
        .set_speed_limit(bytes_per_sec)
        .await
        .map_err(|error| scheduler_rpc_error("rate", error))?;
    Ok(json!(true))
}

fn positional_params(params: Option<Value>) -> Result<Vec<Value>, RpcError> {
    match params {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(Value::Array(values)) => Ok(values),
        Some(_) => Err(RpcError::invalid_parameter(
            "only positional parameter arrays are supported",
        )),
    }
}

fn required_string_param(params: &[Value], index: usize) -> Result<String, RpcError> {
    optional_string_param(params, index)?.ok_or_else(|| {
        RpcError::invalid_parameter(format!("parameter {index} must be a non-empty string"))
    })
}

fn optional_string_param(params: &[Value], index: usize) -> Result<Option<String>, RpcError> {
    let Some(value) = params.get(index) else {
        return Ok(None);
    };
    match value {
        Value::Null => Ok(None),
        Value::String(value) if value.trim().is_empty() => Ok(None),
        Value::String(value) => Ok(Some(value.clone())),
        _ => Err(RpcError::invalid_parameter(format!(
            "parameter {index} must be a string"
        ))),
    }
}

fn optional_bool_param(params: &[Value], index: usize) -> Result<Option<bool>, RpcError> {
    let Some(value) = params.get(index) else {
        return Ok(None);
    };
    match value {
        Value::Null => Ok(None),
        Value::Bool(value) => Ok(Some(*value)),
        Value::Number(value) => Ok(value.as_i64().map(|n| n != 0)),
        Value::String(value) if value.eq_ignore_ascii_case("true") => Ok(Some(true)),
        Value::String(value) if value.eq_ignore_ascii_case("false") => Ok(Some(false)),
        _ => Err(RpcError::invalid_parameter(format!(
            "parameter {index} must be a boolean"
        ))),
    }
}

fn optional_i64_param(params: &[Value], index: usize) -> Result<Option<i64>, RpcError> {
    let Some(value) = params.get(index) else {
        return Ok(None);
    };
    match value {
        Value::Null => Ok(None),
        Value::Number(value) => value.as_i64().map(Some).ok_or_else(|| {
            RpcError::invalid_parameter(format!("parameter {index} must be an integer"))
        }),
        Value::String(value) if value.trim().is_empty() => Ok(None),
        Value::String(value) => value.parse::<i64>().map(Some).map_err(|_| {
            RpcError::invalid_parameter(format!("parameter {index} must be an integer"))
        }),
        _ => Err(RpcError::invalid_parameter(format!(
            "parameter {index} must be an integer"
        ))),
    }
}

fn parse_job_id_value(value: &Value) -> Result<JobId, RpcError> {
    match value {
        Value::Number(value) => value
            .as_u64()
            .map(JobId)
            .ok_or_else(|| RpcError::invalid_parameter("job id must be a positive integer")),
        Value::String(value) => value
            .parse::<u64>()
            .map(JobId)
            .map_err(|_| RpcError::invalid_parameter("job id must be a positive integer")),
        _ => Err(RpcError::invalid_parameter(
            "job id must be a positive integer",
        )),
    }
}

fn parse_parameter_pairs(value: Option<&Value>) -> Result<Vec<(String, String)>, RpcError> {
    let mut out = Vec::new();
    let Some(value) = value else {
        return Ok(out);
    };
    match value {
        Value::Null => Ok(out),
        Value::Array(values) => {
            if values
                .iter()
                .all(|value| matches!(value, Value::Object(object) if object.contains_key("Name")))
            {
                for value in values {
                    let object = value.as_object().expect("object checked above");
                    let Some(name) = object.get("Name").and_then(Value::as_str) else {
                        continue;
                    };
                    if let Some(value) = object.get("Value").and_then(value_to_string) {
                        insert_parameter(&mut out, name.to_string(), value);
                    }
                }
                return Ok(out);
            }
            if values.len() % 2 != 0 {
                return Err(RpcError::invalid_parameter(
                    "parameter pairs must contain an even number of values",
                ));
            }
            for pair in values.chunks_exact(2) {
                let Some(name) = pair[0].as_str() else {
                    return Err(RpcError::invalid_parameter(
                        "parameter pair names must be strings",
                    ));
                };
                if let Some(value) = value_to_string(&pair[1]) {
                    insert_parameter(&mut out, name.to_string(), value);
                }
            }
            Ok(out)
        }
        Value::Object(object) => {
            for (key, value) in object {
                if let Some(value) = value_to_string(value) {
                    insert_parameter(&mut out, key.clone(), value);
                }
            }
            Ok(out)
        }
        _ => Err(RpcError::invalid_parameter(
            "parameters must be an array or object",
        )),
    }
}

fn insert_parameter(out: &mut Vec<(String, String)>, name: String, value: String) {
    if let Some(index) = out.iter().position(|(existing, _)| existing == &name) {
        out.remove(index);
    }
    out.push((name, value));
}

fn value_to_string(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        Value::Array(_) | Value::Object(_) => Some(value.to_string()),
    }
}

fn response_parameters(client_request_id: Option<&str>, attributes: &[Attribute]) -> Vec<Value> {
    let mut params = Vec::new();
    if let Some(client_request_id) = client_request_id.filter(|value| !value.trim().is_empty()) {
        params.push(json!({ "Name": "drone", "Value": client_request_id }));
    }
    for attribute in attributes {
        if attribute.key == CLIENT_REQUEST_ID_ATTRIBUTE_KEY
            || attribute.key.eq_ignore_ascii_case("drone")
        {
            continue;
        }
        params.push(json!({ "Name": attribute.key, "Value": attribute.value }));
    }
    params
}

fn priority_label(priority: i64) -> &'static str {
    if priority > 0 {
        "HIGH"
    } else if priority < 0 {
        "LOW"
    } else {
        "NORMAL"
    }
}

fn nzbget_priority(attributes: &[Attribute]) -> i64 {
    attributes
        .iter()
        .find(|attribute| attribute.key.eq_ignore_ascii_case(PRIORITY_ATTRIBUTE_KEY))
        .map(|attribute| match attribute.value.as_str() {
            value if value.eq_ignore_ascii_case("HIGH") => 50,
            value if value.eq_ignore_ascii_case("LOW") => -50,
            _ => 0,
        })
        .unwrap_or(0)
}

fn active_downloads(item: &QueueItem) -> u32 {
    if item.state == QueueItemState::Downloading {
        1
    } else {
        0
    }
}

fn nzbget_queue_status(state: QueueItemState) -> &'static str {
    match state {
        QueueItemState::Queued => "QUEUED",
        QueueItemState::Downloading => "DOWNLOADING",
        QueueItemState::Checking | QueueItemState::Verifying => "VERIFYING_SOURCES",
        QueueItemState::Repairing => "REPAIRING",
        QueueItemState::Extracting => "UNPACKING",
        QueueItemState::Finalizing => "MOVING",
        QueueItemState::PostProcessing => "EXECUTING_SCRIPT",
        QueueItemState::Paused => "PAUSED",
        QueueItemState::Completed => "SUCCESS",
        QueueItemState::Failed => "FAILURE",
    }
}

/// Group status for listgroups. Downloads that finished transfer but wait for
/// a repair/extraction slot report NZBGet's PP_QUEUED instead of QUEUED so
/// clients render them as post-processing, not "not started".
fn nzbget_group_status(item: &QueueItem) -> &'static str {
    use weaver_server_api::QueuePostState;

    if item.state == QueueItemState::Queued
        && matches!(
            item.post_state,
            QueuePostState::QueuedRepair
                | QueuePostState::QueuedExtract
                | QueuePostState::AwaitingRepair
                | QueuePostState::WaitingForVolumes
        )
    {
        return "PP_QUEUED";
    }
    nzbget_queue_status(item.state)
}

fn size_parts(value: u64) -> (u32, u32) {
    ((value & 0xffff_ffff) as u32, (value >> 32) as u32)
}

fn bytes_to_mib(value: u64) -> u64 {
    value / 1_048_576
}

fn is_http_url(value: &str) -> bool {
    value.starts_with("http://") || value.starts_with("https://")
}

fn config_entry(name: &str, value: &str) -> Value {
    json!({ "Name": name, "Value": value })
}

fn category_dest_dir(complete_dir: &str, name: &str) -> String {
    Path::new(complete_dir)
        .join(name)
        .to_string_lossy()
        .into_owned()
}
