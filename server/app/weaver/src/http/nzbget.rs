use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::Extension;
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::HashSet;

use weaver_server_api::{
    Attribute, AttributeInput, CLIENT_REQUEST_ID_ATTRIBUTE_KEY, CategoryResolutionMode,
    PRIORITY_ATTRIBUTE_KEY, QueueItem, QueueItemState, SubmissionOptions, SubmitNzbError,
    fetch_nzb_from_url, history_item_from_row, queue_item_from_job, submit_metadata,
    submit_nzb_bytes_with_options,
};
use weaver_server_core::auth::{ApiKeyCache, CallerScope, LoginAuthCache};
use weaver_server_core::settings::model::SharedConfig;
use weaver_server_core::{Database, HistoryFilter, JobId, SchedulerError, SchedulerHandle};

#[derive(Clone)]
pub(super) struct NzbgetFacadeContext {
    db: Database,
    handle: SchedulerHandle,
    config: SharedConfig,
    auth_cache: LoginAuthCache,
    api_key_cache: ApiKeyCache,
    session_token: super::SessionToken,
    http_client: reqwest::Client,
}

impl NzbgetFacadeContext {
    pub(super) fn new(
        db: Database,
        handle: SchedulerHandle,
        config: SharedConfig,
        auth_cache: LoginAuthCache,
        api_key_cache: ApiKeyCache,
        session_token: super::SessionToken,
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
        }
    }
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

pub(super) async fn jsonrpc_handler(
    Extension(ctx): Extension<NzbgetFacadeContext>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let scope = match resolve_scope_for_facade(&ctx, &headers).await {
        Ok(scope) => scope,
        Err(status) => {
            return rpc_error_response(status, Value::Null, RpcError::access_denied());
        }
    };

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

    match dispatch_method(&ctx, &method, request.params).await {
        Ok(result) => rpc_success_response(id, result),
        Err(error) => rpc_error_response(StatusCode::OK, id, error),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RequiredScope {
    Read,
    Control,
}

fn method_required_scope(method: &str) -> Option<RequiredScope> {
    match method {
        "version" | "status" | "listgroups" | "history" | "config" => Some(RequiredScope::Read),
        "append" | "editqueue" => Some(RequiredScope::Control),
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
        "status" => status(ctx).await,
        "listgroups" => listgroups(ctx).await,
        "history" => history(ctx).await,
        "config" => config(ctx).await,
        "editqueue" => editqueue(ctx, params).await,
        _ => Err(RpcError::invalid_procedure(method)),
    }
}

async fn resolve_scope_for_facade(
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

struct AppendRequest {
    filename: Option<String>,
    content_or_url: String,
    category: Option<String>,
    priority: i64,
    add_paused: bool,
    dupe_key: Option<String>,
    dupe_score: Option<i64>,
    dupe_mode: Option<String>,
    parameters: BTreeMap<String, String>,
}

async fn append(ctx: &NzbgetFacadeContext, params: Option<Value>) -> Result<Value, RpcError> {
    let request = parse_append_params(params)?;
    let (nzb_bytes, fetched_filename) = if is_http_url(&request.content_or_url) {
        match fetch_nzb_from_url(&ctx.http_client, &request.content_or_url).await {
            Ok(fetched) => fetched,
            Err(error) => return append_rejection_result(error),
        }
    } else {
        let bytes = BASE64_STANDARD
            .decode(&request.content_or_url)
            .map_err(|error| RpcError::invalid_parameter(format!("invalid base64: {error}")))?;
        (bytes, None)
    };

    let client_request_id = request.parameters.get("drone").cloned();
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
        request.category,
        metadata,
        SubmissionOptions {
            category_resolution: CategoryResolutionMode::PreserveSubmitted,
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
            ..SubmissionOptions::default()
        },
    )
    .await;

    match submitted {
        Ok(submitted) => Ok(json!(submitted.job_id.0)),
        Err(error) => append_rejection_result(error),
    }
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
        parameters: parse_parameter_pairs(params.get(9))?,
    })
}

async fn status(ctx: &NzbgetFacadeContext) -> Result<Value, RpcError> {
    let jobs: Vec<QueueItem> = ctx
        .handle
        .list_jobs()
        .iter()
        .map(queue_item_from_job)
        .collect();
    let downloaded = jobs.iter().map(|job| job.downloaded_bytes).sum::<u64>();
    let remaining = jobs
        .iter()
        .map(|job| job.total_bytes.saturating_sub(job.downloaded_bytes))
        .sum::<u64>();
    let metrics = ctx.handle.get_metrics();
    let download_rate = metrics.current_download_speed;
    let arr_download_rate = download_rate.min(i32::MAX as u64);
    let config = ctx.config.read().await;
    let download_limit = config.max_download_speed.unwrap_or(0);
    let arr_download_limit = download_limit.min(i32::MAX as u64);
    let (remaining_lo, remaining_hi) = size_parts(remaining);
    let (downloaded_lo, downloaded_hi) = size_parts(downloaded);
    let (rate_lo, rate_hi) = size_parts(download_rate);

    Ok(json!({
        "RemainingSizeLo": remaining_lo,
        "RemainingSizeHi": remaining_hi,
        "RemainingSizeMB": bytes_to_mib(remaining),
        "DownloadedSizeLo": downloaded_lo,
        "DownloadedSizeHi": downloaded_hi,
        "DownloadedSizeMB": bytes_to_mib(downloaded),
        "DownloadRate": arr_download_rate,
        "DownloadRateLo": rate_lo,
        "DownloadRateHi": rate_hi,
        "AverageDownloadRate": arr_download_rate,
        "AverageDownloadRateLo": rate_lo,
        "AverageDownloadRateHi": rate_hi,
        "DownloadLimit": arr_download_limit,
        "DownloadPaused": ctx.handle.is_globally_paused(),
        "ServerPaused": false,
        "Download2Paused": false,
        "PostPaused": false,
        "ScanPaused": false,
        "QuotaReached": false,
        "PostJobCount": 0,
        "UrlCount": 0,
        "NewsServers": config.servers.len(),
        "FreeDiskSpaceLo": 0,
        "FreeDiskSpaceHi": 0,
        "FreeDiskSpaceMB": 0,
    }))
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

    json!({
        "FirstID": item.id,
        "LastID": item.id,
        "NZBID": item.id,
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
        "MinPriority": priority,
        "MaxPriority": priority,
        "ActiveDownloads": active_downloads(item),
        "Status": nzbget_queue_status(item.state),
        "FileCount": 0,
        "Health": item.health,
        "Parameters": response_parameters(item.client_request_id.as_deref(), &item.attributes),
        "ScriptStatuses": [],
        "ServerStats": [],
    })
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
    let mut seen_ids = HashSet::with_capacity(rows.len() + terminal_items.len());
    let mut items = terminal_items
        .iter()
        .filter(|item| seen_ids.insert(item.id))
        .map(nzbget_history_queue_item)
        .collect::<Vec<_>>();
    items.extend(
        rows.iter()
            .filter(|row| seen_ids.insert(row.job_id))
            .map(|row| {
                let item = history_item_from_row(row, None);
                nzbget_history_item(&item)
            }),
    );
    Ok(Value::Array(items))
}

fn nzbget_history_item(item: &weaver_server_api::HistoryItem) -> Value {
    let total = item.total_bytes;
    let (file_lo, file_hi) = size_parts(total);
    let cancelled = item
        .attention
        .as_ref()
        .is_some_and(|attention| attention.code == "CANCELLED");
    let failed = item.state == QueueItemState::Failed && !cancelled;
    let par_status = if cancelled {
        "NONE"
    } else if failed {
        "FAILURE"
    } else {
        "SUCCESS"
    };
    let unpack_status = if cancelled || failed {
        "NONE"
    } else {
        "SUCCESS"
    };
    let move_status = if cancelled { "NONE" } else { "SUCCESS" };
    let script_status = if cancelled { "NONE" } else { "SUCCESS" };
    let delete_status = if cancelled { "MANUAL" } else { "NONE" };
    let status = if cancelled {
        "DELETED"
    } else if failed {
        "FAILURE"
    } else {
        "SUCCESS"
    };
    let output_dir = item.output_dir.clone().unwrap_or_default();

    json!({
        "ID": item.id,
        "NZBID": item.id,
        "Name": item.name,
        "Category": item.category.clone().unwrap_or_default(),
        "FileSizeLo": file_lo,
        "FileSizeHi": file_hi,
        "FileSizeMB": bytes_to_mib(total),
        "DestDir": output_dir,
        "FinalDir": output_dir,
        "ParStatus": par_status,
        "UnpackStatus": unpack_status,
        "MoveStatus": move_status,
        "ScriptStatus": script_status,
        "DeleteStatus": delete_status,
        "MarkStatus": "NONE",
        "Status": status,
        "Message": item.error.clone().unwrap_or_default(),
        "Parameters": response_parameters(item.client_request_id.as_deref(), &item.attributes),
    })
}

fn nzbget_history_queue_item(item: &QueueItem) -> Value {
    let total = item.total_bytes;
    let (file_lo, file_hi) = size_parts(total);
    let failed = item.state == QueueItemState::Failed;
    let par_status = if failed { "FAILURE" } else { "SUCCESS" };
    let unpack_status = if failed { "NONE" } else { "SUCCESS" };
    let output_dir = item.output_dir.clone().unwrap_or_default();

    json!({
        "ID": item.id,
        "NZBID": item.id,
        "Name": item.name,
        "Category": item.category.clone().unwrap_or_default(),
        "FileSizeLo": file_lo,
        "FileSizeHi": file_hi,
        "FileSizeMB": bytes_to_mib(total),
        "DestDir": output_dir,
        "FinalDir": output_dir,
        "ParStatus": par_status,
        "UnpackStatus": unpack_status,
        "MoveStatus": "SUCCESS",
        "ScriptStatus": "SUCCESS",
        "DeleteStatus": "NONE",
        "MarkStatus": "NONE",
        "Status": if failed { "FAILURE" } else { "SUCCESS" },
        "Message": item.error.clone().unwrap_or_default(),
        "Parameters": response_parameters(item.client_request_id.as_deref(), &item.attributes),
    })
}

async fn config(ctx: &NzbgetFacadeContext) -> Result<Value, RpcError> {
    let config = ctx.config.read().await;
    let main_dir = config.data_dir.clone();
    let dest_dir = config.complete_dir();
    let mut entries = vec![
        config_entry("KeepHistory", "7"),
        config_entry("MainDir", &main_dir),
        config_entry("DestDir", &dest_dir),
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

    for (index, (name, dest_dir, aliases)) in categories.iter().enumerate() {
        let prefix = format!("Category{}.", index + 1);
        entries.push(config_entry(&format!("{prefix}Name"), name));
        entries.push(config_entry(&format!("{prefix}DestDir"), dest_dir));
        entries.push(config_entry(&format!("{prefix}Unpack"), "yes"));
        entries.push(config_entry(&format!("{prefix}DefScript"), ""));
        entries.push(config_entry(&format!("{prefix}Aliases"), aliases));
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

async fn editqueue(ctx: &NzbgetFacadeContext, params: Option<Value>) -> Result<Value, RpcError> {
    let params = positional_params(params)?;
    if params.len() < 4 {
        return Err(RpcError::invalid_parameter(
            "editqueue requires command, offset, args, and at least one id",
        ));
    }
    let command = required_string_param(&params, 0)?;
    let ids = params[3..]
        .iter()
        .map(parse_job_id_value)
        .collect::<Result<Vec<_>, _>>()?;
    if ids.is_empty() {
        return Ok(json!(false));
    }

    for job_id in ids {
        let result = match command.as_str() {
            "GroupFinalDelete" => group_final_delete(ctx, job_id).await,
            "HistoryDelete" => ctx.handle.delete_history(job_id, false).await,
            "HistoryRedownload" => ctx.handle.redownload_job(job_id).await,
            other => return Err(RpcError::invalid_action(other)),
        };
        match result {
            Ok(()) => {}
            Err(SchedulerError::JobNotFound(_)) => return Ok(json!(false)),
            Err(error) => {
                return Err(RpcError::invalid_parameter(format!(
                    "{command} failed: {error}"
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

fn parse_parameter_pairs(value: Option<&Value>) -> Result<BTreeMap<String, String>, RpcError> {
    let mut out = BTreeMap::new();
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
                        out.insert(name.to_string(), value);
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
                    out.insert(name.to_string(), value);
                }
            }
            Ok(out)
        }
        Value::Object(object) => {
            for (key, value) in object {
                if let Some(value) = value_to_string(value) {
                    out.insert(key.clone(), value);
                }
            }
            Ok(out)
        }
        _ => Err(RpcError::invalid_parameter(
            "parameters must be an array or object",
        )),
    }
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
        QueueItemState::Paused => "PAUSED",
        QueueItemState::Completed => "SUCCESS",
        QueueItemState::Failed => "FAILURE",
    }
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
