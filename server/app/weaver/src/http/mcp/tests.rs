// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The MCP endpoint end to end through its route: admission refusals, the
// body cap, the tool table, and each tool against a fixture catalog run on a
// schema with no runtime data behind it. Nothing here depends on the skills
// the build embeds.

use std::sync::Arc;

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::extract::Extension;
use axum::http::{HeaderMap, HeaderValue, Request, StatusCode, header};
use axum::routing::post;
use serde_json::{Value, json};
use tower::ServiceExt;
use weaver_server_core::Database;
use weaver_server_core::auth::api_keys::AuthApiKeysQueries;
use weaver_server_core::auth::repository::AuthRepositoryQueries;
use weaver_server_core::auth::{ApiKeyCache, LoginAuthCache, hash_api_key};
use weaver_server_core::security::RuntimeSecurityConfig;

use super::catalog::{Catalog, SkillOperationFile};
use super::server::{self, ResultCap};
use super::{MCP_MAX_REQUEST_BODY_BYTES, McpState, mcp_handler};
use crate::http::graphql::refusal;
use crate::http::{RequestAuthContext, SessionToken};

const HOST: &str = "weaver.lan:9090";
const READ_KEY: &str = "fixture-mcp-read-key";
const CONTROL_KEY: &str = "fixture-mcp-control-key";
const ADMIN_KEY: &str = "fixture-mcp-admin-key";

const TOOL_NAMES: &[&str] = &[
    "whoami",
    "list_operations",
    "describe_operation",
    "run_query",
    "run_mutation",
];

const FIXTURE_FILES: &[SkillOperationFile] = &[
    SkillOperationFile {
        skill: "mcp-fixture-a",
        file: "FixtureVersion.graphql",
        text: "# The running version.\nquery FixtureVersion {\n  version\n}\n",
    },
    SkillOperationFile {
        skill: "mcp-fixture-a",
        file: "FixtureKeys.graphql",
        text: "query FixtureKeys {\n  apiKeys {\n    id\n  }\n}\n",
    },
    SkillOperationFile {
        skill: "mcp-fixture-a",
        file: "FixturePauseJob.graphql",
        text: "# Pause one job.\n# Variables: {\"id\": 7}\nmutation FixturePauseJob($id: Int!) {\n  pauseJob(id: $id)\n}\n",
    },
    SkillOperationFile {
        skill: "mcp-fixture-a",
        file: "FixtureSplit.graphql",
        text: "query FixtureSplit {\n  version\n}\n",
    },
    SkillOperationFile {
        skill: "mcp-fixture-b",
        file: "FixtureSplit.graphql",
        text: "query FixtureSplit {\n  version\n  __typename\n}\n",
    },
    SkillOperationFile {
        skill: "mcp-fixture-b",
        file: "FixtureVersion.graphql",
        text: "# The running version.\nquery FixtureVersion {\n  version\n}\n",
    },
];

fn fixture_catalog() -> &'static Catalog {
    Box::leak(Box::new(Catalog::build(FIXTURE_FILES, &[])))
}

// A schema with no runtime data behind it: `version` and the scope guards
// resolve, and anything that reads runtime state fails with an uncoded
// error.
fn bare_schema() -> weaver_server_api::WeaverSchema {
    async_graphql::Schema::build(
        weaver_server_api::QueryRoot::default(),
        weaver_server_api::MutationRoot::default(),
        weaver_server_api::SubscriptionRoot::default(),
    )
    .finish()
}

fn db_with_keys() -> Database {
    let db = Database::open_in_memory().unwrap();
    for (name, key, scope) in [
        ("fixture-mcp-read", READ_KEY, "read"),
        ("fixture-mcp-control", CONTROL_KEY, "control"),
        ("fixture-mcp-admin", ADMIN_KEY, "admin"),
    ] {
        db.insert_api_key(name, &hash_api_key(key), scope).unwrap();
    }
    db
}

fn router_with(db: Database, security: Arc<RuntimeSecurityConfig>) -> Router {
    Router::new()
        .route("/mcp", post(mcp_handler))
        .layer(Extension(McpState::with_catalog(
            bare_schema(),
            fixture_catalog(),
        )))
        .layer(Extension(RequestAuthContext {
            db,
            auth_cache: LoginAuthCache::default(),
            api_key_cache: ApiKeyCache::default(),
            session_token: SessionToken(Arc::new("fixture-process-token".to_string())),
            security,
        }))
}

fn router() -> Router {
    router_with(db_with_keys(), Arc::new(RuntimeSecurityConfig::default()))
}

fn rpc_request(bearer: Option<&str>, origin: Option<&str>, body: Value) -> Request<Body> {
    let mut request = Request::builder()
        .method("POST")
        .uri("/mcp")
        .header(header::HOST, HOST)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::ACCEPT, "application/json, text/event-stream")
        .header("mcp-protocol-version", "2025-11-25")
        .body(Body::from(body.to_string()))
        .expect("MCP request");
    if let Some(bearer) = bearer {
        request.headers_mut().insert(
            header::AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {bearer}")).expect("authorization"),
        );
    }
    if let Some(origin) = origin {
        request
            .headers_mut()
            .insert(header::ORIGIN, HeaderValue::from_str(origin).expect("origin"));
    }
    request
}

async fn send(router: &Router, request: Request<Body>) -> (StatusCode, HeaderMap, Value) {
    let response = router.clone().oneshot(request).await.expect("MCP response");
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = to_bytes(response.into_body(), 4 * 1024 * 1024)
        .await
        .expect("MCP body");
    let body = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or_else(|error| {
            panic!(
                "MCP body is JSON ({error}): {}",
                String::from_utf8_lossy(&bytes)
            )
        })
    };
    (status, headers, body)
}

fn initialize() -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-11-25",
            "capabilities": {},
            "clientInfo": {"name": "mcp-fixture-client", "version": "0.0.1"}
        }
    })
}

fn list_tools() -> Value {
    json!({"jsonrpc": "2.0", "id": 2, "method": "tools/list"})
}

fn call_tool(name: &str, arguments: Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": 3,
        "method": "tools/call",
        "params": {"name": name, "arguments": arguments}
    })
}

// Calls a tool with `key` and returns the JSON-RPC body.
async fn call(router: &Router, key: &str, name: &str, arguments: Value) -> Value {
    let (status, _, body) = send(
        router,
        rpc_request(Some(key), None, call_tool(name, arguments)),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{name}: {body}");
    body
}

// A tool result's value: the JSON object in its one text block, compact,
// with no structured copy.
fn result_value(name: &str, body: &Value) -> Value {
    let result = &body["result"];
    assert!(result.get("structuredContent").is_none(), "{name}: {body}");
    let content = result["content"].as_array().expect("content");
    assert_eq!(content.len(), 1, "{name}: {body}");
    assert_eq!(content[0]["type"], "text", "{name}: {body}");
    let text = content[0]["text"].as_str().expect("text");
    let value: Value = serde_json::from_str(text).expect("JSON text");
    assert!(value.is_object(), "{name}: {body}");
    assert_eq!(text, value.to_string(), "{name}");
    value
}

fn success(name: &str, body: &Value) -> Value {
    assert_eq!(body["result"]["isError"], false, "{name}: {body}");
    result_value(name, body)
}

// A tool error's first error entry.
fn tool_error(name: &str, body: &Value) -> Value {
    assert_eq!(body["result"]["isError"], true, "{name}: {body}");
    let error = result_value(name, body)["errors"][0].clone();
    assert!(error["code"].is_string(), "{name}: {body}");
    assert!(error["message"].is_string(), "{name}: {body}");
    error
}

// The `confirm_token` a first `run_mutation` call returned, checking that it
// ran nothing.
fn confirm_token(body: &Value) -> String {
    let pending = success("run_mutation", body);
    assert_eq!(pending["status"], "confirmation_required", "{body}");
    assert_eq!(pending["applied"], false, "{body}");
    pending["confirm_token"]
        .as_str()
        .expect("a confirm token")
        .to_string()
}

// A JSON-RPC protocol error's reason.
fn protocol_error_reason(body: &Value) -> Value {
    assert!(body["error"]["code"].is_i64(), "{body}");
    assert!(body.get("result").is_none(), "{body}");
    body["error"]["data"]["reason"].clone()
}

// An HTTP refusal: its status, `{code, error}` body and no-store.
fn assert_refused(status: StatusCode, headers: &HeaderMap, body: &Value, want: StatusCode, code: &str) {
    assert_eq!(status, want, "{body}");
    assert_eq!(body["code"], code, "{body}");
    assert!(body["error"].is_string(), "{body}");
    assert_eq!(headers[header::CACHE_CONTROL], "no-store");
}

#[tokio::test]
async fn mcp_refuses_every_caller_that_is_not_an_api_key() {
    let router = router();

    let (status, headers, body) = send(&router, rpc_request(None, None, initialize())).await;
    assert_refused(
        status,
        &headers,
        &body,
        StatusCode::UNAUTHORIZED,
        refusal::MCP_API_KEY_REQUIRED,
    );
    assert_eq!(headers[header::WWW_AUTHENTICATE], "Bearer");

    let (status, headers, body) = send(
        &router,
        rpc_request(Some("fixture-mcp-wrong-key"), None, initialize()),
    )
    .await;
    assert_refused(
        status,
        &headers,
        &body,
        StatusCode::UNAUTHORIZED,
        refusal::MCP_API_KEY_INVALID,
    );
    assert_eq!(
        headers[header::WWW_AUTHENTICATE],
        "Bearer error=\"invalid_token\""
    );

    let mut request = rpc_request(None, None, initialize());
    request.headers_mut().insert(
        "x-api-key",
        HeaderValue::from_static("fixture-mcp-wrong-key"),
    );
    let (status, headers, body) = send(&router, request).await;
    assert_refused(
        status,
        &headers,
        &body,
        StatusCode::UNAUTHORIZED,
        refusal::MCP_API_KEY_INVALID,
    );

    // A malformed header is an offered credential that is not accepted.
    let mut request = rpc_request(None, None, initialize());
    request.headers_mut().insert(
        header::AUTHORIZATION,
        HeaderValue::from_static("Basic Zml4dHVyZTpmaXh0dXJl"),
    );
    let (status, headers, body) = send(&router, request).await;
    assert_refused(
        status,
        &headers,
        &body,
        StatusCode::UNAUTHORIZED,
        refusal::MCP_API_KEY_INVALID,
    );

    // A key is checked before the body is read.
    let mut request = rpc_request(Some("fixture-mcp-wrong-key"), None, json!({}));
    *request.body_mut() = Body::from(vec![b' '; MCP_MAX_REQUEST_BODY_BYTES + 1]);
    let (status, _, body) = send(&router, request).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{body}");
}

#[tokio::test]
async fn mcp_refuses_a_browser_session() {
    let db = db_with_keys();
    let security = Arc::new(RuntimeSecurityConfig::default());
    security.apply_stored_access_policy_revision(None, None, false);
    assert!(security.authenticated_access_mode());
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let browser_token = "fixture-mcp-browser";
    db.create_browser_session(&weaver_server_core::auth::BrowserSession {
        token_hash: hash_api_key(browser_token)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect(),
        csrf_verifier: "fixture-csrf".to_string(),
        origin: format!("http://{HOST}"),
        client_ip: None,
        remembered: false,
        created_at: now,
        expires_at: now + 3_600,
        revoked_at: None,
    })
    .unwrap();
    let router = router_with(db, security);
    let mut request = rpc_request(None, Some(&format!("http://{HOST}")), initialize());
    request.headers_mut().insert(
        header::COOKIE,
        HeaderValue::from_static("weaver_session=fixture-mcp-browser"),
    );
    let (status, headers, body) = send(&router, request).await;
    assert_refused(
        status,
        &headers,
        &body,
        StatusCode::UNAUTHORIZED,
        refusal::MCP_API_KEY_REQUIRED,
    );
    assert_eq!(headers[header::WWW_AUTHENTICATE], "Bearer");
}

#[tokio::test]
async fn mcp_refuses_a_foreign_origin_and_serves_a_missing_or_same_origin() {
    let router = router();
    let (status, headers, body) = send(
        &router,
        rpc_request(
            Some(ADMIN_KEY),
            Some("https://elsewhere.example.test"),
            initialize(),
        ),
    )
    .await;
    assert_refused(
        status,
        &headers,
        &body,
        StatusCode::FORBIDDEN,
        refusal::MCP_ORIGIN_NOT_ALLOWED,
    );

    for origin in [None, Some(format!("http://{HOST}"))] {
        let (status, _, body) = send(
            &router,
            rpc_request(Some(ADMIN_KEY), origin.as_deref(), initialize()),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{origin:?}: {body}");
    }
}

#[tokio::test]
async fn mcp_refuses_an_oversized_body_declared_or_streamed() {
    let router = router();
    let mut request = rpc_request(Some(ADMIN_KEY), None, json!({}));
    *request.body_mut() = Body::from(vec![b' '; MCP_MAX_REQUEST_BODY_BYTES + 1]);
    let (status, headers, body) = send(&router, request).await;
    assert_refused(
        status,
        &headers,
        &body,
        StatusCode::PAYLOAD_TOO_LARGE,
        refusal::MCP_REQUEST_TOO_LARGE,
    );

    let mut request = rpc_request(Some(ADMIN_KEY), None, json!({}));
    request.headers_mut().insert(
        header::CONTENT_LENGTH,
        HeaderValue::from(MCP_MAX_REQUEST_BODY_BYTES as u64 + 1),
    );
    let (status, headers, body) = send(&router, request).await;
    assert_refused(
        status,
        &headers,
        &body,
        StatusCode::PAYLOAD_TOO_LARGE,
        refusal::MCP_REQUEST_TOO_LARGE,
    );

    // A body of exactly the cap is read: here it is padding around a valid
    // envelope.
    let envelope = list_tools().to_string();
    let mut padded = envelope.into_bytes();
    padded.resize(MCP_MAX_REQUEST_BODY_BYTES, b' ');
    let mut request = rpc_request(Some(ADMIN_KEY), None, json!({}));
    *request.body_mut() = Body::from(padded);
    let (status, _, body) = send(&router, request).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body["result"]["tools"].is_array(), "{body}");
}

#[tokio::test]
async fn mcp_serves_whoami_and_lists_every_tool_in_order() {
    let router = router();
    let (status, headers, body) =
        send(&router, rpc_request(Some(READ_KEY), None, initialize())).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(headers[header::CONTENT_TYPE], "application/json");
    assert_eq!(headers[header::CACHE_CONTROL], "no-store");
    // Stateless: nothing is kept between requests, so no session is issued.
    assert!(headers.get("mcp-session-id").is_none());
    let result = &body["result"];
    assert_eq!(result["serverInfo"]["name"], "weaver");
    assert_eq!(result["serverInfo"]["version"], env!("CARGO_PKG_VERSION"));
    assert!(result["capabilities"]["tools"].is_object());
    assert!(result["capabilities"]["prompts"].is_null());
    assert!(result["capabilities"]["resources"].is_null());
    assert!(result["instructions"].is_string());

    let (status, _, body) = send(&router, rpc_request(Some(READ_KEY), None, list_tools())).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let tools = body["result"]["tools"].as_array().expect("tools");
    let names = tools
        .iter()
        .map(|tool| tool["name"].as_str().expect("name"))
        .collect::<Vec<_>>();
    assert_eq!(names, TOOL_NAMES);
    for tool in tools {
        let name = &tool["name"];
        assert!(tool.get("title").is_none(), "{name}");
        assert!(tool["description"].is_string(), "{name}");
        let annotations = &tool["annotations"];
        if *name == "run_mutation" {
            assert_eq!(annotations["readOnlyHint"], false, "{name}");
            assert_eq!(annotations["destructiveHint"], true, "{name}");
            assert_eq!(annotations["idempotentHint"], false, "{name}");
        } else {
            assert_eq!(annotations["readOnlyHint"], true, "{name}");
            assert!(annotations.get("destructiveHint").is_none(), "{name}");
        }
        let open_world = *name == "run_query" || *name == "run_mutation";
        assert_eq!(annotations["openWorldHint"], open_world, "{name}");
        assert_eq!(tool["inputSchema"]["type"], "object", "{name}");
        assert!(tool["inputSchema"]["properties"].is_object(), "{name}");
    }
    assert_eq!(
        tools[0]["inputSchema"],
        json!({"type": "object", "properties": {}})
    );

    for (key, scope) in [
        (READ_KEY, "READ"),
        (CONTROL_KEY, "CONTROL"),
        (ADMIN_KEY, "ADMIN"),
    ] {
        let body = call(&router, key, "whoami", json!({})).await;
        assert_eq!(success("whoami", &body), json!({"scope": scope}));
    }
}

#[tokio::test]
async fn mcp_refuses_unknown_tools_and_bad_arguments_without_echoing_them() {
    let router = router();
    let body = call(&router, READ_KEY, "mcp-fixture-not-a-tool", json!({})).await;
    assert_eq!(protocol_error_reason(&body), refusal::MCP_TOOL_UNKNOWN);
    assert!(!body.to_string().contains("mcp-fixture-not-a-tool"), "{body}");

    for (name, arguments, argument) in [
        (
            "run_query",
            json!({"name": "FixtureVersion", "mcpfixtureextra": 1}),
            None,
        ),
        ("run_query", json!({"name": 5}), Some("name")),
        ("run_query", json!({"name": "x".repeat(101)}), Some("name")),
        ("run_query", json!({"variables": {}}), Some("name")),
        (
            "run_query",
            json!({"name": "FixtureVersion", "variables": [1]}),
            Some("variables"),
        ),
        (
            "describe_operation",
            json!({"name": "FixtureVersion", "document": "yes"}),
            Some("document"),
        ),
        ("list_operations", json!({"skill": 5}), Some("skill")),
        ("whoami", json!({"user": "someone"}), None),
        (
            "run_mutation",
            json!({"name": "FixturePauseJob", "confirm_token": "x".repeat(65)}),
            Some("confirm_token"),
        ),
    ] {
        let body = call(&router, READ_KEY, name, arguments.clone()).await;
        assert_eq!(
            protocol_error_reason(&body),
            refusal::MCP_ARGUMENTS_INVALID,
            "{name} {arguments}"
        );
        assert_eq!(
            body["error"]["data"].get("argument").and_then(Value::as_str),
            argument,
            "{name} {arguments}"
        );
        assert!(!body.to_string().contains("mcpfixtureextra"), "{body}");
        assert!(!body.to_string().contains("someone"), "{body}");
    }

    // A skill with no served operation is a coded refusal naming the skills
    // there are, and never echoes the one sent.
    let body = call(
        &router,
        READ_KEY,
        "list_operations",
        json!({"skill": "mcp-fixture-no-skill"}),
    )
    .await;
    let error = tool_error("list_operations", &body);
    assert_eq!(error["code"], "INVALID_INPUT", "{body}");
    assert_eq!(error["reason"], refusal::MCP_SKILL_UNKNOWN, "{body}");
    assert_eq!(error["skills"], json!(["mcp-fixture-a", "mcp-fixture-b"]));
    assert!(!body.to_string().contains("mcp-fixture-no-skill"), "{body}");

    for name in ["run_query", "describe_operation"] {
        let body = call(
            &router,
            READ_KEY,
            name,
            json!({"name": "FixtureKeys", "skill": "mcp-fixture-b"}),
        )
        .await;
        let error = tool_error(name, &body);
        assert_eq!(error["reason"], refusal::MCP_OPERATION_UNKNOWN, "{body}");
        assert_eq!(error["skills"], json!(["mcp-fixture-a"]), "{body}");

        let body = call(&router, READ_KEY, name, json!({"name": "NotServed"})).await;
        let error = tool_error(name, &body);
        assert_eq!(error["reason"], refusal::MCP_OPERATION_UNKNOWN, "{body}");
        assert!(error.get("skills").is_none(), "{body}");
        assert!(!body.to_string().contains("NotServed"), "{body}");
    }
}

#[tokio::test]
async fn list_operations_groups_the_catalog_by_skill_and_filters_to_one() {
    let router = router();
    let body = call(&router, READ_KEY, "list_operations", json!({})).await;
    let groups = success("list_operations", &body);
    let a = groups["mcp-fixture-a"].as_array().expect("skill a");
    assert!(
        a.contains(&json!(["FixtureVersion", "q", "The running version."])),
        "{body}"
    );
    assert!(
        a.contains(&json!(["FixturePauseJob", "m", "Pause one job."])),
        "{body}"
    );
    // An operation with no comment takes its root field's schema
    // description.
    let keys = a
        .iter()
        .find(|row| row[0] == "FixtureKeys")
        .expect("FixtureKeys row");
    assert_eq!(keys[1], "q");
    assert!(keys[2].is_string(), "{body}");

    let body = call(
        &router,
        READ_KEY,
        "list_operations",
        json!({"skill": "mcp-fixture-b"}),
    )
    .await;
    let groups = success("list_operations", &body);
    assert_eq!(
        groups.as_object().unwrap().keys().collect::<Vec<_>>(),
        ["mcp-fixture-b"]
    );
}

#[tokio::test]
async fn describe_operation_gives_variables_notes_and_the_document_on_request() {
    let router = router();
    let body = call(
        &router,
        READ_KEY,
        "describe_operation",
        json!({"name": "FixturePauseJob"}),
    )
    .await;
    let described = success("describe_operation", &body);
    assert_eq!(described["name"], "FixturePauseJob");
    assert_eq!(described["kind"], "m");
    assert_eq!(described["confirm"], true);
    assert_eq!(described["notes"], "Pause one job.\nVariables: {\"id\": 7}");
    assert_eq!(described["variables"], json!({"id": "Int!"}));
    assert!(described.get("document").is_none());

    let body = call(
        &router,
        READ_KEY,
        "describe_operation",
        json!({"name": "FixtureVersion", "document": true}),
    )
    .await;
    let described = success("describe_operation", &body);
    assert_eq!(described["kind"], "q");
    assert!(described.get("confirm").is_none());
    assert_eq!(described["document"], FIXTURE_FILES[0].text);
}

#[tokio::test]
async fn a_name_that_differs_between_skills_needs_its_skill() {
    let router = router();
    let body = call(
        &router,
        READ_KEY,
        "run_query",
        json!({"name": "FixtureSplit"}),
    )
    .await;
    let error = tool_error("run_query", &body);
    assert_eq!(error["reason"], refusal::MCP_OPERATION_AMBIGUOUS, "{body}");
    assert_eq!(error["skills"], json!(["mcp-fixture-a", "mcp-fixture-b"]));

    let body = call(
        &router,
        READ_KEY,
        "run_query",
        json!({"name": "FixtureSplit", "skill": "mcp-fixture-b"}),
    )
    .await;
    let data = success("run_query", &body);
    assert_eq!(data["version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(data["__typename"], "QueryRoot");

    // Identical texts are one operation, run without a skill.
    let body = call(
        &router,
        READ_KEY,
        "run_query",
        json!({"name": "FixtureVersion"}),
    )
    .await;
    assert_eq!(
        success("run_query", &body),
        json!({"version": env!("CARGO_PKG_VERSION")})
    );
}

#[tokio::test]
async fn run_query_maps_graphql_refusals_and_refuses_the_other_kind() {
    let router = router();
    // The schema's own guard refuses a read key, and its code comes through.
    let body = call(
        &router,
        READ_KEY,
        "run_query",
        json!({"name": "FixtureKeys"}),
    )
    .await;
    let error = tool_error("run_query", &body);
    assert_eq!(error["code"], "FORBIDDEN", "{body}");
    assert_eq!(error["message"], "admin scope required", "{body}");
    assert_eq!(error["path"], json!(["apiKeys"]), "{body}");
    assert!(error.get("reason").is_none(), "{body}");

    for (tool, name, other) in [
        ("run_query", "FixturePauseJob", "run_mutation"),
        ("run_mutation", "FixtureVersion", "run_query"),
    ] {
        let body = call(&router, ADMIN_KEY, tool, json!({"name": name})).await;
        let error = tool_error(tool, &body);
        assert_eq!(error["code"], "INVALID_INPUT", "{body}");
        assert_eq!(
            error["reason"],
            refusal::MCP_OPERATION_KIND_MISMATCH,
            "{body}"
        );
        assert_eq!(error["tool"], other, "{body}");
    }
}

#[tokio::test]
async fn a_result_past_the_cap_is_refused_with_its_sizes() {
    let router = router();
    let mut request = rpc_request(
        Some(READ_KEY),
        None,
        call_tool("run_query", json!({"name": "FixtureVersion"})),
    );
    request.extensions_mut().insert(ResultCap(8));
    let (status, _, body) = send(&router, request).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let error = tool_error("run_query", &body);
    assert_eq!(error["code"], "PAYLOAD_TOO_LARGE", "{body}");
    assert_eq!(error["reason"], refusal::MCP_RESULT_TOO_LARGE, "{body}");
    assert_eq!(error["max_bytes"], 8, "{body}");
    let expected = json!({"version": env!("CARGO_PKG_VERSION")})
        .to_string()
        .len();
    assert_eq!(error["result_bytes"], expected, "{body}");
}

#[tokio::test]
async fn a_lasting_mutation_runs_only_on_a_confirmed_second_call() {
    let router = router();
    let arguments = json!({"name": "FixturePauseJob", "variables": {"id": 7}});
    let with_token = |token: &str, id: u64| {
        json!({
            "name": "FixturePauseJob",
            "variables": {"id": id},
            "confirm_token": token,
        })
    };

    // The first call runs nothing and summarizes the call.
    let body = call(&router, CONTROL_KEY, "run_mutation", arguments.clone()).await;
    let pending = success("run_mutation", &body);
    assert_eq!(pending["summary"]["operation"], "FixturePauseJob");
    assert_eq!(pending["summary"]["skills"], json!(["mcp-fixture-a"]));
    assert_eq!(pending["summary"]["purpose"], "Pause one job.");
    assert_eq!(pending["summary"]["variables"], json!({"id": 7}));
    assert_eq!(pending["expires_in_seconds"], 300);
    let token = confirm_token(&body);
    assert_eq!(token.len(), 64);

    // A changed call spends the token without running.
    let body = call(&router, CONTROL_KEY, "run_mutation", with_token(&token, 8)).await;
    let error = tool_error("run_mutation", &body);
    assert_eq!(error["reason"], refusal::MCP_CONFIRMATION_MISMATCH, "{body}");
    let body = call(&router, CONTROL_KEY, "run_mutation", with_token(&token, 7)).await;
    let error = tool_error("run_mutation", &body);
    assert_eq!(error["reason"], refusal::MCP_CONFIRMATION_UNKNOWN, "{body}");

    // Another key can neither use nor spend a token.
    let token = confirm_token(&call(&router, CONTROL_KEY, "run_mutation", arguments.clone()).await);
    let body = call(&router, ADMIN_KEY, "run_mutation", with_token(&token, 7)).await;
    let error = tool_error("run_mutation", &body);
    assert_eq!(error["reason"], refusal::MCP_CONFIRMATION_UNKNOWN, "{body}");

    // The confirmed call runs. The fixture schema has no scheduler, so the
    // run fails inside the resolver: proof it got past the guard.
    let body = call(&router, CONTROL_KEY, "run_mutation", with_token(&token, 7)).await;
    let error = tool_error("run_mutation", &body);
    assert_eq!(error["code"], refusal::MCP_GRAPHQL_ERROR, "{body}");
    assert_eq!(error["path"], json!(["pauseJob"]), "{body}");

    // A token works once.
    let body = call(&router, CONTROL_KEY, "run_mutation", with_token(&token, 7)).await;
    let error = tool_error("run_mutation", &body);
    assert_eq!(error["reason"], refusal::MCP_CONFIRMATION_UNKNOWN, "{body}");

    // A confirmed call still meets the schema's guard.
    let token = confirm_token(&call(&router, READ_KEY, "run_mutation", arguments).await);
    let body = call(&router, READ_KEY, "run_mutation", with_token(&token, 7)).await;
    let error = tool_error("run_mutation", &body);
    assert_eq!(error["code"], "FORBIDDEN", "{body}");
    assert_eq!(error["message"], "control scope required", "{body}");
}

#[test]
fn graphql_errors_become_tool_errors_built_from_extensions() {
    let mut extensions = async_graphql::ErrorExtensionValues::default();
    extensions.set("code", "REFUSED");
    extensions.set("params", json!({"freeBytes": 7}).to_string());
    extensions.set("matched", 3);
    let mut coded = async_graphql::ServerError::new("not enough room: code NOT_THIS", None);
    coded.extensions = Some(extensions);
    let uncoded = async_graphql::ServerError::new("Unknown field \"nope\"", None);
    let response = async_graphql::Response::from_errors(vec![coded, uncoded]);

    let (content, failed) = server::graphql_value(response);
    assert!(failed);
    assert_eq!(
        content,
        json!({"errors": [
            {
                "code": "REFUSED",
                "message": "not enough room: code NOT_THIS",
                "params": "{\"freeBytes\":7}",
                "matched": 3
            },
            {"code": refusal::MCP_GRAPHQL_ERROR, "message": "Unknown field \"nope\""}
        ]})
    );
}

#[test]
fn a_partial_graphql_result_keeps_its_data_and_error_paths() {
    let data = async_graphql::Value::from_json(json!({
        "version": "fixture",
        "jobs": null
    }))
    .expect("data");
    let mut failed = async_graphql::ServerError::new("fixture failure", None);
    failed.path = vec![
        async_graphql::PathSegment::Field("jobs".into()),
        async_graphql::PathSegment::Index(0),
    ];
    let mut response = async_graphql::Response::new(data);
    response.errors.push(failed);

    let (content, is_error) = server::graphql_value(response);
    assert!(is_error);
    assert_eq!(
        content,
        json!({
            "errors": [{
                "code": refusal::MCP_GRAPHQL_ERROR,
                "message": "fixture failure",
                "path": ["jobs", 0]
            }],
            "data": {"version": "fixture", "jobs": null}
        })
    );

    // Data with nothing resolved is left out.
    let mut response = async_graphql::Response::new(
        async_graphql::Value::from_json(json!({"version": null})).unwrap(),
    );
    response
        .errors
        .push(async_graphql::ServerError::new("fixture failure", None));
    let (content, _) = server::graphql_value(response);
    assert!(content.get("data").is_none(), "{content}");
}
