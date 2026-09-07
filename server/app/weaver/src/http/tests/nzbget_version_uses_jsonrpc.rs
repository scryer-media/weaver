//! `tests` tests, part of a mechanical split of the original file.

use super::*;

#[tokio::test]
async fn nzbget_version_uses_jsonrpc_11_envelope_and_echoes_id() {
    let app = nzbget_test_router(
        Database::open_in_memory().unwrap(),
        test_scheduler_handle(),
        test_config(),
        ApiKeyCache::default(),
    );

    let (status, payload) = post_nzbget(
        app,
        serde_json::json!({
            "method": "version",
            "params": [],
            "id": "arr-version"
        }),
        "Bearer session-token",
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["version"], "1.1");
    assert_eq!(payload["id"], "arr-version");
    assert_eq!(payload["result"], "16.0-weaver");
}

#[tokio::test]
async fn nzbget_unknown_method_returns_nzbget_error_envelope() {
    let app = nzbget_test_router(
        Database::open_in_memory().unwrap(),
        test_scheduler_handle(),
        test_config(),
        ApiKeyCache::default(),
    );

    let (status, payload) = post_nzbget(
        app,
        serde_json::json!({
            "method": "sysinfo",
            "params": [],
            "id": 12
        }),
        "Bearer session-token",
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["version"], "1.1");
    assert_eq!(payload["id"], 12);
    assert_eq!(payload["error"]["name"], "JSONRPCError");
    assert_eq!(payload["error"]["code"], 1);
}

#[tokio::test]
async fn nzbget_rbac_allows_read_keys_and_rejects_read_key_mutations() {
    let app = nzbget_test_router(
        Database::open_in_memory().unwrap(),
        test_scheduler_handle(),
        test_config(),
        api_key_cache("read-key", "read"),
    );

    let (status, payload) = post_nzbget(
        app.clone(),
        serde_json::json!({
            "method": "status",
            "params": [],
            "id": "read-ok"
        }),
        "Bearer read-key",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(payload.get("result").is_some());

    let (status, payload) = post_nzbget(
        app,
        serde_json::json!({
            "method": "append",
            "params": [],
            "id": "read-denied"
        }),
        "Bearer read-key",
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(payload["error"]["code"], 401);
}

#[tokio::test]
async fn nzbget_auth_accepts_basic_password_as_persistent_api_key() {
    let app = nzbget_test_router(
        Database::open_in_memory().unwrap(),
        test_scheduler_handle(),
        test_config(),
        ApiKeyCache::default(),
    );

    let (status, payload) = post_nzbget(
        app,
        serde_json::json!({
            "method": "version",
            "params": [],
            "id": "basic"
        }),
        &basic_auth("session-token"),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["result"], "16.0-weaver");
}

#[tokio::test]
async fn nzbget_auth_rejects_missing_and_invalid_basic_auth() {
    let app = nzbget_test_router(
        Database::open_in_memory().unwrap(),
        test_scheduler_handle(),
        test_config(),
        ApiKeyCache::default(),
    );
    let request = serde_json::json!({
        "method": "version",
        "params": [],
        "id": "auth"
    });

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/jsonrpc")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(request.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    let (status, payload) = post_nzbget(app, request, "Basic not-base64").await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(payload["error"]["code"], 401);
}

#[tokio::test]
async fn nzbget_invalid_auth_returns_without_polling_the_body() {
    let app = nzbget_test_router(
        Database::open_in_memory().unwrap(),
        test_scheduler_handle(),
        test_config(),
        ApiKeyCache::default(),
    );
    let (_writer, reader) = tokio::io::duplex(1);
    let response = tokio::time::timeout(
        std::time::Duration::from_millis(250),
        app.oneshot(
            Request::builder()
                .method("POST")
                .uri("/jsonrpc")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, "Basic not-base64")
                .body(Body::from_stream(tokio_util::io::ReaderStream::new(reader)))
                .unwrap(),
        ),
    )
    .await
    .expect("authentication must complete without polling the pending body")
    .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn nzbget_rpc_body_limit_is_exactly_32_mib() {
    let app = nzbget_test_router(
        Database::open_in_memory().unwrap(),
        test_scheduler_handle(),
        test_config(),
        ApiKeyCache::default(),
    );
    let rpc = serde_json::json!({"method": "version", "params": [], "id": "limit"})
        .to_string()
        .into_bytes();

    let mut accepted = rpc.clone();
    accepted.resize(routes::NZBGET_RPC_BODY_LIMIT_BYTES, b' ');
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/jsonrpc")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, "Bearer session-token")
                .body(Body::from(accepted))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let mut oversized = rpc;
    oversized.resize(routes::NZBGET_RPC_BODY_LIMIT_BYTES + 1, b' ');
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/jsonrpc")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, "Bearer session-token")
                .body(Body::from(oversized))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn nzbget_append_accepts_arr_v16_base64_payload_and_preserves_drone() {
    use base64::Engine as _;

    let db = Database::open_in_memory().unwrap();
    let handle = scheduler_handle_with_mock_commands(vec![]);
    let app = nzbget_test_router(
        db,
        handle.clone(),
        test_config(),
        api_key_cache("control-key", "control"),
    );
    let nzb_b64 = base64::engine::general_purpose::STANDARD
        .encode(minimal_nzb("Silver.Horizon.S05.720p.BluRay.DD5.1.x264-WVR"));

    let (status, payload) = post_nzbget(
        app,
        serde_json::json!({
            "method": "append",
            "params": [
                "Silver.Horizon.S05.720p.BluRay.DD5.1.x264-WVR.nzb",
                nzb_b64,
                "tv",
                50,
                false,
                false,
                "",
                0,
                "all",
                ["drone", "sonarrdroneid"]
            ],
            "id": "append"
        }),
        "Bearer control-key",
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert!(payload["result"].as_u64().unwrap() >= 10_000);

    let jobs = handle.list_jobs();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].category.as_deref(), Some("tv"));
    assert!(jobs[0].metadata.iter().any(|(key, value)| {
        key == weaver_server_api::CLIENT_REQUEST_ID_ATTRIBUTE_KEY && value == "sonarrdroneid"
    }));
    assert!(
        jobs[0].metadata.iter().any(|(key, value)| key
            == weaver_server_api::PRIORITY_ATTRIBUTE_KEY
            && value == "HIGH")
    );
}

#[tokio::test]
async fn nzbget_append_accepts_base64_payload_with_embedded_whitespace() {
    use base64::Engine as _;

    let handle = scheduler_handle_with_mock_commands(vec![]);
    let app = nzbget_test_router(
        Database::open_in_memory().unwrap(),
        handle.clone(),
        test_config(),
        api_key_cache("control-key", "control"),
    );
    let encoded =
        base64::engine::general_purpose::STANDARD.encode(minimal_nzb("Whitespace.Wrapped.Release"));
    // Base64 is canonically line-wrapped, and some clients pad with stray
    // spaces; with XML-RPC's `trim_text` disabled, that whitespace now
    // reaches the facade verbatim. It must be stripped at the byte level
    // before decoding rather than rejected as invalid base64.
    let wrapped = encoded
        .as_bytes()
        .chunks(16)
        .map(|chunk| String::from_utf8_lossy(chunk).into_owned())
        .collect::<Vec<_>>()
        .join("\n ");

    let (status, payload) = post_nzbget(
        app,
        serde_json::json!({
            "method": "append",
            "params": [
                "Whitespace.Wrapped.Release.nzb",
                wrapped,
                "tv",
                0,
                false,
                false,
                "",
                0,
                "all",
                []
            ],
            "id": "append-whitespace"
        }),
        "Bearer control-key",
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert!(payload["result"].as_u64().unwrap() >= 10_000);
    assert_eq!(handle.list_jobs().len(), 1);
}

#[tokio::test]
async fn nzbget_append_canonicalizes_submitted_category_for_facade() {
    use base64::Engine as _;

    let db = Database::open_in_memory().unwrap();
    let handle = scheduler_handle_with_mock_commands(vec![]);
    let config = test_config();
    {
        let mut config_write = config.write().await;
        config_write
            .categories
            .push(weaver_server_core::categories::CategoryConfig {
                id: 1,
                name: "TV".into(),
                dest_dir: None,
                aliases: String::new(),
            });
    }
    let app = nzbget_test_router(
        db,
        handle.clone(),
        config,
        api_key_cache("control-key", "control"),
    );
    let nzb_b64 =
        base64::engine::general_purpose::STANDARD.encode(minimal_nzb("Case.Category.Release"));

    let (status, payload) = post_nzbget(
        app.clone(),
        serde_json::json!({
            "method": "append",
            "params": [
                "Case.Category.Release.nzb",
                nzb_b64,
                "tv",
                0,
                false,
                false,
                "",
                0,
                "all",
                ["drone", "case-category"]
            ],
            "id": "append-category"
        }),
        "Bearer control-key",
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert!(payload["result"].as_u64().unwrap() >= 10_000);
    assert_eq!(handle.list_jobs()[0].category.as_deref(), Some("TV"));

    let (status, groups_payload) = post_nzbget(
        app,
        serde_json::json!({
            "method": "listgroups",
            "params": [],
            "id": "listgroups"
        }),
        "Bearer control-key",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(groups_payload["result"][0]["Category"], "TV");
}

#[tokio::test]
async fn nzbget_append_rejects_unsafe_category_before_queuing() {
    use base64::Engine as _;

    let db = Database::open_in_memory().unwrap();
    let handle = scheduler_handle_with_mock_commands(vec![]);
    let app = nzbget_test_router(
        db,
        handle.clone(),
        test_config(),
        api_key_cache("control-key", "control"),
    );
    let nzb_b64 =
        base64::engine::general_purpose::STANDARD.encode(minimal_nzb("Unsafe.Category.Release"));

    let (status, payload) = post_nzbget(
        app,
        serde_json::json!({
            "method": "append",
            "params": [
                "Unsafe.Category.Release.nzb",
                nzb_b64,
                "../../outside",
                0,
                false,
                false,
                "",
                0,
                "all",
                []
            ],
            "id": "append-unsafe-category"
        }),
        "Bearer control-key",
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["error"]["code"], 2);
    assert!(handle.list_jobs().is_empty());
}

#[tokio::test]
async fn nzbget_append_rejection_returns_zero_for_invalid_nzb() {
    use base64::Engine as _;

    let db = Database::open_in_memory().unwrap();
    let handle = scheduler_handle_with_mock_commands(vec![]);
    let app = nzbget_test_router(
        db,
        handle.clone(),
        test_config(),
        api_key_cache("control-key", "control"),
    );
    let invalid_nzb_b64 = base64::engine::general_purpose::STANDARD.encode("not an nzb");

    let (status, payload) = post_nzbget(
        app,
        serde_json::json!({
            "method": "append",
            "params": [
                "Invalid.Release.nzb",
                invalid_nzb_b64,
                "tv",
                0,
                false,
                false,
                "",
                0,
                "all",
                ["drone", "invalid-release"]
            ],
            "id": "append-invalid"
        }),
        "Bearer control-key",
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["result"], 0);
    assert!(handle.list_jobs().is_empty());
}

#[tokio::test]
async fn nzbget_append_add_paused_is_initially_paused() {
    use base64::Engine as _;

    let db = Database::open_in_memory().unwrap();
    let handle = scheduler_handle_with_mock_commands(vec![]);
    let app = nzbget_test_router(
        db,
        handle.clone(),
        test_config(),
        api_key_cache("control-key", "control"),
    );
    let nzb_b64 = base64::engine::general_purpose::STANDARD.encode(minimal_nzb("Paused.Release"));

    let (status, payload) = post_nzbget(
        app.clone(),
        serde_json::json!({
            "method": "append",
            "params": [
                "Paused.Release.nzb",
                nzb_b64,
                "tv",
                0,
                false,
                true,
                "",
                0,
                "all",
                ["drone", "paused-release"]
            ],
            "id": "append-paused"
        }),
        "Bearer control-key",
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert!(payload["result"].as_u64().unwrap() >= 10_000);
    let jobs = handle.list_jobs();
    assert_eq!(jobs[0].status, JobStatus::Paused);
    assert_eq!(
        jobs[0].download_state,
        weaver_server_core::DownloadState::Queued
    );
    assert_eq!(jobs[0].run_state, weaver_server_core::RunState::Paused);

    let (status, groups_payload) = post_nzbget(
        app,
        serde_json::json!({
            "method": "listgroups",
            "params": [],
            "id": "listgroups-paused"
        }),
        "Bearer control-key",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(groups_payload["result"][0]["Status"], "PAUSED");
}

#[tokio::test]
async fn nzbget_append_rejects_private_url_payloads_for_prowlarr_shape() {
    let db = Database::open_in_memory().unwrap();
    let handle = scheduler_handle_with_mock_commands(vec![]);
    let app = nzbget_test_router(
        db,
        handle.clone(),
        test_config(),
        api_key_cache("control-key", "control"),
    );

    let (status, payload) = post_nzbget(
        app,
        serde_json::json!({
            "method": "append",
            "params": [
                "",
                "http://127.0.0.1:9/download.nzb",
                "Prowlarr",
                0,
                false,
                false,
                "",
                0,
                "all",
                ["drone", "prowlarrdroneid"]
            ],
            "id": "append-url"
        }),
        "Bearer control-key",
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["error"]["code"], 2);
    assert!(
        payload["error"]["message"]
            .as_str()
            .unwrap()
            .contains("not allowed")
    );
    assert!(handle.list_jobs().is_empty());
}

#[tokio::test]
async fn nzbget_status_and_listgroups_support_sonarr_progress_queries() {
    let job = nzbget_test_job(
        42,
        JobStatus::Downloading,
        weaver_server_core::DownloadState::Downloading,
        6_000_000_000,
        1_500_000_000,
        vec![
            (
                weaver_server_api::CLIENT_REQUEST_ID_ATTRIBUTE_KEY.to_string(),
                "drone-progress".to_string(),
            ),
            (
                weaver_server_api::PRIORITY_ATTRIBUTE_KEY.to_string(),
                "HIGH".to_string(),
            ),
            (
                weaver_server_core::ingest::ORIGINAL_TITLE_METADATA_KEY.to_string(),
                "Silver.Horizon.S05E01.720p.BluRay.DD5.1.x264-WVR".to_string(),
            ),
            ("drone".to_string(), "spoofed-drone".to_string()),
        ],
    );
    let metrics = PipelineMetrics::new();
    let (cmd_tx, _cmd_rx) = mpsc::channel(1);
    let (event_tx, _) = broadcast::channel(1);
    let shared_state = SharedPipelineState::new(metrics.clone(), vec![job]);
    tokio::time::sleep(std::time::Duration::from_millis(60)).await;
    metrics
        .bytes_downloaded
        .store(1_048_576, std::sync::atomic::Ordering::Relaxed);
    shared_state.refresh_metrics_snapshot();
    let handle = SchedulerHandle::new(cmd_tx, event_tx, shared_state);
    let config = test_config();
    config
        .write()
        .await
        .servers
        .push(weaver_server_core::servers::ServerConfig {
            id: 7,
            host: "news.example.com".into(),
            port: 563,
            tls: true,
            username: None,
            password: None,
            connections: 8,
            active: true,
            supports_pipelining: false,
            pipelining_depth: None,
            priority: 0,
            backfill: false,
            retention_days: 0,
            max_download_speed: 0,
            download_quota: Default::default(),
            tls_ca_cert: None,
            tls_name_mismatch_certificate_der: None,
        });
    let app = nzbget_test_router(
        Database::open_in_memory().unwrap(),
        handle,
        config,
        ApiKeyCache::default(),
    );

    let (status, status_payload) = post_nzbget(
        app.clone(),
        serde_json::json!({
            "method": "status",
            "params": [],
            "id": "status"
        }),
        "Bearer session-token",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(status_payload["result"]["RemainingSizeHi"], 1);
    assert_eq!(status_payload["result"]["DownloadPaused"], false);
    assert!(
        status_payload["result"]["DownloadRate"].as_u64().unwrap() > 0,
        "status should use the speed-bearing metrics snapshot"
    );
    // FreeDiskSpaceMB must stay numeric after moving the disk_space() lookup
    // off the config-lock critical section and behind the TTL cache — a
    // missing/unreadable complete_dir degrades to 0, never a null or error.
    assert!(status_payload["result"]["FreeDiskSpaceMB"].is_u64());
    assert_eq!(
        status_payload["result"]["NewsServers"],
        serde_json::json!([{"ID": 7, "Active": true}])
    );

    let auth = basic_auth("session-token");
    let (xml_status, xml_body) = post_nzbget_xmlrpc(
        app.clone(),
        "<methodCall><methodName>status</methodName></methodCall>",
        &auth,
    )
    .await;
    assert_eq!(xml_status, StatusCode::OK);
    assert!(xml_body.contains("<name>NewsServers</name><value><array><data>"));
    assert!(xml_body.contains("<name>Active</name><value><boolean>1</boolean></value>"));
    assert!(xml_body.contains("<name>ID</name><value><i4>7</i4></value>"));

    let (status, groups_payload) = post_nzbget(
        app,
        serde_json::json!({
            "method": "listgroups",
            "params": [],
            "id": "listgroups"
        }),
        "Bearer session-token",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let group = &groups_payload["result"][0];
    assert_eq!(group["NZBID"], 42);
    assert_eq!(
        group["NZBName"],
        "Silver.Horizon.S05E01.720p.BluRay.DD5.1.x264-WVR"
    );
    assert_eq!(group["FileSizeHi"], 1);
    assert_eq!(group["RemainingSizeHi"], 1);
    assert_eq!(group["PausedSizeLo"], 0);
    assert_eq!(group["ActiveDownloads"], 1);
    assert_eq!(group["Status"], "DOWNLOADING");
    assert_eq!(group["FileCount"], 2);
    assert_eq!(group["RemainingFileCount"], 1);
    assert_eq!(group["RemainingParCount"], 1);
    let parameters = group["Parameters"].as_array().unwrap();
    let drone_parameters = parameters
        .iter()
        .filter(|parameter| parameter["Name"] == "drone")
        .collect::<Vec<_>>();
    assert_eq!(drone_parameters.len(), 1);
    assert_eq!(drone_parameters[0]["Value"], "drone-progress");
}

#[tokio::test]
async fn nzbget_status_clamps_download_rate_to_arr_int() {
    let metrics = PipelineMetrics::new();
    let (cmd_tx, _cmd_rx) = mpsc::channel(1);
    let (event_tx, _) = broadcast::channel(1);
    let shared_state = SharedPipelineState::new(metrics.clone(), vec![]);
    tokio::time::sleep(std::time::Duration::from_millis(60)).await;
    metrics
        .bytes_downloaded
        .store((i32::MAX as u64) * 4, std::sync::atomic::Ordering::Relaxed);
    shared_state.refresh_metrics_snapshot();
    assert!(shared_state.metrics_snapshot().current_download_speed > i32::MAX as u64);
    let handle = SchedulerHandle::new(cmd_tx, event_tx, shared_state);
    let config = test_config();
    {
        let mut config_write = config.write().await;
        config_write.max_download_speed = Some((i32::MAX as u64) * 4);
    }
    let app = nzbget_test_router(
        Database::open_in_memory().unwrap(),
        handle,
        config,
        ApiKeyCache::default(),
    );

    let (status, payload) = post_nzbget(
        app,
        serde_json::json!({
            "method": "status",
            "params": [],
            "id": "status-clamp"
        }),
        "Bearer session-token",
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["result"]["DownloadRate"], i32::MAX);
    assert_eq!(payload["result"]["AverageDownloadRate"], i32::MAX);
    assert_eq!(payload["result"]["DownloadLimit"], i32::MAX);
}

#[tokio::test]
async fn nzbget_history_returns_arr_status_fields_and_drone_parameter() {
    let db = Database::open_in_memory().unwrap();
    let metadata = serde_json::to_string(&vec![
        (
            weaver_server_api::CLIENT_REQUEST_ID_ATTRIBUTE_KEY.to_string(),
            "drone-history".to_string(),
        ),
        (
            weaver_server_core::ingest::ORIGINAL_TITLE_METADATA_KEY.to_string(),
            "Complete.Release.S01E01.1080p.WEB-DL".to_string(),
        ),
    ])
    .unwrap();
    db.insert_job_history(&weaver_server_core::JobHistoryRow {
        job_id: 100,
        job_hash: None,
        name: "Complete.Release".into(),
        status: "complete".into(),
        error_message: None,
        total_bytes: 123,
        downloaded_bytes: 123,
        optional_recovery_bytes: 0,
        optional_recovery_downloaded_bytes: 0,
        failed_bytes: 0,
        health: 1000,
        category: Some("tv".into()),
        output_dir: Some("/downloads/tv/Complete.Release".into()),
        nzb_path: None,
        created_at: 1_700_000_000,
        completed_at: 1_700_000_100,
        metadata: Some(metadata),
    })
    .unwrap();
    db.insert_job_history(&weaver_server_core::JobHistoryRow {
        job_id: 101,
        job_hash: None,
        name: "Failed.Release".into(),
        status: "failed".into(),
        error_message: Some("article failures".into()),
        total_bytes: 456,
        downloaded_bytes: 100,
        optional_recovery_bytes: 0,
        optional_recovery_downloaded_bytes: 0,
        failed_bytes: 356,
        health: 100,
        category: Some("tv".into()),
        output_dir: Some("/downloads/tv/Failed.Release".into()),
        nzb_path: None,
        created_at: 1_700_000_000,
        completed_at: 1_700_000_200,
        metadata: None,
    })
    .unwrap();
    let app = nzbget_test_router(
        db,
        test_scheduler_handle(),
        test_config(),
        ApiKeyCache::default(),
    );

    let (status, payload) = post_nzbget(
        app,
        serde_json::json!({
            "method": "history",
            "params": [],
            "id": "history"
        }),
        "Bearer session-token",
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let items = payload["result"].as_array().unwrap();
    let complete = items.iter().find(|item| item["ID"] == 100).unwrap();
    let failed = items.iter().find(|item| item["ID"] == 101).unwrap();
    assert_eq!(complete["NZBName"], "Complete.Release.S01E01.1080p.WEB-DL");
    assert_eq!(complete["ParStatus"], "SUCCESS");
    assert_eq!(complete["UnpackStatus"], "SUCCESS");
    assert_eq!(complete["Parameters"][0]["Name"], "drone");
    assert_eq!(complete["Parameters"][0]["Value"], "drone-history");
    // A failed job no longer claims a PAR failure it can't attribute. Sonarr/Radarr
    // read failure only from the granular fields, so the failure is signaled via
    // DeleteStatus="HEALTH" (their delete-failed set), not the compound Status;
    // par/unpack stay NONE (no false stage claim).
    assert_eq!(failed["ParStatus"], "NONE");
    assert_eq!(failed["Status"], "FAILURE/HEALTH");
    assert_eq!(failed["DeleteStatus"], "HEALTH");
    assert_eq!(failed["Message"], "article failures");
}

#[tokio::test]
async fn nzbget_history_includes_terminal_memory_items_missing_from_db() {
    let job = nzbget_test_job(
        202,
        JobStatus::Complete,
        weaver_server_core::DownloadState::Complete,
        123,
        123,
        vec![(
            weaver_server_api::CLIENT_REQUEST_ID_ATTRIBUTE_KEY.to_string(),
            "drone-terminal-memory".to_string(),
        )],
    );
    let handle = scheduler_handle_with_mock_commands(vec![job]);
    let app = nzbget_test_router(
        Database::open_in_memory().unwrap(),
        handle,
        test_config(),
        ApiKeyCache::default(),
    );

    let (status, groups_payload) = post_nzbget(
        app.clone(),
        serde_json::json!({
            "method": "listgroups",
            "params": [],
            "id": "listgroups-terminal"
        }),
        "Bearer session-token",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(groups_payload["result"].as_array().unwrap().is_empty());

    let (status, history_payload) = post_nzbget(
        app,
        serde_json::json!({
            "method": "history",
            "params": [],
            "id": "history-terminal"
        }),
        "Bearer session-token",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let items = history_payload["result"].as_array().unwrap();
    let item = items.iter().find(|item| item["ID"] == 202).unwrap();
    assert_eq!(item["ParStatus"], "SUCCESS");
    assert_eq!(item["Parameters"][0]["Name"], "drone");
    assert_eq!(item["Parameters"][0]["Value"], "drone-terminal-memory");
}

#[tokio::test]
async fn nzbget_history_prefers_persisted_rows_over_terminal_duplicates() {
    let db = Database::open_in_memory().unwrap();
    db.insert_job_history(&nzbget_history_row(
        202,
        "complete",
        1_700_000_300,
        Some(drone_metadata("drone-db-duplicate")),
    ))
    .unwrap();
    db.insert_job_history(&nzbget_history_row(
        203,
        "complete",
        1_700_000_400,
        Some(drone_metadata("drone-db-newer")),
    ))
    .unwrap();
    let job = nzbget_test_job(
        202,
        JobStatus::Complete,
        weaver_server_core::DownloadState::Complete,
        123,
        123,
        vec![(
            weaver_server_api::CLIENT_REQUEST_ID_ATTRIBUTE_KEY.to_string(),
            "drone-terminal-memory".to_string(),
        )],
    );
    let app = nzbget_test_router(
        db,
        scheduler_handle_with_mock_commands(vec![job]),
        test_config(),
        ApiKeyCache::default(),
    );

    let (status, history_payload) = post_nzbget(
        app,
        serde_json::json!({
            "method": "history",
            "params": [],
            "id": "history-terminal-first"
        }),
        "Bearer session-token",
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let items = history_payload["result"].as_array().unwrap();
    let item = items.iter().find(|item| item["ID"] == 202).unwrap();
    assert_eq!(
        items.iter().filter(|item| item["ID"] == 202).count(),
        1,
        "persisted and terminal entries must be deduplicated"
    );
    assert_eq!(item["Parameters"][0]["Value"], "drone-db-duplicate");
    assert_eq!(item["HistoryTime"], 1_700_000_300);
    assert!(items.iter().any(|item| item["ID"] == 203));
}

#[tokio::test]
async fn nzbget_history_maps_cancelled_db_rows_to_manual_delete() {
    let db = Database::open_in_memory().unwrap();
    db.insert_job_history(&nzbget_history_row(
        301,
        "cancelled",
        1_700_000_500,
        Some(drone_metadata("drone-cancelled")),
    ))
    .unwrap();
    let terminal = nzbget_test_job(
        301,
        JobStatus::Failed {
            error: "runtime failure".to_string(),
        },
        weaver_server_core::DownloadState::Failed,
        123,
        123,
        vec![(
            weaver_server_api::CLIENT_REQUEST_ID_ATTRIBUTE_KEY.to_string(),
            "drone-terminal-failed".to_string(),
        )],
    );
    let app = nzbget_test_router(
        db,
        scheduler_handle_with_mock_commands(vec![terminal]),
        test_config(),
        ApiKeyCache::default(),
    );

    let (status, history_payload) = post_nzbget(
        app,
        serde_json::json!({
            "method": "history",
            "params": [],
            "id": "history-cancelled"
        }),
        "Bearer session-token",
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let items = history_payload["result"].as_array().unwrap();
    let item = items.iter().find(|item| item["ID"] == 301).unwrap();
    assert_eq!(item["DeleteStatus"], "MANUAL");
    assert_eq!(item["ParStatus"], "NONE");
    assert_eq!(item["UnpackStatus"], "NONE");
    assert_eq!(item["MoveStatus"], "NONE");
    assert_eq!(item["ScriptStatus"], "NONE");
    assert_eq!(item["MarkStatus"], "NONE");
    assert_eq!(item["Parameters"][0]["Value"], "drone-cancelled");
    assert_eq!(item["HistoryTime"], 1_700_000_500);
}

#[tokio::test]
async fn nzbget_history_repeat_poll_is_memo_transparent() {
    let db = Database::open_in_memory().unwrap();
    db.insert_job_history(&nzbget_history_row(
        401,
        "complete",
        1_700_000_600,
        Some(drone_metadata("drone-memo")),
    ))
    .unwrap();
    let app = nzbget_test_router(
        db,
        test_scheduler_handle(),
        test_config(),
        ApiKeyCache::default(),
    );

    let (status_1, payload_1) = post_nzbget(
        app.clone(),
        serde_json::json!({"method": "history", "params": [], "id": "history-memo-1"}),
        "Bearer session-token",
    )
    .await;
    let (status_2, payload_2) = post_nzbget(
        app,
        serde_json::json!({"method": "history", "params": [], "id": "history-memo-2"}),
        "Bearer session-token",
    )
    .await;

    assert_eq!(status_1, StatusCode::OK);
    assert_eq!(status_2, StatusCode::OK);
    // The second poll hits the per-job-id memo (completed_at unchanged), and
    // must reproduce the exact same entry as the freshly-built first poll.
    assert_eq!(payload_1["result"], payload_2["result"]);
    let items = payload_2["result"].as_array().unwrap();
    let item = items.iter().find(|item| item["ID"] == 401).unwrap();
    assert_eq!(item["ParStatus"], "SUCCESS");
    assert_eq!(item["Parameters"][0]["Value"], "drone-memo");
}

#[tokio::test]
async fn nzbget_config_exposes_real_categories_and_keep_history() {
    let config = test_config();
    {
        let mut config_write = config.write().await;
        config_write
            .categories
            .push(weaver_server_core::categories::CategoryConfig {
                id: 1,
                name: "tv".into(),
                dest_dir: Some("/media/tv".into()),
                aliases: "series,shows".into(),
            });
    }
    let app = nzbget_test_router(
        Database::open_in_memory().unwrap(),
        test_scheduler_handle(),
        config,
        ApiKeyCache::default(),
    );

    let (status, payload) = post_nzbget(
        app,
        serde_json::json!({
            "method": "config",
            "params": [],
            "id": "config"
        }),
        "Bearer session-token",
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let entries = payload["result"].as_array().unwrap();
    let value_for = |name: &str| {
        entries
            .iter()
            .find(|entry| entry["Name"] == name)
            .and_then(|entry| entry["Value"].as_str())
            .unwrap()
    };
    assert_eq!(value_for("KeepHistory"), "7");
    assert_eq!(value_for("Category1.Name"), "tv");
    assert_eq!(value_for("Category1.DestDir"), "/media/tv");
    assert_eq!(value_for("Category1.Aliases"), "series,shows");
}

#[tokio::test]
async fn nzbget_config_emits_virtual_literal_alias_categories_for_arr_test() {
    let config = test_config();
    {
        let mut config_write = config.write().await;
        config_write
            .categories
            .push(weaver_server_core::categories::CategoryConfig {
                id: 1,
                name: "TV".into(),
                dest_dir: Some("/media/tv".into()),
                aliases: "sonarr, movie*".into(),
            });
    }
    let app = nzbget_test_router(
        Database::open_in_memory().unwrap(),
        test_scheduler_handle(),
        config,
        ApiKeyCache::default(),
    );

    let (status, payload) = post_nzbget(
        app,
        serde_json::json!({
            "method": "config",
            "params": [],
            "id": "config-aliases"
        }),
        "Bearer session-token",
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let entries = payload["result"].as_array().unwrap();
    let value_for = |name: &str| {
        entries
            .iter()
            .find(|entry| entry["Name"] == name)
            .and_then(|entry| entry["Value"].as_str())
            .unwrap()
    };
    let category_names = (1..=3)
        .map(|index| value_for(&format!("Category{index}.Name")))
        .collect::<Vec<_>>();

    assert_eq!(category_names, vec!["TV", "tv", "sonarr"]);
    assert_eq!(value_for("Category1.DestDir"), "/media/tv");
    assert_eq!(value_for("Category2.DestDir"), "/media/tv");
    assert_eq!(value_for("Category3.DestDir"), "/media/tv");
    assert!(entries.iter().all(|entry| entry["Value"] != "movie*"));
}

#[tokio::test]
async fn nzbget_group_final_delete_does_not_resurface_cancelled_history() {
    let db = Database::open_in_memory().unwrap();
    let job = nzbget_test_job(
        77,
        JobStatus::Queued,
        weaver_server_core::DownloadState::Queued,
        100,
        0,
        vec![(
            weaver_server_api::CLIENT_REQUEST_ID_ATTRIBUTE_KEY.to_string(),
            "drone-delete".to_string(),
        )],
    );
    let handle = scheduler_handle_with_mock_commands_and_db(vec![job], db.clone());
    let app = nzbget_test_router(
        db.clone(),
        handle.clone(),
        test_config(),
        api_key_cache("control-key", "control"),
    );

    let (status, payload) = post_nzbget(
        app.clone(),
        serde_json::json!({
            "method": "editqueue",
            "params": ["GroupFinalDelete", 0, "", 77],
            "id": "delete"
        }),
        "Bearer control-key",
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["result"], true);
    assert!(handle.list_jobs().is_empty());
    assert!(db.get_job_history(77).unwrap().is_none());

    let (status, history_payload) = post_nzbget(
        app,
        serde_json::json!({
            "method": "history",
            "params": [],
            "id": "history"
        }),
        "Bearer control-key",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(history_payload["result"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn nzbget_editqueue_maps_arr_actions() {
    let job = nzbget_test_job(
        77,
        JobStatus::Queued,
        weaver_server_core::DownloadState::Queued,
        100,
        0,
        vec![],
    );
    let handle = scheduler_handle_with_mock_commands(vec![job]);
    let app = nzbget_test_router(
        Database::open_in_memory().unwrap(),
        handle.clone(),
        test_config(),
        api_key_cache("control-key", "control"),
    );

    let (status, payload) = post_nzbget(
        app.clone(),
        serde_json::json!({
            "method": "editqueue",
            "params": ["GroupFinalDelete", 0, "", 77],
            "id": "delete"
        }),
        "Bearer control-key",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["result"], true);
    assert!(handle.list_jobs().is_empty());

    let (status, payload) = post_nzbget(
        app.clone(),
        serde_json::json!({
            "method": "editqueue",
            "params": ["HistoryDelete", 0, "", 77],
            "id": "history-delete"
        }),
        "Bearer control-key",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["result"], true);

    let (status, payload) = post_nzbget(
        app.clone(),
        serde_json::json!({
            "method": "editqueue",
            "params": ["HistoryRedownload", 0, "", 77],
            "id": "history-redownload"
        }),
        "Bearer control-key",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["result"], true);

    let (status, payload) = post_nzbget(
        app,
        serde_json::json!({
            "method": "editqueue",
            "params": ["UnsupportedAction", 0, "", 77],
            "id": "unsupported"
        }),
        "Bearer control-key",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["error"]["code"], 3);
}

#[tokio::test]
async fn nzbget_editqueue_supports_v13_three_param_shape_with_id_array() {
    let job = nzbget_test_job(
        88,
        JobStatus::Queued,
        weaver_server_core::DownloadState::Queued,
        100,
        0,
        vec![],
    );
    let handle = scheduler_handle_with_mock_commands(vec![job]);
    let app = nzbget_test_router(
        Database::open_in_memory().unwrap(),
        handle.clone(),
        test_config(),
        api_key_cache("control-key", "control"),
    );

    let (status, payload) = post_nzbget(
        app.clone(),
        serde_json::json!({
            "method": "editqueue",
            "params": ["GroupPause", "", [88]],
            "id": "pause-v13"
        }),
        "Bearer control-key",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["result"], true);
    assert_eq!(handle.list_jobs()[0].status, JobStatus::Paused);

    let (status, payload) = post_nzbget(
        app,
        serde_json::json!({
            "method": "editqueue",
            "params": ["GroupResume", "", [88]],
            "id": "resume-v13"
        }),
        "Bearer control-key",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["result"], true);
    assert_eq!(handle.list_jobs()[0].status, JobStatus::Queued);
}

#[tokio::test]
async fn nzbget_editqueue_category_priority_and_parameter_updates() {
    let job = nzbget_test_job(
        90,
        JobStatus::Queued,
        weaver_server_core::DownloadState::Queued,
        100,
        0,
        vec![],
    );
    let handle = scheduler_handle_with_mock_commands(vec![job]);
    let app = nzbget_test_router(
        Database::open_in_memory().unwrap(),
        handle.clone(),
        test_config(),
        api_key_cache("control-key", "control"),
    );

    // nzb360 sends GroupApplyCategory (not GroupSetCategory) in legacy shape.
    let (status, payload) = post_nzbget(
        app.clone(),
        serde_json::json!({
            "method": "editqueue",
            "params": ["GroupApplyCategory", 0, "movies", [90]],
            "id": "category"
        }),
        "Bearer control-key",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["result"], true);
    assert_eq!(handle.list_jobs()[0].category.as_deref(), Some("movies"));

    let (status, payload) = post_nzbget(
        app.clone(),
        serde_json::json!({
            "method": "editqueue",
            "params": ["GroupSetCategory", 0, "../../outside", [90]],
            "id": "unsafe-category"
        }),
        "Bearer control-key",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["error"]["code"], 2);
    assert_eq!(handle.list_jobs()[0].category.as_deref(), Some("movies"));

    let (status, payload) = post_nzbget(
        app.clone(),
        serde_json::json!({
            "method": "editqueue",
            "params": ["GroupSetPriority", 0, "900", [90]],
            "id": "priority"
        }),
        "Bearer control-key",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["result"], true);

    let (status, groups) = post_nzbget(
        app.clone(),
        serde_json::json!({
            "method": "listgroups",
            "params": [],
            "id": "groups"
        }),
        "Bearer control-key",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let group = &groups["result"][0];
    assert_eq!(group["Category"], "movies");
    assert_eq!(group["MaxPriority"], 50);

    // Generic parameters round-trip into the Parameters list.
    let (status, payload) = post_nzbget(
        app.clone(),
        serde_json::json!({
            "method": "editqueue",
            "params": ["GroupSetParameter", 0, "MyTag=abc", [90]],
            "id": "parameter"
        }),
        "Bearer control-key",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["result"], true);
    let metadata = &handle.list_jobs()[0].metadata;
    assert!(
        metadata
            .iter()
            .any(|(key, value)| key == "MyTag" && value == "abc")
    );

    // Unpack passwords apply as weaver's durable password override and never
    // leak into the visible metadata/Parameters listings.
    let (status, payload) = post_nzbget(
        app.clone(),
        serde_json::json!({
            "method": "editqueue",
            "params": ["GroupSetParameter", 0, "*Unpack:Password=hunter2", [90]],
            "id": "password"
        }),
        "Bearer control-key",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["result"], true);
    assert_eq!(handle.list_jobs()[0].password.as_deref(), Some("hunter2"));
    assert!(
        handle.list_jobs()[0]
            .metadata
            .iter()
            .all(|(_, value)| value != "hunter2")
    );

    // An empty value clears the password again.
    let (_, payload) = post_nzbget(
        app,
        serde_json::json!({
            "method": "editqueue",
            "params": ["GroupSetParameter", 0, "*Unpack:Password=", [90]],
            "id": "password-clear"
        }),
        "Bearer control-key",
    )
    .await;
    assert_eq!(payload["result"], true);
    assert_eq!(handle.list_jobs()[0].password, None);
}

#[tokio::test]
async fn nzbget_editqueue_unsupported_commands_return_false_without_fault() {
    let job = nzbget_test_job(
        91,
        JobStatus::Queued,
        weaver_server_core::DownloadState::Queued,
        100,
        0,
        vec![],
    );
    let handle = scheduler_handle_with_mock_commands(vec![job]);
    let app = nzbget_test_router(
        Database::open_in_memory().unwrap(),
        handle,
        test_config(),
        api_key_cache("control-key", "control"),
    );

    for command in ["GroupMoveBefore", "GroupSetName", "FilePause", "GroupSort"] {
        let (status, payload) = post_nzbget(
            app.clone(),
            serde_json::json!({
                "method": "editqueue",
                "params": [command, 0, "", [91]],
                "id": command
            }),
            "Bearer control-key",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{command}");
        assert_eq!(payload["result"], false, "{command}");
        assert!(payload["error"].is_null(), "{command}");
    }
}

#[tokio::test]
async fn nzbget_editqueue_rejects_more_than_max_ids() {
    let app = nzbget_test_router(
        Database::open_in_memory().unwrap(),
        test_scheduler_handle(),
        test_config(),
        api_key_cache("control-key", "control"),
    );

    // One id over the 10_000-id cap; defuses a would-be orchestrator-loop
    // monopolization from a single oversized call (e.g. 100k ids).
    let ids: Vec<u64> = (1..=10_001).collect();
    let (status, payload) = post_nzbget(
        app,
        serde_json::json!({
            "method": "editqueue",
            "params": ["GroupPause", 0, "", ids],
            "id": "too-many-ids"
        }),
        "Bearer control-key",
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["error"]["code"], 2);
}

#[tokio::test]
async fn nzbget_status_classifies_downloading_while_extracting_as_active() {
    // A job the pipeline projects as still downloading during post-processing
    // (status = Extracting but download_state = Downloading — the incremental
    // extraction case) must be reported as actively downloading, NOT as a
    // standby post-processing job. `status()` must classify via the projected
    // runtime lanes (queue_item_state_from_job_info), not the coarse status.
    let job = nzbget_test_job(
        7,
        JobStatus::Extracting,
        weaver_server_core::DownloadState::Downloading,
        1_000,
        400,
        vec![],
    );
    let app = nzbget_test_router(
        Database::open_in_memory().unwrap(),
        scheduler_handle_with_mock_commands(vec![job]),
        test_config(),
        api_key_cache("control-key", "control"),
    );

    let (status, payload) = post_nzbget(
        app,
        serde_json::json!({"method": "status", "params": [], "id": "standby"}),
        "Bearer control-key",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    // Actively downloading -> not standby, and not counted as a post-proc job.
    assert_eq!(payload["result"]["ServerStandBy"], false);
    assert_eq!(payload["result"]["PostJobCount"], 0);
    assert_eq!(payload["result"]["ParJobCount"], 0);
}

#[tokio::test]
async fn nzbget_global_pause_resume_and_scheduleresume_auto_resume() {
    let handle = scheduler_handle_with_mock_commands(vec![]);
    let app = nzbget_test_router(
        Database::open_in_memory().unwrap(),
        handle.clone(),
        test_config(),
        api_key_cache("control-key", "control"),
    );

    let (status, payload) = post_nzbget(
        app.clone(),
        serde_json::json!({"method": "pausedownload", "params": [], "id": 1}),
        "Bearer control-key",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["result"], true);
    assert!(handle.is_globally_paused());

    let (_, payload) = post_nzbget(
        app.clone(),
        serde_json::json!({"method": "scheduleresume", "params": [1], "id": 2}),
        "Bearer control-key",
    )
    .await;
    assert_eq!(payload["result"], true);

    let (_, payload) = post_nzbget(
        app.clone(),
        serde_json::json!({"method": "status", "params": [], "id": 3}),
        "Bearer control-key",
    )
    .await;
    assert!(payload["result"]["ResumeTime"].as_u64().unwrap() > 0);

    tokio::time::sleep(std::time::Duration::from_millis(1600)).await;
    assert!(
        !handle.is_globally_paused(),
        "scheduleresume timer should resume downloads"
    );
    let (_, payload) = post_nzbget(
        app.clone(),
        serde_json::json!({"method": "status", "params": [], "id": 4}),
        "Bearer control-key",
    )
    .await;
    assert_eq!(payload["result"]["ResumeTime"], 0);

    // A manual pause after arming a timer cancels the pending resume.
    let (_, payload) = post_nzbget(
        app.clone(),
        serde_json::json!({"method": "scheduleresume", "params": [1], "id": 5}),
        "Bearer control-key",
    )
    .await;
    assert_eq!(payload["result"], true);
    let (_, payload) = post_nzbget(
        app.clone(),
        serde_json::json!({"method": "pausedownload", "params": [], "id": 6}),
        "Bearer control-key",
    )
    .await;
    assert_eq!(payload["result"], true);
    tokio::time::sleep(std::time::Duration::from_millis(1600)).await;
    assert!(
        handle.is_globally_paused(),
        "manual pause must cancel a pending scheduled resume"
    );

    let (_, payload) = post_nzbget(
        app,
        serde_json::json!({"method": "resumedownload", "params": [], "id": 7}),
        "Bearer control-key",
    )
    .await;
    assert_eq!(payload["result"], true);
    assert!(!handle.is_globally_paused());
}

#[tokio::test]
async fn nzbget_rate_persists_limit_and_pausescan_toggles_watch_folder() {
    let db = Database::open_in_memory().unwrap();
    let config = test_config();
    let handle = scheduler_handle_with_mock_commands(vec![]);
    let app = nzbget_test_router(
        db.clone(),
        handle,
        config.clone(),
        api_key_cache("control-key", "control"),
    );

    let (status, payload) = post_nzbget(
        app.clone(),
        serde_json::json!({"method": "rate", "params": [2500], "id": "rate"}),
        "Bearer control-key",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["result"], true);
    assert_eq!(
        config.read().await.max_download_speed,
        Some(2500 * 1024),
        "rate should update the shared config in KB/s -> bytes/s"
    );
    assert_eq!(
        db.get_setting("max_download_speed").unwrap().as_deref(),
        Some("2560000")
    );

    let (_, payload) = post_nzbget(
        app.clone(),
        serde_json::json!({"method": "pausescan", "params": [], "id": "pausescan"}),
        "Bearer control-key",
    )
    .await;
    assert_eq!(payload["result"], true);
    assert!(config.read().await.watch_folder.scanning_paused);
    assert_eq!(
        db.get_setting("watch_folder.scanning_paused")
            .unwrap()
            .as_deref(),
        Some("true")
    );

    let (_, status_payload) = post_nzbget(
        app.clone(),
        serde_json::json!({"method": "status", "params": [], "id": "status"}),
        "Bearer control-key",
    )
    .await;
    assert_eq!(status_payload["result"]["ScanPaused"], true);

    let (_, payload) = post_nzbget(
        app,
        serde_json::json!({"method": "resumescan", "params": [], "id": "resumescan"}),
        "Bearer control-key",
    )
    .await;
    assert_eq!(payload["result"], true);
    assert!(!config.read().await.watch_folder.scanning_paused);
}

#[tokio::test]
async fn nzbget_history_reports_compound_status_history_time_and_stage_timings() {
    let db = Database::open_in_memory().unwrap();
    db.insert_job_history(&nzbget_history_row(400, "complete", 1_700_000_400, None))
        .unwrap();
    db.insert_job_history(&nzbget_history_row(401, "failed", 1_700_000_500, None))
        .unwrap();
    db.insert_job_history(&nzbget_history_row(402, "cancelled", 1_700_000_600, None))
        .unwrap();
    // Stage boundaries in epoch milliseconds: 300s download, 40s repair.
    let stage_event =
        |kind: &str, timestamp: i64| weaver_server_core::history::timeline::JobEvent {
            job_id: 400,
            timestamp,
            kind: kind.into(),
            message: String::new(),
            file_id: None,
        };
    db.insert_job_events(&[
        stage_event("DownloadStarted", 1_700_000_000_000),
        stage_event("DownloadFinished", 1_700_000_300_000),
        stage_event("RepairStarted", 1_700_000_310_000),
        stage_event("RepairComplete", 1_700_000_350_000),
    ])
    .unwrap();
    let app = nzbget_test_router(
        db,
        test_scheduler_handle(),
        test_config(),
        ApiKeyCache::default(),
    );

    let (status, payload) = post_nzbget(
        app,
        serde_json::json!({"method": "history", "params": [false], "id": "history"}),
        "Bearer session-token",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let items = payload["result"].as_array().unwrap();
    let by_id = |id: u64| items.iter().find(|item| item["ID"] == id).unwrap();
    assert_eq!(by_id(400)["Status"], "SUCCESS/ALL");
    assert_eq!(by_id(400)["HistoryTime"], 1_700_000_400);
    assert_eq!(by_id(400)["DownloadTimeSec"], 300);
    assert_eq!(by_id(400)["RepairTimeSec"], 40);
    assert_eq!(by_id(400)["PostTotalTimeSec"], 40);
    assert_eq!(by_id(401)["Status"], "FAILURE/HEALTH");
    assert_eq!(by_id(401)["DownloadTimeSec"], 0);
    assert_eq!(by_id(402)["Status"], "DELETED/MANUAL");
    assert_eq!(by_id(402)["Deleted"], true);
}

#[tokio::test]
async fn nzbget_listfiles_reports_nzb_files_with_progress() {
    let db = Database::open_in_memory().unwrap();
    let job = nzbget_test_job(
        55,
        JobStatus::Downloading,
        weaver_server_core::DownloadState::Downloading,
        700_000,
        150_000,
        vec![],
    );
    db.create_active_job(&weaver_server_core::ActiveJob {
        job_id: JobId(55),
        nzb_hash: [7u8; 32],
        nzb_path: "/tmp/weaver/nzb/55.nzb".into(),
        nzb_zstd: two_file_nzb().into_bytes(),
        output_dir: "/tmp/weaver/intermediate/55".into(),
        created_at: 1_700_000_000,
        category: Some("tv".into()),
        metadata: vec![],
        status: "downloading",
        download_state: "downloading",
        post_state: "idle",
        run_state: "active",
        paused_resume_status: None,
        paused_resume_download_state: None,
        paused_resume_post_state: None,
    })
    .unwrap();
    db.upsert_file_progress_batch(&[weaver_server_core::ActiveFileProgress {
        job_id: JobId(55),
        file_index: 0,
        contiguous_bytes_written: 150_000,
    }])
    .unwrap();
    let handle = scheduler_handle_with_mock_commands(vec![job]);
    let app = nzbget_test_router(db, handle, test_config(), ApiKeyCache::default());

    let (status, payload) = post_nzbget(
        app.clone(),
        serde_json::json!({"method": "listfiles", "params": [0, 0, 55], "id": "files"}),
        "Bearer session-token",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let files = payload["result"].as_array().unwrap();
    assert_eq!(files.len(), 2);
    let alpha = files
        .iter()
        .find(|file| file["Filename"] == "alpha.rar")
        .unwrap();
    assert_eq!(alpha["NZBID"], 55);
    assert_eq!(alpha["FileSizeLo"], 600_000);
    assert_eq!(alpha["RemainingSizeLo"], 450_000);
    let beta = files
        .iter()
        .find(|file| file["Filename"] == "beta.par2")
        .unwrap();
    assert_eq!(beta["FileSizeLo"], 100_000);
    assert_eq!(beta["RemainingSizeLo"], 100_000);

    let (status, payload) = post_nzbget(
        app,
        serde_json::json!({"method": "listfiles", "params": [0, 0, 9999], "id": "missing"}),
        "Bearer session-token",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["error"]["code"], 2);
}

#[tokio::test]
async fn nzbget_postqueue_and_group_post_fields_report_stage_progress() {
    let mut job = nzbget_test_job(
        60,
        JobStatus::Repairing,
        weaver_server_core::DownloadState::Complete,
        1_000_000,
        1_000_000,
        vec![],
    );
    job.post_state = weaver_server_core::PostState::Repairing;
    job.phase_progress = vec![weaver_server_core::JobPhaseProgress {
        phase: weaver_server_core::JobPhase::Repairing,
        completed_bytes: 500_000,
        total_bytes: 1_000_000,
        progress_percent: 50.0,
        rate_bps: Some(1_000_000),
        estimated_remaining_ms: Some(500),
        started_at_epoch_ms: 1_700_000_000_000.0,
        updated_at_epoch_ms: 1_700_000_004_000.0,
    }];
    let mut queued = nzbget_test_job(
        61,
        JobStatus::QueuedPostProcessing,
        weaver_server_core::DownloadState::Complete,
        1_000_000,
        1_000_000,
        vec![],
    );
    queued.post_state = weaver_server_core::PostState::QueuedPostProcessing;
    let handle = scheduler_handle_with_mock_commands(vec![job, queued]);
    let app = nzbget_test_router(
        Database::open_in_memory().unwrap(),
        handle,
        test_config(),
        ApiKeyCache::default(),
    );

    let (status, payload) = post_nzbget(
        app.clone(),
        serde_json::json!({"method": "postqueue", "params": [0], "id": "postqueue"}),
        "Bearer session-token",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let entry = &payload["result"][0];
    assert_eq!(entry["NZBID"], 60);
    assert_eq!(entry["Stage"], "REPAIRING");
    assert_eq!(entry["StageProgress"], 500);
    assert_eq!(entry["StageTimeSec"], 4);
    assert_eq!(payload["result"][1]["NZBID"], 61);
    assert_eq!(payload["result"][1]["Stage"], "QUEUED");

    let (_, status_payload) = post_nzbget(
        app.clone(),
        serde_json::json!({"method": "status", "params": [], "id": "status"}),
        "Bearer session-token",
    )
    .await;
    assert_eq!(status_payload["result"]["PostJobCount"], 2);

    let (_, groups) = post_nzbget(
        app,
        serde_json::json!({"method": "listgroups", "params": [], "id": "groups"}),
        "Bearer session-token",
    )
    .await;
    let group = &groups["result"][0];
    assert_eq!(group["Status"], "REPAIRING");
    assert_eq!(group["PostStageProgress"], 500);
    assert_eq!(group["PostInfoText"], "Repairing (50%)");
}

#[tokio::test]
async fn nzbget_log_and_loadlog_expose_job_events() {
    let db = Database::open_in_memory().unwrap();
    db.insert_job_events(&[
        weaver_server_core::history::timeline::JobEvent {
            job_id: 42,
            timestamp: 1_700_000_100,
            kind: "download-started".into(),
            message: "download started".into(),
            file_id: None,
        },
        weaver_server_core::history::timeline::JobEvent {
            job_id: 42,
            timestamp: 1_700_000_200,
            kind: "repair-failed".into(),
            message: "repair failed hard".into(),
            file_id: None,
        },
    ])
    .unwrap();
    let job = nzbget_test_job(
        42,
        JobStatus::Downloading,
        weaver_server_core::DownloadState::Downloading,
        100,
        10,
        vec![],
    );
    let handle = scheduler_handle_with_mock_commands(vec![job]);
    let app = nzbget_test_router(db, handle, test_config(), ApiKeyCache::default());

    let (status, payload) = post_nzbget(
        app.clone(),
        serde_json::json!({"method": "loadlog", "params": [42, 0, 100], "id": "loadlog"}),
        "Bearer session-token",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let entries = payload["result"].as_array().unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0]["Kind"], "INFO");
    assert_eq!(entries[1]["Kind"], "ERROR");
    assert_eq!(entries[1]["Text"], "repair failed hard");
    assert_ne!(entries[0]["ID"], entries[1]["ID"]);
    assert_eq!(entries[0]["Time"], entries[1]["Time"]);
    let second_id = entries[1]["ID"].as_u64().unwrap();

    let (_, payload) = post_nzbget(
        app.clone(),
        serde_json::json!({
            "method": "loadlog",
            "params": [42, second_id, 100],
            "id": "loadlog-page"
        }),
        "Bearer session-token",
    )
    .await;
    let page = payload["result"].as_array().unwrap();
    assert_eq!(page.len(), 1);
    assert_eq!(page[0]["ID"], second_id);

    let (status, payload) = post_nzbget(
        app,
        serde_json::json!({"method": "log", "params": [0, 20], "id": "log"}),
        "Bearer session-token",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let entries = payload["result"].as_array().unwrap();
    assert_eq!(entries.len(), 2);
    assert!(
        entries[0]["Text"]
            .as_str()
            .unwrap()
            .starts_with("[Silver.Horizon.S05")
    );
}

#[tokio::test]
async fn nzbget_appendurl_validates_url_and_shape() {
    let app = nzbget_test_router(
        Database::open_in_memory().unwrap(),
        scheduler_handle_with_mock_commands(vec![]),
        test_config(),
        api_key_cache("control-key", "control"),
    );

    // Legacy nzb360 shape with a non-URL in the URL slot is rejected.
    let (status, payload) = post_nzbget(
        app.clone(),
        serde_json::json!({
            "method": "appendurl",
            "params": ["release.nzb", "tv", 0, false, "not-a-url"],
            "id": "bad-url"
        }),
        "Bearer control-key",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["error"]["code"], 2);

    // Private addresses are refused by the fetch guard rather than fetched.
    let (status, payload) = post_nzbget(
        app,
        serde_json::json!({
            "method": "appendurl",
            "params": ["release.nzb", "tv", 0, false, "http://127.0.0.1:9/x.nzb"],
            "id": "private-url"
        }),
        "Bearer control-key",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["error"]["code"], 2);
}

const XMLRPC_VERSION_CALL: &str = r#"<?xml version="1.0"?>
<methodCall><methodName>version</methodName><params/></methodCall>"#;

#[tokio::test]
async fn nzbget_xmlrpc_version_and_unknown_method_roundtrip() {
    let app = nzbget_test_router(
        Database::open_in_memory().unwrap(),
        test_scheduler_handle(),
        test_config(),
        ApiKeyCache::default(),
    );

    let (status, body) =
        post_nzbget_xmlrpc(app.clone(), XMLRPC_VERSION_CALL, "Bearer session-token").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body.contains("<methodResponse><params><param><value><string>16.0-weaver</string></value></param></params></methodResponse>"),
        "unexpected body: {body}"
    );

    let call = r#"<methodCall><methodName>bogusmethod</methodName></methodCall>"#;
    let (status, body) = post_nzbget_xmlrpc(app, call, "Bearer session-token").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("<fault>"), "unexpected body: {body}");
    assert!(body.contains("faultCode"), "unexpected body: {body}");
}

#[tokio::test]
async fn nzbget_xmlrpc_rejects_invalid_credentials() {
    let app = nzbget_test_router(
        Database::open_in_memory().unwrap(),
        test_scheduler_handle(),
        test_config(),
        ApiKeyCache::default(),
    );

    let (status, body) =
        post_nzbget_xmlrpc(app, XMLRPC_VERSION_CALL, &basic_auth("wrong-token")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert!(body.contains("<fault>"), "unexpected body: {body}");
}

#[tokio::test]
async fn nzbget_xmlrpc_editqueue_nzb360_shape_pauses_group() {
    let job = nzbget_test_job(
        77,
        JobStatus::Queued,
        weaver_server_core::DownloadState::Queued,
        100,
        0,
        vec![],
    );
    let handle = scheduler_handle_with_mock_commands(vec![job]);
    let app = nzbget_test_router(
        Database::open_in_memory().unwrap(),
        handle.clone(),
        test_config(),
        api_key_cache("control-key", "control"),
    );

    // Exact nzb360 wire shape: 4-arg legacy editqueue with int offset and an
    // <array> of ids.
    let call = r#"<?xml version="1.0"?>
<methodCall>
  <methodName>editqueue</methodName>
  <params>
    <param><value><string>GroupPause</string></value></param>
    <param><value><i4>0</i4></value></param>
    <param><value><string></string></value></param>
    <param><value><array><data><value><i4>77</i4></value></data></array></value></param>
  </params>
</methodCall>"#;
    let (status, body) = post_nzbget_xmlrpc(app.clone(), call, "Bearer control-key").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body.contains("<value><boolean>1</boolean></value>"),
        "unexpected body: {body}"
    );
    assert_eq!(handle.list_jobs()[0].status, JobStatus::Paused);

    // Read scope may not drive control methods over XML-RPC either.
    let read_app = nzbget_test_router(
        Database::open_in_memory().unwrap(),
        scheduler_handle_with_mock_commands(vec![]),
        test_config(),
        api_key_cache("read-key", "read"),
    );
    let call = call.replace("GroupPause", "GroupResume");
    let (status, body) = post_nzbget_xmlrpc(read_app, &call, "Bearer read-key").await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(body.contains("<fault>"), "unexpected body: {body}");
}

#[tokio::test]
async fn nzbget_xmlrpc_append_returns_job_id() {
    use base64::Engine as _;

    let db = Database::open_in_memory().unwrap();
    let handle = scheduler_handle_with_mock_commands(vec![]);
    let app = nzbget_test_router(
        db,
        handle.clone(),
        test_config(),
        api_key_cache("control-key", "control"),
    );

    let content = base64::engine::general_purpose::STANDARD.encode(minimal_nzb("XmlAdd"));
    let call = format!(
        r#"<?xml version="1.0"?>
<methodCall>
  <methodName>append</methodName>
  <params>
    <param><value><string>XmlAdd.nzb</string></value></param>
    <param><value><string>{content}</string></value></param>
    <param><value><string>tv</string></value></param>
    <param><value><i4>0</i4></value></param>
    <param><value><boolean>0</boolean></value></param>
    <param><value><boolean>0</boolean></value></param>
    <param><value><string></string></value></param>
    <param><value><i4>0</i4></value></param>
    <param><value><string>Score</string></value></param>
  </params>
</methodCall>"#
    );
    let (status, body) = post_nzbget_xmlrpc(app.clone(), &call, "Bearer control-key").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("<i4>"), "unexpected body: {body}");
    let jobs = handle.list_jobs();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].category.as_deref(), Some("tv"));

    // listgroups over XML-RPC returns a struct array for the added job.
    let call = r#"<methodCall><methodName>listgroups</methodName></methodCall>"#;
    let (status, body) = post_nzbget_xmlrpc(app, call, "Bearer control-key").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body.contains("<member><name>NZBID</name>"),
        "unexpected body: {body}"
    );
    assert!(
        body.contains("<name>Status</name><value><string>QUEUED</string></value>"),
        "unexpected body: {body}"
    );
}

#[tokio::test]
async fn nzbget_loadconfig_alias_exposes_categories() {
    let app = nzbget_test_router(
        Database::open_in_memory().unwrap(),
        test_scheduler_handle(),
        test_config(),
        ApiKeyCache::default(),
    );

    let (status, payload) = post_nzbget(
        app,
        serde_json::json!({"method": "loadconfig", "params": [], "id": "loadconfig"}),
        "Bearer session-token",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let entries = payload["result"].as_array().unwrap();
    assert!(
        entries
            .iter()
            .any(|entry| entry["Name"] == "Category1.Name")
    );
}

#[tokio::test]
async fn nzbget_editqueue_move_commands_reorder_queue() {
    let handle = scheduler_handle_with_mock_commands(vec![
        reorder_test_job(1),
        reorder_test_job(2),
        reorder_test_job(3),
    ]);
    let app = nzbget_test_router(
        Database::open_in_memory().unwrap(),
        handle,
        test_config(),
        api_key_cache("control-key", "control"),
    );

    // nzb360 legacy shape: MoveTop with the id array in position 3.
    let (status, payload) = post_nzbget(
        app.clone(),
        serde_json::json!({
            "method": "editqueue",
            "params": ["GroupMoveTop", 0, "", [3]],
            "id": "top"
        }),
        "Bearer control-key",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["result"], true);
    assert_eq!(listgroups_ids(app.clone()).await, vec![3, 1, 2]);

    // nzb360 MoveOffset: delta rides in the legacy Offset argument.
    let (_, payload) = post_nzbget(
        app.clone(),
        serde_json::json!({
            "method": "editqueue",
            "params": ["GroupMoveOffset", 2, "", [3]],
            "id": "offset"
        }),
        "Bearer control-key",
    )
    .await;
    assert_eq!(payload["result"], true);
    assert_eq!(listgroups_ids(app.clone()).await, vec![1, 2, 3]);

    // v13+ shape: delta as the Param string, ids as an array.
    let (_, payload) = post_nzbget(
        app.clone(),
        serde_json::json!({
            "method": "editqueue",
            "params": ["GroupMoveOffset", "-1", [2]],
            "id": "offset-v13"
        }),
        "Bearer control-key",
    )
    .await;
    assert_eq!(payload["result"], true);
    assert_eq!(listgroups_ids(app.clone()).await, vec![2, 1, 3]);

    let (_, payload) = post_nzbget(
        app.clone(),
        serde_json::json!({
            "method": "editqueue",
            "params": ["GroupMoveBottom", 0, "", [2]],
            "id": "bottom"
        }),
        "Bearer control-key",
    )
    .await;
    assert_eq!(payload["result"], true);
    assert_eq!(listgroups_ids(app.clone()).await, vec![1, 3, 2]);

    // Unknown ids answer false, matching the other editqueue commands.
    let (_, payload) = post_nzbget(
        app,
        serde_json::json!({
            "method": "editqueue",
            "params": ["GroupMoveTop", 0, "", [99]],
            "id": "missing"
        }),
        "Bearer control-key",
    )
    .await;
    assert_eq!(payload["result"], false);
}

#[tokio::test]
async fn nzbget_servervolumes_report_quota_window_usage() {
    use chrono::TimeZone as _;

    let db = Database::open_in_memory().unwrap();
    db.insert_server(&weaver_server_core::servers::ServerConfig {
        id: 1,
        host: "news.example.com".into(),
        port: 563,
        tls: true,
        username: None,
        password: None,
        connections: 8,
        active: true,
        supports_pipelining: false,
        pipelining_depth: None,
        priority: 0,
        backfill: false,
        retention_days: 0,
        max_download_speed: 0,
        download_quota: Default::default(),
        tls_ca_cert: None,
        tls_name_mismatch_certificate_der: None,
    })
    .unwrap();
    db.upsert_server_download_usage(&weaver_server_core::servers::ServerDownloadUsage {
        server_id: 1,
        lifetime_bytes: 5 * 1024 * 1024,
        quota_baseline_bytes: 2 * 1024 * 1024,
        window_start: Some(chrono::Utc.timestamp_opt(1_700_000_000, 0).unwrap()),
        window_end: None,
        updated_at: chrono::Utc.timestamp_opt(1_700_000_500, 0).unwrap(),
    })
    .unwrap();
    let app = nzbget_test_router(
        db,
        test_scheduler_handle(),
        test_config(),
        ApiKeyCache::default(),
    );

    let (status, payload) = post_nzbget(
        app,
        serde_json::json!({"method": "servervolumes", "params": [], "id": "volumes"}),
        "Bearer session-token",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let entries = payload["result"].as_array().unwrap();
    assert_eq!(entries.len(), 2, "aggregate entry plus one server");
    assert_eq!(entries[0]["ServerID"], 0);
    assert_eq!(entries[0]["TotalSizeMB"], 5);
    assert_eq!(entries[1]["ServerID"], 1);
    assert_eq!(entries[1]["TotalSizeMB"], 5);
    assert_eq!(entries[1]["CustomSizeMB"], 3);
    assert_eq!(entries[1]["CustomTime"], 1_700_000_000);
    // Weaver tracks no rolling series, but NZBGet's wire contract is
    // fixed-length windows (60 sec / 60 min / 24 hr), zero-filled — a strict
    // client may index a fixed offset, so we keep the lengths.
    assert_eq!(entries[1]["BytesPerSeconds"].as_array().unwrap().len(), 60);
    assert_eq!(entries[1]["BytesPerMinutes"].as_array().unwrap().len(), 60);
    assert_eq!(entries[1]["BytesPerHours"].as_array().unwrap().len(), 24);
}

#[tokio::test]
async fn nzbget_scheduleresume_persists_and_recovers_across_restart() {
    let db = Database::open_in_memory().unwrap();
    let handle = scheduler_handle_with_mock_commands(vec![]);
    let app = nzbget_test_router(
        db.clone(),
        handle.clone(),
        test_config(),
        api_key_cache("control-key", "control"),
    );

    let (_, payload) = post_nzbget(
        app.clone(),
        serde_json::json!({"method": "pausedownload", "params": [], "id": 1}),
        "Bearer control-key",
    )
    .await;
    assert_eq!(payload["result"], true);
    let (_, payload) = post_nzbget(
        app.clone(),
        serde_json::json!({"method": "scheduleresume", "params": [3600], "id": 2}),
        "Bearer control-key",
    )
    .await;
    assert_eq!(payload["result"], true);
    let stored = db
        .get_setting("nzbget.scheduled_resume_at")
        .unwrap()
        .expect("scheduleresume must persist its deadline");
    assert!(stored.parse::<u64>().unwrap() > 0);

    // A manual resume clears the persisted deadline.
    let (_, payload) = post_nzbget(
        app.clone(),
        serde_json::json!({"method": "resumedownload", "params": [], "id": 3}),
        "Bearer control-key",
    )
    .await;
    assert_eq!(payload["result"], true);
    assert_eq!(db.get_setting("nzbget.scheduled_resume_at").unwrap(), None);

    // Simulate a restart with an elapsed deadline: the recovery task resumes
    // downloads and clears the setting.
    db.set_setting("nzbget.scheduled_resume_at", "1000")
        .unwrap();
    handle.pause_all().await.unwrap();
    assert!(handle.is_globally_paused());
    let _restarted = nzbget_test_router(
        db.clone(),
        handle.clone(),
        test_config(),
        api_key_cache("control-key", "control"),
    );
    for _ in 0..50 {
        if !handle.is_globally_paused() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(
        !handle.is_globally_paused(),
        "elapsed scheduled resume must resume downloads at startup"
    );
    for _ in 0..50 {
        if db
            .get_setting("nzbget.scheduled_resume_at")
            .unwrap()
            .is_none()
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert_eq!(db.get_setting("nzbget.scheduled_resume_at").unwrap(), None);
}

#[tokio::test]
async fn nzbget_feed_bridge_exposes_weaver_rss() {
    let db = Database::open_in_memory().unwrap();
    db.insert_rss_feed(&weaver_server_core::RssFeedRow {
        id: 1,
        name: "indexer".into(),
        url: "https://indexer.example/rss".into(),
        enabled: true,
        poll_interval_secs: 900,
        username: None,
        password: None,
        default_category: Some("tv".into()),
        default_metadata: vec![],
        etag: None,
        last_modified: None,
        last_polled_at: None,
        last_success_at: None,
        last_error: None,
        consecutive_failures: 0,
    })
    .unwrap();
    db.insert_rss_seen_item(&weaver_server_core::RssSeenItemRow {
        feed_id: 1,
        item_id: "item-1".into(),
        item_title: "Show.S01E01.720p".into(),
        published_at: Some(1_700_000_000),
        size_bytes: Some(750 * 1024 * 1024),
        decision: "submitted".into(),
        seen_at: 1_700_000_100,
        job_id: Some(10),
        item_url: Some("https://indexer.example/get/1".into()),
        error: None,
    })
    .unwrap();
    db.insert_rss_seen_item(&weaver_server_core::RssSeenItemRow {
        feed_id: 1,
        item_id: "item-2".into(),
        item_title: "Show.S01E02.720p".into(),
        published_at: Some(1_700_000_200),
        size_bytes: None,
        decision: "ignored".into(),
        seen_at: 1_700_000_300,
        job_id: None,
        item_url: None,
        error: None,
    })
    .unwrap();
    let app = nzbget_test_router(
        db,
        test_scheduler_handle(),
        test_config(),
        ApiKeyCache::default(),
    );

    // Feeds surface as FeedN config entries.
    let (status, payload) = post_nzbget(
        app.clone(),
        serde_json::json!({"method": "loadconfig", "params": [], "id": "feeds-config"}),
        "Bearer session-token",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let entries = payload["result"].as_array().unwrap();
    let by_name = |name: &str| {
        entries
            .iter()
            .find(|entry| entry["Name"] == name)
            .unwrap_or_else(|| panic!("missing config entry {name}"))["Value"]
            .clone()
    };
    assert_eq!(by_name("Feed1.Name"), "indexer");
    // The feed URL is intentionally NOT exposed: it embeds the indexer API key and
    // config/loadconfig are reachable with a read-scoped key. Only Name + Interval.
    assert!(
        entries.iter().all(|entry| entry["Name"] != "Feed1.URL"),
        "feed URL must not be exposed via config"
    );
    assert_eq!(by_name("Feed1.Interval"), "15");

    let (status, payload) = post_nzbget(
        app,
        serde_json::json!({"method": "viewfeed", "params": [1], "id": "viewfeed"}),
        "Bearer session-token",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let items = payload["result"].as_array().unwrap();
    assert_eq!(items.len(), 2);
    let grabbed = items
        .iter()
        .find(|item| item["Title"] == "Show.S01E01.720p")
        .unwrap();
    assert_eq!(grabbed["Status"], "FETCHED");
    assert_eq!(grabbed["MatchStatus"], "ACCEPTED");
    assert_eq!(grabbed["SizeMB"], 750);
    let skipped = items
        .iter()
        .find(|item| item["Title"] == "Show.S01E02.720p")
        .unwrap();
    assert_eq!(skipped["Status"], "BACKLOG");
    assert_eq!(skipped["MatchStatus"], "IGNORED");
}

#[tokio::test]
async fn nzbget_rpc_routes_accept_bodies_beyond_default_axum_limit() {
    use base64::Engine as _;

    let app = nzbget_test_router(
        Database::open_in_memory().unwrap(),
        scheduler_handle_with_mock_commands(vec![]),
        test_config(),
        api_key_cache("control-key", "control"),
    );

    // 3 MiB of base64 payload: over axum's 2 MiB default, far under the NZB
    // upload limit. Not a valid NZB, so append answers 0 — reaching the RPC
    // layer at all is what this guards (a missing limit override yields 413).
    let content = base64::engine::general_purpose::STANDARD.encode(vec![b'x'; 3 * 1024 * 1024]);
    let (status, payload) = post_nzbget(
        app,
        serde_json::json!({
            "method": "append",
            "params": ["big.nzb", content],
            "id": "big-body"
        }),
        "Bearer control-key",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["result"], 0);
}

#[tokio::test]
async fn resolve_scope_requires_explicit_auth_when_login_is_disabled() {
    let db = Database::open_in_memory().unwrap();
    let auth_cache = LoginAuthCache::default();
    let api_key_cache = ApiKeyCache::default();
    let headers = HeaderMap::new();
    let security = weaver_server_core::security::RuntimeSecurityConfig::default();
    let result = auth::resolve_scope(
        &db,
        &auth_cache,
        &api_key_cache,
        "session-token",
        &security,
        auth::BrowserSessionPolicy::Denied,
        &headers,
    )
    .await;
    assert_eq!(result, Err(StatusCode::UNAUTHORIZED));
}

#[tokio::test]
async fn resolve_scope_rejects_process_token_bearer_without_login() {
    let db = Database::open_in_memory().unwrap();
    let auth_cache = LoginAuthCache::default();
    let api_key_cache = ApiKeyCache::default();
    let mut headers = HeaderMap::new();
    headers.insert(
        header::AUTHORIZATION,
        HeaderValue::from_static("Bearer session-token"),
    );
    let security = weaver_server_core::security::RuntimeSecurityConfig::default();
    let result = auth::resolve_scope(
        &db,
        &auth_cache,
        &api_key_cache,
        "session-token",
        &security,
        auth::BrowserSessionPolicy::Denied,
        &headers,
    )
    .await;
    assert_eq!(result, Err(StatusCode::UNAUTHORIZED));
}

#[tokio::test]
async fn resolve_scope_accepts_session_cookie_only_from_trusted_peer() {
    let db = Database::open_in_memory().unwrap();
    let auth_cache = LoginAuthCache::default();
    let api_key_cache = ApiKeyCache::default();
    let mut headers = HeaderMap::new();
    headers.insert(
        header::COOKIE,
        HeaderValue::from_static("weaver_session=session-token"),
    );

    let security = {
        let security = weaver_server_core::security::RuntimeSecurityConfig::default();
        security.set_trusted_cidrs(vec!["127.0.0.0/8".parse().unwrap()]);
        security
    };
    let peer = "127.0.0.1:49152".parse().unwrap();
    let result = auth::resolve_scope(
        &db,
        &auth_cache,
        &api_key_cache,
        "session-token",
        &security,
        auth::BrowserSessionPolicy::TrustedPeer(Some(peer)),
        &headers,
    )
    .await;

    assert_eq!(result, Ok(CallerScope::Local));
}

#[tokio::test]
async fn resolve_scope_rejects_trusted_session_cookie_when_login_is_enabled() {
    let db = Database::open_in_memory().unwrap();
    let password_hash = hash_password(&test_password()).unwrap();
    let auth_cache = LoginAuthCache::default();
    auth_cache.replace(Some(CachedLoginAuth::new(
        "admin",
        password_hash,
        jwt::generate_jwt_secret(),
    )));
    let api_key_cache = ApiKeyCache::default();
    let mut headers = HeaderMap::new();
    headers.insert(
        header::COOKIE,
        HeaderValue::from_static("weaver_session=session-token"),
    );
    let security = {
        let security = weaver_server_core::security::RuntimeSecurityConfig::default();
        security.set_trusted_cidrs(vec!["0.0.0.0/0".parse().unwrap()]);
        security
    };
    let peer = "192.0.2.1:49152".parse().unwrap();

    let result = auth::resolve_scope(
        &db,
        &auth_cache,
        &api_key_cache,
        "session-token",
        &security,
        auth::BrowserSessionPolicy::TrustedPeer(Some(peer)),
        &headers,
    )
    .await;

    assert_eq!(result, Err(StatusCode::UNAUTHORIZED));
}

#[tokio::test]
async fn resolve_scope_rejects_session_cookie_from_untrusted_peer() {
    let db = Database::open_in_memory().unwrap();
    let auth_cache = LoginAuthCache::default();
    let api_key_cache = ApiKeyCache::default();
    let mut headers = HeaderMap::new();
    headers.insert(
        header::COOKIE,
        HeaderValue::from_static("weaver_session=session-token"),
    );
    let security = {
        let security = weaver_server_core::security::RuntimeSecurityConfig::default();
        security.set_trusted_cidrs(vec!["127.0.0.0/8".parse().unwrap()]);
        security
    };
    let peer = "192.0.2.1:49152".parse().unwrap();

    let result = auth::resolve_scope(
        &db,
        &auth_cache,
        &api_key_cache,
        "session-token",
        &security,
        auth::BrowserSessionPolicy::TrustedPeer(Some(peer)),
        &headers,
    )
    .await;

    assert_eq!(result, Err(StatusCode::UNAUTHORIZED));
}

#[tokio::test]
async fn explicit_invalid_api_key_does_not_fall_back_to_trusted_browser_cookie() {
    let db = Database::open_in_memory().unwrap();
    let auth_cache = LoginAuthCache::default();
    let api_key_cache = ApiKeyCache::default();
    let mut headers = HeaderMap::new();
    headers.insert(
        header::AUTHORIZATION,
        HeaderValue::from_static("Bearer invalid"),
    );
    headers.insert(
        header::COOKIE,
        HeaderValue::from_static("weaver_session=session-token"),
    );
    let security = {
        let security = weaver_server_core::security::RuntimeSecurityConfig::default();
        security.set_trusted_cidrs(vec!["127.0.0.0/8".parse().unwrap()]);
        security
    };
    let peer = "127.0.0.1:49152".parse().unwrap();

    let result = auth::resolve_scope(
        &db,
        &auth_cache,
        &api_key_cache,
        "session-token",
        &security,
        auth::BrowserSessionPolicy::TrustedPeer(Some(peer)),
        &headers,
    )
    .await;

    assert_eq!(result, Err(StatusCode::UNAUTHORIZED));
}

#[tokio::test]
async fn conflicting_api_key_headers_are_rejected() {
    let db = Database::open_in_memory().unwrap();
    let auth_cache = LoginAuthCache::default();
    let api_key_cache = ApiKeyCache::default();
    let mut headers = HeaderMap::new();
    headers.insert(
        header::AUTHORIZATION,
        HeaderValue::from_static("Bearer first"),
    );
    headers.insert("x-api-key", HeaderValue::from_static("second"));
    let security = weaver_server_core::security::RuntimeSecurityConfig::default();

    let result = auth::resolve_scope(
        &db,
        &auth_cache,
        &api_key_cache,
        "session-token",
        &security,
        auth::BrowserSessionPolicy::Denied,
        &headers,
    )
    .await;

    assert_eq!(result, Err(StatusCode::UNAUTHORIZED));
}

#[tokio::test]
async fn resolve_scope_rejects_process_token_in_x_api_key() {
    let db = Database::open_in_memory().unwrap();
    let auth_cache = LoginAuthCache::default();
    let api_key_cache = ApiKeyCache::default();
    let mut headers = HeaderMap::new();
    headers.insert("x-api-key", HeaderValue::from_static("session-token"));
    let security = weaver_server_core::security::RuntimeSecurityConfig::default();

    let result = auth::resolve_scope(
        &db,
        &auth_cache,
        &api_key_cache,
        "session-token",
        &security,
        auth::BrowserSessionPolicy::Denied,
        &headers,
    )
    .await;

    assert_eq!(result, Err(StatusCode::UNAUTHORIZED));
}

#[tokio::test]
async fn resolve_scope_accepts_cached_jwt_without_db_lookup() {
    let db = Database::open_in_memory().unwrap();
    let password_hash = hash_password(&test_password()).unwrap();
    let auth_cache = LoginAuthCache::default();
    let api_key_cache = ApiKeyCache::default();
    let auth = CachedLoginAuth::new("admin", password_hash, jwt::generate_jwt_secret());
    let token = jwt::create_jwt("admin", &auth.jwt_secret, JWT_TTL_SECS);
    auth_cache.replace(Some(auth));

    let mut headers = HeaderMap::new();
    headers.insert(
        header::COOKIE,
        HeaderValue::from_str(&format!("weaver_jwt={token}")).unwrap(),
    );

    let security = weaver_server_core::security::RuntimeSecurityConfig::default();
    let result = auth::resolve_scope(
        &db,
        &auth_cache,
        &api_key_cache,
        "session-token",
        &security,
        auth::BrowserSessionPolicy::Denied,
        &headers,
    )
    .await;
    assert_eq!(result, Ok(CallerScope::Admin));
}

#[tokio::test]
async fn resolve_scope_accepts_cached_api_key_without_db_lookup() {
    let db = Database::open_in_memory().unwrap();
    let auth_cache = LoginAuthCache::default();
    let api_key_cache = ApiKeyCache::default();
    let raw_key = "wvr_cached";
    api_key_cache.upsert(ApiKeyAuthRow {
        key_hash: hash_api_key(raw_key),
        id: 42,
        scope: "read".to_string(),
    });

    let mut headers = HeaderMap::new();
    headers.insert(
        header::AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {raw_key}")).unwrap(),
    );

    let security = weaver_server_core::security::RuntimeSecurityConfig::default();
    let result = auth::resolve_scope(
        &db,
        &auth_cache,
        &api_key_cache,
        "session-token",
        &security,
        auth::BrowserSessionPolicy::Denied,
        &headers,
    )
    .await;
    assert_eq!(result, Ok(CallerScope::Read));
}

#[tokio::test]
async fn login_handler_rejects_legacy_scrypt_hash() {
    let db = Database::open_in_memory().unwrap();
    let legacy_hash =
        "$scrypt$ln=16,r=8,p=1$MDAwMDAwMDAwMDAwMDAwMA$MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA"
            .to_string();
    db.set_auth_credentials("admin", &legacy_hash).unwrap();
    let auth_cache = LoginAuthCache::from_credentials(
        db.get_auth_credentials().unwrap(),
        db.get_or_create_jwt_signing_secret().unwrap(),
    );
    let app = auth_test_router(db.clone(), auth_cache.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/login")
                .header(header::CONTENT_TYPE, "application/json")
                .body(login_body("admin", &test_password()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let stored = db.get_auth_credentials().unwrap().unwrap();
    assert_eq!(stored.password_hash, legacy_hash);
    assert_eq!(auth_cache.snapshot().unwrap().password_hash, legacy_hash);
}

#[tokio::test]
async fn login_handler_wrong_password_keeps_argon2_hash_and_cache() {
    let db = Database::open_in_memory().unwrap();
    let argon2_hash = hash_password(&test_password()).unwrap();
    db.set_auth_credentials("admin", &argon2_hash).unwrap();
    let auth_cache = LoginAuthCache::from_credentials(
        db.get_auth_credentials().unwrap(),
        db.get_or_create_jwt_signing_secret().unwrap(),
    );
    let original = auth_cache.snapshot().unwrap();
    let app = auth_test_router(db.clone(), auth_cache.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/login")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"username":"admin","password":"wrong"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let stored = db.get_auth_credentials().unwrap().unwrap();
    assert_eq!(stored.password_hash, argon2_hash);
    assert_eq!(auth_cache.snapshot().unwrap(), original);
}

#[tokio::test]
async fn login_handler_wrong_username_with_valid_password_is_unauthorized() {
    let db = Database::open_in_memory().unwrap();
    let argon2_hash = hash_password(&test_password()).unwrap();
    db.set_auth_credentials("admin", &argon2_hash).unwrap();
    let auth_cache = LoginAuthCache::from_credentials(
        db.get_auth_credentials().unwrap(),
        db.get_or_create_jwt_signing_secret().unwrap(),
    );
    let original = auth_cache.snapshot().unwrap();
    let app = auth_test_router(db.clone(), auth_cache.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/login")
                .header(header::CONTENT_TYPE, "application/json")
                .body(login_body("not-admin", &test_password()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let stored = db.get_auth_credentials().unwrap().unwrap();
    assert_eq!(stored.password_hash, argon2_hash);
    assert_eq!(auth_cache.snapshot().unwrap(), original);
}

#[tokio::test]
async fn login_handler_rate_limits_repeated_failures() {
    let db = Database::open_in_memory().unwrap();
    let argon2_hash = hash_password(&test_password()).unwrap();
    db.set_auth_credentials("admin", &argon2_hash).unwrap();
    let auth_cache = LoginAuthCache::from_credentials(
        db.get_auth_credentials().unwrap(),
        db.get_or_create_jwt_signing_secret().unwrap(),
    );
    let app = auth_test_router(db, auth_cache);

    for _ in 0..auth::LOGIN_MAX_FAILURES {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/login")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"username":"admin","password":"wrong"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    let throttled_wrong = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/login")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"username":"admin","password":"wrong"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(throttled_wrong.status(), StatusCode::TOO_MANY_REQUESTS);

    let throttled_correct = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/login")
                .header(header::CONTENT_TYPE, "application/json")
                .body(login_body("admin", &test_password()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(throttled_correct.status(), StatusCode::TOO_MANY_REQUESTS);
}

#[tokio::test]
async fn login_handler_malformed_hash_fails_cleanly() {
    let db = Database::open_in_memory().unwrap();
    db.set_auth_credentials("admin", "not-a-phc-hash").unwrap();
    let auth_cache = LoginAuthCache::from_credentials(
        db.get_auth_credentials().unwrap(),
        db.get_or_create_jwt_signing_secret().unwrap(),
    );
    let original = auth_cache.snapshot().unwrap();
    let app = auth_test_router(db.clone(), auth_cache.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/login")
                .header(header::CONTENT_TYPE, "application/json")
                .body(login_body("admin", &test_password()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let stored = db.get_auth_credentials().unwrap().unwrap();
    assert_eq!(stored.password_hash, "not-a-phc-hash");
    assert_eq!(auth_cache.snapshot().unwrap(), original);
}

#[tokio::test]
async fn auth_status_handler_uses_cached_auth_state() {
    let db = Database::open_in_memory().unwrap();
    let password_hash = hash_password(&test_password()).unwrap();
    let auth_cache = LoginAuthCache::default();
    let auth = CachedLoginAuth::new("admin", password_hash, jwt::generate_jwt_secret());
    let token = jwt::create_jwt("admin", &auth.jwt_secret, JWT_TTL_SECS);
    auth_cache.replace(Some(auth));
    let app = auth_test_router(db, auth_cache);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/auth/status")
                .header(header::COOKIE, format!("weaver_jwt={token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["enabled"], true);
    assert_eq!(payload["authenticated"], true);
}

#[tokio::test]
async fn auth_status_describes_the_deployment_only_while_setup_is_pending() {
    let payload = auth_status_payload(auth_status_test_router(
        Database::open_in_memory().unwrap(),
        LoginAuthCache::default(),
        weaver_server_core::security::RuntimeSecurityConfig::default(),
    ))
    .await;

    assert_eq!(payload["setupRequired"], true);
    assert_eq!(payload["setup"]["bindEditable"], true);
    let deployment = payload["setup"]["deployment"].as_str().unwrap();
    assert!(
        ["native", "docker", "container"].contains(&deployment),
        "{deployment}"
    );

    // An environment-pinned address is reported as unaskable rather than asked
    // and then ignored.
    let security = {
        let mut security = weaver_server_core::security::RuntimeSecurityConfig::default();
        security.bind_address_source = weaver_server_core::security::BindAddressSource::Environment;
        security
    };
    let payload = auth_status_payload(auth_status_test_router(
        Database::open_in_memory().unwrap(),
        LoginAuthCache::default(),
        security,
    ))
    .await;
    assert_eq!(payload["setup"]["bindEditable"], false);
}

#[tokio::test]
async fn auth_status_omits_the_setup_facts_once_setup_cannot_run() {
    let db = Database::open_in_memory().unwrap();
    let auth_cache = LoginAuthCache::default();
    auth_cache.replace(Some(CachedLoginAuth::new(
        "admin",
        hash_password(&test_password()).unwrap(),
        jwt::generate_jwt_secret(),
    )));
    let payload = auth_status_payload(auth_status_test_router(
        db,
        auth_cache,
        weaver_server_core::security::RuntimeSecurityConfig::default(),
    ))
    .await;

    assert_eq!(payload["setupRequired"], false);
    assert!(payload.get("setup").is_none());

    // No credentials, but a peer the operator already trusts: the app renders,
    // so there is no wizard to inform either.
    let security = {
        let security = weaver_server_core::security::RuntimeSecurityConfig::default();
        security.set_trusted_cidrs(vec!["127.0.0.0/8".parse().unwrap()]);
        security
    };
    let payload = auth_status_payload(auth_status_test_router(
        Database::open_in_memory().unwrap(),
        LoginAuthCache::default(),
        security,
    ))
    .await;

    assert_eq!(payload["setupRequired"], false);
    assert!(payload.get("setup").is_none());
}

#[tokio::test]
async fn a_configured_no_login_instance_never_asks_an_outside_browser_to_set_up() {
    // The Loop-1 pin. A no-login install with a widened bind has no
    // credentials and trusts nothing but loopback, so every other browser sees
    // exactly what a fresh install looks like — and used to be handed a wizard
    // whose endpoint refuses it, on every visit, forever. Setup is offered to
    // exactly the peers that could complete it: loopback. An outside browser
    // never gets it, configured or not — the entry page tells it where setup
    // runs instead.
    let lan_browser = "192.168.1.20:49152";

    let fresh_loopback = auth_status_payload(auth_status_test_router_from_peer(
        Database::open_in_memory().unwrap(),
        LoginAuthCache::default(),
        weaver_server_core::security::RuntimeSecurityConfig::default(),
        "127.0.0.1:49152",
    ))
    .await;
    assert_eq!(fresh_loopback["setupRequired"], true);
    assert!(fresh_loopback.get("setup").is_some());

    let fresh_lan = auth_status_payload(auth_status_test_router_from_peer(
        Database::open_in_memory().unwrap(),
        LoginAuthCache::default(),
        weaver_server_core::security::RuntimeSecurityConfig::default(),
        lan_browser,
    ))
    .await;
    assert_eq!(
        fresh_lan["setupRequired"], false,
        "a peer the wizard endpoint would refuse must not be told to run it"
    );
    assert!(fresh_lan.get("setup").is_none());

    let configured = {
        let security = weaver_server_core::security::RuntimeSecurityConfig::default();
        security.apply_stored_trust(Some("no_login"), None);
        security
    };
    let payload = auth_status_payload(auth_status_test_router_from_peer(
        Database::open_in_memory().unwrap(),
        LoginAuthCache::default(),
        configured,
        lan_browser,
    ))
    .await;

    assert_eq!(payload["setupRequired"], false);
    assert_eq!(payload["authenticated"], false);
    // And the deployment facts stay unspoken: this endpoint is
    // unauthenticated, and there is no wizard left to inform.
    assert!(payload.get("setup").is_none());
}

#[tokio::test]
async fn configured_missing_credentials_does_not_offer_setup_without_explicit_reset() {
    // Recovery is armed only by startup after WEAVER_RESET_LOGIN. A test that
    // supplies just the configured, credential-less state must fail closed.
    let security = weaver_server_core::security::RuntimeSecurityConfig::default();
    security.apply_stored_trust(Some("login_required"), None);
    assert!(security.security_configured());

    let payload = auth_status_payload(auth_status_test_router_from_peer(
        Database::open_in_memory().unwrap(),
        LoginAuthCache::default(),
        security,
        "127.0.0.1:49152",
    ))
    .await;

    assert_eq!(payload["setupRequired"], false);
    assert!(payload.get("setup").is_none());
}

#[tokio::test]
async fn an_env_pinned_deployment_never_asks_an_outside_browser_to_set_up() {
    // Loop 2's other half: `WEAVER_TRUSTED_CIDRS` declares the policy in the
    // deployment, so a browser outside those networks has nothing to complete.
    let security = {
        let mut security = weaver_server_core::security::RuntimeSecurityConfig::default();
        security.trust_env_pinned = true;
        security.set_trusted_cidrs(vec!["10.0.0.0/8".parse().unwrap()]);
        security.apply_stored_trust(None, None);
        security
    };
    let payload = auth_status_payload(auth_status_test_router_from_peer(
        Database::open_in_memory().unwrap(),
        LoginAuthCache::default(),
        security,
        "192.168.1.20:49152",
    ))
    .await;

    assert_eq!(payload["setupRequired"], false);
    assert!(payload.get("setup").is_none());
}

#[tokio::test]
async fn job_nzb_download_handler_returns_uncompressed_history_nzb() {
    let db = Database::open_in_memory().unwrap();
    let handle = test_scheduler_handle();
    let xml = minimal_nzb("Silver.Horizon.S05.720p.BluRay.DD5.1.x264-WVR");
    let nzb_zstd = weaver_server_core::ingest::compress_nzb_bytes(xml.as_bytes()).unwrap();
    db.create_active_job(&weaver_server_core::ActiveJob {
        job_id: JobId(10_000),
        nzb_hash: weaver_server_core::ingest::hash_persisted_nzb_bytes(&nzb_zstd),
        nzb_path: std::path::PathBuf::from("Silver.Horizon.S05.720p.BluRay.DD5.1.x264-WVR.nzb"),
        nzb_zstd,
        output_dir: std::path::PathBuf::from("/tmp/weaver-http-test"),
        created_at: 1_700_000_000,
        category: Some("tv".to_string()),
        metadata: vec![],
        status: "queued",
        download_state: "queued",
        post_state: "idle",
        run_state: "active",
        paused_resume_status: None,
        paused_resume_download_state: None,
        paused_resume_post_state: None,
    })
    .unwrap();
    db.archive_job(
        JobId(10_000),
        &weaver_server_core::JobHistoryRow {
            job_id: 10_000,
            job_hash: None,
            name: "Silver Horizon".to_string(),
            status: "complete".to_string(),
            error_message: None,
            total_bytes: 123,
            downloaded_bytes: 123,
            optional_recovery_bytes: 0,
            optional_recovery_downloaded_bytes: 0,
            failed_bytes: 0,
            health: 1000,
            category: Some("tv".to_string()),
            output_dir: None,
            nzb_path: Some("Silver.Horizon.S05.720p.BluRay.DD5.1.x264-WVR.nzb".to_string()),
            created_at: 1_700_000_000,
            completed_at: 1_700_000_100,
            metadata: Some(
                serde_json::to_string(&vec![(
                    weaver_server_core::ingest::ORIGINAL_TITLE_METADATA_KEY.to_string(),
                    "Silver.Horizon.S05.720p.BluRay.DD5.1.x264-WVR".to_string(),
                )])
                .unwrap(),
            ),
        },
    )
    .unwrap();
    let app = job_nzb_test_router(db, handle);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/jobs/10000/nzb")
                .header(header::AUTHORIZATION, "Bearer session-token")
                .header(header::ACCEPT_ENCODING, "gzip")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("application/x-nzb")
    );
    assert_eq!(
        response
            .headers()
            .get(header::CONTENT_DISPOSITION)
            .and_then(|value| value.to_str().ok()),
        Some("attachment; filename=\"Silver.Horizon.S05.720p.BluRay.DD5.1.x264-WVR.nzb\"")
    );
    assert_eq!(
        response
            .headers()
            .get(header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok()),
        Some(xml.len().to_string().as_str())
    );
    assert!(response.headers().get(header::CONTENT_ENCODING).is_none());

    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(body, Bytes::from(xml));
}

#[tokio::test]
async fn job_output_file_download_handler_streams_history_file() {
    let db = Database::open_in_memory().unwrap();
    let handle = test_scheduler_handle();
    let temp_dir = tempfile::tempdir().unwrap();
    let output_dir = temp_dir.path().join("job-output");
    std::fs::create_dir_all(&output_dir).unwrap();
    let file_path = output_dir.join("episode-01.mkv");
    std::fs::write(&file_path, b"video-bytes").unwrap();
    db.insert_job_history(&weaver_server_core::JobHistoryRow {
        job_id: 10_001,
        job_hash: None,
        name: "Silver Horizon".to_string(),
        status: "complete".to_string(),
        error_message: None,
        total_bytes: 123,
        downloaded_bytes: 123,
        optional_recovery_bytes: 0,
        optional_recovery_downloaded_bytes: 0,
        failed_bytes: 0,
        health: 1000,
        category: Some("tv".to_string()),
        output_dir: Some(output_dir.display().to_string()),
        nzb_path: None,
        created_at: 1_700_000_000,
        completed_at: 1_700_000_100,
        metadata: None,
    })
    .unwrap();
    let app = job_nzb_test_router(db, handle);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/jobs/10001/output-file")
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .header(header::ACCEPT_ENCODING, "gzip")
                .header(header::AUTHORIZATION, "Bearer session-token")
                .body(Body::from(format!("path={}", file_path.display())))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(header::CONTENT_DISPOSITION)
            .and_then(|value| value.to_str().ok()),
        Some("attachment; filename=\"episode-01.mkv\"")
    );
    assert_eq!(
        response
            .headers()
            .get(header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok()),
        Some("11")
    );
    assert!(response.headers().get(header::CONTENT_ENCODING).is_none());
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(body, Bytes::from_static(b"video-bytes"));
}
