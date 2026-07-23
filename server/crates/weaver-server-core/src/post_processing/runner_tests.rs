use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use hmac::{Hmac, Mac};
use sha2::Sha256;

use super::discovery::{DiscoveryOptions, discover_extensions};
use super::manifest::parse_native_manifest;
use super::model::{
    ExtensionAdapter, ExtensionDigest, NzbgetCompatibilityName, OptionName, PipelineFailureStage,
    PipelineOutcome, ResolvedOption, ResolvedOptionValue, SecretOptionValue, TimeoutPolicy,
    VerifiedExtensionDigest,
};
use super::persistence::LogStream;
use super::runner::{
    CapturedOutputLine, ControlEffects, ExecutionDisposition, ExtensionExecutionRequest,
    ExtensionExecutionResult, InterpreterConfig, JobExecutionContext, NzbgetScriptStatus,
    RunnerError, adapter_contract_for_test, bounded_output_for_test, control_effects_for_test,
    execute_extension, redact_bytes_for_test, redact_execution_result_for_test,
    webhook_url_for_test,
};

type CapturedWebhookRequests = Arc<Mutex<Vec<(HeaderMap, Vec<u8>)>>>;

#[derive(Clone, Default)]
struct WebhookCapture {
    attempts: Arc<AtomicUsize>,
    requests: CapturedWebhookRequests,
}

async fn webhook_handler(
    State(capture): State<WebhookCapture>,
    headers: HeaderMap,
    body: Bytes,
) -> (StatusCode, &'static str) {
    capture
        .requests
        .lock()
        .unwrap()
        .push((headers, body.to_vec()));
    if capture.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
        (StatusCode::INTERNAL_SERVER_ERROR, "retry")
    } else {
        (StatusCode::OK, "top-secret")
    }
}

fn native_manifest() -> super::model::ExtensionManifest {
    parse_native_manifest(
        r#"{
            "schema_version": 1,
            "kind": "native",
            "id": "example.runner",
            "name": "Runner",
            "version": "1",
            "entrypoint": "run.sh",
            "commands": [],
            "options": []
        }"#,
        VerifiedExtensionDigest::from_verified_package_digest(
            ExtensionDigest::new(format!("blake3:{}", "b".repeat(64))).unwrap(),
        ),
    )
    .unwrap()
}

fn request(manifest: super::model::ExtensionManifest) -> ExtensionExecutionRequest {
    ExtensionExecutionRequest {
        attempt_id: "attempt-test".into(),
        manifest,
        managed_path: PathBuf::from("/managed"),
        options: vec![],
        approved_roots: vec![],
        context: JobExecutionContext {
            job_id: 42,
            name: "Example Job".into(),
            nzb_filename: "example.nzb".into(),
            category: Some("movies".into()),
            group: Some("alt.binaries.test".into()),
            source_url: Some("https://example.invalid/failure".into()),
            working_directory: PathBuf::from("/work/job"),
            final_directory: PathBuf::from("/complete/job"),
            pipeline_outcome: PipelineOutcome::Failed {
                stage: PipelineFailureStage::Extract,
                code: "extract_failed".into(),
                message: "archive failed".into(),
            },
            par_status: 2,
            unpack_status: 2,
            compatibility: super::runner::CompatibilityFacts::default(),
        },
        timeout_policy: TimeoutPolicy::Default24Hours,
        termination_grace: Duration::from_secs(10),
        interpreters: InterpreterConfig::default(),
        control_token: None,
        diagnostic_command: None,
        supervisor_executable: None,
    }
}

#[test]
fn sab_adapter_supplies_the_documented_eight_arguments() {
    let native = native_manifest();
    let sab = super::model::ExtensionManifest::new(
        ExtensionAdapter::Sabnzbd,
        None,
        "SAB script".into(),
        native.revision().clone(),
        "run.sh".into(),
        vec![],
        vec![],
        vec![],
    )
    .unwrap();
    let mut request = request(sab);
    request.context.compatibility.total_bytes = 1_000;
    request.context.compatibility.downloaded_bytes = 900;
    request.context.compatibility.password = Some("job-password".into());
    request.context.compatibility.failure_message = Some("unpack failed".into());
    let (args, env) = adapter_contract_for_test(&request).unwrap();
    assert_eq!(args.len(), 8);
    assert_eq!(args[0], "/work/job");
    assert_eq!(args[6], "2");
    assert_eq!(args[7], "");
    assert_eq!(env["SAB_NZO_ID"], "42");
    assert_eq!(env["SAB_COMPLETE_DIR"], args[0]);
    assert_eq!(env["SAB_PP_STATUS"], args[6]);
    assert_eq!(env["SAB_FAIL_MSG"], "unpack failed");
    assert_eq!(env["SAB_URL"], "https://example.invalid/failure");
    assert_eq!(env["SAB_FAILURE_URL"], "");
    assert_eq!(env["SAB_BYTES"], "1000");
    assert_eq!(env["SAB_BYTES_DOWNLOADED"], "900");
    assert_eq!(env["SAB_PASSWORD"], "job-password");
    assert!(!env.contains_key("SAB_API_KEY"));

    for (stage, expected) in [
        (PipelineFailureStage::Download, "-1"),
        (PipelineFailureStage::Verify, "1"),
        (PipelineFailureStage::Repair, "1"),
        (PipelineFailureStage::Move, "2"),
    ] {
        request.context.pipeline_outcome = PipelineOutcome::Failed {
            stage,
            code: "failed".into(),
            message: "failed".into(),
        };
        assert_eq!(adapter_contract_for_test(&request).unwrap().0[6], expected);
    }
}

#[test]
fn nzbget_adapter_supplies_status_options_and_control_commands() {
    let native = native_manifest();
    let nzbget = super::model::ExtensionManifest::new(
        ExtensionAdapter::Nzbget,
        Some(NzbgetCompatibilityName::new("Example").unwrap()),
        "NZBGet script".into(),
        native.revision().clone(),
        "run.sh".into(),
        vec![],
        vec![],
        vec![],
    )
    .unwrap();
    let mut request = request(nzbget);
    request.options.push(ResolvedOption::new(
        super::model::OptionName::new("Api.Token").unwrap(),
        ResolvedOptionValue::Secret(SecretOptionValue::for_execution("secret-value")),
    ));
    request.context.compatibility.health_milli = 952;
    request.context.compatibility.critical_health_milli = 900;
    request.context.compatibility.previous_script_status = NzbgetScriptStatus::Failure;
    request.context.compatibility.data_dir = Some(PathBuf::from("/data"));
    request.context.compatibility.intermediate_dir = Some(PathBuf::from("/intermediate"));
    request.context.compatibility.complete_dir = Some(PathBuf::from("/complete"));
    request.context.compatibility.temp_dir = Some(PathBuf::from("/tmp"));
    request.context.compatibility.app_dir = Some(PathBuf::from("/app"));
    let (args, env) = adapter_contract_for_test(&request).unwrap();
    assert!(args.is_empty());
    assert_eq!(env["NZBPP_STATUS"], "FAILURE/UNPACK");
    assert_eq!(env["NZBPP_TOTALSTATUS"], "FAILURE");
    assert_eq!(env["NZBPP_PARSTATUS"], "2");
    assert_eq!(env["NZBPP_SCRIPTSTATUS"], "FAILURE");
    assert_eq!(env["NZBPP_HEALTH"], "952");
    assert_eq!(env["NZBPO_Api.Token"], "secret-value");
    assert_eq!(env["NZBPO_API_TOKEN"], "secret-value");
    assert_eq!(env["NZBOP_MAINDIR"], "/data");
    assert_eq!(env["NZBOP_INTERDIR"], "/intermediate");
    assert_eq!(env["NZBOP_DESTDIR"], "/complete");

    request.context.pipeline_outcome = PipelineOutcome::Succeeded;
    request.context.par_status = 0;
    request.context.unpack_status = 0;
    assert_eq!(
        adapter_contract_for_test(&request).unwrap().1["NZBPP_STATUS"],
        "SUCCESS/HEALTH"
    );
    request.context.par_status = 2;
    assert_eq!(
        adapter_contract_for_test(&request).unwrap().1["NZBPP_STATUS"],
        "SUCCESS/ALL"
    );

    let lines = vec![
        CapturedOutputLine {
            sequence: 0,
            stream: LogStream::Stdout,
            bytes: b"[NZB] DIRECTORY=/work/job/new\n".to_vec(),
        },
        CapturedOutputLine {
            sequence: 1,
            stream: LogStream::Stdout,
            bytes: b"[NZB] NZBPR_RESULT=ok\n".to_vec(),
        },
        CapturedOutputLine {
            sequence: 2,
            stream: LogStream::Stdout,
            bytes: b"[NZB] MARK=BAD\n".to_vec(),
        },
    ];
    let effects = control_effects_for_test(ExtensionAdapter::Nzbget, &lines).unwrap();
    assert_eq!(effects.directory, Some(PathBuf::from("/work/job/new")));
    assert_eq!(effects.parameters["RESULT"], "ok");
    assert!(effects.mark_bad);
}

#[test]
fn control_output_survives_display_truncation_and_enforces_its_own_limit() {
    let mut lines = vec![b"display header\n".to_vec()];
    lines.push(b"[WEAVER] {\"type\":\"directory\",\"path\":\"/work/next\"}\n".to_vec());
    lines.extend((0..80).map(|_| vec![b'x'; 64 * 1024]));
    let (display, effects, truncated) =
        bounded_output_for_test(ExtensionAdapter::Native, lines).unwrap();
    assert!(truncated);
    assert_eq!(effects.directory, Some(PathBuf::from("/work/next")));
    assert!(
        display
            .iter()
            .all(|line| !line.bytes.starts_with(b"[WEAVER] "))
    );

    let oversized_control = (0..65)
        .map(|_| {
            let mut line = b"[NZB] NZBPR_VALUE=".to_vec();
            line.extend(std::iter::repeat_n(b'x', 65_500));
            line.push(b'\n');
            line
        })
        .collect();
    assert!(matches!(
        bounded_output_for_test(ExtensionAdapter::Nzbget, oversized_control),
        Err(RunnerError::SupervisorProtocol(_))
    ));
}

#[test]
fn nzbget_diagnostic_uses_command_context_without_job_context() {
    let native = native_manifest();
    let nzbget = super::model::ExtensionManifest::new(
        ExtensionAdapter::Nzbget,
        Some(NzbgetCompatibilityName::new("Example").unwrap()),
        "NZBGet script".into(),
        native.revision().clone(),
        "run.sh".into(),
        vec![],
        vec![],
        vec![],
    )
    .unwrap();
    let mut request = request(nzbget);
    request.diagnostic_command = Some("ConnectionTest".into());
    request.options.push(ResolvedOption::new(
        super::model::OptionName::new("Api.Token").unwrap(),
        ResolvedOptionValue::Secret(SecretOptionValue::for_execution("secret-value")),
    ));

    let (args, env) = adapter_contract_for_test(&request).unwrap();

    assert!(args.is_empty());
    assert_eq!(env["NZBCP_COMMAND"], "ConnectionTest");
    assert_eq!(env["NZBPO_API_TOKEN"], "secret-value");
    assert!(!env.contains_key("NZBPP_NZBID"));
}

#[test]
fn secret_values_are_redacted_even_when_embedded_in_hostile_output() {
    let redacted = redact_bytes_for_test(
        b"prefix secret-value suffix secret-value",
        &[b"secret-value".to_vec()],
    );
    assert_eq!(
        String::from_utf8(redacted).unwrap(),
        "prefix [REDACTED] suffix [REDACTED]"
    );
}

#[test]
fn redaction_covers_chunk_boundaries_keys_errors_and_rejects_sensitive_paths() {
    let secrets = [b"top-secret".to_vec()];
    let mut result = ExtensionExecutionResult {
        disposition: ExecutionDisposition::Failed,
        exit_code: Some(1),
        output: vec![
            CapturedOutputLine {
                sequence: 0,
                stream: LogStream::Stdout,
                bytes: b"prefix top-".to_vec(),
            },
            CapturedOutputLine {
                sequence: 1,
                stream: LogStream::Stdout,
                bytes: b"secret suffix".to_vec(),
            },
        ],
        output_truncated: false,
        effects: ControlEffects {
            parameters: [("top-secret-key".into(), "top-secret-value".into())]
                .into_iter()
                .collect(),
            metadata: [(
                "top-secret-metadata".into(),
                serde_json::json!({"top-secret-json-key": "top-secret-json-value"}),
            )]
            .into_iter()
            .collect(),
            ..ControlEffects::default()
        },
        error_message: Some("failed with top-secret".into()),
    };
    redact_execution_result_for_test(&mut result, &secrets).unwrap();
    let serialized = format!("{result:?}");
    assert!(!serialized.contains("top-secret"));
    assert!(serialized.contains("[REDACTED]"));

    result.effects.directory = Some(PathBuf::from("/tmp/top-secret"));
    assert!(matches!(
        redact_execution_result_for_test(&mut result, &secrets),
        Err(RunnerError::SensitiveControlEffect)
    ));
}

#[test]
fn webhook_urls_reject_embedded_user_information() {
    let mut request = request(native_manifest());
    request.options = vec![ResolvedOption::new(
        OptionName::new("webhook_url").unwrap(),
        ResolvedOptionValue::String("https://user:password@example.test/hook".into()),
    )];
    assert!(matches!(
        webhook_url_for_test(&request),
        Err(RunnerError::InvalidWebhookConfiguration(_))
    ));
}

#[tokio::test]
async fn webhook_retries_with_stable_idempotency_and_hmac_and_redacts_credentials() {
    let capture = WebhookCapture::default();
    let app = Router::new()
        .route("/hook", post(webhook_handler))
        .with_state(capture.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let data = tempfile::tempdir().unwrap();
    let package = data.path().join("scripts").join("webhook");
    fs::create_dir_all(&package).unwrap();
    fs::write(
        package.join("weaver-extension.json"),
        r#"{
            "schema_version": 1,
            "kind": "webhook",
            "id": "example.webhook",
            "name": "Webhook",
            "version": "1",
            "entrypoint": "webhook",
            "commands": [],
            "options": []
        }"#,
    )
    .unwrap();
    let manifest = discover_extensions(
        data.path(),
        DiscoveryOptions {
            enabled: true,
            bare_script_adapter: None,
        },
    )
    .unwrap()
    .remove(0)
    .manifest;
    let mut request = request(manifest);
    request.managed_path = package;
    request.timeout_policy =
        TimeoutPolicy::Finite(super::model::NonZeroTimeoutSeconds::new(10).unwrap());
    request.options = vec![
        ResolvedOption::new(
            OptionName::new("webhook_url").unwrap(),
            ResolvedOptionValue::String(format!("http://{address}/hook")),
        ),
        ResolvedOption::new(
            OptionName::new("webhook_retries").unwrap(),
            ResolvedOptionValue::Integer(1),
        ),
        ResolvedOption::new(
            OptionName::new("webhook_hmac_secret").unwrap(),
            ResolvedOptionValue::Secret(SecretOptionValue::for_execution("hmac-secret")),
        ),
        ResolvedOption::new(
            OptionName::new("webhook_bearer_token").unwrap(),
            ResolvedOptionValue::Secret(SecretOptionValue::for_execution("top-secret")),
        ),
    ];

    let result = execute_extension(request, None).await.unwrap();
    server.abort();

    assert_eq!(result.disposition, ExecutionDisposition::Succeeded);
    assert_eq!(capture.attempts.load(Ordering::SeqCst), 2);
    let requests = capture.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].1, requests[1].1);
    assert_eq!(
        requests[0].0["idempotency-key"],
        requests[1].0["idempotency-key"]
    );
    assert_eq!(requests[0].0["authorization"], "Bearer top-secret");
    let payload: serde_json::Value = serde_json::from_slice(&requests[0].1).unwrap();
    assert_eq!(payload["options"], serde_json::json!({}));
    let mut mac = Hmac::<Sha256>::new_from_slice(b"hmac-secret").unwrap();
    mac.update(&requests[0].1);
    let expected = format!("sha256={}", hex::encode(mac.finalize().into_bytes()));
    assert_eq!(
        requests[0].0["x-weaver-signature"].to_str().unwrap(),
        expected
    );
    let output = result
        .output
        .iter()
        .flat_map(|line| line.bytes.iter().copied())
        .collect::<Vec<_>>();
    let output = String::from_utf8(output).unwrap();
    assert!(!output.contains("top-secret"));
    assert!(output.contains("[REDACTED]"));
}
