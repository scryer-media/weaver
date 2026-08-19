use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use super::model::{
    NzbgetCompatibilityName, OptionName, OptionValue, PipelineFailureStage, PipelineOutcome,
    ResolvedOption, ScriptAdapter, ScriptManifest, SecretOptionValue,
};
use super::runner::{
    CompatibilityFacts, ExecutionDisposition, InterpreterConfig, JobExecutionContext,
    MAX_SCRIPT_OUTPUT_BYTES, NzbgetScriptStatus, ScriptExecutionRequest, adapter_contract_for_test,
    adapter_disposition_for_test, bounded_output_for_test, redact_bytes_for_test,
};

fn manifest(adapter: ScriptAdapter) -> ScriptManifest {
    let compatibility_name = match adapter {
        ScriptAdapter::Nzbget => Some(NzbgetCompatibilityName::new("email").unwrap()),
        ScriptAdapter::Sabnzbd => None,
    };
    ScriptManifest::new(
        adapter,
        compatibility_name,
        "Runner".into(),
        Some("1.0.0".into()),
        "run.sh".into(),
        vec![],
        vec![],
    )
    .unwrap()
}

fn request(adapter: ScriptAdapter) -> ScriptExecutionRequest {
    ScriptExecutionRequest {
        manifest: manifest(adapter),
        root: PathBuf::from("/scripts"),
        options: vec![],
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
            compatibility: CompatibilityFacts::default(),
        },
        timeout: Some(Duration::from_secs(60)),
        termination_grace: Duration::from_secs(10),
        interpreters: InterpreterConfig::default(),
        supervisor_executable: None,
    }
}

fn contract(request: &ScriptExecutionRequest) -> (Vec<String>, BTreeMap<String, String>) {
    adapter_contract_for_test(request).unwrap()
}

#[test]
fn sab_adapter_supplies_the_documented_eight_arguments_and_sab_variables() {
    let mut request = request(ScriptAdapter::Sabnzbd);
    request.options = vec![ResolvedOption::new(
        OptionName::new("ApiKey").unwrap(),
        OptionValue::String("value".into()),
    )];
    let (args, env) = contract(&request);

    assert_eq!(
        args,
        vec![
            "/work/job".to_string(),
            "example.nzb".to_string(),
            "Example Job".to_string(),
            String::new(),
            "movies".to_string(),
            "alt.binaries.test".to_string(),
            // Extract failure is SABnzbd's status 2.
            "2".to_string(),
            String::new(),
        ]
    );
    assert_eq!(env.get("SAB_NZO_ID").unwrap(), "42");
    assert_eq!(env.get("SAB_FINAL_NAME").unwrap(), "Example Job");
    assert_eq!(env.get("SAB_FILENAME").unwrap(), "example.nzb");
    assert_eq!(env.get("SAB_CAT").unwrap(), "movies");
    assert_eq!(env.get("SAB_GROUP").unwrap(), "alt.binaries.test");
    assert_eq!(env.get("SAB_COMPLETE_DIR").unwrap(), "/work/job");
    assert_eq!(env.get("SAB_STATUS").unwrap(), "Running");
    assert_eq!(env.get("SAB_PP_STATUS").unwrap(), "2");
    assert_eq!(
        env.get("SAB_URL").unwrap(),
        "https://example.invalid/failure"
    );
    assert_eq!(env.get("SAB_REPAIR").unwrap(), "1");
    assert_eq!(env.get("SAB_UNPACK").unwrap(), "1");
    assert_eq!(env.get("SAB_SCRIPT").unwrap(), "run.sh");
    assert_eq!(env.get("SAB_OPTION_APIKEY").unwrap(), "value");
    // Fields weaver has no equivalent for are still present and empty, because
    // SABnzbd scripts read them unconditionally.
    for empty in [
        "SAB_CORRECT_PASSWORD",
        "SAB_DUPLICATE",
        "SAB_DUPLICATE_KEY",
        "SAB_ENCRYPTED",
        "SAB_OVERSIZED",
        "SAB_PP",
        "SAB_PRIORITY",
        "SAB_UNWANTED_EXT",
        "SAB_FAILURE_URL",
    ] {
        assert_eq!(env.get(empty).map(String::as_str), Some(""), "{empty}");
    }
}

#[test]
fn sab_pipeline_status_follows_the_failing_stage() {
    let mut request = request(ScriptAdapter::Sabnzbd);
    for (stage, expected) in [
        (PipelineFailureStage::Verify, "1"),
        (PipelineFailureStage::Repair, "1"),
        (PipelineFailureStage::Extract, "2"),
        (PipelineFailureStage::Move, "2"),
        (PipelineFailureStage::Download, "-1"),
    ] {
        request.context.pipeline_outcome = PipelineOutcome::Failed {
            stage,
            code: "x".into(),
            message: "y".into(),
        };
        let (_, env) = contract(&request);
        assert_eq!(env.get("SAB_PP_STATUS").map(String::as_str), Some(expected));
    }
    request.context.pipeline_outcome = PipelineOutcome::Succeeded;
    let (_, env) = contract(&request);
    assert_eq!(env.get("SAB_PP_STATUS").map(String::as_str), Some("0"));
}

#[test]
fn nzbget_adapter_supplies_nzbpp_nzbpo_and_nzbop_variables_with_no_positional_args() {
    let mut request = request(ScriptAdapter::Nzbget);
    request.options = vec![ResolvedOption::new(
        OptionName::new("Server.Timeout").unwrap(),
        OptionValue::Integer(30),
    )];
    request.context.compatibility = CompatibilityFacts {
        health_milli: 900,
        critical_health_milli: 800,
        data_dir: Some(PathBuf::from("/data")),
        intermediate_dir: Some(PathBuf::from("/data/intermediate")),
        complete_dir: Some(PathBuf::from("/data/complete")),
        temp_dir: Some(PathBuf::from("/tmp")),
        app_dir: Some(PathBuf::from("/opt/weaver")),
        previous_script_status: NzbgetScriptStatus::Success,
        ..CompatibilityFacts::default()
    };
    let (args, env) = contract(&request);

    assert!(args.is_empty(), "NZBGet scripts read the environment only");
    assert_eq!(env.get("NZBPP_NZBID").unwrap(), "42");
    assert_eq!(env.get("NZBPP_NZBNAME").unwrap(), "Example Job");
    assert_eq!(env.get("NZBPP_DIRECTORY").unwrap(), "/work/job");
    assert_eq!(env.get("NZBPP_NZBFILENAME").unwrap(), "example.nzb");
    assert_eq!(env.get("NZBPP_QUEUEDFILE").unwrap(), "example.nzb");
    assert_eq!(env.get("NZBPP_FINALDIR").unwrap(), "/complete/job");
    assert_eq!(env.get("NZBPP_CATEGORY").unwrap(), "movies");
    assert_eq!(env.get("NZBPP_STATUS").unwrap(), "FAILURE/UNPACK");
    assert_eq!(env.get("NZBPP_TOTALSTATUS").unwrap(), "FAILURE");
    assert_eq!(env.get("NZBPP_SCRIPTSTATUS").unwrap(), "SUCCESS");
    assert_eq!(env.get("NZBPP_PARSTATUS").unwrap(), "2");
    assert_eq!(env.get("NZBPP_UNPACKSTATUS").unwrap(), "2");
    assert_eq!(env.get("NZBPP_HEALTH").unwrap(), "900");
    assert_eq!(env.get("NZBPP_CRITICALHEALTH").unwrap(), "800");
    // Options keep their documented name and also gain the normalized alias,
    // because legacy scripts read whichever the manifest declared.
    assert_eq!(env.get("NZBPO_Server.Timeout").unwrap(), "30");
    assert_eq!(env.get("NZBPO_SERVER_TIMEOUT").unwrap(), "30");
    assert_eq!(env.get("NZBOP_MainDir").unwrap(), "/data");
    assert_eq!(env.get("NZBOP_MAINDIR").unwrap(), "/data");
    assert_eq!(env.get("NZBOP_InterDir").unwrap(), "/data/intermediate");
    assert_eq!(env.get("NZBOP_DestDir").unwrap(), "/data/complete");
    assert_eq!(env.get("NZBOP_TempDir").unwrap(), "/tmp");
    assert_eq!(env.get("NZBOP_AppDir").unwrap(), "/opt/weaver");
    assert!(env.contains_key("NZBOP_Version"));
}

#[test]
fn nzbget_pipeline_status_follows_the_failing_stage_and_success_detail() {
    let mut request = request(ScriptAdapter::Nzbget);
    for (stage, expected) in [
        (PipelineFailureStage::Download, "FAILURE/HEALTH"),
        (PipelineFailureStage::Verify, "FAILURE/PAR"),
        (PipelineFailureStage::Repair, "FAILURE/PAR"),
        (PipelineFailureStage::Extract, "FAILURE/UNPACK"),
        (PipelineFailureStage::Move, "FAILURE/MOVE"),
    ] {
        request.context.pipeline_outcome = PipelineOutcome::Failed {
            stage,
            code: "x".into(),
            message: "y".into(),
        };
        let (_, env) = contract(&request);
        assert_eq!(env.get("NZBPP_STATUS").map(String::as_str), Some(expected));
    }
    request.context.pipeline_outcome = PipelineOutcome::Succeeded;
    let (_, env) = contract(&request);
    assert_eq!(env.get("NZBPP_STATUS").unwrap(), "SUCCESS/ALL");
    request.context.par_status = 0;
    request.context.unpack_status = 0;
    let (_, env) = contract(&request);
    assert_eq!(env.get("NZBPP_STATUS").unwrap(), "SUCCESS/HEALTH");
}

#[test]
fn boolean_options_use_the_yes_no_spelling_both_ecosystems_expect() {
    let mut request = request(ScriptAdapter::Nzbget);
    request.options = vec![
        ResolvedOption::new(
            OptionName::new("Enabled").unwrap(),
            OptionValue::Boolean(true),
        ),
        ResolvedOption::new(
            OptionName::new("Disabled").unwrap(),
            OptionValue::Boolean(false),
        ),
    ];
    let (_, env) = contract(&request);
    assert_eq!(env.get("NZBPO_Enabled").unwrap(), "yes");
    assert_eq!(env.get("NZBPO_Disabled").unwrap(), "no");
}

#[test]
fn secret_options_reach_the_script_verbatim() {
    let mut request = request(ScriptAdapter::Nzbget);
    request.options = vec![ResolvedOption::new(
        OptionName::new("Token").unwrap(),
        OptionValue::Secret(SecretOptionValue::from_admin_input("hunter2")),
    )];
    let (_, env) = contract(&request);
    assert_eq!(env.get("NZBPO_Token").unwrap(), "hunter2");
}

#[test]
fn exit_codes_map_onto_each_ecosystem_contract() {
    use ExecutionDisposition::{Failed, Skipped, Succeeded, Warned};
    // SABnzbd: zero succeeds, anything else is a warning on the job.
    assert_eq!(
        adapter_disposition_for_test(ScriptAdapter::Sabnzbd, Some(0)),
        Succeeded
    );
    assert_eq!(
        adapter_disposition_for_test(ScriptAdapter::Sabnzbd, Some(1)),
        Warned
    );
    assert_eq!(
        adapter_disposition_for_test(ScriptAdapter::Sabnzbd, None),
        Warned
    );
    // NZBGet: 92 par-check request is acknowledged, 93 success, 94 error, 95 none.
    assert_eq!(
        adapter_disposition_for_test(ScriptAdapter::Nzbget, Some(92)),
        Succeeded
    );
    assert_eq!(
        adapter_disposition_for_test(ScriptAdapter::Nzbget, Some(93)),
        Succeeded
    );
    assert_eq!(
        adapter_disposition_for_test(ScriptAdapter::Nzbget, Some(94)),
        Failed
    );
    assert_eq!(
        adapter_disposition_for_test(ScriptAdapter::Nzbget, Some(95)),
        Skipped
    );
    assert_eq!(
        adapter_disposition_for_test(ScriptAdapter::Nzbget, Some(0)),
        Failed
    );
    assert_eq!(
        adapter_disposition_for_test(ScriptAdapter::Nzbget, None),
        Failed
    );
}

#[test]
fn captured_output_keeps_the_tail_within_the_persisted_cap() {
    let line = vec![b'x'; 1024];
    let count = (MAX_SCRIPT_OUTPUT_BYTES as usize / line.len()) + 8;
    let (output, truncated) = bounded_output_for_test(vec![line.clone(); count]);
    assert!(truncated, "an over-cap script must report truncation");
    assert!(output.len() as u64 <= MAX_SCRIPT_OUTPUT_BYTES);

    let (output, truncated) = bounded_output_for_test(vec![b"short\n".to_vec()]);
    assert!(!truncated);
    assert_eq!(output, b"short\n");
}

#[test]
fn secrets_are_removed_from_captured_output() {
    let secrets = vec![b"hunter2".to_vec()];
    let redacted = redact_bytes_for_test(b"token=hunter2 and hunter2 again", &secrets);
    assert_eq!(
        String::from_utf8(redacted).unwrap(),
        "token=[REDACTED] and [REDACTED] again"
    );
    // An empty secret must not turn every byte boundary into a redaction.
    let untouched = redact_bytes_for_test(b"plain", &[Vec::new()]);
    assert_eq!(untouched, b"plain");
}
