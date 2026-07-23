#![cfg(unix)]

use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use weaver_server_core::post_processing::discovery::{DiscoveryOptions, discover_extensions};
use weaver_server_core::post_processing::model::{
    NonZeroTimeoutSeconds, OptionName, PipelineOutcome, ResolvedOption, ResolvedOptionValue,
    SecretOptionValue, TimeoutPolicy,
};
use weaver_server_core::post_processing::runner::{
    ExecutionDisposition, ExtensionExecutionRequest, InterpreterConfig, JobExecutionContext,
    RunnerError, execute_extension,
};

fn native_package(
    data_dir: &Path,
    package_name: &str,
    entrypoint: &str,
    script: &str,
) -> (
    weaver_server_core::post_processing::model::ExtensionManifest,
    PathBuf,
) {
    let package = data_dir.join("scripts").join(package_name);
    fs::create_dir_all(&package).unwrap();
    fs::write(
        package.join("weaver-extension.json"),
        serde_json::json!({
            "schema_version": 1,
            "kind": "native",
            "id": format!("test.{package_name}"),
            "name": package_name,
            "version": "1.0.0",
            "entrypoint": entrypoint,
            "commands": [],
            "options": []
        })
        .to_string(),
    )
    .unwrap();
    fs::write(package.join(entrypoint), script).unwrap();
    let mut discovered = discover_extensions(
        data_dir,
        DiscoveryOptions {
            enabled: true,
            bare_script_adapter: None,
        },
    )
    .unwrap();
    let extension = discovered
        .drain(..)
        .find(|extension| extension.source_path == package)
        .expect("native package should be discovered");
    (extension.manifest, package)
}

fn request(
    manifest: weaver_server_core::post_processing::model::ExtensionManifest,
    managed_path: PathBuf,
    working_directory: PathBuf,
    timeout_policy: TimeoutPolicy,
) -> ExtensionExecutionRequest {
    ExtensionExecutionRequest {
        attempt_id: "integration-attempt".into(),
        manifest,
        managed_path,
        options: vec![ResolvedOption::new(
            OptionName::new("ApiToken").unwrap(),
            ResolvedOptionValue::Secret(SecretOptionValue::from_admin_input("super-secret-value")),
        )],
        approved_roots: vec![],
        context: JobExecutionContext {
            job_id: 42,
            name: "Unicode job ✓".into(),
            nzb_filename: "input file.nzb".into(),
            category: Some("movies".into()),
            group: None,
            source_url: None,
            working_directory: working_directory.clone(),
            final_directory: working_directory,
            pipeline_outcome: PipelineOutcome::Succeeded,
            par_status: 2,
            unpack_status: 2,
            compatibility: Default::default(),
        },
        timeout_policy,
        termination_grace: Duration::from_millis(100),
        interpreters: InterpreterConfig::default(),
        control_token: Some("control-token-value".into()),
        diagnostic_command: None,
        supervisor_executable: Some(PathBuf::from(env!("CARGO_BIN_EXE_weaver"))),
    }
}

#[tokio::test]
async fn same_binary_supervisor_handles_spaces_unicode_clean_env_and_redaction() {
    let data = tempfile::tempdir().unwrap();
    let working_directory = data.path().join("work dir ✓");
    fs::create_dir_all(&working_directory).unwrap();
    let script = r#"#!/bin/sh
printf '%s\n' "$WEAVER_PP_CONTEXT"
printf '%s\n' "$WEAVER_PP_CONTROL_TOKEN"
printf 'CARGO=%s\n' "${CARGO-unset}"
printf '%s\n' '[WEAVER] {"type":"progress","percent":42}'
printf 'stderr-line\n' >&2
"#;
    let (manifest, package) =
        native_package(data.path(), "native-unicode", "run script ✓.sh", script);
    let result = execute_extension(
        request(
            manifest,
            package,
            working_directory,
            TimeoutPolicy::Finite(NonZeroTimeoutSeconds::new(10).unwrap()),
        ),
        None,
    )
    .await
    .unwrap();

    assert_eq!(result.disposition, ExecutionDisposition::Succeeded);
    let output = result
        .output
        .iter()
        .flat_map(|line| line.bytes.iter().copied())
        .collect::<Vec<_>>();
    let output = String::from_utf8_lossy(&output);
    assert!(output.contains("[REDACTED]"));
    assert!(!output.contains("super-secret-value"));
    assert!(!output.contains("control-token-value"));
    assert!(output.contains("CARGO=unset"));
    assert!(output.contains("stderr-line"));
    assert_eq!(result.effects.progress.as_ref().unwrap()["percent"], 42);
}

#[tokio::test]
async fn execution_rejects_a_managed_package_changed_after_approval() {
    let data = tempfile::tempdir().unwrap();
    let working_directory = data.path().join("tamper-work");
    fs::create_dir_all(&working_directory).unwrap();
    let (manifest, package) = native_package(
        data.path(),
        "native-tamper",
        "run.sh",
        "#!/bin/sh\nexit 0\n",
    );
    fs::write(package.join("run.sh"), "#!/bin/sh\nexit 42\n").unwrap();

    let result = execute_extension(
        request(
            manifest,
            package,
            working_directory,
            TimeoutPolicy::Finite(NonZeroTimeoutSeconds::new(10).unwrap()),
        ),
        None,
    )
    .await;

    assert!(matches!(result, Err(RunnerError::UntrustedPackage)));
}

#[tokio::test]
async fn timeout_kills_the_supervisor_process_group_and_descendants() {
    let data = tempfile::tempdir().unwrap();
    let working_directory = data.path().join("timeout-work");
    fs::create_dir_all(&working_directory).unwrap();
    let marker = data.path().join("descendant-survived");
    let script = format!(
        "#!/bin/sh\n(sleep 2; printf survived > '{}') &\nsleep 30\n",
        marker.display()
    );
    let (manifest, package) = native_package(data.path(), "native-timeout", "timeout.sh", &script);
    let result = execute_extension(
        request(
            manifest,
            package,
            working_directory,
            TimeoutPolicy::Finite(NonZeroTimeoutSeconds::new(1).unwrap()),
        ),
        None,
    )
    .await
    .unwrap();

    assert_eq!(result.disposition, ExecutionDisposition::TimedOut);
    tokio::time::sleep(Duration::from_millis(2_500)).await;
    assert!(
        !marker.exists(),
        "a timed-out extension descendant escaped the supervisor process group"
    );
}

#[test]
fn parent_pipe_loss_helper() {
    let Some(data_path) = std::env::var_os("WEAVER_PARENT_PIPE_TEST_DIR") else {
        return;
    };
    let data_path = PathBuf::from(data_path);
    let working_directory = data_path.join("parent-pipe-work");
    fs::create_dir_all(&working_directory).unwrap();
    let pid_marker = data_path.join("extension-started");
    let escaped_marker = data_path.join("extension-descendant-survived");
    let script = format!(
        "#!/bin/sh\nprintf '%s' \"$$\" > '{}'\n(sleep 2; printf survived > '{}') &\nsleep 30\n",
        pid_marker.display(),
        escaped_marker.display()
    );
    let (manifest, package) =
        native_package(&data_path, "native-parent-pipe", "parent-pipe.sh", &script);
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let _ = execute_extension(
            request(
                manifest,
                package,
                working_directory,
                TimeoutPolicy::Unlimited,
            ),
            None,
        )
        .await;
    });
}

#[test]
fn parent_pipe_loss_kills_a_silent_extension_process_tree() {
    let data = tempfile::tempdir().unwrap();
    let mut parent = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("parent_pipe_loss_helper")
        .arg("--nocapture")
        .env("WEAVER_PARENT_PIPE_TEST_DIR", data.path())
        .spawn()
        .unwrap();
    let started = data.path().join("extension-started");
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !started.exists() && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(25));
    }
    assert!(
        started.exists(),
        "extension never reached its silent wait state"
    );

    parent.kill().unwrap();
    let _ = parent.wait().unwrap();
    std::thread::sleep(Duration::from_millis(2_500));
    assert!(
        !data.path().join("extension-descendant-survived").exists(),
        "a silent extension tree survived loss of its Weaver parent pipe"
    );
}
