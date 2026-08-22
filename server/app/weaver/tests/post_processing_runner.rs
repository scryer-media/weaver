//! End-to-end post-processing: real scripts, the real supervisor, the real executor.
//!
//! A test harness cannot serve as its own process supervisor, so every request
//! points `supervisor_executable` at the built `weaver` binary — the same binary
//! that re-executes itself in production.

#![cfg(unix)]

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use weaver_server_core::persistence::Database;
use weaver_server_core::post_processing::executor::PostProcessingExecutor;
use weaver_server_core::post_processing::listing::resolve_script;
use weaver_server_core::post_processing::model::{
    OptionName, OptionValue, PipelineOutcome, PostProcessingSettings, PostProcessingSummary,
    ResolvedOption, ScriptAdapter, ScriptList, ScriptListEntry, ScriptLists, ScriptName,
    ScriptStatus, SecretOptionValue,
};
use weaver_server_core::post_processing::runner::{
    ExecutionDisposition, InterpreterConfig, JobExecutionContext, MAX_SCRIPT_OUTPUT_BYTES,
    ScriptExecutionRequest, execute_script,
};

fn supervisor() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_weaver"))
}

fn write_script(data_dir: &Path, name: &str, body: &str) -> ScriptName {
    write_script_in(&data_dir.join("scripts"), name, body)
}

fn write_script_in(scripts: &Path, name: &str, body: &str) -> ScriptName {
    fs::create_dir_all(scripts).unwrap();
    let path = scripts.join(name);
    fs::write(&path, body).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
    ScriptName::new(name).unwrap()
}

fn context(job_id: u64, working_directory: PathBuf) -> JobExecutionContext {
    JobExecutionContext {
        job_id,
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
    }
}

fn request(
    data_dir: &Path,
    script: &ScriptName,
    working_directory: PathBuf,
    timeout: Option<Duration>,
) -> ScriptExecutionRequest {
    let discovered = resolve_script(&data_dir.join("scripts"), script).unwrap();
    ScriptExecutionRequest {
        manifest: discovered.manifest,
        root: discovered.root,
        options: vec![ResolvedOption::new(
            OptionName::new("ApiToken").unwrap(),
            OptionValue::Secret(SecretOptionValue::from_admin_input("super-secret-value")),
        )],
        context: context(42, working_directory),
        timeout,
        termination_grace: Duration::from_millis(100),
        interpreters: InterpreterConfig::default(),
        supervisor_executable: Some(supervisor()),
    }
}

fn executor(db: &Database, data_dir: &Path) -> PostProcessingExecutor {
    PostProcessingExecutor::new(db.clone(), data_dir.join("scripts"), 1)
        .with_supervisor_executable(supervisor())
}

fn enable_execution(db: &Database) {
    db.save_post_processing_settings(&PostProcessingSettings {
        execution_enabled: true,
        termination_grace_seconds: 1,
        ..PostProcessingSettings::default()
    })
    .unwrap();
}

fn set_global_list(db: &Database, entries: Vec<ScriptListEntry>) {
    db.save_post_processing_script_lists(&ScriptLists {
        global: ScriptList::new(entries).unwrap(),
        ..ScriptLists::default()
    })
    .unwrap();
}

#[tokio::test]
async fn the_same_binary_supervisor_delivers_a_clean_sab_environment_and_redacts_secrets() {
    let data = tempfile::tempdir().unwrap();
    let working_directory = data.path().join("work dir ✓");
    fs::create_dir_all(&working_directory).unwrap();
    let script = write_script(
        data.path(),
        "sab script ✓.sh",
        r#"#!/bin/sh
printf 'NAME=%s\n' "$SAB_FINAL_NAME"
printf 'CAT=%s\n' "$SAB_CAT"
printf 'ARG1=%s\n' "$1"
printf 'TOKEN=%s\n' "$SAB_OPTION_APITOKEN"
printf 'CARGO=%s\n' "${CARGO-unset}"
printf 'stderr-line\n' >&2
"#,
    );

    let result = execute_script(
        request(
            data.path(),
            &script,
            working_directory.clone(),
            Some(Duration::from_secs(30)),
        ),
        None,
    )
    .await
    .unwrap();

    assert_eq!(result.disposition, ExecutionDisposition::Succeeded);
    assert_eq!(result.exit_code, Some(0));
    let output = String::from_utf8(result.output).unwrap();
    assert!(output.contains("NAME=Unicode job ✓"), "{output}");
    assert!(output.contains("CAT=movies"), "{output}");
    assert!(
        output.contains(&format!("ARG1={}", working_directory.display())),
        "{output}"
    );
    // The environment is rebuilt from scratch, so the test runner's own
    // variables cannot leak into a script.
    assert!(output.contains("CARGO=unset"), "{output}");
    assert!(output.contains("stderr-line"), "{output}");
    // A secret reaches the script but never the captured output.
    assert!(output.contains("TOKEN=[REDACTED]"), "{output}");
    assert!(!output.contains("super-secret-value"), "{output}");
}

#[tokio::test]
async fn nzbget_exit_codes_are_honoured_end_to_end() {
    let data = tempfile::tempdir().unwrap();
    let working_directory = data.path().join("work");
    fs::create_dir_all(&working_directory).unwrap();

    for (exit, expected) in [
        (93, ExecutionDisposition::Succeeded),
        (94, ExecutionDisposition::Failed),
        (95, ExecutionDisposition::Skipped),
    ] {
        let script = write_script(
            data.path(),
            &format!("nzbget-{exit}.sh"),
            &format!(
                "#!/bin/sh\n### NZBGET POST-PROCESSING SCRIPT ###\nprintf 'NZBID=%s\\n' \"$NZBPP_NZBID\"\nexit {exit}\n"
            ),
        );
        let result = execute_script(
            request(
                data.path(),
                &script,
                working_directory.clone(),
                Some(Duration::from_secs(30)),
            ),
            None,
        )
        .await
        .unwrap();
        assert_eq!(result.disposition, expected, "exit {exit}");
        assert_eq!(result.exit_code, Some(exit));
        assert!(
            String::from_utf8(result.output)
                .unwrap()
                .contains("NZBID=42")
        );
    }
}

#[tokio::test]
async fn a_script_that_outlives_its_timeout_is_killed_after_the_grace_period() {
    let data = tempfile::tempdir().unwrap();
    let working_directory = data.path().join("work");
    fs::create_dir_all(&working_directory).unwrap();
    let script = write_script(
        data.path(),
        "sleeper.sh",
        "#!/bin/sh\ntrap '' TERM\nprintf 'started\\n'\nsleep 120\n",
    );

    let started = Instant::now();
    let result = execute_script(
        request(
            data.path(),
            &script,
            working_directory,
            Some(Duration::from_millis(200)),
        ),
        None,
    )
    .await
    .unwrap();

    assert_eq!(result.disposition, ExecutionDisposition::TimedOut);
    assert!(
        result.error_message.as_deref() == Some("post-processing script timed out"),
        "{:?}",
        result.error_message
    );
    // A script that ignores SIGTERM is still gone once the grace period expires.
    assert!(
        started.elapsed() < Duration::from_secs(30),
        "the grace kill did not fire"
    );
}

#[tokio::test]
async fn output_beyond_the_cap_keeps_the_tail_and_reports_truncation() {
    let data = tempfile::tempdir().unwrap();
    let working_directory = data.path().join("work");
    fs::create_dir_all(&working_directory).unwrap();
    let script = write_script(
        data.path(),
        "chatty.sh",
        r#"#!/bin/sh
i=0
payload=$(printf 'x%.0s' $(seq 1 1024))
while [ "$i" -lt 400 ]; do
  printf 'line-%s %s\n' "$i" "$payload"
  i=$((i + 1))
done
printf 'FINAL-LINE\n'
"#,
    );

    let result = execute_script(
        request(
            data.path(),
            &script,
            working_directory,
            Some(Duration::from_secs(60)),
        ),
        None,
    )
    .await
    .unwrap();

    assert_eq!(result.disposition, ExecutionDisposition::Succeeded);
    assert!(result.output_truncated);
    assert!(result.output.len() as u64 <= MAX_SCRIPT_OUTPUT_BYTES);
    let output = String::from_utf8_lossy(&result.output);
    assert!(output.contains("FINAL-LINE"), "the tail must survive");
    assert!(!output.contains("line-0 "), "the head is what gets dropped");
}

#[tokio::test]
async fn the_executor_runs_a_list_in_order_and_rolls_the_worst_outcome_up() {
    let data = tempfile::tempdir().unwrap();
    let working_directory = data.path().join("work");
    fs::create_dir_all(&working_directory).unwrap();
    let db = Database::open_in_memory().unwrap();
    enable_execution(&db);

    let first = write_script(
        data.path(),
        "first.sh",
        "#!/bin/sh\nprintf 'first\\n' >> \"$SAB_COMPLETE_DIR/order.txt\"\n",
    );
    let second = write_script(
        data.path(),
        "second.sh",
        "#!/bin/sh\nprintf 'second\\n' >> \"$SAB_COMPLETE_DIR/order.txt\"\nexit 7\n",
    );
    let third = write_script(
        data.path(),
        "third.sh",
        "#!/bin/sh\nprintf 'third\\n' >> \"$SAB_COMPLETE_DIR/order.txt\"\n",
    );
    let list = ScriptList::new(vec![
        ScriptListEntry::new(first),
        ScriptListEntry::new(second),
        ScriptListEntry::new(third.clone()),
    ])
    .unwrap();

    let report = executor(&db, data.path())
        .execute_job(
            101,
            list,
            context(101, working_directory.clone()),
            None,
            None,
        )
        .await
        .unwrap();

    assert_eq!(
        fs::read_to_string(working_directory.join("order.txt")).unwrap(),
        "first\nsecond\nthird\n",
        "scripts run sequentially in list order"
    );
    // A nonzero SABnzbd exit is a warning, and one warning degrades the rollup
    // without stopping the rest of the list.
    assert_eq!(report.summary, PostProcessingSummary::Warning);
    assert_eq!(report.results.len(), 3);
    assert_eq!(report.results[0].status, ScriptStatus::Succeeded);
    assert_eq!(report.results[1].status, ScriptStatus::Warning);
    assert_eq!(report.results[1].exit_code, Some(7));
    assert_eq!(report.results[2].status, ScriptStatus::Succeeded);
    assert_eq!(report.results[2].adapter, ScriptAdapter::Sabnzbd);
    assert_eq!(report.results[2].script, third);
}

#[tokio::test]
async fn a_disabled_script_is_kept_in_the_list_but_never_executed() {
    let data = tempfile::tempdir().unwrap();
    let working_directory = data.path().join("work");
    fs::create_dir_all(&working_directory).unwrap();
    let db = Database::open_in_memory().unwrap();
    enable_execution(&db);

    let skipped = write_script(
        data.path(),
        "skipped.sh",
        "#!/bin/sh\nprintf 'ran\\n' > \"$SAB_COMPLETE_DIR/skipped.txt\"\n",
    );
    let list = ScriptList::new(vec![ScriptListEntry {
        script: skipped,
        enabled: false,
        timeout_seconds: None,
    }])
    .unwrap();

    let report = executor(&db, data.path())
        .execute_job(
            102,
            list,
            context(102, working_directory.clone()),
            None,
            None,
        )
        .await
        .unwrap();

    assert_eq!(report.summary, PostProcessingSummary::NotRun);
    assert!(report.results.is_empty());
    assert!(!working_directory.join("skipped.txt").exists());
}

#[tokio::test]
async fn execution_is_refused_while_the_master_switch_is_off() {
    let data = tempfile::tempdir().unwrap();
    let working_directory = data.path().join("work");
    fs::create_dir_all(&working_directory).unwrap();
    let db = Database::open_in_memory().unwrap();
    // Settings are left at their defaults, which is off.
    let script = write_script(
        data.path(),
        "never.sh",
        "#!/bin/sh\nprintf 'ran\\n' > \"$SAB_COMPLETE_DIR/never.txt\"\n",
    );
    let list = ScriptList::new(vec![ScriptListEntry::new(script)]).unwrap();

    let report = executor(&db, data.path())
        .execute_job(
            103,
            list,
            context(103, working_directory.clone()),
            None,
            None,
        )
        .await
        .unwrap();

    assert_eq!(report.summary, PostProcessingSummary::NotRun);
    assert!(!working_directory.join("never.txt").exists());
}

#[tokio::test]
async fn cancelling_a_job_stops_the_run_and_records_the_cancellation() {
    let data = tempfile::tempdir().unwrap();
    let working_directory = data.path().join("work");
    fs::create_dir_all(&working_directory).unwrap();
    let db = Database::open_in_memory().unwrap();
    enable_execution(&db);

    let slow = write_script(data.path(), "slow.sh", "#!/bin/sh\nsleep 120\n");
    let later = write_script(
        data.path(),
        "later.sh",
        "#!/bin/sh\nprintf 'ran\\n' > \"$SAB_COMPLETE_DIR/later.txt\"\n",
    );
    let list = ScriptList::new(vec![
        ScriptListEntry::new(slow),
        ScriptListEntry::new(later),
    ])
    .unwrap();

    let executor = executor(&db, data.path());
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let run = {
        let executor = executor.clone();
        let context = context(104, working_directory.clone());
        tokio::spawn(async move {
            executor
                .execute_job(104, list, context, None, Some(started_tx))
                .await
        })
    };
    started_rx.await.unwrap();
    // The cancel registration is installed before the first script starts, but
    // the script itself needs a moment to be spawned.
    for _ in 0..200 {
        if executor.cancel_job(104) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let report = run.await.unwrap().unwrap();
    assert_eq!(report.summary, PostProcessingSummary::Cancelled);
    assert_eq!(
        report.results.len(),
        1,
        "the list stops at the cancellation"
    );
    assert_eq!(report.results[0].status, ScriptStatus::Cancelled);
    assert!(!working_directory.join("later.txt").exists());
}

#[tokio::test]
async fn a_missing_script_warns_instead_of_failing_the_job() {
    let data = tempfile::tempdir().unwrap();
    let working_directory = data.path().join("work");
    fs::create_dir_all(&working_directory).unwrap();
    fs::create_dir_all(data.path().join("scripts")).unwrap();
    let db = Database::open_in_memory().unwrap();
    enable_execution(&db);

    let list = ScriptList::new(vec![ScriptListEntry::new(
        ScriptName::new("renamed.sh").unwrap(),
    )])
    .unwrap();
    let report = executor(&db, data.path())
        .execute_job(105, list, context(105, working_directory), None, None)
        .await
        .unwrap();

    // Renaming a script must not start failing jobs: that is the failure class
    // the approval model manufactured.
    assert_eq!(report.summary, PostProcessingSummary::Warning);
    assert_eq!(report.results[0].status, ScriptStatus::Warning);
    assert!(
        report.results[0]
            .error_message
            .as_deref()
            .unwrap_or_default()
            .contains("renamed.sh")
    );
}

#[tokio::test]
async fn results_are_persisted_on_the_job_and_a_rerun_replaces_them() {
    let data = tempfile::tempdir().unwrap();
    let working_directory = data.path().join("work");
    fs::create_dir_all(&working_directory).unwrap();
    let db = Database::open_in_memory().unwrap();
    enable_execution(&db);
    db.insert_job_history(&weaver_server_core::JobHistoryRow {
        job_id: 106,
        job_hash: None,
        name: "rerun".into(),
        status: "complete".into(),
        error_message: None,
        total_bytes: 1,
        downloaded_bytes: 1,
        optional_recovery_bytes: 0,
        optional_recovery_downloaded_bytes: 0,
        failed_bytes: 0,
        health: 1_000,
        category: None,
        output_dir: Some(working_directory.to_string_lossy().into_owned()),
        nzb_path: None,
        created_at: 1,
        completed_at: 2,
        metadata: None,
    })
    .unwrap();

    let script = write_script(
        data.path(),
        "counter.sh",
        "#!/bin/sh\nprintf 'x' >> \"$SAB_COMPLETE_DIR/runs.txt\"\n",
    );
    let list = ScriptList::new(vec![ScriptListEntry::new(script.clone())]).unwrap();
    set_global_list(&db, vec![ScriptListEntry::new(script)]);
    let executor = executor(&db, data.path());

    executor
        .execute_job(
            106,
            list.clone(),
            context(106, working_directory.clone()),
            None,
            None,
        )
        .await
        .unwrap();
    let stored = db.job_post_processing_results(106).unwrap();
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].status, ScriptStatus::Succeeded);

    // A rerun executes the job's list again against the retained output.
    let resolved = executor.resolve_job_scripts(None, &[]).unwrap();
    assert_eq!(resolved.entries().len(), 1);
    executor
        .execute_job(
            106,
            resolved,
            context(106, working_directory.clone()),
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        fs::read_to_string(working_directory.join("runs.txt")).unwrap(),
        "xx",
        "the rerun really re-executed the script"
    );
    let stored = db.job_post_processing_results(106).unwrap();
    assert_eq!(stored.len(), 1, "results describe the latest pass");
    assert!(stored[0].finished_at_epoch_ms > 0);
}

#[tokio::test]
async fn stored_options_are_validated_against_the_manifest_and_delivered_as_nzbpo() {
    let data = tempfile::tempdir().unwrap();
    let working_directory = data.path().join("work");
    fs::create_dir_all(&working_directory).unwrap();
    let db = Database::open_in_memory().unwrap();
    enable_execution(&db);

    let package = data.path().join("scripts/email");
    fs::create_dir_all(&package).unwrap();
    fs::write(
        package.join("manifest.json"),
        serde_json::json!({
            "main": "run.sh",
            "name": "email",
            "kind": "POST-PROCESSING",
            "displayName": "Email",
            "version": "1.0.0",
            "author": "Author",
            "homepage": "https://example.invalid",
            "license": "GNU",
            "about": "About",
            "description": [],
            "requirements": [],
            "queueEvents": "",
            "taskTime": "",
            "sections": [],
            "commands": [],
            "options": [
                {
                    "name": "Host",
                    "displayName": "Host",
                    "value": "mail.example.invalid",
                    "description": [],
                    "select": []
                },
                {
                    "name": "Token",
                    "displayName": "Token",
                    "value": "",
                    "description": [],
                    "select": [],
                    "secret": true
                }
            ]
        })
        .to_string(),
    )
    .unwrap();
    fs::write(
        package.join("run.sh"),
        "#!/bin/sh\nprintf 'HOST=%s TOKEN=%s\\n' \"$NZBPO_Host\" \"$NZBPO_Token\" > \"$NZBPP_DIRECTORY/env.txt\"\nexit 93\n",
    )
    .unwrap();
    fs::set_permissions(package.join("run.sh"), fs::Permissions::from_mode(0o755)).unwrap();

    let script = ScriptName::new("email").unwrap();
    db.save_post_processing_script_options(
        &script,
        &[ResolvedOption::new(
            OptionName::new("Token").unwrap(),
            OptionValue::Secret(SecretOptionValue::from_admin_input("hunter2")),
        )],
    )
    .unwrap();

    let list = ScriptList::new(vec![ScriptListEntry::new(script)]).unwrap();
    let report = executor(&db, data.path())
        .execute_job(
            107,
            list,
            context(107, working_directory.clone()),
            None,
            None,
        )
        .await
        .unwrap();

    assert_eq!(report.summary, PostProcessingSummary::Succeeded);
    let env = fs::read_to_string(working_directory.join("env.txt")).unwrap();
    // The manifest default fills in, and the stored secret is decrypted for the
    // process only.
    assert_eq!(env.trim(), "HOST=mail.example.invalid TOKEN=hunter2");
}

#[tokio::test]
async fn a_script_outside_its_package_root_is_refused() {
    let data = tempfile::tempdir().unwrap();
    let working_directory = data.path().join("work");
    fs::create_dir_all(&working_directory).unwrap();
    let package = data.path().join("scripts/escape");
    fs::create_dir_all(&package).unwrap();
    fs::write(
        package.join("manifest.json"),
        serde_json::json!({
            "main": "run.sh",
            "name": "escape",
            "kind": "POST-PROCESSING",
            "displayName": "Escape",
            "version": "1.0.0",
            "author": "Author",
            "homepage": "https://example.invalid",
            "license": "GNU",
            "about": "About",
            "description": [],
            "requirements": [],
            "queueEvents": "",
            "taskTime": "",
            "sections": [],
            "commands": [],
            "options": []
        })
        .to_string(),
    )
    .unwrap();
    let outside = data.path().join("outside.sh");
    fs::write(&outside, "#!/bin/sh\nprintf 'escaped\\n'\n").unwrap();
    fs::set_permissions(&outside, fs::Permissions::from_mode(0o755)).unwrap();
    std::os::unix::fs::symlink(&outside, package.join("run.sh")).unwrap();

    let error = execute_script(
        request(
            data.path(),
            &ScriptName::new("escape").unwrap(),
            working_directory,
            Some(Duration::from_secs(10)),
        ),
        None,
    )
    .await;
    assert!(error.is_err(), "a symlinked entrypoint must not execute");
}

#[tokio::test]
async fn changing_the_scripts_directory_pins_admitted_work_and_updates_future_jobs() {
    let data = tempfile::tempdir().unwrap();
    let old_root = data.path().join("scripts");
    let new_root = data.path().join("replacement-scripts");
    let first = write_script_in(
        &old_root,
        "first.sh",
        "#!/bin/sh\nprintf 'started\\n' > \"$SAB_COMPLETE_DIR/started\"\nwhile [ ! -e \"$SAB_COMPLETE_DIR/release\" ]; do sleep 0.01; done\nprintf 'first\\n' >> \"$SAB_COMPLETE_DIR/order.txt\"\n",
    );
    let second = write_script_in(
        &old_root,
        "second.sh",
        "#!/bin/sh\nprintf 'old-second\\n' >> \"$SAB_COMPLETE_DIR/order.txt\"\n",
    );
    write_script_in(
        &new_root,
        "second.sh",
        "#!/bin/sh\nprintf 'new-second\\n' >> \"$SAB_COMPLETE_DIR/order.txt\"\n",
    );

    let first_working_directory = data.path().join("first-work");
    fs::create_dir_all(&first_working_directory).unwrap();
    let db = Database::open_in_memory().unwrap();
    enable_execution(&db);
    let executor = executor(&db, data.path());
    let admitted = executor.clone();
    let first_context = context(701, first_working_directory.clone());
    let admitted_list = ScriptList::new(vec![
        ScriptListEntry::new(first),
        ScriptListEntry::new(second.clone()),
    ])
    .unwrap();

    let admitted_root = executor.script_directory();
    executor.set_script_directory(new_root);
    let running = tokio::spawn(async move {
        admitted
            .execute_job_at_script_directory(
                admitted_root,
                701,
                admitted_list,
                first_context,
                None,
                None,
            )
            .await
            .unwrap()
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while !first_working_directory.join("started").exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the first script should begin");

    fs::write(first_working_directory.join("release"), "").unwrap();
    running.await.unwrap();
    assert_eq!(
        fs::read_to_string(first_working_directory.join("order.txt")).unwrap(),
        "first\nold-second\n",
        "the already admitted list must continue resolving from its original root"
    );

    let second_working_directory = data.path().join("second-work");
    fs::create_dir_all(&second_working_directory).unwrap();
    executor
        .execute_job(
            702,
            ScriptList::new(vec![ScriptListEntry::new(second)]).unwrap(),
            context(702, second_working_directory.clone()),
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        fs::read_to_string(second_working_directory.join("order.txt")).unwrap(),
        "new-second\n",
        "a later job resolves scripts from the replacement root"
    );
}
