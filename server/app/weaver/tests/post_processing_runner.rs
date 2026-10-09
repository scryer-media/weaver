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
use weaver_server_core::post_processing::instances::{
    InstanceTrigger, ScriptInstance, ScriptInstanceDraft,
};
use weaver_server_core::post_processing::listing::resolve_script;
use weaver_server_core::post_processing::model::{
    OptionName, OptionValue, PipelineOutcome, PostProcessingSettings, PostProcessingSummary,
    ResolvedOption, ScriptAdapter, ScriptName, ScriptStatus, SecretOptionValue,
};
use weaver_server_core::post_processing::runner::{
    ExecutionDisposition, InterpreterConfig, JobExecutionContext, MAX_SCRIPT_OUTPUT_BYTES,
    RunIdentity, ScriptExecutionRequest, execute_script,
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

/// A script timeout no test run can reach, for tests that are not about it.
const OUT_OF_REACH: Duration = Duration::from_secs(3600);

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
        identity: RunIdentity {
            run_id: "run-1".into(),
            instance_id: "instance-1".into(),
            instance_name: script.as_str().into(),
            trigger: InstanceTrigger::PostProcessing.to_string(),
            api_url: None,
            token: None,
            output: Default::default(),
        },
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

/// A post-processing instance of `script`, not yet saved.
fn draft(script: &ScriptName) -> ScriptInstanceDraft {
    ScriptInstanceDraft::new(script.clone(), InstanceTrigger::PostProcessing)
}

/// A saved post-processing instance of `script` that the pass waits for.
fn instance(db: &Database, script: &ScriptName) -> ScriptInstance {
    db.create_script_instance(draft(script)).unwrap()
}

/// A saved post-processing instance of `script` that nothing waits for.
fn not_waited_for(db: &Database, script: &ScriptName) -> ScriptInstance {
    db.create_script_instance(draft(script).fire_and_forget())
        .unwrap()
}

/// A pipe a test script stops at, so the test decides when the script goes on.
fn gate(directory: &Path, name: &str) -> PathBuf {
    let path = directory.join(name);
    let made = std::process::Command::new("mkfifo")
        .arg(&path)
        .status()
        .unwrap();
    assert!(made.success());
    path
}

/// Lets the script stopped at `gate` go on. Returns once the script is there
/// to be let through, however long it takes to arrive.
async fn open_gate(gate: PathBuf) {
    tokio::task::spawn_blocking(move || fs::write(gate, "go\n").unwrap())
        .await
        .unwrap();
}

/// Returns once the script has written to `gate`, which it does when it starts.
async fn wait_at_gate(gate: PathBuf) {
    tokio::task::spawn_blocking(move || fs::read(gate).unwrap())
        .await
        .unwrap();
}

/// A finished job for script runs to be recorded against.
fn finished_job(db: &Database, job_id: u64, working_directory: &Path) {
    db.insert_job_history(&weaver_server_core::JobHistoryRow {
        job_id,
        job_hash: None,
        name: "finished".into(),
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
        server_attribution: None,
    })
    .unwrap();
}

#[tokio::test]
async fn an_instance_runs_its_script_whatever_the_script_says_it_is_for() {
    use weaver_server_core::post_processing::listing::list_scripts;
    use weaver_server_core::post_processing::model::{ScriptEventLabel, ScriptKind};

    let data = tempfile::tempdir().unwrap();
    let working = data.path().join("work");
    fs::create_dir(&working).unwrap();
    let db = Database::open_in_memory().unwrap();
    enable_execution(&db);
    let mut instances = Vec::new();
    for kind in [
        ScriptKind::Queue,
        ScriptKind::Scan,
        ScriptKind::Scheduler,
        ScriptKind::Feed,
    ] {
        let name = format!("{}.sh", kind.as_str());
        let script = write_script(
            data.path(),
            &name,
            &format!(
                "#!/bin/sh\n### NZBGET {} SCRIPT ###\nprintf '{}\\n' >> ran.txt\nexit 93\n",
                kind.as_str(),
                kind.as_str()
            ),
        );
        instances.push(instance(&db, &script));
    }
    let listing = list_scripts(&data.path().join("scripts")).unwrap();
    assert_eq!(listing.scripts.len(), 4);
    assert!(listing.problems.is_empty());
    let report = executor(&db, data.path())
        .execute_job(901, instances, context(901, working.clone()), None, None)
        .await
        .unwrap();
    assert_eq!(report.summary, PostProcessingSummary::Succeeded);
    assert_eq!(report.results.len(), 4);
    assert!(
        report
            .results
            .iter()
            .all(|result| result.status == ScriptStatus::Succeeded
                && result.exit_code == Some(93)
                && result.event == ScriptEventLabel::PostProcessing)
    );
    assert_eq!(
        fs::read_to_string(working.join("ran.txt"))
            .unwrap()
            .lines()
            .count(),
        4
    );
}

#[tokio::test]
async fn a_script_is_told_how_the_scripts_before_it_ended() {
    let data = tempfile::tempdir().unwrap();
    let working = data.path().join("work");
    fs::create_dir(&working).unwrap();
    let db = Database::open_in_memory().unwrap();
    enable_execution(&db);
    let script = write_script(
        data.path(),
        "combined.sh",
        "#!/bin/sh\n### NZBGET POST-PROCESSING/QUEUE SCRIPT ###\nprintf '%s/%s' \"$NZBPP_TOTALSTATUS\" \"$NZBPP_SCRIPTSTATUS\"\nexit 93\n",
    );
    let failing = write_script(
        data.path(),
        "failing.sh",
        "#!/bin/sh\n### NZBGET POST-PROCESSING SCRIPT ###\nexit 94\n",
    );
    // The same script may be wired more than once.
    let instances = vec![
        instance(&db, &script),
        instance(&db, &failing),
        db.create_script_instance(draft(&script).named("Again"))
            .unwrap(),
    ];
    let report = executor(&db, data.path())
        .execute_job(902, instances, context(902, working), None, None)
        .await
        .unwrap();
    assert_eq!(report.summary, PostProcessingSummary::Failed);
    assert_eq!(report.results[0].status, ScriptStatus::Succeeded);
    assert_eq!(report.results[0].output_tail, "SUCCESS/NONE");
    assert_eq!(report.results[1].status, ScriptStatus::Failed);
    assert_eq!(report.results[2].status, ScriptStatus::Succeeded);
    assert_eq!(report.results[2].output_tail, "SUCCESS/FAILURE");
    assert_eq!(report.results[2].instance_name.as_deref(), Some("Again"));
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
            Some(OUT_OF_REACH),
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
async fn exit_codes_are_honoured_end_to_end() {
    let data = tempfile::tempdir().unwrap();
    let working_directory = data.path().join("work");
    fs::create_dir_all(&working_directory).unwrap();

    for (exit, expected) in [
        (0, ExecutionDisposition::Succeeded),
        (93, ExecutionDisposition::Succeeded),
        (94, ExecutionDisposition::Failed),
        (95, ExecutionDisposition::Skipped),
        (7, ExecutionDisposition::Failed),
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
                Some(OUT_OF_REACH),
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
async fn a_go_script_is_run_by_go_run_and_keeps_the_exit_code_it_chose() {
    let data = tempfile::tempdir().unwrap();
    let working_directory = data.path().join("work");
    fs::create_dir_all(&working_directory).unwrap();
    let scripts = data.path().join("scripts");
    fs::create_dir_all(&scripts).unwrap();
    // No executable bit: the extension alone makes it a script.
    fs::write(
        scripts.join("report.go"),
        "// ### NZBGET POST-PROCESSING SCRIPT ###\n\npackage main\n\nfunc main() {}\n",
    )
    .unwrap();
    let script = ScriptName::new("report.go").unwrap();
    // Stands in for the Go toolchain, which a machine running these tests need
    // not have. It reports how it was called and ends the way `go run` ends
    // when the program it built exits with 95.
    let go = write_script_in(
        &data.path().join("toolchain"),
        "go",
        r#"#!/bin/sh
printf 'ARGS='
for argument in "$@"; do printf '[%s]' "$argument"; done
printf '\n'
printf 'GOCACHE=%s\n' "$GOCACHE"
printf 'GOPROXY=%s\n' "$GOPROXY"
printf 'NZBID=%s\n' "$NZBPP_NZBID"
printf 'exit status 95\n' >&2
exit 1
"#,
    );

    let mut request = request(
        data.path(),
        &script,
        working_directory.clone(),
        Some(OUT_OF_REACH),
    );
    request.interpreters.go = Some(data.path().join("toolchain").join(go.as_str()));
    request.context.compatibility.data_dir = Some(data.path().into());
    let result = execute_script(request, None).await.unwrap();

    // `go run` itself exited 1; the status it reported for the script is the
    // one that counts.
    assert_eq!(result.disposition, ExecutionDisposition::Skipped);
    assert_eq!(result.exit_code, Some(95));
    let output = String::from_utf8(result.output).unwrap();
    // The script file follows `run`, then the arguments every script is given.
    assert!(
        output.contains(&format!(
            "ARGS=[run][{}][{}][input file.nzb][Unicode job ✓][][movies][][0][]\n",
            fs::canonicalize(scripts.join("report.go"))
                .unwrap()
                .display(),
            working_directory.display()
        )),
        "{output}"
    );
    assert!(
        output.contains(&format!(
            "GOCACHE={}\n",
            data.path().join(".weaver-go-cache").display()
        )),
        "{output}"
    );
    assert!(output.contains("GOPROXY=off\n"), "{output}");
    assert!(output.contains("NZBID=42\n"), "{output}");
}

#[tokio::test]
async fn a_script_that_outlives_its_timeout_is_killed_after_the_grace_period() {
    let data = tempfile::tempdir().unwrap();
    let working_directory = data.path().join("work");
    fs::create_dir_all(&working_directory).unwrap();
    let script = write_script(
        data.path(),
        "sleeper.sh",
        // It ignores SIGTERM and never exits on its own, so only the timeout
        // kill can end it; if that kill never fires, the runner bounds the hang.
        "#!/bin/sh\ntrap '' TERM\nprintf 'started\\n'\nwhile :; do sleep 3600; done\n",
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
    // Returning at all proves a script that ignores SIGTERM is gone: it never
    // exits on its own. It cannot be killed before its timeout.
    assert!(
        started.elapsed() >= Duration::from_millis(200),
        "killed before the timeout elapsed: {:?}",
        started.elapsed()
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
while [ "$i" -lt 2048 ]; do
  printf 'line-%s %s\n' "$i" "$payload"
  i=$((i + 1))
done
printf 'FINAL-LINE\n'
"#,
    );

    let result = execute_script(
        request(data.path(), &script, working_directory, Some(OUT_OF_REACH)),
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
    let written = (0..2048)
        .map(|i| format!("line-{i} ").len() as u64 + 1025)
        .sum::<u64>()
        + "FINAL-LINE\n".len() as u64;
    assert_eq!(
        result.output_bytes, written,
        "every byte written is counted, kept or not"
    );
}

#[tokio::test]
async fn the_executor_runs_instances_in_order_and_rolls_the_worst_outcome_up() {
    let data = tempfile::tempdir().unwrap();
    let working_directory = data.path().join("work");
    let final_directory = data.path().join("complete");
    fs::create_dir_all(&working_directory).unwrap();
    fs::create_dir_all(&final_directory).unwrap();
    let db = Database::open_in_memory().unwrap();
    enable_execution(&db);

    let first = write_script(
        data.path(),
        "first.sh",
        "#!/bin/sh\nprintf 'first\\n' >> \"$SAB_COMPLETE_DIR/order.txt\"\npwd > \"$SAB_COMPLETE_DIR/cwd.txt\"\n",
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
    let list = vec![
        instance(&db, &first),
        instance(&db, &second),
        instance(&db, &third),
    ];

    let mut job_context = context(101, working_directory.clone());
    job_context.final_directory = final_directory.clone();
    let report = executor(&db, data.path())
        .execute_job(101, list, job_context, None, None)
        .await
        .unwrap();

    assert_eq!(
        fs::read_to_string(final_directory.join("order.txt")).unwrap(),
        "first\nsecond\nthird\n",
        "scripts run sequentially in run order"
    );
    assert_eq!(
        fs::read_to_string(final_directory.join("cwd.txt")).unwrap(),
        format!(
            "{}\n",
            fs::canonicalize(&final_directory).unwrap().display()
        ),
        "scripts run from the retained output directory"
    );
    assert!(
        !working_directory.join("order.txt").exists(),
        "scripts do not write post-processing output into the staging directory"
    );
    // A script that fails takes the rollup with it without stopping the
    // scripts after it.
    assert_eq!(report.summary, PostProcessingSummary::Failed);
    assert_eq!(report.results.len(), 3);
    assert_eq!(report.results[0].status, ScriptStatus::Succeeded);
    assert_eq!(report.results[1].status, ScriptStatus::Failed);
    assert_eq!(report.results[1].exit_code, Some(7));
    assert_eq!(report.results[2].status, ScriptStatus::Succeeded);
    assert_eq!(report.results[2].adapter, ScriptAdapter::Sabnzbd);
    assert_eq!(report.results[2].script, third);
}

#[tokio::test]
async fn a_disabled_instance_is_never_executed() {
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
    let list = vec![
        db.create_script_instance(draft(&skipped).disabled())
            .unwrap(),
    ];

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
    let list = vec![instance(&db, &script)];

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
    let list = vec![instance(&db, &slow), instance(&db, &later)];

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
    // the script itself needs a moment to be spawned. Wait for the cancel to
    // land, however long the spawn takes.
    while !executor.cancel_job(104) {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let report = run.await.unwrap().unwrap();
    assert_eq!(report.summary, PostProcessingSummary::Cancelled);
    assert_eq!(
        report.results.len(),
        1,
        "the pass stops at the cancellation"
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

    let list = vec![instance(&db, &ScriptName::new("renamed.sh").unwrap())];
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
        server_attribution: None,
    })
    .unwrap();

    let script = write_script(
        data.path(),
        "counter.sh",
        "#!/bin/sh\nprintf 'x' >> \"$SAB_COMPLETE_DIR/runs.txt\"\n",
    );
    let list = vec![instance(&db, &script)];
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

    // A rerun executes the job's instances again against the retained output.
    let resolved = executor.resolve_job_scripts(None).unwrap();
    assert_eq!(resolved, list);
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
async fn an_instance_gives_its_script_the_inputs_it_holds_and_nothing_else() {
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
        "#!/bin/sh\nprintf 'HOST=%s TOKEN=%s PORT=%s/%s/%s\\n' \"${NZBPO_Host-unset}\" \"$NZBPO_Token\" \"$NZBPO_Port\" \"$SAB_OPTION_PORT\" \"$WEAVER_INPUT_PORT\" > \"$NZBPP_DIRECTORY/env.txt\"\nexit 93\n",
    )
    .unwrap();
    fs::set_permissions(package.join("run.sh"), fs::Permissions::from_mode(0o755)).unwrap();

    // The script declares `Host` with a default and knows nothing of `Port`.
    let script = ScriptName::new("email").unwrap();
    let token = db.create_secret("Mail token", "hunter2").unwrap();
    let saved = db
        .create_script_instance(
            draft(&script)
                .secret_input("Token", &token.id)
                .input("Port", "587"),
        )
        .unwrap();
    assert!(
        saved
            .inputs
            .iter()
            .all(|input| input.secret.is_none() || input.value.is_empty())
    );

    let list = vec![saved];
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
    // What was saved is what the script is given, under every naming: a
    // default the script declares is not filled in, an input it does not
    // declare is passed on, and the stored secret is decrypted for the
    // process only.
    assert_eq!(env.trim(), "HOST=unset TOKEN=hunter2 PORT=587/587/587");
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
            Some(OUT_OF_REACH),
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
    let second = instance(&db, &second);
    let admitted_list = vec![instance(&db, &first), second.clone()];

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
    // Generous on purpose. What is under test is that the script *begins*
    // before it is released, not how fast it begins: starting it means
    // launching the supervisor, which is this very binary, and a macOS host
    // that treats a freshly built binary as needing launch verification can
    // spend several seconds in the loader before `main` runs. Five seconds
    // was inside that window and made this fail on a loaded machine.
    async {
        while !first_working_directory.join("started").exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    .await;

    fs::write(first_working_directory.join("release"), "").unwrap();
    running.await.unwrap();
    assert_eq!(
        fs::read_to_string(first_working_directory.join("order.txt")).unwrap(),
        "first\nold-second\n",
        "already admitted work must continue resolving from its original root"
    );

    let second_working_directory = data.path().join("second-work");
    fs::create_dir_all(&second_working_directory).unwrap();
    executor
        .execute_job(
            702,
            vec![second],
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

#[tokio::test]
async fn a_script_nothing_waits_for_has_no_part_in_the_pass() {
    let data = tempfile::tempdir().unwrap();
    let working_directory = data.path().join("work");
    fs::create_dir_all(&working_directory).unwrap();
    let db = Database::open_in_memory().unwrap();
    enable_execution(&db);
    finished_job(&db, 110, &working_directory);
    let gate = gate(&working_directory, "gate");

    let detached = write_script(
        data.path(),
        "detached.sh",
        "#!/bin/sh\nread line < \"$SAB_COMPLETE_DIR/gate\"\nprintf 'detached\\n'\nexit 7\n",
    );
    let waited = write_script(data.path(), "waited.sh", "#!/bin/sh\nprintf 'waited\\n'\n");
    let list = vec![not_waited_for(&db, &detached), instance(&db, &waited)];

    // The first script cannot end until its gate opens, so a pass that
    // returns before then did not wait for it. Its exit status would have
    // failed the pass had it counted.
    let report = executor(&db, data.path())
        .execute_job(
            110,
            list,
            context(110, working_directory.clone()),
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(report.summary, PostProcessingSummary::Succeeded);
    assert_eq!(report.results.len(), 1);
    assert_eq!(report.results[0].script, waited);
    assert!(!report.results[0].background);
    assert!(db.event_script_results(110).unwrap().is_empty());

    open_gate(gate).await;
    db.background_scripts_settled().await;
    let recorded = db.event_script_results(110).unwrap();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].script, detached);
    assert!(recorded[0].background);
    assert_eq!(recorded[0].status, ScriptStatus::Failed);
    assert_eq!(recorded[0].exit_code, Some(7));
    assert_eq!(recorded[0].output_tail, "detached\n");
    let pass = db.job_post_processing_results(110).unwrap();
    assert_eq!(pass.len(), 1, "the pass is as it ended");
    assert_eq!(pass[0].script, waited);
}

#[tokio::test]
async fn a_pass_with_nothing_to_wait_for_does_not_queue_behind_another_job() {
    let data = tempfile::tempdir().unwrap();
    let working_directory = data.path().join("work");
    fs::create_dir_all(&working_directory).unwrap();
    let db = Database::open_in_memory().unwrap();
    enable_execution(&db);
    finished_job(&db, 112, &working_directory);
    let release = gate(&working_directory, "release");

    // One script at a time runs here, and this one keeps that turn until it
    // is released.
    let holder = write_script(
        data.path(),
        "holder.sh",
        "#!/bin/sh\nread line < \"$SAB_COMPLETE_DIR/release\"\n",
    );
    let quick = write_script(data.path(), "quick.sh", "#!/bin/sh\nprintf 'quick\\n'\n");
    let executor = executor(&db, data.path());
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let holding = {
        let executor = executor.clone();
        let list = vec![instance(&db, &holder)];
        let context = context(111, working_directory.clone());
        tokio::spawn(async move {
            executor
                .execute_job(111, list, context, None, Some(started_tx))
                .await
        })
    };
    started_rx.await.unwrap();

    let missing = ScriptName::new("renamed.sh").unwrap();
    let list = vec![not_waited_for(&db, &missing), not_waited_for(&db, &quick)];
    let report = executor
        .execute_job(
            112,
            list,
            context(112, working_directory.clone()),
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(report.summary, PostProcessingSummary::NotRun);
    assert!(report.results.is_empty());

    db.background_scripts_settled().await;
    let recorded = db.event_script_results(112).unwrap();
    assert_eq!(recorded.len(), 2);
    assert!(recorded.iter().all(|result| result.background));
    let ran = recorded
        .iter()
        .find(|result| result.script == quick)
        .unwrap();
    assert_eq!(ran.status, ScriptStatus::Succeeded);
    assert_eq!(ran.output_tail, "quick\n");
    // A script that could not be started has nowhere else to say so.
    let unstarted = recorded
        .iter()
        .find(|result| result.script == missing)
        .unwrap();
    assert_eq!(unstarted.status, ScriptStatus::Warning);
    assert!(
        unstarted
            .error_message
            .as_deref()
            .unwrap_or_default()
            .contains("renamed.sh")
    );

    open_gate(release).await;
    let held = holding.await.unwrap().unwrap();
    assert_eq!(held.summary, PostProcessingSummary::Succeeded);
}

#[tokio::test]
async fn a_turn_belongs_to_one_script_and_not_to_the_whole_job() {
    let data = tempfile::tempdir().unwrap();
    let working_directory = data.path().join("work");
    fs::create_dir_all(&working_directory).unwrap();
    let db = Database::open_in_memory().unwrap();
    enable_execution(&db);
    let first_release = gate(&working_directory, "first");
    let second_release = gate(&working_directory, "second");

    let first = write_script(
        data.path(),
        "first.sh",
        "#!/bin/sh\nread line < \"$SAB_COMPLETE_DIR/first\"\n",
    );
    let second = write_script(
        data.path(),
        "second.sh",
        "#!/bin/sh\nread line < \"$SAB_COMPLETE_DIR/second\"\n",
    );
    let other = write_script(data.path(), "other.sh", "#!/bin/sh\nprintf 'other\\n'\n");

    // One script at a time runs here, and the first job's first script has
    // that turn until it is released.
    let executor = executor(&db, data.path());
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let two_scripts = {
        let executor = executor.clone();
        let list = vec![instance(&db, &first), instance(&db, &second)];
        let context = context(114, working_directory.clone());
        tokio::spawn(async move {
            executor
                .execute_job(114, list, context, None, Some(started_tx))
                .await
        })
    };
    started_rx.await.unwrap();

    // The other job asks for a turn while the first job holds the only one, so
    // it is next in line when that turn is given back.
    let one_script = executor.execute_job(
        115,
        vec![instance(&db, &other)],
        context(115, working_directory.clone()),
        None,
        None,
    );
    tokio::pin!(one_script);
    let in_line = std::future::poll_fn(|cx| {
        std::task::Poll::Ready(std::future::Future::poll(one_script.as_mut(), cx).is_pending())
    })
    .await;
    assert!(in_line, "the other job waits while the only turn is taken");

    // The first job's second script is never released before the other job
    // ends, so the other job can only end on the turn the first script gave up.
    open_gate(first_release).await;
    let report = one_script.await.unwrap();
    assert_eq!(report.summary, PostProcessingSummary::Succeeded);
    assert_eq!(report.results.len(), 1);
    assert_eq!(report.results[0].output_tail, "other\n");
    assert!(
        !two_scripts.is_finished(),
        "the first job still has a script to run"
    );

    open_gate(second_release).await;
    let report = two_scripts.await.unwrap().unwrap();
    assert_eq!(report.summary, PostProcessingSummary::Succeeded);
    assert_eq!(report.results.len(), 2);
}

#[tokio::test]
async fn cancelling_a_job_stops_a_script_nothing_waits_for() {
    let data = tempfile::tempdir().unwrap();
    let working_directory = data.path().join("work");
    fs::create_dir_all(&working_directory).unwrap();
    let db = Database::open_in_memory().unwrap();
    enable_execution(&db);
    finished_job(&db, 113, &working_directory);
    let started = gate(&working_directory, "started");
    // Never opened: only the cancel can end the script.
    gate(&working_directory, "never");

    let detached = write_script(
        data.path(),
        "detached.sh",
        "#!/bin/sh\nprintf 'up\\n' > \"$SAB_COMPLETE_DIR/started\"\nread line < \"$SAB_COMPLETE_DIR/never\"\n",
    );
    let executor = executor(&db, data.path());
    let list = vec![not_waited_for(&db, &detached)];
    executor
        .execute_job(
            113,
            list,
            context(113, working_directory.clone()),
            None,
            None,
        )
        .await
        .unwrap();
    assert!(
        !db.cancel_background_scripts(999),
        "another job's cancel finds nothing to stop"
    );

    wait_at_gate(started).await;
    assert!(
        !executor.cancel_job(113),
        "the pass is over, so there is none to stop"
    );
    db.background_scripts_settled().await;
    let recorded = db.event_script_results(113).unwrap();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].script, detached);
    assert!(recorded[0].background);
    assert_eq!(recorded[0].status, ScriptStatus::Cancelled);
}
