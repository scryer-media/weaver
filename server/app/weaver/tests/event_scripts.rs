#![cfg(unix)]

use std::collections::BTreeMap;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use weaver_server_core::jobs::record::ActiveJob;
use weaver_server_core::post_processing::directives::{Directive, ScriptOutputEvent};
use weaver_server_core::post_processing::effects::apply_job_directive;
use weaver_server_core::post_processing::events::{
    EventContext, drain_queue, run_event, wait_for_event,
};
use weaver_server_core::post_processing::instances::{InstanceTrigger, ScriptInstanceDraft};
use weaver_server_core::post_processing::listing::resolve_script;
use weaver_server_core::post_processing::model::{
    PipelineOutcome, PostProcessingSettings, QueueEvent, ScriptEventLabel, ScriptKind, ScriptName,
    ScriptStatus,
};
use weaver_server_core::post_processing::output::ScriptRunFilter;
use weaver_server_core::post_processing::runner::{
    CompatibilityFacts, ExecutionDisposition, ExecutionSpec, InterpreterConfig,
    JobExecutionContext, RunIdentity, execute_spec,
};
use weaver_server_core::{Database, JobId};

fn setup() -> (Database, tempfile::TempDir) {
    let db = Database::open_in_memory().unwrap();
    let data = tempfile::tempdir().unwrap();
    db.initialize_post_processing_script_directory(data.path(), None)
        .unwrap();
    db.save_post_processing_settings(&PostProcessingSettings {
        execution_enabled: true,
        ..Default::default()
    })
    .unwrap();
    (db, data)
}

fn script(db: &Database, name: &str, body: &str) -> ScriptName {
    let root = db.post_processing_script_directory().unwrap();
    fs::write(root.join(name), body).unwrap();
    fs::set_permissions(root.join(name), fs::Permissions::from_mode(0o755)).unwrap();
    ScriptName::new(name).unwrap()
}

const DOWNLOADED: InstanceTrigger = InstanceTrigger::Queue(QueueEvent::NzbDownloaded);

/// An instance of `script` that `trigger` starts.
fn on(trigger: InstanceTrigger, script: &ScriptName) -> ScriptInstanceDraft {
    ScriptInstanceDraft::new(script.clone(), trigger)
}

/// Replaces the saved instances with `drafts`, in that order.
fn select(db: &Database, drafts: Vec<ScriptInstanceDraft>) {
    for instance in db.script_instances().unwrap() {
        assert!(db.delete_script_instance(&instance.id).unwrap());
    }
    for draft in drafts {
        db.create_script_instance(draft).unwrap();
    }
}

fn job(db: &Database, directory: &Path) -> JobExecutionContext {
    db.create_active_job(&ActiveJob {
        job_id: JobId(42),
        nzb_hash: [42; 32],
        nzb_path: directory.join("source.nzb"),
        nzb_zstd: Vec::new(),
        output_dir: directory.into(),
        created_at: 1,
        category: None,
        metadata: vec![("seed".into(), "input".into())],
        status: "downloading",
        download_state: "downloading",
        post_state: "idle",
        run_state: "active",
        paused_resume_status: None,
        paused_resume_download_state: None,
        paused_resume_post_state: None,
        password_override: Some("private-password".into()),
    })
    .unwrap();
    JobExecutionContext {
        job_id: 42,
        name: "sample".into(),
        nzb_filename: "sample.nzb".into(),
        category: None,
        group: None,
        source_url: None,
        working_directory: directory.into(),
        final_directory: directory.into(),
        pipeline_outcome: PipelineOutcome::Succeeded,
        par_status: 0,
        unpack_status: 0,
        compatibility: CompatibilityFacts {
            data_dir: Some(directory.into()),
            complete_dir: Some(directory.into()),
            password: Some("private-password".into()),
            ..Default::default()
        },
    }
}

fn jobless(directory: &Path, event: ScriptEventLabel, env: &[(&str, String)]) -> EventContext {
    EventContext {
        job_id: None,
        event,
        category: None,
        cwd: directory.into(),
        env: env
            .iter()
            .map(|(key, value)| ((*key).into(), value.clone()))
            .collect(),
        facts: Default::default(),
        instances: None,
        scratch: None,
    }
}

fn supervisor() -> Option<PathBuf> {
    Some(PathBuf::from(env!("CARGO_BIN_EXE_weaver")))
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

#[tokio::test]
async fn scan_distinguishes_supervisor_launch_failure_from_script_exit_127() {
    let (db, data) = setup();
    let missing_interpreter = script(
        &db,
        "missing.py",
        "### NZBGET SCAN SCRIPT ###\nprint('must not run')\n",
    );
    let mut settings = db.post_processing_settings().unwrap();
    settings.python_interpreter = Some(
        data.path()
            .join("missing-interpreter")
            .to_string_lossy()
            .into_owned(),
    );
    db.save_post_processing_settings(&settings).unwrap();
    select(&db, vec![on(InstanceTrigger::Scan, &missing_interpreter)]);
    let mut event = jobless(data.path(), ScriptEventLabel::Scan, &[]);
    let failed = run_event(&db, &mut event, "scan-launch-failed", None, supervisor())
        .await
        .unwrap();
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0].status, ScriptStatus::Failed);
    assert_eq!(failed[0].exit_code, None);
    assert!(
        failed[0]
            .error_message
            .as_deref()
            .unwrap()
            .contains("did not confirm script launch")
    );

    let completed_script = script(
        &db,
        "exit127.sh",
        "#!/bin/sh\n### NZBGET SCAN SCRIPT ###\nprintf 'script ran\\n'\nexit 127\n",
    );
    select(&db, vec![on(InstanceTrigger::Scan, &completed_script)]);
    let completed = run_event(&db, &mut event, "scan-exit-127", None, supervisor())
        .await
        .unwrap();
    assert_eq!(completed.len(), 1);
    // The script ran and chose that code itself, which is a failure of its
    // own and not one of launching it.
    assert_eq!(completed[0].status, ScriptStatus::Failed);
    assert_eq!(completed[0].exit_code, Some(127));
    assert_eq!(completed[0].output_tail, "script ran\n");
    assert!(
        !completed[0]
            .error_message
            .as_deref()
            .is_some_and(|message| message.contains("did not confirm script launch"))
    );
}

#[tokio::test]
async fn a_recorded_run_says_whether_it_printed_more_than_was_kept() {
    let (db, data) = setup();
    // 400 or 100 lines of 99 characters and a newline, then a last line.
    let printing = |lines: usize| {
        format!(
            "#!/bin/sh\n### NZBGET SCAN SCRIPT ###\ni=0\nwhile [ $i -lt {lines} ]; do printf '%099d\\n' $i; i=$((i + 1)); done\nprintf 'last line\\n'\nexit 0\n"
        )
    };
    let over = script(&db, "over.sh", &printing(400));
    let under = script(&db, "under.sh", &printing(100));
    select(
        &db,
        vec![
            on(InstanceTrigger::Scan, &over),
            on(InstanceTrigger::Scan, &under),
        ],
    );
    let mut event = jobless(data.path(), ScriptEventLabel::Scan, &[]);
    let results = run_event(&db, &mut event, "scan-output", None, supervisor())
        .await
        .unwrap();
    assert_eq!(results.len(), 2);
    assert!(results[0].output_truncated);
    assert!(!results[1].output_truncated);

    let recorded = db.script_runs(ScriptRunFilter::default(), None, 8).unwrap();
    let run = |script: &ScriptName| {
        recorded
            .iter()
            .find(|run| &run.result.script == script)
            .unwrap()
    };
    let (over, under) = (run(&over), run(&under));
    assert!(
        over.result.output_truncated,
        "40,010 bytes do not fit in 32 KiB"
    );
    let kept = db
        .script_output(over.result.output_id.as_deref().unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(kept.len(), 32 * 1024);
    assert!(kept.ends_with("last line\n"));

    assert!(!under.result.output_truncated, "10,010 bytes fit");
    let kept = db
        .script_output(under.result.output_id.as_deref().unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(kept.len(), 100 * 100 + "last line\n".len());
}

#[tokio::test]
async fn queue_parameters_stream_to_persistence_and_the_next_script() {
    let (db, data) = setup();
    let context = job(&db, data.path());
    let first = script(
        &db,
        "first.sh",
        "#!/bin/sh\n### NZBGET QUEUE SCRIPT ###\nprintf '[NZB] NZBPR_First=from-queue\\n' >&2\nexit 17\n",
    );
    let second = script(
        &db,
        "second.sh",
        "#!/bin/sh\n### NZBGET QUEUE SCRIPT ###\nprintf '[NZB] NZBPR_Second=%s/%s\\n' \"$NZBPR_FIRST\" \"$NZBPR_SEED\"\nexit 0\n",
    );
    select(&db, vec![on(DOWNLOADED, &first), on(DOWNLOADED, &second)]);
    let mut event = EventContext::from_job(&context, QueueEvent::NzbDownloaded);
    let results = run_event(&db, &mut event, "queue-test", None, supervisor())
        .await
        .unwrap();
    // What the first script asked for is kept and reaches the second, though
    // the first went on to fail.
    assert_eq!(
        results
            .iter()
            .map(|result| result.status)
            .collect::<Vec<_>>(),
        [ScriptStatus::Failed, ScriptStatus::Succeeded]
    );
    assert!(results.iter().all(|result| result.instance_id.is_some()));
    assert_eq!(
        db.job_script_effects(42).unwrap().parameters["Second"],
        "from-queue/input"
    );
    assert_eq!(db.event_script_results(42).unwrap().len(), 2);
    assert_eq!(db.job_post_processing_results(42).unwrap().len(), 0);
}

#[tokio::test]
async fn an_instance_whose_script_is_gone_is_recorded_and_releases_the_downloaded_barrier() {
    let (db, data) = setup();
    let context = job(&db, data.path());
    let gone = ScriptName::new("gone.sh").unwrap();
    let next = script(
        &db,
        "next.sh",
        "#!/bin/sh\n### NZBGET QUEUE SCRIPT ###\nprintf '[NZB] NZBPR_Next=executed\\n'\nexit 0\n",
    );
    select(&db, vec![on(DOWNLOADED, &gone), on(DOWNLOADED, &next)]);
    let mut event = EventContext::from_job(&context, QueueEvent::NzbDownloaded);
    let results = run_event(&db, &mut event, "missing-queue-script", None, supervisor())
        .await
        .unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].script, gone);
    assert_eq!(results[0].status, ScriptStatus::Warning);
    assert_eq!(results[0].exit_code, None);
    assert!(results[0].error_message.is_some());
    assert_eq!(results[1].script, next);
    assert_eq!(results[1].status, ScriptStatus::Succeeded);
    let effects = db.job_script_effects(42).unwrap();
    assert_eq!(effects.parameters["Next"], "executed");
    assert!(!effects.marked_bad);

    // An instance that could not run is still a completed queue run, so
    // native completion can continue after the durable downloaded barrier.
    select(&db, vec![on(DOWNLOADED, &gone)]);
    let run_id = db.enqueue_script_event(&event, 1).unwrap().unwrap();
    drain_queue(db.clone()).await.unwrap();
    wait_for_event(&db, &run_id).await.unwrap();
    assert!(db.script_event_finished(&run_id).unwrap());
    assert_eq!(db.queue_script_count().unwrap(), 0);
    let retained = db.event_script_results(42).unwrap();
    assert_eq!(
        retained
            .iter()
            .filter(|result| result.status == ScriptStatus::Warning)
            .count(),
        2
    );
}

#[tokio::test]
async fn cancellation_keeps_redacted_directives_emitted_before_the_kill() {
    let (db, data) = setup();
    let mut context = job(&db, data.path());
    let name = script(
        &db,
        "stream.sh",
        "#!/bin/sh\n### NZBGET QUEUE SCRIPT ###\nprintf '[NZB] NZBPR_Token=private-password\\n' >&2\nwhile :; do :; done\n",
    );
    let found = resolve_script(&db.post_processing_script_directory().unwrap(), &name).unwrap();
    let spec = ExecutionSpec {
        manifest: found.manifest,
        root: found.root,
        options: Vec::new(),
        cwd: data.path().into(),
        env: BTreeMap::new(),
        argv: Vec::new(),
        timeout: None,
        termination_grace: Duration::from_millis(1),
        kind: ScriptEventLabel::Queue(QueueEvent::NzbDownloaded),
        run_id: "cancel-test".into(),
        identity: RunIdentity {
            run_id: "cancel-test".into(),
            instance_id: "instance".into(),
            instance_name: "stream".into(),
            trigger: DOWNLOADED.to_string(),
            api_url: None,
            token: None,
            output: Default::default(),
        },
        facts: context.compatibility.clone(),
        interpreters: InterpreterConfig::default(),
        supervisor_executable: supervisor(),
    };
    let (sender, mut receiver) = tokio::sync::mpsc::channel(8);
    let (cancel, cancellation) = tokio::sync::watch::channel(false);
    let (result, ()) = tokio::join!(
        execute_spec(spec, Some(cancellation), Some(sender)),
        async {
            let Some(ScriptOutputEvent::Directive(directive)) = receiver.recv().await else {
                panic!("expected streamed directive");
            };
            apply_job_directive(&db, &mut context, directive).unwrap();
            assert_eq!(
                db.job_script_effects(42).unwrap().parameters["Token"],
                "[REDACTED]"
            );
            cancel.send_replace(true);
            while receiver.recv().await.is_some() {}
        }
    );
    assert_eq!(result.unwrap().disposition, ExecutionDisposition::Cancelled);
    assert_eq!(
        db.job_script_effects(42).unwrap().parameters["Token"],
        "[REDACTED]"
    );
}

#[tokio::test]
async fn scan_rewrites_input_and_the_next_script_sees_the_category_it_chose() {
    let (db, data) = setup();
    let path = data.path().join("input.nzb");
    fs::write(&path, "original").unwrap();
    let first = script(
        &db,
        "scan.sh",
        "#!/bin/sh\n### NZBGET SCAN SCRIPT ###\nprintf rewritten > \"$NZBNP_FILENAME\"\nprintf '[NZB] CATEGORY=movies\\n[NZB] PAUSED=1\\n[NZB] TOP=1\\n'\nexit 17\n",
    );
    let second = script(
        &db,
        "category.sh",
        "#!/bin/sh\n### NZBGET SCAN SCRIPT ###\nprintf '[NZB] NZBPR_Category=%s\\n' \"$NZBNP_CATEGORY\"\nexit 0\n",
    );
    select(
        &db,
        vec![
            on(InstanceTrigger::Scan, &first),
            on(InstanceTrigger::Scan, &second),
        ],
    );
    let mut context = jobless(
        data.path(),
        ScriptEventLabel::Scan,
        &[("NZBNP_FILENAME", path.to_string_lossy().into_owned())],
    );
    let results = run_event(&db, &mut context, "scan-test", None, supervisor())
        .await
        .unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(fs::read_to_string(path).unwrap(), "rewritten");
    assert_eq!(context.category.as_deref(), Some("movies"));
    assert_eq!(context.env["NZBNP_PAUSED"], "1");
    assert_eq!(context.env["NZBNP_TOP"], "1");
    assert!(
        context
            .facts
            .parameters
            .contains(&("Category".into(), "movies".into()))
    );
}

#[tokio::test]
async fn scan_names_and_categories_reach_the_existing_filesystem_guards() {
    let (db, data) = setup();
    let path = data.path().join("input.nzb");
    fs::write(&path, "original").unwrap();
    let scan = script(
        &db,
        "names.sh",
        "#!/bin/sh\n### NZBGET SCAN SCRIPT ###\nprintf '[NZB] NZBNAME=../bad/name\\tpart\\n[NZB] CATEGORY=../escape\\n'\nexit 0\n",
    );
    select(&db, vec![on(InstanceTrigger::Scan, &scan)]);
    let mut context = jobless(
        data.path(),
        ScriptEventLabel::Scan,
        &[("NZBNP_FILENAME", path.to_string_lossy().into_owned())],
    );
    let results = run_event(&db, &mut context, "scan-name-guards", None, supervisor())
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert!(
        weaver_server_core::categories::resolve_submission_category(
            &[],
            context.category.as_deref()
        )
        .is_err()
    );
    let name = &context.env["NZBNP_NZBNAME"];
    assert_eq!(name, "../bad/name\tpart");
    let component = weaver_server_core::jobs::working_dir::sanitize_dirname(name);
    assert_eq!(Path::new(&component).components().count(), 1);
    assert!(
        !component
            .chars()
            .any(|character| character.is_control() || character == '/' || character == '\\')
    );
    assert_eq!(fs::read_to_string(path).unwrap(), "original");
}

#[tokio::test]
async fn deleted_scan_input_skips_later_scripts() {
    let (db, data) = setup();
    let path = data.path().join("input.nzb");
    fs::write(&path, "original").unwrap();
    let first = script(
        &db,
        "delete.sh",
        "#!/bin/sh\n### NZBGET SCAN SCRIPT ###\nrm \"$NZBNP_FILENAME\"\n",
    );
    let second = script(
        &db,
        "never.sh",
        "#!/bin/sh\n### NZBGET SCAN SCRIPT ###\nprintf unexpected > unexpected\n",
    );
    select(
        &db,
        vec![
            on(InstanceTrigger::Scan, &first),
            on(InstanceTrigger::Scan, &second),
        ],
    );
    let mut context = jobless(
        data.path(),
        ScriptEventLabel::Scan,
        &[("NZBNP_FILENAME", path.to_string_lossy().into_owned())],
    );
    assert_eq!(
        run_event(&db, &mut context, "scan-delete", None, supervisor())
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(!path.exists());
    assert!(!data.path().join("unexpected").exists());
}

#[tokio::test]
async fn a_feed_script_that_fails_does_not_stop_the_ones_after_it() {
    let (db, data) = setup();
    let path = data.path().join("feed.xml");
    fs::write(&path, "original").unwrap();
    let first = script(
        &db,
        "failure.sh",
        "#!/bin/sh\n### NZBGET FEED SCRIPT ###\nexit 1\n",
    );
    let second = script(
        &db,
        "rewrite.sh",
        "#!/bin/sh\n### NZBGET FEED SCRIPT ###\nprintf rewritten > \"$NZBFP_FILENAME\"\nexit 93\n",
    );
    select(
        &db,
        vec![
            on(InstanceTrigger::Feed, &first),
            on(InstanceTrigger::Feed, &second),
        ],
    );
    let mut context = jobless(
        data.path(),
        ScriptEventLabel::Feed(7),
        &[
            ("NZBFP_FILENAME", path.to_string_lossy().into_owned()),
            ("NZBFP_FEEDID", "7".into()),
        ],
    );
    let results = run_event(&db, &mut context, "feed-test", None, supervisor())
        .await
        .unwrap();
    assert_eq!(
        results
            .iter()
            .map(|result| result.status)
            .collect::<Vec<_>>(),
        [ScriptStatus::Failed, ScriptStatus::Succeeded]
    );
    assert_eq!(fs::read_to_string(path).unwrap(), "rewritten");
}

#[test]
fn directives_reject_foreign_markers_and_later_user_parameters_win() {
    let (db, data) = setup();
    let mut context = job(&db, data.path());
    let foreign = data.path().join("foreign");
    fs::create_dir(&foreign).unwrap();
    weaver_server_core::jobs::working_dir::mark_weaver_owned_working_dir(
        data.path(),
        &foreign,
        JobId(99),
    )
    .unwrap();
    assert!(
        apply_job_directive(
            &db,
            &mut context,
            Directive::FinalDirectory(foreign.to_string_lossy().into_owned())
        )
        .is_err()
    );
    apply_job_directive(
        &db,
        &mut context,
        Directive::Parameter {
            name: "value".into(),
            value: "script".into(),
        },
    )
    .unwrap();
    db.update_active_job(
        JobId(42),
        &weaver_server_core::JobUpdate {
            metadata: weaver_server_core::FieldUpdate::Set(vec![("value".into(), "user".into())]),
            ..Default::default()
        },
    )
    .unwrap();
    db.job_script_effects(42)
        .unwrap()
        .apply_to_context(&mut context);
    assert_eq!(
        context.compatibility.parameters,
        [("value".into(), "user".into())]
    );
}

#[tokio::test]
async fn a_queue_script_nothing_waits_for_does_not_hold_its_event() {
    let (db, data) = setup();
    let context = job(&db, data.path());
    let gate = gate(data.path(), "gate");
    let detached = script(
        &db,
        "detached.sh",
        &format!(
            "#!/bin/sh\n### NZBGET QUEUE SCRIPT ###\nread line < '{}'\nprintf '[NZB] NZBPR_Detached=ran\\n'\nexit 0\n",
            gate.display()
        ),
    );
    let waited = script(
        &db,
        "waited.sh",
        "#!/bin/sh\n### NZBGET QUEUE SCRIPT ###\nexit 0\n",
    );
    select(
        &db,
        vec![
            on(DOWNLOADED, &detached).fire_and_forget(),
            on(DOWNLOADED, &waited),
        ],
    );
    let mut event = EventContext::from_job(&context, QueueEvent::NzbDownloaded);

    // The first script cannot end until its gate opens, so an event that
    // returns before then did not wait for it.
    let results = run_event(&db, &mut event, "detached-queue", None, supervisor())
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].script, waited);
    assert!(!results[0].background);
    assert_eq!(db.event_script_results(42).unwrap().len(), 1);

    open_gate(gate).await;
    db.background_scripts_settled().await;
    let recorded = db.event_script_results(42).unwrap();
    assert_eq!(recorded.len(), 2);
    let run = recorded
        .iter()
        .find(|result| result.script == detached)
        .unwrap();
    assert!(run.background);
    assert_eq!(run.status, ScriptStatus::Succeeded);
    // What it asked for still reaches the job, whenever that turns out to be.
    assert_eq!(
        db.job_script_effects(42).unwrap().parameters["Detached"],
        "ran"
    );
}

#[tokio::test]
async fn a_scan_script_nothing_waits_for_does_not_hold_the_scan() {
    let (db, data) = setup();
    let gate = gate(data.path(), "gate");
    let path = data.path().join("input.nzb");
    fs::write(&path, "original").unwrap();
    let scan = script(
        &db,
        "scan.sh",
        &format!(
            "#!/bin/sh\n### NZBGET SCAN SCRIPT ###\nread line < '{}'\nprintf rewritten > \"$NZBNP_FILENAME\"\nexit 0\n",
            gate.display()
        ),
    );
    select(
        &db,
        vec![on(InstanceTrigger::Scan, &scan).fire_and_forget()],
    );
    let mut context = jobless(
        data.path(),
        ScriptEventLabel::Scan,
        &[("NZBNP_FILENAME", path.to_string_lossy().into_owned())],
    );

    // The script cannot end until its gate opens, so a scan that returns
    // before then did not wait for it, and it has changed nothing yet.
    let results = run_event(&db, &mut context, "scan-detached", None, supervisor())
        .await
        .unwrap();
    assert!(results.is_empty());
    assert_eq!(fs::read_to_string(&path).unwrap(), "original");

    open_gate(gate).await;
    db.background_scripts_settled().await;
    let recorded = db
        .script_runs(
            ScriptRunFilter {
                kind: Some(ScriptKind::Scan),
                ..Default::default()
            },
            None,
            8,
        )
        .unwrap();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].result.script, scan);
    assert!(recorded[0].result.background);
    assert_eq!(recorded[0].result.status, ScriptStatus::Succeeded);
    assert_eq!(fs::read_to_string(path).unwrap(), "rewritten");
}

#[tokio::test]
async fn a_scheduled_script_nothing_waits_for_leaves_the_waited_scripts_their_turn() {
    let (db, data) = setup();
    let started = gate(data.path(), "started");
    let release = gate(data.path(), "release");
    let path = data.path().join("input.nzb");
    fs::write(&path, "original").unwrap();
    // One script at a time is waited for by default, and this one keeps that
    // turn until it is released.
    let holder = script(
        &db,
        "holder.sh",
        &format!(
            "#!/bin/sh\n### NZBGET SCAN SCRIPT ###\nprintf 'up\\n' > '{}'\nread line < '{}'\nexit 0\n",
            started.display(),
            release.display()
        ),
    );
    let nightly = script(
        &db,
        "nightly.sh",
        "#!/bin/sh\n### NZBGET SCHEDULER SCRIPT ###\nprintf 'nightly\\n'\nexit 0\n",
    );
    select(
        &db,
        vec![
            on(InstanceTrigger::Scan, &holder),
            on(InstanceTrigger::Schedule, &nightly).fire_and_forget(),
        ],
    );
    let scan = {
        let db = db.clone();
        let mut context = jobless(
            data.path(),
            ScriptEventLabel::Scan,
            &[("NZBNP_FILENAME", path.to_string_lossy().into_owned())],
        );
        tokio::spawn(async move {
            run_event(&db, &mut context, "scan-holder", None, supervisor())
                .await
                .unwrap()
        })
    };
    wait_at_gate(started).await;

    let mut scheduled = jobless(data.path(), ScriptEventLabel::Scheduler(3), &[]);
    let results = run_event(&db, &mut scheduled, "scheduler:3:test", None, supervisor())
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].script, nightly);
    assert!(results[0].background);
    assert_eq!(results[0].status, ScriptStatus::Succeeded);
    assert_eq!(results[0].output_tail, "nightly\n");

    open_gate(release).await;
    assert_eq!(scan.await.unwrap().len(), 1);
}
