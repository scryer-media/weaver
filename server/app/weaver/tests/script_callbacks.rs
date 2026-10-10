//! A running script calling weaver back: real scripts, the real supervisor,
//! and the run's token played the way the API plays it.
//!
//! Each script stops at a gate once it has started, and while it waits there
//! the test asks for things in its name, exactly as the GraphQL layer would
//! with the token the script was handed.

#![cfg(unix)]

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::sync::RwLock;
use weaver_server_core::jobs::record::ActiveJob;
use weaver_server_core::post_processing::callbacks::{RunAction, RunActionError};
use weaver_server_core::post_processing::directives::{Directive, ScriptLogLevel};
use weaver_server_core::post_processing::events::{EventContext, run_event};
use weaver_server_core::post_processing::executor::PostProcessingExecutor;
use weaver_server_core::post_processing::instances::{
    InstanceTrigger, ScriptInstance, ScriptInstanceDraft,
};
use weaver_server_core::post_processing::model::{
    PipelineOutcome, PostProcessingSettings, PostProcessingSummary, QueueEvent, ScriptEventLabel,
    ScriptName, ScriptStatus,
};
use weaver_server_core::post_processing::runner::{CompatibilityFacts, JobExecutionContext};
use weaver_server_core::post_processing::test_run::start_script_test;
use weaver_server_core::settings::{Config, SharedConfig};
use weaver_server_core::{Database, JobId};

/// Nothing listens here: the address is only handed to the script.
const API_URL: &str = "http://127.0.0.1:1/graphql";

const DOWNLOADED: InstanceTrigger = InstanceTrigger::Queue(QueueEvent::NzbDownloaded);

/// A database whose scripts may run, without an address to call back on.
fn setup_without_api() -> (Database, tempfile::TempDir) {
    let db = Database::open_in_memory().unwrap();
    let data = tempfile::tempdir().unwrap();
    db.initialize_post_processing_script_directory(data.path(), None)
        .unwrap();
    db.save_post_processing_settings(&PostProcessingSettings {
        execution_enabled: true,
        termination_grace_seconds: 1,
        ..Default::default()
    })
    .unwrap();
    (db, data)
}

/// A database whose scripts may run and call back.
fn setup() -> (Database, tempfile::TempDir) {
    let (db, data) = setup_without_api();
    db.set_script_api_url(API_URL);
    (db, data)
}

fn script(db: &Database, name: &str, body: &str) -> ScriptName {
    let root = db.post_processing_script_directory().unwrap();
    fs::write(root.join(name), body).unwrap();
    fs::set_permissions(root.join(name), fs::Permissions::from_mode(0o755)).unwrap();
    ScriptName::new(name).unwrap()
}

/// Replaces the saved instances with one of each of `drafts`, in that order.
fn select(db: &Database, drafts: Vec<ScriptInstanceDraft>) -> Vec<ScriptInstance> {
    for instance in db.script_instances().unwrap() {
        assert!(db.delete_script_instance(&instance.id).unwrap());
    }
    drafts
        .into_iter()
        .map(|draft| db.create_script_instance(draft).unwrap())
        .collect()
}

fn on(trigger: InstanceTrigger, script: &ScriptName) -> ScriptInstanceDraft {
    ScriptInstanceDraft::new(script.clone(), trigger)
}

/// An active download 42 for commands to be applied to.
fn job(db: &Database, directory: &Path) -> JobExecutionContext {
    db.create_active_job(&ActiveJob {
        job_id: JobId(42),
        nzb_hash: [42; 32],
        nzb_path: directory.join("source.nzb"),
        nzb_zstd: Vec::new(),
        output_dir: directory.into(),
        created_at: 1,
        category: None,
        metadata: Vec::new(),
        status: "downloading",
        download_state: "downloading",
        post_state: "idle",
        run_state: "active",
        paused_resume_status: None,
        paused_resume_download_state: None,
        paused_resume_post_state: None,
        password_override: None,
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
            ..Default::default()
        },
    }
}

fn scan(directory: &Path, input: &Path) -> EventContext {
    EventContext {
        job_id: None,
        event: ScriptEventLabel::Scan,
        category: None,
        cwd: directory.into(),
        env: [(
            "NZBNP_FILENAME".to_string(),
            input.to_string_lossy().into_owned(),
        )]
        .into_iter()
        .collect(),
        facts: Default::default(),
        instances: None,
        scratch: None,
    }
}

fn supervisor() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_weaver"))
}

fn executor(db: &Database) -> PostProcessingExecutor {
    PostProcessingExecutor::new(db.clone(), db.post_processing_script_directory().unwrap())
        .with_supervisor_executable(supervisor())
}

fn config(data: &Path) -> SharedConfig {
    Arc::new(RwLock::new(Config {
        data_dir: data.to_string_lossy().into_owned(),
        intermediate_dir: None,
        complete_dir: None,
        buffer_pool: None,
        servers: vec![],
        categories: vec![],
        retry: None,
        max_download_speed: None,
        cleanup_after_extract: None,
        propagation_delay_secs: None,
        watch_folder: weaver_server_core::watch_folder::WatchFolderConfig::default(),
        duplicate_policy: weaver_server_core::jobs::DuplicatePolicy::default(),
        direct_store: None,
        direct_unpack: None,
        delivery_naming: None,
        hardware_profile: None,
        metrics: Default::default(),
        config_path: None,
    }))
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

/// What a script told the test about its run when it reached its first gate.
struct Started {
    run_id: String,
    token: String,
    api_url: String,
}

/// Returns once the script has written who it is to `gate`.
async fn wait_at_gate(gate: PathBuf) -> Started {
    let written = tokio::task::spawn_blocking(move || fs::read_to_string(gate).unwrap())
        .await
        .unwrap();
    let mut lines = written.lines().map(str::to_string);
    let mut next = || lines.next().expect("the script wrote three lines");
    Started {
        run_id: next(),
        token: next(),
        api_url: next(),
    }
}

/// The lines a script writes to `started` before it stops at `release`.
fn announce_and_wait(started: &Path, release: &Path) -> String {
    format!(
        "printf '%s\\n%s\\n%s\\n' \"$WEAVER_RUN_ID\" \"${{WEAVER_RUN_TOKEN:-unset}}\" \"${{WEAVER_API_URL:-unset}}\" > '{}'\nread line < '{}'\n",
        started.display(),
        release.display()
    )
}

fn parameter(name: &str, value: &str) -> RunAction {
    RunAction::Command(Directive::Parameter {
        name: name.into(),
        value: value.into(),
    })
}

#[tokio::test]
async fn a_queue_script_reaches_its_own_download_only_while_it_runs() {
    let (db, data) = setup();
    let context = job(&db, data.path());
    let started = gate(data.path(), "started");
    let release = gate(data.path(), "release");
    let name = script(
        &db,
        "queue.sh",
        &format!(
            "#!/bin/sh\n### NZBGET QUEUE SCRIPT ###\n{}exit 0\n",
            announce_and_wait(&started, &release)
        ),
    );
    let instance = select(&db, vec![on(DOWNLOADED, &name)]).remove(0);
    let event = {
        let db = db.clone();
        let mut event = EventContext::from_job(&context, QueueEvent::NzbDownloaded);
        tokio::spawn(async move {
            let results = run_event(&db, &mut event, "queue-callback", None, Some(supervisor()))
                .await
                .unwrap();
            (results, event)
        })
    };

    let me = wait_at_gate(started).await;
    assert_eq!(me.api_url, API_URL);
    let live = db
        .script_run_for_token(&me.token)
        .expect("the token names the run going on");
    assert_eq!(live.run_id, me.run_id);
    assert_eq!(live.instance_id, instance.id);
    assert_eq!(live.job_id, Some(42));
    assert_eq!(
        live.event,
        ScriptEventLabel::Queue(QueueEvent::NzbDownloaded)
    );
    assert!(!live.test);

    db.script_run_action(&me.run_id, parameter("Key", "from-api"))
        .await
        .unwrap();
    assert_eq!(
        db.job_script_effects(42).unwrap().parameters["Key"],
        "from-api"
    );
    // A queue script may not move its download to another category, however
    // it asks.
    let refused = db
        .script_run_action(
            &me.run_id,
            RunAction::Command(Directive::Category("tv".into())),
        )
        .await;
    assert!(
        matches!(refused, Err(RunActionError::NotAllowed(_))),
        "{refused:?}"
    );
    let effects = db.job_script_effects(42).unwrap();
    assert_eq!(effects.parameters.len(), 1);
    assert!(!effects.marked_bad);

    open_gate(release).await;
    let (results, event) = event.await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].status, ScriptStatus::Succeeded);
    assert_eq!(event.category, None);

    // The run is over, and its token with it.
    assert_eq!(db.script_run_for_token(&me.token), None);
    assert_eq!(
        db.script_run_action(&me.run_id, parameter("Late", "too-late"))
            .await,
        Err(RunActionError::Ended)
    );
    assert!(
        !db.job_script_effects(42)
            .unwrap()
            .parameters
            .contains_key("Late")
    );
}

#[tokio::test]
async fn what_a_script_sends_through_the_api_is_stored_without_its_secrets() {
    let (db, data) = setup();
    let context = job(&db, data.path());
    let started = gate(data.path(), "started");
    let release = gate(data.path(), "release");
    let package = db
        .post_processing_script_directory()
        .unwrap()
        .join("notify");
    fs::create_dir_all(&package).unwrap();
    fs::write(
        package.join("manifest.json"),
        serde_json::json!({
            "main": "run.sh",
            "name": "notify",
            "kind": "QUEUE",
            "displayName": "Notify",
            "version": "1.0.0",
            "author": "Author",
            "homepage": "https://example.invalid",
            "license": "GNU",
            "about": "About",
            "description": [],
            "requirements": [],
            "queueEvents": "NZB_DOWNLOADED",
            "taskTime": "",
            "sections": [],
            "commands": [],
            "options": [{
                "name": "Token",
                "displayName": "Token",
                "value": "",
                "description": [],
                "select": [],
                "secret": true
            }]
        })
        .to_string(),
    )
    .unwrap();
    fs::write(
        package.join("run.sh"),
        format!(
            "#!/bin/sh\n{}exit 0\n",
            announce_and_wait(&started, &release)
        ),
    )
    .unwrap();
    fs::set_permissions(package.join("run.sh"), fs::Permissions::from_mode(0o755)).unwrap();
    let token = db.create_secret("Notify token", "hunter2").unwrap();
    select(
        &db,
        vec![on(DOWNLOADED, &ScriptName::new("notify").unwrap()).secret_input("Token", &token.id)],
    );
    let event = {
        let db = db.clone();
        let mut event = EventContext::from_job(&context, QueueEvent::NzbDownloaded);
        tokio::spawn(async move {
            run_event(&db, &mut event, "queue-secrets", None, Some(supervisor()))
                .await
                .unwrap()
        })
    };

    let me = wait_at_gate(started).await;
    db.script_run_action(&me.run_id, parameter("Secret", "key=hunter2"))
        .await
        .unwrap();
    db.script_run_action(&me.run_id, parameter("Token", &format!("t={}", me.token)))
        .await
        .unwrap();
    let parameters = db.job_script_effects(42).unwrap().parameters;
    assert_eq!(parameters["Secret"], "key=[REDACTED]");
    assert_eq!(parameters["Token"], "t=[REDACTED]");

    open_gate(release).await;
    let results = event.await.unwrap();
    assert_eq!(results[0].status, ScriptStatus::Succeeded);
}

#[tokio::test]
async fn a_scan_script_chooses_the_category_through_the_api_and_the_next_script_sees_it() {
    let (db, data) = setup();
    let input = data.path().join("input.nzb");
    fs::write(&input, "original").unwrap();
    let started = gate(data.path(), "started");
    let release = gate(data.path(), "release");
    let first = script(
        &db,
        "scan.sh",
        &format!(
            "#!/bin/sh\n### NZBGET SCAN SCRIPT ###\n{}exit 0\n",
            announce_and_wait(&started, &release)
        ),
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
    let scanning = {
        let db = db.clone();
        let mut context = scan(data.path(), &input);
        tokio::spawn(async move {
            let results = run_event(&db, &mut context, "scan-callback", None, Some(supervisor()))
                .await
                .unwrap();
            (results, context)
        })
    };

    let me = wait_at_gate(started).await;
    let live = db.script_run_for_token(&me.token).unwrap();
    assert_eq!(live.job_id, None);
    assert_eq!(live.event, ScriptEventLabel::Scan);
    db.script_run_action(
        &me.run_id,
        RunAction::Command(Directive::Category("movies".into())),
    )
    .await
    .unwrap();
    db.script_run_action(&me.run_id, RunAction::Command(Directive::Paused(true)))
        .await
        .unwrap();
    open_gate(release).await;

    let (results, context) = scanning.await.unwrap();
    assert_eq!(
        results
            .iter()
            .map(|result| result.status)
            .collect::<Vec<_>>(),
        [ScriptStatus::Succeeded, ScriptStatus::Succeeded]
    );
    // The same as `[NZB] CATEGORY=movies` and `[NZB] PAUSED=1` would have done.
    assert_eq!(context.category.as_deref(), Some("movies"));
    assert_eq!(context.env["NZBNP_PAUSED"], "1");
    assert!(
        context
            .facts
            .parameters
            .contains(&("Category".into(), "movies".into()))
    );
}

#[tokio::test]
async fn a_post_processing_script_that_says_it_failed_fails_though_it_exits_zero() {
    let (db, data) = setup();
    let context = job(&db, data.path());
    let started = gate(data.path(), "started");
    let release = gate(data.path(), "release");
    let name = script(
        &db,
        "fails.sh",
        &format!(
            "#!/bin/sh\n### NZBGET POST-PROCESSING SCRIPT ###\n{}exit 0\n",
            announce_and_wait(&started, &release)
        ),
    );
    let instances = select(&db, vec![on(InstanceTrigger::PostProcessing, &name)]);
    let pass = {
        let executor = executor(&db);
        tokio::spawn(async move {
            executor
                .execute_job(42, instances, context, None, None)
                .await
        })
    };

    let me = wait_at_gate(started).await;
    let live = db.script_run_for_token(&me.token).unwrap();
    assert_eq!(live.job_id, Some(42));
    assert_eq!(live.event, ScriptEventLabel::PostProcessing);
    db.script_run_action(&me.run_id, RunAction::Command(Directive::MarkBad))
        .await
        .unwrap();
    assert!(db.job_script_effects(42).unwrap().marked_bad);
    db.script_run_action(
        &me.run_id,
        RunAction::Fail("the archive is incomplete".into()),
    )
    .await
    .unwrap();
    open_gate(release).await;

    let report = pass.await.unwrap().unwrap();
    assert_eq!(report.summary, PostProcessingSummary::Failed);
    assert_eq!(report.results.len(), 1);
    assert_eq!(report.results[0].status, ScriptStatus::Failed);
    assert_eq!(report.results[0].exit_code, Some(0));
    assert_eq!(
        report.results[0].error_message.as_deref(),
        Some("the archive is incomplete")
    );
    assert!(db.job_script_effects(42).unwrap().marked_bad);
}

#[tokio::test]
async fn a_logged_line_joins_the_output_and_the_token_never_shows_in_it() {
    let (db, data) = setup();
    let context = job(&db, data.path());
    let started = gate(data.path(), "started");
    let release = gate(data.path(), "release");
    let name = script(
        &db,
        "logs.sh",
        &format!(
            "#!/bin/sh\n### NZBGET POST-PROCESSING SCRIPT ###\nprintf 'before\\n'\n{}printf 'after\\n%s\\n' \"$WEAVER_RUN_TOKEN\"\nexit 0\n",
            announce_and_wait(&started, &release)
        ),
    );
    let instances = select(&db, vec![on(InstanceTrigger::PostProcessing, &name)]);
    let pass = {
        let executor = executor(&db);
        tokio::spawn(async move {
            executor
                .execute_job(42, instances, context, None, None)
                .await
        })
    };

    let me = wait_at_gate(started).await;
    db.script_run_action(
        &me.run_id,
        RunAction::Log {
            level: ScriptLogLevel::Warning,
            text: format!("logged with {}", me.token),
        },
    )
    .await
    .unwrap();
    open_gate(release).await;

    let report = pass.await.unwrap().unwrap();
    assert_eq!(report.summary, PostProcessingSummary::Succeeded);
    let output = &report.results[0].output_tail;
    assert!(!output.contains(&me.token), "{output}");
    // The logged line was in before the script was let past its gate, so it
    // comes ahead of everything printed after it.
    let logged = output.find("logged with [REDACTED]\n").expect(output);
    let after = output.find("after\n[REDACTED]\n").expect(output);
    assert!(logged < after, "{output}");
    // When the script's own line before the gate is taken from its pipe is up
    // to the reader, so only that it is there is certain here.
    assert!(output.contains("before\n"), "{output}");
}

#[tokio::test]
async fn a_test_run_reports_what_its_script_asks_for_and_applies_none_of_it() {
    let (db, data) = setup();
    let config = config(data.path());
    let started = gate(data.path(), "started");
    let release = gate(data.path(), "release");
    let name = script(
        &db,
        "tested.sh",
        &format!(
            "#!/bin/sh\n### NZBGET QUEUE SCRIPT ###\nprintf 'before\\n'\n{}printf 'after\\n'\nexit 0\n",
            announce_and_wait(&started, &release)
        ),
    );
    let instance = select(&db, vec![on(DOWNLOADED, &name)]).remove(0);
    let test = start_script_test(&db, &config, instance.id.clone(), Some(supervisor()))
        .await
        .unwrap();

    let me = wait_at_gate(started).await;
    let live = db.script_run_for_token(&me.token).unwrap();
    assert_eq!(live.run_id, test.id);
    assert_eq!(live.instance_id, instance.id);
    // The made-up download the script was handed as `WEAVER_JOB_ID`.
    assert_eq!(live.job_id, Some(2_000_000_000));
    assert!(live.test);

    // Wait for the script's own line, so the logged one is known to follow it.
    db.script_test_when(&test.id, |run| run.log.contains("before\n"))
        .await
        .unwrap();
    db.script_run_action(&me.run_id, parameter("Key", "value"))
        .await
        .unwrap();
    db.script_run_action(
        &me.run_id,
        RunAction::Log {
            level: ScriptLogLevel::Info,
            text: "logged".into(),
        },
    )
    .await
    .unwrap();
    db.script_run_action(&me.run_id, RunAction::Fail("not this time".into()))
        .await
        .unwrap();
    let running = db.script_test(&test.id).unwrap();
    assert_eq!(running.outcome, None);
    assert_eq!(running.log, "before\nlogged\n");
    assert_eq!(running.commands, ["NZBPR_Key=value", "FAIL=not this time"]);

    open_gate(release).await;
    let run = db
        .script_test_when(&test.id, |run| run.outcome.is_some())
        .await
        .unwrap();
    let outcome = run.outcome.unwrap();
    assert_eq!(outcome.status, ScriptStatus::Failed);
    assert_eq!(outcome.exit_code, Some(0));
    assert_eq!(outcome.error_message.as_deref(), Some("not this time"));
    let logged = run.log.find("logged\n").expect(&run.log);
    let after = run.log.find("after\n").expect(&run.log);
    assert!(logged < after, "{}", run.log);
    assert_eq!(run.commands, ["NZBPR_Key=value", "FAIL=not this time"]);
    // Nothing reached a download, made up or real.
    assert!(
        db.job_script_effects(2_000_000_000)
            .unwrap()
            .parameters
            .is_empty()
    );
    assert_eq!(db.script_run_for_token(&me.token), None);
}

#[tokio::test]
async fn a_script_is_handed_no_token_while_weaver_has_no_address_for_it() {
    let (db, data) = setup_without_api();
    let context = job(&db, data.path());
    let started = gate(data.path(), "started");
    let release = gate(data.path(), "release");
    let name = script(
        &db,
        "unreachable.sh",
        &format!(
            "#!/bin/sh\n### NZBGET QUEUE SCRIPT ###\n{}exit 0\n",
            announce_and_wait(&started, &release)
        ),
    );
    select(&db, vec![on(DOWNLOADED, &name)]);
    let event = {
        let db = db.clone();
        let mut event = EventContext::from_job(&context, QueueEvent::NzbDownloaded);
        tokio::spawn(async move {
            run_event(&db, &mut event, "queue-no-api", None, Some(supervisor()))
                .await
                .unwrap()
        })
    };

    let me = wait_at_gate(started).await;
    assert_eq!(me.token, "unset");
    assert_eq!(me.api_url, "unset");
    assert!(db.live_script_run(&me.run_id).is_some());
    open_gate(release).await;

    let results = event.await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].status, ScriptStatus::Succeeded);
    assert_eq!(results[0].error_message, None);
}

#[tokio::test]
async fn a_cancelled_run_stays_cancelled_though_its_script_said_it_failed() {
    let (db, data) = setup();
    let context = job(&db, data.path());
    let started = gate(data.path(), "started");
    // Never opened: only the cancel can end the script.
    let release = gate(data.path(), "release");
    let name = script(
        &db,
        "cancelled.sh",
        &format!(
            "#!/bin/sh\n### NZBGET QUEUE SCRIPT ###\n{}exit 0\n",
            announce_and_wait(&started, &release)
        ),
    );
    select(&db, vec![on(DOWNLOADED, &name)]);
    let (cancel, cancellation) = tokio::sync::watch::channel(false);
    let event = {
        let db = db.clone();
        let mut event = EventContext::from_job(&context, QueueEvent::NzbDownloaded);
        tokio::spawn(async move {
            run_event(
                &db,
                &mut event,
                "queue-cancelled",
                Some(cancellation),
                Some(supervisor()),
            )
            .await
            .unwrap()
        })
    };

    let me = wait_at_gate(started).await;
    db.script_run_action(&me.run_id, RunAction::Fail("giving up".into()))
        .await
        .unwrap();
    cancel.send_replace(true);

    let results = event.await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].status, ScriptStatus::Cancelled);
    assert_ne!(results[0].error_message.as_deref(), Some("giving up"));
    assert_eq!(db.script_run_for_token(&me.token), None);
}
