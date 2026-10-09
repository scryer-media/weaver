#![cfg(unix)]

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::sync::RwLock;
use weaver_server_core::Database;
use weaver_server_core::post_processing::model::{
    OptionName, OptionValue, PostProcessingSettings, QueueEvent, ResolvedOption, ScriptAdapter,
    ScriptEventLabel, ScriptName, ScriptStatus, SecretOptionValue,
};
use weaver_server_core::post_processing::output::ScriptRunFilter;
use weaver_server_core::post_processing::test_run::{
    ScriptTestError, ScriptTestRequest, ScriptTestSnapshot, TestTrigger, start_script_test,
};
use weaver_server_core::settings::{Config, SharedConfig};

struct Fixture {
    db: Database,
    config: SharedConfig,
    data: tempfile::TempDir,
}

fn fixture(execution_enabled: bool) -> Fixture {
    let db = Database::open_in_memory().unwrap();
    let data = tempfile::tempdir().unwrap();
    db.initialize_post_processing_script_directory(data.path(), None)
        .unwrap();
    db.save_post_processing_settings(&PostProcessingSettings {
        execution_enabled,
        ..Default::default()
    })
    .unwrap();
    let config = Config {
        data_dir: data.path().to_string_lossy().into_owned(),
        intermediate_dir: None,
        complete_dir: None,
        buffer_pool: None,
        servers: vec![],
        categories: vec![],
        retry: None,
        max_download_speed: None,
        cleanup_after_extract: None,
        isp_bandwidth_cap: None,
        propagation_delay_secs: None,
        watch_folder: weaver_server_core::watch_folder::WatchFolderConfig::default(),
        duplicate_policy: weaver_server_core::jobs::DuplicatePolicy::default(),
        direct_store: None,
        direct_unpack: None,
        delivery_naming: None,
        hardware_profile: None,
        metrics: Default::default(),
        config_path: None,
    };
    Fixture {
        db,
        config: Arc::new(RwLock::new(config)),
        data,
    }
}

fn script(fixture: &Fixture, name: &str, body: &str) -> ScriptName {
    let root = fixture.db.post_processing_script_directory().unwrap();
    fs::write(root.join(name), body).unwrap();
    fs::set_permissions(root.join(name), fs::Permissions::from_mode(0o755)).unwrap();
    ScriptName::new(name).unwrap()
}

fn supervisor() -> Option<PathBuf> {
    Some(PathBuf::from(env!("CARGO_BIN_EXE_weaver")))
}

/// A pipe a test script stops at for good: nothing ever writes to it, so the
/// script stays there until its run is cancelled.
fn gate(directory: &Path, name: &str) -> PathBuf {
    let path = directory.join(name);
    let made = std::process::Command::new("mkfifo")
        .arg(&path)
        .status()
        .unwrap();
    assert!(made.success());
    path
}

async fn start(
    fixture: &Fixture,
    script: &ScriptName,
    trigger: TestTrigger,
) -> Result<ScriptTestSnapshot, ScriptTestError> {
    start_script_test(
        &fixture.db,
        &fixture.config,
        ScriptTestRequest {
            script: script.clone(),
            trigger,
            category: None,
        },
        supervisor(),
    )
    .await
}

/// The test run `id` once its script has ended, however long that takes.
async fn ended(fixture: &Fixture, id: &str) -> ScriptTestSnapshot {
    fixture
        .db
        .script_test_when(id, |run| run.outcome.is_some())
        .await
        .expect("the test run is kept")
}

async fn run_to_end(
    fixture: &Fixture,
    script: &ScriptName,
    trigger: TestTrigger,
) -> ScriptTestSnapshot {
    let started = start(fixture, script, trigger).await.unwrap();
    ended(fixture, &started.id).await
}

fn status(run: &ScriptTestSnapshot) -> ScriptStatus {
    run.outcome.as_ref().expect("the run has ended").status
}

fn input<'a>(run: &'a ScriptTestSnapshot, name: &str) -> Option<&'a str> {
    run.inputs
        .iter()
        .find(|(input, _)| input == name)
        .map(|(_, value)| value.as_str())
}

/// The scratch directories test runs have left under the data directory.
fn scratch_directories(fixture: &Fixture) -> Vec<PathBuf> {
    fs::read_dir(fixture.data.path())
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.file_name()
                .is_some_and(|name| name.to_string_lossy().starts_with("script-test-"))
        })
        .collect()
}

#[tokio::test]
async fn a_post_processing_test_hands_over_a_made_up_download_and_leaves_nothing_behind() {
    let fixture = fixture(true);
    let script = script(
        &fixture,
        "tidy.sh",
        "#!/bin/sh\n### NZBGET POST-PROCESSING SCRIPT ###\n\
         printf 'name=%s status=%s\\n' \"$NZBPP_NZBNAME\" \"$NZBPP_TOTALSTATUS\"\n\
         cat \"$NZBPP_DIRECTORY/weaver-test.txt\"\n\
         printf '[NZB] FINALDIR=/elsewhere\\n[NZB] MARK=BAD\\n[NZB] NZBPR_Seen=yes\\n'\n\
         exit 93\n",
    );

    let started = start(&fixture, &script, TestTrigger::PostProcessing)
        .await
        .unwrap();
    assert_eq!(started.script, script);
    assert_eq!(started.event, ScriptEventLabel::PostProcessing);
    assert_eq!(started.adapter, ScriptAdapter::Nzbget);
    assert_eq!(started.outcome, None);
    assert_eq!(
        input(&started, "NZBPP_NZBNAME"),
        Some("Weaver.Test.Download")
    );
    assert_eq!(input(&started, "NZBPP_TOTALSTATUS"), Some("SUCCESS"));
    let directory = PathBuf::from(input(&started, "NZBPP_DIRECTORY").unwrap());
    assert!(
        directory.starts_with(fixture.data.path()),
        "the made-up download lives under the data directory: {directory:?}"
    );

    let run = ended(&fixture, &started.id).await;
    assert_eq!(status(&run), ScriptStatus::Succeeded);
    assert_eq!(run.outcome.as_ref().unwrap().exit_code, Some(93));
    assert!(run.log.contains("name=Weaver.Test.Download status=SUCCESS"));
    assert!(run.log.contains("Made up by weaver for a script test run."));
    assert!(!run.log.contains("[NZB]"), "commands are not log lines");
    // Reported in the order the script issued them, and applied to nothing.
    assert_eq!(
        run.commands,
        ["FINALDIR=/elsewhere", "MARK=BAD", "NZBPR_Seen=yes"]
    );
    assert!(!run.commands_truncated);

    assert!(
        fixture
            .db
            .script_runs(ScriptRunFilter::default(), None, 10)
            .unwrap()
            .is_empty(),
        "a test run is not a recorded run"
    );
    assert!(
        fixture
            .db
            .job_post_processing_results(2_000_000_000)
            .unwrap()
            .is_empty()
    );
    assert!(!directory.exists());
    assert_eq!(scratch_directories(&fixture), Vec::<PathBuf>::new());
    assert_eq!(fixture.db.script_test(&run.id), Some(run));
}

#[tokio::test]
async fn a_script_without_a_header_is_tested_with_its_positional_arguments() {
    let fixture = fixture(true);
    let script = script(
        &fixture,
        "plain.sh",
        "#!/bin/sh\nprintf 'directory=%s name=%s status=%s\\n' \"$1\" \"$3\" \"$7\"\nexit 0\n",
    );

    let run = run_to_end(&fixture, &script, TestTrigger::PostProcessing).await;

    assert_eq!(run.adapter, ScriptAdapter::Sabnzbd);
    assert_eq!(status(&run), ScriptStatus::Succeeded);
    assert_eq!(run.arguments.len(), 8);
    assert_eq!(run.arguments[2], "Weaver.Test.Download");
    assert_eq!(
        run.log.trim(),
        format!(
            "directory={} name=Weaver.Test.Download status=0",
            run.arguments[0]
        )
    );
}

#[tokio::test]
async fn saved_options_reach_the_script_and_stay_out_of_what_a_test_reports() {
    let fixture = fixture(true);
    let root = fixture.db.post_processing_script_directory().unwrap();
    let package = root.join("email");
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
        "#!/bin/sh\n\
         [ \"$NZBPO_Token\" = hunter2 ] && printf 'token delivered\\n'\n\
         printf 'host=%s token=%s\\n' \"$NZBPO_Host\" \"$NZBPO_Token\"\n\
         exit 93\n",
    )
    .unwrap();
    fs::set_permissions(package.join("run.sh"), fs::Permissions::from_mode(0o755)).unwrap();
    let script = ScriptName::new("email").unwrap();
    fixture
        .db
        .save_post_processing_script_options(
            &script,
            &[ResolvedOption::new(
                OptionName::new("Token").unwrap(),
                OptionValue::Secret(SecretOptionValue::from_admin_input("hunter2")),
            )],
        )
        .unwrap();

    let run = run_to_end(&fixture, &script, TestTrigger::PostProcessing).await;

    assert_eq!(status(&run), ScriptStatus::Succeeded);
    assert!(run.log.contains("token delivered"));
    assert!(run.log.contains("host=mail.example.invalid"));
    assert!(
        !run.log.contains("hunter2"),
        "a secret never reaches the log"
    );
    assert!(!run.inputs.is_empty());
    for (name, value) in &run.inputs {
        assert!(
            !name.starts_with("NZBPO_") && !name.starts_with("NZBOP_"),
            "{name} was not made up for the test"
        );
        assert!(!value.contains("hunter2"));
    }
}

#[tokio::test]
async fn a_running_test_shows_its_output_and_ends_when_cancelled() {
    let fixture = fixture(true);
    let gate = gate(fixture.data.path(), "gate");
    let script = script(
        &fixture,
        "slow.sh",
        &format!(
            "#!/bin/sh\nprintf 'before the gate\\n'\nread line < '{}'\nprintf 'after the gate\\n'\nexit 0\n",
            gate.display()
        ),
    );

    let started = start(&fixture, &script, TestTrigger::PostProcessing)
        .await
        .unwrap();
    let running = fixture
        .db
        .script_test_when(&started.id, |run| run.log.contains("before the gate"))
        .await
        .unwrap();
    assert_eq!(running.outcome, None, "the script is still at the gate");
    assert_eq!(scratch_directories(&fixture).len(), 1);

    assert!(fixture.db.cancel_script_test(&started.id));
    let run = ended(&fixture, &started.id).await;

    assert_eq!(status(&run), ScriptStatus::Cancelled);
    assert!(run.log.contains("before the gate"));
    assert!(!run.log.contains("after the gate"));
    assert_eq!(scratch_directories(&fixture), Vec::<PathBuf>::new());
    assert!(
        !fixture.db.cancel_script_test(&started.id),
        "a run that has ended has nothing to cancel"
    );
    assert!(!fixture.db.cancel_script_test("no-such-run"));
}

#[tokio::test]
async fn each_trigger_is_tested_with_the_variables_a_real_one_carries() {
    let fixture = fixture(true);
    let script = script(
        &fixture,
        "everything.sh",
        "#!/bin/sh\n### NZBGET QUEUE/SCAN/SCHEDULER/FEED SCRIPT ###\n\
         printf 'event=%s delete=%s mark=%s url=%s\\n' \"$NZBNA_EVENT\" \"$NZBNA_DELETESTATUS\" \"$NZBNA_MARKSTATUS\" \"$NZBNA_URLSTATUS\"\n\
         printf 'task=%s feed=%s\\n' \"$NZBSP_TASKID\" \"$NZBFP_FEEDID\"\n\
         [ -n \"$NZBNP_FILENAME\" ] && grep -c '<segment ' \"$NZBNP_FILENAME\" | sed 's/^/scan segments=/'\n\
         [ -n \"$NZBFP_FILENAME\" ] && grep -c '<item>' \"$NZBFP_FILENAME\" | sed 's/^/feed items=/'\n\
         printf '[NZB] CATEGORY=movies\\n'\n\
         exit 93\n",
    );

    let deleted = run_to_end(
        &fixture,
        &script,
        TestTrigger::Queue(QueueEvent::NzbDeleted),
    )
    .await;
    assert_eq!(
        deleted.event,
        ScriptEventLabel::Queue(QueueEvent::NzbDeleted)
    );
    assert_eq!(status(&deleted), ScriptStatus::Succeeded);
    assert!(
        deleted
            .log
            .contains("event=NZB_DELETED delete=MANUAL mark=NONE url=NONE")
    );
    assert_eq!(
        input(&deleted, "NZBNA_NZBNAME"),
        Some("Weaver.Test.Download")
    );
    assert!(deleted.arguments.is_empty());
    // A queue script may not file a download under a category, so the line
    // is reported as any real run would report it.
    assert!(deleted.commands.is_empty());
    assert!(
        deleted
            .log
            .contains("Command CATEGORY is not allowed for queue:NZB_DELETED")
    );

    let marked = run_to_end(&fixture, &script, TestTrigger::Queue(QueueEvent::NzbMarked)).await;
    assert!(
        marked
            .log
            .contains("event=NZB_MARKED delete=NONE mark=BAD url=NONE")
    );

    let fetched = run_to_end(
        &fixture,
        &script,
        TestTrigger::Queue(QueueEvent::UrlCompleted),
    )
    .await;
    assert!(
        fetched
            .log
            .contains("event=URL_COMPLETED delete=NONE mark=NONE url=SUCCESS")
    );
    assert_eq!(
        input(&fetched, "NZBNA_URL"),
        Some("https://example.invalid/Weaver.Test.Download.nzb")
    );

    let scan = run_to_end(&fixture, &script, TestTrigger::Scan).await;
    assert_eq!(scan.event, ScriptEventLabel::Scan);
    assert_eq!(status(&scan), ScriptStatus::Succeeded);
    assert!(scan.log.contains("scan segments=1"));
    assert_eq!(scan.commands, ["CATEGORY=movies"]);
    assert_eq!(
        input(&scan, "NZBNP_NZBNAME"),
        Some("Weaver.Test.Download.nzb")
    );

    let scheduled = run_to_end(&fixture, &script, TestTrigger::Scheduler).await;
    assert_eq!(scheduled.event, ScriptEventLabel::Scheduler(0));
    assert_eq!(status(&scheduled), ScriptStatus::Succeeded);
    assert!(scheduled.log.contains("task=0 feed=\n"));
    assert_eq!(scheduled.inputs, [("NZBSP_TASKID".into(), "0".into())]);

    let feed = run_to_end(&fixture, &script, TestTrigger::Feed).await;
    assert_eq!(feed.event, ScriptEventLabel::Feed(0));
    assert_eq!(status(&feed), ScriptStatus::Succeeded);
    assert!(feed.log.contains("task= feed=0\n"));
    assert!(feed.log.contains("feed items=1"));

    assert_eq!(scratch_directories(&fixture), Vec::<PathBuf>::new());
    assert!(
        fixture
            .db
            .script_runs(ScriptRunFilter::default(), None, 10)
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn a_feed_script_that_does_not_report_success_fails_its_test() {
    let fixture = fixture(true);
    let script = script(
        &fixture,
        "feed.sh",
        "#!/bin/sh\n### NZBGET FEED SCRIPT ###\nexit 0\n",
    );

    let run = run_to_end(&fixture, &script, TestTrigger::Feed).await;

    assert_eq!(status(&run), ScriptStatus::Failed);
    assert_eq!(run.outcome.unwrap().exit_code, Some(0));
}

#[tokio::test]
async fn a_test_is_refused_while_scripts_may_not_run() {
    let fixture = fixture(false);
    let script = script(&fixture, "plain.sh", "#!/bin/sh\nexit 0\n");

    let refused = start(&fixture, &script, TestTrigger::PostProcessing).await;

    assert!(
        matches!(refused, Err(ScriptTestError::Refused(_))),
        "{refused:?}"
    );
    assert_eq!(scratch_directories(&fixture), Vec::<PathBuf>::new());
}

#[tokio::test]
async fn a_test_is_refused_for_a_trigger_the_script_does_not_declare() {
    let fixture = fixture(true);
    let plain = script(&fixture, "plain.sh", "#!/bin/sh\nexit 0\n");
    let added = script(
        &fixture,
        "added.sh",
        "#!/bin/sh\n### NZBGET QUEUE SCRIPT ###\n### QUEUE EVENTS: NZB_ADDED ###\nexit 0\n",
    );

    let scan = start(&fixture, &plain, TestTrigger::Scan).await;
    assert!(
        matches!(scan, Err(ScriptTestError::NotDeclared { .. })),
        "{scan:?}"
    );
    let post_processing = start(&fixture, &added, TestTrigger::PostProcessing).await;
    assert!(
        matches!(post_processing, Err(ScriptTestError::NotDeclared { .. })),
        "{post_processing:?}"
    );
    let deleted = start(&fixture, &added, TestTrigger::Queue(QueueEvent::NzbDeleted)).await;
    assert_eq!(
        deleted.unwrap_err().to_string(),
        "script 'added.sh' does not declare queue:NZB_DELETED"
    );
    let missing = start(
        &fixture,
        &ScriptName::new("missing.sh").unwrap(),
        TestTrigger::PostProcessing,
    )
    .await;
    assert!(
        matches!(missing, Err(ScriptTestError::Unavailable(_))),
        "{missing:?}"
    );

    let declared = run_to_end(&fixture, &added, TestTrigger::Queue(QueueEvent::NzbAdded)).await;
    assert_eq!(status(&declared), ScriptStatus::Succeeded);
    assert_eq!(scratch_directories(&fixture), Vec::<PathBuf>::new());
}

#[tokio::test]
async fn one_test_more_than_may_run_at_once_is_refused() {
    let fixture = fixture(true);
    let gate = gate(fixture.data.path(), "gate");
    let script = script(
        &fixture,
        "slow.sh",
        &format!("#!/bin/sh\nread line < '{}'\nexit 0\n", gate.display()),
    );

    let mut running = Vec::new();
    for _ in 0..4 {
        running.push(
            start(&fixture, &script, TestTrigger::PostProcessing)
                .await
                .unwrap()
                .id,
        );
    }
    let refused = start(&fixture, &script, TestTrigger::PostProcessing).await;
    assert!(matches!(refused, Err(ScriptTestError::Busy)), "{refused:?}");
    assert_eq!(
        scratch_directories(&fixture).len(),
        4,
        "a refused test leaves no scratch directory"
    );

    assert!(fixture.db.cancel_script_test(&running[0]));
    assert_eq!(
        status(&ended(&fixture, &running[0]).await),
        ScriptStatus::Cancelled
    );
    // The place the cancelled run held is free again.
    let next = start(&fixture, &script, TestTrigger::PostProcessing)
        .await
        .unwrap()
        .id;
    running.push(next);

    for id in &running[1..] {
        assert!(fixture.db.cancel_script_test(id));
    }
    for id in &running[1..] {
        assert_eq!(status(&ended(&fixture, id).await), ScriptStatus::Cancelled);
    }
    assert_eq!(scratch_directories(&fixture), Vec::<PathBuf>::new());
}

#[tokio::test]
async fn only_the_last_few_ended_tests_are_kept() {
    let fixture = fixture(true);
    let script = script(&fixture, "plain.sh", "#!/bin/sh\nexit 0\n");

    let mut runs = Vec::new();
    for _ in 0..17 {
        runs.push(
            run_to_end(&fixture, &script, TestTrigger::PostProcessing)
                .await
                .id,
        );
    }
    // Room is made as a run is admitted: with seventeen ended runs kept, the
    // next one pushes the oldest out.
    let last = run_to_end(&fixture, &script, TestTrigger::PostProcessing)
        .await
        .id;

    assert_eq!(fixture.db.script_test(&runs[0]), None);
    assert!(fixture.db.script_test(&runs[2]).is_some());
    assert!(fixture.db.script_test(&last).is_some());
}
