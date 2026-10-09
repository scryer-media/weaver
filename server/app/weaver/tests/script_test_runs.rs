#![cfg(unix)]

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::sync::RwLock;
use weaver_server_core::Database;
use weaver_server_core::post_processing::instances::{
    InstanceTrigger, ScriptInstance, ScriptInstanceDraft,
};
use weaver_server_core::post_processing::model::{
    PostProcessingSettings, QueueEvent, ScriptAdapter, ScriptEventLabel, ScriptName, ScriptStatus,
};
use weaver_server_core::post_processing::output::ScriptRunFilter;
use weaver_server_core::post_processing::test_run::{
    ScriptTestError, ScriptTestSnapshot, start_script_test,
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

/// A saved instance of `script` that `trigger` starts.
fn instance(fixture: &Fixture, script: &ScriptName, trigger: InstanceTrigger) -> ScriptInstance {
    fixture
        .db
        .create_script_instance(ScriptInstanceDraft::new(script.clone(), trigger))
        .unwrap()
}

async fn start(
    fixture: &Fixture,
    instance: &ScriptInstance,
) -> Result<ScriptTestSnapshot, ScriptTestError> {
    start_script_test(
        &fixture.db,
        &fixture.config,
        instance.id.clone(),
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

async fn run_to_end(fixture: &Fixture, instance: &ScriptInstance) -> ScriptTestSnapshot {
    let started = start(fixture, instance).await.unwrap();
    ended(fixture, &started.id).await
}

/// Test a new instance of `script` on `trigger` and return the run once it
/// has ended.
async fn test_on(
    fixture: &Fixture,
    script: &ScriptName,
    trigger: InstanceTrigger,
) -> ScriptTestSnapshot {
    run_to_end(fixture, &instance(fixture, script, trigger)).await
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
async fn a_test_run_is_held_to_the_limit_a_real_run_of_its_instance_gets() {
    let fixture = fixture(true);
    let script = script(
        &fixture,
        "limits.sh",
        "#!/bin/sh\n### NZBGET POST-PROCESSING SCRIPT ###\n### NZBGET QUEUE SCRIPT ###\n### QUEUE EVENTS: NZB_ADDED ###\nexit 93\n",
    );
    let settings = fixture.db.post_processing_settings().unwrap();
    for (trigger, expected) in [
        (
            InstanceTrigger::PostProcessing,
            weaver_server_core::post_processing::runner::DEFAULT_TIMEOUT,
        ),
        (
            InstanceTrigger::Queue(QueueEvent::NzbAdded),
            std::time::Duration::from_secs(settings.event_scripts.event_script_timeout_seconds),
        ),
    ] {
        let instance = instance(&fixture, &script, trigger);
        assert_eq!(instance.timeout_seconds, None);
        assert_eq!(instance.time_limit(&settings), expected);
        let run = run_to_end(&fixture, &instance).await;
        assert_eq!(run.timeout_seconds, expected.as_secs(), "{trigger:?}");
        assert_eq!(status(&run), ScriptStatus::Succeeded);
    }
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

    let tidy = fixture
        .db
        .create_script_instance(
            ScriptInstanceDraft::new(script.clone(), InstanceTrigger::PostProcessing)
                .named("Tidy up"),
        )
        .unwrap();

    let started = start(&fixture, &tidy).await.unwrap();
    assert_eq!(started.instance_id, tidy.id);
    assert_eq!(started.instance_name, "Tidy up");
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

    let run = test_on(&fixture, &script, InstanceTrigger::PostProcessing).await;

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
async fn saved_inputs_reach_the_script_and_stay_out_of_what_a_test_reports() {
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
         [ \"$NZBPO_Pass\" = hunter4 ] && printf 'own secret delivered\\n'\n\
         printf 'host=%s token=%s pass=%s\\n' \"$NZBPO_Host\" \"$NZBPO_Token\" \"$NZBPO_Pass\"\n\
         exit 93\n",
    )
    .unwrap();
    fs::set_permissions(package.join("run.sh"), fs::Permissions::from_mode(0o755)).unwrap();
    let script = ScriptName::new("email").unwrap();
    let token = fixture.db.create_secret("Mail token", "hunter2").unwrap();
    let email = fixture
        .db
        .create_script_instance(
            ScriptInstanceDraft::new(script, InstanceTrigger::PostProcessing)
                .input("Host", "mail.example.invalid")
                .secret_input("Token", &token.id)
                .sealed_input("Pass", "hunter4"),
        )
        .unwrap();

    let run = run_to_end(&fixture, &email).await;

    assert_eq!(status(&run), ScriptStatus::Succeeded);
    assert!(run.log.contains("token delivered"));
    assert!(run.log.contains("own secret delivered"));
    assert!(run.log.contains("host=mail.example.invalid"));
    assert!(
        !run.log.contains("hunter2") && !run.log.contains("hunter4"),
        "a secret never reaches the log"
    );
    assert!(!run.inputs.is_empty());
    for (name, value) in &run.inputs {
        assert!(
            ["NZBPO_", "NZBOP_", "SAB_OPTION_", "WEAVER_INPUT_"]
                .iter()
                .all(|prefix| !name.starts_with(prefix)),
            "{name} was not made up for the test"
        );
        assert!(!value.contains("hunter2") && !value.contains("hunter4"));
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

    let slow = instance(&fixture, &script, InstanceTrigger::PostProcessing);
    let started = start(&fixture, &slow).await.unwrap();
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

/// A shell loop printing `lines` lines of 99 characters and a newline.
fn printing(lines: usize) -> String {
    format!(
        "i=0\nwhile [ $i -lt {lines} ]; do printf '%099d\\n' $i; i=$((i + 1)); done\nprintf 'last line\\n'\n"
    )
}

#[tokio::test]
async fn a_test_says_whether_it_printed_more_than_the_log_keeps() {
    let fixture = fixture(true);
    let (over, under) = (
        script(
            &fixture,
            "over.sh",
            &format!("#!/bin/sh\n{}exit 0\n", printing(400)),
        ),
        script(
            &fixture,
            "under.sh",
            &format!("#!/bin/sh\n{}exit 0\n", printing(100)),
        ),
    );

    let over = test_on(&fixture, &over, InstanceTrigger::PostProcessing).await;
    assert_eq!(status(&over), ScriptStatus::Succeeded);
    assert!(over.log_truncated, "40,010 bytes do not fit in 32 KiB");
    assert!(over.log.len() <= 32 * 1024);
    assert!(over.log.ends_with("last line\n"));

    let under = test_on(&fixture, &under, InstanceTrigger::PostProcessing).await;
    assert!(!under.log_truncated, "10,010 bytes fit");
    assert_eq!(under.log.len(), 100 * 100 + "last line\n".len());
}

#[tokio::test]
async fn a_test_cancelled_after_printing_too_much_still_says_so() {
    let fixture = fixture(true);
    let gate = gate(fixture.data.path(), "gate");
    let script = script(
        &fixture,
        "flood.sh",
        &format!(
            "#!/bin/sh\n{}read line < '{}'\nexit 0\n",
            printing(400),
            gate.display()
        ),
    );

    let flood = instance(&fixture, &script, InstanceTrigger::PostProcessing);
    let started = start(&fixture, &flood).await.unwrap();
    fixture
        .db
        .script_test_when(&started.id, |run| run.log.contains("last line"))
        .await
        .unwrap();
    assert!(fixture.db.cancel_script_test(&started.id));
    let run = ended(&fixture, &started.id).await;

    assert_eq!(status(&run), ScriptStatus::Cancelled);
    assert!(run.log_truncated);
    assert!(run.log.ends_with("last line\n"));
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

    let deleted = test_on(
        &fixture,
        &script,
        InstanceTrigger::Queue(QueueEvent::NzbDeleted),
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

    let marked = test_on(
        &fixture,
        &script,
        InstanceTrigger::Queue(QueueEvent::NzbMarked),
    )
    .await;
    assert!(
        marked
            .log
            .contains("event=NZB_MARKED delete=NONE mark=BAD url=NONE")
    );

    let fetched = test_on(
        &fixture,
        &script,
        InstanceTrigger::Queue(QueueEvent::UrlCompleted),
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

    let scan = test_on(&fixture, &script, InstanceTrigger::Scan).await;
    assert_eq!(scan.event, ScriptEventLabel::Scan);
    assert_eq!(status(&scan), ScriptStatus::Succeeded);
    assert!(scan.log.contains("scan segments=1"));
    assert_eq!(scan.commands, ["CATEGORY=movies"]);
    assert_eq!(
        input(&scan, "NZBNP_NZBNAME"),
        Some("Weaver.Test.Download.nzb")
    );

    let scheduled = test_on(&fixture, &script, InstanceTrigger::Schedule).await;
    assert_eq!(scheduled.event, ScriptEventLabel::Scheduler(0));
    assert_eq!(status(&scheduled), ScriptStatus::Succeeded);
    assert!(scheduled.log.contains("task=0 feed=\n"));
    // A scheduled run is about no download, so little is made up for it.
    assert_eq!(
        scheduled
            .inputs
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>(),
        ["NZBSP_TASKID", "WEAVER_CATEGORY", "WEAVER_DIRECTORY"]
    );
    assert_eq!(input(&scheduled, "NZBSP_TASKID"), Some("0"));

    let feed = test_on(&fixture, &script, InstanceTrigger::Feed).await;
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
async fn a_feed_script_is_read_by_the_exit_codes_every_script_is() {
    let fixture = fixture(true);
    let plain = script(
        &fixture,
        "feed.sh",
        "#!/bin/sh\n### NZBGET FEED SCRIPT ###\nexit 0\n",
    );
    let failing = script(
        &fixture,
        "failing.sh",
        "#!/bin/sh\n### NZBGET FEED SCRIPT ###\nexit 1\n",
    );

    let run = test_on(&fixture, &plain, InstanceTrigger::Feed).await;
    assert_eq!(status(&run), ScriptStatus::Succeeded);
    assert_eq!(run.outcome.unwrap().exit_code, Some(0));

    let run = test_on(&fixture, &failing, InstanceTrigger::Feed).await;
    assert_eq!(status(&run), ScriptStatus::Failed);
    assert_eq!(run.outcome.unwrap().exit_code, Some(1));
}

#[tokio::test]
async fn a_test_is_refused_while_scripts_may_not_run() {
    let fixture = fixture(false);
    let script = script(&fixture, "plain.sh", "#!/bin/sh\nexit 0\n");

    let plain = instance(&fixture, &script, InstanceTrigger::PostProcessing);
    let refused = start(&fixture, &plain).await;

    assert!(
        matches!(refused, Err(ScriptTestError::Refused(_))),
        "{refused:?}"
    );
    assert_eq!(scratch_directories(&fixture), Vec::<PathBuf>::new());
}

#[tokio::test]
async fn an_instance_is_tested_on_its_trigger_whatever_its_script_declares() {
    let fixture = fixture(true);
    let plain = script(&fixture, "plain.sh", "#!/bin/sh\nexit 0\n");
    let added = script(
        &fixture,
        "added.sh",
        "#!/bin/sh\n### NZBGET QUEUE SCRIPT ###\n### QUEUE EVENTS: NZB_ADDED ###\nprintf 'event=%s\\n' \"$NZBNA_EVENT\"\nexit 0\n",
    );

    let scan = test_on(&fixture, &plain, InstanceTrigger::Scan).await;
    assert_eq!(scan.event, ScriptEventLabel::Scan);
    assert_eq!(status(&scan), ScriptStatus::Succeeded);

    let post_processing = test_on(&fixture, &added, InstanceTrigger::PostProcessing).await;
    assert_eq!(post_processing.event, ScriptEventLabel::PostProcessing);
    assert_eq!(status(&post_processing), ScriptStatus::Succeeded);

    let deleted = test_on(
        &fixture,
        &added,
        InstanceTrigger::Queue(QueueEvent::NzbDeleted),
    )
    .await;
    assert_eq!(status(&deleted), ScriptStatus::Succeeded);
    assert!(deleted.log.contains("event=NZB_DELETED"));

    // An instance that is turned off can still be tested.
    let off = fixture
        .db
        .create_script_instance(
            ScriptInstanceDraft::new(added, InstanceTrigger::Queue(QueueEvent::NzbAdded))
                .disabled(),
        )
        .unwrap();
    let declared = run_to_end(&fixture, &off).await;
    assert_eq!(status(&declared), ScriptStatus::Succeeded);
    assert!(declared.log.contains("event=NZB_ADDED"));
    assert_eq!(scratch_directories(&fixture), Vec::<PathBuf>::new());
}

#[tokio::test]
async fn a_test_needs_an_instance_whose_script_is_there() {
    let fixture = fixture(true);
    let gone = instance(
        &fixture,
        &ScriptName::new("missing.sh").unwrap(),
        InstanceTrigger::PostProcessing,
    );

    let missing = start(&fixture, &gone).await;
    assert!(
        matches!(missing, Err(ScriptTestError::Unavailable(_))),
        "{missing:?}"
    );

    assert!(fixture.db.delete_script_instance(&gone.id).unwrap());
    let deleted = start(&fixture, &gone).await;
    assert!(
        matches!(deleted, Err(ScriptTestError::NotFound)),
        "{deleted:?}"
    );
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

    let slow = instance(&fixture, &script, InstanceTrigger::PostProcessing);
    let mut running = Vec::new();
    for _ in 0..4 {
        running.push(start(&fixture, &slow).await.unwrap().id);
    }
    let refused = start(&fixture, &slow).await;
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
    let next = start(&fixture, &slow).await.unwrap().id;
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

    let plain = instance(&fixture, &script, InstanceTrigger::PostProcessing);
    let mut runs = Vec::new();
    for _ in 0..17 {
        runs.push(run_to_end(&fixture, &plain).await.id);
    }
    // Room is made as a run is admitted: with seventeen ended runs kept, the
    // next one pushes the oldest out.
    let last = run_to_end(&fixture, &plain).await.id;

    assert_eq!(fixture.db.script_test(&runs[0]), None);
    assert!(fixture.db.script_test(&runs[2]).is_some());
    assert!(fixture.db.script_test(&last).is_some());
}
