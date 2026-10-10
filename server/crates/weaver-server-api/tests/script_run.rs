mod common;

use async_graphql::{Request, Response};
use common::{TestHarness, assert_no_errors, response_data};
use serde_json::json;
use weaver_server_api::auth::{CallerIdentity, CallerScope};
use weaver_server_core::post_processing::callbacks::{RunAction, RunRequest, RunRequests};
use weaver_server_core::post_processing::directives::{Directive, DupeMode, ScriptLogLevel};
use weaver_server_core::post_processing::model::{QueueEvent, ScriptEventLabel};
use weaver_server_core::post_processing::runner::RunIdentity;

// A run as the server opens one, with the test standing where the script's
// output is read: whatever the script asks for arrives at what this returns.
fn open(
    harness: &TestHarness,
    run_id: &str,
    job_id: Option<u64>,
    event: ScriptEventLabel,
) -> RunRequests {
    open_run(harness, run_id, job_id, event, false)
}

fn open_run(
    harness: &TestHarness,
    run_id: &str,
    job_id: Option<u64>,
    event: ScriptEventLabel,
    test: bool,
) -> RunRequests {
    let mut identity = RunIdentity {
        run_id: run_id.into(),
        instance_id: "instance-1".into(),
        instance_name: "Tidy up".into(),
        ..Default::default()
    };
    harness
        .db
        .open_script_run(&mut identity, job_id, &event, None, test)
}

// Ask as the script of run `run_id` would, with the token it was handed.
async fn as_run(harness: &TestHarness, run_id: &str, query: &str) -> Response {
    harness
        .schema
        .execute(
            Request::new(query)
                .data(CallerScope::ScriptRun)
                .data(CallerIdentity::ScriptRun(run_id.into())),
        )
        .await
}

// Take the next `count` things the script asks for and agree to each.
async fn agree(requests: &mut RunRequests, count: usize) -> Vec<RunAction> {
    let mut taken = Vec::new();
    for _ in 0..count {
        let RunRequest { action, reply } = requests.next().await;
        reply.send(Ok(())).unwrap();
        taken.push(action);
    }
    taken
}

// The one error a refused request came back with, as its code and message.
fn refused(response: &Response) -> (String, String) {
    assert_eq!(response.errors.len(), 1, "{:?}", response.errors);
    let error = serde_json::to_value(&response.errors[0]).unwrap();
    (
        error["extensions"]["code"]
            .as_str()
            .unwrap_or_default()
            .to_string(),
        error["message"].as_str().unwrap().to_string(),
    )
}

#[tokio::test]
async fn only_a_running_script_has_a_run_of_its_own() {
    let harness = TestHarness::new().await;
    for query in [
        "{ scriptRun { runId } }",
        "mutation { scriptRun { markBad } }",
    ] {
        // An administrator is not a script.
        let (code, _) = refused(&harness.execute(query).await);
        assert_eq!(code, "NOT_A_SCRIPT_RUN", "{query}");
        // A script whose run is over is no longer one either.
        let (code, message) = refused(&as_run(&harness, "run-gone", query).await);
        assert_eq!(code, "SCRIPT_RUN_ENDED", "{query}");
        assert_eq!(message, "the script run has ended");
    }

    let requests = open(&harness, "run-1", None, ScriptEventLabel::Scan);
    assert_no_errors(&as_run(&harness, "run-1", "{ scriptRun { runId } }").await);
    drop(requests);
    let (code, _) = refused(&as_run(&harness, "run-1", "{ scriptRun { runId } }").await);
    assert_eq!(code, "SCRIPT_RUN_ENDED");
}

#[tokio::test]
async fn a_script_reads_its_own_run_and_the_download_it_is_about() {
    let harness = TestHarness::new().await;
    let job_id = harness.submit_test_nzb("CallbackSample").await;
    const RUN: &str =
        "{ scriptRun { runId instanceId instanceName event kind test jobId job { id name } } }";

    let _queue = open(
        &harness,
        "run-queue",
        Some(job_id),
        ScriptEventLabel::Queue(QueueEvent::NzbAdded),
    );
    let response = as_run(&harness, "run-queue", RUN).await;
    assert_no_errors(&response);
    assert_eq!(
        response_data(&response)["scriptRun"],
        json!({
            "runId": "run-queue",
            "instanceId": "instance-1",
            "instanceName": "Tidy up",
            "event": "queue:NZB_ADDED",
            "kind": "QUEUE",
            "test": false,
            "jobId": job_id,
            "job": { "id": job_id, "name": "CallbackSample" },
        })
    );

    // A run that is about no download has none, and one whose download has
    // left the queue no longer has it.
    let _scheduled = open(
        &harness,
        "run-scheduled",
        None,
        ScriptEventLabel::Scheduler(3),
    );
    let _late = open(
        &harness,
        "run-late",
        Some(job_id + 1000),
        ScriptEventLabel::PostProcessing,
    );
    for (run, kind, job) in [
        ("run-scheduled", "SCHEDULER", None),
        ("run-late", "POST_PROCESSING", Some(job_id + 1000)),
    ] {
        let response = as_run(&harness, run, "{ scriptRun { kind jobId job { id } } }").await;
        assert_no_errors(&response);
        assert_eq!(
            response_data(&response)["scriptRun"],
            json!({ "kind": kind, "jobId": job, "job": null }),
            "{run}"
        );
    }
}

#[tokio::test]
async fn each_action_reaches_the_run_as_the_command_it_names() {
    let harness = TestHarness::new().await;

    // A scan script names the download it is looking at.
    let mut scan = open(&harness, "run-scan", None, ScriptEventLabel::Scan);
    let (response, taken) = tokio::join!(
        as_run(
            &harness,
            "run-scan",
            r#"mutation { scriptRun {
                setCategory(category: "tv")
                setName(name: "Renamed")
                setPriority(priority: 50)
                setPaused(paused: true)
                setTop(top: false)
                setParameter(name: "token", value: "from the api")
                setDuplicate(key: "show-1", score: -5, mode: FORCE)
            } }"#,
        ),
        agree(&mut scan, 9),
    );
    assert_no_errors(&response);
    assert_eq!(
        response_data(&response)["scriptRun"],
        json!({
            "setCategory": true,
            "setName": true,
            "setPriority": true,
            "setPaused": true,
            "setTop": true,
            "setParameter": true,
            "setDuplicate": true,
        })
    );
    // The order the script asked in is the order they are taken in.
    assert_eq!(
        taken,
        [
            Directive::Category("tv".into()),
            Directive::Name("Renamed".into()),
            Directive::Priority(50),
            Directive::Paused(true),
            Directive::Top(false),
            Directive::Parameter {
                name: "token".into(),
                value: "from the api".into(),
            },
            Directive::DupeKey("show-1".into()),
            Directive::DupeScore(-5),
            Directive::DupeMode(DupeMode::Force),
        ]
        .map(RunAction::Command)
    );

    // A post-processing script says where the download ended up, or that it
    // is no good.
    let mut post = open(
        &harness,
        "run-post",
        Some(7),
        ScriptEventLabel::PostProcessing,
    );
    let root = harness.config.read().await.complete_dir();
    let moved = std::path::Path::new(&root)
        .join("moved")
        .to_string_lossy()
        .into_owned();
    let final_dir = std::path::Path::new(&root)
        .join("final")
        .to_string_lossy()
        .into_owned();
    let query = format!(
        "mutation {{ scriptRun {{ setDirectory(path: {}) setFinalDirectory(path: {}) markBad }} }}",
        json!(moved),
        json!(final_dir)
    );
    let (response, taken) =
        tokio::join!(as_run(&harness, "run-post", &query,), agree(&mut post, 3),);
    assert_no_errors(&response);
    assert_eq!(
        taken,
        [
            Directive::Directory(moved),
            Directive::FinalDirectory(final_dir),
            Directive::MarkBad,
        ]
        .map(RunAction::Command)
    );

    // Any script may write to its log and say that it failed, whatever
    // started it.
    let mut scheduled = open(
        &harness,
        "run-scheduled",
        None,
        ScriptEventLabel::Scheduler(1),
    );
    let (response, taken) = tokio::join!(
        as_run(
            &harness,
            "run-scheduled",
            r#"mutation { scriptRun {
                log(level: WARNING, text: "disk is nearly full")
                fail(reason: "nothing left to clean")
            } }"#,
        ),
        agree(&mut scheduled, 2),
    );
    assert_no_errors(&response);
    assert_eq!(
        taken,
        [
            RunAction::Log {
                level: ScriptLogLevel::Warning,
                text: "disk is nearly full".into(),
            },
            RunAction::Fail("nothing left to clean".into()),
        ]
    );
}

#[tokio::test]
async fn an_action_the_trigger_does_not_allow_never_reaches_the_run() {
    let harness = TestHarness::new().await;
    let root = json!(harness.config.read().await.complete_dir()).to_string();
    let set_directory = format!("setDirectory(path: {root})");
    let set_final_directory = format!("setFinalDirectory(path: {root})");
    // Nothing takes what these runs are asked: a request that got as far as
    // the run would wait for an answer that never comes.
    let _scheduled = open(
        &harness,
        "run-scheduled",
        None,
        ScriptEventLabel::Scheduler(1),
    );
    let _queue = open(
        &harness,
        "run-queue",
        Some(7),
        ScriptEventLabel::Queue(QueueEvent::NzbAdded),
    );
    let _scan = open(&harness, "run-scan", None, ScriptEventLabel::Scan);

    for (run, action, command, event) in [
        (
            "run-scheduled",
            r#"setCategory(category: "tv")"#,
            "CATEGORY",
            "scheduler:1",
        ),
        (
            "run-scheduled",
            r#"setParameter(name: "token", value: "x")"#,
            "NZBPR",
            "scheduler:1",
        ),
        ("run-scheduled", "markBad", "MARK", "scheduler:1"),
        (
            "run-queue",
            set_directory.as_str(),
            "DIRECTORY",
            "queue:NZB_ADDED",
        ),
        (
            "run-queue",
            "setPriority(priority: 1)",
            "PRIORITY",
            "queue:NZB_ADDED",
        ),
        ("run-scan", "markBad", "MARK", "scan"),
        ("run-scan", set_final_directory.as_str(), "FINALDIR", "scan"),
    ] {
        let response = as_run(
            &harness,
            run,
            &format!("mutation {{ scriptRun {{ {action} }} }}"),
        )
        .await;
        assert_eq!(
            refused(&response),
            (
                "NOT_ALLOWED_FOR_TRIGGER".into(),
                format!("Command {command} is not allowed for {event}")
            ),
            "{run}: {action}"
        );
    }

    // What could not have been written on one `[NZB]` line is not a command
    // here either, and a log entry or a reason has to say something.
    for action in [
        r#"setParameter(name: "", value: "x")"#,
        r#"setParameter(name: "weaver.internal", value: "x")"#,
        r#"setParameter(name: "token", value: "two\nlines")"#,
        r#"setCategory(category: "tv\n[NZB] MARK=BAD")"#,
        "setDuplicate",
        r#"fail(reason: "  ")"#,
        r#"fail(reason: "two\nlines")"#,
    ] {
        let response = as_run(
            &harness,
            "run-scan",
            &format!("mutation {{ scriptRun {{ {action} }} }}"),
        )
        .await;
        assert_eq!(refused(&response).0, "REFUSED", "{action}");
    }
}

#[tokio::test]
async fn what_the_run_makes_of_a_request_is_what_the_script_is_told() {
    let harness = TestHarness::new().await;
    let mut requests = open(
        &harness,
        "run-post",
        Some(7),
        ScriptEventLabel::PostProcessing,
    );
    let query = format!(
        "mutation {{ scriptRun {{ setDirectory(path: {}) }} }}",
        json!(harness.config.read().await.complete_dir())
    );

    // The run could not do it.
    let (response, ()) = tokio::join!(as_run(&harness, "run-post", &query), async {
        let RunRequest { reply, .. } = requests.next().await;
        reply
            .send(Err("the directory is outside the download roots".into()))
            .unwrap();
    });
    assert_eq!(
        refused(&response),
        (
            "REFUSED".into(),
            "the directory is outside the download roots".into()
        )
    );

    // The run ended while the request was waiting for it.
    let (response, ()) = tokio::join!(as_run(&harness, "run-post", &query), async {
        let waiting = requests.next().await;
        drop(requests);
        drop(waiting);
    });
    assert_eq!(refused(&response).0, "SCRIPT_RUN_ENDED");

    // And nothing reaches a run that is over.
    assert_eq!(
        refused(&as_run(&harness, "run-post", &query).await).0,
        "SCRIPT_RUN_ENDED"
    );
}

#[tokio::test]
async fn a_second_run_under_the_same_id_leaves_the_first_alone() {
    let harness = TestHarness::new().await;
    harness.db.set_script_api_url("http://127.0.0.1:1/graphql");
    let mut first = RunIdentity {
        run_id: "run-1".into(),
        instance_id: "instance-1".into(),
        ..Default::default()
    };
    let _first = harness.db.open_script_run(
        &mut first,
        Some(7),
        &ScriptEventLabel::PostProcessing,
        None,
        false,
    );
    let first_token = first.token.clone().expect("the first run can call back");

    let mut second = RunIdentity {
        run_id: "run-1".into(),
        instance_id: "instance-2".into(),
        ..Default::default()
    };
    let second_requests = harness.db.open_script_run(
        &mut second,
        Some(8),
        &ScriptEventLabel::PostProcessing,
        None,
        false,
    );
    // The second goes ahead without calling back.
    assert_eq!((second.token, second.api_url), (None, None));
    let live = harness.db.live_script_run("run-1").unwrap();
    assert_eq!(
        (live.instance_id.as_str(), live.job_id),
        ("instance-1", Some(7))
    );

    // Its end is not the end of the first.
    drop(second_requests);
    assert_eq!(
        harness
            .db
            .script_run_for_token(&first_token)
            .map(|run| run.instance_id),
        Some("instance-1".to_string())
    );
}

#[tokio::test]
async fn a_script_run_reads_only_allowed_queries_and_changes_only_its_own_run() {
    let harness = TestHarness::new().await;
    let mut real = open_run(
        &harness,
        "run-real",
        Some(7),
        ScriptEventLabel::PostProcessing,
        false,
    );
    let mut test = open_run(
        &harness,
        "run-test",
        Some(7),
        ScriptEventLabel::PostProcessing,
        true,
    );

    for (run, requests) in [("run-real", &mut real), ("run-test", &mut test)] {
        // Nothing an administrator's key may change is the run's to change.
        for mutation in [
            r#"mutation { createApiKey(name: "from a script", scope: ADMIN) { rawKey } }"#,
            r#"mutation { updateScriptInstance(id: "instance-1", input: {
                script: "tidy.sh", trigger: POST_PROCESSING, enabled: false
            }) { id } }"#,
        ] {
            let (code, _) = refused(&as_run(&harness, run, mutation).await);
            assert_eq!(code, "NOT_ALLOWED_FOR_SCRIPT_RUN", "{run}: {mutation}");
        }

        // An alias cannot turn an administrator query into an allowed one.
        for query in ["{ apiKeys { id } }", "{ queueItems: apiKeys { id } }"] {
            assert_eq!(
                refused(&as_run(&harness, run, query).await).0,
                "NOT_ALLOWED_FOR_SCRIPT_RUN"
            );
        }
        let response = as_run(
            &harness,
            run,
            "{ queueItems { id } historyItems { id } scriptRun { runId } }",
        )
        .await;
        assert_no_errors(&response);
        assert_eq!(response_data(&response)["scriptRun"]["runId"], run);

        // And its own run is still its to ask of.
        let (response, taken) = tokio::join!(
            as_run(
                &harness,
                run,
                r#"mutation { scriptRun { setParameter(name: "Seen", value: "yes") } }"#,
            ),
            agree(requests, 1),
        );
        assert_no_errors(&response);
        assert_eq!(
            taken,
            [RunAction::Command(Directive::Parameter {
                name: "Seen".into(),
                value: "yes".into(),
            })]
        );
    }

    let keys = harness.execute("{ apiKeys { id } }").await;
    assert_no_errors(&keys);
    assert_eq!(response_data(&keys)["apiKeys"], json!([]));
}

#[tokio::test]
async fn set_duplicate_sends_all_of_what_it_names_or_none_of_it() {
    let harness = TestHarness::new().await;
    let mut scan = open(&harness, "run-scan", None, ScriptEventLabel::Scan);

    // The key could not be written on one line, so neither the score nor the
    // mode that came with it is sent.
    let response = as_run(
        &harness,
        "run-scan",
        r#"mutation { scriptRun { setDuplicate(key: "two\nlines", score: 5, mode: ALL) } }"#,
    )
    .await;
    assert_eq!(refused(&response).0, "REFUSED");

    // The next thing the run is handed is the next thing asked for.
    let (response, taken) = tokio::join!(
        as_run(
            &harness,
            "run-scan",
            r#"mutation { scriptRun { setCategory(category: "tv") } }"#,
        ),
        agree(&mut scan, 1),
    );
    assert_no_errors(&response);
    assert_eq!(
        taken,
        [RunAction::Command(Directive::Category("tv".into()))]
    );

    // Nor does a run whose trigger allows none of them get any.
    let _post = open(
        &harness,
        "run-post",
        Some(7),
        ScriptEventLabel::PostProcessing,
    );
    let response = as_run(
        &harness,
        "run-post",
        r#"mutation { scriptRun { setDuplicate(key: "show-1", score: 5) } }"#,
    )
    .await;
    assert_eq!(refused(&response).0, "NOT_ALLOWED_FOR_TRIGGER");
}
