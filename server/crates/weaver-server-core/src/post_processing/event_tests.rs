use super::*;
use crate::post_processing::instances::ScriptInstanceDraft;
use crate::post_processing::model::ScriptName;

fn configured() -> (Database, tempfile::TempDir) {
    let db = Database::open_in_memory().unwrap();
    let data = tempfile::tempdir().unwrap();
    let scripts = db
        .initialize_post_processing_script_directory(data.path(), None)
        .unwrap();
    std::fs::write(
        scripts.join("queue.sh"),
        "#!/bin/sh\n### NZBGET QUEUE SCRIPT ###\nexit 0\n",
    )
    .unwrap();
    db.save_post_processing_settings(&PostProcessingSettings {
        execution_enabled: true,
        ..Default::default()
    })
    .unwrap();
    // One instance for each thing that can happen to a download.
    for event in QueueEvent::ALL {
        db.create_script_instance(queue_instance(event)).unwrap();
    }
    (db, data)
}

fn queue_instance(event: QueueEvent) -> ScriptInstanceDraft {
    ScriptInstanceDraft::new(
        ScriptName::new("queue.sh").unwrap(),
        InstanceTrigger::Queue(event),
    )
}

fn event(job_id: u64, event: QueueEvent) -> EventContext {
    EventContext {
        job_id: Some(job_id),
        event: ScriptEventLabel::Queue(event),
        category: None,
        cwd: PathBuf::new(),
        env: BTreeMap::new(),
        facts: Default::default(),
        instances: None,
        scratch: None,
    }
}

#[test]
fn queue_priority_supersession_and_recovery_are_durable() {
    let (db, _data) = configured();
    let file = db
        .enqueue_script_event(&event(1, QueueEvent::FileDownloaded), 1)
        .unwrap()
        .unwrap();
    let added = db
        .enqueue_script_event(&event(2, QueueEvent::NzbAdded), 2)
        .unwrap()
        .unwrap();
    let deleted = db
        .enqueue_script_event(&event(3, QueueEvent::NzbDeleted), 3)
        .unwrap()
        .unwrap();
    assert_eq!(db.claim_script_event().unwrap().unwrap().0, deleted);
    db.recover_script_events().unwrap();
    assert!(db.script_event_finished(&deleted).unwrap());
    assert!(!db.script_event_finished(&added).unwrap());
    let downloaded = db
        .enqueue_script_event(&event(1, QueueEvent::NzbDownloaded), 4)
        .unwrap()
        .unwrap();
    assert!(db.script_event_finished(&file).unwrap());
    assert_eq!(db.claim_script_event().unwrap().unwrap().0, downloaded);
    db.finish_script_event(&downloaded).unwrap();
    assert_eq!(
        db.downloaded_script_event(1).unwrap(),
        Some((downloaded, true))
    );
    assert_eq!(db.claim_script_event().unwrap().unwrap().0, added);
    assert!(db.claim_script_event().unwrap().is_none());
}

#[test]
fn file_event_deduplication_and_interval_use_the_specific_job() {
    let (db, _data) = configured();
    let context = event(7, QueueEvent::FileDownloaded);
    let first = db.enqueue_script_event(&context, 1000).unwrap().unwrap();
    assert!(db.enqueue_script_event(&context, 2000).unwrap().is_none());
    db.finish_script_event(&first).unwrap();
    let second = db.enqueue_script_event(&context, 2000).unwrap().unwrap();
    db.finish_script_event(&second).unwrap();
    let mut settings = db.post_processing_settings().unwrap();
    settings.event_scripts.file_downloaded_event_interval = 10;
    db.save_post_processing_settings(&settings).unwrap();
    assert!(db.enqueue_script_event(&context, 11999).unwrap().is_none());
    assert!(
        db.enqueue_script_event(&event(8, QueueEvent::FileDownloaded), 11999)
            .unwrap()
            .is_some()
    );
    assert!(db.enqueue_script_event(&context, 12000).unwrap().is_some());
    settings.event_scripts.file_downloaded_event_interval = -1;
    db.save_post_processing_settings(&settings).unwrap();
    assert!(
        db.enqueue_script_event(&event(9, QueueEvent::FileDownloaded), 12000)
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn deleted_jobs_drop_pending_nonterminal_events_without_execution() {
    let (db, _data) = configured();
    let run = db
        .enqueue_script_event(&event(5, QueueEvent::NzbAdded), 1)
        .unwrap()
        .unwrap();
    drain_queue(db.clone()).await.unwrap();
    assert!(db.script_event_finished(&run).unwrap());
    assert_eq!(db.queue_script_count().unwrap(), 0);
    assert!(db.event_script_results(5).unwrap().is_empty());
}

#[test]
fn disabled_execution_has_no_queue_side_effects() {
    let (db, _data) = configured();
    db.save_post_processing_settings(&PostProcessingSettings::default())
        .unwrap();
    assert!(
        db.enqueue_script_event(&event(1, QueueEvent::NzbAdded), 1)
            .unwrap()
            .is_none()
    );
    assert_eq!(db.queue_script_count().unwrap(), 0);
}

#[test]
fn queue_environment_uses_nzb_identity_and_all_status_fields() {
    let context = EventContext::from_job(
        &JobExecutionContext {
            job_id: 42,
            name: "sample".into(),
            nzb_filename: "sample.nzb".into(),
            category: None,
            group: None,
            source_url: Some("https://example.invalid/file.nzb".into()),
            working_directory: PathBuf::new(),
            final_directory: PathBuf::new(),
            pipeline_outcome: super::super::model::PipelineOutcome::Succeeded,
            par_status: 0,
            unpack_status: 0,
            compatibility: Default::default(),
        },
        QueueEvent::NzbDownloaded,
    );
    assert_eq!(context.env["NZBNA_NZBID"], "42");
    assert_eq!(context.env["NZBNA_LASTID"], "42");
    assert_eq!(context.env["NZBNA_EVENT"], "NZB_DOWNLOADED");
    for key in ["NZBNA_DELETESTATUS", "NZBNA_MARKSTATUS", "NZBNA_URLSTATUS"] {
        assert_eq!(context.env[key], "NONE");
    }
}

#[tokio::test]
async fn disabling_execution_stops_an_event_waiting_for_capacity() {
    let (db, _data) = configured();
    let mut settings = db.post_processing_settings().unwrap();
    settings.event_scripts.event_script_concurrency = 1;
    db.save_post_processing_settings(&settings).unwrap();
    *db.script_runtime.running.lock().unwrap() = 1;
    let permit = EventPermit(db.script_runtime.clone());
    let mut context = event(7, QueueEvent::NzbAdded);
    let run = run_event(&db, &mut context, "waiting-disabled", None, None);
    tokio::pin!(run);
    tokio::select! {
        biased;
        _ = &mut run => panic!("event must wait for the occupied slot"),
        _ = std::future::ready(()) => {},
    }
    assert!(
        db.script_runtime
            .cancellations
            .lock()
            .unwrap()
            .contains_key("waiting-disabled")
    );
    settings.execution_enabled = false;
    db.save_post_processing_settings(&settings).unwrap();
    drop(permit);
    assert!(run.await.unwrap().is_empty());
    assert!(db.script_runtime.cancellations.lock().unwrap().is_empty());
    assert_eq!(*db.script_runtime.running.lock().unwrap(), 0);
}

fn history(db: &Database, job_id: u64) {
    db.insert_job_history(&crate::JobHistoryRow {
        job_id,
        job_hash: None,
        name: "script-history".into(),
        status: "cancelled".into(),
        error_message: None,
        total_bytes: 0,
        downloaded_bytes: 0,
        optional_recovery_bytes: 0,
        optional_recovery_downloaded_bytes: 0,
        failed_bytes: 0,
        health: 1000,
        category: None,
        output_dir: None,
        nzb_path: None,
        created_at: 1,
        completed_at: 1,
        metadata: None,
        server_attribution: None,
    })
    .unwrap();
}

#[tokio::test]
async fn history_delete_wakes_an_existing_durable_event_waiter() {
    let (db, _data) = configured();
    history(&db, 17);
    let run_id = db
        .enqueue_script_event(&event(17, QueueEvent::NzbDeleted), 1)
        .unwrap()
        .unwrap();
    // A recovered started row has no process registration in this instance.
    let (claimed, _, registration) = db.claim_script_event().unwrap().unwrap();
    assert_eq!(claimed, run_id);
    drop(registration);
    let waiting = wait_for_event(&db, &run_id);
    tokio::pin!(waiting);
    tokio::select! {
        biased;
        _ = &mut waiting => panic!("unfinished durable event must block"),
        () = std::future::ready(()) => {},
    }
    assert!(db.delete_job_history(17).unwrap());
    waiting.await.unwrap();
}

#[tokio::test]
async fn deleted_event_row_does_not_release_an_admitted_or_running_script() {
    let (db, _data) = configured();
    history(&db, 18);
    let run_id = db
        .enqueue_script_event(&event(18, QueueEvent::NzbDeleted), 1)
        .unwrap()
        .unwrap();
    let (_, _, claim) = db.claim_script_event().unwrap().unwrap();
    assert!(db.delete_job_history(18).unwrap());
    let waiting = wait_for_job_events(&db, 18);
    tokio::pin!(waiting);
    tokio::select! {
        biased;
        _ = &mut waiting => panic!("claimed run must block even before process registration"),
        () = std::future::ready(()) => {},
    }
    let (cancel, _) = watch::channel(false);
    db.script_runtime
        .cancellations
        .lock()
        .unwrap()
        .insert(run_id.clone(), (Some(18), cancel));
    let running = RunRegistration {
        runtime: db.script_runtime.clone(),
        run_id,
        forwarding: None,
    };
    drop(claim);
    tokio::select! {
        biased;
        _ = &mut waiting => panic!("running process must survive durable row deletion"),
        () = std::future::ready(()) => {},
    }
    drop(running);
    waiting.await.unwrap();
}

#[tokio::test]
async fn history_delete_cancels_a_claim_before_run_registration() {
    let (db, _data) = configured();
    history(&db, 19);
    let run_id = db
        .enqueue_script_event(&event(19, QueueEvent::NzbDeleted), 1)
        .unwrap()
        .unwrap();
    let (_, mut context, claim) = db.claim_script_event().unwrap().unwrap();
    assert!(db.script_runtime.cancellations.lock().unwrap().is_empty());
    assert!(db.delete_job_history(19).unwrap());
    db.cancel_event_scripts(19);
    assert!(*claim.cancellation.borrow());
    let results = run_event(
        &db,
        &mut context,
        &run_id,
        Some(claim.cancellation.clone()),
        None,
    )
    .await
    .unwrap();
    assert!(results.is_empty());
    assert!(db.script_runtime.cancellations.lock().unwrap().is_empty());
    drop(claim);
    assert!(db.script_runtime.claimed.lock().unwrap().is_empty());
    wait_for_job_events(&db, 19).await.unwrap();
}

#[test]
fn running_file_event_allows_one_trailing_notification_without_consuming_duplicate_sequences() {
    let (db, _data) = configured();
    let context = event(25, QueueEvent::FileDownloaded);
    let first = db.enqueue_script_event(&context, 1).unwrap().unwrap();
    let (_, _, claim) = db.claim_script_event().unwrap().unwrap();
    let trailing = db.enqueue_script_event(&context, 2).unwrap().unwrap();
    assert_ne!(first, trailing);
    assert!(db.enqueue_script_event(&context, 3).unwrap().is_none());
    let next = db
        .enqueue_script_event(&event(26, QueueEvent::NzbAdded), 4)
        .unwrap()
        .unwrap();
    let seq = |id: &str| {
        id.strip_prefix("script-event-")
            .unwrap()
            .parse::<u64>()
            .unwrap()
    };
    assert_eq!(seq(&next), seq(&trailing) + 1);
    drop(claim);
}

#[test]
fn downloaded_supersedes_all_pending_events_but_not_started_events() {
    let (db, _data) = configured();
    let started = db
        .enqueue_script_event(&event(27, QueueEvent::NzbAdded), 1)
        .unwrap()
        .unwrap();
    let (_, _, claim) = db.claim_script_event().unwrap().unwrap();
    let pending = [
        QueueEvent::FileDownloaded,
        QueueEvent::NzbMarked,
        QueueEvent::NzbDeleted,
    ]
    .map(|kind| {
        db.enqueue_script_event(&event(27, kind), 2)
            .unwrap()
            .unwrap()
    });
    db.enqueue_script_event(&event(27, QueueEvent::NzbDownloaded), 3)
        .unwrap()
        .unwrap();
    assert!(!db.script_event_finished(&started).unwrap());
    for run in pending {
        assert!(db.script_event_finished(&run).unwrap());
    }
    drop(claim);
}

#[tokio::test]
async fn malformed_payload_is_quarantined_without_poisoning_another_job() {
    let (db, _data) = configured();
    let bad = db
        .enqueue_script_event(&event(28, QueueEvent::NzbDownloaded), 1)
        .unwrap()
        .unwrap();
    let good = db
        .enqueue_script_event(&event(29, QueueEvent::NzbAdded), 2)
        .unwrap()
        .unwrap();
    let datastore = db.datastore();
    let malformed = bad.clone();
    db.run_sql_blocking(async move {
        SqlRuntime::execute(
            datastore.read_exec(),
            "UPDATE script_event_queue SET payload = {} WHERE run_id = {}",
            &[SqlArg::Text("{}".into()), SqlArg::Text(malformed)],
        )
        .await?;
        Ok(())
    })
    .unwrap();
    drain_queue(db.clone()).await.unwrap();
    assert!(
        wait_for_event(&db, &bad)
            .await
            .unwrap_err()
            .to_string()
            .contains("payload is invalid")
    );
    wait_for_event(&db, &good).await.unwrap();
    assert_eq!(db.queue_script_count().unwrap(), 0);
}

#[test]
fn a_held_script_instance_admission_hint_is_not_worked_out_again() {
    let (db, _data) = configured();
    for instance in db.script_instances().unwrap() {
        db.delete_script_instance(&instance.id).unwrap();
    }
    refresh_admission_hint(&db).unwrap();
    assert!(!db.queue_scripts_possible());

    // Hold a hint the saved instances contradict: a refresh that read them
    // would replace it.
    db.script_runtime.admission_hint.lock().unwrap().1 = Some(AdmissionHint::Possible);
    assert!(!refresh_admission_hint(&db).unwrap());
    assert!(db.queue_scripts_possible());

    // Dropping the hint, as every save does, makes the next refresh look.
    db.invalidate_queue_script_admission();
    refresh_admission_hint(&db).unwrap();
    assert!(!db.queue_scripts_possible());
}

#[test]
fn script_instance_reads_only_the_named_instance() {
    let (db, _data) = configured();
    let all = db.script_instances().unwrap();
    assert!(all.len() > 1);
    for expected in &all {
        let found = db.script_instance(&expected.id).unwrap().unwrap();
        assert_eq!(found.id, expected.id);
        assert_eq!(found.trigger, expected.trigger);
        assert_eq!(found.inputs.len(), expected.inputs.len());
        assert_eq!(found.categories, expected.categories);
    }
    assert!(db.script_instance("no-such-instance").unwrap().is_none());
}

#[test]
fn the_subscriber_hint_follows_the_settings_and_the_saved_instances() {
    let (db, data) = configured();
    db.save_post_processing_settings(&PostProcessingSettings::default())
        .unwrap();
    refresh_admission_hint(&db).unwrap();
    assert!(!db.queue_scripts_possible());
    db.save_post_processing_settings(&PostProcessingSettings {
        execution_enabled: true,
        ..Default::default()
    })
    .unwrap();
    assert!(db.queue_scripts_possible());

    for instance in db.script_instances().unwrap() {
        db.delete_script_instance(&instance.id).unwrap();
    }
    refresh_admission_hint(&db).unwrap();
    assert!(!db.queue_scripts_possible());
    // An instance on another trigger has nothing to do with the queue.
    db.create_script_instance(ScriptInstanceDraft::new(
        ScriptName::new("queue.sh").unwrap(),
        InstanceTrigger::PostProcessing,
    ))
    .unwrap();
    refresh_admission_hint(&db).unwrap();
    assert!(!db.queue_scripts_possible());
    // Nor does one that is turned off.
    db.create_script_instance(queue_instance(QueueEvent::NzbAdded).disabled())
        .unwrap();
    refresh_admission_hint(&db).unwrap();
    assert!(!db.queue_scripts_possible());

    db.create_script_instance(queue_instance(QueueEvent::NzbAdded).category("tv"))
        .unwrap();
    // Saving an instance drops the hint, so the next event looks again.
    assert!(db.queue_scripts_possible());
    refresh_admission_hint(&db).unwrap();
    assert!(db.queue_scripts_possible());

    // The hint only says an event might have something to run. Whether this
    // one does is down to its trigger and its category.
    let mut context = event(30, QueueEvent::NzbAdded);
    assert!(!has_subscriber(&db, &context).unwrap());
    context.category = Some("TV".into());
    assert!(has_subscriber(&db, &context).unwrap());
    context.event = ScriptEventLabel::Queue(QueueEvent::NzbDeleted);
    assert!(!has_subscriber(&db, &context).unwrap());

    // A new scripts directory turns every instance off until the operator has
    // looked it over.
    db.replace_post_processing_script_directory(&data.path().join("replacement"))
        .unwrap();
    assert!(db.queue_scripts_possible());
    context.event = ScriptEventLabel::Queue(QueueEvent::NzbAdded);
    assert!(!has_subscriber(&db, &context).unwrap());
    assert!(!db.queue_scripts_possible());
}

#[test]
fn an_instance_runs_on_its_trigger_whatever_its_script_says_about_itself() {
    let (db, _data) = configured();
    let path = db
        .post_processing_script_directory()
        .unwrap()
        .join("queue.sh");
    let context = event(31, QueueEvent::NzbAdded);
    assert!(has_subscriber(&db, &context).unwrap());
    // A header is a starting point for a new instance and nothing more.
    std::fs::write(
        &path,
        "#!/bin/sh\n### NZBGET POST-PROCESSING SCRIPT ###\nexit 0\n",
    )
    .unwrap();
    assert!(has_subscriber(&db, &context).unwrap());
    std::fs::write(&path, "#!/bin/sh\nexit 0\n").unwrap();
    assert!(has_subscriber(&db, &context).unwrap());
}

#[tokio::test]
async fn an_instance_whose_script_is_gone_is_reported_and_holds_nothing_up() {
    let (db, _data) = configured();
    std::fs::remove_file(
        db.post_processing_script_directory()
            .unwrap()
            .join("queue.sh"),
    )
    .unwrap();
    let saved = db
        .script_instances()
        .unwrap()
        .into_iter()
        .find(|instance| instance.trigger == InstanceTrigger::Queue(QueueEvent::NzbAdded))
        .unwrap();
    let mut context = event(37, QueueEvent::NzbAdded);
    context.job_id = None;
    let results = run_event(&db, &mut context, "gone", None, None)
        .await
        .unwrap();

    assert_eq!(results.len(), 1);
    let result = &results[0];
    assert_eq!(result.status, ScriptStatus::Warning);
    assert_eq!(result.exit_code, None);
    assert_eq!(result.instance_id.as_deref(), Some(saved.id.as_str()));
    assert_eq!(result.instance_name.as_deref(), Some("queue.sh"));
    assert_eq!(result.event, ScriptEventLabel::Queue(QueueEvent::NzbAdded));
    assert!(result.error_message.is_some());
    assert!(!result.background);
    assert_eq!(*db.script_runtime.running.lock().unwrap(), 0);
}

#[test]
fn script_log_batches_mark_truncation_and_remain_utf8_bounded() {
    let mut logs = String::new();
    append_log(&mut logs, &"é".repeat(5000));
    assert!(logs.len() <= 8192);
    assert!(logs.ends_with("[script log batch truncated]\n"));
    let retained = logs.clone();
    append_log(&mut logs, "more output");
    assert_eq!(logs, retained);
}

#[test]
fn jobless_parameter_stream_rejects_aggregate_growth_without_changing_facts() {
    let (db, _data) = configured();
    let mut context = event(32, QueueEvent::NzbAdded);
    context.job_id = None;
    context.facts.parameters = (0..4096)
        .map(|index| (format!("parameter{index}"), "value".into()))
        .collect();
    let before = context.facts.parameters.clone();
    assert!(
        apply_directive(
            &db,
            &mut context,
            Directive::Parameter {
                name: "extra".into(),
                value: "value".into()
            }
        )
        .is_err()
    );
    assert_eq!(context.facts.parameters, before);
}

#[tokio::test]
async fn admission_preserves_event_order_and_coalesces_pending_files() {
    let (db, _data) = configured();
    history(&db, 33);
    db.script_runtime.admissions.lock().unwrap().running = true;
    db.script_runtime
        .worker_active
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let first = db.admit_queue_script_event(event(33, QueueEvent::FileDownloaded), false);
    let duplicate = db.admit_queue_script_event(event(33, QueueEvent::FileDownloaded), false);
    let marked = db.admit_queue_script_event(event(33, QueueEvent::NzbMarked), false);
    let downloaded = db.admit_queue_script_event(event(33, QueueEvent::NzbDownloaded), true);
    assert_eq!(
        db.script_runtime.admissions.lock().unwrap().pending.len(),
        3
    );
    assert!(duplicate.await.unwrap().unwrap().is_none());
    drain_admissions(db.clone()).await;
    let first = first.await.unwrap().unwrap().unwrap();
    let marked = marked.await.unwrap().unwrap().unwrap();
    let downloaded = downloaded.await.unwrap().unwrap().unwrap();
    assert!(db.script_event_finished(&first).unwrap());
    assert!(db.script_event_finished(&marked).unwrap());
    assert_eq!(db.claim_script_event().unwrap().unwrap().0, downloaded);
}

#[tokio::test]
async fn deletion_waits_pending_admission_and_removed_history_cannot_enqueue_afterward() {
    let (db, _data) = configured();
    history(&db, 34);
    db.script_runtime.admissions.lock().unwrap().running = true;
    let admission = db.admit_queue_script_event(event(34, QueueEvent::NzbDeleted), false);
    let waiting = wait_for_job_events_stopped(&db, 34);
    tokio::pin!(waiting);
    tokio::select! {
        biased;
        result = &mut waiting => panic!("pending admission was not awaited: {result:?}"),
        () = std::future::ready(()) => {},
    }
    assert!(db.delete_job_history(34).unwrap());
    drain_admissions(db.clone()).await;
    assert!(admission.await.unwrap().unwrap().is_none());
    waiting.await.unwrap();
    assert_eq!(db.queue_script_count().unwrap(), 0);
}

#[tokio::test]
async fn failed_terminal_write_retries_before_claiming_more_and_retains_only_its_run_error() {
    let (db, _data) = configured();
    let first = db
        .enqueue_script_event(&event(35, QueueEvent::NzbAdded), 1)
        .unwrap()
        .unwrap();
    let other = db
        .enqueue_script_event(&event(36, QueueEvent::NzbAdded), 2)
        .unwrap()
        .unwrap();
    let datastore = db.datastore();
    db.run_sql_blocking(async move {
        SqlRuntime::execute(datastore.read_exec(), "CREATE TRIGGER fail_script_finish BEFORE UPDATE OF state ON script_event_queue WHEN NEW.state = 'done' BEGIN SELECT RAISE(ABORT, 'test finish failure'); END", &[]).await?;
        Ok(())
    }).unwrap();
    assert!(drain_queue(db.clone()).await.is_err());
    assert_eq!(db.script_runtime.failed_runs.lock().unwrap().len(), 1);
    assert!(
        db.script_event_error(&first)
            .unwrap()
            .unwrap()
            .contains("test finish failure")
    );
    assert!(db.script_event_error(&other).unwrap().is_none());
    assert!(!db.script_event_finished(&other).unwrap());
    let datastore = db.datastore();
    db.run_sql_blocking(async move {
        SqlRuntime::execute(
            datastore.read_exec(),
            "DROP TRIGGER fail_script_finish",
            &[],
        )
        .await?;
        Ok(())
    })
    .unwrap();
    drain_queue(db.clone()).await.unwrap();
    assert!(db.script_runtime.failed_runs.lock().unwrap().is_empty());
    assert!(wait_for_event(&db, &first).await.is_err());
    wait_for_event(&db, &other).await.unwrap();
}
