use super::*;
use crate::post_processing::executor::PostProcessingExecutor;
use crate::post_processing::instances::{InstanceTrigger, ScriptInstanceDraft};
use crate::post_processing::model::{PostProcessingSettings, PostProcessingSummary, ScriptName};
use std::os::unix::fs::PermissionsExt;
use tokio::io::AsyncWriteExt;

// Re-execute this test binary as the real supervisor, without requiring an
// independently built application binary in the core library's unit suite.
#[test]
fn post_processing_supervisor_child() {
    if std::env::var_os("WEAVER_TEST_POST_PROCESSING_SUPERVISOR").as_deref()
        == Some(std::ffi::OsStr::new("1"))
    {
        // The harness has already written its banner to stdout, and the
        // supervisor's launch preamble must be the first byte the runner
        // reads. The wrapper parked the runner's pipe on descriptor 3 and sent
        // the harness to /dev/null; hand the pipe back before speaking.
        // SAFETY: descriptor 3 is the wrapper's duplicate of the runner's
        // stdout pipe, and nothing in this process has opened another 3.
        unsafe {
            assert_eq!(libc::dup2(3, 1), 1);
            libc::close(3);
        }
        std::process::exit(crate::post_processing::runner::run_supervisor_stdio());
    }
}

#[tokio::test]
async fn late_completion_preserves_output_until_a_blocked_script_succeeds() {
    check_late_completion_with_blocked_script(93, PostProcessingSummary::Succeeded).await;
}

#[tokio::test]
async fn late_completion_preserves_output_and_a_blocked_scripts_failure() {
    check_late_completion_with_blocked_script(94, PostProcessingSummary::Failed).await;
}

async fn check_late_completion_with_blocked_script(
    exit_code: i32,
    expected_summary: PostProcessingSummary,
) {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp).await;
    let scripts = temp.path().join("scripts");
    tokio::fs::create_dir_all(&scripts).await.unwrap();
    let ready = scripts.join("ready");
    let release = scripts.join("release");
    assert!(
        tokio::process::Command::new("mkfifo")
            .arg(&ready)
            .arg(&release)
            .status()
            .await
            .unwrap()
            .success()
    );
    let script = scripts.join("gated.sh");
    tokio::fs::write(
        &script,
        format!(
            r#"#!/bin/sh
### NZBGET POST-PROCESSING SCRIPT ###
set -eu
script_dir=${{0%/*}}
printf '%s\n' "$NZBPP_DIRECTORY" > "$script_dir/ready"
IFS= read -r gate < "$script_dir/release"
test "$gate" = release
test -f "$NZBPP_DIRECTORY/payload.mkv"
printf '%s\n' "$NZBPP_DIRECTORY" > "$script_dir/observed-directory"
exit {exit_code}
"#
        ),
    )
    .await
    .unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
    let supervisor = temp.path().join("supervisor.sh");
    let test_binary = std::env::current_exe().unwrap();
    let quoted_binary = test_binary.to_str().unwrap().replace('\'', "'\"'\"'");
    tokio::fs::write(
        &supervisor,
        format!(
            "#!/bin/sh\nexport WEAVER_TEST_POST_PROCESSING_SUPERVISOR=1\nexec '{quoted_binary}' --exact pipeline::tests::post_processing_completion::post_processing_supervisor_child --nocapture 3>&1 1>/dev/null\n"
        ),
    )
    .await
    .unwrap();
    std::fs::set_permissions(&supervisor, std::fs::Permissions::from_mode(0o755)).unwrap();
    pipeline
        .db
        .replace_post_processing_script_directory(&scripts)
        .unwrap();
    pipeline
        .db
        .save_post_processing_settings(&PostProcessingSettings {
            execution_enabled: true,
            ..Default::default()
        })
        .unwrap();
    pipeline
        .db
        .create_script_instance(ScriptInstanceDraft::new(
            ScriptName::new("gated.sh").unwrap(),
            InstanceTrigger::PostProcessing,
        ))
        .unwrap();
    pipeline.terminal_post_processing_executor =
        PostProcessingExecutor::new(pipeline.db.clone(), scripts.clone())
            .with_supervisor_executable(supervisor);
    pipeline.terminal_post_processing_executor.pause();

    let job_id = JobId(10071);
    let job_name = "Late Completion With Live Script";
    let spec = minimal_job_state(job_id, job_name, PathBuf::new()).spec;
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    tokio::fs::write(working_dir.join("payload.mkv"), b"payload")
        .await
        .unwrap();
    let destination = complete_dir.join(crate::jobs::working_dir::sanitize_dirname(job_name));
    pipeline.start_move_to_complete(job_id).await.unwrap();
    settle_inflight_moves(&mut pipeline).await;

    assert_eq!(
        pipeline.jobs[&job_id].status,
        JobStatus::QueuedPostProcessing
    );
    assert_eq!(pipeline.jobs[&job_id].working_dir, destination);
    assert_eq!(
        tokio::fs::read(destination.join("payload.mkv"))
            .await
            .unwrap(),
        b"payload"
    );
    // The executor is paused, so this is a real queued admission, not a race
    // against the script-start notification.
    pipeline.check_job_completion(job_id).await;
    settle_inflight_moves(&mut pipeline).await;
    assert_eq!(
        pipeline.jobs[&job_id].status,
        JobStatus::QueuedPostProcessing
    );
    assert_eq!(pipeline.jobs[&job_id].working_dir, destination);

    pipeline.terminal_post_processing_executor.resume();
    match pipeline
        .terminal_post_processing_done_rx
        .recv()
        .await
        .unwrap()
    {
        TerminalPostProcessingEvent::Started(started_job) => {
            assert_eq!(started_job, job_id);
            pipeline.handle_terminal_post_processing_started(started_job);
        }
        TerminalPostProcessingEvent::Done(_) => panic!("script finished before its gate"),
        _ => panic!("unexpected terminal post-processing event before the script started"),
    }
    let script_directory = tokio::fs::read_to_string(&ready).await.unwrap();
    assert_eq!(script_directory.trim_end(), destination.to_str().unwrap());
    // Opening the writer pairs with the script's blocking read. Keeping it
    // open holds the script until the test explicitly releases it below.
    let mut gate = tokio::fs::OpenOptions::new()
        .write(true)
        .open(&release)
        .await
        .unwrap();
    assert_eq!(pipeline.jobs[&job_id].status, JobStatus::PostProcessing);
    for _ in 0..2 {
        pipeline.check_job_completion(job_id).await;
        settle_inflight_moves(&mut pipeline).await;
        assert_eq!(pipeline.jobs[&job_id].status, JobStatus::PostProcessing);
        assert_eq!(pipeline.jobs[&job_id].working_dir, destination);
        assert_eq!(
            tokio::fs::read(destination.join("payload.mkv"))
                .await
                .unwrap(),
            b"payload"
        );
        assert!(
            !complete_dir
                .join(format!("{job_name}.#{}", job_id.0))
                .exists()
        );
    }
    gate.write_all(b"release\n").await.unwrap();
    gate.flush().await.unwrap();
    drop(gate);
    let done = match pipeline
        .terminal_post_processing_done_rx
        .recv()
        .await
        .unwrap()
    {
        TerminalPostProcessingEvent::Done(done) => done,
        TerminalPostProcessingEvent::Started(_) => panic!("script started twice"),
        _ => panic!("unexpected terminal post-processing event before the script finished"),
    };
    assert_eq!(done.job_id, job_id);
    let report = done.result.as_ref().unwrap();
    assert_eq!(report.summary, expected_summary, "{report:?}");
    assert_eq!(report.results.len(), 1);
    assert_eq!(report.results[0].exit_code, Some(exit_code));
    pipeline.handle_terminal_post_processing_done(done);
    let finished = pipeline
        .finished_jobs
        .iter()
        .find(|job| job.job_id == job_id)
        .unwrap();
    if exit_code == 93 {
        assert_eq!(finished.status, JobStatus::Complete);
    } else {
        assert!(
            matches!(&finished.status, JobStatus::Failed { error } if error.contains("post-processing"))
        );
    }
    assert_eq!(finished.output_dir.as_deref(), destination.to_str());
    assert_eq!(
        tokio::fs::read_to_string(scripts.join("observed-directory"))
            .await
            .unwrap(),
        script_directory
    );
    assert_eq!(
        tokio::fs::read(destination.join("payload.mkv"))
            .await
            .unwrap(),
        b"payload"
    );
    assert_eq!(std::fs::read_dir(&complete_dir).unwrap().count(), 1);
    assert!(pipeline.inflight_terminal_post_processing.is_empty());
}

// Point `pipeline` at a script directory holding one script that records the
// directory it was handed and succeeds, run through the real supervisor.
async fn install_recording_script(pipeline: &mut Pipeline, temp: &tempfile::TempDir) -> PathBuf {
    install_scripts(
        pipeline,
        temp,
        &[(
            "record.sh",
            r#"#!/bin/sh
### NZBGET POST-PROCESSING SCRIPT ###
set -eu
script_dir=${0%/*}
test -f "$NZBPP_DIRECTORY/payload.bin"
printf '%s\n' "$NZBPP_DIRECTORY" >> "$script_dir/observed-directory"
exit 93
"#,
        )],
    )
    .await
}

// Point `pipeline` at a script directory holding `scripts`, each one entry of
// the post-processing list in that order, run through the real supervisor.
async fn install_scripts(
    pipeline: &mut Pipeline,
    temp: &tempfile::TempDir,
    scripts_to_write: &[(&str, &str)],
) -> PathBuf {
    let scripts = temp.path().join("scripts");
    tokio::fs::create_dir_all(&scripts).await.unwrap();
    for (name, body) in scripts_to_write {
        let script = scripts.join(name);
        tokio::fs::write(&script, body).await.unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    let supervisor = temp.path().join("supervisor.sh");
    let test_binary = std::env::current_exe().unwrap();
    let quoted_binary = test_binary.to_str().unwrap().replace('\'', "'\"'\"'");
    tokio::fs::write(
        &supervisor,
        format!(
            "#!/bin/sh\nexport WEAVER_TEST_POST_PROCESSING_SUPERVISOR=1\nexec '{quoted_binary}' --exact pipeline::tests::post_processing_completion::post_processing_supervisor_child --nocapture 3>&1 1>/dev/null\n"
        ),
    )
    .await
    .unwrap();
    std::fs::set_permissions(&supervisor, std::fs::Permissions::from_mode(0o755)).unwrap();
    pipeline
        .db
        .replace_post_processing_script_directory(&scripts)
        .unwrap();
    pipeline
        .db
        .save_post_processing_settings(&PostProcessingSettings {
            execution_enabled: true,
            ..Default::default()
        })
        .unwrap();
    // The instance is saved with the rest of what a restart keeps.
    if pipeline.db.script_instances().unwrap().is_empty() {
        for (name, _) in scripts_to_write {
            pipeline
                .db
                .create_script_instance(ScriptInstanceDraft::new(
                    ScriptName::new(*name).unwrap(),
                    InstanceTrigger::PostProcessing,
                ))
                .unwrap();
        }
    }
    pipeline.terminal_post_processing_executor =
        PostProcessingExecutor::new(pipeline.db.clone(), scripts.clone())
            .with_supervisor_executable(supervisor);
    scripts
}

// A restart after the final move but before the first script started must
// resume the scripts against the delivered output. The move already emptied
// the working directory, so finalizing the job again can only fail it with
// "no delivery source exists".
#[tokio::test]
async fn restart_between_final_move_and_first_script_resumes_scripts_on_delivered_output() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, complete_dir) = new_direct_pipeline(&temp).await;
    let scripts = install_recording_script(&mut pipeline, &temp).await;
    // Admission queues behind the paused executor, so the run never reaches
    // the point where it records that scripts started: the restart lands in
    // exactly the window between the move and that durable marker.
    pipeline.terminal_post_processing_executor.pause();

    let job_id = JobId(10072);
    let job_name = "Amber Orchard Restart Window";
    let payload = b"delivered payload".to_vec();
    let spec = standalone_job_spec(job_name, &[("payload.bin".into(), payload.len() as u32)]);
    let nzb = format!(
        r#"<?xml version="1.0"?><nzb xmlns="http://www.newzbin.com/DTD/2003/nzb"><file poster="fixture" date="0" subject="&quot;payload.bin&quot;"><groups><group>alt.binaries.test</group></groups><segments><segment bytes="{}" number="1">segment-0@example.com</segment></segments></file></nzb>"#,
        payload.len()
    );
    let working_dir = insert_active_job_with_persisted_nzb(
        &mut pipeline,
        job_id,
        spec,
        crate::ingest::compress_nzb_bytes(nzb.as_bytes()).unwrap(),
    )
    .await;
    write_and_complete_file(&mut pipeline, job_id, 0, "payload.bin", &payload).await;
    persist_completed_file_hash(&pipeline, job_id, 0, "payload.bin", &payload).await;

    pipeline.start_move_to_complete(job_id).await.unwrap();
    settle_inflight_moves(&mut pipeline).await;

    let destination = complete_dir.join(crate::jobs::working_dir::sanitize_dirname(job_name));
    assert_eq!(
        pipeline.jobs[&job_id].status,
        JobStatus::QueuedPostProcessing
    );
    assert_eq!(pipeline.jobs[&job_id].working_dir, destination);
    assert!(!working_dir.exists());
    assert_eq!(
        pipeline.db.job_post_processing_summary(job_id.0).unwrap(),
        Some(PostProcessingSummary::NotRun)
    );
    retire_pipeline_database(pipeline).await;

    // Startup, as the server runs it: the interrupted-run sweep, then the
    // recovery scan that turns active rows into restore requests.
    let (mut restored, _, _) = new_direct_pipeline(&temp).await;
    assert_eq!(
        restored.db.recover_interrupted_post_processing().unwrap(),
        0
    );
    let startup = crate::operations::recovery::recover_server_state(
        &restored.db,
        &temp.path().join("data"),
        &intermediate_dir,
    )
    .await
    .unwrap();
    let request = startup
        .to_restore
        .into_iter()
        .find(|candidate| candidate.job_id == job_id)
        .expect("the job must come back as in-progress work")
        .request;
    assert_eq!(request.status, JobStatus::QueuedPostProcessing);
    assert_eq!(request.working_dir, destination);

    install_recording_script(&mut restored, &temp).await;
    restored.restore_job(request).await.unwrap();

    // Nothing of completion runs again: no move is in flight and the job is
    // waiting on its scripts, pointed at the delivered output.
    assert!(restored.inflight_moves.is_empty());
    assert!(restored.move_done_rx.try_recv().is_err());
    assert_eq!(
        restored.jobs[&job_id].status,
        JobStatus::QueuedPostProcessing
    );
    assert_eq!(restored.jobs[&job_id].working_dir, destination);

    loop {
        match restored
            .terminal_post_processing_done_rx
            .recv()
            .await
            .unwrap()
        {
            TerminalPostProcessingEvent::Started(started_job) => {
                assert_eq!(started_job, job_id);
                restored.handle_terminal_post_processing_started(started_job);
            }
            TerminalPostProcessingEvent::Done(done) => {
                assert_eq!(done.job_id, job_id);
                let report = done.result.as_ref().unwrap();
                assert_eq!(
                    report.summary,
                    PostProcessingSummary::Succeeded,
                    "{report:?}"
                );
                restored.handle_terminal_post_processing_done(done);
                break;
            }
            _ => panic!("unexpected terminal post-processing event"),
        }
    }

    let finished = restored
        .finished_jobs
        .iter()
        .find(|job| job.job_id == job_id)
        .unwrap();
    assert_eq!(finished.status, JobStatus::Complete);
    assert_eq!(finished.output_dir.as_deref(), destination.to_str());
    assert_eq!(
        tokio::fs::read(destination.join("payload.bin"))
            .await
            .unwrap(),
        payload
    );
    // One delivery, never a second `<name>.#<id>` claim.
    assert_eq!(std::fs::read_dir(&complete_dir).unwrap().count(), 1);
    assert_eq!(
        tokio::fs::read_to_string(scripts.join("observed-directory"))
            .await
            .unwrap(),
        format!("{}\n", destination.display())
    );
}

const NEVER_RERUN_SCRIPT: &str = r#"#!/bin/sh
### NZBGET POST-PROCESSING SCRIPT ###
set -eu
script_dir=${0%/*}
: > "$script_dir/first-ran"
exit 93
"#;

const STATUS_RECORDING_SCRIPT: &str = r#"#!/bin/sh
### NZBGET POST-PROCESSING SCRIPT ###
set -eu
script_dir=${0%/*}
for name in NZBPP_STATUS NZBPP_TOTALSTATUS NZBPP_PARSTATUS NZBPP_UNPACKSTATUS WEAVER_STATUS; do
    eval "value=\${$name}"
    printf '%s=%s\n' "$name" "$value" >> "$script_dir/observed-status"
done
exit 93
"#;

// A restart part way through a failed job's scripts resumes the ones that
// had not started, and they are told the failure the scripts before the
// restart were told, though the restart lost the extraction that the unpack
// status came from. The script that was running is not run again.
#[tokio::test]
async fn restart_part_way_through_a_failed_jobs_scripts_resumes_them_with_its_failure() {
    use crate::post_processing::model::{
        PipelineFailureStage, PipelineOutcome, PostProcessingFacts, PostProcessingResume,
        ScriptStatus, StartedScript,
    };
    use crate::post_processing::runner_tests::status_env;

    let list = [
        ("first.sh", NEVER_RERUN_SCRIPT),
        ("record.sh", STATUS_RECORDING_SCRIPT),
    ];
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, _) = new_direct_pipeline(&temp).await;
    let scripts = install_scripts(&mut pipeline, &temp, &list).await;
    // The pass is launched but none of its scripts start before the restart.
    pipeline.terminal_post_processing_executor.pause();

    let job_id = JobId(10073);
    let payload = b"failed payload".to_vec();
    let spec = standalone_job_spec(
        "Cobalt Ridge Failed Restart",
        &[("payload.bin".into(), payload.len() as u32)],
    );
    let nzb = format!(
        r#"<?xml version="1.0"?><nzb xmlns="http://www.newzbin.com/DTD/2003/nzb"><file poster="fixture" date="0" subject="&quot;payload.bin&quot;"><groups><group>alt.binaries.test</group></groups><segments><segment bytes="{}" number="1">segment-0@example.com</segment></segments></file></nzb>"#,
        payload.len()
    );
    insert_active_job_with_persisted_nzb(
        &mut pipeline,
        job_id,
        spec,
        crate::ingest::compress_nzb_bytes(nzb.as_bytes()).unwrap(),
    )
    .await;
    // The archive was extracted, then the output could not be moved.
    pipeline
        .extracted_archives
        .insert(job_id, ["payload".to_string()].into_iter().collect());
    pipeline.jobs.get_mut(&job_id).unwrap().status = JobStatus::Moving;
    pipeline.fail_job(job_id, "could not move the output".to_string());
    assert_eq!(
        pipeline.jobs[&job_id].status,
        JobStatus::QueuedPostProcessing
    );
    let failure = "could not move the output".to_string();

    // What the pass told its scripts is kept on the job.
    pipeline.db.flush_write_queue().await.unwrap();
    let facts = pipeline
        .db
        .job_post_processing_facts(job_id.0)
        .unwrap()
        .expect("the pass keeps what it told its scripts");
    assert_eq!(
        facts,
        PostProcessingFacts {
            outcome: PipelineOutcome::Failed {
                stage: PipelineFailureStage::Move,
                code: "PIPELINE_FAILURE".into(),
                message: failure.clone(),
            },
            par_status: 0,
            unpack_status: 2,
        }
    );
    let before = status_env(
        &pipeline
            .terminal_post_processing_context(job_id, &facts)
            .unwrap(),
    );
    assert_eq!(before["NZBPP_TOTALSTATUS"], "FAILURE");
    assert_eq!(before["NZBPP_UNPACKSTATUS"], "2");

    // The first script was running when weaver stopped.
    let first = pipeline
        .db
        .script_instances()
        .unwrap()
        .into_iter()
        .find(|instance| instance.script.as_str() == "first.sh")
        .unwrap();
    pipeline
        .db
        .mark_job_post_processing_resumed(
            job_id.0,
            &PostProcessingResume {
                started: vec![StartedScript {
                    id: first.id.clone(),
                    name: first.name.clone(),
                    script: first.script.clone(),
                    waited: true,
                }],
                results: vec![],
            },
        )
        .unwrap();
    retire_pipeline_database(pipeline).await;

    let (mut restored, _, _) = new_direct_pipeline(&temp).await;
    // Opening the database ran the startup sweep.
    assert_eq!(
        restored.db.job_post_processing_summary(job_id.0).unwrap(),
        Some(PostProcessingSummary::Interrupted)
    );
    let startup = crate::operations::recovery::recover_server_state(
        &restored.db,
        &temp.path().join("data"),
        &intermediate_dir,
    )
    .await
    .unwrap();
    let request = startup
        .to_restore
        .into_iter()
        .find(|candidate| candidate.job_id == job_id)
        .expect("the job must come back as in-progress work")
        .request;
    assert_eq!(request.status, JobStatus::QueuedPostProcessing);

    install_scripts(&mut restored, &temp, &list).await;
    restored.restore_job(request).await.unwrap();
    assert!(!restored.extracted_archives.contains_key(&job_id));
    let kept = restored
        .db
        .job_post_processing_facts(job_id.0)
        .unwrap()
        .unwrap();
    assert_eq!(
        status_env(
            &restored
                .terminal_post_processing_context(job_id, &kept)
                .unwrap()
        ),
        before
    );

    loop {
        match restored
            .terminal_post_processing_done_rx
            .recv()
            .await
            .unwrap()
        {
            TerminalPostProcessingEvent::Started(started_job) => {
                assert_eq!(started_job, job_id);
                restored.handle_terminal_post_processing_started(started_job);
            }
            TerminalPostProcessingEvent::Done(done) => {
                assert_eq!(done.job_id, job_id);
                assert_eq!(done.primary_failure.as_deref(), Some(failure.as_str()));
                let report = done.result.as_ref().unwrap();
                assert_eq!(
                    report
                        .results
                        .iter()
                        .map(|result| (result.script.as_str(), result.status))
                        .collect::<Vec<_>>(),
                    [
                        ("first.sh", ScriptStatus::Interrupted),
                        ("record.sh", ScriptStatus::Succeeded),
                    ],
                    "{report:?}"
                );
                restored.handle_terminal_post_processing_done(done);
                break;
            }
            _ => panic!("unexpected terminal post-processing event"),
        }
    }

    assert!(!scripts.join("first-ran").exists());
    let observed = tokio::fs::read_to_string(scripts.join("observed-status"))
        .await
        .unwrap()
        .lines()
        .map(|line| {
            let (name, value) = line.split_once('=').unwrap();
            (name.to_string(), value.to_string())
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    let expected = before
        .iter()
        .filter(|(name, _)| observed.contains_key(*name))
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(observed.len(), 5);
    assert_eq!(observed, expected);
    let finished = restored
        .finished_jobs
        .iter()
        .find(|job| job.job_id == job_id)
        .unwrap();
    assert!(
        matches!(finished.status, JobStatus::Failed { .. }),
        "{:?}",
        finished.status
    );
}
