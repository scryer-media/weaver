use super::*;
use crate::post_processing::executor::PostProcessingExecutor;
use crate::post_processing::model::{
    PostProcessingSettings, PostProcessingSummary, ScriptList, ScriptListEntry, ScriptLists,
    ScriptName,
};
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
        .save_post_processing_script_lists(&ScriptLists {
            global: ScriptList::new(vec![ScriptListEntry::new(
                ScriptName::new("gated.sh").unwrap(),
            )])
            .unwrap(),
            ..Default::default()
        })
        .unwrap();
    pipeline.terminal_post_processing_executor =
        PostProcessingExecutor::new(pipeline.db.clone(), scripts.clone(), 1)
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
