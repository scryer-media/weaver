// Where a chase stages its output, and who makes that directory.
//
// Arming runs on the pipeline task, so it must not touch the staging tree at
// all: the worker creates it on its own thread before it decodes anything.

use super::*;
use crate::pipeline::direct_unpack::wiring::{AbortLatch, DemotionReason};

// Wait for a condition a blocking worker will make true. No deadline: the
// test runner bounds a condition that never arrives.
async fn yield_until(mut condition: impl FnMut() -> bool) {
    while !condition() {
        tokio::task::yield_now().await;
    }
}

// A worker that cannot make its staging directory ends in a demotion that
// names the directory, and arming — which runs on the pipeline task — never
// found out, because it never looked.
#[tokio::test]
async fn a_worker_that_cannot_make_its_staging_is_demoted_for_it() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    enable_direct_unpack(&mut pipeline);
    let job_id = JobId(44700);
    let set_name = "generated_split_store_plain.7z";

    let files = sevenz_fixture_bytes(set_name);
    let spec = rar_job_spec("Amber Lantern Split", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    // A file where the job's staging level should be: the directory the
    // worker opens without following links cannot be opened at all.
    let job_level = pipeline
        .complete_dir
        .join(".weaver-direct-unpack")
        .join(job_id.0.to_string());
    std::fs::create_dir_all(job_level.parent().unwrap()).unwrap();
    std::fs::write(&job_level, b"not a directory").unwrap();

    write_and_complete_file(&mut pipeline, job_id, 0, &files[0].0, &files[0].1).await;
    assert!(
        pipeline.direct_unpack.is_armed(job_id, set_name),
        "arming does no filesystem work, so it cannot see the staging fault"
    );

    yield_until(|| {
        pipeline
            .direct_unpack
            .armed_worker_finished(job_id, set_name)
    })
    .await;
    pipeline.reap_direct_unpack().await;

    let outcome = pipeline
        .direct_unpack
        .outcome(job_id, set_name)
        .expect("the reap records the failed chase");
    assert!(outcome.result.is_err());
    assert_eq!(
        pipeline
            .direct_unpack
            .counters()
            .demoted_staging_unavailable,
        1
    );
    assert_eq!(pipeline.direct_unpack.counters().demoted_decode_failed, 0);
    assert_eq!(
        pipeline.direct_unpack.latched_reason(job_id, set_name),
        Some("staging_unavailable")
    );
    assert!(
        std::fs::metadata(&job_level).unwrap().is_file(),
        "nothing replaced what was in the way"
    );

    pipeline.direct_unpack_shutdown("test teardown").await;
}

// Generation numbers restart with the process, so a directory at a new
// generation's path can only be an earlier process's leftovers. It is
// cleared rather than adopted: the chase's budget starts from zero because
// the directory it writes into starts empty.
#[test]
fn a_stale_tree_at_a_generation_path_is_cleared_not_adopted() {
    let temp_dir = tempfile::tempdir().unwrap();
    let job_level = temp_dir.path().join(".weaver-direct-unpack").join("44710");

    let stale_dir = job_level.join("copper_gate.7z.0");
    std::fs::create_dir_all(stale_dir.join("nested")).unwrap();
    std::fs::write(stale_dir.join("nested/leftover.bin"), b"old output").unwrap();
    let root = crate::pipeline::extraction::ExtractionRoot::create_empty(&stale_dir).unwrap();
    drop(root);
    assert_eq!(std::fs::read_dir(&stale_dir).unwrap().count(), 0);

    let stale_file = job_level.join("copper_gate.7z.1");
    std::fs::write(&stale_file, b"old output").unwrap();
    crate::pipeline::extraction::ExtractionRoot::create_empty(&stale_file).unwrap();
    assert!(std::fs::metadata(&stale_file).unwrap().is_dir());
    assert_eq!(std::fs::read_dir(&stale_file).unwrap().count(), 0);

    #[cfg(unix)]
    {
        let outside = temp_dir.path().join("outside");
        std::fs::create_dir_all(&outside).unwrap();
        std::fs::write(outside.join("keep.bin"), b"not staging").unwrap();
        let planted = job_level.join("copper_gate.7z.2");
        std::os::unix::fs::symlink(&outside, &planted).unwrap();
        crate::pipeline::extraction::ExtractionRoot::create_empty(&planted).unwrap();
        assert!(
            !std::fs::symlink_metadata(&planted)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert!(
            outside.join("keep.bin").exists(),
            "a link at the generation path is unlinked, never followed"
        );
    }
}

// Hold every staging deletion until the test releases it.
fn hold_staging_cleanups(pipeline: &mut Pipeline) -> std::sync::Arc<tokio::sync::Semaphore> {
    let hold = std::sync::Arc::new(tokio::sync::Semaphore::new(0));
    pipeline.direct_unpack.staging_cleanup_hold = Some(std::sync::Arc::clone(&hold));
    hold
}

// End a running chase the way a pause does, and reap its worker.
async fn abort_retryably_and_reap(pipeline: &mut Pipeline, job_id: JobId, set_name: &str) {
    pipeline.direct_unpack_abort_set(
        job_id,
        set_name,
        "paused",
        AbortLatch::Retryable,
        DemotionReason::DownloadEnded,
    );
    yield_until(|| {
        pipeline
            .direct_unpack
            .draining_worker_finished(job_id, set_name)
    })
    .await;
    pipeline.reap_direct_unpack().await;
    assert!(!pipeline.direct_unpack.is_draining(job_id, set_name));
}

// A set that re-arms after a retryable abort stages into a new directory,
// and it does so while the previous arm's tree is still waiting to be
// deleted: the two never share a path, so neither waits for the other. The
// outcome names the directory its own worker wrote.
#[tokio::test]
async fn a_re_armed_set_stages_into_a_new_generation() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    enable_direct_unpack(&mut pipeline);
    let hold = hold_staging_cleanups(&mut pipeline);
    let job_id = JobId(44720);
    let set_name = "generated_split_store_plain.7z";

    let files = sevenz_fixture_bytes(set_name);
    assert!(files.len() > 1, "the fixture is a split set");
    let spec = rar_job_spec("Amber Lantern Split", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    write_and_complete_file(&mut pipeline, job_id, 0, &files[0].0, &files[0].1).await;
    assert!(pipeline.direct_unpack.is_armed(job_id, set_name));
    let first = pipeline.direct_unpack_staging_dir(job_id, set_name);
    yield_until(|| first.is_dir()).await;

    abort_retryably_and_reap(&mut pipeline, job_id, set_name).await;
    assert_eq!(
        pipeline.direct_unpack.latched_reason(job_id, set_name),
        None
    );

    // The next part's completion re-arms the set with the old tree still
    // on disk, its deletion held.
    for (file_index, (filename, bytes)) in files.iter().enumerate().skip(1) {
        write_and_complete_file(&mut pipeline, job_id, file_index as u32, filename, bytes).await;
    }
    assert_eq!(pipeline.direct_unpack.counters().armed, 2);
    let second = pipeline.direct_unpack_staging_dir(job_id, set_name);
    assert_ne!(first, second, "each arm stages into its own directory");
    assert_eq!(
        first.parent(),
        second.parent(),
        "both under the job's level"
    );
    assert!(first.is_dir(), "the retired tree's deletion is still held");

    yield_until(|| {
        pipeline
            .direct_unpack
            .armed_worker_finished(job_id, set_name)
    })
    .await;
    pipeline.reap_direct_unpack().await;
    let outcome = pipeline
        .direct_unpack
        .outcome(job_id, set_name)
        .expect("the second chase finished");
    assert_eq!(outcome.staging_dir, second);
    let members = &outcome.result.as_ref().expect("a clean chase").extracted;
    assert!(!members.is_empty());
    for member in members {
        assert!(
            second.join(member).is_file(),
            "{member} staged in its arm's tree"
        );
    }

    hold.add_permits(1);
    pipeline.settle_direct_unpack_staging_cleanups().await;
    assert!(!first.exists(), "the retired tree is deleted once released");
    assert!(second.is_dir(), "and the live one is untouched");

    pipeline.direct_unpack_shutdown("test teardown").await;
}

// Reaping an aborted chase hands its tree to a deletion the pipeline task
// does not wait for. The reap returns with the tree still there, and the
// tree is gone once that deletion is observed to finish.
#[tokio::test]
async fn reaping_an_aborted_chase_does_not_wait_for_its_staging_to_go() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    enable_direct_unpack(&mut pipeline);
    let hold = hold_staging_cleanups(&mut pipeline);
    let job_id = JobId(44730);
    let set_name = "generated_split_store_plain.7z";

    let files = sevenz_fixture_bytes(set_name);
    let spec = rar_job_spec("Amber Lantern Split", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    write_and_complete_file(&mut pipeline, job_id, 0, &files[0].0, &files[0].1).await;
    assert!(pipeline.direct_unpack.is_armed(job_id, set_name));
    let staging = pipeline.direct_unpack_staging_dir(job_id, set_name);
    yield_until(|| staging.is_dir()).await;

    abort_retryably_and_reap(&mut pipeline, job_id, set_name).await;
    assert!(
        staging.is_dir(),
        "the reap returned without deleting the tree itself"
    );

    hold.add_permits(1);
    pipeline.settle_direct_unpack_staging_cleanups().await;
    assert!(!staging.exists());

    pipeline.direct_unpack_shutdown("test teardown").await;
}

// Forgetting a job — a reprocess, a nested rebuild — drops its finished
// chases, and their trees go with them rather than waiting for the sweep.
#[tokio::test]
async fn forgetting_a_job_retires_its_finished_chases_staging() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    enable_direct_unpack(&mut pipeline);
    let hold = hold_staging_cleanups(&mut pipeline);
    let job_id = JobId(44740);
    let set_name = "generated_split_store_plain.7z";

    chase_a_complete_split_set(&mut pipeline, job_id, set_name).await;
    let staging = pipeline.direct_unpack_staging_dir(job_id, set_name);
    assert!(
        pipeline
            .direct_unpack
            .outcome(job_id, set_name)
            .is_some_and(|outcome| outcome.result.is_ok())
    );
    assert!(staging.is_dir());

    pipeline.direct_unpack_forget_job(job_id);
    assert!(pipeline.direct_unpack.outcome(job_id, set_name).is_none());
    assert!(
        staging.is_dir(),
        "the deletion is not done on the pipeline task"
    );

    hold.add_permits(1);
    pipeline.settle_direct_unpack_staging_cleanups().await;
    assert!(!staging.exists());
}

// An install that fails leaves the chase's tree behind — both installs only
// remove it once every member is in place — so consumption retires it before
// falling back to conventional extraction.
#[tokio::test]
async fn a_failed_install_retires_the_chases_staging() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    enable_direct_unpack(&mut pipeline);
    let job_id = JobId(44750);
    let set_name = "generated_split_store_plain.7z";

    chase_a_complete_split_set(&mut pipeline, job_id, set_name).await;
    let staging = pipeline.direct_unpack_staging_dir(job_id, set_name);
    let member = pipeline
        .direct_unpack
        .outcome(job_id, set_name)
        .and_then(|outcome| outcome.result.as_ref().ok())
        .map(|members| members.extracted[0].clone())
        .expect("a finished chase");
    assert!(staging.join(&member).is_file());

    // A non-empty directory where the member has to land: neither a rename
    // nor a copy can put a file there.
    let blocker = pipeline.extraction_staging_dir(job_id).join(&member);
    std::fs::create_dir_all(&blocker).unwrap();
    std::fs::write(blocker.join("occupied.bin"), b"in the way").unwrap();

    pipeline.extract_7z_set(job_id, set_name).await.unwrap();
    let _ = next_extraction_done(&mut pipeline).await;
    assert_eq!(pipeline.direct_unpack.counters().consumed, 1);
    assert!(
        blocker.join("occupied.bin").is_file(),
        "the install could not have placed the member"
    );

    // The deletion is detached from consumption; wait for this tree to go.
    yield_until(|| !staging.exists()).await;
}

// A later part's completion does not open part one for its signature
// header while part one has fewer than 32 committed bytes: the pipeline
// task already knows the answer would be "not yet". Bytes already sitting in
// the file do not count until they are committed.
#[tokio::test]
async fn arming_does_not_open_part_one_before_its_header_is_committed() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    enable_direct_unpack(&mut pipeline);
    let job_id = JobId(44760);
    let set_name = "generated_split_store_plain.7z";

    let files = sevenz_fixture_bytes(set_name);
    assert!(files.len() > 2, "the fixture needs two later parts");
    let spec = rar_job_spec("Amber Lantern Split", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;
    let working_dir = pipeline.jobs.get(&job_id).unwrap().working_dir.clone();
    let part_one = working_dir.join(&files[0].0);
    std::fs::write(&part_one, &files[0].1[..64]).unwrap();

    write_and_complete_file(&mut pipeline, job_id, 1, &files[1].0, &files[1].1).await;
    assert_eq!(
        crate::pipeline::direct_unpack::wiring::signature_header_reads_of(&part_one),
        0,
        "nothing is committed on part one, so its file is not opened"
    );
    assert!(!pipeline.direct_unpack.is_armed(job_id, set_name));
    assert_eq!(
        pipeline.direct_unpack.latched_reason(job_id, set_name),
        None
    );

    // Once the header is committed the next completion reads it and arms.
    pipeline.pending_file_progress.insert(
        NzbFileId {
            job_id,
            file_index: 0,
        },
        64,
    );
    write_and_complete_file(&mut pipeline, job_id, 2, &files[2].0, &files[2].1).await;
    assert_eq!(
        crate::pipeline::direct_unpack::wiring::signature_header_reads_of(&part_one),
        1
    );
    assert!(pipeline.direct_unpack.is_armed(job_id, set_name));

    pipeline.direct_unpack_shutdown("test teardown").await;
}
