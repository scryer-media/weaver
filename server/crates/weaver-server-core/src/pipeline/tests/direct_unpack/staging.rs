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
    let done = next_extraction_done(&mut pipeline).await;
    pipeline.handle_extraction_done(done).await;
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

#[tokio::test]
async fn native_chase_streams_nested_store_only_after_source_acceptance() {
    use sevenz_turbo::{ArchiveEntry, ArchiveWriter, EncoderConfiguration, EncoderMethod};
    fn archive(entries: &[(&str, Vec<u8>)], method: EncoderMethod) -> Vec<u8> {
        let mut writer = ArchiveWriter::new(std::io::Cursor::new(Vec::new())).unwrap();
        writer.set_content_methods(vec![EncoderConfiguration::new(method)]);
        for (name, bytes) in entries {
            writer
                .push_archive_entry(
                    ArchiveEntry::new_file(name),
                    Some(std::io::Cursor::new(bytes.clone())),
                )
                .unwrap();
        }
        writer.finish().unwrap().into_inner()
    }
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    enable_direct_unpack(&mut pipeline);
    let job = JobId(44719);
    let leaf = vec![39; 131073];
    let inner = archive(&[("leaf.bin", leaf.clone())], EncoderMethod::COPY);
    let ordinary = vec![71; 65539];
    let outer = archive(
        &[("ordinary.bin", ordinary.clone()), ("inner.7z", inner)],
        EncoderMethod::LZMA2,
    );
    let files = insert_single_7z_job(&mut pipeline, job, outer).await;
    let set = "silver_horizon.7z";
    write_and_complete_file(&mut pipeline, job, 0, &files[0].0, &files[0].1).await;
    while !pipeline.direct_unpack.armed_worker_finished(job, set) {
        while let Ok(done) = pipeline.extract_done_rx.try_recv() {
            assert!(
                matches!(
                    &done,
                    crate::pipeline::ExtractionDone::ScopedNamespace { .. }
                ),
                "source acceptance fences terminal decoded publication"
            );
            pipeline.handle_extraction_done(done).await;
        }
        tokio::task::yield_now().await;
    }
    pipeline.reap_direct_unpack().await;
    while let Ok(done) = pipeline.extract_done_rx.try_recv() {
        assert!(matches!(
            &done,
            crate::pipeline::ExtractionDone::ScopedNamespace { .. }
        ));
        pipeline.handle_extraction_done(done).await;
    }
    let outcome = pipeline.direct_unpack.outcome(job, set).unwrap();
    assert_eq!(outcome.result.as_ref().unwrap().extracted, ["ordinary.bin"]);
    let staging = outcome.staging_dir.clone();
    assert_eq!(
        std::fs::read(staging.join("ordinary.bin")).unwrap(),
        ordinary
    );
    assert!(!staging.join("inner.7z").exists());
    pipeline.extract_7z_set(job, set).await.unwrap();
    let done = loop {
        let done = pipeline.extract_done_rx.recv().await.unwrap();
        if matches!(
            &done,
            ExtractionDone::Scoped {
                decoded: Some(_),
                ..
            }
        ) {
            break done;
        }
        pipeline.handle_extraction_done(done).await;
    };
    let crate::pipeline::ExtractionDone::Scoped {
        decoded: Some(decoded),
        result: Ok(result),
        ..
    } = done
    else {
        panic!("accepted chase must retain the nested namespace");
    };
    assert_eq!(
        result.extracted,
        ["ordinary.bin"],
        "decoded repair must retain its ordinary sibling roster"
    );
    let selected = decoded.publication().unwrap();
    assert_eq!(std::fs::read(&selected["leaf.bin"]).unwrap(), leaf);
}

#[tokio::test]
async fn scoped_native_install_failure_is_reported_instead_of_empty_success() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    enable_direct_unpack(&mut pipeline);
    let job = JobId(44751);
    let set = "generated_split_store_plain.7z";
    let files = sevenz_fixture_bytes(set);
    insert_active_job(
        &mut pipeline,
        job,
        rar_job_spec("Amber Lantern Split", &files),
    )
    .await;
    for (index, (name, bytes)) in files.iter().enumerate() {
        write_and_complete_file(&mut pipeline, job, index as u32, name, bytes).await;
    }
    yield_until(|| pipeline.direct_unpack.armed_worker_finished(job, set)).await;
    pipeline.reap_direct_unpack().await;
    assert!(pipeline.has_scoped_chase(job, set));
    let member = pipeline
        .direct_unpack
        .outcome(job, set)
        .unwrap()
        .result
        .as_ref()
        .unwrap()
        .extracted[0]
        .clone();
    let blocker = pipeline.extraction_staging_dir(job).join(member);
    std::fs::create_dir_all(&blocker).unwrap();
    std::fs::write(blocker.join("occupied.bin"), b"occupied").unwrap();
    pipeline.extract_7z_set(job, set).await.unwrap();
    loop {
        let done = pipeline.extract_done_rx.recv().await.unwrap();
        if let ExtractionDone::NativeChaseResolved {
            job_id,
            set_name,
            result,
            ..
        } = &done
        {
            assert_eq!(*job_id, job);
            assert_eq!(set_name, set);
            let Err(error) = result else {
                panic!("ordinary payload installation failure must not become empty success");
            };
            assert!(
                error
                    .to_string()
                    .contains("scoped direct-unpack output installation failed")
            );
            pipeline.handle_extraction_done(done).await;
            break;
        } else {
            pipeline.handle_extraction_done(done).await;
        }
    }
    assert_eq!(
        std::fs::read(blocker.join("occupied.bin")).unwrap(),
        b"occupied"
    );
}

#[tokio::test]
async fn independent_extraction_retires_unaccepted_native_request_before_nested_gate() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    enable_direct_unpack(&mut pipeline);
    let job = JobId(44752);
    let set = "generated_split_store_plain.7z";
    let files = sevenz_fixture_bytes(set);
    insert_active_job(
        &mut pipeline,
        job,
        rar_job_spec("Amber Lantern Split", &files),
    )
    .await;
    for (index, (name, bytes)) in files.iter().enumerate() {
        write_and_complete_file(&mut pipeline, job, index as u32, name, bytes).await;
    }
    yield_until(|| pipeline.direct_unpack.armed_worker_finished(job, set)).await;
    pipeline.reap_direct_unpack().await;
    assert!(pipeline.archive_runtime[&job].pending());
    pipeline
        .extracted_archives
        .entry(job)
        .or_default()
        .insert(set.to_owned());
    let _ = pipeline.maybe_start_nested_extraction(job).await.unwrap();
    assert!(!pipeline.has_scoped_chase(job, set));
    assert!(
        !pipeline.archive_runtime[&job].pending(),
        "an independently satisfied archive must not strand a speculative request"
    );
    assert!(
        pipeline.extract_done_rx.try_recv().is_err(),
        "discarded native output must not publish a completion"
    );
}

#[tokio::test]
async fn revoked_native_owner_cannot_acknowledge_an_installed_set() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    enable_direct_unpack(&mut pipeline);
    let job = JobId(44753);
    let set = "generated_split_store_plain.7z";
    chase_a_complete_split_set(&mut pipeline, job, set).await;
    let owner = pipeline.chase_stream_owner(job, set).unwrap();
    pipeline
        .archive_runtime
        .get_mut(&job)
        .unwrap()
        .sessions
        .revoke(owner)
        .unwrap();
    pipeline
        .handle_extraction_done(ExtractionDone::NativeChaseResolved {
            job_id: job,
            set_name: set.to_owned(),
            owner,
            kind: crate::jobs::assembly::ArchiveType::SevenZip,
            result: Ok(Some(FullSetExtractionOutcome {
                extracted: vec!["stale.bin".to_owned()],
                failed: Vec::new(),
                selected_password: None,
            })),
            retirement: None,
        })
        .await;
    assert!(!pipeline.has_scoped_chase(job, set));
    assert!(
        !pipeline
            .extracted_archives
            .get(&job)
            .is_some_and(|sets| sets.contains(set))
    );
    assert!(
        !pipeline
            .extracted_members
            .get(&job)
            .is_some_and(|names| names.contains("stale.bin"))
    );
}

#[tokio::test]
async fn obsolete_native_ack_joins_its_producer_without_retiring_replacement() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    enable_direct_unpack(&mut pipeline);
    let job = JobId(44754);
    let set = "generated_split_store_plain.7z";
    chase_a_complete_split_set(&mut pipeline, job, set).await;
    let old = pipeline.chase_stream_owner(job, set).unwrap();
    pipeline.discard_chase_stream_joined(job, set).await;
    let destination = pipeline.extraction_staging_dir(job);
    let budget = pipeline.extraction_budget(job, &destination).unwrap();
    let replacement = pipeline
        .admit_root_stream(
            job,
            set,
            Vec::new(),
            crate::jobs::assembly::ArchiveType::SevenZip,
            destination,
            budget,
        )
        .unwrap();
    let replacement_owner = replacement.owner;
    let retired = temp.path().join("retired-native");
    std::fs::create_dir_all(&retired).unwrap();
    let writer_root = retired.clone();
    let joined = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let observed = joined.clone();
    let producer = tokio::spawn(async move {
        std::fs::write(writer_root.join("last-write.bin"), b"complete").unwrap();
        observed.store(true, std::sync::atomic::Ordering::SeqCst);
        Ok(FullSetExtractionOutcome {
            extracted: Vec::new(),
            failed: Vec::new(),
            selected_password: None,
        })
    });
    pipeline
        .handle_extraction_done(ExtractionDone::NativeChaseResolved {
            job_id: job,
            set_name: set.to_owned(),
            owner: old,
            kind: crate::jobs::assembly::ArchiveType::SevenZip,
            result: Ok(None),
            retirement: Some(
                crate::pipeline::completion::finalize::extract::ChaseRetirement::for_test(
                    retired.clone(),
                    producer,
                ),
            ),
        })
        .await;
    assert!(joined.load(std::sync::atomic::Ordering::SeqCst));
    assert!(!retired.exists());
    assert!(
        pipeline.archive_runtime[&job]
            .sessions
            .execution_current(replacement_owner)
    );
    assert_eq!(
        pipeline.archive_runtime[&job].graph_owners[&(set.to_owned(), 0)],
        replacement_owner
    );
    drop(replacement);
}

#[tokio::test]
async fn successful_pending_install_preserves_native_scope_authority() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    enable_direct_unpack(&mut pipeline);
    let job = JobId(44755);
    let set = "generated_split_store_plain.7z";
    let files = sevenz_fixture_bytes(set);
    insert_active_job(
        &mut pipeline,
        job,
        rar_job_spec("Pending native authority", &files),
    )
    .await;
    write_and_complete_file(&mut pipeline, job, 0, &files[0].0, &files[0].1).await;
    let disposition = pipeline.take_direct_unpack_disposition(job, set);
    let crate::pipeline::direct_unpack::wiring::ChaseDisposition::Pending(pending) = disposition
    else {
        panic!("incomplete source must transfer a pending worker");
    };
    let budget = pending.budget.clone();
    let working = pipeline.jobs[&job].working_dir.clone();
    for (index, (name, bytes)) in files.iter().enumerate().skip(1) {
        std::fs::write(working.join(name), bytes).unwrap();
        pending
            .coverage
            .advance_watermark(index, bytes.len() as u64);
        pending
            .coverage
            .note_committed_range(index, 0, bytes.len() as u64);
        pending.coverage.mark_part_complete(index);
    }
    let destination = pipeline.extraction_staging_dir(job);
    let resolution = crate::pipeline::completion::finalize::extract::install_direct_unpack_checked(
        crate::pipeline::direct_unpack::wiring::ChaseDisposition::Pending(pending),
        destination,
        std::sync::Arc::new(crate::jobs::PhaseCounters::default()),
        job,
        set,
        None,
        None,
    )
    .await;
    assert!(resolution.result.is_ok_and(|outcome| outcome.is_some()));
    assert!(resolution.retirement.is_none());
    assert!(
        budget.stopped_failure().is_none(),
        "successful consumer must not cancel the producer budget"
    );
    assert!(pipeline.accept_chase_stream(job, set));
    let owner = pipeline.chase_stream_owner(job, set).unwrap();
    assert!(pipeline.archive_runtime[&job].sessions.accepts(owner));
}

#[tokio::test]
async fn forgotten_completed_chase_retains_staging_until_descendant_joins() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    enable_direct_unpack(&mut pipeline);
    let job = JobId(44756);
    let set = "generated_split_store_plain.7z";
    chase_a_complete_split_set(&mut pipeline, job, set).await;
    let outcome = pipeline.direct_unpack.outcome(job, set).unwrap();
    let staging = outcome.staging_dir.clone();
    let member = staging.join(&outcome.result.as_ref().unwrap().extracted[0]);
    let expected = std::fs::read(&member).unwrap();
    let owner = pipeline.chase_stream_owner(job, set).unwrap();
    let runtime = pipeline.archive_runtime.get_mut(&job).unwrap();
    let child = runtime
        .admit(
            Some(owner),
            crate::pipeline::extraction::session::InputVersion(0),
        )
        .unwrap();
    let child_owner = child.owner;
    let (cancelled, cancellation) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    let worker = tokio::spawn(async move {
        child.cancelled().await;
        cancelled.send(()).unwrap();
        released.await.unwrap();
        assert_eq!(
            std::fs::read(member).unwrap(),
            expected,
            "retired descendant still owns its source until joined"
        );
        drop(child);
    });
    runtime.track_worker(child_owner, worker);
    pipeline.purge_terminal_job_runtime(job);
    cancellation.await.unwrap();
    assert!(
        staging.is_dir(),
        "cancellation must not delete an unjoined source"
    );
    release.send(()).unwrap();
    pipeline.settle_direct_unpack_staging_cleanups().await;
    assert!(!staging.exists());
}
