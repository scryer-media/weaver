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
    // The chase may stream the root and start the inner's engine, but
    // nothing it or the engine reports reaches the job's staging before the
    // source is accepted.
    let staging = pipeline.extraction_staging_dir(job);
    let mut chased = Vec::new();
    while !pipeline.direct_unpack.armed_worker_finished(job, set) {
        while let Ok(done) = pipeline.extract_done_rx.try_recv() {
            chased.push(variant(&done));
            pipeline.handle_extraction_done(done).await;
            assert!(
                !staging.join("leaf.bin").exists(),
                "source acceptance fences the nested leaf's publication: {chased:?}"
            );
        }
        tokio::task::yield_now().await;
    }
    pipeline.reap_direct_unpack().await;
    while let Ok(done) = pipeline.extract_done_rx.try_recv() {
        chased.push(variant(&done));
        pipeline.handle_extraction_done(done).await;
    }
    assert!(
        !staging.join("leaf.bin").exists(),
        "source acceptance fences the nested leaf's publication: {chased:?}"
    );
    let outcome = pipeline.direct_unpack.outcome(job, set).unwrap();
    assert_eq!(outcome.result.as_ref().unwrap().extracted, ["ordinary.bin"]);
    let chase_staging = outcome.staging_dir.clone();
    assert_eq!(
        std::fs::read(chase_staging.join("ordinary.bin")).unwrap(),
        ordinary
    );
    assert!(!chase_staging.join("inner.7z").exists());

    // Accepting the source releases the root's verdict and the inner's, and
    // the leaf lands once every namespace has one.
    pipeline.extract_7z_set(job, set).await.unwrap();
    let mut finish = Vec::new();
    loop {
        match pipeline.archive_runtime.get(&job) {
            Some(runtime) if runtime.publication_ready() => break,
            Some(_) => {}
            None => panic!("the archive runtime left after {finish:?}"),
        }
        let done = pipeline.extract_done_rx.recv().await.unwrap();
        finish.push(variant(&done));
        match &done {
            ExtractionDone::Scoped {
                result: Err(error), ..
            } => panic!("extraction failed after {finish:?}: {error}"),
            // The root's verdict carries the decoded namespace its child
            // engine reads, and its own roster is the ordinary sibling alone.
            ExtractionDone::Scoped {
                decoded: Some(_),
                result: Ok(result),
                ..
            } => assert_eq!(result.extracted, ["ordinary.bin"]),
            _ => {}
        }
        pipeline.handle_extraction_done(done).await;
    }
    assert!(
        finish
            .iter()
            .any(|done| done.starts_with("Scoped(decoded=true")),
        "the root reports through its decoded namespace: {finish:?}"
    );
    assert_eq!(
        std::fs::read(staging.join("ordinary.bin")).unwrap(),
        ordinary
    );
    assert_eq!(std::fs::read(staging.join("leaf.bin")).unwrap(), leaf);
    assert!(!staging.join("inner.7z").exists());
    pipeline.direct_unpack_shutdown("test teardown").await;
}

// The completion's kind and verdict, for a failure message.
fn variant(done: &ExtractionDone) -> String {
    match done {
        ExtractionDone::NativeChaseResolved { result, .. } => {
            format!("NativeChaseResolved(ok={})", result.is_ok())
        }
        ExtractionDone::ScopedNamespace { namespace, .. } => {
            format!("ScopedNamespace({namespace})")
        }
        ExtractionDone::Scoped {
            decoded, result, ..
        } => format!(
            "Scoped(decoded={}, ok={})",
            decoded.is_some(),
            result.is_ok()
        ),
        ExtractionDone::Batch { result, .. } => format!("Batch(ok={})", result.is_ok()),
        ExtractionDone::FullSet { result, .. } => format!("FullSet(ok={})", result.is_ok()),
    }
}

// An AES outer whose inner archive is AES under a different password. The
// inner's password is the first candidate, so the outer must not stop at it,
// and the nested engine must find its own key among the same candidates.
// Run once with the outer's headers in the clear and once with them
// encrypted. A third run stores the outer in the clear and offers the inner's
// key only as a settings password, so the nested engine has no job password
// to start from.
#[tokio::test]
async fn encrypted_outer_streams_its_encrypted_inner_archive() {
    use sevenz_turbo::encoder_options::{AesEncoderOptions, Lzma2Options};
    use sevenz_turbo::{ArchiveEntry, ArchiveWriter, EncoderConfiguration, EncoderMethod};
    const OUTER_PASSWORD: &str = "outer-cobalt-key";
    const INNER_PASSWORD: &str = "inner-saffron-key";
    #[derive(Clone, Copy, Debug)]
    enum Outer {
        Plain,
        Encrypted { encrypt_header: bool },
    }
    // Without a password the archive is stored, so nothing about the outer
    // stands between the chase and the inner's bytes.
    fn archive(
        entries: &[(&str, Vec<u8>)],
        password: Option<&str>,
        encrypt_header: bool,
    ) -> Vec<u8> {
        let mut writer = ArchiveWriter::new(std::io::Cursor::new(Vec::new())).unwrap();
        writer.set_content_methods(match password {
            Some(password) => vec![
                EncoderConfiguration::from(Lzma2Options::from_level(1)),
                AesEncoderOptions {
                    password: sevenz_turbo::Password::new(password),
                    iv: [11; 16],
                    salt: [19; 16],
                    num_cycles_power: 8,
                }
                .into(),
            ],
            None => vec![EncoderConfiguration::new(EncoderMethod::COPY)],
        });
        writer.set_encrypt_header(encrypt_header);
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

    for (job, outer) in [
        (
            JobId(44781),
            Outer::Encrypted {
                encrypt_header: false,
            },
        ),
        (
            JobId(44782),
            Outer::Encrypted {
                encrypt_header: true,
            },
        ),
        (JobId(44783), Outer::Plain),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
        enable_direct_unpack(&mut pipeline);
        // Non-uniform bytes, past one 128 KiB frame.
        let leaf: Vec<u8> = (0..131_101u32)
            .map(|index| (index.wrapping_mul(2_654_435_761) >> 13) as u8)
            .collect();
        let inner = archive(&[("leaf.bin", leaf.clone())], Some(INNER_PASSWORD), false);
        let ordinary = vec![71; 65539];
        let (outer_password, encrypt_header) = match outer {
            Outer::Plain => (None, false),
            Outer::Encrypted { encrypt_header } => (Some(OUTER_PASSWORD), encrypt_header),
        };
        let archive_bytes = archive(
            &[("ordinary.bin", ordinary.clone()), ("inner.7z", inner)],
            outer_password,
            encrypt_header,
        );

        // The spec's password is the first candidate (`Explicit`); the global
        // setting follows it (`Settings`). The plain outer's job has no spec
        // password, so the inner's key is a settings candidate alone.
        let (settings, explicit, expected) = match outer {
            Outer::Plain => (INNER_PASSWORD, None, vec![INNER_PASSWORD]),
            Outer::Encrypted { .. } => (
                OUTER_PASSWORD,
                Some(INNER_PASSWORD),
                vec![INNER_PASSWORD, OUTER_PASSWORD],
            ),
        };
        pipeline
            .db
            .save_archive_password_settings(Some(vec![settings.into()]), None)
            .unwrap();
        let files = vec![("silver_horizon.7z".to_string(), archive_bytes)];
        let mut spec = rar_job_spec("Silver Horizon", &files);
        spec.password = explicit.map(Into::into);
        insert_active_job(&mut pipeline, job, spec).await;
        assert_eq!(
            pipeline
                .archive_password_candidates_for_job(job)
                .candidates
                .iter()
                .map(|candidate| candidate.value())
                .collect::<Vec<_>>(),
            expected,
            "{outer:?}: the job's password candidates"
        );
        let set = "silver_horizon.7z";
        write_and_complete_file(&mut pipeline, job, 0, &files[0].0, &files[0].1).await;

        // An encrypted header lists only under the outer's key, so the chase
        // proves that candidate and streams the ordinary leaf, leaving the
        // inner to a child engine; the plain outer needs no key at all. A
        // clear header lists under any key, so the chase takes the first
        // candidate, the inner's, runs to its refusal, and the full-set path
        // carries the candidates.
        if pipeline.direct_unpack.is_armed(job, set) {
            while !pipeline.direct_unpack.armed_worker_finished(job, set) {
                while let Ok(done) = pipeline.extract_done_rx.try_recv() {
                    pipeline.handle_extraction_done(done).await;
                }
                tokio::task::yield_now().await;
            }
            pipeline.reap_direct_unpack().await;
        }
        while let Ok(done) = pipeline.extract_done_rx.try_recv() {
            pipeline.handle_extraction_done(done).await;
        }
        let chased = pipeline
            .direct_unpack
            .outcome(job, set)
            .and_then(|outcome| outcome.result.as_ref().ok())
            .map(|result| result.extracted.as_slice());
        match outer {
            Outer::Encrypted {
                encrypt_header: false,
            } => assert_eq!(
                chased, None,
                "{outer:?}: the chase opened the outer with the inner's key"
            ),
            Outer::Encrypted {
                encrypt_header: true,
            }
            | Outer::Plain => assert_eq!(
                chased,
                Some(["ordinary.bin".to_string()].as_slice()),
                "{outer:?}: the chase streams the outer"
            ),
        }

        pipeline.extract_7z_set(job, set).await.unwrap();
        // The outer closes first and delegates the inner to its own engine;
        // the leaf is published once every namespace has its verdict.
        let mut finish = Vec::new();
        loop {
            match pipeline.archive_runtime.get(&job) {
                Some(runtime) if runtime.publication_ready() => break,
                Some(_) => {}
                None => panic!("{outer:?}: the archive runtime left after {finish:?}"),
            }
            let done = pipeline.extract_done_rx.recv().await.unwrap();
            finish.push(variant(&done));
            if let ExtractionDone::FullSet {
                result: Err(error), ..
            }
            | ExtractionDone::Scoped {
                result: Err(error), ..
            } = &done
            {
                panic!("{outer:?}: extraction failed after {finish:?}: {error}");
            }
            pipeline.handle_extraction_done(done).await;
            assert!(
                pipeline
                    .jobs
                    .get(&job)
                    .is_some_and(|state| !matches!(state.status, crate::JobStatus::Failed { .. })),
                "{outer:?}: job failed after {finish:?}: {:?}",
                pipeline.jobs.get(&job).map(|state| &state.status)
            );
        }
        let staging = pipeline.extraction_staging_dir(job);
        assert_eq!(
            std::fs::read(staging.join("ordinary.bin")).unwrap(),
            ordinary,
            "{outer:?}: the outer's ordinary leaf"
        );
        assert_eq!(
            std::fs::read(staging.join("leaf.bin")).unwrap(),
            leaf,
            "{outer:?}: the nested engine's leaf"
        );
        assert!(
            !staging.join("inner.7z").exists(),
            "{outer:?}: the inner archive never lands on disk"
        );
        pipeline.direct_unpack_shutdown("test teardown").await;
    }
}

// A member named like an archive that is not one. Routing takes it as a child
// by its name, its first bytes refute the name, and no repair could change
// that, so it lands as the ordinary file it is and the job completes.
#[tokio::test]
async fn a_routed_child_that_is_no_archive_lands_as_a_file() {
    use sevenz_turbo::{ArchiveEntry, ArchiveWriter, EncoderConfiguration, EncoderMethod};
    let messages = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let _capture =
        tracing::subscriber::set_default(CapturedMessages(std::sync::Arc::clone(&messages)));
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    enable_direct_unpack(&mut pipeline);
    let job = JobId(44784);
    // Non-uniform bytes with no archive signature, past one 128 KiB frame.
    let notes: Vec<u8> = (0..131_101u32)
        .map(|index| (index.wrapping_mul(2_654_435_761) >> 11) as u8 | 0x80)
        .collect();
    let ordinary = vec![71; 65539];
    let mut writer = ArchiveWriter::new(std::io::Cursor::new(Vec::new())).unwrap();
    writer.set_content_methods(vec![EncoderConfiguration::new(EncoderMethod::COPY)]);
    for (name, bytes) in [("ordinary.bin", &ordinary), ("notes.7z", &notes)] {
        writer
            .push_archive_entry(
                ArchiveEntry::new_file(name),
                Some(std::io::Cursor::new(bytes.clone())),
            )
            .unwrap();
    }
    let files = vec![(
        "silver_horizon.7z".to_string(),
        writer.finish().unwrap().into_inner(),
    )];
    insert_active_job(&mut pipeline, job, rar_job_spec("Silver Horizon", &files)).await;
    let set = "silver_horizon.7z";
    write_and_complete_file(&mut pipeline, job, 0, &files[0].0, &files[0].1).await;
    if pipeline.direct_unpack.is_armed(job, set) {
        while !pipeline.direct_unpack.armed_worker_finished(job, set) {
            while let Ok(done) = pipeline.extract_done_rx.try_recv() {
                pipeline.handle_extraction_done(done).await;
            }
            tokio::task::yield_now().await;
        }
        pipeline.reap_direct_unpack().await;
    }
    while let Ok(done) = pipeline.extract_done_rx.try_recv() {
        pipeline.handle_extraction_done(done).await;
    }

    pipeline.extract_7z_set(job, set).await.unwrap();
    let mut finish = Vec::new();
    loop {
        match pipeline.archive_runtime.get(&job) {
            Some(runtime) if runtime.publication_ready() => break,
            Some(_) => {}
            None => panic!("the archive runtime left after {finish:?}"),
        }
        let done = pipeline.extract_done_rx.recv().await.unwrap();
        finish.push(variant(&done));
        pipeline.handle_extraction_done(done).await;
        assert!(
            pipeline
                .jobs
                .get(&job)
                .is_some_and(|state| !matches!(state.status, crate::JobStatus::Failed { .. })),
            "job failed after {finish:?}: {:?}",
            pipeline.jobs.get(&job).map(|state| &state.status)
        );
    }
    let staging = pipeline.extraction_staging_dir(job);
    assert_eq!(
        std::fs::read(staging.join("ordinary.bin")).unwrap(),
        ordinary
    );
    assert_eq!(std::fs::read(staging.join("notes.7z")).unwrap(), notes);
    let materialized: Vec<_> = messages
        .lock()
        .unwrap()
        .iter()
        .filter(|message| message.contains("materialized as a file"))
        .cloned()
        .collect();
    assert_eq!(materialized.len(), 1, "{materialized:?}");
    assert!(
        materialized[0].contains("reason=\"not_an_archive\""),
        "{materialized:?}"
    );
    pipeline.direct_unpack_shutdown("test teardown").await;
}

// Every message the thread logs, with its fields.
struct CapturedMessages(std::sync::Arc<std::sync::Mutex<Vec<String>>>);

impl tracing::Subscriber for CapturedMessages {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        struct Fields(String);
        impl tracing::field::Visit for Fields {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                use std::fmt::Write;
                let _ = write!(self.0, " {field}={value:?}");
            }
        }
        let mut fields = Fields(String::new());
        event.record(&mut fields);
        self.0.lock().unwrap().push(fields.0);
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
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
