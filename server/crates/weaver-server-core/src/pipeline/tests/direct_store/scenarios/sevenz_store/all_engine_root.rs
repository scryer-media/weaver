// A stored root whose every member is handed to a child engine leaves nothing
// of its own in the output: its installation is empty, and the graph is what
// holds its members.

use super::*;
use crate::pipeline::direct_store::restart::ArchiveRestoreRows;
use std::io::Write;

struct FinalizedRoot {
    pipeline: Pipeline,
    spec: JobSpec,
    working: PathBuf,
    output: PathBuf,
    _temp: tempfile::TempDir,
}

fn leaf() -> Vec<u8> {
    payload(41, 196_613)
}

// A stored 7z root whose one member, `name`, is a child engine, with every
// article routed: the root has finalized and its child has not been driven.
async fn finalize_all_engine_root(
    job: JobId,
    name: &'static str,
    member: Vec<u8>,
) -> FinalizedRoot {
    let root = build_7z(&[Entry::file(name, member)], EncoderMethod::COPY, None);
    let volumes = split_volumes(&root, 1);
    let spec = sevenz_job_spec(&volumes, 2);

    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let working = insert_active_job(&mut pipeline, job, spec.clone()).await;
    submit_volume_article(&mut pipeline, job, &volumes, 0, 0).await;
    submit_volume_article(&mut pipeline, job, &volumes, 0, 1).await;
    assert!(
        pipeline
            .direct_store
            .set(job, 0)
            .is_some_and(|set| set.is_finalized()),
        "{}",
        debug_job_state(&pipeline, job)
    );
    let output = complete_dir.join(crate::jobs::working_dir::sanitize_dirname(&spec.name));
    FinalizedRoot {
        pipeline,
        spec,
        working,
        output,
        _temp: temp,
    }
}

#[tokio::test]
async fn a_store_root_whose_only_member_is_a_child_engine_completes() {
    let job = JobId(862101);
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    encoder.write_all(&leaf()).unwrap();
    let mut root = finalize_all_engine_root(job, "leaf.bin.gz", encoder.finish().unwrap()).await;
    root.pipeline.check_job_completion(job).await;
    drain_rar_refreshes(&mut root.pipeline).await;
    drive_extractions_observing(&mut root.pipeline, job, |_| {}).await;
    assert!(
        matches!(
            job_status_for_assert(&root.pipeline, job),
            Some(JobStatus::Complete)
        ),
        "{}",
        debug_job_state(&root.pipeline, job)
    );
    assert_eq!(std::fs::read(root.output.join("leaf.bin")).unwrap(), leaf());
    assert!(!root.output.join("leaf.bin.gz").exists());
}

// A 7z child born an engine learns its length from the root's map after it
// is bound, so its engine can seek to the map at its end and finish.
#[tokio::test]
async fn a_store_root_whose_only_member_is_a_7z_child_engine_completes() {
    let job = JobId(862103);
    let inner = build_7z(
        &[Entry::file("leaf.bin", leaf())],
        EncoderMethod::LZMA2,
        None,
    );
    let mut root = finalize_all_engine_root(job, "inner.7z", inner).await;
    root.pipeline.check_job_completion(job).await;
    drain_rar_refreshes(&mut root.pipeline).await;
    drive_extractions_observing(&mut root.pipeline, job, |_| {}).await;
    assert!(
        matches!(
            job_status_for_assert(&root.pipeline, job),
            Some(JobStatus::Complete)
        ),
        "{}",
        debug_job_state(&root.pipeline, job)
    );
    assert_eq!(std::fs::read(root.output.join("leaf.bin")).unwrap(), leaf());
    assert!(!root.output.join("inner.7z").exists());
}

// The checkpoint the finalization committed records the root as installed
// with no output of its own, and a restart restores it finalized from that
// alone instead of refusing the job's whole graph. The restored job then
// fetches nothing, re-dispatches the child the root handed `inner.7z` to, and
// publishes its leaf.
#[tokio::test]
async fn an_all_engine_root_restores_installed_from_its_empty_marker() {
    let job = JobId(862102);
    let inner = build_7z(
        &[Entry::file("leaf.bin", leaf())],
        EncoderMethod::LZMA2,
        None,
    );
    let mut root = finalize_all_engine_root(job, "inner.7z", inner).await;
    let rows = root
        .pipeline
        .load_archive_restore_rows(job, &root.working)
        .await
        .unwrap();
    let ArchiveRestoreRows::Graph(graph) = &rows else {
        panic!("a nested root's finalization commits the job graph");
    };
    let markers: Vec<_> = graph
        .installed
        .iter()
        .map(|installed| {
            (
                installed.marker.members.len(),
                installed.marker.directories.len(),
            )
        })
        .collect();
    assert_eq!(markers, [(0, 0)]);

    let restored = root
        .pipeline
        .restore_direct_store_coverage(job, &root.spec, &root.working, &HashMap::new(), rows)
        .await
        .unwrap();
    assert_eq!(restored.installed.len(), 1);
    assert!(restored.sets.iter().all(|set| set.is_finalized()));
    let segments: usize = root.spec.files.iter().map(|file| file.segments.len()).sum();
    assert_eq!(restored.skip.len(), segments);

    let FinalizedRoot {
        pipeline,
        spec,
        working,
        output,
        _temp: temp,
    } = root;
    let mut pipeline = pipeline;
    pipeline
        .demand_direct_store_barriers_for_all_jobs(BarrierDemand::Shutdown)
        .await;
    retire_pipeline_database(pipeline).await;

    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    pipeline
        .restore_job(RestoreJobRequest {
            job_id: job,
            job_hash: [0; 32],
            spec,
            complete_files: HashSet::new(),
            file_progress: HashMap::new(),
            detected_archives: HashMap::new(),
            file_identities: HashMap::new(),
            extracted_members: HashSet::new(),
            status: JobStatus::Downloading,
            download_state: None,
            post_state: None,
            run_state: None,
            queued_repair_at_epoch_ms: None,
            queued_extract_at_epoch_ms: None,
            paused_resume_status: None,
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            working_dir: working.clone(),
        })
        .await
        .unwrap();
    assert!(
        pipeline
            .direct_store
            .set(job, 0)
            .is_some_and(|set| set.is_finalized()),
        "{}",
        debug_job_state(&pipeline, job)
    );
    assert_eq!(
        peek_queued_segments(&mut pipeline, job),
        Vec::<(u32, u32)>::new(),
        "an installed root's volumes must not be fetched again"
    );

    pipeline.check_job_completion(job).await;
    drain_rar_refreshes(&mut pipeline).await;
    drive_extractions_observing(&mut pipeline, job, |_| {}).await;
    assert!(
        matches!(
            job_status_for_assert(&pipeline, job),
            Some(JobStatus::Complete)
        ),
        "{}",
        debug_job_state(&pipeline, job)
    );
    assert_eq!(std::fs::read(output.join("leaf.bin")).unwrap(), leaf());
    assert!(!output.join("inner.7z").exists());
}
