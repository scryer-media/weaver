use super::*;
use std::io::Write;

fn zip(files: &[(&str, &[u8])]) -> Vec<u8> {
    let mut archive = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
    for (name, bytes) in files {
        archive
            .start_file(
                *name,
                zip::write::SimpleFileOptions::default()
                    .compression_method(zip::CompressionMethod::Stored),
            )
            .unwrap();
        archive.write_all(bytes).unwrap();
    }
    archive.finish().unwrap().into_inner()
}

fn protect(root: &Path, name: &str, source: &Path) -> Vec<PathBuf> {
    use par2_rs::create::{BlockSizing, Par2Creator, Par2CreatorOptions, RecoveryAmount};
    let mut options = Par2CreatorOptions::with_output(
        root.join(name),
        Some(root.to_path_buf()),
        vec![source.to_path_buf()],
    );
    options.block_sizing = BlockSizing::Bytes(1024);
    options.recovery_amount = RecoveryAmount::Count(1);
    options.volume_count = Some(1);
    let creator = Par2Creator::new(options);
    creator
        .create(&creator.plan().unwrap())
        .unwrap()
        .output_paths
}

async fn scoped_attempt(
    pipeline: &mut Pipeline,
    successful: bool,
) -> crate::pipeline::extraction::output::channel::StreamOwner {
    let done = next_extraction_done(pipeline).await;
    let ExtractionDone::Scoped {
        job_id,
        owner,
        result,
        ..
    } = &done
    else {
        panic!("expected scoped archive worker");
    };
    assert_eq!(
        result
            .as_ref()
            .is_ok_and(|outcome| outcome.failed.is_empty()),
        successful,
        "worker error: {:?}; failed members: {}",
        result.as_ref().err(),
        result
            .as_ref()
            .map(|outcome| outcome.failed.len())
            .unwrap_or(0)
    );
    let owner = *owner;
    let job_id = *job_id;
    pipeline.handle_extraction_done(done).await;
    if !successful {
        assert!(
            pipeline
                .archive_runtime
                .get(&job_id)
                .is_some_and(|runtime| runtime.pending()),
            "repair must dispatch a fresh generation: {}",
            debug_job_state(pipeline, job_id)
        );
    }
    owner
}

#[tokio::test]
async fn scoped_failure_repairs_and_replaces_generation_then_repairs_its_child() {
    let temp = TempDir::new().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let job_id = JobId(97131);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec("recursive-recovery", &[("posted.bin".into(), 1)]),
    )
    .await;
    let staging = pipeline.extraction_staging_dir(job_id);
    std::fs::create_dir_all(&staging).unwrap();
    let creator = temp.path().join("creator");
    std::fs::create_dir(&creator).unwrap();
    let payload: Vec<_> = (0..4096u32)
        .map(|index| (index * 29 + index / 13) as u8)
        .collect();
    let inner = zip(&[("leaf.bin", &payload)]);
    let inner_path = creator.join("inner.zip");
    std::fs::write(&inner_path, &inner).unwrap();
    let inner_recovery = protect(&creator, "inner.par2", &inner_path);
    let mut damaged_inner = inner.clone();
    let at = inner
        .windows(32)
        .position(|bytes| bytes == &payload[..32])
        .unwrap();
    damaged_inner[at + 1800] ^= 1;
    let mut middle_members = vec![("inner.zip".to_owned(), damaged_inner.clone())];
    for carrier in &inner_recovery {
        middle_members.push((
            carrier.file_name().unwrap().to_str().unwrap().to_owned(),
            std::fs::read(carrier).unwrap(),
        ));
    }
    let middle = zip(&middle_members
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect::<Vec<_>>());
    let middle_path = creator.join("middle.zip");
    std::fs::write(&middle_path, &middle).unwrap();
    let middle_recovery = protect(&creator, "middle.par2", &middle_path);
    let mut damaged_middle = middle.clone();
    let at = middle
        .windows(32)
        .position(|bytes| bytes == &damaged_inner[..32])
        .unwrap();
    damaged_middle[at + 700] ^= 1;
    std::fs::write(staging.join("middle.zip"), &damaged_middle).unwrap();
    let mut published = HashSet::from(["middle.zip".to_owned()]);
    for carrier in middle_recovery {
        let name = carrier.file_name().unwrap().to_str().unwrap().to_owned();
        std::fs::copy(carrier, staging.join(&name)).unwrap();
        published.insert(name);
    }
    pipeline.extracted_members.insert(job_id, published);
    set_job_status_for_test(&mut pipeline, job_id, JobStatus::Extracting);
    assert!(matches!(
        pipeline
            .maybe_start_nested_extraction(job_id)
            .await
            .unwrap(),
        crate::pipeline::completion::finalize::NestedExtractionDecision::Started
    ));
    let failed_parent = scoped_attempt(&mut pipeline, false).await;
    let repaired_parent = scoped_attempt(&mut pipeline, true).await;
    assert_eq!(failed_parent.node, repaired_parent.node);
    assert!(repaired_parent.generation > failed_parent.generation);
    assert_eq!(
        std::fs::read(staging.join("middle.zip")).unwrap(),
        damaged_middle
    );
    assert_eq!(
        std::fs::read(staging.join("inner.zip")).unwrap(),
        damaged_inner
    );
    let parent_effective = pipeline.archive_runtime[&job_id]
        .retained_work
        .iter()
        .find(|(owner, _)| *owner == repaired_parent)
        .unwrap()
        .1
        .inputs[0]
        .clone();
    assert_eq!(std::fs::read(&parent_effective).unwrap(), middle);
    assert!(matches!(
        pipeline
            .maybe_start_nested_extraction(job_id)
            .await
            .unwrap(),
        crate::pipeline::completion::finalize::NestedExtractionDecision::Started
    ));
    let runtime = &pipeline.archive_runtime[&job_id];
    let producer = runtime.output_owners["inner.zip"];
    assert!(
        runtime
            .output_owners
            .iter()
            .any(|(name, owner)| name.ends_with(".par2") && *owner == producer)
    );
    assert!(
        !pipeline.extracted_members[&job_id].contains("inner.par2"),
        "nested recovery uses its producer roster independently of the root extraction roster"
    );
    let failed_child = scoped_attempt(&mut pipeline, false).await;
    let repaired_child = scoped_attempt(&mut pipeline, true).await;
    assert_eq!(failed_child.node, repaired_child.node);
    assert!(repaired_child.generation > failed_child.generation);
    assert_ne!(repaired_child.node, repaired_parent.node);
    assert_eq!(std::fs::read(staging.join("leaf.bin")).unwrap(), payload);
    assert_eq!(
        std::fs::read(staging.join("inner.zip")).unwrap(),
        damaged_inner
    );
    assert_eq!(
        std::fs::read(staging.join("middle.zip")).unwrap(),
        damaged_middle
    );
    assert_eq!(std::fs::read(parent_effective).unwrap(), middle);
    let runtime = &pipeline.archive_runtime[&job_id];
    assert!(runtime.sessions.ancestry_verified(repaired_child));
    assert_ne!(
        runtime.sessions.original(repaired_child.node),
        runtime.sessions.effective(repaired_child.node)
    );
    assert!(matches!(
        pipeline.db.load_archive_authority(job_id).unwrap(),
        crate::jobs::archive_checkpoint::ArchiveAuthority::Job(_)
    ));
}
