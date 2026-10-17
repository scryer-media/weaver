use super::*;

async fn root_dispatch(entries: &[Entry]) -> (Pipeline, JobId, ExtractionDone, PathBuf) {
    root_dispatch_with_child_completions(entries, true).await
}

async fn root_dispatch_with_child_completions(
    entries: &[Entry],
    complete_children: bool,
) -> (Pipeline, JobId, ExtractionDone, PathBuf) {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let archive = build_7z(entries, EncoderMethod::LZMA2, None);
    let volumes = split_volumes(&archive, 1);
    let job_id = JobId(861201);
    let spec = sevenz_job_spec(&volumes, 1);
    insert_active_job(&mut pipeline, job_id, spec).await;
    for (index, (name, bytes)) in volumes.iter().enumerate() {
        write_and_complete_file(&mut pipeline, job_id, index as u32, name, bytes).await;
    }
    let set = pipeline.jobs[&job_id]
        .assembly
        .archive_topologies()
        .keys()
        .next()
        .unwrap()
        .clone();
    pipeline.taint_direct_unpack_set(job_id, &set);
    pipeline.extract_7z_set(job_id, &set).await.unwrap();
    let owner = pipeline.archive_runtime[&job_id].graph_owners[&(set.clone(), 0)];
    let done = loop {
        let done = next_extraction_done(&mut pipeline).await;
        if matches!(&done,ExtractionDone::Scoped{owner:actual,..}if *actual==owner) {
            break done;
        }
        if complete_children || !matches!(&done, ExtractionDone::Scoped { .. }) {
            pipeline.handle_extraction_done(done).await;
            assert!(
                pipeline.jobs.contains_key(&job_id),
                "namespace progress removed the root fixture job: {}",
                debug_job_state(&pipeline, job_id)
            );
        }
    };
    // Keep the temporary tree alive until the returned pipeline is dropped.
    let path = temp.keep();
    (pipeline, job_id, done, path)
}

// Handles extraction verdicts until the job has a terminal status.
async fn drain_until_terminal(pipeline: &mut Pipeline, job: JobId) {
    while !matches!(
        job_status_for_assert(pipeline, job),
        Some(JobStatus::Complete) | Some(JobStatus::Failed { .. })
    ) {
        let done = next_extraction_done(pipeline).await;
        pipeline.handle_extraction_done(done).await;
    }
}

#[tokio::test]
async fn ordinary_root_keeps_file_output_and_never_builds_decoded_graph() {
    let bytes = vec![71; 32769];
    let (mut pipeline, job_id, done, path) =
        root_dispatch(&[Entry::file("ordinary.bin", bytes.clone())]).await;
    let ExtractionDone::Scoped {
        decoded: None,
        result: Ok(outcome),
        ..
    } = &done
    else {
        panic!("ordinary root should not allocate a decoded namespace");
    };
    assert_eq!(outcome.extracted, ["ordinary.bin"]);
    let staging = pipeline.extraction_staging_dir(job_id);
    assert_eq!(std::fs::read(staging.join("ordinary.bin")).unwrap(), bytes);
    pipeline.handle_extraction_done(done).await;
    assert!(!pipeline.archive_runtime.contains_key(&job_id));
    drop(pipeline);
    std::fs::remove_dir_all(path).unwrap();
}

// A stored 7z inside a compressed root: the root's decoder yields the child
// in order and cannot seek to its map, so the child is read by an engine
// rather than routed, and the root's own files are untouched by it.
#[tokio::test]
async fn compressed_root_hands_a_stored_child_to_an_engine_and_retains_original_graph() {
    use crate::pipeline::direct_store::nested::NodeMode;
    let leaf = vec![37; 131073];
    let inner = build_7z(
        &[Entry::file("leaf.bin", leaf.clone())],
        EncoderMethod::COPY,
        None,
    );
    let ordinary = vec![93; 65539];
    let (mut pipeline, job_id, done, path) = root_dispatch(&[
        Entry::file("ordinary.bin", ordinary.clone()),
        Entry::file("inner.7z", inner),
    ])
    .await;
    let ExtractionDone::Scoped {
        decoded: Some(decoded),
        result: Ok(outcome),
        ..
    } = &done
    else {
        panic!("compressed root must use scoped archive output");
    };
    assert!(outcome.failed.is_empty());
    assert_eq!(decoded.graph.nodes.len(), 1);
    assert_eq!(decoded.graph.node(1).mode, NodeMode::Engine);
    // The engine owns the child's member, so the root's namespace routes none.
    assert!(decoded.publication().unwrap().is_empty());
    let staging = pipeline.extraction_staging_dir(job_id);
    assert_eq!(
        std::fs::read(staging.join("ordinary.bin")).unwrap(),
        ordinary
    );
    assert!(!staging.join("inner.7z").exists());
    let root_files: Vec<_> = pipeline.jobs[&job_id]
        .assembly
        .files()
        .map(|file| file.filename().to_owned())
        .collect();
    pipeline.handle_extraction_done(done).await;
    assert_eq!(
        pipeline.jobs[&job_id]
            .assembly
            .files()
            .map(|file| file.filename().to_owned())
            .collect::<Vec<_>>(),
        root_files
    );
    // The child's engine reports after the root's decoder; its verdict is
    // what publishes the leaf.
    while !staging.join("leaf.bin").exists() {
        let done = next_extraction_done(&mut pipeline).await;
        pipeline.handle_extraction_done(done).await;
    }
    assert_eq!(std::fs::read(staging.join("leaf.bin")).unwrap(), leaf);
    drop(pipeline);
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn persistent_sink_reuses_namespace_until_authoritative_close() {
    use crate::pipeline::extraction::output::ArchiveOutput;
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let job = JobId(861202);
    let spec = sevenz_job_spec(&[("source.7z".into(), vec![0; 32])], 1);
    insert_active_job(&mut pipeline, job, spec).await;
    let first = pipeline
        .scoped_archive_sink(
            job,
            "source",
            "first",
            Vec::new(),
            crate::jobs::assembly::ArchiveType::SevenZip,
        )
        .await
        .unwrap();
    let second = pipeline
        .scoped_archive_sink(
            job,
            "source",
            "second",
            Vec::new(),
            crate::jobs::assembly::ArchiveType::SevenZip,
        )
        .await
        .unwrap();
    tokio::task::spawn_blocking(move || {
        first.directory(0, Path::new("first")).unwrap();
        second.directory(0, Path::new("second")).unwrap();
    })
    .await
    .unwrap();
    assert!(pipeline.extract_done_rx.try_recv().is_err());
    pipeline.close_scoped_archive_sink(job, "source");
    let done = loop {
        let done = next_extraction_done(&mut pipeline).await;
        if matches!(&done, ExtractionDone::ScopedNamespace { .. }) {
            pipeline.handle_extraction_done(done).await;
        } else {
            break done;
        }
    };
    let ExtractionDone::Scoped {
        decoded: Some(decoded),
        result: Ok(_),
        ..
    } = &done
    else {
        panic!("persistent collector should close successfully");
    };
    use crate::pipeline::extraction::catalog::MemberCatalog;
    assert_eq!(
        decoded
            .catalog
            .members()
            .iter()
            .map(|member| member.name)
            .collect::<Vec<_>>(),
        ["first", "second"]
    );
    pipeline.handle_extraction_done(done).await;
    assert!(pipeline.extracted_archives[&job].contains("source"));
    let runtime = &pipeline.archive_runtime[&job];
    assert!(!runtime.scoped_decision_pending("source"));
    assert!(
        runtime
            .retained_work
            .iter()
            .any(|(_, work)| work.root_set.as_deref() == Some("source"))
    );
}

#[tokio::test]
async fn persistent_sink_failure_before_first_frame_preserves_error() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let job = JobId(861203);
    let spec = sevenz_job_spec(&[("source.7z".into(), vec![0; 32])], 1);
    insert_active_job(&mut pipeline, job, spec).await;
    let sink = pipeline
        .scoped_archive_sink(
            job,
            "source",
            "inner.7z",
            Vec::new(),
            crate::jobs::assembly::ArchiveType::SevenZip,
        )
        .await
        .unwrap();
    pipeline.revoke_scoped_archive_sink(
        job,
        "source",
        crate::pipeline::extraction::ExtractionError::terminal("producer failed before header"),
    );
    drop(sink);
    let done = next_extraction_done(&mut pipeline).await;
    let ExtractionDone::Scoped {
        owner,
        request,
        result: Err(error),
        ..
    } = done
    else {
        panic!("failure must not become empty success");
    };
    assert!(error.to_string().contains("producer failed before header"));
    assert!(
        pipeline
            .archive_runtime
            .get_mut(&job)
            .unwrap()
            .complete(owner, request)
            .is_some()
    );
}

#[tokio::test]
async fn decoded_root_repair_includes_only_its_producers_ordinary_siblings() {
    use crate::pipeline::extraction::runtime::VirtualInputSource;
    use crate::pipeline::extraction::session::InputVersion;
    let compressed = build_7z(
        &[Entry::file("leaf.bin", vec![39; 4096])],
        EncoderMethod::LZMA2,
        None,
    );
    let (mut pipeline, job, done, path) = root_dispatch(&[
        Entry::file("compressed.7z", compressed),
        Entry::file("local.par2", vec![53; 128]),
    ])
    .await;
    let ExtractionDone::Scoped {
        owner,
        request,
        decoded: Some(decoded),
        result: Ok(outcome),
        ..
    } = done
    else {
        panic!("expected decoded parent completion");
    };
    assert!(outcome.failed.is_empty());
    assert_eq!(decoded.graph.nodes.len(), 1);
    let source = VirtualInputSource {
        root_set: format!("decoded-{}-{}", owner.node, owner.generation),
        node: 1,
    };
    let staging = pipeline.extraction_staging_dir(job);
    std::fs::remove_file(staging.join("local.par2")).unwrap();
    std::fs::write(staging.join("unrelated.par2"), [71; 128]).unwrap();
    std::fs::create_dir(staging.join("empty")).unwrap();
    let runtime = pipeline.archive_runtime.get_mut(&job).unwrap();
    let mut root_work = runtime.complete(owner, request).unwrap();
    // Accepted native chase work has no conventional root continuation.
    root_work.root_set = None;
    root_work.ordinary = vec!["local.par2".into(), "empty".into()];
    let unrelated = runtime.admit(None, InputVersion(0)).unwrap().owner;
    let mut unrelated_work = root_work.clone();
    unrelated_work.ordinary = vec!["unrelated.par2".into()];
    runtime.retained_work.push((unrelated, unrelated_work));
    runtime.completed.push((owner, root_work, Vec::new()));
    runtime
        .graph_owners
        .insert((source.root_set.clone(), 0), owner);
    runtime.decoded.push(*decoded);
    assert!(
        pipeline
            .virtual_repair_inventory_for_test(job, &source)
            .is_none()
    );
    pipeline
        .archive_runtime
        .get_mut(&job)
        .unwrap()
        .sessions
        .close(owner)
        .unwrap();
    assert!(
        pipeline
            .virtual_repair_inventory_for_test(job, &source)
            .is_none(),
        "native sidecars must be installed before recovery reads them"
    );
    std::fs::write(staging.join("local.par2"), [53; 128]).unwrap();
    let inventory = pipeline
        .virtual_repair_inventory_for_test(job, &source)
        .unwrap();
    assert_eq!(
        inventory
            .inputs
            .values()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>(),
        ["compressed.7z"]
    );
    assert_eq!(
        inventory
            .siblings
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["local.par2"]
    );
    assert_eq!(
        std::fs::read(&inventory.siblings["local.par2"]).unwrap(),
        vec![53; 128]
    );
    assert_eq!(inventory.siblings["local.par2"], staging.join("local.par2"));
    drop(pipeline);
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn decoded_mixed_graph_restart_replays_child_and_publishes_leaf() {
    use crate::pipeline::direct_store::manifest::graph::JobGraph;
    use crate::pipeline::direct_store::restart::ArchiveRestoreRows;
    let leaf = vec![43; 65539];
    let compressed = build_7z(
        &[Entry::file("leaf.bin", leaf.clone())],
        EncoderMethod::LZMA2,
        None,
    );
    let stored = build_7z(
        &[Entry::file("compressed.7z", compressed)],
        EncoderMethod::COPY,
        None,
    );
    let (mut pipeline, job, done, path) =
        root_dispatch_with_child_completions(&[Entry::file("stored.7z", stored)], false).await;
    let ExtractionDone::Scoped {
        owner,
        request,
        decoded: Some(decoded),
        result: Ok(outcome),
        ..
    } = done
    else {
        panic!("expected decoded parent completion");
    };
    assert!(outcome.failed.is_empty());
    let working = pipeline.jobs[&job].working_dir.clone();
    let spec = pipeline.jobs[&job].spec.clone();
    let staging = pipeline.extraction_staging_dir(job);
    assert!(!staging.join("leaf.bin").exists());
    let runtime = pipeline.archive_runtime.get_mut(&job).unwrap();
    let work = runtime.complete(owner, request).unwrap();
    runtime.sessions.close(owner).unwrap();
    runtime.accept_namespace(&format!("decoded-{}-{}", owner.node, owner.generation));
    runtime.retained_work.push((owner, work));
    runtime.decoded.push(*decoded);
    let old_root = runtime.root;
    let mut graph = JobGraph::empty(1);
    runtime.append_checkpoint(&mut graph).unwrap();
    for root in &graph.roots {
        if let Some(decoded) = &root.decoded {
            decoded
                .registry
                .validate()
                .unwrap_or_else(|error| panic!("{error}: {:?}", decoded.registry));
        }
    }
    let graph = JobGraph::decode(&graph.encode().unwrap()).unwrap();
    pipeline.archive_runtime.remove(&job);
    let restored = pipeline
        .restore_direct_store_coverage(
            job,
            &spec,
            &working,
            &HashMap::new(),
            ArchiveRestoreRows::Graph(Box::new(graph)),
        )
        .await
        .unwrap();
    assert!(
        restored.sets.is_empty(),
        "decoded graph restore must not invent posted roots"
    );
    assert_ne!(pipeline.archive_runtime[&job].root, old_root);
    assert!(
        !pipeline.archive_runtime[&job].replay.is_empty(),
        "in-flight child must remain replayable"
    );
    pipeline.resume_scoped_replay(job).await.unwrap();
    loop {
        let done = next_extraction_done(&mut pipeline).await;
        let replayed_leaf = matches!(&done,
            ExtractionDone::Scoped { owner, result: Ok(outcome), .. }
                if owner.root != old_root && outcome.extracted.iter().any(|name| name == "leaf.bin")
        );
        if let ExtractionDone::Scoped {
            owner,
            result: Err(error),
            ..
        } = &done
        {
            assert_eq!(owner.root, old_root, "restored child failed: {error}");
        }
        pipeline.handle_extraction_done(done).await;
        if replayed_leaf {
            break;
        }
    }
    assert_eq!(std::fs::read(staging.join("leaf.bin")).unwrap(), leaf);
    assert!(!pipeline.archive_runtime[&job].pending());
    drop(pipeline);
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn scoped_publication_preserves_empty_directory_and_member_metadata() {
    use crate::pipeline::extraction::output::{ArchiveOutput, Integrity, Metadata};
    use std::io::Write;
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let job = JobId(861204);
    insert_active_job(
        &mut pipeline,
        job,
        sevenz_job_spec(&[("source.7z".into(), vec![0; 32])], 1),
    )
    .await;
    let directory = pipeline
        .scoped_archive_sink(
            job,
            "source",
            "empty",
            Vec::new(),
            crate::jobs::assembly::ArchiveType::SevenZip,
        )
        .await
        .unwrap();
    let file = pipeline
        .scoped_archive_sink(
            job,
            "source",
            "leaf.bin",
            Vec::new(),
            crate::jobs::assembly::ArchiveType::SevenZip,
        )
        .await
        .unwrap();
    let modified =
        std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1_700_000_000);
    tokio::task::spawn_blocking(move || {
        directory.directory(0, Path::new("empty")).unwrap();
        directory
            .directory_metadata(
                0,
                Path::new("empty"),
                Metadata {
                    modified: Some(modified),
                    accessed: None,
                    unix_mode: Some(0o750),
                },
            )
            .unwrap();
        let mut member = file.file(0, Path::new("leaf.bin"), Some(4)).unwrap();
        member.write_all(b"leaf").unwrap();
        member
            .metadata(Metadata {
                modified: Some(modified),
                accessed: None,
                unix_mode: Some(0o640),
            })
            .unwrap();
        member
            .finish_data()
            .unwrap()
            .integrity(Integrity::Verified)
            .unwrap();
    })
    .await
    .unwrap();
    pipeline.close_scoped_archive_sink(job, "source");
    let done = next_extraction_done(&mut pipeline).await;
    assert!(
        matches!(&done, ExtractionDone::Scoped { result: Ok(outcome), .. } if outcome.failed.is_empty())
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let ExtractionDone::Scoped {
            decoded: Some(decoded),
            ..
        } = &done
        else {
            panic!("expected private decoded output");
        };
        let selected = decoded.publication().unwrap();
        assert_eq!(
            std::fs::metadata(&selected["leaf.bin"])
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o640
        );
        assert_eq!(
            std::fs::metadata(&selected["empty"])
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o750
        );
    }
    pipeline.handle_extraction_done(done).await;
    let staging = pipeline.extraction_staging_dir(job);
    assert_eq!(std::fs::read(staging.join("leaf.bin")).unwrap(), b"leaf");
    let file = std::fs::metadata(staging.join("leaf.bin")).unwrap();
    let directory = std::fs::metadata(staging.join("empty")).unwrap();
    assert!(directory.is_dir());
    assert_eq!(file.modified().unwrap(), modified);
    assert_eq!(directory.modified().unwrap(), modified);
    let working = pipeline.jobs[&job].working_dir.clone();
    let mut graph = crate::pipeline::direct_store::manifest::graph::JobGraph::empty(1);
    pipeline.archive_runtime[&job]
        .append_checkpoint(&mut graph)
        .unwrap();
    let graph =
        crate::pipeline::direct_store::manifest::graph::JobGraph::decode(&graph.encode().unwrap())
            .unwrap();
    graph.probe(&working, &staging).unwrap();
    assert_eq!(
        graph.files.len(),
        1,
        "directory must not acquire byte-file authority"
    );
    let private_directory = pipeline.archive_runtime[&job]
        .retained_work
        .iter()
        .find_map(|(_, work)| work.outputs.get("empty"))
        .unwrap();
    std::fs::remove_dir(private_directory).unwrap();
    assert!(graph.probe(&working, &staging).is_err());
    assert!(
        !private_directory.exists(),
        "graph probe must not recreate missing directories"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let baseline = temp.path().join("normalized-output");
        std::fs::create_dir_all(baseline.join("directory")).unwrap();
        std::fs::write(baseline.join("file"), b"ordinary").unwrap();
        crate::runtime::fs::normalize_extracted_paths(
            &baseline,
            &["file".into(), "directory".into()],
        )
        .unwrap();
        assert_eq!(
            file.permissions().mode() & 0o777,
            std::fs::metadata(baseline.join("file"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777
        );
        assert_eq!(
            directory.permissions().mode() & 0o777,
            std::fs::metadata(baseline.join("directory"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777
        );
    }
}

#[tokio::test]
async fn scoped_publication_reserves_ordinary_names_before_their_installation() {
    let stored = build_7z(
        &[Entry::file("leaf.bin", vec![29; 1024])],
        EncoderMethod::COPY,
        None,
    );
    let (mut pipeline, job, done, path) = root_dispatch(&[
        Entry::file("leaf.bin", vec![31; 1024]),
        Entry::file("inner.7z", stored),
    ])
    .await;
    let staging = pipeline.extraction_staging_dir(job);
    // Model the native handoff window before ordinary output installation.
    std::fs::remove_file(staging.join("leaf.bin")).unwrap();
    pipeline.handle_extraction_done(done).await;
    // The child is an engine, and its output is what collides.
    drain_until_terminal(&mut pipeline, job).await;
    assert!(matches!(
        job_status_for_assert(&pipeline, job),
        Some(JobStatus::Failed { .. })
    ));
    assert!(!staging.join("leaf.bin").exists());
    drop(pipeline);
    std::fs::remove_dir_all(path).unwrap();
}

fn store_archive_with_dataless_outputs() -> Vec<u8> {
    let mut writer = ArchiveWriter::new(std::io::Cursor::new(Vec::new())).unwrap();
    writer.set_content_methods(vec![EncoderConfiguration::new(EncoderMethod::COPY)]);
    let mut directory = ArchiveEntry::new_file("empty-directory");
    directory.is_directory = true;
    writer
        .push_archive_entry::<std::io::Cursor<Vec<u8>>>(directory, None)
        .unwrap();
    writer
        .push_archive_entry::<std::io::Cursor<Vec<u8>>>(ArchiveEntry::new_file("empty.bin"), None)
        .unwrap();
    writer
        .push_archive_entry(
            ArchiveEntry::new_file("leaf.bin"),
            Some(std::io::Cursor::new(vec![47; 1024])),
        )
        .unwrap();
    writer.finish().unwrap().into_inner()
}

// A stored child under a compressed root is read by an engine, and the
// engine's dataless outputs land exactly as a routed child's would.
#[tokio::test]
async fn store_child_dataless_outputs_land_through_its_engine() {
    let later = build_7z(
        &[Entry::file("later.bin", vec![61; 1024])],
        EncoderMethod::COPY,
        None,
    );
    let (mut pipeline, job, done, path) = root_dispatch(&[
        Entry::file("inner.7z", store_archive_with_dataless_outputs()),
        Entry::file("later.7z", later),
    ])
    .await;
    let ExtractionDone::Scoped {
        decoded: Some(decoded),
        result: Ok(outcome),
        ..
    } = &done
    else {
        panic!("expected decoded parent");
    };
    assert!(outcome.failed.is_empty());
    assert!(
        decoded
            .graph
            .nodes
            .iter()
            .all(|node| { node.mode == crate::pipeline::direct_store::nested::NodeMode::Engine })
    );
    assert!(decoded.publication().unwrap().is_empty());
    pipeline.handle_extraction_done(done).await;
    let staging = pipeline.extraction_staging_dir(job);
    while !(staging.join("leaf.bin").exists() && staging.join("later.bin").exists()) {
        let done = next_extraction_done(&mut pipeline).await;
        pipeline.handle_extraction_done(done).await;
    }
    assert!(staging.join("empty-directory").is_dir());
    assert_eq!(std::fs::read(staging.join("empty.bin")).unwrap(), b"");
    assert_eq!(
        std::fs::read(staging.join("leaf.bin")).unwrap(),
        vec![47; 1024]
    );
    assert_eq!(
        std::fs::read(staging.join("later.bin")).unwrap(),
        vec![61; 1024]
    );
    drop(pipeline);
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn store_child_empty_file_cannot_replace_ordinary_payload() {
    let (mut pipeline, job, done, path) = root_dispatch(&[
        Entry::file("empty.bin", vec![59; 1024]),
        Entry::file("inner.7z", store_archive_with_dataless_outputs()),
    ])
    .await;
    let staging = pipeline.extraction_staging_dir(job);
    pipeline.handle_extraction_done(done).await;
    // The child is an engine, and its output is what collides.
    drain_until_terminal(&mut pipeline, job).await;
    assert!(matches!(
        job_status_for_assert(&pipeline, job),
        Some(JobStatus::Failed { .. })
    ));
    assert_eq!(
        std::fs::read(staging.join("empty.bin")).unwrap(),
        vec![59; 1024]
    );
    drop(pipeline);
    std::fs::remove_dir_all(path).unwrap();
}
