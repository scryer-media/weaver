use super::*;

#[tokio::test]
async fn compressed_store_compressed_dispatches_before_parent_close_and_fences_publication() {
    use crate::pipeline::completion::finalize::extract::{
        SevenZipDecodeMemory, SevenZipExtractionContext, extract_7z_to,
    };
    use crate::pipeline::extraction::ExtractionRoot;
    use crate::pipeline::extraction::output::{FileOutput, select::SelectOutput};

    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let job = JobId(861211);
    let leaf = vec![47; 196613];
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
    let outer = build_7z(
        &[Entry::file("stored.7z", stored)],
        EncoderMethod::LZMA2,
        None,
    );
    let volumes = split_volumes(&outer, 1);
    insert_active_job(&mut pipeline, job, sevenz_job_spec(&volumes, 1)).await;
    let working = pipeline.jobs[&job].working_dir.clone();
    let input = working.join(&volumes[0].0);
    std::fs::write(&input, &outer).unwrap();
    let staging = pipeline.extraction_staging_dir(job);
    let budget = pipeline.extraction_budget(job, &staging).unwrap();
    let mut dispatch = pipeline
        .admit_root_stream(
            job,
            &volumes[0].0,
            vec![(0, input)],
            crate::jobs::assembly::ArchiveType::SevenZip,
            staging.clone(),
            Arc::clone(&budget),
        )
        .unwrap();
    let parent = dispatch.owner;
    let request = dispatch.request.clone();
    let channel = dispatch.channel.take().unwrap();
    let context = SevenZipExtractionContext {
        job_id: job,
        set_name: volumes[0].0.clone(),
        root: Arc::new(ExtractionRoot::open(&staging).unwrap()),
        budget,
        password: Password::empty(),
        event_tx: tokio::sync::broadcast::channel(1).0,
        phase_counters: Arc::new(crate::jobs::PhaseCounters::default()),
        decode_memory: SevenZipDecodeMemory::ReservedForFixedThreads {
            end_header_bytes: u64::from_le_bytes(outer[20..28].try_into().unwrap()),
            allowance_cap: u64::MAX,
        },
        decode_threads: 1,
    };
    let (release, closed) = tokio::sync::oneshot::channel();
    let worker = tokio::task::spawn_blocking(move || {
        let result = {
            let output = SelectOutput {
                first: channel.clone(),
                second: FileOutput {
                    root: &context.root,
                    budget: &context.budget,
                },
                select_first: |_, _: &Path| true,
            };
            channel.run(|_| {
                extract_7z_to(context.stream_context(), &output, || {
                    Ok(std::io::Cursor::new(outer.clone()))
                })
            })
        };
        drop(channel);
        closed.blocking_recv().expect("explicit parent close");
        result
    });
    let sender = pipeline.extract_done_tx.clone();
    let collector = tokio::spawn(async move {
        let (result, decoded) = dispatch.collect(worker).await;
        sender
            .send(ExtractionDone::Scoped {
                job_id: job,
                owner: parent,
                request,
                decoded,
                result,
            })
            .await
            .unwrap();
    });

    // Completion of this specific descendant proves the production progress
    // handler dispatched its decoder while the parent's close gate was held.
    loop {
        let done = pipeline.extract_done_rx.recv().await.unwrap();
        let descendant = match &done {
            ExtractionDone::Scoped {
                result: Err(error), ..
            } => panic!("nested decode failed: {error}"),
            ExtractionDone::Scoped {
                owner,
                result: Ok(result),
                ..
            } if *owner != parent => {
                assert!(result.failed.is_empty());
                result.extracted.iter().any(|name| name == "leaf.bin")
            }
            ExtractionDone::Scoped { owner, .. } if *owner == parent => {
                panic!("parent closed before its explicit gate")
            }
            _ => false,
        };
        pipeline.handle_extraction_done(done).await;
        assert!(
            pipeline
                .jobs
                .get(&job)
                .is_some_and(|state| !matches!(state.status, crate::JobStatus::Failed { .. })),
            "composition must remain active until its descendant completes: {:?}",
            pipeline.jobs.get(&job).map(|state| &state.status)
        );
        assert!(
            !staging.join("leaf.bin").exists(),
            "unverified ancestry must fence output"
        );
        if descendant {
            break;
        }
    }
    assert!(!pipeline.archive_runtime[&job].publication_ready());
    assert!(!staging.join("stored.7z").exists());
    assert!(!staging.join("compressed.7z").exists());
    release.send(()).unwrap();
    loop {
        let done = pipeline.extract_done_rx.recv().await.unwrap();
        if let ExtractionDone::Scoped {
            result: Err(error), ..
        } = &done
        {
            panic!("nested decode failed: {error}");
        }
        let root_done = matches!(&done, ExtractionDone::Scoped { owner, .. } if *owner == parent);
        pipeline.handle_extraction_done(done).await;
        assert!(
            pipeline
                .jobs
                .get(&job)
                .is_some_and(|state| !matches!(state.status, crate::JobStatus::Failed { .. })),
            "composition must remain active until its root completes: {:?}",
            pipeline.jobs.get(&job).map(|state| &state.status)
        );
        if root_done {
            break;
        }
    }
    collector.await.unwrap();
    assert_eq!(std::fs::read(staging.join("leaf.bin")).unwrap(), leaf);
    assert!(!staging.join("stored.7z").exists());
    assert!(!staging.join("compressed.7z").exists());
    assert!(pipeline.archive_runtime[&job].publication_ready());
    assert!(
        pipeline.extract_done_rx.try_recv().is_err(),
        "delegated nodes must not decode twice"
    );
}
