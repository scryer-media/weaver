use super::*;

fn work() -> NamespaceWork {
    NamespaceWork {
        virtual_source: None,
        original_virtual_source: None,
        kind: crate::jobs::assembly::ArchiveType::SevenZip,
        set_name: "set".into(),
        volumes: Vec::new(),
        output: "private".into(),
        inputs: Vec::new(),
        original_inputs: Vec::new(),
        input_names: Vec::new(),
        destination: "staging".into(),
        outputs: BTreeMap::new(),
        root_set: None,
        ordinary: Vec::new(),
        selected_password: None,
    }
}

#[test]
fn request_identity_rejects_previous_runtime_and_duplicate_completion() {
    let mut old = ArchiveRuntime::new();
    let old_owner = old.admit(None, InputVersion(0)).unwrap().owner;
    let old_request = old.dispatch(old_owner, work()).unwrap();
    let mut current = ArchiveRuntime::new();
    let owner = current.admit(None, InputVersion(0)).unwrap().owner;
    let request = current.dispatch(owner, work()).unwrap();
    assert!(current.complete(owner, old_request).is_none());
    assert!(current.pending());
    assert!(current.complete(owner, request.clone()).is_some());
    assert!(current.complete(owner, request).is_none());
}

#[test]
fn sibling_and_ancestor_integrity_gate_publication() {
    let mut runtime = ArchiveRuntime::new();
    let parent = runtime.admit(None, InputVersion(0)).unwrap().owner;
    let child = runtime.admit(Some(parent), InputVersion(1)).unwrap().owner;
    let request = runtime.dispatch(child, work()).unwrap();
    let work = runtime.complete(child, request).unwrap();
    runtime.sessions.close(child).unwrap();
    runtime.completed.push((child, work, Vec::new()));
    assert!(!runtime.publication_ready());
    runtime.sessions.close(parent).unwrap();
    assert!(runtime.publication_ready());
    let sibling = runtime.admit(None, InputVersion(2)).unwrap().owner;
    let request = runtime.dispatch(sibling, super::tests::work()).unwrap();
    assert!(!runtime.publication_ready());
    assert!(runtime.complete(sibling, request).is_some());
    runtime.fail(sibling);
    assert!(!runtime.publication_ready());
}

#[test]
fn failure_aborts_gated_and_live_inputs() {
    use crate::pipeline::extraction::input::LiveArchiveInput;
    let mut runtime = ArchiveRuntime::new();
    let owner = runtime.admit(None, InputVersion(0)).unwrap().owner;
    let gate = LiveArchiveInput::new();
    let live = LiveArchiveInput::new();
    runtime.gates.insert(("set".into(), 1), gate.clone());
    runtime.live_inputs.push((owner, live.clone()));
    runtime.fail(owner);
    assert!(gate.wait_volume(0).is_err());
    assert!(live.wait_volume(0).is_err());
}

#[test]
fn removal_aborts_gated_and_live_inputs() {
    use crate::pipeline::extraction::input::LiveArchiveInput;
    let mut runtime = ArchiveRuntime::new();
    let owner = runtime.admit(None, InputVersion(0)).unwrap().owner;
    let gate = LiveArchiveInput::new();
    let live = LiveArchiveInput::new();
    runtime.gates.insert(("set".into(), 1), gate.clone());
    runtime.live_inputs.push((owner, live.clone()));
    drop(runtime);
    assert!(gate.wait_volume(0).is_err());
    assert!(live.wait_volume(0).is_err());
}

#[test]
fn ancestor_revocation_rejects_late_child_completion() {
    let mut runtime = ArchiveRuntime::new();
    let parent = runtime.admit(None, InputVersion(0)).unwrap().owner;
    let child = runtime.admit(Some(parent), InputVersion(1)).unwrap().owner;
    let request = runtime.dispatch(child, work()).unwrap();
    runtime.sessions.revoke(parent).unwrap();
    assert!(runtime.complete(child, request).is_none());
    assert!(!runtime.pending());
}

#[test]
fn replay_restarts_parent_and_child_with_fresh_generations() {
    let mut original = ArchiveRuntime::new();
    let parent = original
        .graph_lease("archive", 0, None, InputVersion(0))
        .unwrap()
        .owner;
    let child = original
        .graph_lease("archive", 1, Some(0), InputVersion(0))
        .unwrap()
        .owner;
    let mut restored =
        ArchiveRuntime::restore_checkpoint(&original.sessions.capture_checkpoint()).unwrap();
    let parent = StreamOwner {
        root: restored.root,
        ..parent
    };
    let child = StreamOwner {
        root: restored.root,
        ..child
    };
    restored.graph_owners.insert(("archive".into(), 0), parent);
    restored.graph_owners.insert(("archive".into(), 1), child);
    let fresh = restored.resume_owner(child).unwrap().owner;
    let fresh_parent = restored.graph_owners[&("archive".into(), 0)];
    assert!(fresh.generation > child.generation);
    assert!(fresh_parent.generation > parent.generation);
    restored.sessions.close(fresh).unwrap();
    assert!(!restored.sessions.ancestry_verified(fresh));
    restored.sessions.close(fresh_parent).unwrap();
    assert!(restored.sessions.ancestry_verified(fresh));
}

#[tokio::test]
async fn discarding_speculative_parent_retires_descendants_without_terminal_events() {
    let mut runtime = ArchiveRuntime::new();
    let parent = runtime
        .graph_lease("native", 0, None, InputVersion(0))
        .unwrap()
        .owner;
    let child = runtime
        .graph_lease("native", 1, Some(0), InputVersion(0))
        .unwrap();
    let request = runtime.dispatch(parent, work()).unwrap();
    let child_request = runtime.dispatch(child.owner, work()).unwrap();
    runtime.mark_speculative(&request).unwrap();
    let sibling = runtime.admit(None, InputVersion(0)).unwrap().owner;
    let sibling_request = runtime.dispatch(sibling, work()).unwrap();
    let child_owner = child.owner;
    let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
    sender.send(()).await.unwrap();
    let (joined, finished) = tokio::sync::oneshot::channel();
    runtime.track_worker(
        child_owner,
        tokio::spawn(async move {
            assert!(!child.send_event(&sender, ()).await);
            joined.send(()).unwrap();
        }),
    );
    runtime.discard_speculative(&request).await.unwrap();
    finished.await.unwrap();
    assert_eq!(receiver.recv().await, Some(()));
    assert_eq!(receiver.recv().await, None);
    assert!(runtime.complete(child_owner, child_request).is_none());
    assert!(runtime.complete(sibling, sibling_request).is_some());
    assert!(!runtime.pending());
    assert!(runtime.publication_ready());
    assert!(!runtime.graph_owners.keys().any(|(set, _)| set == "native"));
}

#[test]
fn abandoning_speculation_removes_cancelled_descendant_authority_immediately() {
    let mut runtime = ArchiveRuntime::new();
    let parent = runtime.admit(None, InputVersion(0)).unwrap().owner;
    let child = runtime.admit(Some(parent), InputVersion(0)).unwrap().owner;
    let request = runtime.dispatch(parent, work()).unwrap();
    let child_request = runtime.dispatch(child, work()).unwrap();
    runtime.mark_speculative(&request).unwrap();
    runtime.abandon_speculative(&request).unwrap();
    assert!(!runtime.pending());
    assert!(runtime.complete(child, child_request).is_none());
    assert!(runtime.publication_ready());
}

#[tokio::test]
async fn direct_graph_retirement_joins_descendants_before_releasing_original_backing() {
    let mut runtime = ArchiveRuntime::new();
    let root = runtime
        .graph_lease("direct", 0, None, InputVersion(0))
        .unwrap()
        .owner;
    let child = runtime
        .graph_lease("direct", 1, Some(0), InputVersion(0))
        .unwrap();
    let mut child_work = work();
    let original = PathBuf::from("original.part01.rar");
    child_work.original_inputs.push(original.clone());
    runtime.dispatch(child.owner, child_work).unwrap();
    assert!(runtime.retains_input_path(&original));
    let (joined, finished) = tokio::sync::oneshot::channel();
    runtime.track_worker(
        child.owner,
        tokio::spawn(async move {
            child.cancelled().await;
            joined.send(()).unwrap();
        }),
    );
    runtime.retire_graph_namespace("direct").await.unwrap();
    finished.await.unwrap();
    assert!(!runtime.sessions.execution_current(root));
    assert!(!runtime.retains_input_path(&original));
    assert!(!runtime.pending());
    assert!(runtime.graph_owners.is_empty());
    assert!(runtime.workers.is_empty());
}

#[tokio::test]
async fn parent_retirement_joins_descendants_already_fenced_by_speculative_abandon() {
    let mut runtime = ArchiveRuntime::new();
    let root = runtime
        .graph_lease("direct", 0, None, InputVersion(0))
        .unwrap()
        .owner;
    let native = runtime.admit(Some(root), InputVersion(0)).unwrap().owner;
    let descendant = runtime.admit(Some(native), InputVersion(0)).unwrap();
    let request = runtime.dispatch(native, work()).unwrap();
    runtime.mark_speculative(&request).unwrap();
    runtime.dispatch(descendant.owner, work()).unwrap();
    let (joined, mut finished) = tokio::sync::oneshot::channel();
    runtime.track_worker(
        descendant.owner,
        tokio::spawn(async move {
            descendant.cancelled().await;
            joined.send(()).unwrap();
        }),
    );
    runtime.abandon_speculative(&request).unwrap();
    assert_eq!(runtime.workers.len(), 1);
    runtime.retire_graph_namespace("direct").await.unwrap();
    assert!(
        runtime.workers.is_empty(),
        "an already-draining descendant still requires a join"
    );
    finished
        .try_recv()
        .expect("the join barrier must observe descendant completion");
    assert!(!runtime.pending());
    assert!(runtime.graph_owners.is_empty());
}

mod placement {
    use super::*;
    use crate::pipeline::direct_store::nested::catalog::RoutingNamespace;
    use crate::pipeline::direct_store::nested::{NodeSpan, ROOT_NODE};
    use crate::pipeline::direct_store::plan::DirectSetPlan;
    use crate::pipeline::direct_store::router::{DirectDestination, RoutedSpan};
    use crate::pipeline::extraction::output::channel::Event;
    use crate::pipeline::extraction::output::channel::{ChannelOutput, Generation};
    use crate::pipeline::extraction::output::{ArchiveOutput, Integrity};
    use crate::pipeline::extraction::{ExtractionRoot, JobExtractionBudget};
    use decoded::{OpenPartials, SpanWrite, write_spans};
    use std::io::Write;
    use std::path::Path;

    fn budget(root: &Path) -> Arc<JobExtractionBudget> {
        JobExtractionBudget::new(
            Arc::new(crate::pipeline::extraction::ExtractionLimits {
                max_job_bytes: 1 << 30,
                max_member_bytes: 1 << 30,
                max_entries: 1_000,
                max_ratio: 100,
                max_seconds: 3_600,
                min_free_bytes: 1,
                max_memory_bytes: 64 * 1024 * 1024,
            }),
            root.to_owned(),
            1 << 30,
            0,
            0,
            crate::operations::metrics::PipelineMetrics::new(),
        )
        .unwrap()
    }

    // A namespace bridge the way a root stream admits one.
    fn bridge(
        runtime: &mut ArchiveRuntime,
        budget: &Arc<JobExtractionBudget>,
    ) -> (
        StreamOwner,
        ChannelOutput,
        Generation,
        tokio::sync::mpsc::Receiver<crate::pipeline::extraction::output::channel::Frame>,
    ) {
        let owner = runtime.admit(None, InputVersion(0)).unwrap().owner;
        let (sender, receiver) = tokio::sync::mpsc::channel(limits::FRAME_CHANNEL_DEPTH);
        let (output, generation) = ChannelOutput::new(
            owner,
            budget
                .try_reserve_memory(limits::BRIDGE_ADMISSION_BYTES)
                .unwrap(),
            sender,
            tokio::runtime::Handle::current(),
            Arc::clone(budget),
        )
        .unwrap();
        runtime.sessions.attach_channel(generation.clone()).unwrap();
        (owner, output, generation, receiver)
    }

    fn span(relative: &str, offset: u64, bytes: &'static [u8]) -> SpanWrite {
        SpanWrite {
            relative: relative.into(),
            offset,
            bytes: bytes.chunks(3).map(bytes::Bytes::from_static).collect(),
        }
    }

    // Two members interleaved and placed out of order, the way nested routing
    // emits them within one frame.
    fn interleaved() -> Vec<SpanWrite> {
        vec![
            span("a", 6, b"ghijk"),
            span("b", 0, b"0123"),
            span("a", 0, b"abcdef"),
            span("b", 4, b"456789"),
            span("a", 11, b"l"),
        ]
    }

    #[tokio::test]
    async fn one_batch_lands_the_bytes_one_task_per_span_would() {
        let directory = tempfile::tempdir().unwrap();
        let mut runtime = ArchiveRuntime::new();
        let budget = budget(directory.path());
        let (_, _output, generation, _receiver) = bridge(&mut runtime, &budget);
        let batched = ExtractionRoot::open(&directory.path().join("batched")).unwrap();
        let single = ExtractionRoot::open(&directory.path().join("single")).unwrap();
        let (landed, failure) = write_spans(&batched, &generation, interleaved());
        assert_eq!(landed, 5);
        assert!(failure.is_none());
        for write in interleaved() {
            assert_eq!(write_spans(&single, &generation, vec![write]).0, 1);
        }
        for name in ["a", "b"] {
            assert_eq!(
                std::fs::read(directory.path().join("batched").join(name)).unwrap(),
                std::fs::read(directory.path().join("single").join(name)).unwrap(),
            );
        }
        assert_eq!(
            std::fs::read(directory.path().join("batched/a")).unwrap(),
            b"abcdefghijkl"
        );
        assert_eq!(
            std::fs::read(directory.path().join("batched/b")).unwrap(),
            b"0123456789"
        );
    }

    #[tokio::test]
    async fn a_revoked_generation_lands_nothing() {
        let directory = tempfile::tempdir().unwrap();
        let mut runtime = ArchiveRuntime::new();
        let budget = budget(directory.path());
        let (_, _output, generation, _receiver) = bridge(&mut runtime, &budget);
        let root = ExtractionRoot::open(&directory.path().join("revoked")).unwrap();
        generation.revoke();
        let (landed, failure) = write_spans(&root, &generation, interleaved());
        assert_eq!(landed, 0);
        assert!(matches!(failure, Some(RuntimeError::Revoked)));
        assert!(!directory.path().join("revoked/a").exists());
    }

    // Unlinking a file an open handle still writes to is how these two tests
    // tell a reused handle from a reopened one. Windows keeps the name until
    // the last handle closes, so they read only on Unix.
    #[cfg(unix)]
    #[test]
    fn a_partial_handle_is_reused_until_the_bound_evicts_it() {
        let directory = tempfile::tempdir().unwrap();
        let root = ExtractionRoot::open(&directory.path().join("partials")).unwrap();
        let first = directory.path().join("partials/first");
        let mut partials = OpenPartials::default();
        partials.write(&root, span("first", 0, b"abc")).unwrap();
        // Unlinking the partial shows whether the next write reopens it: a
        // reused handle writes into the unlinked file, a reopen recreates it.
        std::fs::remove_file(&first).unwrap();
        partials.write(&root, span("first", 3, b"def")).unwrap();
        assert!(!first.exists());
        assert!(partials.holds(Path::new("first")));
        for index in 0..decoded::OPEN_PARTIALS {
            partials
                .write(&root, span(&format!("other-{index}"), 0, b"x"))
                .unwrap();
        }
        assert!(!partials.holds(Path::new("first")));
        assert!(partials.holds(Path::new("other-0")));
        partials.write(&root, span("first", 6, b"ghi")).unwrap();
        assert_eq!(std::fs::read(&first).unwrap(), b"\0\0\0\0\0\0ghi");
        assert!(!partials.holds(Path::new("other-0")));
    }

    #[cfg(unix)]
    #[test]
    fn dropping_the_table_closes_every_handle() {
        let directory = tempfile::tempdir().unwrap();
        let root = ExtractionRoot::open(&directory.path().join("closed")).unwrap();
        let path = directory.path().join("closed/member");
        let mut partials = OpenPartials::default();
        partials.write(&root, span("member", 0, b"abc")).unwrap();
        drop(partials);
        // With the handle closed the next table must reopen, recreating the
        // unlinked partial instead of writing into the old inode.
        std::fs::remove_file(&path).unwrap();
        let mut partials = OpenPartials::default();
        partials.write(&root, span("member", 0, b"def")).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"def");
    }

    #[tokio::test]
    async fn a_streamed_member_reaches_its_backing_unchanged() {
        let directory = tempfile::tempdir().unwrap();
        let private = directory.path().join("private");
        let budget = budget(directory.path());
        let mut runtime = ArchiveRuntime::new();
        let (owner, output, generation, mut receiver) = bridge(&mut runtime, &budget);
        let sessions =
            ArchiveSessions::restore_checkpoint(&runtime.sessions.capture_checkpoint(), owner.root)
                .unwrap();
        let mut state = decoded::DecodedState::new(
            crate::jobs::ids::JobId(1),
            generation,
            sessions,
            DirectSetPlan::namespace(owner, &private),
            limits::namespace_router_resources(),
            budget,
            private.clone(),
        )
        .unwrap();
        let payload: Vec<u8> = (0..300_000u32).map(|index| (index % 251) as u8).collect();
        let expected = payload.clone();
        let producer = tokio::task::spawn_blocking(move || {
            let mut member = output
                .file(0, Path::new("leaf.bin"), Some(payload.len() as u64))
                .unwrap();
            for chunk in payload.chunks(70_000) {
                member.write_all(chunk).unwrap();
            }
            member
                .finish_data()
                .unwrap()
                .integrity(Integrity::Verified)
                .unwrap();
        });
        while let Some(frame) = receiver.recv().await {
            state.accept(frame).await.unwrap();
        }
        producer.await.unwrap();
        state.close().await.unwrap();
        let published = state.publication().unwrap();
        assert_eq!(std::fs::read(&published["leaf.bin"]).unwrap(), expected);
    }

    // A root span of the decoded namespace, as routing emits one for a member
    // no child archive claims.
    fn root_span(generation: u64, member_id: u32, offset: u64, bytes: bytes::Bytes) -> NodeSpan {
        NodeSpan {
            owner: ROOT_NODE,
            generation,
            raw_child: None,
            span: RoutedSpan {
                owner: Default::default(),
                destination: DirectDestination::Member { member_id },
                destination_offset: offset,
                volume_index: 0,
                source_offset: offset,
                bytes: vec![bytes],
            },
        }
    }

    // Streams the members into a decoded namespace and catalogues their
    // bytes without placing them, returning each byte frame as the root span
    // routing would emit for it. The caller then decides how the spans meet
    // `place`.
    async fn catalogued(
        runtime: &mut ArchiveRuntime,
        budget: &Arc<JobExtractionBudget>,
        private: &Path,
        members: &'static [(&'static str, &'static [u8])],
    ) -> (decoded::DecodedState, Vec<NodeSpan>) {
        let (owner, output, generation, mut receiver) = bridge(runtime, budget);
        let sessions =
            ArchiveSessions::restore_checkpoint(&runtime.sessions.capture_checkpoint(), owner.root)
                .unwrap();
        let mut state = decoded::DecodedState::new(
            crate::jobs::ids::JobId(1),
            generation,
            sessions,
            DirectSetPlan::namespace(owner, private),
            limits::namespace_router_resources(),
            Arc::clone(budget),
            private.to_owned(),
        )
        .unwrap();
        let producer = tokio::task::spawn_blocking(move || {
            for (index, (name, bytes)) in members.iter().enumerate() {
                let mut member = output
                    .file(index, Path::new(name), Some(bytes.len() as u64))
                    .unwrap();
                member.write_all(bytes).unwrap();
                member
                    .finish_data()
                    .unwrap()
                    .integrity(Integrity::Verified)
                    .unwrap();
            }
        });
        let mut spans = Vec::new();
        while let Some(frame) = receiver.recv().await {
            let Event::Bytes { offset, bytes } = &frame.event else {
                state.accept(frame).await.unwrap();
                continue;
            };
            let member = state.catalog.accept(&frame).unwrap();
            let generation = state.namespace().generation();
            spans.push(root_span(generation, member, *offset, bytes.clone()));
        }
        producer.await.unwrap();
        (state, spans)
    }

    // Each registered partial and the ranges placed in it. The registry
    // refuses this inventory while any write ticket is still pending, so
    // reading it also proves every ticket was settled or released.
    fn placed_ranges(state: &decoded::DecodedState) -> BTreeMap<PathBuf, Vec<(u64, u64)>> {
        state
            .registry
            .checkpoint_claims()
            .expect("every write ticket is settled or released")
            .into_iter()
            .map(|(_, relative, _, ranges)| (relative, ranges))
            .collect()
    }

    #[tokio::test]
    async fn a_materialization_without_memory_releases_its_tickets() {
        let directory = tempfile::tempdir().unwrap();
        let private = directory.path().join("private");
        let budget = budget(directory.path());
        let mut runtime = ArchiveRuntime::new();
        let (owner, output, generation, mut receiver) = bridge(&mut runtime, &budget);
        let sessions =
            ArchiveSessions::restore_checkpoint(&runtime.sessions.capture_checkpoint(), owner.root)
                .unwrap();
        let mut state = decoded::DecodedState::new(
            crate::jobs::ids::JobId(1),
            generation,
            sessions,
            DirectSetPlan::namespace(owner, &private),
            limits::namespace_router_resources(),
            Arc::clone(&budget),
            private.clone(),
        )
        .unwrap();
        // Named as a 7z, so routing binds the member to a child node that
        // holds its bytes until the signature header reads. These carry no
        // 7z magic, so the node is refused as no archive and what it held is
        // owed to the parent as the member: a copy, which needs a permit.
        let payload: Vec<u8> = (0..4096u32).map(|index| (index * 7 % 256) as u8).collect();
        // Hold all the memory the job has left, so the copy cannot reserve
        // its permit once its writes are admitted.
        let mut held = Vec::new();
        while let Some(permit) = budget.try_reserve_memory(limits::MATERIALIZATION_PERMIT_BYTES) {
            held.push(permit);
        }
        let producer = tokio::task::spawn_blocking(move || {
            let mut member = output
                .file(0, Path::new("notes.7z"), Some(payload.len() as u64))
                .unwrap();
            member.write_all(&payload).unwrap();
            member
                .finish_data()
                .unwrap()
                .integrity(Integrity::Verified)
                .unwrap();
        });
        let mut refusal = Ok(());
        while let Some(frame) = receiver.recv().await {
            if refusal.is_ok() {
                refusal = state.accept(frame).await;
            }
        }
        producer.await.unwrap();
        if refusal.is_ok() {
            refusal = state.close().await;
        }
        assert!(
            matches!(refusal, Err(RuntimeError::Admission("materialization"))),
            "the copy is refused its memory: {refusal:?}"
        );
        let claims = placed_ranges(&state);
        assert!(!claims.is_empty(), "the copy admitted its writes");
        assert!(
            claims.values().all(Vec::is_empty),
            "the refused copy placed nothing: {claims:?}"
        );
    }

    const MEMBERS: &[(&str, &[u8])] = &[
        ("first.bin", b"abcdef"),
        ("second.bin", b"ghijkl"),
        ("third.bin", b"mnopqr"),
    ];

    #[tokio::test]
    async fn a_failed_write_settles_the_spans_before_it_and_releases_the_rest() {
        let directory = tempfile::tempdir().unwrap();
        let private = directory.path().join("private");
        let budget = budget(directory.path());
        let mut runtime = ArchiveRuntime::new();
        let (mut state, spans) = catalogued(&mut runtime, &budget, &private, MEMBERS).await;
        assert_eq!(spans.len(), MEMBERS.len(), "one byte frame per member");
        // A regular file where the second member's partial needs a directory
        // fails that span's open after the first span has landed.
        std::fs::write(private.join("blocker"), b"").unwrap();
        let DirectDestination::Member { member_id: second } = spans[1].span.destination else {
            unreachable!("root spans address members");
        };
        state
            .destinations
            .insert(second, private.join("blocker/second"));

        let error = state.place(spans).await.unwrap_err();
        assert!(
            matches!(
                error,
                RuntimeError::Io {
                    step: "decoded write",
                    ..
                }
            ),
            "the writer's own failure is returned: {error}"
        );
        assert_eq!(
            placed_ranges(&state),
            BTreeMap::from([
                (PathBuf::from("stream-0"), vec![(0, 6)]),
                (PathBuf::from("blocker/second"), Vec::new()),
                (PathBuf::from("stream-2"), Vec::new()),
            ]),
            "only the landed span is placed"
        );
        assert_eq!(std::fs::read(private.join("stream-0")).unwrap(), b"abcdef");
        assert!(!private.join("stream-2").exists());
    }

    #[tokio::test]
    async fn a_refused_span_still_places_the_spans_admitted_before_it() {
        let directory = tempfile::tempdir().unwrap();
        let private = directory.path().join("private");
        let budget = budget(directory.path());
        let mut runtime = ArchiveRuntime::new();
        let (mut state, mut spans) = catalogued(&mut runtime, &budget, &private, MEMBERS).await;
        let generation = state.namespace().generation();
        // No member was catalogued under this id, so the span has no partial.
        spans.insert(
            2,
            root_span(generation, 99, 0, bytes::Bytes::from_static(b"stray")),
        );

        let error = state.place(spans).await.unwrap_err();
        assert!(
            matches!(error, RuntimeError::Invariant("decoded destination")),
            "the refusal is returned: {error}"
        );
        assert_eq!(
            placed_ranges(&state),
            BTreeMap::from([
                (PathBuf::from("stream-0"), vec![(0, 6)]),
                (PathBuf::from("stream-1"), vec![(0, 6)]),
            ]),
            "the spans before the refusal are placed and nothing after it is admitted"
        );
        assert_eq!(std::fs::read(private.join("stream-0")).unwrap(), b"abcdef");
        assert_eq!(std::fs::read(private.join("stream-1")).unwrap(), b"ghijkl");
        assert!(!private.join("stream-2").exists());
    }
}
