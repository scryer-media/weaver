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
    assert!(runtime.protects_input(&original));
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
    assert!(!runtime.protects_input(&original));
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
