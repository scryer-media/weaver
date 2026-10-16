use super::*;

#[test]
fn repeated_local_close_preserves_ancestor_integrity_gate() {
    let mut sessions = ArchiveSessions::new(23, 0);
    let outer = sessions.register(None, InputVersion(0)).unwrap();
    let parent = sessions.start(outer).unwrap();
    let inner = sessions
        .register(Some(parent.owner), InputVersion(1))
        .unwrap();
    let child = sessions.start(inner).unwrap();
    sessions.close(child.owner).unwrap();
    sessions.close(child.owner).unwrap();
    assert!(!sessions.ancestry_verified(child.owner));
    assert!(sessions.execution_current(child.owner));
    sessions.close(parent.owner).unwrap();
    assert!(sessions.ancestry_verified(child.owner));
    sessions.close(child.owner).unwrap();
    sessions.revoke(parent.owner).unwrap();
    assert_eq!(sessions.close(child.owner), Err(SessionError::Stale));
    assert!(!sessions.ancestry_verified(child.owner));
}

#[tokio::test]
async fn retirement_joins_a_producer_with_a_full_coordinator_queue() {
    let mut sessions = ArchiveSessions::new(19, 0);
    let node = sessions.register(None, InputVersion(0)).unwrap();
    let lease = sessions.start(node).unwrap();
    let owner = lease.owner;
    let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
    sender.send(1).await.unwrap();
    let completion = tokio::spawn(async move { lease.send_event(&sender, 2).await });
    sessions.revoke(owner).unwrap();
    assert!(!completion.await.unwrap());
    assert_eq!(receiver.recv().await, Some(1));
    assert_eq!(receiver.recv().await, None);
}

#[test]
fn ancestor_failure_revokes_completed_descendants_and_pins_old_backings() {
    let mut sessions = ArchiveSessions::new(17, 0);
    let outer = sessions.register(None, InputVersion(1)).unwrap();
    let parent = sessions.start(outer).unwrap();
    let child = sessions
        .register(Some(parent.owner), InputVersion(2))
        .unwrap();
    let inner = sessions.start(child).unwrap();
    let grandchild = sessions
        .register(Some(inner.owner), InputVersion(3))
        .unwrap();
    let leaf = sessions.start(grandchild).unwrap();
    sessions.close(leaf.owner).unwrap();
    sessions.close(inner.owner).unwrap();
    assert!(!sessions.ancestry_verified(leaf.owner));
    sessions.close(parent.owner).unwrap();
    assert!(sessions.ancestry_verified(leaf.owner));

    let pinned = Arc::clone(&inner.backings);
    let revoked = sessions.revoke(parent.owner).unwrap();
    assert_eq!(revoked, [parent.owner, inner.owner, leaf.owner]);
    for lease in [&parent, &inner, &leaf] {
        assert!(lease.cancellation.load(Ordering::Acquire));
        assert!(!sessions.accepts(lease.owner));
        assert!(!sessions.ancestry_verified(lease.owner));
        assert_eq!(sessions.close(lease.owner), Err(SessionError::Stale));
    }
    assert!(matches!(
        sessions.prepare_replacement(outer, InputVersion(4)),
        Err(SessionError::Busy)
    ));
    for owner in revoked {
        sessions.joined(owner).unwrap();
    }
    let prepared = sessions
        .prepare_replacement(outer, InputVersion(4))
        .unwrap();
    sessions.committed_replacement(prepared, 1).unwrap();
    assert_eq!(sessions.original(outer), Some(InputVersion(1)));
    assert_eq!(sessions.effective(outer), Some(InputVersion(4)));
    let replacement = sessions.start(outer).unwrap();
    assert_eq!(replacement.owner.node, parent.owner.node);
    assert_eq!(replacement.owner.generation, parent.owner.generation + 1);
    assert!(!sessions.accepts(inner.owner));
    let old_inner = inner.owner;
    drop(inner);
    assert!(!sessions.backing_leases_drained(old_inner));
    drop(pinned);
    assert!(sessions.backing_leases_drained(old_inner));
}

#[test]
fn inner_repair_replaces_selection_without_retiring_parent_or_sibling() {
    let mut sessions = ArchiveSessions::new(41, 8);
    let root = sessions.register(None, InputVersion(1)).unwrap();
    let parent = sessions.start(root).unwrap();
    let child = sessions
        .register(Some(parent.owner), InputVersion(2))
        .unwrap();
    let first = sessions.start(child).unwrap();
    let sibling = sessions
        .register(Some(parent.owner), InputVersion(3))
        .unwrap();
    let other = sessions.start(sibling).unwrap();
    sessions.close(parent.owner).unwrap();
    sessions.close(other.owner).unwrap();
    assert_eq!(sessions.revoke(first.owner).unwrap(), [first.owner]);
    sessions.joined(first.owner).unwrap();
    let prepared = sessions
        .prepare_replacement(child, InputVersion(4))
        .unwrap();
    sessions.committed_replacement(prepared, 9).unwrap();
    let second = sessions.start(child).unwrap();
    assert_eq!(sessions.nodes[&child].depth, 2);
    assert_eq!(sessions.original(child), Some(InputVersion(2)));
    assert_eq!(sessions.effective(child), Some(InputVersion(4)));
    assert!(sessions.accepts(parent.owner));
    assert!(sessions.ancestry_verified(other.owner));
    sessions.close(second.owner).unwrap();
    assert!(sessions.ancestry_verified(second.owner));
    // Engine replacement is not a new archive nesting layer.
    assert!(
        sessions
            .register(Some(second.owner), InputVersion(5))
            .is_ok()
    );
}

#[test]
fn a_structurally_stale_checkpoint_cannot_select_a_replacement() {
    let mut sessions = ArchiveSessions::new(11, 0);
    let node = sessions.register(None, InputVersion(1)).unwrap();
    let first = sessions.start(node).unwrap();
    sessions.revoke(first.owner).unwrap();
    sessions.joined(first.owner).unwrap();
    let prepared = sessions.prepare_replacement(node, InputVersion(2)).unwrap();
    sessions.register(None, InputVersion(3)).unwrap();
    assert_eq!(
        sessions.committed_replacement(prepared, 1),
        Err(SessionError::Stale)
    );
    assert_eq!(sessions.effective(node), Some(InputVersion(1)));
    assert!(!sessions.backing_leases_drained(first.owner));
    assert_eq!(sessions.checkpoint, 1);
    let prepared = sessions.prepare_replacement(node, InputVersion(2)).unwrap();
    sessions.committed_replacement(prepared, 2).unwrap();
    assert_eq!(sessions.effective(node), Some(InputVersion(2)));
}

#[test]
fn foreign_job_and_retired_parent_cannot_admit_work() {
    let mut sessions = ArchiveSessions::new(5, 0);
    let node = sessions.register(None, InputVersion(1)).unwrap();
    let lease = sessions.start(node).unwrap();
    let foreign = StreamOwner {
        root: 6,
        ..lease.owner
    };
    assert_eq!(
        sessions.register(Some(foreign), InputVersion(2)),
        Err(SessionError::Stale)
    );
    assert_eq!(sessions.revoke(foreign), Err(SessionError::Stale));
    sessions.revoke(lease.owner).unwrap();
    assert_eq!(
        sessions.register(Some(lease.owner), InputVersion(2)),
        Err(SessionError::Stale)
    );
    assert!(matches!(sessions.start(node), Err(SessionError::Busy)));
}

#[test]
fn revocation_wakes_input_waiters_and_is_retained_for_late_waiters() {
    use std::future::Future;
    use std::task::{Context, Wake, Waker};

    struct Signalled(AtomicBool);
    impl Wake for Signalled {
        fn wake(self: Arc<Self>) {
            self.0.store(true, Ordering::Release);
        }
    }

    let mut sessions = ArchiveSessions::new(5, 0);
    let node = sessions.register(None, InputVersion(1)).unwrap();
    let lease = sessions.start(node).unwrap();
    let signal = Arc::new(Signalled(AtomicBool::new(false)));
    let waker = Waker::from(Arc::clone(&signal));
    let mut context = Context::from_waker(&waker);
    let mut waiting = Box::pin(lease.cancelled());
    assert!(waiting.as_mut().poll(&mut context).is_pending());
    sessions.revoke(lease.owner).unwrap();
    assert!(signal.0.load(Ordering::Acquire));
    assert!(waiting.as_mut().poll(&mut context).is_ready());
    assert!(
        Box::pin(lease.cancelled())
            .as_mut()
            .poll(&mut context)
            .is_ready()
    );
}
