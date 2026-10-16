use super::*;
use crate::pipeline::extraction::session::InputVersion;
use crate::pipeline::extraction::{ExtractionLimits, JobExtractionBudget};

fn budget(path: &Path) -> Arc<JobExtractionBudget> {
    JobExtractionBudget::new(
        Arc::new(ExtractionLimits {
            max_job_bytes: 8192,
            max_member_bytes: 8192,
            max_entries: 32,
            max_ratio: 100,
            max_seconds: 60,
            min_free_bytes: 1,
            max_memory_bytes: 64 * 1024 * 1024,
        }),
        path.to_owned(),
        8192,
        0,
        0,
        crate::operations::metrics::PipelineMetrics::new(),
    )
    .unwrap()
}

fn destination(namespace: StreamOwner) -> Destination {
    Destination {
        namespace,
        owner: SpanOwner::default(),
        destination: DirectDestination::Member { member_id: 0 },
    }
}

#[test]
fn namespace_collisions_and_revoked_completions_cannot_claim_other_files() {
    let temp = tempfile::tempdir().unwrap();
    let budget = budget(temp.path());
    let mut registry =
        DestinationRegistry::new(budget.try_reserve_memory(16 * 1024).unwrap()).unwrap();
    let mut sessions = ArchiveSessions::new(1, 0);
    let root = sessions.register(None, InputVersion(1)).unwrap();
    let parent = sessions.start(root).unwrap();
    let child = sessions
        .register(Some(parent.owner), InputVersion(2))
        .unwrap();
    let nested = sessions.start(child).unwrap();
    let first = destination(parent.owner);
    let second = destination(nested.owner);
    let a = registry
        .register(&sessions, first, Path::new("a/leaf"), Some(8))
        .unwrap();
    let b = registry
        .register(&sessions, second, Path::new("b/leaf"), Some(8))
        .unwrap();
    assert_ne!(a, b);
    let complete = registry.begin_write(&sessions, first, 0, 4).unwrap();
    let stale = registry.begin_write(&sessions, second, 0, 8).unwrap();
    assert_eq!(
        registry.write_target(&complete).unwrap(),
        (Path::new("a/leaf"), 0, 4)
    );
    assert!(matches!(
        registry.capture(budget.try_reserve_memory(16 * 1024).unwrap()),
        Err(SessionError::Busy)
    ));
    registry.placed(&sessions, complete).unwrap();
    sessions.revoke(nested.owner).unwrap();
    assert_eq!(registry.placed(&sessions, stale), Err(SessionError::Stale));
    let snapshot = registry
        .capture(budget.try_reserve_memory(registry.capture_bytes()).unwrap())
        .unwrap();
    assert_eq!(snapshot.files[0].id, a);
    assert_eq!(snapshot.files[0].placed.ranges(), &[(0, 4)]);
    assert_eq!(snapshot.files[1].id, b);
    assert!(snapshot.files[1].placed.is_empty());
    assert_eq!(snapshot.files[0].relative, Path::new("a/leaf"));
    assert_eq!(snapshot.files[0].length, Some(8));
    assert!(!snapshot.files[0].sealed);
    assert_eq!(Arc::strong_count(&snapshot.files[0].readers), 2);
}

#[test]
fn immutable_backings_reject_overlap_aliases_and_retired_tickets() {
    let temp = tempfile::tempdir().unwrap();
    let budget = budget(temp.path());
    let mut registry =
        DestinationRegistry::new(budget.try_reserve_memory(16 * 1024).unwrap()).unwrap();
    let mut sessions = ArchiveSessions::new(1, 0);
    let node = sessions.register(None, InputVersion(1)).unwrap();
    let lease = sessions.start(node).unwrap();
    let sink = destination(lease.owner);
    registry
        .register(&sessions, sink, Path::new("a/leaf"), Some(8))
        .unwrap();
    let other = Destination {
        destination: DirectDestination::Envelope { volume_index: 0 },
        ..sink
    };
    for path in [
        "A/LEAF",
        "a",
        "a/leaf/child",
        "../escape",
        "a//leaf",
        "a/./leaf",
        "a/leaf.",
        "a/leaf ",
        "a/leaf:stream",
        "a/con.bin",
        "a/com1",
        "a/é",
        "a/e\u{301}",
    ] {
        assert!(
            registry
                .register(&sessions, other, Path::new(path), Some(8))
                .is_err()
        );
    }
    let write = registry.begin_write(&sessions, sink, 0, 4).unwrap();
    assert!(registry.begin_write(&sessions, sink, 2, 4).is_err());
    registry.placed(&sessions, write).unwrap();
    assert!(registry.begin_write(&sessions, sink, 2, 4).is_err());
    assert!(registry.begin_write(&sessions, sink, u64::MAX, 2).is_err());
    let tail = registry.begin_write(&sessions, sink, 4, 4).unwrap();
    registry.retire(sink);
    assert_eq!(registry.placed(&sessions, tail), Err(SessionError::Stale));
    assert_eq!(
        registry.files.values().next().unwrap().placed.ranges(),
        &[(0, 4)]
    );
}

#[test]
fn failed_writes_release_metadata_admission_without_claiming_sparse_length() {
    let temp = tempfile::tempdir().unwrap();
    let budget = budget(temp.path());
    let mut registry =
        DestinationRegistry::new(budget.try_reserve_memory(REGISTRY_CHARGE + 1024).unwrap())
            .unwrap();
    let mut sessions = ArchiveSessions::new(1, 0);
    let node = sessions.register(None, InputVersion(1)).unwrap();
    let lease = sessions.start(node).unwrap();
    let sink = destination(lease.owner);
    registry
        .register(&sessions, sink, Path::new("leaf"), Some(8))
        .unwrap();
    assert!(matches!(
        DestinationRegistry::new(budget.try_reserve_memory(REGISTRY_CHARGE - 1).unwrap()),
        Err(SessionError::Admission)
    ));
    let initial = registry.charged;
    for _ in 0..100 {
        let write = registry.begin_write(&sessions, sink, 0, 4).unwrap();
        assert!(matches!(
            registry.begin_write(&sessions, sink, 4, 4),
            Err(SessionError::Admission)
        ));
        registry.failed(write).unwrap();
        assert_eq!(registry.charged, initial);
    }
    assert!(registry.files.values().next().unwrap().placed.is_empty());
    assert!(matches!(
        registry.capture(budget.try_reserve_memory(1).unwrap()),
        Err(SessionError::Admission)
    ));
}

#[test]
fn sealing_requires_completed_coverage_and_explicit_empty_creation() {
    let temp = tempfile::tempdir().unwrap();
    let budget = budget(temp.path());
    let mut registry =
        DestinationRegistry::new(budget.try_reserve_memory(16 * 1024).unwrap()).unwrap();
    let mut sessions = ArchiveSessions::new(1, 0);
    let node = sessions.register(None, InputVersion(1)).unwrap();
    let lease = sessions.start(node).unwrap();
    let sink = destination(lease.owner);
    registry
        .register(&sessions, sink, Path::new("leaf"), None)
        .unwrap();
    assert_eq!(
        registry.seal(&sessions, sink, 4),
        Err(SessionError::InvalidTransition)
    );
    let write = registry.begin_write(&sessions, sink, 0, 4).unwrap();
    assert_eq!(registry.seal(&sessions, sink, 4), Err(SessionError::Busy));
    registry.placed(&sessions, write).unwrap();
    assert_eq!(
        registry.seal(&sessions, sink, 3),
        Err(SessionError::InvalidTransition)
    );
    registry.seal(&sessions, sink, 4).unwrap();
    assert!(registry.begin_write(&sessions, sink, 4, 1).is_err());
    let empty = Destination {
        destination: DirectDestination::Member { member_id: 1 },
        ..sink
    };
    registry
        .register(&sessions, empty, Path::new("empty"), Some(0))
        .unwrap();
    let snapshot = registry
        .capture(budget.try_reserve_memory(16 * 1024).unwrap())
        .unwrap();
    assert!(!snapshot.files[1].sealed);
    registry.seal(&sessions, empty, 0).unwrap();
    let snapshot = registry
        .capture(budget.try_reserve_memory(16 * 1024).unwrap())
        .unwrap();
    assert!(snapshot.files[1].sealed);
    assert_eq!(snapshot.files[1].length, Some(0));
}
