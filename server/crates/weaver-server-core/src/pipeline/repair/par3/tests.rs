use super::*;
use par3_rs::source::MemorySourceAccess;

const INDEX: &[u8] = include_bytes!("../backend/fixtures/set.par3");
const RECOVERY: &[u8] = include_bytes!("../backend/fixtures/set.vol0+1.par3");

fn source(bytes: &[u8]) -> Arc<dyn SourceAccess> {
    let mut memory = MemorySourceAccess::default();
    memory.insert(SourceId(0), 1, Arc::from(bytes));
    Arc::new(memory)
}

fn inputs() -> [(String, Vec<u8>); 3] {
    [
        (
            "a.bin".into(),
            (0..5000u32).map(|i| (i * 7 + 3) as u8).collect(),
        ),
        ("b.txt".into(), b"qrstuvwxyz".to_vec()),
        (
            "sub/c.bin".into(),
            (0..4000u32).map(|i| (i * 13 + 1) as u8).collect(),
        ),
    ]
}

#[test]
fn retained_job_assessment_reuses_evidence_and_invalidates_changed_sources() {
    use par3_rs::session::RepairStatus;
    let root = tempfile::tempdir().unwrap();
    let mut job = Par3Job::default();
    for (index, (name, bytes)) in inputs().into_iter().enumerate() {
        let path = root.path().join(&name);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, &bytes).unwrap();
        let ranges = if index == 0 {
            // The backing has correct bytes here, but this hole has not been
            // published. Correct physical bytes are not evidence of arrival.
            vec![0..2000, 4000..bytes.len() as u64]
        } else {
            std::iter::once(0..bytes.len() as u64).collect()
        };
        job.publish_file(SourceId(index as u64 + 1), path, name, ranges)
            .unwrap();
    }
    job.publish_carrier(
        SourceId(0),
        source(INDEX),
        INDEX.len() as u64,
        std::iter::once(0..INDEX.len() as u64).collect(),
        false,
    )
    .unwrap();
    job.scan(SourceId(0)).unwrap();
    job.assess().unwrap();
    let set = job.sets.values().next().unwrap();
    let view = set.view.as_ref().unwrap();
    assert_eq!(view.status, RepairStatus::NeedRecovery);
    assert_eq!(view.files[0].verified_prefix, 2000);
    assert_eq!(view.files[0].unresolved.len(), 1);
    assert_eq!(view.files[0].unresolved[0], 2000..4000);
    let verifications = set.native.diagnostics().source_verifications;
    job.assess().unwrap();
    assert_eq!(
        job.sets
            .values()
            .next()
            .unwrap()
            .native
            .diagnostics()
            .source_verifications,
        verifications
    );
    let mut recovery = MemorySourceAccess::default();
    recovery.insert(SourceId(99), 1, Arc::from(RECOVERY));
    job.publish_carrier(
        SourceId(99),
        Arc::new(recovery),
        RECOVERY.len() as u64,
        std::iter::once(0..RECOVERY.len() as u64).collect(),
        true,
    )
    .unwrap();
    job.scan(SourceId(99)).unwrap();
    job.assess().unwrap();
    let set = job.sets.values().next().unwrap();
    assert_eq!(set.native.diagnostics().source_verifications, verifications);
    let view = set.view.as_ref().unwrap();
    assert_eq!(view.status, RepairStatus::Ready);
    assert_eq!(view.requirements[0].available, [0]);
    assert_eq!(view.requirements[0].cohort, 0);
    assert_eq!(view.requirements[0].cohorts, 1);
    job.publish_file(
        SourceId(1),
        root.path().join("a.bin"),
        "a.bin".into(),
        std::iter::once(0..5000).collect(),
    )
    .unwrap();
    assert!(job.sets.values().next().unwrap().view.is_none());
    job.assess().unwrap();
    assert_eq!(
        job.sets
            .values()
            .next()
            .unwrap()
            .view
            .as_ref()
            .unwrap()
            .status,
        RepairStatus::Complete
    );
    let mut changed = inputs()[0].1.clone();
    changed[2300] ^= 1;
    std::fs::write(root.path().join("a.bin"), &changed).unwrap();
    // Failed freshness checks must not leave an earlier Complete view visible.
    assert!(matches!(
        job.assess(),
        Err(EngineError::SourceChanged(SourceId(1)))
    ));
    assert!(job.sets.values().next().unwrap().view.is_none());
    job.publish_file(
        SourceId(1),
        root.path().join("a.bin"),
        "a.bin".into(),
        std::iter::once(0..5000).collect(),
    )
    .unwrap();
    job.assess().unwrap();
    assert_eq!(
        job.sets
            .values()
            .next()
            .unwrap()
            .view
            .as_ref()
            .unwrap()
            .status,
        RepairStatus::Ready
    );
}

#[test]
fn split_headers_payloads_and_replayed_arrivals_retain_authenticated_sets() {
    let mut job = Par3Job::default();
    for end in [1, 7, 47, 71, 80, 255, INDEX.len()] {
        job.publish_carrier(
            SourceId(0),
            source(INDEX),
            INDEX.len() as u64,
            std::iter::once(0..end as u64).collect(),
            true,
        )
        .unwrap();
        job.scan(SourceId(0)).unwrap();
    }
    assert_eq!(job.sets.len(), 1);
    assert!(
        job.sets
            .values_mut()
            .next()
            .unwrap()
            .native
            .layout()
            .unwrap()
            .is_some()
    );
    let retained = job.sets.values().next().unwrap().native.retained_bytes();
    let used = job.options.scan_work.used();
    job.scan(SourceId(0)).unwrap();
    assert_eq!(
        job.options.scan_work.used(),
        used,
        "unchanged carrier is not scanned again"
    );
    job.publish_carrier(
        SourceId(0),
        source(INDEX),
        INDEX.len() as u64,
        std::iter::once(0..INDEX.len() as u64).collect(),
        true,
    )
    .unwrap();
    job.scan(SourceId(0)).unwrap();
    assert_eq!(
        job.sets.values().next().unwrap().native.retained_bytes(),
        retained
    );
}

#[test]
fn recovery_payload_waits_for_interior_hole_and_counts_only_once() {
    let mut job = Par3Job::default();
    let len = RECOVERY.len() as u64;
    job.publish_carrier(
        SourceId(0),
        source(RECOVERY),
        len,
        vec![0..1100, 2100..len],
        true,
    )
    .unwrap();
    job.scan(SourceId(0)).unwrap();
    assert_eq!(available_recovery(&mut job), 0);
    job.publish_carrier(
        SourceId(0),
        source(RECOVERY),
        len,
        std::iter::once(0..len).collect(),
        true,
    )
    .unwrap();
    job.scan(SourceId(0)).unwrap();
    assert_eq!(available_recovery(&mut job), 1);
    // Replaying complete official packets from another source generation is
    // idempotent, including recovery whose matrix arrived in the same carrier.
    job.publish_carrier(
        SourceId(0),
        source(RECOVERY),
        len,
        std::iter::once(0..len).collect(),
        false,
    )
    .unwrap();
    job.scan(SourceId(0)).unwrap();
    assert_eq!(available_recovery(&mut job), 1);
}

fn available_recovery(job: &mut Par3Job) -> usize {
    job.assess().unwrap();
    job.sets
        .values()
        .filter_map(|set| set.view.as_ref())
        .flat_map(|view| &view.requirements)
        .map(|requirement| requirement.available.len())
        .sum()
}
