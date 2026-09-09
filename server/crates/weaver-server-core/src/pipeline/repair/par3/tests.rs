use super::*;
use par3_rs::source::MemorySourceAccess;

const INDEX: &[u8] = include_bytes!("../backend/fixtures/set.par3");
const RECOVERY: &[u8] = include_bytes!("../backend/fixtures/set.vol0+1.par3");

#[test]
fn scanning_resumes_after_a_hole_and_revisits_the_unfinished_packet_on_arrival() {
    let bytes = include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../../e2e/internal/weaver/testdata/par3-native/set.vol1+2.par3"
    ));
    let len = bytes.len() as u64;
    let mut job = Par3Job::default();
    job.publish_carrier(
        SourceId(0),
        source(bytes),
        len,
        vec![0..1100, 2100..len],
        true,
    )
    .unwrap();
    job.scan(SourceId(0)).unwrap();
    assert_eq!(available_recovery(&mut job), 1);
    assert_eq!(job.carriers[&SourceId(0)].needed, Some(1100));
    job.publish_carrier(
        SourceId(0),
        source(bytes),
        len,
        std::iter::once(0..len).collect(),
        true,
    )
    .unwrap();
    job.scan(SourceId(0)).unwrap();
    assert_eq!(available_recovery(&mut job), 2);
    assert_eq!(job.carriers[&SourceId(0)].needed, None);
}

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
fn native_repair_reads_virtual_sources_and_materializes_only_the_damaged_file() {
    use crate::pipeline::direct_store::{
        ByteRanges,
        provider::{HeldRun, VirtualVolume},
    };
    let root = tempfile::tempdir().unwrap();
    let mut job = Par3Job::default();
    for (index, (name, bytes)) in inputs().into_iter().enumerate() {
        let bytes: Arc<[u8]> = bytes.into();
        let len = bytes.len() as u64;
        let held = if index == 0 {
            vec![
                HeldRun::memory(0, Arc::clone(&bytes), 0, 2000),
                HeldRun::memory(4000, Arc::clone(&bytes), 4000, 1000),
            ]
        } else {
            vec![HeldRun::memory(0, Arc::clone(&bytes), 0, len)]
        };
        let volume = VirtualVolume {
            volume_index: index as u32,
            envelope: root.path().join(format!("{index}.unused-envelope")),
            extents: Vec::new(),
            partials: Arc::default(),
            covered: ByteRanges::new(),
            envelope_covered: ByteRanges::new(),
            held: Arc::new(held),
            len,
            ciphers: Arc::default(),
        };
        let image = virtual_source::VirtualInput::new(volume, &job.options).unwrap();
        job.publish_virtual(SourceId(index as u64 + 1), image, name)
            .unwrap();
    }
    for (id, name, bytes) in [(99, "set.par3", INDEX), (98, "set.vol0+1.par3", RECOVERY)] {
        let path = root.path().join(name);
        std::fs::write(&path, bytes).unwrap();
        job.scan_file(SourceId(id), path, None).unwrap();
    }
    job.assess().unwrap();
    let (&id, set) = job.sets.first_key_value().unwrap();
    assert_eq!(
        set.view.as_ref().unwrap().status,
        par3_rs::session::RepairStatus::Ready
    );
    let unresolved = &set.view.as_ref().unwrap().files[0].unresolved;
    assert_eq!(unresolved.len(), 1);
    assert_eq!(unresolved[0], 2000..4000);
    let verifications = set.native.diagnostics().source_verifications;
    job.assess().unwrap();
    assert_eq!(
        job.sets[&id].native.diagnostics().source_verifications,
        verifications
    );
    let output = root.path().join("output");
    std::fs::create_dir(&output).unwrap();
    let report = job.repair(id, &output).unwrap();
    assert_eq!(report.installed.len(), 1);
    assert_eq!(std::fs::read(output.join("a.bin")).unwrap(), inputs()[0].1);
    assert!(!output.join("b.txt").exists());
    assert!(!output.join("sub/c.bin").exists());
    assert!(!root.path().join("0.unused-envelope").exists());
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
    let published_before = job.sources.snapshot(SourceId(1)).unwrap();
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
    assert_eq!(job.sources.snapshot(SourceId(1)).unwrap(), published_before);
    assert_eq!(
        job.sets
            .values()
            .next()
            .unwrap()
            .native
            .diagnostics()
            .source_verifications,
        verifications,
        "new visibility must retain already verified extents"
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
fn unchanged_disk_publications_retain_evidence_but_withdrawal_and_rebinding_do_not() {
    let root = tempfile::tempdir().unwrap();
    let mut job = Par3Job::default();
    let inputs = inputs();
    for (index, (name, bytes)) in inputs.iter().enumerate() {
        let path = root.path().join(name);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, bytes).unwrap();
        job.publish_file(
            SourceId(index as u64 + 1),
            path,
            name.clone(),
            std::iter::once(0..bytes.len() as u64).collect(),
        )
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
    let before = job
        .sets
        .values()
        .next()
        .unwrap()
        .native
        .diagnostics()
        .source_verifications;
    let snapshot = job.sources.snapshot(SourceId(1)).unwrap().unwrap();
    let revision = job.sources.revision(SourceId(1)).unwrap();
    for (index, (name, bytes)) in inputs.iter().enumerate() {
        job.publish_file(
            SourceId(index as u64 + 1),
            root.path().join(name),
            name.clone(),
            std::iter::once(0..bytes.len() as u64).collect(),
        )
        .unwrap();
    }
    assert!(job.sets.values().next().unwrap().view.is_some());
    assert_eq!(job.sources.snapshot(SourceId(1)).unwrap(), Some(snapshot));
    assert_eq!(job.sources.revision(SourceId(1)).unwrap(), revision);
    job.assess().unwrap();
    assert_eq!(
        job.sets
            .values()
            .next()
            .unwrap()
            .native
            .diagnostics()
            .source_verifications,
        before
    );

    // An explicit write fence must retire evidence even when the disk bytes
    // and their metadata happen to be unchanged at the next publication.
    job.sources.withdraw(SourceId(1)).unwrap();
    let (name, bytes) = &inputs[0];
    job.publish_file(
        SourceId(1),
        root.path().join(name),
        name.clone(),
        std::iter::once(0..bytes.len() as u64).collect(),
    )
    .unwrap();
    assert!(job.sets.values().next().unwrap().view.is_none());
    assert_ne!(job.sources.snapshot(SourceId(1)).unwrap(), Some(snapshot));
    job.assess().unwrap();
    assert_eq!(
        job.sets
            .values()
            .next()
            .unwrap()
            .native
            .diagnostics()
            .source_verifications,
        before + 1
    );

    let alias = root.path().join("alias.bin");
    std::fs::hard_link(root.path().join(name), &alias).unwrap();
    job.publish_file(
        SourceId(1),
        alias,
        "alias.bin".into(),
        std::iter::once(0..bytes.len() as u64).collect(),
    )
    .unwrap();
    assert!(job.sets.values().next().unwrap().view.is_none());
    assert!(!job.bindings.contains_key(name));
    assert_eq!(job.bindings.get("alias.bin"), Some(&SourceId(1)));
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

#[test]
fn disk_carrier_replays_validate_identity_and_logical_generation() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("set.vol0+1.par3");
    std::fs::write(&path, RECOVERY).unwrap();
    let alias = root.path().join("renamed.par3");
    std::fs::hard_link(&path, &alias).unwrap();
    let mut job = Par3Job::default();
    let id = SourceId(0);
    job.scan_file(id, path.clone(), None).unwrap();
    assert_eq!(available_recovery(&mut job), 1);
    let snapshot = job.sources.snapshot(id).unwrap();
    let revision = job.sources.revision(id).unwrap();
    let scanned = job.options.scan_work.used();
    for ranges in [
        None,
        Some(std::iter::once(0..RECOVERY.len() as u64).collect()),
    ] {
        job.scan_file(id, path.clone(), ranges).unwrap();
        assert_eq!(job.sources.snapshot(id).unwrap(), snapshot);
        assert_eq!(job.sources.revision(id).unwrap(), revision);
        assert_eq!(job.options.scan_work.used(), scanned);
        assert_eq!(available_recovery(&mut job), 1);
    }
    job.sources.withdraw(id).unwrap();
    job.scan_file(id, path.clone(), None).unwrap();
    assert_ne!(job.sources.snapshot(id).unwrap(), snapshot);
    assert!(job.options.scan_work.used() > scanned);
    assert_eq!(available_recovery(&mut job), 1);

    // Even identical inode metadata cannot establish a filename binding.
    let snapshot = job.sources.snapshot(id).unwrap();
    job.scan_file(id, alias, None).unwrap();
    assert_ne!(job.sources.snapshot(id).unwrap(), snapshot);
    assert_eq!(available_recovery(&mut job), 1);
}

#[test]
fn disk_carrier_visibility_retains_pending_packet_hashes() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("set.vol0+1.par3");
    std::fs::write(&path, RECOVERY).unwrap();
    let mut job = Par3Job::default();
    let id = SourceId(0);
    let len = RECOVERY.len() as u64;
    job.scan_file(id, path.clone(), Some(std::iter::once(0..1100).collect()))
        .unwrap();
    let snapshot = job.sources.snapshot(id).unwrap();
    assert_eq!(available_recovery(&mut job), 0);
    assert_eq!(job.carriers[&id].needed, Some(1100));
    assert_eq!(job.carriers[&id].resume, None);
    let read = job.options.diagnostics.source_io().read_bytes;
    job.scan_file(id, path.clone(), Some(std::iter::once(0..2100).collect()))
        .unwrap();
    assert_eq!(job.sources.snapshot(id).unwrap(), snapshot);
    assert_eq!(
        job.options.diagnostics.source_io().read_bytes - read,
        1000,
        "the packet prefix must not be read or hashed again"
    );
    assert_eq!(available_recovery(&mut job), 0);
    job.scan_file(id, path, Some(std::iter::once(0..len).collect()))
        .unwrap();
    assert_eq!(job.sources.snapshot(id).unwrap(), snapshot);
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
