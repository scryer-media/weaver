use super::*;
use par3_rs::source::MemorySourceAccess;

pub(super) const INDEX: &[u8] = include_bytes!("../backend/fixtures/set.par3");
pub(super) const RECOVERY: &[u8] = include_bytes!("../backend/fixtures/set.vol0+1.par3");

/// Scan work a replayed open of an unchanged disk source charges. Windows has
/// no inode identity, so each open hashes the whole source once to establish
/// its generation; Unix identifies it from metadata for free.
fn replay_open_cost(len: usize) -> u64 {
    if cfg!(windows) { len as u64 + 1 } else { 0 }
}

#[test]
fn retired_binding_identity_cannot_be_published() {
    let mut job = Par3Job::default();
    let id = bindings::RETIRED_SOURCE;
    let ranges = || std::iter::once(0..INDEX.len() as u64).collect();
    assert!(matches!(
        job.publish_access(id, source(INDEX), "a.bin".into(), ranges(), None),
        Err(EngineError::InvalidState("reserved PAR3 source identity"))
    ));
    assert!(matches!(
        job.publish_carrier(id, source(INDEX), INDEX.len() as u64, ranges(), false),
        Err(EngineError::InvalidState("reserved PAR3 source identity"))
    ));
    assert!(matches!(
        job.scan_embedded(
            id,
            PathBuf::from("absent.zip"),
            "archive.zip".into(),
            None,
            0
        ),
        Err(EngineError::InvalidState("reserved PAR3 source identity"))
    ));
    assert!(job.sources.snapshot(id).unwrap().is_none());
    assert!(job.bindings.is_empty());
    assert!(job.carriers.is_empty());
}

#[test]
fn embedded_name_rebinding_withdraws_old_native_identity_without_rescanning() {
    let bytes = include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../../e2e/internal/weaver/testdata/par3-inside/archive.zip"
    ));
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("archive.zip");
    std::fs::write(&path, bytes).unwrap();
    let mut job = Par3Job::default();
    job.scan_embedded(SourceId(0), path.clone(), "archive.zip".into(), None, 0)
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
        par3_rs::session::RepairStatus::Complete
    );
    let scanned = job.options.scan_work.used();
    let read = job.options.diagnostics.source_io().read_bytes;
    job.scan_embedded(SourceId(0), path.clone(), "alias.zip".into(), None, 0)
        .unwrap();
    job.assess().unwrap();
    let view = job.sets.values().next().unwrap().view.as_ref().unwrap();
    assert!(!view.files[0].complete);
    assert!(view.files[0].source.is_none());
    assert!(view.embedded_source.is_none());
    assert!(view.verified_sources.is_empty());
    assert_eq!(
        job.options.scan_work.used(),
        scanned + replay_open_cost(bytes.len())
    );
    let matched_read = job.options.diagnostics.source_io().read_bytes;
    assert!(
        matched_read > read,
        "the renamed carrier needs an identity check"
    );
    let found = job.take_name_match().unwrap().unwrap();
    assert_eq!(found.name, "archive.zip");
    assert_eq!(found.source, SourceId(0));
    for _ in 0..3 {
        job.assess().unwrap();
    }
    assert_eq!(job.options.diagnostics.source_io().read_bytes, matched_read);
    assert_eq!(
        job.options.scan_work.used(),
        scanned + replay_open_cost(bytes.len())
    );
    job.scan_embedded(SourceId(0), path, "archive.zip".into(), None, 0)
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
        par3_rs::session::RepairStatus::Complete
    );
    assert_eq!(
        job.options.scan_work.used(),
        scanned + 2 * replay_open_cost(bytes.len())
    );
}

#[test]
fn shared_description_consistency_requires_no_source_reads() {
    const ROOT: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../../e2e/internal/weaver/testdata/par3-shared"
    );
    for conflict in [false, true] {
        let mut job = Par3Job::default();
        job.scan_file(SourceId(99), PathBuf::from(ROOT).join("cauchy.par3"), None)
            .unwrap();
        job.scan_file(
            SourceId(100),
            PathBuf::from(ROOT).join(if conflict {
                "conflict.par3"
            } else {
                "fft.par3"
            }),
            None,
        )
        .unwrap();
        assert_eq!(
            job.sets.len(),
            2,
            "distinct official input sets must be admitted"
        );
        let root = tempfile::tempdir().unwrap();
        if conflict {
            // Even an available candidate must not be hashed or rewritten once
            // authenticated descriptions already contradict each other.
            let path = root.path().join("payload.bin");
            std::fs::write(&path, vec![0; 262144 + 123]).unwrap();
            job.publish_file(
                SourceId(0),
                path,
                "payload.bin".into(),
                std::iter::once(0..262144 + 123).collect(),
            )
            .unwrap();
        }
        let read = job.options.diagnostics.source_io().read_bytes;
        for _ in 0..3 {
            let result = job.assess();
            if conflict {
                assert!(matches!(
                    result,
                    Err(EngineError::InvalidState(
                        "contradictory authenticated PAR3 descriptions for one output path"
                    ))
                ));
            } else {
                result.unwrap();
            }
            assert_eq!(job.options.diagnostics.source_io().read_bytes, read);
        }
    }
}

#[test]
fn embedded_late_metadata_rewinds_once_and_preserves_hole_continuity() {
    let bytes = include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../../e2e/internal/weaver/testdata/par3-inside/archive.zip"
    ));
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("archive.zip");
    // Change only a stored member byte, well before the original ZIP footer
    // and all official packets, so assessment exposes its recovery requirement.
    let mut damaged = bytes.to_vec();
    damaged[1024] ^= 1;
    std::fs::write(&path, damaged).unwrap();
    let source = SourceId(0);
    // Locate a complete official metadata suffix with no recovery packets.
    // Packet signatures only select candidates; the native parser admits them.
    let (mut job, start) = bytes
        .windows(par3_rs::MAGIC.len())
        .enumerate()
        .rev()
        .filter(|(_, marker)| *marker == par3_rs::MAGIC)
        .find_map(|(at, _)| {
            let mut job = Par3Job::default();
            job.scan_file_from(source, path.clone(), None, at as u64)
                .unwrap();
            let complete = job
                .sets
                .values_mut()
                .any(|set| set.native.layout().unwrap().is_some());
            (complete && available_recovery(&mut job) == 0).then_some((job, at as u64))
        })
        .expect("official fixture has late metadata after recovery");
    job.scan_embedded(source, path.clone(), "archive.zip".into(), None, start)
        .unwrap();
    assert!(job.carriers[&source].scan_start < start);
    assert!(available_recovery(&mut job) > 0);
    let read = job.options.diagnostics.source_io().read_bytes;
    let scanned = job.options.scan_work.used();
    for _ in 0..3 {
        job.scan_embedded(source, path.clone(), "archive.zip".into(), None, start)
            .unwrap();
    }
    assert_eq!(job.options.diagnostics.source_io().read_bytes, read);
    assert_eq!(
        job.options.scan_work.used(),
        scanned + 3 * replay_open_cost(bytes.len())
    );

    // The same rewind must preserve the retry point for an unavailable prefix
    // of the first packet; no unavailable carrier bytes become implicit zeroes.
    let floor = job.carriers[&source].scan_start;
    let len = bytes.len() as u64;
    let mut missing = Par3Job::default();
    missing
        .scan_embedded(
            source,
            path.clone(),
            "archive.zip".into(),
            Some(vec![0..floor, floor + 16..len]),
            start,
        )
        .unwrap();
    assert!(missing.carriers[&source].resume.is_some());
    missing
        .scan_embedded(source, path, "archive.zip".into(), None, start)
        .unwrap();
    assert_eq!(
        available_recovery(&mut missing),
        available_recovery(&mut job)
    );
    assert!(missing.carriers[&source].needed.is_none());
}

#[test]
fn retained_sessions_share_the_process_memory_pool_and_release_their_charge() {
    // Share one isolated pool between these sessions; concurrent tests must
    // not affect the baseline or the post-drop accounting assertion.
    let options = ExecutionOptions::default();
    let mut first = Par3Job {
        options: options.clone(),
        ..Par3Job::default()
    };
    let second = Par3Job {
        options,
        ..Par3Job::default()
    };
    let before = second.options.memory.used();
    first
        .publish_carrier(
            SourceId(0),
            source(INDEX),
            INDEX.len() as u64,
            std::iter::once(0..INDEX.len() as u64).collect(),
            true,
        )
        .unwrap();
    first.scan(SourceId(0)).unwrap();
    assert!(first.options.memory.used() > before);
    assert_eq!(second.options.memory.used(), first.options.memory.used());
    assert!(second.options.memory.available() < second.options.memory.limit());
    drop(first);
    assert_eq!(second.options.memory.used(), before);
}

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

pub(super) fn inputs() -> [(String, Vec<u8>); 3] {
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
    let incremental_runs = set.verification_runs;
    let elapsed_before = set.verification_elapsed;
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
    let set = job.sets.values().next().unwrap();
    assert_eq!(set.verification_runs, incremental_runs + 1);
    assert!(set.verification_elapsed > elapsed_before);
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
    job.assess().unwrap();
    let set = job.sets.values().next().unwrap();
    let view = set.view.as_ref().unwrap();
    assert_eq!(view.status, par3_rs::session::RepairStatus::NeedRecovery);
    assert!(
        view.files[0].source.is_none(),
        "retired names need a new explicit binding"
    );
    assert!(!view.files[0].complete);
    assert!(view.files[1..].iter().all(|file| file.complete));
    assert_eq!(set.native.diagnostics().source_verifications, before + 1);
    job.publish_file(
        SourceId(1),
        root.path().join(name),
        name.clone(),
        std::iter::once(0..bytes.len() as u64).collect(),
    )
    .unwrap();
    job.assess().unwrap();
    let set = job.sets.values().next().unwrap();
    assert_eq!(
        set.view.as_ref().unwrap().status,
        par3_rs::session::RepairStatus::Complete
    );
    assert_eq!(
        set.view.as_ref().unwrap().files[0].source,
        Some(SourceId(1))
    );
    assert_eq!(set.native.diagnostics().source_verifications, before + 2);
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
    let mut scanned = job.options.scan_work.used();
    for ranges in [
        None,
        Some(std::iter::once(0..RECOVERY.len() as u64).collect()),
    ] {
        job.scan_file(id, path.clone(), ranges).unwrap();
        scanned += replay_open_cost(RECOVERY.len());
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

/// The fixture index's Root packet, so a test can damage exactly one copy of
/// one vital packet and leave every other packet in the carrier intact.
const ROOT_PACKET: std::ops::Range<usize> = 672..781;

/// Damage a packet in place: past its 48-byte header, so the scanner still
/// finds the packet and still reads its declared length, and only the hash it
/// carries no longer describes the body behind it. That is what a bad sector
/// or a truncated article looks like to the scanner.
fn with_damaged_packet(carrier: &[u8], packet: std::ops::Range<usize>) -> Vec<u8> {
    let mut bytes = carrier.to_vec();
    for byte in &mut bytes[packet.start + 48..packet.end] {
        *byte ^= 0xff;
    }
    bytes
}

/// The input set every packet of the fixture carriers belongs to, read from
/// the first packet's own header.
fn fixture_set_id() -> par3_rs::InputSetId {
    par3_rs::InputSetId(INDEX[32..40].try_into().expect("packet header"))
}

/// Append one packet of weaver's choosing to a carrier, built and hashed by
/// the engine's own packet builder so it authenticates like any other.
fn carrier_with(carrier: &[u8], body: par3_rs::packet::PacketBody) -> Vec<u8> {
    let mut bytes = carrier.to_vec();
    bytes.extend_from_slice(&par3_rs::packet::Packet::new(fixture_set_id(), body).to_bytes());
    bytes
}

fn scan_carrier(bytes: &[u8]) -> Par3Job {
    let mut job = Par3Job::default();
    let len = bytes.len() as u64;
    job.publish_carrier(
        SourceId(0),
        source(bytes),
        len,
        std::iter::once(0..len).collect(),
        true,
    )
    .unwrap();
    job.scan(SourceId(0)).unwrap();
    job
}

/// Deliverable: a damaged copy of a vital packet is reported as a damaged byte
/// range at its own offset, and the scan continues past it.
#[test]
fn a_damaged_vital_packet_is_reported_at_its_own_offset() {
    let bytes = with_damaged_packet(INDEX, ROOT_PACKET);
    let job = scan_carrier(&bytes);
    let damage = job.damage_report();
    assert_eq!(damage.len(), 1, "one carrier was damaged: {damage:?}");
    assert_eq!(damage[0].source, SourceId(0));
    assert_eq!(
        damage[0].first_damage_offset, ROOT_PACKET.start as u64,
        "the damaged run starts where the refused packet did"
    );
    assert_eq!(
        damage[0].damaged_bytes,
        ROOT_PACKET.len() as u64,
        "only the refused packet's own bytes are damaged"
    );
    let families = job.authenticated_families();
    assert_eq!(
        families[carriers::Par3PacketKind::Root.index()],
        0,
        "the only Root copy in this carrier did not authenticate"
    );
    assert_eq!(
        families[carriers::Par3PacketKind::File.index()],
        3,
        "every packet behind the damage still authenticated"
    );
    assert_eq!(families[carriers::Par3PacketKind::Start.index()], 1);
    assert_eq!(families[carriers::Par3PacketKind::Matrix.index()], 1);
    assert_eq!(families[carriers::Par3PacketKind::Directory.index()], 1);
}

/// Deliverable: the volume carrier repeats every vital packet, so an index
/// that never arrives costs the set nothing.
#[test]
fn the_volume_carrier_alone_holds_every_vital_packet() {
    let job = scan_carrier(RECOVERY);
    let families = job.authenticated_families();
    for kind in carriers::Par3PacketKind::VITAL {
        assert_ne!(
            families[kind.index()],
            0,
            "the volume carrier alone must supply an authenticated {} packet",
            kind.label()
        );
    }
    assert!(
        job.damage_report().is_empty(),
        "an undamaged carrier reports no damage"
    );
}

/// Deliverable: a set with no authenticated Root copy anywhere names Root as
/// the packet family it is short of.
#[test]
fn a_set_with_no_authenticated_root_names_the_missing_family() {
    let bytes = with_damaged_packet(INDEX, ROOT_PACKET);
    let job = scan_carrier(&bytes);
    let families = job.authenticated_families();
    let missing = carriers::Par3PacketKind::VITAL
        .into_iter()
        .find(|kind| families[kind.index()] == 0);
    assert_eq!(missing, Some(carriers::Par3PacketKind::Root));
    let outcome = outcome::Par3Outcome::MetadataIncomplete {
        missing: outcome::MissingMetadata {
            sets: 1,
            carriers_awaiting_bytes: 0,
            unresolved_files: 3,
            missing_vital: missing,
        },
    };
    assert!(
        outcome.to_string().contains("no authenticated root packet"),
        "{outcome}"
    );
}

/// Deliverable: a link or permission packet is counted as present and ignored,
/// never applied and never a reason to refuse the set.
///
/// The count is the set's own: an option packet is retained by the set that
/// admitted it, so the set has to be resolved before anyone can be told how
/// many it carries. That is the same moment the plan becomes reportable.
#[test]
fn an_option_packet_is_reported_and_never_applied() {
    let bytes = carrier_with(
        INDEX,
        par3_rs::packet::PacketBody::Opaque {
            packet_type: par3_rs::packet::PacketType::UnixPermissions,
            body: vec![1, 2, 3, 4],
        },
    );
    let mut job = scan_carrier(&bytes);
    for set in job.sets.values_mut() {
        set.assess().expect("the index carrier resolves its set");
    }
    let tally = job.option_packet_tally();
    assert_eq!(tally.present, 1, "the option packet authenticated");
    assert_eq!(tally.referenced, 0);
    assert_eq!(tally.unresolved, 0);
    assert!(
        !tally.is_silent(),
        "a present option packet is worth a line"
    );
    assert!(
        job.damage_report().is_empty(),
        "an option packet is not damage"
    );
}

/// Deliverable: an option packet a File packet points at but that nothing
/// authenticated is counted as unresolved, and the carrier still scans.
#[test]
fn an_unresolved_option_reference_is_counted_and_does_not_stop_the_scan() {
    let bytes = carrier_with(
        INDEX,
        par3_rs::packet::PacketBody::File(par3_rs::packet::file::FilePacket {
            name: "kestrel.bin".into(),
            quick_rolling_hash: 0,
            fingerprint: [0; 16],
            option_hashes: vec![[0x5a; 16]],
            chunks: Vec::new(),
        }),
    );
    let job = scan_carrier(&bytes);
    let tally = job.option_packet_tally();
    assert_eq!(tally.referenced, 1, "the File packet named one option");
    assert_eq!(tally.unresolved, 1, "nothing authenticated that option");
    assert_eq!(tally.present, 0);
    assert!(
        job.authenticated_families()[carriers::Par3PacketKind::Root.index()] != 0,
        "the rest of the carrier still authenticated"
    );
}

/// Deliverable: hostile metadata stops at a named engine ceiling rather than
/// at host exhaustion, and the ceiling reaches the pipeline as a typed limit.
#[test]
fn hostile_metadata_stops_at_a_named_ceiling() {
    let mut bytes = Vec::new();
    // One authenticated Creator packet per fabricated input set, built by the
    // engine's own builder. Nothing about them is malformed: the hostility is
    // that there are more sets than one job will ever hold open.
    for index in 0..=MAX_SETS as u64 {
        let mut id = [0u8; 8];
        id.copy_from_slice(&index.to_le_bytes());
        bytes.extend_from_slice(
            &par3_rs::packet::Packet::new(
                par3_rs::InputSetId(id),
                par3_rs::packet::PacketBody::Creator(par3_rs::packet::creator::CreatorPacket::new(
                    "kestrel",
                )),
            )
            .to_bytes(),
        );
    }
    let mut job = Par3Job::default();
    let len = bytes.len() as u64;
    job.publish_carrier(
        SourceId(0),
        source(&bytes),
        len,
        std::iter::once(0..len).collect(),
        true,
    )
    .unwrap();
    let error = job.scan(SourceId(0)).expect_err("the set ceiling holds");
    assert_eq!(
        outcome::execution_limit(&error),
        Some("job PAR3 set count"),
        "the refusal names its own ceiling: {error}"
    );
    let outcome = outcome::Par3Outcome::NotExecutable {
        limit: outcome::execution_limit(&error)
            .expect("named limit")
            .into(),
    };
    assert_eq!(
        outcome.class(),
        crate::operations::metrics::Par3OutcomeClass::NotExecutable
    );
    assert!(
        outcome.to_string().contains("job PAR3 set count"),
        "{outcome}"
    );
}
