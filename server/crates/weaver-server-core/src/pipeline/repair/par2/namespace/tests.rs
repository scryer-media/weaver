use super::*;
use crate::operations::metrics::PipelineMetrics;
use crate::pipeline::extraction::{ExtractionLimits, JobExtractionBudget};
use par2_rs::create::{BlockSizing, Par2Creator, Par2CreatorOptions, RecoveryAmount};

fn create(root: &Path, name: &str, files: Vec<PathBuf>) -> Vec<PathBuf> {
    let slices: u64 = files
        .iter()
        .map(|path| std::fs::metadata(path).unwrap().len().div_ceil(1024))
        .sum();
    create_with_recovery(
        root,
        name,
        files,
        RecoveryAmount::Count((slices + 1).try_into().unwrap()),
    )
}

fn create_with_recovery(
    root: &Path,
    name: &str,
    files: Vec<PathBuf>,
    recovery: RecoveryAmount,
) -> Vec<PathBuf> {
    let mut options =
        Par2CreatorOptions::with_output(root.join(name), Some(root.to_path_buf()), files);
    options.block_sizing = BlockSizing::Bytes(1024);
    options.recovery_amount = recovery;
    options.volume_count = Some(1);
    let creator = Par2Creator::new(options);
    creator
        .create(&creator.plan().unwrap())
        .unwrap()
        .output_paths
}

fn memory(root: &Path, process: Arc<ProcessMemoryBudget>) -> MemoryPermit {
    let budget = JobExtractionBudget::new_with_process_memory(
        Arc::new(ExtractionLimits {
            max_job_bytes: 1024 * 1024,
            max_member_bytes: 1024 * 1024,
            max_entries: 64,
            max_ratio: 100,
            max_seconds: 3600,
            min_free_bytes: 1,
            max_memory_bytes: 128 * 1024 * 1024,
        }),
        process,
        root.to_path_buf(),
        1024 * 1024,
        0,
        0,
        PipelineMetrics::new(),
    )
    .unwrap();
    budget.try_reserve_memory(64 * 1024 * 1024).unwrap()
}

fn run(carriers: &[PathBuf], root: &Path) -> NamespaceOutcome {
    let process = Arc::new(ProcessMemoryBudget::new(128 * 1024 * 1024));
    let repair = NamespaceRepair::scan(
        carriers,
        process.clone(),
        par2_rs::CancellationToken::new(),
        1024 * 1024,
    )
    .unwrap();
    let result = repair
        .repair(
            root,
            &root.with_file_name("donor-scratch"),
            Vec::new(),
            memory(root, process.clone()),
        )
        .unwrap();
    assert_eq!(process.reserved_bytes(), 0);
    result
}

#[test]
fn recovered_recovery_carrier_repairs_the_next_namespace() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("source");
    let outer = temp.path().join("outer");
    let inner = temp.path().join("inner");
    for path in [&source, &outer, &inner] {
        std::fs::create_dir(path).unwrap();
    }
    let payload = vec![97; 4096];
    std::fs::write(source.join("leaf.bin"), &payload).unwrap();
    let inner_carriers = create(&source, "inner.par2", vec![source.join("leaf.bin")]);
    // Protect the recovery carrier itself. Its authenticated packets include
    // the inner set's metadata, so no intact inner index is needed.
    let recovery = inner_carriers
        .iter()
        .max_by_key(|path| std::fs::metadata(path).unwrap().len())
        .unwrap();
    let outer_carriers = create(&source, "outer.par2", vec![recovery.clone()]);
    let original: Vec<_> = inner_carriers
        .iter()
        .chain(&outer_carriers)
        .map(|path| (path.clone(), std::fs::read(path).unwrap()))
        .collect();
    let first = run(&outer_carriers, &outer);
    assert_eq!(first.unresolved_sets, 0);
    assert_eq!(
        first
            .verified
            .iter()
            .map(|member| member.path.clone())
            .collect::<Vec<_>>(),
        [PathBuf::from(recovery.file_name().unwrap())]
    );
    let recovered = outer.join(recovery.file_name().unwrap());
    assert_eq!(
        std::fs::read(&recovered).unwrap(),
        std::fs::read(recovery).unwrap()
    );
    let second = run(&[recovered], &inner);
    assert_eq!(second.unresolved_sets, 0);
    assert_eq!(
        second
            .verified
            .iter()
            .map(|member| member.path.clone())
            .collect::<Vec<_>>(),
        [PathBuf::from("leaf.bin")]
    );
    assert_eq!(second.verified[0].length, payload.len() as u64);
    assert_eq!(std::fs::read(inner.join("leaf.bin")).unwrap(), payload);
    for (path, bytes) in original {
        assert_eq!(std::fs::read(path).unwrap(), bytes);
    }
}

#[test]
fn insufficient_namespace_never_claims_verified_outputs() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("source");
    let target = temp.path().join("target");
    std::fs::create_dir(&source).unwrap();
    std::fs::create_dir(&target).unwrap();
    std::fs::write(source.join("leaf.bin"), [7; 4096]).unwrap();
    create(&source, "repair.par2", vec![source.join("leaf.bin")]);
    let outcome = run(&[source.join("repair.par2")], &target);
    assert_eq!(outcome.unresolved_sets, 1);
    assert!(outcome.verified.is_empty());
    assert!(!target.join("leaf.bin").exists());
}

#[test]
fn repaired_shared_donor_advances_an_earlier_insufficient_set() {
    repaired_donor_advances("shared.bin");
}

#[test]
fn newly_reconstructed_extra_donor_advances_an_earlier_insufficient_set() {
    repaired_donor_advances("extra.bin");
}

fn repaired_donor_advances(donor: &str) {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("source");
    let target = temp.path().join("target");
    std::fs::create_dir(&source).unwrap();
    std::fs::create_dir(&target).unwrap();
    std::fs::write(source.join("shared.bin"), [11; 1024]).unwrap();
    std::fs::write(source.join(donor), [11; 1024]).unwrap();
    std::fs::write(source.join("leaf.bin"), [37; 1024]).unwrap();
    let mut carriers = create_with_recovery(
        &source,
        "dependent.par2",
        vec![source.join("shared.bin"), source.join("leaf.bin")],
        RecoveryAmount::Count(1),
    );
    carriers.extend(create_with_recovery(
        &source,
        "donor.par2",
        vec![source.join(donor)],
        RecoveryAmount::Count(1),
    ));
    let process = Arc::new(ProcessMemoryBudget::new(128 * 1024 * 1024));
    let mut repair = NamespaceRepair::scan(
        &carriers,
        process.clone(),
        par2_rs::CancellationToken::new(),
        1024 * 1024,
    )
    .unwrap();
    repair
        .sets
        .sort_by_key(|set| !set.files.values().any(|file| file.filename == "leaf.bin"));
    assert_eq!(repair.sets.len(), 2);
    let outcome = repair
        .repair(
            &target,
            &temp.path().join("donor-scratch"),
            Vec::new(),
            memory(&target, process.clone()),
        )
        .unwrap();
    assert_eq!(outcome.unresolved_sets, 0);
    let mut expected = vec![
        PathBuf::from("leaf.bin"),
        PathBuf::from("shared.bin"),
        PathBuf::from(donor),
    ];
    expected.sort();
    expected.dedup();
    assert_eq!(
        outcome
            .verified
            .iter()
            .map(|member| member.path.clone())
            .collect::<Vec<_>>(),
        expected
    );
    assert_eq!(std::fs::read(target.join("leaf.bin")).unwrap(), [37; 1024]);
    assert_eq!(process.reserved_bytes(), 0);
}

#[test]
fn conflicting_case_aliases_are_rejected_before_repair() {
    let temp = tempfile::tempdir().unwrap();
    let mut carriers = Vec::new();
    for (directory, name, byte) in [("one", "A.bin", 11), ("two", "a.bin", 37)] {
        let root = temp.path().join(directory);
        std::fs::create_dir(&root).unwrap();
        std::fs::write(root.join(name), [byte; 1024]).unwrap();
        carriers.extend(create(&root, "repair.par2", vec![root.join(name)]));
    }
    let process = Arc::new(ProcessMemoryBudget::new(128 * 1024 * 1024));
    let result = NamespaceRepair::scan(
        &carriers,
        process.clone(),
        par2_rs::CancellationToken::new(),
        1024 * 1024,
    );
    assert!(matches!(result, Err(NamespaceError::Invalid(error)) if error.contains("alias")));
    assert_eq!(process.reserved_bytes(), 0);
}

#[test]
fn cancelled_namespace_preserves_cancellation_classification() {
    let cancellation = par2_rs::CancellationToken::new();
    cancellation.cancel();
    let result = NamespaceRepair::scan(
        &[],
        Arc::new(ProcessMemoryBudget::new(128 * 1024 * 1024)),
        cancellation,
        1024,
    );
    assert!(matches!(result, Err(NamespaceError::Cancelled)));
}

#[test]
fn orphan_recovery_group_does_not_block_a_complete_sibling_set() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("source");
    let target = temp.path().join("target");
    std::fs::create_dir(&source).unwrap();
    std::fs::create_dir(&target).unwrap();
    std::fs::write(source.join("orphan.bin"), [5; 1024]).unwrap();
    let orphan_files = create(&source, "orphan.par2", vec![source.join("orphan.bin")]);
    let mut recovery_packets = Vec::new();
    for path in orphan_files {
        let bytes = std::fs::read(path).unwrap();
        let mut offset = 0;
        while offset < bytes.len() {
            let length =
                u64::from_le_bytes(bytes[offset + 8..offset + 16].try_into().unwrap()) as usize;
            if bytes[offset + 48..offset + 64].ends_with(b"RecvSlic") {
                recovery_packets.extend_from_slice(&bytes[offset..offset + length]);
            }
            offset += length;
        }
    }
    assert!(!recovery_packets.is_empty());
    let orphan = source.join("recovery-only.par2");
    std::fs::write(&orphan, recovery_packets).unwrap();
    std::fs::write(source.join("leaf.bin"), [17; 1024]).unwrap();
    let mut carriers = create(&source, "complete.par2", vec![source.join("leaf.bin")]);
    carriers.push(orphan);
    let outcome = run(&carriers, &target);
    assert_eq!(outcome.unresolved_sets, 1);
    assert_eq!(outcome.incomplete_metadata.len(), 1);
    assert_eq!(outcome.verified.len(), 1);
    assert_eq!(outcome.verified[0].path, Path::new("leaf.bin"));
    assert_eq!(std::fs::read(target.join("leaf.bin")).unwrap(), [17; 1024]);
    assert!(!target.join("orphan.bin").exists());
}

#[test]
fn par2_reconstructed_par3_carrier_repairs_the_next_generation() {
    use crate::pipeline::extraction::output::channel::StreamOwner;
    use crate::pipeline::extraction::runtime::repair::{
        NamespaceInput, NamespaceRepairRequest, RepairFormat,
    };

    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("source");
    let outer = temp.path().join("outer");
    let inner = temp.path().join("inner");
    for root in [&source, &outer, &inner] {
        std::fs::create_dir(root).unwrap();
    }
    let recovery = source.join("inner.par3");
    let original_recovery = include_bytes!("../../backend/fixtures/set.vol0+1.par3");
    std::fs::write(&recovery, original_recovery).unwrap();
    let carriers = create(&source, "outer.par2", vec![recovery.clone()]);
    // Neither an intact inner index nor its recovery carrier is present in
    // the effective generation until the outer operation reconstructs it.
    let first = run(&carriers, &outer);
    assert_eq!(first.unresolved_sets, 0);
    assert_eq!(first.verified.len(), 1);
    let restored_carrier = outer.join("inner.par3");
    assert_eq!(std::fs::read(&restored_carrier).unwrap(), original_recovery);
    let original_a: Vec<_> = (0..5000u32).map(|i| (i * 7 + 3) as u8).collect();
    let mut damaged_a = original_a.clone();
    damaged_a[2300] ^= 1;
    let original_c: Vec<_> = (0..4000u32).map(|i| (i * 13 + 1) as u8).collect();
    std::fs::create_dir(source.join("sub")).unwrap();
    let mut inventory = Vec::new();
    for (name, path, bytes) in [
        ("a.bin", source.join("a.bin"), damaged_a.clone()),
        ("b.txt", source.join("b.txt"), b"qrstuvwxyz".to_vec()),
        ("sub/c.bin", source.join("sub/c.bin"), original_c.clone()),
        (
            "inner.par3",
            restored_carrier.clone(),
            original_recovery.to_vec(),
        ),
    ] {
        if path != restored_carrier {
            std::fs::write(&path, &bytes).unwrap();
        }
        inventory.push(NamespaceInput {
            path,
            name: name.into(),
            ranges: std::iter::once(0..bytes.len() as u64).collect(),
        });
    }
    let process = Arc::new(ProcessMemoryBudget::new(128 * 1024 * 1024));
    let lease = memory(&inner, process.clone());
    let owner = StreamOwner {
        root: 17,
        node: 2,
        generation: 3,
    };
    let outcome = NamespaceRepairRequest {
        owner,
        format: RepairFormat::Par3,
        inputs: inventory,
        output: inner.clone(),
        donors: temp.path().join("inner-donors"),
        output_limit: 1024 * 1024,
        process: process.clone(),
        cancellation: Default::default(),
        memory: lease,
    }
    .execute()
    .unwrap();
    assert_eq!(outcome.owner, owner);
    assert_eq!(outcome.unresolved_sets, 0);
    assert_eq!(outcome.verified.len(), 3);
    assert_eq!(std::fs::read(inner.join("a.bin")).unwrap(), original_a);
    assert_eq!(std::fs::read(inner.join("b.txt")).unwrap(), b"qrstuvwxyz");
    assert_eq!(std::fs::read(inner.join("sub/c.bin")).unwrap(), original_c);
    assert_eq!(std::fs::read(source.join("a.bin")).unwrap(), damaged_a);
    assert_eq!(std::fs::read(restored_carrier).unwrap(), original_recovery);
    assert_eq!(process.reserved_bytes(), 0);
}

#[test]
fn generation_request_uses_immutable_external_partial_donors() {
    use crate::pipeline::extraction::output::channel::StreamOwner;
    use crate::pipeline::extraction::runtime::repair::{
        NamespaceInput, NamespaceRepairRequest, RepairFormat,
    };

    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("source");
    let target = temp.path().join("target");
    std::fs::create_dir(&source).unwrap();
    std::fs::create_dir(&target).unwrap();
    let original: Vec<_> = (0..4096u32)
        .map(|index| (index * 19 + index / 17) as u8)
        .collect();
    let data = source.join("data.bin");
    std::fs::write(&data, &original).unwrap();
    let mut paths = create_with_recovery(
        &source,
        "set.par2",
        vec![data.clone()],
        RecoveryAmount::Count(1),
    );
    let mut damaged = original.clone();
    damaged[1800] ^= 1;
    std::fs::write(&data, &damaged).unwrap();
    paths.push(data.clone());
    let snapshots: Vec<_> = paths
        .iter()
        .map(|path| (path.clone(), std::fs::read(path).unwrap()))
        .collect();
    let inputs = paths
        .into_iter()
        .map(|path| {
            let length = std::fs::metadata(&path).unwrap().len();
            NamespaceInput {
                name: path.file_name().unwrap().to_str().unwrap().into(),
                path,
                ranges: std::iter::once(0..length).collect(),
            }
        })
        .collect();
    let process = Arc::new(ProcessMemoryBudget::new(128 * 1024 * 1024));
    let outcome = NamespaceRepairRequest {
        owner: StreamOwner {
            root: 91,
            node: 4,
            generation: 7,
        },
        format: RepairFormat::Par2,
        inputs,
        output: target.clone(),
        donors: temp.path().join("donors"),
        output_limit: 1024 * 1024,
        process: process.clone(),
        cancellation: Default::default(),
        memory: memory(&target, process.clone()),
    }
    .execute()
    .unwrap();
    assert_eq!(outcome.unresolved_sets, 0);
    assert_eq!(outcome.verified.len(), 1);
    assert_eq!(outcome.verified[0].path, Path::new("data.bin"));
    assert_eq!(outcome.verified[0].length, original.len() as u64);
    assert_eq!(std::fs::read(target.join("data.bin")).unwrap(), original);
    assert_eq!(std::fs::read(data).unwrap(), damaged);
    for (path, bytes) in snapshots {
        assert_eq!(std::fs::read(path).unwrap(), bytes);
    }
    assert!(!temp.path().join("donors").exists());
    assert_eq!(process.reserved_bytes(), 0);
}
