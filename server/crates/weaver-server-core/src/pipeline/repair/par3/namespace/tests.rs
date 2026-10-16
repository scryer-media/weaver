use crate::operations::metrics::PipelineMetrics;
use crate::pipeline::extraction::output::channel::StreamOwner;
use crate::pipeline::extraction::runtime::repair::{
    NamespaceInput, NamespaceRepairOutcome, NamespaceRepairRequest, RepairFormat,
};
use crate::pipeline::extraction::safety::{
    ExtractionLimits, JobExtractionBudget, ProcessMemoryBudget,
};
use std::path::{Path, PathBuf};
use std::sync::Arc;

fn create(root: &Path, name: &str, files: &[PathBuf]) -> Vec<PathBuf> {
    let blocks: u64 = files
        .iter()
        .map(|file| {
            std::fs::metadata(root.join(file))
                .unwrap()
                .len()
                .div_ceil(1024)
        })
        .sum();
    par3_rs::create(
        &par3_rs::InputSpec::new(root, files),
        &root.join(name),
        &par3_rs::CreateOptions::default()
            .with_block_size(1024)
            .with_recovery(par3_rs::RecoveryAmount::Blocks(blocks + 1)),
    )
    .unwrap()
    .files_written
}

fn run(
    paths: &[PathBuf],
    output: &Path,
    donors: &Path,
    format: RepairFormat,
    generation: u64,
) -> NamespaceRepairOutcome {
    let process = Arc::new(ProcessMemoryBudget::new(128 << 20));
    let budget = JobExtractionBudget::new_with_process_memory(
        Arc::new(ExtractionLimits {
            max_job_bytes: 1 << 20,
            max_member_bytes: 1 << 20,
            max_entries: 64,
            max_ratio: 100,
            max_seconds: 3600,
            min_free_bytes: 1,
            max_memory_bytes: 128 << 20,
        }),
        process.clone(),
        output.to_path_buf(),
        1 << 20,
        0,
        0,
        PipelineMetrics::new(),
    )
    .unwrap();
    let memory = budget.try_reserve_memory(64 << 20).unwrap();
    let inputs = paths
        .iter()
        .map(|path| NamespaceInput {
            path: path.clone(),
            name: path.file_name().unwrap().to_str().unwrap().into(),
            ranges: std::iter::once(0..std::fs::metadata(path).unwrap().len()).collect(),
        })
        .collect();
    let result = NamespaceRepairRequest {
        owner: StreamOwner {
            root: 8,
            node: 5,
            generation,
        },
        format,
        inputs,
        output: output.to_path_buf(),
        donors: donors.to_path_buf(),
        output_limit: 1 << 20,
        process: process.clone(),
        cancellation: Default::default(),
        memory,
    }
    .execute()
    .unwrap();
    assert_eq!(result.owner.generation, generation);
    assert_eq!(process.reserved_bytes(), 0);
    result
}

#[test]
fn par3_reconstructs_par2_recovery_for_the_next_generation() {
    recursive(RepairFormat::Par2);
}

#[test]
fn par3_reconstructs_par3_recovery_for_the_next_generation() {
    recursive(RepairFormat::Par3);
}

fn recursive(inner_format: RepairFormat) {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("source");
    let outer = temp.path().join("outer");
    let inner = temp.path().join("inner");
    for root in [&source, &outer, &inner] {
        std::fs::create_dir(root).unwrap();
    }
    let payload: Vec<_> = (0..4096u32)
        .map(|index| (index * 23 + index / 31) as u8)
        .collect();
    let data = source.join("leaf.bin");
    std::fs::write(&data, &payload).unwrap();
    let inner_carriers = match inner_format {
        RepairFormat::Par2 => {
            use par2_rs::create::{BlockSizing, Par2Creator, Par2CreatorOptions, RecoveryAmount};
            let mut options = Par2CreatorOptions::with_output(
                source.join("inner.par2"),
                Some(source.clone()),
                vec![data],
            );
            options.block_sizing = BlockSizing::Bytes(1024);
            options.recovery_amount = RecoveryAmount::Count(5);
            options.volume_count = Some(1);
            let creator = Par2Creator::new(options);
            creator
                .create(&creator.plan().unwrap())
                .unwrap()
                .output_paths
        }
        RepairFormat::Par3 => create(&source, "inner.par3", &[PathBuf::from("leaf.bin")]),
    };
    // Protect every recovery volume, preserving all authenticated metadata.
    let relative: Vec<_> = inner_carriers
        .iter()
        .map(|path| PathBuf::from(path.file_name().unwrap()))
        .collect();
    let outer_carriers = create(&source, "outer.par3", &relative);
    let originals: Vec<_> = inner_carriers
        .iter()
        .chain(&outer_carriers)
        .map(|path| (path.clone(), std::fs::read(path).unwrap()))
        .collect();
    let first = run(
        &outer_carriers,
        &outer,
        &temp.path().join("outer-donors"),
        RepairFormat::Par3,
        1,
    );
    assert_eq!(first.unresolved_sets, 0);
    assert_eq!(first.verified.len(), inner_carriers.len());
    let recovered: Vec<_> = first
        .verified
        .iter()
        .map(|member| outer.join(&member.path))
        .collect();
    let second = run(
        &recovered,
        &inner,
        &temp.path().join("inner-donors"),
        inner_format,
        2,
    );
    assert_eq!(second.unresolved_sets, 0);
    assert_eq!(second.verified.len(), 1);
    assert_eq!(second.verified[0].path, Path::new("leaf.bin"));
    assert_eq!(second.verified[0].length, payload.len() as u64);
    assert_eq!(std::fs::read(inner.join("leaf.bin")).unwrap(), payload);
    for (path, bytes) in originals {
        assert_eq!(std::fs::read(path).unwrap(), bytes);
    }
}
