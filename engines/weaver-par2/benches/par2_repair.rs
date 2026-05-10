#[allow(dead_code)]
#[path = "../tests/support/benchmark_support.rs"]
mod benchmark_support;

use std::collections::BTreeMap;
use std::fs;
use std::time::{Duration, Instant};

use benchmark_support::{crate_bench_scenarios, select_scenarios, stage_scenario};
use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use md5::{Digest, Md5};
use tempfile::tempdir;
use weaver_par2::{
    DiskFileAccess, FactorDst, FileDescription, FileId, Par2FileSet, Par2RepairStatus,
    Par2Repairer, Par2RepairerOptions, RecoverySetId, SliceChecksum, SliceChecksumState,
    mul_acc_multi_region, mul_acc_region, verify_slices,
};

fn bench_filter() -> Vec<String> {
    std::env::var("WEAVER_PAR2_BENCH_SCENARIOS")
        .ok()
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|part| !part.is_empty())
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

fn benchmark_scenarios() -> Vec<benchmark_support::Scenario> {
    select_scenarios(crate_bench_scenarios(), &bench_filter())
}

fn repairer_options(
    staged: &benchmark_support::StagedScenario,
    repair: bool,
) -> Par2RepairerOptions {
    let mut options = Par2RepairerOptions::new(
        staged.temp.path().to_path_buf(),
        vec![staged.main_par2.clone()],
    );
    options.recovery_paths = staged.recovery_par2.clone();
    options.repair = repair;
    options
}

fn synthetic_par2_file(filename: &str, data: &[u8], slice_size: u64) -> (Par2FileSet, FileId) {
    let hash_full = weaver_par2::checksum::md5(data);
    let hash_16k = weaver_par2::checksum::md5(&data[..data.len().min(16 * 1024)]);
    let mut file_id_bytes = [0u8; 16];
    file_id_bytes[..8].copy_from_slice(&slice_size.to_le_bytes());
    file_id_bytes[8..12].copy_from_slice(&(data.len() as u32).to_le_bytes());
    file_id_bytes[12..].copy_from_slice(&0xA5A5_5A5Au32.to_le_bytes());
    let file_id = FileId::from_bytes(file_id_bytes);

    let checksums = data
        .chunks(slice_size as usize)
        .map(|chunk| {
            let mut state = SliceChecksumState::new();
            state.update(chunk);
            let pad_to = (chunk.len() as u64 != slice_size).then_some(slice_size);
            let (crc32, md5) = state.finalize(pad_to);
            SliceChecksum { crc32, md5 }
        })
        .collect::<Vec<_>>();

    let mut files = std::collections::HashMap::new();
    files.insert(
        file_id,
        FileDescription {
            file_id,
            hash_full,
            hash_16k,
            length: data.len() as u64,
            par2_name: filename.to_string(),
            filename: filename.to_string(),
        },
    );

    let mut slice_checksums = std::collections::HashMap::new();
    slice_checksums.insert(file_id, checksums);

    (
        Par2FileSet {
            recovery_set_id: RecoverySetId::from_bytes([0x42; 16]),
            slice_size,
            recovery_file_ids: vec![file_id],
            non_recovery_file_ids: Vec::new(),
            files,
            slice_checksums,
            recovery_slices: BTreeMap::new(),
            creator: None,
        },
        file_id,
    )
}

fn bench_verify_plan(c: &mut Criterion) {
    let mut group = c.benchmark_group("repairer_verify_plan");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(30));

    for scenario in benchmark_scenarios() {
        group.bench_with_input(
            BenchmarkId::from_parameter(scenario.name),
            &scenario,
            |b, scenario| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let staged = stage_scenario(scenario);
                        let repairer = Par2Repairer::new(repairer_options(&staged, false));
                        let started = Instant::now();
                        let outcome = repairer.verify_or_repair().expect("verify_or_repair");
                        total += started.elapsed();
                        assert_eq!(
                            outcome.status,
                            Par2RepairStatus::RepairPossible,
                            "{} expected a repairable verify-plan outcome",
                            scenario.name
                        );
                    }
                    total
                });
            },
        );
    }

    group.finish();
}

fn bench_repair_workflow(c: &mut Criterion) {
    let mut group = c.benchmark_group("repairer_repair_workflow");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(60));

    for scenario in benchmark_scenarios() {
        group.bench_with_input(
            BenchmarkId::from_parameter(scenario.name),
            &scenario,
            |b, scenario| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let staged = stage_scenario(scenario);
                        let repairer = Par2Repairer::new(repairer_options(&staged, true));
                        let started = Instant::now();
                        let outcome = repairer.verify_or_repair().expect("verify_or_repair");
                        total += started.elapsed();
                        assert_eq!(
                            outcome.status,
                            Par2RepairStatus::Repaired,
                            "{} expected a repaired workflow outcome",
                            scenario.name
                        );
                    }
                    total
                });
            },
        );
    }

    group.finish();
}

fn bench_verify_slices_batched_io(c: &mut Criterion) {
    let mut group = c.benchmark_group("verify_slices_batched_md5");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(20));

    for slice_size in [64 * 1024u64, 1024 * 1024u64] {
        let data = (0..(16 * 1024 * 1024))
            .map(|index| (index as u8).wrapping_mul(19).wrapping_add(5))
            .collect::<Vec<_>>();
        let filename = format!("verify-{slice_size}.bin");
        let (set, file_id) = synthetic_par2_file(&filename, &data, slice_size);
        let dir = tempdir().expect("tempdir");
        fs::write(dir.path().join(&filename), &data).expect("write benchmark data");
        let access = DiskFileAccess::new(dir.path().to_path_buf(), &set);

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{slice_size}_byte_slices")),
            &slice_size,
            |b, _| {
                b.iter(|| {
                    let result =
                        verify_slices(black_box(&set), black_box(&file_id), black_box(&access))
                            .expect("verify_slices");
                    black_box(result);
                });
            },
        );
    }

    group.finish();
}

fn bench_gf_kernel(c: &mut Criterion) {
    let mut group = c.benchmark_group("gf_kernel");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(30));

    let mut src = vec![0u8; 65_536];
    let mut dst = vec![0u8; 65_536];
    for (index, byte) in src.iter_mut().enumerate() {
        *byte = (index % 251) as u8 | 1;
    }

    group.bench_function("mul_acc_region_64kb", |b| {
        b.iter(|| {
            mul_acc_region(0x1234, &src, &mut dst);
        });
    });

    let factors: Vec<u16> = (1..=450).collect();
    let mut dsts: Vec<Vec<u8>> = (0..450).map(|_| vec![0u8; 65_536]).collect();
    group.bench_function("mul_acc_multi_region_64kb_x450", |b| {
        b.iter(|| {
            let mut pairs: Vec<FactorDst<'_>> = factors
                .iter()
                .zip(dsts.iter_mut())
                .map(|(&factor, dst)| FactorDst {
                    factor,
                    dst: dst.as_mut_slice(),
                })
                .collect();
            mul_acc_multi_region(&mut pairs, &src);
        });
    });

    group.finish();
}

fn bench_md5_hotloop(c: &mut Criterion) {
    let mut group = c.benchmark_group("par2_md5_hotloop");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(20));

    let data: Vec<u8> = (0..(8 * 1024 * 1024))
        .map(|index| (index as u8).wrapping_mul(17).wrapping_add(3))
        .collect();

    group.bench_function("native_backend", |b| {
        b.iter(|| {
            black_box(weaver_par2::checksum::md5(black_box(&data)));
        });
    });

    group.bench_function("rustcrypto_fallback", |b| {
        b.iter(|| {
            let mut hasher = Md5::new();
            hasher.update(black_box(&data));
            let digest: [u8; 16] = hasher.finalize().into();
            black_box(digest);
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_verify_plan,
    bench_repair_workflow,
    bench_verify_slices_batched_io,
    bench_gf_kernel,
    bench_md5_hotloop
);
criterion_main!(benches);
