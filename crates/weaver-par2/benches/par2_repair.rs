use std::ffi::OsStr;
use std::fs::{self, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use tempfile::TempDir;
use weaver_par2::{
    DiskFileAccess, Par2FileSet, Repairability, execute_repair, plan_repair, verify_all,
};

const SLICE_SIZE: u64 = 65536;

fn fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/rar5_heavy_damage")
}

fn load_par2(dir: &Path) -> Par2FileSet {
    let par2_paths: Vec<PathBuf> = fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension() == Some(OsStr::new("par2")))
        .collect();
    Par2FileSet::from_paths(&par2_paths).unwrap()
}

/// Create a corrupted working copy and disk-backed PAR2 set.
fn corrupted_copy(damage_count: usize) -> (TempDir, Par2FileSet) {
    let fixture = fixture_root();
    let temp = TempDir::new().unwrap();
    for entry in fs::read_dir(&fixture).unwrap() {
        let entry = entry.unwrap();
        fs::copy(entry.path(), temp.path().join(entry.file_name())).unwrap();
    }

    let rar_path = temp.path().join("fixture_rar5_heavy_damage.rar");
    let rar_size = fs::metadata(&rar_path).unwrap().len();
    let total_slices = ((rar_size + SLICE_SIZE - 1) / SLICE_SIZE) as usize;
    let stride = total_slices / (damage_count + 1);

    let mut f = OpenOptions::new().write(true).open(&rar_path).unwrap();
    for i in 0..damage_count {
        let offset = (stride * (i + 1)) as u64 * SLICE_SIZE + 100;
        f.seek(SeekFrom::Start(offset)).unwrap();
        f.write_all(&vec![0xA5; 1024]).unwrap();
    }
    drop(f);

    let par2_set = load_par2(temp.path());
    (temp, par2_set)
}

fn bench_verify(c: &mut Criterion) {
    let mut group = c.benchmark_group("verify");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(30));

    for damage in [28, 100, 250, 450] {
        group.bench_with_input(
            BenchmarkId::new("damaged_slices", damage),
            &damage,
            |b, &damage| {
                // Setup outside the timed region: create corrupted copy once per sample.
                let (temp, par2_set) = corrupted_copy(damage);
                b.iter(|| {
                    let access = DiskFileAccess::new(temp.path().to_path_buf(), &par2_set);
                    let result = verify_all(&par2_set, &access);
                    assert!(matches!(
                        result.repairable,
                        Repairability::Repairable { .. }
                    ));
                    result
                });
            },
        );
    }
    group.finish();
}

fn bench_repair(c: &mut Criterion) {
    let mut group = c.benchmark_group("repair");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(60));

    for damage in [28, 100, 250, 450] {
        group.bench_with_input(
            BenchmarkId::new("damaged_slices", damage),
            &damage,
            |b, &damage| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        // Fresh corrupted copy each iteration (repair modifies files).
                        let (temp, par2_set) = corrupted_copy(damage);
                        let mut access = DiskFileAccess::new(temp.path().to_path_buf(), &par2_set);
                        let verification = verify_all(&par2_set, &access);

                        // Time only the repair.
                        let start = std::time::Instant::now();
                        let plan = plan_repair(&par2_set, &verification).unwrap();
                        execute_repair(&plan, &par2_set, &mut access).unwrap();
                        total += start.elapsed();
                    }
                    total
                });
            },
        );
    }
    group.finish();
}

fn bench_full_cycle(c: &mut Criterion) {
    let mut group = c.benchmark_group("full_cycle");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(60));

    for damage in [28, 100, 250, 450] {
        group.bench_with_input(
            BenchmarkId::new("damaged_slices", damage),
            &damage,
            |b, &damage| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let (temp, par2_set) = corrupted_copy(damage);
                        let mut access = DiskFileAccess::new(temp.path().to_path_buf(), &par2_set);

                        let start = std::time::Instant::now();

                        let verification = verify_all(&par2_set, &access);
                        let plan = plan_repair(&par2_set, &verification).unwrap();
                        execute_repair(&plan, &par2_set, &mut access).unwrap();
                        let repaired = verify_all(&par2_set, &access);
                        assert!(matches!(repaired.repairable, Repairability::NotNeeded));

                        total += start.elapsed();
                    }
                    total
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

    // Isolate the GF multiply kernel: mul_acc_region on a 64KB buffer.
    let mut src = vec![0u8; 65536];
    let mut dst = vec![0u8; 65536];
    // Fill with non-zero data so the multiply isn't trivially skipped.
    for (i, byte) in src.iter_mut().enumerate() {
        *byte = (i % 251) as u8 | 1;
    }

    group.bench_function("mul_acc_region_64kb", |b| {
        b.iter(|| {
            weaver_par2::mul_acc_region(0x1234, &src, &mut dst);
        });
    });

    // Multi-region: 450 factors applied to same src.
    let factors: Vec<u16> = (1..=450).collect();
    let mut dsts: Vec<Vec<u8>> = (0..450).map(|_| vec![0u8; 65536]).collect();

    group.bench_function("mul_acc_multi_region_64kb_x450", |b| {
        b.iter(|| {
            let mut pairs: Vec<weaver_par2::gf_simd::FactorDst<'_>> = factors
                .iter()
                .zip(dsts.iter_mut())
                .map(|(&f, d)| weaver_par2::gf_simd::FactorDst {
                    factor: f,
                    dst: d.as_mut_slice(),
                })
                .collect();
            weaver_par2::mul_acc_multi_region(&mut pairs, &src);
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_verify,
    bench_repair,
    bench_full_cycle,
    bench_gf_kernel
);
criterion_main!(benches);
