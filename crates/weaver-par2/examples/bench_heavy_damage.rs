//! Benchmark: verify + repair on a PAR2 fixture directory.
//!
//! Runs multiple iterations with warmup to produce stable timing and peak RSS.
//!
//! Usage:
//!   DAMAGE=450 cargo run -p weaver-par2 --release --example bench_heavy_damage
//!   DAMAGE=450 WARMUP=2 ITERS=5 cargo run -p weaver-par2 --release --example bench_heavy_damage
//!   DAMAGE=450 MEMORY_LIMIT_MB=50 cargo run -p weaver-par2 --release --example bench_heavy_damage
//!   DAMAGE=450 MEMORY_LIMIT_MB=0 cargo run -p weaver-par2 --release --example bench_heavy_damage
//!   FIXTURE_DIR=/path/to/fixture DAMAGE=450 cargo run -p weaver-par2 --release --example bench_heavy_damage

use std::ffi::OsStr;
use std::fs::{self, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use tempfile::TempDir;
use weaver_par2::{
    DiskFileAccess, Par2FileSet, RepairOptions, Repairability, execute_repair_with_options,
    plan_repair, verify_all,
};

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn env_memory_limit_bytes() -> Option<usize> {
    match std::env::var("MEMORY_LIMIT_MB") {
        Ok(value) => {
            let trimmed = value.trim();
            if trimmed == "0" || trimmed.eq_ignore_ascii_case("none") {
                None
            } else {
                Some(trimmed.parse::<usize>().unwrap() * 1024 * 1024)
            }
        }
        Err(_) => RepairOptions::default().memory_limit,
    }
}

fn peak_rss_bytes() -> u64 {
    unsafe {
        let mut info: libc::rusage = std::mem::zeroed();
        libc::getrusage(libc::RUSAGE_SELF, &mut info);
        // macOS reports ru_maxrss in bytes; Linux in KB.
        #[cfg(target_os = "macos")]
        {
            info.ru_maxrss as u64
        }
        #[cfg(not(target_os = "macos"))]
        {
            info.ru_maxrss as u64 * 1024
        }
    }
}

struct RunResult {
    verify: Duration,
    repair: Duration,
    repair_slices: usize,
    reverify: Duration,
}

impl RunResult {
    fn total(&self) -> Duration {
        self.verify + self.repair + self.reverify
    }
}

struct FixtureInfo {
    dir: PathBuf,
    rar_name: String,
    rar_size: u64,
    slice_size: u64,
    total_slices: usize,
}

fn env_fixture_dir() -> PathBuf {
    std::env::var_os("FIXTURE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/rar5_heavy_damage")
        })
}

fn load_fixture_info(dir: PathBuf) -> FixtureInfo {
    let par2_set = load_par2(&dir);
    let rar_name = fs::read_dir(&dir)
        .unwrap()
        .filter_map(|entry| entry.ok())
        .find_map(|entry| {
            let path = entry.path();
            (path.extension() == Some(OsStr::new("rar")))
                .then(|| entry.file_name().to_string_lossy().to_string())
        })
        .expect("fixture directory must contain a .rar file");
    let rar_path = dir.join(&rar_name);
    let rar_size = fs::metadata(&rar_path).unwrap().len();
    let slice_size = par2_set.slice_size;
    let total_slices = ((rar_size + slice_size - 1) / slice_size) as usize;

    FixtureInfo {
        dir,
        rar_name,
        rar_size,
        slice_size,
        total_slices,
    }
}

fn copy_file_fast(src: &Path, dst: &Path) {
    #[cfg(target_os = "macos")]
    {
        if Command::new("cp")
            .arg("-c")
            .arg(src)
            .arg(dst)
            .status()
            .is_ok_and(|status| status.success())
        {
            return;
        }
    }

    #[cfg(target_os = "linux")]
    {
        if Command::new("cp")
            .args(["--reflink=auto", "--sparse=always"])
            .arg(src)
            .arg(dst)
            .status()
            .is_ok_and(|status| status.success())
        {
            return;
        }
    }

    fs::copy(src, dst).unwrap();
}

/// Prepare a corrupted working copy and return the temp dir.
fn prepare_working_copy(
    fixture: &Path,
    rar_name: &str,
    slice_size: u64,
    damage_count: usize,
    total_slices: usize,
) -> TempDir {
    let temp = TempDir::new().unwrap();
    for entry in fs::read_dir(fixture).unwrap() {
        let entry = entry.unwrap();
        copy_file_fast(&entry.path(), &temp.path().join(entry.file_name()));
    }

    let rar_path = temp.path().join(rar_name);
    let damage_stride = total_slices / (damage_count + 1);
    let damage_bytes = [0xA5_u8; 1024];

    let mut f = OpenOptions::new().write(true).open(&rar_path).unwrap();
    for i in 0..damage_count {
        let offset = (damage_stride * (i + 1)) as u64 * slice_size + 100;
        f.seek(SeekFrom::Start(offset)).unwrap();
        f.write_all(&damage_bytes).unwrap();
    }
    drop(f);

    temp
}

fn load_par2(dir: &Path) -> Par2FileSet {
    let par2_paths: Vec<PathBuf> = fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension() == Some(OsStr::new("par2")))
        .collect();
    Par2FileSet::from_paths(&par2_paths).unwrap()
}

fn run_once(dir: &Path, par2_set: &Par2FileSet, repair_options: &RepairOptions) -> RunResult {
    // Verify
    let t0 = Instant::now();
    let mut access = DiskFileAccess::new(dir.to_path_buf(), par2_set);
    let verification = verify_all(par2_set, &access);
    let verify = t0.elapsed();
    assert!(
        matches!(verification.repairable, Repairability::Repairable { .. }),
        "expected repairable, got {:?}",
        verification.repairable,
    );

    // Repair
    let t1 = Instant::now();
    let plan = plan_repair(par2_set, &verification).unwrap();
    let repair_slices = plan.missing_slices.len();
    execute_repair_with_options(&plan, par2_set, &mut access, repair_options).unwrap();
    let repair = t1.elapsed();

    // Re-verify
    let t2 = Instant::now();
    let repaired = verify_all(par2_set, &access);
    let reverify = t2.elapsed();
    assert!(
        matches!(repaired.repairable, Repairability::NotNeeded),
        "expected clean after repair, got {:?}",
        repaired.repairable,
    );

    RunResult {
        verify,
        repair,
        repair_slices,
        reverify,
    }
}

fn median(values: &mut [f64]) -> f64 {
    values.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = values.len();
    if n % 2 == 0 {
        (values[n / 2 - 1] + values[n / 2]) / 2.0
    } else {
        values[n / 2]
    }
}

fn main() {
    let damage_count = env_usize("DAMAGE", 450);
    let warmup_iters = env_usize("WARMUP", 2);
    let bench_iters = env_usize("ITERS", 5);
    let repair_options = RepairOptions {
        memory_limit: env_memory_limit_bytes(),
        ..RepairOptions::default()
    };

    let fixture = load_fixture_info(env_fixture_dir());

    eprintln!("weaver-par2 benchmark");
    eprintln!(
        "  archive:  {}MB ({} slices × {}KB)",
        fixture.rar_size / 1024 / 1024,
        fixture.total_slices,
        fixture.slice_size / 1024
    );
    eprintln!("  damage:   {damage_count} slices");
    match repair_options.memory_limit {
        Some(limit) => eprintln!("  memory:   {}MB repair budget", limit / 1024 / 1024),
        None => eprintln!("  memory:   unlimited fast path"),
    }
    eprintln!("  warmup:   {warmup_iters} iterations");
    eprintln!("  measured: {bench_iters} iterations");
    eprintln!();

    // Warmup: run full cycles to stabilize caches, JIT, rayon pool, etc.
    for i in 0..warmup_iters {
        let temp = prepare_working_copy(
            &fixture.dir,
            &fixture.rar_name,
            fixture.slice_size,
            damage_count,
            fixture.total_slices,
        );
        let par2_set = load_par2(temp.path());
        let r = run_once(temp.path(), &par2_set, &repair_options);
        eprintln!(
            "  warmup {}/{}: total={:.3}s (v={:.3}s r={:.3}s rv={:.3}s)",
            i + 1,
            warmup_iters,
            r.total().as_secs_f64(),
            r.verify.as_secs_f64(),
            r.repair.as_secs_f64(),
            r.reverify.as_secs_f64(),
        );
    }

    // Measured iterations.
    let mut verify_times = Vec::with_capacity(bench_iters);
    let mut repair_times = Vec::with_capacity(bench_iters);
    let mut reverify_times = Vec::with_capacity(bench_iters);
    let mut total_times = Vec::with_capacity(bench_iters);
    let mut repair_slices = 0;

    let rss_before = peak_rss_bytes();

    for i in 0..bench_iters {
        let temp = prepare_working_copy(
            &fixture.dir,
            &fixture.rar_name,
            fixture.slice_size,
            damage_count,
            fixture.total_slices,
        );
        let par2_set = load_par2(temp.path());
        let r = run_once(temp.path(), &par2_set, &repair_options);

        repair_slices = r.repair_slices;
        verify_times.push(r.verify.as_secs_f64());
        repair_times.push(r.repair.as_secs_f64());
        reverify_times.push(r.reverify.as_secs_f64());
        total_times.push(r.total().as_secs_f64());

        eprintln!(
            "  iter {}/{}: total={:.3}s (v={:.3}s r={:.3}s rv={:.3}s)",
            i + 1,
            bench_iters,
            r.total().as_secs_f64(),
            r.verify.as_secs_f64(),
            r.repair.as_secs_f64(),
            r.reverify.as_secs_f64(),
        );
    }

    let rss_after = peak_rss_bytes();

    eprintln!();
    eprintln!("─── results ({bench_iters} iterations, {repair_slices} slices repaired) ───");
    eprintln!();
    eprintln!(
        "  {:12} {:>10} {:>10} {:>10} {:>10}",
        "", "median", "min", "max", "stdev"
    );

    for (label, times) in [
        ("verify", &mut verify_times),
        ("repair", &mut repair_times),
        ("re-verify", &mut reverify_times),
        ("total", &mut total_times),
    ] {
        let med = median(times);
        let min = times.iter().copied().fold(f64::INFINITY, f64::min);
        let max = times.iter().copied().fold(f64::NEG_INFINITY, f64::max);
        let mean = times.iter().sum::<f64>() / times.len() as f64;
        let variance = times.iter().map(|t| (t - mean).powi(2)).sum::<f64>() / times.len() as f64;
        let stdev = variance.sqrt();

        eprintln!("  {label:12} {med:>9.3}s {min:>9.3}s {max:>9.3}s {stdev:>9.4}s",);
    }

    eprintln!();
    eprintln!(
        "  peak RSS:  {:.1}MB (process high-water mark)",
        rss_after as f64 / 1024.0 / 1024.0
    );
    eprintln!(
        "  RSS delta: {:.1}MB (growth during benchmark)",
        (rss_after - rss_before) as f64 / 1024.0 / 1024.0
    );
}
