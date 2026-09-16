//! Matched PAR2 command-line measurements; pipeline/download acceptance is separate.

use anyhow::{Context, Result, bail, ensure};
use clap::Args;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::Read;
use std::num::NonZeroUsize;
use std::path::{Component, Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Instant;

#[derive(Args)]
pub(crate) struct Options {
    /// Optimized Weaver binary built from the baseline revision.
    #[arg(long)]
    baseline: PathBuf,
    /// Optimized Weaver binary built from the candidate revision.
    #[arg(long)]
    candidate: PathBuf,
    /// Damaged input tree. The runner only reads it and makes private copies.
    #[arg(long)]
    fixture: PathBuf,
    /// Byte-exact clean input tree, including the same PAR2 carriers.
    #[arg(long)]
    expected: PathBuf,
    /// Relative path of the index inside both trees.
    #[arg(long)]
    seed: PathBuf,
    /// New result directory; existing directories are refused.
    #[arg(long)]
    output: PathBuf,
    #[arg(long, default_value = "4")]
    threads: NonZeroUsize,
}

type Manifest = BTreeMap<PathBuf, (u64, String)>;

fn hash_file(path: &Path) -> Result<(u64, String)> {
    let mut file = File::open(path)?;
    let mut hash = Sha256::new();
    let mut buffer = [0; 64 << 10];
    let mut len = 0;
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        len += count as u64;
        hash.update(&buffer[..count]);
    }
    Ok((len, hex::encode(hash.finalize())))
}

fn manifest(root: &Path) -> Result<Manifest> {
    fn visit(root: &Path, dir: &Path, files: &mut Manifest) -> Result<()> {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let kind = entry.file_type()?;
            if kind.is_dir() {
                visit(root, &entry.path(), files)?;
            } else {
                ensure!(kind.is_file(), "fixture entries must be regular files");
                let path = entry.path();
                files.insert(path.strip_prefix(root)?.to_owned(), hash_file(&path)?);
            }
        }
        Ok(())
    }
    let mut files = Manifest::new();
    visit(root, root, &mut files)?;
    ensure!(!files.is_empty(), "fixture is empty");
    Ok(files)
}

fn check_outputs(root: &Path, expected: &Manifest) -> Result<()> {
    for (path, hash) in expected {
        ensure!(
            hash_file(&root.join(path))? == *hash,
            "output hash differs: {}",
            path.display()
        );
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum Workload {
    CleanVerify,
    DamagedVerify,
    Repair,
}

impl Workload {
    fn name(self) -> &'static str {
        match self {
            Self::CleanVerify => "clean-verify",
            Self::DamagedVerify => "damaged-verify",
            Self::Repair => "repair",
        }
    }
}

#[derive(Debug)]
struct Usage {
    user_seconds: f64,
    system_seconds: f64,
    max_rss_bytes: u64,
    block_inputs: u64,
    block_outputs: u64,
}

fn parse_usage(text: &str, macos: bool) -> Result<Usage> {
    if !macos {
        let fields: Vec<_> = text
            .lines()
            .rev()
            .find_map(|line| line.strip_prefix("WEAVER_RESOURCE_USAGE "))
            .context("missing GNU time resource record")?
            .split_whitespace()
            .collect();
        ensure!(fields.len() == 5, "invalid GNU time resource record");
        return Ok(Usage {
            user_seconds: fields[0].parse()?,
            system_seconds: fields[1].parse()?,
            max_rss_bytes: fields[2]
                .parse::<u64>()?
                .checked_mul(1024)
                .context("RSS counter overflow")?,
            block_inputs: fields[3].parse()?,
            block_outputs: fields[4].parse()?,
        });
    }
    let seconds = |key: &str| -> Result<f64> {
        text.lines()
            .rev()
            .find_map(|line| line.strip_prefix(key))
            .context("missing BSD time CPU counter")?
            .trim()
            .parse()
            .map_err(Into::into)
    };
    let count = |key: &str| -> Result<u64> {
        text.lines()
            .rev()
            .find_map(|line| line.trim().strip_suffix(key))
            .context("missing BSD time resource counter")?
            .trim()
            .parse()
            .map_err(Into::into)
    };
    Ok(Usage {
        user_seconds: seconds("user ")?,
        system_seconds: seconds("sys ")?,
        max_rss_bytes: count("maximum resident set size")?,
        block_inputs: count("block input operations")?,
        block_outputs: count("block output operations")?,
    })
}

/// Two-sided 95% Student-t interval on paired log elapsed-time ratios.
/// Only the prescribed 10- and 30-pair checkpoints are evaluated.
fn interval(ratios: &[f64]) -> Result<(f64, f64, f64)> {
    let critical = match ratios.len() {
        10 => 2.262_157_163,
        30 => 2.045_229_642,
        _ => bail!("confidence intervals require 10 or 30 pairs"),
    };
    ensure!(
        ratios.iter().all(|r| r.is_finite() && *r > 0.0),
        "invalid elapsed-time ratio"
    );
    let n = ratios.len() as f64;
    let mean = ratios.iter().map(|r| r.ln()).sum::<f64>() / n;
    let variance = ratios.iter().map(|r| (r.ln() - mean).powi(2)).sum::<f64>() / (n - 1.0);
    let delta = critical * (variance / n).sqrt();
    Ok((mean.exp(), (mean - delta).exp(), (mean + delta).exp()))
}

fn trial(
    options: &Options,
    binary: &Path,
    workload: Workload,
    label: &str,
    damaged: &Manifest,
    clean: &Manifest,
) -> Result<(f64, Value)> {
    let run_dir = options.output.join(label);
    fs::create_dir(&run_dir)?;
    let work = tempfile::Builder::new()
        .prefix("input-")
        .tempdir_in(&run_dir)?;
    let (input, hashes) = if matches!(workload, Workload::CleanVerify) {
        (&options.expected, clean)
    } else {
        (&options.fixture, damaged)
    };
    for path in hashes.keys() {
        let destination = work.path().join(path);
        fs::create_dir_all(destination.parent().context("missing output parent")?)?;
        fs::copy(input.join(path), destination)?;
    }
    // Exclude copying and integrity checks from the timed command. Both sides
    // use fresh copies with the same warm-cache policy.
    check_outputs(work.path(), hashes)?;
    let stderr = run_dir.join("stderr.log");
    let mut command = Command::new("/usr/bin/time");
    if cfg!(target_os = "macos") {
        command.args(["-l", "-p"]);
    } else {
        command.args(["-f", "WEAVER_RESOURCE_USAGE %U %S %M %I %O"]);
    }
    command.arg(binary).arg("par2");
    if matches!(workload, Workload::Repair) {
        command.args(["repair", "--working-dir"]).arg(work.path());
    } else {
        command.arg("verify");
    }
    command
        .arg(work.path().join(&options.seed))
        .env("RAYON_NUM_THREADS", options.threads.to_string())
        .current_dir(work.path())
        .stdin(Stdio::null())
        .stdout(File::create(run_dir.join("stdout.log"))?)
        .stderr(File::create(&stderr)?);
    let start = Instant::now();
    let status = command.status()?;
    let elapsed = start.elapsed().as_secs_f64();
    let expected_exit = if matches!(workload, Workload::DamagedVerify) {
        1
    } else {
        0
    };
    ensure!(
        status.code() == Some(expected_exit),
        "unexpected exit {status} in {label}; see {}",
        stderr.display()
    );
    check_outputs(
        work.path(),
        if matches!(workload, Workload::Repair) {
            clean
        } else {
            hashes
        },
    )?;
    let usage = parse_usage(&fs::read_to_string(stderr)?, cfg!(target_os = "macos"))?;
    let result = json!({
        "elapsed_seconds": elapsed, "exit_code": status.code(),
        "user_seconds": usage.user_seconds, "system_seconds": usage.system_seconds,
        "max_rss_bytes": usage.max_rss_bytes,
        "block_input_operations": usage.block_inputs, "block_output_operations": usage.block_outputs,
        "expected_output_hashes_match": true,
    });
    fs::write(
        run_dir.join("result.json"),
        serde_json::to_vec_pretty(&result)?,
    )?;
    Ok((elapsed, result))
}

pub(crate) fn run(mut options: Options) -> Result<()> {
    ensure!(
        cfg!(any(target_os = "macos", target_os = "linux")),
        "requires BSD or GNU time on macOS/Linux"
    );
    ensure!(
        !options.seed.as_os_str().is_empty()
            && options
                .seed
                .components()
                .all(|c| matches!(c, Component::Normal(_))),
        "seed must be a relative file path without parent traversal"
    );
    options.baseline = options.baseline.canonicalize()?;
    options.candidate = options.candidate.canonicalize()?;
    options.fixture = options.fixture.canonicalize()?;
    options.expected = options.expected.canonicalize()?;
    let output_name = options
        .output
        .file_name()
        .context("output requires a new directory name")?;
    let parent = options
        .output
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let output = parent.canonicalize()?.join(output_name);
    ensure!(
        !output.starts_with(&options.fixture) && !output.starts_with(&options.expected),
        "results must be outside both input trees"
    );
    let baseline_hash = hash_file(&options.baseline)?;
    let candidate_hash = hash_file(&options.candidate)?;
    let damaged = manifest(&options.fixture)?;
    let clean = manifest(&options.expected)?;
    ensure!(
        clean.contains_key(&options.seed) && clean.get(&options.seed) == damaged.get(&options.seed),
        "both trees must contain the same seed carrier"
    );
    ensure!(
        damaged != clean,
        "damaged fixture must differ from clean input"
    );
    ensure!(
        damaged.keys().all(|path| clean.contains_key(path)),
        "damaged tree contains an unexpected file"
    );
    for (path, hash) in &clean {
        if path
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("par2"))
        {
            ensure!(
                damaged.get(path) == Some(hash),
                "carrier differs between fixtures"
            );
        }
    }
    fs::create_dir(&output)?;
    options.output = output;
    fs::write(
        options.output.join("metadata.json"),
        serde_json::to_vec_pretty(&json!({
            "scope": "PAR2 CLI only; excludes downloads, repeated sessions and extraction",
            "started_at": chrono::Utc::now().to_rfc3339(),
            "os": std::env::consts::OS, "arch": std::env::consts::ARCH,
            "available_cpus": std::thread::available_parallelism()?.get(), "threads": options.threads.get(),
            "baseline": { "path": options.baseline, "sha256": baseline_hash.1 },
            "candidate": { "path": options.candidate, "sha256": candidate_hash.1 },
            "damaged": damaged, "clean": clean, "seed": options.seed,
            "warmups": 2, "minimum_pairs": 10, "maximum_pairs": 30,
            "upper_ratio_limit": 1.02, "cache_policy": "fresh copy hashed before each command",
        }))?,
    )?;
    let mut summaries = Vec::new();
    let mut passed = true;
    for workload in [
        Workload::CleanVerify,
        Workload::DamagedVerify,
        Workload::Repair,
    ] {
        let mut ratios = Vec::new();
        for pair in 0usize..32 {
            let mut elapsed = [0.0; 2];
            let order = if pair % 2 == 0 { [0, 1] } else { [1, 0] };
            for side in order {
                let (binary, name) = if side == 0 {
                    (&options.baseline, "baseline")
                } else {
                    (&options.candidate, "candidate")
                };
                let label = format!(
                    "{}-{}-{:02}-{name}",
                    workload.name(),
                    if pair < 2 { "warmup" } else { "sample" },
                    if pair < 2 { pair } else { pair - 2 }
                );
                elapsed[side] = trial(&options, binary, workload, &label, &damaged, &clean)?.0;
            }
            if pair < 2 {
                continue;
            }
            ratios.push(elapsed[1] / elapsed[0]);
            if ratios.len() == 10 || ratios.len() == 30 {
                let (ratio, lower, upper) = interval(&ratios)?;
                if upper <= 1.02 || lower > 1.02 || ratios.len() == 30 {
                    let accepted = upper <= 1.02;
                    passed &= accepted;
                    println!(
                        "{}: ratio {ratio:.4}, 95% interval [{lower:.4}, {upper:.4}], {} pairs, {}",
                        workload.name(),
                        ratios.len(),
                        if accepted { "pass" } else { "not accepted" }
                    );
                    summaries.push(json!({"workload": workload.name(), "pairs": ratios.len(),
                        "ratio": ratio, "lower_95": lower, "upper_95": upper, "accepted": accepted,
                        "paired_ratios": ratios}));
                    fs::write(
                        options.output.join("summary.json"),
                        serde_json::to_vec_pretty(&summaries)?,
                    )?;
                    break;
                }
            }
        }
    }
    ensure!(
        hash_file(&options.baseline)? == baseline_hash
            && hash_file(&options.candidate)? == candidate_hash,
        "a benchmark binary changed during measurement"
    );
    ensure!(
        manifest(&options.fixture)? == damaged && manifest(&options.expected)? == clean,
        "a fixture changed during measurement"
    );
    fs::write(
        options.output.join("completion.json"),
        serde_json::to_vec_pretty(&json!({
            "inputs_unchanged": true, "binaries_unchanged": true,
            "accepted": passed, "workloads_completed": summaries.len(),
        }))?,
    )?;
    ensure!(
        passed,
        "PAR2 CLI non-regression gate not established; see summary.json"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paired_interval_preserves_regressions_and_rejects_invalid_samples() {
        let (ratio, lower, upper) = interval(&[1.05; 10]).unwrap();
        assert!((ratio - 1.05).abs() < 1e-12);
        assert!(lower > 1.02 && upper > 1.02);
        assert!(interval(&[1.0; 9]).is_err());
        assert!(interval(&[f64::NAN; 10]).is_err());
        assert!(interval(&[0.0; 30]).is_err());
        let varied = [0.96, 1.04, 0.96, 1.04, 0.96, 1.04, 0.96, 1.04, 0.96, 1.04];
        let (_, lower, upper) = interval(&varied).unwrap();
        assert!(lower < 1.0 && upper > 1.02);
    }

    #[test]
    fn resource_parsers_preserve_units_and_require_all_counters() {
        let mac = parse_usage("real 1.00\nuser 0.75\nsys 0.10\n 8192 maximum resident set size\n 3 block input operations\n 7 block output operations\n", true).unwrap();
        let linux = parse_usage(
            "program log\nWEAVER_RESOURCE_USAGE 0.75 0.10 8 3 7\n",
            false,
        )
        .unwrap();
        assert_eq!(mac.max_rss_bytes, linux.max_rss_bytes);
        assert_eq!(mac.user_seconds, 0.75);
        assert_eq!(linux.system_seconds, 0.10);
        assert_eq!(mac.block_inputs, 3);
        assert_eq!(linux.block_outputs, 7);
        assert!(parse_usage("real 1.0", true).is_err());
        assert!(parse_usage("WEAVER_RESOURCE_USAGE 0.1", false).is_err());
    }
}
