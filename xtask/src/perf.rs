use crate::TaskContext;
use anyhow::{Context, Result, bail};
use chrono::Local;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};
use tempfile::{Builder, TempDir};

pub(crate) fn run_par2_x86(ctx: &TaskContext, args: Vec<String>) -> Result<()> {
    let options = parse_par2_x86_args(args)?;
    crate::require_command("perf")?;
    if !Path::new("/usr/bin/time").is_file() {
        bail!("/usr/bin/time is required");
    }
    if options.taskset_cpuset.is_some() {
        crate::require_command("taskset")?;
    }
    let arch = crate::run_capture(ctx.command("uname").arg("-m")).unwrap_or_default();
    match arch.trim() {
        "x86_64" | "amd64" => {}
        other => bail!("this command is intended for x86_64 hosts, got {other}"),
    }

    if !options.weaver_bin.is_file() {
        bail!(
            "weaver binary is not executable: {}",
            options.weaver_bin.display()
        );
    }
    if !options.fixture_dir.is_dir() {
        bail!(
            "fixture directory not found: {}",
            options.fixture_dir.display()
        );
    }
    if let Some(turbo) = &options.turbo_bin
        && !turbo.is_file()
    {
        bail!("turbo binary is not executable: {}", turbo.display());
    }

    let seed_par2 = options
        .seed_par2
        .clone()
        .unwrap_or(discover_seed_par2(&options.fixture_dir)?);
    if !seed_par2.is_file() {
        bail!("seed PAR2 file not found: {}", seed_par2.display());
    }

    let out_dir = options.out_dir.clone().unwrap_or_else(|| {
        ctx.path(&format!(
            "tmp/par2-x86/{}-{}",
            Local::now().format("%Y%m%d-%H%M%S"),
            options.label
        ))
    });
    fs::create_dir_all(&out_dir)?;

    let meta_file = out_dir.join("meta.txt");
    let summary_file = out_dir.join("summary.txt");
    let seed_basename = seed_par2
        .file_name()
        .and_then(|name| name.to_str())
        .context("invalid seed par2 basename")?
        .to_string();
    let fixture_seed = options.fixture_dir.join(&seed_basename);
    if !fixture_seed.is_file() {
        bail!(
            "fixture copy of seed PAR2 file not found: {}",
            fixture_seed.display()
        );
    }

    write_par2_metadata(ctx, &options, &seed_par2, &meta_file)?;
    fs::write(&summary_file, "")?;

    println!("Output dir: {}", out_dir.display());
    println!("Fixture: {}", options.fixture_dir.display());
    println!("Seed PAR2: {}", fixture_seed.display());
    println!("Weaver binary: {}", options.weaver_bin.display());
    if let Some(turbo) = &options.turbo_bin {
        println!("Turbo binary: {}", turbo.display());
    }

    let mut temp_dirs = Vec::<TempDir>::new();

    println!("Running Weaver verify on the original damaged fixture");
    let weaver_verify = build_weaver_cmd(&options, "verify", &fixture_seed);
    run_expected(
        ctx,
        1,
        &out_dir.join("weaver-verify.time"),
        &out_dir.join("weaver-verify.log"),
        &weaver_verify,
    )?;
    append_time_summary(
        &summary_file,
        "weaver verify",
        &out_dir.join("weaver-verify.time"),
    )?;

    if options.run_verify_perf {
        println!("Recording Weaver verify hot paths with perf");
        run_perf_record_expected(
            ctx,
            1,
            &out_dir.join("weaver-verify.perf.data"),
            &out_dir.join("weaver-verify.perf.log"),
            options.perf_freq,
            &weaver_verify,
        )?;
        let verify_report = out_dir.join("weaver-verify.perf.report.txt");
        perf_report(
            ctx,
            &out_dir.join("weaver-verify.perf.data"),
            &verify_report,
        )?;
        append_perf_symbol_summary(
            &summary_file,
            "weaver verify perf symbols",
            &verify_report,
            &["md5_multi_x86", "md5_asm"],
        )?;
    }

    let weaver_repair_dir = make_workdir(
        ctx,
        &options.fixture_dir,
        &out_dir,
        "weaver-repair",
        options.keep_workdirs,
        &mut temp_dirs,
    )?;
    let weaver_repair_seed = weaver_repair_dir.join(&seed_basename);
    let weaver_repair = build_weaver_cmd(&options, "repair", &weaver_repair_seed);
    println!("Running Weaver repair on a fresh copy");
    run_expected(
        ctx,
        0,
        &out_dir.join("weaver-repair.time"),
        &out_dir.join("weaver-repair.log"),
        &weaver_repair,
    )?;
    append_time_summary(
        &summary_file,
        "weaver repair",
        &out_dir.join("weaver-repair.time"),
    )?;

    let weaver_post_verify = build_weaver_cmd(&options, "verify", &weaver_repair_seed);
    run_expected(
        ctx,
        0,
        &out_dir.join("weaver-post-repair-verify.time"),
        &out_dir.join("weaver-post-repair-verify.log"),
        &weaver_post_verify,
    )?;
    append_time_summary(
        &summary_file,
        "weaver post-repair verify",
        &out_dir.join("weaver-post-repair-verify.time"),
    )?;

    if options.run_repair_perf_stat {
        let workdir = make_workdir(
            ctx,
            &options.fixture_dir,
            &out_dir,
            "weaver-repair-perf-stat",
            options.keep_workdirs,
            &mut temp_dirs,
        )?;
        let seed = workdir.join(&seed_basename);
        let command = build_weaver_cmd(&options, "repair", &seed);
        println!("Collecting Weaver repair perf stat counters");
        run_perf_stat_expected(
            ctx,
            0,
            &out_dir.join("weaver-repair.perf.stat.txt"),
            &out_dir.join("weaver-repair.perf.stat.log"),
            &command,
        )?;
    }

    if options.run_repair_perf_record {
        let workdir = make_workdir(
            ctx,
            &options.fixture_dir,
            &out_dir,
            "weaver-repair-perf-record",
            options.keep_workdirs,
            &mut temp_dirs,
        )?;
        let seed = workdir.join(&seed_basename);
        let command = build_weaver_cmd(&options, "repair", &seed);
        println!("Recording Weaver repair hot paths with perf");
        run_perf_record_expected(
            ctx,
            0,
            &out_dir.join("weaver-repair.perf.data"),
            &out_dir.join("weaver-repair.perf.log"),
            options.perf_freq,
            &command,
        )?;
        let report = out_dir.join("weaver-repair.perf.report.txt");
        perf_report(ctx, &out_dir.join("weaver-repair.perf.data"), &report)?;
        append_perf_symbol_summary(
            &summary_file,
            "weaver repair perf symbols",
            &report,
            &[
                "mul_acc_multi_region_gfni_avx2",
                "mul_acc_region_gfni_avx512",
                "mul_acc_region_avx512",
                "mul_acc_region_avx2",
                "mul_acc_region_ssse3",
            ],
        )?;
    }

    if let Some(turbo_bin) = &options.turbo_bin {
        println!("Running turbo verify on the original damaged fixture");
        let turbo_verify = build_turbo_cmd(&options, turbo_bin, "verify", &fixture_seed);
        run_expected(
            ctx,
            1,
            &out_dir.join("turbo-verify.time"),
            &out_dir.join("turbo-verify.log"),
            &turbo_verify,
        )?;
        append_time_summary(
            &summary_file,
            "turbo verify",
            &out_dir.join("turbo-verify.time"),
        )?;

        let turbo_repair_dir = make_workdir(
            ctx,
            &options.fixture_dir,
            &out_dir,
            "turbo-repair",
            options.keep_workdirs,
            &mut temp_dirs,
        )?;
        let turbo_repair_seed = turbo_repair_dir.join(&seed_basename);
        let turbo_repair = build_turbo_cmd(&options, turbo_bin, "repair", &turbo_repair_seed);
        println!("Running turbo repair on a fresh copy");
        run_expected(
            ctx,
            0,
            &out_dir.join("turbo-repair.time"),
            &out_dir.join("turbo-repair.log"),
            &turbo_repair,
        )?;
        append_time_summary(
            &summary_file,
            "turbo repair",
            &out_dir.join("turbo-repair.time"),
        )?;
    }

    let mut summary_tail = String::from("\noutputs\n");
    summary_tail.push_str(&format!("  meta:    {}\n", meta_file.display()));
    summary_tail.push_str(&format!("  summary: {}\n", summary_file.display()));
    if options.run_verify_perf {
        summary_tail.push_str(&format!(
            "  verify perf report: {}\n",
            out_dir.join("weaver-verify.perf.report.txt").display()
        ));
    }
    if options.run_repair_perf_stat {
        summary_tail.push_str(&format!(
            "  repair perf stat:   {}\n",
            out_dir.join("weaver-repair.perf.stat.txt").display()
        ));
    }
    if options.run_repair_perf_record {
        summary_tail.push_str(&format!(
            "  repair perf report: {}\n",
            out_dir.join("weaver-repair.perf.report.txt").display()
        ));
    }
    append_to_file(&summary_file, &summary_tail)?;

    println!("Done.");
    print!("{}", fs::read_to_string(&summary_file)?);
    Ok(())
}

pub(crate) fn run_real_download(ctx: &TaskContext, args: Vec<String>) -> Result<()> {
    crate::require_command("curl")?;

    let options = parse_real_download_args(args, ctx)?;
    if !options.env_file.is_file() {
        bail!("env file not found: {}", options.env_file.display());
    }
    if !options.nzb_file.is_file() {
        bail!("nzb file not found: {}", options.nzb_file.display());
    }
    if !options.weaver_bin.is_file() {
        bail!(
            "weaver binary not executable: {}",
            options.weaver_bin.display()
        );
    }

    let env_values = parse_env_file(&options.env_file)?;
    for key in [
        "NNTP_HOST",
        "NNTP_PORT",
        "NNTP_TLS",
        "NNTP_USERNAME",
        "NNTP_PASSWORD",
        "NNTP_CONNECTIONS",
    ] {
        if !env_values.contains_key(key) {
            bail!("{key} missing in env file");
        }
    }

    fs::create_dir_all(options.out_dir.join("data/intermediate"))?;
    fs::create_dir_all(options.out_dir.join("data/complete"))?;

    let config_path = options.out_dir.join("weaver.toml");
    let log_path = options.out_dir.join("weaver.log");
    let pid_path = options.out_dir.join("weaver.pid");
    let samples_path = options.out_dir.join("samples.jsonl");
    let summary_path = options.out_dir.join("summary.json");
    let perf_stat_path = options.out_dir.join("perf-stat.txt");
    let perf_data_path = options.out_dir.join("perf.data");
    let perf_report_path = options.out_dir.join("perf-report.txt");
    let submit_payload_path = options.out_dir.join("submit-payload.json");
    let query_payload_path = options.out_dir.join("query-payload.json");
    let cancel_payload_path = options.out_dir.join("cancel-payload.json");

    fs::write(
        &config_path,
        render_real_download_config(&options.out_dir, &env_values),
    )?;

    let log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)?;
    let log_err = log.try_clone()?;
    let mut child = ctx.command(&options.weaver_bin);
    child
        .env(
            "RUST_LOG",
            env::var("RUST_LOG").unwrap_or_else(|_| "warn".to_string()),
        )
        .args(["--config"])
        .arg(&config_path)
        .args(["serve", "--port", &options.port.to_string()])
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(log_err));
    let mut weaver = child.spawn()?;
    let weaver_pid = weaver.id();
    fs::write(&pid_path, format!("{weaver_pid}\n"))?;
    let guard = ChildGuard::new(&mut weaver);

    let graphql_url = format!("http://127.0.0.1:{}/graphql", options.port);
    wait_for_graphql(ctx, &graphql_url)?;

    let mut perf_child = None;
    if env::var("PERF_STAT").ok().as_deref() == Some("1") {
        let mut command = ctx.command("sudo");
        command
            .args([
                "-n",
                "perf",
                "stat",
                "-d",
                "-p",
                &weaver_pid.to_string(),
                "-o",
            ])
            .arg(&perf_stat_path)
            .args(["--", "sleep", &options.duration_sec.to_string()]);
        perf_child = Some(command.spawn()?);
    } else if env::var("PERF_RECORD").ok().as_deref() == Some("1") {
        let freq = env::var("PERF_FREQ").unwrap_or_else(|_| "999".to_string());
        let mut command = ctx.command("sudo");
        command
            .args([
                "-n",
                "perf",
                "record",
                "-F",
                &freq,
                "-g",
                "-p",
                &weaver_pid.to_string(),
                "-o",
            ])
            .arg(&perf_data_path)
            .args(["--", "sleep", &options.duration_sec.to_string()]);
        perf_child = Some(command.spawn()?);
    }

    let submit_payload = build_submit_payload(&options)?;
    fs::write(
        &submit_payload_path,
        serde_json::to_vec_pretty(&submit_payload)?,
    )?;
    let submit_response = graphql_post(ctx, &graphql_url, &submit_payload)?;
    let job_id = submit_response
        .get("data")
        .and_then(|data| data.get("submitNzb"))
        .and_then(|submit| submit.get("item"))
        .and_then(|item| item.get("id"))
        .and_then(Value::as_i64)
        .context("submit failed: missing job id")?;

    let start = Instant::now();
    let mut last_print = Duration::ZERO;
    let mut terminal_status;
    let mut samples = Vec::<Value>::new();
    let sample_interval = Duration::from_millis(options.sample_ms);

    loop {
        let elapsed = start.elapsed();
        let payload = json!({
            "query": "query($id: Int!) { job(id: $id) { id status progress health totalBytes downloadedBytes optionalRecoveryBytes optionalRecoveryDownloadedBytes failedBytes error } metrics { currentDownloadSpeed articlesPerSec decodeRateMbps bytesDownloaded bytesDecoded bytesCommitted segmentsDownloaded segmentsDecoded segmentsCommitted } }",
            "variables": { "id": job_id },
        });
        fs::write(&query_payload_path, serde_json::to_vec(&payload)?)?;
        let snapshot = graphql_post(ctx, &graphql_url, &payload)?;
        let elapsed_ms = elapsed.as_millis() as u64;
        let mut sampled = snapshot
            .get("data")
            .cloned()
            .context("missing data in job query response")?;
        sampled["elapsed_ms"] = json!(elapsed_ms);
        append_json_line(&samples_path, &sampled)?;
        samples.push(sampled.clone());

        terminal_status = sampled
            .get("job")
            .and_then(|job| job.get("status"))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();

        if elapsed.saturating_sub(last_print) >= Duration::from_secs(5) {
            last_print = elapsed;
            let progress = sampled
                .get("job")
                .and_then(|job| job.get("progress"))
                .and_then(Value::as_f64)
                .unwrap_or_default()
                * 100.0;
            let speed = sampled
                .get("metrics")
                .and_then(|metrics| metrics.get("currentDownloadSpeed"))
                .and_then(Value::as_i64)
                .unwrap_or_default();
            let health = sampled
                .get("job")
                .and_then(|job| job.get("health"))
                .and_then(Value::as_f64)
                .unwrap_or_default()
                / 10.0;
            println!(
                "status={} elapsed_ms={} progress={} speed_bps={} health={}",
                terminal_status, elapsed_ms, progress, speed, health
            );
        }

        if matches!(terminal_status.as_str(), "COMPLETE" | "FAILED") {
            break;
        }

        if options.duration_sec != 0 && elapsed >= Duration::from_secs(options.duration_sec) {
            let payload = json!({
                "query": "mutation($id: Int!) { cancelJob(id: $id) }",
                "variables": { "id": job_id },
            });
            fs::write(&cancel_payload_path, serde_json::to_vec(&payload)?)?;
            let _ = graphql_post(ctx, &graphql_url, &payload);
            terminal_status = "CANCELLED".to_string();
            break;
        }

        std::thread::sleep(sample_interval);
    }

    if let Some(mut child) = perf_child {
        let _ = child.wait();
        if perf_data_path.is_file() {
            let mut report = ctx.command("sudo");
            report
                .args(["-n", "perf", "report", "--stdio", "-i"])
                .arg(&perf_data_path);
            if let Ok(output) = crate::run_capture(&mut report) {
                fs::write(&perf_report_path, output)?;
            }
        }
    }

    let summary = summarize_real_download(job_id, &terminal_status, &samples)?;
    fs::write(&summary_path, serde_json::to_vec_pretty(&summary)?)?;
    println!("{}", serde_json::to_string_pretty(&summary)?);
    println!("artifacts: {}", options.out_dir.display());

    drop(guard);
    Ok(())
}

#[derive(Clone)]
struct Par2X86Options {
    weaver_bin: PathBuf,
    turbo_bin: Option<PathBuf>,
    fixture_dir: PathBuf,
    seed_par2: Option<PathBuf>,
    out_dir: Option<PathBuf>,
    threads: usize,
    taskset_cpuset: Option<String>,
    label: String,
    perf_freq: u32,
    run_verify_perf: bool,
    run_repair_perf_stat: bool,
    run_repair_perf_record: bool,
    keep_workdirs: bool,
}

fn parse_par2_x86_args(args: Vec<String>) -> Result<Par2X86Options> {
    let mut options = Par2X86Options {
        weaver_bin: PathBuf::new(),
        turbo_bin: None,
        fixture_dir: PathBuf::new(),
        seed_par2: None,
        out_dir: None,
        threads: cpu_count()?,
        taskset_cpuset: None,
        label: "x86-par2".to_string(),
        perf_freq: 999,
        run_verify_perf: true,
        run_repair_perf_stat: true,
        run_repair_perf_record: true,
        keep_workdirs: false,
    };

    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--weaver" => options.weaver_bin = PathBuf::from(require_value(&mut iter, "--weaver")?),
            "--turbo" => {
                options.turbo_bin = Some(PathBuf::from(require_value(&mut iter, "--turbo")?))
            }
            "--fixture" => {
                options.fixture_dir = PathBuf::from(require_value(&mut iter, "--fixture")?)
            }
            "--seed-par2" => {
                options.seed_par2 = Some(PathBuf::from(require_value(&mut iter, "--seed-par2")?))
            }
            "--output-dir" => {
                options.out_dir = Some(PathBuf::from(require_value(&mut iter, "--output-dir")?))
            }
            "--threads" => options.threads = require_value(&mut iter, "--threads")?.parse()?,
            "--taskset" => options.taskset_cpuset = Some(require_value(&mut iter, "--taskset")?),
            "--label" => options.label = require_value(&mut iter, "--label")?,
            "--perf-freq" => {
                options.perf_freq = require_value(&mut iter, "--perf-freq")?.parse()?
            }
            "--no-verify-perf" => options.run_verify_perf = false,
            "--no-repair-perf-stat" => options.run_repair_perf_stat = false,
            "--no-repair-perf-record" => options.run_repair_perf_record = false,
            "--keep-workdirs" => options.keep_workdirs = true,
            "-h" | "--help" => {
                print_par2_help();
                std::process::exit(0);
            }
            _ => bail!("unknown argument: {arg}"),
        }
    }

    if options.weaver_bin.as_os_str().is_empty() {
        bail!("--weaver is required");
    }
    if options.fixture_dir.as_os_str().is_empty() {
        bail!("--fixture is required");
    }
    Ok(options)
}

fn print_par2_help() {
    println!(
        "Run Weaver PAR2 verify/repair hot-path profiling on an x86 Linux box.\n\nUsage:\n  cargo xtask perf par2-x86 --weaver /path/to/weaver --fixture /path/to/fixture_dir [--seed-par2 /path/to/main.par2] [--turbo /path/to/par2] [--output-dir /path/to/out] [--threads 16] [--taskset 0-15] [--label skylake-repair] [--perf-freq 999] [--no-verify-perf] [--no-repair-perf-stat] [--no-repair-perf-record] [--keep-workdirs]"
    );
}

#[derive(Clone)]
struct PreparedCommand {
    argv: Vec<String>,
}

fn build_weaver_cmd(
    options: &Par2X86Options,
    subcommand: &str,
    seed_path: &Path,
) -> PreparedCommand {
    let mut argv = vec![
        "env".to_string(),
        format!("RAYON_NUM_THREADS={}", options.threads),
    ];
    if let Some(cpuset) = &options.taskset_cpuset {
        argv.extend(["taskset".to_string(), "-c".to_string(), cpuset.clone()]);
    }
    argv.push(options.weaver_bin.display().to_string());
    argv.extend(["par2".to_string(), subcommand.to_string()]);
    if subcommand == "repair" {
        argv.extend([
            "--working-dir".to_string(),
            seed_path
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .display()
                .to_string(),
        ]);
    }
    argv.push(seed_path.display().to_string());
    PreparedCommand { argv }
}

fn build_turbo_cmd(
    options: &Par2X86Options,
    turbo_bin: &Path,
    subcommand: &str,
    seed_path: &Path,
) -> PreparedCommand {
    let mut argv = Vec::new();
    if let Some(cpuset) = &options.taskset_cpuset {
        argv.extend(["taskset".to_string(), "-c".to_string(), cpuset.clone()]);
    }
    argv.push(turbo_bin.display().to_string());
    argv.push(subcommand.to_string());
    if subcommand == "repair" {
        argv.push(format!("-t{}", options.threads));
    }
    argv.push(seed_path.display().to_string());
    PreparedCommand { argv }
}

fn run_expected(
    ctx: &TaskContext,
    expected_status: i32,
    time_file: &Path,
    log_file: &Path,
    prepared: &PreparedCommand,
) -> Result<()> {
    let stdout = File::create(log_file)?;
    let stderr = stdout.try_clone()?;
    let mut command = ctx.command("/usr/bin/time");
    command
        .args(["-v", "-o"])
        .arg(time_file)
        .args(&prepared.argv)
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));
    let status = command.status()?;
    fs::write(
        time_file.with_extension("time.status"),
        format!("{}\n", status.code().unwrap_or(-1)),
    )?;
    if status.code().unwrap_or(-1) != expected_status {
        bail!(
            "command failed with exit {} (expected {expected_status}); see {}",
            status.code().unwrap_or(-1),
            log_file.display()
        );
    }
    Ok(())
}

fn run_perf_stat_expected(
    ctx: &TaskContext,
    expected_status: i32,
    stat_file: &Path,
    log_file: &Path,
    prepared: &PreparedCommand,
) -> Result<()> {
    let stdout = File::create(log_file)?;
    let stderr = stdout.try_clone()?;
    let mut command = ctx.command("perf");
    command
        .args(["stat", "-d", "-o"])
        .arg(stat_file)
        .args(["--"])
        .args(&prepared.argv)
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));
    let status = command.status()?;
    fs::write(
        stat_file.with_extension("txt.status"),
        format!("{}\n", status.code().unwrap_or(-1)),
    )?;
    if status.code().unwrap_or(-1) != expected_status {
        bail!(
            "perf stat command failed with exit {} (expected {expected_status}); see {}",
            status.code().unwrap_or(-1),
            log_file.display()
        );
    }
    Ok(())
}

fn run_perf_record_expected(
    ctx: &TaskContext,
    expected_status: i32,
    perf_data: &Path,
    log_file: &Path,
    perf_freq: u32,
    prepared: &PreparedCommand,
) -> Result<()> {
    let stdout = File::create(log_file)?;
    let stderr = stdout.try_clone()?;
    let mut command = ctx.command("perf");
    command
        .args(["record", "-F", &perf_freq.to_string(), "-g", "-o"])
        .arg(perf_data)
        .args(["--"])
        .args(&prepared.argv)
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));
    let status = command.status()?;
    fs::write(
        perf_data.with_extension("data.status"),
        format!("{}\n", status.code().unwrap_or(-1)),
    )?;
    if status.code().unwrap_or(-1) != expected_status {
        bail!(
            "perf record command failed with exit {} (expected {expected_status}); see {}",
            status.code().unwrap_or(-1),
            log_file.display()
        );
    }
    Ok(())
}

fn perf_report(ctx: &TaskContext, perf_data: &Path, report_file: &Path) -> Result<()> {
    let output = File::create(report_file)?;
    let mut command = ctx.command("perf");
    command
        .args(["report", "--stdio", "-i"])
        .arg(perf_data)
        .stdout(Stdio::from(output));
    crate::run_checked(&mut command)
}

fn write_par2_metadata(
    ctx: &TaskContext,
    options: &Par2X86Options,
    seed_par2: &Path,
    meta_file: &Path,
) -> Result<()> {
    let mut meta = String::new();
    meta.push_str(&format!("label={}\n", options.label));
    meta.push_str(&format!("started_at={}\n", Local::now().to_rfc3339()));
    meta.push_str(&format!("fixture_dir={}\n", options.fixture_dir.display()));
    meta.push_str(&format!("seed_par2={}\n", seed_par2.display()));
    meta.push_str(&format!("weaver_bin={}\n", options.weaver_bin.display()));
    meta.push_str(&format!(
        "turbo_bin={}\n",
        options
            .turbo_bin
            .as_ref()
            .map(|path| path.display().to_string())
            .unwrap_or_default()
    ));
    meta.push_str(&format!("threads={}\n", options.threads));
    meta.push_str(&format!(
        "taskset={}\n",
        options.taskset_cpuset.clone().unwrap_or_default()
    ));
    meta.push_str(&format!("perf_freq={}\n\n", options.perf_freq));

    if let Ok(uname) = crate::run_capture(ctx.command("uname").arg("-a")) {
        meta.push_str("uname:\n");
        meta.push_str(&uname);
        meta.push('\n');
    }
    if crate::command_available("lscpu")? {
        let mut lscpu_command = ctx.command("lscpu");
        if let Ok(lscpu) = crate::run_capture(&mut lscpu_command) {
            meta.push_str("lscpu:\n");
            meta.push_str(&lscpu);
            meta.push('\n');
        }
    }
    if let Ok(version) = crate::run_capture(ctx.command("perf").arg("--version")) {
        meta.push_str("perf:\n");
        meta.push_str(&version);
        meta.push('\n');
    }
    meta.push_str("weaver file:\n");
    meta.push_str(&file_description(ctx, &options.weaver_bin));
    meta.push_str(&format!("{}\n\n", sha256_file(&options.weaver_bin)?));
    if let Some(turbo) = &options.turbo_bin {
        meta.push_str("turbo file:\n");
        meta.push_str(&file_description(ctx, turbo));
        meta.push_str(&format!("{}\n\n", sha256_file(turbo)?));
    }
    fs::write(meta_file, meta)?;
    Ok(())
}

fn file_description(ctx: &TaskContext, path: &Path) -> String {
    if crate::command_available("file").unwrap_or(false) {
        let mut command = ctx.command("file");
        command.arg(path);
        crate::run_capture(&mut command).unwrap_or_default()
    } else {
        String::new()
    }
}

fn append_time_summary(summary_file: &Path, label: &str, time_file: &Path) -> Result<()> {
    let time = fs::read_to_string(time_file)?;
    let fields = parse_colon_fields(&time);
    append_to_file(
        summary_file,
        &format!(
            "{label}\n  wall: {}\n  user: {}\n  sys:  {}\n  rss:  {} kB\n",
            fields
                .get("Elapsed (wall clock) time (h:mm:ss or m:ss)")
                .cloned()
                .unwrap_or_default(),
            fields
                .get("User time (seconds)")
                .cloned()
                .unwrap_or_default(),
            fields
                .get("System time (seconds)")
                .cloned()
                .unwrap_or_default(),
            fields
                .get("Maximum resident set size (kbytes)")
                .cloned()
                .unwrap_or_default()
        ),
    )
}

fn append_perf_symbol_summary(
    summary_file: &Path,
    label: &str,
    report_file: &Path,
    symbols: &[&str],
) -> Result<()> {
    let report = fs::read_to_string(report_file).unwrap_or_default();
    let found = symbols
        .iter()
        .filter(|symbol| report.contains(**symbol))
        .copied()
        .collect::<Vec<_>>();
    let mut text = format!("{label}\n");
    if found.is_empty() {
        text.push_str("  found: none of the expected symbols\n");
    } else {
        for symbol in found {
            text.push_str(&format!("  found: {symbol}\n"));
        }
    }
    append_to_file(summary_file, &text)
}

fn append_to_file(path: &Path, text: &str) -> Result<()> {
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    file.write_all(text.as_bytes())?;
    Ok(())
}

fn parse_colon_fields(text: &str) -> BTreeMap<String, String> {
    text.lines()
        .filter_map(|line| line.split_once(": "))
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
}

fn cpu_count() -> Result<usize> {
    if crate::command_available("nproc")? {
        let mut command = Command::new("nproc");
        return Ok(command
            .output()?
            .stdout
            .iter()
            .map(|b| *b as char)
            .collect::<String>()
            .trim()
            .parse()?);
    }
    let output = Command::new("getconf").arg("_NPROCESSORS_ONLN").output()?;
    Ok(String::from_utf8_lossy(&output.stdout).trim().parse()?)
}

fn discover_seed_par2(fixture_dir: &Path) -> Result<PathBuf> {
    let mut primary = Vec::new();
    let mut all = Vec::new();
    for entry in fs::read_dir(fixture_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) == Some("par2") {
            let name = path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or_default();
            all.push(path.clone());
            if !name.contains(".vol") {
                primary.push(path);
            }
        }
    }
    all.sort();
    primary.sort();
    match (primary.len(), all.len()) {
        (1, _) => Ok(primary.remove(0)),
        (_, 1) => Ok(all.remove(0)),
        _ => bail!(
            "could not uniquely determine the seed PAR2 file in {}; pass --seed-par2",
            fixture_dir.display()
        ),
    }
}

fn make_workdir(
    ctx: &TaskContext,
    fixture_dir: &Path,
    out_dir: &Path,
    prefix: &str,
    keep_workdirs: bool,
    temp_dirs: &mut Vec<TempDir>,
) -> Result<PathBuf> {
    let temp_dir = Builder::new().prefix(prefix).tempdir_in(out_dir)?;
    copy_tree(ctx, fixture_dir, temp_dir.path())?;
    if keep_workdirs {
        Ok(temp_dir.keep())
    } else {
        let path = temp_dir.path().to_path_buf();
        temp_dirs.push(temp_dir);
        Ok(path)
    }
}

fn copy_tree(ctx: &TaskContext, src: &Path, dst: &Path) -> Result<()> {
    fs::create_dir_all(dst)?;
    let mut first = ctx.command("cp");
    first
        .args(["-a", "--reflink=auto", "--sparse=always"])
        .arg(src.join("."))
        .arg(dst);
    if crate::run_status(&mut first)?.success() {
        return Ok(());
    }
    let mut fallback = ctx.command("cp");
    fallback.args(["-a"]).arg(src.join(".")).arg(dst);
    crate::run_checked(&mut fallback)
}

#[derive(Clone)]
struct RealDownloadOptions {
    env_file: PathBuf,
    nzb_file: PathBuf,
    out_dir: PathBuf,
    weaver_bin: PathBuf,
    port: u16,
    duration_sec: u64,
    sample_ms: u64,
    category: String,
    job_password: String,
}

fn parse_real_download_args(args: Vec<String>, ctx: &TaskContext) -> Result<RealDownloadOptions> {
    let mut env_file = None::<PathBuf>;
    let mut nzb_file = None::<PathBuf>;
    let mut out_dir = None::<PathBuf>;
    let mut weaver_bin = ctx.path("target/release/weaver");
    let mut port = 19090u16;
    let mut duration_sec = 180u64;
    let mut sample_ms = 500u64;
    let mut category = "movies".to_string();
    let mut job_password = String::new();

    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--env" => env_file = Some(PathBuf::from(require_value(&mut iter, "--env")?)),
            "--nzb" => nzb_file = Some(PathBuf::from(require_value(&mut iter, "--nzb")?)),
            "--out-dir" => out_dir = Some(PathBuf::from(require_value(&mut iter, "--out-dir")?)),
            "--weaver" => weaver_bin = PathBuf::from(require_value(&mut iter, "--weaver")?),
            "--port" => port = require_value(&mut iter, "--port")?.parse()?,
            "--duration-sec" => {
                duration_sec = require_value(&mut iter, "--duration-sec")?.parse()?
            }
            "--sample-ms" => sample_ms = require_value(&mut iter, "--sample-ms")?.parse()?,
            "--category" => category = require_value(&mut iter, "--category")?,
            "--job-password" => job_password = require_value(&mut iter, "--job-password")?,
            "-h" | "--help" => {
                print_real_download_help();
                std::process::exit(0);
            }
            _ => bail!("Unknown argument: {arg}"),
        }
    }

    let env_file = env_file.context("--env is required")?;
    let nzb_file = nzb_file.context("--nzb is required")?;
    let out_dir = out_dir.unwrap_or_else(|| {
        ctx.path(&format!(
            "tmp/real-download-{}",
            Local::now().format("%Y%m%d-%H%M%S")
        ))
    });

    Ok(RealDownloadOptions {
        env_file,
        nzb_file,
        out_dir,
        weaver_bin,
        port,
        duration_sec,
        sample_ms,
        category,
        job_password,
    })
}

fn print_real_download_help() {
    println!(
        "Usage: cargo xtask perf real-download --env /path/to/nntp.env --nzb /path/to/file.nzb [--out-dir PATH] [--weaver PATH] [--port N] [--duration-sec N] [--sample-ms N] [--category NAME] [--job-password VALUE]"
    );
}

fn require_value(iter: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    iter.next()
        .with_context(|| format!("{flag} requires a value"))
}

fn parse_env_file(path: &Path) -> Result<BTreeMap<String, String>> {
    let text = fs::read_to_string(path)?;
    let mut values = BTreeMap::new();
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let Some((key, value)) = trimmed.split_once('=') else {
            continue;
        };
        values.insert(
            key.trim().to_string(),
            value.trim().trim_matches('"').to_string(),
        );
    }
    Ok(values)
}

fn render_real_download_config(out_dir: &Path, env_values: &BTreeMap<String, String>) -> String {
    let data_dir = out_dir.join("data");
    format!(
        "data_dir = \"{}\"\nintermediate_dir = \"{}\"\ncomplete_dir = \"{}\"\ncleanup_after_extract = true\nmax_retries = 3\n\n[[servers]]\nid = 1\nhost = \"{}\"\nport = {}\ntls = {}\nusername = \"{}\"\npassword = \"{}\"\nconnections = {}\nactive = true\npriority = 0\n\n[[categories]]\nid = 1\nname = \"movies\"\n\n[[categories]]\nid = 2\nname = \"tv\"\n",
        escape_toml_path(&data_dir),
        escape_toml_path(&data_dir.join("intermediate")),
        escape_toml_path(&data_dir.join("complete")),
        escape_toml_string(env_values["NNTP_HOST"].as_str()),
        env_values["NNTP_PORT"],
        env_values["NNTP_TLS"].trim().to_lowercase(),
        escape_toml_string(env_values["NNTP_USERNAME"].as_str()),
        escape_toml_string(env_values["NNTP_PASSWORD"].as_str()),
        env_values["NNTP_CONNECTIONS"],
    )
}

fn escape_toml_path(path: &Path) -> String {
    escape_toml_string(&path.display().to_string())
}

fn escape_toml_string(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

fn wait_for_graphql(ctx: &TaskContext, graphql_url: &str) -> Result<()> {
    let payload = json!({ "query": "{ version }" });
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut last_error = String::new();
    while Instant::now() < deadline {
        match graphql_post(ctx, graphql_url, &payload) {
            Ok(_) => return Ok(()),
            Err(error) => last_error = error.to_string(),
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    bail!("timed out waiting for weaver graphql at {graphql_url}: {last_error}");
}

fn graphql_post(ctx: &TaskContext, graphql_url: &str, payload: &Value) -> Result<Value> {
    let encoded = serde_json::to_string(payload)?;
    let output = ctx
        .command("curl")
        .args([
            "-fsS",
            "-H",
            "Content-Type: application/json",
            "--data-binary",
            &encoded,
            graphql_url,
        ])
        .output()?;
    if !output.status.success() {
        bail!("{}", String::from_utf8_lossy(&output.stderr));
    }
    let response: Value = serde_json::from_slice(&output.stdout)?;
    Ok(response)
}

fn build_submit_payload(options: &RealDownloadOptions) -> Result<Value> {
    use base64::Engine;
    let nzb_bytes = fs::read(&options.nzb_file)?;
    let nzb_base64 = base64::engine::general_purpose::STANDARD.encode(nzb_bytes);
    let mut input = json!({
        "nzbBase64": nzb_base64,
        "filename": options
            .nzb_file
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("job.nzb"),
        "category": options.category,
    });
    if !options.job_password.is_empty() {
        input["password"] = json!(options.job_password);
    }
    Ok(json!({
        "query": "mutation($input: SubmitNzbInput!) { submitNzb(input: $input) { accepted item { id state name } } }",
        "variables": { "input": input },
    }))
}

fn append_json_line(path: &Path, value: &Value) -> Result<()> {
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    serde_json::to_writer(&mut file, value)?;
    file.write_all(b"\n")?;
    Ok(())
}

fn summarize_real_download(job_id: i64, terminal_status: &str, samples: &[Value]) -> Result<Value> {
    let first = samples.first().context("no samples captured")?;
    let last = samples.last().context("no samples captured")?;
    let mut first_byte_ms = None::<u64>;
    for sample in samples {
        let downloaded = sample
            .get("job")
            .and_then(|job| job.get("downloadedBytes"))
            .and_then(Value::as_u64)
            .unwrap_or_default();
        let failed = sample
            .get("job")
            .and_then(|job| job.get("failedBytes"))
            .and_then(Value::as_u64)
            .unwrap_or_default();
        if downloaded + failed > 0 {
            first_byte_ms = sample.get("elapsed_ms").and_then(Value::as_u64);
            break;
        }
    }
    let peak_download_speed = samples
        .iter()
        .filter_map(|sample| {
            sample
                .get("metrics")
                .and_then(|metrics| metrics.get("currentDownloadSpeed"))
                .and_then(Value::as_u64)
        })
        .max()
        .unwrap_or_default();
    let peak_articles_per_sec = samples
        .iter()
        .filter_map(|sample| {
            sample
                .get("metrics")
                .and_then(|metrics| metrics.get("articlesPerSec"))
                .and_then(Value::as_f64)
        })
        .fold(0.0_f64, f64::max);
    let peak_decode_rate = samples
        .iter()
        .filter_map(|sample| {
            sample
                .get("metrics")
                .and_then(|metrics| metrics.get("decodeRateMbps"))
                .and_then(Value::as_f64)
        })
        .fold(0.0_f64, f64::max);
    let lowest_health = samples
        .iter()
        .filter_map(|sample| {
            sample
                .get("job")
                .and_then(|job| job.get("health"))
                .and_then(Value::as_i64)
        })
        .min()
        .unwrap_or_default();
    Ok(json!({
        "job_id": job_id,
        "status": terminal_status,
        "duration_ms": last.get("elapsed_ms").and_then(Value::as_u64).unwrap_or_default(),
        "time_to_first_byte_ms": first_byte_ms,
        "peak_download_speed": peak_download_speed,
        "peak_articles_per_sec": peak_articles_per_sec,
        "peak_decode_rate_mbps": peak_decode_rate,
        "lowest_health": lowest_health,
        "final_total_bytes": last.get("job").and_then(|job| job.get("totalBytes")).and_then(Value::as_u64).unwrap_or_default(),
        "final_downloaded_bytes": last.get("job").and_then(|job| job.get("downloadedBytes")).and_then(Value::as_u64).unwrap_or_default(),
        "final_failed_bytes": last.get("job").and_then(|job| job.get("failedBytes")).and_then(Value::as_u64).unwrap_or_default(),
        "sample_count": samples.len(),
        "started_status": first.get("job").and_then(|job| job.get("status")).and_then(Value::as_str).unwrap_or_default(),
    }))
}

fn sha256_file(path: &Path) -> Result<String> {
    let bytes = fs::read(path)?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

struct ChildGuard<'a> {
    child: &'a mut std::process::Child,
}

impl<'a> ChildGuard<'a> {
    fn new(child: &'a mut std::process::Child) -> Self {
        Self { child }
    }
}

impl Drop for ChildGuard<'_> {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
