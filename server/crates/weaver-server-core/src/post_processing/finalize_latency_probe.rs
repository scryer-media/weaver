//! Measurement harness for post-processing finalize latency.
//!
//! The field symptom is the `ui-post-processing` e2e flow: a ~100 ms extension
//! attempt whose *run* takes seconds to reach a terminal state. The prevailing
//! explanation was contention on the shared serialized DB writer, but the
//! reported numbers include 4.6–16.5 s on an **idle** machine, which contention
//! with a live download cannot produce. This harness settles it by timing the
//! real persistence calls `execute_steps` makes, on a real file-backed SQLite
//! database in WAL mode, one factor at a time.
//!
//! It is `#[ignore]`d: it is a measurement, not an assertion, and it is slow.
//!
//! ```text
//! cargo test -p weaver-server-core --release finalize_latency -- --ignored --nocapture
//! ```
//!
//! Knobs (all optional):
//! - `PROBE_REPEATS`      samples per scenario (default 25)
//! - `PROBE_LINES`        output lines per attempt (default 4000, ~the field case)
//! - `PROBE_HISTORY_ROWS` job_history rows to seed for the scan scenarios (default 5000)

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use super::model::{
    AttemptStatus, ExtensionSelection, PipelineOutcome, PostProcessingSummary, RunStatus,
    SubmissionPlanSelection,
};
use super::persistence::{LogStream, TerminalIntent};
use crate::persistence::Database;

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(default)
}

/// Wall-clock samples for one scenario, reported as a distribution rather than
/// a single number — the whole reason the previous n=2 comparison was
/// inconclusive.
struct Samples {
    label: &'static str,
    values: Vec<Duration>,
}

impl Samples {
    fn new(label: &'static str) -> Self {
        Self {
            label,
            values: Vec::new(),
        }
    }

    fn push(&mut self, value: Duration) {
        self.values.push(value);
    }

    fn report(&self) -> String {
        if self.values.is_empty() {
            return format!("{:<28} (no samples)", self.label);
        }
        let mut sorted = self.values.clone();
        sorted.sort();
        let at = |q: f64| sorted[((sorted.len() - 1) as f64 * q).round() as usize];
        let total: Duration = sorted.iter().sum();
        format!(
            "{:<28} n={:<4} mean={:>9.2}ms p50={:>9.2}ms p90={:>9.2}ms max={:>9.2}ms",
            self.label,
            sorted.len(),
            total.as_secs_f64() * 1000.0 / sorted.len() as f64,
            at(0.50).as_secs_f64() * 1000.0,
            at(0.90).as_secs_f64() * 1000.0,
            sorted.last().unwrap().as_secs_f64() * 1000.0,
        )
    }
}

/// Everything `execute_steps` needs to reach the finalize calls, prepared once.
struct Harness {
    db: Database,
    _dir: tempfile::TempDir,
    manifest: super::model::ExtensionManifest,
    plan: super::model::FrozenPlan,
    next_job_id: u64,
}

impl Harness {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        // A real file so the WAL, page cache and fsync behaviour are the
        // deployed ones; `open_in_memory` skips WAL entirely.
        let db = Database::open(&dir.path().join("weaver.db")).unwrap();
        let manifest = super::persistence_tests::probe_manifest();
        db.upsert_discovered_extension(&manifest, Some("/data/scripts/example"), 10)
            .unwrap();
        let revision = manifest.revision();
        db.approve_extension_revision(
            revision.extension_id(),
            revision.revision_id(),
            "/data/managed/blake3/aaaa",
            20,
        )
        .unwrap();
        let selection = SubmissionPlanSelection::extensions(vec![ExtensionSelection::pinned(
            revision.extension_id().clone(),
            revision.revision_id().clone(),
        )])
        .unwrap();
        let plan = db
            .resolve_post_processing_plan(Some(&selection), None)
            .unwrap();
        Self {
            db,
            _dir: dir,
            manifest,
            plan,
            next_job_id: 1,
        }
    }

    /// Seed `count` archived jobs, each carrying a distinct
    /// `post_processing_run_id`, so the finalize-time `UPDATE job_history ...
    /// WHERE post_processing_run_id = ?` has a realistically sized table to
    /// scan. That column has no index (migration 0037).
    fn seed_history(&mut self, count: usize) {
        for index in 0..count {
            let job_id = 1_000_000 + index as u64;
            let mut row = super::persistence_tests::probe_history_row(job_id);
            row.name = format!("seeded history job {job_id}");
            self.db.archive_job(crate::jobs::JobId(job_id), &row).ok();
        }
    }

    /// One attempt driven to the point the extension process has just exited —
    /// i.e. the state `execute_steps` is in when it starts persisting output.
    fn running_attempt(&mut self) -> (super::model::RunId, super::model::AttemptId) {
        let job_id = self.next_job_id;
        self.next_job_id += 1;
        let timestamp = 1_000 + job_id as i64 * 10;
        let run_id = self
            .db
            .create_post_processing_run(
                job_id,
                &self.plan,
                &PipelineOutcome::Succeeded,
                TerminalIntent::Complete,
                None,
                timestamp,
            )
            .unwrap();
        let attempt_id = self
            .db
            .enqueue_post_processing_attempt(
                &run_id,
                &self.plan.steps()[0],
                self.manifest.adapter(),
                None,
                timestamp + 1,
            )
            .unwrap();
        assert!(
            self.db
                .mark_post_processing_attempt_starting(
                    &attempt_id,
                    &serde_json::json!({"program": "process.sh"}),
                    "/work/job",
                    timestamp + 2,
                )
                .unwrap()
        );
        assert!(
            self.db
                .mark_post_processing_attempt_running(&attempt_id)
                .unwrap()
        );
        (run_id, attempt_id)
    }
}

/// The exact sequence `execute_steps` runs once the extension process exits.
struct FinalizePhases {
    append_logs: Duration,
    finish_attempt: Duration,
    finish_run: Duration,
}

fn finalize_once(
    db: &Database,
    run_id: &super::model::RunId,
    attempt_id: &super::model::AttemptId,
    lines: &[(LogStream, Vec<u8>)],
) -> FinalizePhases {
    let started = Instant::now();
    db.append_post_processing_logs(attempt_id, lines, 5_000)
        .unwrap();
    let append_logs = started.elapsed();

    let started = Instant::now();
    db.finish_post_processing_attempt(
        attempt_id,
        AttemptStatus::Succeeded,
        Some(0),
        None,
        None,
        false,
        5_001,
    )
    .unwrap();
    let finish_attempt = started.elapsed();

    let started = Instant::now();
    db.finish_post_processing_run(
        run_id,
        RunStatus::Succeeded,
        PostProcessingSummary::Succeeded,
        5_002,
    )
    .unwrap();
    let finish_run = started.elapsed();

    FinalizePhases {
        append_logs,
        finish_attempt,
        finish_run,
    }
}

fn output_lines(count: usize) -> Vec<(LogStream, Vec<u8>)> {
    // 120 bytes/line is a typical chatty-extension line; 4000 of them is ~470 KB,
    // under the 4 MiB retention cap, so the retention DELETE loop stays out of
    // the measurement unless a scenario asks for it.
    (0..count)
        .map(|index| {
            (
                LogStream::Stdout,
                format!(
                    "[{index:06}] extension progress line padded to a realistic width {:-<60}",
                    ""
                )
                .into_bytes(),
            )
        })
        .collect()
}

/// Background writes through the ordered writer queue, standing in for the live
/// download pipeline (`try_queue_write("update_active_job")` is what the
/// orchestrator actually calls while a job runs).
fn spawn_writer_load(db: &Database, stop: Arc<AtomicBool>) -> std::thread::JoinHandle<u64> {
    let db = db.clone();
    std::thread::spawn(move || {
        let mut queued = 0_u64;
        while !stop.load(Ordering::Relaxed) {
            let job_id = 2_000_000 + (queued % 8);
            let row = super::persistence_tests::probe_history_row(job_id);
            if db
                .try_queue_write("probe_pipeline_write", move |db| {
                    db.archive_job(crate::jobs::JobId(job_id), &row).map(|_| ())
                })
                .is_ok()
            {
                queued += 1;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        queued
    })
}

fn run_scenario(
    label: &'static str,
    harness: &mut Harness,
    repeats: usize,
    lines: &[(LogStream, Vec<u8>)],
    contended: bool,
) -> (Samples, Samples, Samples, Samples) {
    let mut total = Samples::new("  total");
    let mut append = Samples::new("  append_logs");
    let mut attempt = Samples::new("  finish_attempt");
    let mut run = Samples::new("  finish_run");

    let stop = Arc::new(AtomicBool::new(false));
    let load = contended.then(|| spawn_writer_load(&harness.db, stop.clone()));

    for _ in 0..repeats {
        let (run_id, attempt_id) = harness.running_attempt();
        let started = Instant::now();
        let phases = finalize_once(&harness.db, &run_id, &attempt_id, lines);
        total.push(started.elapsed());
        append.push(phases.append_logs);
        attempt.push(phases.finish_attempt);
        run.push(phases.finish_run);
    }

    if let Some(load) = load {
        stop.store(true, Ordering::Relaxed);
        let queued = load.join().unwrap();
        println!("{label} (contending writes queued: {queued})");
    } else {
        println!("{label}");
    }
    (total, append, attempt, run)
}

fn print_scenario(samples: (Samples, Samples, Samples, Samples)) {
    let (total, append, attempt, run) = samples;
    println!("{}", total.report());
    println!("{}", append.report());
    println!("{}", attempt.report());
    println!("{}", run.report());
    println!();
}

/// The in-memory output-capture path, measured apart from the database.
///
/// This exists because of a hypothesis that turned out to be WRONG, and the
/// negative result is worth keeping. `BoundedOutput` retains the first captured
/// line plus a 4 MiB tail, and evicts with `VecDeque::remove(1)` — which reads
/// like an O(n) shift of the whole retained window on every over-cap line, i.e.
/// O(lines x retained) for a chatty extension. It is not: `VecDeque::remove`
/// shifts whichever side of the index is shorter, and index 1 is always one
/// element from the front, so the eviction is O(1) already.
///
/// Doubling the line count here confirms it — the cost tracks the input
/// linearly, with no quadratic blow-up past the cap. Do not "fix" this again
/// without re-running it.
#[test]
#[ignore = "measurement harness; run explicitly with --ignored --nocapture"]
fn output_capture_scaling() {
    use super::model::ExtensionAdapter;

    // ~120 B/line, so 40k lines already exceeds the 4 MiB cap and every push
    // beyond it is an evicting one.
    let line = |index: usize| {
        format!(
            "[{index:06}] extension progress line padded to a realistic width {:-<60}",
            ""
        )
        .into_bytes()
    };

    println!("\n=== output capture (BoundedOutput) scaling ===");
    println!(
        "(quadratic retention would roughly 4x the time for each 2x in lines; linear 2x's it)"
    );
    let mut previous: Option<(usize, f64)> = None;
    for count in [10_000_usize, 20_000, 40_000, 80_000] {
        let lines = (0..count).map(line).collect::<Vec<_>>();
        let started = Instant::now();
        let (retained, _, truncated) =
            super::runner::bounded_output_for_test(ExtensionAdapter::Native, lines).unwrap();
        let elapsed = started.elapsed().as_secs_f64() * 1000.0;
        let scaling = previous
            .map(|(prev_count, prev_ms)| {
                format!(
                    "   x{:.2} time for x{:.0} lines",
                    elapsed / prev_ms.max(f64::MIN_POSITIVE),
                    count as f64 / prev_count as f64
                )
            })
            .unwrap_or_default();
        println!(
            "  lines={count:<8} retained={:<7} truncated={truncated:<6} {elapsed:>8.2}ms{scaling}",
            retained.len()
        );
        previous = Some((count, elapsed));
    }
    println!();
}

#[test]
#[ignore = "measurement harness; run explicitly with --ignored --nocapture"]
fn finalize_latency_breakdown() {
    let repeats = env_usize("PROBE_REPEATS", 25);
    let line_count = env_usize("PROBE_LINES", 4000);
    let history_rows = env_usize("PROBE_HISTORY_ROWS", 5000);
    let lines = output_lines(line_count);
    let few_lines = output_lines(10);

    println!(
        "\n=== post-processing finalize latency ===\nrepeats={repeats} lines={line_count} history_rows={history_rows}\n"
    );

    let mut harness = Harness::new();
    print_scenario(run_scenario(
        "A. idle, 10 output lines, empty history",
        &mut harness,
        repeats,
        &few_lines,
        false,
    ));

    let mut harness = Harness::new();
    print_scenario(run_scenario(
        "B. idle, full output, empty history",
        &mut harness,
        repeats,
        &lines,
        false,
    ));

    let mut harness = Harness::new();
    let seeding = Instant::now();
    harness.seed_history(history_rows);
    println!(
        "(seeded {history_rows} job_history rows in {:.1}s)",
        seeding.elapsed().as_secs_f64()
    );
    print_scenario(run_scenario(
        "C. idle, full output, seeded history",
        &mut harness,
        repeats,
        &lines,
        false,
    ));

    let mut harness = Harness::new();
    harness.seed_history(history_rows);
    print_scenario(run_scenario(
        "D. contended, full output, seeded history",
        &mut harness,
        repeats,
        &lines,
        true,
    ));
}

/// End-to-end reproduction of a supervised post-processing attempt, using a real
/// child process.
///
/// The finalize DB work measures at a few milliseconds, so the seconds seen in
/// the field are somewhere else in the run. This drives the genuine
/// `execute_extension` path — real supervisor process, real pipes, real capture
/// tasks — against a script that emits a controlled number of output lines, and
/// reports the phases the runner probes now emit:
///
/// - `pp.runner.spawn_supervisor` — forking the weaver binary as supervisor
/// - `pp.runner.wait_for_exit` + `pp.runner.exit_poll_ticks` — the 50 ms poll loop
/// - `pp.runner.drain_output` — joining the capture tasks after exit
///
/// The supervisor is the weaver binary itself (`std::env::current_exe`), and a
/// libtest binary does not carry the supervisor entrypoint, so this needs a real
/// weaver build passed explicitly:
///
/// ```text
/// PROBE_WEAVER_BIN=/path/to/target/release/weaver \
///   cargo test --release -p weaver-server-core --lib supervised_attempt -- --ignored --nocapture
/// ```
#[test]
#[ignore = "measurement harness; needs PROBE_WEAVER_BIN, run with --ignored --nocapture"]
fn supervised_attempt_phase_breakdown() {
    use super::model::TimeoutPolicy;
    use super::runner::{ExtensionExecutionRequest, InterpreterConfig, JobExecutionContext};
    use std::path::PathBuf;

    let Ok(weaver_bin) = std::env::var("PROBE_WEAVER_BIN") else {
        println!("\nPROBE_WEAVER_BIN not set — skipping supervised reproduction.");
        println!("Build weaver and re-run with PROBE_WEAVER_BIN=<path to weaver binary>.\n");
        return;
    };
    let weaver_bin = PathBuf::from(weaver_bin);
    assert!(
        weaver_bin.is_file(),
        "PROBE_WEAVER_BIN does not point at a file: {}",
        weaver_bin.display()
    );

    let repeats = env_usize("PROBE_REPEATS", 10);
    println!("\n=== supervised attempt phases (real child process) ===");
    println!("supervisor: {}", weaver_bin.display());
    println!(
        "  {:<10} {:>12} {:>12} {:>12}",
        "lines", "attempt_ms", "per_line_us", "output_lines"
    );

    for line_count in [0_usize, 100, 1_000, 4_000] {
        // Build a real discoverable package so the manifest carries the genuine
        // package digest; the runner verifies it and rejects anything else as
        // UntrustedPackage.
        let data = tempfile::tempdir().unwrap();
        // discover_extensions scans `<data_dir>/scripts/*`, not the data dir itself.
        let package = data.path().join("scripts").join("probe-extension");
        std::fs::create_dir_all(&package).unwrap();
        std::fs::write(
            package.join("weaver-extension.json"),
            r#"{
                "schema_version": 1,
                "kind": "native",
                "id": "probe.supervised",
                "name": "Probe",
                "version": "1",
                "entrypoint": "run.sh",
                "commands": [],
                "options": []
            }"#,
        )
        .unwrap();
        let script = package.join("run.sh");
        std::fs::write(
            &script,
            format!(
                "#!/bin/sh\ni=0\nwhile [ $i -lt {line_count} ]; do\n  echo \"[$i] extension progress line padded to a realistic width ------------------------------\"\n  i=$((i+1))\ndone\nexit 0\n"
            ),
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        }

        let manifest = super::discovery::discover_extensions(
            data.path(),
            super::discovery::DiscoveryOptions {
                enabled: true,
                bare_script_adapter: None,
            },
        )
        .unwrap();
        assert!(
            !manifest.is_empty(),
            "probe package was not discovered under {}/scripts",
            data.path().display()
        );
        let manifest = manifest.into_iter().next().unwrap().manifest;
        let work = tempfile::tempdir().unwrap();

        let mut totals = Samples::new("attempt");
        let mut produced = 0_usize;
        for index in 0..repeats {
            let request = ExtensionExecutionRequest {
                attempt_id: format!("probe-attempt-{line_count}-{index}"),
                manifest: manifest.clone(),
                managed_path: package.clone(),
                options: vec![],
                approved_roots: vec![],
                context: JobExecutionContext {
                    job_id: 42,
                    name: "Probe Job".into(),
                    nzb_filename: "probe.nzb".into(),
                    category: Some("movies".into()),
                    group: None,
                    source_url: None,
                    working_directory: work.path().to_path_buf(),
                    final_directory: work.path().to_path_buf(),
                    pipeline_outcome: PipelineOutcome::Succeeded,
                    par_status: 0,
                    unpack_status: 0,
                    compatibility: Default::default(),
                },
                timeout_policy: TimeoutPolicy::Default24Hours,
                termination_grace: Duration::from_secs(10),
                interpreters: InterpreterConfig::default(),
                control_token: None,
                diagnostic_command: None,
                supervisor_executable: Some(weaver_bin.clone()),
            };
            let runtime = tokio::runtime::Runtime::new().unwrap();
            let started = Instant::now();
            let result = runtime
                .block_on(super::runner::execute_extension(request, None))
                .unwrap();
            totals.push(started.elapsed());
            produced = result.output.len();
        }
        let mean_ms = totals.values.iter().sum::<Duration>().as_secs_f64() * 1000.0
            / totals.values.len() as f64;
        let per_line_us = if line_count == 0 {
            0.0
        } else {
            mean_ms * 1000.0 / line_count as f64
        };
        println!("  {line_count:<10} {mean_ms:>12.2} {per_line_us:>12.2} {produced:>12}");
        println!("    {}", totals.report().trim());
    }
    println!(
        "\n(the runner's own phase buckets — pp.runner.spawn_supervisor / wait_for_exit /\n \
         exit_poll_ticks / drain_output — are emitted by the perf probe when\n \
         WEAVER_PROFILE_HOT_PATHS=1 is set)\n"
    );
}

/// Cost of the recursive artifact walk `execute_steps` runs on entry and again
/// after any step that moves the working directory.
#[test]
#[ignore = "measurement harness; run explicitly with --ignored --nocapture"]
fn artifact_walk_scaling() {
    println!("\n=== collect_artifact_paths scaling ===");
    println!("  {:<10} {:>10} {:>12}", "files", "walk_ms", "found");
    for file_count in [100_usize, 1_000, 10_000, 30_000] {
        let root = tempfile::tempdir().unwrap();
        // A realistic extracted layout: nested directories, not one flat dir.
        for index in 0..file_count {
            let dir = root.path().join(format!("d{}", index / 100));
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join(format!("f{index}.bin")), b"x").unwrap();
        }
        let started = Instant::now();
        let found = super::service::collect_artifact_paths_for_test(root.path(), 10_000);
        let elapsed = started.elapsed().as_secs_f64() * 1000.0;
        println!("  {file_count:<10} {elapsed:>10.2} {found:>12}");
    }
    println!("(MAX_DISCOVERED_ARTIFACTS caps the result at 10000)\n");
}

/// Read latency on the writer lane vs the read lane, under pipeline-style write
/// load.
///
/// This is the workload the split was made for: GraphQL/queue reads while the
/// download pipeline is writing. Both arms submit the *identical* query future;
/// the only difference is which executor it lands on, so the delta is the lane
/// and nothing else.
#[test]
#[ignore = "measurement harness; run explicitly with --ignored --nocapture"]
fn read_lane_under_write_load() {
    use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};

    let repeats = env_usize("PROBE_REPEATS", 60);
    let history_rows = env_usize("PROBE_HISTORY_ROWS", 5000);

    let mut harness = Harness::new();
    harness.seed_history(history_rows);
    println!("\n=== read latency: writer lane vs read lane ===");
    println!("history_rows={history_rows} repeats={repeats} per cell");

    // A representative UI read: a filtered, ordered page over job_history.
    let query = |db: &Database, on_read_lane: bool| {
        let datastore = db.datastore();
        let future = async move {
            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT job_id, name, status, completed_at FROM job_history
                  WHERE status = {} ORDER BY completed_at DESC LIMIT 50",
                &[SqlArg::Text("complete".to_string())],
            )
            .await?;
            Ok::<_, crate::StateError>(rows.len())
        };
        if on_read_lane {
            db.run_sql_blocking_read(future)
        } else {
            db.run_sql_blocking(future)
        }
    };

    for contended in [false, true] {
        let stop = Arc::new(AtomicBool::new(false));
        let load = contended.then(|| spawn_writer_load(&harness.db, stop.clone()));
        // Let the writer queue actually fill before sampling.
        if contended {
            std::thread::sleep(Duration::from_millis(250));
        }

        let mut writer_lane = Samples::new("  writer lane");
        let mut read_lane = Samples::new("  read lane");
        for _ in 0..repeats {
            let started = Instant::now();
            query(&harness.db, false).unwrap();
            writer_lane.push(started.elapsed());

            let started = Instant::now();
            query(&harness.db, true).unwrap();
            read_lane.push(started.elapsed());
        }

        if let Some(load) = load {
            stop.store(true, Ordering::Relaxed);
            let queued = load.join().unwrap();
            println!("\ncontended (background writes queued: {queued})");
        } else {
            println!("\nidle");
        }
        println!("{}", writer_lane.report());
        println!("{}", read_lane.report());
        let w: Duration = writer_lane.values.iter().sum();
        let r: Duration = read_lane.values.iter().sum();
        println!(
            "  ratio (>1 = read lane faster): {:.2}x",
            w.as_secs_f64() / r.as_secs_f64().max(f64::MIN_POSITIVE)
        );
    }
    println!();
}
