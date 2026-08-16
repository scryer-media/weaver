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
