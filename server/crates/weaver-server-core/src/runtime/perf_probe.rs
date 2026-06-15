use std::collections::BTreeMap;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

const ENABLE_ENV: &str = "WEAVER_PROFILE_HOT_PATHS";
const INTERVAL_ENV: &str = "WEAVER_PROFILE_HOT_PATHS_INTERVAL_SECS";
const TOP_N_ENV: &str = "WEAVER_PROFILE_HOT_PATHS_TOP_N";

#[derive(Clone, Default)]
struct Bucket {
    count: u64,
    total_ns: u128,
    max_ns: u128,
}

struct Profiler {
    started: Instant,
    last_emit: Mutex<Instant>,
    buckets: Mutex<BTreeMap<String, Bucket>>,
    interval: Duration,
    top_n: usize,
}

pub(crate) fn enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var(ENABLE_ENV)
            .ok()
            .map(|value| {
                let normalized = value.trim().to_ascii_lowercase();
                !(normalized.is_empty()
                    || normalized == "0"
                    || normalized == "false"
                    || normalized == "off")
            })
            .unwrap_or(false)
    })
}

pub(crate) fn record(label: &'static str, elapsed: Duration) {
    if !enabled() {
        return;
    }
    profiler().record(label.to_string(), elapsed);
}

pub(crate) fn scope(label: &'static str) -> Scope {
    Scope {
        label,
        started: Instant::now(),
        enabled: enabled(),
    }
}

pub(crate) fn record_sql_op(engine: &'static str, op_name: &'static str, elapsed: Duration) {
    if !enabled() {
        return;
    }
    profiler().record(format!("sql.tx.{engine}.{op_name}"), elapsed);
}

pub(crate) fn record_sql_statement(
    engine: &'static str,
    action: &'static str,
    template: &str,
    elapsed: Duration,
) {
    if !enabled() {
        return;
    }
    let detail = classify_sql(template);
    profiler().record(format!("sql.{engine}.{action}.{detail}"), elapsed);
}

pub(crate) struct Scope {
    label: &'static str,
    started: Instant,
    enabled: bool,
}

impl Drop for Scope {
    fn drop(&mut self) {
        if self.enabled {
            profiler().record(self.label.to_string(), self.started.elapsed());
        }
    }
}

fn profiler() -> &'static Profiler {
    static PROFILER: OnceLock<Profiler> = OnceLock::new();
    PROFILER.get_or_init(|| {
        let interval = std::env::var(INTERVAL_ENV)
            .ok()
            .and_then(|value| value.trim().parse::<u64>().ok())
            .filter(|seconds| *seconds > 0)
            .map(Duration::from_secs)
            .unwrap_or_else(|| Duration::from_secs(10));
        let top_n = std::env::var(TOP_N_ENV)
            .ok()
            .and_then(|value| value.trim().parse::<usize>().ok())
            .filter(|count| *count > 0)
            .unwrap_or(32);
        let now = Instant::now();
        Profiler {
            started: now,
            last_emit: Mutex::new(now),
            buckets: Mutex::new(BTreeMap::new()),
            interval,
            top_n,
        }
    })
}

impl Profiler {
    fn record(&self, label: String, elapsed: Duration) {
        {
            let mut buckets = self.buckets.lock().expect("perf probe buckets poisoned");
            let bucket = buckets.entry(label).or_default();
            let elapsed_ns = elapsed.as_nanos();
            bucket.count = bucket.count.saturating_add(1);
            bucket.total_ns = bucket.total_ns.saturating_add(elapsed_ns);
            bucket.max_ns = bucket.max_ns.max(elapsed_ns);
        }
        self.emit_if_due();
    }

    fn emit_if_due(&self) {
        let now = Instant::now();
        {
            let mut last_emit = self.last_emit.lock().expect("perf probe timer poisoned");
            if now.duration_since(*last_emit) < self.interval {
                return;
            }
            *last_emit = now;
        }

        let mut rows = {
            let buckets = self.buckets.lock().expect("perf probe buckets poisoned");
            buckets
                .iter()
                .map(|(label, bucket)| (label.clone(), bucket.clone()))
                .collect::<Vec<_>>()
        };
        rows.sort_by(|(_, left), (_, right)| {
            right
                .total_ns
                .cmp(&left.total_ns)
                .then_with(|| right.max_ns.cmp(&left.max_ns))
        });

        tracing::info!(
            target: "weaver::perf_probe",
            elapsed_secs = now.duration_since(self.started).as_secs(),
            bucket_count = rows.len(),
            top_n = self.top_n,
            "weaver hot-path profile summary"
        );
        for (label, bucket) in rows.into_iter().take(self.top_n) {
            let avg_ns = if bucket.count == 0 {
                0
            } else {
                bucket.total_ns / u128::from(bucket.count)
            };
            tracing::info!(
                target: "weaver::perf_probe",
                bucket = %label,
                count = bucket.count,
                total_ms = ns_to_ms(bucket.total_ns),
                avg_us = ns_to_us(avg_ns),
                max_ms = ns_to_ms(bucket.max_ns),
                "weaver hot-path profile bucket"
            );
        }
    }
}

fn ns_to_ms(ns: u128) -> u64 {
    u64::try_from(ns / 1_000_000).unwrap_or(u64::MAX)
}

fn ns_to_us(ns: u128) -> u64 {
    u64::try_from(ns / 1_000).unwrap_or(u64::MAX)
}

fn classify_sql(template: &str) -> &'static str {
    let lower = template.to_ascii_lowercase();
    for (needle, label) in [
        ("pg_advisory_xact_lock", "advisory_lock"),
        ("active_file_progress", "active_file_progress"),
        ("active_file_identities", "active_file_identities"),
        ("active_segments", "active_segments"),
        ("active_par2_files", "active_par2_files"),
        ("active_par2_metadata", "active_par2_metadata"),
        ("active_failed_extractions", "active_failed_extractions"),
        ("active_extracted_members", "active_extracted_members"),
        ("active_extraction_chunks", "active_extraction_chunks"),
        ("active_rar_verified_suspect", "active_rar_verified_suspect"),
        ("active_rar_volume_facts", "active_rar_volume_facts"),
        ("active_detected_archives", "active_detected_archives"),
        ("active_files", "active_files"),
        ("active_jobs", "active_jobs"),
        (
            "bandwidth_usage_minute_buckets",
            "bandwidth_usage_minute_buckets",
        ),
        ("job_events", "job_events"),
        ("integration_events", "integration_events"),
        ("job_history", "job_history"),
        ("diagnostic_runs", "diagnostic_runs"),
        ("settings", "settings"),
        ("servers", "servers"),
        ("rss_", "rss"),
        ("pragma", "pragma"),
    ] {
        if lower.contains(needle) {
            return label;
        }
    }

    match lower.split_whitespace().next() {
        Some("select") => "select",
        Some("insert") => "insert",
        Some("update") => "update",
        Some("delete") => "delete",
        Some("with") => "with",
        Some("pragma") => "pragma",
        Some("vacuum") => "vacuum",
        Some("create") => "create",
        Some("alter") => "alter",
        Some("drop") => "drop",
        _ => "other",
    }
}
