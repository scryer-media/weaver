use std::collections::BTreeMap;
#[cfg(unix)]
use std::mem::MaybeUninit;
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
    started_cpu: Option<CpuUsage>,
    last_emit: Mutex<Instant>,
    buckets: Mutex<BTreeMap<String, Bucket>>,
    cpu_buckets: Mutex<BTreeMap<String, Bucket>>,
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

pub(crate) fn record_owned(label: String, elapsed: Duration) {
    if !enabled() {
        return;
    }
    profiler().record(label, elapsed);
}

pub(crate) fn record_cpu_sample(label: &'static str, elapsed: Duration) {
    if !enabled() {
        return;
    }
    profiler().record_cpu(label.to_string(), elapsed);
}

pub(crate) fn scope(label: &'static str) -> Scope {
    Scope {
        label,
        started: Instant::now(),
        enabled: enabled(),
    }
}

pub(crate) fn cpu_scope(label: &'static str) -> CpuScope {
    let enabled = enabled();
    CpuScope {
        label,
        started: enabled.then(thread_cpu_time).flatten(),
        enabled,
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

pub(crate) struct CpuScope {
    label: &'static str,
    started: Option<Duration>,
    enabled: bool,
}

impl Drop for CpuScope {
    fn drop(&mut self) {
        if !self.enabled {
            return;
        }
        let Some(started) = self.started else {
            return;
        };
        let Some(current) = thread_cpu_time() else {
            return;
        };
        profiler().record_cpu(self.label.to_string(), current.saturating_sub(started));
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
            started_cpu: process_cpu_usage(),
            last_emit: Mutex::new(now),
            buckets: Mutex::new(BTreeMap::new()),
            cpu_buckets: Mutex::new(BTreeMap::new()),
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

    fn record_cpu(&self, label: String, elapsed: Duration) {
        {
            let mut buckets = self
                .cpu_buckets
                .lock()
                .expect("perf probe cpu buckets poisoned");
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

        let wall = now.duration_since(self.started);
        if let Some(cpu) = self
            .started_cpu
            .and_then(|started| process_cpu_usage().map(|current| current.saturating_sub(started)))
        {
            let cpu_total = cpu.total();
            tracing::info!(
                target: "weaver::perf_probe",
                elapsed_secs = wall.as_secs(),
                bucket_count = rows.len(),
                top_n = self.top_n,
                cpu_sample_available = true,
                cpu_user_ms = duration_ms(cpu.user),
                cpu_system_ms = duration_ms(cpu.system),
                cpu_total_ms = duration_ms(cpu_total),
                cpu_util_pct = cpu_util_pct(cpu_total, wall),
                "weaver hot-path profile summary"
            );
        } else {
            tracing::info!(
                target: "weaver::perf_probe",
                elapsed_secs = wall.as_secs(),
                bucket_count = rows.len(),
                top_n = self.top_n,
                cpu_sample_available = false,
                "weaver hot-path profile summary"
            );
        }
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

        let mut cpu_rows = {
            let buckets = self
                .cpu_buckets
                .lock()
                .expect("perf probe cpu buckets poisoned");
            buckets
                .iter()
                .map(|(label, bucket)| (label.clone(), bucket.clone()))
                .collect::<Vec<_>>()
        };
        cpu_rows.sort_by(|(_, left), (_, right)| {
            right
                .total_ns
                .cmp(&left.total_ns)
                .then_with(|| right.max_ns.cmp(&left.max_ns))
        });
        for (label, bucket) in cpu_rows.into_iter().take(self.top_n) {
            let avg_ns = if bucket.count == 0 {
                0
            } else {
                bucket.total_ns / u128::from(bucket.count)
            };
            tracing::info!(
                target: "weaver::perf_probe",
                bucket = %label,
                count = bucket.count,
                cpu_total_ms = ns_to_ms(bucket.total_ns),
                cpu_avg_us = ns_to_us(avg_ns),
                cpu_max_ms = ns_to_ms(bucket.max_ns),
                "weaver hot-path CPU bucket"
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

#[derive(Clone, Copy)]
struct CpuUsage {
    user: Duration,
    system: Duration,
}

impl CpuUsage {
    fn total(self) -> Duration {
        self.user.saturating_add(self.system)
    }

    fn saturating_sub(self, earlier: Self) -> Self {
        Self {
            user: self.user.saturating_sub(earlier.user),
            system: self.system.saturating_sub(earlier.system),
        }
    }
}

fn duration_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn cpu_util_pct(cpu: Duration, wall: Duration) -> u64 {
    let wall_ms = wall.as_millis();
    if wall_ms == 0 {
        return 0;
    }
    u64::try_from(cpu.as_millis().saturating_mul(100) / wall_ms).unwrap_or(u64::MAX)
}

#[cfg(unix)]
fn process_cpu_usage() -> Option<CpuUsage> {
    let mut usage = MaybeUninit::<libc::rusage>::uninit();
    let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if rc != 0 {
        return None;
    }
    let usage = unsafe { usage.assume_init() };
    Some(CpuUsage {
        user: timeval_to_duration(usage.ru_utime),
        system: timeval_to_duration(usage.ru_stime),
    })
}

#[cfg(unix)]
fn timeval_to_duration(timeval: libc::timeval) -> Duration {
    let seconds = u64::try_from(timeval.tv_sec).unwrap_or(0);
    let micros = u32::try_from(timeval.tv_usec).unwrap_or(0).min(999_999);
    Duration::new(seconds, micros.saturating_mul(1_000))
}

#[cfg(not(unix))]
fn process_cpu_usage() -> Option<CpuUsage> {
    None
}

#[cfg(unix)]
fn thread_cpu_time() -> Option<Duration> {
    let mut timespec = MaybeUninit::<libc::timespec>::uninit();
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, timespec.as_mut_ptr()) };
    if rc != 0 {
        return None;
    }
    let timespec = unsafe { timespec.assume_init() };
    timespec_to_duration(timespec)
}

#[cfg(unix)]
fn timespec_to_duration(timespec: libc::timespec) -> Option<Duration> {
    let seconds = u64::try_from(timespec.tv_sec).ok()?;
    let nanos = u32::try_from(timespec.tv_nsec).ok()?.min(999_999_999);
    Some(Duration::new(seconds, nanos))
}

#[cfg(not(unix))]
fn thread_cpu_time() -> Option<Duration> {
    None
}

fn classify_sql(template: &str) -> String {
    let lower = template.to_ascii_lowercase();
    if lower.contains("update active_jobs") && lower.contains("download_state") {
        return "active_jobs_runtime".to_string();
    }
    if lower.contains("with active_file_complete_active") {
        return "complete_file".to_string();
    }
    if lower.contains("with active_extracted_member_active") {
        return "add_extracted_member".to_string();
    }

    for (needle, label) in [
        ("pg_advisory_xact_lock", "advisory_lock"),
        (
            "bandwidth_usage_minute_buckets",
            "bandwidth_usage_minute_buckets",
        ),
        ("pragma", "pragma"),
    ] {
        if lower.contains(needle) {
            return label.to_string();
        }
    }

    for table in [
        "active_file_progress",
        "active_file_identities",
        "active_par2_files",
        "active_failed_extractions",
        "active_extracted",
        "active_extraction_chunks",
        "active_rar_verified_suspect",
        "active_rar_volume_facts",
        "active_detected_archives",
        "active_archive_headers",
        "active_volume_status",
        "active_files",
        "active_jobs",
        "job_events",
        "integration_events",
        "job_history",
        "diagnostic_runs",
        "settings",
        "servers",
    ] {
        if lower.contains(table) {
            return table_operation_label(&lower, table);
        }
    }
    if lower.contains("rss_") {
        return "rss".to_string();
    }

    match lower.split_whitespace().next() {
        Some("select") => dynamic_sql_label(&lower, "select"),
        Some("insert") => dynamic_sql_label(&lower, "insert"),
        Some("update") => dynamic_sql_label(&lower, "update"),
        Some("delete") => dynamic_sql_label(&lower, "delete"),
        Some("with") => dynamic_sql_label(&lower, "with"),
        Some("pragma") => "pragma".to_string(),
        Some("vacuum") => "vacuum".to_string(),
        Some("create") => "create".to_string(),
        Some("alter") => "alter".to_string(),
        Some("drop") => "drop".to_string(),
        _ => "other".to_string(),
    }
}

fn table_operation_label(lower_sql: &str, table: &str) -> String {
    let operation = if lower_sql.contains(&format!("insert into {table}")) {
        "insert"
    } else if lower_sql.contains(&format!("update {table}")) {
        "update"
    } else if lower_sql.contains(&format!("delete from {table}")) {
        "delete"
    } else if lower_sql.contains(&format!("from {table}")) {
        "select"
    } else {
        "touch"
    };
    format!("{table}.{operation}")
}

fn dynamic_sql_label(lower_sql: &str, operation: &'static str) -> String {
    if let Some(table) = first_table_after(lower_sql, "insert into") {
        return format!("insert.{table}");
    }
    if let Some(table) = first_table_after(lower_sql, "update") {
        return format!("update.{table}");
    }
    if let Some(table) = first_table_after(lower_sql, "delete from") {
        return format!("delete.{table}");
    }
    if let Some(table) = first_table_after(lower_sql, "from") {
        return format!("{operation}.{table}");
    }
    operation.to_string()
}

fn first_table_after(lower_sql: &str, marker: &str) -> Option<String> {
    let index = lower_sql.find(marker)?;
    let rest = lower_sql[index + marker.len()..].trim_start();
    let token = rest.split_whitespace().next()?;
    let table = token
        .trim_matches(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '_' || ch == '.'))
        .trim_start_matches("only.")
        .trim_start_matches("public.")
        .to_string();
    if table.is_empty() { None } else { Some(table) }
}
