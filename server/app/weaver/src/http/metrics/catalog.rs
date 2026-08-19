//! The metric catalogue: every family `/metrics` can emit, as data.
//!
//! Nothing outside this file may invent a metric name. Because the encoder
//! only accepts a [`MetricFamily`], adding a series means adding an entry here,
//! which in turn means it gets a HELP, a TYPE, a declared label set, and a row
//! in `docs/metrics.md` — the doc test fails otherwise.

use super::encode::{MetricFamily, MetricKind};

macro_rules! metric_families {
    ($(
        $ident:ident = ($name:literal, $kind:ident, [$($label:literal),* $(,)?], $help:literal
            $(, deprecated_by = $replacement:literal)? $(,)?);
    )*) => {
        $(
            pub(crate) static $ident: MetricFamily = MetricFamily {
                name: $name,
                kind: MetricKind::$kind,
                labels: &[$($label),*],
                help: $help,
                deprecated_by: metric_families!(@deprecated $($replacement)?),
            };
        )*

        // The catalogue exists for the exposition and documentation tests;
        // production rendering reaches for the individual statics.
        #[allow(dead_code)]
        static CATALOG: &[&MetricFamily] = &[$(&$ident),*];

        /// Every family the exporter is capable of emitting.
        #[allow(dead_code)]
        pub(crate) fn metric_catalog() -> &'static [&'static MetricFamily] {
            CATALOG
        }
    };
    (@deprecated) => { None };
    (@deprecated $replacement:literal) => { Some($replacement) };
}

metric_families! {
    // ---- process / build ------------------------------------------------
    BUILD_INFO = ("weaver_build_info", Gauge,
        ["version", "commit", "target_arch", "target_os", "decoder_tier", "database_backend",
         "tls_backend"],
        "Static build information; always 1.");
    START_TIME_SECONDS = ("weaver_start_time_seconds", Gauge, [],
        "Unix timestamp at which this process started.");

    // ---- global gate ----------------------------------------------------
    PIPELINE_PAUSED = ("weaver_pipeline_paused", Gauge, [],
        "Whether the entire pipeline is globally paused.");
    DOWNLOAD_GATE = ("weaver_pipeline_download_gate", Gauge, ["reason"],
        "Current global download gate; exactly one reason is 1.");
    SCHEDULED_SPEED_LIMIT = ("weaver_pipeline_scheduled_speed_limit_bytes_per_second", Gauge, [],
        "Speed limit imposed by the active schedule; zero means no scheduled limit.");

    // ---- ISP bandwidth cap ----------------------------------------------
    CAP_ENABLED = ("weaver_bandwidth_cap_enabled", Gauge, [],
        "Whether the ISP bandwidth cap policy is enabled.");
    CAP_USED_BYTES = ("weaver_bandwidth_cap_used_bytes", Gauge, [],
        "Current ISP bandwidth cap usage in bytes.");
    CAP_LIMIT_BYTES = ("weaver_bandwidth_cap_limit_bytes", Gauge, [],
        "Configured ISP bandwidth cap limit in bytes.");
    CAP_REMAINING_BYTES = ("weaver_bandwidth_cap_remaining_bytes", Gauge, [],
        "Remaining ISP bandwidth cap bytes in the active window.");
    CAP_RESERVED_BYTES = ("weaver_bandwidth_cap_reserved_bytes", Gauge, [],
        "Bytes conservatively reserved for in-flight downloads against the active cap window.");
    CAP_WINDOW_END_SECONDS = ("weaver_bandwidth_cap_window_end_seconds", Gauge, [],
        "Active ISP bandwidth cap window end as a unix timestamp.");

    // ---- queue mix ------------------------------------------------------
    PIPELINE_JOBS = ("weaver_pipeline_jobs", Gauge, ["status"],
        "Number of known jobs by status.");

    // ---- duplicate admission --------------------------------------------
    DUPLICATE_ADMISSION = ("weaver_duplicate_admission_decisions_total", Counter,
        ["origin", "status"],
        "Duplicate admission decisions by intake origin and public status.");
    SEMANTIC_DUPLICATE_LIFECYCLE = ("weaver_semantic_duplicate_lifecycle_total", Counter, ["event"],
        "Semantic duplicate arbitration lifecycle events.");

    // ---- throughput totals ----------------------------------------------
    BYTES_DOWNLOADED = ("weaver_pipeline_bytes_downloaded_total", Counter, [],
        "Total bytes downloaded by the pipeline.");
    BYTES_DECODED = ("weaver_pipeline_bytes_decoded_total", Counter, [],
        "Total bytes decoded by the pipeline.");
    BYTES_COMMITTED = ("weaver_pipeline_bytes_committed_total", Counter, [],
        "Total bytes committed to disk by the pipeline.");
    SEGMENTS_DOWNLOADED = ("weaver_pipeline_segments_downloaded_total", Counter, [],
        "Total segments downloaded.");
    SEGMENTS_DECODED = ("weaver_pipeline_segments_decoded_total", Counter, [],
        "Total segments decoded.");
    SEGMENTS_COMMITTED = ("weaver_pipeline_segments_committed_total", Counter, [],
        "Total segments committed.");
    SEGMENTS_RETRIED = ("weaver_pipeline_segments_retried_total", Counter, [],
        "Total segments retried.");
    SEGMENTS_FAILED_PERMANENT = ("weaver_pipeline_segments_failed_permanent_total", Counter, [],
        "Total segments permanently failed.");
    PARKED_INFRASTRUCTURE_WORK = ("weaver_pipeline_parked_infrastructure_work", Gauge, [],
        "Segments parked while NNTP infrastructure is unavailable.");
    GENERATION_RECOVERY_REQUEUES = ("weaver_nntp_generation_recovery_requeues_total", Counter, [],
        "Segments requeued after stale NNTP generation failures.");

    // ---- adaptive capacity probes ---------------------------------------
    PROBE_ATTEMPTS = ("weaver_nntp_capacity_probe_attempts_total", Counter, [],
        "Adaptive-capacity provider connection probes attempted.");
    PROBE_SUCCESSES = ("weaver_nntp_capacity_probe_successes_total", Counter, [],
        "Adaptive-capacity probes that restored one connection.");
    PROBE_REJECTIONS = ("weaver_nntp_capacity_probe_rejections_total", Counter, [],
        "Adaptive-capacity probes rejected by provider limits.");
    PROBE_TRANSPORT_FAILURES = ("weaver_nntp_capacity_probe_transport_failures_total", Counter, [],
        "Adaptive-capacity probes that failed during transport setup.");
    PROBE_STALE_GENERATION = ("weaver_nntp_capacity_probe_stale_generation_total", Counter, [],
        "Probe results ignored after an NNTP generation replacement.");

    // ---- failures --------------------------------------------------------
    DOWNLOAD_FAILURES = ("weaver_pipeline_download_failures_total", Counter, ["kind"],
        "Failed article download attempts by kind.");
    ARTICLES_NOT_FOUND = ("weaver_pipeline_articles_not_found_total", Counter, [],
        "Total articles not found.");
    DECODE_ERRORS = ("weaver_pipeline_decode_errors_total", Counter, [],
        "Total decode errors.");
    CRC_ERRORS = ("weaver_pipeline_crc_errors_total", Counter, [],
        "Total CRC errors.");

    // ---- queues and buffers ---------------------------------------------
    DOWNLOAD_QUEUE_DEPTH = ("weaver_pipeline_download_queue_depth", Gauge, [],
        "Download queue depth.");
    ACTIVE_DOWNLOADS = ("weaver_pipeline_active_downloads", Gauge, [],
        "Active article downloads.");
    ACTIVE_DECODES = ("weaver_pipeline_active_decodes", Gauge, [],
        "Active decode tasks.");
    DECODE_PENDING = ("weaver_pipeline_decode_pending", Gauge, [],
        "Decode pending queue depth.");
    DECODE_PENDING_BYTES = ("weaver_pipeline_decode_pending_bytes", Gauge, [],
        "Raw article bytes queued for decode.");
    DECODE_ACTIVE_BYTES = ("weaver_pipeline_decode_active_bytes", Gauge, [],
        "Raw article bytes currently being decoded.");
    COMMIT_PENDING = ("weaver_pipeline_commit_pending", Gauge, [],
        "Commit pending queue depth.");
    RECOVERY_QUEUE_DEPTH = ("weaver_pipeline_recovery_queue_depth", Gauge, [],
        "Recovery queue depth.");
    WRITE_BUFFERED_BYTES = ("weaver_pipeline_write_buffered_bytes", Gauge, [],
        "Buffered write bytes.");
    WRITE_BUFFERED_SEGMENTS = ("weaver_pipeline_write_buffered_segments", Gauge, [],
        "Buffered write segments.");
    DECODE_SOFT_LIMIT = ("weaver_pipeline_decode_pressure_soft_limit_bytes", Gauge, [],
        "Decode soft pressure limit in bytes.");
    DECODE_HARD_LIMIT = ("weaver_pipeline_decode_pressure_hard_limit_bytes", Gauge, [],
        "Decode hard pressure limit in bytes.");
    WRITE_SOFT_LIMIT = ("weaver_pipeline_write_pressure_soft_limit_bytes", Gauge, [],
        "Write soft pressure limit in bytes.");
    WRITE_HARD_LIMIT = ("weaver_pipeline_write_pressure_hard_limit_bytes", Gauge, [],
        "Write hard pressure limit in bytes.");
    PRESSURE_STATE = ("weaver_pipeline_download_pressure_state", Gauge, ["state"],
        "Download backpressure state; exactly one state is 1.");
    PRESSURE_REASON = ("weaver_pipeline_download_pressure_reason", Gauge, ["reason"],
        "Download backpressure reason; exactly one reason is 1.");

    // ---- hot dispatch ----------------------------------------------------
    HOT_JOB_ID = ("weaver_pipeline_hot_dispatch_job_id", Gauge, [],
        "Current hot-dispatch job id, or 0 when no job owns hot dispatch.");
    HOT_MODE = ("weaver_pipeline_hot_dispatch_mode", Gauge, ["mode"],
        "Current hot-dispatch sharing mode; exactly one mode is 1.");
    HOT_UNDERFILL_MS = ("weaver_pipeline_hot_dispatch_underfill_milliseconds", Gauge, [],
        "Current hot-job unused-capacity underfill window age in milliseconds.",
        deprecated_by = "weaver_pipeline_hot_dispatch_underfill_seconds");
    HOT_UNDERFILL_SECONDS = ("weaver_pipeline_hot_dispatch_underfill_seconds", Gauge, [],
        "Current hot-job unused-capacity underfill window age.");
    HOT_LENT_CONNECTIONS = ("weaver_pipeline_hot_dispatch_lent_connections", Gauge, [],
        "Active NNTP connection tasks lent to spillover jobs.");
    HOT_WARMUP_COMPLETE = ("weaver_pipeline_hot_dispatch_warmup_complete", Gauge, [],
        "Whether the current hot-dispatch warmup gate is complete.");
    HOT_LAST_SPILLOVER_DECISION = ("weaver_pipeline_hot_dispatch_last_spillover_decision", Gauge,
        ["decision"], "Last hot-dispatch spillover decision; exactly one decision is 1.");
    HOT_SPILLOVER_DECISIONS = ("weaver_pipeline_hot_dispatch_spillover_decisions_total", Counter,
        ["decision"], "Hot-dispatch spillover decisions by reason.");
    HOT_SPEED = ("weaver_pipeline_hot_dispatch_speed_bytes_per_second", Gauge, [],
        "Two-second hot-job BODY throughput.");
    HOT_LAST_EXPANSION_KIND_CODE = ("weaver_pipeline_hot_dispatch_last_expansion_kind", Gauge, [],
        "Last hot-job expansion event kind, as an opaque numeric code.",
        deprecated_by = "weaver_pipeline_hot_dispatch_expansion_kind");
    HOT_EXPANSION_KIND = ("weaver_pipeline_hot_dispatch_expansion_kind", Gauge, ["kind"],
        "Last hot-job expansion event kind; exactly one kind is 1.");
    HOT_LAST_EXPANSION_SPEED = ("weaver_pipeline_hot_dispatch_last_expansion_speed_bytes_per_second",
        Gauge, ["phase"], "Last hot-job expansion before/after speeds.");
    HOT_EXCLUSIVE_PEAK = ("weaver_pipeline_hot_dispatch_exclusive_peak_bytes_per_second", Gauge, [],
        "Peak hot-job speed observed while exclusive.");
    HOT_SPILLOVER_SPEED = ("weaver_pipeline_hot_dispatch_spillover_speed_bytes_per_second", Gauge,
        ["phase"], "Hot-job speed before and after the current spillover loan.");
    HOT_SPILLOVER_ACTIVE_LOANS = ("weaver_pipeline_hot_dispatch_spillover_active_loans", Gauge, [],
        "Active measured spillover loans.");
    HOT_EXPANSION_IMPROVEMENT_PCT =
        ("weaver_pipeline_hot_dispatch_recent_expansion_improvement_percent", Gauge, [],
        "Best recent lane/pipeline expansion improvement, in percent.",
        deprecated_by = "weaver_pipeline_hot_dispatch_recent_expansion_improvement_ratio");
    HOT_EXPANSION_IMPROVEMENT_RATIO =
        ("weaver_pipeline_hot_dispatch_recent_expansion_improvement_ratio", Gauge, [],
        "Best recent lane/pipeline expansion improvement as a ratio, where 0.1 is a 10% gain.");
    HOT_BEST_MODE_BLOCK_CODE = ("weaver_pipeline_hot_dispatch_best_mode_block_reason", Gauge, [],
        "Last best-mode spillover block reason, as an opaque numeric code.",
        deprecated_by = "weaver_pipeline_hot_dispatch_best_mode_block");
    HOT_BEST_MODE_BLOCK = ("weaver_pipeline_hot_dispatch_best_mode_block", Gauge, ["reason"],
        "Last best-mode spillover block reason; exactly one reason is 1.");

    // ---- download lanes --------------------------------------------------
    LANES_ACTIVE_BY_MODE = ("weaver_pipeline_download_lanes_active", Gauge, ["mode"],
        "Active article download lanes by pipelining mode.");
    LANE_STATES_ACTIVE = ("weaver_pipeline_download_lane_states_active", Gauge, ["state"],
        "Active article download lanes by scheduler state.");
    LANES_ACTIVE_TOTAL = ("weaver_pipeline_download_lanes_active_total", Gauge, [],
        "Total active article download lanes.",
        deprecated_by = "weaver_pipeline_download_lanes");
    LANES = ("weaver_pipeline_download_lanes", Gauge, [],
        "Total active article download lanes.");
    LANE_PARKS = ("weaver_pipeline_download_lane_parks_total", Counter, ["reason"],
        "Article download lane parks by reason.");
    LANE_LEASE_ITEMS = ("weaver_pipeline_download_lane_lease_items_total", Counter, [],
        "Article work items leased to download lanes.");
    LANE_REFILLS = ("weaver_pipeline_download_lane_refills_total", Counter, ["result"],
        "Lane refill scheduler decisions.");

    // ---- BODY pipelining proof -------------------------------------------
    BODY_PROOF_EVENTS = ("weaver_pipeline_body_proof_events_total", Counter, ["event"],
        "BODY pipelining proof events.");
    BODY_REPLAY_ITEMS = ("weaver_pipeline_body_replay_items_total", Counter, [],
        "BODY items returned unresolved after a lane reset or failure.");

    // ---- latent-IP replacement -------------------------------------------
    IP_TRIAL_EXTRA_CONNECTIONS = ("weaver_ip_replacement_trial_extra_connections", Gauge, [],
        "Configured over-max IP replacement trial burst budget.");
    IP_BURST_ACTIVE = ("weaver_ip_replacement_burst_active", Gauge, [],
        "Whether an over-max IP replacement trial is active.");
    IP_OVER_MAX_CONNECTIONS = ("weaver_ip_replacement_over_max_connections", Gauge, [],
        "Current over-max IP replacement trial connections.");
    IP_RTT_ENTRIES = ("weaver_ip_rtt_ewma_entries", Gauge, [],
        "Number of tracked per-server/per-IP BODY RTT EWMAs.");
    IP_RTT_SLOWEST_MS = ("weaver_ip_rtt_ewma_slowest_ms", Gauge, [],
        "Slowest tracked per-IP BODY RTT EWMA in milliseconds.",
        deprecated_by = "weaver_ip_rtt_ewma_slowest_seconds");
    IP_RTT_SLOWEST_SECONDS = ("weaver_ip_rtt_ewma_slowest_seconds", Gauge, [],
        "Slowest tracked per-IP BODY RTT EWMA.");
    IP_TRIALS = ("weaver_ip_replacement_trials_total", Counter, ["outcome"],
        "IP replacement trial outcomes.");
    IP_OLD_CONNECTIONS_RETIRED = ("weaver_ip_replacement_old_connections_retired_total", Counter, [],
        "Old-IP connections retired after accepted replacement trials.");

    // ---- observed limiter and stalls --------------------------------------
    OBSERVED_LIMITER = ("weaver_pipeline_download_observed_limiter", Gauge, ["limiter"],
        "Observed downloader limiter derived from pressure, queue, and server permits; exactly one limiter is 1.");
    PRESSURE_STALLS = ("weaver_pipeline_download_pressure_stalls_total", Counter, [],
        "Hard pressure stalls started.");
    RESTART_DURABLE_LEAD_BLOCKED = ("weaver_pipeline_download_restart_durable_lead_blocked_total",
        Counter, [], "Restart durable lead dispatch blocks.");
    PRESSURE_STALL_DURATION = ("weaver_pipeline_download_pressure_stall_duration_seconds", Counter,
        [], "Cumulative completed hard pressure stall duration.",
        deprecated_by = "weaver_pipeline_download_pressure_stall_seconds_total");
    PRESSURE_STALL_SECONDS = ("weaver_pipeline_download_pressure_stall_seconds_total", Counter, [],
        "Cumulative completed hard pressure stall duration.");
    PRESSURE_CURRENT_STALL = ("weaver_pipeline_download_pressure_current_stall_seconds", Gauge, [],
        "Duration of the hard pressure stall in progress; zero when not stalled.");
    DIRECT_WRITE_EVICTIONS = ("weaver_pipeline_direct_write_evictions_total", Counter, [],
        "Direct write evictions.");
    DIRECT_STORE_SETS = ("weaver_direct_store_sets_total", Counter, ["event"],
        "RAR direct-store set lifecycle events.");

    // ---- post-download workers --------------------------------------------
    VERIFY_ACTIVE = ("weaver_pipeline_verify_active", Gauge, [], "Active verification workers.");
    REPAIR_ACTIVE = ("weaver_pipeline_repair_active", Gauge, [], "Active repair workers.");
    EXTRACT_ACTIVE = ("weaver_pipeline_extract_active", Gauge, [], "Active extraction workers.");
    DISK_WRITE_LATENCY_US = ("weaver_pipeline_disk_write_latency_microseconds", Gauge, [],
        "Disk write latency in microseconds.",
        deprecated_by = "weaver_pipeline_disk_write_latency_seconds");
    DISK_WRITE_LATENCY_SECONDS = ("weaver_pipeline_disk_write_latency_seconds", Gauge, [],
        "Disk write latency.");

    // ---- rates -------------------------------------------------------------
    CURRENT_DOWNLOAD_SPEED = ("weaver_pipeline_current_download_speed_bytes_per_second", Gauge, [],
        "Current download speed, as a recent-window rate (EMA).");
    ARTICLES_PER_SECOND = ("weaver_pipeline_articles_per_second", Gauge, [],
        "Article completion throughput, as a recent-window rate (EMA).");
    DECODE_RATE_MIB = ("weaver_pipeline_decode_rate_mebibytes_per_second", Gauge, [],
        "Decode throughput in MiB/s, as a recent-window rate (EMA).",
        deprecated_by = "weaver_pipeline_decode_rate_bytes_per_second");
    DECODE_RATE_BYTES = ("weaver_pipeline_decode_rate_bytes_per_second", Gauge, [],
        "Decode throughput, as a recent-window rate (EMA).");

    // ---- per-job -----------------------------------------------------------
    JOB_INFO = ("weaver_job_info", Gauge, ["job_id", "job_name", "category", "has_password"],
        "Descriptive labels for a job; always 1. Join on job_id to label the value series.");
    JOB_STATUS = ("weaver_job_status", Gauge, ["job_id", "status"],
        "The job's current status; always 1. Only the active status is emitted, so a job contributes one series rather than one per possible status.");
    JOB_PROGRESS_RATIO = ("weaver_job_progress_ratio", Gauge, ["job_id"],
        "Fractional job progress from 0 to 1.");
    JOB_TOTAL_BYTES = ("weaver_job_total_bytes", Gauge, ["job_id"],
        "Expected total bytes for the job.");
    JOB_DOWNLOADED_BYTES = ("weaver_job_downloaded_bytes", Gauge, ["job_id"],
        "Downloaded bytes for the job.");
    JOB_OPTIONAL_RECOVERY_BYTES = ("weaver_job_optional_recovery_bytes", Gauge, ["job_id"],
        "Optional recovery bytes available for the job.");
    JOB_OPTIONAL_RECOVERY_DOWNLOADED_BYTES = ("weaver_job_optional_recovery_downloaded_bytes",
        Gauge, ["job_id"], "Optional recovery bytes downloaded for the job.");
    JOB_FAILED_BYTES = ("weaver_job_failed_bytes", Gauge, ["job_id"],
        "Permanently failed bytes for the job.");
    JOB_HEALTH_PER_MILLE = ("weaver_job_health_per_mille", Gauge, ["job_id"],
        "Job health in per-mille.");
    JOB_CREATED_AT_SECONDS = ("weaver_job_created_at_seconds", Gauge, ["job_id"],
        "Unix creation timestamp for the job.");

    // ---- per-server health --------------------------------------------------
    SERVER_INFO = ("weaver_server_info", Gauge,
        ["server_id", "server", "host", "port", "tls", "priority", "backfill"],
        "Static per-server configuration; always 1.");
    SERVER_STATE = ("weaver_server_state", Gauge, ["server_id", "server", "state"],
        "Server health state as a state-set; exactly one state per server is 1.");
    SERVER_STATE_REASON = ("weaver_server_state_reason", Gauge, ["server_id", "server", "reason"],
        "Why the server is cooling down or disabled; 'none' while healthy or degraded.");
    SERVER_STATE_UNTIL_SECONDS = ("weaver_server_state_until_seconds", Gauge,
        ["server_id", "server"],
        "Unix timestamp at which the current cooldown or disable expires; zero when neither applies.");
    SERVER_DISABLED_TOTAL = ("weaver_server_disabled_total", Counter, ["server_id", "server"],
        "Times this server has been disabled since process start.");
    SERVER_SUCCESS_TOTAL = ("weaver_server_success_total", Counter, ["server_id", "server"],
        "Total successful operations per server.");
    SERVER_FAILURE_TOTAL = ("weaver_server_failure_total", Counter, ["server_id", "server"],
        "Total failed operations per server.");
    SERVER_CONSECUTIVE_FAILURES = ("weaver_server_consecutive_failures", Gauge,
        ["server_id", "server"], "Current run of consecutive failures per server.");
    SERVER_LATENCY_MS = ("weaver_server_latency_ms", Gauge, ["server_id", "server"],
        "EWMA latency in milliseconds per server.",
        deprecated_by = "weaver_server_latency_seconds");
    SERVER_LATENCY_SECONDS = ("weaver_server_latency_seconds", Gauge, ["server_id", "server"],
        "EWMA request latency per server.");
    SERVER_CONNECTIONS_AVAILABLE = ("weaver_server_connections_available", Gauge,
        ["server_id", "server"], "Available connection permits per server.");
    SERVER_CONNECTIONS_ACTIVE = ("weaver_server_connections_active", Gauge,
        ["server_id", "server"], "Connections currently checked out per server.");
    SERVER_CONNECTIONS_MAX = ("weaver_server_connections_max", Gauge, ["server_id", "server"],
        "Maximum connections per server.");
    SERVER_CONNECTIONS_CONFIGURED = ("weaver_server_connections_configured", Gauge,
        ["server_id", "server"], "Operator-configured maximum connections per server.");
    SERVER_CONNECTIONS_EFFECTIVE = ("weaver_server_connections_effective", Gauge,
        ["server_id", "server"],
        "Runtime maximum connections after provider capacity adaptation.");
    SERVER_CAPACITY_PENALTY_MS = ("weaver_server_capacity_penalty_until_epoch_ms", Gauge,
        ["server_id", "server"],
        "Provider capacity penalty deadline in unix epoch milliseconds.",
        deprecated_by = "weaver_server_capacity_penalty_until_seconds");
    SERVER_CAPACITY_PENALTY_SECONDS = ("weaver_server_capacity_penalty_until_seconds", Gauge,
        ["server_id", "server"], "Provider capacity penalty deadline as a unix timestamp.");
    SERVER_CAPACITY_REDUCTIONS = ("weaver_server_capacity_reductions_total", Counter,
        ["server_id", "server"],
        "Runtime connection-cap reductions caused by provider rejections.");
    SERVER_PREMATURE_DEATHS = ("weaver_server_premature_deaths", Gauge, ["server_id", "server"],
        "Recent connections that died before reaching 60s of age.");
    NNTP_RUNTIME_GENERATION = ("weaver_nntp_runtime_generation", Gauge, [],
        "Active NNTP runtime generation.");

    // ---- per-server transfer policy ------------------------------------------
    SERVER_DOWNLOAD_LIFETIME_BYTES = ("weaver_server_download_lifetime_bytes", Counter,
        ["server_id", "server"], "Raw NNTP BODY bytes received per durable server.",
        deprecated_by = "weaver_server_download_bytes_total");
    SERVER_DOWNLOAD_BYTES = ("weaver_server_download_bytes_total", Counter,
        ["server_id", "server"], "Raw NNTP BODY bytes received per durable server.");
    SERVER_DOWNLOAD_RATE_LIMIT = ("weaver_server_download_rate_limit_bytes_per_second", Gauge,
        ["server_id", "server"],
        "Configured aggregate per-server BODY rate limit; zero is unlimited.");
    SERVER_DOWNLOAD_THROTTLE_SECONDS = ("weaver_server_download_throttle_seconds_total", Counter,
        ["server_id", "server"], "Time spent waiting on a per-server download rate limit.");
    SERVER_QUOTA_ENABLED = ("weaver_server_download_quota_enabled", Gauge, ["server_id", "server"],
        "Whether a per-server BODY quota is enabled.");
    SERVER_QUOTA_LIMIT_BYTES = ("weaver_server_download_quota_limit_bytes", Gauge,
        ["server_id", "server"],
        "Configured per-server BODY quota for the current window; 0 when no quota is configured, so clamp the denominator before dividing by it.");
    SERVER_QUOTA_USED_BYTES = ("weaver_server_download_quota_used_bytes", Gauge,
        ["server_id", "server"], "BODY bytes charged in the current server quota window.");
    SERVER_QUOTA_RESERVED_BYTES = ("weaver_server_download_quota_reserved_bytes", Gauge,
        ["server_id", "server"], "Estimated BODY bytes reserved by in-flight work.");
    SERVER_QUOTA_REMAINING_BYTES = ("weaver_server_download_quota_remaining_bytes", Gauge,
        ["server_id", "server"],
        "Remaining admissible BODY bytes in the current server quota window.");
    SERVER_QUOTA_BLOCKED = ("weaver_server_download_quota_blocked", Gauge, ["server_id", "server"],
        "Whether the server currently rejects new BODY requests because of its quota.");

    // ---- extraction ------------------------------------------------------------
    EXTRACTION_REJECTIONS = ("weaver_extraction_rejections_total", Counter, ["reason"],
        "Archive entries refused by extraction guardrails, by reason.");

    // ---- post-processing --------------------------------------------------------
    PP_QUEUE_DEPTH = ("weaver_post_processing_queue_depth", Gauge, [],
        "Jobs waiting for a post-processing slot.");
    PP_ACTIVE_ATTEMPTS = ("weaver_post_processing_active_attempts", Gauge, [],
        "Post-processing scripts currently running.");
    PP_ATTEMPT_DURATION = ("weaver_post_processing_attempt_duration_seconds", Summary, [],
        "Wall-clock duration of completed post-processing script executions.");
    PP_ATTEMPT_RESULTS = ("weaver_post_processing_attempt_results", Counter, ["result"],
        "Completed post-processing script executions by outcome.",
        deprecated_by = "weaver_post_processing_attempts_total");
    PP_ATTEMPTS = ("weaver_post_processing_attempts_total", Counter, ["result"],
        "Completed post-processing script executions by outcome.");
    PP_OUTPUT_TRUNCATIONS_LEGACY = ("weaver_post_processing_output_truncations", Counter, [],
        "Post-processing script executions whose captured output was truncated.",
        deprecated_by = "weaver_post_processing_output_truncations_total");
    PP_OUTPUT_TRUNCATIONS = ("weaver_post_processing_output_truncations_total", Counter, [],
        "Post-processing script executions whose captured output was truncated.");

    // ---- per-server article outcomes ---------------------------------------------
    SERVER_ARTICLE_ATTEMPTS = ("weaver_server_article_attempts_total", Counter,
        ["server_id", "server", "outcome", "recovery"],
        "Article fetch attempts per server by outcome, split by whether the attempt came from the recovery (PAR2 top-up) queue. Every combination is emitted, so the series pre-exist at zero.");
    SERVER_ARTICLE_LATENCY = ("weaver_server_article_latency_seconds", Histogram,
        ["server_id", "server"], "Per-server article fetch latency.");

    // ---- job lifecycle --------------------------------------------------------------
    JOBS_SUBMITTED = ("weaver_jobs_submitted_total", Counter, ["origin", "category"],
        "Jobs accepted into the queue, by intake origin and category. Category is empty when the job has none.");
    JOBS_FINISHED = ("weaver_jobs_finished_total", Counter, ["result", "category"],
        "Jobs that reached a terminal state, by result and category.");
    JOB_DURATION = ("weaver_job_duration_seconds", Histogram, ["result"],
        "End-to-end wall-clock time from job submission to its terminal state.");
    JOB_STAGE_DURATION = ("weaver_job_stage_duration_seconds", Histogram, ["stage"],
        "Wall-clock time each job spent in a pipeline stage.");
    VERIFICATIONS = ("weaver_verifications_total", Counter, ["result"],
        "PAR2 verification outcomes.");
    REPAIRS = ("weaver_repairs_total", Counter, ["result"], "PAR2 repair outcomes.");
    REPAIR_SLICES_REPAIRED = ("weaver_repair_slices_repaired_total", Counter, [],
        "PAR2 slices reconstructed by repair.");
    EXTRACTIONS = ("weaver_extractions_total", Counter, ["result"], "Archive extraction outcomes.");
    FILES_MISSING = ("weaver_files_missing_total", Counter, [],
        "Files that could not be completed because segments were unavailable.");
    MISSING_SEGMENTS = ("weaver_missing_segments_total", Counter, [],
        "Segments that were unavailable across every configured server.");
    BYTES_BY_CATEGORY = ("weaver_bytes_downloaded_by_category_total", Counter, ["category"],
        "Decoded bytes attributed to each category. Category is empty when the job has none.");

    // ---- pipeline stage histograms ----------------------------------------------------
    DISK_WRITE_DURATION = ("weaver_pipeline_disk_write_duration_seconds", Histogram, [],
        "Time spent in individual disk write calls.");
    DECODE_TASK_DURATION = ("weaver_pipeline_decode_task_duration_seconds", Histogram, [],
        "Time spent decoding one article. Conditional: emitted only once the decode path has recorded a measurement.");
    EXTRACT_MEMBER_DURATION = ("weaver_pipeline_extract_member_duration_seconds", Histogram, [],
        "Time spent extracting one archive member. Conditional: emitted only once the extraction path has recorded a measurement.");

    // ---- database runtime ---------------------------------------------------------------
    DB_RUNTIME_INFO = ("weaver_db_runtime_info", Gauge, ["engine"],
        "Database engine in use; always 1.");
    DB_RUNTIME_CONCURRENCY = ("weaver_db_runtime_concurrency", Gauge, [],
        "Configured database executor concurrency; always 1 for the serialized sqlite worker.");
    DB_RUNTIME_IN_FLIGHT = ("weaver_db_runtime_in_flight", Gauge, [],
        "Database operations submitted and not yet answered.");
    DB_RUNTIME_BLOCKED_SUBMISSIONS = ("weaver_db_runtime_blocked_submissions_total", Counter, [],
        "Database submissions that had to wait on a full executor queue.");
    DB_OP_DURATION = ("weaver_db_op_duration_seconds", Histogram, ["engine"],
        "Database operation latency, measured around the executor round-trip.");

    // ---- process ---------------------------------------------------------------------------
    // Standard process collector names, deliberately unprefixed so the usual
    // dashboards and recording rules work unchanged. Every one is conditional:
    // platforms differ in what they can answer cheaply, and a value that cannot
    // be read is omitted rather than reported as zero.
    PROCESS_CPU_SECONDS = ("process_cpu_seconds_total", Counter, [],
        "Total user and system CPU time spent by this process.");
    PROCESS_RESIDENT_MEMORY = ("process_resident_memory_bytes", Gauge, [],
        "Resident set size of this process.");
    PROCESS_VIRTUAL_MEMORY = ("process_virtual_memory_bytes", Gauge, [],
        "Virtual memory size of this process.");
    PROCESS_OPEN_FDS = ("process_open_fds", Gauge, [],
        "File descriptors currently open by this process.");
    PROCESS_MAX_FDS = ("process_max_fds", Gauge, [],
        "Maximum file descriptors this process may open.");
    PROCESS_THREADS = ("process_threads", Gauge, [], "Threads in this process.");
    PROCESS_START_TIME_SECONDS = ("process_start_time_seconds", Gauge, [],
        "Unix start time of this process. Falls back to when the metrics exporter was built if the platform cannot report it.");

    // ---- filesystem capacity -------------------------------------------------------------------
    DISK_TOTAL_BYTES = ("weaver_disk_total_bytes", Gauge, ["role", "path"],
        "Total capacity of the filesystem backing a configured directory role.");
    DISK_AVAILABLE_BYTES = ("weaver_disk_available_bytes", Gauge, ["role", "path"],
        "Space available to this process on the filesystem backing a configured directory role.");

    // ---- HTTP surface ----------------------------------------------------------------------------
    HTTP_REQUESTS = ("weaver_http_requests_total", Counter, ["route", "method", "status"],
        "HTTP requests served, by route template, method and status code. Routes are templates rather than raw paths, and status codes outside a known allow-list collapse to their class boundary; scrapes of /metrics itself are not counted.");
    HTTP_REQUEST_DURATION = ("weaver_http_request_duration_seconds", Histogram, ["route"],
        "HTTP request latency by route template, measured around the handler and every layer wrapped about it.");
}
