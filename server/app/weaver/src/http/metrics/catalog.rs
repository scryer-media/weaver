// The metric catalogue: every family `/metrics` can emit, as data.
//
// Nothing outside this file may invent a metric name. Because the encoder
// only accepts a [`MetricFamily`], adding a series means adding an entry here,
// which in turn means it gets a HELP, a TYPE and a declared label set.

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

        // Every family the exporter is capable of emitting.
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

    // ---- egress download quotas ------------------------------------------
    EGRESS_QUOTA_ENABLED = ("weaver_egress_download_quota_enabled", Gauge, ["egress_id"],
        "Whether an egress has a download quota.");
    EGRESS_QUOTA_LIMIT_BYTES = ("weaver_egress_download_quota_limit_bytes", Gauge, ["egress_id"],
        "Configured egress download quota for the current window; 0 when no quota is configured, so clamp the denominator before dividing by it.");
    EGRESS_QUOTA_USED_BYTES = ("weaver_egress_download_quota_used_bytes", Gauge, ["egress_id"],
        "Bytes charged in the current egress quota window.");
    EGRESS_QUOTA_RESERVED_BYTES = ("weaver_egress_download_quota_reserved_bytes", Gauge,
        ["egress_id"], "Bytes reserved for in-flight downloads against the egress quota.");
    EGRESS_QUOTA_REMAINING_BYTES = ("weaver_egress_download_quota_remaining_bytes", Gauge,
        ["egress_id"], "Remaining admissible bytes in the current egress quota window.");
    EGRESS_QUOTA_BLOCKED = ("weaver_egress_download_quota_blocked", Gauge, ["egress_id"],
        "Whether the egress currently turns downloads away because of its quota.");
    DOWNLOAD_BLOCK_WINDOW_END_SECONDS = ("weaver_download_quota_block_window_end_seconds", Gauge, [],
        "When the quota holding downloads resets, as a unix timestamp; 0 while no quota holds them.");

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
    WRITE_PENDING_BYTES = ("weaver_pipeline_write_pending_bytes", Gauge, [],
        "Total pending write bytes, including UU spool files.");
    UU_SPOOLED_BYTES = ("weaver_pipeline_uu_spooled_bytes", Gauge, [],
        "Disk-backed UU bytes waiting for their missing prefix.");
    UU_SPOOLED_SEGMENTS = ("weaver_pipeline_uu_spooled_segments", Gauge, [],
        "Disk-backed UU segments waiting for their missing prefix.");
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
    LANE_INFLIGHT_BYTES = ("weaver_pipeline_download_lane_inflight_bytes", Gauge, [],
        "Raw article bytes handed to download lanes and not yet answered, summed across lanes.");
    DOWNLOAD_JOBS_ELIGIBLE = ("weaver_pipeline_download_jobs_eligible", Gauge, [],
        "Jobs competing for articles: the hot job plus every job queued behind it with fetchable work.");
    DOWNLOAD_JOBS_HOT = ("weaver_pipeline_download_jobs_hot", Gauge, [],
        "One while a job is being downloaded, zero while none is.");
    LANE_PARKS = ("weaver_pipeline_download_lane_parks_total", Counter, ["reason"],
        "Article download lane parks by reason.");
    LANE_LEASE_ITEMS = ("weaver_pipeline_download_lane_lease_items_total", Counter, [],
        "Article work items leased to download lanes.");
    LANE_REFILLS = ("weaver_pipeline_download_lane_refills_total", Counter, ["result"],
        "Lane refill scheduler decisions.");
    SCHEDULER_HANDOUTS = ("weaver_pipeline_download_scheduler_handouts_total", Counter, ["kind"],
        "Article handouts cut by the per-server scheduler, by kind.");
    SCHEDULER_IDLE_WITH_SERVABLE = ("weaver_pipeline_download_scheduler_idle_with_servable_total", Counter, [],
        "Scheduler answered idle while a job still had a servable article on that server; always zero when the scheduler is correct.");
    SCHEDULER_SCAN_ITEMS_SKIPPED = ("weaver_pipeline_download_scheduler_scan_items_skipped_total", Counter, [],
        "Queued articles a scheduler queue scan looked at and passed over.");
    SCHEDULER_SCAN_NO_MATCH = ("weaver_pipeline_download_scheduler_scan_no_match_total", Counter, [],
        "Scheduler queue scans that found no article the asking server may fetch.");
    SCHEDULER_HOT_BLOCKED = ("weaver_pipeline_download_scheduler_hot_blocked_total", Counter, ["clause"],
        "Times the hot job had nothing for the asking server, once per clause that refused it.");

    // ---- BODY pipelining proof -------------------------------------------
    BODY_PROOF_EVENTS = ("weaver_pipeline_body_proof_events_total", Counter, ["event"],
        "BODY pipelining proof events.");
    BODY_REPLAY_ITEMS = ("weaver_pipeline_body_replay_items_total", Counter, [],
        "BODY items returned unresolved after a lane reset or failure.");

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
    DEOBFUSCATED_MEMBERS = ("weaver_deobfuscated_members_renamed_total", Counter, [],
        "Delivered files renamed out of an obfuscated in-archive member name.");

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
    SERVER_SOCKETS = ("weaver_server_sockets", Gauge,
        ["server_id", "server", "phase"], "Physical socket occupancy, including idle and closing transports.");
    SERVER_LOCAL_ADMISSION_DENIALS = ("weaver_server_local_admission_denials_total", Counter,
        ["server_id", "server"], "Physical socket acquisitions denied locally.");
    SERVER_PROVIDER_REFUSALS = ("weaver_server_provider_refusals_total", Counter,
        ["server_id", "server"], "Actual connection-capacity refusals received from the provider.");
    SERVER_RECOVERY_EPOCH = ("weaver_server_recovery_epoch", Gauge,
        ["server_id", "server"], "Current transport recovery epoch.");
    SERVER_RECOVERY_PROBE = ("weaver_server_recovery_probe", Gauge,
        ["server_id", "server"], "Whether a demanded transport recovery probe is owned.");
    SERVER_RECOVERY_WAIT_SECONDS = ("weaver_server_recovery_wait_seconds", Gauge,
        ["server_id", "server"], "Remaining delay before a fresh demanded recovery attempt.");
    SERVER_CAPACITY_PENALTY_MS = ("weaver_server_capacity_penalty_until_epoch_ms", Gauge,
        ["server_id", "server"],
        "Provider over-limit holdoff deadline in unix epoch milliseconds.",
        deprecated_by = "weaver_server_capacity_penalty_until_seconds");
    SERVER_CAPACITY_PENALTY_SECONDS = ("weaver_server_capacity_penalty_until_seconds", Gauge,
        ["server_id", "server"], "Provider over-limit holdoff deadline as a unix timestamp.");
    SERVER_ADDRESS_INFO = ("weaver_server_address_info", Gauge,
        ["server_id", "server", "address"],
        "Resolved server addresses; 1 for the address new connections are pinned to, 0 for the rest.");
    SERVER_ADDRESS_CONNECT_SECONDS = ("weaver_server_address_connect_seconds", Gauge,
        ["server_id", "server", "address"], "Smoothed TCP connect time per resolved server address.");
    SERVER_ADDRESS_RACES = ("weaver_server_address_races_total", Counter,
        ["server_id", "server", "outcome"], "Address races run to pick the address new connections dial, by outcome.");
    SERVER_ADDRESS_REPINS = ("weaver_server_address_repins_total", Counter,
        ["server_id", "server", "reason"], "Changes of the pinned server address, by what decided them: the reason a race ran, or measured delivery.");
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
        "Jobs waiting for a turn to run a post-processing script.");
    PP_ACTIVE_ATTEMPTS = ("weaver_post_processing_active_attempts", Gauge, [],
        "Post-processing scripts currently running.");
    PP_ATTEMPT_DURATION = ("weaver_post_processing_attempt_duration_seconds", Histogram,
        ["script", "kind", "waited", "status"],
        "Wall-clock duration of finished script runs, by script, run kind, whether the job waited on the run, and status. Every run kind records here.");
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
    PP_RUNS_STARTED = ("weaver_post_processing_runs_started_total", Counter, ["kind"],
        "Script runs that took a slot, by run kind.");
    PP_RUNS_RUNNING = ("weaver_post_processing_runs_running", Gauge, ["kind"],
        "Script runs holding a slot now, by run kind.");
    PP_WAITING_FOR_SLOT = ("weaver_post_processing_waiting_for_slot", Gauge, ["kind"],
        "Script runs waiting for a slot now, by run kind.");
    PP_SLOT_WAITS = ("weaver_post_processing_slot_waits_total", Counter, ["kind"],
        "Script runs that entered a slot wait, by run kind.");
    PP_CONCURRENCY_LIMIT = ("weaver_post_processing_concurrency_limit", Gauge, [],
        "Configured number of script runs that may hold a slot at once.");
    PP_SLOTS_IN_USE = ("weaver_post_processing_slots_in_use", Gauge, [],
        "Script runs holding a slot in the shared pool, as the pool counts them.");
    PP_SLOTS_WAITING = ("weaver_post_processing_slots_waiting", Gauge, [],
        "Script runs queued for a slot in the shared pool, as the pool counts them.");
    PP_QUEUE_EVENT_BACKLOG = ("weaver_post_processing_queue_event_backlog", Gauge, [],
        "Queue events recorded for scripts and not yet started.");
    PP_REFUSALS = ("weaver_post_processing_refusals_total", Counter, ["kind"],
        "Script events refused before any script ran, by run kind.");
    PP_JOB_SUMMARIES = ("weaver_post_processing_job_summaries_total", Counter, ["summary"],
        "Jobs whose post-processing pass finished, by the pass's summary.");
    PP_INTERRUPTED_RECOVERED = ("weaver_post_processing_interrupted_recovered_total", Counter, [],
        "Jobs whose post-processing pass was cut off by a restart and resumed.");
    PP_RETAINED_RUNS = ("weaver_post_processing_retained_runs_total", Counter, ["action"],
        "Script run records by what retention did with them: kept, discarded on arrival, or pruned.");
    PP_SCRIPT_RUNS = ("weaver_post_processing_script_runs_total", Counter,
        ["script", "kind", "adapter", "waited", "status"],
        "Finished script runs by script, run kind, adapter, whether the job waited on the run, and status.");
    PP_SCRIPT_NONZERO_EXITS = ("weaver_post_processing_script_nonzero_exits_total", Counter, ["script"],
        "Script runs that exited with a non-zero code.");
    PP_SCRIPT_LAST_EXIT_CODE = ("weaver_post_processing_script_last_exit_code", Gauge, ["script"],
        "Exit code of the script's most recent run that exited; absent until one has.");
    PP_SCRIPT_LAST_DURATION = ("weaver_post_processing_script_last_duration_seconds", Gauge, ["script"],
        "Duration of the script's most recent finished run.");
    PP_SCRIPT_LAST_RUN_TIMESTAMP = ("weaver_post_processing_script_last_run_timestamp_seconds", Gauge,
        ["script"], "When the script's most recent run finished, as a unix timestamp.");
    PP_SCRIPT_LAST_RUN_STATUS = ("weaver_post_processing_script_last_run_status", Gauge,
        ["script", "status"], "Status of the script's most recent run; exactly one status is 1.");
    PP_SCRIPT_OUTPUT_BYTES = ("weaver_post_processing_script_output_bytes_total", Counter, ["script"],
        "Output bytes the script's runs produced before compression.");
    PP_SCRIPT_OUTPUT_STORED_BYTES = ("weaver_post_processing_script_output_stored_bytes_total", Counter,
        ["script"], "Compressed output bytes kept for the script's runs.");
    PP_SCRIPT_OUTPUT_TRUNCATIONS = ("weaver_post_processing_script_output_truncations_total", Counter,
        ["script"], "Script runs whose captured output was truncated.");
    PP_SCRIPT_PRUNED_RUNS = ("weaver_post_processing_script_pruned_runs_total", Counter, ["script"],
        "Script run records retention removed to make room for newer runs.");

    // ---- schedules ----------------------------------------------------------------
    SCHEDULE_EVALUATIONS = ("weaver_schedule_evaluations_total", Counter, [],
        "Schedule evaluator ticks.");
    SCHEDULE_ACTIONS = ("weaver_schedule_actions_total", Counter, ["action", "track", "outcome"],
        "Schedule actions applied, by action kind, the state track it drives, and outcome.");
    SCHEDULE_ONE_SHOT_FIRES = ("weaver_schedule_one_shot_fires_total", Counter, ["action"],
        "One-shot schedule rules that came due.");
    SCHEDULE_REPLAYS = ("weaver_schedule_replays_total", Counter, ["reason"],
        "Schedule actions applied as a replay rather than at their time, by reason.");
    SCHEDULE_CLOCK_JUMPS = ("weaver_schedule_clock_jumps_total", Counter, ["evaluator"],
        "Wall-clock jumps the schedule evaluators detected.");
    SCHEDULE_ADMISSION_HOLD = ("weaver_schedule_admission_hold", Gauge, ["reason"],
        "Why the schedule holds download admission; exactly one reason is 1, and none means no hold.");
    SCHEDULE_RULES = ("weaver_schedule_rules", Gauge, ["action"],
        "Configured schedule rules by action kind.");
    SCHEDULE_RULES_ENABLED = ("weaver_schedule_rules_enabled", Gauge, ["action"],
        "Enabled schedule rules by action kind.");
    SCHEDULE_RULE_ENABLED = ("weaver_schedule_rule_enabled", Gauge, ["rule_id", "action"],
        "Whether each configured schedule rule is enabled.");
    SCHEDULE_RULE_FIRES = ("weaver_schedule_rule_fires_total", Counter, ["rule_id", "outcome"],
        "Actions each configured schedule rule applied, by outcome.");
    SCHEDULE_RULE_LAST_FIRE = ("weaver_schedule_rule_last_fire_timestamp_seconds", Gauge, ["rule_id"],
        "When each schedule rule last applied its action, as a unix timestamp; 0 until it has.");
    SCHEDULE_RULE_LAST_OUTCOME = ("weaver_schedule_rule_last_outcome", Gauge, ["rule_id", "outcome"],
        "Outcome of each schedule rule's last action; all zero until it has fired.");

    // ---- networking -----------------------------------------------------------------
    NETWORK_LEG_STATE = ("weaver_network_leg_state", Gauge,
        ["consumer", "position", "egress_id", "state"],
        "Health of each route leg; exactly one state is 1.");
    NETWORK_LEG_STATE_SINCE = ("weaver_network_leg_state_since_timestamp_seconds", Gauge,
        ["consumer", "position", "egress_id"],
        "When a scrape first saw the leg's current state, as a unix timestamp.");
    NETWORK_LEG_LIVE = ("weaver_network_leg_live", Gauge, ["consumer", "position", "egress_id"],
        "Whether the leg's consumer has a live dialer; 0 for a configured but dormant route.");
    NETWORK_LEG_TARGET = ("weaver_network_leg_target_connections", Gauge,
        ["consumer", "position", "egress_id"], "Connections the leg is weighted to carry.");
    NETWORK_LEG_OPEN = ("weaver_network_leg_open_connections", Gauge,
        ["consumer", "position", "egress_id"], "Connections open on the leg.");
    NETWORK_LEG_OPENING = ("weaver_network_leg_opening_connections", Gauge,
        ["consumer", "position", "egress_id"], "Connections being set up on the leg.");
    NETWORK_LEG_THROUGHPUT = ("weaver_network_leg_throughput_bytes_per_second", Gauge,
        ["consumer", "position", "egress_id"],
        "Bytes per second the leg carried over its last few seconds.");
    NETWORK_LEG_RUNG = ("weaver_network_leg_rung", Gauge, ["consumer", "position", "egress_id"],
        "Ladder rung the leg's last connection used, counted from 0; absent on a leg without one.");
    NETWORK_RUNG_STATE = ("weaver_network_rung_state", Gauge,
        ["consumer", "position", "rung", "state"],
        "State of each ladder rung; exactly one state is 1.");
    NETWORK_EGRESS_HEALTH = ("weaver_network_egress_health", Gauge, ["egress_id", "health"],
        "Health of each configured egress; exactly one health is 1.");
    NETWORK_EGRESS_HEALTH_SINCE = ("weaver_network_egress_health_since_timestamp_seconds", Gauge,
        ["egress_id"], "When a scrape first saw the egress's current health, as a unix timestamp.");
    NETWORK_EGRESS_ENABLED = ("weaver_network_egress_enabled", Gauge, ["egress_id"],
        "Whether each configured egress is enabled.");
    NETWORK_EGRESS_DIALS = ("weaver_network_egress_dials_total", Counter, ["egress_id", "result"],
        "Route leg connection attempts by egress and result; ignored results are no evidence about the path.");
    NETWORK_EGRESS_COOLDOWNS = ("weaver_network_egress_cooldowns_total", Counter, ["egress_id"],
        "Times a route leg on the egress entered a cooldown.");
    EGRESS_DOWNLOAD_BYTES = ("weaver_egress_download_bytes_total", Counter, ["egress_id"],
        "Raw NNTP BODY bytes received over each egress.");
    EGRESS_DOWNLOAD_RATE_LIMIT = ("weaver_egress_download_rate_limit_bytes_per_second", Gauge,
        ["egress_id"], "Configured download rate limit of each egress; 0 means unlimited.");
    EGRESS_DOWNLOAD_THROTTLE_SECONDS = ("weaver_egress_download_throttle_seconds_total", Counter,
        ["egress_id"], "Time spent waiting on an egress download rate limit.");
    NETWORK_POOL_MEMBERS = ("weaver_network_pool_members", Gauge, ["pool_id", "egress_id", "stage"],
        "Members of each proxy pool stage by stage: total, warmed or blocked.");
    NETWORK_POOL_CONNECTIONS = ("weaver_network_pool_connections", Gauge,
        ["pool_id", "egress_id", "state"], "Connections across a proxy pool's members, open or opening.");
    NETWORK_POOL_RACES = ("weaver_network_pool_races_total", Counter, ["pool_id", "egress_id", "result"],
        "Proxy pool connection races, won or failed.");
    NETWORK_POOL_MEMBER_HANDSHAKE = ("weaver_network_pool_member_session_handshake_seconds", Gauge,
        ["pool_id", "egress_id", "member_id"],
        "Duration of each pool member's last session handshake; absent until it has one.");
    NETWORK_PROXIES = ("weaver_network_proxies", Gauge, ["kind", "enabled"],
        "Configured proxy profiles by kind and whether they are enabled.");
    NETWORK_DNS_RESOLUTIONS = ("weaver_network_dns_resolutions_total", Counter, ["resolver", "result"],
        "Host name lookups made for routed connections, by resolver and result.");
    NETWORK_RUNG_COOLDOWNS = ("weaver_network_rung_cooldowns_total", Counter, ["rung"],
        "Times a ladder rung entered a cooldown, by rung position.");
    NETWORK_RUNG_FALLBACKS = ("weaver_network_rung_fallbacks_total", Counter, ["rung"],
        "Connections a ladder made on a rung past the first, by the rung that connected.");
    NETWORK_LEG_REVOCATIONS = ("weaver_network_leg_revocations_total", Counter, [],
        "Route stages revoked, closing the connections that ran over them.");
    TUNNEL_STREAMS = ("weaver_tunnel_streams_total", Counter, ["kind", "result"],
        "Streams opened through a proxy hop, by proxy kind and result.");
    TUNNEL_SESSION_PREPARES = ("weaver_tunnel_session_prepares_total", Counter, ["kind", "result"],
        "Session preparations on session-based proxy hops, by proxy kind and result.");
    TUNNEL_SESSION_RETIREMENTS = ("weaver_tunnel_session_retirements_total", Counter, ["kind"],
        "Idle proxy sessions retired, by proxy kind.");

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

    // ---- buffer pool -----------------------------------------------------------------------
    BUFFER_POOL_IN_USE_BYTES = ("weaver_runtime_buffer_pool_in_use_bytes", Gauge, ["tier"],
        "Pre-allocated article buffer bytes currently checked out, by tier.");
    BUFFER_POOL_TOTAL_BYTES = ("weaver_runtime_buffer_pool_total_bytes", Gauge, ["tier"],
        "Pre-allocated article buffer bytes the pool owns, by tier.");
    BUFFER_POOL_WAITS = ("weaver_runtime_buffer_pool_waits_total", Counter, [],
        "Buffer acquisitions that had to wait for a buffer to come back.");

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

    // ---- PAR3 recovery -----------------------------------------------------------------------------
    // Every series here is fed from `MetricsSnapshot::par3`, which is a plain
    // fixed-size struct copy taken on the pipeline's own snapshot tick. The
    // phase and reason label values are the stringified enum codes; the codes
    // themselves are the exported gauge contract and are never renumbered.
    PAR3_ADMISSION_REFUSED = ("weaver_par3_admission_refused_total", Counter, ["reason"],
        "PAR3 memory reservations refused, by the budget that refused them.");
    PAR3_WAITING_FOR_MEMORY = ("weaver_par3_waiting_for_memory", Gauge, [],
        "PAR3 jobs currently parked waiting for a peer work unit to release memory.");
    PAR3_WAITING_FOR_MEMORY_SECONDS = ("weaver_par3_waiting_for_memory_seconds_total", Counter, [],
        "Cumulative time PAR3 jobs spent parked waiting for memory.");
    PAR3_SPILLS = ("weaver_par3_spills_to_disk_total", Counter, [],
        "PAR3 sources reconstructed on disk after an in-memory image was refused.");
    PAR3_RESERVED_BYTES = ("weaver_par3_reserved_bytes", Gauge, [],
        "Bytes reserved against the PAR3 engine memory budget as of the last work-unit handback.");
    PAR3_RESERVED_PEAK_BYTES = ("weaver_par3_reserved_peak_bytes", Gauge, [],
        "Peak bytes ever reserved against the PAR3 engine memory budget.");
    PAR3_RETAINED_BYTES = ("weaver_par3_retained_bytes", Gauge, [],
        "Bytes reserved against weaver's own PAR3 host-state and retained-payload budgets.");
    PAR3_STRIPE_BYTES = ("weaver_par3_effective_stripe_bytes", Gauge, [],
        "Repair stripe size requested of the engine for the most recent PAR3 work unit.");

    PAR3_LEDGER_BYTES = ("weaver_par3_ledger_bytes", Gauge, ["category"],
        "Bytes the PAR3 engine has reserved in each of its own memory categories, as of the last work-unit handback.");
    PAR3_LEDGER_PEAK_BYTES = ("weaver_par3_ledger_peak_bytes", Gauge, ["category"],
        "Highest bytes the PAR3 engine ever held in each of its own memory categories.");
    PAR3_ENGINE_ADMITTED_STRIPE_BYTES = ("weaver_par3_engine_admitted_stripe_bytes", Gauge, [],
        "Codec stripe size the PAR3 engine actually admitted for its most recent pass.");
    PAR3_ENGINE_ADMITTED_STRIPE_BUFFERS = ("weaver_par3_engine_admitted_stripe_buffers", Gauge, [],
        "Stripe-sized buffers the PAR3 engine's most recent admission covered.");
    PAR3_ENGINE_ADMITTED_OUTPUT_TILE = ("weaver_par3_engine_admitted_output_tile_rows", Gauge, [],
        "Lost rows the PAR3 engine solved per tile in its most recent Cauchy pass.");
    PAR3_ENGINE_ADMITTED_VERIFY_BATCH = ("weaver_par3_engine_admitted_verify_batch_files", Gauge, [],
        "Files in the PAR3 engine's most recently admitted verification batch.");
    PAR3_ENGINE_ADMITTED_WORKERS = ("weaver_par3_engine_admitted_workers", Gauge, [],
        "Worker threads the PAR3 engine's most recently admitted pool holds; one means it ran on the calling thread.");
    PAR3_ENGINE_ADMITTED_WINDOW_BYTES = ("weaver_par3_engine_admitted_window_bytes", Gauge, [],
        "Bytes in the PAR3 engine's most recently admitted sequential read window.");
    PAR3_ENGINE_REFUSALS = ("weaver_par3_engine_refusals_total", Counter, ["cause"],
        "Admissions the PAR3 engine refused, by the cause it gave.");
    PAR3_ENGINE_NARROWED = ("weaver_par3_engine_narrowed_total", Counter, ["width"],
        "Stages the PAR3 engine ran narrower than configured, by the width that had to give. These are not stalls.");
    PAR3_ENGINE_REREAD_BYTES = ("weaver_par3_engine_reread_bytes_total", Counter, [],
        "Source bytes the PAR3 engine read again because one pass could not hold the block.");
    PAR3_ENGINE_RECONSTRUCTED_BYTES = ("weaver_par3_engine_reconstructed_bytes_total", Counter, [],
        "Bytes the PAR3 codec reconstructed and scattered into staged output.");
    PAR3_ENGINE_CACHE_ENTRIES = ("weaver_par3_engine_cache_entries", Gauge, [],
        "Entries the PAR3 engine's admission and deduplication caches currently retain.");
    PAR3_ENGINE_CACHE_BYTES = ("weaver_par3_engine_cache_bytes", Gauge, [],
        "Bytes the PAR3 engine's admission and deduplication cache entries are charged.");
    PAR3_ENGINE_CODEC_TRANSFORM_CALLS = ("weaver_par3_engine_codec_transform_calls_total", Counter, [],
        "Additive-transform invocations the PAR3 codec made, including the ones a pruning plan split.");
    PAR3_ENGINE_CODEC_BUTTERFLIES = ("weaver_par3_engine_codec_butterflies_total", Counter, [],
        "Butterflies the PAR3 codec executed across those transform calls.");
    PAR3_ENGINE_CODEC_BUTTERFLIES_SKIPPED = ("weaver_par3_engine_codec_butterflies_skipped_total", Counter, [],
        "Butterflies a PAR3 transform plan established were not needed; add these to the executed ones for what the same decode would have cost unpruned.");
    PAR3_ENGINE_CODEC_MULTIPLY_ACCUMULATES = ("weaver_par3_engine_codec_multiply_accumulates_total", Counter, [],
        "Symbol-wide multiply-accumulates those butterflies performed: one per butterfly per symbol in the rows it joined.");
    PAR3_ENGINE_CODEC_FACTORS_COMPUTED = ("weaver_par3_engine_codec_factors_computed_total", Counter, [],
        "Cauchy code-matrix elements the PAR3 codec computed.");
    PAR3_ENGINE_CODEC_FACTORS_REUSED = ("weaver_par3_engine_codec_factors_reused_total", Counter, [],
        "Cauchy code-matrix elements the PAR3 codec answered from an admitted table instead of recomputing.");

    PAR3_PENDING_WORK = ("weaver_par3_pending_work_depth", Gauge, [],
        "PAR3 work units queued and not yet dispatched.");
    PAR3_PENDING_BYTES = ("weaver_par3_pending_work_bytes", Gauge, [],
        "Host bytes leased by queued PAR3 work units.");
    PAR3_DISPATCH_REFUSED = ("weaver_par3_dispatch_refused_total", Counter, ["reason"],
        "Dispatch attempts that left queued PAR3 work waiting, by what was unavailable.");
    PAR3_DISPATCH_WAIT_SECONDS = ("weaver_par3_dispatch_wait_seconds_total", Counter, [],
        "Cumulative time PAR3 work units spent queued before a worker took them.");
    PAR3_WORKERS_ADMITTED = ("weaver_par3_workers_admitted", Gauge, [],
        "CPU workers currently allotted across the PAR3 work slots.");
    PAR3_IN_FLIGHT = ("weaver_par3_in_flight", Gauge, [],
        "PAR3 work units currently owned by a blocking worker.");

    PAR3_RECOVERY_WINDOWS = ("weaver_par3_recovery_windows_admitted_total", Counter, [],
        "PAR3 recovery acquisition windows admitted.");
    PAR3_RECOVERY_ARTICLES = ("weaver_par3_recovery_articles_total", Counter, ["outcome"],
        "Recovery articles a PAR3 window requested, and how they ended.");
    PAR3_RECOVERY_NEEDED_BYTES = ("weaver_par3_recovery_needed_bytes", Gauge, [],
        "Recovery bytes the deficient cohorts of the current assessments are short by.");
    PAR3_COHORTS_WITH_DEFICIT = ("weaver_par3_cohorts_with_deficit", Gauge, [],
        "Cohorts across the current assessments that still need recovery blocks.");
    PAR3_WAITS = ("weaver_par3_waits_total", Counter, ["reason"],
        "PAR3 acquisition passes that could not proceed, by what they were waiting on.");

    PAR3_SOURCE_READ_BYTES = ("weaver_par3_source_read_bytes_total", Counter, [],
        "Bytes the PAR3 engine read from published sources.");
    PAR3_SOURCE_READS = ("weaver_par3_source_reads_total", Counter, [],
        "Read calls the PAR3 engine made against published sources.");
    PAR3_REASSESSMENTS = ("weaver_par3_reassessments_total", Counter, [],
        "PAR3 assessment work units dispatched.");
    PAR3_REASSESSMENTS_ZERO_READ = ("weaver_par3_reassessments_zero_read_total", Counter, [],
        "PAR3 assessments that completed without reading a single source byte.");
    PAR3_REVERIFY_GENERATION_CHANGED = ("weaver_par3_reverify_generation_changed_total", Counter, [],
        "PAR3 work units whose job was written to before they handed back.");
    PAR3_VERIFY_SERIAL_FALLBACK = ("weaver_par3_verify_serial_fallback_total", Counter, [],
        "PAR3 work units re-run alone after a shared-CPU attempt hit native pressure.");
    PAR3_READER_CACHE = ("weaver_par3_reader_cache_total", Counter, ["event"],
        "Encrypted virtual reader cache hits and evictions.");

    PAR3_DONOR_SEARCHES = ("weaver_par3_donor_searches_total", Counter, [],
        "Bounded PAR3 donor searches dispatched.");
    PAR3_DONOR_SEARCH_EXHAUSTED = ("weaver_par3_donor_search_exhausted_total", Counter, [],
        "PAR3 donor searches that exhausted their candidate list.");
    PAR3_DONOR_READ_BYTES = ("weaver_par3_donor_read_bytes_total", Counter, [],
        "Bytes read while searching for PAR3 donor blocks.");
    PAR3_DONOR_TIME_CAP = ("weaver_par3_donor_time_cap_hits_total", Counter, [],
        "PAR3 extents whose donor search stopped on its own per-extent time cap.");

    PAR3_STAGE_CALLS = ("weaver_par3_stage_calls_total", Counter, ["stage"],
        "PAR3 engine stage invocations, folded in once per work-unit handback.");
    PAR3_STAGE_SECONDS = ("weaver_par3_stage_seconds_total", Counter, ["stage"],
        "Time the PAR3 engine spent in each stage, folded in once per work-unit handback.");
    PAR3_FILE_SYNC_CALLS = ("weaver_par3_file_sync_calls_total", Counter, [],
        "Output file sync calls the PAR3 engine made.");
    PAR3_FILE_SYNC_SECONDS = ("weaver_par3_file_sync_seconds_total", Counter, [],
        "Time the PAR3 engine spent syncing output files.");

    PAR3_REPAIRS_STARTED = ("weaver_par3_repairs_started_total", Counter, [],
        "PAR3 repair work units dispatched.");
    PAR3_OUTCOMES = ("weaver_par3_outcomes_total", Counter, ["class"],
        "Typed PAR3 verdicts reached, by class. Waits and informational classes are counted here too, so this is not a failure count.");
    PAR3_REPAIR_COHORTS = ("weaver_par3_repair_cohorts_processed_total", Counter, [],
        "Cohorts a PAR3 repair actually processed.");
    PAR3_REPAIR_BYTES = ("weaver_par3_repair_bytes_reconstructed_total", Counter, [],
        "Protected bytes the PAR3 engine reconstructed, taken from its own Repair stage total at handback.");
    PAR3_REPAIR_CANCELLED = ("weaver_par3_repair_cancelled_total", Counter, [],
        "PAR3 repairs that were cancelled or interrupted.");
    PAR3_READBACK_WINDOWS = ("weaver_par3_readback_windows_total", Counter, [],
        "PAR3 readback work units dispatched after installation.");
    PAR3_READBACK_BYTES = ("weaver_par3_readback_bytes_total", Counter, [],
        "Bytes read back from installed PAR3 output.");
    PAR3_READBACK_MISMATCH = ("weaver_par3_readback_mismatch_total", Counter, [],
        "PAR3 readbacks whose placement was refused after installation.");

    PAR3_PACKETS = ("weaver_par3_packets_total", Counter, ["outcome"],
        "PAR3 packets the carrier scanner authenticated or rejected.");
    PAR3_CARRIER_RANGES_UNAVAILABLE = ("weaver_par3_carrier_ranges_unavailable_total", Counter, [],
        "Holes the PAR3 carrier scanner had to seek past.");
    PAR3_CARRIER_DAMAGED_BYTES = ("weaver_par3_carrier_damaged_bytes_total", Counter, [],
        "Readable PAR3 carrier bytes that produced no authenticated packet.");

    PAR3_SLOT_PHASE = ("weaver_par3_slot_phase", Gauge, ["slot", "phase"],
        "Current phase of each PAR3 work slot; exactly one phase is 1 per slot.");
    PAR3_SLOT_JOB = ("weaver_par3_slot_job_id", Gauge, ["slot"],
        "Job id owning each PAR3 work slot; zero when the slot is free.");
    PAR3_SLOT_PHASE_SECONDS = ("weaver_par3_slot_phase_seconds", Gauge, ["slot"],
        "Time each PAR3 work slot has been in its current phase.");
    PAR3_SLOT_STALL_SECONDS = ("weaver_par3_slot_current_stall_seconds", Gauge, ["slot"],
        "Age of the stall in progress on each PAR3 work slot; zero when it is not stalled.");
    PAR3_STALLS = ("weaver_par3_stalls_total", Counter, [],
        "PAR3 work slot stalls started.");
    PAR3_STALL_SECONDS = ("weaver_par3_stall_seconds_total", Counter, [],
        "Cumulative time PAR3 work slots spent stalled, credited when each stall clears.");
    PAR3_CURRENT_STALL_SECONDS = ("weaver_par3_current_stall_seconds", Gauge, [],
        "Oldest PAR3 stall in progress across the work slots; zero when nothing is stalled.");
    PAR3_STALL_THRESHOLD_SECONDS = ("weaver_par3_stall_threshold_seconds", Gauge, [],
        "Age at which a PAR3 work slot with no progress is declared stalled.");

    // ---- HTTP surface ----------------------------------------------------------------------------
    HTTP_REQUESTS = ("weaver_http_requests_total", Counter, ["route", "method", "status"],
        "HTTP requests served, by route template, method and status code. Routes are templates rather than raw paths, and status codes outside a known allow-list collapse to their class boundary; scrapes of /metrics itself are not counted.");
    HTTP_REQUEST_DURATION = ("weaver_http_request_duration_seconds", Histogram, ["route"],
        "HTTP request latency by route template, measured around the handler and every layer wrapped about it.");
}
