# Weaver metrics

Weaver exposes Prometheus text-format metrics at **`GET /metrics`** on the same
port as the web UI and GraphQL API (`9090` by default).

## Scraping

### Authentication

`/metrics` requires a readable API key or session by default. Set
`WEAVER_METRICS_AUTH_REQUIRED=0` to expose it unauthenticated — only do that
when the port is already restricted to your monitoring network, because the
endpoint reveals job names, categories, and server hostnames.

With auth left on, give Prometheus an API key with read scope:

```yaml
scrape_configs:
  - job_name: weaver
    scrape_interval: 30s
    scrape_timeout: 10s
    metrics_path: /metrics
    authorization:
      type: Bearer
      credentials_file: /etc/prometheus/weaver-api-key
    static_configs:
      - targets: ["weaver:9090"]
```

### Scrape interval

**30 seconds is the recommended interval**, and 15s is the floor. Each scrape
takes a snapshot of the scheduler, briefly locks the NNTP health map to read
counters, and samples free disk space. None of
that is on the download hot path, but it is not free either — scraping every
second buys nothing, because the rate gauges are already smoothed over a
multi-second window.

If you scrape faster than 15s, expect the throughput gauges
(`weaver_pipeline_current_download_speed_bytes_per_second`,
`weaver_pipeline_articles_per_second`) to look noisier than the true transfer
rate rather than more accurate.

## Label conventions

### `server_id` versus `server`

Every per-server series carries both:

- **`server_id`** — the durable server identity. Stable across restarts,
  hostname changes, and reordering of the server list. **Join and alert on
  this.**
- **`server`** — `host:port`. Human-readable, and what the pre-0.8.4 metrics
  used, so it is kept for existing dashboards. It changes if you re-point a
  server at a new hostname.

Static per-server configuration lives on `weaver_server_info`:

```promql
weaver_server_connections_available
  * on(server_id) group_left(host, priority, backfill) weaver_server_info
```

### Per-job series and the info-metric pattern

Descriptive job labels live on `weaver_job_info`; the value series carry only
`job_id`. That keeps a rename or a status transition from minting a whole new
set of time series:

```promql
weaver_job_downloaded_bytes
  * on(job_id) group_left(job_name, category) weaver_job_info
```

Job status is its own state-set, so you can still filter by it:

```promql
weaver_job_progress_ratio
  and on(job_id) (weaver_job_status{status="downloading"} == 1)
```

### Controlling per-job cardinality

Per-job series are the only unbounded label dimension in the exporter — the
runtime remembers up to a thousand finished jobs. The `[metrics]` config table
decides which of them are exported:

```toml
[metrics]
# "active" (default) — only jobs that are still moving
# "all"              — every job the runtime remembers, finished ones included
# "off"              — no per-job series at all
per_job_series = "active"
```

`weaver_pipeline_jobs{status}` reports the aggregate queue mix under every
setting, so `off` still leaves you able to alert on queue depth and failures.

### State-sets

Several metrics are *state-sets*: one series per possible value, exactly one of
which is `1`. `weaver_pipeline_download_gate`, `weaver_server_state`,
`weaver_pipeline_download_pressure_state` and
`weaver_pipeline_download_observed_limiter` all work this way. Query them by
comparing to 1 rather than by looking for a missing series:

```promql
weaver_pipeline_download_gate{reason="isp_cap"} == 1
```

`weaver_job_status` is the exception: it emits only the job's **current**
status, at value 1, rather than a zero for each of the other thirteen. A job
therefore contributes one status series instead of fourteen. Select on the
label rather than on the value:

```promql
weaver_job_status{status="downloading"}
```

### Histograms

`weaver_server_article_latency_seconds`, `weaver_job_duration_seconds`,
`weaver_job_stage_duration_seconds`, `weaver_pipeline_disk_write_duration_seconds`,
`weaver_pipeline_decode_task_duration_seconds`,
`weaver_pipeline_extract_member_duration_seconds`,
`weaver_db_op_duration_seconds` and `weaver_http_request_duration_seconds` are
Prometheus histograms: `_bucket{le=…}` (cumulative), `_sum` and `_count`. Use
`histogram_quantile` over a rate of the buckets, and always aggregate `le`
together with whatever else you group by:

```promql
histogram_quantile(
  0.95,
  sum by (le, server) (rate(weaver_server_article_latency_seconds_bucket[5m]))
)
```

Two of them appear only once their stage has run:
`weaver_pipeline_decode_task_duration_seconds` arrives with the first decoded
article, and `weaver_pipeline_extract_member_duration_seconds` with the first
extracted archive member. Until then they are absent entirely rather than
reporting a fabricated all-zero series that would read as "nothing is slow", so
a freshly started process, or one that has only ever handled jobs with no
archives, will not have them. Do not alert on their absence.

### Process metrics

`process_cpu_seconds_total`, `process_resident_memory_bytes`,
`process_virtual_memory_bytes`, `process_open_fds`, `process_max_fds`,
`process_threads` and `process_start_time_seconds` deliberately carry **no
`weaver_` prefix**: they are the standard process-collector names, so the usual
dashboards and recording rules work unchanged. Every one is optional — a value
this platform cannot read cheaply is omitted rather than reported as zero — so
guard panels with `or` / `absent` if you support several operating systems.

## Useful PromQL

**Download throughput** — prefer the counter rate over the speed gauge for
graphs and alerts; the gauge is a smoothed instantaneous reading:

```promql
rate(weaver_pipeline_bytes_downloaded_total[1m])
```

**Bytes downloaded today:**

```promql
increase(weaver_pipeline_bytes_downloaded_total[24h])
```

**Per-server share of traffic:**

```promql
sum by (server_id) (rate(weaver_server_download_bytes_total[5m]))
```

**Per-server availability over the last hour** (fraction of time healthy):

```promql
avg_over_time(weaver_server_state{state="healthy"}[1h])
```

**Server error ratio:**

```promql
rate(weaver_server_failure_total[5m])
  / clamp_min(
      rate(weaver_server_success_total[5m]) + rate(weaver_server_failure_total[5m]),
      1
    )
```

**Queue mix:**

```promql
sum by (status) (weaver_pipeline_jobs)
```

**Why is the downloader slow right now?** — the observed limiter answers in one
series:

```promql
weaver_pipeline_download_observed_limiter == 1
```

The `limiter` label is one of, in the order the exporter decides them:

| Value | Meaning |
| --- | --- |
| `gated` | Paused, or held by a gate — see `weaver_pipeline_download_gate`. |
| `idle` | Nothing to do: no required work queued and nothing in flight. |
| `infrastructure_unavailable` | Work is waiting and nothing is on the wire because every remaining article is parked on an NNTP infrastructure retry. Look at `weaver_server_state` and `weaver_pipeline_parked_infrastructure_work`, not at Weaver's own queues. |
| `pressure_limited` | Backpressure is holding dispatch — `weaver_pipeline_download_pressure_reason` says whether decode or write is behind. |
| `decode_lagging` | Downloads are outrunning the decoders. |
| `network_limited` | Every server connection permit is checked out; more connections or more servers is the only lever. |
| `active` | Downloading with headroom on every axis. |
| `dispatch_limited` | Work is queued, nothing is in flight, and none of the above explains it — a scheduler-side limit. |

**Is the pipeline gated, and by what?**

```promql
weaver_pipeline_download_gate == 1
```

**Connection saturation per server:**

```promql
1 - (
  weaver_server_connections_available
    / clamp_min(weaver_server_connections_max, 1)
)
```

**Quota consumption:**

```promql
weaver_server_download_quota_used_bytes
  / clamp_min(weaver_server_download_quota_limit_bytes, 1)
```

**Post-processing failure rate:**

```promql
rate(weaver_post_processing_attempts_total{result="failed"}[15m])
```

**Article miss ratio per server** — the single most useful provider-quality
signal; a server whose `not_found` share is climbing has retention trouble:

```promql
sum by (server) (rate(weaver_server_article_attempts_total{outcome="not_found"}[5m]))
  / clamp_min(sum by (server) (rate(weaver_server_article_attempts_total[5m])), 0.001)
```

**Article latency p95 per server:**

```promql
histogram_quantile(
  0.95,
  sum by (le, server) (rate(weaver_server_article_latency_seconds_bucket[5m]))
)
```

**Job throughput and failure share:**

```promql
sum by (result) (rate(weaver_jobs_finished_total[1h]))
```

**Slowest pipeline stage (p95):**

```promql
histogram_quantile(
  0.95,
  sum by (le, stage) (rate(weaver_job_stage_duration_seconds_bucket[1h]))
)
```

**Traffic by category:**

```promql
sum by (category) (rate(weaver_bytes_downloaded_by_category_total[1h]))
```

**Free space on the completed-downloads filesystem:**

```promql
weaver_disk_available_bytes{role="complete"}
  / clamp_min(weaver_disk_total_bytes{role="complete"}, 1)
```

**HTTP error rate by route:**

```promql
sum by (route) (rate(weaver_http_requests_total{status=~"5.."}[5m]))
  / clamp_min(sum by (route) (rate(weaver_http_requests_total[5m])), 0.001)
```

**Database executor saturation** — `in_flight` pinned at `concurrency` with
`blocked_submissions_total` climbing means the database is the bottleneck:

```promql
weaver_db_runtime_in_flight / clamp_min(weaver_db_runtime_concurrency, 1)
```

## Deprecations

These names still render with their original values, and their `# HELP` text
names the replacement. **They are scheduled for removal in the next minor
release** — migrate dashboards and alerts now.

| Deprecated | Replacement | Why |
| --- | --- | --- |
| `weaver_server_download_lifetime_bytes` | `weaver_server_download_bytes_total` | counters end in `_total` |
| `weaver_pipeline_download_pressure_stall_duration_seconds` | `weaver_pipeline_download_pressure_stall_seconds_total` | counters end in `_total` |
| `weaver_post_processing_attempt_results` | `weaver_post_processing_attempts_total` | counters end in `_total` |
| `weaver_post_processing_output_truncations` | `weaver_post_processing_output_truncations_total` | counters end in `_total` |
| `weaver_pipeline_download_lanes_active_total` | `weaver_pipeline_download_lanes` | gauges do not end in `_total` |
| `weaver_server_latency_ms` | `weaver_server_latency_seconds` | base units are seconds |
| `weaver_ip_rtt_ewma_slowest_ms` | `weaver_ip_rtt_ewma_slowest_seconds` | base units are seconds |
| `weaver_pipeline_disk_write_latency_microseconds` | `weaver_pipeline_disk_write_latency_seconds` | base units are seconds |
| `weaver_pipeline_hot_dispatch_underfill_milliseconds` | `weaver_pipeline_hot_dispatch_underfill_seconds` | base units are seconds |
| `weaver_server_capacity_penalty_until_epoch_ms` | `weaver_server_capacity_penalty_until_seconds` | timestamps are unix seconds |
| `weaver_pipeline_decode_rate_mebibytes_per_second` | `weaver_pipeline_decode_rate_bytes_per_second` | base units are bytes |
| `weaver_pipeline_hot_dispatch_recent_expansion_improvement_percent` | `weaver_pipeline_hot_dispatch_recent_expansion_improvement_ratio` | ratios are 0–1, not percent |
| `weaver_pipeline_hot_dispatch_last_expansion_kind` | `weaver_pipeline_hot_dispatch_expansion_kind` | opaque code replaced by a state-set |
| `weaver_pipeline_hot_dispatch_best_mode_block_reason` | `weaver_pipeline_hot_dispatch_best_mode_block` | opaque code replaced by a state-set |

### Breaking change: `weaver_server_state`

`weaver_server_state` used to be an unlabelled boolean per server: `1` for
healthy, `0` for anything else. It is now a **state-set** carrying a `state`
label with one series per state, so the old query

```promql
weaver_server_state == 0        # was: "server is not healthy"
```

matches every non-current state on every server and is no longer meaningful.
Use instead:

```promql
weaver_server_state{state="healthy"} == 0
```

The reason a server left `healthy` is available separately on
`weaver_server_state_reason`, when the health machine recorded one.

## Metric catalogue

Every family the exporter can emit. This table is checked against the
exporter's own catalogue by a unit test, so it cannot silently drift.

Regenerate it with:

```sh
cargo test -p weaver regenerate_docs_metrics_table -- --ignored --nocapture
```

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `weaver_build_info` | gauge | `version`, `commit`, `target_arch`, `target_os`, `decoder_tier`, `database_backend`, `tls_backend` | Static build information; always 1. |
| `weaver_start_time_seconds` | gauge | — | Unix timestamp at which this process started. |
| `weaver_pipeline_paused` | gauge | — | Whether the entire pipeline is globally paused. |
| `weaver_pipeline_download_gate` | gauge | `reason` | Current global download gate; exactly one reason is 1. |
| `weaver_pipeline_scheduled_speed_limit_bytes_per_second` | gauge | — | Speed limit imposed by the active schedule; zero means no scheduled limit. |
| `weaver_bandwidth_cap_enabled` | gauge | — | Whether the ISP bandwidth cap policy is enabled. |
| `weaver_bandwidth_cap_used_bytes` | gauge | — | Current ISP bandwidth cap usage in bytes. |
| `weaver_bandwidth_cap_limit_bytes` | gauge | — | Configured ISP bandwidth cap limit in bytes. |
| `weaver_bandwidth_cap_remaining_bytes` | gauge | — | Remaining ISP bandwidth cap bytes in the active window. |
| `weaver_bandwidth_cap_reserved_bytes` | gauge | — | Bytes conservatively reserved for in-flight downloads against the active cap window. |
| `weaver_bandwidth_cap_window_end_seconds` | gauge | — | Active ISP bandwidth cap window end as a unix timestamp. |
| `weaver_pipeline_jobs` | gauge | `status` | Number of known jobs by status. |
| `weaver_duplicate_admission_decisions_total` | counter | `origin`, `status` | Duplicate admission decisions by intake origin and public status. |
| `weaver_semantic_duplicate_lifecycle_total` | counter | `event` | Semantic duplicate arbitration lifecycle events. |
| `weaver_pipeline_bytes_downloaded_total` | counter | — | Total bytes downloaded by the pipeline. |
| `weaver_pipeline_bytes_decoded_total` | counter | — | Total bytes decoded by the pipeline. |
| `weaver_pipeline_bytes_committed_total` | counter | — | Total bytes committed to disk by the pipeline. |
| `weaver_pipeline_segments_downloaded_total` | counter | — | Total segments downloaded. |
| `weaver_pipeline_segments_decoded_total` | counter | — | Total segments decoded. |
| `weaver_pipeline_segments_committed_total` | counter | — | Total segments committed. |
| `weaver_pipeline_segments_retried_total` | counter | — | Total segments retried. |
| `weaver_pipeline_segments_failed_permanent_total` | counter | — | Total segments permanently failed. |
| `weaver_pipeline_parked_infrastructure_work` | gauge | — | Segments parked while NNTP infrastructure is unavailable. |
| `weaver_nntp_generation_recovery_requeues_total` | counter | — | Segments requeued after stale NNTP generation failures. |
| `weaver_nntp_capacity_probe_attempts_total` | counter | — | Adaptive-capacity provider connection probes attempted. |
| `weaver_nntp_capacity_probe_successes_total` | counter | — | Adaptive-capacity probes that restored one connection. |
| `weaver_nntp_capacity_probe_rejections_total` | counter | — | Adaptive-capacity probes rejected by provider limits. |
| `weaver_nntp_capacity_probe_transport_failures_total` | counter | — | Adaptive-capacity probes that failed during transport setup. |
| `weaver_nntp_capacity_probe_stale_generation_total` | counter | — | Probe results ignored after an NNTP generation replacement. |
| `weaver_pipeline_download_failures_total` | counter | `kind` | Failed article download attempts by kind. |
| `weaver_pipeline_articles_not_found_total` | counter | — | Total articles not found. |
| `weaver_pipeline_decode_errors_total` | counter | — | Total decode errors. |
| `weaver_pipeline_crc_errors_total` | counter | — | Total CRC errors. |
| `weaver_pipeline_download_queue_depth` | gauge | — | Download queue depth. |
| `weaver_pipeline_active_downloads` | gauge | — | Active article downloads. |
| `weaver_pipeline_active_decodes` | gauge | — | Active decode tasks. |
| `weaver_pipeline_decode_pending` | gauge | — | Decode pending queue depth. |
| `weaver_pipeline_decode_pending_bytes` | gauge | — | Raw article bytes queued for decode. |
| `weaver_pipeline_decode_active_bytes` | gauge | — | Raw article bytes currently being decoded. |
| `weaver_pipeline_commit_pending` | gauge | — | Commit pending queue depth. |
| `weaver_pipeline_recovery_queue_depth` | gauge | — | Recovery queue depth. |
| `weaver_pipeline_write_buffered_bytes` | gauge | — | Buffered write bytes. |
| `weaver_pipeline_write_buffered_segments` | gauge | — | Buffered write segments. |
| `weaver_pipeline_decode_pressure_soft_limit_bytes` | gauge | — | Decode soft pressure limit in bytes. |
| `weaver_pipeline_decode_pressure_hard_limit_bytes` | gauge | — | Decode hard pressure limit in bytes. |
| `weaver_pipeline_write_pressure_soft_limit_bytes` | gauge | — | Write soft pressure limit in bytes. |
| `weaver_pipeline_write_pressure_hard_limit_bytes` | gauge | — | Write hard pressure limit in bytes. |
| `weaver_pipeline_download_pressure_state` | gauge | `state` | Download backpressure state; exactly one state is 1. |
| `weaver_pipeline_download_pressure_reason` | gauge | `reason` | Download backpressure reason; exactly one reason is 1. |
| `weaver_pipeline_hot_dispatch_job_id` | gauge | — | Current hot-dispatch job id, or 0 when no job owns hot dispatch. |
| `weaver_pipeline_hot_dispatch_mode` | gauge | `mode` | Current hot-dispatch sharing mode; exactly one mode is 1. |
| `weaver_pipeline_hot_dispatch_underfill_milliseconds` | gauge | — | Current hot-job unused-capacity underfill window age in milliseconds. **Deprecated — use `weaver_pipeline_hot_dispatch_underfill_seconds`.** |
| `weaver_pipeline_hot_dispatch_underfill_seconds` | gauge | — | Current hot-job unused-capacity underfill window age. |
| `weaver_pipeline_hot_dispatch_lent_connections` | gauge | — | Active NNTP connection tasks lent to spillover jobs. |
| `weaver_pipeline_hot_dispatch_warmup_complete` | gauge | — | Whether the current hot-dispatch warmup gate is complete. |
| `weaver_pipeline_hot_dispatch_last_spillover_decision` | gauge | `decision` | Last hot-dispatch spillover decision; exactly one decision is 1. |
| `weaver_pipeline_hot_dispatch_spillover_decisions_total` | counter | `decision` | Hot-dispatch spillover decisions by reason. |
| `weaver_pipeline_hot_dispatch_speed_bytes_per_second` | gauge | — | Two-second hot-job BODY throughput. |
| `weaver_pipeline_hot_dispatch_last_expansion_kind` | gauge | — | Last hot-job expansion event kind, as an opaque numeric code. **Deprecated — use `weaver_pipeline_hot_dispatch_expansion_kind`.** |
| `weaver_pipeline_hot_dispatch_expansion_kind` | gauge | `kind` | Last hot-job expansion event kind; exactly one kind is 1. |
| `weaver_pipeline_hot_dispatch_last_expansion_speed_bytes_per_second` | gauge | `phase` | Last hot-job expansion before/after speeds. |
| `weaver_pipeline_hot_dispatch_exclusive_peak_bytes_per_second` | gauge | — | Peak hot-job speed observed while exclusive. |
| `weaver_pipeline_hot_dispatch_spillover_speed_bytes_per_second` | gauge | `phase` | Hot-job speed before and after the current spillover loan. |
| `weaver_pipeline_hot_dispatch_spillover_active_loans` | gauge | — | Active measured spillover loans. |
| `weaver_pipeline_hot_dispatch_recent_expansion_improvement_percent` | gauge | — | Best recent lane/pipeline expansion improvement, in percent. **Deprecated — use `weaver_pipeline_hot_dispatch_recent_expansion_improvement_ratio`.** |
| `weaver_pipeline_hot_dispatch_recent_expansion_improvement_ratio` | gauge | — | Best recent lane/pipeline expansion improvement as a ratio, where 0.1 is a 10% gain. |
| `weaver_pipeline_hot_dispatch_best_mode_block_reason` | gauge | — | Last best-mode spillover block reason, as an opaque numeric code. **Deprecated — use `weaver_pipeline_hot_dispatch_best_mode_block`.** |
| `weaver_pipeline_hot_dispatch_best_mode_block` | gauge | `reason` | Last best-mode spillover block reason; exactly one reason is 1. |
| `weaver_pipeline_download_lanes_active` | gauge | `mode` | Active article download lanes by pipelining mode. |
| `weaver_pipeline_download_lane_states_active` | gauge | `state` | Active article download lanes by scheduler state. |
| `weaver_pipeline_download_lanes_active_total` | gauge | — | Total active article download lanes. **Deprecated — use `weaver_pipeline_download_lanes`.** |
| `weaver_pipeline_download_lanes` | gauge | — | Total active article download lanes. |
| `weaver_pipeline_download_lane_parks_total` | counter | `reason` | Article download lane parks by reason. |
| `weaver_pipeline_download_lane_lease_items_total` | counter | — | Article work items leased to download lanes. |
| `weaver_pipeline_download_lane_refills_total` | counter | `result` | Lane refill scheduler decisions. |
| `weaver_pipeline_body_proof_events_total` | counter | `event` | BODY pipelining proof events. |
| `weaver_pipeline_body_replay_items_total` | counter | — | BODY items returned unresolved after a lane reset or failure. |
| `weaver_ip_replacement_trial_extra_connections` | gauge | — | Configured over-max IP replacement trial burst budget. |
| `weaver_ip_replacement_burst_active` | gauge | — | Whether an over-max IP replacement trial is active. |
| `weaver_ip_replacement_over_max_connections` | gauge | — | Current over-max IP replacement trial connections. |
| `weaver_ip_rtt_ewma_entries` | gauge | — | Number of tracked per-server/per-IP BODY RTT EWMAs. |
| `weaver_ip_rtt_ewma_slowest_ms` | gauge | — | Slowest tracked per-IP BODY RTT EWMA in milliseconds. **Deprecated — use `weaver_ip_rtt_ewma_slowest_seconds`.** |
| `weaver_ip_rtt_ewma_slowest_seconds` | gauge | — | Slowest tracked per-IP BODY RTT EWMA. |
| `weaver_ip_replacement_trials_total` | counter | `outcome` | IP replacement trial outcomes. |
| `weaver_ip_replacement_old_connections_retired_total` | counter | — | Old-IP connections retired after accepted replacement trials. |
| `weaver_pipeline_download_observed_limiter` | gauge | `limiter` | Observed downloader limiter derived from pressure, queue, and server permits; exactly one limiter is 1. |
| `weaver_pipeline_download_pressure_stalls_total` | counter | — | Hard pressure stalls started. |
| `weaver_pipeline_download_restart_durable_lead_blocked_total` | counter | — | Restart durable lead dispatch blocks. |
| `weaver_pipeline_download_pressure_stall_duration_seconds` | counter | — | Cumulative completed hard pressure stall duration. **Deprecated — use `weaver_pipeline_download_pressure_stall_seconds_total`.** |
| `weaver_pipeline_download_pressure_stall_seconds_total` | counter | — | Cumulative completed hard pressure stall duration. |
| `weaver_pipeline_download_pressure_current_stall_seconds` | gauge | — | Duration of the hard pressure stall in progress; zero when not stalled. |
| `weaver_pipeline_direct_write_evictions_total` | counter | — | Direct write evictions. |
| `weaver_direct_store_sets_total` | counter | `event` | RAR direct-store set lifecycle events. |
| `weaver_deobfuscated_members_renamed_total` | counter | — | Delivered files renamed out of an obfuscated in-archive member name. |
| `weaver_pipeline_verify_active` | gauge | — | Active verification workers. |
| `weaver_pipeline_repair_active` | gauge | — | Active repair workers. |
| `weaver_pipeline_extract_active` | gauge | — | Active extraction workers. |
| `weaver_pipeline_disk_write_latency_microseconds` | gauge | — | Disk write latency in microseconds. **Deprecated — use `weaver_pipeline_disk_write_latency_seconds`.** |
| `weaver_pipeline_disk_write_latency_seconds` | gauge | — | Disk write latency. |
| `weaver_pipeline_current_download_speed_bytes_per_second` | gauge | — | Current download speed, as a recent-window rate (EMA). |
| `weaver_pipeline_articles_per_second` | gauge | — | Article completion throughput, as a recent-window rate (EMA). |
| `weaver_pipeline_decode_rate_mebibytes_per_second` | gauge | — | Decode throughput in MiB/s, as a recent-window rate (EMA). **Deprecated — use `weaver_pipeline_decode_rate_bytes_per_second`.** |
| `weaver_pipeline_decode_rate_bytes_per_second` | gauge | — | Decode throughput, as a recent-window rate (EMA). |
| `weaver_job_info` | gauge | `job_id`, `job_name`, `category`, `has_password` | Descriptive labels for a job; always 1. Join on job_id to label the value series. |
| `weaver_job_status` | gauge | `job_id`, `status` | Job status as a state-set; exactly one status per job_id is 1. |
| `weaver_job_progress_ratio` | gauge | `job_id` | Fractional job progress from 0 to 1. |
| `weaver_job_total_bytes` | gauge | `job_id` | Expected total bytes for the job. |
| `weaver_job_downloaded_bytes` | gauge | `job_id` | Downloaded bytes for the job. |
| `weaver_job_optional_recovery_bytes` | gauge | `job_id` | Optional recovery bytes available for the job. |
| `weaver_job_optional_recovery_downloaded_bytes` | gauge | `job_id` | Optional recovery bytes downloaded for the job. |
| `weaver_job_failed_bytes` | gauge | `job_id` | Permanently failed bytes for the job. |
| `weaver_job_health_per_mille` | gauge | `job_id` | Job health in per-mille. |
| `weaver_job_created_at_seconds` | gauge | `job_id` | Unix creation timestamp for the job. |
| `weaver_server_info` | gauge | `server_id`, `server`, `host`, `port`, `tls`, `priority`, `backfill` | Static per-server configuration; always 1. |
| `weaver_server_state` | gauge | `server_id`, `server`, `state` | Server health state as a state-set; exactly one state per server is 1. |
| `weaver_server_state_reason` | gauge | `server_id`, `server`, `reason` | Why the server is cooling down or disabled; 'none' while healthy or degraded. |
| `weaver_server_state_until_seconds` | gauge | `server_id`, `server` | Unix timestamp at which the current cooldown or disable expires; zero when neither applies. |
| `weaver_server_disabled_total` | counter | `server_id`, `server` | Times this server has been disabled since process start. |
| `weaver_server_success_total` | counter | `server_id`, `server` | Total successful operations per server. |
| `weaver_server_failure_total` | counter | `server_id`, `server` | Total failed operations per server. |
| `weaver_server_consecutive_failures` | gauge | `server_id`, `server` | Current run of consecutive failures per server. |
| `weaver_server_latency_ms` | gauge | `server_id`, `server` | EWMA latency in milliseconds per server. **Deprecated — use `weaver_server_latency_seconds`.** |
| `weaver_server_latency_seconds` | gauge | `server_id`, `server` | EWMA request latency per server. |
| `weaver_server_connections_available` | gauge | `server_id`, `server` | Available connection permits per server. |
| `weaver_server_connections_active` | gauge | `server_id`, `server` | Connections currently checked out per server. |
| `weaver_server_connections_max` | gauge | `server_id`, `server` | Maximum connections per server. |
| `weaver_server_connections_configured` | gauge | `server_id`, `server` | Operator-configured maximum connections per server. |
| `weaver_server_connections_effective` | gauge | `server_id`, `server` | Runtime maximum connections after provider capacity adaptation. |
| `weaver_server_capacity_penalty_until_epoch_ms` | gauge | `server_id`, `server` | Provider capacity penalty deadline in unix epoch milliseconds. **Deprecated — use `weaver_server_capacity_penalty_until_seconds`.** |
| `weaver_server_capacity_penalty_until_seconds` | gauge | `server_id`, `server` | Provider capacity penalty deadline as a unix timestamp. |
| `weaver_server_capacity_reductions_total` | counter | `server_id`, `server` | Runtime connection-cap reductions caused by provider rejections. |
| `weaver_server_premature_deaths` | gauge | `server_id`, `server` | Recent connections that died before reaching 60s of age. |
| `weaver_nntp_runtime_generation` | gauge | — | Active NNTP runtime generation. |
| `weaver_server_download_lifetime_bytes` | counter | `server_id`, `server` | Raw NNTP BODY bytes received per durable server. **Deprecated — use `weaver_server_download_bytes_total`.** |
| `weaver_server_download_bytes_total` | counter | `server_id`, `server` | Raw NNTP BODY bytes received per durable server. |
| `weaver_server_download_rate_limit_bytes_per_second` | gauge | `server_id`, `server` | Configured aggregate per-server BODY rate limit; zero is unlimited. |
| `weaver_server_download_throttle_seconds_total` | counter | `server_id`, `server` | Time spent waiting on a per-server download rate limit. |
| `weaver_server_download_quota_enabled` | gauge | `server_id`, `server` | Whether a per-server BODY quota is enabled. |
| `weaver_server_download_quota_limit_bytes` | gauge | `server_id`, `server` | Configured per-server BODY quota for the current window; zero when disabled. |
| `weaver_server_download_quota_used_bytes` | gauge | `server_id`, `server` | BODY bytes charged in the current server quota window. |
| `weaver_server_download_quota_reserved_bytes` | gauge | `server_id`, `server` | Estimated BODY bytes reserved by in-flight work. |
| `weaver_server_download_quota_remaining_bytes` | gauge | `server_id`, `server` | Remaining admissible BODY bytes in the current server quota window. |
| `weaver_server_download_quota_blocked` | gauge | `server_id`, `server` | Whether the server currently rejects new BODY requests because of its quota. |
| `weaver_extraction_rejections_total` | counter | `reason` | Archive entries refused by extraction guardrails, by reason. |
| `weaver_post_processing_queue_depth` | gauge | — | Jobs waiting for a post-processing slot. |
| `weaver_post_processing_active_attempts` | gauge | — | Post-processing scripts currently running. |
| `weaver_post_processing_attempt_duration_seconds` | summary | — | Wall-clock duration of completed post-processing script executions. |
| `weaver_post_processing_attempt_results` | counter | `result` | Completed post-processing script executions by outcome. **Deprecated — use `weaver_post_processing_attempts_total`.** |
| `weaver_post_processing_attempts_total` | counter | `result` | Completed post-processing script executions by outcome. |
| `weaver_post_processing_output_truncations` | counter | — | Post-processing script executions whose captured output was truncated. **Deprecated — use `weaver_post_processing_output_truncations_total`.** |
| `weaver_post_processing_output_truncations_total` | counter | — | Post-processing script executions whose captured output was truncated. |
| `weaver_server_article_attempts_total` | counter | `server_id`, `server`, `outcome`, `recovery` | Article fetch attempts per server by outcome, split by whether the attempt came from the recovery (PAR2 top-up) queue. Every combination is emitted, so the series pre-exist at zero. |
| `weaver_server_article_latency_seconds` | histogram | `server_id`, `server` | Per-server article fetch latency. |
| `weaver_jobs_submitted_total` | counter | `origin`, `category` | Jobs accepted into the queue, by intake origin and category. Category is empty when the job has none. |
| `weaver_jobs_finished_total` | counter | `result`, `category` | Jobs that reached a terminal state, by result and category. |
| `weaver_job_duration_seconds` | histogram | `result` | End-to-end wall-clock time from job submission to its terminal state. |
| `weaver_job_stage_duration_seconds` | histogram | `stage` | Wall-clock time each job spent in a pipeline stage. |
| `weaver_verifications_total` | counter | `result` | PAR2 verification outcomes. |
| `weaver_repairs_total` | counter | `result` | PAR2 repair outcomes. |
| `weaver_repair_slices_repaired_total` | counter | — | PAR2 slices reconstructed by repair. |
| `weaver_extractions_total` | counter | `result` | Archive extraction outcomes. |
| `weaver_files_missing_total` | counter | — | Files that could not be completed because segments were unavailable. |
| `weaver_missing_segments_total` | counter | — | Segments that were unavailable across every configured server. |
| `weaver_bytes_downloaded_by_category_total` | counter | `category` | Decoded bytes attributed to each category. Category is empty when the job has none. |
| `weaver_pipeline_disk_write_duration_seconds` | histogram | — | Time spent in individual disk write calls. |
| `weaver_pipeline_decode_task_duration_seconds` | histogram | — | Time spent decoding one article. Conditional: emitted only once the decode path has recorded a measurement. |
| `weaver_pipeline_extract_member_duration_seconds` | histogram | — | Time spent extracting one archive member. Conditional: emitted only once the extraction path has recorded a measurement. |
| `weaver_db_runtime_info` | gauge | `engine` | Database engine in use; always 1. |
| `weaver_db_runtime_concurrency` | gauge | — | Configured database executor concurrency; always 1 for the serialized sqlite worker. |
| `weaver_db_runtime_in_flight` | gauge | — | Database operations submitted and not yet answered. |
| `weaver_db_runtime_blocked_submissions_total` | counter | — | Database submissions that had to wait on a full executor queue. |
| `weaver_db_op_duration_seconds` | histogram | `engine` | Database operation latency, measured around the executor round-trip. |
| `process_cpu_seconds_total` | counter | — | Total user and system CPU time spent by this process. |
| `process_resident_memory_bytes` | gauge | — | Resident set size of this process. |
| `process_virtual_memory_bytes` | gauge | — | Virtual memory size of this process. |
| `process_open_fds` | gauge | — | File descriptors currently open by this process. |
| `process_max_fds` | gauge | — | Maximum file descriptors this process may open. |
| `process_threads` | gauge | — | Threads in this process. |
| `process_start_time_seconds` | gauge | — | Unix start time of this process. Falls back to when the metrics exporter was built if the platform cannot report it. |
| `weaver_disk_total_bytes` | gauge | `role`, `path` | Total capacity of the filesystem backing a configured directory role. |
| `weaver_disk_available_bytes` | gauge | `role`, `path` | Space available to this process on the filesystem backing a configured directory role. |
| `weaver_http_requests_total` | counter | `route`, `method`, `status` | HTTP requests served, by route template, method and status code. Routes are templates rather than raw paths, and status codes outside a known allow-list collapse to their class boundary; scrapes of /metrics itself are not counted. |
| `weaver_http_request_duration_seconds` | histogram | `route` | HTTP request latency by route template, measured around the handler and every layer wrapped about it. |

## Dashboards and alerts

- [`contrib/grafana/weaver-overview.json`](../contrib/grafana/weaver-overview.json)
  — a Grafana 10/11 dashboard covering overview, pipeline, servers, and
  storage/post-processing.
- [`contrib/prometheus/weaver-alerts.yml`](../contrib/prometheus/weaver-alerts.yml)
  — alerting rules for disabled servers, gated downloads, stuck backpressure,
  post-processing failures, and quota exhaustion.
