# Diagnostics bundle

`GET $WEAVER_URL/api/system/diagnostics` returns a zstd-compressed tar
(`Content-Type: application/zstd`), named
`weaver-diagnostics-<generated_at_utc with : as ->.tar.zst`. Fetch it with
`scripts/fetch-bundle.sh <file>`. In the web UI: **System info → Download
diagnostics**.

## Who may fetch it

- An `ADMIN` key, or a browser session with full access.
- A `READ` or `CONTROL` key: bare HTTP 403, no body.
- A missing or refused key: bare HTTP 401, no body.
- A build failure: HTTP 500 with `{"error": "failed to write diagnostics archive: ..."}`.

Building takes at least 10 seconds: Weaver samples its metrics twice, 10
seconds apart.

## Members

| File | Holds |
|---|---|
| `manifest.json` | Version, time, sanitizer report, files, errors (below) |
| `system-info.json` | `systemInfo`, `version` |
| `metrics.json` | `metrics`, `globalQueueState` |
| `metrics-1.txt`, `metrics-2.txt` | Prometheus samples 10 s apart |
| `server-health.json` | `serverHealth` |
| `queue.json` | Queue snapshot, up to 2000 jobs |
| `servers.json` | News server settings |
| `settings.json` | Settings |
| `schedules.json` | Schedules |
| `service-log.txt` | In-memory service log as text lines |
| `pipeline-internals.json`, `pipeline-turn-phases.json` | Pipeline internals |
| `database.json` | Database facts |
| `nzb-reports.json` | Per-job NZB reports |
| `host.json` | OS, architecture, deployment, cores, CPU and memory limits, storage per role (`data`, `intermediate`, `complete`): class, filesystem, total, used, free, error |
| log files | Weaver's log files under their own names, rotated `.gz` included; each kept to its last 32 MiB; an oversized `.gz` is left out |
| `config/config-N.toml` | Each TOML file in the data folder, redacted; one that cannot be parsed is left out |

A file can be partial or missing; the manifest says which and why.

### manifest.json

```json
{
  "weaver_version": "...",
  "generated_at_utc": "...",
  "sanitized": {"stand_ins": 0, "history_jobs_total": 0, "note": "...",
                "table": {"complete": true, "steps": [], "...": "..."}},
  "files": [{"name": "queue.json", "bytes": 1234, "partial": false}],
  "errors": [{"name": "service-log.txt", "error": "...", "reason": "timed_out"}]
}
```

Error `reason`: `timed_out`, `read_failed`, `unreadable`,
`pipeline_stopped`, `stopped`, `too_large`.

`sanitized.table.complete` false: the stand-in table was not fully read;
`steps[]` give each step's `state` (`complete`, `partial`, `failed`,
`not_reached`). Lines naming unread jobs are left out.

## What is redacted

Always on; cannot be turned off.

1. **Secret keys.** In JSON, any key whose name (lower case, letters and
   digits only) contains `password`, `passwd`, `passphrase`, `secret`,
   `token`, `apikey`, `credential`, `username`, `privatekey`,
   `encryptionkey`, `jwt` or `authorization` has its value set to
   `<redacted>`. In the config TOML: keys containing `password`, `secret`,
   `key`, `token` or `username`.
2. **Stand-ins.** Every member but `nzb-reports.json`, the logs included,
   replaces names with stable stand-ins: news servers, proxies, network
   routes, schedules, jobs and their files, categories, metadata, folders and
   the host. Sizes, counts, timings, ports, states, error kinds and versions
   are kept. Names come from the 2,000 newest history jobs and the live
   state.

The same stand-in means the same thing across members, so a job can be
followed from `queue.json` to the logs. For real names, ask the operator or
use the live operations.

`nzb-reports.json` is built from fixed names and is not passed through the
stand-in pass.

## Unpack and read

Into a new, empty directory the operator agrees to:

```sh
mkdir weaver-diag && zstd -dc bundle.tar.zst | tar -xf - -C weaver-diag
B=weaver-diag
jq '{v: .weaver_version, at: .generated_at_utc, errors: .errors,
  sanitized_complete: .sanitized.table.complete}' "$B/manifest.json"
jq -r '.files[] | "\(.name) \(.bytes) \(.partial)"' "$B/manifest.json"
sh "$S"/scripts/log-search.sh "$B/service-log.txt" --summary
```

## Which members matter per symptom

| Symptom | Read |
|---|---|
| Job stuck | `metrics.json` (`globalQueueState`), `queue.json`, `service-log.txt` |
| Failed vs Partial | `queue.json`, `nzb-reports.json`, `service-log.txt` |
| Server unreachable | `server-health.json`, `service-log.txt`, log files |
| Disk full | `host.json` storage, `system-info.json` |
| Torrent not connecting | `service-log.txt`, log files, `settings.json` |

Never `cat` a member. Count, filter and cap with jq or `log-search.sh`.
