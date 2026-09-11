# Weaver 0.11.6 release notes

## Highlights

Weaver 0.11.6 fixes a download stall that could hold the whole queue at 0 B/s,
with every connection shown as in use, until Weaver was restarted. It also stops
Weaver from lowering its own connection count after a stall, and adds a
one-click diagnostics package to System Info for problem reports.

## What changed

### Download reliability

- **Write-pressure stalls now clear on their own.** When a direct-store set fell
  back to conventional volume assembly in the middle of a download (a
  demotion), articles for its files could pile up in the write buffer with
  nothing allowed to write them out. That latched the write-pressure limit and
  stopped all downloading, and nothing ran relief afterwards, so the stall
  lasted until a restart. Affected jobs typically logged `waiting for bounded
  PAR2 metadata discovery before finalization` every 30 seconds. Weaver now
  holds further downloads for those files until the demotion finishes, and
  relieves the write backlog on its periodic tick and after the demotion,
  writing buffered data out until the limit clears and then resuming
  downloads.
- **Connections are not held while writes catch up.** A download lane that
  meets write pressure now parks and returns its pool connection, and is
  redispatched once pressure clears. Previously it waited on its refill request
  while keeping its socket and pool slot, so a long backlog could hold every
  connection idle.
- **The connection count no longer ratchets down under pressure.** Previously,
  every 15 seconds of sustained write pressure lowered the number of
  connections Weaver would use by one, down to a single connection, and the
  count only recovered while the queue was empty. After a long stall, downloads
  could continue at a fraction of their normal speed. Weaver now always uses
  the configured connection total.
- **Connections closed by the server are released promptly.** An idle
  connection that the provider has closed is now detected within 15 seconds,
  and again before it is reused, and its slot is returned to the pool.
  Previously it kept its slot until a download was routed to it, and that
  first request failed only after a read timeout, costing a retry.

### Diagnostics

- **Download a diagnostics package from System Info.** A new **Download
  diagnostics package** button on the System Info page builds a `.tar.zst`
  archive to attach to a problem report. It contains Prometheus metrics sampled
  twice, ten seconds apart; a snapshot of every top-level GraphQL query; the
  in-app log buffer; Weaver's log files; system facts; a read-only snapshot of
  pipeline internals; and the configuration and server list with passwords,
  API keys and other secrets removed. Collection takes about 15 seconds.
- The archive is served by `GET /api/system/diagnostics`, which requires an
  administrator.

### Windows

- **Memory and storage are measured instead of assumed.** Weaver now reads
  physical and available memory on Windows and classifies its storage
  (filesystem, network share, and whether the drive has a seek penalty, as a
  spinning disk does) instead of falling back to fixed 16 GiB / 8 GiB memory
  figures and an unknown storage type. Extraction concurrency and other sizing
  that depends on these facts now use the measured values.

### Internal cleanup

- **Unused tuner machinery removed.** The runtime tuner now adapts only
  extraction concurrency, following the measured disk. Parameters it computed
  but nothing used (a bandwidth estimate and the write-queue, free-buffer and
  repair-thread parameters) are gone, along with the persisted `tuner.*`
  overrides, which were stored and reloaded but never applied to the running
  tuner.

## Upgrade notes

- No database migration or GraphQL schema change is required. The diagnostics
  download is a new REST route.
- Stored `tuner.*` override settings are now ignored. They never affected the
  running tuner, so behavior does not change; existing values stay in the
  database untouched.
