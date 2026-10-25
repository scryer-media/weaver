# Weaver 0.16.5 release notes

## Highlights

- **Responsive queue controls.** Queue reads and control requests no longer
  wait for the download pipeline's current disk, repair, or extraction work.
  Pause, resume, cancellation, and metadata edits are recorded in the shared
  queue and applied at the appropriate pipeline boundary.
- **Resume around missing articles.** Restart checkpoints now record all
  written article ranges, including bytes beyond an early gap. A missing
  article near the start of a file no longer reduces the whole download to a
  crawl, and restarts fetch only the ranges the checkpoint is missing.
- **Fix downloads stuck on nonexistent work.** Count in-flight articles from
  what live connections actually hold. Duplicate leases, lane reassignment,
  cancellation, and stall recovery no longer leave phantom downloads blocking
  completion or requiring a restart.

## Upgrade note

The download checkpoint format changes in this release for both SQLite and
PostgreSQL. Old prefix-only checkpoints cannot be converted and are discarded.
In-progress files relying on those checkpoints will download again after the
upgrade; subsequent restarts use the new article-range coverage.

## Downloads and recovery

- **Clean shutdown during PAR2 repair.** Shutdown interrupts both retained and
  inline PAR2 repair sessions. An interrupted pass does not fail the job or
  record a repair verdict; the pass runs again after restart.
- **Avoid stalled direct-store admission.** A RAR set that cannot advance
  within its storage allowance yields its connections and falls back instead
  of crawling toward the scratch-space limit. Reject conflicting article
  placements before a bad extent can invalidate neighboring articles.
- **Handle refused credentials decisively.** When no eligible provider can
  serve an article because credentials are refused, mark it missing instead
  of requeueing it indefinitely. Report an unservable batch once rather than
  logging one warning per article. A server with no idle connection is not
  treated as an available alternative for rotation.
- **Consistent dead-post diagnosis.** Coordinate health probes with the
  first-article sample so an unavailable post receives one consistent failure
  verdict regardless of which detector finishes first.
- **Accurate speeds and usage.** Per-job download rates and the global gauge
  use the same wire-byte totals and sampling window. Include repeated-article
  transfers, correct speed-test peaks at the first and last sample intervals,
  and retain pending bandwidth usage until its database write commits.

## Queue, files, and settings

- **Consistent queue state.** Reflect pending pause and resume requests in
  their responses, show globally paused jobs as paused, and explain why a
  started job has no active connection. Keep cancelled jobs listed until their
  history rows commit, and label operator cancellations correctly in history.
- **Safer job directories and renames.** Jobs whose names sanitize alike get
  distinct working directories. Cached repeated articles follow a renamed
  job, and Windows renames release the source directory handle before moving
  it.
- **Name extensionless payloads by content.** Give recognized unpacked payloads
  their content's extension while honoring the unwanted-extension policy.
  Refused names are not introduced by the delivery naming pass.
- **Provider settings stay readable.** A provider-list failure shows its
  reason and a Retry action instead of an empty configuration. Failed TLS
  diagnostics do not prevent providers from being listed or managed.

## Security

- Update `ctutils` to 0.4.4 to address the constant-time mask-generation and
  comparison issue tracked as RUSTSEC-2026-0385.
