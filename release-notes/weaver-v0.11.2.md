# Weaver 0.11.2 release notes

> Draft — these notes describe the net change from `weaver-v0.11.1` to
> `weaver-v0.11.2`. They are release-candidate notes only; the release has not
> been published.

## Highlights

Weaver 0.11.2 keeps NNTP connections warm where the scheduler can actually
use them. A job's owned lanes now prefer their cached server connection, warm
payload connections as a download becomes eligible, and keep critical work
from leaking into another job's payload lanes.

Direct-store repair is more reliable when a volume is repaired in several
pieces or when scratch data is still pinned by a reader. Repair gates now wait
for the completed rewrite, and scratch compaction accounts for both its new
and retired images until every reader has released them.

## What changed

### Faster, steadier downloads

- **Owned connection lanes are usable while warm.** Download scheduling now
  routes a lease to an idle worker with the matching cached connection before
  considering a fresh dial. Workers without a connection can also start their
  payload dials together as a job clears its initial barrier, rather than
  serializing handshakes behind it.
- **No lost worker wake-ups.** Runs waiting for an owned worker are held in a
  shared queue that an available worker drains before publishing itself idle.
  This keeps a release from being stranded behind a busy worker's private
  channel.
- **Critical lanes stay with the hot job.** When critical work drains, only
  the current hot job's lane may transition down to payload. Other jobs park
  normally so their payload work still follows the regular reclaim and
  scheduling rules.

### Direct-store and repair correctness

- **Repair gates evaluate completed volumes.** A multi-part PAR2 rewrite is
  now recorded until its final piece arrives, then evaluated as a whole. This
  prevents header-encrypted and ordinary RAR volumes from being rejected
  against a mix of repaired bytes and still-damaged bytes.
- **Independent damaged volumes wait their turn.** A volume with damage still
  on record is left alone while another volume's rewrite settles, preventing a
  successful repair from unnecessarily demoting the set.
- **Pinned scratch compaction respects limits.** The replacement scratch image
  is admitted against the shared scratch and free-space budget before it is
  written. Retired images remain charged until their pinned readers finish,
  and image handover now handles platforms that do not allow an open file to
  be replaced in place.
- **Repair leftovers are not rescanned as payload.** Temporary repaired-file
  copies are excluded from every set's extra-file scan until the job's PAR2
  verification is complete, avoiding repeated full reads of files that cannot
  be useful candidates.

### Queue display

- **Stable download-rate display.** Queue-row download rates use a fixed-width
  monospace field with tabular numerals. The progress bar has a fixed responsive
  width, so changing rate values no longer shift the bar horizontally.

### Benchmark harness

- **Failed shaper acquisition no longer blocks later runs.** If a benchmark's
  shaper lease cannot be released immediately because foreign downstream
  connections are still present, cleanup continues until the lease is returned
  instead of stranding every later suite behind it.

## Upgrade notes

- No database migration or API schema change is required for this release.
- Direct-store remains configured as before. Under pinned-scratch pressure it
  may demote a set rather than exceed the shared disk reserve.
