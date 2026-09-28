# Weaver 0.14.0 release notes

Everything below is new since 0.13.4.

## Highlights

- **Hardware profiles put resource limits in one place.** Choose Efficient,
  Balanced, or Performance during setup or in Settings. Weaver offers only
  profiles the machine can support, accounting for container limits, and uses
  its recommendation until a profile is selected. Each profile sets 7z decode
  memory, extraction memory, worker counts, and the concurrent-download limit.
- **News server connections select an address by observed performance.** New
  connections race resolved addresses and pin the quickest usable one. Later
  delivery measurements can move the pin when another address shows a
  sustained lead; periodic races also discover provider address changes.
  A connect still tries addresses beyond the bounded race before declaring
  the server unreachable. Server metrics report the pin, races, and repins.
- **Logs are easier to use when a download stalls.** The log filter can be
  changed while Weaver runs. Each job retains a bounded debug history that is
  replayed when dispatch is genuinely withholding queued work. Routine
  lifecycle messages and false stall warnings have been reduced.
- **Repair decisions respect independent recovery sets.** PAR2 recovery from
  one set no longer counts toward losses in another when deciding whether to
  abort or defer a damaged job. Weaver also avoids re-fetching and re-parsing
  PAR2 data it already holds.

## What changed

### Downloads and repair

- The address plan replaces the old IP replacement trial. It keeps the
  concurrent address race bounded, falls back through the remaining resolved
  addresses, and uses delivery measurements without opening extra trial
  connections. Cold first responses no longer distort the download-depth
  model's latency and throughput estimates.
- A header-encrypted direct-store set can reuse password candidates found in
  the NZB when it is admitted later or restored after a restart. Restore
  rechecks the archive key before resuming the set.
- Restoring a finalized direct-store set now checks every recorded output,
  including empty files and directories, before treating the set as finished.
- A PAR3 image is refreshed when an encrypted article lands in a hold without
  immediately writing to its destination, so repair sees that article's
  newly available bytes.

### Settings, history, and diagnostics

- The finished-jobs page is now named **History**.
- Profile choices appear in setup and Settings only when the machine supports
  more than one profile. The UI shows the limits reported by Weaver itself.
- History deletion handles an ownership marker whose device number changed
  across a restart: it can remove the record while leaving a directory it
  cannot safely identify, and reports what was left. A delete that times out
  now says that the background operation may still finish.
- Job debug histories are captured with less contention and replayed as
  structured log records. Stall reports are limited to queued work that
  dispatch cannot hand out.

### End-to-end validation

- The end-to-end harness supports Docker and Podman through a common container
  engine layer. Its `doctor` command checks the selected engine and Compose
  provider before a run, and the Podman path includes a dedicated Compose
  overlay and cleanup handling.

## Upgrade notes

- An existing installation with no saved hardware profile uses Weaver's
  recommended profile. A 7z decode limit applies to newly admitted work;
  worker counts and the extraction-memory ceiling take effect after restart.
  An explicit `WEAVER_EXTRACTION_MAX_MEMORY_BYTES` still overrides that ceiling.
- The retired `ipReplacementTrialExtraConnections` setting is removed from
  saved configuration on load. Its API field and trial metrics are gone;
  clients and dashboards that referenced them need to use the address-plan
  fields and metrics instead.
- Finalized direct-store markers from 0.13.4 are not accepted by the expanded
  output check. Affected sets download again so Weaver can verify all outputs
  before restoring them as finished.
- A runtime log-filter change lasts until restart; startup log directives
  apply again when Weaver starts.
