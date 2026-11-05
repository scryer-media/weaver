# Weaver 0.16.9 release notes

Everything below is new since 0.16.8.

## Highlights

- **Less overhead while downloading and unpacking.** Graph checkpoints are
  gathered into batches, RAR decoder output is delivered in larger pieces,
  and retained checkpoint bytes use a more compact encoding.
- **Stream stored ZIP files with traditional encryption.** Entries encrypted
  with ZipCrypto can write their plaintext as data arrives, with password
  checks, checksum verification, repair and restart support.
- **More reliable delivery on mounted storage.** Files are closed before
  moving or removing them, copy fallbacks preserve existing destinations,
  and mount-controlled ownership and permissions no longer turn otherwise
  successful delivery into failure.
- **Progress follows the work.** Moving updates while files are copied, and
  a finished download shows 100% even when unused recovery bytes stayed
  parked.

## Checkpoints and performance

- **Batch checkpoint writes.** Each graph step still records its changes
  after its bytes reach disk. The per-job writer combines those changes
  until 256 MiB of dirty coverage or five seconds makes a write due, instead
  of starting a database transaction for every article.
- **Commit before dependent work.** File moves that need a committed name,
  feed verdicts, job closure and graceful shutdown force pending checkpoint
  changes into the database queue rather than waiting for the age limit.
- **Refetch recent work after a crash.** An abrupt stop can leave recent
  articles outside the last committed checkpoint; restore fetches those
  articles again. Database stalls can extend the normal batching window.
  A checkpoint continues to describe only bytes that were written to disk.
- **Gather small RAR decoder writes.** Output smaller than a megabyte is
  accumulated before reaching the graph and disk writer, reducing messages,
  writes and checkpoint changes. Buffered output is also delivered before
  handling a decoder error.
- **Store retained bytes compactly.** Checkpoints encode retained byte
  buffers as MessagePack binary data rather than arrays of integers. The
  reader continues to accept the earlier encoding.
- **Apply answered read requests once.** A graph read-ahead request that
  has already been answered no longer gets applied a second time.

## ZIP extraction and repair

- **Unpack stored ZipCrypto entries as they arrive.** Traditional encrypted
  entries using the Stored method join the streaming ZIP path, including
  mixed plain and encrypted members, empty files and supported data
  descriptors. Other compression and encryption methods continue through
  the extraction engine.
- **Keep encrypted repair and restart work reproducible.** Existing
  plaintext can reproduce the ciphertext needed for repair or engine
  handoff. Restarts rebuild cipher state from offered passwords without
  saving passwords or live cipher keys in checkpoints.
- **Retry passwords after header repair.** A corrected encryption header
  clears password rejections made against the damaged header. If plaintext
  was already written, Weaver preserves its old cipher state and hands the
  corrected archive to the extraction engine.
- **Accept stale local size fields safely.** The ZIP engine can use the
  central directory's sizes when local sizes are stale, while still checking
  the entry's identity, encoding and checksum. Conflicting headers and
  duplicate names remain invalid.
- **Ignore encryption on entries that are not extracted.** An ignored
  encrypted link or device entry no longer makes an otherwise successful
  ZIP incomplete, including jobs restored from earlier checkpoints.

## Filesystem and delivery safety

- **Release handles before publishing or removing files.** Completion,
  archive fallback and cleanup relinquish the files they no longer need.
  This avoids open-file removal problems such as NFS `.nfs` entries and
  Windows sharing violations.
- **Copy safely when rename and hard links are unavailable.** File moves
  fall back to an exclusively created destination. Failed-copy cleanup
  removes only the file that operation created and can still identify,
  preserving unrelated files and replacements at the same path.
- **Recognize paths that name the same file.** Alias checks happen before
  copy fallback. A move through another spelling of the same directory
  entry no longer risks unlinking its only name; interrupted hard-link
  moves can still finish removing the extra source link.
- **Respect mount-managed metadata.** Unsupported or permission-denied
  ownership and mode updates are reported as warnings. Missing files,
  storage exhaustion and data I/O errors still fail delivery.
- **Let unrelated files use their final names.** A member waiting for a
  committed rename no longer makes every later member open under a
  temporary name. Each name waits only for the file or pending move that
  actually holds it.
- **Avoid moves for equivalent names.** Names supplied again with different
  case or Unicode normalization use the same comparison as the name
  ledger. A PAR2 spelling that already names the member does not move it.

## Web interface

- **Show incremental Moving progress.** Opening a sampled phase wakes the
  independent progress ticker immediately, so copied bytes reach the UI
  while the pipeline is awaiting other work.
- **Finish the Downloading bar at 100%.** Delivery and completion show a
  full download bar even when optional repair data was never fetched. This
  also applies to bars briefly retained during a phase change; actual
  downloaded-byte totals remain unchanged.

## Regression coverage

The ordinary suites add coverage for checkpoint batching and forced-write
ordering, encrypted stored ZIPs, repaired encryption headers, restored cipher
state, retained-byte encoding compatibility, filename moves and incremental
phase progress. The regression harness also adds encrypted ZIP fixtures and
an isolated FUSE lane for filesystems that refuse exclusive rename, hard
links or metadata changes.
