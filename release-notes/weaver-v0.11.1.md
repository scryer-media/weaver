# Weaver 0.11.1 release notes

> Draft — these notes describe the net change from `weaver-v0.11.0` to
> `weaver-v0.11.1`. They are release-candidate notes only; the release has not
> been published.

## Highlights

Weaver 0.11.1 makes direct-store recovery substantially more predictable
under real-world pressure. It now accounts for held data across every active
set, preserves working-directory headroom, and pages held ranges without
silently bringing them back into memory during a repair read. Header-encrypted
RAR sets reuse their derived key for the whole set rather than deriving it for
every arriving article.

The NNTP path keeps a warm lane pool per server and makes recovery work share
the normal pipelined transport. Health probing no longer delays a download when
recovery already answers the question, and quota exhaustion is now reported
correctly when a dispatch routes around a server.

## What changed

### Direct-store RAR recovery

- **Process-wide direct-store limits.** Resident holds and scratch space are
  now budgeted across all active direct-store sets, rather than multiplying a
  per-set allowance by queue depth. The limits take host resources into
  account, and a spill that would consume reserved free space is refused before
  it can leave the working directory unsafe.
- **Paged holds stay paged.** PAR2 providers read a held run from its current
  in-memory or scratch-image location on demand. A provider no longer copies
  every held range back into RAM, and pinned readers remain valid while scratch
  compaction replaces the active image.
- **Correct virtual-volume length.** Held ranges beyond an input hole now
  contribute to the virtual volume's length, so PAR2 does not rebuild bytes
  that have already arrived.
- **Faster header-encrypted RAR5.** Weaver derives a header-encrypted set's
  key once and shares the cache across its articles and parse walks. This
  removes repeated PBKDF2 work from an otherwise healthy set.
- **Reliable repaired-volume reparse.** The reparse gate now treats bytes below
  `short_at` as the required image, allowing an end-of-volume repair to be
  re-walked even when the final article never arrives. When a held RAR volume
  must be re-read, Weaver rebuilds the split-member chain from live volumes
  instead of destructively re-reading one cached segment.

### Download and NNTP behavior

- **Warm connection lanes per server.** BODY, recovery, and health work reuse
  a server's normal pipelined lane pool. Returning a connection makes it
  available before its permit can trigger an unnecessary fresh dial.
- **Health probes yield to useful work.** A probe is armed only when it can
  change the result, does not move the job into a checking state, and does not
  consume a download connection. PAR2 recovery can promote terminal damage
  without waiting for an uninformative probe.
- **Accurate quota-blocked state.** A server skipped during dispatch because
  the requested article exceeds its remaining quota now latches the blocked
  signal, allowing the all-fill-blocked gate to engage correctly.

### Archive safety and repair

- **Bounded split-7z discovery.** Recovered parts are found by enumerating the
  working directory once, rather than probing every number up to a suffix an
  NZB subject declares. Maliciously large split-volume numbers no longer turn
  topology recovery into billions of metadata lookups.
- **Bounded archive topology work.** Weaver no longer materializes an
  unbounded set of candidate deletable volumes. Declared split counts remain
  accurate while extraction and readiness checks stop at the first missing
  part.
- **Faster short-block relocation.** `par2-rs` 0.10.2 bounds its relocation
  scan to the bytes the merged scan state cannot account for, avoiding repeated
  full-candidate reads during header-encrypted repair.

### Benchmark harness

- Native benchmark sessions can run the NNTP server and shaper as local
  processes on macOS and Windows, with the round trip carried by the shaper
  when Linux `tc netem` is unavailable.
- Preflight now validates the exact raw stack and client launchers a session
  will use, including occupied ports, certificates, unpackers, encryption-key
  format, and paused client state. Windows CPU accounting covers the complete
  client process tree.

### Dependencies

- Updated `par2-rs` to 0.10.2, `unrar-rs` for shared header-crypt cache and
  repaired-volume handling, and `reedsolomon-rs` for faster RAR3 recovery
  volumes.
- Removed the unused `age` dependency and its older crypto/i18n subtree.
- Refreshed compatible Rust dependencies, including `rustls` and `encoding_rs`.

## Upgrade notes

- Direct-store remains enabled according to the existing configuration, but it
  now enforces a shared resident/scratch budget and preserves disk headroom.
  Under resource pressure, a set can demote rather than risk unbounded memory
  or disk consumption.
- No database migration or API schema change is required for this release.
- Operators using the native benchmark harness should run its preflight before
  a session; it now fails early for an occupied port, an invalid launcher, a
  missing unpacker, or a client that starts paused.

## Reliability fixes included in this release

- Prevented header-encrypted repaired RAR volumes from failing to re-enter the
  direct-store route at the end-of-archive record.
- Prevented a live RAR rebuild from losing split-member chain state after a
  held volume was re-read.
- Prevented direct-store repair providers from defeating the configured hold
  budget by copying paged data into memory.
- Prevented large declared split-7z suffixes from stalling the orchestration
  task on metadata probes.
- Prevented quota-exhausted owned lanes from appearing available to the
  download gate.
- Prevented health probes from delaying recovery when the recovery set already
  covers the observed loss.
