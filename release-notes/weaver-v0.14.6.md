# Weaver 0.14.6 release notes

Everything below is new since 0.14.5.

## Highlights

- Direct-store fixes address RAR restart, repair, and completion defects found
  by the first full archive campaign run in CI. Those defects were reproduced
  on 0.14.5 and checked against the cases that found them.
- yEnc decoding accepts an additional malformed escape that other downloaders
  retain, and the x86 decode and CRC paths do less work per article.
- Updated PAR3 and Reed-Solomon libraries improve source verification and
  repair behavior.

## What changed

- An obfuscated RAR5 set admitted from its own headers now survives a
  restart. The volume prefix and length that bind each volume to its PAR2
  description are read back from the placed bytes on resume, so the set is
  no longer demoted as unbindable, and a demotion racing the resumed
  download no longer leaves a member partial and a doubled envelope name in
  the output.
- A header-encrypted RAR5 set that resumes from a restart or crash with every
  article lost now hands the job's passwords to the router before PAR2
  repaired volumes are routed. It previously demoted under "no password" for
  a password the job held all along.
- An obfuscated RAR4 volume that the headers parse as volume 0 of its own set
  before PAR2 rebinds it no longer loses that volume. A full-set extraction
  of any of the job's RAR sets now counts as active work, so completion
  neither starts a duplicate extraction nor finalizes the job and discards
  the volume while an extraction is about to open it.
- A RAR chase now arms when an article arrives twice. Completing a part
  retires its progress floor, and a duplicate article completed the part
  again and retired the floor for good, so the chase saw floor zero and
  never armed. The arming gate now also accepts the part's committed first
  segment. The job still finished before this fix, but through conventional
  extraction instead of the direct path.
- A chase that arms over a set whose parts are already complete is always
  consumed. The completion check joins a chase it armed itself instead of
  returning, and a chase that finishes on its own now schedules a
  completion check, so a resumed job no longer waits on the periodic
  reconcile pass, or forever, with its output left in staging.
- A duplicate article can leave a cached RAR volume prefix shorter than the
  required identification window. Reading a longer prefix back from the
  placed volume now replaces that short cache entry, preserving direct-store
  routing for an otherwise healthy set.
- A whole-buffer yEnc decode now keeps an article whose last body line ends
  with a lone `=` before `=yend`. It decodes the data like the streaming path
  and other downloaders, then reports any size or CRC mismatch instead of
  failing immediately with a malformed-escape error.
- On supported x86 CPUs, yEnc trailer detection and CRC folding use improved
  vector paths. The AVX2 assembly decoder folds bounded output while it is
  still cache-resident; other decoder tiers retain their existing path.
- PAR3 repair now uses `par3-rs` 0.4.5 for larger source reads, improved
  network-share verification, and a pre-write snapshot check for in-place
  repair. `reedsolomon-rs` 0.4.8 adds vectorized recovery operations.
- The direct-store campaign tests model 7z map slots from the container and
  wait for an in-flight RAR topology refresh before declaring a schedule
  stalled. Both were test-side defects that reported product failures that
  did not exist.

## Upgrade notes

There are no configuration or database migration changes in this release.
