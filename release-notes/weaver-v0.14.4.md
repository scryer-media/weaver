# Weaver 0.14.4 release notes

Everything below is new since 0.14.3.

## Highlights

- **Archive repair handles more damaged and obfuscated sets.** Direct-store
  recovery now keeps related RAR volumes together, restores missing bytes, and
  finishes repaired sets without leaving stale volume files behind.
- **PAR2 and PAR3 recovery survives more ordering and restart cases.** Held
  files, renamed outputs, encrypted volumes, and multiple damaged sets can
  settle after repair without losing the download's final output.
- **Resource limits follow the hardware profile.** Extraction, PAR3 repair,
  and held-file work use limits sized for the selected machine profile.

## What changed

### Download and repair

- PAR2 repair carries restored bytes through direct-store scratch files and
  repairs every damaged set found in one recovery pass. It also matches RAR
  volumes without sequence numbers to the files their contents identify.
- Direct-store repair preserves a clean sibling set when another set is
  demoted, and repairs renamed or partially written volumes without demoting
  the rest of the archive unnecessarily.
- Encrypted and obfuscated RAR sets keep their routing across restarts and
  repair. Finalization removes leftover obfuscated volume files after their
  recovered output is complete.
- PAR3 can publish a held file once its first article arrives, read the edges
  of later encrypted volumes, and settle sets rebuilt beside damaged posted
  copies.
- Duplicate articles and late completion checks no longer advance a job
  before its pending copy or post-processing work is settled.

### Validation

- Archive extraction and repair coverage now includes more multi-volume,
  encrypted, solid, obfuscated, PAR2, and PAR3 schedules. The large archive
  campaigns run through a separate manually dispatched CI workflow.

## Upgrade notes

- There are no configuration or database migration changes in this release.
