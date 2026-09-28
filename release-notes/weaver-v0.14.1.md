# Weaver 0.14.1 release notes

Everything below is new since 0.14.0.

## Highlights

- **A damaged download that PAR2 could repair no longer fails at
  extraction.** When direct unpack met damaged bytes, Weaver could lose track
  of the damage before the download finished. It then skipped the PAR2 pass,
  treated the recovery set as clean, and handed the damaged files to the
  extractor, which failed the job. Damage is now recorded against the archive
  set itself, so the PAR2 pass runs and repairs the files before anything
  extracts them.

## What changed

### Repair and extraction

- **Damage found during direct unpack stays on record.** The damage report was
  held by the direct-unpack worker and went with it. A worker that failed on
  the damaged bytes, finished first, or was retired before the report arrived
  left nothing for the completion check to read. The report now outlives the
  worker in each of those cases.
- **A PAR2 analysis that found damage is always acted on.** An analysis that
  finished just after the reason for requesting it had gone could be left
  unread while the set was settled as clean. A finished analysis that found
  damage now forces the full PAR2 pass.
- **A failed extraction gets one PAR2 pass before the job fails.** This applies
  to single-stream archives such as `tar.xz`, `gz` and `zip`, when PAR2
  verification had been skipped because a clean extraction would itself prove
  the data. If extraction fails, Weaver now verifies the set, repairs it when
  it can, and extracts again. A second failure is final and reports the
  extractor's own error. 7z and RAR sets keep their existing repair routes.
- **Extraction waits for the PAR2 verdict when damage is already known.** A
  job whose direct unpack reported damage no longer opens its archives before
  the recovery set has ruled. This avoids reading a whole set only to reach a
  failure that was already known.

### Tests

- A test server in the NNTP client's test suite no longer fails a test when
  the client closes its connection while a reply is still being written.

## Upgrade notes

- A job with damage that PAR2 cannot repair takes longer to fail than before,
  because the recovery set is now read before the failure is reported.
- An archive that fails to extract for a reason other than damage, such as a
  wrong password on a `zip`, costs one PAR2 verification pass before the job
  fails. This happens at most once per recovery set.
- There are no configuration, database or API changes.
