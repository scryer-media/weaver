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

- **A hardware profile change applies without a restart.** Every limit a
  profile decides now reaches the next download, decode or extraction that
  starts. Work already running finishes under the limits it started with.
- **The schedule can switch the hardware profile.** A rule can put Efficient
  in force during the evening and Performance overnight.

## What changed

### Hardware profiles

- **Profile changes are live.** Choosing a profile used to change only the 7z
  decoder allowance until the next start. It now also changes the download
  cap, the decode threads, the extraction and repair threads, and the
  extraction memory ceiling, for the work that starts after the change.
- **A new schedule action, "Switch the hardware profile".** The profile a rule
  puts in force stays in force until the next profile rule fires, across
  midnight and across days the rules skip. Profile rules are separate from
  pause and speed-limit rules: neither kind ends the other. When no profile
  rule is enabled, the profile chosen in settings applies.
- **Settings show when a schedule is in charge.** The profile setting says
  which profile a schedule currently has in force. A choice made meanwhile is
  saved and applies when no profile rule does.
- **Profiles are judged by the CPUs Weaver may use.** A process pinned to
  fewer CPUs than the host has cores was offered profiles and thread counts
  sized for the whole host. The CPU set now caps them, and caps concurrent
  extractions on fast storage.
- **A saved profile the machine can no longer honour reads as not chosen.**
  The settings page opens on the recommended profile, which is what runs.

### Download health

- **A set is charged what its lost files need.** While a health failure is
  deferred to PAR2, a damaged file was counted as one slice however many
  articles it had lost. It is now counted as the bytes it lost, and at least
  one slice.
- **A file that several recovery sets describe is charged to the set best
  able to repair it,** not always to the first one.

### Working directories

- **Upgrading a working directory's ownership marker cannot lose it.** The new
  marker is written beside the old one and renamed over it, so a crash in
  between leaves the old marker in place.

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

- The end-to-end harness checks that a rootless container engine maps both
  of the user IDs the services run as, and its cancellation test no longer
  depends on a long sleep.
- A test server in the NNTP client's test suite no longer fails a test when
  the client closes its connection while a reply is still being written.

## Upgrade notes

- A job with damage that PAR2 cannot repair takes longer to fail than before,
  because the recovery set is now read before the failure is reported.
- An archive that fails to extract for a reason other than damage, such as a
  wrong password on a `zip`, costs one PAR2 verification pass before the job
  fails. This happens at most once per recovery set.
- Restarting after a profile change is no longer needed.
- The `WEAVER_EXTRACTION_MAX_MEMORY_BYTES` environment variable still
  outranks every profile's memory ceiling.
- The GraphQL API gains fields and changes none: `active` and `scheduled` on
  the hardware profile settings, `hardwareProfile` on schedules and schedule
  input, and the `hardware_profile` schedule action type.
- There are no configuration or database changes.
