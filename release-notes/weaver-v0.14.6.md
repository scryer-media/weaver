# Weaver 0.14.6 release notes

Everything below is new since 0.14.5.

## Highlights

- 0.14.6 is a light release that fixes the direct-store defects the first
  full archive campaign run in CI exposed. Every fix shipped here was
  reproduced on 0.14.5 and verified against the campaign cases that found it.

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
- The direct-store campaign tests model 7z map slots from the container and
  wait for an in-flight RAR topology refresh before declaring a schedule
  stalled. Both were test-side defects that reported product failures that
  did not exist.

## Upgrade notes

There are no configuration or database migration changes in this release.
