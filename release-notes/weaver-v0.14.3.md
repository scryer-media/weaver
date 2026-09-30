# Weaver 0.14.3 release notes

Everything below is new since 0.14.2.

## Highlights

- **Downloads keep moving while direct-store data is made durable.** On slow
  disks and network shares, a coverage checkpoint could pause every download
  while destinations were flushed. Those flushes now run away from the
  download pipeline. Verification and restart reads no longer hold up unrelated
  jobs either.
- **An application upgrade restarts the newly installed build.** On Linux, a
  restart could launch the backed-up executable instead, leaving the upgrade
  marked failed. Weaver now remembers the executable path before replacing it
  and recognizes a later successful boot of the target build.
- **Dead-post diagnosis is consistent under load.** A download health failure
  now waits for the first-article sample while its outstanding articles can
  still settle, so the job can report that a post is gone instead of failing
  early with a byte-count error.

## What changed

### Download and repair

- Coverage checkpoints flush file data without blocking the pipeline task or
  the disk-write owner. A checkpoint still waits for the flush results before
  it records durable coverage. File-handle closure also waits for any related
  flush work to finish.
- Direct-store checkpoints now distinguish each in-flight flush. A request
  joining one no longer waits through the pipeline's bounded completion
  channel, and a late completion cannot settle a newer checkpoint.
- PAR2 verification reads and checkpoint recovery after a restart run outside
  the pipeline task. Other jobs can continue while a slow volume answers.
- Temporary envelope and repair files are removed after their cached handles
  close. This avoids leftover staging directories on NFS, where unlinking an
  open file can leave a busy temporary entry.
- On Apple platforms, network mounts that do not support the stronger device
  flush can use the ordinary file flush they do support. Local volumes retain
  the stronger flush.

### Upgrades and diagnostics

- The upgrade restart uses the executable that was running before the old
  binary was moved aside. If the wrong build previously booted and removed the
  upgrade journal, a later boot of the intended build can complete that run;
  unrelated failed runs stay failed.
- Superseded coverage checkpoints and stale saved RAR volume facts produce
  fewer warnings. Stale volume facts are reported once per set with a count
  and the first decoding error.

## Upgrade notes

- There are no configuration or database migration changes in this release.
