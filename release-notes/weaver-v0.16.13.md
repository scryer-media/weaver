# Weaver 0.16.13 release notes

Everything below is new since 0.16.12.

## Highlights

- **Restarts no longer read every downloaded byte back.** A restored job
  used to read each article it already had back from disk, check it, and
  write it again, all before it could download anything. Ten restoring
  jobs could hold every download slot while they did, so no job moved.
  A restore now trusts its checkpoint and the files' lengths: anything a
  file no longer reaches is fetched again, and nothing else is read,
  checked or rewritten.
- **A complete folder on another drive or a network share gets its files
  directly.** When the complete folder is on a different device from the
  intermediate folder, delivered files are now written there as they are
  extracted, under a hidden `.weaver-staging` folder. Finishing the job is
  then a rename instead of a copy. Archive volumes, recovery files and
  scratch data stay in the intermediate folder. On a test rig with an NFS
  share, the final move went from 13.4 s to 0.04 s.
- **Zip archives extract while they download.** The zip engine waited for
  every volume before it inflated a byte, so all of the extraction ran
  after the download ended. It now reads the archive's directory from the
  last volume's tail as soon as it arrives, and inflates each entry as its
  bytes land, the way the 7z engine already did.
- **Archives with large dictionaries no longer fail on memory.** A RAR,
  xz or 7z set whose dictionary is wider than the hardware profile allows
  now decodes with that window on a scratch file in the job's folder (or
  the data folder, if the job's folder has no room) instead of in RAM.
  An archive that still cannot get the memory it needs fails with the
  sizes, and the job offers **Retry with no limit**.

## Memory

- **Each hardware profile sets the window one decoder may hold.**
  Efficient allows 256 MiB, Balanced 1 GiB and Performance 4 GiB, for RAR,
  xz and zstd alike. RAR was held to 256 MiB on every profile, and xz and
  zstd to 128 MiB.
- **A wider window goes to a scratch file.** A run that needs more than
  its profile's window borrows the rest from a preallocated scratch file,
  which is probed before use. On Balanced and Performance, a run the
  scratch file cannot take starts again once in RAM, up to the machine's
  memory less a quarter. On Efficient it fails. Where no scratch window
  can be mapped (some Windows setups), the run stays in RAM above the
  profile's window.
- **Archives share one decode pool and wait for room.** Decode runs from
  every job reserve from one pool sized to fit one run at the profile's
  window. A run that does not fit beside the others waits for memory
  instead of failing. Only a run larger than the whole pool is refused.
  The pool is separate from download holds, so the most Weaver uses is
  the hold limit plus the decode pool.
- **A job that needs too much decode memory says so.** A dictionary or
  PPMd model over the limit used to fail as unsupported. It now fails as
  too large, with what the decoder needed and what was allowed, for
  example `needs 2.0 GiB of decode memory, 1.0 GiB allowed`. Retry with
  no limit asks for the password again where the archive has one, as
  Reprocess does.
- **Holds past their budget spill to disk instead of failing the job.**
  Bytes that arrive before their place in an archive is known are held in
  RAM, then in scratch. When both are full, they now go to a spill file
  beside the member instead of refusing the volume. A multi-volume stored
  RAR posted under other names than its PAR2 set, fetched out of order,
  used to fail with nothing delivered. Spilled bytes survive a restart,
  and a member an older build refused for its holds is reopened from its
  files.
- **ZipCrypto data held for its keys is capped at 64 MiB.** Past that,
  the bytes wait in the job's ordinary holds and are deciphered in order,
  each byte once.

## Archives

- **Encrypted zips ask for a password straight away.** An AES zip with no
  password used to ask only when the download ended, then decrypt the
  whole set afterwards. It now asks as soon as an encrypted entry needs a
  password, so decryption runs alongside the download.
- **ZipCrypto entries decrypt from the front.** A single `.zip` with a
  stored ZipCrypto entry of known size waited for the directory at the
  end of the archive before decrypting anything. It now decrypts from the
  start as the articles arrive, and checks what it read against the
  directory once that arrives.
- **Slow archive work leaves the main loop.** ZipCrypto decryption and
  password key derivation for RAR and 7z now run on the post-processing
  pool, so they no longer hold up the loop that drives downloads. An
  article lost while that work runs waits for it and is placed through
  the archive's own map once it is done, so recovery files still mend the
  member it falls in.
- **A nested encrypted zip fetched from its tail no longer crashes.** A
  zip inside a zip, encrypted, whose last article came first could replay
  the same bytes forever and end the process with a stack overflow.
- **A split encrypted 7z with PAR2 no longer fails after a repair.** A
  repair could come back while the 7z was off deriving its key, and
  reading the repaired bytes back through it failed the job with `lost
  proven bytes`. Key work now waits for a repair that is out, and a
  recovery set rules only once no archive has work pending.
- **A password given while an archive derives its key is kept.** It was
  offered to an archive that was not there, and the archive never heard it.
- **Large stored members are no longer refused by the extraction ratio.**
  A stored member split over many volumes was counted at its full size
  while only the volumes seen so far counted toward the archive. A
  27.5 GB member in 100 MiB volumes was refused with
  `WEAVER_EXTRACTION_REJECTED[ratio]`. The ratio is now taken over every
  file in the post except recovery data.
- **The extraction deadline counts only decoding time.** It ran on the
  wall clock from the first extracted member, so a job whose articles came
  slowly, or that waited for a slot, could fail on time no decoder spent.
  Waiting for articles, a slot, memory or a thread, and time paused, no
  longer count.
- **A RAR volume still under its posted name joins its set.** Once PAR2
  renamed a set's volumes, a volume that still carried the posted name was
  taken for a different set. Each arrival then started the set over, and
  a volume could be left behind.
- **A 7z password ruled wrong on damaged bytes is tried again.** When the
  bytes that checked the password were damaged, the password was marked
  wrong for good. It is now tried again when good bytes arrive.
- **Pausing waits for archive work in progress,** and work that fails off
  the main loop now fails the job instead of being lost.

## Restarts

- **Stored RAR members keep their checksums across a restart.** A RAR
  store now saves its members' running CRCs in its checkpoint, so a member
  that was open at the checkpoint is checked at close without reading the
  file. A wrong byte still fails the check. Jobs checkpointed by 0.16.12
  read the member once, as before.
- **Recovery files are read after the restore, not during it.** A restored
  PAR2, PAR3 or SFV set reads back only the bytes its checkpoint leaves
  out, after the job is back in place and off the main loop, one set at a
  time per job. The job downloads nothing until that read finishes, and
  nothing on a recovery file is written again.
- **A job that stops before its first checkpoint starts clean.** Since
  0.16.9 a job can stop before its first checkpoint is saved. Restarted
  that way, it started again over the files its last run wrote, so a
  split 7z joined before the stop could leave its join file in the
  completed folder beside the extracted file. A fresh start now clears
  the job's folders first.

## Downloads

- **Pipeline depth settles where it should.** The depth explorer took its
  baseline from the first window, while connections were still coming up
  and each one ran faster than it would once they shared the link. Every
  deeper trial then looked slower, reverted, and was held off for ten
  minutes. Now:
  - A server whose first probe takes 50 to 400 ms opens at depth 4 instead
    of 2.
  - The first climb on a server with no proven depth goes straight to the
    depth the link model asks for, and the next clean window confirms it.
  - A deeper depth that moves data as fast as the shallower one is kept.
  - A depth reached by guess that turns out to be unclean drops one step
    without a hold.
- **The storage behind each job is logged.** When a job is added, an
  `output storage` line says whether its output lands on network storage
  and whether the complete folder is on a different device. On Linux it
  also tells an SSD from a spinning disk.

## Database

- **Writes return their connection at once.** The database writer handed
  its connection back to the pool only when the next write came. Under
  load, enough connections stayed checked out to make reads time out
  waiting for the pool.

## Queue

- **Renaming a downloading job no longer splits its files.** Renaming a
  job between articles moved its working folder, but the writes still
  under way kept the old path, so the rest of the job went to a new folder
  with the old name. A job that is downloading now keeps its working
  folder; a job still waiting in the queue still has its folder moved. The new name still
  names the completed folder.

## Housekeeping

- **A failed job's files are deleted a day after it fails.** A new
  setting, **Delete incomplete files for failed jobs during housekeeping**,
  is on by default. Housekeeping removes the job's working and staging
  folders 24 hours after the failure; the history entry stays. It also
  removes a working folder whose marker names a job that no longer exists,
  once the marker is a day old. Folders without a Weaver marker are never
  touched.
- **Deleting a job from history removes its staging folder,** with or
  without its files. Reprocess and restart from history clear the job's
  old working folders first.

## Settings

- **Settings that do not parse fall back to the defaults.** Stored
  post-processing settings or a content policy that cannot be read used to
  stop Weaver from starting, or pause and fail jobs. Weaver now uses the
  defaults until the setting is saved again.
- **A failed read of the unacceptable-extensions setting no longer refuses
  every file.** Files wait until the setting can be read. If it still
  cannot when the job would deliver, the job pauses and names the setting.
- **Out-of-space errors say so.** A disk reserve refusal now names the
  folder, the free space, the write and the reserve, in GiB and MiB.

## Dependencies

- sevenz-turbo 0.30.0, lzma-turbo 0.12.0 and unrar-rs 0.16.1, which let
  Weaver lend their decoders a window.
- RUSTSEC-2026-0459 is allowed until 2026-12-09. The fix needs
  libsqlite3-sys 0.39, which sqlx 0.9.0 does not accept yet.

## Regression coverage

- **The crash test's read-back checks runs of articles.** The archive
  matrix's crash check read every article back one at a time at every
  checkpoint, which ran some PAR2 cases past the time limit. It now reads
  articles that lie end to end in one read.
- **The store-proof tests do an object's lent work.** Since key
  derivation is lent out, the test rig did not derive keys, so its
  encrypted RAR5 set never unlocked.
- **A loss during key derivation is tested on every pull request.** An
  encrypted stored RAR under a PAR2 set loses its last articles while its
  key is derived, and must still deliver its member whole.
- **Lent work is tested under every serve order.** 180 new tests run
  encrypted zip, 7z and RAR posts, flat and nested, in order, backwards,
  tail first, with articles lost or damaged beside PAR2, restarted after
  every article, and with each loan held and handed back late.
- **The extraction ratio test fails without its fix.** Its articles are
  now cut so the first volume is written before the later ones are seen,
  as in a real 100 MiB volume set.
- **Holds are tested at every budget.** Every hold budget at every site,
  two jobs on one budget, a lower ceiling after a restart, and every
  archive kind in every serve order under tight holds.
- **ZipCrypto keys run on loans only,** reading every cipher byte once.
- **The e2e compose retry removes dependent containers first.** Podman
  refuses to remove a container while another still shares its network,
  so the port-collision retry failed before it ran.
