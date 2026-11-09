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
- **The e2e compose retry removes dependent containers first.** Podman
  refuses to remove a container while another still shares its network,
  so the port-collision retry failed before it ran.
