# Weaver 0.16.15 release notes

Everything below is new since 0.16.14.

## Highlights

- **A crafted post can no longer make Weaver use unbounded memory, CPU or
  stack.** This release hardens the archive, repair and queue paths
  against posts built to exhaust them. Each fix bounds or streams the
  work; none adds a limit an honest download can reach.
- **Large ZIP archives open.** The ZIP reader's 8 MiB directory limit and
  50,000-entry limit are gone. A directory of any size is read a piece at
  a time as its bytes arrive.
- **When a PAR2 set and a PAR3 set disagree, PAR2 decides.** A post whose
  two sets describe different bytes for the same file ends with the bytes
  the PAR2 set describes, and the two sets can no longer repair each
  other's work in a loop.

## Archives

- **A ZIP central directory is read as its bytes arrive.** A stated
  directory size allocates nothing by itself, so a forged size costs no
  more than the bytes really posted. The configured extraction limits
  (`WEAVER_EXTRACTION_MAX_ENTRIES` and the rest) are unchanged.
- **A multi-threaded XZ decode stays inside its memory grant.** The
  decoder reserves one worker's memory for each thread it may run, and the
  volume's length caps the workers counted at the blocks it can hold, so a
  small file still reserves little. A file of a few blocks whose first
  block compresses worse than the rest may decode on fewer threads than
  before; it still completes.
- **A stream saved by an older version restores.** The saved stream format
  moves to version 5, and a version 4 stream is read as before.
- **A RAR volume announces one recovery record.** A volume carrying many
  recovery-record headers made a window for each and bound every one
  again whenever one attached. The first is announced, and a record that
  attaches binds alone.
- **A RAR4 header whose sizes wrap past 64 bits ends the volume walk**
  instead of sending it backwards.
- **Two RAR5 volumes claiming the same position no longer recurse.** A
  position a volume's own headers rule out places it nowhere.
- **A wrong RAR4 password is read back a piece at a time.** Re-sealing an
  encrypted range reads and enciphers 1 MiB at a time from its checkpoint,
  where it read the whole range into memory. The right password still
  opens the archive afterwards.
- **Displaced envelope bytes are read back a piece at a time.** When an
  archive's map moves bytes Weaver had kept, they are read and held 1 MiB
  at a time, where one read could cover a whole volume.
- **A name that ends inside a longer character is classified, not
  sliced.** A file name whose last four bytes split a multi-byte character
  could stop the classifier.

## Repair

- **PAR2 wins a conflict with PAR3.** Where the two sets agree, nothing
  changes. Where they disagree, the PAR2 set's bytes stand, and a later
  repair by the other set is turned away on bytes already repaired.
- **A PAR2 volume is read again no more than sixteen times its length.**
  A volume crafted with overlapping packets could be re-read without end.
- **A replay asked for during a replay runs after it,** and the sets a
  post names are founded one after another, where both could recurse to
  the depth of a crafted post.
- **Repair holds less memory.** Held copies go a piece at a time, recovery
  blocks beyond what the repair needs are not all held, and block cuts
  keep only the distinct boundaries.
- **A replay copy stops at its first refused write** and removes the
  partial file. It is no longer refused before it starts, so a repair
  that fits completes.
- **Matching files to a PAR2 set by length takes one pass.** A post with
  many same-length files that only their length can place was matched by
  rescanning every file for every file.
- **Binding asks only the sets that describe a member.** With many PAR2
  sets in one job, every posting was tried against every set on every
  recovery delivery. A pass where nothing changed no longer grows with
  the number of sets, and whether a member is covered is one lookup.

## Queue and downloads

- **A retried article shares its file's group list.** A retry after a
  failed download, a wait or a failed decode copied every newsgroup name
  of the file per article. With a large group list and many failing
  articles that reached gigabytes.
- **An add waiting to be installed holds no article list.**
- **Abandoning a dead post's file walks the queues once,** not once per
  lost article.
- **Releasing a name looks up only the members waiting for it.**

## Cleanup

- **A category folder that cannot be searched is passed over on Linux
  too.**

## Build

- **The release script runs its cheap checks first.**

## Regression coverage

- **Every fix has a test that counts work, bytes, reads or allocations,**
  never time.
- **The length-matching and set-binding changes are each checked against
  the code they replace:** 20,000 seeded shapes for length matching, and
  a job of six overlapping sets carried through binds, a repair and a
  restore for binding.
- **7z end-header sizes are held to the bytes posted.** A stated size of
  2^63, a size one byte past the file, and a size stated before any length
  is known each keep no more than the bytes that arrived.
