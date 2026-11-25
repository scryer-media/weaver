# Weaver 0.17.3 release notes

Everything below is new since 0.17.2. The 0.17.2 tag did not produce a
release: its macOS arm64 package ran out of build time. 0.17.3 is the
first release of the 0.17 line, so read the 0.17.0, 0.17.1 and 0.17.2
notes with these.

## Fixes

- **A full weighted route hears the right sockets.** Usenet connections
  waiting on a full network route could keep listening for sockets that
  no longer free for them after the route's shares changed, and wait
  with no end. The line now listens to whatever the route names.
- **A failed seed-mode check stops all uploads.** When a torrent added as
  already complete had a piece fail its check, another piece could still
  be sent to a peer before the full recheck took over. Nothing is sent
  from that moment until the recheck is done.
- **A pause and a resume at the same moment.** On a magnet still fetching
  its metadata, a pause and a resume that overlapped could leave it
  resumed but idle until resumed again. The two now land one after the
  other.
- **A recheck on a full disk keeps seeding.** A torrent rechecked while
  it did not fit on the disk could end up paused, depending on timing.
  It now always keeps running with only its download held.
- **A restart on a nearly full disk keeps a started torrent going.** A
  torrent that had already downloaded keeps its place across a restart,
  even when what it has left no longer fits. Depending on timing at
  start-up it could be judged as a new start instead, and be held. It
  now keeps its place either way.
- **Room made as a write fails is seen.** A job waiting on a full disk
  compares free space with a reading from before its write failed. A
  disk check already running at the failure was always taken as that
  earlier reading, so room made in that moment went unseen and the job
  waited for a further rise. A check that read after the failure now
  counts as showing the room.
- **Export of a torrent file with extra bytes after it.** The
  qBittorrent-compatible export answered 409 to a Control key for a
  torrent file that ends in, say, a newline. It now exports, with those
  bytes as they were added.

## Faster

- **Trackers.** An announce answer updates its own tracker's row only,
  and replacing a torrent's trackers looks each one up once. A torrent
  with thousands of trackers no longer slows with the square of their
  number.
- **`torrents/info` with a sort.** The qBittorrent-compatible list sorts
  by the one column asked for and builds only the page it returns.
- **Port slots and web seeds.** Pruning saved port slots and removing
  refused web seeds each take one pass.
- **Torrent file export** does its work off the network threads.

## Tests and build

- **macOS arm64 package.** Its build has 240 minutes.
- **Tests that could wait with no end.** Several tests waited on one
  event while another outcome was possible, and hung when it came. Each
  now waits on what it checks, or ends when the other outcome arrives.
- **Tests that raced.** A torrent file-list test compared rows before
  the torrent had settled, and a full-disk test made room before the
  running disk check had read. Both now wait for that first.
