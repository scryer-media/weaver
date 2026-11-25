# Weaver 0.17.2 release notes

Everything below is new since 0.17.1. The 0.17.1 tag did not produce a
release: its build failed its tests, and its arm64 Linux package waited
for a build machine that never came. 0.17.2 is the first release of the
0.17 line, so read the 0.17.0 and 0.17.1 notes with these.

## Fixes

- **A skipped file that is wanted again finds its peers.** When every file
  a torrent wanted was complete, Weaver let its peers go. Wanting a skipped
  file again could then leave the torrent idle, with no peer dialled, until
  a tracker or the DHT named a new one. The peers let go are now dialled
  again at once.
- **A freed socket on a full route starts one dial.** Usenet connections
  waiting for room on a full network route are let go one per freed socket.
  A late signal for a socket already taken could let a second one dial the
  full route, to be refused. The route is now asked for room first.

## Tests and build

- **Release test override.** A release whose tests failed can be approved
  by hand and published anyway. The approval is a reviewer on the
  `release-test-override` environment; without one, the release stays
  blocked.
- **arm64 Linux package.** It builds on the standard hosted arm64 runner,
  with 120 minutes.
- **Torrent regression case.** The test of a seed let go while nothing was
  wanted now covers the case above.
- **Full-route test.** The test of lanes waiting on a full route starts
  its lanes one at a time. Started together they contended for a lock, and
  one could give up before it joined the line, which hung the test.
