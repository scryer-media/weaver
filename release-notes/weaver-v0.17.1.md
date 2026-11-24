# Weaver 0.17.1 release notes

Everything below is new since 0.17.0. The 0.17.0 tag did not produce a
release: its build failed its tests on Linux and Windows. 0.17.1 is the
first release of the 0.17 line, so read the 0.17.0 notes with these.

## Fixes

- **A restart no longer fetches recovery volumes nobody asked for.** After
  a restart, Weaver reads a job's recovery volumes again from disk. When
  that reading finished, the PAR2 set could be handed to the checker before
  the job had been asked what it still needed. A set checked at once then
  left its recovery volumes marked as wanted, and they were downloaded
  although nothing needed them. The job is now asked first, as it is on
  every other path.

## Dependencies

- **crc-turbo 1.11.1.** The CRC library moves from 1.11.0 to 1.11.1.

## Tests and build

- **Windows tests.** Tests spell paths as the platform does and bind no
  loopback device. The restart-and-move runs box their futures, which
  overflowed the smaller Windows thread stack. The in-place hand-over test
  runs on Unix only, where a file's number can be read.
- **Tracker announce comparison.** The test that compares the old and new
  policy pass waits for a running torrent's tracker to hear it before it
  counts announces.
- **qBittorrent list polls.** The test that counts database statements
  across list polls waits for the API key's last-used write before it
  starts counting.
- **CI.** The clippy job has 45 minutes.
