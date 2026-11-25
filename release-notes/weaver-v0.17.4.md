# Weaver 0.17.4 release notes

Everything below is new since 0.17.3. The 0.17.3 tag run did not publish
a container image: one torrent test failed on it, and the image job was
skipped. 0.17.4 carries the fix for that test, so read the 0.17.0 to
0.17.3 notes with these.

## Fixes

- **A gateway's private address is never shown as yours.** On a torrent
  leg through a tunnel, such as WireGuard, the gateway inside the tunnel
  answers a port mapping with the tunnel's own private address. That
  address could be shown as "Tracker sees" and as the gateway address
  when no tracker or peer had reported one yet. A private address is no
  longer shown there; the field stays empty until a public one is known.

## Tests and build

- **The failed seed-mode check test.** It asked for two pieces at once,
  and the undamaged one could be checked and sent before the damaged
  one failed. It now asks for the other piece only once the check has
  failed. This is the test that failed on the 0.17.3 tag run.
- **Web formatting.** Everything under `apps/weaver-web` is formatted
  with Prettier 3.10.1. The format check runs in CI and in the release
  script, and a failure blocks.
