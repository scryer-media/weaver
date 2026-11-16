# Weaver 0.16.17 release notes

Everything below is new since 0.16.16.

## Highlights

- **This release carries 0.16.16 to every platform.** The 0.16.16 tag
  failed one test on Windows, so its release did not finish. 0.16.17 is
  the same Weaver with that failure fixed. If you were waiting for
  0.16.16, read its notes: the disk-space holds, the per-disk writers,
  archiving a history entry, the old RAR sets and the hardening fixes are
  all here.

## WireGuard

- **A WireGuard socket that is not connected to one peer reads on after a
  refused datagram.** Windows reports a datagram sent to a closed port on
  the sender's next read, and such a socket then stopped reading for
  good. The tunnels Weaver builds use a connected socket, which already
  read past a refusal, so a tunnel you configured behaves as it did. The
  peer the tests run against used the other kind, went silent after a
  session gave up its port, and failed the test. The fix is in
  proxy-tunnels 0.5.3.
