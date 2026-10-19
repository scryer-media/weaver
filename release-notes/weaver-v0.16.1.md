# Weaver 0.16.1 release notes

## Release validation fixes

- **Native Windows error fixtures.** Connection-failure and descriptor-capacity
  tests use Windows OS error numbers instead of passing C-runtime error numbers
  to Windows error decoding. Unix fixtures retain their native error codes.
- **Explicit proxy rejection.** The proxy-pool test receives a SOCKS rejection
  from a running fixture instead of racing a closed port against a connection
  timeout. It checks the destination, reply code, and reporting member.
- **Isolated WireGuard scenarios.** Tunnel setup, target forwarding, and
  missing-name resolution each use fresh peers. A missing resolution now reports
  the returned fault and failing proxy in the test output.
