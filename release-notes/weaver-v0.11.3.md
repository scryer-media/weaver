# Weaver 0.11.3 release notes

## Highlights

Weaver 0.11.3 tightens recovery and connection-reset behavior. Direct-store
reconstruction now retains its buffered articles until the reconstruction work
hands them back, and NNTP group discovery can retry the request that revealed
the requirement without degrading the server's health.

## What changed

### Download and NNTP reliability

- **Group-required NNTP requests retry correctly.** A server's `412` response
  now records the required group and is treated as a transient result. The
  retry can select the learned group, rather than failing the article or
  reducing the server's health.
- **Queued work is returned before a client reset.** When server configuration
  resets owned download lanes, queued leases are returned to the scheduler
  before a worker can run them against the retired client. This preserves the
  articles and their scheduler accounting during a connection-generation
  change.

### Direct-store recovery correctness

- **Reconstruction-owned bytes are not flushed early.** An idle decode flush
  now leaves buffered articles with their reconstruction ticket until its
  handback completes. Other files can still drain normally, while the rebuilt
  volume retains the correct ordering and whole-file CRC verification.

### Dependencies

- Refreshed compatible web dependencies and tooling, including TanStack
  Virtual, React DOM type definitions, Oxlint, PostCSS, and their transitive
  packages. The frontend dependency graph contains no `lodash`; its audit
  reports no known vulnerabilities.

### Benchmark infrastructure

- The client benchmark harness and its published result data now live in the
  separate `scryer-media/usenet-bench` repository. Weaver retains a pointer
  under `ci/bench`.

## Upgrade notes

- No database migration or API schema change is required for this release.
- Existing direct-store and NNTP server configuration continues to work
  unchanged.
