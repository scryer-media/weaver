# Proxy profiles and consumer routing

Settings → Proxies manages HTTP CONNECT, SOCKS5, SSH and userspace WireGuard
profiles. Server and RSS editors assign up to eight distinct profiles in order.
Direct access is a separate final fallback. Assigning the first profile in the
UI turns that fallback off. Existing consumers retain direct access, and API
updates that omit `routing` preserve the saved policy. An empty ladder with
direct access disabled is blocked.

Profiles can be disabled without removing their assignments. Referenced
profiles cannot be deleted. SSH supports password or Ed25519 authentication,
including encrypted keys. Successful authentication persists the first host-key
fingerprint; subsequent connections check that pin before authenticating.
Resetting trust is an explicit settings action. WireGuard imports retain the
first peer, DNS, MTU and keepalive, display ignored-field/additional-peer
warnings, and never execute hooks.

## Runtime and persistence

`engines/weaver-tunnel` owns protocol implementations and local SOCKS bridges.
`weaver-server-core::proxies` owns encrypted persistence, policies, revisions,
health, cooldowns and one explicitly initialized application runtime. Source
provenance and dependency features are recorded in
`engines/weaver-tunnel/PROVENANCE.md`.

SSH and WireGuard sessions are reused per profile revision. Bridges carry both
async and blocking NNTP sockets; TLS uses the original provider name. Proxied
connections expose no provider IP rather than reporting loopback as the
provider. Direct NNTP retains its existing TCP path with a revocation registry.

Every production loopback bridge requires a fresh 256-bit random secret.
Async/blocking NNTP, certificate inspection and RSS authenticate before asking
it to connect. Credentials stay in memory and are excluded from diagnostics.
SOCKS negotiation is limited to five seconds; cancellation during dialing
stops the pending ladder and preserves at most 8 KiB of pipelined client data.

Each connection tries permitted routes sequentially. Transport failures cool a
consumer/profile pair for 30 seconds; one recovery probe is admitted after the
deadline. Destination authentication, certificate errors and missing articles
keep their existing NNTP semantics. Failed transfers are discarded before an
article retry. Healthy connections continue until their own policy or profile
changes.

NNTP reports unexpected response loss through a per-connection callback for
the selected route. Orderly QUIT, authentication rejection and certificate
inspection do not mark a healthy route as failed. Route establishment has its
own bounded budget; pool-capacity waits and subsequent NNTP commands retain
their existing soft timeout, including extra download lanes.

RSS uses the same policy for polling, redirects and originating NZB downloads.
Each candidate route resolves and validates destination addresses before
connecting to those exact addresses. HTTP Host/TLS names remain unchanged.
HTTP CONNECT, SOCKS5 and SSH use bounded DNS-over-TCP to configured resolver
IPs; WireGuard uses its tunnel resolver. Environment proxy settings cannot
override the policy. Cross-origin redirects do not carry feed credentials.

Migration 46 supplies equivalent SQLite/PostgreSQL profile and route tables.
Credential fields are one encrypted JSON value under Weaver's existing key;
API responses provide presence flags. Key verification and both backup paths
include profiles and policies. Legacy archives without proxy tables restore
the direct defaults. Writes complete before new revisions activate, and
mutations await closure of obsolete routes and sessions.

Profile saves and SSH trust writes serialize on the stored row. Ordinary
edits preserve a fingerprint learned concurrently; only explicit trust reset
clears it. Credential re-encryption can retain the revision when the public
profile is unchanged.

Limits include 128 profiles, eight routes per consumer, 4,096 cached consumers,
1,024 connections per bridge, bounded DNS records/messages and bounded tunnel
socket buffers. Shutdown revokes bridges and sockets and stops tunnel tasks.
No OS tunnel interface, host route or firewall change is created.
Authentication prevents other local processes from forwarding through a bridge,
but local connection floods can still occupy its bounded pre-authentication
slots until the five-second handshake deadline. Shared-host denial-of-service
hardening remains a low-severity follow-up.

## Validation

Validated locally on Apple Silicon with isolated loopback fixtures and
disposable databases:

- Full workspace Nextest sweep after review fixes: 3,725 passing tests, ten
  default skips (live-provider checks and opt-in diagnostics/performance tests).
- Rust formatting and prescribed host Clippy targets with warnings denied.
- Frontend lint/typecheck, production build, 33 unit tests and five schema
  compatibility utility tests.
- All 99 executable frontend GraphQL documents validate against the regenerated
  schema. There are no breaking schema changes; the strict compatibility tool
  flags the two planned optional server/feed `routing` inputs as dangerous
  additions.
- Protocol, authentication/trust, WireGuard handshake/TCP/UDP/DNS/lifecycle,
  routing order/cooldown/recovery/cancellation, revoked direct sockets,
  blocked host access, DNS validation, redirects, inherited NZB routing,
  truncated response and active-response cancellation fixtures.
- Plain NNTP, implicit TLS and async STARTTLS through all four profile types;
  blocking NNTP where already supported; original-name certificate inspection,
  pooled article retry and provider IP accounting.
- Encrypted profile/policy/trust backup round-trip, wrong-key rejection,
  replacement-key rewriting, legacy defaults and settings authorization.
- Review regressions cover unauthenticated/wrong bridge credentials, distinct
  bridge secrets, canceled dialing, pipelined byte preservation, concurrent
  SSH trust/save operations, normal and unexpected NNTP closure in async and
  blocking clients, article/lane failover budgets and bounded pool waits.
- Browser checks against a disposable Weaver instance: all four profile editors,
  WireGuard paste/import warnings and values, server/RSS assignment, ladder
  reordering/persistence, direct-fallback defaults and referenced deletion errors.

The opt-in `local_route_throughput` test transfers three verified 16 MiB samples
per route. The post-review run produced these median rates:

| Transport | MiB/s |
| --- | ---: |
| Direct, existing socket path | 714.0 |
| Direct, revocation registered | 703.5 |
| HTTP CONNECT bridge | 688.0 |
| SOCKS5 bridge | 676.6 |
| SSH bridge | 152.3 |
| WireGuard bridge | 38.6 |

These are debug/test-build transport measurements, including byte verification,
not production or WAN throughput guarantees. The approximately 1.5% difference
between the direct samples is within the scope of this local measurement.

PostgreSQL runtime tests were not exercised: no disposable PostgreSQL server is
installed. An Intel macOS cross-check was attempted, but the compiler in use
lacks that target's standard library. Linux and Windows builds were not run.
Blocking STARTTLS remains unsupported as in the base implementation.

## Reproduction

From this worktree:

```sh
rtk cargo fmt --all -- --check
rtk cargo clippy --workspace --offline --locked --lib --bins --tests --examples --benches -- -D warnings
rtk cargo nextest run --workspace --offline --locked --no-fail-fast
rtk proxy cargo nextest run -p weaver-nntp --test proxy_routing --offline --locked --run-ignored only --no-capture local_route_throughput
```

The network fixture suites require local listener permission. Frontend commands
are `npm run lint`, `npm test`, `npm run test:graphql-compat` and `npm run build`
under `apps/weaver-web`. No release, publication or deployment is part of this
implementation or validation.
