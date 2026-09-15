# Proxy profiles and consumer routing

Settings → Proxies manages HTTP CONNECT, HTTP/3 CONNECT, SOCKS5, SSH and userspace WireGuard
profiles. Server and RSS editors assign up to eight distinct profiles in order.
Direct access is a separate final fallback. Assigning the first profile in the
UI turns that fallback off. Existing consumers retain direct access, and API
updates that omit `routing` preserve the saved policy. An empty ladder with
direct access disabled is blocked.

Profiles can be disabled without removing their assignments. Referenced
profiles cannot be deleted. SSH requires an Ed25519 private key, including
encrypted keys with passphrases. Password authentication is not supported.
Successful authentication persists the first host-key fingerprint. Configured
pins are checked during key exchange, and persisted trust is authorized again
before every forwarded channel, including channels on cached sessions.
Resetting trust is an explicit settings action. WireGuard imports retain the
first peer, DNS, MTU and keepalive, display ignored-field/additional-peer
warnings, and never execute hooks.

## Runtime and persistence

`engines/weaver-tunnel` adapts the tag-pinned `proxy-tunnels` SSH/WireGuard/HTTP3
engine and owns HTTP CONNECT/SOCKS5 upstream transports, routed DNS and bridges.
`weaver-server-core::proxies` owns encrypted persistence, policies, revisions,
health, cooldowns and one explicitly initialized application runtime. Source
provenance and dependency features are recorded in
`engines/weaver-tunnel/PROVENANCE.md`.

SSH, WireGuard and HTTP/3 sessions are reused per profile revision. Async and blocking
NNTP use revocable in-process streams. Async s2n accepts the routed stream
directly; blocking s2n uses synchronous receive/send callbacks over that stream,
with a stable boxed callback context and bounded I/O waits on the owned runtime.
No loopback socket or forwarding task carries NNTP bytes. Rustls, plain NNTP,
STARTTLS and certificate inspection use the same routed stream abstraction.
TLS retains the original provider name, and proxied connections expose no
provider IP. Direct host connections retain their TCP path and s2n's FD path.
This removes relay copies and kernel crossings, but is not end-to-end zero-copy:
smoltcp and TLS still copy data into their receiving buffers.

Every production loopback bridge requires a fresh 256-bit random secret.
RSS authenticates before asking a bridge to connect. In-process NNTP callers
hold an internal route handle. Credentials stay in memory and are excluded from diagnostics.
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
HTTP CONNECT, HTTP/3 CONNECT, SOCKS5 and SSH use bounded DNS-over-TCP to configured resolver
IPs; WireGuard uses its tunnel resolver. Environment proxy settings cannot
override the policy. Cross-origin redirects do not carry feed credentials.

HTTP/3 profiles require an HTTP/3 forward proxy with a publicly trusted TLS
certificate and reachable UDP port (443 by default). Optional Basic credentials
authenticate CONNECT requests. The profile connection test opens a tunnel to a
configured DNS server IP on port 53; without one, test an assigned NNTP server.
There is no automatic HTTP/1 downgrade. Direct access remains controlled by the
consumer's fallback switch. Each session supports up to 128 admitted streams;
QUIC connection receive/send windows are 32 MiB, and stream receive windows are
2 MiB. Up to four sessions, including draining connections, can coexist.

RSS relay pumps use 64 KiB buffers in each direction. The 1,024-connection
bridge limit bounds these copy buffers to 128 MiB per bridge. NNTP retains its
direct in-process stream path through s2n or rustls.

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
1,024 connections per bridge shared across relay and in-process callers,
bounded DNS records/messages and bounded tunnel
socket buffers. Shutdown revokes bridges and sockets and stops tunnel tasks.
No OS tunnel interface, host route or firewall change is created.
Authentication prevents other local processes from forwarding through a bridge,
but local connection floods can still occupy its bounded pre-authentication
slots until the five-second handshake deadline. Shared-host denial-of-service
hardening remains a low-severity follow-up.

## WireGuard download tuning

Weaver opts into the shared engine's download tuning; Scryer's existing
constructors retain 64 KiB TCP buffers and OS UDP defaults. Weaver's TCP
sockets have 1 MiB receive and send buffers, with negotiated window
scaling. This provides window headroom for gigabit aggregate downloads across
twenty connections at 100 ms RTT. Twenty sockets reserve 40 MiB of TCP buffer
storage. The allocation cap includes connecting and lingering sockets, limiting
TCP buffers to 512 MiB per tunnel; cancelled connections release their buffers
immediately. When the allocation cap is reached, new dials fail until closing
sockets are reclaimed. Fully acknowledged TIME-WAIT sockets can be reclaimed
under buffer pressure, preserving capacity during rapid reconnects. Linger
deadlines wake the stack even without other traffic.

The encrypted UDP socket requests a 4 MiB receive buffer to absorb packet
bursts. Kernels may clamp or reject the request; rejection preserves the OS
defaults. No system networking settings change. Ready TCP work yields to other
tasks without waiting for a zero-duration timer to advance the timer wheel.
MTU, encryption, DNS routing and direct-access policy retain their behavior.

For a gigabit provider, twenty download connections are a useful starting
point when the provider permits them. A single connection at high RTT remains
limited by its TCP window; this tuning targets aggregate download throughput.

## Validation before shared-engine extraction

Validated locally on Apple Silicon with isolated loopback fixtures and
disposable databases:

- Full workspace Nextest sweep after WireGuard tuning: 3,730 passing tests, eleven
  default skips (live-provider checks and opt-in diagnostics/performance tests).
- The new TCP allocation cap initially exposed a stall in the existing rapid
  reconnect test: the test peer exhausted buffers retained in TIME-WAIT.
  Reclaiming fully acknowledged closes under pressure and scheduling linger
  deadlines fixed it. The unchanged reconnect test passes, together with new
  cancellation, buffer-limit, idle-reclamation and download-backpressure tests.
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

### Optimized WireGuard measurements

The `wireguard_download_throughput` fixture uses real WireGuard encryption and
loopback UDP, the authenticated SOCKS bridge and verified TCP downloads. An
optional bounded queue delays decrypted packets entering the peer, including
download acknowledgements, to model the TCP effect of RTT. This isolates window
behavior without a second UDP relay's burst loss. It is an asymmetric latency
simulation, not a physical WAN, loss simulation, or complete NNTP application
benchmark. Handshake and connection setup are outside the download timer.

Apple Silicon, optimized `e2e` Cargo profile, median of three samples, 8 MiB
per connection; rates are application payload Mbps:

| ACK delay | Connections | 64 KiB windows | 1 MiB windows |
| --- | ---: | ---: | ---: |
| 0 ms | 1 | 888.4 | 1040.7 |
| 0 ms | 20 | 1295.4 | 1139.4 |
| 50 ms | 1 | 8.5 | 114.0 |
| 50 ms | 20 | 185.6 | 1123.4 |
| 100 ms | 1 | 4.4 | 59.0 |
| 100 ms | 20 | 95.9 | 1074.4 |

Both columns use identical UDP tuning, pump behavior and fixtures; only TCP
buffer capacity differs. The comparison temporarily restored the old 64 KiB
capacity, then restored 1 MiB before validation. Loopback rates vary with
scheduling and burst size; the gain is primarily in keeping multiple downloads
busy at higher RTT. Actual provider throughput also depends on its connection
limits, congestion, packet loss, CPU capacity and tunnel MTU.

Longer optimized runs, three samples of 32 MiB per connection (640 MiB across
twenty downloads), produced these median aggregate rates:

| ACK delay | Twenty-connection payload Mbps |
| --- | ---: |
| 0 ms | 1318.3 |
| 50 ms | 1324.5 |
| 100 ms | 1369.6 |

After the final socket-reclamation correction, three repeated 32 MiB samples
on the busy host measured 896.5, 889.2 and 875.9 Mbps across twenty connections
at 0, 50 and 100 ms ACK delay respectively. Other builds, tests and a virtual
machine were consuming CPU during this recheck; contention may contribute to
the lower rates. These results do not establish guaranteed 1 Gbps throughput
on every system or physical VPN link. The earlier sustained measurements
preceded that lifecycle correction.

The existing optimized `local_route_throughput` measurement also passed:
WireGuard 171.4 MiB/s; direct 3000.0 MiB/s without revocation registration and
3004.1 MiB/s with registration. These measurements include payload verification.
No dependency versions or features changed for this tuning.

## Shared-engine and direct-I/O validation

Before HTTP/3, shared-engine validation used the former `v0.20.0` tag from
`scryer-media/proxy-tunnels`, resolving to `7c63bad3f43e8aaea2ffa5b9190866406ce4eb10`.
No local path override was required. Protocol source and dependency files were
identical to the validated commit `8c54eb1348d58afe72cd158fe0d084074617a3a1`.

- Shared crate: 79 tests, formatting and all-target/all-feature Clippy passed.
- Scryer's application crate compiled unchanged against that shared source in
  an isolated snapshot of its Git-pinned integration. All 27 selected tunnel
  and transport integration tests passed; the remaining Scryer tests were not run.
- Weaver's full locked workspace Nextest sweep passed: 3,676 tests, 11 skipped.
- Weaver formatting, prescribed workspace Clippy, frontend lint/build,
  33 frontend tests and five GraphQL compatibility utility tests passed.
- Explicit async-s2n runs passed through all four proxy types, including plain
  NNTP, implicit TLS, STARTTLS, certificate inspection and article retry.
- Blocking s2n callbacks pass stalled-read deadline and immediate route-revocation
  tests. Fatal I/O errors are no longer retried based on s2n's residual blocked
  status; retryability follows the error type.
- Direct-stream tests prove operation after the loopback listener is stopped,
  cancellation of abandoned dials, shared connection admission, revocation of
  blocked reads/writes and waiting for pending dials to unwind.
- Apple Silicon validation and Intel macOS checks for the tunnel/NNTP crates
  passed. The Intel check used rustup's installed compiler and target libraries;
  the Homebrew compiler lacks the cross-target standard library.

The release-profile benchmark verified 27 samples of 640 MiB each (16.875 GiB
total), with twenty connections. Median payload rates from three samples:

| ACK delay | WireGuard relay Mbps | Direct WireGuard Mbps | Direct WireGuard + s2n Mbps |
| --- | ---: | ---: | ---: |
| 0 ms | 495.0 | 717.1 | 799.4 |
| 50 ms | 372.1 | 333.2 | 556.7 |
| 100 ms | 354.2 | 628.4 | 601.4 |

These timings were noisy: individual rates ranged from 168.6 to 986.2 Mbps.
A concurrent CPU snapshot showed a VM consuming over five cores, alongside
other processes. The measurements verify payload integrity through the new
path but do not establish a reliable speedup or sustained 1 Gbps on this host.
The s2n column includes TLS; the other two columns measure WireGuard-encrypted
TCP without TLS. The fixture does not include NNTP decoding or disk writes.

PostgreSQL runtime tests and Linux/Windows builds were not run. Blocking
STARTTLS remains unsupported as in the base implementation.

## HTTP/3 integration validation

The current shared engine uses signed tag `v0.1.0`, resolving to
`01481be7fca27a7864fe40e9a4b95fcfa9698ce2`. The standalone crate version sequence
was reset before this integration; the old `v0.20.0` tag mentioned in historical
results above is no longer published. The current tag includes bounded attempts
across proxy endpoint addresses and handling of informational CONNECT responses.

Apple Silicon validation with disposable local fixtures:

- Locked full workspace Nextest: 3,688 passed, 11 existing opt-in tests skipped.
- Formatting and prescribed host Clippy targets with warnings denied passed.
- Explicit s2n proxy routing: six tests passed across all five transports,
  including blocking read deadlines/revocation, async STARTTLS, destination
  certificate inspection, provider identity and pooled article retry.
- HTTP/3 RSS fixtures verified routed DNS, validated destination IPs, redirects,
  private-address rejection, credential stripping across origins and inherited
  NZB routing without duplicate submissions. Failed HTTP/3 establishment never
  opened the host destination, and a profile test without DNS never tried TCP.
- Frontend lint/typecheck, production build, 33 tests and five schema utility
  tests passed. All 99 executable GraphQL documents validate. Schema comparison
  reports only the expected `HTTP3_CONNECT` enum addition as dangerous, with no
  breaking changes; the compatibility checker remains strict.
- Native macOS, Linux and Windows feature graphs have no active ring backend.
  No unrelated registry versions or Windows dependency assignments changed.
- Intel macOS compile checks passed for the tunnel and NNTP crates, using
  rustup's installed compiler and target standard library.

The optimized `local_route_throughput` harness verified three 16 MiB samples
per route. Median payload rates on this host:

| Route | MiB/s |
| --- | ---: |
| Direct, existing socket path | 2855.9 |
| Direct, revocation registered | 2903.0 |
| HTTP CONNECT relay | 3077.0 |
| SOCKS5 relay | 3070.2 |
| SSH relay | 840.2 |
| WireGuard relay | 171.6 |
| HTTP/3 CONNECT relay | 327.1 |

These short, same-machine measurements include byte verification. They do not
measure WAN latency/loss, NNTP decoding or disk writes, and do not establish a
provider throughput guarantee. PostgreSQL runtime tests, Linux/Windows execution
and browser-driven UI checks were not rerun for this addition.

## Application e2e fallback validation

The `proxy-routing` behavior flow now runs in the e2e full suite on SQLite and
PostgreSQL. It drives the public GraphQL API against the Linux Weaver binary,
with an isolated mixed HTTP CONNECT/SOCKS5 ladder, fake NNTP articles, RSS/NZB
responses and a DNS/destination canary. Explicit direct controls prove that the
canary sees host connections and host DNS; blocked cases require zero such
events. It covers ordering, disabled profiles, empty/exhausted blocked ladders,
allowed direct fallback, real-time cooldown/primary recovery, redirects,
inherited NZB routing, article retry after proxy loss and active direct-stream
revocation. Protocol-specific SSH, WireGuard and HTTP/3 fixtures remain in Rust.

The first run exposed a save-time validation bug: changing an active server to
disallow direct fallback was rejected when all proxies were down. Routing
changes with unchanged endpoint, credentials, TLS identity and activation state
now persist without requiring connectivity, then revoke the old routes before
the mutation returns. Endpoint/authentication changes and server activation
retain normal connection validation.

Validation after that fix:

- The complete new flow passed on disposable SQLite and PostgreSQL stacks,
  each in about 38 seconds, including the real 30-second recovery cooldown.
- Three Node fixture tests passed, covering host-DNS observation, bounded
  destination forwarding and pipelined HTTP CONNECT/SOCKS5 authentication.
- The affected Go harness and Compose-isolation packages passed. Their existing
  corpus-dependent unit test used the already available local fixture assets.
- Rust formatting and prescribed host Clippy passed; locked full workspace
  Nextest passed 3,688 tests with 11 existing skips.
- E2e TypeScript checks and project audits passed. No dependency changes.

The remaining full e2e flows were not rerun. Each proxy flow saves ordered
canary events and case boundaries as `proxy-routing-evidence.json` alongside
the Playwright report. Run it independently from `e2e` with
`task release-gate -- proxy-routing`.

## Reproduction

From this worktree:

```sh
rtk cargo fmt --all -- --check
rtk cargo clippy --workspace --offline --locked --lib --bins --tests --examples --benches -- -D warnings
rtk cargo nextest run --workspace --offline --locked --no-fail-fast
rtk proxy cargo nextest run --release -p weaver-nntp --test proxy_routing --offline --locked --run-ignored only --no-capture --no-fail-fast local_route_throughput
rtk proxy env WEAVER_NNTP_TLS_BACKEND=s2n cargo nextest run -p weaver-nntp --test proxy_routing --offline --locked --no-fail-fast
rtk proxy env WEAVER_NNTP_TLS_BACKEND=s2n WEAVER_THROUGHPUT_MIB=32 WEAVER_THROUGHPUT_SAMPLES=3 WEAVER_THROUGHPUT_CONNECTIONS=20 cargo nextest run --release -p weaver-nntp --test wireguard_throughput --offline --locked --run-ignored only --no-capture --no-fail-fast
```

The network fixture suites require local listener permission. Frontend commands
are `npm run lint`, `npm test`, `npm run test:graphql-compat` and `npm run build`
under `apps/weaver-web`. No release, publication or deployment is part of this
implementation or validation.
