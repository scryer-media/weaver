# Security Policy

## Reporting a vulnerability

Please report security vulnerabilities privately through [GitHub Security Advisories](https://github.com/scryer-media/weaver/security/advisories/new). Do not open a public issue while a vulnerability is being investigated.

Include affected versions, reproduction steps, impact, and any proposed mitigation. We aim to acknowledge reports within seven days and provide a remediation plan or status update within 30 days.

Please allow time for a coordinated disclosure before publishing details. We will credit reporters who would like to be named.

## Security invariants

These hold for every change. A code review checks the diff against each one it touches. A change that breaks one is a defect, not a trade-off.

### No surprise leaks: routes, proxies and DNS

The app does what the operator configured, and nothing else. No connection, datagram, listener or name lookup leaves by a path the saved route does not name.

- **One way out.** Every outbound connection leaves on a saved route. Only the routing layer dials, binds, listens, joins multicast, resolves names or builds HTTP clients. Everything else asks it (`proxies::routed_fetch`, `proxies::with_routed_client`, the leg's dialer). `clippy.toml` bans the direct calls; each exception carries `#[expect(clippy::disallowed_methods, reason = ...)]` and is pinned by `proxies/route_guard_tests.rs`. A new exception needs the operator's approval.
- **Fail closed.** A proxy that is down, disabled, misconfigured or still opening stalls its consumer. It never falls back to the host's own address. The only direct fallback is the one the operator saved on that route.
- **Names follow the route.** A name is resolved the way the route's naming says: by the host, by listed servers, by the proxy's exit, or by servers reached through the proxy. No name reaches the host resolver unless the route says the host resolves. The `host_name_lookup` lint (`lints/host_name_lookup`) and `clippy.toml` enforce this; the host lookup has one entry point.
- **A name is dialled where it was resolved.** An address learned on one leg or rung is never dialled on another. A rung that did not answer the lookup asks its own naming.
- **No downgrade.** DNS over TLS or HTTPS that fails its handshake or certificate check fails the lookup. It does not retry in plain text or on another server class.
- **Tunnel endpoints too.** The name of a proxy or tunnel endpoint is resolved by the resolver Weaver injects, on the path below that hop. The tunnel library never resolves it itself.
- **A torrent route is one exit.** One leg, one rung, no direct fallback (`TorrentRouteFault`). A torrent on a tunnel route takes no host port, and its trackers, DHT, peers, web seeds, port mapping and local discovery use that route's egress only. Two torrents on different routes never share a socket.
- **Changes cut over cleanly.** Saving a route change ends the old route's connections and opens before the new route is used. Nothing keeps flowing on the path the operator just removed. A save that changes nothing touches nothing.
- **The setting shown is the setting used.** A route, proxy or naming setting takes effect on every consumer it names: NNTP, RSS and other fetches, updates, trust-root refreshes, speed tests, trackers, DHT, peers and web seeds. A new consumer joins a route before it ships; there is no unrouted default.
- **Identity is not leaked sideways.** The host's address is never sent to a tracker, peer or proxy exit as a parameter when the route hides it. Local service discovery, UPnP and NAT-PMP act only on the egress the route names.

### Torrent surface

Torrent files, magnets, trackers, peers, DHT nodes and web seeds are attacker input.

- **Bounded parse.** A torrent file is at most 100 MiB and 10,000,000 bencode tokens; the piece count is capped at libtorrent's `0x200000`. Metainfo work is linear in its size: no per-file rescan of paths, no recursion on depth.
- **Limits before work.** Per-source limits apply before a packet is parsed (DHT, uTP, peer wire, PEX). A frame is measured by its bytes on the wire, not by the fields recognised in it. An unsupported extension is dropped once, cheaply, and is not logged per message.
- **Written bytes stay inside the torrent.** A path from metainfo is cleaned and joined under the torrent's save location; no component escapes it. A file the operator set to "do not download" is never created or written in place: the piece bytes that overlap it go to the side store.
- **Deletes are narrow.** Removing a torrent's data deletes only the files that torrent lists, then folders only once empty. Never a recursive delete at a path taken from metainfo. Only folders Weaver can prove it manages (a marker or a database row) are removed.
- **Seed only what is verified.** A piece is advertised and served only when its hash was checked against data now on disk. A file that is missing or short after a restart puts the torrent in the files-missing state; it is not announced as held.
- **Trackers and web seeds are fetched like hostile URLs.**
  - A host named by the torrent is checked as a resolved address. Where the proxy resolves names, a plainly local name is refused: `localhost`, a single label, `.lan`, `.local`, `.internal`, `.home.arpa`.
  - A loopback tracker follows libtorrent's rule exactly: the path starts with `/announce` and has no `/`, `\` or `%` after it. A loopback scrape is never sent.
  - A redirect from a public address to a local one is refused. Trust granted to an operator-added tracker does not carry across a redirect.
  - Internationalised host names are refused where libtorrent refuses them.
  - TLS certificates are always verified.
  - A cookie is sent only to the origin it was saved for and does not follow a redirect to another origin.
- **Credentials stay with their scope.** Passkeys and other tracker, web seed, RSS and proxy URL secrets are shown only to an Admin key. Read and Control keys, the compatibility facades, events, metrics, logs and error text get the redacted form. A request error is stored with its URL stripped.
- **Private torrents** use their trackers only: no DHT, no peer exchange, no local discovery.
- **The API costs what it returns.** `/graphql` checks the credential before reading a body. A socket holds at most 32 live subscriptions; a credential holds at most 8 sockets. State-changing facade calls are POST only. A poll or subscription never rebuilds every torrent per tick.
- **Listeners are accounted.** Every listening socket is taken through the route's ledger and released when its session ends.
- **No bound that breaks real downloads.** A new limit comes with a measured margin over real torrents, swarms and clients, and its refusal says what was refused. With no data, ask the operator.
