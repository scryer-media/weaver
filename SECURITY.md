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
- **Names follow the route.** A name is resolved the way the route's DNS profile says: by the system, by the proxy, or by Weaver asking the profile's listed servers. No name reaches the host resolver unless the profile says the system answers. On a leg with no proxy, a profile that has the proxy answer fails the lookup; the system answers there only when the profile says so. The `host_name_lookup` lint (`lints/host_name_lookup`) and `clippy.toml` enforce this; the host lookup has one entry point.
- **A name is dialled where it was resolved.** An address learned on one leg or rung is never dialled on another. A rung that did not answer the lookup asks its own naming.
- **No downgrade.** A DNS server reached over TLS or HTTPS that fails its handshake or certificate check is not asked in plain text. The next listed server is asked instead, and a plain server is never asked after an encrypted one. When no listed server answers, the lookup fails.
- **Tunnel endpoints too.** The name of a proxy or tunnel endpoint is resolved by the resolver Weaver injects, on the path below that hop. The tunnel library never resolves it itself. Each proxy selects the DNS profile that finds its own address. On the host's profile, a first hop's address is looked up by the system resolver from the egress, and an inner hop's by the hop that carries it: a SOCKS5 or HTTP proxy is handed the name, a WireGuard tunnel's DNS servers are asked through it, the host is never asked, and a tunnel with no DNS servers fails the dial and is refused on save. A proxy name profile asks its listed servers instead, from the egress for a first hop and on the path below the hop for an inner one; a route's profile never does. There is one session per egress and path, whichever routes ride it. Pointing a proxy at another profile, or editing that profile, ends no session: it closes the DNS connections kept for the old setting, and the next lookup of its address uses the new setting.
- **A torrent route is one exit.** One leg, one rung, no direct fallback (`TorrentRouteFault`). A torrent on a tunnel route takes no host port, and its trackers, DHT, peers, web seeds, port mapping and local discovery use that route's egress only. Two torrents on different legs never share a socket. Torrent routes on the same leg share its session, whatever their DNS profiles: each torrent's tracker and web seed lookups name by its own route's profile, and the session's own, its DHT's bootstrap among them, by that of the first of those routes by key.
- **Changes cut over cleanly.** Saving a route change ends the old route's connections and opens before the new route is used. Nothing keeps flowing on the path the operator just removed. A save that changes nothing touches nothing. A DNS profile or server list change ends no connection that carries traffic: it wipes the answer cache and closes the DNS connections kept for the old setting, and every later lookup uses the new setting. A connection to a DNS server kept between lookups belongs to the proxy or tunnel instance that opened it: it closes when that instance is edited, turned off or retired, and a rebuilt instance never uses it.
- **The setting shown is the setting used.** A route, proxy or naming setting takes effect on every consumer it names: NNTP, RSS and other fetches, updates, trust-root refreshes, speed tests, trackers, DHT, peers and web seeds. A new consumer joins a route before it ships; there is no unrouted default.
- **Identity is not leaked sideways.** The host's address is never sent to a tracker, peer or proxy exit as a parameter when the route hides it. Local service discovery, UPnP and NAT-PMP act only on the egress the route names.

### Torrent surface

Torrent files, magnets, trackers, peers, DHT nodes and web seeds are attacker input.

- **Bounded parse.** A torrent file is at most 100 MiB and 10,000,000 bencode tokens; the piece count is capped at libtorrent's `0x200000`. Metainfo work is linear in its size: no per-file rescan of paths, no recursion on depth.
- **Limits before work.** Per-source limits apply before a packet is parsed (DHT, uTP, peer wire, PEX). A frame is measured by its bytes on the wire, not by the fields recognised in it. An unsupported extension is dropped once, cheaply, and is not logged per message.
- **Written bytes stay inside the torrent.** A path from metainfo is cleaned and joined under the torrent's save location; no component escapes it. A link under the save location is followed only when it leads to a place inside that location; a file reached through a link that leads outside is not opened, read, written, grown or deleted, and the refusal says why. A move checks its folders the same way before it starts. A file the operator set to "do not download" is never created or written in place: the piece bytes that overlap it go to the side store.
- **Save locations follow the key.** A Control key may add, save or move a torrent only at or under the default save folder or a category's folder, compared by name after cleaning, with no `..`; elsewhere it is refused (HTTP 403 on the facade, FORBIDDEN on GraphQL) and nothing is written. Only an Admin key may name any other path.
- **Deletes are narrow.** Removing a torrent's data deletes only the files that torrent lists, then folders only once empty. Never a recursive delete at a path taken from metainfo. Only folders Weaver can prove it manages (a marker or a database row) are removed.
- **Seed only what is verified.** A piece is advertised and served only when its hash was checked against data now on disk. A file that is missing or short after a restart puts the torrent in the files-missing state; it is not announced as held.
- **Trackers and web seeds are fetched like hostile URLs.**
  - A host named by the torrent is checked as a resolved address. Where the proxy resolves names, a plainly local name is refused: `localhost`, a single label, `.lan`, `.local`, `.internal`, `.home.arpa`.
  - A loopback tracker follows libtorrent's rule exactly: the path starts with `/announce` and has no `/`, `\` or `%` after it.
  - A redirect from a public address to a local one is refused. Trust granted to an operator-added tracker does not carry across a redirect.
  - Internationalised host names are refused where libtorrent refuses them.
  - TLS certificates are always verified.
  - A cookie is sent only to the host it was saved for, on any port, as a browser scopes a cookie. It does not follow a redirect to another host, and is not sent over http once the fetch began over https.
- **Credentials stay with their scope.**
  - A tracker URL with its passkey, and an exported `.torrent` that carries one, are shown to an Admin or a Control key, as qBittorrent shows them to any signed-in user. A Read key gets the redacted form, on the API and on the compatibility facades.
  - Web seed, RSS and proxy URL secrets are shown only to an Admin key. Read and Control keys get the redacted form.
  - Events, metrics, logs, diagnostics bundles and error text get the redacted form whatever the key. A request error is stored with its URL stripped.
- **Private torrents** use their trackers only: no DHT, no peer exchange, no local discovery.
- **The API costs what it returns.** `/graphql` checks the credential before reading a body. A socket holds at most 32 live subscriptions; a credential holds at most 8 sockets. State-changing facade calls are POST only. A poll or subscription never rebuilds every torrent per tick.
- **Listeners are accounted.** Every listening socket is taken through the route's ledger and released when its session ends.
- **No bound that breaks real downloads.** A new limit comes with a measured margin over real torrents, swarms and clients, and its refusal says what was refused. With no data, ask the operator.
