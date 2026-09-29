# Outbound networking

Settings → Networking manages outbound paths independently of the HTTP listen
address in Security. Overview shows live egresses, route legs and consumers;
Egress, Proxies, Routes and Bandwidth contain their controls. The old Proxies
and Bandwidth settings links redirect to these pages.

## Egress interfaces

System follows the operating system's routing table. It is always present,
cannot be renamed, disabled or deleted, and can have a download limit. Add an
Interface binding to select a link, or a Source address binding to bind an
address assigned to the host. A missing or down interface, or an address that
is unavailable, deprecated or tentative, makes that egress unavailable. IPv6
link-local and `fd00::/8` addresses are excluded. Discovery failures are shown
as unknown and do not silently switch a configured binding to System.

Interface binding is available on Linux and macOS. Windows advertises System
and Source address; Interface saves are refused. Source binding alone does
not install a route: Linux may need source policy routing, macOS may need a
scoped route, and a Windows adapter needs a suitable gateway. Endpoint hostname
resolution follows the default route; use IP literals for proxy endpoints
when resolving through another link would be inappropriate.

The Test action makes an isolated connection and reports its source address
and connection time. It does not change production pins or failure evidence.
An egress referenced by a route cannot be deleted. Restoring a route that
references an absent egress uses System and exposes a warning on that leg.

## Routes and failover

Each NNTP server or RSS feed has one to eight legs. Each leg selects an egress
and either connects directly or tries an ordered ladder of proxies, pools or
chains. Direct fallback is explicit. A proxy-only ladder never gains direct
access when its proxies fail. Existing one-path configurations retain their
behavior; old proxy-only backups become one proxy-only leg on System.

Weights are integer percentages totaling 100. Drag a divider or edit a weight;
the adjacent leg absorbs the change. NNTP distributes its configured connection
cap using largest remainders, with ties resolved by leg position. Redistribute
reassigns unavailable legs' shares to healthy legs. Hold parks those shares,
reducing the usable budget. Neither mode exceeds the server cap. The simulation
controls preview these allocations without changing the daemon.

RSS uses the first healthy leg, then its ladder in order. Weights do not choose
feeds at random. DNS, redirects, responses and originating NZB downloads use
the selected path. Each pool attempt leases one member across DNS and HTTP;
a failed path advances to the next member, and successful HTTP setup records
the preferred member for later requests.
Private destinations are validated and cross-origin redirects drop feed
credentials. Environment proxy variables do not override a saved route.

Weight changes preserve connections and move only excess idle connections when
another leg needs capacity. Changing a ladder revokes that leg's streams.
Failures attributed to an egress or proxy trigger leg/rung cooldown and recovery
probes. Destination failures and unattributed stream resets do not cool a leg. Provider
authentication, certificates and missing articles retain their normal NNTP
meaning. Fatal trust or policy failures stop the leg instead of falling through
to another proxy or direct access. Idle servers can probe a recovering leg
without waiting for a new download.

## Proxy pools and chains

Profiles support SOCKS5, HTTP CONNECT, SSH, WireGuard and HTTP/3 CONNECT.
See [proxy engine details](proxy-routing.md) for authentication and tuning.
A pool contains distinct profiles of one kind. Its selection evidence is shared
per pool and egress: sessions warm before open-channel races, the fastest open
is pinned, and sustained delivery can replace that pin after two confirming
verdicts. Two failed connects or setups make a pin suspect. Failed races enter
a holdoff; disabled members are never dialed. Unpinned, unused sessions retire
after ten minutes and can warm again when needed.

A chain contains two or three distinct profiles. TCP hops may follow any
transport; a UDP tunnel must be first because TCP-only hops cannot carry its
UDP transport. Duplicate use within a leg and incompatible chains are refused
at save time. WireGuard instance limits account for unique paths and every pool
enabled member before accepting a route. The memory-derived admission limit
is fixed for the process lifetime so transient memory pressure does not change
save validation. Referenced profiles and pools cannot be deleted.
Pool tests use isolated sessions and require a destination for SOCKS5/CONNECT.

## Live flow and bandwidth

The flow updates once per second. Ribbons show open connections, leg cards show
open/target counts and selected rungs, and consumer cells show open or parked
capacity. Expand pools to inspect handshake, open-channel and delivery evidence.
State words and glyphs accompany colors. Large configurations collapse by
egress. Server and feed forms link to their route editor and include a compact
flow. Download SVG freezes the current view with its timestamp. Names, addresses
and hostnames shown in the view are included; credentials are excluded.

The global limit controls dispatch. Egress limits pace reads across all consumers
sharing that egress, including RSS; server limits apply after egress pacing.
Zero means unlimited. These shared pacing schedules survive pool rebuilds.

## Host and container setup

See [network setup](network-setup.md) for container host networking, a pinned
HTTP listen address and capability examples. A bridge container sees its own
interfaces; selecting a host interface requires host networking or explicit
container network attachments. The UI shows a hint when only a private bridge
interface is visible.

Linux kernels before 5.7 may require `CAP_NET_RAW` for `SO_BINDTODEVICE`.
For a systemd service, an operator can add `AmbientCapabilities=CAP_NET_RAW` to
the service configuration. Containers retain granted `NET_RAW` only with the
explicit `WEAVER_RETAIN_NET_RAW=true` environment setting. Weaver does not
require `NET_ADMIN`, create OS tunnel interfaces or alter host routes.
macOS interface binding requires no elevated privilege. macOS 15 Local Network
privacy may prompt for LAN destinations; that prompt behavior is unverified.

## API compatibility

Use the additive `route` fields for legs, pools and chains. Deprecated `routing`
fields remain readable as a compatibility projection. Legacy writes that would
discard an advanced route are refused; omitting routing fields preserves it.
Supplying both `route` and `routing` is an error. Removal of the deprecated API
and publication of the companion transport dependency belong to a later,
separately authorized release.

Live NNTP, RSS, and server draft probes use the composable networking runtime.
The compatibility runtime retains deprecated status projections and isolated
legacy profile probes for older clients. Its first-leg projection is never used
to execute or overwrite an advanced route.
