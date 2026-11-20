# Weaver 0.17.0 release notes

Everything below is new since 0.16.17.

## Highlights

- **Torrents are opt-in.** Torrent support is off unless the server is started with `WEAVER_TORRENTS=1` (or `true`). Without it there is no Torrents screen, no torrent setting, no qBittorrent-compatible API, and a torrent or magnet add is refused with a message naming the variable. With it set, torrents still stay off until you enable them in Settings.
- **A BitTorrent client in Weaver.** Weaver has its own torrent engine:
  v1, v2 and hybrid torrents, magnets, its own DHT, peer exchange, local
  discovery, web seeds, uTP and encrypted connections. Torrents share the
  queue, categories, tags, routes, bandwidth, disk and unpacking with
  Usenet jobs.
- **Safe on private trackers.** Announces, events and counters follow
  libtorrent 2.0. A private torrent uses its trackers only. Passkeys never
  reach a log, and they are encrypted at rest.
- **A qBittorrent-compatible API.** Sonarr, Radarr and other clients that
  speak qBittorrent's WebAPI can add and manage torrents in Weaver.
- **DNS you choose.** Saved DNS server lists and DNS profiles, set per
  route and per proxy. Plain DNS, DNS over TCP, TLS or HTTPS, and DNSSEC
  checked by Weaver itself.
- **Bandwidth, quotas and usage across both kinds of job.** One download
  and upload limit shared between Usenet and torrents, upload caps and
  quotas per egress, and a per-day usage history with new dashboards.
- **More releases complete.** Many fixes to nested archives, RAR sets,
  PAR2 and RAR recovery, splits and naming, found by running real NZBs.

## Torrents: engine

Weaver ships a new engine crate, `weaver-torrent`, and a new uTP crate,
`weaver-utp`.

- **Torrent formats.** v1, v2 and hybrid metainfo, magnet links (with
  `so=`, `x.pe` and `ws=`), SSL torrents, and v2 piece layers fetched from
  peers. A `.torrent`, a DHT message and an extended message are bounded
  before they are parsed.
- **Peer wire.** The fast extension, extended handshake, PEX, holepunch,
  suggested and allowed-fast pieces, `DONT_HAVE`, and end game. One piece
  picker, rarest first. Requests are kept as deep as a peer delivers in
  three seconds.
- **Peer discovery.** Trackers over HTTP, HTTPS and UDP, tier by tier with
  libtorrent's back-off and intervals. Weaver's own DHT (BEP 5), with
  libtorrent's refresh pace, reply timeout, node failure and search
  limits, and a saved routing table. An IPv6 DHT runs when the route's
  IPv6 traffic leaves from a reachable address. Local service discovery
  rejoins its groups when the network changes.
- **Web seeds** (BEP 19 and BEP 17) are HTTP peers. Their credentials stay
  out of errors.
- **Transports.** TCP and uTP are switched on their own. You pick the
  transport dialled first while downloading and while seeding. The
  defaults are both on, TCP first while downloading and uTP first while
  seeding.
- **uTP.** LEDBAT congestion control, paced sending, SACK resends, path
  packet-size probes, and batched datagram calls on Linux. Interop was
  tested against qBittorrent 5.3.0.
- **Encryption.** MSE with RC4, with fast word-at-a-time paths on x86-64
  and aarch64. The handshake order follows libtorrent, including PEX's
  encryption flag. Encryption may be set to prefer, require or disabled.
- **Port mapping.** UPnP, PCP and NAT-PMP, on the interface the route's
  traffic takes. Mappings are removed when the session stops.
- **Peer bans.** Smart ban blames the sender of a bad block. Self
  connections are banned. Bans can come from an IP blocklist, banned
  addresses and privileged ports.
- **Checks and resume.** A hashing thread per session. Checkpoints keep
  partial pieces. After a restart only files whose modified time changed
  are checked again.
- **Torrent analyzer.** `weaver torrent analyze`, and the web analyzer,
  give a redacted report of a `.torrent` or magnet.

## Torrents: behaviour and identity

- **Weaver announces as Weaver, or as qBittorrent 5.3.0** where a tracker
  rule asks for it. A torrent keeps one peer id and one tracker key from
  add to remove.
- **Events are exact.** `started` goes until a tracker answers it.
  `completed` goes once, only for a run that began short of a seed. It is
  kept across a pause or a route move. `stopped` goes only to trackers
  that answered `started`.
- **Counters are exact.** `uploaded` and `downloaded` count from each
  start. Corrupt and redundant bytes are reported. A route move is not a
  start, so the counters run on across it.
- **Private torrents.** No DHT, PEX, local discovery or holepunch. A
  private magnet asks its trackers first. Peers found off-tracker are
  dropped once a tracker answers.
- **No scrape and no super-seeding.** Weaver never scrapes trackers, and a
  request to super-seed is refused.

## Torrents: rules, settings and the queue

- **Tracker rules.** Match torrents by tracker. A rule sets the client
  name, discovery switches, encryption, transports and dial order, upload
  cap, seeding goals, unpack, forced start, route, SSL identity, and
  whether the schedule may pause its torrents.
- **Tracker files** apply rules in bulk, with a preview first. Settings
  take one override chain: torrent, then tracker rule, then category, then
  global.
- **Seeding.** Ratio, seed time and inactivity goals, and a seeding
  window on the schedule. When a goal is met the torrent pauses, stops or
  is removed. A removal with an unmet goal asks first.
- **Queue.** Queue positions, forced start, sequential download,
  first-and-last-piece first, file priorities, per-torrent rate limits,
  set location and recheck.
- **Seed mode.** A complete torrent added for seeding is verified as it
  serves.
- **Files you skip are never written.** Pieces that overlap a skipped file
  are kept in a side store.
- **Files missing.** A torrent whose files are gone is held stopped as
  files missing.
- **Unpacking.** A torrent with archives is unpacked through the same
  engine and status rules as an NZB job. It is on by default, and a
  category, tracker rule or torrent may override it. The unpack has its
  own line and never touches the torrent's state, history or counters.
- **Torrents off.** With torrents turned off, nothing offers a new
  torrent, and RSS torrent items wait and are retried later. Turning
  torrents off asks how running torrents should end.

## Torrents: integration

- **qBittorrent facade.** Answers as qBittorrent 5.3.0. It covers
  `torrents`, `transfer`, `sync/maindata` (deltas since `rid`),
  `torrentPeers`, `app`, `rss` and categories. An add that names an
  unknown category creates it. Reads come from published rows, and
  actions on many torrents are one write.
- **Scripts.** Torrents raise `TORRENT_` events. `JOB_ADDED`,
  `JOB_DOWNLOADED` and `JOB_DELETED` are raised for every kind of job.
  `FILE_DOWNLOADED` fires for torrent files. `ROUTE_DOWN` and `ROUTE_UP`
  report a route going wholly down and coming back. Scripts get
  `NZBNA_TAGS` and the new `WEAVER_` variables for torrents, routes,
  quotas and seeding.
- **Tags.** One set of labels per job, for Usenet jobs and torrents
  alike. History filters by tag in SQL.
- **Disk.** Torrents book room on the same disk ledger as Usenet jobs. A
  torrent short of room keeps seeding and waits. Piece writes go through
  each volume's one writer, and one read pool per volume is shared by
  torrents and Usenet.
- **Memory.** A piece in flight takes memory as its blocks arrive. A piece
  larger than the budget is written block by block.
- **Moves and deletes** take only what Weaver created. A move never
  replaces a file. A torrent's files are reached only inside its save
  folder.
- **Speed test.** Torrent routes get a Torrent test, and the speed test's
  upload rides the bandwidth provider.
- **Metrics.** Torrents, trackers, route sessions, piece checks and disk
  calls are exported to Prometheus.

## Networking

- **WireGuard ingress.** A torrent session on a WireGuard leg accepts
  incoming peers inside the tunnel.
- **Tunnel socket budget.** A WireGuard tunnel's sockets are budgeted in
  bytes, shared between Usenet, torrent peers and a reserve by the
  bandwidth split rule. Usenet waits for room on a full tunnel instead of
  leaving by a lower rung. A Usenet socket takes 1 MiB under Balanced and
  2 MiB under Performance.
- **SOCKS5 UDP.** A SOCKS5 hop carries datagrams through UDP ASSOCIATE.
  A torrent session routed through it gets DHT, uTP and UDP trackers
  through the proxy and no other way.
- **Pools for torrent routes.** Each torrent sticks to one pool member.
- **Route health per hop.** A proxy's health follows each of its hops. A
  slow or failing destination no longer marks a live proxy down.
- **WireGuard.** Tunnels batch their UDP and run on their own thread. One
  live session per key pair. Routes fail over and reconnect at once.
- **Legacy routes.** The old per-hop providers and consumer ladder are
  deleted.
- **No leaks.** Only the routing layer resolves names. The host resolver
  has one caller, and a lint refuses any other.

## DNS

- **DNS profiles.** Saved server lists (up to 10 servers each) and
  profiles, chosen per route, per proxy and per egress.
- **Transports.** Servers are asked over plain UDP and TCP, TLS or HTTPS.
  A server that fails its TLS check is skipped for the next one. It is
  never asked in plain text. Encrypted connections are kept between
  lookups.
- **DNSSEC.** A new crate, `weaver-dnssec`, checks answers from the root
  keys down and names the reason for each refusal. Private names read as
  unsigned. Ed448 signatures are not checked. It uses aws-lc-rs, with an
  optional pure-Rust backend.
- **Server test.** A DNS page tests each server's plain, TCP, TLS, HTTPS
  and DNSSEC support through the chosen route, running the lookup's own
  code.
- **Editor.** The DNSSEC field is an Off/On switch, and the DNS over TLS
  name field is now called Certificate hostname.

## Bandwidth, quotas and usage

- **One bandwidth setting** holds the global download and upload limits
  and the split between Usenet and torrents. It is also a schedule rule.
- **Egresses** have download and upload limits, and quotas with a
  download cap and soft and hard upload caps.
- **Pause by class.** Pause or resume all Usenet jobs or all torrents.
- **Usage history.** Usage is kept per day, by consumer. The Overview,
  Providers, Integrity and Torrents dashboards read it.
- **Removed servers.** A removed server whose quota resets on a calendar
  keeps its data used, shown as removed, until that reset.
- **An idle instance writes nothing.**

## Usenet, archives and repair

- **Partial means something usable.** A job is Partial only when a whole,
  usable file that is not furniture was delivered. Clients that select
  the new `outcome` field see `PARTIAL`. A Partial job's reason names
  what it lacks.
- **Unwanted extensions.** The action is now a setting: fail the job, or
  delete only the unwanted files (the default). A job ends by the action
  it started with. A torrent of only unwanted files is added with every
  file left out.
- **Naming.** A posted file takes its yEnc or uuencode header name once,
  when it is a clear name. A leftover `&quot;` is dropped from names.
- **Splits.** Two-digit split sets join, with guards for version-like
  names.
- **Nested archives.** Below the root, only release layers are opened.
  Their contents land beside the archive. A set scattered across nested
  archives is joined in the layer above.
- **RAR.**
  - Nameless and renamed sets posted in any order complete.
  - Sets with misstated sizes are proved by CRC.
  - A RAR5 volume that lost its front is rebuilt by its own recovery
    record.
  - Obfuscated RAR5 `.rev` files repair their set.
  - A set short of a volume names the missing one.
- **PAR2 and PAR3.** Recovery is counted by what the graph bound. A
  volume that lost its front is claimed by a verified packet past the
  hole. A file one set cannot rebuild goes to the next set.
- **Damage outside every set.** When a recovery set verifies, damage to a
  file no set describes does not shorten the job.
- **yEnc.** A part one byte short that fails its CRC is decoded again with
  the dropped dot kept. Layout conflicts name both articles. The encoder
  has a kernel per CPU tier.
- **Servers.** Unknown NNTP replies fail over to the next server. Replies
  that are the server's own trouble cost no article. An article whose
  tries are spent on one server moves to the next.
- **Health.** The health probe and the first-article gate count the
  recovery the graph bound, and ask only articles not known gone.
- **Passwords.** A job given a password that opens nothing says so.

## History, facades and diagnostics

- **NZBGet facade.** `HistoryDelete` archives the job and keeps it in
  Weaver's own history. A stopped rate reads as paused.
- **Completion.** A job shows finished only once its history is written.
- **Diagnostics.** The bundle never comes out empty. A failed step costs
  only its own part. WireGuard keys are redacted wherever they appear.
- **Write probe switch.** `WEAVER_VOLUME_WRITE_PROBE=off` skips the
  per-volume write probe.

## Security

- **GraphQL limits.** `/graphql` checks the caller before it reads a
  bounded body. Each socket is limited to 32 subscriptions, and each
  credential to 8 sockets.
- **Key scopes.**
  - Tracker passkeys, feed URLs, article links and exported web seed
    secrets are for Admin keys only.
  - A Control key saves and moves only under the save folders.
- **Encrypted at rest.** Tracker passkeys and RSS links are encrypted with
  the instance key.
- **Requests.** The facade takes a change only as a POST, and refuses
  internationalized host names.

## API changes

- **New queries.**
  - Torrents: `torrents`, `torrent`, `torrentPeers`, `torrentTrackers`,
    `torrentHistory`, `torrentRoutes`, `torrentPresence`.
  - Trackers: `trackerRules`, `trackerHosts`.
  - DNS: `dnsProfiles`, `dnsServerLists`.
  - Other: `jobTags`, `networking`, `scheduleSeeding`, `usageHistory`,
    `integrityHistory`.
- **New mutations.**
  - Submitting: `submit`, `submitTorrent`, `analyzeTorrent`.
  - Torrent actions: `pauseTorrent`, `resumeTorrent`, `removeTorrent`,
    `recheckTorrent`, `reannounceTorrent`, `dhtAnnounceTorrent`,
    `setTorrentLocation`, `setTorrentCategory`, `setTorrentForced`,
    `setTorrentSequential`, `setTorrentFirstLastPieceFirst`,
    `setTorrentFilePriorities`, `setTorrentRateLimits`,
    `setTorrentSeeding`.
  - Torrent trackers and peers: `addTorrentTrackers`,
    `removeTorrentTrackers`, `replaceTorrentTrackers`, `banTorrentPeer`.
  - Tracker rules: `saveTrackerRule`, `deleteTrackerRule`,
    `unbindTrackerRule`, `previewTrackerFile`, `applyTrackerFile`.
  - Tags: `createTags`, `deleteTags`, `setJobTags`, `addJobTags`,
    `removeJobTags`.
  - DNS: `saveDnsServerList`, `deleteDnsServerList`, `saveDnsProfile`,
    `deleteDnsProfile`, `testDnsServers`.
  - Bandwidth and pausing: `setBandwidth`, `setEgressQuota`,
    `pauseClass`, `resumeClass`.
  - Post-processing: `setUnwantedExtensionAction`.
- **New subscription:** `torrentUpdates`, which follows only the
  torrents named.
- **New fields.**
  - `outcome: JobOutcome` (`COMPLETE`, `PARTIAL`, `FAILED`) on jobs,
    queue items and history items. `state` and `status` still read a
    partial job as failed.
  - A `tag` filter on `jobs` and `jobCount`.
  - A `torrent` argument on `testVia`.
- **Deprecated:** `setSpeedLimit` and `setQueueSpeedLimit`. Use
  `setBandwidth`.
- **Removed or changed.**
  - Speed limits: `maxDownloadSpeed` is gone from `GeneralSettings` and
    its input. The global limit is in `setBandwidth`.
  - Egresses: `maxDownloadSpeed`, `downloadQuota` and
    `downloadQuotaUsage` are replaced by `downloadBps`, `uploadBps`,
    `quota` and `quotaUsage`. `DownloadQuotaUsage` is removed.
  - Proxies: `ProxyProfile.resolver` and the `ProxyResolver` enum are
    replaced by `dnsProfileId`.
  - Schedules: a schedule speed limit takes `kind`, `id`, `download` and
    `upload` in place of `bytesPerSec`.
  - Nullable limits: `speedLimitBytesPerSec`, `scheduledSpeedLimit` and
    `speedLimitInForce` are now nullable. Null is no limit.

## Upgrade notes

- **Migrations.** Database migrations 93 to 133 run automatically on
  SQLite and Postgres. A 0.16.17 database upgrades in place.
- **Torrents stay off** until the server is started with
  `WEAVER_TORRENTS=1` and torrents are turned on in Settings.
- **Proxy DNS becomes DNS profiles.** Each distinct DNS setting a proxy
  had becomes a saved profile, and each route takes the profile of its
  first proxy. A route whose proxy resolved names keeps having the proxy
  resolve them. Routes without a proxy use the built-in System profile.
- **Unwanted files are deleted by default.** Jobs holding a file on the
  unwanted extension list now end Partial instead of Failed. To keep the
  old behaviour, set the action to fail the job.
- **Speed limit clients.** Clients that set `maxDownloadSpeed` must move
  to `setBandwidth`.

## Dependencies

- **New crates:** `weaver-torrent`, `weaver-utp`, `weaver-dnssec`
  (MIT OR Apache-2.0), and the `host_name_lookup` lint.
- **Updated:**
  - proxy-tunnels 0.5.3 to 0.7.0.
  - par2-rs 0.12.0 to 0.13.0.
  - unrar-rs 0.16.3 to 0.16.6. 0.16.5 unpacks the RAR4 solid PPMd set
    that 0.16.3 refused.
- **New for DNS:** `rustls-native-certs`, so DNS over TLS trusts the
  host's roots, plus `rsa` and `ml-dsa` for DNSSEC.

## Regression coverage

- **Torrents.**
  - Tracker-visible behaviour has tests that read the announces a
    tracker receives.
  - A golden replay covers the piece tracker.
  - e2e has a torrent lane with a loopback tracker.
  - The qBittorrent facade and the torrent browser flows have scenarios.
- **Archives.** Tests cover zip, 7z and RAR damage beside each recovery
  kind, nameless RAR sets in every posting order, mixed-width numbering,
  and scene zips holding RAR sets.
- **Migrations.** A database at 0.16.16's schema upgrades and keeps its
  history, queue, NZB and server rows. Migrations 120 to 122 have Postgres
  tests.
- **DNS.** The DNS page specs run in their own release-gate partition.
