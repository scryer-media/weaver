# Weaver 0.15.1 release notes

Everything below is new since 0.15.0.

## Highlights

- **Default outbound has its own route.** NZBs fetched by URL, release checks,
  upgrade downloads, trust-root refreshes and release-name lookups now follow
  the route under Networking. Providers and RSS feeds keep their own routes.
- **Test the speed of a route.** The new Speed page measures bandwidth or a
  Usenet download through a selected route, shows live charts, and compares
  runs. Individual legs, ladder rungs, proxies and egresses can be taken down
  for a test without changing the saved route.
- **Choose who resolves a proxy's destinations.** Each proxy names Weaver or
  the proxy as its resolver, with host DNS or specified DNS servers. Endpoint
  tests report how each host was resolved and which pool member carried it.
- **Encrypted archives use the same password candidates everywhere.**
  Passwords supplied by integrations and `X-DNZB-Password` response headers
  join those from the job, NZB, filename, settings and password file. Encrypted
  stored 7z members can stay on the direct-store route.
- **Better NZBGet compatibility and Windows upgrades.** Queue editing,
  priorities, history statuses and script callbacks cover more client
  behavior. Windows upgrades stop leftover processes, retain tray startup
  preferences and recover from abandoned reboot waits.

## What changed

### Networking and name resolution

- **Default outbound is visible and editable.** It has a row on Routes, a
  card in the flow and an entry on the Speed page. With no saved route it
  uses direct access through the System egress. Saving a blocked route does
  not grant an unconfigured direct fallback.
- Weaver's HTTP fetches share one routing path. Redirects are resolved and
  checked through the same route attempt, and the connection remains attached
  to that route while the response body is read. RSS uses the feed's route;
  an NZB submitted by URL uses Default outbound. An RSS `304 Not Modified`
  response is treated as an unchanged feed.
- Every provider connection uses its provider's route. The NNTP transport no
  longer has a separate direct connection path to fall back to when routing
  cannot provide one. Source checks guard against adding new network clients
  or direct dials outside the routing layer.
- **Name resolution is explicit per proxy.** The editor lets a proxy resolve
  a destination itself, or have Weaver resolve it first. Host DNS and custom
  DNS servers are selectable. In a chain, each hop's resolver determines how
  the next host is found. RSS routes can use proxies without a custom DNS
  server list.
- DNS answers are shared and retained for their TTL across provider address
  selection, direct dials and endpoint tests. Proxy lookups have their own
  timeout window. Upgrading saved profiles preserves their previous resolver
  behavior.
- **Test via uses a drafted route.** Proxy, pool and egress tests can choose
  the path used to reach the target. Results name how each host was resolved;
  a pool test exercises the pool and names the member it selected. Failures
  carry stable codes and values, rendered as translated explanations in the
  interface.
- The flow follows live traffic, draws pools through the members carrying
  them, and highlights the path under the pointer. Single-leg routes are
  drawn without an extra split, and chain connections run between their
  hops. A pool can be saved while empty, but needs a member before a route
  can use it.

### Route speed tests

- **Bandwidth or Usenet.** Bandwidth tests use Cloudflare, SABnzbd, OVH,
  Hetzner, LibreSpeed or a custom URL. Usenet tests can use the test posting
  or an uploaded NZB. LibreSpeed can select a server by probing through the
  chosen route; its download and ping endpoints must stay on that server's
  HTTPS origin.
- Bandwidth tests increase parallel connections while throughput improves,
  then finish when the measured rate settles or the run reaches its limit.
  The reported average excludes warm-up. Usenet tests use the normal article
  fetch and decoder, with decoded bytes reflected in the live download
  counter.
- Charts show the run's live rate and path. Recent runs can be selected for
  comparison, and the run list can be restricted to the selected route.
  Runs and uploaded test NZBs are held in memory rather than added to the
  download queue or saved across restarts.
- Tests pause normal download dispatch and drain its connections before
  measuring. A Usenet test starts only if the pool can lend every planned
  connection. Finishing, cancelling or failing a test releases its own pause
  without clearing an operator or schedule pause.
- **Tests bypass speed limits but respect download quotas.** Their traffic
  is charged to the applicable provider and egress quotas. Bandwidth runs
  reserve egress quota in small slices as bytes arrive; exhausting a quota
  stops the affected path.
- Path overrides belong to the test alone. Taking down a leg, rung, proxy
  or egress in a draft does not disable it in the saved configuration.

### Archives, passwords and filenames

- **One password list reaches every archive consumer.** Direct store, direct
  unpack and conventional extraction receive the same ordered candidates.
  Supplied integration passwords and repeated `X-DNZB-Password` headers on
  the final NZB response are honored. Redirect response headers do not supply
  passwords. Candidates supplied with an active job are encrypted in the
  database and survive a restart.
- Password sources have separate bounds, so a long global list cannot crowd
  out a job's supplied or NZB passwords. Settings and the password file
  contribute at most 50 distinct candidates together. Saving other General
  settings no longer clears the selected password file.
- Encrypted stored 7z members can route directly to their destinations.
  Header-encrypted 7z sets also pass the job's password into the positional
  direct-unpack reader.
- Eligible LZMA2 7z extraction uses positional reads and guarded outputs,
  allowing workers to read independent source ranges without sharing a seek
  cursor. Repair invalidates old read state before work resumes, and output
  is accepted only after its verification settles. Other codec graphs keep
  their existing extraction path.
- 7z member publication uses the decoder's CRC verdict instead of hashing
  the output a second time. Tracking verified members also avoids repeatedly
  scanning the entire member list for archives with many files.
- Extraction distinguishes damaged data, missing passwords, unsafe archive
  structure, resource refusals and temporary capacity pressure by their error
  causes. Repair and retry decisions no longer depend on general error
  wording. Password failures retain the marker integrations recognize.
- **History preserves full filenames.** When a filename must be shortened,
  sanitized or disambiguated for the filesystem, `HistoryItem.fullFileNames`
  records its on-disk name and full intended name. The mapping follows later
  PAR2 and delivery renames, including files removed after extraction.
- Windows positional output checks compare aliases by volume and file
  identity. A failed working-directory ownership marker write removes only
  the empty directory just created, rather than recursively removing a path
  whose ownership was not established.

### NZBGet compatibility and script callbacks

- RPC routing accepts credentials in the path, trailing slashes and GET
  calls with the method in the path. Basic authentication is matched without
  case sensitivity and authentication failures send `WWW-Authenticate`.
- All NZBGet priority levels map to Weaver's scheduling, while responses
  retain the priority and category the client submitted. History also keeps
  duplicate keys, scores and modes.
- `editqueue` gains rename, sorting, move-before and move-after operations,
  history marks, parameter setters and retry handling.
- `HistoryDelete` hides a record from the facade's normal history without
  erasing Weaver's history. `history(true)` includes hidden records, and
  visibility is applied before pagination.
- History reports repair, unpack, move and deletion outcomes from the stored
  failure cause. Download time, total post-processing time, critical health
  and wide XML-RPC integers are reported more accurately.
- Scripts receive the NZBGet-compatible control address and can authenticate
  to the facade with their run token. Callbacks work when Weaver is served
  beneath a base URL. Tokens permit a defined set of compatibility methods;
  they do not gain access to RSS feeds or administrator operations.

### Interface, logs and recovery

- **General settings save as they change or when a field is left.** Archive
  passwords are edited in a dialog, clearing them asks for confirmation, and
  unwanted extensions now live under General downloads. Bandwidth settings
  have moved into Networking's Egress page.
- Live updates preserve unchanged data so a metrics sample or provider
  update redraws the figures that changed rather than the whole screen.
  Production web builds also use the React compiler.
- Settings tables use consistent Edit and Test actions, provider rows expose
  their actions in the table, and Scripts lists secrets before script jobs.
- **File logs rotate while Weaver runs.** The default size threshold is
  10 MiB, with five archived files per log stream. Old segments are compressed
  in the background, diagnostics include rotated logs, and shutdown drains
  buffered events. `WEAVER_LOG_MAX_SIZE` sets the byte threshold and
  `WEAVER_LOG_MAX_FILES` the archive count; both take positive integers.
  Prometheus reports dropped log events and sink write failures.
- Windows upgrades stop leftover Weaver processes in the installed folder
  before MSI checks files in use, preserve an existing tray startup setting,
  and capture the installer log. The tray contains its server process and
  stops it on shutdown. An abandoned reboot-required upgrade eventually
  releases its hold instead of blocking every later upgrade.
- A fresh installation can restore a backup even after background outbound
  traffic has recorded egress usage. Active jobs and NNTP usage retain their
  restore guards.

### Libraries

- Archive and repair libraries move to `sevenz-turbo` 0.29.1, `lzma-turbo`
  0.10.1, `unrar-rs` 0.12.0, `par2-rs` 0.10.11, `par3-rs` 0.7.1 and
  `reedsolomon-rs` 0.4.11. ZIP support moves to `zip` 9. The application
  updater and artifact verification libraries are updated alongside the
  routing and Windows upgrade changes.

## Upgrade notes

- Database migrations run automatically. They add facade history visibility,
  failure and mark state, encrypted active-job password candidates, and
  explicit proxy resolver settings. Saved proxy profiles retain how they
  resolved names before the upgrade.
- Default outbound uses direct System egress until configured. Review it if
  NZB URL fetches, release checks, upgrades or release-name lookups must use a
  proxy or tunnel. Provider and feed routes remain separate.
- **Default outbound covers Weaver's own requests, not script processes.**
  Network calls made by a script still use the host or container's normal
  networking unless the script or that environment configures another path.
- New proxy ladders initially allow direct fallback. Turn that off when the
  route must not fall back to a direct connection. A route's configured
  fallback is distinct from the removed unconfigured NNTP fallback.
- General no longer has a single Save action for the panel. Each field saves
  independently, and clearing stored archive passwords requires confirmation.
- Speed tests move real data and consume download quotas even though they
  bypass speed limits. Their results and uploaded NZBs are not retained
  across a restart.
- GraphQL gains full-filename history records and structured proxy-failure
  codes and parameters. The new routing, resolver and speed-test surfaces
  are exposed through the API as well as the interface.
