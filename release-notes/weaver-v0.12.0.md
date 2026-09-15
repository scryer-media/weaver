# Weaver 0.12.0 release notes

## Highlights

Weaver 0.12.0 is a large release.

- **A redesigned interface, now the default.** It walks a new install through
  setup, speaks nine languages, and shows every running phase of a job at once.
  The classic interface is still one click away.
- **A simpler, safer access model for new installs.** An administrator login
  from the first start, a one-time setup code when Weaver can be reached from
  other machines, and browser sessions you can revoke. Existing installs keep
  their current access settings until you choose to move them over.
- **Proxy routing (beta).** Provider connections and RSS feeds can go through
  HTTP CONNECT, HTTP/3, SOCKS5, SSH or WireGuard, in an order you choose.
- **PAR3 verification and repair**, alongside PAR2.
- **More archives unpack while they download:** compressed and mixed RAR,
  ZIP64, TAR and its compressed forms, single compressed files, and plain split
  files.
- **Job detail shows which providers served each job.**

## What changed

### The new interface

- **It is the default.** A browser that has never picked an interface opens the
  new one. A browser that had chosen the classic interface keeps it. Both have a
  switch at the foot of their navigation, and the choice is per browser.
- **First-run setup.** A new install with no provider is walked through its
  interface language, first Usenet provider, working and completed folders, and
  categories before the interface opens. It shows once per install.
- **Nine languages.** English, German, Spanish, French, Italian, Japanese,
  Korean, Portuguese and Chinese.
- **Provider certificates.** When a provider's certificate is issued for a
  different host name, setup names the hosts it covers and lets you trust it.
  Settings can confirm, retest and forget trusted certificates.
- **Queue and history.**
  - Each running phase (download, verification, repair, extraction) has its
    own bar.
  - Queue rows have pause and cancel buttons.
  - An empty queue accepts a dropped NZB.
  - An NZB the duplicate policy blocked can be force-added from the Add NZB
    dialog.
  - Jobs are named by their full release name.
  - The category rail lists your configured categories. Selecting several shows
    all of them, in history as well as the queue.
  - Finished jobs can be deleted with or without their files. Deletes run in
    the background.
- **Settings and system information.**
  - Path settings have a folder browser.
  - The speed limit can be set from the Throughput gauge.
  - Storage shows every filesystem at once, counting a disk shared by several
    categories only once.
  - Provider connection meters follow the live connection count.
  - System info names the compute kernel each library picked for your CPU (yEnc,
    CRC32, PAR2, RAR hashing and AES), and any environment variable that
    overrode it.
- **Upgrade and error pages.** While a database upgrade runs at startup, Weaver
  answers on its usual address with a progress page and reloads into the
  interface when it is ready. If the interface fails or Weaver stops answering,
  a single error page retries on its own.
- **New Weaver mark** across the interface, the tray, the installers and the
  icons used when you install the web app on a device.

### Access and security

- **New installs require a login.** Setup creates an administrator login. When
  Weaver listens beyond loopback, as the container image does, or has trusted
  proxies configured, setup also asks for a one-time code such as `K7P-M2X`.
  Weaver prints the code at startup under a "FIRST-TIME SETUP: ACTION REQUIRED"
  banner. It stays valid until setup succeeds or Weaver restarts. A Weaver
  listening only on `127.0.0.1` skips the code; set
  `WEAVER_REQUIRE_SETUP_CODE=1` to ask for one anyway.
- **Unattended setup.** Set `WEAVER_BOOTSTRAP_LOGIN_USERNAME` and exactly one of
  `WEAVER_BOOTSTRAP_LOGIN_PASSWORD` or `WEAVER_BOOTSTRAP_LOGIN_PASSWORD_FILE`.
  Bootstrap credentials never overwrite a login that already exists.
- **Sessions per browser.**
  - A remembered session lasts at most 30 days.
  - Sign-out ends the current browser's session, and Sign out all ends every
    session.
  - Some changes ask for your password again if it was not entered in the last
    15 minutes: password changes, sign-out-all, network access, API key
    creation, backups, and post-processing scripts.
  - The new interface has a sign-out button in its top bar.
- **Network access in one place.** Settings → Security → Network access edits
  trusted proxies, the networks where a remembered login is accepted, and the
  bind address together. It previews whether a change would lock out the
  browser making it. Weaver reads `X-Forwarded-For` only from a named proxy.
  Under the new model a trusted network never signs a browser in; it only
  limits where a remembered login is accepted.
- **Hardening.**
  - An open live-update connection closes as soon as its credential is revoked
    or expires, for example after a password change, an API key removal or a
    network access change.
  - Live-update connections from a page served on another origin are refused.
  - Responses forbid being framed by other sites.
  - New passwords need at least 8 characters.
  - Archive passwords saved with a job are encrypted at rest.

### Proxy routing (beta)

- **Proxy profiles.** Settings → Proxies manages HTTP CONNECT, HTTP/3 CONNECT,
  SOCKS5, SSH (Ed25519 keys, including passphrase-protected ones) and userspace
  WireGuard profiles.
  - A WireGuard profile can be filled by pasting or choosing a `.conf` file.
  - SSH trusts the host key it first sees and checks it on every channel after
    that.
  - Weaver creates no system tunnel interface, route or firewall rule.
- **Ordered routes.** Each provider and RSS feed takes up to eight profiles, tried
  in order, with direct access as a separate final fallback.
  - Existing providers and feeds keep direct access.
  - A route that fails at the transport level rests for 30 seconds before one
    connection tries it again.
  - TLS still verifies the provider's own name, and proxied connections do not
    expose the provider's address.
  - RSS polling, redirects and NZB downloads follow the feed's routes.
    Environment proxy variables cannot override them.

### Archives and repair

- **PAR3.** Weaver verifies, repairs and extracts jobs protected by PAR3.
  - It downloads only the recovery data a repair needs.
  - It restores scrambled file names from the protected content, and handles
    PAR3 protection embedded inside ZIP, ZIP64 and 7z archives.
  - It does not create PAR3 sets.
  - In a job that carries both, PAR2 takes priority. Embedded PAR3 is looked for
    only in jobs without PAR2.
  - Waiting for an archive volume the NZB does not list is still PAR2-only.
- **More unpacking during download.** Direct unpack now also covers:
  - RAR archives with compressed members, or a mix of compressed and stored
    members;
  - ZIP64 archives, including archives larger than 4 GiB;
  - TAR, compressed TAR, and single gzip, bzip2, XZ, Zstandard, Brotli and
    DEFLATE files;
  - plain split files, joined as their parts arrive.

  Output is installed only after verification and repair settle. A repair that
  changes bytes the unpack already read discards that output and extracts from
  the repaired archive.
- **Extraction that outlasts the download is visible.** When direct unpack is
  still finishing after the last article arrives, the job now shows an
  Extracting bar and a timeline lane instead of appearing idle.
- **PAR2 before extraction.** A numbered archive part that the NZB did not
  include and PAR2 rebuilt now joins its set before extraction for every split
  set, not only split 7z. Files rejected by their CRC are repaired before
  archive output is accepted.

### Downloads and reliability

- **Providers used.** Job detail lists each provider that served the job, with
  its articles, bytes and share. The counts are kept in history, and a deleted
  provider keeps its counts. Articles whose provider cannot be identified are
  left out rather than credited to another provider.
- **Provider connection limits count every socket.** A connection now holds its
  slot from dialing until it is fully closed, including idle and replacement
  connections, so every open socket counts toward a provider's limit. After a
  transport failure, recovery is proven by one real article and retried after 30
  to 60 seconds. New metrics report socket occupancy, local denials, provider
  refusals and recovery state.
- **Shared memory budget.** NZB metadata, extraction, direct unpack, PAR2 packet
  data and repeated-article work reserve from one process memory budget, and a
  resource failure stays with the job that hit it.
- **Retries are not lost behind a refetch.** A refetch after a direct-unpack
  fallback or a CRC recovery could cancel another article's scheduled retry.
  The job then failed with nothing recorded as failed. Pending retries now
  survive those refetches.
- **Providers that require a newsgroup first.** While Weaver learns that a
  provider needs a newsgroup selected before it serves an article, that article
  no longer loses a retry and the provider is no longer marked unhealthy.
- **Jobs are not lost across a crash at completion.** A job that stopped between
  recording its final status and writing its history entry was missing from both
  queue and history after a restart. It is now archived to history, with its
  byte counts recorded as zero.

### Platform fixes

- **macOS:** large network shares no longer read as 0% full. Their capacity
  was read with 32-bit block counts.
- **Windows:** restoring a backup no longer fails with "access denied". A
  restore retried after a failure no longer hits file-sharing errors.

## Upgrade notes

- **Database migrations.** Four migrations run on first start: proxy profiles
  and routes, repair outputs, per-job provider counts, and browser sessions.
  A progress page is shown while they run. Migrations are forward-only, so take
  a backup first if you may need to return to 0.11.
- **Existing installs keep their access behaviour.** To move to the new model,
  set `WEAVER_ACCESS_MODE=authenticated` and restart.
  - If no login is stored, setup opens, asking for a setup code when Weaver
    listens beyond loopback.
  - Stored logins and API keys are kept, but browsers sign in again.
  - The new interface shows a one-time notice with the steps for your kind of
    deployment.
- **New installs that set `WEAVER_TRUSTED_CIDRS`** without
  `WEAVER_ACCESS_MODE=authenticated` start on the older loginless model.
- **Embedding Weaver in another page no longer works.** This includes dashboard
  iframes.
- **Reverse proxies:**
  - Preserve `Origin` and the original `Host`, or send `X-Forwarded-Host` from a
    trusted proxy, or list the public host name in `WEAVER_HTTP_ALLOWED_HOSTS`.
    Otherwise live updates are refused.
  - List only the proxy's own address as a trusted proxy.
- **Passwords shorter than 8 characters** keep working until you change them.
  Bootstrap credentials must now meet the minimum.
- **The default interface changes.** Browsers that never chose one open the new
  interface. Use the switch at the foot of the navigation to go back.
- **GraphQL.** The schema gains proxy, network access, first-run setup, provider
  attribution and compute kernel fields. Nothing is removed, so existing clients
  are unaffected.
- **Proxy routing is beta.**
