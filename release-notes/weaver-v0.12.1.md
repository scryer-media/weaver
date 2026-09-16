# Weaver 0.12.1 release notes

## Highlights

- **PAR3 repair finishes large sets.** Sets that need more recovery data than
  one fetch round no longer stop with "no admissible recovery remains" after the
  first few hundred blocks.
- **Scryer's "create API key" button works in the new interface.** The key is
  created as soon as the page opens and shown selected, ready to copy.
- **The release check can be turned off** with `WEAVER_UPDATE_CHECK=0`.
- **First-run setup without a code is tied to a local address**, closing a
  path a web page could use to claim a fresh install on the same machine.

## What changed

### PAR3

- **Large and interleaved sets recover.** Three faults could end a repair as
  unrecoverable while recovery blocks were still available:
  - a recovery round claimed every block in a volume's range, not only the
    ones it fetched;
  - blocks released while a worker was busy were never offered again;
  - volumes of an interleaved set, which are numbered by row, could be
    skipped as if they held nothing new.
- **Sets with missing or damaged index files.** Recovery reads the start,
  matrix and root packets that each volume repeats, so a set whose index file
  never arrives can still be repaired. If no trustworthy copy of one of those
  packets exists, the job names it instead of waiting forever. The outcome
  also reports how much of each volume was damaged or unreadable.
- **Checks before anything is written.** A repair is refused before its first
  output byte when the set names a path Weaver will not create (absolute,
  parent, reserved device name and similar), or when the working folder cannot
  hold the repaired files plus one staging copy.
- **Memory limits.** A repair that cannot fit within the memory budget on its
  own now fails and moves on to the next option instead of waiting forever.
  A search for PAR3 data inside an archive that was turned away for lack of
  memory is tried again later instead of being skipped for the rest of the job.
- **Cleanup.** After a successful repair, PAR3 files are removed along with
  the archives, as PAR2 files already were.
- **Restarts.** A job restarted while it is fetching recovery data picks up
  where it was, without re-reading source files that did not change.
- **Metrics.** New `weaver_par3_ledger_*` and `weaver_par3_engine_*` families
  report the repair engine's memory use, admissions, refusals and cache
  activity.
- The PAR3 engine is now par3-rs 0.4.1.

### Downloads and reliability

- **PAR2 repair leftovers.** The damaged original a repair renames aside no
  longer ends up in the completed folder when the set was first reported as
  intact and only repaired later.
- **Servers configured through environment variables** are now asked whether
  they support pipelining, as servers added in Settings already were. Before,
  they always ran one request at a time. An explicit pipelining setting is
  still used as given. A server that cannot be reached at startup keeps the
  one-at-a-time default and logs why.
- **Providers used survives a restart.** A job's per-provider counts are saved
  while it downloads, at most every five seconds and on shutdown, instead of
  only when it finishes.

### Proxy routing (beta)

- **DNS through a proxy** keeps the addresses it found when the DNS server
  answers for IPv4 or IPv6 only, instead of treating the name as unresolvable.
- **Failover is faster when a proxy is down.** A proxy that cannot reach the
  DNS server is tried once per lookup instead of twice before the next proxy
  in the order.

### Interface

- **API key links.** Opening
  `settings/security?createApiKey=1&name=…&scope=…` in the new interface
  creates the key immediately and opens it selected, with a copy button inside
  the key field. If Weaver needs your password first, it asks, then creates the
  key. The link is removed from the address bar first, so reloading the page
  does not create a second key. The link formats the classic interface accepted
  still work.
- **Setup restart.** The restart offered at the end of first-run setup is no
  longer refused.

### Access and security

- **First-run setup without a code** (a fresh install listening only on
  loopback) now requires the address bar to show `localhost` or an IP address,
  with the request coming from that same page. A web page on another site could
  otherwise reach a pending setup through a host name that points at
  `127.0.0.1`. Access by host name after setup is unchanged, and no host list
  needs to be configured.
- **SOCKS5 profiles** with a password but no username are rejected. SOCKS5
  only sends a password together with a username, so such a profile was
  connecting without authentication.
- **Release check switch.** Set `WEAVER_UPDATE_CHECK=0` (or `false`) to stop
  Weaver from checking GitHub for new releases. No request is made, and the
  update status says the check is off.

## Upgrade notes

- **One database migration** runs on first start. It adds a column that keeps
  a running job's provider counts.
- **SOCKS5 profiles with a password and no username** stop connecting until
  you add the username or remove the password.
- **Setting up a fresh loopback-only install without a code:** open it at
  `http://localhost:<port>` or `http://127.0.0.1:<port>`, not through a
  custom host name.
- **GraphQL.** No schema changes.
