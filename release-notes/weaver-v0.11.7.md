# Weaver 0.11.7 release notes

## Highlights

Weaver 0.11.7 fixes a stall in which one "too many connections" answer from a
news server could leave every download lane idle for ten minutes, showing all
connections as in use while nothing downloaded. Lanes now keep their sockets
across job boundaries, and a server that refuses a connection is asked again
after half a minute instead of ten.

It also corrects a server's download-quota reading, which could show room on a
server that had none left.

## What changed

### Download reliability

- **Connections survive job boundaries.** A download lane used to close its
  socket when the next job's newsgroups differed from the one it was opened
  for, and open a new one. While a provider was refusing new sockets, every
  lane did this at the same job boundary and none could reconnect, so
  throughput dropped to zero until the refusal pause ended. Lanes now keep the
  socket and, only on servers that require a selected group, send `GROUP` for
  the new job on the connection they already hold. On most servers nothing at
  all is sent.
- **A refused connection pauses new sockets for 30 seconds, not 10 minutes.**
  When a server answers a connect with "too many connections", Weaver still
  stops opening sockets to it and keeps the established ones running. The
  pause now starts at 30 seconds. When it ends, a single probe connection asks
  whether the server will accept again: if it does, all lanes may connect; if
  it is refused, the pause doubles, up to the previous 10 minutes. A restart
  while the provider still counts the old process's sessions therefore
  recovers in about half a minute, while a server that keeps refusing is left
  alone for longer each time. The log line for the refusal now carries the
  pause length and how many refusals the current episode has seen, and an
  info line marks the moment the server accepts again.

### Server quotas

- **A server at its download quota is reported as blocked again.** Weaver
  checks, before each batch of work, whether any server could take a download
  at all. That check asked for no particular article, which every server with
  a byte of headroom can supply, and its answer was recorded as though the
  server had just accepted real work — clearing the "at quota" mark the last
  real request had set. A server whose quota was spent could therefore show as
  having room, and the reason given for held-back downloads could read as
  something other than the server quota. The quota itself was always enforced:
  no download exceeded a configured cap, and work still moved to another
  server, so this was what you were told rather than what Weaver did. The
  check is now read-only, and only a request the server actually turns away
  marks it as at quota.

## Upgrade notes

- No database migration, GraphQL schema change or settings change is required.
- If you saw `provider refused a new connection as over its limit` in the log
  followed by minutes of idle lanes, this release addresses that. Configuring
  a few connections fewer than the provider's limit still avoids the refusal
  in the first place.
- If a server's download quota looked unspent while its work was going
  elsewhere, its quota reading will now be correct without any action. Recorded
  usage was never wrong, so no quota needs resetting.
