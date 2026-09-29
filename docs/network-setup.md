# Network setup and browser access

```yaml
services:
  weaver:
    image: ghcr.io/scryer-media/weaver:latest
    container_name: weaver
    environment:
      - PUID=1000
      - PGID=1000
      - TZ=Etc/UTC
    volumes:
      - /path/to/weaver/config:/config # this is the critical volume with all your config data and encryption key
    ports:
      - 9090:9090
    restart: unless-stopped
```

New installations require an administrator login. When Weaver listens on more
than loopback (the container's `0.0.0.0`, a LAN address) or behind
`WEAVER_TRUSTED_PROXIES`, the wizard also asks for a one-time setup code, which
Weaver prints once at startup such as `K7P-M2X`, in a banner headed
"FIRST-TIME SETUP: ACTION REQUIRED" (with JSON logging, a `WARN` record with a
`setup_code` field). Enter it in the browser wizard; the hyphen is optional.
The code is valid until setup succeeds or Weaver restarts and is never exposed
by unauthenticated HTTP. A Weaver on the default `127.0.0.1` can only be opened
from its own machine, so its wizard needs no code; set
`WEAVER_REQUIRE_SETUP_CODE=1` to ask for one anyway. For unattended setup, use
bootstrap credentials:

```yaml
      - WEAVER_ACCESS_MODE=authenticated
      - WEAVER_BOOTSTRAP_LOGIN_USERNAME=admin
      - WEAVER_BOOTSTRAP_LOGIN_PASSWORD_FILE=/run/secrets/weaver-login
```

Mount the password as a Compose secret or another read-only file. Set exactly
one of `WEAVER_BOOTSTRAP_LOGIN_PASSWORD` and
`WEAVER_BOOTSTRAP_LOGIN_PASSWORD_FILE`; invalid or incomplete bootstrap input
fails startup. Existing credentials are retained. The new `WEAVER_ACCESS_MODE`
accepts only `authenticated`; blank means unset and other nonempty values are
errors. It is optional for new installations and explicitly migrates an existing
installation to authenticated browser access.
For a legacy installation with no stored login, setting only
`WEAVER_ACCESS_MODE=authenticated` opens setup (with a one-time startup code
when Weaver listens beyond loopback);
bootstrap credentials and reset recovery are not required. Pending setup survives
restarts and removing the migration override. An installation that has already
completed authenticated setup never reopens setup merely because its credentials
are missing; use explicit reset recovery in that case.

`WEAVER_TRUSTED_CIDRS` can restrict where a remembered browser login is
accepted in authenticated mode. It never makes an unknown browser an
administrator; a password login outside the list creates an ordinary session.

Weaver binds to `127.0.0.1` by default, so a native install is never exposed by
accident; the container image ships `WEAVER_HTTP_BIND_ADDRESS=0.0.0.0`, since a
container's exposure is decided by the port you publish. Set that variable
yourself for a native LAN or reverse-proxy deployment, or use a specific
non-loopback address. The address is also editable in **Settings → Security**,
which is the normal route for a desktop or service install; under Docker that
setting only moves the address within the container's own network namespace,
where the ports you publish with `-p` — or `--network host` — are what actually
decide exposure, so pin `WEAVER_HTTP_BIND_ADDRESS` at the deployment level
instead. The variable always wins over the stored setting.

Binding and browser authentication are separate: binding to a LAN interface
does not trust its clients. Agents and integrations use persistent, scoped API
keys instead of browser sessions.

Behind a reverse proxy, list only its socket address or CIDR in **Settings →
Security → Network access**, alongside the optional remembered-login CIDRs.
`WEAVER_TRUSTED_PROXIES` remains available as a deployment override and makes
that field read-only. Weaver accepts `X-Forwarded-For` only from that named
peer and removes trusted hops right to left. Missing, malformed, oversized, or
all-trusted chains are unresolved and cannot satisfy remembered-login network
restrictions. Do not trust Docker gateways or broad private ranges as a proxy
workaround. Configure the proxy to preserve `Origin` and append
`X-Forwarded-For`; HTTPS is recommended for remote access.

For an unattended first start, configure `WEAVER_BOOTSTRAP_LOGIN_USERNAME` and
exactly one of `WEAVER_BOOTSTRAP_LOGIN_PASSWORD` or
`WEAVER_BOOTSTRAP_LOGIN_PASSWORD_FILE`. Bootstrap credentials are used only
when no login is already stored; they never overwrite an existing login.

Sessions are per browser. Remembered sessions last at most 30 days; ordinary
cookies expire when the browser session ends, with the same 30-day server cap.
Cookies are host-only, HttpOnly, SameSite, and Secure
on HTTPS. Logout revokes the current session, and **Sign out all** revokes every
browser session. Password changes, sign-out-all, the combined trusted-network,
trusted-proxy and bind-address policy, API-key creation, backup operations, and
execution-sensitive configuration require a password verification from the
same browser session within 15 minutes.

Existing installations retain legacy access behavior until explicitly
migrated. Legacy trusted CIDRs retain their compatibility meaning and host
defenses remain active. In authenticated mode, internal Docker names and valid
external hosts work unless `WEAVER_HTTP_ALLOWED_HOSTS` explicitly restricts
them.

Existing bind, proxy, trusted-CIDR, cookie-security, strict-security, and host
environment overrides remain supported. Migration preserves stored credentials
and API keys; browsers sign in again to obtain individually revocable sessions.

Native installs listen on `127.0.0.1` by default; the container image listens
on `0.0.0.0`, with published ports controlling exposure. Listener changes can
require restart; the interface reports saved versus active state and preserves
drafts after rejected writes. For recovery, use `WEAVER_RESET_LOGIN=1`, finish
authenticated setup with the startup code or bootstrap credentials, then
remove the reset variable.

## Outbound networking

**Settings → Networking** controls outbound connections. The HTTP listen
address remains under Security. An egress can use system routing, an interface,
or a specific local source address. Routes contain weighted legs; each leg is
direct or an ordered ladder of proxies, same-kind proxy pools, and chains.
WireGuard and HTTP/3 may only be the first hop of a chain. RSS uses the first
healthy leg and requires DNS servers on every proxy path.

Redistribute moves a failed leg's connection share to healthy legs. Hold parks
that share. Rebalancing recalls idle sockets; an article already in progress
finishes before its socket is replaced. Global bandwidth limits apply at
dispatch, followed by egress and server limits on reads. Zero means unlimited.

The live overview shows the selected path, source address, open/target count,
and throughput. Expand a pool to inspect handshake, stream-open and delivery
evidence. Exported SVGs include the displayed hostnames and addresses, but no
credentials. A pool can warm every member at once, so account for the provider's
session allowance. Weaver also limits canonical WireGuard instances to
`clamp(available memory / 4 / 516 MiB, 1, 8)`; a member used from two egresses
counts twice, while consumers sharing a member and egress share one instance.

### Native systems

- Linux interface binding uses `SO_BINDTODEVICE`. If the kernel refuses it
  with a permissions error, grant the service `AmbientCapabilities=CAP_NET_RAW`
  and `CapabilityBoundingSet=CAP_NET_RAW` in a systemd override. `NET_ADMIN`
  is unnecessary. Source binding alone does not choose the outgoing route;
  configure an appropriate source policy rule or select an interface.
- macOS interface binding uses `IP_BOUND_IF`/`IPV6_BOUND_IF` and does not need
  elevated privileges. Source binding still needs an appropriate scoped route.
  Local Network privacy on macOS 15 may prompt for LAN destinations; this
  interaction has not been verified.
- Windows advertises System and SourceAddress bindings. The source adapter
  needs a usable default gateway; interface binding is not advertised because
  equivalent enforcement has not been verified.

A missing interface or source address fails closed. The health monitor notices
interfaces disappearing and returning. Restoring configuration onto a different
machine may leave configured egresses Down until their bindings are updated.

### Containers

Bridge networking exposes container interfaces, not the host's physical links.
For host-link selection on Linux, use host networking and pin the HTTP listener:

```yaml
services:
  weaver:
    image: ghcr.io/scryer-media/weaver:latest
    network_mode: host
    environment:
      - PUID=1000
      - PGID=1000
      - WEAVER_HTTP_BIND_ADDRESS=127.0.0.1
      # Enable together with NET_RAW only when required by the host kernel:
      # - WEAVER_RETAIN_NET_RAW=true
    volumes:
      - /path/to/weaver/config:/config
    # Enable only if the host kernel requires it for interface binding:
    # cap_add: [NET_RAW]
    restart: unless-stopped
```

Alternatively, attach multiple container networks and select their interfaces.
Docker Desktop's host networking remains within its virtualization boundary;
it does not expose macOS or Windows physical interfaces to a Linux container.
The root entrypoint retains an already-granted `NET_RAW` capability only when
`WEAVER_RETAIN_NET_RAW=true` is explicitly set. It uses `setpriv`'s inheritable
and ambient capability sets when switching to PUID. The default drops it.
It never grants a capability absent from the container's permitted set.
See the [setpriv manual](https://man7.org/linux/man-pages/man1/setpriv.1.html)
for capability inheritance details.
