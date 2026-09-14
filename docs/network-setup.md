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

New installations require an administrator login. Weaver prints a one-time
setup code once at startup; enter it in the browser wizard. The code is valid
until setup succeeds or Weaver restarts and is never exposed by unauthenticated
HTTP. Native launchers show the same code. For unattended setup, use bootstrap
credentials:

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
`WEAVER_ACCESS_MODE=authenticated` opens setup with a one-time startup code;
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
