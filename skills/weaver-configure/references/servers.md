# News servers

Web UI: **Settings → Providers**. The API calls them servers.

## ServerInput

`addServer(input)` and `updateServer(id, input)` take the same input.
Update is a full replacement: a field left out is not "unchanged" unless the
last column says so.

| Field | Type | Default | Left out on update | Meaning |
|---|---|---|---|---|
| `host` | String! | – | required | Name or address. Trimmed; an `http://`, `https://`, `nntp://` or `nntps://` prefix and a trailing `/` are dropped. Empty is refused. |
| `port` | Int! | – | required | 0 to 65535. |
| `tls` | Boolean! | – | required | Connect with TLS. |
| `username` | String | null | kept | Login name. Trimmed; on update, blank clears it. Secret: only via `add-secret.sh`. |
| `password` | String | null | kept | Trimmed; blank = keep. Stored encrypted. There is no way to clear it through the API. Secret: only via `add-secret.sh`. |
| `connections` | Int! | – | required | 0 to 65535 connections Weaver may open at once. No other bound: never more than the provider's plan allows. |
| `active` | Boolean! | `true` | → `true` | Off = Weaver does not use the server. |
| `priority` | Int! | `0` | → `0` | 0 to 65535. Priority group: lower is tried first within the fill tier. |
| `backfill` | Boolean! | `false` | → `false` | Backfill servers serve only articles the fill tier could not: missing or corrupt there, or outside a fill server's retention. |
| `retentionDays` | Int! | `0` | → `0` | Days of articles the server holds. Older articles skip it without a network attempt. 0 = unlimited. |
| `maxDownloadSpeed` | Int | `0` | kept | Bytes per second across all its connections. 0 = unlimited. |
| `downloadQuota` | ServerDownloadQuotaInput | off | kept | Byte allowance; below. Sent, it replaces the whole quota. |
| `tlsCaCert` | String | null | **cleared** | Path, on the Weaver host, to a PEM CA certificate to trust for this server. Blank = none. |
| `tlsNameMismatchCertificateDerBase64` | String | null | **cleared** | One adopted certificate, base64 DER, 1 to 65536 bytes, accepted only after a hostname mismatch. Needs `tls: true`. |
| `route`, `routing` | – | – | kept | Out of scope. Never send them. |

Weaver never returns the password, and this skill never reads the
username back.

### Keeping what is stored on update

1. Read `ServerSettings` → `server.json`.
2. Build the input from it, then change only what was agreed:

```sh
jq '{id: .data.server.id, input: (.data.server | {
      host, port, tls, connections, active, priority, backfill,
      retentionDays, maxDownloadSpeed, tlsCaCert,
      tlsNameMismatchCertificateDerBase64,
      downloadQuota: (.downloadQuota | {enabled, limitBytes, period,
        resetTimeMinutesLocal, weeklyResetWeekday, monthlyResetDay})})}' \
  server.json > base.json
jq '.input.connections = 30' base.json > vars.json
```

3. Leave `username` and `password` out to keep them. To change one,
   `add-secret.sh vars.json username ...` (or `password`) adds it.

## Connection tests

Contacting the provider:

| Call | When it connects |
|---|---|
| `AddServer` | `active: true` (the default): tests before saving. A failed test saves nothing. |
| `UpdateServer` | `active: true`: tests before saving. A failed test saves nothing. |
| `TestServerConnection` | Always. Uses the stored login and the saved route. Saves nothing. |

Tests run over the server's saved route. A server added here gets no route
of its own: it is tested and then used under the default route, which
connects directly from the Weaver host. If the operator's servers must go
through a proxy or tunnel, do not add it here. The operator adds it in the
web UI with **Kill switch** on (saved off, untested, nothing allowed out),
sets its route under Networking, then enables it.

Reading a test (`TestServerConnection`):

- `success: false` + `message`: the provider refused or could not be
  reached. Not a GraphQL error.
- `legs[]`: one entry per route leg tried.
- `adoptableTlsNameMismatchCertificate` set: the certificate names another
  host (`names`, `sha256Fingerprint`). Report it. Trusting it is the
  operator's call, in the web UI: **Settings → Providers** → the provider →
  **Test connection** → **Trust this certificate**. This skill never adopts
  a certificate; it only resends the stored one.
- A save that tests also records `supportsPipelining` and the TLS cipher
  suite. Changing `host` or `port` drops the learned pipelining depth.

After a save, Weaver restarts its news connections. If that fails
(`INTERNAL`), the setting was saved but the old connections still run:
re-read; the operator retries or restarts Weaver.

## ServerDownloadQuotaInput

All fields required when the quota is sent. Counts raw article body bytes.

| Field | Type | Default | Meaning |
|---|---|---|---|
| `enabled` | Boolean! | `false` | Off = no limit. |
| `limitBytes` | Int! | `0` | Allowance per window. Must be > 0 when enabled. |
| `period` | enum | `ONE_TIME` | `ONE_TIME`, `DAILY`, `WEEKLY`, `MONTHLY`. |
| `resetTimeMinutesLocal` | Int! | `0` | Minutes after local midnight the window starts, 0 to 1439. |
| `weeklyResetWeekday` | enum | `MON` | `MON` to `SUN`, for `WEEKLY`. |
| `monthlyResetDay` | Int! | `1` | 1 to 31, for `MONTHLY`; past a short month's end it is that month's last day. |

- `ONE_TIME` has no window: usage never resets on its own. The operator
  resets it with **Reset usage** in the provider's editor. This skill does
  not reset usage.
- Changing `period`, the reset time, weekday or day, or turning the quota on,
  starts counting again from that moment.
- `ListServers` shows `downloadQuota.blocked`: true = allowance used up.

## Removing a server

Never through this skill. The operator does it: **Settings → Providers** →
the provider → **Remove provider** → confirm **Remove provider**. Downloads
in flight move to the remaining providers.
