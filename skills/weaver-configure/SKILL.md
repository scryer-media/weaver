---
name: weaver-configure
description: Reads and changes an operator's Weaver news servers and categories through the API - adds or edits a usenet provider (host, port, TLS, connections, priority, backfill, retention, speed cap, data quota), tests a saved provider's connection, and adds or edits a category's name, folder and aliases - showing every change before it is made. Use when an operator wants to add, edit, enable, disable, reprioritise or test a news server, or add or edit a download category, on their own Weaver server.
---

# Configure news servers and categories

Exact, reviewed writes. Reads are safe. Every write and every connection test
is shown to the operator first and waits for a yes.

## Prerequisites

- Complete `weaver-connect` first (sets `WEAVER_URL`, `WEAVER_API_KEY`;
  provides `../weaver-connect/scripts/gql.sh`, below
  `G="$S"/../weaver-connect/scripts/gql.sh`).
- `S` = this skill's directory (absolute). Work in a separate directory the
  operator agrees to, so output never lands inside the skill. Files that hold
  a login go in a private subdirectory of it.
- Scope: `ADMIN` (**Full administration**) for every operation, reads too.
- Local tools: `jq`.

## Operations

| File | Root field | Scope |
|---|---|---|
| `ListServers` | `servers` | ADMIN |
| `ServerSettings` | `server` | ADMIN |
| `AddServer` | `addServer` | ADMIN |
| `UpdateServer` | `updateServer` | ADMIN |
| `TestServerConnection` | `testConfiguredServerConnection` | ADMIN |
| `ListCategories` | `categories` | ADMIN |
| `AddCategory` | `addCategory` | ADMIN |
| `UpdateCategory` | `updateCategory` | ADMIN |

Each file's leading comment gives its purpose and a variables example.

## Rules

- No schema or source reads. Files in `operations/` are complete for this
  skill.
- Every write needs the operator's yes on a summary from
  `scripts/show-change.sh`: names, hosts, ports, connection counts. Never a
  password or username.
- A connection test contacts the operator's news provider. `AddServer` and
  `UpdateServer` on an active server test before saving, and
  `TestServerConnection` always does. Each needs a yes that names the host.
- Logins only from a file or environment variable the operator set, via
  `scripts/add-secret.sh`. Never ask for one in the conversation, never print
  one, never put one on a command line.
- Updates replace the record. Build the input from the stored state, change
  only the agreed fields (`references/servers.md`,
  `references/categories.md`).
- Never send `route`, `routing` or a category's torrent fields, never set a
  new adopted certificate, never reset quota usage. Give the web UI area
  instead.
- Never remove a server or category. Give the operator the web UI path
  (references, "Removing").
- Every response to a file; read with jq.
- On a refusal, find it in `references/refusal-codes.md`. Most carry no
  code: match the message start, show it, never retry the same input.

## Procedure

1. **Pick the area**; load only its reference:
   - news servers, quota, connection tests: `references/servers.md`
   - categories: `references/categories.md`

2. **Read current state**, save it:
   `sh "$G" "$S"/operations/ListServers.graphql > servers.json` (or
   `ListCategories`). Show one line per item, not raw JSON:

   ```sh
   jq -r '.data.servers[] | "\(.id) \(.host):\(.port) tls=\(.tls) conns=\(.connections) active=\(.active) prio=\(.priority) backfill=\(.backfill)"' servers.json
   ```

3. **Agree on the change** in plain terms. Ask only for values not given. For
   a login, the operator writes it to a file (or exports a variable) and
   gives you the path or name. Updating a server that has a login always
   needs its username again: the update clears a username left out.

4. **Build the variables file.**
   - Server update: `ServerSettings` with `{"id": N}` → `server.json`; jq
     into `{id, input}` (`references/servers.md`); change the agreed fields.
   - Server add: write `{"input": {...}}` with jq.
   - Login: `sh "$S"/scripts/add-secret.sh vars.json username vars.json --env NNTP_USER`
     (same for `password`). Leave `password` out of an update to keep it.
   - Category: `{id, input: {name, destDir, aliases}}` from the list entry.

5. **Show the change**:
   `sh "$S"/scripts/show-change.sh before.json vars.json`, where `before.json`
   is `.data.server` from `server.json`, the category's list entry, or `{}`
   for an add. Present its lines as before/after. Add a plain note when:
   - the server is active: saving tests it against `<host>` first;
   - a field shows as cleared or reset (`tlsCaCert`, the adopted
     certificate, `username`, `destDir`, `aliases`);
   - `active` turns off (Weaver stops using the server) or `connections`
     rises (must stay within the provider's plan).
   Wait for a yes.

6. **Write**: `sh "$G" "$S"/operations/<Write>.graphql vars.json > result.json`.
   Non-zero exit: `references/refusal-codes.md`, explain, back to step 3.

7. **Confirm**: re-run the list read, compare with what was sent, and show
   the changed fields only. A difference is reported, not fixed silently.

8. **Test on request**: `TestServerConnection` with `{"id": N}`, after a yes
   naming the host. Report `success`, `message`, `latencyMs`; for a
   certificate that names another host, see `references/servers.md`.

9. **Hand back login files.** Tell the operator which local files hold a
   login (their file and any `vars.json` built by `add-secret.sh`) so they
   can dispose of them. Do not remove them yourself.

## Stop and ask

- Before every write and every connection test (steps 5 and 8).
- When a server must reach its provider through a proxy or tunnel: adding
  it here would connect directly (`references/servers.md`).
- When the request needs something not covered: removals, routes, proxies,
  DNS, RSS, torrents, quota reset, adopting a certificate, global settings.
  Give the web UI area instead.
