# Refusal codes

A refused write is a GraphQL error; `gql.sh` exits 1 and prints
`<code> ... <message>`. Most server and category refusals carry no code
(`NO_CODE`). For those, the message is the only signal: match it to a row
below, show it to the operator, and never retry the same input.

## With a code

| `extensions.code` | From | Do |
|---|---|---|
| `FORBIDDEN` | Any operation here: the key is not `ADMIN` | Stop. The operator needs a **Full administration** key. |
| `INVALID_INPUT` | `AddCategory`, `UpdateCategory`: the name is not one safe folder name and no `destDir` is set | Pick another name, or set `destDir` (`categories.md`). |
| Route codes | `AddServer`, `UpdateServer`, `TestServerConnection`: the saved route cannot be used | Report the code and message. Routes are fixed in the web UI. |

## Without a code (`NO_CODE`)

Servers:

| Message starts with | Do |
|---|---|
| `server host must not be empty` | Ask for the host. |
| `server connection test failed:` | Nothing saved. Show the reason (login, host, port, TLS). Change only with a new yes. |
| `a hostname-mismatch TLS certificate requires TLS to be enabled` | Keep `tls: true`, or drop the adopted certificate with the operator's yes. |
| `adopted TLS certificate must be` | The certificate was altered. Rebuild the input from `ServerSettings`. |
| `server max download speed exceeds database range` | Lower `maxDownloadSpeed`. |
| `server download quota limit exceeds database range` | Lower `limitBytes`. |
| `enabled server download quota limit must be greater than zero` | Set `limitBytes` or turn the quota off. |
| `server download quota reset time must be between 0 and 1439` | Fix `resetTimeMinutesLocal`. |
| `server download quota monthly reset day must be between 1 and 31` | Fix `monthlyResetDay`. |
| `server <id> not found` | Re-read `ListServers`. |
| `route has no legs to test` | The saved route has nothing to test through. Report; routes are fixed in the web UI. |
| `server setting was saved, but the runtime remains on the prior NNTP generation` | Saved, but not in use yet. Re-read `ListServers`; the operator retries or restarts Weaver. Do not repeat the write on your own. |

Categories:

| Message starts with | Do |
|---|---|
| `category name must not be empty` | Ask for the name. |
| `category '<name>' already exists` | Names are unique ignoring letter case. Pick another, or update the existing one. |
| `category <id> not found` | Re-read `ListCategories`. |

A wrong variable type or a missing required field is refused before any
resolver runs: fix the variables file against the operation's comment.
