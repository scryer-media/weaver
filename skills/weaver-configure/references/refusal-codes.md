# Refusal codes

A refused write is a GraphQL error; `gql.sh` exits 1 and prints
`<code> ... <message>`. Decide on the code. Show the message to the
operator, and never retry the same input.

| `extensions.code` | From | Do |
|---|---|---|
| `FORBIDDEN` | Any operation here: the key is not `ADMIN` | Stop. The operator needs a **Full administration** key. |
| `INVALID_INPUT` | `AddServer`, `UpdateServer`: empty host, `maxDownloadSpeed` or a quota field out of range, an adopted certificate that is not base64 DER of 1 to 65536 bytes or is sent with `tls: false` | Fix the field the message names (`servers.md`). A changed certificate: rebuild the input from `ServerSettings`. |
| `INVALID_INPUT` | `AddCategory`, `UpdateCategory`: empty name, or a name that is not one safe folder name and no `destDir` | Ask for the name, or set `destDir` (`categories.md`). |
| `NAME_TAKEN` | `AddCategory`, `UpdateCategory`: the name exists, ignoring letter case | Pick another, or update the existing one. |
| `NOT_FOUND` | A server or category id that does not exist | Re-read `ListServers` or `ListCategories`. |
| `SERVER_TEST_FAILED` | `AddServer`, `UpdateServer` on an active server, `TestServerConnection`: the provider refused or could not be reached, or the route has no legs | Nothing saved. Show the reason (login, host, port, TLS). Change only with a new yes. |
| `TLS_NAME_MISMATCH` | `AddServer`, `UpdateServer`: the provider's certificate names another host | Nothing saved. Report it; trusting a certificate is the operator's call in the web UI (`servers.md`). |
| Route codes | `AddServer`, `UpdateServer`, `TestServerConnection`: the saved route cannot be used | Report the code and message. Routes are fixed in the web UI. |
| `INTERNAL` | `AddServer`, `UpdateServer`: saved, but the news connections did not restart | Re-read `ListServers`; the operator retries or restarts Weaver. Do not repeat the write. |

A wrong variable type or a missing required field is refused before any
resolver runs: fix the variables file against the operation's comment.
