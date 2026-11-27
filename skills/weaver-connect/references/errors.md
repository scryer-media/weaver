# Error codes

Shape:

```json
{"message": "...", "extensions": {"code": "ROUTE_PROXY_REPEATED", "params": {"limit": 2}}}
```

- `extensions.code`: the stable code, SCREAMING_SNAKE. Codes are stable;
  sentences are not.
- `extensions.params`: an object of the values the sentence names, present
  only when it names any.
- `extensions.matched`: on a batch `CONFLICT`, the count that matches now.
- A wrong variable type or a missing required variable is refused before any
  resolver runs: fix the variables file against the operation's comment.

Decide on `code` and its facts only. Quote the message to the operator when
useful; never parse it.

## Access

| `extensions.code` | Meaning | Do |
|---|---|---|
| `FORBIDDEN` | The key's scope does not cover this field (`read scope required`, `control scope required`, `admin scope required`), or a `CONTROL` key named a torrent folder outside the save folders | Name the scope it needs (`scopes.md`). Stop. |
| `REAUTH_REQUIRED` | A browser session must confirm its password. Not sent to API keys | Only seen from a browser. |
| `NOT_ALLOWED_FOR_SCRIPT_RUN` | A post-processing script's run token asked for something outside its own job | Not for API keys. |

## Input and lookup

| `extensions.code` | Meaning | Do |
|---|---|---|
| `INVALID_INPUT` | Input refused | Fix the input from the message. If unclear, show it and ask. |
| `NOT_FOUND` | Id does not exist | Re-read ids from a list query. Never guess ids. |
| `CURSOR_INVALID`, `CURSOR_EXPIRED` | A paging cursor is not one Weaver issued, or is too old | Start the listing again from the first page. |
| `CONFLICT` | The item already exists, changed underneath, or a batch selection no longer matches the confirmed count (`matched`) | Re-read, show the new state or count, get a new yes. |
| `DUPLICATE` | An add matched a download Weaver already has | Report; the operator decides. |
| `NAME_TAKEN` | A name already exists | Pick another name. |
| `SECRET_IN_USE` | A stored secret is still used | Report what uses it. |
| `BATCH_TOO_LARGE` | Too many items in one call | Split the call. |

## Queue and adds

| `extensions.code` | Meaning | Do |
|---|---|---|
| `REFUSED` | The queue refused the change in its current state (being removed, intake closed) | Re-read the item's state, then decide. |
| `PARKED` | A held duplicate candidate was replaced before it could start | Report the message. |
| `NO_NETWORK` | Nothing can carry the download now | Check server and route health; report. |
| `NZB_DISABLED` | Usenet downloads are off or not running | Report. |
| `SCHEDULER_ERROR` | The queue could not apply the change | Report the message. |

## Torrents

| `extensions.code` | Meaning | Do |
|---|---|---|
| `TORRENTS_DISABLED` | The torrent client is not running | Report. Torrents are opt-in. |
| `TORRENTS_UNAVAILABLE` | Torrents cannot be turned on: the server was started without them (`WEAVER_TORRENTS`) | Report; the operator decides. |
| `TORRENTS_LOADING` | Stored torrents are still being read at start | Ask again once, a little later. |
| `CONFLICT` | A queue cancel of a private torrent was refused: its seeding goal is not met. `removeTorrent` and `removeTorrents` are never refused for it | Report; the operator decides. |

## Server faults

| `extensions.code` | Meaning | Do |
|---|---|---|
| `INTERNAL` | Server fault | Report the message. Do not retry in a loop. |
| `STORAGE_FULL` | The disk holding Weaver's database has no room; nothing was written | Report; the operator frees space. No retry. |

## Servers and scripts

| `extensions.code` | Meaning | Do |
|---|---|---|
| `SERVER_TEST_FAILED` | A news server test failed, or a save that tests first; nothing saved | Show the message (login, host, port, TLS). |
| `TLS_NAME_MISMATCH` | The same, and the provider's certificate names another host | Report; trusting it is the operator's call in the web UI. |
| `STRICT_SECURITY` | `WEAVER_STRICT_SECURITY=1` on the host refuses the setting | Host setting; report and stop. |
| `SCRIPT_HEADER_INVALID` | A script's header or manifest cannot be read, or declares nothing | Wire the script by hand (`weaver-migrate`). |
| `SCRIPT_TEST_BUSY` | Four script tests are already running | Ask again when one ends. |

Other codes are refusals with a code of their own (settings, network access,
routes); their message and `params` say what was refused.

## HTTP-level refusals

`gql.sh` exits 2 when the body is not a GraphQL response. Refusals before the
schema runs answer `{"code": "...", "error": "..."}`:

| HTTP | `code` | Meaning | Do |
|---|---|---|---|
| 401 | `INVALID_API_KEY` | A key was sent and refused: unknown, revoked, malformed, or different in `Authorization` and `X-API-Key` | Ask the operator for a new key. No retry. |
| 401 | `AUTHENTICATION_REQUIRED` | No key reached Weaver | Check `WEAVER_API_KEY` is exported. |
| 403 | `BROWSER_VERIFICATION_REQUIRED` | A browser origin or CSRF check failed | Not sent to a request with a key; check a proxy is not stripping the header. |
| 413 | `REQUEST_TOO_LARGE` | The request body is over the limit | Send less in one request. |

`GET $WEAVER_URL/api/system/diagnostics` (the bundle) answers the same 401
and 403 codes, plus:

| HTTP | `code` | Meaning | Do |
|---|---|---|---|
| 403 | `FORBIDDEN` | The key is not `ADMIN` | Name the scope; stop. |
| 500 | `INTERNAL` | The bundle could not be built | Report the `error`. |

Other answers:

- **HTTP 421 `request Host is not allowed`** (plain text): the host name in
  `WEAVER_URL` is not one this Weaver accepts. IP addresses and `localhost`
  are always accepted. The operator uses one of those, or adds the name to
  `WEAVER_HTTP_ALLOWED_HOSTS`.
- **HTTP 400 `invalid Host header`** (plain text): the request's host could
  not be read. Check `WEAVER_URL`.
- **HTML instead of JSON**: `WEAVER_URL` points at a proxy page or the wrong
  base path.

## MCP

Refusals at `POST $WEAVER_URL/mcp` answer `{"code": "...", "error": "..."}`:

| HTTP | `code` | Meaning | Do |
|---|---|---|---|
| 403 | `MCP_DISABLED` | The MCP server is turned off; answered before any other check | The operator turns on **Allow the MCP server** (**Settings → Security**). If the switch is locked, `WEAVER_MCP_ENABLED` sets it on the host: the operator's call. |
| 401 | `MCP_API_KEY_REQUIRED` | No key, or a browser session or local access instead of a key | Configure the key in the MCP client. |
| 401 | `MCP_API_KEY_INVALID` | A key was sent and refused | Ask the operator for a new key. No retry. |
| 403 | `MCP_ORIGIN_NOT_ALLOWED` | A browser page on another origin made the call | Call from the MCP client, not a page. |
| 413 | `MCP_REQUEST_TOO_LARGE` | The request body is over 1 MiB | Send less in one call. |
| 400 | `MCP_REQUEST_BODY_UNREADABLE` | The request body could not be read | Check the client and any proxy. |

A call the tool cannot take is a JSON-RPC `invalid_params` error with
`data.reason`:

| `data.reason` | Meaning | Do |
|---|---|---|
| `MCP_TOOL_UNKNOWN` | No tool has this name | Use a tool from section 5 of SKILL.md. |
| `MCP_ARGUMENTS_INVALID` | An argument is missing, of the wrong type, or not taken; `argument` names it | Fix that argument. |

A tool error comes back with `isError` and
`errors[{code, reason?, message, path?, ...}]`. A GraphQL error keeps its own
`code` (`FORBIDDEN`, `INVALID_INPUT`, ...; tables above). The MCP layer's
own refusals:

| Code | Meaning | Do |
|---|---|---|
| `MCP_OPERATION_UNKNOWN` | No operation has this name; `skills` lists skills that have it when another skill does | Call `list_operations`, or name one of `skills`. |
| `MCP_OPERATION_AMBIGUOUS` | Skills ship different text under this name; `skills` lists them | Name one with `skill`. |
| `MCP_SKILL_UNKNOWN` | No served operation is in this skill; `skills` lists those that are | Use one of them. |
| `MCP_OPERATION_KIND_MISMATCH` | A query sent to `run_mutation`, or the reverse; `tool` names the right one | Use that tool. |
| `MCP_RESULT_TOO_LARGE` | The result is over 32 KiB (`result_bytes`, `max_bytes`); `applied: true` when a mutation already took effect | Narrow with paging or filters. After `applied: true`, do not repeat it; re-read instead. |
| `MCP_CONFIRMATION_MISMATCH` | The token was for a different call; it is now spent | Show the changed call; ask the operator again. |
| `MCP_CONFIRMATION_UNKNOWN` | The token is unknown, expired or used | Call again without a token for a new summary and yes. |
| `MCP_ADMIN_REQUIRED` | Reason under `FORBIDDEN`: a diagnostics tool needs an `ADMIN` key | Name the scope; stop. |
| `MCP_LOG_PATTERN_INVALID` | Reason under `BAD_USER_INPUT`: `search_service_logs` refused the pattern | Fix it: a case-insensitive regex, at most 256 characters. |
| `MCP_GRAPHQL_ERROR` | A GraphQL error that carried no code of its own | Report the message. |
| `MCP_SCHEMA_UNREADABLE` | The server could not read its own schema | Server fault: report. |
