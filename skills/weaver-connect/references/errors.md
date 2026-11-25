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
- Some failures (state, storage, a step that could not start) carry no
  `extensions` at all. `gql.sh` prints them as `NO_CODE`.

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
| `SEEDING_GOAL_UNMET` | A removal was refused because the seeding goal is not met | Report; the operator decides. |

## Server faults

| `extensions.code` | Meaning | Do |
|---|---|---|
| `INTERNAL` | Server fault | Report the message. Do not retry in a loop. |
| `NO_CODE` (no `extensions`) | Server fault without a code | Same. |

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

Other answers:

- **HTTP 421 `request Host is not allowed`** (plain text): the host name in
  `WEAVER_URL` is not one this Weaver accepts. IP addresses and `localhost`
  are always accepted. The operator uses one of those, or adds the name to
  `WEAVER_HTTP_ALLOWED_HOSTS`.
- **HTTP 400 `invalid Host header`** (plain text): the request's host could
  not be read. Check `WEAVER_URL`.
- **HTML instead of JSON**: `WEAVER_URL` points at a proxy page or the wrong
  base path.
