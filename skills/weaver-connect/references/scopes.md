# Key scopes

A Weaver API key has one scope, chosen when it is made. There are no user
accounts: the key acts as itself.

| Scope | Web label | May |
|---|---|---|
| `READ` | Read only | Read the queue, history, job events, torrents, server health, disk and system status |
| `CONTROL` | Control the queue | Read, plus add, pause, resume, reorder, recategorise and remove jobs and torrents |
| `ADMIN` | Full administration | Everything: settings, news servers, categories, RSS, schedules, service logs, the diagnostics bundle, backups, system actions, API keys |

Pick the lowest scope the task needs. Diagnosis needs `READ`; service logs,
news server settings and the diagnostics bundle need `ADMIN`.

A few fields answer any key: `version`, `isPaused`, `metrics`,
`downloadBlock`, `job(id)`.

## What a refusal looks like

A field the key's scope does not cover fails with `extensions.code`
`FORBIDDEN` and the message `read scope required`, `control scope required`
or `admin scope required`. Report that scope to the operator and stop. Do not
ask for a broader key unless the task needs it.

`GET /api/system/diagnostics` refuses a non-`ADMIN` key with HTTP 403, code
`FORBIDDEN`.

`CallerScope` (`callerScope`) names the key's scope; any key may ask.

## Secrets by scope

- Tracker URLs: whole (with any passkey) for `CONTROL` and `ADMIN`; scheme,
  host and port only for `READ`. Skills never select them.
- RSS link attributes on a job: `ADMIN` only.
- A `CONTROL` key may move a torrent only under the default save folder or a
  category's folder. Elsewhere it gets `FORBIDDEN`.

## Other credentials

Not for agents:

- A browser session (password login, or a trusted-network browser without
  login) acts with full access.
- A post-processing script's run token reads only its own job. Anything else
  gets `NOT_ALLOWED_FOR_SCRIPT_RUN`.

## Key management

An `ADMIN` key can create and revoke API keys through the API. Skills never
do this. Key management stays with the operator in the web UI.
