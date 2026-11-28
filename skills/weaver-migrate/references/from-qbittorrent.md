# From qBittorrent

## Before anything

Torrents in Weaver are available unless the host environment variable
`WEAVER_TORRENTS` turns them off (any value but `1` or `true`), and run once
turned on in Weaver's settings. Turned off by the variable, Weaver has no
qBittorrent API at all. Both are the operator's.

## What carries over

| Item | How |
|---|---|
| Categories | `weaver-configure` (`references/categories.md` there) |
| "Run external program" commands | this skill, as `QUEUE` jobs |
| Torrents, resume data, seeding state | not imported: finish them in qBittorrent or re-add them in Weaver |
| Everything else (trackers, speed, RSS) | not covered here; the Weaver web UI |

Weaver has no import path for qBittorrent: not `qBittorrent.conf`, not its
torrents or resume data.

## Scripts

Weaver has no qBittorrent script adapter. qBittorrent runs a command line
with `%X` placeholders; Weaver runs a script file from its scripts directory
with no arguments and hands the values as variables. Every qBittorrent
command therefore needs a small wrapper script: always a changed copy.

| qBittorrent hook | Weaver job |
|---|---|
| run on torrent added | `QUEUE`, `queueEvent` `TORRENT_ADDED` |
| run on torrent finished | `QUEUE`, `queueEvent` `TORRENT_DOWNLOADED` |

A torrent never waits on its scripts. The program the command calls must
exist on the Weaver host, on the reduced `PATH` (`references/scripts.md`).

## Placeholders

Get the command line from the operator (copied from qBittorrent's settings
into a file); then `script-needs.sh --command <file>`.

| Placeholder | Weaver variable |
|---|---|
| `%N` torrent name | `NZBNA_NZBNAME` (also `WEAVER_JOB_NAME`) |
| `%L` category | `NZBNA_CATEGORY` (also `WEAVER_CATEGORY`) |
| `%G` tags | `NZBNA_TAGS`, comma separated |
| `%F` content path | `NZBNA_DIRECTORY`: the file of a single-file torrent named as the torrent, else its folder |
| `%D` save path | `WEAVER_DIRECTORY`, also the working directory (Weaver's complete folder before the torrent is placed) |
| `%I` info hash v1 | `WEAVER_INFOHASH_V1`, empty when none |
| `%J` info hash v2 | `WEAVER_INFOHASH_V2`, empty when none |
| `%T` current tracker | `WEAVER_TRACKER`: the tracker last announced to, else the first. Can hold a passkey: the wrapper must never print it |
| `%R` root path | no equivalent |
| `%C` number of files | no equivalent |
| `%Z` torrent size | no equivalent |
| `%K` torrent ID | no equivalent; `WEAVER_INFOHASH_V1` / `V2` identify the torrent |

"No equivalent": pass an empty string and tell the operator, or keep that
command in qBittorrent.

## Example wrapper

qBittorrent command:

```
/usr/local/bin/notify.sh "%N" "%L" "%F" "%I"
```

Wrapper `notify-wrapper.sh` for the scripts directory, executable:

```sh
#!/bin/sh
exec /usr/local/bin/notify.sh "$NZBNA_NZBNAME" "$NZBNA_CATEGORY" \
  "$NZBNA_DIRECTORY" "$WEAVER_INFOHASH_V1"
```

Then `CreateScriptJob` with `"trigger": "QUEUE"`,
`"queueEvent": "TORRENT_DOWNLOADED"`, `"enabled": false`.

## Old config file

`qBittorrent.conf` holds the Web UI password hash and other secrets. Never
print it. The command line is copied by the operator, not read from the file.
A secret the program needs moves file to file with `add-script-secret.sh`
and becomes an input (`WEAVER_INPUT_<NAME>` in the wrapper).

## Pointing an Arr app at Weaver

Keep the Arr app's **qBittorrent** download client type; change it to point at
Weaver:

| Field | Value |
|---|---|
| Host, port, TLS | from `WEAVER_URL` |
| URL base | Weaver's base path from `WEAVER_URL`, if any (the API sits at `<base>/api/v2/`) |
| Username | anything |
| Password | a Weaver API key with **Control the queue** scope, made by the operator |
| Category | a Weaver category |

A **Control the queue** key may create a category by name through this API;
setting its folder or seeding goals that way needs **Full administration**.
Create categories in Weaver first (`weaver-configure`).

Keep both clients until the operator cuts over. The cut-over is the
operator's.
