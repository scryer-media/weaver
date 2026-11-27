# Codes to branch on

GraphQL error codes: `../weaver-connect/references/errors.md`. This file
covers the values inside answers. Times ending `EpochMs` are milliseconds
since the epoch (UTC); convert before quoting them.

## Job state and outcome

`state` (`QueueItemState`) is where a job is. `outcome` (`JobOutcome`) is how
it ended: `COMPLETE`, `PARTIAL`, `FAILED`, or null while it runs. Decide on
`outcome`, not `state`: a Partial job shows `state` `FAILED`.

| `state` | Meaning |
|---|---|
| `AWAITING_QUEUE_SCRIPTS` | Queue scripts run before it starts |
| `QUEUED` | Waiting to download, or queued for repair, unpack or post-processing (`postState` says which) |
| `DOWNLOADING` | Fetching articles |
| `FETCHING_REPAIR_DATA` | Payload in; recovery articles still being fetched |
| `FINALIZING_DOWNLOAD` | Download finishing |
| `CHECKING`, `VERIFYING` | Checking files against the recovery data |
| `REPAIRING` | Repairing from recovery data |
| `EXTRACTING` | Unpacking archives |
| `FINALIZING` | Moving output to the complete folder |
| `POST_PROCESSING` | Post-processing scripts run |
| `COMPLETED` | Delivered whole |
| `FAILED` | Failed, cancelled, or Partial (see `outcome`) |
| `PAUSED` | Held by the operator |
| `FETCHING_METADATA`, `CHECKING_FILES`, `SEEDING`, `STALLED` | Torrent jobs |

**Partial.** Only when at least one truly usable file was delivered: a
complete end file the operator can open, on disk, after every unpack. Never
usable on their own: an archive volume, a split part, a recovery file (PAR2,
PAR3, `.rev`), an SFV. Nothing usable is Failed. Examples:

- One `.mkv` in a five-volume RAR, one volume lost, nothing rebuilds it:
  Failed.
- Two files in the archive, one unpacked whole: Partial.
- No recovery, three plain files, one lost an article: Partial.

`health` is per mille: 1000 means no article lost. `failedBytes` > 0 means
articles were lost.

## Why a live job waits

`waitReason` (`QueueWaitReason`), after download:

| Value | Meaning |
|---|---|
| `REPAIR_CAPACITY`, `EXTRACTION_CAPACITY`, `POST_PROCESSING_CAPACITY` | Another job holds that step's slot |
| `DISK_SPACE` | Waits for room on a disk |
| `DISK_TOO_SMALL` | The disk cannot hold what is left even when empty |

`downloadWaitReason` (string), before or during download:

| Value | Meaning |
|---|---|
| `propagation_delay` | Waits for the propagation delay after posting |
| `nzb_slot` | Waits its turn for an active download slot |
| `disk_space` | Waits for room; `diskNeedBytes` vs `diskFreeBytes` |
| `disk_too_small` | The disk never holds what is left above the reserve; it never starts there and never fails for it |
| `login_refused:<host:port>[, ...]` | Its work waits on servers that refuse the login |
| `NNTP authentication blocked` | An article waits a second after a login refusal |
| `Restoring saved job` | Being restored after a restart |

`diskWrite`: `slow` true means its volume writes slowly; `draining` true
means writes wait for buffered data to reach disk.

## Attention codes (`attention.code`)

| Code | On | Meaning |
|---|---|---|
| `JOB_FAILED` | live, history | The job failed; `message` is the error |
| `ATTENTION` | live, history | An error on a job that did not fail |
| `CANCELLED` | history | Cancelled |
| `WAITING_FOR_VOLUMES` | live | Unpack waits for archive volumes still to arrive |
| `AWAITING_REPAIR` | live | Unpack waits for a repair |
| `UNHEALTHY_ARTICLES` | live | Articles lost (`failedBytes` > 0) and not finished |

## Global holds (`downloadBlock.kind`)

| Kind | Meaning | Look at |
|---|---|---|
| `NONE` | Nothing holds downloads | |
| `MANUAL_PAUSE` | The operator paused all | `resumeAtEpochMs` |
| `SPEED_TEST` | A speed test runs | |
| `SCHEDULED` | The schedule holds or limits | `scheduleHoldReason`, `speedLimitInForce` |
| `EGRESS_QUOTA` | A network route's data quota is spent | `egressName`, `usedBytes`, `limitBytes`, `windowEndsAtEpochMs` |
| `SERVER_QUOTA` | A news server's data quota is spent | same |

`pausedClasses` lists the job classes (`NZB`, `TORRENT`) paused on their own.

## News servers (`ServerHealth`)

- `state`: `healthy`, `degraded`, `cooling_down`, `disabled`.
- `activity`: `downloading`, `idle`, `preparing`, `over_limit`,
  `cooling_down`, `degraded`, `disabled`.
- `down`: out of use (a fault or a transport cooldown). A capacity hold
  (`over_limit`, `capacityPenaltyUntilEpochMs`) is not down.
- `failureCount`: refused, timed out, dropped or out-of-protocol requests.
  "No such article" is not a failure; it counts in `articlesNotFound`.
- `prematureDeaths`: connections that died young, recently.
- `tier`: `PRIMARY` (top priority level) or `BACKUP`.

No field names the last login or connection error: use
`downloadWaitReason` `login_refused:` and `ServiceLogs`.

## Job events (`JobEvent.kind`)

The ones that explain a failure:

| Kind | Meaning |
|---|---|
| `ARTICLE_NOT_FOUND`, `SEGMENT_FAILED_PERMANENT` | Articles missing on every server |
| `FILE_MISSING` | A whole file never arrived |
| `REPAIR_FAILED` | Recovery could not rebuild |
| `EXTRACTION_FAILED`, `EXTRACTION_MEMBER_FAILED` | Unpack failed, whole or one member |
| `ARCHIVE_DEMOTED` | An archive was set aside |
| `RECOVERY_SET_UNUSED` | A recovery set was not used |
| `SCRIPT_ERROR`, `SCRIPT_WARNING` | A post-processing script complained |
| `JOB_FAILED` | The final failure |

## Archive and recovery (`downloadSummary`)

- `layout`: `PLAIN`, `ARCHIVE`, `MIXED`.
- Node `givenUp` true with `reason` (e.g. `encrypted`, `compressed`): Weaver
  stopped trying that archive. `passwordNeeded` true: it needs a password.
- Recovery `kind`: `par2`, `par3`, `sfv`, `rar_recovery`, `rar_record`.
  `outcome`: `clean`, `repaired`, `impossible`, `waiting`, `unread`.
- `post.missingArticles`, `post.failedArticles`: article losses in the
  posting.

`historyItem.failedForMemory` true: the job failed because a decode window
was over the memory profile's ceiling.

## Torrents

`state` (`TorrentState`):

| State | Meaning |
|---|---|
| `QUEUED`, `CHECKING_RESUME_DATA` | Waiting, or starting from what was saved |
| `FETCHING_METADATA` | A magnet still fetching its metadata |
| `CHECKING_FILES` | Checking pieces on disk |
| `DOWNLOADING`, `STALLED` | Fetching; stalled means no data arrives |
| `SEEDING`, `STOPPED` | Seeding; stopped after its seeding goal |
| `PAUSED` | Held by the operator |
| `MOVING` | Its folder is being moved |
| `NEEDS_CERTIFICATE` | An SSL torrent with no certificate: connects to no peer, announces nowhere |
| `FILES_MISSING` | None of its files were there at load (e.g. a drive not mounted). A start, recheck or move looks again |

`stateReason` (`TorrentStateReason`): `DISK_SPACE`, `DISK_TOO_SMALL`,
`DISK_FULL` (a write found no room; fetches nothing, keeps seeding),
`GOAL_MET`, `HELD_MANUAL`, `HELD_SCHEDULED`, `HELD_SPEED_TEST`,
`FETCH_TRACKERS` (metadata from trackers only), `FETCH_DISCOVERY` (trackers,
DHT and local discovery), `FETCH_WAITING` (no peer named that may serve it).

Tracker `status`: `NOT_CONTACTED`, `WORKING`, `NOT_WORKING`,
`TRACKER_ERROR` (the tracker answered with an error; see `message`),
`UNREACHABLE`, `DISABLED`. `fails` counts failures in a row.

A private torrent uses its trackers only: no DHT, no peer exchange, no local
discovery.

Route `reason` (`TorrentRouteReason`, on a torrent as `routeReason`):

| Reason | Meaning |
|---|---|
| `OPENING` | The route's session is opening |
| `NOT_ASKED_TO_OPEN` | Nothing has asked it to open |
| `LEG_DOWN`, `LEG_MISSING`, `LEG_REVOKED`, `LEG_OPEN_FAILED` | The route's network leg is down, missing, revoked or would not open |
| `QUOTA_SILENCED` | The route's data quota is reached |
| `TORRENTS_DISABLED` | Torrents are off |
| `TORRENT_ROUTE_LEGS`, `TORRENT_ROUTE_DIRECT_FALLBACK`, `TORRENT_ROUTE_RUNGS` | The route's settings are broken; `detail` says how |
| `LISTEN_PORT_UNAVAILABLE`, `LISTEN_PORT_BIND_FAILED` | No listen port, or it could not be bound |
| `STATE_DIRECTORY_FAILED`, `SESSION_STATE_UNREADABLE`, `SESSION_BUILD_FAILED` | The session could not be set up |
| `UTP_UNAVAILABLE` | TCP is off and the route carries no uTP |
| `BLOCKLIST_FAILED` | The blocklist could not be loaded |
| `SESSION_LIMIT` | Every session is in use by other routes |

Route `mappings[].state` (`TorrentPortMappingState`): `MAPPED` (the gateway
forwards the port), `FAILED` (the last request to the gateway for it failed,
whether to map or to remove; its `error` says which), `REMOVED` (not mapped:
removed on close, or closed by the gateway).

`torrentPresence`: `available` false means the server was started with
`WEAVER_TORRENTS` turned off, and nothing torrent-related runs; `enabled`
means available and turned on in settings; `jobs` means a torrent job
exists; `running` means one is not paused or stopped; `windingDown` means
torrents are off and the running ones are let finish.

`unpacking.result`: `WHOLE`, `PART`, `FAILED`, `REFUSED`, with `reason`.
`unpacking.reasonCode` (`TorrentUnpackReason`), null for `WHOLE`: `REFUSED`,
`NOTHING_DELIVERED`, `NOT_PLACED`, `DELIVERED_IN_PART`, `UNWANTED_REMOVED`
(files made with an unwanted extension were removed), `UNWANTED_NOT_REMOVED`
(one could not be removed). Unpackings that ended before this code existed
read null.

`contentReady`: the download is complete and its unpacking, where one
applies, is over, whatever its result. False while a check runs or its
files are missing, and once it downloads again. `seedingGoalMet` and `seedingGoals`
(`RATIO`, `SEED_TIME`, `INACTIVE_TIME`, each with `met`, `current`,
`target`) say where the torrent stands against its seeding goals.
