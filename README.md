<p align="center">
  <img src="docs/img/weaver-hero.webp" alt="Weaver" width="200" />
</p>

<h1 align="center">Weaver</h1>

<p align="center">
  A modern, all-in-one Usenet downloader built in Rust.<br/>
  Download, repair, and extract — in a single binary.
</p>

<p align="center">
  <a href="https://github.com/scryer-media/weaver/releases"><img src="https://img.shields.io/github/v/release/scryer-media/weaver" alt="Release" /></a>
  <a href="https://ghcr.io/scryer-media/weaver"><img src="https://img.shields.io/badge/container-ghcr.io-blue" alt="Container" /></a>
  <a href="https://github.com/sponsors/NZBMan"><img src="https://img.shields.io/badge/Sponsor-%E2%9D%A4%EF%B8%8F-db61a2?logo=githubsponsors&logoColor=white" alt="Sponsor NZBMan" /></a>
</p>

---

<p align="center">
  <img src="docs/img/weaver-overview.webp" alt="weaver overview" width="800"/>
</p>

## What is Weaver?

Weaver is a Usenet binary downloader that handles the entire pipeline — downloading articles, decoding, PAR2 verification and repair, and extraction (RAR, 7z, etc) — all within a single self-contained binary. No need to install `unrar`, `par2repair`, or any other external tools.

Instead of the traditional sequential approach (download everything, then repair, then extract), Weaver can run downloading and extraction concurrently*. Extraction begins as soon as the first archive volume finishes downloading, so files appear on disk while the rest of the job is still in progress.

### Key Features

- **Single binary** — no external `unrar`, `par2`, or other tools required
- **Ultra fast** — weaver is native compiled machine code and can run faster than NZBGet for certain files, due to running all operations in one process 
- **Incremental extraction** — starts extracting files while still downloading
- **Real-time updates** — websocket push for job progress and system events, less chatty than other tools
- **Monthly download quotas** — configurable monthly data limits to work with ISP bandwidth caps
- **Observable** — Built in metrics and timeline views help visualize what happens during download with support for prometheus 

## Install

Installation instructions can be found on the [Weaver docs website](https://www.scryer.media/weaver/docs/installation/)

## Docker

Weaver publishes a first-party container image:

- `ghcr.io/scryer-media/weaver:latest` with both Linux binaries bundled per architecture and a CPU-aware launcher that picks the best one at startup

Published GHCR images are keyless-signed with Sigstore Cosign.

The Docker contract is intentionally small:

- Persist app data in `/config`
- Use `PUID` / `PGID` when you want the container to re-own `/config` and then drop privileges
- `TZ` defaults to `Etc/UTC`
- `UMASK` is optional and accepts standard octal values such as `022`
- `--user=1000:1000` and `--read-only=true` are both supported

### docker-compose

```yaml
services:
  weaver:
    image: ghcr.io/scryer-media/weaver:latest
    container_name: weaver
    environment:
      - PUID=1000
      - PGID=1000
      - TZ=Etc/UTC
      - UMASK=022 # optional
    volumes:
      - /path/to/weaver/config:/config
    ports:
      - 9090:9090
    restart: unless-stopped
```

### docker run

```bash
docker run -d \
  --name=weaver \
  -e PUID=1000 \
  -e PGID=1000 \
  -e TZ=Etc/UTC \
  -e UMASK=022 \
  -p 9090:9090 \
  -v /path/to/weaver/config:/config \
  --restart unless-stopped \
  ghcr.io/scryer-media/weaver:latest
```

If you run the container as root, the entrypoint will re-own `/config` to `PUID` / `PGID` and then drop privileges before starting `weaver`. If you run with `--user=1000:1000`, make sure the bind mount is already owned by that uid/gid because the ownership repair path is skipped in non-root mode.

For hardened deployments, `weaver` supports `--read-only=true` as long as `/config` remains writable.

Maintainer note: this is a first-party `weaver` image. Any future LSIO adoption is a separate track and should not change the current Docker contract without an explicit migration plan.

## API

Weaver exposes a **GraphQL API** at `/graphql` with full query, mutation, and subscription support. The same API powers the web UI, so anything you can do in the interface is available programmatically.

WebSocket subscriptions provide real-time push updates for job progress, server status, and system events.

Weaver also exposes an NZBGet-compatible facade on two transports: JSON-RPC at `/jsonrpc` (Sonarr, Radarr, Prowlarr) and XML-RPC at `/xmlrpc` (nzb360 and other mobile clients). Use a Weaver API key with `control` scope as the NZBGet password; the username is ignored. A read-scoped key is enough for Test/version/config calls but cannot grab, control, or remove downloads. Fresh Radarr setups use NZBGet's default password (`tegbzn6789`), so replace it with the Weaver API key.

The facade covers the surfaces those clients actually call: `version`, `status`, `listgroups`, `listfiles`, `history` (compound `SUCCESS/ALL`-style statuses plus per-stage `DownloadTimeSec`/`RepairTimeSec`/`UnpackTimeSec` durations), `config`/`loadconfig` (categories and `FeedN` entries from weaver's RSS feeds), `viewfeed`/`previewfeed`/`fetchfeeds` (bridged to weaver RSS), `postqueue`, `servervolumes` (per-server quota-window usage), `log`/`loadlog`/`writelog`, `append` (v13 9-arg), `appendurl` (legacy 5-arg), `rate`, `pausedownload`/`resumedownload`, `pausescan`/`resumescan` (watch-folder scanning), `pausepost`/`resumepost` (accepted; weaver has no separate post-processing pause), and `scheduleresume` (nzb360's "pause for X minutes" — the timer survives restarts). `editqueue` accepts both the legacy 4-arg `(Command, Offset, Param, IDs)` and v13 3-arg shapes and maps `GroupPause`/`GroupResume`, `GroupDelete` (and park/dupe variants), `GroupFinalDelete`, `GroupMoveTop`/`GroupMoveBottom`/`GroupMoveOffset` (durable manual queue order; reorders within a priority band — HIGH/NORMAL/LOW still dominates dispatch), `GroupApplyCategory`/`GroupSetCategory`, `GroupSetPriority` (numbers or FORCE/HIGH/NORMAL/LOW names; FORCE collapses to HIGH), `GroupSetParameter` (including `*Unpack:Password=…`, which maps to weaver's durable password override and never appears in visible attributes), dupe-key/score/mode setters, `HistoryDelete`/`HistoryFinalDelete`, `HistoryReturn`/`HistoryRedownload`, `HistoryProcess`, and `HistoryMarkGood`. Commands with no weaver equivalent (anchor-relative moves, sorting, per-file operations, renames) return `false` rather than pretending to succeed. `status` reports real free disk space, paused flags, quota state, scheduled-resume time, article-cache bytes, and active connection count; `listgroups` carries real `FileCount`/`RemainingFileCount`/`RemainingParCount`. RPC bodies up to 1.5× the NZB upload limit are accepted on both endpoints, so large base64 NZBs are not rejected by the transport.

## License

GPL-3.0-or-later with the UnRAR source-code restriction for RAR extraction
and recovery code. See [LICENSE](LICENSE) for details.
