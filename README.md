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

Weaver publishes a first-party container image at `ghcr.io/scryer-media/weaver:latest`.

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

If you run the container as root, the launcher will re-own `/config` to `PUID` / `PGID` and then drop privileges before starting `weaver`. If you run with `--user=1000:1000`, make sure the bind mount is already owned by that uid/gid because the ownership repair path is skipped in non-root mode.

For hardened deployments, `weaver` supports `--read-only=true` as long as `/config` remains writable.

Maintainer note: this is a first-party `weaver` image. Any future LSIO adoption is a separate track and should not change the current Docker contract without an explicit migration plan.

## API

Weaver exposes a **GraphQL API** at `/graphql` with full query, mutation, and subscription support. The same API powers the web UI, so anything you can do in the interface is available programmatically.

WebSocket subscriptions provide real-time push updates for job progress, server status, and system events.

## License

GPLv3 — see [LICENSE](LICENSE) for details.
