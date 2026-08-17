<h1 align="center">Weaver</h1>

<p align="center">
  A modern, all-in-one Usenet downloader built in Rust.<br/>
  Download, repair, and extract — in a single binary.
</p>

<p align="center">
  <a href="https://github.com/scryer-media/weaver/releases"><img src="https://img.shields.io/github/v/release/scryer-media/weaver" alt="Release" /></a>
  <a href="https://ghcr.io/scryer-media/weaver"><img src="https://img.shields.io/badge/container-ghcr.io-blue" alt="Container" /></a>
  <a href="https://securityscorecards.dev/viewer/?uri=github.com/scryer-media/weaver"><img src="https://api.scorecard.dev/projects/github.com/scryer-media/weaver/badge" alt="OpenSSF Scorecard" /></a>
</p>

<p align="center">
  <a href="https://www.scryer.media/weaver/donate/"><img src="https://img.shields.io/badge/Donate-%E2%9D%A4%EF%B8%8F-db61a2?logo=githubsponsors&logoColor=white" alt="Donate to Weaver" /></a>
  <a href="https://www.reddit.com/r/scryer_media/"><img src="https://img.shields.io/badge/Reddit-r%2Fscryer__media-FF4500?logo=reddit&logoColor=white" alt="Weaver on Reddit" /></a>
  <a href="https://discord.gg/SQmtZTanqm"><img src="https://img.shields.io/badge/Discord-Join%20the%20community-5865F2?logo=discord&logoColor=white" alt="Weaver on Discord" /></a>
</p>

<p align="center">
  <a href="https://www.scryer.media/weaver/"><img src="docs/img/weaver-overview.webp" alt="Weaver web interface" width="800" /></a>
</p>

## What is Weaver?

Weaver is a Usenet binary downloader that handles the entire pipeline — downloading articles, decoding, PAR2 verification and repair, and extraction (RAR, 7z, etc) — all within a single self-contained binary. No need to install `unrar`, `par2repair`, or any other external tools.

Built on [rarpar](https://github.com/scryer-media/rarpar), the world's fastest Rust libraries for RAR extraction and PAR2 repair.

Instead of the traditional sequential approach (download everything, then repair, then extract), Weaver can run downloading and extraction concurrently*. Extraction begins as soon as the first archive volume finishes downloading, so files appear on disk while the rest of the job is still in progress.

### Key Features

- **Single binary** — no external `unrar`, `par2`, or other tools required
- **Ultra fast** — weaver is native compiled machine code
- **Incremental extraction** — starts extracting files while still downloading
- **Real-time updates** — websocket push for job progress and system events, less chatty than other tools
- **Download quotas** — configurable daily, weekly, or monthly data limits to work with ISP bandwidth caps
- **Observable** — Built in metrics and timeline views help visualize what happens during download with support for prometheus 

## Install

All unstallation instructions can be found on the [Weaver docs website](https://www.scryer.media/weaver/docs/installation/)

## Docker

### Unraid

[Weaver on Unraid community](https://ca.unraid.net/apps/weaver-15o1lnd0hwah5h)

### Self hosted

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
      # Docker and reverse-proxy deployments must explicitly bind beyond loopback.
      - WEAVER_HTTP_BIND_ADDRESS=0.0.0.0
      - WEAVER_HTTP_ALLOWED_HOSTS=weaver,weaver.example.me # permit the Compose service name and any reverse proxy names
    volumes:
      - /path/to/weaver/config:/config # this is the critical volume with all your config data and encryption key
    ports:
      - 9090:9090
    restart: unless-stopped
```

Weaver binds to `127.0.0.1` by default. Set `WEAVER_HTTP_BIND_ADDRESS=0.0.0.0`
for Docker, LAN, or reverse-proxy deployments, or use a specific non-loopback
address. Binding and browser trust are separate: binding to a LAN interface
does not trust its clients. To deliberately allow loginless browser access,
configure `WEAVER_TRUSTED_CIDRS` with explicit client networks, for example
`127.0.0.0/8,::1/128` for local access only. Matching clients receive full
administrative browser access; agents and integrations must use persistent,
scoped API keys instead.

For an unattended first start, configure `WEAVER_BOOTSTRAP_LOGIN_USERNAME` and
exactly one of `WEAVER_BOOTSTRAP_LOGIN_PASSWORD` or
`WEAVER_BOOTSTRAP_LOGIN_PASSWORD_FILE`. Bootstrap credentials are used only
when no login is already stored; they never overwrite an existing login.

## API

Weaver exposes a **GraphQL API** at `/graphql` with full query, mutation, and subscription support. The same API powers the web UI, so anything you can do in the interface is available programmatically.

`/metrics` remains authenticated by default. Give Prometheus a persistent
Read-scoped Weaver API key using its standard bearer authorization support:

```yaml
scrape_configs:
  - job_name: weaver
    static_configs:
      - targets: ["weaver:9090"]
    authorization:
      type: Bearer
      credentials_file: /run/secrets/weaver-metrics-api-key
```

This sends `Authorization: Bearer <key>` without reusing browser credentials.

## License

GPL-3.0-or-later with the UnRAR source-code restriction for RAR extraction. See [LICENSE](LICENSE) for details.
