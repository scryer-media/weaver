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

All installation instructions can be found on the [Weaver docs website](https://www.scryer.media/weaver/docs/installation/)

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
      - WEAVER_HTTP_ALLOWED_HOSTS=weaver,weaver.example.me # permit the Compose service name and any reverse proxy names
      # First-run setup: pick one of the two blocks below.
      # - WEAVER_TRUSTED_CIDRS=192.168.0.0/16 # browsers in these networks get full access without a login
      # - WEAVER_BOOTSTRAP_LOGIN_USERNAME=admin # creates the login on first start, then sign in normally
      # - WEAVER_BOOTSTRAP_LOGIN_PASSWORD_FILE=/run/secrets/weaver-login # the password, read from a mounted file
    volumes:
      - /path/to/weaver/config:/config # this is the critical volume with all your config data and encryption key
    ports:
      - 9090:9090
    restart: unless-stopped
```

First-run setup for a container happens through the variables commented out
above, because no browser reaches a container as "the machine itself" — a
native install runs the setup wizard in the browser instead. Until one of them
is set, the container's web page explains this rather than showing a wizard it
could not accept.

Weaver binds to `127.0.0.1` by default, so a native install is never exposed by
accident; the container image ships `WEAVER_HTTP_BIND_ADDRESS=0.0.0.0`, since a
container's exposure is decided by the port you publish. Set that variable
yourself for a native LAN or reverse-proxy deployment, or use a specific
non-loopback address. The address is also editable in **Settings → Security**,
which is the normal route for a desktop or service install; under Docker that
setting only moves the address within the container's own network namespace,
where the ports you publish with `-p` — or `--network host` — are what actually
decide exposure, so pin `WEAVER_HTTP_BIND_ADDRESS` at the deployment level
instead. The variable always wins over the stored setting.

Binding and browser trust are separate: binding to a LAN interface
does not trust its clients. To deliberately allow loginless browser access,
configure `WEAVER_TRUSTED_CIDRS` with explicit client networks, for example
`127.0.0.0/8,::1/128` for local access only. Matching clients receive full
administrative browser access; agents and integrations must use persistent,
scoped API keys instead.

For an unattended first start, configure `WEAVER_BOOTSTRAP_LOGIN_USERNAME` and
exactly one of `WEAVER_BOOTSTRAP_LOGIN_PASSWORD` or
`WEAVER_BOOTSTRAP_LOGIN_PASSWORD_FILE`. Bootstrap credentials are used only
when no login is already stored; they never overwrite an existing login.

RSS feeds may use local, private, link-local, or container-network addresses
by default. Feed Basic Auth credentials are sent only to requests whose scheme,
host, and effective port exactly match the configured feed URL; redirected or
item requests to another origin never receive them. Set
`WEAVER_RSS_ALLOW_PRIVATE_NETWORK=false` to limit RSS fetching to public
egress. Decompressed RSS feed bodies are capped at 16 MiB; the separate NZB
response limit remains configurable with `WEAVER_NZB_DECOMPRESSED_LIMIT_BYTES`.

## API

Weaver exposes a **GraphQL API** at `/graphql` with full query, mutation, and subscription support. The same API powers the web UI, so anything you can do in the interface is available programmatically.

## Metrics & dashboards

Weaver serves Prometheus metrics at `/metrics` on the same port as the web UI. They cover download throughput, pipeline backpressure, per-server health and quotas, and post-processing — enough to answer "why is this slow right now?" without opening the UI.

`/metrics` is authenticated by default; to disable, set `WEAVER_METRICS_AUTH_REQUIRED=0`.
Otherwise give Prometheus a persistent Read-scoped Weaver API key using its standard bearer authorization support:

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

See [docs/metrics.md](docs/metrics.md) for the full metric catalogue, label conventions, and useful PromQL. Ready-made [Grafana dashboard](contrib/grafana/weaver-overview.json) and [Prometheus alert rules](contrib/prometheus/weaver-alerts.yml) live under `contrib/`.

## License

Weaver-authored source code is licensed under GPL-3.0-or-later. Official builds include `unrar-rs` for RAR support; that component remains subject to the UnRAR restriction; Weaver's GPL code combines with it under a GPLv3 section 7 linking permission. See [LICENSE](LICENSE) and [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md) for details.
