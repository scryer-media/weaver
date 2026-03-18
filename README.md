<p align="center">
  <img src="WeaverDark.png" alt="Weaver" width="200" />
</p>

<h1 align="center">Weaver</h1>

<p align="center">
  A modern, all-in-one Usenet downloader built in Rust.<br/>
  Download, repair, and extract — in a single binary.
</p>

<p align="center">
  <a href="https://github.com/scryer-media/weaver/releases"><img src="https://img.shields.io/github/v/release/scryer-media/weaver" alt="Release" /></a>
  <a href="https://github.com/scryer-media/weaver/blob/main/LICENSE"><img src="https://img.shields.io/github/license/scryer-media/weaver" alt="License" /></a>
  <a href="https://ghcr.io/scryer-media/weaver"><img src="https://img.shields.io/badge/container-ghcr.io-blue" alt="Container" /></a>
</p>

---

## What is Weaver?

Weaver is a Usenet binary downloader that handles the entire pipeline — downloading articles, decoding, PAR2 verification and repair, and RAR extraction — all within a single self-contained binary. No need to install `unrar`, `par2repair`, or any other external tools.

Instead of the traditional sequential approach (download everything, then repair, then extract), Weaver runs all stages concurrently. Extraction begins as soon as the first archive volume finishes downloading, so files appear on disk while the rest of the job is still in progress.

### Key Features

- **Single binary** — everything built in, no external dependencies to install
- **Incremental extraction** — starts extracting files while still downloading
- **Modern web UI** — responsive interface with real-time progress updates
- **Multi-server support** — connect to multiple Usenet providers with priority and failover
- **Built-in PAR2 repair** — automatic verification and repair without external tools
- **Built-in RAR extraction** — handles RAR4 and RAR5 archives natively, including solid and encrypted archives
- **Monthly quotas** — set monthly, weekly, or daily data limits with configurable billing windows
- **Prometheus metrics** — monitor performance with your existing observability stack

## Install

### Homebrew (macOS / Linux)

```bash
brew tap scryer-media/weaver
brew install weaver-usenet
```

To run as a background service (auto-starts on login, restarts on crash):

```bash
brew services start weaver-usenet
brew services stop weaver-usenet
brew services restart weaver-usenet
```

### Binary

Download the latest release from the [releases page](https://github.com/scryer-media/weaver/releases). Available for Linux (x86_64, arm64) and macOS (Apple Silicon, Intel).

### Docker

See below.

## Quick Start with Docker

The fastest way to run Weaver is with Docker Compose. Create a `docker-compose.yml`:

```yaml
services:
  weaver:
    image: ghcr.io/scryer-media/weaver:latest
    ports:
      - "9090:9090"
    volumes:
      - weaver-config:/data
      - /path/to/downloads:/downloads

volumes:
  weaver-config:
```

Then:

```bash
docker compose up -d
```

Open **http://localhost:9090** and you're ready to go.

### Configuration

Weaver stores its configuration and database in `/data` inside the container. Mount a volume or host directory there to persist settings across restarts.

Set your download directory in **Settings > General** once the UI is running, or mount it as shown above.

### Reverse Proxy (Subpath)

To host Weaver at a subpath like `https://example.com/weaver/`:

```yaml
services:
  weaver:
    image: ghcr.io/scryer-media/weaver:latest
    command: ["--config", "/data", "serve", "--port", "9090", "--base-url", "/weaver"]
    ports:
      - "9090:9090"
    volumes:
      - weaver-config:/data
      - /path/to/downloads:/downloads
```

Then configure your reverse proxy (nginx, Traefik, Caddy, etc.) to forward `/weaver/` to Weaver's port.

### Environment Variables

| Variable | Description |
|----------|-------------|
| `RUST_LOG` | Logging level (`error`, `warn`, `info`, `debug`, `trace`). Default: `info` |

## Building from Source

Requires **Rust 1.94+** and **Node.js 25+**.

```bash
# Build the frontend
cd apps/weaver-web && npm ci && npm run build && cd ../..

# Build the binary
cargo build --release

# Run
./target/release/weaver --config /path/to/data serve --port 9090
```

## How It Works

Weaver models each download as a **job graph** — a directed acyclic graph where each stage (download, decode, verify, repair, extract) is a node with explicit dependencies. This replaces the rigid sequential pipeline of traditional downloaders with a flexible system that can:

- Begin extraction as soon as volume 1 is ready, without waiting for the entire download
- Prioritize downloading first archive volumes and PAR2 metadata
- Start PAR2 verification while later segments are still arriving
- Dynamically adjust concurrency based on system load

The scheduler continuously evaluates what work can proceed, balancing disk I/O, network throughput, CPU usage, and memory across all active jobs.

## Comparison

| | Weaver | NZBGet | SABnzbd |
|---|---|---|---|
| Language | Rust | C++ | Python |
| Runtime | Native binary | Native binary | Python interpreter + dependencies |
| Idle memory | ~24 MB | ~8 MB | ~68 MB |
| External tools required | None | `unrar`, `par2` | `unrar`, `par2` |
| PAR2 repair | Built-in | External `par2` | External `par2` |
| RAR extraction | Built-in (RAR4 + RAR5) | External `unrar` | External `unrar` |
| Incremental extraction | Yes | Yes | No |
| Real-time updates | WebSocket | Polling | Polling |
| Monthly quotas | Yes | No | Yes |
| Prometheus metrics | Yes | No | No |
| Obfuscation handling | Planned | Yes | Yes |

> **Note:** NZBGet has been discontinued by its original author. [NZBGet v24+](https://github.com/nzbgetcom/nzbget) is maintained by the community. SABnzbd is actively maintained.

## API

Weaver exposes a **GraphQL API** at `/graphql` with full query, mutation, and subscription support. The same API powers the web UI, so anything you can do in the interface is available programmatically.

WebSocket subscriptions provide real-time push updates for job progress, server status, and system events.

## License

GPLv3 — see [LICENSE](LICENSE) for details.
