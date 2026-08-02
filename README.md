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
  <a target="_blank" href="https://www.scryer.media/weaver/donate/"><img src="https://img.shields.io/badge/Sponsor-%E2%9D%A4%EF%B8%8F-db61a2?logo=githubsponsors&logoColor=white" alt="Sponsor NZBMan" /></a>
</p>

---

<p align="center">
  <a target="_blank" href="https://www.scryer.media/weaver/"><img src="docs/img/weaver-overview.webp" alt="weaver overview" width="800"/></a>
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

Encryption-at-rest setup is automatic: Weaver creates and reuses an encryption key in macOS Keychain, Windows Credential Manager (including WinGet portable installs), or a mode-`0600` key file in the Linux data directory. Existing `WEAVER_ENCRYPTION_KEY` overrides take precedence.

## Docker

Weaver publishes a first-party container image:

- `ghcr.io/scryer-media/weaver:latest` with both Linux binaries bundled per architecture and a CPU-aware launcher that picks the best one at startup

Published GHCR images are keyless-signed with Sigstore Cosign.

The Docker contract is intentionally small:

- Persist app data in `/config`
- Use `PUID` / `PGID` when you want the container to re-own `/config` and then drop privileges
- `TZ` defaults to `Etc/UTC`
- `UMASK` has no image default and is optional; it accepts standard octal values such as `022`
- `--user=1000:1000` and `--read-only=true` are both supported

Completed downloads default to `/config/complete` and in-progress work to `/config/intermediate`.
If you only mount `/config`, finished media accumulates inside the config volume. Mount a
separate downloads volume and point Weaver at it with `WEAVER_COMPLETE_DIR` and
`WEAVER_INTERMEDIATE_DIR`. Both are first-run seeds: they apply only while the corresponding
setting is still empty, and are ignored once Weaver has started or the value has been changed
in the UI.

When neither `WEAVER_ENCRYPTION_KEY` nor the Docker secret at `/run/secrets/weaver_encryption_key` is provided, Weaver creates `/config/encryption.key` with mode `0600`. Preserve that file with the rest of `/config`; existing external keys take precedence and are not copied into the volume.

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

If you run the container as root, the entrypoint will re-own `/config` to `PUID` / `PGID` and then drop privileges before starting `weaver`. If you run with `--user=1000:1000`, make sure the bind mount is already owned by that uid/gid because the ownership repair path is skipped in non-root mode.

For hardened deployments, `weaver` supports `--read-only=true` as long as `/config` remains writable.


## API

Weaver exposes a **GraphQL API** at `/graphql` with full query, mutation, and subscription support. The same API powers the web UI, so anything you can do in the interface is available programmatically.


## License

GPL-3.0-or-later with the UnRAR source-code restriction for RAR extraction. See [LICENSE](LICENSE) for details.
