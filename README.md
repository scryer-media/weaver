# Weaver

A unified Usenet binary downloader, repair, and extraction engine written in pure Rust.

Weaver replaces the traditional "download, repair, unpack" pipeline with an integrated job graph where all stages -- NNTP download, yEnc decode, article assembly, PAR2 verification/repair, and RAR extraction -- are orchestrated as a single coordinated workflow.

## Goals

- **Single binary** -- no external `unrar`, `par2`, or other tools required
- **Job graph scheduling** -- all stages are nodes in a dependency graph, not sequential phases
- **Archive-aware downloading** -- prioritize first RAR volumes and PAR2 metadata; evaluate extraction readiness in real time
- **Shared buffer pools** -- minimize copies and disk I/O across decode, verify, and extract stages
- **Structured failure handling** -- every error is classified (corruption, missing volume, encryption, filesystem), not scraped from subprocess output
- **Resumable state** -- crash recovery with deterministic replay from journaled checkpoints
- **GraphQL API with subscriptions** -- real-time per-job and per-file lifecycle events with machine-readable status

## Architecture

```
crates/
  weaver/           # Binary entry point
  weaver-core/      # Shared types, config, error types, buffer pools
  weaver-nntp/      # NNTP protocol client
  weaver-nzb/       # NZB XML parsing
  weaver-yenc/      # yEnc decoding
  weaver-assembly/  # Segment/file completeness tracking
  weaver-par2/      # PAR2 verification and repair
  weaver-rar/       # RAR decompression
  weaver-scheduler/ # Global job graph and resource balancing
  weaver-state/     # Persistent state store and journal
  weaver-api/       # GraphQL API with subscriptions

apps/
  weaver-web/       # Vite + React 19 + Tailwind v4 frontend
```

## Building

```bash
cargo build --workspace
cargo test --workspace
```

## License

GPLv3 -- see [LICENSE](LICENSE) for details.
