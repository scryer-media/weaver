## Workflow Orchestration

### 1. Plan Mode Default
- Enter plan mode for ANY non-trivial task (3+ steps or architectural decisions)
- If something goes sideways, STOP and re-plan immediately
- Use plan mode for verification steps, not just building
- Write detailed specs upfront to reduce ambiguity

### 2. Subagent Strategy
- Use subagents liberally to keep main context window clean
- Offload research, exploration, and parallel analysis to subagents
- One task per subagent for focused execution

### 3. Self-Improvement Loop
- After ANY correction from the user: update `tasks/lessons.md` with the pattern
- Write rules for yourself that prevent the same mistake
- Review lessons at session start

### 4. Verification Before Done
- Never mark a task complete without proving it works
- Run tests, check logs, demonstrate correctness
- Ask yourself: "Would a staff engineer approve this?"

### 5. Autonomous Bug Fixing
- When given a bug report: just fix it
- Point at logs, errors, failing tests, then resolve them
- Zero context switching required from the user

## Task Management

1. **Plan First**: Write plan to `tasks/todo.md` with checkable items
2. **Track Progress**: Mark items complete as you go
3. **Capture Lessons**: Update `tasks/lessons.md` after corrections

## Core Principles

- **Simplicity First**: Make every change as simple as possible
- **No Laziness**: Find root causes. No temporary fixes. Senior developer standards.
- **Minimal Impact**: Changes should only touch what's necessary

## Architecture

Weaver is a unified Usenet binary downloader, repair, and extraction engine written in pure Rust. It replaces the traditional "download, repair, unpack" pipeline with an integrated job graph.

### Crate Layout (`crates/`)

| Crate | Purpose |
|-------|---------|
| `weaver` | Binary entry point — CLI, server startup, config loading |
| `weaver-core` | Shared types, error types, config, buffer pool primitives |
| `weaver-nntp` | NNTP protocol client — async sockets, connection pools per server |
| `weaver-nzb` | NZB XML parsing into typed article/file/group structures |
| `weaver-yenc` | yEnc decoding into pooled buffers, streaming CRC |
| `weaver-assembly` | Segment/file completeness tracking, article assembly |
| `weaver-par2` | PAR2 verification and repair — incremental verify, repairability estimation |
| `weaver-rar` | RAR decompression — header parse, volume topology, member extraction |
| `weaver-scheduler` | Global job graph — artifact dependency tracking, resource balancing |
| `weaver-state` | Persistent state store — resumable journal, crash recovery |
| `weaver-api` | GraphQL API with subscriptions — typed event model, per-job lifecycle |

### Frontend (`apps/weaver-web/`)

Vite + React 19 + React Router 7 + Tailwind v4. Uses `urql` with `graphql-ws` for GraphQL queries, mutations, and subscriptions. Dev server proxies `/graphql` (including WebSocket upgrades) to the Rust backend.

### Key Design Decisions

- **Single binary**: No external `unrar`, `par2`, or other tools required
- **Job graph, not pipeline**: All stages (download, decode, verify, repair, extract) are nodes in a dependency graph, not sequential phases
- **Buffer pooling**: Slab/arena-backed buffers shared across decode, verify, and extract stages to minimize copies and disk I/O
- **Archive-aware scheduling**: Downloader prioritizes first RAR volumes and PAR2 metadata; extraction eligibility evaluated in real time
- **Structured errors**: Every failure is classified (corruption, missing volume, encryption, filesystem) — no subprocess stdout scraping
- **Adaptive runtime**: Probes system capabilities (disk IOPS, CPU cores, SIMD support, available RAM) at startup and continuously monitors actual performance to dynamically tune concurrency, buffer sizes, and scheduling priorities

## Build & Test

```bash
# Rust
cargo build --workspace --locked
cargo test --workspace --locked

# Frontend
cd apps/weaver-web && npm ci && npm run build
```

## Commit Rules

- Never add a Co-Authored-By line or otherwise put Claude's name on commits.
- Always sign tags.

## Related Projects

- **Scryer**: Media management application (sister project)
