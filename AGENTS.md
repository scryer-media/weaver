# AGENTS Instructions

## Architectural Boundaries

Use this order when making architectural decisions:

1. Confirm the requested change matches existing crate boundaries (see CLAUDE.md).
2. Data flows downward: `weaver` (binary) -> `weaver-api` / `weaver-scheduler` -> domain crates -> `weaver-core`.
3. Domain crates (`weaver-nntp`, `weaver-nzb`, `weaver-yenc`, `weaver-par2`, `weaver-rar`) must NOT depend on each other.
4. Only `weaver-scheduler` and `weaver-assembly` orchestrate cross-domain interactions.
5. `weaver-core` is the leaf dependency — it depends on nothing else in the workspace.
6. `weaver-state` is used by the scheduler and assembly layers, not by protocol/format crates directly.

## Dependency Rules

- Domain crates expose trait-based interfaces; concrete implementations live in the crate itself.
- The binary crate (`weaver`) wires everything together at startup.
- Async runtime is tokio. All I/O-bound crates use async.
- CPU-bound work (PAR2 math, RAR decompression, yEnc decode) uses `spawn_blocking` or rayon, never blocks the tokio runtime.

## Adding New Crates

1. Create `crates/<name>/Cargo.toml` and `crates/<name>/src/lib.rs`.
2. Add to workspace members in root `Cargo.toml`.
3. Add shared dependencies to `[workspace.dependencies]` if they will be used by 2+ crates.
4. Update the crate layout table in `CLAUDE.md`.

## Error Handling

- All crates use `thiserror` for error types.
- Errors are structured and classified, never stringly-typed.
- The API layer converts domain errors into user-facing responses.

## Testing

- Unit tests live in the same file as the code they test (`#[cfg(test)]` modules).
- Integration tests live in `tests/` at the workspace root.
- Use `cargo test --workspace` to run everything.

## Deploying / Running

When the user asks to run, restart, or deploy weaver, always use:

```bash
bash scripts/deploy.sh
```

This script:
1. Kills any existing weaver process (debug or release)
2. Cleans old log file
3. Builds a release binary
4. Starts with `RUST_LOG=info`, logging stderr to `/tmp/weaver.log`
5. Confirms it's running and shows initial log output

Logs are always at `/tmp/weaver.log` — use `tail -f /tmp/weaver.log` or `grep` to inspect.

Never start weaver manually with `cargo run` or direct binary invocation.
