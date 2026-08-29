# Weaver end-to-end harness

The integration and release-gate suite for [Weaver](../README.md). It posts
real yEnc articles to fake NNTP servers with Nyuu, drives Weaver through its
GraphQL API and its browser UI, and asserts on the product's own state —
completed files, metrics, database rows, what the UI shows.

It is a self-contained Go module (`github.com/scryer-media/weaver/e2e`) with
its own Compose stack, fixture corpus and Playwright project, so a release gate
runs from a clean checkout of this repository plus the published fixture
corpus. It depends on no other checkout: the fake NNTP server is pulled as the
public Go module [`e2e-nntp`](https://github.com/scryer-media/e2e-nntp), and
Weaver itself is built from the enclosing repository.

The harness is an acceptance oracle. When it finds a bug, fix the product; the
suite is the criterion, not the workaround.

## Contents

- [Requirements](#requirements)
- [Quickstart](#quickstart)
- [Layout](#layout)
- [The fixture corpus](#the-fixture-corpus)
- [Commands](#commands)
- [What `full` runs](#what-full-runs)
- [The product-behavior release gate](#the-product-behavior-release-gate)
- [Configuration](#configuration)
- [Benchmarking and PGO](#benchmarking-and-pgo)
- [Notes](#notes)

## Requirements

| Requirement | Used for |
| --- | --- |
| Go 1.26+ | the harness, the corpus tool and the fixture generator (`go run ./cmd/…`) |
| [Task](https://taskfile.dev) | the `task` targets below; every one maps to a `weaver-e2e` subcommand, which is the real interface |
| Docker with Compose v2 | the stack: two NNTP servers, Nyuu, Toxiproxy, SABnzbd, NZBGet, PostgreSQL, Playwright, a fixed RSS fixture, and Weaver itself |
| Rust (the toolchain in `../rust-toolchain.toml`) | building the Weaver binary for managed-local phases and the local Weaver image; the harness asks `rustup which cargo` so a stray older toolchain on `PATH` cannot be picked up |
| Node.js with npm | only to develop the Playwright project locally (`playwright-weaver/`); the tests themselves run in a container. Use npm and the committed `package-lock.json`, never pnpm |
| Disk | the full fixture corpus is ~6.2 GiB; the `functional` profile alone ~5.9 GiB |

Everything runs from this directory.

## Quickstart

```bash
cd e2e

# 1. Build the CLI (writes ./.bin/weaver-e2e).
task build

# 2. Optional: make the fixture payloads present up front. Every suite does this
#    itself before seeding — reuse what is on disk, fetch the rest from the
#    published corpus, generate only what is still missing.
task hydrate PROFILE=functional          # or: go run ./cmd/corpus ensure --profile functional

# 3. The canonical release command: every pipeline phase plus the product-behavior gate.
task full

# Or a slice of it:
task test -- rar5-multivolume            # one scenario through Weaver
task release-gate -- rate-limits         # one product-behavior flow (SQLite and PostgreSQL)
task release-gate -- datastore-matrix    # the explicit SQLite/PostgreSQL expansion

# 4. Tear down everything the harness started.
task stack:down -- -v
```

`task --list` shows every target; `weaver-e2e` with no arguments prints its
subcommands.

## Layout

| Path | What it is |
| --- | --- |
| `cmd/weaver-e2e` | The CLI |
| `internal/weaver` | The harness: flows, seeding, assertions, the release gate |
| `internal/composeutil` | Compose network and subnet helpers |
| `docker-compose.yml` | The 10-service stack: `weaver`, `weaver-postgres`, `weaver-playwright`, `rss-fixture`, `nntp`, `nntp2`, `nyuu`, `toxiproxy`, `sabnzbd`, `nzbget` |
| `services/` | Config and build contexts for the stack services |
| `playwright-weaver/` | The Playwright project (image `weaver-e2e-playwright:local`); `tests/` is live-mounted into the container |
| `testdata/<slug>/` | One directory per scenario: `scenario.json` is tracked, the payload bytes are hydrated; `testdata/shared/` holds the source clips the generators encode from |
| `test-corpus/` | The corpus ledger, profiles, toolchain lock and published-manifest lock |
| `cmd/corpus`, `internal/corpus` | The corpus tool: `ensure`, `hydrate`, `verify`, `build`, `sign`, `publish` |
| `cmd/fixturegen`, `internal/fixturegen` | The fixture generator: one declarative recipe per scenario, run on pinned oracle images |
| `fixtures/` | Seeded NZB output written by `weaver-e2e seed`; not tracked |
| `docs/` | [`test-corpus.md`](docs/test-corpus.md) and [`generators.md`](docs/generators.md) |

## The fixture corpus

The payload bytes under `testdata/` — 237 archives, parity sets, split volumes,
recovery volumes and source clips — are **not in git**. They are a signed,
content-addressed object set described by `test-corpus/sources.json` and
published by a manual workflow. Before anything is seeded the harness makes
the fixtures it needs present, always in the same order:

1. **reuse** what is on disk and matches the ledger;
2. **fetch** what is missing from the published corpus, when
   `test-corpus/lock.json` pins one;
3. **generate** locally only what is still missing after that.

`full`, `functional`, `release-gate` and `seed-all` run that as a digest-checked
pre-flight over their whole profile; every fixture is size-checked again as it
is seeded, so a scenario seeded on its own is covered too. The same thing by
hand:

```bash
task hydrate PROFILE=functional          # chaos | tcp-chaos | restart | release-gate | shared | all
go run ./cmd/corpus ensure --slug rar5-multivolume
task fetch PROFILE=functional            # published corpus only, never generates
task corpus:verify -- --all-present --offline
```

`E2E_FIXTURES=fetch` forbids local generation (what a CI lane wants);
`E2E_FIXTURES=off` skips the check entirely. Every digest is BLAKE3; a fetch
verifies the manifest against the pinned lock and every object against the
manifest before writing a byte; `verify` fails closed if the ledger, the tree
and the lock disagree in either direction. A locally generated fixture is a
*local corpus revision*: the ledger's digests for that scenario are refreshed
(byte-reproducible families come back identical and change nothing), so do
not commit that ledger change unless you are publishing it.

Every fixture is generated. `go run ./cmd/fixturegen` rebuilds any scenario
from its recipe on the oracle images pinned in `test-corpus/toolchains.json` —
official RARLAB releases, the official 7-Zip console binary, par2cmdline-turbo,
a digest-pinned FFmpeg, and Go's own archive and codec writers. RAR bytes are
only ever written by RARLAB's `rar`; every title in the corpus is invented.

[`docs/test-corpus.md`](docs/test-corpus.md) covers the ledger, profiles,
verification chain and publication; [`docs/generators.md`](docs/generators.md)
covers the recipes and how to add a scenario.

## Commands

| Command | What it does |
| --- | --- |
| `weaver-e2e seed <fixture-dir>` | Post one fixture with Nyuu and generate its NZB |
| `weaver-e2e seed-all` | Seed every scenario in `testdata/` |
| `weaver-e2e verify` | Verify article availability |
| `weaver-e2e status` | Check NNTP and Weaver health |
| `weaver-e2e scenarios` | List scenarios and their expected outcomes |
| `weaver-e2e submit <slug>` | Submit one seeded NZB directly to Weaver |
| `weaver-e2e test <slug>…` / `test-all` | Run selected scenarios, or the whole baseline suite, through Weaver |
| `weaver-e2e functional` | The functional phases with the live dashboard |
| `weaver-e2e chaos <config>` / `chaos-test` | Send one NNTP fault-injection command, or run the NNTP-chaos suite (baseline round first) |
| `weaver-e2e tcp-chaos` | The Toxiproxy TCP-chaos suite against a managed local Weaver |
| `weaver-e2e tls-test` | The TLS NNTP validation subset |
| `weaver-e2e adaptive-dispatch` | Multi-server dispatch: server 1 behind Toxiproxy latency, server 2 direct; fails unless server 2 gets a material majority of BODY fetches |
| `weaver-e2e restart-test <case>…` / `restart-all` | Managed-local restart and crash cases with per-case DB, filesystem and NNTP artifacts |
| `weaver-e2e container-restart` | Restart the Docker Weaver service and require its persisted encryption key to keep its fingerprint and `0600` mode |
| `weaver-e2e full` | Everything above that belongs in a release, plus the product-behavior gate, under one release manifest |
| `weaver-e2e release-gate [all\|<flow>\|datastore-matrix]` | The product-behavior gate: all flows, one flow, or the explicit datastore matrix |
| `weaver-e2e release-console [latest\|<run-dir>]` | Serve a release-gate artifact directory on loopback |
| `weaver-e2e download-bench [slug…]` | Sequential download-hotpath benchmark with per-run artifacts |
| `weaver-e2e pgo [slug…]` | Drive the workload against an instrumented Weaver and collect LLVM `.profraw` |

## What `full` runs

`full` is the canonical release command. It runs the API-driven download,
NNTP-chaos, TCP-chaos and restart phases, and launches the product-behavior
gate. Each phase gets its own Compose project, ports, fixtures directory and
Weaver state, so phases overlap as soon as their own stacks are ready and never
interfere. Fixture-driven phases pipeline `seed → run`; a seedless Docker phase
also performs `docker compose restart weaver` and checks the encryption-key
file before and after. Host-exposed services bind to random free ports per
stack, recorded in `E2E_RUNTIME_PORTS_FILE` so later commands such as `status`
reuse them while the stack is up.

## The product-behavior release gate

The gate complements the download lane; it does not replay it through the
browser. Browser flows own actions and visible outcomes a user performs in
Weaver. API/metrics flows own runtime policies whose reliable oracle is public
API state, Prometheus metrics or fake-service counters. Probe traffic may
create load for those policies but never asserts article integrity, repair,
extraction or final output.

| Owner | Flows | Primary oracle |
| --- | --- | --- |
| Browser | `ui-settings-crud`, `ui-security`, `ui-ingress-automation`, `ui-post-processing`, `ui-runtime-observability` | Visible controls, validation, persistence, failure and recovery state, browser health |
| Browser backup matrix | `ui-backup-restore-{sqlite,postgres}-to-{sqlite,postgres}` | Visible backup/restore behavior plus target-state persistence |
| API/metrics | `rate-limits`, `bandwidth-and-server-quotas`, `provider-connection-cap`, `encryption-key-lifecycle`, `duplicate-and-queue-policy` | Public API responses, Weaver metrics, fake-service counters, lifecycle evidence |
| Existing command flow | `adaptive-dispatch` | Its own acceptance oracle |

```bash
task release-gate -- all               # default set, expanded over each flow's datastores
task release-gate -- rate-limits       # one flow; still runs on SQLite and PostgreSQL
task release-gate -- datastore-matrix  # full expansion incl. the four backup directions
task release-console -- latest         # reopen the newest run
```

A run writes a top-level `release-gate.json` manifest and one directory per
flow and datastore with status, timing, captured output, Playwright artifacts
and — on failure — Compose logs, service state and a Weaver metrics snapshot.
`latest.json` is a stable pointer to the newest run. Flows run in parallel;
`E2E_WEAVER_RELEASE_GATE_JOBS` (default 8, capped at 16) sets the width.

Inside `playwright-weaver/`, `npm run audit` enforces three rules on every
`ui-*.spec.ts`: the project is self-contained; primary actions and assertions
go through the browser (no GraphQL, REST, metrics or request-context
shortcuts); and selectors use accessible roles and labels or product-owned test
IDs — no XPath, ancestor traversal, positional locators or structure-dependent
CSS. Setup state, external-system controls and runtime introspection are only
reachable through the narrow helpers under `tests/support/`. Run
`npm run typecheck` and the audits before adding a flow to the registry.

## Configuration

Everything is an environment variable; nothing is required for the default
Docker path.

| Variable | Default | Purpose |
| --- | --- | --- |
| `E2E_DIR` | auto-detected | Harness root (found by walking up to `docker-compose.yml`) |
| `E2E_WEAVER_REPO` | parent of `E2E_DIR` | Weaver source tree for `cargo build` and the local image build |
| `E2E_PROJECT` | `e2e` | Compose project name for the current run |
| `E2E_WEAVER_IMAGE` | `ghcr.io/scryer-media/weaver:latest` | Weaver image for shared-stack runs; unset builds `weaver-e2e-weaver:local` from `E2E_WEAVER_REPO` |
| `E2E_NNTP_MODULE_VERSION` | `v0.1.0` | Published `e2e-nntp` module version the fake NNTP image is built from |
| `E2E_NNTP_SOURCE_DIR` | unset | Developer override: build the fake NNTP image from this local module root instead. Never guessed; a missing directory is an error |
| `E2E_NNTP_PIPELINING` / `E2E_NNTP2_PIPELINING` | `0` | Advertise RFC 4644 `PIPELINING` per fake server |
| `E2E_FIXTURES` | `auto` | How missing fixtures are obtained before seeding: `auto` fetches from the published corpus then generates what is still missing; `fetch` never generates; `off` does nothing |
| `E2E_WEAVER_POSTGRES_DB` / `_USER` / `_PASSWORD` | `weaver` / `weaver` / `weaver-pass` | PostgreSQL settings for the PostgreSQL phases |
| `FIXTURES_DIR` / `TESTDATA_DIR` | `<e2e>/fixtures` / `<e2e>/testdata` | Seeded output and scenario sources |
| `E2E_RUN_DIR` | temp dir | State directory for managed-local runs (`tcp-chaos`, `tls-test`, restart, local `download-bench`) |
| `E2E_RUNTIME_PORTS_FILE` | temp file | Keeps runtime-assigned host ports stable across related invocations; pin a port by exporting the matching `E2E_*_PORT` |
| `E2E_SEED_JOBS` | `4` (`2` inside `full`) | Concurrent `seed-all` workers; `1` for sequential |
| `E2E_KEEP_STACKS` | `0` | Keep isolated stacks after `full` / `release-gate` |
| `E2E_WEAVER_RELEASE_GATE_JOBS` | `8` | Release-gate flow parallelism, capped at 16 |
| `E2E_WEAVER_RELEASE_GATE_ROOT` | `/tmp/weaver-e2e-release-gate` | Root for release manifests, per-flow artifacts and `latest.json` |
| `E2E_FORCE_REBUILD_WEAVER_IMAGE` / `_WEAVER_PLAYWRIGHT_IMAGE` / `_NYUU_IMAGE` / `_E2E_INFRA_IMAGES` | `0` | Force-rebuild the corresponding local images |
| `E2E_VERBOSE` | `0` | Stream raw Docker and Nyuu output instead of summaries |
| `E2E_PW_VIDEO` | `retain-on-failure` | Playwright video: `on`, `off`, or failure-only |
| `WEAVER_URL` / `WEAVER_PORT` / `WEAVER_BIN` | runtime-assigned / auto-detected | Weaver endpoint and binary for managed-local runs |
| `E2E_RESTART_PROFILE` | `hardened` | Restart-suite expectations: `hardened` (documented gaps fail) or `current` |
| `E2E_RESTART_ONLY_CASE` / `_TIMEOUT_SEC` / `_KEEP_ARTIFACTS` | unset / `900` / `1` | Restart-suite scoping |
| `E2E_SUSPEND_TOLERANCE_SEC` | `30` | Extra wall clock allowed before host sleep or clock jumps are reported |
| `CHAOS_ONLY_ROUND` / `TCP_CHAOS_ONLY_ROUND` | unset | Run only one chaos round |
| `DOWNLOAD_BENCH_LOCAL_WEAVER` / `_ITERATIONS` / `_SAMPLE_MS` / `_TIMEOUT_SEC` / `_OUTPUT_DIR` / `_SLUGS` / `_CONNECTIONS` | `0` / `3` / `250` / `300` / temp / `single-mkv,large-segments` / `8` | Download-benchmark knobs |
| `ADAPTIVE_DISPATCH_SCENARIO` / `_LATENCY_MS` / `_JITTER_MS` / `_MIN_DIRECT_PCT` / `_CONNECTIONS` | `large-segments` / `75` / `10` / `60` / `8` | Adaptive-dispatch knobs |
| `E2E_WEAVER_PROFILE_DIR` | `<run dir>/pgo/profraw` | Where managed-local runs write `.profraw` |

Further optional knobs the code reads (internal tuning or product-flag
mirrors): `E2E_NNTP_USERNAME` / `E2E_NNTP_PASSWORD` (default `e2e-user` /
`e2e-pass`), `E2E_NNTP_IMAGE`, `E2E_WEAVER_POSTGRES_HOST` / `_PORT` /
`_SSLMODE`, `WEAVER_DATABASE_URL`, `E2E_SEED_PROFILE`, `E2E_SEED_RETRIES`,
`E2E_FULL_PHASE_SLUGS`, `E2E_RESTART_MAX_CONCURRENT_EXTRACTIONS`,
`E2E_WEAVER_RELEASE_GATE_MAX_MINUTES`, `E2E_WEAVER_PLAYWRIGHT_IMAGE`,
`E2E_WEAVER_PLAYWRIGHT_ARTIFACTS_DIR`, `E2E_WEAVER_MODE`,
`E2E_WEAVER_BASE_URL`, `E2E_WEAVER_PORT`, `E2E_LOCAL_WEAVER_PORT`,
`E2E_NYUU_BACKUP_HOST` / `_PORT`, `E2E_TOXIPROXY_API_PORT` /
`TOXIPROXY_URL`, `E2E_TLS_SCENARIOS`, `E2E_WEAVER_PGO_OUTPUT_DIR`,
`DOWNLOAD_BENCH_NNTP_PORT`, `ADAPTIVE_DISPATCH_SAMPLE_MS` / `_TIMEOUT_SEC` /
`_SEED_RETRIES` / `_MESSAGE_PREFIX`, and `WEAVER_RAR_DIRECT_STORE` (passed
through to the product).

## Benchmarking and PGO

Host-side download-hotpath benchmarking runs a local Weaver process and submits
download-only scenarios one at a time:

```bash
task build
DOWNLOAD_BENCH_LOCAL_WEAVER=1 WEAVER_BIN=/path/to/weaver task bench:download -- single-mkv large-segments
task bench:perf-record -- single-mkv large-segments   # Linux; PERF_FREQ tunes the sample rate
task bench:perf-stat
```

Each run writes `summary.json` (per-run timings, per-scenario averages),
`*-samples.json` (sampled job and metrics snapshots) and, for managed-local
runs, `weaver.log`, `weaver.pid` and the generated config.

For profile-guided optimisation, build an instrumented Weaver, point
`WEAVER_BIN` at it, and let the harness drive the workload; `pgo` runs the
baseline suite, the TLS subset and the download benchmark, and fails if no new
`.profraw` files appeared:

```bash
WEAVER_BIN=/path/to/instrumented/weaver E2E_WEAVER_PROFILE_DIR=/tmp/weaver-profraw task pgo
```

## Notes

- `test-all` and `chaos-test` reset NNTP chaos state before they start, so
  stale fault injection cannot poison a baseline run.
- `seed`, `seed-all`, `test-all`, `chaos-test`, `tcp-chaos` and `tls-test`
  bring up the services they need and wait for readiness.
- `restart-test` / `restart-all` run against a managed local Weaver, rebuild
  the NNTP image so control-plane metrics match the harness, and report `PASS`,
  `DOCUMENTED_GAP` or `FAIL` in the `current` profile; a documented gap is a
  `FAIL` in the default `hardened` profile.
- `tcp-chaos` starts `nntp2` and `toxiproxy`, writes a temporary Weaver config
  and runs a local Weaver inside `E2E_RUN_DIR` on a runtime-assigned port.
- `testdata/par2-obfuscated-rar-rewrite` deliberately reuses the clean
  `rar5-multivolume` RAR payload through `fixtureAssets` and stores only its
  PAR2 sidecars; `go run ./cmd/fixturegen --scenario par2-obfuscated-rar-rewrite`
  regenerates them.
- Generated NZBs keep their existing dates when reseeded, and new ones use a
  stable date, so reruns do not churn `.nzb` files.
- Third-party image pins in `docker-compose.yml` are deliberate. This suite is
  local-only, so pinning keeps runs deterministic; bump a pin on purpose when
  validating a newer release rather than floating it to `:latest`.
- Long polls and readiness waits stop with a suspend warning if the host
  sleeps or the wall clock jumps during a run.
- Do not assume a failing archive scenario has PAR2; read the fixture's NZB
  before building a repair theory.
