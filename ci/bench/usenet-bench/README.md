# usenet-bench

A reproducible, product-neutral download benchmark for **Weaver**, **SABnzbd**
and **NZBGet**.

It does not guess what a "typical Usenet release" looks like. It generates a
declared matrix of clean and deliberately damaged multi-volume RAR fixtures on
pinned RARLAB and PAR2 toolchains, posts them once to a local
[e2e-nntp](https://github.com/scryer-media/e2e-nntp) server behind a
server-side bandwidth shaper, and then runs every client through the same
randomized, sequential plan — one fresh client process or container per run,
output verified independently by BLAKE3 before it is deleted. What comes out is
a per-fixture, per-target result record and a paired statistical summary, not a
single number.

Everything needed to reproduce a published run is in git: the fixture matrix,
the source-locked toolchains, the NZB creation command, the rendered client
configuration and the result schema. Generated archives, posted articles,
downloaded data and run artifacts are ignored by git and never committed.

## Contents

- [Requirements](#requirements)
- [Quickstart](#quickstart)
- [Repository layout](#repository-layout)
- [The fixture matrix](#the-fixture-matrix)
- [Step by step](#step-by-step)
  - [1. Generate fixtures](#1-generate-fixtures)
  - [2. Build the NNTP server image](#2-build-the-nntp-server-image)
  - [3. Start the server and shaper](#3-start-the-server-and-shaper)
  - [4. Post the corpus with Nyuu](#4-post-the-corpus-with-nyuu)
  - [5. Write a plan](#5-write-a-plan)
  - [6. Run the sequential suite](#6-run-the-sequential-suite)
  - [7. Summarize](#7-summarize)
- [Native macOS and Windows lanes](#native-macos-and-windows-lanes)
- [What is measured](#what-is-measured)
- [TLS policy](#tls-policy)
- [What this does not claim](#what-this-does-not-claim)

## Requirements

| Requirement | Why |
| --- | --- |
| Go 1.26+ | every tool here is `go run ./cmd/…`; the module has one dependency (`zeebo/blake3`) |
| Docker with Compose v2 | the RARLAB, PAR2, Nyuu, NNTP-server and shaper images, and the Docker client lane |
| `linux/amd64` emulation on arm64 hosts | the RARLAB and Nyuu images are `linux/amd64` on purpose (see below); Docker Desktop provides it, on plain Linux run `docker run --privileged --rm tonistiigi/binfmt --install amd64` |
| ~10 GiB free disk | a full corpus with the `bluray-disc` fixture; smoke runs need far less |
| Native product installs | only for the optional `macos-native` / `windows-native` lanes |

Every command below is run from this directory. Paths under `/scratch/…` are
placeholders — use any directory outside the repository.

## Quickstart

The Docker-only smoke path, end to end:

```bash
# 1. Two fixtures: one clean RAR5 case and one raw-MKV case for the queue benchmark.
go run ./cmd/fixturegen --fixture rar5-7-headers-normal-solid-headers-compressible --output /scratch/fixtures
go run ./cmd/fixturegen --direct-mkv --output /scratch/fixtures

# 2. Build the pinned NNTP server image from the published module.
go run ./cmd/nntpbench image build --version v0.1.0 --tag e2e-nntp:local \
  --provenance /scratch/runs/nntp-image-provenance.json

# 3. Bring up server + shaper at a declared link rate; keep the test CA.
openssl rand -hex 24 > /scratch/runs/nntp-password
go run ./cmd/nntpbench server-env --server-link 1gbit --output /scratch/runs/server-1gbit.env
NNTP_BENCH_PASSWORD_FILE=/scratch/runs/nntp-password docker compose -p nntp-bench \
  --env-file /scratch/runs/server-1gbit.env -f configs/server/compose-shaper.example.yml up --build -d
NNTP_BENCH_PASSWORD_FILE=/scratch/runs/nntp-password docker compose -p nntp-bench \
  --env-file /scratch/runs/server-1gbit.env -f configs/server/compose-shaper.example.yml \
  cp nntp:/certs/ca.pem /scratch/runs/nntp-ca.pem

# 4. Post the fixture over the private upstream network (never through the shaper).
go run ./cmd/nntpbench seed --fixture-dir /scratch/fixtures/rar5-7-headers-normal-solid-headers-compressible \
  --run-id smoke-1 --network nntp-bench_nntp_upstream --nntp-host nntp-upstream \
  --username fixture-user --password-file /scratch/runs/nntp-password

# 5. Plan, 6. run, 7. summarize.
go run ./cmd/nntpbench plan --fixtures rar5-7-headers-normal-solid-headers-compressible \
  --archive-toolchains vanilla --profile stock --server-link 1gbit --repetitions 20 --seed 1 \
  --targets docker-linux --output /scratch/runs/plan.json
go build -o /scratch/bin/clientadapter ./cmd/clientadapter
cp configs/adapters.example.json /scratch/runs/adapters.json   # set the adapter path + network name
go run ./cmd/nntpbench sequential --plan /scratch/runs/plan.json --adapters /scratch/runs/adapters.json \
  --target docker-linux --fixtures-root /scratch/fixtures --artifacts /scratch/runs/artifacts \
  --nntp-host nntp --shaper-control-url http://127.0.0.1:8080 --tls-ca-file /scratch/runs/nntp-ca.pem \
  --username fixture-user --password-file /scratch/runs/nntp-password
go run ./cmd/nntpbench summarize --artifacts /scratch/runs/artifacts --baseline sabnzbd --candidate weaver
```

Each step is explained below. `go run ./cmd/nntpbench` with no arguments
lists every subcommand; `-h` on any of them prints its options.

## Repository layout

| Path | What it is |
| --- | --- |
| `cmd/fixturegen` | Generates the RAR / PAR2 / recovery-volume fixtures on the pinned toolchain images |
| `cmd/nntpbench` | The controller: `image build`, `seed`, `server-env`, `plan`, `sequential`, `queue-transition`, `run`, `summarize`, `preflight`, `verify-output` |
| `cmd/clientadapter` | Docker lane: starts one fresh, digest-pinned client container per run and drives its public API |
| `cmd/nativeadapter` | macOS / Windows lane: launches one fresh native client process per run |
| `cmd/nntpshaper` | Transparent TCP proxy that meters the server's egress under an exclusive run lease |
| `configs/adapters*.example.json` | Digest-pinned client catalogs for the Docker, macOS and Windows lanes — copy, then edit |
| `configs/clients/baseline.json` | The cross-client configuration baseline every rendered config is derived from |
| `configs/server/compose-shaper.example.yml` | The server + shaper topology |
| `docker/` | Dockerfiles for the pinned RARLAB writers, `par2cmdline-turbo`, Nyuu and the shaper |
| `fixtures/matrix.json`, `fixtures/corpus.json` | The declared fixture matrix and corpus description |
| `internal/` | The Go packages behind the commands |

## The fixture matrix

The corpus is a compatibility and repair coverage set, not a model of what is
posted to Usenet. Ordinary cases contain one 150 MiB synthetic video file split
into 32 MiB RAR volumes; one multi-input case contains four 48 MiB videos.
Together they cover the RARLAB writer eras and their archive families across:

| Axis | Values |
| --- | --- |
| Writer era | RAR 3.93, 4.20, 5.00, 6.24, 7.23 (official RARLAB Linux releases, SHA-256 verified in the image build) |
| Archive lane | legacy RAR4 (3.93 / 4.20 writers) or RAR5 (5.00 / 6.24 / 7.23 writers, explicit `-ma5`) |
| Compression | store (`-m0`) or release-style normal compression (`-m5`, solid where declared, maximum dictionary, RAR5 quick-open disabled) |
| Solidity | non-solid, solid |
| Encryption | none, data encryption, encrypted headers |
| Input data | incompressible, moderately compressible |

That yields 18 clean RAR fixtures. `writer_era` is deliberately separate from
`rar_format`: RAR 6 and 7 are writer releases, not new on-disk formats.

One `bluray-disc` fixture exercises a disc-shaped topology — a 5 GiB
`BDMV/STREAM/00000.m2ts`, four small menu streams and 508 tiny metadata
members in one solid RAR5 archive. `--bluray-large-file-bytes`,
`--bluray-small-file-count` and `--bluray-small-file-bytes` scale it down for
smoke runs without changing what it represents. It is synthetic; it is not a
Blu-ray image and not a claim about typical posts.

Four repair fixtures add deterministic damage without duplicating the clean
cases:

| Profile | Posted repair material | Deliberate fault |
| --- | --- | --- |
| `par2-light` | PAR2 at 10 % redundancy | 128 deterministic byte flips in one non-leading volume |
| `par2-heavy` | PAR2 at 35 % redundancy | one complete non-leading volume absent |
| `rar-recovery-volume-light` | one RAR recovery volume | one non-leading volume absent |
| `rar-recovery-volume-heavy` | two RAR recovery volumes | two non-leading volumes absent |

PAR2 material comes from the pinned `par2cmdline-turbo` 1.4.0 image. Before a
repair fixture is accepted the generator copies its posted input aside,
performs the declared repair with the pinned PAR2 or RARLAB tool, and
RARLAB-tests the reconstructed archive against the payload oracle. Only the
damaged input is kept.

RAR bytes are only ever written by RARLAB's own `rar`; add an older writer
only when an official, source-locked RARLAB release exists for it. Nothing
here pulls historical binaries from mirrors.

## Step by step

### 1. Generate fixtures

`fixturegen` builds and runs the pinned RARLAB / PAR2 images itself, and
`--direct-mkv` uses the same image for its deterministic FFmpeg payload, so
Docker is required for every invocation.

```bash
go run ./cmd/fixturegen --list

# One benchmark-sized movie case (150 MiB payload by default).
go run ./cmd/fixturegen --fixture rar5-7-headers-normal-solid-headers-compressible --output /scratch/fixtures

# The disc topology at smoke scale.
go run ./cmd/fixturegen --fixture rar5-7-bluray-normal-solid-none-incompressible \
  --bluray-large-file-bytes 256MiB --bluray-small-file-count 64 --bluray-small-file-bytes 32KiB \
  --output /scratch/fixtures

# The raw-download fixture the queue-transition benchmark uses.
go run ./cmd/fixturegen --direct-mkv --output /scratch/fixtures
```

Every fixture directory contains `archive/` — the exact bytes to post: RAR
volumes plus any PAR2 or `.rev` files, with deliberately missing volumes absent
but their pre-damage digests kept — and `fixture-manifest.json` with the RAR
flags, the toolchain that wrote it, the repair profile, BLAKE3 digests of the
source and posted files, and the extracted-output oracle. The source payload
is deleted once RARLAB has tested the archive; the manifest is enough to verify
client output. The generator refuses to overwrite an existing fixture directory
so the data behind a published result cannot be replaced silently.

### 2. Build the NNTP server image

The server is [e2e-nntp](https://github.com/scryer-media/e2e-nntp), a
deterministic fake NNTP server published as a Go module, not as an image. Build
it locally at an exact version:

```bash
go run ./cmd/nntpbench image build --version v0.1.0 --tag e2e-nntp:local \
  --provenance /scratch/runs/nntp-image-provenance.json

# Developer override: an explicitly named local checkout.
go run ./cmd/nntpbench image build --source-dir /path/to/e2e-nntp --tag e2e-nntp:local \
  --provenance /scratch/runs/nntp-image-provenance.json
```

`--provenance` is required and must point inside the run's artifact root; it
records the tag, image ID, platform, binary SHA-256 and either the module
version or a redacted `source-directory` label. Nothing assumes a checkout at
any particular path. The Compose topology uses `pull_policy: never`, so it only
starts a tag built this way; export `E2E_NNTP_IMAGE` if you used a tag other
than `e2e-nntp:local`, and `NNTP_SHAPER_IMAGE` to keep a run-specific shaper
tag apart from the default `weaver-nntp-bench-shaper:dev`.

### 3. Start the server and shaper

`nntpshaper` is a transparent TCP proxy in front of the server. It gives all
client connections one aggregate egress budget and shapes only response bytes —
the protocol and article bytes actually delivered — leaving client commands
unthrottled. TLS passes through unterminated, so verifying clients still check
the server's certificate.

```bash
go run ./cmd/nntpbench server-env --server-link 1gbit --output /scratch/runs/server-1gbit.env

NNTP_BENCH_PASSWORD_FILE=/scratch/runs/nntp-password docker compose -p nntp-bench \
  --env-file /scratch/runs/server-1gbit.env -f configs/server/compose-shaper.example.yml up --build -d

# Keep the generated test CA with the run inputs for CA-verifying clients.
NNTP_BENCH_PASSWORD_FILE=/scratch/runs/nntp-password docker compose -p nntp-bench \
  --env-file /scratch/runs/server-1gbit.env -f configs/server/compose-shaper.example.yml \
  cp nntp:/certs/ca.pem /scratch/runs/nntp-ca.pem
```

`1gbit` is exactly 1 000 000 000 bit/s and `10gbit` exactly ten times that,
each with a declared 1 MiB burst; `unlimited` is zero rate and burst; `custom`
requires both values. The link profile is persisted in the plan, every run and
every result. The server sits on a private upstream network; clients resolve
`nntp` to the shaper on the benchmark network.

For every shaped run the controller takes a random exclusive execution lease
on the shaper and captures strict control-plane snapshots before and after the
client: configured rate and burst, executable digest, lease identity, counter
continuity, zero active connections at both boundaries, a non-zero byte delta,
and per-source connection and byte counters. A second lease or a second
downstream source fails the run instead of contaminating it. The NNTP data
plane cannot carry the lease token through implicit TLS, so a dedicated
benchmark host or network namespace with no other NNTP clients is a
publication prerequisite; the lease and source counters enforce the boundary
between cooperating runs and detect every differently sourced connection.

By default the topology publishes the shaper only on `127.0.0.1`. For a remote
native lane set `NNTP_PUBLIC_BIND_ADDR` to a specific LAN address, firewall it
to the benchmark host, and give the certificate a stable DNS SAN (for example
`nntp.bench.test`) that maps to the shaper — NZBGet's strict verification
checks the hostname as well as the CA. `NNTP_PUBLIC_PLAINTEXT_PORT` and
`NNTP_PUBLIC_TLS_PORT` default to 119 and 563.

### 4. Post the corpus with Nyuu

[Nyuu](https://github.com/animetosho/Nyuu) — not a custom poster — writes the
yEnc articles and the NZB. Post over the private upstream network so setup
never crosses the shaper:

```bash
go run ./cmd/nntpbench seed \
  --fixture-dir /scratch/fixtures/rar5-7-headers-normal-solid-headers-compressible \
  --run-id 2026-08-02-a \
  --network nntp-bench_nntp_upstream --nntp-host nntp-upstream \
  --username "${NNTP_BENCH_USERNAME:-fixture-user}" --password-file "$NNTP_BENCH_PASSWORD_FILE"
```

The pinned Nyuu image is `linux/amd64` because its native `yencode` module
does not build on arm64 Alpine; Docker emulates it on Apple Silicon. Posting is
corpus setup, never a metric. It uses plaintext port 119 once; the same
persisted articles are then downloaded over 119 and implicit-TLS 563.

### 5. Write a plan

```bash
go run ./cmd/nntpbench plan \
  --fixtures rar5-7-headers-normal-solid-headers-compressible,rar4-store-store-nonsolid-none-incompressible \
  --archive-toolchains vanilla --profile stock --server-link 10gbit \
  --repetitions 20 --seed 20260802 --output /scratch/runs/plan.json
```

A plan is the exact run order. It randomizes target / fixture / transport /
repetition blocks and the client and toolchain lanes inside each block, and it
is strictly sequential, so timed clients never share the server. Both client
profiles keep the full client-by-packaging matrix for every
`(fixture, transport, repetition)`:

| Client | `docker-linux` | `macos-native` | `windows-native` |
| --- | --- | --- | --- |
| Weaver | digest-pinned image, public GraphQL API | local service, public GraphQL API | local service, public GraphQL API |
| SABnzbd | digest-pinned image, public API | native distributable, public API | native distributable, public API |
| NZBGet | digest-pinned image, JSON-RPC | native executable, JSON-RPC | native executable, JSON-RPC |

`--profile stock` and `--profile equivalent-throughput` are reported
separately; neither is a fallback for the other. `archive_toolchain` is a
first-class plan, adapter, config and result field: `vanilla` is the stock
benchmark, and the optional `rarpar` Docker lanes (see below) are never pooled
with it. `--targets docker-linux` writes a Docker-only plan; otherwise the plan
carries all three targets and each host runs only its own.

### 6. Run the sequential suite

This is the primary measurement. Every persisted run gets fresh client state
and a fresh process or container, exactly one fixture is submitted through the
client's public API, the harness waits for the terminal state, verifies the
output with BLAKE3, and only then deletes it.

```bash
go build -o /scratch/bin/clientadapter ./cmd/clientadapter

# Copy the catalog; set the adapter path and the Compose network name.
# Change an image only by replacing its full digest — a floating tag is rejected.
cp configs/adapters.example.json /scratch/runs/adapters.json

go run ./cmd/nntpbench sequential \
  --plan /scratch/runs/plan.json --adapters /scratch/runs/adapters.json \
  --target docker-linux --fixtures-root /scratch/fixtures --artifacts /scratch/runs/artifacts \
  --nntp-host nntp --shaper-control-url http://127.0.0.1:8080 \
  --tls-ca-file /scratch/runs/nntp-ca.pem \
  --username "${NNTP_BENCH_USERNAME:-fixture-user}" --password-file "$NNTP_BENCH_PASSWORD_FILE"
```

`--nntp-host` is resolved by the client containers (`nntp` is the shaper alias);
`--shaper-control-url` is dialled from the host, so it is the shaper's
published control port, not the service name. A shaped plan refuses to start
without it.

The artifact root is reserved only after configuration validates and holds an
immutable `execution-manifest.json`, plan and catalog snapshots with SHA-256
digests, the harness-executable digest, a host fingerprint and secret-redacted
arguments. Use a new artifact root per invocation; nothing is overwritten.

`CLIENT_POLL_INTERVAL` / `NATIVE_POLL_INTERVAL` default to `10ms` and set the
terminal-observation precision recorded in every artifact; a run is excluded
unless that interval is at most 1 % of its submission-to-terminal duration.

Two other modes exist and are labelled apart from the headline:

- `queue-transition` — generate and seed `direct-mkv-200mb`, plan **only** that
  fixture with 20 runs per lane, and measure first-submission-to-last-terminal
  wall clock across forced duplicates. It reports no per-job scores.
- `run` — a cold, one-NZB diagnostic. Never the headline result.

Independent output verification is also available on its own:

```bash
go run ./cmd/nntpbench verify-output \
  --fixture-dir /scratch/fixtures/rar5-7-headers-normal-solid-headers-compressible \
  --output-dir /scratch/runs/run-0001/complete
```

#### Optional: rarpar-backed Docker lanes

The two `rarpar` catalog entries swap the clients' external unpackers for a
**published** [rarpar](https://github.com/scryer-media/rarpar) release binary —
never a source build, never a floating tag. Download the `linux/amd64` release
artifact, keep it and its checksum with the run inputs, and fill in:

```json
"CLIENT_RARPAR_BINARY": "/scratch/rarpar/rarpar-linux-amd64",
"CLIENT_RARPAR_VERSION": "0.2.5",
"CLIENT_RARPAR_SHA256": "<lowercase 64-character release digest>"
```

The adapter checks the digest before every timed run and records
`rarpar <version> sha256:<digest>` in the result; the host path is not
retained. SABnzbd finds `unrar` and `par2` on `PATH`, so it gets the binary as
`rarpar`, a one-line `unrar` shim and a `par2` copy (rarpar accepts both argv
shapes). NZBGet has no `Par2Cmd`, so its lane keeps the built-in PAR2 engine and
points only `UnrarCmd` at the shim; its provenance says so.

### 7. Summarize

```bash
go run ./cmd/nntpbench summarize \
  --artifacts /scratch/runs/artifacts --baseline sabnzbd --candidate weaver --minimum-blocks 20
```

Only verified, passed sequential artifacts are admitted. Clients are paired
inside the same randomized repetition block, stratified by fixture, profile,
target, transport / TLS validation, archive toolchain and server link. Each
stratum reports the raw medians and coefficients of variation, the paired
geometric-mean ratio and a deterministic 10 000-resample bootstrap 95 %
interval on the log ratio. There is no outlier deletion and no pooled score. A
missing, failed or unverified run, an incomplete pair, fewer than 20 complete
blocks, or terminal-observation uncertainty above 1 % fails the summary closed.

## Native macOS and Windows lanes

The native lanes run the same sequential measurement with one fresh client
process per run. Build the launcher, copy the OS catalog, and use a separate
artifact root per operating system:

```bash
# macOS (on the Mac host)
go build -o /scratch/bin/nativeadapter ./cmd/nativeadapter
cp configs/adapters.macos.example.json /scratch/runs/adapters.macos.json
go run ./cmd/nntpbench sequential \
  --plan /scratch/runs/plan.json --adapters /scratch/runs/adapters.macos.json \
  --target macos-native --fixtures-root /scratch/fixtures --artifacts /scratch/runs/artifacts-macos \
  --nntp-host <server address reachable from the Mac> --shaper-control-url http://<shaper control address>:8080 \
  --tls-ca-file /scratch/runs/nntp-ca.pem \
  --username "${NNTP_BENCH_USERNAME:-fixture-user}" --password-file "$NNTP_BENCH_PASSWORD_FILE"

# Windows: cross-build, or build on the Windows host, then run both there with the Windows catalog.
GOOS=windows GOARCH=amd64 go build -o nativeadapter.exe ./cmd/nativeadapter
GOOS=windows GOARCH=amd64 go build -o nntpbench.exe ./cmd/nntpbench
```

Run the non-mutating preflight first; it prints the expected local
executables and fails if the OS or a binary is missing:

```bash
go run ./cmd/nntpbench preflight --target macos-native \
  --adapter /scratch/bin/nativeadapter --weaver /path/to/weaver --nzbget /path/to/nzbget
```

Notes for the native catalogs:

- `adapters.macos.example.json` uses the installed
  `/Applications/SABnzbd.app/Contents/MacOS/SABnzbd` and leaves the Weaver and
  NZBGet paths as explicit replacements; the Windows catalog uses explicit
  paths throughout. The harness never installs a product implicitly — stage
  pinned installers, record their versions and hashes in the catalog, and use an
  isolated working directory (for example `C:\bench`).
- `NATIVE_LAUNCH_COMMAND` is a JSON argv array, never a shell string, and may
  use `{{config_dir}}`, `{{nzb_path}}`, `{{output_dir}}`, `{{fixture_dir}}` and
  `{{api_port}}`. Commands must stay in the foreground so the launcher can
  collect CPU time and stop them cleanly.
- `NATIVE_CLIENT_VERSION` must equal what the product reports through its own
  API (SABnzbd `version`, NZBGet `version`, Weaver GraphQL `version`); a
  mismatch fails the run before any NZB is submitted.
- For native Weaver set `WEAVER_ENCRYPTION_KEY` in the adapter environment so
  no Keychain prompt is waited on. Both lanes render `WEAVER_STARTUP_IOPS=50000`
  so Weaver's startup disk probe never runs inside the measured process (an
  operator value already in the environment is preserved and recorded).
- Copy the immutable plan and the generated fixture / NZB directories to each
  native host; do not regenerate the corpus per OS. The fixture manifest and
  output hashes are the cross-host equivalence check.
- Native Instruments / ETW traces are useful attribution artifacts, not a
  cross-product CPU metric; keep them apart from the benchmark JSON.

## What is measured

Every adapter renders and saves the client's configuration before starting it.
In the primary mode all three clients are driven through their public local
control API — Weaver's GraphQL service, SABnzbd `addfile`, NZBGet JSON-RPC
`append` — and each client image is `image@sha256:<digest>`. The fixture
password is passed only for fixtures whose manifest says encryption requires
it; no client gets a fixture-specific fast path. The container log is kept in
the run's config directory.

Per run the artifact records:

- `submission_to_terminal_nanoseconds` — **the primary timing**: from
  immediately before the API submission to the client's observed terminal
  state. `fixture_wall_clock_nanoseconds` (acceptance to terminal) and
  `processing_wall_clock_nanoseconds` (first observed active state to
  terminal) are secondary; a fixture that goes terminal before an active state
  is ever observed invalidates the suite rather than reporting queue latency as
  work. `status_poll_interval_nanoseconds` is the observation bound.
- Independent output verification — every expected file must pass size and
  BLAKE3 checks or the run is ineligible. Verification runs after both
  timestamps and is not charged to any product; its duration is recorded
  separately.
- `cpu_time_nanoseconds` — Docker lane: the container cgroup CPU counter from
  fresh-container creation to terminal (cold startup included, the Go
  controller excluded). Native lanes: the launched process's user + system time
  (`client_process` scope), never promoted to a whole-tree value. Do not divide
  this cold-scope counter by the narrower primary wall clock.
- `instructions_retired` — Docker lane on native Linux: cgroup-scoped
  `perf stat -a -G … -e instructions` over the same interval, raw output kept as
  `config/perf-instructions.txt`. Where `perf` cannot attach (Docker Desktop,
  most macOS setups) and on native lanes it is recorded as `unavailable` with a
  reason — never as zero, never omitted.

Every counter carries its scope, collector and collector version, so results
compare like for like instead of treating an unavailable hardware counter as a
score.

## TLS policy

TLS and plaintext runs use the same articles, NZB, server limits and client
connection count. The server's generated CA is mounted into the Docker Weaver
and NZBGet runs and given to native launchers as an explicit path; those TLS
results are labelled `tls-ca-verified`.

SABnzbd is the explicit exception: its local-CA trust path is not reliable in
this harness, so its TLS adapter uses `ssl=1` with `ssl_verify=0` and its
results are labelled **`tls-unverified`**, never `tls-ca-verified`. This is
confined to the isolated benchmark network and measures encrypted transport,
not authenticated TLS; the setting appears in every rendered SABnzbd config and
result artifact.

See [`configs/clients/baseline.json`](configs/clients/baseline.json) for the
cross-client baseline and
[`configs/adapters.example.json`](configs/adapters.example.json) for the
digest-pinned catalog shape.

## What this does not claim

- That any fixture mix is the statistical distribution of Usenet.
- That cross-posted groups are independent observations.
- That Docker / Linux, native macOS and native Windows telemetry can be pooled
  into one CPU or instruction ranking — target and collector scope stay
  first-class result dimensions.
- Any client result without its fixture manifest, client version or image
  digest, effective configuration, plan and output-hash record.
- That the client matrix is exhaustive; clients outside the catalog are simply
  not measured.
