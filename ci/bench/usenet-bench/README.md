# Reproducible Usenet client benchmark

This is a product-neutral benchmark foundation for Weaver, SABnzbd, and
NZBGet. It intentionally does not infer a “typical Usenet release.” Instead,
it generates a declared matrix of clean and deliberately repairable
multi-volume RAR fixtures and reports which combinations each client handles
well.

Generated archives, posted articles, downloaded data, and benchmark results
are ignored by Git. The checked-in matrix, source-locked toolchains, NZB
creation command, rendered client configuration, and result artifacts are the
reproducibility record.

## What the initial matrix covers

The base cases each have four inner files and are split into 32 MiB RAR
volumes. They cover the source-locked RARLAB 3.x–7.x writer eras and their
declared archive families across:

| Axis | Values |
| --- | --- |
| Writer era | RAR 3.93, 4.20, 5.00, 6.24, 7.23 |
| Archive lane | RAR3, RAR4, or RAR5; RAR 6/7 writers emit the RAR5 family |
| Compression | store (`-m0`), normal (`-m5`) |
| Solidity | non-solid, solid |
| Encryption | none, data encryption, encrypted headers |
| Input data | incompressible, moderately compressible |

That is 120 generic fixtures. Fifteen additional `bluray-disc` fixtures
exercise every writer-era/encryption combination with a
declared disc-shaped file topology: one 1 GiB `BDMV/STREAM/00000.m2ts` file,
512 128 KiB playlist/clip-info/metadata-shaped files, normal compression, and
solid archives. The generator accepts
`--bluray-large-file-bytes`, `--bluray-small-file-count`, and
`--bluray-small-file-bytes` so smoke runs can scale that shape down without
changing what it represents. These are intentionally synthetic—not a claim
that they are byte-for-byte Blu-ray images or statistically typical posts.

Fourteen separately declared repair fixtures add deterministic corruption
without multiplying every clean case. RAR3, RAR4, and RAR5 each receive all
four profiles; two RAR5 `bluray-disc` cases exercise the heavy profiles:

| Profile | Posted repair material | Deliberate fault |
| --- | --- | --- |
| `par2-light` | PAR2 at 10% redundancy | 128 deterministic byte flips in one non-leading RAR volume |
| `par2-heavy` | PAR2 at 35% redundancy | two complete non-leading RAR volumes absent |
| `rar-recovery-volume-light` | one RAR recovery volume (`.rev` or legacy equivalent) | one non-leading RAR volume absent |
| `rar-recovery-volume-heavy` | two RAR recovery volumes | two non-leading RAR volumes absent |

These profiles are compatibility and repair workloads, not a population claim.
The source-locked `par2cmdline` 0.8.1 image creates PAR2 material from its
verified source archive. Before a repair fixture is accepted, the generator
copies its posted input to a temporary verification directory, performs the
declared repair with the pinned PAR2 or RARLAB tool, and RARLAB-tests the
reconstructed archive against the original payload oracle. The deliberately
damaged input is retained; the verification copy is discarded.

`writer_era` is deliberately separate from `rar_format`: RAR 6 and 7 are
writer releases, not separate RAR6/RAR7 on-disk format names. The old 3.93
and 4.20 RARLAB Docker images use their native legacy defaults; RAR 5.00,
6.24, and 7.23 receive the explicit `-ma5` selector. This is a compatibility
matrix, not a claim to model exotic compressors or every historical quirk.

The source lock uses official RARLAB Linux releases 3.93, 4.20, 5.00, 6.24,
and 7.23. Their SHA-256 values are verified inside the Docker build; the old
releases intentionally select RARLAB's included `rar_static` binary so the
container remains reproducible on an amd64 host. Add an older writer release
only when it has a legal, source-locked RARLAB artifact; do not pull historical
binaries from an arbitrary mirror. RARLAB's tool and format documentation are
the source of truth for its archive capabilities.

## Generate fixtures

Run from this directory:

```bash
go run ./cmd/fixturegen --list

# A real benchmark-sized case (four 64 MiB files by default).
go run ./cmd/fixturegen \
  --fixture modern-rar5-normal-solid-headers-compressible \
  --output /scratch/nntp-bench-fixtures

# A disc-topology smoke case at a deliberately reduced scale.
go run ./cmd/fixturegen \
  --fixture modern-rar5-bluray-normal-solid-headers-incompressible \
  --bluray-large-file-bytes 256MiB \
  --bluray-small-file-count 64 \
  --bluray-small-file-bytes 32KiB \
  --output /scratch/nntp-bench-fixtures
```

Every generated fixture contains:

- `archive/` — the exact NNTP input to post: RAR volumes plus any PAR2 or
  RAR recovery-volume files. Deliberately missing RAR volumes are not posted;
  their pre-corruption digests remain in the manifest.
- `fixture-manifest.json` — selected RAR flags, source-locked generator,
  `repair` profile/fault metadata, source and posted-file SHA-256 digests, and
  the extracted-output SHA-256 oracle.

The temporary source payload is removed after RARLAB successfully tests the
archive. The manifest is sufficient to validate final client output without
storing a duplicate unpacked payload.

The generator refuses to overwrite an existing fixture directory. This avoids
silently replacing the data behind a published result.

## Build the local NNTP image

`e2e-nntp` publishes source and version tags, not a service image. Build the
local image before bringing up the shaper topology. `--provenance` is required
and must point inside the benchmark artifact root; it records the exact local
tag, image ID, target platform, binary SHA-256, and either an exact module
version or the redacted `source-directory` label.

```bash
# Private development checkout.
go run ./cmd/nntpbench image build \
  --source-dir "$E2E_NNTP_SOURCE" \
  --tag e2e-nntp:local \
  --provenance /scratch/nntp-bench-runs/nntp-image-provenance.json

# After a public e2e-nntp release, use an immutable module version instead.
go run ./cmd/nntpbench image build \
  --version v0.1.0 \
  --tag e2e-nntp:v0.1.0 \
  --provenance /scratch/nntp-bench-runs/nntp-image-provenance.json
```

Choose one of the two source modes; the provenance path deliberately refuses
to overwrite an earlier record. Never commit that JSON or a local source path.
The Compose topology uses `pull_policy: never`, so it can only start the tag
built by this command. If using a tag other than `e2e-nntp:local`, export it
as `E2E_NNTP_IMAGE` before running Compose.

## Post with Nyuu

Nyuu, not a custom poster, makes standard yEnc articles and the NZB. Start the
local server/shaper topology below, then post to its private upstream Docker
network so corpus setup is never routed through the bandwidth shaper.

```bash
go run ./cmd/nntpbench seed \
  --fixture-dir /scratch/nntp-bench-fixtures/modern-rar5-normal-solid-headers-compressible \
  --run-id 2026-08-02-a \
  --network nntp-bench_nntp_upstream \
  --nntp-host nntp-upstream \
  --username "${NNTP_BENCH_USERNAME:-fixture-user}" \
  --password-file "$NNTP_BENCH_PASSWORD_FILE"
```

The pinned Nyuu image is built as `linux/amd64`; this is intentional because
Nyuu's native `yencode` module does not build on arm64 Alpine. Docker emulates
it on Apple Silicon. Posting is corpus setup, never a performance metric, and
uses plaintext port 119 once. The same persisted articles are subsequently
downloaded over port 119 and implicit TLS port 563.

## Simulate the NNTP link at the server side

`nntpshaper` is a transparent TCP proxy that gives all client connections one
aggregate server-egress budget. It shapes only response bytes, so it counts
the NNTP protocol and article payload actually delivered to the client—not an
idealized payload size—and leaves client commands unthrottled. TLS is passed
through without termination; verified clients still validate the E2E server's
certificate.

Create an immutable environment file for the named profile, then pass it to
the example topology. The NNTP server is on a private upstream network; all
benchmark clients resolve `nntp` to the shaper on the benchmark network.

```bash
go run ./cmd/nntpbench server-env \
  --server-link 1gbit \
  --output /scratch/nntp-bench-runs/server-1gbit.env

NNTP_BENCH_PASSWORD_FILE=/scratch/nntp-bench-runs/nntp-password \
docker compose \
  -p nntp-bench \
  --env-file /scratch/nntp-bench-runs/server-1gbit.env \
  -f configs/server/compose-shaper.example.yml up --build -d

# Preserve the generated test CA with the rest of the run inputs for verified
# TLS clients. The persistent named volume is not an artifact record.
NNTP_BENCH_PASSWORD_FILE=/scratch/nntp-bench-runs/nntp-password \
docker compose -p nntp-bench \
  --env-file /scratch/nntp-bench-runs/server-1gbit.env \
  -f configs/server/compose-shaper.example.yml \
  cp nntp:/certs/ca.pem /scratch/nntp-bench-runs/nntp-ca.pem
```

`1gbit` is exactly 1,000,000,000 bit/s and `10gbit` is exactly
10,000,000,000 bit/s, each with a declared 1 MiB aggregate burst. `unlimited`
uses zero rate and burst; `custom` requires both values explicitly. The same
link profile is persisted in the plan, every run, and the adapter result. Use
the upstream network for Nyuu corpus setup so posting is never accidentally
part of the download measurement.

## Build a fair client schedule

```bash
go run ./cmd/nntpbench plan \
  --fixtures modern-rar5-normal-solid-headers-compressible,classic-rar4-store-nonsolid-none-incompressible \
  --archive-toolchains vanilla,rarpar \
  --server-link 10gbit \
  --repetitions 5 \
  --seed 20260802 \
  --output /scratch/nntp-bench-runs/plan.json
```

The default plan keeps the full **stock** 3×3 client-by-packaging matrix for
every `(fixture, transport, repetition)` tuple:

| Client | `docker-linux` | `macos-native` | `windows-native` |
| --- | --- | --- | --- |
| Weaver | pinned Linux image, one-shot CLI | local production CLI | local production CLI |
| SABnzbd | pinned Linux image, public API | native distributable, public API | native distributable, public API |
| NZBGet | pinned Linux image, public API | native executable, JSON-RPC | native executable, JSON-RPC |

It also adds two Docker-only Rarpar lanes, recorded as
`archive_toolchain=rarpar`: SABnzbd+Rarpar and NZBGet+Rarpar. The Docker
target therefore has five lanes; macOS and Windows retain three vanilla lanes
each. This distinction is intentional. The stock native SABnzbd and NZBGet
distributions bundle or link part of their archive stack and do not expose the
same replacement points. Repackaging them just to force Rarpar would not be a
native stock-package comparison.

`archive_toolchain` is a first-class plan, adapter, rendered-config, and result
field. Never pool `vanilla` and `rarpar` rows. Use
`--archive-toolchains vanilla` for a stock-only plan; the `vanilla,rarpar`
default is the side-by-side configuration.

Use `--targets docker-linux` to create a Docker-only plan while iterating.
Otherwise the saved plan includes all three target IDs; each host later runs
only its own ID. The plan randomizes target/fixture/transport/repetition
blocks and client/toolchain lanes within each block. It is sequential, so
clients never share the server's bandwidth in a timed run. The persisted plan
is the exact run order, client profile, archive toolchain, server-link
contract, and packaging target used for reporting.

## Run queue suites (primary measurement)

Build the Docker adapter once. `queue` starts one fresh client container per
client/toolchain/transport/repetition lane, submits every fixture NZB to that
client before waiting for any completion, and verifies every output under the
same queue directory. Client lanes remain sequential, so timed competitors do
not share server bandwidth; fixture jobs within one lane deliberately share
the client's normal queue, cache, and transition behaviour.

```bash
go build -o /scratch/nntp-bench-bin/clientadapter ./cmd/clientadapter

# Copy the catalog, set the adapter path and Compose network name, and change
# images only by replacing their full digest. A floating tag is rejected.
cp configs/adapters.example.json /scratch/nntp-bench-runs/adapters.json

go run ./cmd/nntpbench queue \
  --plan /scratch/nntp-bench-runs/plan.json \
  --adapters /scratch/nntp-bench-runs/adapters.json \
  --target docker-linux \
  --fixtures-root /scratch/nntp-bench-fixtures \
  --artifacts /scratch/nntp-bench-runs/artifacts \
  --nntp-host nntp \
  --tls-ca-file /scratch/nntp-bench-runs/nntp-ca.pem \
  --username "${NNTP_BENCH_USERNAME:-fixture-user}" \
  --password-file "$NNTP_BENCH_PASSWORD_FILE"
```

`run` is retained as a separately labelled cold, one-NZB diagnostic. It must
not be used as the headline queue-throughput result.

### Rarpar-backed Docker lanes

The two `rarpar` catalog entries require a **published**, platform-matching
Rarpar release binary—not a local source build and not a floating container
tag. Download the `linux/amd64` Docker-compatible release artifact, retain the
download and its release checksum with the benchmark inputs, and fill in all
three fields in the copied catalog:

```json
"CLIENT_RARPAR_BINARY": "/scratch/rarpar/rarpar-linux-amd64",
"CLIENT_RARPAR_VERSION": "0.2.5",
"CLIENT_RARPAR_SHA256": "<lowercase 64-character release digest>"
```

Before starting a timed run, the adapter checks that executable's SHA-256,
then copies it into the fresh per-run config directory. The declared release
version and verified digest become `rarpar <version> sha256:<digest>` in the
result; this works when the Docker artifact's Linux architecture cannot execute
on the host. The result deliberately does not retain the host binary path.

SABnzbd discovers its external `unrar` and `par2` commands through `PATH`.
For the Rarpar lane, the adapter stages a `unrar` compatibility shim and a
narrow `par2 r` shim that calls `rarpar par repair`; the release binary remains
named `rarpar`. NZBGet does not provide a `Par2Cmd` setting—its stock PAR2
engine is linked internally. Its Rarpar lane therefore explicitly disables
that internal unpack/PAR route and runs a tracked NZBGet post-processing
extension that performs `rarpar par repair`, then `rarpar rar extract`. This
is why the lane is labelled `rarpar`, not falsely described as stock NZBGet's
internal PAR2 implementation.

The base corpus remains clean, while the explicitly named repair fixtures post
PAR2 sidecars or RAR recovery volumes with declared damage. Report repair rows
by `repair.profile` and `archive_toolchain`; never pool them with clean
extraction rows or imply that their mix measures Usenet prevalence.

For native targets, build the host-local launcher and use a separate adapter
catalog and artifact root for each operating system:

```bash
# macOS: run this on the Mac host.
go build -o /scratch/nntp-bench-bin/nativeadapter ./cmd/nativeadapter
cp configs/adapters.macos.example.json /scratch/nntp-bench-runs/adapters.macos.json

go run ./cmd/nntpbench run \
  --plan /scratch/nntp-bench-runs/plan.json \
  --adapters /scratch/nntp-bench-runs/adapters.macos.json \
  --target macos-native \
  --fixtures-root /scratch/nntp-bench-fixtures \
  --artifacts /scratch/nntp-bench-runs/artifacts-macos \
  --nntp-host <server-address-reachable-from-macos> \
  --tls-ca-file /scratch/nntp-bench-runs/nntp-ca.pem \
  --username "${NNTP_BENCH_USERNAME:-fixture-user}" \
  --password-file "$NNTP_BENCH_PASSWORD_FILE"

# Windows: cross-build on macOS or build natively on the Windows host, then
# run nntpbench and nativeadapter on that host with the Windows catalog.
GOOS=windows GOARCH=amd64 go build -o nativeadapter.exe ./cmd/nativeadapter
GOOS=windows GOARCH=amd64 go build -o nntpbench.exe ./cmd/nntpbench
```

Before timing either native lane, use the non-mutating target preflight. It
prints a machine-readable allowlist of the expected local executables and
fails if the OS or a required binary is missing:

```bash
go run ./cmd/nntpbench preflight \
  --target macos-native \
  --adapter /scratch/nntp-bench-bin/nativeadapter \
  --weaver /absolute/path/to/weaver \
  --nzbget /absolute/path/to/nzbget
```

`adapters.macos.example.json` uses the installed
`/Applications/SABnzbd.app/Contents/MacOS/SABnzbd` distributable, as required
for native macOS runs. It intentionally leaves the Weaver and NZBGet paths as
explicit replacements. The Windows catalog likewise uses explicit executable
paths. `NATIVE_LAUNCH_COMMAND` is a JSON argv array, never a shell string; it
may use `{{config_dir}}`, `{{nzb_path}}`, `{{output_dir}}`, and
`{{fixture_dir}}` placeholders. Native SABnzbd/NZBGet commands must stay in
the foreground so the launcher can collect their process CPU time and cleanly
stop them after terminal completion.

For native macOS Weaver runs, set `WEAVER_ENCRYPTION_KEY` in the adapter
environment so the benchmark never waits on a Keychain prompt. Native
Instruments traces are useful attribution artifacts, but not a cross-product
headline CPU metric; preserve them separately from the benchmark JSON.

Copy the immutable plan and the generated fixture/NZB directories to each
native host before execution, while keeping each host's artifacts local. The
fixture manifest and output hashes are the cross-host equivalence check; do
not regenerate the corpus independently per operating system.

For CA-verified native TLS, give the E2E server certificate a stable DNS SAN
that is reachable through the shaper from both hosts (for example,
`nntp.bench.test`), map that name to the shaper address, and pass the same
name with `--nntp-host`. Do not replace it with an IP literal on a
`tls-ca-verified` run: NZBGet strict verification checks the certificate
hostname as well as its issuing CA.

The example Compose topology publishes the shaper only on `127.0.0.1` by
default. For the remote Windows lane, set `NNTP_PUBLIC_BIND_ADDR` to the
benchmark server's specific LAN address, restrict it with the host firewall to
the Windows benchmark host, and map the verified DNS name there. Do not expose
the synthetic service broadly.

The configured Windows benchmark host currently has Git/Rust available but no
SABnzbd or NZBGet executable on `PATH`; the suite deliberately does not
install products implicitly. Stage pinned native installers, record their
versions and hashes in the catalog, and use an isolated working directory such
as `C:\bench` before running the Windows lane.

The Docker adapter renders and saves a product config before starting the
container. In the primary queue mode all three clients use their public local
control API: Weaver's local GraphQL service, SABnzbd `addfile`, and NZBGet
JSON-RPC `append`. The cold diagnostic keeps Weaver's one-shot
`weaver download … --report … --report-ack …` path. The adapter passes the
public fixture password only for fixtures whose manifest says encryption
requires it; no client is given a fixture-specific fast path.

The Docker Weaver catalog must point at an image built after the CLI report
options in this branch are released. Do not substitute a floating tag or run a
pre-report image: the adapter will preserve the exact digest and fail rather
than silently falling back to the HTTP service.

Queue artifacts record the first successful submission, every per-NZB accepted
and terminal timestamp, the final client completion, and final independent
output verification. `queue_wall_clock_nanoseconds` is first submission to
the final client completion; `verified_wall_clock_nanoseconds` extends that
same start point through the output oracle for every NZB. The latter is the
headline end-to-end value. CPU time and retired instructions cover the same
uninterrupted client process. The cold Weaver CLI report and acknowledgement
remain a measurement handshake for the diagnostic mode only.

Each client image must use `image@sha256:<digest>`, and its resolved image
identity/version, archive-toolchain provenance, and rendered-config SHA-256
appear in the result. The adapter keeps the product container log in the run
config directory before it is removed.

The generic runner independently validates completion with:

```bash
go run ./cmd/nntpbench verify-output \
  --fixture-dir /scratch/nntp-bench-fixtures/modern-rar5-normal-solid-headers-compressible \
  --output-dir /scratch/nntp-bench-runs/run-0001/complete
```

The primary timing is **usable output**: all expected unpacked files must pass
size and SHA-256 verification. The adapter records wall time from successful
queue acceptance to the client's terminal completion observation (the Weaver
CLI report or the SABnzbd/NZBGet API). The runner accepts that endpoint only
after the output oracle succeeds; download-only time cannot replace this
outcome.

## CPU time and retired instructions

Every adapter result has two separate counter records, each with a scope,
collector, collector version, and either a measured value or an explicit
unavailability reason:

- `cpu_time_nanoseconds` is sampled from the client container's cgroup CPU
  counter immediately after fresh-container creation and at terminal
  completion. It deliberately includes cold client startup for every product,
  but never the Go benchmark controller.
- `instructions_retired` is collected over that same cold-container-to-terminal
  interval with native-Linux cgroup `perf stat -a -G … -e instructions` when
  permission and hardware counters allow it. Its scope is
  `client_container`: all client and unpacker child processes in the isolated
  Docker cgroup are included, while the Go controller remains outside it. The
  raw `perf` output is retained as `config/perf-instructions.txt` for audit.

For `macos-native` and `windows-native`, `cpu_time_nanoseconds` is the native
launcher process's user-plus-system CPU time (`client_process` scope), captured
after the foreground client exits. It is not silently promoted to a whole
process-tree value. `instructions_retired` is explicitly recorded as
`unavailable` until the native launcher has a validated platform collector
(macOS Instruments/Windows ETW collection needs target-specific calibration).
That preserves the result schema and prevents a made-up zero from becoming a
cross-platform score.

On Docker Desktop/macOS and other hosts where `perf` cannot safely attach, the
instruction counter is recorded as `unavailable` with the reason. It is never
reported as zero or omitted. Results should compare like-for-like execution
targets, telemetry scopes, and collector availability rather than treating
unavailable hardware counters as a performance result.

## TLS policy

TLS and plaintext use the same articles, NZB, server concurrency limits, and
client connection count. The local source-built NNTP server's generated CA is
copied into the benchmark artifact root, mounted into Docker Weaver/NZBGet
runs, and supplied as an explicit local path to native launchers; their TLS
results are labelled `tls-ca-verified`.

SABnzbd is the explicit exception: its local-CA trust path is not reliable in
this harness, so its TLS adapter uses `ssl=1` and `ssl_verify=0`. Its results
are labelled **`tls-unverified`**, never `tls-ca-verified`. This is limited to
the isolated Docker benchmark network and measures encrypted transport
throughput, not authenticated TLS. The setting must appear in every rendered
SABnzbd config and result artifact.

See [`configs/clients/baseline.json`](configs/clients/baseline.json) for the
auditable cross-client baseline and
[`configs/adapters.example.json`](configs/adapters.example.json) for the
digest-pinned adapter catalog shape and its explicit Rarpar release pin.

## What this does not claim

- It does not claim a fixture mix is the statistical distribution of Usenet.
- It does not treat cross-posted groups as independent observations.
- It does not pool Docker/Linux, native macOS, and native Windows telemetry
  into one CPU or instruction ranking; execution target and collector scope
  remain first-class result dimensions.
- It does not report a client result without the exact fixture manifest,
  client version/image digest, effective configuration, plan, and output hash
  record.
- It does not include NZBFast in the client matrix.
