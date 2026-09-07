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
  - [The 7z lane](#the-7z-lane)
  - [The Blu-ray disc topology](#the-blu-ray-disc-topology)
  - [Repair profiles](#repair-profiles)
  - [Posting order](#posting-order)
  - [Pinned 7-Zip writer](#pinned-7-zip-writer)
- [Step by step](#step-by-step)
  - [1. Generate fixtures](#1-generate-fixtures)
  - [2. Build the NNTP server image](#2-build-the-nntp-server-image)
  - [3. Start the server and shaper](#3-start-the-server-and-shaper)
  - [4. Post the corpus with Nyuu](#4-post-the-corpus-with-nyuu)
  - [5. Write a plan](#5-write-a-plan)
  - [6. Run the sequential suite](#6-run-the-sequential-suite)
  - [7. Summarize](#7-summarize)
- [Driving a whole session](#driving-a-whole-session)
- [Pre-seeded NNTP corpus image](#pre-seeded-nntp-corpus-image)
- [Raw stack: the server side without Docker](#raw-stack-the-server-side-without-docker)
- [Native macOS and Windows lanes](#native-macos-and-windows-lanes)
- [Storage profiles (local vs throttled NFS)](#storage-profiles-local-vs-throttled-nfs)
- [What is measured](#what-is-measured)
- [TLS policy](#tls-policy)
- [What this does not claim](#what-this-does-not-claim)

## Requirements

| Requirement | Why |
| --- | --- |
| Go 1.26+ | every tool here is `go run ./cmd/…`; the module has one dependency (`zeebo/blake3`) |
| Docker with Compose v2 | the RARLAB, PAR2, Nyuu, NNTP-server and shaper images, and the Docker client lane. Fixtures and the seeded corpus are always made on a Docker host; measuring on macOS or Windows does not need one (see [Raw stack](#raw-stack-the-server-side-without-docker)) |
| `linux/amd64` emulation on arm64 hosts | the RARLAB and Nyuu images are `linux/amd64` on purpose (see below); Docker Desktop provides it, on plain Linux run `docker run --privileged --rm tonistiigi/binfmt --install amd64` |
| ~10 GiB free disk | a full corpus with the `bluray-disc` fixture; smoke runs need far less |
| Native product installs | only for the optional `macos-native` / `windows-native` lanes |

Every command below is run from this directory. Paths under `/scratch/…` are
placeholders — use any directory outside the repository.

## Quickstart

The Docker-only smoke path, end to end:

```bash
# 1. Two fixtures: one clean RAR5 case and one raw-MKV case for the queue benchmark.
go run ./cmd/fixturegen --fixture rar5-7-store-store-nonsolid-none-incompressible --output /scratch/fixtures
go run ./cmd/fixturegen --direct-mkv --output /scratch/fixtures

# 2. Build the pinned NNTP server image from the published module.
go run ./cmd/nntpbench image build --version v0.1.0 --tag e2e-nntp:local \
  --provenance /scratch/runs/nntp-image-provenance.json

# 3. Bring up server + shaper at a declared link rate; keep the test CA. (A
#    provider-distance round trip is a separate small sweep, see "Fixed round trip".)
openssl rand -hex 24 > /scratch/runs/nntp-password
go run ./cmd/nntpbench server-env --server-link 1gbit --output /scratch/runs/server-1gbit.env
NNTP_BENCH_PASSWORD_FILE=/scratch/runs/nntp-password docker compose -p nntp-bench \
  --env-file /scratch/runs/server-1gbit.env -f configs/server/compose-shaper.example.yml up --build -d
NNTP_BENCH_PASSWORD_FILE=/scratch/runs/nntp-password docker compose -p nntp-bench \
  --env-file /scratch/runs/server-1gbit.env -f configs/server/compose-shaper.example.yml \
  cp nntp:/certs/ca.pem /scratch/runs/nntp-ca.pem

# 4. Post the fixture over the private upstream network (never through the shaper).
go run ./cmd/nntpbench seed --fixture-dir /scratch/fixtures/rar5-7-store-store-nonsolid-none-incompressible \
  --run-id smoke-1 --network nntp-bench_nntp_upstream --nntp-host nntp-upstream \
  --username fixture-user --password-file /scratch/runs/nntp-password

# 5. Plan, 6. run, 7. summarize.
go run ./cmd/nntpbench plan --fixtures rar5-7-store-store-nonsolid-none-incompressible \
  --archive-toolchains vanilla --profile equivalent-throughput --server-link 1gbit --repetitions 20 --seed 1 \
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
| `cmd/fixturegen` | Generates the RAR / 7z / PAR2 / recovery-volume fixtures on the pinned toolchain images |
| `cmd/nntpbench` | The controller: `image build`, `seed`, `seed-image`, `server-env`, `storage-env`, `plan`, `sequential`, `queue-transition`, `run`, `summarize`, `preflight`, `verify-output`, `delete-output` |
| `cmd/clientadapter` | Docker lane: starts one fresh, digest-pinned client container per run and drives its public API |
| `cmd/nativeadapter` | macOS / Windows lane: launches one fresh native client process per run |
| `cmd/nntpshaper` | Transparent TCP proxy that meters the server's egress under an exclusive run lease |
| `configs/adapters*.example.json` | Digest-pinned client catalogs for the Docker, macOS and Windows lanes — copy, then edit |
| `configs/clients/baseline.json` | The cross-client configuration baseline every rendered config is derived from |
| `configs/server/compose-shaper.example.yml` | The server + shaper topology |
| `configs/server/compose-nfs.example.yml` | The throttled NFS export used by the `nfs-*` storage profiles |
| `configs/server/compose-seeded.example.yml` | Overlay that starts the NNTP server from a pre-seeded corpus image |
| `configs/chains/*.example.json` | Whole-session descriptions: `latency-series` on the Docker stack, `raw-native` on the raw one |
| `docker/` | Dockerfiles for the pinned RARLAB writers, the official 7-Zip build, `par2cmdline-turbo`, Nyuu, the shaper and the throttled NFS server |
| `fixtures/matrix.json`, `fixtures/corpus.json` | The declared fixture matrix and corpus description |
| `internal/` | The Go packages behind the commands |

## The fixture matrix

Every set in `fixtures/matrix.json` declares a `class`, and the class travels
from the matrix through each fixture's manifest into every run artifact and the
summary:

| Class | What it stands for | Sets |
| --- | --- | --- |
| `headline` | The common shape of a real post: a stored (`-m0`), multi-volume RAR of already-compressed media. One clean set per source-locked RARLAB writer era (3.93, 4.20, 5.00, 6.24, 7.23), each posted clear, with encrypted headers (`-hp`) and with data-only encryption (`-p`), because most real posts are not encrypted and the encrypted forms are measured beside the clear one, never instead of it. PAR2 repair is part of the common case too: the stored form from the 4.20, 5.00 and 7.23 writers is posted in the same three forms with light damage (`par2-light`) and with an interior volume listed in the NZB but never posted (`par2-heavy-withheld`). | 8 sets, 33 fixtures |
| `breadth` | Shapes a client meets less often and must still handle: a four-movie stored set with RAR5 quick-open records, the official 7-Zip 7z container, a stored Blu-ray-shaped topology in scattered NZB order, PAR2 over 7z, and RAR recovery volumes. RAR compression is deliberately absent: already-compressed media is not recompressed in the wild, so a compressed lane would measure a shape nobody posts. | 7 sets, 7 fixtures |

The summarizer pools per-fixture results only within a class (see
[Summarize](#7-summarize)); the headline aggregate is the figure for the common
case and the breadth aggregate is the compatibility figure. Fixture counts
inside a class are coverage choices, not a model of what is posted to Usenet:
the corpus does not claim a population distribution, and that is why the two
classes are never pooled with each other. Every fixture posts at least 100 MiB
of archive; the
generator refuses to write a smaller one and the controller refuses to run it,
because a smaller download finishes inside the clients' start-up and settle
time and the comparison would measure process launch rather than the
pipeline. Ordinary incompressible cases contain one 150 MiB synthetic video
file split into 32 MiB archive volumes (the withheld-volume sets post one
volume fewer than they list, which is what the floor's headroom is for); the
multi-input cases contain four 40 MiB videos. Compressible cases contain one
270 MiB raw-video file carrying
four bits of deterministic per-sample noise (recorded in the manifest as
`sample_noise_bits`), which the pinned writers compress to roughly 62 %
(LZMA2) to 70 % (RAR -m5) of its size — so the compression lanes still
compress, and the archive still clears the floor. The floor is enforced on the
posted bytes, so a repair fixture's PAR2 volumes count and its withheld
volume does not.
Together they cover the RARLAB writer eras and their archive families across:

| Axis | Values |
| --- | --- |
| Writer era | RAR 3.93, 4.20, 5.00, 6.24, 7.23 and 7-Zip 26.02 (official upstream Linux releases, SHA-256 verified in the image build) |
| Archive format | legacy RAR4 (3.93 / 4.20 writers), RAR5 (5.00 / 6.24 / 7.23 writers, explicit `-ma5`), or 7z (official 7-Zip build) |
| Compression | store (`-m0`) or release-style normal compression (`-m5`, solid where declared, maximum dictionary, RAR5 quick-open disabled except on the `rar5-7-quickopen` set, which keeps a quick-open record for every header) |
| Solidity | non-solid, solid |
| Encryption | none, data encryption, encrypted headers |
| Input data | incompressible, moderately compressible |

That yields 16 clean RAR fixtures: the 15 headline stored lanes (five writer
eras, each clear, header-encrypted and data-encrypted) and the four-movie
quick-open set. Every RAR lane is stored: the generator still writes
release-style `-m5` compression and the compressible payload, but the
checked-in matrix uses neither, because already-compressed media is not
recompressed in the wild. `writer_era` is deliberately separate from
`archive_format`: RAR 6 and 7 are writer releases, not new on-disk formats.

### The 7z lane

`archive_format` also takes `7z`, written by the official 7-Zip console build
(see [Pinned 7-Zip writer](#pinned-7-zip-writer)). A set that uses it names the
writer with `archive_writer`; `generator_toolchain` still names the RARLAB
image, which supplies the FFmpeg payload renderer for every lane. Two clean
7z fixtures (stored, and stored with encrypted headers) and one 7z repair
fixture are in the corpus, all breadth:

| Axis | 7z values |
| --- | --- |
| Compression | store (`-mx0 -m0=Copy`) or LZMA2 at the writer default (`-mx5`) |
| Solidity | `-ms=off` / `-ms=on` |
| Encryption | none, data (`-p`), encrypted headers (`-p` with `-mhe=on`) |
| Volumes | `-v<volume_size>`, so members are `fixture.7z.001`, `.002`, … |

`rar-recovery-volume-*` is rejected at matrix validation for a 7z set: RAR
recovery volumes are a RAR container feature, and pairing them with 7z is an
authoring mistake, not a generation-time surprise. PAR2 works over a 7z volume
set exactly as it does over a RAR one.

Verification differs by lane because the writers differ. RAR fixtures are
tested with RARLAB's own `rar t`. 7z fixtures are *extracted* with the pinned
`7zz` and every extracted member is checked against the payload's BLAKE3
digest, because 7-Zip's own test does not prove the extracted bytes match the
oracle.

### The Blu-ray disc topology

One `bluray-disc` fixture exercises a disc-shaped topology in stored,
non-solid RAR5: a 5 GiB `BDMV/STREAM/00000.m2ts`, eight 96 MiB menu/extra
streams, four small menu streams and 508 tiny metadata members, split into
50 MiB volumes and posted in `scattered` NZB order. It is stored because disc
dumps are posted stored; the topology — one huge member beside hundreds of tiny
ones, arriving out of order — is what it measures.

`--bluray-large-file-bytes`, `--bluray-medium-file-bytes`,
`--bluray-medium-file-count`, `--bluray-small-file-count` and
`--bluray-small-file-bytes` scale it down for smoke runs without changing what
it represents. The small-file numbering runs `BDMV/` first and `CERTIFICATE/`
last, so a reduced `--bluray-small-file-count` writes a `BDMV`-only disc and
the archiver is handed only the roots that exist. It is synthetic; it is not a
Blu-ray image and not a claim about typical posts.

### Repair profiles

Six repair sets add deterministic damage without duplicating the clean
cases. Three are headline: PAR2 over the stored RAR from the 4.20, 5.00 and
7.23 writers, each posted clear, with encrypted headers and with data
encryption, under light damage and with the volume withheld, because a repair
is part of what a real post costs. The other three are breadth: RAR4 and RAR5
recovery volumes, and 7z PAR2 light. `par2-heavy` stays defined for a set
that wants the volume missing from the NZB itself; the checked-in matrix uses
the withheld form, which is what an incomplete post looks like on a server:

| Profile | Posted repair material | Deliberate fault |
| --- | --- | --- |
| `par2-light` | PAR2 at 10 % redundancy | 128 deterministic byte flips in one non-leading volume |
| `par2-heavy` | PAR2 at 35 % redundancy | one complete non-leading volume absent from the NZB |
| `par2-heavy-withheld` | PAR2 at 35 % redundancy | one non-leading volume listed in the NZB but never posted |
| `rar-recovery-volume-light` | one RAR recovery volume | one non-leading volume absent |
| `rar-recovery-volume-heavy` | two RAR recovery volumes | two non-leading volumes absent |

`par2-heavy` and `par2-heavy-withheld` describe the same missing data from two
different client viewpoints. Under `par2-heavy` the NZB never mentions the
volume, so the client knows from the start that it must repair. Under
`par2-heavy-withheld` the NZB lists the volume with article identifiers that
were never posted, so the client requests every article and is refused — which
is what an incomplete post looks like on a real server. The withheld volume's
bytes stay on disk in the fixture so the repair target is still auditable; the
manifest records them under `withheld_files` and the fault as
`kind: "withheld-volume"`.

PAR2 material comes from the pinned `par2cmdline-turbo` 1.4.0 image. Before a
repair fixture is accepted the generator copies its posted input aside —
excluding any withheld volume, so the repair genuinely exercises the recovery
capacity — performs the declared repair with the pinned PAR2 or RARLAB tool,
and checks the reconstructed archive against the payload oracle with the
writer's own reader. Only the damaged input is kept.

RAR bytes are only ever written by RARLAB's own `rar` and 7z bytes only by the
official 7-Zip build; add an older writer only when an official, source-locked
release exists for it. Nothing here pulls historical binaries from mirrors, and
no distribution fork stands in for an upstream writer.

### Posting order

`nzb_order` is a fixture-level axis with two values:

| Value | Meaning |
| --- | --- |
| `sequential` (default) | Files appear in the NZB in sorted volume order, with repair material trailing |
| `scattered` | Files appear in a deterministic pseudo-random permutation, with repair material interleaved among the volumes |

Real posts do not always arrive in volume order, and a client that schedules
work by what the archive needs behaves differently from one that follows the
NZB. `sequential` alone cannot tell those apart, because it hands every client
the convenient order for free. `scattered` removes that gift without
introducing run-to-run noise: no volume is guaranteed to arrive first, and the
first repair file always precedes the last archive volume.

The permutation is a function of the fixture id alone, so a corpus regenerated
or reseeded on another host produces the same order. The seed is the first
eight bytes of `sha256(fixture id)`, and the shuffle is an explicit
Fisher-Yates over SplitMix64 rather than a standard-library shuffle, because
the standard library makes no promise that its algorithms stay byte-stable
across Go releases and a published corpus has to reproduce its order years
later. A degenerate draw — one that still leads with the first volume, or still
leaves every repair file behind every volume — is redrawn deterministically,
and the accepted seed is recorded.

The manifest records `nzb_order`, `nzb_order_seed` and the complete
`nzb_file_order`. After the poster runs, the harness parses the emitted NZB and
asserts its `<file>` order matches that list, failing the seed loudly if it
does not: posting order is a measured axis, so a silent reordering would
invalidate every run over the fixture.

### Pinned 7-Zip writer

`docker/sevenzip/` builds the official 7-Zip Linux console binary from
`7-zip.org` into an image, verifying the archive against the SHA-256 in
`docker/sevenzip/toolchain.json` before installing it. The toolchain file is
the single source of the version, URL, digest and image tag, and it is copied
into every generated fixture's manifest as `archive_writer_toolchain`, so a
fixture states which writer produced it.

Distribution `p7zip` forks are deliberately not used: they are a different
codebase with a different container writer, so a fixture written by one is not
evidence about the other. The base image is the same digest-pinned Debian the
RARLAB and PAR2 images use.

```bash
docker build -f docker/sevenzip/Dockerfile \
  --build-arg SEVENZIP_URL=https://www.7-zip.org/a/7z2602-linux-x64.tar.xz \
  --build-arg SEVENZIP_SHA256=41aaba7b1235304ab5aa0624530c67ae829496cd29e875925271efdccc28c03e \
  --tag weaver-nntp-bench-7zip:26.02 docker/sevenzip
```

`fixturegen` builds it automatically when a selected fixture needs it.

The writer image is generation-side only. On the client side, NZBGet unpacks
7z through an external tool rather than a built-in decoder, so its rendered
config names one explicitly: `SevenZipCmd=/usr/bin/7zz` in the pinned
LinuxServer image, which ships an official 7-Zip build at that path, and
`SevenZipCmd=7z` for a native install, which resolves through the operator's
own PATH. Leaving it unset would let a 7z fixture fail as an NZBGet
configuration gap rather than measure anything. The setting is NZBGet-specific
and the SABnzbd and Weaver renders deliberately carry nothing equivalent, so
the added key cannot shift what the other two clients do.

## Step by step

### 1. Generate fixtures

`fixturegen` builds and runs the pinned RARLAB / PAR2 / 7-Zip images itself
(only the ones the selected fixtures need), and `--direct-mkv` uses the RARLAB
image for its deterministic FFmpeg payload, so Docker is required for every
invocation.

```bash
go run ./cmd/fixturegen --list

# One benchmark-sized movie case (270 MiB compressible payload by default; incompressible cases use 150 MiB).
go run ./cmd/fixturegen --fixture rar5-7-store-store-nonsolid-none-incompressible --output /scratch/fixtures

# A 7z case, written by the pinned official 7-Zip build.
go run ./cmd/fixturegen --fixture sevenzip-store-store-nonsolid-none-incompressible --output /scratch/fixtures

# The disc topology at smoke scale.
go run ./cmd/fixturegen --fixture rar5-7-bluray-store-nonsolid-none-incompressible \
  --bluray-large-file-bytes 256MiB \
  --bluray-medium-file-bytes 16MiB --bluray-medium-file-count 2 \
  --bluray-small-file-count 64 --bluray-small-file-bytes 32KiB \
  --output /scratch/fixtures

# The raw-download fixture the queue-transition benchmark uses.
go run ./cmd/fixturegen --direct-mkv --output /scratch/fixtures
```

Every fixture directory contains `archive/` — the exact bytes to post: archive
volumes plus any PAR2 or `.rev` files, with deliberately missing volumes absent
but their pre-damage digests kept — and `fixture-manifest.json` with the writer
flags, the toolchain that rendered the payload, the toolchain that wrote the
container, the repair profile, the posting order and its seed, any withheld
volumes, BLAKE3 digests of the source and posted files, and the
extracted-output oracle. The source payload is deleted once the writer's own
reader has verified the archive; the manifest is enough to verify client
output. The generator refuses to overwrite an existing fixture directory so the
data behind a published result cannot be replaced silently.

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

The server advertises RFC 4644 `PIPELINING` in its `CAPABILITIES` reply, as
commercial providers do (`NNTP_BENCH_PIPELINING=0` at `compose up` silences
it, which is a diagnostic, never a published configuration). The keyword
matters because the clients treat it differently: Weaver pipelines `BODY`
requests only against a server that advertises it and picks the depth from
the measured round trip, SABnzbd 5 sends the two requests per connection its
own new-server default declares, and NZBGet sends one request per connection.
A server that stays silent therefore benches every client one article per
round trip and hides exactly the difference a shaped round trip exists to
show. The Weaver adapters seed the server's pipelining flag from the
environment (`WEAVER_SERVER_1_PIPELINING=true`, Weaver 0.10.3 or newer),
because an environment-seeded server is never probed the way a server added
through Weaver's UI is.

#### Fixed round trip

The rate cap alone is a link with no distance: a client that opens one
connection per article, or waits for each reply before sending the next
command, pays nothing for it on the loopback path. `--server-rtt` (on
`server-env` and `plan` alike) declares a fixed client<->server round trip
that the shaper container renders with `tc netem` on its benchmark-facing
interface — half the round trip per direction, zero jitter — so a client's
connection setup, TLS handshake, command pipelining and per-connection window
all cost what they cost against a real provider. The value is a whole number
of milliseconds between 1 ms and 5 s; 0 (the default) adds no delay and
touches no qdisc, so a plan without the flag is byte-for-byte what it was
apart from the new `rtt_micros` field.

The round trip is not the headline. The published comparison is the full
fixture matrix at 0 ms, where the link, the decoder, the archive and the
repair are what differ between clients; a delayed link mostly measures how
many requests each client keeps in flight per connection, and it costs a
multiple of the loopback wall clock per run. So the round trip is a **small
sweep over a few fixtures**: one raw-MKV, one clean RAR, one light PAR2
repair and one heavy withheld PAR2 repair, at 50, 100 and 250 ms, TLS only,
five repetitions each. The round trip is part of the stratum, so every
point of the sweep is its own comparison, never pooled with the 0 ms matrix
or with another round trip; read the sweep as a curve of the ratio against
the round trip, alongside the 0 ms matrix as its origin. The 0 ms point of
the same four fixtures comes from the headline matrix, not from a fourth
sweep step. Each step restarts only the shaper (the server keeps its corpus
and its CA) and writes its own plan and artifact root:

```bash
SWEEP_FIXTURES=direct-mkv,rar5-7-store-store-nonsolid-none-incompressible,\
repair-rar5-7-store-par2-par2-light-store-nonsolid-none-incompressible,\
repair-rar5-7-store-par2-par2-heavy-withheld-store-nonsolid-none-incompressible
for rtt in 50ms 100ms 250ms; do
  go run ./cmd/nntpbench server-env --server-link 1gbit --server-rtt $rtt \
    --output /scratch/runs/server-1gbit-$rtt.env
  NNTP_BENCH_PASSWORD_FILE=/scratch/runs/nntp-password docker compose -p nntp-bench \
    --env-file /scratch/runs/server-1gbit-$rtt.env -f configs/server/compose-shaper.example.yml \
    up -d nntp-shaper
  go run ./cmd/nntpbench plan --fixtures $SWEEP_FIXTURES --transports tls \
    --archive-toolchains vanilla --profile equivalent-throughput \
    --server-link 1gbit --server-rtt $rtt --repetitions 5 --seed 20260802 \
    --targets docker-linux --output /scratch/runs/plan-rtt-$rtt.json
  go run ./cmd/nntpbench sequential --plan /scratch/runs/plan-rtt-$rtt.json \
    --adapters /scratch/runs/adapters.json --target docker-linux \
    --fixtures-root /scratch/fixtures --artifacts /scratch/runs/artifacts-rtt-$rtt \
    --nntp-host nntp --shaper-control-url http://127.0.0.1:8080 \
    --tls-ca-file /scratch/runs/nntp-ca.pem \
    --username fixture-user --password-file /scratch/runs/nntp-password
done
```

Restore the 0 ms shaper (`up -d nntp-shaper` with the plain link env file)
before the next headline run; the controller refuses a plan whose declared
round trip disagrees with the shaper's attestation, so a stale shaper fails
loudly rather than quietly shifting a stratum.

The env file carries `NNTP_RTT_MICROS`; the Compose service needs
`cap_add: [NET_ADMIN]` (already in the example topology) so the entrypoint
can program its own network namespace. The client-to-server half is delayed
on an `ifb` mirror of the interface; where the host kernel has no `ifb`
module (Docker Desktop, some minimal servers — `modprobe ifb` on a Linux bench
host) the entrypoint carries the whole round trip on the server-to-client
side instead, which keeps the sender-observed round trip exact but delivers
the client's commands and handshakes undelayed. Which layout ran is recorded,
not assumed: the shaper attests `link_shaping` (schema 4) with the interface,
the ingress mechanism (`ifb-netem` or `none`), the declared per-direction
split, the netem queue limit (sized from the rate and round trip so the
bandwidth-delay product never overflows it) and, re-read from `tc` for every
snapshot, the delays the qdiscs actually carry. The controller refuses a run
whose report disagrees with the plan, whose live delays drifted, or whose
shaper predates the schema; the summarizer applies the same checks to the
persisted before/after snapshots, and the run environment carries
`BENCH_SERVER_RTT_MICROS` so each adapter's rendered-config identity includes
it.

A delayed link also needs a send buffer that covers its bandwidth-delay
product, or the shaper's own kernel — not the declared link — caps every
connection at buffer / RTT. The example topology raises the shaper's
namespace-scoped `net.ipv4.tcp_wmem` and `tcp_rmem` ceilings to 128 MiB for
that reason, and the values in force are recorded in the attestation. The
clients keep their kernel defaults, as a client machine in the field would;
the per-connection ceiling that implies is one of the things the round trip
is there to measure.

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

The shaper also keeps a census of the client's own command stream: every
command line the client sent upstream, tallied by verb, and for
`ARTICLE`/`BODY`/`HEAD`/`STAT` the message-ids it named. Acquiring the lease
resets the set of ids seen, so the post-run snapshot's
`distinct_article_requests` is the run's own, while `article_requests` and
`repeated_article_requests` are cumulative and bracketed like the byte
counters. The run artifact records the delta as `shaper_article_census`.
Downstream bytes are what the shaper wrote into the client's sockets —
application bytes, not wire bytes — so a client whose bytes exceed the NZB's
article bytes either asked for articles twice (repeats > 0) or read past what
it asked for (repeats = 0). A schema-2 shaper carries no census and the field
is absent; a schema-2 or schema-3 shaper cannot render a round trip and is
refused for any plan that declares one. The census reads only the plaintext
listener: the TLS listener relays ciphertext the shaper cannot parse, so TLS
strata carry a zero census and the transfer-evidence table cannot separate
repeats from over-reading there. Verb keys are bounded to alphanumeric tokens
(anything else tallies under `NONVERB`), so a long chain of runs cannot grow
the snapshot without limit.

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
  --fixture-dir /scratch/fixtures/rar5-7-store-store-nonsolid-none-incompressible \
  --run-id 2026-08-02-a \
  --network nntp-bench_nntp_upstream --nntp-host nntp-upstream \
  --username "${NNTP_BENCH_USERNAME:-fixture-user}" --password-file "$NNTP_BENCH_PASSWORD_FILE"
```

The pinned Nyuu image is `linux/amd64` because its native `yencode` module
does not build on arm64 Alpine; Docker emulates it on Apple Silicon. Posting is
corpus setup, never a metric. It uses plaintext port 119 once; the same
persisted articles are then downloaded over 119 and implicit-TLS 563.

Reposting an unchanged corpus every time is pure overhead. See
[Pre-seeded NNTP corpus image](#pre-seeded-nntp-corpus-image) for caching it.

### 5. Write a plan

```bash
go run ./cmd/nntpbench plan \
  --fixtures rar5-7-store-store-nonsolid-none-incompressible,rar4-store-store-nonsolid-none-incompressible \
  --archive-toolchains vanilla --profile equivalent-throughput --server-link 10gbit \
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

The published suite runs `--profile equivalent-throughput` only: SABnzbd
and NZBGet are compared at their best, with direct unpack on, rather than at
their shipping defaults. `--profile stock` remains a valid plan for a
diagnostic and is reported separately when it is run; neither profile is a
fallback for the other. The profiles differ only for
SABnzbd (`direct_unpack`) and NZBGet (`DirectUnpack`); NZBGet's `DirectWrite`
is its shipping default and independent of direct unpack, so it stays `yes`
in both. Weaver is rendered with `WEAVER_DIRECT_UNPACK=on` and
`WEAVER_CLEANUP_AFTER_EXTRACT=true` in both, because those are its shipping
defaults and the benchmark measures the product as shipped (every client
deletes its archive volumes after a successful unpack), so the Weaver column
is the same run configuration under either profile. One Weaver default is
deliberately overridden in both renders:
`WEAVER_PROPAGATION_DELAY_SECS=0`. Weaver holds a post whose NZB is under
five minutes old before downloading it; SABnzbd and NZBGet ship with that
delay at zero, and every benchmark NZB is freshly posted by construction, so
leaving the hold on would time the poster's clock rather than the client.
The override is in the audited environment of every Weaver run.
`archive_toolchain` is a
first-class plan, adapter, config and result field: `vanilla` is the stock
benchmark, and the optional `rarpar` Docker lanes (see below) are never pooled
with it. `--targets docker-linux` writes a Docker-only plan; otherwise the plan
carries all three targets and each host runs only its own.
`--storage-profile` and `--nfs-link` add the storage stratum described in
[Storage profiles](#storage-profiles-local-vs-throttled-nfs); the default is
`local` and a default plan is byte-for-byte what it was apart from the new
`storage_profile` field. `--server-rtt` declares the fixed round trip the
shaper adds (see [Fixed round trip](#fixed-round-trip)); it must match the
`server-env` the shaper was started with, and the plan's `server_link`
carries it as `rtt_micros`.

`--exclude-client client:fixture-id:reason` (repeatable) leaves one client out
of one fixture's blocks, with the reason persisted in the plan under
`client_exclusions`. It exists for the client that deterministically cannot
finish a fixture — SABnzbd on the RAR recovery-volume (`.rev`) fixtures, which
it does not use — where re-running the failure every block costs time and
teaches nothing. The outcome is not dropped: the summarizer counts every
excluded block as that client not finishing (`baseline_excluded` /
`candidate_excluded` inside `completion`), reports the reason under the
comparison's `client_exclusions`, and withholds the paired comparison exactly
as it would after observed failures. A plan that excludes every client from a
fixture, names an undeclared client or fixture, or omits the reason is refused.

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

The controller's exit status separates the two ways a pass can end short of
clean. `0`: every suite passed with verified output. `2`: every suite ran to a
client outcome, but at least one client did not finish (`completed_with_dnf`
artifacts) — a recorded result the summarizer admits, not a reason to stop a
chain of runs. `1`: at least one suite `failed` on the harness side (the
adapter could not run, an attestation was missing, an artifact could not be
written), and that root is not publishable until the cause is fixed.

`CLIENT_JOB_TIMEOUT` / `NATIVE_JOB_TIMEOUT` default to `20m` and bound how long
a submitted job may run without reaching a terminal state. A Docker-lane job
that exceeds it is recorded with terminal status `timed_out` and the client's
last reported status as the reason; the controller counts it as did-not-finish
exactly like a client-reported failure, so a client that hangs on a fixture
becomes a result rather than a stalled pass. The native lane reports the same
condition as the run's error.

`CLIENT_POLL_INTERVAL` / `NATIVE_POLL_INTERVAL` default to `100ms`. Every
client's own completion stamp is an integer second (SABnzbd's history
`completed`, NZBGet's `HistoryTime`), so the controller's external poll is
the one neutral clock, and at 100 ms it costs each client the same ten status
calls a second — a load that is noise against a 100 MiB-plus download. The
width of the window in which the terminal state was observed — from the last
poll that still saw the job running to the poll that saw it finished — is
recorded in every artifact as `terminal_observation_uncertainty_nanoseconds`.
A run is excluded when that window exceeds 1 % of its submission-to-terminal
duration or 250 ms, whichever is larger. The absolute allowance exists
because the window is one poll interval plus how long the client's own status
API takes to answer, not a property of the fixture: a 1 % bound alone would
reject every short run of a client whose API answers in 40 ms while admitting
one that answers in 10 ms — a selection bias against the slower API, not a
precision gain — while a window wider than 250 ms marks a poll the controller
missed and still fails the run.

Two other modes exist and are labelled apart from the headline:

- `queue-transition` — generate and seed `direct-mkv`, plan **only** that
  fixture, and measure first-submission-to-last-verified-output wall clock
  across forced duplicates: the plan's `--repetitions` is the number of copies
  queued per client lane (at least 2; the original design point was 20). It
  reports no per-job scores. `summarize --mode queue-drain --artifacts <root>`
  binds each lane to the snapshotted plan and prints its drain wall clock,
  copies, product identity and TLS label; a lane with a copy that did not
  finish is listed with its recorded failure and no time.
- `run` — a cold, one-NZB diagnostic. Never the headline result.

Independent output verification is also available on its own:

```bash
go run ./cmd/nntpbench verify-output \
  --fixture-dir /scratch/fixtures/rar5-7-store-store-nonsolid-none-incompressible \
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

Only sequential artifacts that describe a client outcome are admitted:
`passed` (verified output) and `completed_with_dnf` (the client reached a
terminal failure, or its output failed neutral verification). Clients are
paired inside the same randomized repetition block, stratified by fixture,
profile, target, transport, archive toolchain, server link and storage
profile. How each client validated TLS is a property of that client's run, not
of the block — SABnzbd's TLS runs are `tls-unverified` while the others are
`tls-ca-verified` — so it is not part of the pairing key; the comparison
carries each client's validation and label under `transport_policies`, and a
client whose policy changes inside one stratum is refused as two products
pooled. Each stratum reports how many blocks each client
finished, then, over the blocks both clients finished, the raw medians and
coefficients of variation, the paired geometric-mean ratio and a deterministic
10 000-resample bootstrap 95 % interval on the log ratio. There is no outlier
deletion and no pooled score. A block where one client did not finish is
excluded from the timing comparison and counted under `completion`; a client
that cannot finish a fixture is a result, and it must not hide the rest of the
run. A client the plan excluded on a fixture (`--exclude-client`) is counted
the same way, as not finishing every block, with the plan's reason reported
under `client_exclusions`. When the failures leave a stratum with fewer than
the minimum paired blocks, that stratum keeps its counts and its comparison is
withheld with a stated reason.

Above the strata, `aggregates` pools the per-fixture comparisons by fixture
class: one entry per class and non-fixture stratum (profile, target,
transport, toolchain, server link, storage profile). Every fixture carries
equal weight — its paired log ratios are averaged first and the fixture means
second — so a fixture that ran more blocks does not count for more, and the
bootstrap resamples blocks within each fixture with the fixture set held
fixed, because the corpus is a declared set and not a sample. The `headline`
aggregate is the figure for the common case and the `breadth` aggregate the
compatibility figure; they are never pooled with each other. A class figure is
withheld, naming the fixtures, whenever any fixture of the class had its own
comparison withheld: a client that could not finish a fixture of the class
does not get a class figure over the fixtures it did finish. An artifact whose
job carries no `fixture_class` was run over a corpus that predates the classes
and fails the summary closed.

A harness-side `failed` suite, a missing or unverified run, an
incomplete pair with no recorded failure, fewer than 20 paired blocks for any
other reason, or terminal-observation uncertainty above its limit (1 % of the
run or 250 ms, whichever is larger) still fails the summary closed.
So does an artifact root that mixes storage profiles: a local run and an NFS
run answer different questions and are summarized separately, never pooled.

Each stratum also carries a `cpu_time` comparison: the two clients'
`cpu_time_nanoseconds` paired over the same blocks, with the same medians,
geometric-mean ratio (candidate over baseline; below 1 means the candidate
spent less CPU) and bootstrap interval. In the Docker lane that counter is the
whole container's cgroup, so a client that hands its work to `unrar`, `par2`
or `7z` is charged for them exactly as a client that does the work in-process
is charged for its own threads; that is the point of the comparison. It is
secondary evidence and never fails the summary closed. `accounting` states,
per client, the counter's scope, collector and collector version and how many
blocks had no measured counter, with the lane's recorded reasons; a block
where either counter is unavailable is dropped from the CPU pairing only. The
comparison is withheld when the two clients were measured at different scopes
(a `client_process` counter and a `client_container` counter are different
quantities), when a client's counter source changes inside one stratum, or
when fewer than two blocks pair; pairing fewer blocks than `--minimum-blocks`
is stated under `caveats` rather than withheld. The NFS profiles' CPU
accounting caveat is carried under `caveats` too.

Each stratum also carries `transfer`: per client, over its finished shaped
blocks, the minimum, median and maximum `shaper_downstream_bytes` and, when the
shaper counted commands, the summed `article_census` (blocks covered, article
requests, distinct message-ids, repeated requests). It is evidence rather than
a comparison — no ratio, no gate — and it is what makes a client that finishes
fast by pulling more than the NZB carries visible next to its wall clock. A
deterministic client lands within a few bytes of itself from block to block;
a spread is a finding, and the census says whether the excess was requested
twice or read past.

## Driving a whole session

A published comparison is not one run. It is a series: several plans, each
measured under its own link conditions, each summarized against both
baselines. `nntpbench chain` runs that series from a single JSON description,
so the same session runs unchanged on Linux, macOS and Windows and nothing
about it depends on a shell.

```bash
# Validate the description and every host precondition without measuring.
nntpbench chain --config runs/latency-series.json --dry-run

# Run it. Re-run one phase by name after fixing whatever broke.
nntpbench chain --config runs/latency-series.json
nntpbench chain --config runs/latency-series.json --only C3-rtt100
```

`configs/chains/latency-series.example.json` is a complete session. Every
relative path in a chain description resolves against the description's own
directory, so a chain, its plans and its corpus move between machines as one
unit.

A chain drives one of two server-side arrangements, named by `stack`. The
default, `docker`, is the Compose topology above. `raw` runs the same server
and shaper as local processes for the hosts that cannot have containers; see
[Raw stack](#raw-stack-the-server-side-without-docker). The two are validated
apart, so a description cannot ask for a Compose file on a raw stack or a
container check on a stack that runs none.

Each phase names its execution mode, its plan, its corpus and its artifact
root, and declares the link conditions it must be measured under. The chain
reconfigures the shaper only when a phase's conditions differ from the phase
before, and it restores a declared resting state when the session ends, so a
finished session never leaves a rate limit or an injected round trip behind.
Phases run out of process: a phase that exhausts memory while rendering its
artifacts cannot take the session down with it, every phase gets an exit status
of its own, and the command line each phase ran is in the log, reproducible by
hand.

A phase may carry a `plan_spec` instead of a plan made by hand. The plan is
then built from the spec, deterministically in its own seed, so the plan built
on one machine is the plan built on the next; a plan already on disk is reused
untouched, because it is the record of what a past session measured. Corpora
declared under `fixture_sets` are named once and shared by every phase that
measures them. `exclude_fixtures` drops fixtures from a corpus by id and
refuses an id the corpus does not contain, so a misspelled exclusion cannot
silently keep the fixture it was meant to remove.

Preconditions are checked once, before the first measurement:

- every declared fixture root exists;
- the NNTP service advertises `PIPELINING` (on a raw stack `raw.pipelining`
  sets it directly, so there is no container to inspect);
- the pinned client image reports the version the session is for;
- every fixture each plan names has a manifest, posts at least the corpus
  floor, and — for a phase that asks for a paired summary — declares a
  headline or breadth class;
- no phase would measure into an artifact root that already holds suites.

The last two are the expensive mistakes. An undersized fixture fails its suite
hours into a run, and an unclassified one runs to completion and then
summarizes to nothing; both are cheap to catch before the shaper is even
touched. Summaries themselves run only after every phase is finished, so
summarizing never competes with a measurement for the machine.

The session writes `chain-<name>-result.json`: every phase with its link
conditions, wall clock, exit status, suite count, log path and summaries. It is
rewritten after each phase, so an interrupted session still leaves an accurate
account of what it measured.

Client images are pinned by digest, never by tag:

```bash
nntpbench pin --adapters runs/adapters.json --template runs/adapters-next.json \
  --client weaver --image ghcr.io/scryer-media/weaver --version 0.11.0 \
  --entrypoint /opt/weaver/weaver
```

`pin` resolves the tag to a digest, saves the previous catalog under a
timestamped name, rewrites the catalog atomically, pre-pulls every client image
so the first phase is not charged for a download, and runs the pinned image to
report the version it actually is.

## Pre-seeded NNTP corpus image

Posting the corpus is setup, not measurement, but it is slow and it is
identical every time the fixtures have not changed. `nntpbench seed-image`
bakes an already-seeded article store into a local Docker image so later runs
start from it instead of reposting.

A cache hit has to mean the server holds exactly the articles the fixtures on
disk describe, so the image is keyed by a fingerprint over:

- the format string `nntp-bench-seed-image-v1`, which is bumped whenever the
  input set or its framing changes;
- every fixture's `fixture-manifest.json` (which already carries a BLAKE3
  digest of every posted byte, so this stays fast on a large corpus);
- the seed parameters that decide what an article is called or how large it
  is: the seed run id, the raw segment size, the newsgroup, and the poster's
  message-id scheme;
- the NNTP server image tag *and* its local image id, so a rebuilt server with
  the same tag is a miss rather than a silent hit.

The image is tagged `weaver-nntp-bench:corpus-<first 12 hex>` and carries the
full fingerprint, the corpus-manifest digest, the seed run id and the
generation time as labels. Status and restore check the labels, not the tag,
so a tag collision cannot be mistaken for a match.

```bash
# After a normal `nntpbench seed` pass over the corpus, with the stack up:
go run ./cmd/nntpbench seed-image status \
  --fixtures-root /scratch/fixtures --run-id seed-2026-09-04 \
  --compose-project usenet-bench

go run ./cmd/nntpbench seed-image capture \
  --fixtures-root /scratch/fixtures --run-id seed-2026-09-04 \
  --compose-project usenet-bench \
  --provenance /scratch/runs/nntp-seed-provenance.json
```

`status` always prints a reason, hit or miss — no local image, a different
fingerprint format, a corpus fingerprint that does not match the fixtures on
disk — so a miss is diagnosable rather than mysterious.

To start the stack from a captured image, restore the baked NZBs and layer the
seeded Compose override on top of the shaper topology. `restore` refuses an
image whose fingerprint does not match the fixtures on disk, and refuses to
overwrite a local NZB that differs from the baked one:

```bash
export NNTP_SEED_IMAGE=$(go run ./cmd/nntpbench seed-image status \
  --fixtures-root /scratch/fixtures --run-id seed-2026-09-04 \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["fingerprint"]["tag"])')

go run ./cmd/nntpbench seed-image restore \
  --fixtures-root /scratch/fixtures --run-id seed-2026-09-04 \
  --provenance /scratch/runs/nntp-seed-provenance.json

docker compose \
  -f configs/server/compose-shaper.example.yml \
  -f configs/server/compose-seeded.example.yml \
  up -d
```

Docker `commit` cannot see a named volume's contents, so capture stages the
article store out of the running container with `docker cp` and rebuilds it as
a Docker build context. The seeded override therefore also drops the
`nntp-data` volume mount — it would shadow the baked `/data/articles` — while
keeping `nntp-certs`, since TLS material is generated per stack and is not part
of the cached corpus. That override needs Docker Compose v2.24 or newer for
`volumes: !override`.

Whether a run's server was pre-seeded or seeded live belongs to the corpus and
the NNTP server, not to any client under test, so it is recorded in the seed
provenance JSON (`preseeded`, the image tag and the fingerprint) rather than in
a client-run stratum.

## Raw stack: the server side without Docker

Two of the hosts this benchmark has to run on cannot have the containerized
server side. Windows has no `netem`, so the round trip cannot be injected at
all. On an ARM Mac every container runs inside a Linux virtual machine whose
scheduling and networking are exactly the things a download benchmark measures,
so a result taken through it describes the VM as much as the client. The raw
stack runs the same two programs — the `e2e-nntp` server and `nntpshaper` in
front of it — as ordinary local processes, with no Docker, no Compose and no
`tc`.

Everything else is unchanged. The clients still reach only the shaper's front
ports, so no run bypasses the link being modelled; the shaper still meters
egress in userspace under an exclusive run lease; and the controller still
takes a before and after attestation from the shaper's control plane.

### The round trip is carried in userspace

Bandwidth shaping was already a userspace leaky bucket, so it is identical on
every host. The round trip is not: on Linux the container's entrypoint renders
it with `tc netem`, which needs `NET_ADMIN` and a kernel that has it. With no
`tc`, the proxy carries the delay itself. Each direction gets a delay line — a
queue, not a sleep in the copy loop, so a burst of chunks costs one delay
rather than one delay each — holding every chunk for half the round trip, and a
connection is charged one whole round trip before the upstream is dialled.

That handshake charge is there because a proxy cannot delay a TCP handshake by
relaying bytes: `netem` delays the SYN exchange, a local listener does not, and
without the charge every client would get its first byte one round trip early.
A client that times TCP establishment separately from the greeting still sees a
local connect; that is the one part of the round trip this mechanism does not
reproduce, and it is why the mechanism is reported rather than assumed.

Each direction's queue is sized from the bandwidth-delay product, and a queue
too small to hold one BDP is refused at startup — a queue that caps the link
below its declared rate would measure the queue, not the link. An unlimited
link has no BDP to derive from, so pair it with `raw.delay_queue_bytes`.

The mechanism is part of every attestation. A netem report and a userspace
report are validated against different shapes and can never be confused for one
another: a userspace report that named a shaped interface, or a netem report
carrying a handshake delay, is refused at startup by the shaper and again by
the controller. Live evidence is the minimum residency each direction actually
held a chunk for, which is jitter-robust in the direction that matters — a
chunk can be released late under load, never early. **Do not merge results
taken under the two mechanisms into one comparison.** They are different link
models; each is internally consistent, and a series should be measured under
one of them throughout.

### Seeding a raw host

Posting needs Nyuu, which is a container, so a raw host never seeds its own
spool. Seed once on a Docker host, then copy the article store across with the
fixtures and NZBs it corresponds to:

```bash
# On the Docker host, with the seeded stack up (see "Pre-seeded NNTP corpus
# image" above for making that repeatable).
mkdir -p /scratch/export
docker cp nntp-bench-nntp-1:/data/articles /scratch/export/
rsync -a /scratch/export/articles/ mac-host:/scratch/spool/articles/
rsync -a /scratch/fixtures/ mac-host:/scratch/fixtures/
```

The fixtures must be the same bytes on both hosts: the article store is
meaningless without the manifests and NZBs that name its articles, and the
manifest hashes are the cross-host equivalence check. Copy them together or
neither.

### Staging the stack

The harness never installs anything — the same rule the native client lanes
follow. Put the two executables somewhere and tell the chain which directory
they are in; it looks for `e2e-nntp` and `nntpshaper` by those exact names,
with `.exe` appended on Windows, and refuses to start if either is missing.

Both are pure Go and cross-compile without cgo, so a bench host needs no Go
toolchain: build on whatever machine has the checkout and copy the directory
across, which is how the controller itself is already staged. The shaper is in
this module; the server is another module, pinned at the version the Docker
lane's `image build` pins — a raw run and a containerized one are only
comparable if they served the same articles from the same server.

```bash
# For this Mac.
go build -trimpath -o /scratch/bin/nntpshaper ./cmd/nntpshaper
GOBIN=/scratch/bin go install github.com/scryer-media/e2e-nntp/cmd/e2e-nntp@v0.1.0

# For a Windows host, cross-built. `go install` refuses a GOBIN while it is
# cross-compiling, so that one lands under GOPATH; copy it in beside the other.
GOOS=windows GOARCH=amd64 CGO_ENABLED=0 \
  go build -trimpath -o /scratch/bin-windows/nntpshaper.exe ./cmd/nntpshaper
GOOS=windows GOARCH=amd64 CGO_ENABLED=0 \
  go install github.com/scryer-media/e2e-nntp/cmd/e2e-nntp@v0.1.0
cp "$(go env GOPATH)/bin/windows_amd64/e2e-nntp.exe" /scratch/bin-windows/
```

### Describing the stack

A chain runs the stack by naming it. `configs/chains/raw-native.example.json`
is a complete native session:

```json
{
  "stack": "raw",
  "raw": {
    "bin_dir": "../bin",
    "data_dir": "../spool/articles",
    "pipelining": true
  }
}
```

Only the two directories are required. The stack fills in the rest, and the
chain settles every endpoint from it — target, host, both ports, the CA file
and the shaper control URL — so a raw description does not repeat what the
stack already decides. Ports default to 11119/11563 upstream, 8119/8563 for the
shaper's front, and 8080 for the control plane: all above 1024, so the stack
needs no privileges on any host, and deliberately unlike the well-known NNTP
ports so nothing else on the host mistakes them for a real news service.
`raw.pipelining` advertises RFC 4644 PIPELINING as commercial providers do and
defaults to on; a silent server benches every client one article per round trip
and hides the difference latency is there to show.

The server starts once per session and the shaper is replaced per phase, for
the same reason the Docker chain only ever recreates the shaper: restarting the
server would reopen the article store between phases and charge one phase for
the other's cold cache. Its link contract is immutable for the life of the
process, which is what lets a run's before and after attestations prove the
conditions never moved underneath it.

### Checking a host

`preflight` covers the raw stack as well as the clients. Point it at the chain
that will run, so the stack it checks is the stack the session uses rather than
a set of directories retyped on a command line:

```bash
go run ./cmd/nntpbench preflight --target macos-native \
  --chain /scratch/runs/raw-native.json \
  --adapter /scratch/bin/nativeadapter --weaver /path/to/weaver \
  --nzbget /path/to/nzbget
```

It reports every condition at once — both executables, the article store and
how many entries it holds, the password file, the port assignments, and whether
each port is actually free — instead of stopping at the first, so a new host is
fixed in one pass. Nothing is started, written or changed. To check a host
before its session is written, name the directories directly with
`--raw-bin-dir` and `--raw-data-dir`; giving both a chain and the flags is
refused rather than silently resolved in favour of one.

A chain re-runs the port probe itself, on a dry run and a real one, before any
plan is built:

```bash
go run ./cmd/nntpbench chain --config /scratch/runs/raw-native.json --dry-run
```

Whether a port is free is the one condition a configuration can never settle,
and on a developer's machine the control plane's 8080 is a popular port. An
empty article store is worth catching there too: it answers `430` to every
article, and a run against it looks like a client that failed rather than a
server with nothing to serve.

A raw chain settles what it can from the stack it is about to start, so the
two cannot disagree: the plan is built for the chain's own execution target
rather than the Docker default, and the clients are told the username the
server is listening for. Naming either in the config still wins; naming a
`plan_spec.targets` list that leaves out the target the chain runs is refused
while the plan is being built, because the phase would otherwise start the
stack and exit non-zero with nothing measured.

### What a raw stack refuses

- **`compose_file`, and any container check.** `require.client_version`
  inspects a client's Docker image and `require.server_pipelining_container`
  inspects a container's environment; a raw stack has neither. Pin the native
  client in the adapter catalog, and set `raw.pipelining` directly.
- **NFS storage profiles.** The throttled export is a container. Native lanes
  are local-storage only.
- **Linux.** A raw stack has no execution target there. Every target that is
  not `docker-linux` names an operating system, and there is no native Linux
  target; recording a raw Linux run as `docker-linux` would file it beside
  results from a different packaging boundary.

## Native macOS and Windows lanes

The native lanes run the same sequential measurement with one fresh client
process per run. The server side they measure against is either a Docker stack
on another host or a [raw stack](#raw-stack-the-server-side-without-docker) on
this one; the client side below is the same either way. Build the launcher,
copy the OS catalog, and use a separate artifact root per operating system:

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

The products are installed by hand — the harness never installs one — so all
it needs is where they are, and the catalog is where it is told. Run the
non-mutating preflight first and point it at that catalog: it resolves the
launcher and the executable at the head of each `NATIVE_LAUNCH_COMMAND` and
fails if the OS or any of them is missing, so a check cannot pass for a client
the run will not launch. Add `--chain` when this host also serves the
benchmark, and the [raw stack](#raw-stack-the-server-side-without-docker) is
covered in the same report:

```bash
go run ./cmd/nntpbench preflight --target macos-native \
  --adapters /scratch/runs/adapters.macos.json \
  --chain /scratch/runs/raw-native.json
```

An unedited catalog reports exactly what is left to stage:

```
missing  adapter   /scratch/nntp-bench-bin/nativeadapter
present  nzbget    /Applications/NZBGet.app/Contents/Resources/daemon/usr/local/bin/nzbget
present  sabnzbd   /Applications/SABnzbd.app/Contents/MacOS/SABnzbd
missing  weaver    /absolute/path/to/weaver
```

Two statuses beyond `missing` exist because a path can resolve and still be the
wrong program:

- `misnamed` — the file on disk has a different name in a different case.
  macOS matches paths case-insensitively, so a catalog entry that says
  `.../MacOS/nzbget` resolves happily against a file called `NZBGet` and the
  run launches something nobody named.
- `launcher` — the path is an app bundle's `Contents/MacOS` entry and the same
  bundle ships the program itself deeper in. That entry is a GUI launcher: it
  starts the real program under *its own* configuration, ignores the argv it
  was handed, and supervises it. Nothing lands on the benchmark's API port, and
  had it landed, the measurement would have been of the host user's own
  settings. Name the inner path, which the report gives. A bundle holding only
  one executable is the program itself and is fine to launch, which is how
  SABnzbd ships.

Naming the executables individually with `--adapter`, `--weaver`, `--sabnzbd`
and `--nzbget` still works for a host with no catalog yet; giving both a
catalog and the per-client flags is refused rather than silently resolved in
favour of one. Preflight does not try to confirm a client's *version* — that
needs the product running, so the adapter asserts it at the start of a run,
before any NZB is submitted.

Notes for the native catalogs:

- `adapters.macos.example.json` uses the installed
  `/Applications/SABnzbd.app/Contents/MacOS/SABnzbd` and the NZBGet daemon at
  `/Applications/NZBGet.app/Contents/Resources/daemon/usr/local/bin/nzbget` —
  *not* that bundle's `Contents/MacOS` launcher — and leaves the Weaver path as
  an explicit replacement; the Windows catalog uses explicit paths throughout. The harness never installs a product implicitly — stage
  pinned installers, record their versions and hashes in the catalog, and use an
  isolated working directory (for example `C:\bench`). `preflight --adapters`
  is what confirms the paths in a filled-in catalog actually resolve. It also
  checks what a product needs from the host that installing it does not
  provide: NZBGet shells out to `unrar` and `7z`, and a host that supplies
  neither does not fail the run outright -- it skips the unpack and fails
  output verification after a full download. A packaged install may ship its
  own beside the daemon (the macOS bundle ships `unrar` and `7za`), and that
  copy wins over PATH because it is the one the product is built against;
  preflight and the rendered config resolve through the same function, so the
  check reports the binary the run will actually invoke.
- `NATIVE_LAUNCH_COMMAND` is a JSON argv array, never a shell string, and may
  use `{{config_dir}}`, `{{nzb_path}}`, `{{output_dir}}`, `{{fixture_dir}}` and
  `{{api_port}}`. Commands must stay in the foreground so the launcher can
  collect CPU time and stop them cleanly.
- `NATIVE_CLIENT_VERSION` must equal what the product reports through its own
  API (SABnzbd `version`, NZBGet `version`, Weaver GraphQL `version`); a
  mismatch fails the run before any NZB is submitted.
- For native Weaver set `WEAVER_ENCRYPTION_KEY` in the adapter environment so
  no Keychain prompt is waited on. It is standard base64 of exactly 32 bytes,
  and nothing else parses. The example catalogs carry one; `preflight
  --adapters` fails a Weaver entry that has none, and fails one whose key
  Weaver would reject. Neither failure is visible at run time: without a key
  the run hangs on the prompt, and with a malformed one Weaver exits during
  startup and the run reports only a client that never became ready. Both lanes render `WEAVER_STARTUP_IOPS=50000`
  so Weaver's startup disk probe never runs inside the measured process (an
  operator value already in the environment is preserved and recorded).
- Both lanes also pin Weaver's trusted-network list (`WEAVER_TRUSTED_CIDRS`:
  loopback for the native lane, the whole address space for the Docker lane,
  whose peer is the bridge gateway of the private benchmark network). Weaver
  0.10 otherwise offers a first-run wizard instead of an anonymous session and
  refuses the adapter's GraphQL calls; pinning the list settles that policy at
  startup with no wizard and no bootstrap login.
- Copy the immutable plan and the generated fixture / NZB directories to each
  native host; do not regenerate the corpus per OS. The fixture manifest and
  output hashes are the cross-host equivalence check. A host running its own
  raw stack needs the article store beside them, copied from the same seed.
- Native Instruments / ETW traces are useful attribution artifacts, not a
  cross-product CPU metric; keep them apart from the benchmark JSON.

## Storage profiles (local vs throttled NFS)

Completion behaviour over slow network storage is a different measurement, not
a variant of the local one. A client that assembles directly into its final
location and one that assembles locally and then copies look identical on fast
local disk and diverge sharply over a NAS link, so the benchmark runs twice:
once with the client's directories on the host's own disk, and once with them
on an export whose link is throttled to a declared rate and fixed delay.

`storage_profile` is a first-class stratum. It travels from the plan into the
queue input, the adapter configuration, the adapter result, the run artifact
and the summarizer's comparison key, and every field is re-validated at each
hop. `local` is the default and the published headline.

| `--storage-profile` | Intermediate directory | Completion directory |
| --- | --- | --- |
| `local` (default) | host disk | host disk |
| `nfs-complete` | host disk | throttled NFS export |
| `nfs-all` | throttled NFS export | throttled NFS export |

Under `local` the Docker lane binds ONE host directory at `/downloads`, so
`/downloads/incomplete` and `/downloads/complete` are two directories on the
same filesystem and every client's final move is a rename, as on a real
single-disk install. (Two separate binds would be two mounts: `rename(2)`
fails with `EXDEV` and each client falls back to copying its whole extracted
output, a write no user's machine performs.) The `nfs-*` profiles are the
opposite by design: the completion directory is a volume on the export, so
the final move is the copy across the network that those profiles exist to
measure.

An `nfs-*` profile must name its link with `--nfs-link`. The named links are
fixed and never change silently:

| `--nfs-link` | Rate | Burst | Round trip |
| --- | --- | --- | --- |
| `nas-100mbit` | 100 Mbit/s | 128 KiB | 1000 µs (500 µs each way) |
| `nas-1gbit` | 1 Gbit/s | 1 MiB | 1000 µs (500 µs each way) |
| `nas-2.5gbit` | 2.5 Gbit/s | 2 MiB | 1000 µs (500 µs each way) |

The delay is a fixed one-way `netem` delay with zero jitter in each direction,
so the observed round trip is the declared one and a repeat of the same plan is
shaped identically.

### Bringing up the export

```bash
# 1. The link the plan will declare, written once as an immutable env file.
go run ./cmd/nntpbench storage-env \
  --storage-profile nfs-complete --nfs-link nas-1gbit --output /scratch/runs/storage.env

# 2. Build the image and start the export on its own network. Run compose from
#    the repository, as with the shaper stack: the example file's build context
#    is relative to it.
docker build -f docker/nfs-server/Dockerfile -t weaver-nntp-bench-nfs:dev .
docker compose --env-file /scratch/runs/storage.env \
  -p bench -f configs/server/compose-nfs.example.yml up -d

# 3. Plan and run against it. The helper binary is the harness's own, built for
#    the container's platform; the controller mounts it into a helper container
#    that takes the NFS server's export volume directly, to verify and empty the
#    export from the server's side of the link.
GOOS=linux GOARCH=amd64 go build -o /scratch/bin/nntpbench-linux ./cmd/nntpbench

go run ./cmd/nntpbench plan \
  --fixtures rar5-7-store-store-nonsolid-none-incompressible \
  --targets docker-linux --storage-profile nfs-complete --nfs-link nas-1gbit \
  --repetitions 20 --seed 20260802 --output /scratch/runs/plan-nfs.json

go run ./cmd/nntpbench sequential \
  --plan /scratch/runs/plan-nfs.json --adapters /scratch/runs/adapters.json \
  --target docker-linux --fixtures-root /scratch/fixtures \
  --artifacts /scratch/runs/artifacts-nfs \
  --nfs-container bench-nfs-1 --nfs-network bench_benchmark_storage \
  --nfs-helper-image weaver-nntp-bench-nfs:dev \
  --nfs-verify-binary /scratch/bin/nntpbench-linux
```

`--nfs-container` and `--nfs-network` are the container and network names
Docker actually created (Compose prefixes both with the project name). The
export is never published on a port.

### What the harness owns

The controller, not the client, owns the storage for a run. Per run it takes an
exclusive lease on the NFS server, creates an empty export subdirectory,
creates the Docker `local` volumes (`type=nfs`, `o=addr=…,<mount options>`,
`device=:/<run>/complete`), and hands the adapter nothing but the volume names.
The client sees the same two container paths under every profile, so no product
can tell which storage lane it is running in. After the timed window the
volumes are removed, the export subdirectory is deleted and the lease is
released; a failure to clean up is reported rather than swallowed.

Output verification stays outside the timed window and stays the harness's own:
the controller cannot read the export without a host-side root mount, so it runs
its own `verify-output` inside a helper container
(`verification_strategy: helper_container_server_side_harness_verify_output`).
That helper does not mount the export over NFS. It attaches the NFS server's own
export volume with `--volumes-from <container>:ro` and reads
`/export/<run>/complete` on the server's local filesystem, so a multi-gibibyte
output is never pulled back through the shaped link — which on a 100 Mbit/s
profile would add minutes of wall clock and server load to every run. Deletion
works the same way, with the volume attached read-write.

That server-side view is complete because of NFS close-to-open semantics: the
harness reads the export only after the client has reported terminal status and
closed its files, and closing flushes the client's writes to the server. A local
reader on the server then sees the server's own page cache, so no `sync` is
needed. The `--volumes-from` helper carries no benchmark network at all, so it
cannot reach the export any other way. Only the negotiated-mount evidence still
goes over NFS: a throwaway helper mounts the run's volume and reads
`/proc/mounts`, which moves no output bytes.

### Attestation

Every NFS run carries a `storage_attestation` alongside the shaper snapshots,
and a run that cannot prove its link is refused rather than published. It
records, before and after the measured window:

- `tc -s qdisc show` for the interface and, when the ifb redirect is in use, for
  the ifb device — with the raw output kept beside the parsed values.
- `/proc/net/rpc/nfsd`, for the server's own read and written byte counters.
- The negotiated client mount line from `/proc/mounts`, the server's
  `exportfs -v` output, the container's kernel release and the shaper
  environment it was given.

It then asserts that the live `tc` rate and delay match the declared profile
(within the 1 % tc prints to), that the shaper did not change mid-run, that both
qdiscs moved a non-zero number of bytes, that the server moved at least 1 MiB,
that the mount negotiated NFSv4.1, and that the same exclusive lease was held
from start to finish. `nntpbench run` prints a one-line summary per suite.

The client-to-server direction has two possible mechanisms. Linux cannot shape
ingress directly, so the container mirrors it to an `ifb` device and applies the
same `tbf` + `netem` pair there. Where the host kernel has no `ifb` module it
falls back to `tc … ingress police`, which enforces the rate by dropping rather
than queueing and cannot add delay. Which mechanism ran is recorded in the
attestation, and the delay assertion is replaced by an assertion of zero delay,
so a policed run can never be read as a delayed one.

### Verifying on a Linux bench host

The unit tests cover the wiring with fakes, and one opt-in test drives a real
server. Before publishing storage numbers from a new host, confirm in order:

1. `docker build -f docker/nfs-server/Dockerfile -t weaver-nntp-bench-nfs:dev .`
2. The export is a Docker volume, not the container overlay — the entrypoint
   refuses the overlay by name if it is not.
3. The container log says `ingress via ifb-tbf+netem`. If it says
   `ingress-police`, the host kernel has no `ifb` module (`modprobe ifb`), and
   the client-to-server direction is dropping instead of queueing.
4. `docker exec <container> tc -s qdisc show dev eth0` shows a root `tbf` at the
   declared rate with a `netem` child at half the declared round trip, and
   `… dev ifb-nfs` shows the same pair.
5. `docker exec <container> /usr/local/bin/nntpbench-nfs-control health` exits
   zero, and `/proc/fs/nfsd/versions` shows `+4.1`.
6. The live session test passes end to end:

   ```bash
   NNTPBENCH_NFS_CONTAINER=<container> NNTPBENCH_NFS_NETWORK=<network> \
     NNTPBENCH_NFS_VERIFY_BINARY=/scratch/bin/nntpbench-linux \
     go test ./internal/benchmark -run LiveServer -v
   ```

   It takes the lease, mounts the export, moves bytes through it, validates the
   attestation and asserts that nothing is left behind. It is skipped when those
   variables are unset, so it never runs in an ordinary `go test ./...`.
7. `docker volume ls` and `docker ps -a` are unchanged after a suite: the
   session removes its own volumes, export subdirectory and lease.

### Limitations

- **Linux hosts only, in practice.** The image needs a host kernel with `nfsd`
  and runs privileged; the export must be a Docker volume, because no kernel NFS
  server can export the container's overlay filesystem. Docker Desktop's VM will
  usually run the server but has no `ifb` module, so it silently takes the
  policing fallback: usable for wiring checks, not for published numbers.
- A userspace server (NFS-Ganesha) would remove the privileged kernel
  dependency. It is not implemented here; the kernel server is what a NAS
  actually runs, and swapping it would change what is being measured.
- **`cpu_time` excludes NFS client kernel time for the `nfs` profiles.** The
  mount is performed by the host kernel on behalf of the Docker daemon, so the
  client container's cgroup counter does not see it. The caveat is copied into
  every NFS attestation; do not compare `cpu_time` across storage profiles.
- The native macOS and Windows lanes are `local` only. Mounting an export there
  would need the operator's own kernel to mount it as root, so a plan that pairs
  an `nfs-*` profile with a native target is refused when it is written.
- `queue-transition` is `local` only for the same reason its verifier walks the
  whole output tree: it is a duplicate-instance check, not a storage
  measurement.
- One run at a time. The server's lease is exclusive, and a second controller
  gets a refusal rather than a quietly shared link.

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
  this cold-scope counter by the narrower primary wall clock. Under the `nfs`
  storage profiles this counter excludes NFS client kernel time, which the host
  kernel spends outside the client's cgroup; the caveat is recorded in every
  NFS attestation. `summarize` pairs this counter per stratum as `cpu_time`
  (see [7. Summarize](#7-summarize)).
- `shaper_downstream_bytes` and `shaper_article_census` — shaped runs: the
  application bytes the shaper wrote to the client and, with a schema-3
  shaper, how many article requests the client sent, how many distinct
  message-ids they named and how many repeated one already requested in the
  run. `summarize` reports them per stratum as `transfer`.
- `instructions_retired` — Docker lane on native Linux: cgroup-scoped
  `perf stat -a -G … -e instructions` over the same interval, raw output kept as
  `config/perf-instructions.txt`. Where `perf` cannot attach (Docker Desktop,
  most macOS setups) and on native lanes it is recorded as `unavailable` with a
  reason — never as zero, never omitted.

- `storage_profile` — where the client's intermediate and completion
  directories lived, with the link's fixed rate, burst and round trip. It is a
  stratum, not an annotation: `local` and `nfs-*` results are never pooled, and
  an NFS artifact without a valid `storage_attestation` is refused by the
  summarizer.
- `server_link` — the NNTP link the client downloaded over: the aggregate
  egress rate and burst, and the fixed round trip (`rtt_micros`) the shaper
  added. The round trip is part of the stratum: a 1 Gbit result at 0 and at
  250 ms are different comparisons. A run with a round trip carries the
  shaper's `link_shaping` report in both attestation snapshots, with the
  delays re-read from `tc` at each.

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
result artifact. The label is reported on every comparison that includes
SABnzbd, but it is not part of the summarizer's pairing key: a `tls` block
pairs SABnzbd's `tls-unverified` run with Weaver's `tls-ca-verified` one, and
the report says so rather than leaving the pair unformed.


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
- That a round trip injected by `netem` and one carried by the shaper in
  userspace are the same link. Both are attested and each is internally
  consistent; the mechanism is recorded per run and results taken under the two
  do not belong in one comparison.
- Any client result without its fixture manifest, client version or image
  digest, effective configuration, plan and output-hash record.
- That the client matrix is exhaustive; clients outside the catalog are simply
  not measured.
