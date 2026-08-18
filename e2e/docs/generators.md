# The fixture generators

Every byte under `testdata/` except `scenario.json` is produced by one Go
program: `cmd/fixturegen`, driving the declarative recipes in
`internal/fixturegen`. There are no shell scripts. Nothing is downloaded except
the oracle archives `test-corpus/toolchains.json` pins, and every one of those
is verified against its SHA-256 twice — once in Go before Docker is called, and
once inside the Dockerfile before the binary is installed.

## Running it

```sh
# What exists, what family it belongs to, and whether it is byte-reproducible.
go run ./cmd/fixturegen --list

# Rebuild the whole corpus into testdata/ and refresh the ledger.
go run ./cmd/fixturegen --all

# One scenario, or several.
go run ./cmd/fixturegen --scenario rar5-single --scenario par2-repair

# Fill in only what is absent, which is what a partial checkout wants.
go run ./cmd/fixturegen --all --only-missing

# Build somewhere else and look before replacing anything. `--out` never
# touches testdata/, the ledger, or any scenario.json.
go run ./cmd/fixturegen --all --out target/fixturegen/preview

# The publish workflow's fan-out surface: the generation matrix as JSON, one
# family's scenarios, the whole artifact cache, and the ledger paths a family
# owns (--verify holds them present and refuses anything unledgered on disk).
go run ./cmd/fixturegen --list-json
go run ./cmd/fixturegen --family rar-recovery-volumes --skip-ledger
go run ./cmd/fixturegen --build-artifacts
go run ./cmd/fixturegen --paths --family zip --with-scenarios --verify
```

Other flags: `--workers` (default 4) bounds how many scenarios build at once,
`--verbose` echoes oracle output, `--skip-ledger` leaves `sources.json` alone,
`--root` points at the harness from elsewhere. Exit codes: `0` success, `1`
failure, `2` usage.

A full run is slow by design: it encodes roughly 4 500 seconds of video and
writes about 6.2 GiB of archives. Budget a couple of hours — nearly all of it
video, since every other family derives from the clips and rebuilds in minutes
once they are cached.

Every oracle image is pinned to `linux/amd64`, which is what CI runs, so CI
emulates nothing — and the RARLAB 3.93/4.20/5.00 releases are 32-bit x86
binaries that an amd64 kernel's IA-32 support runs directly. **On an arm64
host** (an Apple Silicon Mac, say) Docker needs QEMU binfmt handlers for both
architectures before any of this works; without them `docker build` fails with
`exec /bin/sh: exec format error`. Install them once per Docker VM:

```sh
docker run --privileged --rm tonistiigi/binfmt --install amd64,386
```

That is a developer-machine step. The publish workflow deliberately does not
run it: it holds the corpus write credentials, and a privileged third-party
container does not belong in that job.

Intermediate work lives in `target/fixturegen/`: `artifacts/` is the shared
build cache, `work/` is per-scenario scratch that is removed on success.
Deleting `target/fixturegen/` forces a full rebuild, including the video.

## The recipe format

A recipe is a Go value in `internal/fixturegen/recipes.go`:

```go
add(Recipe{
    Slug:   "par2-7z-repair",
    Family: "PAR2",
    Notes:  "Parity over a 7z with two 64 KiB blocks zeroed well past the header, so repair runs before extraction.",
    Inputs: []string{samplePayloadPath},
    Build: sequence(
        publish("sevenzip-single", "archive.7z"),
        par2(PAR2Spec{Base: "archive.7z.par2", SliceSize: 65536, RecoveryBlocks: 4, Sources: []string{"archive.7z"}}),
        zeroOutput("archive.7z", 128*65536, 2*65536),
    ),
})
```

- **`Slug`** is the directory under `testdata/`.
- **`Family`** groups recipes that share a shape and a review argument.
- **`Notes`** is what a reviewer reads to decide whether the shape is right.
- **`Inputs`** are root-relative ledger paths whose bytes the fixture is built
  from. They land in the ledger entry's `inputs`.
- **`ByteReproducible`** is true only where nothing stamps a time or draws a
  salt: the zip, tar and stream-codec families.
- **`Build`** populates the scenario's output directory, using the vocabulary
  below.
- **`ExpectedOutputs`** maps an extracted member name to the host file whose
  BLAKE3 the scenario pins. Only the seven scenarios that carry an
  `expectedOutputBLAKE3` block declare it.

The file set is deliberately *not* restated in the recipe. The ledger entries
for the slug are the contract: a recipe that produces a file nobody ledgered,
or misses one that is, fails with both lists.

The build vocabulary: `publish`/`publishAll` (copy a cached artifact),
`sequence` (compose steps), `par2`, `zeroOutput`, `truncateOutput`,
`dropOutput`, `compressOutput`, `zipOf`, `tarOf`, `streamCodec`,
`splitSevenZip`, `renameMultivolume`, `nestRAR`, `swapOutputs`, plus
`env.RAR`, `env.SevenZip`, `env.PAR2`, `env.Clip`, `env.Stage`.

### Artifacts

`internal/fixturegen/artifacts.go` holds the shared build cache. An artifact is
built at most once and reused by every recipe that needs *those exact bytes*.
That is not only an optimisation: PAR2 sidecars are parity over specific bytes,
and `par2-obfuscated-rar-rewrite`'s sidecars have to match
`rar5-multivolume`'s volumes, which three further scenarios also stage. Any set
two scenarios must agree on byte for byte is an artifact.

The payload artifacts are `clip-sample` (120 s of noisy 1080p H.264, the
payload almost every archive wraps), `clip-preview` (5 MiB exactly),
`clip-episodes` (three 25 MiB byte ranges of the 1080p clip), `clip-small` and
`clip-shared` (the five source clips).

## Who wrote each byte

| Toolchain id | Pin | Writes |
| --- | --- | --- |
| `rarlab-3.93` | `rarlinux-3.9.3.tar.gz`, SHA-256 `55122286…8eff8` | RAR (available, currently unused by any recipe) |
| `rarlab-4.20` | `rarlinux-4.2.0.tar.gz`, SHA-256 `6826646b…68eb5` | the direct-store RAR4 set |
| `rarlab-5.00` | `rarlinux-5.0.0.tar.gz`, SHA-256 `4f942d79…c78610` | the direct-store RAR5 sets |
| `rarlab-6.24` | `rarlinux-x64-624.tar.gz`, SHA-256 `88e22a8e…db1eb` | every general-corpus RAR4 archive (`-ma4`) |
| `rarlab-7.23` | `rarlinux-x64-723.tar.gz`, SHA-256 `759b4b6a…cab588` | every general-corpus RAR5 archive (`-ma5`) |
| `sevenzip-26.02` | `https://www.7-zip.org/a/7z2602-linux-x64.tar.xz`, SHA-256 `41aaba7b…c28c03e` | every 7z container |
| `par2cmdline-turbo-1.4.0` | `v1.4.0.tar.gz`, SHA-256 `6f2cb042…d1b972` | every PAR2 recovery set |
| `uudeview-0.5.20` | `uudeview_0.5.20.orig.tar.gz`, SHA-256 `a2a44fa5…70a414` | every uuencoded article, and the decoder each one is proved against |
| `ffmpeg-7.1-ubuntu2404` | image digest `sha256:292a972c…71931d` | every video clip |
| `go-fixture-bytes` | Go 1.26.2, stdlib | payload streams and every byte edit |
| `go-archive-zip` | Go 1.26.2, `archive/zip` | zip containers, including ZipCrypto |
| `go-archive-tar` | Go 1.26.2, `archive/tar` | tar containers |
| `go-compress-gzip` | Go 1.26.2, `compress/gzip` | gzip streams |
| `go-compress-flate` | Go 1.26.2, `compress/flate` | raw DEFLATE streams |
| `go-klauspost-zstd@v1.19.2` | `github.com/klauspost/compress v1.19.2` | zstd streams |
| `go-dsnet-bzip2@v0.0.1` | `github.com/dsnet/compress v0.0.1` | bzip2 streams |
| `go-andybalholm-brotli@v1.2.2` | `github.com/andybalholm/brotli v1.2.2` | brotli streams |

The SHA-256 of each container pin was obtained by downloading the artifact over
HTTPS and hashing the bytes that arrived; the URL in the table is the one that
was fetched. The full digests are in `test-corpus/toolchains.json`, which is
the only place they are authoritative.

The Dockerfiles the container pins name live in
`internal/fixturegen/docker/{rarlab,par2,sevenzip}/Dockerfile`. Each takes the
URL and the digest as build arguments and runs `sha256sum -c` before it
installs anything, so a changed upstream download fails the build rather than
silently entering the corpus.

### RAR is RARLAB's alone

**Every RAR file in this corpus is written by RARLAB's own `rar`.** UnRAR's
licence forbids using UnRAR-derived code to *create* RAR archives, so no Go
code, no third-party library and no hand-assembled header ever authors or edits
a RAR structure here. What Go does to a RARLAB-created archive afterwards is
format-agnostic byte handling only: rename it, withhold a volume, split or
concatenate byte ranges, truncate it, or overwrite a deterministic range.
Nothing in this package understands a RAR header.

That covers `.rev` recovery volumes too: they are a RAR structure, so they come
from RARLAB's `-rv<N>` in the same invocation that writes the data volumes.
The recovery-volume family's damage is then whole *files* removed from the
output directory — never an edit inside a `.rev`.

If a scenario needs a RAR shape RARLAB's CLI cannot produce, the scenario does
not belong in this corpus. Listing and verifying with `unrar` is fine; creating
is not.

The other formats are open, and are written by whichever pinned oracle is most
convenient: PAR2 by par2cmdline-turbo, 7z by the official 7-Zip console binary
(never a distribution `p7zip` fork), video by the pinned FFmpeg image, and
zip/tar/gzip/DEFLATE/zstd/bzip2/brotli by Go.

## The families

| Family | Slugs | Shape |
| --- | --- | --- |
| shared clips | `shared` | the five synthetic source clips |
| direct media | `single-mkv`, `health-failure`, `large-segments`, `split-plain-mkv` | loose payloads and a plain byte split |
| RAR5 | 19 slugs: single, solid, corrupted, `-p` and `-hp` encrypted, multi-member, solid multi-member, multivolume, solid multivolume, filename normalisation and dedupe, unicode names, empty, missing-middle + PAR2 | RARLAB 7.23 |
| RAR4 | 9 slugs: single, solid, corrupted, header- and member-encrypted, multi-member, multivolume, both encrypted variants | RARLAB 6.24 |
| RAR recovery volumes | `rar5-recovery-volume-light`, `rar5-recovery-volume-heavy`, `rar5-recovery-volume-insufficient`, `rar4-recovery-volume-light` | RARLAB `-rv<N>` writes the data volumes and standalone `.rev` recovery volumes in one invocation; Go then withholds whole data volumes (light: one of four with one `.rev`; heavy: two with two; insufficient: two with one, which must fail cleanly). RARLAB 7.23 (RAR5 `.rev`) and 6.24 `-ma4` (RAR3-format `.rev`) |
| nested | `nested-rar`, `nested-3deep`, `nested-5deep`, `nested-obfuscated-split-7z` | archives inside archives, two to five deep |
| 7z | `single-7z`, `single-7z-corrupted`, `7z-encrypted`, `split-7z`, `split-7z-encrypted`, `split-7z-corrupted` | LZMA2, split by byte range |
| obfuscated | `obfuscated-rar`, `obfuscated-rar-unknown-numeric`, `obfuscated-rar-split-topology`, `obfuscated-split-7z` | hex and numeric names: `.10/.11/.12`, `.100/.101/.102` |
| PAR2 | 17 slugs: ordinary repair, heavy damage ×4, insufficient parity, small repair ×5, multivolume, RAR4, 7z, direct payload, sidecar-only rewrite, multi-swap placement | par2cmdline-turbo |
| zip | `zip-unencrypted`, `zip-encrypted`, `zip-corrupted` | stored members; ZipCrypto for the encrypted one |
| tar | `tar-archive`, `tar-corrupted`, `tgz-archive`, `tar-gzip-archive`, `tbz2-archive`, `tar-bzip2-archive`, `targz-archive`, `targz-corrupted` | ustar padded to GNU tar's 10 KiB blocking factor |
| stream codec | `gzip-single`, `gzip-corrupted`, `deflate-single`, `bzip2-single`, `zstd-single`, `brotli-single` | bare streams, no container |
| direct store | 6 slugs | stored, non-solid RAR sets the direct-store router must carry |
| mixed | `mixed-archive` | a RAR beside a loose clip and an NFO |
| uuencode | `uu-release`, `uu-mixed-yenc`, `uu-preamble-tail`, `uu-missing-middle` | the only fixtures nyuu cannot post — it is a yEnc poster with no encoding selector — so their article bodies ship pre-encoded and the seeder posts them itself. uuenview writes the encoding and splits it at line boundaries; uudeview decodes every one back before it is published. `uu-preamble-tail` carries the corpus's one deliberate deviation from oracle output: an unpadded final group, the broken-encoder probe |

Nine scenario directories own no bytes at all — they stage another scenario's
assets through `fixtureAssets`, or the harness injects their fault. They are
declared in `ScenarioOnly`, and `--list` prints them with the reason.

### Damage is deterministic

Every "corrupted" fixture zeroes the same 1 MiB window at 10 MiB, far enough
into the file that the container header still parses and the failure surfaces
as a checksum mismatch rather than an unreadable archive. Truncated variants
drop exactly 1 MiB from the tail. PAR2 damage is expressed in *blocks*: the
recipe reads the slice size back out of the index file par2cmdline wrote and
zeroes a whole number of blocks, so "sixteen of fifty blocks against one
recovery block" stays true when a payload changes size.

## Adding a scenario

1. Add `testdata/<slug>/scenario.json` describing what the product should do.
2. Add the ledger entries for its files to `test-corpus/sources.json`. The
   generator will fill in `size`, `blake3` and `source`; it will not invent an
   entry, because adding a fixture is a reviewed decision.
3. Add the glob to the profiles it belongs to in `test-corpus/profiles.json`.
4. Add a `Recipe` — or, if it carries no bytes of its own, an entry in
   `ScenarioOnly` with the reason.
5. If two scenarios must agree on the same bytes, make an `Artifact` and
   publish it from both.
6. `go test ./internal/fixturegen/` — the table test fails on any scenario
   directory that is neither.
7. `go run ./cmd/fixturegen --scenario <slug>`, then
   `go run ./cmd/corpus verify --all-present --offline`.

## Regeneration is a corpus revision

Generated fixtures are **shape-reproducible, not byte-reproducible**: RAR and
7z stamp creation times into their headers, encryption draws a salt, and the
video encoders are not bit-exact across builds. Re-running a recipe therefore
legitimately changes digests.

So a regeneration is never an in-place replacement. It is a new corpus
revision: run the generator on the pinned toolchains, let it refresh sizes,
digests and provenance in `test-corpus/sources.json`, publish, and pin. The
published bytes are the canonical fixtures; the recipe and the toolchain ids
are recorded so a future revision can reproduce the *shape*.

The zip, tar and stream-codec families are the exception — they are
byte-reproducible from their recipe and their payload, because nothing in the
chain stamps a time. Their payload comes from an encoder, so they only stay
identical while the clip does.

## Fixture naming

Every title in this corpus is invented. Scenarios use synthetic release names
so the harness never carries a real film or television title, and recipes must
keep it that way — including member names inside archives.
