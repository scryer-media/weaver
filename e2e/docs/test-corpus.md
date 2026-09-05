# The fixture corpus

Everything under `testdata/` except `scenario.json` is the **fixture corpus**:
237 archives, parity sets, split volumes, recovery volumes and source clips — about 6.2 GiB — that
the seeder posts to the fake NNTP servers before any scenario runs. The bytes
are not in git. They are published as a signed, content-addressed object set;
`go run ./cmd/corpus ensure` — which the harness runs itself before seeding —
reuses what a checkout already has, hydrates the rest from that object set,
and generates only what is still missing.

What *is* in git is the description: one ledger entry per fixture path with its
size, its BLAKE3 digest and where it came from; the named subsets a lane
hydrates; and the one published manifest a checkout trusts.

There is no Git-LFS transport, and nothing is imported from another
repository. A fixture is either **generated** — `go run ./cmd/fixturegen`
rebuilds it from a declarative recipe, running every oracle it uses on a
toolchain pinned by `test-corpus/toolchains.json` — or **blocked**, meaning no
such recipe exists yet. Blocked entries verify like any other file. What they
block is publication.

Every entry is currently `generated`. How the recipes work, which oracle wrote
which byte, and how to add a scenario are in [generators.md](generators.md).

## Files

| Path | Role |
| --- | --- |
| `test-corpus/sources.json` | The ledger. One entry per fixture: path, size, BLAKE3, container format, and its source (`generated` with generator/toolchains/inputs, or `blocked` with a reason). Also the generator table. |
| `test-corpus/profiles.json` | Named subsets as path globs — what one suite hydrates. Derived from the harness's own slug lists. |
| `test-corpus/toolchains.json` | The generator-toolchain lock: five RARLAB writers, the official 7-Zip console binary, par2cmdline-turbo, UUDeview, the digest-pinned FFmpeg encoder image, and the Go writers, each pinned by URL and SHA-256 or by image digest. A toolchain change is a deliberate corpus revision. |
| `test-corpus/lock.json` | The published manifest this checkout hydrates from: its BLAKE3, its URL, the Sigstore identity that must have signed it, and the commit it was published from. Empty digest means nothing is published yet. |
| `cmd/fixturegen`, `internal/fixturegen` | The generator: one declarative recipe per scenario directory, plus the Dockerfiles for the oracle images the lock pins. See [generators.md](generators.md). |
| `.github/workflows/e2e-corpus-publish.yml` | The manual, main-only, protected workflow that reconstructs, verifies, signs and uploads a corpus revision, one runner per recipe family. (Repository root, not `e2e/`.) |

Every digest in this corpus is **BLAKE3**, not SHA-256: these files are
media-shaped and run to hundreds of megabytes, so hashing throughput is the
cost that actually shows up. A full pass over the tree takes a few seconds.

## Commands

```sh
# What developers run before anything else — and what every suite runs itself:
# reuse what matches the ledger, fetch the rest from the published corpus,
# generate only what is still missing (--no-generate for a fetch-only lane).
go run ./cmd/corpus ensure --profile functional
go run ./cmd/corpus ensure --slug rar5-multivolume --quick
task hydrate PROFILE=chaos

# The published corpus only; refuses while nothing is pinned.
go run ./cmd/corpus hydrate --profile functional
task fetch PROFILE=functional

# Ledger vs tree vs lock, offline; --all-present requires the whole corpus.
go run ./cmd/corpus verify --all-present --offline
task corpus:verify -- --all-present --offline

# The same, plus the published manifest and its Sigstore signature.
go run ./cmd/corpus verify --require-signature

# What the profiles resolve to, from the ledger, with no network.
go run ./cmd/corpus profiles

# Build a manifest, its digest and its provenance from the tree.
go run ./cmd/corpus build --out target/test-corpus/build
# (--update-ledger refreshes sizes and digests of paths already listed;
#  it never adds or removes entries — that is a reviewed decision.)

# Publish workflow only.
go run ./cmd/corpus sign --dir target/test-corpus/build
go run ./cmd/corpus publish --dir target/test-corpus/build --dry-run
```

Exit codes: `0` success, `1` findings or failure, `2` usage.

## Profiles

| Profile | What it is |
| --- | --- |
| `functional` | `canonicalFixtureSlugs` — the full functional corpus (232 files, ~5.9 GiB) |
| `chaos` | `chaosFixtureSlugs` plus the STAT health probe (16 files, ~0.7 GiB) |
| `tcp-chaos` | `tcpChaosFixtureSlugs` (16 files, ~0.7 GiB) |
| `restart` | `restartFixtureSlugs` (18 files, ~0.4 GiB) |
| `release-gate` | the union of the four seed profiles the gate's phases run |
| `shared` | the source clips generators encode their payloads from — generator inputs only, not part of any slug profile |
| `all` | everything (237 files, ~6.2 GiB) |

A profile resolves to the sorted ledger paths matching any `include` glob and
no `exclude` glob; every profile excludes `**/scenario.json`, which is tracked
in git and never hydrated. The manifest freezes the resolved lists, so a fetch
never re-derives membership from the working tree, and a profile that resolves
to nothing is an error.

The slug profiles are derived from the slug lists in `internal/weaver/main.go`.
Adding a slug there means adding its glob here — `verify` catches the drift the
moment a fixture appears that no ledger entry describes.

## Verification chain

```
git commit ──► test-corpus/lock.json ──► manifest BLAKE3 ──► manifest.json ──► file BLAKE3 ──► object bytes
                                    └──► Sigstore bundle: cosign verify-blob
                                         --certificate-identity <publish workflow @ refs/heads/main>
                                         --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

- `hydrate` (and the fetch step of `ensure`) verifies the manifest against the locked digest and every object
  against its manifest digest **before writing anything**. Files land through a
  temporary file and a rename, so a fixture path is either whole or absent. A
  file already present with the right size and digest is left alone.
- `verify` recomputes the manifest from the checkout and requires it to equal
  the locked digest, so a ledger edit without a republication fails closed. It
  also checks both directions of the tree: every listed path that exists must
  match, and every fixture that exists must be listed. An unledgered fixture is
  a failure — a fixture the harness can read but nobody described is exactly
  the thing this ledger exists to prevent.
- The identity is matched literally, never as a regexp, so a workflow on
  another branch or in a fork can never satisfy the lock.
- Objects are immutable. Uploads are conditional (`If-None-Match: *`); a `412`
  is followed by a public read-back whose digest must match, and any mismatch
  aborts the whole publication rather than rewriting a key.

## Bootstrap

Nothing is published yet: `test-corpus/lock.json` carries an empty manifest
digest, so `hydrate` refuses and says so, and `ensure` — the harness's own
pre-flight — skips the fetch and generates whatever a checkout is missing.
Once a manifest is pinned the same `ensure` fetches first and generates only
what the published corpus does not carry (a scenario added after the last
publication, say). The corpus files currently live only in developer checkouts
and in what the generator rebuilds.

Getting from here to a hydrating harness is a sequence, not a switch:

1. **Write the generators.** Publication is refused while any ledger entry is
   `blocked`. **This step is done**: `cmd/fixturegen` reproduces all 237
   fixtures from pinned oracles, and no entry is blocked.
2. **Dispatch the publish workflow from `main`.** Generation fans out the
   way the rarpar corpus workflow's does: one stage-0 runner builds the
   artifact cache — the encoded clips and intermediate archives every family
   derives from, built once because the encoder is not byte-reproducible and
   scenario.json pins extracted bytes — plus the shared clips; one runner per
   recipe family then restores that cache, builds only the oracle images its
   own recipes drive (`fixturegen --list-json` is the matrix), generates its
   fixtures and hands them on as an artifact. `assemble` rebuilds the tree
   from the artifacts alone, holds it to the ledger's exact path set,
   refreshes the ledger's sizes and digests, and builds the manifest; the
   publish job signs and uploads exactly what `assemble` verified, then
   prints the lock entry.
3. **Pin it in a reviewed PR** carrying the workflow's updated `sources.json`,
   the rewritten `scenario.json` files (both in the run's artifacts) and the
   lock entry. The workflow never commits; pinning is always a review.
4. From that commit on, `ensure` (and `hydrate`) fetch from the bucket in
   every lane; generation becomes the fallback for what is not published.

The `.gitignore` rules make this concrete today: of everything under
`testdata/`, git sees only the 100 `scenario.json` files. No fixture byte is
staged, and none should be — the ledger is how they are described and the
bucket is where they live.

## Generation is a corpus revision

Generated fixtures are **shape-reproducible, not byte-reproducible**: RAR and
7z stamp creation times into their headers, encryption draws a salt, and some
payloads come from an encoder rather than a fixed byte stream. Re-running a
recipe therefore legitimately changes digests. The zip, tar and stream-codec
families are the exception: nothing in their chain stamps a time, so they are
byte-reproducible from their recipe and their payload.

So a regeneration is never an in-place replacement. It is a new corpus
revision: `go run ./cmd/fixturegen --all` on the pinned toolchains, which
refreshes sizes, digests and provenance of paths already listed, then publish
and pin. It never adds or removes a ledger entry — adding a fixture is a
reviewed decision, and a recipe that produces a file nobody ledgered fails.
The published bytes are the canonical fixtures; the recipe and the toolchain
ids are recorded so a future revision can reproduce the *shape*.

## What the generator produces

`go run ./cmd/corpus verify` prints every blocked path; there are none. Every
fixture is rebuilt by one recipe in `cmd/fixturegen`, and the ledger records
which pinned oracles wrote it and which ledger paths it was built from.

| Family | Scenarios | Files | Size |
| --- | --- | --- | --- |
| shared clips | 1 | 5 | ~313 MiB |
| direct media | 4 | 5 | ~246 MiB |
| RAR5 | 20 | 40 | ~1475 MiB |
| RAR4 | 9 | 13 | ~712 MiB |
| nested | 4 | 4 | ~321 MiB |
| 7z | 6 | 12 | ~479 MiB |
| obfuscated | 4 | 12 | ~329 MiB |
| PAR2 | 18 | 98 | ~933 MiB |
| zip | 3 | 3 | ~246 MiB |
| tar | 8 | 8 | ~665 MiB |
| stream codec | 6 | 6 | ~181 MiB |
| direct store | 6 | 20 | ~102 MiB |
| mixed | 1 | 3 | ~85 MiB |
| RAR recovery volumes | 5 | 19 | ~378 MiB |
| **total** | **95** | **248** | **~6.3 GiB** |

Nine further scenario directories own no bytes at all: they stage another
scenario's assets through `scenario.json`'s `fixtureAssets`, or the harness
injects their fault at post time. `go run ./cmd/fixturegen --list` prints them
with the reason.

The generator's own contract is the ledger: it refreshes size, digest and
provenance for paths already listed, and it never adds or removes an entry. A
recipe that produces a file nobody ledgered — or misses one that is — fails
with both lists, which is what stops a fixture appearing that no entry
describes.

## Fixture naming

Every title in this corpus is invented. Scenarios use synthetic release names
so the harness never carries a real film or television title, and generators
must keep it that way.
