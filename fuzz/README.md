# Weaver fuzz targets

The standalone fuzz crate covers Weaver's untrusted NZB XML and yEnc article
boundaries without adding fuzzing dependencies to shipped binaries or ordinary
workspace builds.

Build both targets with the installed nightly toolchain:

```bash
cargo +nightly fuzz build -O --debug-assertions
```

Use a temporary writable corpus for local campaigns so the checked-in seed
inputs stay unchanged:

```bash
cp -R fuzz/seeds/nzb_parser /tmp/weaver-nzb-corpus
cargo +nightly fuzz run nzb_parser /tmp/weaver-nzb-corpus
```

Replace `nzb_parser` with `yenc_article` for the yEnc target. To reproduce a
crash downloaded from CI, pass its path after the target name:

```bash
cargo +nightly fuzz run nzb_parser path/to/crash-input
```

For a campaign that should accumulate coverage across runs, copy the seeds to
a durable directory outside the checkout and keep reusing that directory.
Periodically reduce it before review:

```bash
cargo +nightly fuzz cmin -O --debug-assertions nzb_parser /path/to/nzb-corpus
```

The checked-in seeds are a small, reviewed semantic corpus distilled from
longer local campaigns; generated hash-named inputs should be minimized and
inspected before being added here.
