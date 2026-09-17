# Weaver 0.12.3 release notes

## Highlights

- **7z and xz archives are decoded by Weaver's own decoders.** LZMA, LZMA2
  and xz streams now go through `lzma-turbo`, a Rust port of 7-Zip's
  reference decoder, and 7z archives through `sevenz-turbo`, which is built on
  it. Single-threaded decoding runs at 7-Zip's speed, and multi-block xz
  streams and 7z archives written with several threads decode in parallel.
- **7z archives are bounded before they are believed.** Every size a 7z
  header declares (its end header, an encoded header's unpacked size, the
  entry and coder counts, each block's dictionary) is checked against the
  job's extraction limits before anything is allocated for it. An archive
  whose dictionary would not fit the memory ceiling is refused when it is
  opened, not decoded under a permit that pretends it fits.

## What changed

### Archives

- **xz.** The system `liblzma` is no longer linked; nothing in the build
  depends on it. A multi-block xz file decodes in parallel under the job's
  memory budget: the decoder trims its thread count to what the budget can
  hold, and one that cannot hold a single worker falls back to sequential
  decoding. Concurrent xz extractions no longer wait on one process-wide
  parallel decoder.
- **7z.** Conventional extraction decodes LZMA2 blocks with as many threads
  as the post-processing pool has, bounded by the job's memory ceiling.
  Direct unpack chases the download on one thread, so output starts while
  the block is still arriving, and widens to the same thread count whenever
  complete runs have piled up behind it — after a park, or when the
  download outruns one thread — narrowing again once the backlog is gone.
  Its decode reservation includes the room to widen, and a job whose memory
  ceiling leaves no such room keeps decoding on one thread. Header
  checksums are verified as before.
- **Watch-folder 7z inputs** are opened under fixed bounds on the end header
  and the decoder footprint, so a dropped file cannot make Weaver allocate on
  its say-so.
- **AES-256 7z archives** are decrypted through the same AWS-LC backend the
  rest of Weaver's cryptography uses, instead of a separate pure-Rust AES.

### Downloads

- **One bad article can no longer stop a single-server setup.** An article
  with junk between its yEnc trailer and the NNTP terminator failed every
  fetch as a connection fault, which kept its retry budget, counted against
  the server, and made it every recovery probe of that server: with one
  server configured, downloads stopped until the job was removed. The junk
  is now drained, and an article that keeps breaking connections while other
  articles download pays for its retries and fails like any other bad
  article. An outage, where nothing downloads, still spends no budget.
- Failed recovery probes are logged, and pipeline diagnostics name each
  job's worst transport-failure segment.

### Direct unpack

- **Damaged RAR sets no longer lose five minutes.** A chased RAR volume that
  finished after its set armed never published what the recovery data said
  about it, so the PAR2 pass that would have repaired it was not forced and
  the chase waited out its consumption deadline.
- **A job no longer sits in Extracting on bytes already on disk.** Extraction
  could take over a running chase before the chase heard that its last part
  had finished; the handoff now tells it first.

### Extraction

- **RAR extraction writes faster.** Chunk-sized writes go straight to the
  file instead of through an 8 MiB copy, and each member is no longer synced
  to the device on the extraction thread. A member whose output is missing
  or the wrong size after a restart is extracted again, as before.

### PAR3, setup and startup

- A PAR3 recovery volume whose filename disagrees with its contents is
  fetched last instead of dropped, so a renamed volume can still close a
  deficit. The output space check counts each rebuilt file once.
- Codeless first-run setup compares the origin's port with the request's,
  so another page on the same host cannot create the administrator.
- Servers seeded from the environment are probed concurrently under one
  10-second deadline instead of holding startup one after another.

### PAR2 repair

- **PAR2 repair works in 128 MiB by default, up from 64 MiB.** A set with
  tens of thousands of blocks and a few thousand missing was cut into many
  small passes at 64 MiB, and each pass has a fixed cost, so a heavily damaged
  large job could take many minutes. `WEAVER_PAR2_REPAIR_MEMORY_LIMIT_BYTES`
  still overrides it.

### SSH proxies

- **SSH proxy profiles accept Ed25519 host keys only**, over
  `curve25519-sha256` key exchange. ECDSA host keys were accepted before; a
  profile whose server offers only RSA or ECDSA host keys now fails to connect
  with "no matching host key algorithm" until the server is given an Ed25519
  key. Private keys were already Ed25519 only. The RSA, NIST-curve and ML-KEM
  code is no longer built in.

### Logs page

- **The Logs page no longer freezes the app.** Only the rows on screen are
  rendered, so a full 2000-line buffer costs what a screenful does. Bringing
  the window back to the foreground used to re-number and redraw every line
  and could leave the tail marked disconnected while it was connected; a
  line now keeps its number for as long as it is buffered, and a refresh
  that brings nothing new redraws nothing.
- **A job waiting on PAR2 metadata says so once**, not every time its
  completion check comes round.

### Windows

- **The new icon shows after an upgrade.** Windows caches an application's
  icon by path, so an upgraded install could keep showing the previous
  icon on the Start menu, taskbar and shortcut. The tray asks the shell to
  read its icons again the first time it runs after a version change.
