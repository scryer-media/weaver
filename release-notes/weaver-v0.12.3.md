# Weaver 0.12.3 release notes

## Highlights

- **7z and xz archives are decoded by Weaver's own decoders.** LZMA, LZMA2
  and xz streams now go through `lzma-fast`, a Rust port of 7-Zip's
  reference decoder, and 7z archives through `sevenz-fast`, which is built on
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
  Direct unpack still decodes with one thread, because it must produce
  output while the block is arriving. Header checksums are verified as
  before.
- **Watch-folder 7z inputs** are opened under fixed bounds on the end header
  and the decoder footprint, so a dropped file cannot make Weaver allocate on
  its say-so.
- **AES-256 7z archives** are decrypted through the same AWS-LC backend the
  rest of Weaver's cryptography uses; the RustCrypto AES implementation is no
  longer built in.
