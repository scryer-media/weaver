# Weaver 0.12.4 release notes

## Highlights

- **The Linux arm64 portable build is back.** 0.12.3 never produced one: the
  LZMA decoder's hand-written aarch64 assembly left its assembler macros
  defined, and with link-time optimization those names leaked into the yEnc
  engine's own inline assembly, which then failed to assemble. The decoder now
  discards every macro it defines at the end of the file, so each compilation
  unit starts clean regardless of the order they are emitted in.

## What changed

### Archives

- **An xz file that outgrows its allowance is reported as too large, not as
  corrupt.** A block whose header declares no uncompressed size — what
  single-threaded `xz` writes — was decoded until the extraction limit was
  reached and then asked for its end marker. Finding another block instead,
  the decoder called the file damaged. It now says the stream exceeded the
  allowance it was given, which is what actually happened.

### Dependencies

- `lzma-turbo` 0.3.3, `sevenz-turbo` 0.23.2 and `par2-rs` 0.10.5.
