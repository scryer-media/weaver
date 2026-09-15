# Official shared-file PAR3 indexes

The three unmodified indexes come from official `par3cmdline` revision
`2971702e501f1350b1c7b9d11369af9157d6ed56`. Cauchy and FFT use different block
sizes over identical `payload.bin` bytes; the conflict set describes an alternate
input differing at byte zero. No packet is edited. Only complete recovery
carriers are omitted from these metadata-only fixtures.

From this directory, run `go run generate.go /absolute/path/to/par3` to regenerate
all indexes, creation transcripts and input/output provenance. The Go generator
uses only the standard library. The initial generator is the approved macOS
scratch reference described in `../par3-native/README.md`.

`TestPar3GeometryE2E` independently creates the same shared inputs and exercises
verification, repair, conflict refusal and delivery through NNTP. The Rust test
uses these indexes to assert consistency checking requires no protected-source
reads, including repeated assessments.
