# Official embedded PAR3 fixture

`archive.zip` is the unmodified output of the pinned official `par3cmdline`
insertion command. `generate.py` builds a deterministic stored ZIP, invokes the
reference, and records its executable digest, arguments, input/output digests
and transcript. It requires no third-party Python packages.

Regenerate with `python3 generate.py /absolute/path/to/par3`. Use reference
revision `2971702e501f1350b1c7b9d11369af9157d6ed56`; the initial fixture uses the
approved macOS/ARM64 scratch adaptation described in `../par3-native/README.md`.
Tests select later packet positions and hide carrier ranges; they never alter
protection packets. This small fixture validates authenticated scan rewinds and
unchanged-arrival I/O reuse. The native Go suite separately exercises large
ZIP, ZIP64 and 7z archives through NNTP and extraction.
