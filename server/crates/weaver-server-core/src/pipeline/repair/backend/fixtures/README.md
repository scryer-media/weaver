# PAR3 backend fixtures

These carriers are unchanged official `par3cmdline` output, copied from the
hex-encoded artifacts in `rarpar/crates/par3-rs/tests/common/mod.rs` at
`ca50d43c81a73dc018c6df137fd3e0004ea8f930`. Decoding that representation changes
no packet bytes.

Producer: `Parchive/par3cmdline` commit
`2971702e501f1350b1c7b9d11369af9157d6ed56`, release CMake build on Debian
bookworm linux/arm64, using portable BLAKE3 and the documented SSE2NEON build
adaptation. Command:

```text
par3 create -B<in> -s2000 -c2 -R -v -C"rarpar oracle" set.par3 a.bin b.txt sub
```

- `a.bin`: 5,000 bytes; byte `i` is `(i * 7 + 3) & 255`.
- `b.txt`: ASCII `qrstuvwxyz`.
- `sub/c.bin`: 4,000 bytes; byte `i` is `(i * 13 + 1) & 255`.
- `set.par3`: 1,050 bytes; SHA-256
  `c268890da0539138002df8c0e28abd00b66a8a0fe6a5d2038c67ee32811550c2`.
- `set.vol0+1.par3`: 3,138 bytes; SHA-256
  `46c56b90bdfa9cb8ad51ae3012929e58c4706f8e968915cbc9f6dfd466c32a23`.

Tests regenerate and damage protected inputs. They do not modify carrier bytes
or construct packets. The second official recovery carrier is intentionally
omitted so the test exercises a recovery deficit followed by one arrival.
