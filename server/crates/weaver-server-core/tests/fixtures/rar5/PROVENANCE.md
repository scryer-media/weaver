# Mixed-member RAR fixture

`rar5_multifile_lz.rar` is copied unchanged from the rarpar generated test
corpus at commit `7cca1455b3fc8f5391f4d9a358a7de69f0d5fba3`, from
`crates/unrar-rs/tests/fixtures/rar5/rar5_multifile_lz.rar`.

Its provenance is recorded in that repository's `test-corpus/sources.json`:
the `edge_cases` generator uses the RARLAB 7.20 writer. The archive contains
the synthetic stored members `hello.txt` and `second.txt`, and the compressed
member `zeros_64k.bin`. It is part of the project's GPL-3.0 test corpus.

- Size: 235 bytes
- BLAKE3: `c19a1c2ba9a6fc47004ad1fb23c47d61e70597b99cffc865643a16e41aafc176`
- SHA-256: `71dd0c6c06873ea2957a7db76a5909d8660a489f26a84cabba8bad8f3415f7e8`

Preserve the corpus bytes. Regeneration uses the original writer recipe;
header timestamps mean it is reproducible in archive structure, not byte for byte.
