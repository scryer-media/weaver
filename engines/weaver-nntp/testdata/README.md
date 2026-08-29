# weaver-nntp test data

## `uu_logo_full.nntp`

A complete NNTP `BODY` response carrying a uuencode-encoded SVG: status line,
`begin 644 logo-full.svg`, dot-stuffed body, backtick terminator, `end`, and the
multiline terminator.

Adapted from the sabctools test suite (`tests/uufiles/logo_full.nntp`), which
distributes it under the GNU General Public License v2 or later; weaver is
GPL-3.0, so the file is redistributed here under GPL-3.0 as permitted by the
"or later" clause. It is reproduced byte for byte.

It is kept because it is the only genuine field-captured uuencode article in
either reference decoder's suite. Every other uuencode fixture in this
repository is synthesised by an encoder written against the format; this one is
a real posting, and it is what pins weaver's decoder against something nobody
designed to make weaver pass. The expected values it is checked against —
`logo-full.svg`, 2184 decoded bytes, CRC32 `0x6BC2917D` — are the same three the
sabctools suite asserts, so a divergence is a genuine disagreement between the
two decoders rather than a disagreement about the fixture.
