// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// SSE2 tier: rapidyenc `do_encode_sse<ISA_LEVEL_SSE2>` (src/encoder_sse2.cc,
// src/encoder_sse_base.h) on the shared line loop (`x86_common`).
//
// Detection is rapidyenc's SSE2 compare (src/encoder_sse_base.h:224-235):
// four PCMPEQB per 16 bytes, 32 bytes per block. Without PSHUFB rapidyenc
// expands escapes one bit at a time with a shift-and-merge per escape
// (`sse2_expand_bytes`, :101-127). This tier instead stores the plain block,
// then for each escape writes '=', re-stores the block's rest from the
// source one byte later, and patches the escaped byte: two loads and two
// stores per escape, no serial vector chain. At `DENSE` escapes the whole
// block is written again one table entry per byte. A block without escapes finds
// its line end by a branch, as rapidyenc's SSE2 path prefers branching.

use std::arch::x86_64::*;

use super::scalar::encode_simd_frame;
use super::tables::ESCAPED;
use super::x86_common::{Block, EOL, FIRST, enter, first_only_bit, popcnt_table};

pub(super) struct Sse2;

// Escapes at which `store` rewrites the block byte by byte.
const DENSE: usize = 4;

// A source byte's encoding as one little-endian u16: entry `c` is `c + 42`
// (one byte), entry `256 + c` is '=' and `c + 106` (two bytes).
static ENC2: [u16; 512] = build_enc2();

const fn build_enc2() -> [u16; 512] {
    let mut t = [0u16; 512];
    let mut c = 0;
    while c < 256 {
        t[c] = (c as u8).wrapping_add(42) as u16;
        t[256 + c] = b'=' as u16 | (((c as u8).wrapping_add(42 + 64) as u16) << 8);
        c += 1;
    }
    t
}

// 0xff at each byte that encodes to NUL, LF, CR or '='.
#[inline(always)]
unsafe fn cmp(d: __m128i) -> __m128i {
    unsafe {
        let a = _mm_or_si128(
            _mm_cmpeq_epi8(d, _mm_set1_epi8(-42)),
            _mm_cmpeq_epi8(d, _mm_set1_epi8((b'\n' as i8).wrapping_sub(42))),
        );
        let b = _mm_or_si128(
            _mm_cmpeq_epi8(d, _mm_set1_epi8((b'=' as i8).wrapping_sub(42))),
            _mm_cmpeq_epi8(d, _mm_set1_epi8((b'\r' as i8).wrapping_sub(42))),
        );
        _mm_or_si128(a, b)
    }
}

impl Block for Sse2 {
    const N: usize = 32;
    const READ_AHEAD: usize = 64;
    type V = (__m128i, __m128i);
    const BRANCH_NOESC: bool = true;
    // rapidyenc's SSE2 loop loads at the top of each block.
    const PIPELINE: bool = false;

    #[inline(always)]
    unsafe fn load(s: *const u8) -> Self::V {
        unsafe { (_mm_loadu_si128(s.cast()), _mm_loadu_si128(s.add(16).cast())) }
    }

    #[inline(always)]
    unsafe fn crit_mask(v: Self::V) -> u64 {
        unsafe {
            let m = |d| _mm_movemask_epi8(cmp(d)) as u32 as u64;
            m(v.0) | (m(v.1) << 16)
        }
    }

    #[inline(always)]
    unsafe fn store(v: Self::V, s: *const u8, m: u64, p: *mut u8) -> usize {
        unsafe {
            let k42 = _mm_set1_epi8(42);
            _mm_storeu_si128(p.cast(), _mm_add_epi8(v.0, k42));
            _mm_storeu_si128(p.add(16).cast(), _mm_add_epi8(v.1, k42));
            escapes(s, m, p)
        }
    }

    #[inline(always)]
    fn popcnt(x: u64) -> usize {
        popcnt_table(x)
    }
}

// `Sse2::store` after its two plain stores: the block's encoding at `p` is
// already stored, escape by escape, as there.
//
// SAFETY: as `Block::store`.
#[inline(always)]
unsafe fn escapes(s: *const u8, m: u64, p: *mut u8) -> usize {
    unsafe {
        let k42 = _mm_set1_epi8(42);
        let mut mm = m;
        let mut e = 0usize;
        while mm != 0 {
            let j = mm.trailing_zeros() as usize;
            mm &= mm - 1;
            let o = p.add(j + e);
            o.write(b'=');
            _mm_storeu_si128(
                o.add(1).cast(),
                _mm_add_epi8(_mm_loadu_si128(s.add(j).cast()), k42),
            );
            _mm_storeu_si128(
                o.add(17).cast(),
                _mm_add_epi8(_mm_loadu_si128(s.add(j + 16).cast()), k42),
            );
            o.add(1).write((*s.add(j)).wrapping_add(42 + 64));
            e += 1;
            if e == DENSE {
                return dense(s, m, p);
            }
        }
        32 + e
    }
}

// A dense block: the whole block again one byte at a time, one table u16
// stored per source byte and the pointer advanced by its width, which costs
// less than four stores per escape. A fixed count unrolls fully. Returns the
// width.
//
// SAFETY: as `Block::store`.
#[inline(always)]
unsafe fn dense(s: *const u8, m: u64, p: *mut u8) -> usize {
    unsafe {
        let mut q = p;
        let mut i = 0;
        while i < 32 {
            let x = ((m >> i) & 1) as usize;
            let e2 = ENC2[*s.add(i) as usize + 256 * x];
            q.cast::<u16>().write_unaligned(e2.to_le());
            q = q.add(1 + x);
            i += 1;
        }
        q as usize - p as usize
    }
}

// A line's last byte and CRLF at output offset `r` of the block at `s`
// (stored at `p`), `k` the source byte there and `st` 1 when its escape
// straddles the last column. Returns the new source and output; the next
// line's first byte is left to the block that starts the line, which folds
// the first-char rule into its mask (see `main_loop`).
// `x86_common::step`'s line end, without the next-vector bookkeeping.
//
// SAFETY: `k + 2` bytes readable at `s`; 4 bytes writable at `p + r`.
#[inline(always)]
unsafe fn line_end(
    s: *const u8,
    p: *mut u8,
    r: usize,
    k: usize,
    st: usize,
) -> (*const u8, *mut u8) {
    unsafe {
        let e = EOL[*s.add(k) as usize] >> (8 * st);
        p.add(r).cast::<u32>().write_unaligned(e.to_le());
        (s.add(k + 1), p.add(r + 3 + (e >> 27) as usize))
    }
}

// The SSE2 line loop, in rapidyenc's shape (src/encoder_sse_base.h:206-720):
// a block without escapes (`_encode_loop_branch_fast_noesc`, :474-484) is a
// self-contained loop: load, compare, test, store, advance, the column test,
// and back to the load, with no join on a shared block width. A block with
// escapes or a line end leaves that loop, is handled, and the next block
// re-enters it. Rules and output are those of `x86_common::main_loop`.
//
// SAFETY: as `x86_common::main_loop`.
#[inline(always)]
unsafe fn main_loop(
    ls: usize,
    column: &mut usize,
    src: *const u8,
    len: usize,
    dst: *mut u8,
) -> (usize, usize) {
    // Every block reads `READ_AHEAD` bytes (the escape path's re-stores) and
    // a line end the byte two past it; the scalar tail gets the rest.
    if len < 2 * Sse2::N + 1 {
        return (0, 0);
    }
    unsafe {
        let end = src.add(len);
        let limit = end.sub(Sse2::READ_AHEAD);
        let mut s = src;
        let mut p = dst;
        let mut col = *column;
        let last = ls - 1;
        enter(last, &mut col, &mut s, &mut p);
        if col == 0 {
            let c = *s;
            if first_only_bit(c) != 0 {
                p.cast::<[u8; 2]>().write_unaligned(ESCAPED[c as usize]);
                p = p.add(2);
                s = s.add(1);
                col = 2;
            }
        }
        let mut room = (last - col) as isize;
        // One input index counting up to zero toward an end pointer, as
        // rapidyenc (`es[i]`, `while(i < 0)`, src/encoder_sse_base.h:170-172,
        // :720) and the NEON port (neon.rs:333-376).
        let es = limit;
        let mut i = s.offset_from(es);
        // The source bytes that encode to NUL, LF, CR and '=', and the +42.
        let k42 = _mm_set1_epi8(42);
        let cz = _mm_set1_epi8(-42);
        let clf = _mm_set1_epi8((b'\n' as i8).wrapping_sub(42));
        let ccr = _mm_set1_epi8((b'\r' as i8).wrapping_sub(42));
        let ceq = _mm_set1_epi8((b'=' as i8).wrapping_sub(42));
        // The encoded NUL, LF, CR and '='.
        let ez = _mm_setzero_si128();
        let elf = _mm_set1_epi8(b'\n' as i8);
        let ecr = _mm_set1_epi8(b'\r' as i8);
        let eeq = _mm_set1_epi8(b'=' as i8);
        // 1 when the next block starts a line (after a line end left to
        // Rust): the asm enters at its line-start block.
        let mut fl = 0usize;
        while i <= 0 {
            // The block loop in asm. LLVM joins the two block kinds into one
            // loop, carries copies of the room and the index through the
            // block without escapes, and lays the escape path out as a chain
            // of taken jumps; written out, each kind has its own loop with
            // its own copy of the load and compare (tail duplication), so a
            // run of either kind takes one branch per block, as rapidyenc's
            // `noesc` loop does (:206-264, :474-484). The escape path is
            // this tier's `escapes` for up to three escapes. The escape loop's
            // compare is rapidyenc's: raw bytes against the bytes that encode
            // to NUL, LF, CR and '=', OR-ed as a tree
            // (src/encoder_sse_base.h:224-235), so the +42 is off the path to
            // the block-kind branch, which mispredicts where escapes are
            // scattered. The plain block is stored before the test, as this
            // tier's `store` does.
            //
            // It leaves with `m` set and nothing advanced: `w` 0 for a block
            // with four or more escapes, found before any patch by clearing
            // the low bit three times (rapidyenc tests for many bits the
            // same way before any expand, :278-281), or `w` the width of a
            // patched block whose output reaches the line's last column.
            // Or it leaves with `m` clear and `i` past zero.
            //
            // A block without escapes that reaches the last column ends the
            // line inside the asm, as rapidyenc's `noesc` branch goes
            // straight to its line end (:484, :616-627): output offset `r`
            // (the room plus 32) is source byte `r`, so its EOL entry is
            // stored at the output pointer plus the (negative) room. The
            // line's first byte is then always taken: its `FIRST` entry
            // stored and the width one more when that entry is an escape,
            // without a branch, as x86_common.rs:551-557 does, and the next
            // block starts one byte on. The index waits on no data, and the
            // block-kind branch sees only true escapes. Leaving the asm here
            // instead would test the block kind again after the exit, a
            // branch on the data at every line end.
            macro_rules! load_cmp {
                () => {
                    concat!(
                        "movdqu {a}, xmmword ptr [{es} + {i}]\n",
                        "movdqu {b}, xmmword ptr [{es} + {i} + 16]\n",
                        "movdqa {x}, {a}\n",
                        "pcmpeqb {x}, {cz}\n",
                        "movdqa {y}, {a}\n",
                        "pcmpeqb {y}, {clf}\n",
                        "por {x}, {y}\n",
                        "movdqa {y}, {a}\n",
                        "pcmpeqb {y}, {ceq}\n",
                        "movdqa {z}, {a}\n",
                        "pcmpeqb {z}, {ccr}\n",
                        "por {y}, {z}\n",
                        "por {x}, {y}\n",
                        "pmovmskb {m:e}, {x}\n",
                        "movdqa {x}, {b}\n",
                        "pcmpeqb {x}, {cz}\n",
                        "movdqa {y}, {b}\n",
                        "pcmpeqb {y}, {clf}\n",
                        "por {x}, {y}\n",
                        "movdqa {y}, {b}\n",
                        "pcmpeqb {y}, {ceq}\n",
                        "movdqa {z}, {b}\n",
                        "pcmpeqb {z}, {ccr}\n",
                        "por {y}, {z}\n",
                        "por {x}, {y}\n",
                        "pmovmskb {c:e}, {x}\n",
                        "paddb {a}, {k42}\n",
                        "paddb {b}, {k42}\n",
                        "movdqu xmmword ptr [{p}], {a}\n",
                        "movdqu xmmword ptr [{p} + 16], {b}\n",
                        "shl {c:e}, 16\n",
                        "or {m:e}, {c:e}\n",
                    )
                };
            }
            // The no-escape loop compares the encoded bytes instead: the +42
            // serves the store and the compare, one copy fewer per vector,
            // and the block is stored before the test, so the last compare
            // of each vector may overwrite it.
            macro_rules! load_cmp_enc {
                () => {
                    concat!(
                        "movdqu {a}, xmmword ptr [{es} + {i}]\n",
                        "movdqu {b}, xmmword ptr [{es} + {i} + 16]\n",
                        "paddb {a}, {k42}\n",
                        "paddb {b}, {k42}\n",
                        "movdqu xmmword ptr [{p}], {a}\n",
                        "movdqu xmmword ptr [{p} + 16], {b}\n",
                        "movdqa {x}, {a}\n",
                        "pcmpeqb {x}, {ez}\n",
                        "movdqa {y}, {a}\n",
                        "pcmpeqb {y}, {elf}\n",
                        "por {x}, {y}\n",
                        "movdqa {y}, {a}\n",
                        "pcmpeqb {y}, {ecr}\n",
                        "por {x}, {y}\n",
                        "pcmpeqb {a}, {eeq}\n",
                        "por {a}, {x}\n",
                        "pmovmskb {m:e}, {a}\n",
                        "movdqa {x}, {b}\n",
                        "pcmpeqb {x}, {ez}\n",
                        "movdqa {y}, {b}\n",
                        "pcmpeqb {y}, {elf}\n",
                        "por {x}, {y}\n",
                        "movdqa {y}, {b}\n",
                        "pcmpeqb {y}, {ecr}\n",
                        "por {x}, {y}\n",
                        "pcmpeqb {b}, {eeq}\n",
                        "por {b}, {x}\n",
                        "pmovmskb {c:e}, {b}\n",
                        "shl {c:e}, 16\n",
                        "or {m:e}, {c:e}\n",
                    )
                };
            }
            // Escape `e` of the block (`e` earlier ones): the lowest bit of
            // `$b` is its source index `j`; '=' at `p + j + e`, the rest of
            // the block re-stored one byte on, the escaped byte patched in,
            // the width counted.
            macro_rules! esc {
                ($b:literal, $e:literal) => {
                    concat!(
                        "bsf {j}, ",
                        $b,
                        "\n",
                        "mov byte ptr [{p} + {j} + ",
                        $e,
                        "], 0x3d\n",
                        "movdqu {x}, xmmword ptr [{s} + {j}]\n",
                        "paddb {x}, {k42}\n",
                        "movdqu xmmword ptr [{p} + {j} + ",
                        $e,
                        " + 1], {x}\n",
                        "movdqu {x}, xmmword ptr [{s} + {j} + 16]\n",
                        "paddb {x}, {k42}\n",
                        "movdqu xmmword ptr [{p} + {j} + ",
                        $e,
                        " + 17], {x}\n",
                        "movzx {c:e}, byte ptr [{s} + {j}]\n",
                        "add {c:e}, 106\n",
                        "mov byte ptr [{p} + {j} + ",
                        $e,
                        " + 1], {c:l}\n",
                        "inc {w:e}\n",
                    )
                };
            }
            let m: u32;
            let w: usize;
            core::arch::asm!(
                "xor {m:e}, {m:e}",
                "test {fl}, {fl}",
                "jnz 7f",
                "jmp 3f",
                // A block without escapes: advance, or leave at a line end
                // or the end.
                "2:",
                "add {p}, 32",
                "sub {room}, 32",
                "js 6f",
                "add {i}, 32",
                "jg 9f",
                "3:",
                load_cmp_enc!(),
                "jz 2b",
                // A block with escapes.
                "4:",
                "lea {s}, [{es} + {i}]",
                "lea {c}, [{m:r} - 1]",
                "mov {t}, {m:r}",
                "and {t}, {c}",
                "lea {c}, [{t} - 1]",
                "mov {u}, {t}",
                "and {u}, {c}",
                "lea {c}, [{u} - 1]",
                "and {c}, {u}",
                "jnz 8f",
                "mov {w:e}, 32",
                esc!("{m:r}", "0"),
                "test {t}, {t}",
                "jz 5f",
                esc!("{t}", "1"),
                "test {u}, {u}",
                "jz 5f",
                esc!("{u}", "2"),
                "5:",
                "cmp {room}, {w}",
                "jl 12f",
                "xor {m:e}, {m:e}",
                "sub {room}, {w}",
                "add {p}, {w}",
                "add {i}, 32",
                "jg 9f",
                load_cmp!(),
                "jnz 4b",
                "jmp 2b",
                "6:",
                "lea {c}, [{i} + {room} + 32]",
                "lea {s}, [rip + {eol}]",
                "movzx {j:e}, byte ptr [{es} + {c}]",
                "mov {j:e}, dword ptr [{s} + 4*{j}]",
                "mov dword ptr [{p} + {room}], {j:e}",
                "shr {j:e}, 27",
                "lea {p}, [{p} + {room} + 3]",
                "add {p}, {j}",
                "lea {i}, [{c} + 1]",
                "mov {room}, {last}",
                // The block that starts a line.
                "7:",
                "test {i}, {i}",
                "jg 9f",
                "lea {s}, [rip + {first}]",
                "movzx {t:e}, byte ptr [{es} + {i}]",
                "movzx {t:e}, word ptr [{s} + 2*{t}]",
                "mov word ptr [{p}], {t:x}",
                "xor {u:e}, {u:e}",
                "cmp {t:l}, 0x3d",
                "sete {u:l}",
                "lea {p}, [{p} + {u} + 1]",
                "sub {room}, {u}",
                "dec {room}",
                "inc {i}",
                "jle 3b",
                "jmp 9f",
                // An escape block that reaches the last column. With one
                // escape, `locate_one` (x86_common.rs:108-111) and the line
                // end of `line_end`; with more, leave for `locate`.
                "12:",
                "test {t}, {t}",
                "jnz 9f",
                "bsf {j}, {m:r}",
                "xor {c:e}, {c:e}",
                "cmp {room}, {j}",
                "seta {c:l}",
                "mov {u}, {room}",
                "sub {u}, {c}",
                "lea {c}, [{j} + 1]",
                "xor {w:e}, {w:e}",
                "cmp {room}, {c}",
                "sete {w:l}",
                "movzx {j:e}, byte ptr [{s} + {u}]",
                "lea {c}, [rip + {eol}]",
                "mov {j:e}, dword ptr [{c} + 4*{j}]",
                "mov {c:e}, {j:e}",
                "shr {c:e}, 8",
                "test {w}, {w}",
                "cmovnz {j:e}, {c:e}",
                "mov dword ptr [{p} + {room}], {j:e}",
                "shr {j:e}, 27",
                "lea {p}, [{p} + {room} + 3]",
                "add {p}, {j}",
                "lea {i}, [{i} + {u} + 1]",
                "mov {room}, {last}",
                "xor {m:e}, {m:e}",
                "jmp 7b",
                "8:",
                "xor {w:e}, {w:e}",
                "9:",
                es = in(reg) es,
                i = inout(reg) i,
                p = inout(reg) p,
                room = inout(reg) room,
                m = out(reg) m,
                c = out(reg) _,
                s = out(reg) _,
                t = out(reg) _,
                u = out(reg) _,
                j = out(reg) _,
                w = out(reg) w,
                last = in(reg) last,
                fl = in(reg) fl,
                eol = sym EOL,
                first = sym FIRST,
                a = out(xmm_reg) _,
                b = out(xmm_reg) _,
                x = out(xmm_reg) _,
                y = out(xmm_reg) _,
                z = out(xmm_reg) _,
                k42 = in(xmm_reg) k42,
                cz = in(xmm_reg) cz,
                clf = in(xmm_reg) clf,
                ccr = in(xmm_reg) ccr,
                ceq = in(xmm_reg) ceq,
                ez = in(xmm_reg) ez,
                elf = in(xmm_reg) elf,
                ecr = in(xmm_reg) ecr,
                eeq = in(xmm_reg) eeq,
                options(nostack),
            );
            if m != 0 {
                let s = es.offset(i);
                let m = m as u64;
                // The asm stored the plain block already, and patched it
                // unless it is dense.
                let w = if w == 0 { dense(s, m, p) } else { w };
                let nr = room - w as isize;
                if nr >= 0 {
                    p = p.add(w);
                    i += Sse2::N as isize;
                    room = nr;
                    fl = 0;
                } else {
                    let r = room as usize;
                    let (k, st) = Sse2::locate(m, r);
                    let ns;
                    (ns, p) = line_end(s, p, r, k, st);
                    i = ns.offset_from(es);
                    room = last as isize;
                    fl = 1;
                    continue;
                }
            } else {
                break;
            }
        }
        let s = es.offset(i);
        *column = last - room as usize;
        (s as usize - src as usize, p as usize - dst as usize)
    }
}

// SAFETY: the `RawEncodeFn` contract; the CPU has SSE2.
#[target_feature(enable = "sse2")]
pub(super) unsafe fn encode_raw(
    line_size: usize,
    column: &mut usize,
    src: *const u8,
    len: usize,
    dst: *mut u8,
    is_end: bool,
) -> usize {
    unsafe {
        encode_simd_frame(
            line_size,
            column,
            src,
            len,
            dst,
            is_end,
            |ls, col, s, n, d| main_loop(ls, col, s, n, d),
        )
    }
}
