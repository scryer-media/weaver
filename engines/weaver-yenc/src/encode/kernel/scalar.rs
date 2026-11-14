// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The scalar encoder, and the frame every SIMD tier runs inside.
//
// `encode_raw` is a faithful port of rapidyenc `do_encode_generic`
// (src/encoder.cc:33-151 at upstream d223ecc). `encode_simd_frame` is a port
// of the `do_encode_simd` wrapper (src/encoder_common.h:23-97): it runs a
// tier's main loop, then finishes whatever the main loop left with the same
// scalar tail rapidyenc uses. `encode_generic` starts each line with one
// 4-byte store of CRLF and the first byte (rapidyenc src/encoder.cc:117-129),
// so it may store one byte past the returned length, inside the
// `encode_max_len` budget (the `RawEncodeFn` contract in `mod.rs` allows it).
//
// Column bookkeeping uses `isize` like rapidyenc's `int`; the domain checks
// in `mod.rs` keep every value far from overflow.

use super::tables::{DOT, ESCAPE, ESCAPED};

// Lines shorter than this skip the SIMD main loop entirely and run the scalar
// encoder (rapidyenc src/encoder_common.h:25-28). A tier frame must keep it:
// rapidyenc's output for these sizes comes from `do_encode_generic`.
pub(super) const SIMD_MIN_LINE_SIZE: usize = 12;

// Write one byte and advance the cursor.
//
// SAFETY (callers): `*p` is inside the destination, which the caller sized to
// `encode_max_len(len, line_size)`; the bound argument is in `mod.rs`.
#[inline(always)]
unsafe fn put1(p: &mut *mut u8, b: u8) {
    unsafe {
        p.write(b);
        *p = p.add(1);
    }
}

// Write two bytes and advance the cursor. Same safety contract as `put1`.
#[inline(always)]
unsafe fn put2(p: &mut *mut u8, b: [u8; 2]) {
    unsafe {
        p.cast::<[u8; 2]>().write_unaligned(b);
        *p = p.add(2);
    }
}

// `opaque(p)` and `opaque2(p, col)` return their arguments unchanged,
// through an empty `asm!` the optimiser cannot see into. They end the escape
// branch of a per-byte `if`: without them LLVM merges the two branches'
// `p += 1` / `p += 2` (and the matching `col` updates) into one add of a phi,
// which costs every unescaped byte a constant move, and on x86-64 a taken
// jump. With them each branch keeps its own increment, as in rapidyenc's
// compiled loop (a post-indexed store on AArch64, `inc` on x86-64). They emit
// no instruction. (avx512vbmi2.rs uses the same barrier.)
#[inline(always)]
// The template is empty, so the pointer is never dereferenced.
#[allow(unused_mut, clippy::pointers_in_nomem_asm_block)]
fn opaque(mut p: *mut u8) -> *mut u8 {
    #[cfg(any(
        target_arch = "x86",
        target_arch = "x86_64",
        target_arch = "arm",
        target_arch = "aarch64",
        target_arch = "riscv32",
        target_arch = "riscv64",
        target_arch = "loongarch64"
    ))]
    // SAFETY: an empty template; it touches nothing but the register.
    unsafe {
        std::arch::asm!("/* {0} */", inout(reg) p, options(pure, nomem, nostack, preserves_flags));
    }
    p
}

#[inline(always)]
// The template is empty, so the pointer is never dereferenced.
#[allow(unused_mut, clippy::pointers_in_nomem_asm_block)]
fn opaque2(mut p: *mut u8, mut col: isize) -> (*mut u8, isize) {
    #[cfg(any(
        target_arch = "x86",
        target_arch = "x86_64",
        target_arch = "arm",
        target_arch = "aarch64",
        target_arch = "riscv32",
        target_arch = "riscv64",
        target_arch = "loongarch64"
    ))]
    // SAFETY: an empty template; it touches nothing but the two registers.
    unsafe {
        std::arch::asm!(
            "/* {0} {1} */",
            inout(reg) p,
            inout(reg) col,
            options(pure, nomem, nostack, preserves_flags)
        );
    }
    (p, col)
}

// Bytes written between two cursors in the same destination.
#[inline(always)]
fn dist(from: *mut u8, to: *mut u8) -> isize {
    (to as usize).wrapping_sub(from as usize) as isize
}

// Scalar tier: rapidyenc `do_encode_generic`, plus the `len == 0` guard of
// the `do_encode_simd` wrapper.
//
// Every shipped rapidyenc build on x86 and ARM routes `rapidyenc_encode_ex`
// through `do_encode_simd`, which returns 0 for empty input before touching
// anything (src/encoder_common.h:24). `do_encode_generic` itself reads one
// byte past an empty input when the column is 0 and reads `dest[-1]` when it
// is not; that is undefined, so ours is the wrapper's: no output, column
// unchanged.
//
// # Safety
//
// The `RawEncodeFn` contract in `mod.rs`: `src` is readable for `len` bytes,
// `dst` is writable for `encode_max_len(len, line_size)` bytes, the two do
// not overlap, and `line_size` / `*column` are inside the documented domain.
pub(super) unsafe fn encode_raw(
    line_size: usize,
    column: &mut usize,
    src: *const u8,
    len: usize,
    dst: *mut u8,
    is_end: bool,
) -> usize {
    // The loop's code starts 16 bytes into a 64-byte line, wherever the
    // linker puts this function. Measured on Zen 4: with the loop head at
    // offset 0, 32 or 48 of its line, the escape branches mispredict twice as
    // often (160M against 79M per 4 GiB of random data). The jump runs once
    // per call; the padding never runs.
    #[cfg(target_arch = "x86_64")]
    unsafe {
        core::arch::asm!(
            "jmp 2f",
            ".p2align 6, 0xcc",
            ".skip 16, 0xcc",
            "2:",
            options(nomem, nostack, preserves_flags)
        )
    };
    if len == 0 {
        return 0;
    }
    // SAFETY: forwarded contract; `len >= 1` as `encode_generic` requires.
    unsafe { encode_generic(line_size, column, src, len, dst, is_end) }
}

// rapidyenc `do_encode_generic` (src/encoder.cc:30-145), line for line, with
// its negative index `i` running from `-len` up to 0 against `es`, the end
// of the input.
//
// # Safety
//
// As `encode_raw`, and `len >= 1`.
#[inline(always)]
pub(super) unsafe fn encode_generic(
    line_size: usize,
    column: &mut usize,
    src: *const u8,
    len: usize,
    dst: *mut u8,
    is_end: bool,
) -> usize {
    debug_assert!(len >= 1);
    let ls = line_size as isize;
    let mut p = dst;
    let mut col = *column as isize;

    // SAFETY, whole function: every `es.offset(i)` read happens with
    // `-len <= i < 0` (each read is guarded by a loop or `if` test on `i`,
    // the same tests rapidyenc uses). The output is at most 2 bytes per input
    // byte plus 2 per line break plus one trailing escape; the line-start
    // store writes 4 bytes for 3 or 4, so at most one byte lands past that,
    // and `encode_max_len` has 64 bytes of room beyond it (see `mod.rs`).
    unsafe {
        let es = src.add(len);
        let mut i = -(len as isize);
        if col == 0 {
            let c = *es.offset(i);
            i += 1;
            let e = ESCAPED[c as usize];
            if e[0] != 0 {
                put2(&mut p, e);
                col = 2;
            } else {
                put1(&mut p, c.wrapping_add(42));
                col = 1;
            }
        }
        'end: while i < 0 {
            // Main line: eight bytes at a time while at least nine input bytes
            // remain beyond them and the line has room for eight more.
            // Where the last run of eight began, once one ran. `NonNull`
            // keeps the "none ran" test a null test, as rapidyenc's null
            // `sp` is, without a null pointer that could reach a write.
            let mut sp: Option<std::ptr::NonNull<u8>> = None;
            while i < -1 - 8 && ls - col - 1 > 8 {
                // SAFETY: `p` points into the destination, so it is not null.
                sp = Some(std::ptr::NonNull::new_unchecked(p));
                let start = p;
                for k in 0..8 {
                    let c = *es.offset(i + k);
                    let e = ESCAPE[c as usize];
                    if e != 0 {
                        put1(&mut p, e);
                    } else {
                        // Escapes are rare (4 of 256 byte values): keeping
                        // them out of line lets the loop end fall through
                        // to its back edge, one taken branch per eight bytes.
                        core::hint::cold_path();
                        put2(&mut p, ESCAPED[c as usize]);
                        p = opaque(p);
                    }
                }
                i += 8;
                col += dist(start, p);
            }
            if let Some(sp) = sp
                && col >= ls - 1
            {
                // The last eight overflowed the line: undo them and redo them
                // one at a time below.
                let sp = sp.as_ptr();
                col -= dist(sp, p);
                p = sp;
                i -= 8;
            }
            // Remaining middle-of-line bytes.
            while col < ls - 1 {
                let c = *es.offset(i);
                i += 1;
                let e = ESCAPE[c as usize];
                if e != 0 {
                    put1(&mut p, e);
                    col += 1;
                } else {
                    put2(&mut p, ESCAPED[c as usize]);
                    col += 2;
                    (p, col) = opaque2(p, col);
                }
                if i >= 0 {
                    break 'end;
                }
            }
            // Last byte of the line: TAB and SPACE are escaped here, '.' is
            // not. rapidyenc does not advance `col` for it.
            if col < ls {
                let c = *es.offset(i);
                i += 1;
                // One 2-byte load and test, as rapidyenc's `uint16_t` table
                // gives it; testing `e[0]` would split the store in two.
                let e = u16::from_le_bytes(ESCAPED[c as usize]);
                if e != 0 && c != DOT {
                    put2(&mut p, e.to_le_bytes());
                } else {
                    put1(&mut p, c.wrapping_add(42));
                }
            }
            if i >= 0 {
                break;
            }
            // Line break and the next line's first byte in one 4-byte store:
            // CRLF then the escape pair, or CRLF, the byte and a spare byte
            // the next write overwrites.
            let c = *es.offset(i);
            i += 1;
            let e = u16::from_le_bytes(ESCAPED[c as usize]);
            let first = if e != 0 {
                e
            } else {
                u16::from(c.wrapping_add(42))
            };
            let w = u32::from_le_bytes([b'\r', b'\n', 0, 0]) | u32::from(first) << 16;
            p.cast::<[u8; 4]>().write_unaligned(w.to_le_bytes());
            let wide = (e != 0) as isize;
            p = p.offset(3 + wide);
            col = 1 + wide;
        }
        if is_end {
            fix_trailing_whitespace(dst, &mut p, &mut col);
        }
    }
    *column = col as usize;
    dist(dst, p) as usize
}

// The article's last byte may not be a bare TAB or SPACE: escape it in place.
// (rapidyenc src/encoder.cc:140-148 and src/encoder_common.h:85-94.)
//
// # Safety
//
// At least one byte was written at `dst` before `*p`, and `*p` has room for
// one more byte under the `encode_max_len` budget.
#[inline(always)]
unsafe fn fix_trailing_whitespace(dst: *mut u8, p: &mut *mut u8, col: &mut isize) {
    debug_assert!(dist(dst, *p) >= 1);
    // SAFETY: as stated above.
    unsafe {
        let last = p.sub(1);
        let lc = *last;
        if lc == b'\t' || lc == b' ' {
            last.write(b'=');
            put1(p, lc.wrapping_add(64));
            *col += 1;
        }
    }
}

// The SIMD frames' path for lines too short for a vector loop: rapidyenc
// `do_encode_simd` calls the separately compiled `do_encode_generic`
// (src/encoder_common.h:25-28). Out of line and used only by the frames, so
// the scalar loop's size never moves a SIMD kernel's code, and the scalar
// tier's own `encode_raw` keeps its inlined copy.
//
// # Safety
//
// The `RawEncodeFn` contract, with `len >= 1`.
#[inline(never)]
unsafe fn encode_short_lines(
    line_size: usize,
    column: &mut usize,
    src: *const u8,
    len: usize,
    dst: *mut u8,
    is_end: bool,
) -> usize {
    // SAFETY: forwarded contract.
    unsafe { encode_generic(line_size, column, src, len, dst, is_end) }
}

// The frame every SIMD tier runs in: rapidyenc `do_encode_simd`.
//
// `main_loop(line_size, column, src, len, dst)` encodes some prefix of the
// input the way the matching rapidyenc kernel does, updates `*column`, and
// returns `(consumed, written)`. It may consume nothing. The frame then
// encodes `src[consumed..]` at `dst + written` with rapidyenc's scalar tail
// and applies the end-of-article rule. The main loop is only called when
// `len >= 1` and `line_size >= SIMD_MIN_LINE_SIZE`; any further minimum
// (rapidyenc NEON wants `line_size >= 64` and more than 63 input bytes) is
// the main loop's own early return, exactly as in rapidyenc.
//
// A main loop may store past `dst + written` (vector stores) but never past
// `dst + encode_max_len(len, line_size)`; the bytes past `written` are
// overwritten by the tail or ignored.
//
// # Safety
//
// The `RawEncodeFn` contract, and the main loop must keep it: `consumed <=
// len`, every byte it reports written initialised, and no write outside the
// budget above.
#[inline(always)]
pub(super) unsafe fn encode_simd_frame<F>(
    line_size: usize,
    column: &mut usize,
    src: *const u8,
    len: usize,
    dst: *mut u8,
    is_end: bool,
    main_loop: F,
) -> usize
where
    F: FnOnce(usize, &mut usize, *const u8, usize, *mut u8) -> (usize, usize),
{
    if len < 1 {
        return 0;
    }
    if line_size < SIMD_MIN_LINE_SIZE {
        // SAFETY: forwarded contract, `len >= 1`.
        return unsafe { encode_short_lines(line_size, column, src, len, dst, is_end) };
    }
    let (consumed, written) = main_loop(line_size, column, src, len, dst);
    debug_assert!(consumed <= len);
    // SAFETY: forwarded contract; the main loop reported `consumed <= len`
    // and `written` initialised bytes inside the budget.
    unsafe { encode_tail(line_size, column, src, consumed, len, dst, written, is_end) }
}

// The scalar loop after the SIMD kernel in rapidyenc `do_encode_simd`
// (src/encoder_common.h:39-94): encodes `src[start..len]` at `dst + written`,
// returns the total bytes written from `dst`.
//
// # Safety
//
// As `encode_simd_frame`; `start <= len`, `written` bytes at `dst` are
// initialised, and `start + written >= 1` whenever `is_end` may look back at
// the last written byte (true whenever `len >= 1`: either the main loop wrote
// something or the tail will).
#[allow(clippy::too_many_arguments)]
#[inline(always)]
pub(super) unsafe fn encode_tail(
    line_size: usize,
    column: &mut usize,
    src: *const u8,
    start: usize,
    len: usize,
    dst: *mut u8,
    written: usize,
    is_end: bool,
) -> usize {
    let ls = line_size as isize;
    let mut col = *column as isize;
    let mut i = start;
    // SAFETY, whole block: reads are at `i < len`, guarded as in rapidyenc;
    // writes stay inside the `encode_max_len` budget for the reasons given
    // in `encode_generic`.
    unsafe {
        let mut p = dst.add(written);
        if col == 0 && i < len {
            let c = *src.add(i);
            i += 1;
            let e = ESCAPED[c as usize];
            if e[0] != 0 {
                put2(&mut p, e);
                col = 2;
            } else {
                put1(&mut p, c.wrapping_add(42));
                col = 1;
            }
        }
        while i < len {
            let mut c = *src.add(i);
            i += 1;
            if col < ls - 1 {
                let e = ESCAPE[c as usize];
                if e == 0 {
                    put2(&mut p, [b'=', c.wrapping_add(42 + 64)]);
                    col += 2;
                } else {
                    put1(&mut p, e);
                    col += 1;
                }
            } else {
                if col < ls {
                    let e = ESCAPED[c as usize];
                    if e[0] != 0 && c != DOT {
                        put2(&mut p, e);
                    } else {
                        put1(&mut p, c.wrapping_add(42));
                    }
                    if i == len {
                        break;
                    }
                    c = *src.add(i);
                    i += 1;
                }
                put2(&mut p, *b"\r\n");
                let e = ESCAPED[c as usize];
                if e[0] != 0 {
                    put2(&mut p, e);
                    col = 2;
                } else {
                    put1(&mut p, c.wrapping_add(42));
                    col = 1;
                }
            }
        }
        if is_end {
            fix_trailing_whitespace(dst, &mut p, &mut col);
        }
        *column = col as usize;
        dist(dst, p) as usize
    }
}
