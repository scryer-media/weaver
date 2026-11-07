// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The memmove the static x86_64 musl build links in place of musl's own.
// musl moves an overlapping range to a higher address with a backward
// `rep movsb`, which x86 runs a byte at a time behind a long start-up. The
// bzip2 decoder's move-to-front list makes exactly that move, a few bytes up
// by one, for every symbol it decodes: under musl's memmove a bzip2 7z
// decoded at half the speed of the C library on the same machine. This one
// moves a short range with every load ahead of every store, a long forward
// range with `rep movsb`, and a long backward range in 16-byte chunks from
// the top down.
//
// Nothing in here may become a call to memmove: the loop moves its chunks
// through inline assembly, and every other move has a fixed size, which
// the compiler expands in place.

use core::arch::asm;
use core::arch::x86_64::{__m128i, _mm_loadu_si128, _mm_storeu_si128};

/// Moves `n` bytes from `src` to `dst`; the ranges may overlap.
///
/// # Safety
///
/// `src` must be valid to read and `dst` valid to write for `n` bytes.
pub(crate) unsafe fn move_bytes(dst: *mut u8, src: *const u8, n: usize) -> *mut u8 {
    // SAFETY: the caller's contract covers every access below, each within
    // the first `n` bytes of `src` or `dst`.
    unsafe {
        if n <= 32 {
            short(dst, src, n);
        } else if (dst as usize).wrapping_sub(src as usize) >= n {
            // Below the source, or clear of it: a forward byte copy reads
            // every byte before anything overwrites it.
            asm!(
                "rep movsb",
                inout("rcx") n => _,
                inout("rdi") dst => _,
                inout("rsi") src => _,
                options(nostack, preserves_flags),
            );
        } else {
            // Above the source and overlapping it: from the top down, each
            // chunk is read before any store reaches it. The bottom chunk is
            // read first, since the last chunk stored may cover it.
            let bottom = _mm_loadu_si128(src.cast::<__m128i>());
            let mut at = n;
            while at > 16 {
                at -= 16;
                asm!(
                    "movdqu {chunk}, [{from}]",
                    "movdqu [{to}], {chunk}",
                    from = in(reg) src.add(at),
                    to = in(reg) dst.add(at),
                    chunk = out(xmm_reg) _,
                    options(nostack, preserves_flags),
                );
            }
            _mm_storeu_si128(dst.cast::<__m128i>(), bottom);
        }
    }
    dst
}

// Up to 32 bytes as two loads that may overlap each other, both read before
// either is stored, so the direction of the move does not matter.
unsafe fn short(dst: *mut u8, src: *const u8, n: usize) {
    // SAFETY: each pair covers bytes `0..n` of both ranges and no more.
    unsafe {
        if n >= 16 {
            let low = _mm_loadu_si128(src.cast::<__m128i>());
            let high = _mm_loadu_si128(src.add(n - 16).cast::<__m128i>());
            _mm_storeu_si128(dst.add(n - 16).cast::<__m128i>(), high);
            _mm_storeu_si128(dst.cast::<__m128i>(), low);
        } else if n >= 8 {
            let low = src.cast::<u64>().read_unaligned();
            let high = src.add(n - 8).cast::<u64>().read_unaligned();
            dst.add(n - 8).cast::<u64>().write_unaligned(high);
            dst.cast::<u64>().write_unaligned(low);
        } else if n >= 4 {
            let low = src.cast::<u32>().read_unaligned();
            let high = src.add(n - 4).cast::<u32>().read_unaligned();
            dst.add(n - 4).cast::<u32>().write_unaligned(high);
            dst.cast::<u32>().write_unaligned(low);
        } else if n >= 2 {
            let low = src.cast::<u16>().read_unaligned();
            let high = src.add(n - 2).cast::<u16>().read_unaligned();
            dst.add(n - 2).cast::<u16>().write_unaligned(high);
            dst.cast::<u16>().write_unaligned(low);
        } else if n == 1 {
            *dst = *src;
        }
    }
}

#[cfg(all(target_os = "linux", target_env = "musl"))]
#[unsafe(no_mangle)]
unsafe extern "C" fn memmove(dst: *mut u8, src: *const u8, n: usize) -> *mut u8 {
    // SAFETY: memmove's own contract is `move_bytes`'s.
    unsafe { move_bytes(dst, src, n) }
}

#[cfg(test)]
mod tests {
    use super::move_bytes;

    // Every length up to a few chunks past the short moves, at every
    // distance up and down up to past a chunk, against the same move made
    // through a copy.
    #[test]
    fn every_length_and_overlap_moves_as_a_copy_would() {
        let base: Vec<u8> = (0..512u32).map(|at| (at * 7 + 3) as u8).collect();
        for n in 0..160 {
            for distance in 0..80 {
                for (from, to) in [(100, 100 + distance), (100 + distance, 100)] {
                    let mut moved = base.clone();
                    let mut copied = base.clone();
                    let bytes = base[from..from + n].to_vec();
                    copied[to..to + n].copy_from_slice(&bytes);
                    // SAFETY: both ranges lie inside `moved`.
                    unsafe {
                        let start = moved.as_mut_ptr();
                        move_bytes(start.add(to), start.add(from), n);
                    }
                    assert_eq!(moved, copied, "{n} bytes from {from} to {to}");
                }
            }
        }
    }
}
