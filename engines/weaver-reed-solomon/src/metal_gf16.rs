//! GF(2^16) multiply-accumulate on Apple GPUs (Metal, unified memory).
//!
//! Same product as the CPU kernels: `dst[j] ^= factor[j][s] * src[s]` over
//! GF(2^16) with the PAR2 generator polynomial. The GPU formulation is the
//! 4x16-entry nibble-table one — each output's tables are staged in
//! threadgroup memory, so the inner loop is table lookups against on-chip
//! SRAM while unified memory makes host buffers directly addressable.
//!
//! Dispatch is runtime-probed: this module only exists on macOS builds, a
//! Metal device is looked up once on first use, and callers engage a
//! session only when the batch is large enough to amortize dispatch
//! latency (`WEAVER_GF16_METAL=0` disables, `=1` forces for testing).
//! Everything falls back to the CPU kernels when any of that fails —
//! sessions surface errors instead of panicking.

use objc2::{
    rc::{Retained, autoreleasepool},
    runtime::ProtocolObject,
};
use objc2_foundation::{NSRange, NSString};
use objc2_metal::{
    MTLBlitCommandEncoder, MTLBuffer, MTLCommandBuffer, MTLCommandBufferStatus, MTLCommandEncoder,
    MTLCommandQueue, MTLCompileOptions, MTLComputeCommandEncoder, MTLComputePipelineState,
    MTLCreateSystemDefaultDevice, MTLDevice, MTLLibrary, MTLResource, MTLResourceOptions, MTLSize,
};
use std::{ffi::c_void, ptr::NonNull, sync::OnceLock};

#[link(name = "CoreGraphics", kind = "framework")]
unsafe extern "C" {}

type Buffer = Retained<ProtocolObject<dyn MTLBuffer>>;
type CommandBuffer = Retained<ProtocolObject<dyn MTLCommandBuffer>>;
type CommandQueue = Retained<ProtocolObject<dyn MTLCommandQueue>>;
type ComputePipelineState = Retained<ProtocolObject<dyn MTLComputePipelineState>>;
type Device = Retained<ProtocolObject<dyn MTLDevice>>;

/// Widest source batch a single dispatch accepts; sized to the streaming
/// repair batch and to the 8.25 KiB of threadgroup memory its tables need.
pub const MAX_SOURCES: usize = 66;

const TABLE_WORDS_PER_FACTOR: usize = 64; // 4 nibble positions x 16 entries
const PREFERRED_THREADS_PER_GROUP: usize = 256;

/// Auto-engage threshold: outputs x sources x region bytes per repair.
/// Below this the CPU path wins on dispatch + upload overhead.
const MIN_EFFECTIVE_BYTES: u64 = 256 * 1024 * 1024;

fn gf16_mul(mut a: u16, mut b: u16) -> u16 {
    let mut r = 0u16;
    while b != 0 {
        if b & 1 != 0 {
            r ^= a;
        }
        let carry = a & 0x8000 != 0;
        a <<= 1;
        if carry {
            a ^= 0x100B;
        }
        b >>= 1;
    }
    r
}

fn shader_source() -> String {
    format!(
        r#"
#include <metal_stdlib>
using namespace metal;

constant uint MAX_SOURCES = {max_sources}u;

kernel void gf16_mulacc(
    device const ushort* srcs    [[buffer(0)]],   // sources x words
    device ushort* dsts          [[buffer(1)]],   // outputs x words
    device const ushort* tables  [[buffer(2)]],   // factor value x 64
    device const ushort* factors [[buffer(3)]],   // outputs x sources
    constant uint& words         [[buffer(4)]],
    constant uint& sources       [[buffer(5)]],
    uint3 tg_pos                 [[threadgroup_position_in_grid]],
    uint3 tid3                   [[thread_position_in_threadgroup]],
    uint3 tg_dims                [[threads_per_threadgroup]])
{{
    uint tid = tid3.x;
    uint tg_size = tg_dims.x;
    uint out = tg_pos.y;

    // Stage this output's coefficient tables (one 64-entry table per
    // source, addressed by the factor value) into threadgroup memory.
    threadgroup ushort tg_tables[MAX_SOURCES * 64u];
    uint tab_count = sources * 64u;
    for (uint i = tid; i < tab_count; i += tg_size) {{
        uint f = factors[out * sources + (i >> 6u)];
        tg_tables[i] = tables[f * 64u + (i & 63u)];
    }}
    threadgroup_barrier(mem_flags::mem_threadgroup);

    uint w0 = (tg_pos.x * tg_size + tid) * 4u;
    if (w0 >= words) {{
        return;
    }}

    if (w0 + 4u <= words) {{
        // Rows are laid out at `words` granularity; index rows in scalar
        // words and vector-cast within the row.
        uint v = w0 >> 2u;
        ushort4 acc = ushort4(((device const packed_ushort4*)(dsts + out * words))[v]);
        for (uint s = 0; s < sources; s++) {{
            threadgroup const ushort* t = tg_tables + s * 64u;
            ushort4 w = ushort4(((device const packed_ushort4*)(srcs + s * words))[v]);
            acc.x ^= t[w.x & 15u] ^ t[16u + ((w.x >> 4u) & 15u)]
                   ^ t[32u + ((w.x >> 8u) & 15u)] ^ t[48u + (w.x >> 12u)];
            acc.y ^= t[w.y & 15u] ^ t[16u + ((w.y >> 4u) & 15u)]
                   ^ t[32u + ((w.y >> 8u) & 15u)] ^ t[48u + (w.y >> 12u)];
            acc.z ^= t[w.z & 15u] ^ t[16u + ((w.z >> 4u) & 15u)]
                   ^ t[32u + ((w.z >> 8u) & 15u)] ^ t[48u + (w.z >> 12u)];
            acc.w ^= t[w.w & 15u] ^ t[16u + ((w.w >> 4u) & 15u)]
                   ^ t[32u + ((w.w >> 8u) & 15u)] ^ t[48u + (w.w >> 12u)];
        }}
        ((device packed_ushort4*)(dsts + out * words))[v] = packed_ushort4(acc);
    }} else {{
        // Trailing 1-3 words of an odd-length region.
        for (uint w = w0; w < words; w++) {{
            ushort acc = dsts[out * words + w];
            for (uint s = 0; s < sources; s++) {{
                threadgroup const ushort* t = tg_tables + s * 64u;
                ushort x = srcs[s * words + w];
                acc ^= t[x & 15u] ^ t[16u + ((x >> 4u) & 15u)]
                     ^ t[32u + ((x >> 8u) & 15u)] ^ t[48u + (x >> 12u)];
            }}
            dsts[out * words + w] = acc;
        }}
    }}
}}
"#,
        max_sources = MAX_SOURCES
    )
}

/// Device-global Metal state, created once. MTLDevice, MTLCommandQueue and
/// MTLComputePipelineState are documented thread-safe; sessions built on
/// top hold their own buffers and are single-threaded.
struct MetalShared {
    device: Device,
    queue: CommandQueue,
    pipeline: ComputePipelineState,
    threads_per_group: usize,
}

unsafe impl Send for MetalShared {}
unsafe impl Sync for MetalShared {}

enum MetalGate {
    Auto,
    Force,
    Off,
}

fn metal_gate() -> MetalGate {
    match std::env::var("WEAVER_GF16_METAL") {
        Ok(v) if v == "0" || v.eq_ignore_ascii_case("false") => MetalGate::Off,
        Ok(v) if v == "1" || v.eq_ignore_ascii_case("true") => MetalGate::Force,
        _ => MetalGate::Auto,
    }
}

fn shared_context() -> Option<&'static MetalShared> {
    static CONTEXT: OnceLock<Option<MetalShared>> = OnceLock::new();
    CONTEXT
        .get_or_init(|| {
            // Every Metal call that can return autoreleased objects runs
            // inside a pool: these threads are plain Rust threads with no
            // ambient pool, so without one the objects leak until thread
            // exit.
            autoreleasepool(|_| {
                if matches!(metal_gate(), MetalGate::Off) {
                    return None;
                }
                let device = MTLCreateSystemDefaultDevice()?;
                let queue = device.newCommandQueue()?;
                let queue_label = NSString::from_str("weaver.gf16");
                queue.setLabel(Some(&queue_label));
                let source = NSString::from_str(&shader_source());
                let options = MTLCompileOptions::new();
                let library = device
                    .newLibraryWithSource_options_error(&source, Some(&options))
                    .ok()?;
                let function_name = NSString::from_str("gf16_mulacc");
                let function = library.newFunctionWithName(&function_name)?;
                let pipeline = device
                    .newComputePipelineStateWithFunction_error(&function)
                    .ok()?;
                let threads_per_group =
                    PREFERRED_THREADS_PER_GROUP.min(pipeline.maxTotalThreadsPerThreadgroup());
                if threads_per_group == 0 {
                    return None;
                }
                Some(MetalShared {
                    device,
                    queue,
                    pipeline,
                    threads_per_group,
                })
            })
        })
        .as_ref()
}

/// Wait for a command buffer and require clean completion; a faulted or
/// cancelled buffer means the destination contents cannot be trusted.
fn wait_completed(cb: &CommandBuffer) -> bool {
    cb.waitUntilCompleted();
    cb.status() == MTLCommandBufferStatus::Completed
}

/// True when a Metal device is present and the tier is not disabled.
pub fn metal_gf16_available() -> bool {
    shared_context().is_some()
}

/// One repair's GPU residency: reusable upload buffers (double-buffered so
/// a fill can proceed while the previous dispatch runs), one destination
/// buffer that stays resident across every source batch of a chunk, and a
/// factor-value-indexed table cache filled lazily as coefficients first
/// appear.
pub struct MetalGf16Session {
    shared: &'static MetalShared,
    src_bufs: [Buffer; 2],
    factor_bufs: [Buffer; 2],
    dst_buf: Buffer,
    table_buf: Buffer,
    table_filled: Vec<u64>,
    pending: [Option<CommandBuffer>; 2],
    slot: usize,
    outputs: usize,
    max_region_bytes: usize,
    chunk_words: usize,
}

impl MetalGf16Session {
    /// Engage a session when the device exists and the whole repair is big
    /// enough to amortize dispatch (`effective_bytes` = outputs x sources x
    /// region bytes). `WEAVER_GF16_METAL=1` skips the size gate.
    pub fn try_new(outputs: usize, max_region_bytes: usize, effective_bytes: u64) -> Option<Self> {
        let shared = shared_context()?;
        match metal_gate() {
            MetalGate::Off => return None,
            MetalGate::Force => {}
            MetalGate::Auto => {
                if effective_bytes < MIN_EFFECTIVE_BYTES {
                    return None;
                }
            }
        }
        if outputs == 0 || max_region_bytes == 0 || !max_region_bytes.is_multiple_of(2) {
            return None;
        }
        // The kernel indexes destination words as `out * words + w` in
        // 32-bit math; refuse shapes that could wrap rather than corrupt.
        let max_words = (max_region_bytes / 2) as u64;
        if (outputs as u64).saturating_mul(max_words) > u32::MAX as u64 {
            return None;
        }

        let opts = MTLResourceOptions::StorageModeShared;
        let src_len = MAX_SOURCES * max_region_bytes;
        let factor_len = outputs * MAX_SOURCES * 2;
        let dst_len = outputs * max_region_bytes;
        let table_len = 65536 * TABLE_WORDS_PER_FACTOR * 2;
        let max_len = shared.device.maxBufferLength();
        if src_len > max_len || dst_len > max_len || factor_len > max_len || table_len > max_len {
            return None;
        }

        autoreleasepool(|_| {
            let new_labeled = |len: usize, label: &str| {
                let buf = shared.device.newBufferWithLength_options(len, opts)?;
                let label = NSString::from_str(label);
                buf.setLabel(Some(&label));
                Some(buf)
            };
            Some(Self {
                shared,
                src_bufs: [
                    new_labeled(src_len, "weaver.gf16.src0")?,
                    new_labeled(src_len, "weaver.gf16.src1")?,
                ],
                factor_bufs: [
                    new_labeled(factor_len, "weaver.gf16.factors0")?,
                    new_labeled(factor_len, "weaver.gf16.factors1")?,
                ],
                dst_buf: new_labeled(dst_len, "weaver.gf16.dst")?,
                table_buf: new_labeled(table_len, "weaver.gf16.tables")?,
                table_filled: vec![0u64; 65536 / 64],
                pending: [None, None],
                slot: 0,
                outputs,
                max_region_bytes,
                chunk_words: 0,
            })
        })
    }

    fn ensure_table(&mut self, factor: u16) {
        let idx = factor as usize;
        if self.table_filled[idx / 64] & (1 << (idx % 64)) != 0 {
            return;
        }
        self.table_filled[idx / 64] |= 1 << (idx % 64);
        let base = self.table_buf.contents().as_ptr() as *mut u16;
        for k in 0..4u32 {
            for x in 0..16u16 {
                let value = gf16_mul(factor, x << (4 * k));
                unsafe {
                    base.add(idx * TABLE_WORDS_PER_FACTOR + (k as usize) * 16 + x as usize)
                        .write(value);
                }
            }
        }
    }

    /// Start a chunk: the resident destination region (outputs x
    /// `byte_len`) is zeroed on the GPU. The zeroing command buffer is
    /// tracked like a dispatch so its completion status is checked before
    /// results are trusted.
    pub fn begin_chunk(&mut self, byte_len: usize) -> Result<(), &'static str> {
        if byte_len == 0 || !byte_len.is_multiple_of(2) || byte_len > self.max_region_bytes {
            return Err("chunk length unsupported by the metal session");
        }
        self.chunk_words = byte_len / 2;
        autoreleasepool(|_| {
            let cb = self
                .shared
                .queue
                .commandBuffer()
                .ok_or("failed to create metal command buffer")?;
            let blit = cb
                .blitCommandEncoder()
                .ok_or("failed to create metal blit encoder")?;
            blit.fillBuffer_range_value(&self.dst_buf, NSRange::new(0, self.outputs * byte_len), 0);
            blit.endEncoding();
            cb.commit();
            // Park it in the slot the next accumulate uses, so the normal
            // fence path waits it and checks its status.
            if let Some(prev) = self.pending[self.slot].replace(cb)
                && !wait_completed(&prev)
            {
                return Err("prior gpu dispatch failed");
            }
            Ok(())
        })
    }

    /// Accumulate one source batch into the resident chunk destinations:
    /// `dst[j] ^= factor(j, s) * srcs[s]` for every output j. Returns after
    /// queueing — the dispatch overlaps the caller's next batch read; the
    /// upload slot being reused is fenced by waiting its prior dispatch.
    pub fn accumulate(
        &mut self,
        srcs: &[&[u8]],
        factor: impl Fn(usize, usize) -> u16,
    ) -> Result<(), &'static str> {
        let sources = srcs.len();
        if sources == 0 {
            return Ok(());
        }
        if sources > MAX_SOURCES {
            return Err("source batch wider than the metal kernel supports");
        }
        let byte_len = self.chunk_words * 2;
        if byte_len == 0 {
            return Err("accumulate before begin_chunk");
        }

        let slot = self.slot;
        self.slot ^= 1;
        if let Some(prev) = self.pending[slot].take()
            && !wait_completed(&prev)
        {
            return Err("prior gpu dispatch failed");
        }

        // Factors first: fills the lazy table cache before the GPU reads it.
        let factor_ptr = self.factor_bufs[slot].contents().as_ptr() as *mut u16;
        for j in 0..self.outputs {
            for s in 0..sources {
                let f = factor(j, s);
                self.ensure_table(f);
                unsafe { factor_ptr.add(j * sources + s).write(f) };
            }
        }

        let src_ptr = self.src_bufs[slot].contents().as_ptr() as *mut u8;
        for (s, src) in srcs.iter().enumerate() {
            if src.len() != byte_len {
                return Err("source region length mismatch");
            }
            unsafe {
                std::ptr::copy_nonoverlapping(src.as_ptr(), src_ptr.add(s * byte_len), byte_len);
            }
        }

        let words = self.chunk_words as u32;
        let sources_u32 = sources as u32;
        let threads = self.shared.threads_per_group;
        self.pending[slot] = Some(autoreleasepool(
            |_| -> Result<CommandBuffer, &'static str> {
                let cb = self
                    .shared
                    .queue
                    .commandBuffer()
                    .ok_or("failed to create metal command buffer")?;
                let enc = cb
                    .computeCommandEncoder()
                    .ok_or("failed to create metal compute encoder")?;
                enc.setComputePipelineState(&self.shared.pipeline);
                unsafe {
                    enc.setBuffer_offset_atIndex(Some(&self.src_bufs[slot]), 0, 0);
                    enc.setBuffer_offset_atIndex(Some(&self.dst_buf), 0, 1);
                    enc.setBuffer_offset_atIndex(Some(&self.table_buf), 0, 2);
                    enc.setBuffer_offset_atIndex(Some(&self.factor_bufs[slot]), 0, 3);
                    enc.setBytes_length_atIndex(
                        NonNull::from(&words).cast::<c_void>(),
                        std::mem::size_of_val(&words),
                        4,
                    );
                    enc.setBytes_length_atIndex(
                        NonNull::from(&sources_u32).cast::<c_void>(),
                        std::mem::size_of_val(&sources_u32),
                        5,
                    );
                }
                let quads = self.chunk_words.div_ceil(4);
                enc.dispatchThreadgroups_threadsPerThreadgroup(
                    MTLSize {
                        width: quads.div_ceil(threads),
                        height: self.outputs,
                        depth: 1,
                    },
                    MTLSize {
                        width: threads,
                        height: 1,
                        depth: 1,
                    },
                );
                enc.endEncoding();
                cb.commit();
                Ok(cb)
            },
        )?);
        Ok(())
    }

    /// Wait for the chunk's dispatches and copy the accumulated
    /// destinations out. `dst_rows[j][..byte_len]` receives output `j`.
    pub fn finish_chunk(&mut self, dst_rows: &mut [Vec<u8>]) -> Result<(), &'static str> {
        if dst_rows.len() != self.outputs {
            return Err("output row count mismatch");
        }
        for pending in &mut self.pending {
            if let Some(cb) = pending.take()
                && !wait_completed(&cb)
            {
                return Err("gpu dispatch failed before readback");
            }
        }
        let byte_len = self.chunk_words * 2;
        let dst_ptr = self.dst_buf.contents().as_ptr() as *const u8;
        for (j, row) in dst_rows.iter_mut().enumerate() {
            if row.len() < byte_len {
                return Err("output row shorter than chunk");
            }
            unsafe {
                std::ptr::copy_nonoverlapping(
                    dst_ptr.add(j * byte_len),
                    row.as_mut_ptr(),
                    byte_len,
                );
            }
        }
        self.chunk_words = 0;
        Ok(())
    }

    /// Device name for engage-time logging.
    pub fn device_name(&self) -> String {
        autoreleasepool(|_| self.shared.device.name().to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gf_simd::mul_acc_region;

    fn deterministic_bytes(len: usize, salt: usize) -> Vec<u8> {
        (0..len)
            .map(|i| ((i * (salt + 7) + 13) % 251) as u8)
            .collect()
    }

    #[test]
    fn metal_session_matches_cpu_kernels() {
        // Force-engage so the size gate does not skip the test shape; skip
        // entirely on machines without a Metal device.
        let outputs = 5usize;
        let region = 8192usize + 6; // odd tail: exercises the scalar path
        let Some(mut session) = MetalGf16Session::try_new(
            outputs,
            region,
            MIN_EFFECTIVE_BYTES + 1, // pass the auto gate regardless of env
        ) else {
            eprintln!("no Metal device; skipping");
            return;
        };

        let factors: Vec<u16> = (0..outputs * MAX_SOURCES)
            .map(|i| (i as u16).wrapping_mul(0x2F1D).wrapping_add(3) | 1)
            .collect();

        // Two batches accumulate into one chunk, mirroring streaming repair.
        let batch_a: Vec<Vec<u8>> = (0..MAX_SOURCES)
            .map(|s| deterministic_bytes(region, s))
            .collect();
        let batch_b: Vec<Vec<u8>> = (0..17)
            .map(|s| deterministic_bytes(region, s + 100))
            .collect();

        let mut expected: Vec<Vec<u8>> = vec![vec![0u8; region]; outputs];
        for (j, row) in expected.iter_mut().enumerate() {
            for (s, src) in batch_a.iter().enumerate() {
                mul_acc_region(factors[j * MAX_SOURCES + s], src, row);
            }
            for (s, src) in batch_b.iter().enumerate() {
                mul_acc_region(factors[j * MAX_SOURCES + s].wrapping_add(1) | 1, src, row);
            }
        }

        session.begin_chunk(region).unwrap();
        let refs_a: Vec<&[u8]> = batch_a.iter().map(|s| s.as_slice()).collect();
        session
            .accumulate(&refs_a, |j, s| factors[j * MAX_SOURCES + s])
            .unwrap();
        let refs_b: Vec<&[u8]> = batch_b.iter().map(|s| s.as_slice()).collect();
        session
            .accumulate(&refs_b, |j, s| {
                factors[j * MAX_SOURCES + s].wrapping_add(1) | 1
            })
            .unwrap();
        let mut rows: Vec<Vec<u8>> = vec![vec![0u8; region]; outputs];
        session.finish_chunk(&mut rows).unwrap();

        assert_eq!(rows, expected, "GPU accumulate must match CPU kernels");

        // Second chunk on the same session: dst must re-zero.
        session.begin_chunk(64).unwrap();
        let one = deterministic_bytes(64, 9);
        session.accumulate(&[one.as_slice()], |_, _| 7).unwrap();
        let mut rows2: Vec<Vec<u8>> = vec![vec![0u8; 64]; outputs];
        session.finish_chunk(&mut rows2).unwrap();
        let mut want = vec![0u8; 64];
        mul_acc_region(7, &one, &mut want);
        for row in &rows2 {
            assert_eq!(row, &want, "chunk reset must start from zeroed dst");
        }
    }
}
