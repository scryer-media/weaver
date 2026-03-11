//! PPMd variant H context model.
//!
//! Full implementation of the PPMd variant H algorithm including proper context
//! model updates (UpdateModel, CreateSuccessors, rescale).
//!
//! Reference: 7-zip Ppmd7.c (public domain), Shkarin's original PPMd (public domain).

use super::alloc::{NodeRef, SubAllocator, UNIT_SIZE};
use super::range::RangeDecoder;
use super::see::SeeTable;
// --- Constants ---

const MAX_ORDER: usize = 64;
const MAX_FREQ: u8 = 124;
const BIN_SCALE: u32 = 1 << 14; // 16384
const INTERVAL: u16 = 1 << 7; // 128

const INIT_BIN_ESC: [u16; 8] = [
    0x3CDD, 0x1F3F, 0x59BF, 0x48F3, 0x64A1, 0x5ABC, 0x6632, 0x6051,
];

const EXP_ESCAPE: [u8; 16] = [25, 14, 9, 7, 5, 5, 4, 4, 4, 3, 3, 3, 2, 2, 2, 2];

// --- Context layout (12 bytes per context node) ---
// Contexts are allocated as single units from the arena.

/// Byte offset of suffix context ref (u32, stored as byte offset in arena).
const CTX_SUFFIX: usize = 0;
/// Byte offset of NumStats (u16). NumStats = number of symbols (1 = binary).
const CTX_NUM_STATS: usize = 4;
// Union at offset 6 (6 bytes):
//   NumStats == 1: OneState inline — symbol(1) + freq(1) + successor(4)
//   NumStats > 1: SummFreq(2) + Stats pointer(4)
const CTX_SUMM_FREQ: usize = 6;
const CTX_STATS: usize = 8;
// OneState aliases (same offsets, different interpretation):
const CTX_ONE_SYM: usize = 6;
const CTX_ONE_FREQ: usize = 7;
const CTX_ONE_SUCC: usize = 8;

// --- State layout (6 bytes, packed 2 per 12-byte unit) ---
const STATE_SIZE: usize = 6;
const STATE_SYM: usize = 0;
const STATE_FREQ: usize = 1;
const STATE_SUCC: usize = 2;

/// PPMd variant H model.
pub struct Model {
    alloc: SubAllocator,
    see: SeeTable,
    max_order: usize,

    // Context tracking (all stored as byte offsets in arena, 0 = NULL).
    min_context: u32,
    max_context: u32,

    // Found state (byte offset of the matched state, 0 = not found).
    found_state: u32,

    order_fall: i32,

    // Binary summation table [freq-1][combined_index].
    bin_summ: [[u16; 64]; 128],

    // Lookup tables.
    ns2_indx: [u8; 256],
    ns2_bs_indx: [u8; 256],
    hb2_flag: [u8; 256],

    // Mask and counters.
    char_mask: [u8; 256],
    esc_count: u8,
    num_masked: usize,

    // State.
    prev_success: u8,
    hi_bits_flag: u8,
    init_esc: u8,
    run_length: i32,
    init_rl: i32,
}

// --- Helpers for converting between NodeRef and byte offsets ---

#[inline]
fn ref_to_off(node: NodeRef) -> u32 {
    node.offset() as u32
}

#[inline]
fn off_to_ref(off: u32) -> NodeRef {
    NodeRef(off / UNIT_SIZE as u32)
}

impl Model {
    /// Create a new PPMd model.
    pub fn new(max_order: usize, alloc_size: usize) -> Self {
        let max_order = max_order.clamp(2, MAX_ORDER);
        let mut model = Self {
            alloc: SubAllocator::new(alloc_size),
            see: SeeTable::new(),
            max_order,
            min_context: 0,
            max_context: 0,
            found_state: 0,
            order_fall: 0,
            bin_summ: [[0u16; 64]; 128],
            ns2_indx: [0u8; 256],
            ns2_bs_indx: [0u8; 256],
            hb2_flag: [0u8; 256],
            char_mask: [0u8; 256],
            esc_count: 0,
            num_masked: 0,
            prev_success: 0,
            hi_bits_flag: 0,
            init_esc: 0,
            run_length: 0,
            init_rl: -(max_order.min(12) as i32) - 1,
        };
        model.build_lookup_tables();
        model.restart();
        model
    }

    fn build_lookup_tables(&mut self) {
        // NS2BSIndx
        self.ns2_bs_indx[0] = 0;
        self.ns2_bs_indx[1] = 2;
        for i in 2..11 {
            self.ns2_bs_indx[i] = 4;
        }
        for i in 11..256 {
            self.ns2_bs_indx[i] = 6;
        }

        // NS2Indx: 0,1,2 then groups of increasing size
        self.ns2_indx[0] = 0;
        self.ns2_indx[1] = 1;
        self.ns2_indx[2] = 2;
        let mut m = 3u8;
        let mut step = 1usize;
        let mut k = step;
        for i in 3..256 {
            self.ns2_indx[i] = m;
            k -= 1;
            if k == 0 {
                step += 1;
                k = step;
                m = m.saturating_add(1);
            }
        }

        // HB2Flag
        for i in 0..0x40 {
            self.hb2_flag[i] = 0;
        }
        for i in 0x40..256 {
            self.hb2_flag[i] = 0x08;
        }
    }

    /// Reset the model to initial state.
    pub fn restart(&mut self) {
        self.alloc.reset();
        self.see = SeeTable::new();
        self.char_mask = [0; 256];
        self.esc_count = 1;
        self.prev_success = 0;
        self.run_length = self.init_rl;
        self.order_fall = self.max_order as i32;

        // Initialize BinSumm.
        for i in 0..128u16 {
            for (k, &esc) in INIT_BIN_ESC.iter().enumerate() {
                let val = BIN_SCALE as u16 - esc / (i + 2);
                for m in (0..64).step_by(8) {
                    self.bin_summ[i as usize][k + m] = val;
                }
            }
        }

        // Allocate root context.
        let root = self.alloc.alloc_context();
        if root.is_null() {
            return;
        }
        let root_off = ref_to_off(root);
        self.min_context = root_off;
        self.max_context = root_off;

        // Root has 256 symbols.
        self.alloc.write_u32(root, CTX_SUFFIX, 0); // no suffix
        self.alloc.write_u16(root, CTX_NUM_STATS, 256);
        self.alloc.write_u16(root, CTX_SUMM_FREQ, 257); // 256 + 1

        // Allocate states array (256 states, 2 per unit = 128 units).
        let states = self.alloc.alloc_units(128);
        if states.is_null() {
            return;
        }
        let states_off = ref_to_off(states);
        self.alloc.write_u32(root, CTX_STATS, states_off);

        // Initialize 256 states: symbol=i, freq=1, successor=0.
        for i in 0..256u32 {
            let off = states_off as usize + i as usize * STATE_SIZE;
            self.alloc.write_byte_at(off + STATE_SYM, i as u8);
            self.alloc.write_byte_at(off + STATE_FREQ, 1);
            self.alloc.write_u32_at(off + STATE_SUCC, 0);
        }

        // Set FoundState to first state (so found_state_symbol works).
        self.found_state = states_off;
    }

    // --- Context field accessors ---

    #[inline]
    fn ctx_suffix(&self, ctx: u32) -> u32 {
        self.alloc.read_u32_at(ctx as usize + CTX_SUFFIX)
    }

    #[inline]
    fn ctx_num_stats(&self, ctx: u32) -> u16 {
        self.alloc.read_u16_at(ctx as usize + CTX_NUM_STATS)
    }

    #[inline]
    fn set_ctx_num_stats(&mut self, ctx: u32, val: u16) {
        self.alloc.write_u16_at(ctx as usize + CTX_NUM_STATS, val);
    }

    #[inline]
    fn ctx_summ_freq(&self, ctx: u32) -> u16 {
        self.alloc.read_u16_at(ctx as usize + CTX_SUMM_FREQ)
    }

    #[inline]
    fn set_ctx_summ_freq(&mut self, ctx: u32, val: u16) {
        self.alloc.write_u16_at(ctx as usize + CTX_SUMM_FREQ, val);
    }

    #[inline]
    fn ctx_stats(&self, ctx: u32) -> u32 {
        self.alloc.read_u32_at(ctx as usize + CTX_STATS)
    }

    #[inline]
    fn set_ctx_stats(&mut self, ctx: u32, val: u32) {
        self.alloc.write_u32_at(ctx as usize + CTX_STATS, val);
    }

    #[inline]
    fn set_ctx_suffix(&mut self, ctx: u32, val: u32) {
        self.alloc.write_u32_at(ctx as usize + CTX_SUFFIX, val);
    }

    // OneState accessors (when NumStats == 1).
    #[inline]
    fn one_sym(&self, ctx: u32) -> u8 {
        self.alloc.read_byte_at(ctx as usize + CTX_ONE_SYM)
    }
    #[inline]
    fn one_freq(&self, ctx: u32) -> u8 {
        self.alloc.read_byte_at(ctx as usize + CTX_ONE_FREQ)
    }
    #[inline]
    fn one_succ(&self, ctx: u32) -> u32 {
        self.alloc.read_u32_at(ctx as usize + CTX_ONE_SUCC)
    }
    #[inline]
    fn set_one_sym(&mut self, ctx: u32, v: u8) {
        self.alloc.write_byte_at(ctx as usize + CTX_ONE_SYM, v);
    }
    #[inline]
    fn set_one_freq(&mut self, ctx: u32, v: u8) {
        self.alloc.write_byte_at(ctx as usize + CTX_ONE_FREQ, v);
    }
    #[inline]
    fn set_one_succ(&mut self, ctx: u32, v: u32) {
        self.alloc.write_u32_at(ctx as usize + CTX_ONE_SUCC, v);
    }

    // State accessors at arbitrary byte offset.
    #[inline]
    fn st_sym(&self, off: u32) -> u8 {
        self.alloc.read_byte_at(off as usize + STATE_SYM)
    }
    #[inline]
    fn st_freq(&self, off: u32) -> u8 {
        self.alloc.read_byte_at(off as usize + STATE_FREQ)
    }
    #[inline]
    fn st_succ(&self, off: u32) -> u32 {
        self.alloc.read_u32_at(off as usize + STATE_SUCC)
    }
    #[inline]
    fn set_st_sym(&mut self, off: u32, v: u8) {
        self.alloc.write_byte_at(off as usize + STATE_SYM, v);
    }
    #[inline]
    fn set_st_freq(&mut self, off: u32, v: u8) {
        self.alloc.write_byte_at(off as usize + STATE_FREQ, v);
    }
    #[inline]
    fn set_st_succ(&mut self, off: u32, v: u32) {
        self.alloc.write_u32_at(off as usize + STATE_SUCC, v);
    }

    /// Copy a 6-byte state from one offset to another.
    fn copy_state(&mut self, dst: u32, src: u32) {
        for i in 0..STATE_SIZE {
            let b = self.alloc.read_byte_at(src as usize + i);
            self.alloc.write_byte_at(dst as usize + i, b);
        }
    }

    /// Swap two 6-byte states.
    fn swap_states(&mut self, a: u32, b: u32) {
        for i in 0..STATE_SIZE {
            let va = self.alloc.read_byte_at(a as usize + i);
            let vb = self.alloc.read_byte_at(b as usize + i);
            self.alloc.write_byte_at(a as usize + i, vb);
            self.alloc.write_byte_at(b as usize + i, va);
        }
    }

    /// Symbol of the current FoundState.
    fn found_state_symbol(&self) -> u8 {
        if self.found_state == 0 {
            0
        } else {
            self.st_sym(self.found_state)
        }
    }

    /// Check if a successor value is a text pointer.
    fn is_text_succ(&self, succ: u32) -> bool {
        succ != 0 && (succ as usize) < self.alloc.units_start_bytes()
    }

    // =======================================================================
    // Decode entry point
    // =======================================================================

    /// Decode one character. Returns 0-255 on success, -1 on error/restart.
    pub fn decode_char(&mut self, rc: &mut RangeDecoder) -> i32 {
        if self.min_context == 0 || self.alloc.text_exhausted() {
            return -1;
        }

        let ns = self.ctx_num_stats(self.min_context);

        if ns != 1 {
            // Multi-symbol context.
            if !self.decode_symbol1(rc) {
                return -1;
            }
        } else {
            // Binary context.
            self.decode_bin_symbol(rc);
        }

        // Escape loop: walk suffix chain until a symbol is found.
        while self.found_state == 0 {
            loop {
                self.order_fall += 1;
                let suffix = self.ctx_suffix(self.min_context);
                self.min_context = suffix;
                if self.min_context == 0 {
                    return -1;
                }
                // Validate context pointer.
                if (self.min_context as usize) < self.alloc.units_start_bytes() {
                    return -1;
                }
                let ns2 = self.ctx_num_stats(self.min_context) as usize;
                if ns2 != self.num_masked {
                    break;
                }
            }
            if !self.decode_symbol2(rc) {
                return -1;
            }
        }

        let symbol = self.st_sym(self.found_state);

        if self.order_fall == 0 {
            let succ = self.st_succ(self.found_state);
            if succ != 0 && !self.is_text_succ(succ) {
                // Deterministic context jump.
                self.min_context = succ;
                self.max_context = succ;
            } else {
                self.update_model();
                if self.esc_count == 0 {
                    self.clear_mask();
                }
            }
        } else {
            self.update_model();
            if self.esc_count == 0 {
                self.clear_mask();
            }
        }

        symbol as i32
    }

    // =======================================================================
    // decode_bin_symbol (NumStats == 1)
    // =======================================================================

    fn decode_bin_symbol(&mut self, rc: &mut RangeDecoder) {
        let ctx = self.min_context;
        let symbol = self.one_sym(ctx);
        let freq = self.one_freq(ctx);

        // BinSumm index.
        self.hi_bits_flag = self.hb2_flag[self.found_state_symbol() as usize];
        let suffix = self.ctx_suffix(ctx);
        let suffix_ns = if suffix != 0 {
            self.ctx_num_stats(suffix)
        } else {
            1 // Avoid underflow; won't be used if suffix is null.
        };
        let idx1 = self.prev_success as usize
            + self.ns2_bs_indx[suffix_ns.wrapping_sub(1) as usize] as usize
            + self.hi_bits_flag as usize
            + 2 * self.hb2_flag[symbol as usize] as usize
            + ((self.run_length >> 26) as usize & 0x20);
        let idx0 = (freq as usize).saturating_sub(1).min(127);
        let idx1 = idx1.min(63);
        let bs = self.bin_summ[idx0][idx1];

        if rc.decode_binary(bs as u32, BIN_SCALE) {
            // Symbol found.
            self.found_state = ctx + CTX_ONE_SYM as u32;
            let new_freq = if freq < 128 { freq + 1 } else { freq };
            self.set_one_freq(ctx, new_freq);

            // Update BinSumm: increase probability.
            let mean = ((bs as u32 + 32) >> 7) as u16;
            self.bin_summ[idx0][idx1] = bs.wrapping_add(INTERVAL).wrapping_sub(mean);

            self.prev_success = 1;
            self.run_length += 1;
        } else {
            // Escape.
            let mean = ((bs as u32 + 32) >> 7) as u16;
            let new_bs = bs.wrapping_sub(mean);
            self.bin_summ[idx0][idx1] = new_bs;

            self.init_esc = EXP_ESCAPE[(new_bs >> 10) as usize];
            self.num_masked = 1;
            self.char_mask[symbol as usize] = self.esc_count;
            self.prev_success = 0;
            self.found_state = 0;
        }
    }

    // =======================================================================
    // decode_symbol1 (NumStats > 1)
    // =======================================================================

    fn decode_symbol1(&mut self, rc: &mut RangeDecoder) -> bool {
        let ctx = self.min_context;
        let ns = self.ctx_num_stats(ctx) as usize;
        let sum_freq = self.ctx_summ_freq(ctx) as u32;
        let stats = self.ctx_stats(ctx);

        let count = rc.get_current_count(sum_freq);
        if count >= sum_freq {
            return false;
        }

        // Check first symbol.
        let p0_freq = self.st_freq(stats) as u32;
        if count < p0_freq {
            // First symbol matched.
            self.prev_success = if 2 * p0_freq > sum_freq { 1 } else { 0 };
            self.run_length += self.prev_success as i32;
            self.found_state = stats;

            let new_freq = (p0_freq + 4) as u8;
            self.set_st_freq(stats, new_freq);
            self.set_ctx_summ_freq(ctx, (sum_freq + 4) as u16);

            if new_freq > MAX_FREQ {
                self.rescale(ctx);
            }

            rc.decode(0, p0_freq, sum_freq);
            return true;
        }

        if self.found_state == 0 {
            return false;
        }

        self.prev_success = 0;
        let mut hi_cnt = p0_freq;
        let mut i = ns - 1;
        let mut p = stats + STATE_SIZE as u32;

        loop {
            let p_freq = self.st_freq(p) as u32;
            hi_cnt += p_freq;
            if hi_cnt > count {
                // Found a symbol.
                let low = hi_cnt - p_freq;
                rc.decode(low, p_freq, sum_freq);
                self.update1(ctx, p);
                return true;
            }
            i -= 1;
            if i == 0 {
                // Escape.
                self.hi_bits_flag = self.hb2_flag[self.found_state_symbol() as usize];
                let p_sym = self.st_sym(p);
                self.char_mask[p_sym as usize] = self.esc_count;
                self.num_masked = ns;
                self.found_state = 0;

                // Mask all preceding symbols.
                let mut q = p;
                for _ in 0..ns - 1 {
                    q -= STATE_SIZE as u32;
                    let sym = self.st_sym(q);
                    self.char_mask[sym as usize] = self.esc_count;
                }

                let escape_freq = sum_freq - hi_cnt;
                rc.decode(hi_cnt, escape_freq.max(1), sum_freq);
                return true;
            }
            p += STATE_SIZE as u32;
        }
    }

    /// update1: increase freq, maintain sorted order, rescale if needed.
    fn update1(&mut self, ctx: u32, p: u32) {
        self.found_state = p;
        let freq = self.st_freq(p);
        let new_freq = freq.saturating_add(4);
        self.set_st_freq(p, new_freq);

        let sf = self.ctx_summ_freq(ctx);
        self.set_ctx_summ_freq(ctx, sf.wrapping_add(4));

        let stats = self.ctx_stats(ctx);
        // Bubble sort upward if needed.
        if p > stats {
            let prev = p - STATE_SIZE as u32;
            if new_freq > self.st_freq(prev) {
                // Swap with predecessor(s).
                let mut dst = p;
                let tmp_sym = self.st_sym(p);
                let tmp_freq = self.st_freq(p);
                let tmp_succ = self.st_succ(p);
                loop {
                    self.copy_state(dst, dst - STATE_SIZE as u32);
                    dst -= STATE_SIZE as u32;
                    if dst == stats || tmp_freq <= self.st_freq(dst - STATE_SIZE as u32) {
                        break;
                    }
                }
                self.set_st_sym(dst, tmp_sym);
                self.set_st_freq(dst, tmp_freq);
                self.set_st_succ(dst, tmp_succ);
                self.found_state = dst;

                if tmp_freq > MAX_FREQ {
                    self.rescale(ctx);
                }
                return;
            }
        }

        if new_freq > MAX_FREQ {
            self.rescale(ctx);
        }
    }

    // =======================================================================
    // decode_symbol2 (masked context decode during escape)
    // =======================================================================

    fn decode_symbol2(&mut self, rc: &mut RangeDecoder) -> bool {
        let ctx = self.min_context;
        let ns = self.ctx_num_stats(ctx) as usize;
        let diff = ns - self.num_masked;
        if diff == 0 {
            return false;
        }

        // makeEscFreq2
        let esc_freq = self.make_esc_freq2(ctx, diff);

        let stats = self.ctx_stats(ctx);

        // Collect unmasked states.
        let mut unmasked: [(u32, u8); 256] = [(0, 0); 256]; // (state_off, freq)
        let mut n = 0usize;
        let mut hi_cnt = 0u32;
        let mut p = stats;
        let end = stats + (ns as u32) * STATE_SIZE as u32;
        while p < end {
            let sym = self.st_sym(p);
            if self.char_mask[sym as usize] != self.esc_count {
                let freq = self.st_freq(p) as u32;
                hi_cnt += freq;
                unmasked[n] = (p, freq as u8);
                n += 1;
            }
            p += STATE_SIZE as u32;
        }

        let scale = esc_freq + hi_cnt;
        let count = rc.get_current_count(scale);
        if count >= scale {
            return false;
        }

        if count < hi_cnt {
            // Symbol found among unmasked.
            let mut cum = 0u32;
            for &(st_off, freq) in &unmasked[..n] {
                cum += freq as u32;
                if cum > count {
                    let low = cum - freq as u32;
                    rc.decode(low, freq as u32, scale);
                    // SEE update (success).
                    self.see_update_success(ctx, diff);
                    self.update2(ctx, st_off);
                    return true;
                }
            }
        }

        // Escape again.
        rc.decode(hi_cnt, esc_freq, scale);

        // SEE update (escape): add scale to summ.
        self.see_update_escape(ctx, diff, scale);

        // Mask remaining unmasked symbols.
        for &(st_off, _) in &unmasked[..n] {
            let sym = self.st_sym(st_off);
            self.char_mask[sym as usize] = self.esc_count;
        }
        self.num_masked = ns;

        false // escape — FoundState stays NULL, caller loops
    }

    fn make_esc_freq2(&mut self, ctx: u32, diff: usize) -> u32 {
        let ns = self.ctx_num_stats(ctx);
        if ns != 256 {
            let suffix = self.ctx_suffix(ctx);
            let suffix_ns = if suffix != 0 {
                self.ctx_num_stats(suffix) as usize
            } else {
                0
            };
            let sf = self.ctx_summ_freq(ctx);
            let idx0 = self.ns2_indx[diff.saturating_sub(1).min(255)] as usize;
            let idx1 = (if diff < suffix_ns.saturating_sub(ns as usize) {
                1
            } else {
                0
            }) + (if (sf as usize) < 11 * ns as usize {
                2
            } else {
                0
            }) + (if self.num_masked > diff { 4 } else { 0 })
                + self.hi_bits_flag as usize;
            let see_ctx = self.see.get(idx0, idx1);
            see_ctx.get_mean()
        } else {
            let see_ctx = self.see.get_dummy();
            see_ctx.get_mean(); // side effect: adjust dummy summ
            1
        }
    }

    fn see_update_success(&mut self, ctx: u32, diff: usize) {
        let ns = self.ctx_num_stats(ctx);
        if ns != 256 {
            let suffix = self.ctx_suffix(ctx);
            let suffix_ns = if suffix != 0 {
                self.ctx_num_stats(suffix) as usize
            } else {
                0
            };
            let sf = self.ctx_summ_freq(ctx);
            let idx0 = self.ns2_indx[diff.saturating_sub(1).min(255)] as usize;
            let idx1 = (if diff < suffix_ns.saturating_sub(ns as usize) {
                1
            } else {
                0
            }) + (if (sf as usize) < 11 * ns as usize {
                2
            } else {
                0
            }) + (if self.num_masked > diff { 4 } else { 0 })
                + self.hi_bits_flag as usize;
            self.see.get(idx0, idx1).update();
        }
    }

    fn see_update_escape(&mut self, ctx: u32, diff: usize, scale: u32) {
        let ns = self.ctx_num_stats(ctx);
        if ns != 256 {
            let suffix = self.ctx_suffix(ctx);
            let suffix_ns = if suffix != 0 {
                self.ctx_num_stats(suffix) as usize
            } else {
                0
            };
            let sf = self.ctx_summ_freq(ctx);
            let idx0 = self.ns2_indx[diff.saturating_sub(1).min(255)] as usize;
            let idx1 = (if diff < suffix_ns.saturating_sub(ns as usize) {
                1
            } else {
                0
            }) + (if (sf as usize) < 11 * ns as usize {
                2
            } else {
                0
            }) + (if self.num_masked > diff { 4 } else { 0 })
                + self.hi_bits_flag as usize;
            self.see.get(idx0, idx1).summ =
                self.see.get(idx0, idx1).summ.wrapping_add(scale as u16);
        }
    }

    /// update2: set FoundState, increase freq, maybe rescale.
    fn update2(&mut self, ctx: u32, p: u32) {
        self.found_state = p;
        let freq = self.st_freq(p);
        let new_freq = freq.saturating_add(4);
        self.set_st_freq(p, new_freq);

        let sf = self.ctx_summ_freq(ctx);
        self.set_ctx_summ_freq(ctx, sf.wrapping_add(4));
        if new_freq > MAX_FREQ {
            self.rescale(ctx);
        }
        self.esc_count = self.esc_count.wrapping_add(1);
        self.run_length = self.init_rl;
    }

    // =======================================================================
    // rescale
    // =======================================================================

    fn rescale(&mut self, ctx: u32) {
        let old_ns = self.ctx_num_stats(ctx) as usize;
        let stats = self.ctx_stats(ctx);
        let adder: u8 = if self.order_fall != 0 { 1 } else { 0 };

        // Move FoundState to front.
        let found = self.found_state;
        if found != stats {
            let mut p = found;
            while p > stats {
                let prev = p - STATE_SIZE as u32;
                self.swap_states(p, prev);
                p -= STATE_SIZE as u32;
            }
        }

        // Boost first state.
        let f0 = self.st_freq(stats);
        let new_f0 = f0.saturating_add(4);
        self.set_st_freq(stats, new_f0);
        let sf0 = self.ctx_summ_freq(ctx);
        self.set_ctx_summ_freq(ctx, sf0.wrapping_add(4));

        // Halve frequencies, accumulate escape frequency.
        let mut esc_freq = self.ctx_summ_freq(ctx) as i32 - self.st_freq(stats) as i32;
        let first_freq = ((self.st_freq(stats) as u16 + adder as u16) >> 1) as u8;
        self.set_st_freq(stats, first_freq);
        let mut new_summ = first_freq as u16;

        let mut i = old_ns as i32 - 1;
        let mut p = stats + STATE_SIZE as u32;
        while i > 0 {
            esc_freq -= self.st_freq(p) as i32;
            let halved = ((self.st_freq(p) as u16 + adder as u16) >> 1) as u8;
            self.set_st_freq(p, halved);
            new_summ += halved as u16;

            // Maintain sorted order.
            let prev = p - STATE_SIZE as u32;
            if halved > self.st_freq(prev) {
                // Bubble up.
                let tmp_sym = self.st_sym(p);
                let tmp_freq = halved;
                let tmp_succ = self.st_succ(p);
                let mut dst = p;
                loop {
                    self.copy_state(dst, dst - STATE_SIZE as u32);
                    dst -= STATE_SIZE as u32;
                    if dst == stats || tmp_freq <= self.st_freq(dst - STATE_SIZE as u32) {
                        break;
                    }
                }
                self.set_st_sym(dst, tmp_sym);
                self.set_st_freq(dst, tmp_freq);
                self.set_st_succ(dst, tmp_succ);
            }

            p += STATE_SIZE as u32;
            i -= 1;
        }

        // Remove zero-frequency states.
        // p points past last state. Check the last state's freq.
        p -= STATE_SIZE as u32; // back to last state
        if self.st_freq(p) == 0 {
            let mut zero_count = 0i32;
            while self.st_freq(p) == 0 && p > stats {
                zero_count += 1;
                p -= STATE_SIZE as u32;
            }
            if self.st_freq(p) == 0 {
                zero_count += 1;
            }
            esc_freq += zero_count;
            let new_ns = old_ns as u16 - zero_count as u16;
            self.set_ctx_num_stats(ctx, new_ns);

            if new_ns == 1 {
                // Collapse to single-state (OneState) context.
                let tmp_sym = self.st_sym(stats);
                let tmp_freq = self.st_freq(stats);
                let tmp_succ = self.st_succ(stats);

                // Halve freq until escape is small.
                let mut tf = tmp_freq;
                let mut ef = esc_freq;
                while ef > 1 {
                    tf = tf.saturating_sub(tf >> 1);
                    ef >>= 1;
                }

                // Free the stats array.
                self.alloc.free_units(off_to_ref(stats), (old_ns + 1) >> 1);

                // Write OneState inline.
                self.set_one_sym(ctx, tmp_sym);
                self.set_one_freq(ctx, tf);
                self.set_one_succ(ctx, tmp_succ);
                self.found_state = ctx + CTX_ONE_SYM as u32;
                return;
            }
        }

        // Update SummFreq with halved escape.
        new_summ += (esc_freq as u16).wrapping_sub((esc_freq as u16) >> 1);
        self.set_ctx_summ_freq(ctx, new_summ);

        // Shrink stats array if needed.
        let n0 = (old_ns + 1) >> 1;
        let new_ns = self.ctx_num_stats(ctx) as usize;
        let n1 = (new_ns + 1) >> 1;
        if n0 != n1 {
            let new_stats = self.alloc.shrink_units(off_to_ref(stats), n0, n1);
            self.set_ctx_stats(ctx, ref_to_off(new_stats));
        }
        self.found_state = self.ctx_stats(ctx);
    }

    // =======================================================================
    // UpdateModel
    // =======================================================================

    fn update_model(&mut self) {
        let fs_sym = self.st_sym(self.found_state);
        let fs_freq = self.st_freq(self.found_state);
        let fs_succ = self.st_succ(self.found_state);

        // Update suffix context frequencies.
        let suffix = self.ctx_suffix(self.min_context);
        if fs_freq < MAX_FREQ / 4 && suffix != 0 {
            let sns = self.ctx_num_stats(suffix);
            if sns != 1 {
                // Find fs_sym in suffix stats.
                let s_stats = self.ctx_stats(suffix);
                let mut p = s_stats;
                if self.st_sym(p) != fs_sym {
                    let end = s_stats + (sns as u32) * STATE_SIZE as u32;
                    p += STATE_SIZE as u32;
                    while p < end && self.st_sym(p) != fs_sym {
                        p += STATE_SIZE as u32;
                    }
                    if p >= end {
                        // Symbol not found in suffix — skip update.
                        self.do_update_model_core(fs_sym, fs_freq, fs_succ, 0);
                        return;
                    }
                    // Swap with predecessor if freq is higher.
                    let prev = p - STATE_SIZE as u32;
                    if self.st_freq(p) >= self.st_freq(prev) {
                        self.swap_states(p, prev);
                        p = prev;
                    }
                }
                if self.st_freq(p) < MAX_FREQ - 9 {
                    let f = self.st_freq(p) + 2;
                    self.set_st_freq(p, f);
                    let sf = self.ctx_summ_freq(suffix);
                    self.set_ctx_summ_freq(suffix, sf.wrapping_add(2));
                }
                self.do_update_model_core(fs_sym, fs_freq, fs_succ, p);
            } else {
                // Suffix is binary context.
                let f = self.one_freq(suffix);
                if f < 32 {
                    self.set_one_freq(suffix, f + 1);
                }
                self.do_update_model_core(fs_sym, fs_freq, fs_succ, suffix + CTX_ONE_SYM as u32);
            }
        } else {
            self.do_update_model_core(fs_sym, fs_freq, fs_succ, 0);
        }
    }

    /// Core of UpdateModel after suffix freq update.
    /// `p1` is the state offset in the suffix context (0 if none).
    fn do_update_model_core(&mut self, fs_sym: u8, fs_freq: u8, fs_succ: u32, p1: u32) {
        if self.order_fall == 0 {
            // No escape: create successors.
            let new_ctx = self.create_successors(true, p1);
            if new_ctx == 0 {
                self.restart();
                self.esc_count = 0;
                return;
            }
            self.min_context = new_ctx;
            self.max_context = new_ctx;
            // Update found state's successor.
            self.set_st_succ(self.found_state, new_ctx);
            return;
        }

        // OrderFall > 0: store symbol in text region and propagate.
        self.alloc.write_text_byte(fs_sym);
        let successor = self.alloc.text_position() as u32;
        if self.alloc.text_exhausted() {
            self.restart();
            self.esc_count = 0;
            return;
        }

        let final_succ;
        if fs_succ != 0 {
            // Existing successor — may need to create real contexts from text chain.
            if self.is_text_succ(fs_succ) {
                let new_succ = self.create_successors(false, p1);
                if new_succ == 0 {
                    self.restart();
                    self.esc_count = 0;
                    return;
                }
                self.set_st_succ(self.found_state, new_succ);
            }
            self.order_fall -= 1;
            if self.order_fall == 0 {
                final_succ = self.st_succ(self.found_state);
                if self.max_context != self.min_context {
                    self.alloc.text_dec();
                }
            } else {
                final_succ = successor;
            }
        } else {
            // No successor yet: set text pointer as successor.
            self.set_st_succ(self.found_state, successor);
            final_succ = successor;
            // For the propagation loop below, fs_succ becomes min_context.
            // (In unrar: fs.Successor = MinContext)
        }

        let min_ctx = self.min_context;
        let ns = self.ctx_num_stats(min_ctx) as u32;
        let s0 = self.ctx_summ_freq(min_ctx) as u32 - ns - fs_freq as u32 + 1;

        let mut pc = self.max_context;
        while pc != min_ctx {
            let ns1 = self.ctx_num_stats(pc) as u32;

            if ns1 != 1 {
                // Multi-symbol context: expand stats array if needed.
                if (ns1 & 1) == 0 {
                    let old_stats = self.ctx_stats(pc);
                    let new_stats = self
                        .alloc
                        .expand_units(off_to_ref(old_stats), (ns1 >> 1) as usize);
                    if new_stats.is_null() {
                        self.restart();
                        self.esc_count = 0;
                        return;
                    }
                    self.set_ctx_stats(pc, ref_to_off(new_stats));
                }
                // Adjust SummFreq.
                let sf = self.ctx_summ_freq(pc) as u32;
                let adj = (if 2 * ns1 < ns { 1u32 } else { 0 })
                    + 2 * (if 4 * ns1 <= ns && sf <= 8 * ns1 { 1 } else { 0 });
                self.set_ctx_summ_freq(pc, (sf + adj) as u16);
            } else {
                // Single-state: promote to multi-state.
                let new_stats_ref = self.alloc.alloc_units(1);
                if new_stats_ref.is_null() {
                    self.restart();
                    self.esc_count = 0;
                    return;
                }
                let new_stats = ref_to_off(new_stats_ref);
                // Copy OneState to the new stats array.
                let os_sym = self.one_sym(pc);
                let os_freq = self.one_freq(pc);
                let os_succ = self.one_succ(pc);
                self.set_st_sym(new_stats, os_sym);
                let adj_freq = if os_freq < MAX_FREQ / 4 - 1 {
                    os_freq * 2
                } else {
                    MAX_FREQ - 4
                };
                self.set_st_freq(new_stats, adj_freq);
                self.set_st_succ(new_stats, os_succ);
                self.set_ctx_stats(pc, new_stats);
                self.set_ctx_summ_freq(
                    pc,
                    adj_freq as u16 + self.init_esc as u16 + if ns > 3 { 1 } else { 0 },
                );
            }

            // Compute new state's frequency.
            let sf_pc = self.ctx_summ_freq(pc) as u32;
            let cf = 2 * fs_freq as u32 * (sf_pc + 6);
            let sf = s0 + sf_pc;
            let new_freq;
            if cf < 6 * sf {
                new_freq = 1 + (if cf > sf { 1 } else { 0 }) + (if cf >= 4 * sf { 1 } else { 0 });
                let new_sf = sf_pc + 3;
                self.set_ctx_summ_freq(pc, new_sf as u16);
            } else {
                new_freq = 4
                    + (if cf >= 9 * sf { 1 } else { 0 })
                    + (if cf >= 12 * sf { 1 } else { 0 })
                    + (if cf >= 15 * sf { 1 } else { 0 });
                let new_sf = sf_pc + new_freq;
                self.set_ctx_summ_freq(pc, new_sf as u16);
            }

            // Append new state at the end.
            let ns1 = self.ctx_num_stats(pc) as u32;
            let new_state = self.ctx_stats(pc) + ns1 * STATE_SIZE as u32;
            self.set_st_succ(new_state, final_succ);
            self.set_st_sym(new_state, fs_sym);
            self.set_st_freq(new_state, new_freq as u8);
            self.set_ctx_num_stats(pc, (ns1 + 1) as u16);

            pc = self.ctx_suffix(pc);
        }

        let found_succ = self.st_succ(self.found_state);
        if found_succ != 0 {
            self.max_context = found_succ;
            self.min_context = found_succ;
        } else {
            self.max_context = final_succ;
            self.min_context = final_succ;
        }
    }

    // =======================================================================
    // CreateSuccessors
    // =======================================================================

    fn create_successors(&mut self, skip: bool, p1: u32) -> u32 {
        let up_branch = self.st_succ(self.found_state);
        let found_sym = self.st_sym(self.found_state);

        let mut pc = self.min_context;
        let mut ps: [u32; MAX_ORDER + 1] = [0; MAX_ORDER + 1]; // state offsets
        let mut ps_len = 0usize;

        if !skip {
            ps[ps_len] = self.found_state;
            ps_len += 1;
            let suffix = self.ctx_suffix(pc);
            if suffix == 0 {
                // NO_LOOP
                return self.finish_create_successors(&ps[..ps_len], pc, up_branch, found_sym);
            }
        }

        if p1 != 0 {
            // p1 provided: use it and start from suffix.
            let suffix = self.ctx_suffix(pc);
            pc = suffix;
            // Check if p1's successor matches up_branch.
            if self.st_succ(p1) != up_branch {
                return self.st_succ(p1);
            }
            if ps_len < MAX_ORDER + 1 {
                ps[ps_len] = p1;
                ps_len += 1;
            }
            // Fall through to suffix walk.
            if self.ctx_suffix(pc) == 0 {
                // No more suffix to walk.
                return self.finish_create_successors(&ps[..ps_len], pc, up_branch, found_sym);
            }
            pc = self.ctx_suffix(pc);
        } else if !skip {
            pc = self.ctx_suffix(pc);
        }

        // Walk suffix chain.
        loop {
            let ns = self.ctx_num_stats(pc);
            let p;
            if ns != 1 {
                // Multi-symbol: find our symbol.
                let stats = self.ctx_stats(pc);
                p = self.find_state(stats, ns as usize, found_sym);
                if p == 0 {
                    break; // Not found — shouldn't happen in valid data.
                }
            } else {
                // Binary context.
                p = pc + CTX_ONE_SYM as u32;
            }

            if self.st_succ(p) != up_branch {
                // Found a real successor — use it as the base.
                pc = self.st_succ(p);
                break;
            }
            if ps_len > MAX_ORDER {
                return 0; // Safety limit.
            }
            ps[ps_len] = p;
            ps_len += 1;

            let suffix = self.ctx_suffix(pc);
            if suffix == 0 {
                break;
            }
            pc = suffix;
        }

        self.finish_create_successors(&ps[..ps_len], pc, up_branch, found_sym)
    }

    fn finish_create_successors(
        &mut self,
        ps: &[u32],
        mut pc: u32,
        up_branch: u32,
        found_sym: u8,
    ) -> u32 {
        if ps.is_empty() {
            return pc;
        }

        // Read the symbol and successor from the text chain (UpBranch).
        let up_sym;
        let up_succ;
        if up_branch != 0 && self.is_text_succ(up_branch) {
            up_sym = self.alloc.read_byte_at(up_branch as usize);
            up_succ = up_branch + 1;
        } else {
            up_sym = found_sym;
            up_succ = up_branch;
        }

        // Determine the frequency for the new state.
        let up_freq;
        let ns_pc = self.ctx_num_stats(pc);
        if ns_pc != 1 {
            // Validate pc.
            if (pc as usize) < self.alloc.units_start_bytes() {
                return 0;
            }
            let stats = self.ctx_stats(pc);
            let p = self.find_state(stats, ns_pc as usize, up_sym);
            if p == 0 {
                up_freq = 1;
            } else {
                let cf = self.st_freq(p) as u32 - 1;
                let s0 = self.ctx_summ_freq(pc) as u32 - ns_pc as u32 - cf;
                up_freq = if 2 * cf <= s0 {
                    1 + (if 5 * cf > s0 { 1 } else { 0 })
                } else {
                    ((2 * cf + 3 * s0 - 1) / (2 * s0)).min(255) as u8
                };
            }
        } else {
            up_freq = self.one_freq(pc);
        }

        // Create child contexts from ps (in reverse order).
        for &state_off in ps.iter().rev() {
            let child_ref = self.alloc.alloc_context();
            if child_ref.is_null() {
                return 0;
            }
            let child = ref_to_off(child_ref);

            // Initialize as single-state context.
            self.set_ctx_num_stats(child, 1);
            self.set_one_sym(child, up_sym);
            self.set_one_freq(child, up_freq);
            self.set_one_succ(child, up_succ);
            self.set_ctx_suffix(child, pc);

            // Update the state's successor to point to this new child.
            self.set_st_succ(state_off, child);

            pc = child;
        }

        pc
    }

    /// Find a state with the given symbol in a stats array.
    fn find_state(&self, stats: u32, ns: usize, sym: u8) -> u32 {
        let mut p = stats;
        for _ in 0..ns {
            if self.st_sym(p) == sym {
                return p;
            }
            p += STATE_SIZE as u32;
        }
        0 // Not found.
    }

    fn clear_mask(&mut self) {
        self.esc_count = 1;
        self.char_mask = [0; 256];
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_model_creation() {
        let model = Model::new(6, 1024 * 1024);
        assert_ne!(model.min_context, 0);
        assert_ne!(model.max_context, 0);
    }

    #[test]
    fn test_model_restart() {
        let mut model = Model::new(6, 1024 * 1024);
        model.restart();
        assert_ne!(model.min_context, 0);
    }

    #[test]
    fn test_root_has_256_symbols() {
        let model = Model::new(6, 1024 * 1024);
        let ns = model.ctx_num_stats(model.min_context);
        assert_eq!(ns, 256);
    }

    #[test]
    fn test_root_summary_freq() {
        let model = Model::new(6, 1024 * 1024);
        let sf = model.ctx_summ_freq(model.min_context);
        assert_eq!(sf, 257);
    }

    #[test]
    fn test_root_states() {
        let model = Model::new(6, 1024 * 1024);
        let stats = model.ctx_stats(model.min_context);
        // First state: symbol=0, freq=1.
        assert_eq!(model.st_sym(stats), 0);
        assert_eq!(model.st_freq(stats), 1);
        // Last state: symbol=255, freq=1.
        let last = stats + 255 * STATE_SIZE as u32;
        assert_eq!(model.st_sym(last), 255);
        assert_eq!(model.st_freq(last), 1);
    }

    #[test]
    fn test_decode_from_zeros() {
        let mut model = Model::new(6, 1024 * 1024);
        let data = vec![0u8; 256];
        let mut rc = RangeDecoder::new(&data).unwrap();
        // Should decode symbols without crashing. -1 is valid (escape/end).
        for _ in 0..5 {
            let ch = model.decode_char(&mut rc);
            assert!(
                (ch >= 0 && ch < 256) || ch == -1,
                "unexpected decode result: {ch}"
            );
        }
    }
}
