//! PPMd variant H context model.
//!
//! Full implementation of the PPMd variant H algorithm including proper context
//! model updates (UpdateModel, CreateSuccessors, rescale).
//!
//! Reference: 7-zip Ppmd7.c (public domain), Shkarin's original PPMd (public domain).

use super::alloc::{NodeRef, SubAllocator, UNIT_SIZE};
use super::range::RangeCode;
#[cfg(test)]
use super::range::RangeDecoder;
use super::see::SeeTable;
use crate::error::{RarError, RarResult};
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
    debug_output_index: u64,
    model_fault: bool,
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
            debug_output_index: 0,
            model_fault: false,
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
        self.debug_output_index = 0;

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
        succ != 0 && (succ as usize) <= self.alloc.text_position()
    }

    fn context_offset_valid(&self, ctx: u32) -> bool {
        self.alloc.model_offset_valid(ctx)
    }

    fn stats_offset_valid(&self, stats: u32, ns: usize) -> bool {
        if ns == 0 || ns > 256 {
            return false;
        }
        let len = ns.saturating_mul(STATE_SIZE);
        self.alloc.model_range_valid(stats, len)
    }

    fn fail_model(&mut self) -> i32 {
        self.model_fault = true;
        -1
    }

    // =======================================================================
    // Decode entry point
    // =======================================================================

    /// Decode one character. Returns 0-255 on success, -1 on error/restart.
    pub fn decode_char<R: RangeCode>(&mut self, rc: &mut R) -> i32 {
        let debug_output_index = self.debug_output_index;
        if std::env::var_os("WEAVER_RAR4_DEBUG_PPM").is_some()
            && (40272340..=40272346).contains(&debug_output_index)
        {
            let ns = self.ctx_num_stats(self.min_context);
            eprintln!(
                "PPMD decode_char start: index=117 min_context={} ns={} order_fall={} found_state={}",
                self.min_context, ns, self.order_fall, self.found_state
            );
        }
        if self.min_context == 0
            || self.alloc.text_exhausted()
            || !self.context_offset_valid(self.min_context)
        {
            if std::env::var_os("WEAVER_RAR4_DEBUG_PPM").is_some() {
                eprintln!(
                    "PPMD decode_char early fail: min_context={} text_exhausted={} heap_end={}",
                    self.min_context,
                    self.alloc.text_exhausted(),
                    self.alloc.heap_end_bytes()
                );
            }
            return self.fail_model();
        }

        let ns = self.ctx_num_stats(self.min_context);

        if ns != 1 {
            let stats = self.ctx_stats(self.min_context);
            if !self.stats_offset_valid(stats, ns as usize) {
                return self.fail_model();
            }
            // Multi-symbol context.
            if !self.decode_symbol1(rc) {
                if std::env::var_os("WEAVER_RAR4_DEBUG_PPM").is_some() {
                    eprintln!(
                        "PPMD decode_symbol1 failed: min_context={}",
                        self.min_context
                    );
                }
                return -1;
            }
        } else {
            // Binary context.
            if !self.decode_bin_symbol(rc) {
                return -1;
            }
        }

        // Escape loop: walk suffix chain until a symbol is found.
        while self.found_state == 0 {
            rc.normalize();
            loop {
                self.order_fall += 1;
                let prev_ctx = self.min_context;
                let suffix = self.ctx_suffix(self.min_context);
                self.min_context = suffix;
                if self.min_context == 0 {
                    if std::env::var_os("WEAVER_RAR4_DEBUG_PPM").is_some() {
                        eprintln!(
                            "PPMD suffix chain hit null: prev_ctx={} prev_ns={} num_masked={} max_context={} order_fall={}",
                            prev_ctx,
                            self.ctx_num_stats(prev_ctx),
                            self.num_masked,
                            self.max_context,
                            self.order_fall
                        );
                        let mut chain = self.max_context;
                        for depth in 0..8 {
                            if chain == 0 {
                                break;
                            }
                            let ns_chain = self.ctx_num_stats(chain) as usize;
                            if ns_chain == 1 {
                                eprintln!(
                                    "PPMD null max one-state[{depth}]: ctx={} sym={} freq={} suffix={}",
                                    chain,
                                    self.one_sym(chain),
                                    self.one_freq(chain),
                                    self.ctx_suffix(chain)
                                );
                            } else {
                                let stats = self.ctx_stats(chain);
                                let mut syms = Vec::new();
                                let mut p = stats;
                                for _ in 0..ns_chain.min(8) {
                                    syms.push((self.st_sym(p), self.st_freq(p)));
                                    p += STATE_SIZE as u32;
                                }
                                eprintln!(
                                    "PPMD null max states[{depth}]: ctx={} ns={} suffix={} states={:?}",
                                    chain,
                                    ns_chain,
                                    self.ctx_suffix(chain),
                                    syms
                                );
                            }
                            chain = self.ctx_suffix(chain);
                        }
                    }
                    return -1;
                }
                // Validate context pointer.
                if !self.context_offset_valid(self.min_context) {
                    if std::env::var_os("WEAVER_RAR4_DEBUG_PPM").is_some() {
                        eprintln!(
                            "PPMD suffix context invalid: ctx={} p_text={} heap_end={}",
                            self.min_context,
                            self.alloc.text_position(),
                            self.alloc.heap_end_bytes()
                        );
                    }
                    return self.fail_model();
                }
                let ns2 = self.ctx_num_stats(self.min_context) as usize;
                if ns2 != self.num_masked {
                    break;
                }
            }
            if !self.decode_symbol2(rc) {
                if std::env::var_os("WEAVER_RAR4_DEBUG_PPM").is_some() {
                    eprintln!(
                        "PPMD decode_symbol2 failed: min_context={}",
                        self.min_context
                    );
                }
                return -1;
            }
        }

        let symbol = self.st_sym(self.found_state);

        if self.order_fall == 0 {
            let succ = self.st_succ(self.found_state);
            if succ != 0 && !self.is_text_succ(succ) {
                if !self.context_offset_valid(succ) {
                    return self.fail_model();
                }
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

        if std::env::var_os("WEAVER_RAR4_DEBUG_PPM").is_some() {
            eprintln!(
                "PPMD decode_char ok: index={} symbol={} found_state={} order_fall={} min_context={}",
                debug_output_index, symbol, self.found_state, self.order_fall, self.min_context
            );
        }
        rc.normalize();
        self.debug_output_index += 1;
        symbol as i32
    }

    /// Decode one character with checked arena access.
    pub fn decode_char_result<R: RangeCode>(&mut self, rc: &mut R) -> RarResult<Option<u8>> {
        self.alloc.clear_arena_fault();
        self.model_fault = false;
        let ch = self.decode_char(rc);
        let model_fault = self.model_fault;
        let arena_fault = self.alloc.take_arena_fault();
        if model_fault || arena_fault {
            return Err(RarError::CorruptArchive {
                detail: "RAR PPMd model pointer out of bounds".into(),
            });
        }
        if ch < 0 { Ok(None) } else { Ok(Some(ch as u8)) }
    }

    // =======================================================================
    // decode_bin_symbol (NumStats == 1)
    // =======================================================================

    fn decode_bin_symbol<R: RangeCode>(&mut self, rc: &mut R) -> bool {
        let ctx = self.min_context;
        let symbol = self.one_sym(ctx);
        let freq = self.one_freq(ctx);

        // BinSumm index.
        self.hi_bits_flag = self.hb2_flag[self.found_state_symbol() as usize];
        let suffix = self.ctx_suffix(ctx);
        let suffix_ns = if suffix != 0 {
            if !self.context_offset_valid(suffix) {
                self.model_fault = true;
                return false;
            }
            self.ctx_num_stats(suffix)
        } else {
            1 // Avoid underflow; won't be used if suffix is null.
        };
        let idx1 = self.prev_success as usize
            + self.ns2_bs_indx[suffix_ns.wrapping_sub(1).min(255) as usize] as usize
            + self.hi_bits_flag as usize
            + 2 * self.hb2_flag[symbol as usize] as usize
            + ((self.run_length >> 26) as usize & 0x20);
        let idx0 = (freq as usize).saturating_sub(1).min(127);
        let idx1 = idx1.min(63);
        let bs = self.bin_summ[idx0][idx1];
        if std::env::var_os("WEAVER_RAR4_DEBUG_PPM").is_some()
            && (40272340..=40272346).contains(&self.debug_output_index)
        {
            eprintln!(
                "PPMD decode_bin_symbol: ctx={} symbol={} freq={} prev={} suffix={} suffix_ns={} hi={} sym_hi={} run={} idx0={} idx1={} bs={}",
                ctx,
                symbol,
                freq,
                self.prev_success,
                suffix,
                suffix_ns,
                self.hi_bits_flag,
                self.hb2_flag[symbol as usize],
                self.run_length,
                idx0,
                idx1,
                bs
            );
        }

        if bs as u32 > BIN_SCALE {
            return false;
        }
        let threshold = rc.get_threshold(BIN_SCALE);
        if std::env::var_os("WEAVER_RAR4_DEBUG_PPM").is_some()
            && (40272340..=40272346).contains(&self.debug_output_index)
        {
            eprintln!(
                "PPMD decode_bin_symbol threshold: ctx={} threshold={} bs={}",
                ctx, threshold, bs
            );
        }
        if threshold < bs as u32 {
            rc.decode(0, bs as u32, BIN_SCALE);
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
            rc.decode(bs as u32, BIN_SCALE - bs as u32, BIN_SCALE);
            let mean = ((bs as u32 + 32) >> 7) as u16;
            let new_bs = bs.wrapping_sub(mean);
            let Some(&init_esc) = EXP_ESCAPE.get((new_bs >> 10) as usize) else {
                return false;
            };
            if std::env::var_os("WEAVER_RAR4_DEBUG_PPM").is_some()
                && (40272340..=40272346).contains(&self.debug_output_index)
            {
                eprintln!(
                    "PPMD decode_bin_symbol escaped: ctx={} symbol={} new_bs={} init_esc={}",
                    ctx, symbol, new_bs, init_esc
                );
            }
            self.bin_summ[idx0][idx1] = new_bs;

            self.init_esc = init_esc;
            self.num_masked = 1;
            self.char_mask[symbol as usize] = self.esc_count;
            self.prev_success = 0;
            self.found_state = 0;
        }
        true
    }

    // =======================================================================
    // decode_symbol1 (NumStats > 1)
    // =======================================================================

    fn decode_symbol1<R: RangeCode>(&mut self, rc: &mut R) -> bool {
        let ctx = self.min_context;
        let ns = self.ctx_num_stats(ctx) as usize;
        let sum_freq = self.ctx_summ_freq(ctx) as u32;
        let stats = self.ctx_stats(ctx);
        if !self.stats_offset_valid(stats, ns) {
            self.model_fault = true;
            return false;
        }

        let count = rc.get_current_count(sum_freq);
        if std::env::var_os("WEAVER_RAR4_DEBUG_PPM").is_some()
            && (40272340..=40272346).contains(&self.debug_output_index)
        {
            eprintln!(
                "PPMD decode_symbol1: ctx={} ns={} sum_freq={} count={}",
                ctx, ns, sum_freq, count
            );
        }
        if count >= sum_freq {
            if std::env::var_os("WEAVER_RAR4_DEBUG_PPM").is_some() {
                eprintln!(
                    "PPMD decode_symbol1 count overflow: count={} sum_freq={} ctx={} found_state={}",
                    count, sum_freq, ctx, self.found_state
                );
            }
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
            if std::env::var_os("WEAVER_RAR4_DEBUG_PPM").is_some() {
                eprintln!("PPMD decode_symbol1 found_state=0");
            }
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
                rc.decode(hi_cnt, escape_freq, sum_freq);
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
        if p > stats {
            let prev = p - STATE_SIZE as u32;
            if new_freq > self.st_freq(prev) {
                self.swap_states(p, prev);
                self.found_state = prev;
                if self.st_freq(prev) > MAX_FREQ {
                    self.rescale(ctx);
                }
            }
        }
    }

    // =======================================================================
    // decode_symbol2 (masked context decode during escape)
    // =======================================================================

    fn decode_symbol2<R: RangeCode>(&mut self, rc: &mut R) -> bool {
        let ctx = self.min_context;
        let ns = self.ctx_num_stats(ctx) as usize;
        let stats = self.ctx_stats(ctx);
        if !self.stats_offset_valid(stats, ns) {
            self.model_fault = true;
            return false;
        }
        let suffix = self.ctx_suffix(ctx);
        if suffix != 0 && !self.context_offset_valid(suffix) {
            self.model_fault = true;
            return false;
        }
        let Some(diff) = ns.checked_sub(self.num_masked) else {
            if std::env::var_os("WEAVER_RAR4_DEBUG_PPM").is_some() {
                eprintln!(
                    "PPMD decode_symbol2 masked overflow: ctx={} ns={} num_masked={} max_context={} order_fall={}",
                    ctx, ns, self.num_masked, self.max_context, self.order_fall
                );
                if self.max_context != 0 {
                    let ns_max = self.ctx_num_stats(self.max_context) as usize;
                    if ns_max == 1 {
                        eprintln!(
                            "PPMD overflow max one-state: sym={} freq={}",
                            self.one_sym(self.max_context),
                            self.one_freq(self.max_context)
                        );
                    } else {
                        let stats = self.ctx_stats(self.max_context);
                        let mut syms = Vec::new();
                        let mut p = stats;
                        for _ in 0..ns_max.min(8) {
                            syms.push((self.st_sym(p), self.st_freq(p)));
                            p += STATE_SIZE as u32;
                        }
                        eprintln!(
                            "PPMD overflow max_context: ctx={} ns={} suffix={} states={:?}",
                            self.max_context,
                            ns_max,
                            self.ctx_suffix(self.max_context),
                            syms
                        );
                    }
                }
                let mut chain = ctx;
                for depth in 0..6 {
                    if chain == 0 {
                        break;
                    }
                    let ns_chain = self.ctx_num_stats(chain) as usize;
                    if ns_chain == 1 {
                        eprintln!(
                            "PPMD overflow one-state[{depth}]: sym={} freq={}",
                            self.one_sym(chain),
                            self.one_freq(chain)
                        );
                    } else {
                        let stats = self.ctx_stats(chain);
                        let mut syms = Vec::new();
                        let mut p = stats;
                        for _ in 0..ns_chain.min(8) {
                            syms.push((self.st_sym(p), self.st_freq(p)));
                            p += STATE_SIZE as u32;
                        }
                        eprintln!("PPMD overflow states[{depth}]: {:?}", syms);
                    }
                    eprintln!(
                        "PPMD overflow chain[{depth}]: ctx={} ns={} suffix={}",
                        chain,
                        ns_chain,
                        self.ctx_suffix(chain)
                    );
                    chain = self.ctx_suffix(chain);
                }
            }
            return false;
        };
        if diff == 0 {
            if std::env::var_os("WEAVER_RAR4_DEBUG_PPM").is_some() {
                eprintln!(
                    "PPMD decode_symbol2 diff=0: ctx={} ns={} num_masked={}",
                    ctx, ns, self.num_masked
                );
            }
            return false;
        }

        // makeEscFreq2
        let esc_freq = self.make_esc_freq2(ctx, diff);

        // Collect unmasked states.
        let mut unmasked: [(u32, u8); 256] = [(0, 0); 256]; // (state_off, freq)
        let mut n = 0usize;
        let mut hi_cnt = 0u32;
        let mut p = stats;
        let end = stats + (ns as u32) * STATE_SIZE as u32;
        while p < end {
            let sym = self.st_sym(p);
            if self.char_mask[sym as usize] != self.esc_count {
                if n >= unmasked.len() {
                    return false;
                }
                let freq = self.st_freq(p) as u32;
                hi_cnt += freq;
                unmasked[n] = (p, freq as u8);
                n += 1;
            }
            p += STATE_SIZE as u32;
        }

        let scale = esc_freq + hi_cnt;
        let count = rc.get_current_count(scale);
        if std::env::var_os("WEAVER_RAR4_DEBUG_PPM").is_some()
            && (40272340..=40272346).contains(&self.debug_output_index)
        {
            let mut preview = Vec::new();
            for &(st_off, freq) in &unmasked[..n.min(8)] {
                preview.push((self.st_sym(st_off), freq));
            }
            eprintln!(
                "PPMD decode_symbol2: ctx={} ns={} diff={} num_masked={} esc_freq={} hi_cnt={} scale={} count={} preview={:?}",
                ctx, ns, diff, self.num_masked, esc_freq, hi_cnt, scale, count, preview
            );
        }
        if count >= scale {
            if std::env::var_os("WEAVER_RAR4_DEBUG_PPM").is_some() {
                eprintln!(
                    "PPMD decode_symbol2 count overflow: ctx={} ns={} diff={} num_masked={} esc_count={} esc_freq={} hi_cnt={} scale={} count={}",
                    ctx, ns, diff, self.num_masked, self.esc_count, esc_freq, hi_cnt, scale, count
                );
                let stats = self.ctx_stats(ctx);
                let mut syms = Vec::new();
                let mut p = stats;
                for _ in 0..ns.min(16) {
                    syms.push((
                        self.st_sym(p),
                        self.st_freq(p),
                        self.char_mask[self.st_sym(p) as usize],
                    ));
                    p += STATE_SIZE as u32;
                }
                eprintln!("PPMD decode_symbol2 ctx states: {:?}", syms);
            }
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

        true // escape — FoundState stays NULL and decode_char continues down the suffix chain
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
        } else {
            let dummy = self.see.get_dummy();
            dummy.summ = dummy.summ.wrapping_add(scale as u16);
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

        if std::env::var_os("WEAVER_RAR4_DEBUG_PPM").is_some()
            && (110..=120).contains(&self.debug_output_index)
        {
            eprintln!(
                "PPMD update_model: index={} fs_sym={} fs_freq={} fs_succ={} order_fall={} min_context={} max_context={}",
                self.debug_output_index,
                fs_sym,
                fs_freq,
                fs_succ,
                self.order_fall,
                self.min_context,
                self.max_context
            );
        }

        // Update suffix context frequencies.
        let suffix = self.ctx_suffix(self.min_context);
        if fs_freq < MAX_FREQ / 4 && suffix != 0 {
            if !self.context_offset_valid(suffix) {
                self.model_fault = true;
                return;
            }
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
                        if std::env::var_os("WEAVER_RAR4_DEBUG_PPM").is_some() {
                            eprintln!(
                                "PPMD update_model missing suffix symbol: fs_sym={} min_context={} suffix={} suffix_ns={}",
                                fs_sym, self.min_context, suffix, sns
                            );
                        }
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
        let mut next_min_context = fs_succ;

        if self.order_fall == 0 {
            // No escape: create successors.
            let new_ctx = self.create_successors(true, p1);
            if std::env::var_os("WEAVER_RAR4_DEBUG_PPM").is_some() && self.is_text_succ(new_ctx) {
                eprintln!(
                    "PPMD order_fall=0 create_successors returned text: new_ctx={} p1={} found_state={} min_context={} max_context={}",
                    new_ctx, p1, self.found_state, self.min_context, self.max_context
                );
            }
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
                next_min_context = new_succ;
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
            // Match unrar: fs.Successor becomes the current MinContext even though the
            // live FoundState successor now points into the text buffer.
            next_min_context = self.min_context;
        }

        let min_ctx = self.min_context;
        let ns = self.ctx_num_stats(min_ctx) as u32;
        let s0 = (self.ctx_summ_freq(min_ctx) as u32)
            .wrapping_sub(ns)
            .wrapping_sub(fs_freq as u32)
            .wrapping_add(1);

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

        if std::env::var_os("WEAVER_RAR4_DEBUG_PPM").is_some()
            && self.is_text_succ(next_min_context)
        {
            eprintln!(
                "PPMD next_min_context still text: next={} final_succ={} fs_succ={} found_state={} min_ctx={} max_ctx={} order_fall={}",
                next_min_context,
                final_succ,
                fs_succ,
                self.found_state,
                min_ctx,
                self.max_context,
                self.order_fall
            );
        }
        self.max_context = next_min_context;
        self.min_context = next_min_context;
    }

    // =======================================================================
    // CreateSuccessors
    // =======================================================================

    fn create_successors(&mut self, skip: bool, p1: u32) -> u32 {
        let up_branch = self.st_succ(self.found_state);
        let found_sym = self.st_sym(self.found_state);

        if std::env::var_os("WEAVER_RAR4_DEBUG_PPM").is_some()
            && (40272337..=40272346).contains(&self.debug_output_index)
        {
            eprintln!(
                "PPMD create_successors: index={} skip={} p1={} up_branch={} found_sym={} min_context={} max_context={}",
                self.debug_output_index,
                skip,
                p1,
                up_branch,
                found_sym,
                self.min_context,
                self.max_context
            );
        }

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
                let succ = self.st_succ(p1);
                pc = if self.is_text_succ(succ) {
                    let new_ctx = self.materialize_text_successor(pc, p1, succ);
                    if new_ctx == 0 {
                        return 0;
                    }
                    new_ctx
                } else {
                    succ
                };
                return self.finish_create_successors(&ps[..ps_len], pc, up_branch, found_sym);
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
        } else {
            let suffix = self.ctx_suffix(pc);
            if suffix == 0 {
                return self.finish_create_successors(&ps[..ps_len], pc, up_branch, found_sym);
            }
            pc = suffix;
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
            if std::env::var_os("WEAVER_RAR4_DEBUG_PPM").is_some()
                && (40272337..=40272346).contains(&self.debug_output_index)
            {
                eprintln!(
                    "PPMD create_successors loop: index={} pc={} ns={} p={} p_succ={} p_sym={} ps_len={}",
                    self.debug_output_index,
                    pc,
                    ns,
                    p,
                    self.st_succ(p),
                    self.st_sym(p),
                    ps_len
                );
            }

            if self.st_succ(p) != up_branch {
                // Found a real successor — use it as the base.
                let succ = self.st_succ(p);
                pc = if self.is_text_succ(succ) {
                    let new_ctx = self.materialize_text_successor(pc, p, succ);
                    if new_ctx == 0 {
                        return 0;
                    }
                    new_ctx
                } else {
                    succ
                };
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

    fn materialize_text_successor(&mut self, base_ctx: u32, state_off: u32, text_succ: u32) -> u32 {
        let up_sym = self.alloc.read_byte_at(text_succ as usize);
        let up_succ = text_succ + 1;
        let up_freq = self.successor_init_freq(base_ctx, up_sym);

        let child_ref = self.alloc.alloc_context();
        if child_ref.is_null() {
            return 0;
        }
        let child = ref_to_off(child_ref);
        self.set_ctx_num_stats(child, 1);
        self.set_one_sym(child, up_sym);
        self.set_one_freq(child, up_freq);
        self.set_one_succ(child, up_succ);
        self.set_ctx_suffix(child, base_ctx);
        self.set_st_succ(state_off, child);
        if std::env::var_os("WEAVER_RAR4_DEBUG_PPM").is_some()
            && (40272337..=40272346).contains(&self.debug_output_index)
        {
            eprintln!(
                "PPMD materialize_text_successor: index={} base_ctx={} state_off={} text_succ={} child={} up_sym={} up_freq={}",
                self.debug_output_index, base_ctx, state_off, text_succ, child, up_sym, up_freq
            );
        }
        child
    }

    fn successor_init_freq(&self, base_ctx: u32, up_sym: u8) -> u8 {
        let ns_pc = self.ctx_num_stats(base_ctx);
        if ns_pc != 1 {
            let stats = self.ctx_stats(base_ctx);
            let p = self.find_state(stats, ns_pc as usize, up_sym);
            if p == 0 {
                return 1;
            }
            let cf = self.st_freq(p) as u32 - 1;
            let s0 = (self.ctx_summ_freq(base_ctx) as u32)
                .wrapping_sub(ns_pc as u32)
                .wrapping_sub(cf);
            if 2 * cf <= s0 {
                1 + (if 5 * cf > s0 { 1 } else { 0 })
            } else {
                (1 + ((2 * cf + 3 * s0 - 1) / (2 * s0))).min(255) as u8
            }
        } else {
            self.one_freq(base_ctx)
        }
    }

    fn finish_create_successors(
        &mut self,
        ps: &[u32],
        mut pc: u32,
        up_branch: u32,
        found_sym: u8,
    ) -> u32 {
        if ps.is_empty() {
            if std::env::var_os("WEAVER_RAR4_DEBUG_PPM").is_some() && self.is_text_succ(pc) {
                eprintln!(
                    "PPMD finish_create_successors returning text pc={} up_branch={} found_sym={}",
                    pc, up_branch, found_sym
                );
            }
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
                let s0 = (self.ctx_summ_freq(pc) as u32)
                    .wrapping_sub(ns_pc as u32)
                    .wrapping_sub(cf);
                up_freq = if 2 * cf <= s0 {
                    1 + (if 5 * cf > s0 { 1 } else { 0 })
                } else {
                    (1 + ((2 * cf + 3 * s0 - 1) / (2 * s0))).min(255) as u8
                };
            }
        } else {
            up_freq = self.one_freq(pc);
        }

        // Create child contexts from ps (in reverse order).
        if std::env::var_os("WEAVER_RAR4_DEBUG_PPM").is_some()
            && (40272337..=40272346).contains(&self.debug_output_index)
        {
            eprintln!(
                "PPMD finish_create_successors: index={} ps_len={} base_pc={} up_sym={} up_succ={} up_freq={}",
                self.debug_output_index,
                ps.len(),
                pc,
                up_sym,
                up_succ,
                up_freq
            );
        }
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

        if std::env::var_os("WEAVER_RAR4_DEBUG_PPM").is_some()
            && (40272337..=40272346).contains(&self.debug_output_index)
        {
            eprintln!(
                "PPMD finish_create_successors result: index={} new_pc={}",
                self.debug_output_index, pc
            );
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
        // Should decode symbols without crashing. None is valid (escape/end).
        for _ in 0..5 {
            let _ = model.decode_char_result(&mut rc).unwrap();
        }
    }

    #[test]
    fn decode_rejects_context_offset_in_text_region() {
        let mut model = Model::new(6, 1024 * 1024);
        model.min_context = UNIT_SIZE as u32;
        let data = vec![0u8; 256];
        let mut rc = RangeDecoder::new(&data).unwrap();

        let result = model.decode_char_result(&mut rc);

        assert!(matches!(result, Err(RarError::CorruptArchive { .. })));
    }

    #[test]
    fn decode_rejects_stats_offset_in_text_region() {
        let mut model = Model::new(6, 1024 * 1024);
        model.set_ctx_stats(model.min_context, UNIT_SIZE as u32);
        let data = vec![0u8; 256];
        let mut rc = RangeDecoder::new(&data).unwrap();

        let result = model.decode_char_result(&mut rc);

        assert!(matches!(result, Err(RarError::CorruptArchive { .. })));
    }
}
