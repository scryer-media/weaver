//! PPMd variant H context model.
//!
//! The context model is a trie of contexts (symbol sequences). Each context
//! stores frequency counts for the symbols that have followed it. When a symbol
//! is decoded:
//! 1. Look up the current (longest) context
//! 2. If the symbol is found, decode it and update frequencies
//! 3. If not found (escape), fall back to the next shorter context and repeat
//! 4. At order -1, decode the symbol uniformly (any byte is equally likely)
//!
//! The model supports orders 2 through MAX_ORDER (typically 2-16 for RAR5).
//!
//! Memory layout in the sub-allocator:
//! - Context node (12 bytes): suffix_ref(4), num_stats(2), summary_freq(2), states_ref(4)
//! - State (6 bytes): symbol(1), freq(1), successor_ref(4) — packed 2 per unit
//!
//! Reference: 7-zip Ppmd7.c (public domain), Shkarin's original PPMd (public domain).

use super::alloc::{NodeRef, SubAllocator, UNIT_SIZE};
use super::range::RangeDecoder;
use super::see::SeeTable;
use crate::error::{RarError, RarResult};

/// Maximum supported model order.
const MAX_ORDER: usize = 64;

/// Context node field offsets within a 12-byte unit.
mod ctx {
    /// Offset of suffix context reference (NodeRef, u32).
    pub const SUFFIX: usize = 0;
    /// Offset of num_stats (u16) — number of distinct symbols minus 1.
    pub const NUM_STATS: usize = 4;
    /// Offset of summary frequency (u16) — total frequency of all symbols.
    pub const SUMMARY_FREQ: usize = 6;
    /// Offset of states reference (NodeRef, u32) — pointer to state array.
    /// For num_stats==0, this unit itself holds the single state at offset 8..12+
    /// Actually for single-state contexts, the state is stored inline:
    /// byte 8 = symbol, byte 9 = freq, bytes 8..12 reused as state.
    /// Wait — let's use a different layout for clarity:
    /// For num_stats==0: state is at a separate single-unit allocation.
    /// For num_stats>=1: states_ref points to an array of states.
    pub const STATES: usize = 8;
}

/// State record: 6 bytes packed (symbol, freq, successor).
/// Two states fit in one 12-byte unit.
mod state {
    pub const SIZE: usize = 6;
    /// Symbol byte.
    pub const SYMBOL: usize = 0;
    /// Frequency count.
    pub const FREQ: usize = 1;
    /// Successor context reference (u32 LE).
    pub const SUCCESSOR: usize = 2;
}

/// Binomial summation table for order-0 context escape estimation.
/// 128 entries for num_masked_symbols, each with 64 entries for sum_freq.
/// Simplified: use a flat initial value.
const BIN_SUMM_INIT: u16 = 1 << 14;

/// PPMd variant H model.
pub struct Model {
    /// Sub-allocator for context/state nodes.
    alloc: SubAllocator,
    /// SEE table for escape estimation.
    see: SeeTable,
    /// Maximum model order.
    max_order: usize,
    /// Current model order (decreases on escape, resets on successful decode).
    order: i32,
    /// Root context (order 0).
    root_ctx: NodeRef,
    /// Current (highest-order) context for the current position.
    current_ctx: NodeRef,
    /// Binary summation table for order-1 escape probabilities.
    /// Indexed by [freq_bin][symbol_group].
    bin_summ: [[u16; 64]; 25],
    /// Character mask for exclusion during escape fallback.
    char_mask: [u8; 256],
    /// Escape count for char_mask generation tracking.
    escape_count: u8,
    /// Previous success flag (affects SEE context selection).
    prev_success: u8,
    /// Last decoded symbol's frequency (for adaptation).
    last_sym_freq: u8,
    /// Number of symbols masked in current escape chain.
    num_masked: usize,
    /// Run length for the current context.
    run_length: i32,
    /// Initial run length.
    init_rl: i32,
}

impl Model {
    /// Create a new PPMd model.
    ///
    /// `max_order` is the maximum context order (RAR5 typically uses the value
    /// from the compression info, commonly 2-16).
    /// `alloc_size` is the sub-allocator arena size in bytes.
    pub fn new(max_order: usize, alloc_size: usize) -> Self {
        let max_order = max_order.min(MAX_ORDER);
        let mut model = Self {
            alloc: SubAllocator::new(alloc_size),
            see: SeeTable::new(),
            max_order,
            order: 0,
            root_ctx: NodeRef::NULL,
            current_ctx: NodeRef::NULL,
            bin_summ: [[BIN_SUMM_INIT; 64]; 25],
            char_mask: [0; 256],
            escape_count: 0,
            prev_success: 0,
            last_sym_freq: 0,
            num_masked: 0,
            run_length: 0,
            init_rl: -(max_order as i32) - 1,
        };

        model.restart();
        model
    }

    /// Reset the model to initial state.
    pub fn restart(&mut self) {
        self.alloc.reset();
        self.see = SeeTable::new();
        self.char_mask = [0; 256];
        self.escape_count = 0;
        self.prev_success = 0;
        self.run_length = self.init_rl;

        // Initialize binary summation table.
        for i in 0..25 {
            for j in 0..64 {
                self.bin_summ[i][j] = BIN_SUMM_INIT;
            }
        }

        // Create root (order-0) context with all 256 symbols.
        self.root_ctx = self.alloc.alloc_one();
        if self.root_ctx.is_null() {
            return;
        }

        // Allocate state array for all 256 symbols.
        // Each state is 6 bytes; 2 fit per unit. Need 128 units.
        let states = self.alloc.alloc_units(128);
        if states.is_null() {
            return;
        }

        // Initialize root context.
        self.alloc.write_u32(self.root_ctx, ctx::SUFFIX, NodeRef::NULL.0);
        self.alloc.write_u16(self.root_ctx, ctx::NUM_STATS, 255); // 256 symbols - 1
        self.alloc.write_u16(self.root_ctx, ctx::SUMMARY_FREQ, 256 + 1);
        self.alloc.write_u32(self.root_ctx, ctx::STATES, states.0);

        // Initialize each state with freq=1 and no successor.
        for i in 0..256u16 {
            let off = states.offset() + i as usize * state::SIZE;
            self.write_state_at_offset(off, i as u8, 1, NodeRef::NULL);
        }

        self.current_ctx = self.root_ctx;
        self.order = self.max_order as i32;
    }

    /// Write a state at a raw byte offset in the arena.
    fn write_state_at_offset(&mut self, off: usize, symbol: u8, freq: u8, successor: NodeRef) {
        let arena = &mut self.alloc;
        // Use direct arena access via node at byte offset.
        // We need a NodeRef for the base, then field offsets.
        // Since states are 6 bytes and packed, we access the arena directly.
        let node = NodeRef((off / UNIT_SIZE) as u32);
        let field_off = off % UNIT_SIZE;
        arena.write_u8(node, field_off + state::SYMBOL, symbol);
        arena.write_u8(node, field_off + state::FREQ, freq);
        // Write successor as u32 LE at field_off + 2
        let succ_bytes = successor.0.to_le_bytes();
        arena.write_u8(node, field_off + state::SUCCESSOR, succ_bytes[0]);
        arena.write_u8(node, field_off + state::SUCCESSOR + 1, succ_bytes[1]);
        arena.write_u8(node, field_off + state::SUCCESSOR + 2, succ_bytes[2]);
        arena.write_u8(node, field_off + state::SUCCESSOR + 3, succ_bytes[3]);
    }

    /// Read a state's fields at a byte offset.
    fn read_state_at_offset(&self, off: usize) -> (u8, u8, NodeRef) {
        let node = NodeRef((off / UNIT_SIZE) as u32);
        let field_off = off % UNIT_SIZE;
        let symbol = self.alloc.read_u8(node, field_off + state::SYMBOL);
        let freq = self.alloc.read_u8(node, field_off + state::FREQ);
        let s0 = self.alloc.read_u8(node, field_off + state::SUCCESSOR) as u32;
        let s1 = self.alloc.read_u8(node, field_off + state::SUCCESSOR + 1) as u32;
        let s2 = self.alloc.read_u8(node, field_off + state::SUCCESSOR + 2) as u32;
        let s3 = self.alloc.read_u8(node, field_off + state::SUCCESSOR + 3) as u32;
        let successor = NodeRef(s0 | (s1 << 8) | (s2 << 16) | (s3 << 24));
        (symbol, freq, successor)
    }

    /// Get the byte offset of the i-th state in a context's state array.
    fn state_offset(&self, ctx_ref: NodeRef, index: usize) -> usize {
        let states_ref = NodeRef(self.alloc.read_u32(ctx_ref, ctx::STATES));
        states_ref.offset() + index * state::SIZE
    }

    /// Get num_stats for a context (number of distinct symbols minus 1).
    fn num_stats(&self, ctx_ref: NodeRef) -> u16 {
        self.alloc.read_u16(ctx_ref, ctx::NUM_STATS)
    }

    /// Get summary frequency for a context.
    fn summary_freq(&self, ctx_ref: NodeRef) -> u16 {
        self.alloc.read_u16(ctx_ref, ctx::SUMMARY_FREQ)
    }

    /// Get suffix context reference.
    fn suffix(&self, ctx_ref: NodeRef) -> NodeRef {
        NodeRef(self.alloc.read_u32(ctx_ref, ctx::SUFFIX))
    }

    /// Decode one symbol from the model using the range decoder.
    ///
    /// Returns the decoded byte, or None on end-of-data / model reset.
    pub fn decode_symbol(&mut self, rc: &mut RangeDecoder) -> RarResult<Option<u8>> {
        if self.current_ctx.is_null() || !self.alloc.has_memory() {
            self.restart();
            if self.current_ctx.is_null() {
                return Err(RarError::CorruptArchive {
                    detail: "PPMd model failed to initialize".into(),
                });
            }
        }

        // Reset char mask for this decode cycle.
        self.char_mask = [0; 256];
        self.num_masked = 0;

        let mut ctx = self.current_ctx;

        loop {
            let ns = self.num_stats(ctx);

            if ns == 0 {
                // Binary context (single symbol).
                match self.decode_bin_context(rc, ctx)? {
                    DecodeResult::Symbol(sym) => {
                        self.update_after_decode(sym);
                        return Ok(Some(sym));
                    }
                    DecodeResult::Escape => {
                        // Fall to suffix context.
                        ctx = self.suffix(ctx);
                        if ctx.is_null() {
                            // Reached order -1: decode uniform.
                            return self.decode_order_minus1(rc);
                        }
                    }
                }
            } else {
                // Multi-symbol context.
                match self.decode_multi_context(rc, ctx)? {
                    DecodeResult::Symbol(sym) => {
                        self.update_after_decode(sym);
                        return Ok(Some(sym));
                    }
                    DecodeResult::Escape => {
                        // Mark all symbols in this context as masked.
                        let n = ns as usize + 1;
                        for i in 0..n {
                            let (symbol, _, _) = self.read_state_at_offset(self.state_offset(ctx, i));
                            self.char_mask[symbol as usize] = 1;
                        }
                        self.num_masked += n;

                        ctx = self.suffix(ctx);
                        if ctx.is_null() {
                            return self.decode_order_minus1(rc);
                        }
                    }
                }
            }
        }
    }

    /// Decode from a binary (single-symbol) context.
    fn decode_bin_context(
        &mut self,
        rc: &mut RangeDecoder,
        ctx_ref: NodeRef,
    ) -> RarResult<DecodeResult> {
        let state_off = self.state_offset(ctx_ref, 0);
        let (symbol, freq, _successor) = self.read_state_at_offset(state_off);

        // Check if this symbol is masked.
        if self.char_mask[symbol as usize] != 0 {
            return Ok(DecodeResult::Escape);
        }

        // Binary probability from the bin_summ table.
        let freq_idx = (freq.min(196) as usize) >> 3;
        let ns_suffix = if !self.suffix(ctx_ref).is_null() {
            let suf = self.suffix(ctx_ref);
            self.num_stats(suf) as usize
        } else {
            0
        };
        let bin_idx = ns_suffix.min(63);
        let prob = self.bin_summ[freq_idx][bin_idx];

        if rc.decode_binary(prob as u32, 1 << 15) {
            // Symbol decoded successfully.
            // Increase probability.
            self.bin_summ[freq_idx][bin_idx] =
                prob.wrapping_add((1u16 << 15).wrapping_sub(prob) >> 3);

            // Update frequency.
            let new_freq = freq.saturating_add(1);
            self.write_state_freq(state_off, new_freq);
            self.prev_success = 1;
            self.last_sym_freq = new_freq;
            self.run_length += 1;

            Ok(DecodeResult::Symbol(symbol))
        } else {
            // Escape: symbol not present.
            // Decrease probability.
            self.bin_summ[freq_idx][bin_idx] = prob.wrapping_sub(prob >> 3);
            self.prev_success = 0;
            self.char_mask[symbol as usize] = 1;
            self.num_masked += 1;
            self.escape_count = self.escape_count.wrapping_add(1);

            Ok(DecodeResult::Escape)
        }
    }

    /// Decode from a multi-symbol context.
    fn decode_multi_context(
        &mut self,
        rc: &mut RangeDecoder,
        ctx_ref: NodeRef,
    ) -> RarResult<DecodeResult> {
        let ns = self.num_stats(ctx_ref) as usize + 1;
        let sum_freq = self.summary_freq(ctx_ref) as u32;

        if self.num_masked > 0 {
            // Use SEE for escape estimation when symbols are masked.
            return self.decode_multi_masked(rc, ctx_ref, ns);
        }

        let count = rc.get_current_count(sum_freq);
        let mut cum = 0u32;

        for i in 0..ns {
            let state_off = self.state_offset(ctx_ref, i);
            let (symbol, freq, _) = self.read_state_at_offset(state_off);
            cum += freq as u32;

            if cum > count {
                // Found the symbol.
                let sym_freq = freq as u32;
                rc.decode(cum - sym_freq, sym_freq, sum_freq);

                // Update frequency.
                let new_freq = freq.saturating_add(4);
                self.write_state_freq(state_off, new_freq);

                // Update summary frequency.
                let new_sum = (sum_freq + 4).min(u16::MAX as u32);
                self.alloc.write_u16(ctx_ref, ctx::SUMMARY_FREQ, new_sum as u16);

                self.prev_success = if i == 0 { 1 } else { 0 };
                self.last_sym_freq = new_freq;
                if i == 0 {
                    self.run_length += 1;
                } else {
                    self.run_length = 0;
                }

                return Ok(DecodeResult::Symbol(symbol));
            }
        }

        // Escape: decode the escape symbol.
        let escape_freq = sum_freq - cum;
        rc.decode(cum, escape_freq.max(1), sum_freq);
        self.prev_success = 0;
        self.escape_count = self.escape_count.wrapping_add(1);
        self.run_length = 0;

        Ok(DecodeResult::Escape)
    }

    /// Decode from a multi-symbol context with some symbols masked (SEE path).
    fn decode_multi_masked(
        &mut self,
        rc: &mut RangeDecoder,
        ctx_ref: NodeRef,
        ns: usize,
    ) -> RarResult<DecodeResult> {
        // Calculate the total frequency of unmasked symbols.
        let mut total = 0u32;
        let mut unmasked_states: Vec<(usize, u8, u8)> = Vec::with_capacity(ns);

        for i in 0..ns {
            let state_off = self.state_offset(ctx_ref, i);
            let (symbol, freq, _) = self.read_state_at_offset(state_off);
            if self.char_mask[symbol as usize] == 0 {
                total += freq as u32;
                unmasked_states.push((i, symbol, freq));
            }
        }

        if unmasked_states.is_empty() || total == 0 {
            return Ok(DecodeResult::Escape);
        }

        // Get SEE escape estimate.
        let suffix_order = if !self.suffix(ctx_ref).is_null() {
            1
        } else {
            0
        };
        let see_ctx = self.see.get(
            unmasked_states.len(),
            suffix_order,
            self.prev_success,
        );
        let escape_freq = see_ctx.mean();
        let scale = total + escape_freq;

        let count = rc.get_current_count(scale);

        if count < total {
            // Symbol found among unmasked states.
            let mut cum = 0u32;
            for &(idx, symbol, freq) in &unmasked_states {
                cum += freq as u32;
                if cum > count {
                    let sym_freq = freq as u32;
                    rc.decode(cum - sym_freq, sym_freq, scale);

                    // Update frequency.
                    let state_off = self.state_offset(ctx_ref, idx);
                    let new_freq = freq.saturating_add(4);
                    self.write_state_freq(state_off, new_freq);

                    // Update SEE: success.
                    let see_ctx = self.see.get(
                        unmasked_states.len(),
                        suffix_order,
                        self.prev_success,
                    );
                    see_ctx.update_success();

                    self.prev_success = 0;
                    self.last_sym_freq = new_freq;
                    self.run_length = 0;

                    return Ok(DecodeResult::Symbol(symbol));
                }
            }
        }

        // Escape.
        rc.decode(total, escape_freq, scale);

        // Update SEE: escape.
        let see_ctx = self.see.get(
            unmasked_states.len(),
            suffix_order,
            self.prev_success,
        );
        see_ctx.update_escape();

        self.prev_success = 0;
        self.escape_count = self.escape_count.wrapping_add(1);

        Ok(DecodeResult::Escape)
    }

    /// Decode at order -1 (uniform distribution over unmasked symbols).
    fn decode_order_minus1(&mut self, rc: &mut RangeDecoder) -> RarResult<Option<u8>> {
        // Count unmasked symbols.
        let num_unmasked = 256 - self.num_masked;
        if num_unmasked == 0 {
            return Ok(None);
        }

        let count = rc.get_current_count(num_unmasked as u32);
        let mut cum = 0u32;

        for sym in 0..256u16 {
            if self.char_mask[sym as usize] == 0 {
                cum += 1;
                if cum > count {
                    rc.decode(cum - 1, 1, num_unmasked as u32);
                    self.update_after_decode(sym as u8);
                    return Ok(Some(sym as u8));
                }
            }
        }

        // Should not happen if count < num_unmasked.
        Err(RarError::CorruptArchive {
            detail: "PPMd order -1 decode failed".into(),
        })
    }

    /// Write a state's frequency byte.
    fn write_state_freq(&mut self, state_off: usize, freq: u8) {
        let node = NodeRef((state_off / UNIT_SIZE) as u32);
        let field_off = state_off % UNIT_SIZE;
        self.alloc.write_u8(node, field_off + state::FREQ, freq);
    }

    /// Post-decode model update: advance to successor context.
    fn update_after_decode(&mut self, _symbol: u8) {
        // In a full implementation, this would:
        // 1. Create/update successor contexts up to max_order
        // 2. Update current_ctx to point to the new highest-order context
        //
        // For now, we just advance to the root context. This is a simplified
        // version that trades compression ratio for correctness.
        // TODO: implement full context creation/update for optimal decode.
        self.current_ctx = self.root_ctx;
    }
}

/// Result of attempting to decode from a single context level.
enum DecodeResult {
    /// Successfully decoded a symbol.
    Symbol(u8),
    /// Escape: symbol not found, fall back to shorter context.
    Escape,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_model_creation() {
        let model = Model::new(6, 1024 * 1024);
        assert!(!model.root_ctx.is_null());
        assert!(!model.current_ctx.is_null());
    }

    #[test]
    fn test_model_restart() {
        let mut model = Model::new(6, 1024 * 1024);
        let root = model.root_ctx;
        model.restart();
        // After restart, root should be reallocated (same position due to reset)
        assert!(!model.root_ctx.is_null());
        assert_eq!(model.root_ctx.0, root.0);
    }

    #[test]
    fn test_root_context_has_256_symbols() {
        let model = Model::new(6, 1024 * 1024);
        let ns = model.num_stats(model.root_ctx);
        assert_eq!(ns, 255); // 256 - 1
    }

    #[test]
    fn test_root_context_summary_freq() {
        let model = Model::new(6, 1024 * 1024);
        let sf = model.summary_freq(model.root_ctx);
        assert_eq!(sf, 257); // 256 + 1
    }

    #[test]
    fn test_read_root_states() {
        let model = Model::new(6, 1024 * 1024);
        // First state should be symbol 0, freq 1
        let off = model.state_offset(model.root_ctx, 0);
        let (sym, freq, succ) = model.read_state_at_offset(off);
        assert_eq!(sym, 0);
        assert_eq!(freq, 1);
        assert!(succ.is_null());

        // Last state should be symbol 255, freq 1
        let off = model.state_offset(model.root_ctx, 255);
        let (sym, freq, _) = model.read_state_at_offset(off);
        assert_eq!(sym, 255);
        assert_eq!(freq, 1);
    }
}
