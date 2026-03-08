//! Secondary Escape Estimation (SEE) for PPMd variant H.
//!
//! SEE provides adaptive probability estimates for the "escape" symbol in
//! contexts where the PPMd model needs to fall back to a lower-order context.
//! It tracks the frequency of escape events and adjusts estimates accordingly.
//!
//! Reference: Shkarin's PPMd (public domain), 7-zip Ppmd7.c (public domain).

/// A single SEE context that tracks escape probability.
#[derive(Clone, Copy)]
pub struct SeeContext {
    /// Scaled sum of escape frequencies (numerator).
    pub sum: u16,
    /// Right-shift count for extracting the mean estimate.
    pub shift: u8,
    /// Count of updates before adaptation.
    pub count: u8,
}

impl SeeContext {
    /// Create a new SEE context with an initial escape frequency estimate.
    pub fn new(init_val: u16, shift: u8) -> Self {
        Self {
            sum: init_val << shift,
            shift,
            count: 7,
        }
    }

    /// Get the current escape frequency estimate.
    #[inline]
    pub fn mean(&self) -> u32 {
        let m = (self.sum as u32) >> (self.shift as u32);
        // Minimum of 1 to avoid zero-frequency issues in range coder.
        m.max(1)
    }

    /// Update after an escape event occurred (symbol was NOT found in context).
    #[inline]
    pub fn update_escape(&mut self) {
        self.sum = self.sum.wrapping_add(1 << self.shift as u16);
        if self.count > 0 {
            self.count -= 1;
        }
        if self.count == 0 && self.sum > 128 {
            // Halve and potentially increase shift
            if self.shift < 7 {
                self.shift += 1;
            }
            self.sum = (self.sum + 1) >> 1;
            self.count = 3 << self.shift;
        }
    }

    /// Update after a successful symbol decode (symbol WAS found in context).
    #[inline]
    pub fn update_success(&mut self) {
        // Decrease escape estimate slightly.
        let m = self.mean() as u16;
        self.sum = self.sum.saturating_sub(m);
        if self.count > 0 {
            self.count -= 1;
        }
        if self.count == 0 && self.sum > 128 {
            if self.shift < 7 {
                self.shift += 1;
            }
            self.sum = (self.sum + 1) >> 1;
            self.count = 3 << self.shift;
        }
    }
}

/// Number of SEE context bins.
/// Indexed by: (num_stats - 1, recent_escape_flag, suffix_order).
pub const SEE_CONTEXTS: usize = 24;

/// The SEE table: multiple contexts indexed by model state.
pub struct SeeTable {
    pub contexts: [[SeeContext; SEE_CONTEXTS]; 4],
}

impl SeeTable {
    /// Initialize the SEE table with default values.
    pub fn new() -> Self {
        let mut contexts = [[SeeContext::new(0, 0); SEE_CONTEXTS]; 4];

        // Initialize with empirically-derived values from the PPMd spec.
        for ns_idx in 0..4 {
            for i in 0..SEE_CONTEXTS {
                let init_val = match ns_idx {
                    0 => 82,
                    1 => 64,
                    2 => 48,
                    _ => 36,
                };
                contexts[ns_idx][i] = SeeContext::new(init_val, 7);
            }
        }

        Self { contexts }
    }

    /// Look up a SEE context.
    ///
    /// - `num_stats`: number of distinct symbols in the current context
    /// - `suffix_order`: order of the suffix context
    /// - `flags`: context flags (bit 0 = recent escape)
    pub fn get(&mut self, num_stats: usize, suffix_order: usize, flags: u8) -> &mut SeeContext {
        let ns_idx = match num_stats {
            0..=2 => 0,
            3..=4 => 1,
            5..=8 => 2,
            _ => 3,
        };
        let ctx_idx = ((suffix_order.min(11)) * 2 + (flags & 1) as usize).min(SEE_CONTEXTS - 1);
        &mut self.contexts[ns_idx][ctx_idx]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_see_context_mean() {
        let ctx = SeeContext::new(82, 7);
        assert_eq!(ctx.mean(), 82); // 82 << 7 >> 7 = 82
    }

    #[test]
    fn test_see_context_min_mean() {
        let ctx = SeeContext::new(0, 7);
        assert_eq!(ctx.mean(), 1); // minimum is 1
    }

    #[test]
    fn test_see_table_lookup() {
        let mut table = SeeTable::new();
        let ctx = table.get(3, 2, 0);
        assert!(ctx.mean() > 0);
    }

    #[test]
    fn test_see_context_update() {
        let mut ctx = SeeContext::new(82, 7);
        let initial_mean = ctx.mean();
        ctx.update_escape();
        // After escape, sum increases so mean should be >= initial
        assert!(ctx.mean() >= initial_mean);
    }
}
