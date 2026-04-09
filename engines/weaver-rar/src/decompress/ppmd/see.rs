//! Secondary Escape Estimation (SEE) for PPMd variant H.
//!
//! SEE provides adaptive probability estimates for the "escape" symbol in
//! contexts where the PPMd model needs to fall back to a lower-order context.
//!
//! Reference: Shkarin's PPMd (public domain), 7-zip Ppmd7.c (public domain).

/// Period bits for SEE scaling.
const PERIOD_BITS: u8 = 7;

/// A single SEE context that tracks escape probability.
#[derive(Clone, Copy)]
pub struct SeeContext {
    /// Scaled sum of escape frequencies.
    pub summ: u16,
    /// Right-shift count for extracting the mean estimate.
    pub shift: u8,
    /// Count of updates before adaptation.
    pub count: u8,
}

impl SeeContext {
    /// Create a new SEE context with initial value.
    pub fn init(init_val: u16) -> Self {
        let shift = PERIOD_BITS - 4;
        Self {
            summ: init_val << shift,
            shift,
            count: 4,
        }
    }

    /// Create a zeroed SEE context (for the dummy context).
    pub fn dummy() -> Self {
        Self {
            summ: 0,
            shift: PERIOD_BITS,
            count: 0,
        }
    }

    /// Get the current escape frequency estimate (getMean in unrar).
    ///
    /// Returns the mean AND subtracts it from summ.
    /// Minimum return value is 1 to avoid zero-frequency issues.
    #[inline]
    pub fn get_mean(&mut self) -> u32 {
        // RetVal = Summ >> Shift; Summ -= RetVal; return RetVal + (RetVal == 0);
        let ret = (self.summ >> self.shift) as i16;
        self.summ = self.summ.wrapping_sub(ret as u16);
        let ret = ret as u32;
        if ret == 0 { 1 } else { ret }
    }

    /// Update after a successful symbol decode (SEE2.update in unrar).
    #[inline]
    pub fn update(&mut self) {
        if self.shift < PERIOD_BITS {
            self.count = self.count.wrapping_sub(1);
            if self.count == 0 {
                self.summ = self.summ.wrapping_add(self.summ);
                self.count = 3 << self.shift;
                self.shift += 1;
            }
        }
    }
}

/// The SEE table: 25 x 16 contexts indexed by model state.
pub struct SeeTable {
    pub contexts: [[SeeContext; 16]; 25],
    pub dummy: SeeContext,
}

impl Default for SeeTable {
    fn default() -> Self {
        Self::new()
    }
}

impl SeeTable {
    /// Initialize the SEE table matching unrar's init(5*i+10).
    pub fn new() -> Self {
        let mut contexts = [[SeeContext::init(0); 16]; 25];
        for (i, row) in contexts.iter_mut().enumerate() {
            for ctx in row.iter_mut() {
                *ctx = SeeContext::init((5 * i + 10) as u16);
            }
        }
        Self {
            contexts,
            dummy: SeeContext::dummy(),
        }
    }

    /// Look up a SEE context by index.
    ///
    /// `idx0` is NS2Indx[Diff-1] (0..24).
    /// `idx1` is the combined index from context properties (0..15).
    pub fn get(&mut self, idx0: usize, idx1: usize) -> &mut SeeContext {
        &mut self.contexts[idx0.min(24)][idx1.min(15)]
    }

    /// Get the dummy context (for NumStats == 256).
    pub fn get_dummy(&mut self) -> &mut SeeContext {
        &mut self.dummy
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_see_context_init() {
        let ctx = SeeContext::init(20);
        // shift = PERIOD_BITS - 4 = 3
        // summ = 20 << 3 = 160
        assert_eq!(ctx.summ, 160);
        assert_eq!(ctx.shift, 3);
        assert_eq!(ctx.count, 4);
    }

    #[test]
    fn test_see_get_mean() {
        let mut ctx = SeeContext::init(20);
        // summ=160, shift=3 → mean = 160 >> 3 = 20, summ = 160 - 20 = 140
        let mean = ctx.get_mean();
        assert_eq!(mean, 20);
        assert_eq!(ctx.summ, 140);
    }

    #[test]
    fn test_see_get_mean_min_one() {
        let mut ctx = SeeContext {
            summ: 0,
            shift: 3,
            count: 4,
        };
        assert_eq!(ctx.get_mean(), 1);
    }

    #[test]
    fn test_see_table_init() {
        let table = SeeTable::new();
        // contexts[0] should have init(10): summ = 10 << 3 = 80
        assert_eq!(table.contexts[0][0].summ, 80);
        // contexts[24] should have init(130): summ = 130 << 3 = 1040
        assert_eq!(table.contexts[24][0].summ, 1040);
    }

    #[test]
    fn test_see_update() {
        let mut ctx = SeeContext::init(20);
        // shift=3, count=4
        ctx.update(); // count=3
        ctx.update(); // count=2
        ctx.update(); // count=1
        ctx.update(); // count=0 → summ doubles, count = 3 << 3 = 24 (pre-increment), shift becomes 4
        assert_eq!(ctx.shift, 4);
        assert_eq!(ctx.count, 24);
    }
}
