//! GF(2^16) field arithmetic for PAR2 Reed-Solomon coding.
//!
//! PAR2 uses the primitive polynomial `x^16 + x^12 + x^3 + x + 1` (0x1100B).
//! All arithmetic is performed over the Galois field GF(2^16) = GF(65536).
//!
//! This module provides:
//! - Addition (XOR)
//! - Multiplication via log/antilog lookup tables
//! - Exponentiation
//! - Multiplicative inverse
//! - PAR2 constant assignment sequence (powers of 2 with order 65535)

use std::sync::LazyLock;

/// The primitive polynomial for PAR2's GF(2^16): x^16 + x^12 + x^3 + x + 1.
const PRIMITIVE_POLY: u32 = 0x1_100B;

/// Order of the multiplicative group: 2^16 - 1 = 65535.
const ORDER: u32 = 65535;

/// Log and antilog lookup tables for GF(2^16).
struct GfTables {
    /// log_table[a] = discrete log base g of a, for a in 1..65535.
    /// log_table[0] is unused (log of 0 is undefined).
    log_table: [u16; 65536],
    /// antilog_table[i] = g^i for i in 0..65534.
    /// antilog_table[65535] is a duplicate of antilog_table[0] for convenience.
    antilog_table: [u16; 65536],
}

static TABLES: LazyLock<GfTables> = LazyLock::new(|| {
    let mut log_table = [0u16; 65536];
    let mut antilog_table = [0u16; 65536];

    // Generator g = 2 (x) with the primitive polynomial.
    // Build antilog table: antilog[i] = 2^i mod primitive_poly
    let mut val: u32 = 1;
    for i in 0..ORDER {
        antilog_table[i as usize] = val as u16;
        log_table[val as usize] = i as u16;

        // Multiply by 2 (shift left by 1) in GF(2^16)
        val <<= 1;
        if val & 0x10000 != 0 {
            val ^= PRIMITIVE_POLY;
        }
    }

    // Convenience: antilog[65535] = antilog[0] = 1 for the inv() shortcut.
    antilog_table[ORDER as usize] = antilog_table[0];

    GfTables {
        log_table,
        antilog_table,
    }
});

/// Addition in GF(2^16) is XOR.
#[inline]
pub fn add(a: u16, b: u16) -> u16 {
    a ^ b
}

/// Multiplication in GF(2^16) via log/antilog tables.
///
/// Returns 0 if either operand is 0.
#[inline]
pub fn mul(a: u16, b: u16) -> u16 {
    if a == 0 || b == 0 {
        return 0;
    }
    let tables = &*TABLES;
    let log_a = tables.log_table[a as usize] as u32;
    let log_b = tables.log_table[b as usize] as u32;
    let mut log_sum = log_a + log_b;
    if log_sum >= ORDER {
        log_sum -= ORDER;
    }
    tables.antilog_table[log_sum as usize]
}

/// Multiplicative inverse in GF(2^16).
///
/// Returns `antilog(ORDER - log(a))`. Panics if `a == 0` (zero has no inverse).
///
/// # Panics
///
/// Panics if `a == 0`. Callers must ensure the input is nonzero.
/// In the PAR2 repair path, Gaussian elimination guarantees nonzero pivots.
#[inline]
pub fn inv(a: u16) -> u16 {
    assert!(a != 0, "cannot invert zero in GF(2^16)");
    let tables = &*TABLES;
    let log_a = tables.log_table[a as usize] as u32;
    tables.antilog_table[(ORDER - log_a) as usize]
}

/// Exponentiation in GF(2^16): `base^exp`.
///
/// Uses the log table: `base^exp = antilog(log(base) * exp mod ORDER)`.
/// Returns 0 if `base == 0` (except 0^0 which is conventionally 1 in PAR2).
#[inline]
pub fn pow(base: u16, exp: u32) -> u16 {
    if exp == 0 {
        return 1;
    }
    if base == 0 {
        return 0;
    }
    let tables = &*TABLES;
    let log_base = tables.log_table[base as usize] as u64;
    let log_result = (log_base * exp as u64) % ORDER as u64;
    tables.antilog_table[log_result as usize]
}

/// Compute the PAR2 input slice constant assignment sequence.
///
/// PAR2 assigns a 16-bit constant to each input slice. The constants are
/// successive powers of 2 whose exponent is NOT divisible by 3, 5, 17, or 257.
/// This ensures each constant has multiplicative order exactly 65535 in GF(2^16).
///
/// Returns the first `count` such constants.
pub fn input_slice_constants(count: usize) -> Vec<u16> {
    let tables = &*TABLES;
    let mut constants = Vec::with_capacity(count);
    let mut exp: u32 = 1; // Start at exponent 1 (2^1 = 2)

    while constants.len() < count {
        if !is_divisible_by_suborder_factor(exp) {
            constants.push(tables.antilog_table[exp as usize]);
        }
        exp += 1;
        // Safety: there are exactly phi(65535) = 32768 valid exponents in 1..65535
        assert!(
            exp <= ORDER,
            "exhausted all valid exponents before finding {count} constants"
        );
    }

    constants
}

/// Check if an exponent is divisible by any prime factor of 65535's proper subgroup orders.
///
/// 65535 = 3 * 5 * 17 * 257.
/// A power of 2 has order 65535 iff its exponent is NOT divisible by any of these factors.
#[inline]
fn is_divisible_by_suborder_factor(exp: u32) -> bool {
    exp.is_multiple_of(3)
        || exp.is_multiple_of(5)
        || exp.is_multiple_of(17)
        || exp.is_multiple_of(257)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tables_initialized() {
        // antilog[0] should be 1 (2^0 = 1)
        assert_eq!(TABLES.antilog_table[0], 1);
        // antilog[1] should be 2 (2^1 = 2)
        assert_eq!(TABLES.antilog_table[1], 2);
        // log[1] should be 0 (since 2^0 = 1)
        assert_eq!(TABLES.log_table[1], 0);
        // log[2] should be 1 (since 2^1 = 2)
        assert_eq!(TABLES.log_table[2], 1);
    }

    #[test]
    fn add_is_xor() {
        assert_eq!(add(0x1234, 0x5678), 0x1234 ^ 0x5678);
        assert_eq!(add(0, 42), 42);
        assert_eq!(add(42, 42), 0); // a + a = 0 in GF(2^n)
    }

    #[test]
    fn mul_identity() {
        // 1 * a = a
        for a in [0u16, 1, 2, 100, 0xFFFF, 0x8000] {
            assert_eq!(mul(1, a), a, "1 * {a} should be {a}");
            assert_eq!(mul(a, 1), a, "{a} * 1 should be {a}");
        }
    }

    #[test]
    fn mul_zero() {
        assert_eq!(mul(0, 0), 0);
        assert_eq!(mul(0, 12345), 0);
        assert_eq!(mul(12345, 0), 0);
    }

    #[test]
    fn mul_commutativity() {
        let pairs = [(2, 3), (100, 200), (0x1234, 0x5678), (0xFFFF, 0x8000)];
        for (a, b) in pairs {
            assert_eq!(mul(a, b), mul(b, a), "mul({a}, {b}) should be commutative");
        }
    }

    #[test]
    fn mul_associativity() {
        let a = 0x1234u16;
        let b = 0x5678u16;
        let c = 0x9ABCu16;
        assert_eq!(mul(mul(a, b), c), mul(a, mul(b, c)));
    }

    #[test]
    fn inv_identity() {
        // a * inv(a) = 1 for all nonzero a
        for a in [1u16, 2, 3, 100, 0x1234, 0xFFFF, 0x8000] {
            assert_eq!(mul(a, inv(a)), 1, "a * inv(a) should be 1 for a={a}");
        }
    }

    #[test]
    fn inv_exhaustive_sample() {
        // Test inv for a wider range
        for a in 1u16..=1000 {
            assert_eq!(mul(a, inv(a)), 1, "a * inv(a) != 1 for a={a}");
        }
    }

    #[test]
    #[should_panic(expected = "cannot invert zero")]
    fn inv_zero_panics() {
        inv(0);
    }

    #[test]
    fn pow_basic() {
        assert_eq!(pow(2, 0), 1);
        assert_eq!(pow(0, 0), 1); // convention
        assert_eq!(pow(0, 5), 0);
        assert_eq!(pow(2, 1), 2);

        // 2^16 in GF(2^16) with poly 0x1100B:
        // 2^16 = x^16 = x^12 + x^3 + x + 1 = 0x100B
        assert_eq!(pow(2, 16), 0x100B);
    }

    #[test]
    fn pow_matches_repeated_mul() {
        let base = 0x1234u16;
        let mut product = 1u16;
        for exp in 0..20u32 {
            assert_eq!(pow(base, exp), product, "pow({base:#x}, {exp}) mismatch");
            product = mul(product, base);
        }
    }

    #[test]
    fn pow_fermats_little_theorem() {
        // a^65535 = 1 for all nonzero a (Fermat's little theorem in GF(2^16))
        for a in [2u16, 3, 100, 0x1234, 0xFFFF] {
            assert_eq!(pow(a, ORDER), 1, "a^65535 should be 1 for a={a}");
        }
    }

    #[test]
    fn constant_sequence_first_few() {
        let constants = input_slice_constants(10);
        assert_eq!(constants.len(), 10);

        // First valid exponent is 1 (not divisible by 3, 5, 17, or 257).
        // constant[0] = 2^1 = 2
        assert_eq!(constants[0], 2);

        // Second valid exponent is 2 (not div by 3,5,17,257).
        // constant[1] = 2^2 = 4
        assert_eq!(constants[1], 4);

        // Third valid exponent is 4 (3 is div by 3, so skip).
        // constant[2] = 2^4 = 16
        assert_eq!(constants[2], 16);

        // Verify all constants have order exactly 65535
        for (i, &c) in constants.iter().enumerate() {
            assert_eq!(pow(c, ORDER), 1, "constant[{i}] should have a^65535 = 1");
            // Check it does NOT have a smaller order dividing 65535
            for &factor in &[3u32, 5, 17, 257] {
                let suborder = ORDER / factor;
                assert_ne!(
                    pow(c, suborder),
                    1,
                    "constant[{i}] = {c} has suborder {suborder} (factor {factor})"
                );
            }
        }
    }

    #[test]
    fn constants_are_unique() {
        let constants = input_slice_constants(100);
        let mut seen = std::collections::HashSet::new();
        for &c in &constants {
            assert!(seen.insert(c), "duplicate constant: {c}");
        }
    }

    #[test]
    fn distributive_law() {
        let a = 0x1234u16;
        let b = 0x5678u16;
        let c = 0x9ABCu16;
        // a * (b + c) = a*b + a*c
        assert_eq!(mul(a, add(b, c)), add(mul(a, b), mul(a, c)));
    }

    #[test]
    fn generator_has_full_order() {
        // 2 should be a primitive element with order 65535
        assert_ne!(pow(2, 1), 1);
        assert_ne!(pow(2, 3), 1);
        assert_ne!(pow(2, 5), 1);
        assert_ne!(pow(2, 15), 1);
        assert_eq!(pow(2, ORDER), 1);
    }
}
