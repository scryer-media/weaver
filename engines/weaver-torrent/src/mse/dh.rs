// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

//! Diffie-Hellman over the encrypted handshake's fixed 768-bit prime, generator 2.
//!
//! Fixed-width Montgomery arithmetic for this one modulus: 12 little-endian
//! 64-bit limbs.

use std::sync::LazyLock;

/// Length of a public key and of the shared secret, big-endian, zero-padded.
pub(crate) const KEY_LEN: usize = 96;
/// Length of a private key: 160 random bits.
pub(crate) const PRIVATE_KEY_LEN: usize = 20;

const LIMBS: usize = 12;
type U768 = [u64; LIMBS];

/// The prime P from the protocol specification.
pub(crate) const PRIME_HEX: &str = "FFFFFFFFFFFFFFFFC90FDAA22168C234C4C6628B80DC1CD129024E088A67CC74020BBEA63B139B22514A08798E3404DDEF9519B3CD3A431B302B0A6DF25F14374FE1356D6D51C245E485B576625E7EC6F44C42E9A63A36210000000000090563";

struct Modulus {
    p: U768,
    /// -p^-1 mod 2^64.
    n0: u64,
    /// R^2 mod p, R = 2^768.
    r2: U768,
    /// R mod p: one, in Montgomery form.
    one_m: U768,
}

static MODULUS: LazyLock<Modulus> = LazyLock::new(|| {
    let mut be = [0u8; KEY_LEN];
    hex::decode_to_slice(PRIME_HEX, &mut be).expect("the prime constant is valid hex");
    let p = from_be(&be);

    let mut inv: u64 = 1;
    for _ in 0..6 {
        inv = inv.wrapping_mul(2u64.wrapping_sub(p[0].wrapping_mul(inv)));
    }
    let n0 = inv.wrapping_neg();

    // 2^1536 mod p by doubling 1 that many times.
    let mut r2 = [0u64; LIMBS];
    r2[0] = 1;
    for _ in 0..(2 * 64 * LIMBS) {
        let carry = shl1(&mut r2);
        if carry || !lt(&r2, &p) {
            sub_in_place(&mut r2, &p);
        }
    }
    let mut one = [0u64; LIMBS];
    one[0] = 1;
    let one_m = mont_mul(&one, &r2, &p, n0);
    Modulus { p, n0, r2, one_m }
});

fn from_be(be: &[u8; KEY_LEN]) -> U768 {
    let mut out = [0u64; LIMBS];
    for (i, limb) in out.iter_mut().enumerate() {
        let end = KEY_LEN - i * 8;
        let mut b = [0u8; 8];
        b.copy_from_slice(&be[end - 8..end]);
        *limb = u64::from_be_bytes(b);
    }
    out
}

fn to_be(x: &U768) -> [u8; KEY_LEN] {
    let mut out = [0u8; KEY_LEN];
    for (i, limb) in x.iter().enumerate() {
        let end = KEY_LEN - i * 8;
        out[end - 8..end].copy_from_slice(&limb.to_be_bytes());
    }
    out
}

/// a < b
fn lt(a: &U768, b: &U768) -> bool {
    for i in (0..LIMBS).rev() {
        if a[i] != b[i] {
            return a[i] < b[i];
        }
    }
    false
}

/// a -= b, wrapping; returns the borrow.
fn sub_in_place(a: &mut U768, b: &U768) -> bool {
    let mut borrow = false;
    for i in 0..LIMBS {
        let (d1, b1) = a[i].overflowing_sub(b[i]);
        let (d2, b2) = d1.overflowing_sub(u64::from(borrow));
        a[i] = d2;
        borrow = b1 || b2;
    }
    borrow
}

/// a <<= 1; returns the bit shifted out.
fn shl1(a: &mut U768) -> bool {
    let mut carry = 0u64;
    for limb in a.iter_mut() {
        let next = *limb >> 63;
        *limb = (*limb << 1) | carry;
        carry = next;
    }
    carry != 0
}

/// Montgomery product a * b * R^-1 mod p, for a, b < p.
#[allow(clippy::cast_possible_truncation)]
fn mont_mul(a: &U768, b: &U768, p: &U768, n0: u64) -> U768 {
    let mut t = [0u64; LIMBS + 2];
    for &bi in b.iter() {
        let mut c: u64 = 0;
        for j in 0..LIMBS {
            let x = u128::from(t[j]) + u128::from(a[j]) * u128::from(bi) + u128::from(c);
            t[j] = x as u64;
            c = (x >> 64) as u64;
        }
        let x = u128::from(t[LIMBS]) + u128::from(c);
        t[LIMBS] = x as u64;
        t[LIMBS + 1] = (x >> 64) as u64;

        let m = t[0].wrapping_mul(n0);
        let x = u128::from(t[0]) + u128::from(m) * u128::from(p[0]);
        let mut c = (x >> 64) as u64;
        for j in 1..LIMBS {
            let x = u128::from(t[j]) + u128::from(m) * u128::from(p[j]) + u128::from(c);
            t[j - 1] = x as u64;
            c = (x >> 64) as u64;
        }
        let x = u128::from(t[LIMBS]) + u128::from(c);
        t[LIMBS - 1] = x as u64;
        t[LIMBS] = t[LIMBS + 1] + ((x >> 64) as u64);
        t[LIMBS + 1] = 0;
    }
    let mut out = [0u64; LIMBS];
    out.copy_from_slice(&t[..LIMBS]);
    if t[LIMBS] != 0 || !lt(&out, p) {
        sub_in_place(&mut out, p);
    }
    out
}

/// base^exp mod p, for base < p; `exp` is big-endian of any length.
fn modexp(base: &U768, exp: &[u8]) -> U768 {
    let m = &*MODULUS;
    let base_m = mont_mul(base, &m.r2, &m.p, m.n0);
    let mut acc = m.one_m;
    for byte in exp {
        for bit in (0..8).rev() {
            acc = mont_mul(&acc, &acc, &m.p, m.n0);
            if (byte >> bit) & 1 == 1 {
                acc = mont_mul(&acc, &base_m, &m.p, m.n0);
            }
        }
    }
    let mut one = [0u64; LIMBS];
    one[0] = 1;
    mont_mul(&acc, &one, &m.p, m.n0)
}

pub(crate) fn random_private_key() -> [u8; PRIVATE_KEY_LEN] {
    rand::random()
}

/// 2^x mod p.
pub(crate) fn public_key(private: &[u8; PRIVATE_KEY_LEN]) -> [u8; KEY_LEN] {
    let mut two = [0u64; LIMBS];
    two[0] = 2;
    to_be(&modexp(&two, private))
}

/// y^x mod p. `None` if the peer's key is not in 2..p-1, which would make the
/// secret predictable.
pub(crate) fn shared_secret(
    private: &[u8; PRIVATE_KEY_LEN],
    peer_public: &[u8; KEY_LEN],
) -> Option<[u8; KEY_LEN]> {
    let p = &MODULUS.p;
    let y = from_be(peer_public);
    let mut p_minus_1 = *p;
    p_minus_1[0] -= 1;
    let mut two = [0u64; LIMBS];
    two[0] = 2;
    if lt(&y, &two) || !lt(&y, &p_minus_1) {
        return None;
    }
    Some(to_be(&modexp(&y, private)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn small(v: u64) -> U768 {
        let mut x = [0u64; LIMBS];
        x[0] = v;
        x
    }

    #[test]
    fn test_prime_constant_is_768_bits_and_odd() {
        assert_eq!(PRIME_HEX.len(), KEY_LEN * 2);
        let p = MODULUS.p;
        assert_eq!(p[LIMBS - 1], u64::MAX);
        assert_eq!(p[0] & 1, 1);
        assert_eq!(
            p[0].wrapping_mul(MODULUS.n0),
            u64::MAX,
            "n0 = -p^-1 mod 2^64"
        );
        assert_eq!(hex::encode_upper(to_be(&p)), PRIME_HEX);
    }

    #[test]
    fn test_small_powers() {
        assert_eq!(modexp(&small(2), &[10]), small(1024));
        assert_eq!(modexp(&small(3), &[0]), small(1));
        assert_eq!(modexp(&small(7), &[1]), small(7));
        // 2^256: bit 0 of limb 4.
        let mut two_256 = [0u64; LIMBS];
        two_256[4] = 1;
        assert_eq!(modexp(&small(2), &[1, 0]), two_256);
    }

    #[test]
    fn test_two_to_the_768_is_r_mod_p() {
        // p > 2^767, so 2^768 mod p = 2^768 - p, the two's complement of p in 768 bits.
        let mut expected = [0u64; LIMBS];
        sub_in_place(&mut expected, &MODULUS.p);
        let exp768 = [0x03u8, 0x00]; // 768 = 0x0300
        assert_eq!(modexp(&small(2), &exp768), expected);
        assert_eq!(MODULUS.one_m, expected);
    }

    #[test]
    fn test_fermat_little_theorem_holds_for_the_prime() {
        // a^(p-1) = 1 mod p for a prime p. A mistyped constant fails this.
        let mut p_minus_1 = MODULUS.p;
        p_minus_1[0] -= 1;
        let e = to_be(&p_minus_1);
        for a in [2u64, 3, 5, 0xdead_beef] {
            assert_eq!(modexp(&small(a), &e), small(1), "a={a}");
        }
    }

    #[test]
    fn test_both_sides_agree_on_the_secret() {
        for _ in 0..4 {
            let xa = random_private_key();
            let xb = random_private_key();
            let ya = public_key(&xa);
            let yb = public_key(&xb);
            let sa = shared_secret(&xa, &yb).unwrap();
            let sb = shared_secret(&xb, &ya).unwrap();
            assert_eq!(sa, sb);
            assert_ne!(ya, yb);
        }
    }

    #[test]
    fn test_degenerate_public_keys_are_refused() {
        let x = random_private_key();
        let mut k = [0u8; KEY_LEN];
        assert!(shared_secret(&x, &k).is_none());
        k[KEY_LEN - 1] = 1;
        assert!(shared_secret(&x, &k).is_none());
        let mut pm1 = MODULUS.p;
        pm1[0] -= 1;
        assert!(shared_secret(&x, &to_be(&pm1)).is_none());
        assert!(shared_secret(&x, &to_be(&MODULUS.p)).is_none());
        assert!(shared_secret(&x, &[0xff; KEY_LEN]).is_none());
        k[KEY_LEN - 1] = 2;
        assert!(shared_secret(&x, &k).is_some());
    }
}
