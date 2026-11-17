// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

//! The RC4 stream cipher, as the encrypted peer handshake uses it.

/// One entry of the state. On x86-64 each entry has a 32-bit word of its
/// own, so a read of one entry never shares a word with a recent store to a
/// neighbour; AMD cores hold such a read back until the store is done.
/// Elsewhere a byte.
#[cfg(target_arch = "x86_64")]
type Cell = u32;
#[cfg(not(target_arch = "x86_64"))]
type Cell = u8;

/// RC4 state. Encryption and decryption are the same operation.
#[derive(Clone)]
pub(crate) struct Rc4 {
    s: [Cell; 256],
    i: u8,
    j: u8,
    /// Keystream bytes produced, shared with every clone.
    #[cfg(test)]
    ran: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

impl Rc4 {
    pub fn new(key: &[u8]) -> Self {
        assert!(
            !key.is_empty() && key.len() <= 256,
            "RC4 key must be 1..=256 bytes"
        );
        let mut s = [0; 256];
        for (v, i) in s.iter_mut().zip(0..=255u8) {
            *v = Cell::from(i);
        }
        let mut j = 0u8;
        for (i, &k) in (0..256).zip(key.iter().cycle()) {
            j = j.wrapping_add(low(s[i])).wrapping_add(k);
            s.swap(i, usize::from(j));
        }
        Self {
            s,
            i: 0,
            j: 0,
            #[cfg(test)]
            ran: Default::default(),
        }
    }

    /// The handshake's variant: the first 1024 bytes of keystream are thrown away.
    pub fn new_discard_1024(key: &[u8]) -> Self {
        let mut c = Self::new(key);
        c.skip(1024);
        c
    }

    /// XOR the keystream into `buf`.
    pub fn apply(&mut self, buf: &mut [u8]) {
        #[cfg(test)]
        self.count(buf.len());
        #[cfg(target_arch = "x86_64")]
        if std::arch::is_x86_feature_detected!("sse4.1") {
            // SAFETY: the CPU has SSE4.1, checked just above.
            unsafe { self.apply_ahead(buf) };
            return;
        }
        #[cfg(target_arch = "aarch64")]
        self.apply_ahead(buf);
        #[cfg(not(target_arch = "aarch64"))]
        self.apply_words(buf);
    }

    /// Eight keystream bytes are gathered into a word and XORed into the
    /// buffer at once, so each byte costs the two state stores and no store
    /// of its own (libtorrent `rc4_encrypt`, src/pe_crypto.cpp, RC_2_1).
    #[cfg(any(not(target_arch = "aarch64"), test))]
    fn apply_words(&mut self, buf: &mut [u8]) {
        let (mut i, mut j) = (self.i, usize::from(self.j));
        let s = &mut self.s;
        let (words, tail) = buf.as_chunks_mut::<8>();
        for word in words {
            let mut k = 0u64;
            for n in 0..8 {
                k |= u64::from(step(s, &mut i, &mut j)) << (8 * n);
            }
            *word = (u64::from_le_bytes(*word) ^ k).to_le_bytes();
        }
        for b in tail {
            *b ^= step(s, &mut i, &mut j);
        }
        self.i = i;
        self.j = j.to_le_bytes()[0];
    }

    /// Sixteen bytes a round, with the state entries for `i` read three steps
    /// ahead of their use.
    ///
    /// The next `j` is the last one plus `s[i + 1]`. Read after the store to
    /// `s[j]`, as the textbook loop has it, that entry may be the one just
    /// stored, and Intel's larger cores then hold the read back until the
    /// store's address is known: one load and one add per byte in a row,
    /// about 8 cycles. Here `s[i + 3]` is read before the step's two stores,
    /// so it never waits on them, and three steps leave the read time to land
    /// on AMD's cores too; a store that lands on one of the three entries
    /// read ahead (`j` is `i + 1` to `i + 3`, 3 times in 256) takes a cold
    /// path that reads them again. That test runs on a counter of its own,
    /// `j` less the round's first `i`, one add a step, so it never lengthens
    /// the chain from one `j` to the next. The round starts where `i + 1` is
    /// a multiple of 16, so every `i` offset is a constant, and the sixteen
    /// keystream bytes are gathered in one SSE register and XORed in at once.
    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "sse4.1")]
    fn apply_ahead(&mut self, buf: &mut [u8]) {
        use core::arch::x86_64::{
            _mm_cvtsi128_si64, _mm_extract_epi64, _mm_insert_epi8, _mm_setzero_si128,
        };
        let mut at = 0;
        while at < buf.len() && !self.i.wrapping_add(1).is_multiple_of(16) {
            let mut j = usize::from(self.j);
            buf[at] ^= step(&mut self.s, &mut self.i, &mut j);
            self.j = j.to_le_bytes()[0];
            at += 1;
        }
        let (rounds, tail) = buf[at..].as_chunks_mut::<16>();
        if !rounds.is_empty() {
            let s = &mut self.s;
            let mut j = self.j;
            // The round's first i, and the entries for it and the next two.
            let mut b = usize::from(self.i.wrapping_add(1)) & 0xf0;
            let mut t1 = s[b];
            let mut t2 = s[b + 1];
            let mut t3 = s[b + 2];
            // `j` less `b`; only its low byte is read.
            let mut d = u32::from(j.wrapping_sub(b.to_le_bytes()[0]));
            for round in rounds {
                let nb = (b + 16) & 0xf0;
                // Entry `n` of the round, which runs into the next.
                let at = |n: usize| if n < 16 { b + n } else { nb + n - 16 };
                let mut v = _mm_setzero_si128();
                macro_rules! step {
                    ($n:literal) => {
                        let tx = t1;
                        j = j.wrapping_add(low(tx));
                        let jj = usize::from(j);
                        let ty = s[jj];
                        let ahead = s[at($n + 3)];
                        s[jj] = tx;
                        s[b + $n] = ty;
                        t1 = t2;
                        t2 = t3;
                        t3 = ahead;
                        d = d.wrapping_add(tx);
                        if d.wrapping_sub($n + 1).to_le_bytes()[0] < 3 {
                            std::hint::cold_path();
                            t1 = s[at($n + 1)];
                            t2 = s[at($n + 2)];
                            t3 = s[at($n + 3)];
                        }
                        let k = low(s[(tx as usize + ty as usize) & 0xff]);
                        v = _mm_insert_epi8::<$n>(v, i32::from(k));
                    };
                }
                step!(0);
                step!(1);
                step!(2);
                step!(3);
                step!(4);
                step!(5);
                step!(6);
                step!(7);
                step!(8);
                step!(9);
                step!(10);
                step!(11);
                step!(12);
                step!(13);
                step!(14);
                step!(15);
                let (lo, hi) = round.split_at_mut(8);
                let lo: &mut [u8; 8] = lo.try_into().unwrap();
                let hi: &mut [u8; 8] = hi.try_into().unwrap();
                *lo = (u64::from_le_bytes(*lo) ^ _mm_cvtsi128_si64(v) as u64).to_le_bytes();
                *hi = (u64::from_le_bytes(*hi) ^ _mm_extract_epi64::<1>(v) as u64).to_le_bytes();
                d = d.wrapping_sub(16);
                b = nb;
            }
            self.i = b.to_le_bytes()[0].wrapping_sub(1);
            self.j = j;
        }
        let mut j = usize::from(self.j);
        for t in tail {
            *t ^= step(&mut self.s, &mut self.i, &mut j);
        }
        self.j = j.to_le_bytes()[0];
    }

    /// The same idea on aarch64, in plain Rust: sixteen bytes a round, each
    /// entry for `i` read ahead of its use, before the stores of the step
    /// that reads it.
    ///
    /// Arm's Neoverse cores run a read of `s[i + 1]` before an older store
    /// whose address is not known yet, and start again when the two meet,
    /// so the textbook loop throws work away. Here the entries are read in
    /// pairs: the odd step of each pair reads the next two, `s[i + 1]` and
    /// `s[i + 2]`, and the even step reads none. A store that lands on an
    /// entry still to be used takes a cold path that reads it again; the
    /// pair is aligned, so the odd step tests `j` against both entries with
    /// one mask, and the even step has one entry left to test.
    ///
    /// Each of the sixteen rounds of the table is its own copy, so every `i`
    /// in a test, a read or a store is a constant. Whole passes over the
    /// table run the sixteen copies in order with no dispatch between them;
    /// a call's first rounds up to entry 0, and its last after the final
    /// whole pass, pick their copy by the round's first `i`. The state stays
    /// a byte per entry, which every Arm core measured prefers, and the
    /// sixteen keystream bytes are gathered into two words by shift and or.
    #[cfg(target_arch = "aarch64")]
    fn apply_ahead(&mut self, buf: &mut [u8]) {
        let mut at = 0;
        while at < buf.len() && !self.i.wrapping_add(1).is_multiple_of(16) {
            let mut j = usize::from(self.j);
            buf[at] ^= step(&mut self.s, &mut self.i, &mut j);
            self.j = j.to_le_bytes()[0];
            at += 1;
        }
        let (rounds, tail) = buf[at..].as_chunks_mut::<16>();
        if !rounds.is_empty() {
            let s = &mut self.s;
            let mut j = self.j;
            // The round's first i, and the entries for it and the next.
            let mut b = usize::from(self.i.wrapping_add(1)) & 0xf0;
            let mut t = (low(s[b]), low(s[b + 1]));
            let mut rest = rounds;
            while b != 0 {
                let Some((r, more)) = std::mem::take(&mut rest).split_first_mut() else {
                    break;
                };
                round_at(s, &mut j, &mut t, r, b);
                b = (b + 16) & 0xf0;
                rest = more;
            }
            let (passes, rest) = rest.as_chunks_mut::<16>();
            for p in passes {
                round::<0>(s, &mut j, &mut t, &mut p[0]);
                round::<16>(s, &mut j, &mut t, &mut p[1]);
                round::<32>(s, &mut j, &mut t, &mut p[2]);
                round::<48>(s, &mut j, &mut t, &mut p[3]);
                round::<64>(s, &mut j, &mut t, &mut p[4]);
                round::<80>(s, &mut j, &mut t, &mut p[5]);
                round::<96>(s, &mut j, &mut t, &mut p[6]);
                round::<112>(s, &mut j, &mut t, &mut p[7]);
                round::<128>(s, &mut j, &mut t, &mut p[8]);
                round::<144>(s, &mut j, &mut t, &mut p[9]);
                round::<160>(s, &mut j, &mut t, &mut p[10]);
                round::<176>(s, &mut j, &mut t, &mut p[11]);
                round::<192>(s, &mut j, &mut t, &mut p[12]);
                round::<208>(s, &mut j, &mut t, &mut p[13]);
                round::<224>(s, &mut j, &mut t, &mut p[14]);
                round::<240>(s, &mut j, &mut t, &mut p[15]);
            }
            for r in rest {
                round_at(s, &mut j, &mut t, r, b);
                b = (b + 16) & 0xf0;
            }
            self.i = b.to_le_bytes()[0].wrapping_sub(1);
            self.j = j;
        }
        let mut j = usize::from(self.j);
        for t in tail {
            *t ^= step(&mut self.s, &mut self.i, &mut j);
        }
        self.j = j.to_le_bytes()[0];
    }

    /// Advance the keystream by `n` bytes.
    pub fn skip(&mut self, n: usize) {
        #[cfg(test)]
        self.count(n);
        let (mut i, mut j) = (self.i, usize::from(self.j));
        for _ in 0..n {
            step(&mut self.s, &mut i, &mut j);
        }
        self.i = i;
        self.j = j.to_le_bytes()[0];
    }
}

/// The round whose first `i` is `b`, a multiple of 16.
#[cfg(target_arch = "aarch64")]
#[inline(always)]
fn round_at(s: &mut [Cell; 256], j: &mut u8, t: &mut (u8, u8), r: &mut [u8; 16], b: usize) {
    match b {
        0 => round::<0>(s, j, t, r),
        16 => round::<16>(s, j, t, r),
        32 => round::<32>(s, j, t, r),
        48 => round::<48>(s, j, t, r),
        64 => round::<64>(s, j, t, r),
        80 => round::<80>(s, j, t, r),
        96 => round::<96>(s, j, t, r),
        112 => round::<112>(s, j, t, r),
        128 => round::<128>(s, j, t, r),
        144 => round::<144>(s, j, t, r),
        160 => round::<160>(s, j, t, r),
        176 => round::<176>(s, j, t, r),
        192 => round::<192>(s, j, t, r),
        208 => round::<208>(s, j, t, r),
        224 => round::<224>(s, j, t, r),
        240 => round::<240>(s, j, t, r),
        _ => unreachable!("a round starts at a multiple of 16"),
    }
}

/// Sixteen keystream bytes from entry `B`, XORed into `r`. `t` holds the
/// entries for `i` at `B` and `B + 1` on entry, and for the next round's
/// first two on return.
#[cfg(target_arch = "aarch64")]
#[inline(always)]
fn round<const B: usize>(s: &mut [Cell; 256], j: &mut u8, t: &mut (u8, u8), r: &mut [u8; 16]) {
    let (mut t1, mut t2) = *t;
    let mut jv = *j;
    let mut k = [0u64; 2];
    macro_rules! pair {
        ($n:literal) => {
            // Even step: the entry for the odd step is already held.
            let tx = t1;
            jv = jv.wrapping_add(tx);
            let jj = usize::from(jv);
            let ty = low(s[jj]);
            s[B + $n] = ty;
            s[jj] = tx;
            if jj == B + $n + 1 {
                std::hint::cold_path();
                t2 = low(s[B + $n + 1]);
            }
            t1 = t2;
            let byte = low(s[(usize::from(tx) + usize::from(ty)) & 0xff]);
            k[$n / 8] |= u64::from(byte) << (8 * ($n % 8));
            // Odd step: the next pair is read before this step's stores.
            let tx = t1;
            jv = jv.wrapping_add(tx);
            let jj = usize::from(jv);
            let ty = low(s[jj]);
            t1 = low(s[(B + $n + 2) & 0xff]);
            t2 = low(s[(B + $n + 3) & 0xff]);
            s[B + $n + 1] = ty;
            s[jj] = tx;
            if jj & 0xfe == (B + $n + 2) & 0xff {
                std::hint::cold_path();
                t1 = low(s[(B + $n + 2) & 0xff]);
                t2 = low(s[(B + $n + 3) & 0xff]);
            }
            let byte = low(s[(usize::from(tx) + usize::from(ty)) & 0xff]);
            k[($n + 1) / 8] |= u64::from(byte) << (8 * (($n + 1) % 8));
        };
    }
    pair!(0);
    pair!(2);
    pair!(4);
    pair!(6);
    pair!(8);
    pair!(10);
    pair!(12);
    pair!(14);
    let (lo, hi) = r.split_at_mut(8);
    let lo: &mut [u8; 8] = lo.try_into().unwrap();
    let hi: &mut [u8; 8] = hi.try_into().unwrap();
    *lo = (u64::from_le_bytes(*lo) ^ k[0]).to_le_bytes();
    *hi = (u64::from_le_bytes(*hi) ^ k[1]).to_le_bytes();
    *t = (t1, t2);
    *j = jv;
}

/// One keystream byte. Only the low byte of `j` is RC4's `j`: it is carried
/// unmasked so that the dependency from one byte to the next is a single add,
/// with the mask taken off that chain where `j` indexes the state.
#[inline(always)]
fn step(s: &mut [Cell; 256], i: &mut u8, j: &mut usize) -> u8 {
    *i = i.wrapping_add(1);
    let ii = usize::from(*i);
    let ti = s[ii];
    *j = j.wrapping_add(ti as usize);
    let jj = *j & 0xff;
    let tj = s[jj];
    s[ii] = tj;
    s[jj] = ti;
    low(s[(ti as usize + tj as usize) & 0xff])
}

/// An entry's value, which is always below 256.
#[inline(always)]
fn low(v: Cell) -> u8 {
    v.to_le_bytes()[0]
}

#[cfg(test)]
impl Rc4 {
    fn count(&self, n: usize) {
        self.ran
            .fetch_add(n as u64, std::sync::atomic::Ordering::Relaxed);
    }

    /// Keystream bytes this cipher and its clones have produced, the
    /// handshake's discard included.
    pub fn keystream_bytes(&self) -> u64 {
        self.ran.load(std::sync::atomic::Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::Rc4;

    fn encrypt(key: &[u8], plaintext: &[u8]) -> Vec<u8> {
        let mut buf = plaintext.to_vec();
        Rc4::new(key).apply(&mut buf);
        buf
    }

    #[test]
    fn test_rc4_vectors() {
        assert_eq!(
            hex::encode(encrypt(b"Key", b"Plaintext")),
            "bbf316e8d940af0ad3"
        );
        assert_eq!(hex::encode(encrypt(b"Wiki", b"pedia")), "1021bf0420");
        assert_eq!(
            hex::encode(encrypt(b"Secret", b"Attack at dawn")),
            "45a01f645fc35b383552544b9bf5"
        );
    }

    #[test]
    fn test_rc4_keystream_vector() {
        // The keystream of key "Key" is the ciphertext of zeros.
        assert_eq!(
            hex::encode(encrypt(b"Key", &[0u8; 10])),
            "eb9f7781b734ca72a719"
        );
    }

    #[test]
    fn test_rc4_discard_1024() {
        let full = encrypt(b"Key", &[0u8; 1024 + 64]);
        let mut tail = [0u8; 64];
        Rc4::new_discard_1024(b"Key").apply(&mut tail);
        assert_eq!(&tail[..], &full[1024..]);
        assert_ne!(&tail[..], &full[..64]);
    }

    #[test]
    fn test_rc4_is_its_own_inverse_across_split_calls() {
        let plaintext: Vec<u8> = (0..=255u8).cycle().take(3000).collect();
        let mut buf = plaintext.clone();
        let mut enc = Rc4::new_discard_1024(b"some twenty byte key");
        let (a, b) = buf.split_at_mut(1234);
        enc.apply(a);
        enc.apply(b);
        assert_ne!(buf, plaintext);
        Rc4::new_discard_1024(b"some twenty byte key").apply(&mut buf);
        assert_eq!(buf, plaintext);
    }

    /// RFC 6229 keystream blocks: (key, offset, bytes). The offset-0 blocks
    /// are those FFmpeg checks (libavutil/tests/rc4.c), the offset-1024 and
    /// 4096 blocks those libtorrent checks (test/test_pe_crypto.cpp, RC_2_1).
    const RFC6229: &[(&str, usize, &str)] = &[
        ("0102030405", 0, "b2396305f03dc027"),
        ("01020304050607", 0, "293f02d47f37c9b6"),
        ("0102030405060708", 0, "97ab8a1bf0afb961"),
        ("0102030405060708090a0b0c0d0e0f10", 0, "9ac7cc9a609d1ef7"),
        ("0102030405", 1024, "30abbcc7c20b01609f23ee2d5f6bb7df"),
        ("0102030405", 4096, "ff25b58995996707e51fbdf08b34d875"),
        (
            "0102030405060708090a0b0c0d0e0f10",
            1024,
            "bdf0324e6083dcc6d3cedd3ca8c53c16",
        ),
        (
            "0102030405060708090a0b0c0d0e0f10",
            4096,
            "a36a4c301ae8ac13610ccbc12256cacc",
        ),
        (
            "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20",
            1024,
            "7fec5bfd9f9b89ce6548309092d7e958",
        ),
        (
            "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20",
            4096,
            "f3e4c0a2e02d1d01f7f0a74618af2b48",
        ),
    ];

    #[test]
    fn rc4_matches_rfc6229_keystream_blocks() {
        for &(key, offset, block) in RFC6229 {
            let key = hex::decode(key).unwrap();
            let len = block.len() / 2;
            // One call over everything up to the block.
            let whole = encrypt(&key, &vec![0u8; offset + len]);
            assert_eq!(
                hex::encode(&whole[offset..]),
                block,
                "key {key:02x?} at {offset}"
            );
            // Skipped to the block, then odd-sized calls across it.
            let mut c = Rc4::new(&key);
            c.skip(offset);
            let mut got = vec![0u8; len];
            let (a, b) = got.split_at_mut(3);
            c.apply(a);
            c.apply(b);
            assert_eq!(
                hex::encode(&got),
                block,
                "key {key:02x?} at {offset}, skipped"
            );
            // The handshake's cipher starts at offset 1024.
            if offset >= 1024 {
                let mut c = Rc4::new_discard_1024(&key);
                let mut got = vec![0u8; offset - 1024 + len];
                c.apply(&mut got);
                assert_eq!(hex::encode(&got[offset - 1024..]), block);
            }
        }
    }

    /// RC4 one byte at a time, written straight from the cipher's
    /// definition, for the word-at-a-time cipher to be held against.
    struct Reference {
        s: [u8; 256],
        i: u8,
        j: u8,
    }

    impl Reference {
        fn new(key: &[u8]) -> Self {
            let mut s = [0u8; 256];
            for (v, n) in s.iter_mut().zip(0..=255u8) {
                *v = n;
            }
            let mut j = 0u8;
            for i in 0..256 {
                j = j.wrapping_add(s[i]).wrapping_add(key[i % key.len()]);
                s.swap(i, j as usize);
            }
            Self { s, i: 0, j: 0 }
        }

        fn apply(&mut self, buf: &mut [u8]) {
            for b in buf.iter_mut() {
                self.i = self.i.wrapping_add(1);
                self.j = self.j.wrapping_add(self.s[self.i as usize]);
                self.s.swap(self.i as usize, self.j as usize);
                let t = self.s[self.i as usize].wrapping_add(self.s[self.j as usize]);
                *b ^= self.s[t as usize];
            }
        }
    }

    /// splitmix64, so every run draws the same keys, buffers and cuts.
    struct Seeded(u64);

    impl Seeded {
        fn next(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            z ^ (z >> 31)
        }

        fn below(&mut self, n: usize) -> usize {
            usize::try_from(self.next() % u64::try_from(n).unwrap()).unwrap()
        }

        fn bytes(&mut self, len: usize) -> Vec<u8> {
            (0..len).map(|_| self.next().to_le_bytes()[0]).collect()
        }
    }

    fn assert_same_state(c: &Rc4, r: &Reference, what: &str) {
        assert_eq!(c.s.map(super::low), r.s, "{what}: state");
        assert_eq!((c.i, c.j), (r.i, r.j), "{what}: i, j");
    }

    #[test]
    fn rc4_matches_the_byte_reference_for_every_short_length_and_large_buffers() {
        let mut rng = Seeded(0x05ee_d0f7);
        let lengths = (0..=300).chain([1023, 1024, 4099, 16 << 10, (64 << 10) + 5, (1 << 20) + 7]);
        for len in lengths {
            let key_len = 1 + rng.below(256);
            let key = rng.bytes(key_len);
            // Start each pair at a random point of the keystream, so i and j
            // begin anywhere in their range.
            let lead = rng.below(600);
            let mut c = Rc4::new(&key);
            let mut r = Reference::new(&key);
            assert_same_state(&c, &r, "after the key schedule");
            c.skip(lead);
            r.apply(&mut vec![0u8; lead]);
            assert_same_state(&c, &r, "after the lead");
            let plain = rng.bytes(len);
            let mut got = plain.clone();
            let mut want = plain.clone();
            c.apply(&mut got);
            r.apply(&mut want);
            assert_eq!(got, want, "len {len}, key len {key_len}, lead {lead}");
            assert_same_state(&c, &r, "after the buffer");
            // Both go on from the same point.
            let mut c_next = [0u8; 13];
            let mut r_next = [0u8; 13];
            c.apply(&mut c_next);
            r.apply(&mut r_next);
            assert_eq!(c_next, r_next, "len {len}: the bytes that follow");
        }
    }

    #[test]
    fn rc4_in_arbitrary_chunks_gives_the_same_bytes_as_one_call() {
        let mut rng = Seeded(0xc4_0b5);
        for round in 0..64 {
            let key_len = 1 + rng.below(256);
            let key = rng.bytes(key_len);
            let len = if round % 8 == 0 {
                (1 << 18) + rng.below(4096)
            } else {
                rng.below(5000)
            };
            let plain = rng.bytes(len);
            let mut whole = plain.clone();
            let mut one = Rc4::new_discard_1024(&key);
            one.apply(&mut whole);

            let mut cut = plain.clone();
            let mut many = Rc4::new_discard_1024(&key);
            let mut at = 0;
            while at < len {
                // Mostly short and odd, sometimes empty, sometimes long.
                let n = match rng.below(4) {
                    0 => rng.below(9),
                    1 => rng.below(40),
                    2 => rng.below(300),
                    _ => rng.below(20_000),
                }
                .min(len - at);
                many.apply(&mut cut[at..at + n]);
                at += n;
            }
            assert_eq!(cut, whole, "round {round}, len {len}, key len {key_len}");
            assert_eq!((many.s, many.i, many.j), (one.s, one.i, one.j));
            // Applying it again restores the plaintext.
            Rc4::new_discard_1024(&key).apply(&mut cut);
            assert_eq!(cut, plain);
        }
    }

    /// The read-ahead loop and the word loop (on x86-64 the fallback for a
    /// CPU without SSE4.1) against the reference, with the loop changed from
    /// one call to the next so each picks up where the other left off.
    #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
    #[test]
    fn rc4_read_ahead_and_fallback_loops_match_the_reference_from_any_point() {
        #[cfg(target_arch = "x86_64")]
        let ahead = std::arch::is_x86_feature_detected!("sse4.1");
        #[cfg(target_arch = "aarch64")]
        let ahead = true;
        let mut rng = Seeded(0x00a4_eaad);
        for round in 0..48 {
            let key_len = 1 + rng.below(256);
            let key = rng.bytes(key_len);
            let mut c = Rc4::new(&key);
            let mut r = Reference::new(&key);
            let mut calls = 0;
            let mut total = 0;
            while total < 70_000 {
                let len = match rng.below(4) {
                    0 => rng.below(17),
                    1 => rng.below(300),
                    2 => 1448 + rng.below(3),
                    _ => 16_397 + rng.below(16_400),
                };
                let plain = rng.bytes(len);
                let mut got = plain.clone();
                let mut want = plain;
                if ahead && rng.below(2) == 0 {
                    #[cfg(target_arch = "x86_64")]
                    // SAFETY: the CPU has SSE4.1, checked above.
                    unsafe {
                        c.apply_ahead(&mut got)
                    };
                    #[cfg(target_arch = "aarch64")]
                    c.apply_ahead(&mut got);
                } else {
                    c.apply_words(&mut got);
                }
                r.apply(&mut want);
                assert_eq!(got, want, "round {round}, call {calls}, len {len}");
                assert_same_state(&c, &r, "after the call");
                calls += 1;
                total += len;
            }
        }
    }

    #[test]
    fn rc4_skip_is_the_keystream_it_passes_over() {
        let mut rng = Seeded(0x5c1b);
        for _ in 0..200 {
            let key_len = 1 + rng.below(64);
            let key = rng.bytes(key_len);
            let n = rng.below(3000);
            let mut skipped = Rc4::new(&key);
            skipped.skip(n);
            let mut applied = Rc4::new(&key);
            applied.apply(&mut vec![0u8; n]);
            assert_eq!(
                (skipped.s, skipped.i, skipped.j),
                (applied.s, applied.i, applied.j),
                "skip {n}"
            );
        }
    }
}
