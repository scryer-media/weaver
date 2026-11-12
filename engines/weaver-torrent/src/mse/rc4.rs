// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

//! The RC4 stream cipher, as the encrypted peer handshake uses it.

/// RC4 state. Encryption and decryption are the same operation.
#[derive(Clone)]
pub(crate) struct Rc4 {
    s: [u8; 256],
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
        let mut s = [0u8; 256];
        for (v, i) in s.iter_mut().zip(0..=255u8) {
            *v = i;
        }
        let mut j = 0u8;
        for (i, &k) in (0..256).zip(key.iter().cycle()) {
            j = j.wrapping_add(s[i]).wrapping_add(k);
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
    ///
    /// Eight keystream bytes are gathered into a word and XORed into the
    /// buffer at once, so each byte costs the two state stores and no store
    /// of its own (libtorrent `rc4_encrypt`, src/pe_crypto.cpp, RC_2_1).
    pub fn apply(&mut self, buf: &mut [u8]) {
        #[cfg(test)]
        self.count(buf.len());
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

/// One keystream byte. Only the low byte of `j` is RC4's `j`: it is carried
/// unmasked so that the dependency from one byte to the next is a single add,
/// with the mask taken off that chain where `j` indexes the state.
#[inline(always)]
fn step(s: &mut [u8; 256], i: &mut u8, j: &mut usize) -> u8 {
    *i = i.wrapping_add(1);
    let ii = usize::from(*i);
    let ti = s[ii];
    *j = j.wrapping_add(usize::from(ti));
    let jj = *j & 0xff;
    let tj = s[jj];
    s[ii] = tj;
    s[jj] = ti;
    s[usize::from(ti.wrapping_add(tj))]
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
        assert_eq!(c.s, r.s, "{what}: state");
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
