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
        for i in 0..256 {
            j = j.wrapping_add(s[i]).wrapping_add(key[i % key.len()]);
            s.swap(i, j as usize);
        }
        Self { s, i: 0, j: 0 }
    }

    /// The handshake's variant: the first 1024 bytes of keystream are thrown away.
    pub fn new_discard_1024(key: &[u8]) -> Self {
        let mut c = Self::new(key);
        c.skip(1024);
        c
    }

    #[inline]
    fn next_byte(&mut self) -> u8 {
        self.i = self.i.wrapping_add(1);
        self.j = self.j.wrapping_add(self.s[self.i as usize]);
        self.s.swap(self.i as usize, self.j as usize);
        self.s[self.s[self.i as usize].wrapping_add(self.s[self.j as usize]) as usize]
    }

    /// XOR the keystream into `buf`.
    pub fn apply(&mut self, buf: &mut [u8]) {
        for b in buf.iter_mut() {
            *b ^= self.next_byte();
        }
    }

    /// Advance the keystream by `n` bytes.
    pub fn skip(&mut self, n: usize) {
        for _ in 0..n {
            self.next_byte();
        }
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
}
