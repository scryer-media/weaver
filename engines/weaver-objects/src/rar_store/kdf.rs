// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The two RAR key derivations, as pure functions. Both are slow by design;
// the store reaches them only through the process's key cache.
//
// RAR5: PBKDF2-HMAC-SHA256 over the UTF-8 password and the salt. The AES-256
// key is the chain after 2^count rounds, the hash key that keys checksums 16
// rounds after that, and the password check 16 rounds later still, folded to
// eight bytes.
//
// RAR4: 262,144 rounds of SHA-1 over the UTF-16LE password, the salt and a
// three-byte round number. The AES-128 key is the final digest, word by
// word little-endian; each of the 16 IV bytes is the low byte of the fifth
// word of the running digest at every 16,384th round.

use aws_lc_rs::digest::{Context, SHA1_FOR_LEGACY_USE_ONLY};
use aws_lc_rs::hmac;
use zeroize::Zeroizing;

// RAR's own bound on the KDF's exponent.
pub(super) const MAX_COUNT: u8 = 24;

// The longest password either format reads, in characters.
const MAX_PASSWORD: usize = 127;

pub(super) struct Rar5 {
    pub(super) key: Zeroizing<[u8; 32]>,
    pub(super) hash_key: Zeroizing<[u8; 32]>,
    pub(super) check: [u8; 8],
}

pub(super) fn rar5(password: &str, salt: &[u8; 16], count: u8) -> Option<Rar5> {
    if count > MAX_COUNT {
        return None;
    }
    let password: String = password.chars().take(MAX_PASSWORD).collect();
    let mac = hmac::Key::new(hmac::HMAC_SHA256, password.as_bytes());
    let mut first = Vec::with_capacity(salt.len() + 4);
    first.extend_from_slice(salt);
    first.extend_from_slice(&1u32.to_be_bytes());
    let mut round = Zeroizing::new([0u8; 32]);
    round.copy_from_slice(hmac::sign(&mac, &first).as_ref());
    let mut chain = round.clone();
    let mut outputs = [
        Zeroizing::new([0u8; 32]),
        Zeroizing::new([0u8; 32]),
        Zeroizing::new([0u8; 32]),
    ];
    let rounds = [(1u64 << count) - 1, 16, 16];
    for (output, more) in outputs.iter_mut().zip(rounds) {
        for _ in 0..more {
            let next = hmac::sign(&mac, round.as_slice());
            round.copy_from_slice(next.as_ref());
            for (byte, add) in chain.iter_mut().zip(round.iter()) {
                *byte ^= add;
            }
        }
        output.copy_from_slice(chain.as_slice());
    }
    let mut check = [0u8; 8];
    for (index, byte) in outputs[2].iter().enumerate() {
        check[index % 8] ^= byte;
    }
    let [key, hash_key, _] = outputs;
    Some(Rar5 {
        key,
        hash_key,
        check,
    })
}

// RAR5 checksums under a hash key: the HMAC of the CRC folded back to 32 bits.
pub(super) fn keyed_crc(hash_key: &[u8; 32], crc: u32) -> u32 {
    let mac = hmac::Key::new(hmac::HMAC_SHA256, hash_key);
    let digest = hmac::sign(&mac, &crc.to_le_bytes());
    digest
        .as_ref()
        .iter()
        .enumerate()
        .fold(0u32, |value, (index, byte)| {
            value ^ (u32::from(*byte) << ((index & 3) * 8))
        })
}

pub(super) struct Rar4 {
    pub(super) key: Zeroizing<[u8; 16]>,
    pub(super) iv: [u8; 16],
}

const RAR4_ROUNDS: u32 = 0x40000;

// `None` for a password whose bytes and salt fill a SHA-1 block: RAR's own
// SHA-1 writes its working state back into the input there, which a
// standard SHA-1 cannot reproduce, so such a set goes to the engine.
pub(super) fn rar4(password: &str, salt: &[u8; 8]) -> Option<Rar4> {
    let mut raw = Zeroizing::new(Vec::new());
    for unit in password.encode_utf16().take(MAX_PASSWORD) {
        raw.extend_from_slice(&unit.to_le_bytes());
    }
    raw.extend_from_slice(salt);
    if raw.len() > 64 {
        return None;
    }
    let mut context = Context::new(&SHA1_FOR_LEGACY_USE_ONLY);
    let mut iv = [0u8; 16];
    let step = RAR4_ROUNDS / 16;
    for round in 0..RAR4_ROUNDS {
        context.update(&raw);
        context.update(&round.to_le_bytes()[..3]);
        if round % step == 0 {
            let digest = context.clone().finish();
            iv[(round / step) as usize] = digest.as_ref()[19];
        }
    }
    let digest = context.finish();
    let mut key = Zeroizing::new([0u8; 16]);
    for word in 0..4 {
        for byte in 0..4 {
            key[word * 4 + byte] = digest.as_ref()[word * 4 + 3 - byte];
        }
    }
    Some(Rar4 { key, iv })
}
