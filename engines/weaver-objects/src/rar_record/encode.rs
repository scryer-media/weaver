// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Recovery records as RAR writes them, for tests: the RAR5 record a `RR`
// service header carries, and the RAR3 one with its NEWSUB header, each over
// the volume's bytes before that header.

use crc_turbo::{CrcAlgorithm, Digest};

use crate::grid::crc32;

// RAR5 stores a parity unit, and checks a data unit, in slices of this many
// bytes.
const SLICE: usize = 0x1_0000;
// A RAR3 record's unit.
const SECTOR: usize = 512;

// The RAR5 record over `protected` with `parity` parity units: RAR's own
// cut into at most 200 data units of an even size, Reed-Solomon over
// GF(2^16) with the polynomial 0x1100B, every slice behind its own chunk
// header.
pub fn rar5(protected: &[u8], parity: u16) -> Vec<u8> {
    let length = protected.len();
    let nd = length.div_ceil(1024).clamp(1, 200);
    let unit = length.div_ceil(nd).next_multiple_of(2);
    let units: Vec<Vec<u8>> = (0..nd)
        .map(|d| {
            let mut bytes =
                protected[(d * unit).min(length)..((d + 1) * unit).min(length)].to_vec();
            bytes.resize(unit, 0);
            bytes
        })
        .collect();
    let unit_len = |d: usize| unit.min(length - d * unit);
    let slices = unit.div_ceil(SLICE);
    let crcs: Vec<Vec<u64>> = (0..slices)
        .map(|slice| {
            (0..nd)
                .map(|d| {
                    let len = unit_len(d);
                    let from = (slice * SLICE).min(len);
                    let to = ((slice + 1) * SLICE).min(len);
                    slice_crc(&units[d][from..to])
                })
                .collect()
        })
        .collect();
    let header = 0x48 + 8 * nd;
    let row_size = slices * header + unit;
    let last = unit_len(nd - 1) as u64;
    let mut out = Vec::new();
    for k in 0..usize::from(parity) {
        let factors: Vec<u16> = (0..nd).map(|d| inverse(((nd + k) ^ d) as u16)).collect();
        let mut row = vec![0u8; unit];
        for (d, bytes) in units.iter().enumerate() {
            for (word, into) in bytes.chunks_exact(2).zip(row.chunks_exact_mut(2)) {
                let product = multiply(factors[d], u16::from_le_bytes([word[0], word[1]]));
                let sum = u16::from_le_bytes([into[0], into[1]]) ^ product;
                into.copy_from_slice(&sum.to_le_bytes());
            }
        }
        for (slice, crcs) in crcs.iter().enumerate() {
            let offset = slice * SLICE;
            let len = SLICE.min(unit - offset);
            let start = out.len();
            out.extend_from_slice(b"{RB}");
            out.extend_from_slice(&[0; 8]);
            out.extend_from_slice(&((header + len) as u32).to_le_bytes());
            out.extend_from_slice(&(header as u32).to_le_bytes());
            out.extend_from_slice(&[1, 1]);
            out.extend_from_slice(&(offset as u64).to_le_bytes());
            let tail = (SLICE as u64).min(last.wrapping_sub(offset as u64));
            out.extend_from_slice(&(tail as u32).to_le_bytes());
            out.extend_from_slice(&(length as u64).to_le_bytes());
            out.extend_from_slice(&(unit as u64).to_le_bytes());
            out.extend_from_slice(&(row_size as u64).to_le_bytes());
            out.extend_from_slice(&(nd as u16).to_le_bytes());
            out.extend_from_slice(&parity.to_le_bytes());
            out.extend_from_slice(&(k as u16).to_le_bytes());
            for crc in crcs {
                out.extend_from_slice(&crc.to_le_bytes());
            }
            out.extend_from_slice(b"testrrid");
            out.extend_from_slice(&row[offset..offset + len]);
            let crc = crc_turbo::checksum(CrcAlgorithm::Crc64Xz, &out[start + 12..]);
            out[start + 4..start + 12].copy_from_slice(&crc.to_le_bytes());
        }
    }
    out
}

// The RAR3 record's data area over `protected` with `slots` parity sectors:
// each sector's checksum, then each slot the XOR of the sectors that fall
// in it, every sector zero-padded to 512 bytes.
pub fn rar3(protected: &[u8], slots: u32) -> Vec<u8> {
    let slots = slots as usize;
    let mut sums = Vec::new();
    let mut parity = vec![0u8; slots * SECTOR];
    for (index, piece) in protected.chunks(SECTOR).enumerate() {
        let mut sector = [0u8; SECTOR];
        sector[..piece.len()].copy_from_slice(piece);
        sums.extend_from_slice(&(!crc32(&sector) as u16).to_le_bytes());
        let slot = &mut parity[(index % slots) * SECTOR..][..SECTOR];
        for (into, byte) in slot.iter_mut().zip(sector) {
            *into ^= byte;
        }
    }
    sums.extend(parity);
    sums
}

// The RAR3 NEWSUB header named `RR` that opens a record of `slots` parity
// sectors over `protected` bytes.
pub fn rar3_header(protected: u64, slots: u32) -> Vec<u8> {
    let sectors = protected.div_ceil(SECTOR as u64);
    let size = 2 * sectors + u64::from(slots) * SECTOR as u64;
    let mut body = (size as u32).to_le_bytes().to_vec();
    body.extend((size as u32).to_le_bytes());
    body.push(3);
    body.extend(0u32.to_le_bytes());
    body.extend(0x5a21_0000u32.to_le_bytes());
    body.push(29);
    body.push(0x30);
    body.extend(2u16.to_le_bytes());
    body.extend(0u32.to_le_bytes());
    body.extend_from_slice(b"RR");
    body.extend_from_slice(b"Protect+");
    body.extend(slots.to_le_bytes());
    body.extend(sectors.to_le_bytes());
    let mut rest = vec![0x7a];
    rest.extend(0x8000u16.to_le_bytes());
    rest.extend(((7 + body.len()) as u16).to_le_bytes());
    rest.extend(body);
    let mut out = (crc32(&rest) as u16).to_le_bytes().to_vec();
    out.extend(rest);
    out
}

// A RAR3 header as a volume with enciphered headers carries it: `salt`,
// then the header AES-ciphered under `password` and that salt, zero-padded
// to a whole block.
pub fn seal_rar3(header: &[u8], password: &str, salt: [u8; 8]) -> Vec<u8> {
    let (key, iv) = unrar_rs::rar4_derive_key(password, Some(&salt));
    let mut cipher = header.to_vec();
    cipher.resize(header.len().next_multiple_of(16), 0);
    unrar_rs::encrypt_cipher_range_rar4(&key, &iv, &mut cipher).expect("whole blocks");
    let mut out = salt.to_vec();
    out.extend(cipher);
    out
}

// CRC-64/XZ's polynomial and reflection with a zero initial value and no
// final xor.
fn slice_crc(bytes: &[u8]) -> u64 {
    let mut digest = Digest::new_with_init_state(CrcAlgorithm::Crc64Xz, 0);
    digest.update(bytes);
    digest.finalize() ^ u64::MAX
}

fn multiply(a: u16, b: u16) -> u16 {
    let (mut a, mut b, mut product) = (u32::from(a), b, 0u32);
    while b != 0 {
        if b & 1 != 0 {
            product ^= a;
        }
        b >>= 1;
        a <<= 1;
        if a & 0x1_0000 != 0 {
            a ^= 0x1_100b;
        }
    }
    product as u16
}

// a^(2^16 - 2), the inverse of a nonzero element.
fn inverse(a: u16) -> u16 {
    let (mut base, mut exponent, mut result) = (a, 0xfffe_u32, 1u16);
    while exponent != 0 {
        if exponent & 1 != 0 {
            result = multiply(result, base);
        }
        base = multiply(base, base);
        exponent >>= 1;
    }
    result
}
