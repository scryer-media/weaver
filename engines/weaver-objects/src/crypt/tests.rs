// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::testing::payload;

const KEY: [u8; 32] = [7; 32];
const IV: [u8; 16] = [9; 16];

// A payload and its posted cipher stream, padded the way the stream's writer
// pads: with bytes no member holds.
fn posted(plain_len: usize) -> (Vec<u8>, Vec<u8>) {
    let plain = payload(3, plain_len);
    let cipher_len = ceil(plain_len as u64) as usize;
    let mut cipher = plain.clone();
    cipher.extend((0..cipher_len - plain_len).map(|index| 0xA0 + index as u8));
    Keys::new(&KEY).unwrap().encipher(IV, &mut cipher).unwrap();
    (plain, cipher)
}

fn cbc(plain: &[u8], cipher: &[u8]) -> Cbc {
    Cbc::new(&KEY, IV, cipher.len() as u64, plain.len() as u64).unwrap()
}

// Plaintext as it came out, with how it came out.
#[derive(Default)]
struct Out {
    bytes: Vec<u8>,
    coverage: Coverage,
}

impl Out {
    fn take(&mut self, plains: &mut Vec<Plain>) {
        for plain in plains.drain(..) {
            let end = plain.offset as usize + plain.bytes.len();
            if self.bytes.len() < end {
                self.bytes.resize(end, 0);
            }
            self.bytes[plain.offset as usize..end].copy_from_slice(&plain.bytes);
            self.coverage.write(
                ByteRange::at(plain.offset, plain.bytes.len() as u64),
                plain.provenance,
            );
        }
    }
}

// Chunks of the stream in a fixed scrambled order.
fn scrambled(len: usize, chunk: usize) -> Vec<ByteRange> {
    let mut ranges: Vec<ByteRange> = (0..len)
        .step_by(chunk)
        .map(|start| ByteRange::new(start as u64, (start + chunk).min(len) as u64))
        .collect();
    let count = ranges.len();
    for index in 0..count {
        ranges.swap(index, (index * 7 + 3) % count);
    }
    ranges
}

fn reader(plain: &[u8]) -> impl FnMut(ByteRange, &mut Vec<u8>) -> bool + '_ {
    |range, into| {
        into.extend_from_slice(&plain[range.start as usize..range.end as usize]);
        true
    }
}

#[test]
fn bytes_in_any_order_decipher_once_their_seed_is_in() {
    let (plain, cipher) = posted(1000);
    let mut stream = cbc(&plain, &cipher);
    let mut out = Out::default();
    let mut plains = Vec::new();
    for range in scrambled(cipher.len(), 37) {
        stream.deliver(
            range.start,
            &cipher[range.start as usize..range.end as usize],
            Provenance::Fresh,
            &mut plains,
        );
        out.take(&mut plains);
    }
    assert_eq!(out.bytes, plain);
    assert!(out.coverage.verified(ByteRange::new(0, plain.len() as u64)));
    // What is left: the final block, which seals the padding.
    assert_eq!(stream.kept(), 1);
}

#[test]
fn a_block_waits_for_the_one_before_it() {
    let (plain, cipher) = posted(64);
    let mut stream = cbc(&plain, &cipher);
    let mut plains = Vec::new();
    stream.deliver(32, &cipher[32..48], Provenance::Fresh, &mut plains);
    assert!(plains.is_empty());
    // Its seed is in: the block after it deciphers, it still waits.
    stream.deliver(16, &cipher[16..32], Provenance::Fresh, &mut plains);
    assert_eq!(plains.len(), 1);
    assert_eq!(plains[0].offset, 32);
    let mut out = Out::default();
    out.take(&mut plains);
    stream.deliver(0, &cipher[..16], Provenance::Fresh, &mut plains);
    out.take(&mut plains);
    assert_eq!(out.bytes, plain[..48]);
}

#[test]
fn partial_edge_blocks_are_kept_and_seal_back() {
    let (plain, cipher) = posted(64);
    let mut stream = cbc(&plain, &cipher);
    let mut plains = Vec::new();
    stream.deliver(5, &cipher[5..27], Provenance::Fresh, &mut plains);
    assert!(plains.is_empty());
    let mut into = Vec::new();
    assert!(stream.seal(ByteRange::new(5, 27), &mut reader(&plain), &mut into));
    assert_eq!(into, cipher[5..27]);
    assert!(!stream.seal(ByteRange::new(4, 27), &mut reader(&plain), &mut Vec::new()));
    stream.deliver(0, &cipher[..5], Provenance::Fresh, &mut plains);
    let mut out = Out::default();
    out.take(&mut plains);
    assert_eq!(out.bytes, plain[..16]);
}

#[test]
fn the_padding_is_kept_and_the_final_block_seals_with_it() {
    let (plain, cipher) = posted(1000);
    let mut stream = cbc(&plain, &cipher);
    let mut plains = Vec::new();
    stream.deliver(0, &cipher, Provenance::Fresh, &mut plains);
    let mut out = Out::default();
    out.take(&mut plains);
    assert_eq!(out.bytes.len(), plain.len());
    let mut into = Vec::new();
    let tail = ByteRange::new(960, cipher.len() as u64);
    assert!(stream.seal(tail, &mut reader(&plain), &mut into));
    assert_eq!(into, cipher[960..]);
}

#[test]
fn sealing_re_enciphers_from_any_kept_seed() {
    // Past two strides, so the seeds inside the run are what sealing uses.
    let len = 2 * STRIDE as usize + 4096 + 3;
    let (plain, cipher) = posted(len);
    let mut stream = cbc(&plain, &cipher);
    let mut plains = Vec::new();
    for start in (0..cipher.len()).step_by(65_536) {
        let end = (start + 65_536).min(cipher.len());
        stream.deliver(
            start as u64,
            &cipher[start..end],
            Provenance::Fresh,
            &mut plains,
        );
        plains.clear();
    }
    // Two stride seeds and the final block.
    assert_eq!(stream.kept(), 3);
    let cipher_len = cipher.len() as u64;
    for range in [
        ByteRange::new(0, 1),
        ByteRange::new(17, 4000),
        ByteRange::new(STRIDE - 20, STRIDE + 20),
        ByteRange::new(STRIDE + 1, STRIDE + 2),
        ByteRange::new(2 * STRIDE - 1, 2 * STRIDE + 100),
        ByteRange::new(cipher_len - 40, cipher_len),
        ByteRange::new(0, cipher_len),
    ] {
        let mut into = Vec::new();
        assert!(
            stream.seal(range, &mut reader(&plain), &mut into),
            "{range}"
        );
        assert_eq!(
            into,
            cipher[range.start as usize..range.end as usize],
            "{range}"
        );
    }
}

#[test]
fn a_repaired_block_re_chains_the_one_after_it() {
    let (plain, cipher) = posted(80);
    let mut stream = cbc(&plain, &cipher);
    let mut plains = Vec::new();
    let mut garbled = cipher.clone();
    garbled[20] ^= 0xFF;
    stream.deliver(0, &garbled[..16], Provenance::Fresh, &mut plains);
    stream.deliver(16, &garbled[16..32], Provenance::Damaged, &mut plains);
    stream.deliver(32, &garbled[32..], Provenance::Fresh, &mut plains);
    let mut out = Out::default();
    out.take(&mut plains);
    assert_eq!(
        out.coverage.damaged_ranges().collect::<Vec<_>>(),
        vec![ByteRange::new(16, 48)]
    );
    assert_ne!(out.bytes[16..48], plain[16..48]);
    // The damaged block and the one it seeds are what is kept, besides the
    // final block.
    assert!(stream.kept() >= 3);
    // A posted range rebuilt over damage gives back what was posted.
    let mut into = Vec::new();
    assert!(stream.seal(ByteRange::new(0, 80), &mut reader(&out.bytes), &mut into));
    assert_eq!(into, garbled);

    stream.deliver(16, &cipher[16..32], Provenance::Repaired, &mut plains);
    let repaired: Vec<(u64, Provenance)> = plains
        .iter()
        .map(|plain| (plain.offset, plain.provenance))
        .collect();
    assert_eq!(repaired, vec![(16, Provenance::Repaired)]);
    assert_eq!(plains[0].bytes.len(), 32);
    out.take(&mut plains);
    assert_eq!(out.bytes, plain);
    assert!(out.coverage.verified(ByteRange::new(0, 80)));
    assert_eq!(stream.kept(), 1);
}

#[test]
fn a_replay_says_nothing_new() {
    let (plain, cipher) = posted(100);
    let mut stream = cbc(&plain, &cipher);
    let mut plains = Vec::new();
    stream.deliver(0, &cipher, Provenance::Fresh, &mut plains);
    plains.clear();
    stream.deliver(0, &cipher, Provenance::Fresh, &mut plains);
    assert!(plains.is_empty());
}

#[test]
fn a_stream_of_partial_blocks_is_refused() {
    assert!(matches!(
        Cbc::new(&KEY, IV, 17, 10),
        Err(CryptError::Unaligned(17))
    ));
    assert!(matches!(
        Cbc::new(&KEY, IV, 16, 17),
        Err(CryptError::Overlong { .. })
    ));
}
