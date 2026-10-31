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
    let cipher_len = block_ceil(plain_len as u64) as usize;
    let mut cipher = plain.clone();
    cipher.extend((0..cipher_len - plain_len).map(|index| 0xA0 + index as u8));
    MemberCipherKey::Aes256(KEY)
        .encrypt_range(&IV, &mut cipher)
        .unwrap();
    (plain, cipher)
}

fn cbc(plain: &[u8], cipher: &[u8]) -> Cbc {
    Cbc::new(&KEY, IV, cipher.len() as u64, plain.len() as u64).unwrap()
}

// The stream's owner: the plaintext as it placed it, with how it came out,
// and the padding it keeps. The cipher reads both and keeps neither.
#[derive(Default)]
struct Out {
    bytes: Vec<u8>,
    coverage: Coverage,
    padding: Padding,
}

impl Out {
    // Cipher bytes through the stream, and what came out placed.
    fn deliver(
        &mut self,
        stream: &mut Cbc,
        offset: u64,
        bytes: &[u8],
        provenance: Provenance,
    ) -> Vec<Plain> {
        let mut plains = Vec::new();
        stream.deliver(
            &self.coverage,
            &mut self.padding,
            (offset, bytes, provenance),
            &mut reader(&self.bytes),
            &mut plains,
        );
        let said = plains.clone();
        self.take(&mut plains);
        said
    }

    fn seal(&self, stream: &Cbc, range: ByteRange, plain: &[u8], into: &mut Vec<u8>) -> bool {
        stream.seal(
            &self.coverage,
            &self.padding,
            range,
            &mut reader(plain),
            into,
        )
    }

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
    let mut kept = BTreeSet::from([cipher.len() as u64]);
    for range in scrambled(cipher.len(), 37) {
        out.deliver(
            &mut stream,
            range.start,
            &cipher[range.start as usize..range.end as usize],
            Provenance::Fresh,
        );
        if range.start % BLOCK != 0 {
            kept.insert(block_ceil(range.start));
        }
    }
    assert_eq!(out.bytes, plain);
    assert!(out.coverage.verified(ByteRange::new(0, plain.len() as u64)));
    // What is left: the final block, which seals the padding, and each block
    // an arrival started inside.
    assert_eq!(stream.kept(), kept.len());
}

// A span un-said from inside a block, as its owner un-says it: from the
// start of that block. The bytes before the span do not come again, and the
// block comes out again whole from what the chain kept when the span's
// arrival first started inside it.
#[test]
fn a_span_unsaid_from_inside_a_block_comes_out_again_whole() {
    let (plain, cipher) = posted(100);
    let mut stream = cbc(&plain, &cipher);
    let mut out = Out::default();
    let seam = 41;
    out.deliver(&mut stream, 0, &cipher[..seam], Provenance::Fresh);
    out.deliver(&mut stream, seam as u64, &cipher[seam..], Provenance::Fresh);
    assert_eq!(out.bytes, plain);
    let from = block_floor(seam as u64);
    let (plain_len, cipher_len) = (plain.len() as u64, cipher.len() as u64);
    out.coverage.erase(ByteRange::new(from, plain_len));
    out.padding
        .forget(plain_len, ByteRange::new(from, cipher_len));
    let said = out.deliver(&mut stream, seam as u64, &cipher[seam..], Provenance::Fresh);
    let said: Vec<ByteRange> = said
        .iter()
        .map(|plain| ByteRange::at(plain.offset, plain.bytes.len() as u64))
        .collect();
    assert_eq!(said, vec![ByteRange::new(from, plain_len)]);
    assert_eq!(out.bytes, plain);
    assert!(out.coverage.verified(ByteRange::new(0, plain_len)));
}

// The stream came whole, then the same bytes came again from inside a block
// that had come out, as a second copy whose seams differ does: that block is
// kept as one an arrival started inside, so an un-say from that seam brings it
// out again whole though the bytes before the seam do not come again.
#[test]
fn the_same_bytes_again_from_inside_a_block_keep_it_for_an_unsay_there() {
    let (plain, cipher) = posted(100);
    let mut stream = cbc(&plain, &cipher);
    let mut out = Out::default();
    let seam = 41;
    out.deliver(&mut stream, 0, &cipher, Provenance::Fresh);
    let again = out.deliver(&mut stream, seam as u64, &cipher[seam..], Provenance::Fresh);
    assert!(again.is_empty(), "nothing new: {again:?}");
    let from = block_floor(seam as u64);
    let (plain_len, cipher_len) = (plain.len() as u64, cipher.len() as u64);
    out.coverage.erase(ByteRange::new(from, plain_len));
    out.padding
        .forget(plain_len, ByteRange::new(from, cipher_len));
    let said = out.deliver(&mut stream, seam as u64, &cipher[seam..], Provenance::Fresh);
    let said: Vec<ByteRange> = said
        .iter()
        .map(|plain| ByteRange::at(plain.offset, plain.bytes.len() as u64))
        .collect();
    assert_eq!(said, vec![ByteRange::new(from, plain_len)]);
    assert_eq!(out.bytes, plain);
    assert!(out.coverage.verified(ByteRange::new(0, plain_len)));
}

#[test]
fn a_block_waits_for_the_one_before_it() {
    let (plain, cipher) = posted(64);
    let mut stream = cbc(&plain, &cipher);
    let mut out = Out::default();
    let plains = out.deliver(&mut stream, 32, &cipher[32..48], Provenance::Fresh);
    assert!(plains.is_empty());
    // Its seed is in: the block after it deciphers, it still waits.
    let plains = out.deliver(&mut stream, 16, &cipher[16..32], Provenance::Fresh);
    assert_eq!(plains.len(), 1);
    assert_eq!(plains[0].offset, 32);
    out.deliver(&mut stream, 0, &cipher[..16], Provenance::Fresh);
    assert_eq!(out.bytes, plain[..48]);
}

#[test]
fn partial_edge_blocks_are_kept_and_seal_back() {
    let (plain, cipher) = posted(64);
    let mut stream = cbc(&plain, &cipher);
    let mut out = Out::default();
    let plains = out.deliver(&mut stream, 5, &cipher[5..27], Provenance::Fresh);
    assert!(plains.is_empty());
    let mut into = Vec::new();
    assert!(out.seal(&stream, ByteRange::new(5, 27), &plain, &mut into));
    assert_eq!(into, cipher[5..27]);
    assert!(!out.seal(&stream, ByteRange::new(4, 27), &plain, &mut Vec::new()));
    out.deliver(&mut stream, 0, &cipher[..5], Provenance::Fresh);
    assert_eq!(out.bytes, plain[..16]);
}

#[test]
fn the_padding_is_kept_and_the_final_block_seals_with_it() {
    let (plain, cipher) = posted(1000);
    let mut stream = cbc(&plain, &cipher);
    let mut out = Out::default();
    out.deliver(&mut stream, 0, &cipher, Provenance::Fresh);
    assert_eq!(out.bytes.len(), plain.len());
    let mut into = Vec::new();
    let tail = ByteRange::new(960, cipher.len() as u64);
    assert!(out.seal(&stream, tail, &plain, &mut into));
    assert_eq!(into, cipher[960..]);
}

// Past two strides, so the seeds inside the run are what sealing uses.
fn two_strides() -> (Vec<u8>, Vec<u8>) {
    posted(2 * STRIDE as usize + 4096 + 3)
}

fn chunks(len: usize) -> impl Iterator<Item = ByteRange> {
    (0..len)
        .step_by(65_536)
        .map(move |start| ByteRange::new(start as u64, (start + 65_536).min(len) as u64))
}

#[test]
fn sealing_re_enciphers_from_any_kept_seed() {
    let (plain, cipher) = two_strides();
    let mut stream = cbc(&plain, &cipher);
    let mut out = Out::default();
    for range in chunks(cipher.len()) {
        out.deliver(
            &mut stream,
            range.start,
            &cipher[range.start as usize..range.end as usize],
            Provenance::Fresh,
        );
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
        assert!(out.seal(&stream, range, &plain, &mut into), "{range}");
        assert_eq!(
            into,
            cipher[range.start as usize..range.end as usize],
            "{range}"
        );
    }
}

// Blocks staged behind a gap post as they came: no seed is needed for
// them, so the gap never comes into it. Across the gap nothing seals.
#[test]
fn staged_blocks_behind_a_gap_seal_without_it() {
    let (plain, cipher) = posted(300);
    let mut stream = cbc(&plain, &cipher);
    let mut out = Out::default();
    for range in [
        ByteRange::new(0, 64),
        ByteRange::new(160, cipher.len() as u64),
    ] {
        let bytes = &cipher[range.start as usize..range.end as usize];
        out.deliver(&mut stream, range.start, bytes, Provenance::Fresh);
    }
    let cipher_len = cipher.len() as u64;
    for range in [
        ByteRange::new(0, 64),
        ByteRange::new(160, cipher_len),
        ByteRange::new(170, 200),
    ] {
        let mut into = Vec::new();
        assert!(out.seal(&stream, range, &plain, &mut into), "{range}");
        assert_eq!(
            into,
            cipher[range.start as usize..range.end as usize],
            "{range}"
        );
    }
    let mut into = Vec::new();
    assert!(!out.seal(&stream, ByteRange::new(48, 176), &plain, &mut into));
}

// A cipher opened again with nothing kept, over its owner's record of what
// it placed, seals each range before the replay delivers it, says nothing
// new, and keeps the seeds the first one kept.
#[test]
fn a_cipher_opened_again_over_its_owners_record_seals_through_a_replay() {
    let (plain, cipher) = two_strides();
    let mut out = Out::default();
    let mut first = cbc(&plain, &cipher);
    for range in chunks(cipher.len()) {
        let bytes = &cipher[range.start as usize..range.end as usize];
        out.deliver(&mut first, range.start, bytes, Provenance::Fresh);
    }
    let mut again = cbc(&plain, &cipher);
    for range in chunks(cipher.len()) {
        let bytes = &cipher[range.start as usize..range.end as usize];
        let mut into = Vec::new();
        assert!(out.seal(&again, range, &plain, &mut into), "{range}");
        assert!(into == bytes, "{range}");
        let said = out.deliver(&mut again, range.start, bytes, Provenance::Fresh);
        assert!(said.is_empty(), "{range}");
    }
    assert_eq!(again.kept(), first.kept());
}

#[test]
fn a_repaired_block_re_chains_the_one_after_it() {
    let (plain, cipher) = posted(80);
    let mut stream = cbc(&plain, &cipher);
    let mut out = Out::default();
    let mut garbled = cipher.clone();
    garbled[20] ^= 0xFF;
    out.deliver(&mut stream, 0, &garbled[..16], Provenance::Fresh);
    out.deliver(&mut stream, 16, &garbled[16..32], Provenance::Damaged);
    out.deliver(&mut stream, 32, &garbled[32..], Provenance::Fresh);
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
    let placed = out.bytes.clone();
    assert!(out.seal(&stream, ByteRange::new(0, 80), &placed, &mut into));
    assert_eq!(into, garbled);

    let plains = out.deliver(&mut stream, 16, &cipher[16..32], Provenance::Repaired);
    let repaired: Vec<(u64, Provenance)> = plains
        .iter()
        .map(|plain| (plain.offset, plain.provenance))
        .collect();
    assert_eq!(repaired, vec![(16, Provenance::Repaired)]);
    assert_eq!(plains[0].bytes.len(), 32);
    assert_eq!(out.bytes, plain);
    assert!(out.coverage.verified(ByteRange::new(0, 80)));
    assert_eq!(stream.kept(), 1);
}

#[test]
fn a_replay_says_nothing_new() {
    let (plain, cipher) = posted(100);
    let mut stream = cbc(&plain, &cipher);
    let mut out = Out::default();
    out.deliver(&mut stream, 0, &cipher, Provenance::Fresh);
    let plains = out.deliver(&mut stream, 0, &cipher, Provenance::Fresh);
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
