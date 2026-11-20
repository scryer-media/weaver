// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::harness::{ARTICLE, payload};
use std::io::Write;

pub fn ember() -> Vec<u8> {
    payload(9 * ARTICLE + 21, 61)
}

pub fn quill() -> Vec<u8> {
    payload(4 * ARTICLE - 3, 62)
}

pub fn xz(bytes: &[u8]) -> Vec<u8> {
    let props = lzma_turbo::LzmaEncProps::xz_preset(0, false).unwrap();
    let mut encoder = lzma_turbo::XzWriter::new(Vec::new(), &props).unwrap();
    encoder.write_all(bytes).unwrap();
    encoder.finish().unwrap()
}

pub fn bzip2(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = bzip2::write::BzEncoder::new(Vec::new(), bzip2::Compression::fast());
    encoder.write_all(bytes).unwrap();
    encoder.finish().unwrap()
}

pub fn tar(members: &[(&str, Vec<u8>)]) -> Vec<u8> {
    let mut builder = tar::Builder::new(Vec::new());
    for (name, bytes) in members {
        let mut header = tar::Header::new_gnu();
        header.set_size(bytes.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        builder
            .append_data(&mut header, name, bytes.as_slice())
            .unwrap();
    }
    builder.into_inner().unwrap()
}

pub fn members() -> Vec<(&'static str, Vec<u8>)> {
    vec![("ember.bin", ember()), ("quill.bin", quill())]
}

pub fn gzip(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    encoder.write_all(bytes).unwrap();
    encoder.finish().unwrap()
}

pub fn deflate(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = flate2::write::DeflateEncoder::new(Vec::new(), flate2::Compression::fast());
    encoder.write_all(bytes).unwrap();
    encoder.finish().unwrap()
}

pub fn zstd(bytes: &[u8]) -> Vec<u8> {
    zstd::stream::encode_all(bytes, 1).unwrap()
}

pub fn brotli(bytes: &[u8]) -> Vec<u8> {
    let mut output = Vec::new();
    {
        let mut encoder = brotli::CompressorWriter::new(&mut output, 4096, 1, 18);
        encoder.write_all(bytes).unwrap();
    }
    output
}

// Text a compressor codes rather than stores, so damage to it lands in
// coded data the decoder reads.
pub fn prose() -> Vec<u8> {
    b"a lantern by the quill, an ember in the beacon; "
        .iter()
        .copied()
        .cycle()
        .take(12 * ARTICLE + 9)
        .collect()
}

// The bytes with a run in their middle arrived wrong under a good article
// checksum: the signature still opens them and the damage lies past it.
pub fn damaged_in_the_middle(mut bytes: Vec<u8>) -> Vec<u8> {
    let at = bytes.len() / 2;
    for byte in &mut bytes[at..at + 16] {
        *byte ^= 0x5a;
    }
    bytes
}
