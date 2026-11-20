// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::harness::{ARTICLE, Post, par2_set};
use super::sfv::crc;

pub fn delivered(name: &str) -> String {
    format!("lantern beacon/{name}")
}

pub fn posts(volumes: Vec<(String, Vec<u8>)>) -> Vec<Post> {
    volumes
        .into_iter()
        .map(|(name, bytes)| Post::plain(&name, bytes))
        .collect()
}

// The volumes, then an index and a volume of two recovery blocks over them.
pub fn with_par2(volumes: Vec<(String, Vec<u8>)>) -> Vec<Post> {
    let files: Vec<(&str, &[u8])> = volumes
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let set = par2_set("silver.horizon", &files, 2);
    let mut out = posts(volumes.clone());
    out.extend(set);
    out
}

// RAR5's recovery volumes over `volumes`, as RAR writes them: a header
// naming the set's geometry, every data volume's size and CRC32, and each
// one's Cauchy rows over GF(2^16), the volumes zero-padded to the longest.
pub fn rar5_revs(base: &str, volumes: &[&[u8]], count: usize) -> Vec<Post> {
    let data = volumes.len();
    // unrar-rs reads an odd-width set's last parity word short, so a
    // fixture keeps the longest volume even.
    let width = volumes.iter().map(|volume| volume.len()).max().unwrap_or(0);
    assert_eq!(width % 2, 0, "the longest volume is even");
    let padded: Vec<Vec<u8>> = volumes
        .iter()
        .map(|volume| {
            let mut padded = volume.to_vec();
            padded.resize(width, 0);
            padded
        })
        .collect();
    (0..count)
        .map(|index| {
            let row = data + index;
            let mut parity = vec![0u8; width];
            for (column, volume) in padded.iter().enumerate() {
                let factor = reedsolomon_rs::gf::inv((row as u16) ^ (column as u16));
                reedsolomon_rs::gf_simd::mul_acc_region(factor, volume, &mut parity);
            }
            let mut raw = vec![1u8];
            raw.extend_from_slice(&(data as u16).to_le_bytes());
            raw.extend_from_slice(&(count as u16).to_le_bytes());
            raw.extend_from_slice(&(row as u16).to_le_bytes());
            raw.extend_from_slice(&crc(&parity).to_le_bytes());
            for volume in volumes {
                raw.extend_from_slice(&(volume.len() as u64).to_le_bytes());
                raw.extend_from_slice(&crc(volume).to_le_bytes());
            }
            let size = (raw.len() as u32).to_le_bytes();
            let mut rev = b"Rar!\x1aRev".to_vec();
            rev.extend_from_slice(&crc(&[size.as_slice(), raw.as_slice()].concat()).to_le_bytes());
            rev.extend_from_slice(&size);
            rev.extend_from_slice(&raw);
            // A poster's article holds the whole header, which grows with
            // the volume count, so the set's geometry arrives in the first.
            let header = rev.len();
            rev.extend_from_slice(&parity);
            Post {
                least_article: header.div_ceil(ARTICLE) * ARTICLE,
                ..Post::plain(&format!("{base}.part{}.rev", index + 1), rev)
            }
        })
        .collect()
}
