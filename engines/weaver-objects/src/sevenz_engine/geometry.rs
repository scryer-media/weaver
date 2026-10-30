// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The exceptional inferred-length path. Only a valid start header and one
// end-header-CRC-valid zero-tail layout can change the logical piece lengths.
// The source remains untouched, and the ordinary decoder verifies its entries.

use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;

use weaver_graph::{VolumeIndex, Volumes};

use crate::engine::Joined;
use crate::grid::{crc32, crc32_combine};

#[derive(Debug)]
pub(super) enum Failure {
    Source(io::Error),
    Layout,
}

impl From<io::Error> for Failure {
    fn from(error: io::Error) -> Self {
        Self::Source(error)
    }
}

pub(super) fn resolve(volumes: Arc<dyn Volumes>) -> Result<Arc<dyn Volumes>, Failure> {
    if !volumes.uncertain_lengths() {
        return Ok(volumes);
    }
    let input = Joined::new(Arc::clone(&volumes));
    let mut head = [0; 32];
    input.read_exact(0, &mut head)?;
    let Some(total) = crate::sevenz_store::measured(&head) else {
        return Ok(volumes);
    };
    let start = 32u64
        .checked_add(u64::from_le_bytes(head[12..20].try_into().unwrap()))
        .ok_or(Failure::Layout)?;
    let size = u64::from_le_bytes(head[20..28].try_into().unwrap());
    let expected = u32::from_le_bytes(head[28..32].try_into().unwrap());
    let mut physical = Vec::new();
    let mut after = None;
    while let Some(volume) = volumes.next(after)? {
        if volume.0 as usize != physical.len() || physical.len() >= 100_000 {
            return Err(Failure::Layout);
        }
        physical.push(input.closed_len(volume)?);
        after = Some(volume);
    }
    let Some(&first) = physical.first() else {
        return Err(Failure::Layout);
    };
    let mut selected = None;
    let mut buffer = [0; 64 * 1024];
    for padding in 0..=if physical.len() == 1 { 0 } else { 2 } {
        let Some(chunk) = first.checked_sub(padding).filter(|chunk| *chunk >= 32) else {
            continue;
        };
        if total.div_ceil(chunk) != physical.len() as u64 {
            continue;
        }
        let lengths: BTreeMap<_, _> = physical
            .iter()
            .enumerate()
            .map(|(index, _)| {
                (
                    VolumeIndex(index as u32),
                    chunk.min(total - index as u64 * chunk),
                )
            })
            .collect();
        let mut tails = true;
        for (index, &posted) in physical.iter().enumerate() {
            let volume = VolumeIndex(index as u32);
            let logical = lengths[&volume];
            let Some(excess) = posted.checked_sub(logical).filter(|excess| *excess <= 2) else {
                tails = false;
                break;
            };
            let mut tail = [0; 2];
            if volumes.read_at(volume, logical, &mut tail[..excess as usize])? != excess as usize
                || tail != [0; 2]
            {
                tails = false;
                break;
            }
        }
        if !tails {
            continue;
        }
        let candidate: Arc<dyn Volumes> = Arc::new(Logical {
            source: Arc::clone(&volumes),
            lengths,
        });
        let joined = Joined::new(Arc::clone(&candidate));
        let mut at = 0;
        let mut crc = 0;
        while at < size {
            let count = (size - at).min(buffer.len() as u64) as usize;
            joined.read_exact(start + at, &mut buffer[..count])?;
            crc = crc32_combine(crc, crc32(&buffer[..count]), count as u64);
            at += count as u64;
        }
        if crc == expected {
            if selected.is_some() {
                return Err(Failure::Layout);
            }
            selected = Some(candidate);
        }
    }
    selected.ok_or(Failure::Layout)
}

fn invalid() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "7z inferred lengths have no unique proven layout",
    )
}

struct Logical {
    source: Arc<dyn Volumes>,
    lengths: BTreeMap<VolumeIndex, u64>,
}

impl Volumes for Logical {
    fn next(&self, after: Option<VolumeIndex>) -> io::Result<Option<VolumeIndex>> {
        self.source.next(after)
    }
    fn len(&self, volume: VolumeIndex) -> Option<u64> {
        self.lengths.get(&volume).copied()
    }
    fn settled(&self, volume: VolumeIndex) -> bool {
        self.source.settled(volume)
    }
    fn read_at(&self, volume: VolumeIndex, offset: u64, into: &mut [u8]) -> io::Result<usize> {
        let length = self.len(volume).ok_or_else(invalid)?;
        let count = length.saturating_sub(offset).min(into.len() as u64) as usize;
        self.source.read_at(volume, offset, &mut into[..count])
    }
    fn bridged(&self, lost: weaver_graph::Lost) {
        self.source.bridged(lost);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Memory {
        parts: Vec<Vec<u8>>,
        uncertain: bool,
    }
    impl Volumes for Memory {
        fn next(&self, after: Option<VolumeIndex>) -> io::Result<Option<VolumeIndex>> {
            let at = after.map_or(0, |volume| volume.0 + 1);
            Ok(((at as usize) < self.parts.len()).then_some(VolumeIndex(at)))
        }
        fn len(&self, volume: VolumeIndex) -> Option<u64> {
            self.parts
                .get(volume.0 as usize)
                .map(|bytes| bytes.len() as u64)
        }
        fn uncertain_lengths(&self) -> bool {
            self.uncertain
        }
        fn read_at(&self, volume: VolumeIndex, offset: u64, into: &mut [u8]) -> io::Result<usize> {
            let bytes = &self.parts[volume.0 as usize];
            let start = (offset.min(bytes.len() as u64)) as usize;
            let count = into.len().min(bytes.len() - start);
            into[..count].copy_from_slice(&bytes[start..start + count]);
            Ok(count)
        }
    }

    fn archive() -> Vec<u8> {
        let mut bytes: Vec<_> = (0..132).map(|at| (at * 17) as u8).collect();
        bytes[..8].copy_from_slice(b"7z\xbc\xaf\x27\x1c\0\x04");
        bytes[12..20].copy_from_slice(&96u64.to_le_bytes());
        bytes[20..28].copy_from_slice(&4u64.to_le_bytes());
        let end_crc = crc32(&bytes[128..]);
        bytes[28..32].copy_from_slice(&end_crc.to_le_bytes());
        let start_crc = crc32(&bytes[12..32]);
        bytes[8..12].copy_from_slice(&start_crc.to_le_bytes());
        bytes
    }

    #[test]
    fn uniquely_proven_geometry_preserves_posted_bytes() {
        let bytes = archive();
        let parts: Vec<_> = bytes.chunks(44).map(|part| [part, &[0]].concat()).collect();
        let source = Arc::new(Memory {
            parts: parts.clone(),
            uncertain: true,
        });
        let logical = resolve(source.clone()).unwrap();
        assert_eq!(logical.len(VolumeIndex(0)), Some(44));
        let mut read = vec![0; bytes.len()];
        Joined::new(logical).read_exact(0, &mut read).unwrap();
        assert_eq!(read, bytes);
        assert_eq!(source.parts, parts);
    }

    #[test]
    fn nonzero_suffix_is_never_padding() {
        let parts = archive()
            .chunks(44)
            .map(|part| [part, &[1]].concat())
            .collect();
        assert!(
            resolve(Arc::new(Memory {
                parts,
                uncertain: true
            }))
            .is_err()
        );
    }

    #[test]
    fn ambiguous_zero_tail_layout_is_refused() {
        let mut bytes = [0; 122];
        bytes[..8].copy_from_slice(b"7z\xbc\xaf\x27\x1c\0\x04");
        bytes[12..20].copy_from_slice(&88u64.to_le_bytes());
        let start_crc = crc32(&bytes[12..32]);
        bytes[8..12].copy_from_slice(&start_crc.to_le_bytes());
        let parts = vec![
            bytes[..42].to_vec(),
            bytes[42..84].to_vec(),
            bytes[84..].to_vec(),
        ];
        assert!(
            resolve(Arc::new(Memory {
                parts,
                uncertain: true
            }))
            .is_err()
        );
    }
}
