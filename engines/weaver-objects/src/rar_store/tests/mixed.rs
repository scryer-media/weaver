// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Spike: a set that is not solid, with stored files around a compressed one.
// A run asked for the compressed members alone decodes them reading only the
// bytes a store would keep in its envelope: every header block and the
// packed data of compressed members, never a byte of a stored member's data.

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::ops::Range;
use std::sync::{Arc, Mutex};

use unrar_rs::{KdfCache, RarVolumeFacts};
use weaver_graph::{Emit, VolumeIndex, Volumes};

use super::super::claim;
use super::writer::{Codec, Item, Options, Version, write};
use super::{fixture, unrar};
use crate::testing::payload;

// The volumes, answering only reads that lie wholly in the kept ranges and
// recording every other: volume, offset, length asked.
struct Kept {
    volumes: Vec<Vec<u8>>,
    kept: Vec<Vec<Range<u64>>>,
    outside: Mutex<Vec<(u32, u64, usize)>>,
}

impl Kept {
    fn holds(&self, volume: usize, range: Range<u64>) -> bool {
        range.is_empty()
            || self.kept[volume]
                .iter()
                .any(|kept| kept.start <= range.start && range.end <= kept.end)
    }
}

impl Volumes for Kept {
    fn next(&self, after: Option<VolumeIndex>) -> io::Result<Option<VolumeIndex>> {
        let next = after.map_or(0, |after| after.0 + 1);
        Ok(((next as usize) < self.volumes.len()).then_some(VolumeIndex(next)))
    }

    fn len(&self, volume: VolumeIndex) -> Option<u64> {
        self.volumes.get(volume.0 as usize).map(|v| v.len() as u64)
    }

    fn settled(&self, volume: VolumeIndex) -> bool {
        (volume.0 as usize) < self.volumes.len()
    }

    fn read_at(&self, volume: VolumeIndex, offset: u64, into: &mut [u8]) -> io::Result<usize> {
        let bytes = &self.volumes[volume.0 as usize];
        let end = (offset + into.len() as u64).min(bytes.len() as u64);
        let start = offset.min(end);
        if !self.holds(volume.0 as usize, start..end) {
            self.outside
                .lock()
                .unwrap()
                .push((volume.0, offset, into.len()));
            return Err(io::Error::other("a byte the envelope never kept"));
        }
        let taken = (end - start) as usize;
        into[..taken].copy_from_slice(&bytes[start as usize..end as usize]);
        Ok(taken)
    }
}

fn facts(bytes: &[u8]) -> RarVolumeFacts {
    claim::headers(bytes, None, Arc::new(KdfCache::new())).expect("a RAR volume")
}

// One volume's kept ranges, from the headers the store walks: everything
// but the data areas the headers name, and the data areas of compressed
// parts. Says the stored and service data bytes left out.
fn kept_of(bytes: &[u8]) -> (Vec<Range<u64>>, u64, u64) {
    let facts = facts(bytes);
    let mut areas: Vec<(Range<u64>, bool)> = facts
        .members
        .iter()
        .map(|member| {
            (
                member.data_offset..member.data_offset + member.data_size,
                member.compression_method != 0,
            )
        })
        .chain(facts.services.iter().map(|service| {
            (
                service.data_offset..service.data_offset + service.data_size,
                false,
            )
        }))
        .filter(|(range, _)| !range.is_empty())
        .collect();
    areas.sort_by_key(|(range, _)| range.start);
    let stored = facts
        .members
        .iter()
        .filter(|member| member.compression_method == 0)
        .map(|member| member.data_size)
        .sum();
    let service = facts.services.iter().map(|service| service.data_size).sum();
    let mut kept = Vec::new();
    let mut at = 0;
    for (range, compressed) in areas {
        assert!(range.start >= at, "areas overlap");
        if compressed {
            continue;
        }
        if range.start > at {
            kept.push(at..range.start);
        }
        at = range.end;
    }
    if (bytes.len() as u64) > at {
        kept.push(at..bytes.len() as u64);
    }
    (kept, stored, service)
}

// The set's members in archive order, a part that carries one on joined to
// it: each one's name and whether it is compressed.
fn members(volumes: &[Vec<u8>]) -> Vec<(String, bool)> {
    let mut out: Vec<(String, bool)> = Vec::new();
    for bytes in volumes {
        let facts = facts(bytes);
        let mut parts: Vec<_> = facts.members.iter().collect();
        parts.sort_by_key(|part| part.order);
        for part in parts {
            let compressed = part.compression_method != 0;
            if part.split_before {
                let last = out.last().expect("a part carries a member on");
                assert_eq!(last.1, compressed, "{}", part.name);
                continue;
            }
            out.push((part.name.clone(), compressed));
        }
    }
    out
}

struct Ran {
    // Decoded members by name, and whether each closed.
    decoded: BTreeMap<String, (Vec<u8>, bool)>,
    trouble: Vec<Emit>,
    outside: Vec<(u32, u64, usize)>,
    total: u64,
    kept: u64,
    stored: u64,
    service: u64,
}

// A run of the compressed members, or of every member when `all`.
fn run_compressed(volumes: &[Vec<u8>], all: bool) -> Ran {
    let listed = members(volumes);
    let only: BTreeSet<usize> = listed
        .iter()
        .enumerate()
        .filter(|(_, (_, compressed))| *compressed || all)
        .map(|(at, _)| at)
        .collect();
    assert!(!only.is_empty());
    let (mut kept, mut stored, mut service) = (Vec::new(), 0, 0);
    for bytes in volumes {
        let (ranges, s, v) = kept_of(bytes);
        kept.push(ranges);
        stored += s;
        service += v;
    }
    let total: u64 = volumes.iter().map(|v| v.len() as u64).sum();
    let kept_bytes = kept.iter().flatten().map(|r| r.end - r.start).sum();
    let double = Arc::new(Kept {
        volumes: volumes.to_vec(),
        kept,
        outside: Mutex::default(),
    });
    let mut emits = Vec::new();
    let mut sink = |emit| {
        emits.push(emit);
        true
    };
    let view: Arc<dyn Volumes> = double.clone();
    crate::rar_engine::run_only(None, only).run(view, &mut sink, weaver_graph::Budget::default());
    let mut names = BTreeMap::new();
    let mut decoded: BTreeMap<String, (Vec<u8>, bool)> = BTreeMap::new();
    let mut trouble = Vec::new();
    for emit in emits {
        match emit {
            Emit::Member { member, name, .. } => {
                names.insert(member.0, name.clone());
                decoded.insert(name, (Vec::new(), false));
            }
            Emit::Bytes {
                member,
                offset,
                bytes,
                ..
            } => {
                let got = &mut decoded.get_mut(&names[&member.0]).unwrap().0;
                assert_eq!(offset, got.len() as u64);
                got.extend_from_slice(&bytes);
            }
            Emit::MemberClosed(member) => {
                decoded.get_mut(&names[&member.0]).unwrap().1 = true;
            }
            Emit::Checksum { .. } => {}
            other => trouble.push(other),
        }
    }
    let outside = double.outside.lock().unwrap().clone();
    Ran {
        decoded,
        trouble,
        outside,
        total,
        kept: kept_bytes,
        stored,
        service,
    }
}

// The compressed file RAR itself wrote in a fixture: its name, plain bytes,
// packed bytes and how they are coded.
fn lifted(name: &str) -> (Vec<u8>, Vec<u8>, Codec) {
    let bytes = fixture(name);
    let plain = unrar(std::slice::from_ref(&bytes), None);
    assert_eq!(plain.len(), 1);
    let facts = facts(&bytes);
    let member = &facts.members[0];
    assert_ne!(member.compression_method, 0);
    assert!(!facts.is_solid && !member.compression_solid);
    let at = member.data_offset as usize;
    let packed = bytes[at..at + member.data_size as usize].to_vec();
    let codec = Codec {
        method: member.compression_method,
        version: member.compression_version,
        dict: member.dict_size,
    };
    (plain[0].1.clone(), packed, codec)
}

fn mixed_items(version: Version) -> (Vec<Item>, Vec<(&'static str, Vec<u8>)>) {
    let source = match version {
        Version::Rar5 => "extraction_profiles/rar5_lz.rar",
        Version::Rar4 => "extraction_profiles/rar4_lz.rar",
    };
    let (plain, packed, codec) = lifted(source);
    let items = vec![
        Item::File("alpha.mkv", payload(1, 10_000)),
        Item::Packed {
            name: "notes.nfo",
            plain: plain.clone(),
            packed,
            codec,
        },
        Item::File("beta.mkv", payload(2, 5_003)),
        Item::File("gamma.srt", payload(3, 300)),
    ];
    (items, vec![("notes.nfo", plain)])
}

fn check(label: &str, volumes: &[Vec<u8>], expected: &[(&str, Vec<u8>)]) {
    let ran = run_compressed(volumes, false);
    eprintln!(
        "{label}: volumes {} total {} kept {} stored data {} service data {} outside reads {:?}",
        volumes.len(),
        ran.total,
        ran.kept,
        ran.stored,
        ran.service,
        ran.outside
    );
    assert!(ran.trouble.is_empty(), "{label}: {:?}", ran.trouble);
    assert!(ran.outside.is_empty(), "{label}: {:?}", ran.outside);
    assert_eq!(ran.decoded.len(), expected.len(), "{label}");
    for (name, bytes) in expected {
        let (got, closed) = &ran.decoded[*name];
        assert!(*closed, "{label}: {name} closed");
        assert!(got == bytes, "{label}: {name} decoded");
    }
    assert_eq!(ran.service, 0, "{label}");
    assert_eq!(ran.kept, ran.total - ran.stored, "{label}");
    // The double has teeth: a run of every member reads a stored file's
    // data, which the envelope never kept, and the run ends there, as a run
    // whose read failed ends silently.
    let whole = run_compressed(volumes, true);
    assert!(!whole.outside.is_empty(), "{label}");
    assert!(whole.decoded.values().any(|(_, closed)| !closed), "{label}");
}

#[test]
fn a_run_of_the_compressed_members_reads_only_headers_and_packed_data() {
    for version in [Version::Rar5, Version::Rar4] {
        let (items, expected) = mixed_items(version);
        for (label, split) in [
            ("one volume", None),
            // The packed data runs from the first volume into the second.
            ("packed across a boundary", Some(45_000)),
            // Every file's data spans volumes, stored and packed alike.
            ("many volumes", Some(6_000)),
        ] {
            let mut options = Options::new(version);
            options.split = split;
            let volumes = write(&items, &options);
            if split.is_some() {
                assert!(volumes.len() > 1);
            }
            check(&format!("{version:?} {label}"), &volumes, &expected);
        }
    }
}

// Sets RAR itself wrote: two stored files, then a compressed one.
#[test]
fn a_real_mixed_set_decodes_its_compressed_member_from_headers_and_packed_data() {
    for name in [
        "extraction_profiles/rar5_multifile_lz.rar",
        "extraction_profiles/rar4_multifile_lz.rar",
    ] {
        let bytes = fixture(name);
        let all = unrar(std::slice::from_ref(&bytes), None);
        let listed = members(std::slice::from_ref(&bytes));
        let expected: Vec<(&str, Vec<u8>)> = all
            .iter()
            .filter(|(member, _)| {
                listed
                    .iter()
                    .any(|(listed, compressed)| listed == member && *compressed)
            })
            .map(|(member, plain)| (member.as_str(), plain.clone()))
            .collect();
        assert!(listed.iter().any(|(_, compressed)| !compressed), "{name}");
        check(name, std::slice::from_ref(&bytes), &expected);
    }
}
