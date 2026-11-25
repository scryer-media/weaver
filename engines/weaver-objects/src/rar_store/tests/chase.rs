// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A run that chases the download reads what the shelf holds as it comes.
// Whatever order the bytes come in, a compressed member's packed data
// reaches the shelf only once the store has laid the member's header and
// announced it as made, at the member's archive index under its name: a run
// that reads it there makes a member the namespace already holds.

use std::sync::Arc;

use unrar_rs::KdfCache;
use weaver_graph::{ByteRange, Provenance};

use super::PASSWORD;
use super::mixed::packed;
use super::owner::Owner;
use super::store;
use super::writer::{Item, Options, Version, literal, write};
use crate::rar_store::claim;
use crate::testing::payload;

// A compressed member's part: its archive index, its name, and where its
// packed data lies in one volume.
struct Part {
    index: usize,
    name: String,
    volume: u32,
    data: ByteRange,
}

// Every compressed part of the set, read off its whole volumes in the order
// the store holds them, as a run reading them there numbers and names the
// members: a part that carries a member on is that member's.
fn compressed_parts(volumes: &[Vec<u8>], password: Option<&str>) -> Vec<Part> {
    let mut parts = Vec::new();
    let mut index = 0usize;
    let mut name = String::new();
    for (volume, bytes) in volumes.iter().enumerate() {
        let facts = claim::headers(bytes, password, Arc::new(KdfCache::new())).expect("a volume");
        let mut members: Vec<_> = facts.members.iter().collect();
        members.sort_by_key(|member| member.order);
        for member in members {
            if !member.split_before || index == 0 {
                index += 1;
                name.clone_from(&member.name);
            }
            if member.compression_method != 0 && member.data_size > 0 {
                parts.push(Part {
                    index: index - 1,
                    name: name.clone(),
                    volume: volume as u32,
                    data: ByteRange::at(member.data_offset, member.data_size),
                });
            }
        }
    }
    parts
}

// Every compressed part some of whose packed data a run could read from the
// shelf now, whose member the store has not announced as made at its index
// under its name.
fn unannounced(owner: &Owner, parts: &[Part]) -> Vec<String> {
    parts
        .iter()
        .filter(|part| owner.shelf_verified_in(part.volume, part.data))
        .filter(|part| {
            !owner
                .object
                .entries
                .get(part.index)
                .is_some_and(|entry| entry.made && entry.name == part.name)
                || !owner.members.get(&(part.index as u32)).is_some_and(|got| {
                    got.name == crate::rar_store::made::name(&part.name) || got.name == part.name
                })
        })
        .map(|part| {
            format!(
                "{} (index {}) in volume {}",
                part.name, part.index, part.volume
            )
        })
        .collect()
}

// How far the walks read past what the layout holds: volumes whose headers
// were read that the layout does not hold yet.
fn lag(owner: &Owner) -> usize {
    let laid = owner.object.laid_through().unwrap_or(0);
    owner
        .object
        .volumes
        .iter()
        .filter(|(index, found)| index.0 >= laid && found.walk.facts.is_some())
        .count()
}

// What a delivery order saw: the steps checked, the most volumes walked
// past the layout at once, and every step where packed data stood on the
// shelf ahead of its member's announcement.
#[derive(Default)]
struct Seen {
    steps: usize,
    lag: usize,
    // Steps at which some packed data stood on the shelf.
    shelved: usize,
    early: Vec<String>,
}

impl Seen {
    fn check(&mut self, owner: &Owner, parts: &[Part], step: &str) {
        self.steps += 1;
        self.lag = self.lag.max(lag(owner));
        if parts
            .iter()
            .any(|part| owner.shelf_verified_in(part.volume, part.data))
        {
            self.shelved += 1;
        }
        for part in unannounced(owner, parts) {
            self.early.push(format!("{step}: {part}"));
        }
    }
}

fn two_compressed(version: Version) -> Vec<Item> {
    let (notes, _) = packed(version, "notes.nfo");
    vec![
        Item::File("alpha.mkv", payload(1, 10_000)),
        notes,
        Item::File("beta.mkv", payload(2, 5_003)),
        literal(version, "inner.bin", &payload(4, 7_000)),
        Item::File("gamma.srt", payload(3, 300)),
    ]
}

// The volumes in `order`, each in frames of `frame`, each frame forwards or
// from the tail, every step checked; then each closed, checked again.
fn delivered(
    owner: &mut Owner,
    parts: &[Part],
    order: &[usize],
    frame: u64,
    tail_first: bool,
) -> Seen {
    let mut seen = Seen::default();
    for &volume in order {
        let length = owner.volumes[volume].1.len() as u64;
        let mut cuts: Vec<ByteRange> = Vec::new();
        let mut start = 0;
        while start < length {
            let end = (start + frame).min(length);
            cuts.push(ByteRange::new(start, end));
            start = end;
        }
        if tail_first {
            cuts.reverse();
        }
        for cut in cuts {
            owner.deliver(volume, cut);
            seen.check(owner, parts, &format!("volume {volume} {cut:?}"));
        }
    }
    for &volume in order {
        owner.close(volume);
        seen.check(owner, parts, &format!("close {volume}"));
    }
    seen
}

fn orders(count: usize) -> Vec<(&'static str, Vec<usize>)> {
    let forwards: Vec<usize> = (0..count).collect();
    let backwards: Vec<usize> = (0..count).rev().collect();
    // Every other volume first, then the rest: each later one arrives with
    // a gap before it.
    let gapped: Vec<usize> = (0..count)
        .filter(|v| v % 2 == 1)
        .chain((0..count).filter(|v| v % 2 == 0))
        .collect();
    vec![
        ("in order", forwards),
        ("last first", backwards),
        ("odd first", gapped),
    ]
}

// Volumes named in the set's numbering, posted in order, last first and
// with gaps, each in frames forwards and from its tail: no packed byte is
// on the shelf before its member is announced as made.
#[test]
fn packed_data_reaches_the_shelf_only_after_its_member_is_announced() {
    let mut lagged = 0;
    for version in [Version::Rar5, Version::Rar4] {
        for (enciphered, password, headers) in [
            (false, None, false),
            (true, Some(PASSWORD), false),
            (true, Some(PASSWORD), true),
        ] {
            let mut options = Options::new(version);
            options.split = Some(6_000);
            options.numbered = true;
            options.password = password;
            options.encrypt_headers = headers;
            let volumes = write(&two_compressed(version), &options);
            assert!(volumes.len() >= 4);
            let parts = compressed_parts(&volumes, password);
            assert!(parts.len() >= 2, "{version:?}");
            for (order_label, order) in orders(volumes.len()) {
                for tail_first in [false, true] {
                    let label = format!(
                        "{version:?} enciphered {enciphered} headers {headers} {order_label} tail first {tail_first}"
                    );
                    let mut owner = Owner::named(store(), volumes.clone());
                    if let Some(password) = password {
                        owner.offer(password);
                    }
                    let seen = delivered(&mut owner, &parts, &order, 512, tail_first);
                    assert!(seen.early.is_empty(), "{label}: {:?}", seen.early);
                    assert!(seen.shelved > 0, "{label}");
                    lagged = lagged.max(seen.lag);
                    assert!(owner.object.runs(), "{label}");
                }
            }
        }
    }
    // Some order ran the walks ahead of the layout.
    assert!(lagged > 0);
}

// A nameless RAR4 set whose end records state no number, its middle
// volumes held on trust where their links put them, posted in order: the
// walks run ahead of the layout, and still no packed byte is on the shelf
// before its member is announced.
#[test]
fn packed_data_of_a_nameless_set_reaches_the_shelf_only_after_its_member_is_announced() {
    let mut options = Options::new(Version::Rar4);
    options.split = Some(6_000);
    let volumes = write(&two_compressed(Version::Rar4), &options);
    let parts = compressed_parts(&volumes, None);
    let named: Vec<(String, Vec<u8>)> = volumes
        .iter()
        .enumerate()
        .map(|(index, bytes)| (format!("{:x}3c9e1f", index * 7 + 11), bytes.clone()))
        .collect();
    for tail_first in [false, true] {
        let mut owner = Owner::new(store(), named.clone());
        let order: Vec<usize> = (0..volumes.len()).collect();
        let seen = delivered(&mut owner, &parts, &order, 512, tail_first);
        assert!(
            seen.early.is_empty(),
            "tail first {tail_first}: {:?}",
            seen.early
        );
        assert!(seen.shelved > 0);
        assert!(owner.object.runs(), "tail first {tail_first}");
    }
}

// Headers first posted damaged, naming the compressed member otherwise,
// then repaired, in one volume and across several: what is on the shelf
// verified is never a member's packed data ahead of its announcement under
// the name the repaired header gives it.
#[test]
fn packed_data_under_a_repaired_header_reaches_the_shelf_only_after_its_member_is_announced() {
    for version in [Version::Rar5, Version::Rar4] {
        for split in [None, Some(6_000)] {
            let label = format!("{version:?} {split:?}");
            let mut options = Options::new(version);
            options.split = split;
            options.numbered = true;
            let real = write(&two_compressed(version), &options);
            let mut other = two_compressed(version);
            let Item::Packed { name, .. } = &mut other[1] else {
                unreachable!()
            };
            *name = "notez.nfo";
            let wrong = write(&other, &options);
            assert_eq!(real.len(), wrong.len());
            let parts = compressed_parts(&real, None);
            let mut owner = Owner::named(store(), real.clone());
            let mut seen = Seen::default();
            for volume in 0..real.len() {
                let length = real[volume].len() as u64;
                assert_eq!(length, wrong[volume].len() as u64);
                owner.deliver_as(
                    volume,
                    ByteRange::new(0, length),
                    Provenance::Damaged,
                    Some(&wrong[volume]),
                );
                seen.check(&owner, &parts, &format!("{label}: damaged {volume}"));
            }
            for volume in 0..real.len() {
                owner.close(volume);
                seen.check(&owner, &parts, &format!("{label}: close {volume}"));
            }
            for volume in 0..real.len() {
                let first = real[volume]
                    .iter()
                    .zip(&wrong[volume])
                    .position(|(a, b)| a != b);
                if let Some(first) = first {
                    let first = first as u64;
                    let header = ByteRange::new(first.saturating_sub(64), first + 64);
                    owner.deliver_as(volume, header, Provenance::Repaired, None);
                    seen.check(&owner, &parts, &format!("{label}: header of {volume}"));
                }
                let length = real[volume].len() as u64;
                let mut start = 0;
                while start < length {
                    let end = (start + 512).min(length);
                    owner.deliver_as(
                        volume,
                        ByteRange::new(start, end),
                        Provenance::Repaired,
                        None,
                    );
                    seen.check(
                        &owner,
                        &parts,
                        &format!("{label}: repaired {volume} at {start}"),
                    );
                    start = end;
                }
            }
            assert!(seen.early.is_empty(), "{label}: {:?}", seen.early);
            assert!(seen.shelved > 0, "{label}");
            assert!(owner.object.runs(), "{label}");
        }
    }
}

// A nameless RAR4 set posted with two neighbouring volumes under each
// other's numbers, at every place in the set: the names hold each where the
// other belongs, on trust, until the layout refutes the place. Posted in
// order, closed, then the job goes quiet: no packed byte is on the shelf
// ahead of its member's announcement, at the place the store holds it.
#[test]
fn packed_data_of_a_set_held_out_of_place_reaches_the_shelf_only_after_its_member_is_announced() {
    let mut options = Options::new(Version::Rar4);
    options.split = Some(6_000);
    let real = write(&two_compressed(Version::Rar4), &options);
    let count = real.len();
    let mut cuts = 0;
    let mut retracts = 0;
    let mut lagged = 0;
    for swap in 1..count - 1 {
        // Position k is posted as part k + 1 and holds the real volume perm[k].
        let perm: Vec<usize> = (0..count)
            .map(|k| match k {
                _ if k == swap => swap + 1,
                _ if k == swap + 1 => swap,
                _ => k,
            })
            .collect();
        let held: Vec<Vec<u8>> = perm.iter().map(|&r| real[r].clone()).collect();
        let parts = compressed_parts(&held, None);
        let posted: Vec<(String, Vec<u8>)> = perm
            .iter()
            .enumerate()
            .map(|(k, &r)| (format!("alpha.part{}.rar", k + 1), real[r].clone()))
            .collect();
        let mut owner = Owner::new(store(), posted);
        crate::rar_store::counts::take();
        let order: Vec<usize> = (0..count).collect();
        let mut seen = delivered(&mut owner, &parts, &order, 512, false);
        owner.quiet();
        seen.check(&owner, &parts, "quiet");
        cuts += crate::rar_store::counts::take().cut;
        retracts += owner
            .said
            .iter()
            .filter(|emit| matches!(emit, weaver_graph::Emit::Retract(_)))
            .count();
        lagged = lagged.max(seen.lag);
        assert!(seen.early.is_empty(), "swap {swap}: {:?}", seen.early);
    }
    // The layout ran behind the walks, and no place was ever cut or member
    // retracted: what stood held stood.
    assert!(lagged > 0);
    assert_eq!((cuts, retracts), (0, 0));
}
