// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A nameless RAR4 volume no front places, whose end record would number it:
// the RAR engine asks for that record (`Claim::Wants`), and it and the RAR
// store place the volume by the number it states once they are offered it
// with it. The store asks for nothing itself. A volume that states its
// place, a named one and a RAR5 one, asks for nothing.

use weaver_graph::{ByteRange, Claim, Clean, Object, VolumeIndex};

use super::RarStore;
use crate::rar_engine::RarEngine;

// The rarlab set `rar4_solid_four` (or its RAR5 counterpart), each volume
// under a name that numbers nothing, or under its own name when `named`.
fn set(format: &str, named: bool) -> Vec<(String, Vec<u8>)> {
    (1..=4)
        .map(|part| {
            let file = format!("{format}_solid_four.part0{part}.rar");
            let bytes = super::fixture(&format!("extraction_profiles/{file}"));
            let name = match named {
                true => file,
                false => format!("{:016x}", 0x5eed_0000_u64 + part),
            };
            (name, bytes)
        })
        .collect()
}

// The first volume bound with a front cut before its member headers, as a
// store that yields at the solid flag hands it on: nothing in it links the
// second volume.
fn holding(object: &mut dyn Object, set: &[(String, Vec<u8>)]) {
    let (name, bytes) = &set[0];
    object.bound(VolumeIndex(0), name, &bytes[..20], Some(bytes.len() as u64));
}

// The second volume's claim on `object`, by its front short of its end
// record, and then with the tail that claim asked for.
fn claims(object: &mut dyn Object, set: &[(String, Vec<u8>)]) -> (Claim, Option<Claim>) {
    let (name, bytes) = &set[1];
    let length = Some(bytes.len() as u64);
    let front = &bytes[..4096];
    let asked = object.claim(name, Clean::start(front), length);
    let told = match &asked {
        Claim::Wants(range) => {
            let tail = &bytes[range.start as usize..range.end as usize];
            let clean = Clean {
                tail,
                ..Clean::start(front)
            };
            Some(object.claim(name, clean, length))
        }
        _ => None,
    };
    (asked, told)
}

fn end_record(set: &[(String, Vec<u8>)]) -> ByteRange {
    let length = set[1].1.len() as u64;
    ByteRange::new(length - 20, length)
}

// (vi) The engine holding the first volume alone asks for the second's end
// record, and places it at 1 by it.
#[test]
fn the_engine_asks_a_nameless_rar4_volume_for_its_end_record_and_places_it_by_it() {
    let set = set("rar4", false);
    let mut engine = RarEngine::new();
    holding(&mut engine, &set);
    let (asked, told) = claims(&mut engine, &set);
    assert_eq!(asked, Claim::Wants(end_record(&set)));
    assert_eq!(told, Some(vec![VolumeIndex(1)].into()));
}

// (vii) The store asks for nothing, but offered the volume with the end
// record the engine asked for, places it at 1 by it.
#[test]
fn the_store_places_a_nameless_rar4_volume_by_the_end_record_it_is_handed() {
    let set = set("rar4", false);
    let (name, bytes) = &set[1];
    let length = Some(bytes.len() as u64);
    let front = &bytes[..4096];
    let mut store = RarStore::new();
    holding(&mut store, &set);
    assert_eq!(
        store.claim(name, Clean::start(front), length),
        Claim::default()
    );
    let range = end_record(&set);
    let clean = Clean {
        tail: &bytes[range.start as usize..range.end as usize],
        ..Clean::start(front)
    };
    assert_eq!(
        store.claim(name, clean, length),
        Claim::Places(vec![VolumeIndex(1)])
    );
}

// A tail that states no number is all there is: offered with it, neither
// places the volume nor asks again.
#[test]
fn a_tail_that_numbers_nothing_is_not_asked_for_again() {
    let set = set("rar4", false);
    let (name, bytes) = &set[1];
    let length = Some(bytes.len() as u64);
    let front = &bytes[..4096];
    let nothing = [0u8; 20];
    let clean = Clean {
        tail: &nothing,
        ..Clean::start(front)
    };
    let mut engine = RarEngine::new();
    holding(&mut engine, &set);
    assert_eq!(engine.claim(name, clean, length), Claim::default());
    let mut store = RarStore::new();
    holding(&mut store, &set);
    assert_eq!(store.claim(name, clean, length), Claim::default());
}

// (viii) A named volume and a RAR5 one state their places: neither object
// asks for anything.
#[test]
fn a_named_or_rar5_volume_asks_for_nothing() {
    for (format, named) in [("rar4", true), ("rar5", false), ("rar5", true)] {
        let set = set(format, named);
        let objects: [Box<dyn Object>; 2] = [Box::new(RarEngine::new()), Box::new(RarStore::new())];
        for mut object in objects {
            holding(&mut *object, &set);
            let (asked, told) = claims(&mut *object, &set);
            assert!(
                !matches!(asked, Claim::Wants(_)),
                "{format} named {named}: {asked:?}"
            );
            assert_eq!(told, None);
        }
    }
}
