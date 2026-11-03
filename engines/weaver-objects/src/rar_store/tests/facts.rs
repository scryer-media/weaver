// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use unrar_rs::KdfCache;
use weaver_graph::{Facts, Kind, RestoreError};

use super::super::facts::{KeyTuple, StoreFacts};
use super::writer::{Options, Version, write};
use super::{PASSWORD, encrypted, items, split, split_encrypted, store};
use crate::testing::replay::{Step, contains, frames, replay, restores_alike};

fn named(volumes: Vec<Vec<u8>>) -> Vec<(String, Vec<u8>)> {
    let count = volumes.len();
    volumes
        .into_iter()
        .enumerate()
        .map(|(index, bytes)| match count {
            1 => ("alpha.rar".to_owned(), bytes),
            _ => (format!("alpha.part{}.rar", index + 1), bytes),
        })
        .collect()
}

// Every volume bound and sized, then each delivered in frames, in order.
fn steps<'a>(volumes: &'a [(String, Vec<u8>)], frame: usize, password: bool) -> Vec<Step<'a>> {
    let mut steps = Vec::new();
    for (index, (name, bytes)) in volumes.iter().enumerate() {
        steps.push(Step::Bound(index as u32, name));
        steps.push(Step::Sized(index as u32, bytes.len() as u64));
    }
    if password {
        steps.push(Step::Password(PASSWORD));
    }
    for (index, (_, bytes)) in volumes.iter().enumerate() {
        steps.extend(frames(index as u32, bytes, frame));
        steps.push(Step::Closed(index as u32));
    }
    steps
}

// The step after volume `index`'s first frame, for a volume the set has.
fn into_volume(steps: &[Step<'_>], index: u32) -> Option<usize> {
    let at = steps
        .iter()
        .position(|step| matches!(step, Step::Bytes(volume, 0, _) if *volume == index))?;
    Some(at + 1)
}

// A cut after the first header, mid-member, and on each side of a volume
// boundary, with the password offered again and the placed bytes replayed
// as a restart does.
fn every_cut(volumes: &[(String, Vec<u8>)], frame: usize, password: bool) -> Vec<Facts> {
    let steps = steps(volumes, frame, password);
    let first = into_volume(&steps, 0).expect("the set has a first volume");
    let mut cuts = vec![first, steps.len() / 3, steps.len() - 1];
    if let Some(second) = into_volume(&steps, 1) {
        cuts.extend([second - 1, second]);
    }
    cuts.into_iter()
        .map(|cut| {
            let mut again = Vec::new();
            if password {
                again.push(Step::Password(PASSWORD));
            }
            again.extend(replay(&steps[..cut]));
            restores_alike(Box::new(store()), &steps, cut, &again).0
        })
        .collect()
}

#[test]
fn a_split_rar_restored_at_every_point_goes_on_as_the_live_one() {
    for version in [Version::Rar5, Version::Rar4] {
        let volumes = named(write(&items(), &split(version)));
        every_cut(&volumes, 512, false);
    }
}

#[test]
fn a_rar_with_recovery_records_restored_says_each_once() {
    let mut options = split(Version::Rar5);
    options.recovery = true;
    let volumes = named(write(&items(), &options));
    every_cut(&volumes, 1024, false);
}

// The key tuple is what the header states, and the blob holds neither the
// key, the hash key nor the password.
fn keyless(options: &Options) {
    let volumes = named(write(&items(), options));
    let kdf = KdfCache::new();
    for facts in every_cut(&volumes, 700, true) {
        assert!(!contains(&facts.bytes, PASSWORD.as_bytes()));
        let kept: StoreFacts = facts.read(Kind::RarStore).expect("decodes");
        for entry in &kept.entries {
            match entry.keying {
                Some(KeyTuple::Rar5(tuple)) => {
                    let material = kdf
                        .derive_material_rar5(PASSWORD, &tuple.salt, tuple.kdf_count_lg2)
                        .expect("derives");
                    assert!(!contains(&facts.bytes, &material.key));
                    assert!(!contains(&facts.bytes, &material.hash_key));
                }
                Some(KeyTuple::Rar4(salt)) => {
                    let (key, iv) = kdf.derive_key_rar4(PASSWORD, salt.as_ref());
                    assert!(!contains(&facts.bytes, &key));
                    assert!(!contains(&facts.bytes, &iv));
                }
                None => panic!("an encrypted member records its key tuple"),
            }
        }
    }
}

#[test]
fn an_encrypted_rar_restored_opens_again_and_its_facts_hold_no_key() {
    keyless(&split_encrypted(Version::Rar5, false));
    keyless(&split_encrypted(Version::Rar4, false));
    keyless(&split_encrypted(Version::Rar5, true));
    keyless(&encrypted(Version::Rar5, false));
}

#[test]
fn rar_facts_of_another_version_are_refused() {
    let facts = Facts {
        kind: Kind::RarStore,
        version: 99,
        bytes: Vec::new(),
        parts: Vec::new(),
    };
    assert!(matches!(
        crate::restore(&facts, weaver_graph::Budget::default()),
        Err(RestoreError::Version { .. })
    ));
}

// Facts the version before each volume kept the front it was bound with
// are refused, not read with the front missing.
#[test]
fn facts_of_the_previous_version_are_refused() {
    use weaver_graph::{Object, VolumeIndex};
    let volumes = named(write(&items(), &split(Version::Rar5)));
    let mut object = store();
    object.bound(VolumeIndex(0), &volumes[0].0, &volumes[0].1, None);
    let mut facts = object.facts();
    assert_eq!(facts.version, 3);
    assert!(crate::restore(&facts, weaver_graph::Budget::default()).is_ok());
    facts.version = 2;
    assert!(matches!(
        crate::restore(&facts, weaver_graph::Budget::default()),
        Err(RestoreError::Version {
            kind: Kind::RarStore,
            version: 2
        })
    ));
}
