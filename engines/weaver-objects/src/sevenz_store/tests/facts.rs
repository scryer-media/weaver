// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use sevenz_turbo::{ArchiveLimits, Password};
use weaver_graph::Kind;

use super::super::{Lock, SevenZipStore};
use super::{PASSWORD, aes, archive, copy, split, store};
use crate::testing::replay::{Step, contains, frames, restores_alike};

fn steps<'a>(volumes: &'a [(String, Vec<u8>)], frame: usize) -> Vec<Step<'a>> {
    let mut steps = Vec::new();
    for (index, (name, bytes)) in volumes.iter().enumerate() {
        steps.push(Step::Bound(index as u32, name));
        steps.push(Step::Sized(index as u32, bytes.len() as u64));
    }
    for (index, (_, bytes)) in volumes.iter().enumerate() {
        steps.extend(frames(index as u32, bytes, frame));
    }
    steps
}

#[test]
fn a_stored_7z_restored_before_its_end_header_goes_on_as_the_live_one() {
    let volumes = vec![("alpha.7z".to_owned(), archive(copy(), false))];
    let steps = steps(&volumes, 4096);
    for cut in [2, steps.len() / 2, steps.len() - 1] {
        restores_alike(Box::new(store()), &steps, cut, &[]);
    }
}

#[test]
fn a_split_7z_restored_between_its_volumes_goes_on_as_the_live_one() {
    let volumes = split(&archive(copy(), false), 3);
    let steps = steps(&volumes, 2048);
    restores_alike(Box::new(store()), &steps, steps.len() / 2, &[]);
}

// Opened, then restored: the folder's cipher comes back sealed and opens
// again on the password offered anew, and the blob holds neither the key
// nor the password.
#[test]
fn an_encrypted_7z_restored_mid_folder_opens_again_and_its_facts_hold_no_key() {
    let volumes = vec![("alpha.7z".to_owned(), archive(aes(), false))];
    let mut steps = steps(&volumes, 1000);
    // The tail first, with the end header and the proof file, then the
    // password, so the folder is open from there on.
    steps[2..].rotate_right(1);
    steps.insert(3, Step::Password(PASSWORD));
    let (facts, _, _) = restores_alike(
        Box::new(store()),
        &steps,
        steps.len() / 2,
        &[Step::Password(PASSWORD)],
    );
    let kept: SevenZipStore = facts.read(Kind::SevenZipStore).expect("decodes");
    let shape: Vec<_> = kept.folders.iter().map(|lock| match lock { Lock::Plain => 'p', Lock::Locked => 'l', Lock::Open(_) => 'o', Lock::Sealed(_) => 's' }).collect();
    assert!(shape.contains(&'s'), "{shape:?} map {} proof {:?}", kept.map.is_some(), kept.proof);
    let properties = kept
        .map
        .as_ref()
        .and_then(|map| map.folders[0].aes)
        .expect("an AES folder");
    let key = properties
        .derive_key(&Password::new(PASSWORD), &ArchiveLimits::default())
        .expect("derives");
    assert!(!contains(&facts.bytes, key.as_bytes()));
    assert!(!contains(&facts.bytes, PASSWORD.as_bytes()));
}
