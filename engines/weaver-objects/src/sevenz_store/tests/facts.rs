// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use sevenz_turbo::{ArchiveLimits, Password};
use weaver_graph::{Budget, ByteRange, Facts, Kind, Object};

use super::super::{Lock, SevenZipStore};
use super::owner::Owner;
use super::{PASSWORD, aes, archive, copy, end_header, no_trouble, store, streamed};
use crate::crypt::{Cbc, Padding};
use crate::testing::replay::{Step, contains, frames, misfit_paddings, restores_alike};

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

// Opened, then restored: the folder's cipher comes back sealed and opens
// again on the password offered anew, and the blob holds neither the key
// nor the password.
#[test]
fn an_encrypted_7z_restored_mid_folder_opens_again_and_its_facts_hold_no_key() {
    let volumes = vec![("alpha.7z".to_owned(), archive(aes(), false))];
    let mut steps = steps(&volumes, 1000);
    // The signature, then the tail with the end header and the proof file,
    // then the password, so the folder is open from there on.
    steps[3..].rotate_right(2);
    steps.insert(5, Step::Password(PASSWORD));
    let (facts, _, _) = restores_alike(
        Box::new(store()),
        &steps,
        steps.len() / 2,
        &[Step::Password(PASSWORD)],
    );
    let kept: SevenZipStore = facts.read(Kind::SevenZipStore).expect("decodes");
    assert!(
        kept.folders
            .iter()
            .any(|lock| matches!(lock, Lock::Sealed(..)))
    );
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

// Opened under a password its proof file proved, then restored: the
// password offered anew stands proved, so a file whose checksum fails on
// verified bytes after the restart is damage, as it is to the live store,
// and nothing is asked for again.
#[test]
fn a_proved_password_stays_proved_across_a_restore() {
    let mut bytes = archive(aes(), false);
    // A byte of the first file, which the proof file is not.
    bytes[6500] ^= 0x5a;
    let volumes = vec![("alpha.7z".to_owned(), bytes)];
    let mut steps = steps(&volumes, 1000);
    steps[3..].rotate_right(2);
    steps.insert(5, Step::Password(PASSWORD));
    let cut = steps.len() / 2;
    let damaged = steps
        .iter()
        .position(|step| matches!(step, Step::Bytes(0, 6000, _)))
        .expect("the frame with the bad byte");
    assert!(damaged >= cut, "the bad byte arrives after the restart");
    let (facts, _, restored) =
        restores_alike(Box::new(store()), &steps, cut, &[Step::Password(PASSWORD)]);
    let kept: SevenZipStore = facts.read(Kind::SevenZipStore).expect("decodes");
    assert!(kept.proven);
    let after: SevenZipStore = restored.facts().read(Kind::SevenZipStore).expect("decodes");
    assert!(after.proven);
    assert!(
        after.members.iter().any(|said| said.damaged),
        "the failure is damage"
    );
    assert!(
        after
            .folders
            .iter()
            .all(|lock| matches!(lock, Lock::Open(..) | Lock::Sealed(..)))
    );
}

// A folder restored with a chain or a padding that does not fit its stream
// opens afresh with no padding, and its bytes said again come out whole.
#[test]
fn a_restored_chain_or_padding_that_does_not_fit_opens_its_folder_afresh() {
    let bytes = archive(aes(), false);
    let tail = end_header(&bytes).start;
    let [oversized, marked] = misfit_paddings();
    // A padding past its stream, or one past what it holds; or a chain of
    // another shape than the folder's.
    let cases: [(Option<Padding>, bool); 3] = [
        (Some(oversized), false),
        (Some(marked), false),
        (None, true),
    ];
    for (padding, other_chain) in cases {
        let mut owner = Owner::single(store(), bytes.clone());
        // The front, then the back half with the end header and the proof
        // file, then the password, then the folder short of its middle.
        owner.deliver(0, ByteRange::new(0, 1000));
        owner.deliver(0, ByteRange::new(tail / 2 + 1000, bytes.len() as u64));
        owner.offer(PASSWORD);
        owner.deliver(0, ByteRange::new(1000, tail / 2));
        assert!(matches!(owner.object.folders[0], Lock::Open(..)));
        let facts = owner.object.facts();
        let mut kept: SevenZipStore = facts.read(Kind::SevenZipStore).expect("decodes");
        let folder = &kept.map.as_ref().expect("the map").folders[0];
        let (cipher_len, plain_len) = (folder.pack.len(), folder.plain_len);
        let Lock::Sealed(chain, kept_padding) = &mut kept.folders[0] else {
            panic!("an open folder is kept sealed");
        };
        if let Some(padding) = padding {
            *kept_padding = padding;
        }
        if other_chain {
            let other = Cbc::new(&[0; 32], [0; 16], cipher_len + 16, plain_len + 16)
                .expect("a chain of another shape");
            **chain = other.chain();
        }
        let facts = Facts::of(Kind::SevenZipStore, &kept);
        owner.object = facts
            .read::<SevenZipStore>(Kind::SevenZipStore)
            .expect("the facts restore")
            .within(Budget::default());
        owner.offer(PASSWORD);
        let Lock::Open(_, padding) = &owner.object.folders[0] else {
            panic!("the folder opens");
        };
        assert_eq!(**padding, Padding::default());
        owner.whole(1000);
        no_trouble(&owner);
        streamed(&owner);
    }
}
