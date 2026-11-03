// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

#[test]
fn a_recovery_name_corrects_an_ended_but_unnumbered_rar4_volume() {
    let mut options = Options::new(Version::Rar4);
    options.split = Some(512);
    let volumes = write(&[Item::File("member.bin", payload(113, 1536))], &options);
    assert_eq!(volumes.len(), 3);
    let mut owner = Owner::new(
        store(),
        vec![
            ("opaque.001".to_owned(), volumes[0].clone()),
            ("opaque.003".to_owned(), volumes[2].clone()),
        ],
    );
    owner.whole(usize::MAX >> 1);
    let last = owner.object.volumes.get(&VolumeIndex(1)).unwrap();
    assert!(last.walk.ended, "the parse reached the end");
    assert!(
        !last
            .walk
            .facts
            .as_ref()
            .is_some_and(super::super::claim::states_position)
    );
    assert!(
        owner.object.carried(VolumeIndex(1)).is_some(),
        "unmapped source can travel"
    );
    let mut carriable = Vec::new();
    owner.object.carriable(&mut carriable);
    assert_eq!(carriable, vec![VolumeIndex(1)], "the mapped first stays");
    let restored = crate::restore(&owner.object.facts(), weaver_graph::Budget::default()).unwrap();
    let mut again = Vec::new();
    restored.carriable(&mut again);
    assert_eq!(again, carriable, "a restore keeps what can travel");
    let mut out = Vec::new();
    owner
        .object
        .renamed(VolumeIndex(1), "archive.part03.rar", &mut out);
    assert!(
        out.iter()
            .any(|emit| matches!(emit, Emit::GiveBack(VolumeIndex(1)))),
        "corrections={out:?}"
    );
}

#[test]
fn a_canonical_name_places_a_volume_when_held_labels_cannot_number_the_set() {
    for version in [Version::Rar4, Version::Rar5] {
        let mut options = Options::new(version);
        options.split = Some(512);
        let volumes = write(&[Item::File("member.bin", payload(115, 1536))], &options);
        let foreign = write(&[Item::File("foreign.bin", payload(116, 1536))], &options);
        for (engine, first_name) in [
            (false, "opaque.001"),
            (true, "opaque.001"),
            (false, "archive.part01.rar"),
            (true, "archive.part01.rar"),
        ] {
            let mut object: Box<dyn Object> = if engine {
                Box::new(crate::rar_engine::RarEngine::new())
            } else {
                Box::new(store())
            };
            object.bound(
                VolumeIndex(0),
                "opaque.001",
                &volumes[0],
                Some(volumes[0].len() as u64),
            );
            assert_eq!(
                object.claim(
                    "archive.part02.rar",
                    &foreign[1],
                    Some(foreign[1].len() as u64)
                ),
                Vec::<VolumeIndex>::new(),
                "{version:?}, engine={engine}: a name cannot invent membership across incompatible continuations"
            );
            let mut out = Vec::new();
            if first_name != "opaque.001" {
                object.renamed(VolumeIndex(0), first_name, &mut out);
                assert!(out.is_empty());
            }
            assert_eq!(
                object.claim(
                    "archive.part03.rar",
                    &volumes[2],
                    Some(volumes[2].len() as u64)
                ),
                vec![VolumeIndex(2)],
                "{version:?}, engine={engine}, first_name={first_name}"
            );
            object.bound(
                VolumeIndex(2),
                "archive.part03.rar",
                &volumes[2],
                Some(volumes[2].len() as u64),
            );
            if first_name == "opaque.001" {
                object.renamed(VolumeIndex(0), "archive.part01.rar", &mut out);
                assert!(
                    out.is_empty(),
                    "the other rename order keeps the same slots"
                );
            }
        }
    }
}

#[test]
fn engine_rename_uses_names_only_when_headers_state_no_position() {
    for version in [Version::Rar4, Version::Rar5] {
        let mut options = Options::new(version);
        options.split = Some(512);
        let volumes = write(&[Item::File("member.bin", payload(114, 1536))], &options);
        let mut engine = crate::rar_engine::RarEngine::new();
        for (index, bytes) in volumes.iter().take(2).enumerate() {
            engine.bound(
                VolumeIndex(index as u32),
                "opaque",
                bytes,
                Some(bytes.len() as u64),
            );
        }
        let mut out = Vec::new();
        engine.renamed(VolumeIndex(1), "archive.part03.rar", &mut out);
        assert_eq!(
            out.iter()
                .any(|emit| matches!(emit, Emit::GiveBack(VolumeIndex(1)))),
            matches!(version, Version::Rar4),
            "{version:?}: corrections={out:?}"
        );
    }
}

#[test]
fn another_named_sets_ordinal_is_not_positive_membership_evidence() {
    let mut options = Options::new(Version::Rar5);
    options.split = Some(512);
    let one = write(&[Item::File("first.bin", payload(111, 2048))], &options);
    let other = write(&[Item::File("second.bin", payload(112, 2048))], &options);
    assert_eq!(one.len(), 4);
    assert_eq!(other.len(), 4);
    for restored in [false, true] {
        let mut object: Box<dyn Object> = Box::new(store());
        object.bound(
            VolumeIndex(2),
            "first.part03.rar",
            &one[2][..64],
            Some(one[2].len() as u64),
        );
        object.bound(
            VolumeIndex(3),
            "first.part04.rar",
            &one[3][..64],
            Some(one[3].len() as u64),
        );
        if restored {
            object = crate::restore(&object.facts(), weaver_graph::Budget::default()).unwrap();
        }
        object.unposted(VolumeIndex(2), ByteRange::new(0, one[2].len() as u64));
        object.unposted(VolumeIndex(3), ByteRange::new(0, one[3].len() as u64));
        assert_eq!(
            object.claim(
                "second.part03.rar",
                &other[2][..64],
                Some(other[2].len() as u64)
            ),
            Vec::<VolumeIndex>::new(),
            "restored={restored}: a slot number cannot link different named sets"
        );
        assert_eq!(
            object.claim("first.part03.rar", &one[2][..64], Some(one[2].len() as u64)),
            vec![VolumeIndex(2)],
            "the same-name placement remains available"
        );
    }
}
