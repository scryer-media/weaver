// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A nested archive as a compressed member of a mixed set: a stored RAR and a
// stored 7z, each literal-coded beside stored members. The store leaves both
// to its run, the run makes each as it was packed, and what it makes is an
// archive that opens as any other.

use std::io::Cursor;

use sevenz_turbo::{
    ArchiveEntry, ArchiveReader, ArchiveWriter, EncoderConfiguration, EncoderMethod, Password,
};

use super::PASSWORD;
use super::mixed::check;
use super::owner::Owner;
use super::store;
use super::writer::{Item, Options, Version, literal, write};
use crate::testing::payload;

fn inner_files() -> Vec<(&'static str, Vec<u8>)> {
    vec![
        ("ember.mkv", payload(11, 9_000)),
        ("quill.srt", payload(12, 700)),
    ]
}

fn inner_rar() -> Vec<u8> {
    let items: Vec<Item> = inner_files()
        .into_iter()
        .map(|(name, bytes)| Item::File(name, bytes))
        .collect();
    write(&items, &Options::new(Version::Rar5)).remove(0)
}

fn inner_7z() -> Vec<u8> {
    let mut writer = ArchiveWriter::new(Cursor::new(Vec::new())).unwrap();
    writer.set_content_methods(vec![EncoderConfiguration::new(EncoderMethod::COPY)]);
    for (name, bytes) in inner_files() {
        writer
            .push_archive_entry(ArchiveEntry::new_file(name), Some(Cursor::new(bytes)))
            .unwrap();
    }
    writer.finish().unwrap().into_inner()
}

#[test]
fn a_nested_rar_and_7z_packed_in_a_mixed_set_are_made_by_the_run_and_open() {
    let rar = inner_rar();
    let sevenz = inner_7z();
    for version in [Version::Rar5, Version::Rar4] {
        let items = vec![
            Item::File("alpha.mkv", payload(1, 10_000)),
            literal(version, "inner.rar", &rar),
            Item::File("beta.mkv", payload(2, 5_003)),
            literal(version, "inner.7z", &sevenz),
        ];
        for (label, split, password) in [
            ("one volume", None, None),
            ("many volumes", Some(6_000), None),
            ("enciphered", Some(6_000), Some(PASSWORD)),
        ] {
            let mut options = Options::new(version);
            options.split = split;
            options.password = password;
            let label = format!("{version:?} {label}");
            let owner = check(&label, write(&items, &options), password);

            let made = owner.member("inner.rar");
            assert!(made.bytes == rar, "{label}");
            let mut below = Owner::named(store(), vec![made.bytes.clone()]);
            below.whole(4096);
            for (name, bytes) in inner_files() {
                let got = below.member(name);
                assert!(
                    got.closed && got.bytes == bytes,
                    "{label}: inner.rar {name}"
                );
            }

            let made = owner.member("inner.7z");
            assert!(made.bytes == sevenz, "{label}");
            let mut reader =
                ArchiveReader::new(Cursor::new(made.bytes.clone()), Password::empty()).unwrap();
            for (name, bytes) in inner_files() {
                assert!(
                    reader.read_file(name).unwrap() == bytes,
                    "{label}: inner.7z {name}"
                );
            }
        }
    }
}

// A set with two compressed members, delivered in frames: the store runs
// once its layout holds the first, and the one run it starts then makes the
// second too, laid after it started.
#[test]
fn a_set_with_two_compressed_members_runs_once_from_the_first() {
    use weaver_graph::{ByteRange, MemberId, Object};
    let rar = inner_rar();
    let sevenz = inner_7z();
    for version in [Version::Rar5, Version::Rar4] {
        let items = vec![
            Item::File("alpha.mkv", payload(1, 10_000)),
            literal(version, "inner.rar", &rar),
            Item::File("beta.mkv", payload(2, 5_003)),
            literal(version, "inner.7z", &sevenz),
        ];
        let volumes = write(&items, &Options::new(version));
        let length = volumes[0].len() as u64;
        let mut owner = Owner::named(store(), volumes);
        let mut start = 0;
        while start < length && !owner.object.runs() {
            let end = (start + 512).min(length);
            owner.deliver(0, ByteRange::new(start, end));
            start = end;
        }
        assert!(owner.object.runs(), "{version:?}");
        let face = owner.object.engine().expect("a mixed set runs");
        assert!(
            face.makes(MemberId(1)) && !face.makes(MemberId(3)),
            "{version:?}: the run starts at {start} of {length}, before the second is laid"
        );
        let started = owner.start();
        owner.whole(512);
        assert!(owner.object.engine().unwrap().makes(MemberId(3)), "{version:?}");
        assert!(owner.run_started(started).is_empty(), "{version:?}");
        assert!(super::mixed::trouble(&owner).is_empty(), "{version:?}: {:?}", owner.said);
        for (name, bytes) in [("inner.rar", &rar), ("inner.7z", &sevenz)] {
            let got = owner.member(name);
            assert!(got.closed && got.bytes == *bytes, "{version:?}: {name}");
        }
    }
}
