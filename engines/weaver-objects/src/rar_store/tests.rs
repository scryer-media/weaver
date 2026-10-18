// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

mod facts;
mod owner;
mod writer;

use std::io::Cursor;
use std::num::NonZeroUsize;
use std::path::PathBuf;

use weaver_graph::{ByteRange, Emit, Object, Provenance, RecoveryKind, VolumeIndex, Weight, Why};

use super::RarStore;
use crate::testing::payload;
use owner::Owner;
use writer::{Item, Options, Version, write};

const PASSWORD: &str = "a-test-passphrase";

fn files() -> Vec<(&'static str, Vec<u8>)> {
    vec![
        ("alpha.bin", payload(1, 10_000)),
        ("beta.bin", payload(2, 5_003)),
        ("gamma.bin", payload(3, 300)),
    ]
}

fn items() -> Vec<Item> {
    files()
        .into_iter()
        .map(|(name, bytes)| Item::File(name, bytes))
        .collect()
}

fn store() -> RarStore {
    RarStore::new()
}

fn no_trouble(owner: &Owner) {
    for emit in &owner.said {
        assert!(
            !matches!(
                emit,
                Emit::Damaged { .. }
                    | Emit::NeedsEngine(_)
                    | Emit::Refuse(_)
                    | Emit::Retract(_)
                    | Emit::NeedsPassword
            ),
            "unexpected {emit:?}"
        );
    }
}

// Every file whole, closed once, with nothing held back.
fn streamed(owner: &Owner) {
    let files = files();
    assert_eq!(owner.members.len(), files.len());
    for (name, bytes) in &files {
        let got = owner.member(name);
        assert_eq!(got.length, Some(bytes.len() as u64), "{name}");
        assert!(got.closed, "{name} closed");
        assert!(
            got.coverage.verified(ByteRange::new(0, bytes.len() as u64)),
            "{name} verified"
        );
        assert_eq!(&got.bytes, bytes, "{name}");
    }
    assert_eq!(owner.held(), 0);
}

fn posted_whole(owner: &Owner) {
    for volume in 0..owner.volumes.len() {
        let bytes = owner.volumes[volume].1.clone();
        assert_eq!(
            owner.posted(volume).as_deref(),
            Some(bytes.as_slice()),
            "volume {volume} posted"
        );
    }
}

fn find(haystack: &[u8], needle: &[u8]) -> usize {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
        .expect("needle present")
}

// The emits a run makes, with bytes folded into one per member so a run in
// frames compares to one whole.
fn shape(owner: &Owner) -> Vec<String> {
    owner.said.iter().map(|emit| format!("{emit:?}")).collect()
}

fn in_frames(version: Version, frame: usize) -> Owner {
    let volumes = write(&items(), &Options::new(version));
    let mut owner = Owner::named(store(), volumes);
    owner.whole(frame);
    no_trouble(&owner);
    streamed(&owner);
    posted_whole(&owner);
    owner
}

#[test]
fn rar5_one_volume_streams_whole_in_frames_and_bytewise() {
    let whole = in_frames(Version::Rar5, usize::MAX >> 1);
    let frames = in_frames(Version::Rar5, 4096);
    let bytes = in_frames(Version::Rar5, 1);
    assert_eq!(shape(&whole), shape(&frames));
    assert_eq!(shape(&whole), shape(&bytes));
}

#[test]
fn rar4_one_volume_streams_whole_in_frames_and_bytewise() {
    let whole = in_frames(Version::Rar4, usize::MAX >> 1);
    let frames = in_frames(Version::Rar4, 4096);
    let bytes = in_frames(Version::Rar4, 1);
    assert_eq!(shape(&whole), shape(&frames));
    assert_eq!(shape(&whole), shape(&bytes));
}

#[test]
fn a_volume_delivered_backwards_streams() {
    let volumes = write(&items(), &Options::new(Version::Rar5));
    let mut owner = Owner::named(store(), volumes);
    owner.backwards(0, 777);
    owner.close(0);
    no_trouble(&owner);
    streamed(&owner);
    posted_whole(&owner);
}

fn split(version: Version) -> Options {
    let mut options = Options::new(version);
    options.split = Some(6_000);
    options
}

#[test]
fn a_file_split_across_three_volumes_arriving_out_of_order() {
    for version in [Version::Rar5, Version::Rar4] {
        let volumes = write(&items(), &split(version));
        assert_eq!(volumes.len(), 3, "{version:?}");
        let mut owner = Owner::named(store(), volumes);
        for volume in [2, 0, 1] {
            owner.frames(volume, 1000);
        }
        for volume in [2, 0, 1] {
            owner.close(volume);
        }
        no_trouble(&owner);
        streamed(&owner);
        posted_whole(&owner);
    }
}

#[test]
fn rar4_old_numbering_names_its_volumes() {
    let volumes = write(&items(), &split(Version::Rar4));
    let names = ["alpha.rar", "alpha.r00", "alpha.r01"];
    let named = names
        .iter()
        .map(|name| name.to_string())
        .zip(volumes)
        .collect();
    let mut owner = Owner::new(store(), named);
    for volume in [1, 2, 0] {
        owner.frames(volume, 4096);
    }
    for volume in 0..3 {
        owner.close(volume);
    }
    no_trouble(&owner);
    streamed(&owner);
    posted_whole(&owner);
}

// The store is built on the set's last volume, and every earlier one
// arrives after it; each is placed where the set puts it, by its name, or
// by the number a RAR5 volume's headers state. A nameless RAR4 volume
// states its number only in its end record, past any front.
#[test]
fn a_set_arriving_last_volume_first_places_each_volume_where_the_set_puts_it() {
    let rar4_old = |index: usize| match index {
        0 => "alpha.rar".to_owned(),
        _ => format!("alpha.r{:02}", index - 1),
    };
    let nameless = |index: usize| format!("{:x}3c9e1f", index * 7 + 11);
    let shapes: [(Version, &dyn Fn(usize) -> String); 4] = [
        (Version::Rar5, &|index| {
            format!("alpha.part{}.rar", index + 1)
        }),
        (Version::Rar4, &|index| {
            format!("alpha.part{}.rar", index + 1)
        }),
        (Version::Rar4, &rar4_old),
        (Version::Rar5, &nameless),
    ];
    for (version, name) in shapes {
        let volumes = write(&items(), &split(version));
        let last = volumes.len() - 1;
        let named = volumes
            .into_iter()
            .enumerate()
            .map(|(index, bytes)| (name(index), bytes))
            .collect();
        let mut owner = Owner::founded(store(), named, last);
        for volume in (0..=last).rev() {
            owner.frames(volume, 4096);
        }
        for volume in (0..=last).rev() {
            owner.close(volume);
        }
        no_trouble(&owner);
        streamed(&owner);
        posted_whole(&owner);
    }
}

#[test]
fn nameless_volumes_are_grouped_by_their_headers() {
    for version in [Version::Rar5, Version::Rar4] {
        let volumes = write(&items(), &split(version));
        let named = volumes
            .into_iter()
            .enumerate()
            .map(|(index, bytes)| (format!("{:x}3c9e1f", index * 7 + 11), bytes))
            .collect();
        let mut owner = Owner::new(store(), named);
        // A nameless volume is placed by what its front says about the one
        // before it, so each arrives after its predecessor's headers.
        owner.whole(4096);
        no_trouble(&owner);
        streamed(&owner);
        posted_whole(&owner);
    }
}

#[test]
fn a_nameless_volume_without_its_predecessor_is_not_claimed() {
    let volumes = write(&items(), &split(Version::Rar5));
    let mut object = store();
    object.bound(VolumeIndex(0), "c0ffee");
    // Volume 2 of the set says it follows volume 1, which nobody holds.
    let front = &volumes[2][..4096.min(volumes[2].len())];
    assert_eq!(object.claim("d00d", front), None);
}

#[test]
fn directories_and_empty_files_are_announced_and_closed() {
    for version in [Version::Rar5, Version::Rar4] {
        let items = vec![
            Item::Dir("docs"),
            Item::File("docs/empty.bin", Vec::new()),
            Item::File("docs/alpha.bin", payload(4, 900)),
        ];
        let volumes = write(&items, &Options::new(version));
        let mut owner = Owner::named(store(), volumes);
        owner.whole(4096);
        no_trouble(&owner);
        let dir = owner.member("docs/");
        assert_eq!(dir.length, Some(0));
        assert!(dir.closed);
        let empty = owner.member("docs/empty.bin");
        assert_eq!(empty.length, Some(0));
        assert!(empty.closed);
        assert_eq!(owner.member("docs/alpha.bin").bytes, payload(4, 900));
        posted_whole(&owner);
    }
}

#[test]
fn a_solid_set_asks_for_an_engine_before_any_member() {
    for version in [Version::Rar5, Version::Rar4] {
        let mut options = Options::new(version);
        options.solid = true;
        let volumes = write(&items(), &options);
        let mut owner = Owner::named(store(), volumes);
        owner.whole(4096);
        assert!(owner.members.is_empty(), "{version:?}");
        assert!(owner.said.contains(&Emit::NeedsEngine(Why::Compressed)));
    }
}

#[test]
fn a_compressed_file_among_stored_ones_asks_for_an_engine() {
    for version in [Version::Rar5, Version::Rar4] {
        let mut options = Options::new(version);
        options.compressed = Some(1);
        let volumes = write(&items(), &options);
        let mut owner = Owner::named(store(), volumes);
        owner.whole(4096);
        assert!(owner.said.contains(&Emit::NeedsEngine(Why::Compressed)));
        // What came before stays announced for the engine's stand-in, with
        // the bytes routed before the store gave the set up.
        assert_eq!(owner.members.len(), 1);
        let alpha = &owner.member("alpha.bin").bytes;
        assert!(files()[0].1.starts_with(alpha));
        assert!(
            !owner
                .said
                .iter()
                .any(|emit| matches!(emit, Emit::Retract(_)))
        );
    }
}

fn encrypted(version: Version, headers: bool) -> Options {
    let mut options = Options::new(version);
    options.password = Some(PASSWORD);
    options.encrypt_headers = headers;
    options
}

fn split_encrypted(version: Version, headers: bool) -> Options {
    let mut options = encrypted(version, headers);
    options.split = split(version).split;
    options
}

#[test]
fn encrypted_files_open_after_a_password_out_of_order() {
    for version in [Version::Rar5, Version::Rar4] {
        let volumes = write(&items(), &split_encrypted(version, false));
        let mut owner = Owner::named(store(), volumes);
        for volume in [1, 2, 0] {
            owner.backwards(volume, 1500);
        }
        assert!(owner.said.contains(&Emit::NeedsPassword), "{version:?}");
        owner.offer(PASSWORD);
        for volume in 0..3 {
            owner.close(volume);
        }
        owner.said.retain(|emit| *emit != Emit::NeedsPassword);
        no_trouble(&owner);
        streamed(&owner);
        posted_whole(&owner);
    }
}

#[test]
fn rar5_files_without_a_check_value_open_and_verify() {
    let mut options = encrypted(Version::Rar5, false);
    options.check = false;
    options.hash_key = true;
    let volumes = write(&items(), &options);
    let mut owner = Owner::named(store(), volumes);
    owner.offer(PASSWORD);
    owner.whole(4096);
    owner.said.retain(|emit| *emit != Emit::NeedsPassword);
    no_trouble(&owner);
    streamed(&owner);
    posted_whole(&owner);
}

#[test]
fn a_wrong_password_then_the_right_one() {
    let volumes = write(&items(), &encrypted(Version::Rar5, false));
    let mut owner = Owner::named(store(), volumes);
    owner.frames(0, 4096);
    owner.offer("not-the-passphrase");
    assert!(owner.members.values().all(|got| got.bytes.is_empty()));
    owner.offer(PASSWORD);
    owner.close(0);
    owner.said.retain(|emit| *emit != Emit::NeedsPassword);
    no_trouble(&owner);
    streamed(&owner);
    posted_whole(&owner);
}

#[test]
fn encrypted_headers_open_with_the_password() {
    let version = Version::Rar5;
    let volumes = write(&items(), &split_encrypted(version, true));
    let mut owner = Owner::new(
        store(),
        volumes
            .into_iter()
            .enumerate()
            .map(|(index, bytes)| (format!("alpha.part{}.rar", index + 1), bytes))
            .collect(),
    );
    owner.frames(0, 4096);
    assert!(owner.said.contains(&Emit::NeedsPassword), "{version:?}");
    assert!(owner.members.is_empty());
    owner.offer("not-the-passphrase");
    assert!(owner.members.is_empty());
    owner.offer(PASSWORD);
    owner.whole(4096);
    owner.said.retain(|emit| *emit != Emit::NeedsPassword);
    no_trouble(&owner);
    streamed(&owner);
    posted_whole(&owner);
}

#[test]
fn encrypted_headers_without_the_password_are_refused_at_the_last_close() {
    let version = Version::Rar5;
    let volumes = write(&items(), &encrypted(version, true));
    let mut owner = Owner::named(store(), volumes);
    owner.frames(0, 4096);
    owner.offer("not-the-passphrase");
    owner.close(0);
    assert!(
        owner.said.contains(&Emit::Refuse(Why::EncryptedHeader)),
        "{version:?} {:?}",
        owner.said
    );
}

// RAR4 headers carry no password check, so nothing can prove a candidate
// before a header is deciphered with it.
#[test]
fn rar4_encrypted_headers_go_to_an_engine() {
    let volumes = write(&items(), &split_encrypted(Version::Rar4, true));
    let mut owner = Owner::named(store(), volumes);
    owner.offer(PASSWORD);
    owner.frames(0, 4096);
    assert!(owner.members.is_empty());
    assert_eq!(owner.said, vec![Emit::NeedsEngine(Why::EncryptedHeader)]);
}

#[test]
fn encrypted_files_without_the_password_are_refused_at_the_last_close() {
    let volumes = write(&items(), &encrypted(Version::Rar5, false));
    let mut owner = Owner::named(store(), volumes);
    owner.frames(0, 4096);
    owner.offer("not-the-passphrase");
    owner.close(0);
    assert!(owner.said.contains(&Emit::Refuse(Why::Encrypted)));
}

#[test]
fn a_damaged_file_header_waits_for_repair() {
    for version in [Version::Rar5, Version::Rar4] {
        let volumes = write(&items(), &Options::new(version));
        let real = volumes[0].clone();
        let at = find(&real, b"beta.bin");
        let mut bad = real.clone();
        bad[at] ^= 0x20;
        let mut owner = Owner::named(store(), volumes);
        let length = real.len() as u64;
        let header = ByteRange::new(at as u64 - 40, at as u64 + 8);
        owner.deliver(0, ByteRange::new(0, header.start));
        owner.deliver_as(
            0,
            header,
            Provenance::Damaged,
            Some(&bad[header.start as usize..header.end as usize]),
        );
        owner.deliver(0, ByteRange::new(header.end, length));
        let damaged = owner.said.iter().any(|emit| {
            matches!(emit, Emit::Damaged { volume: VolumeIndex(0), range } if range.contains(at as u64))
        });
        assert!(damaged, "{version:?} {:?}", owner.said);
        assert_eq!(owner.members.len(), 1, "only the file before it");
        owner.deliver_as(0, header, Provenance::Repaired, None);
        owner.close(0);
        streamed(&owner);
        posted_whole(&owner);
    }
}

#[test]
fn a_repair_behind_an_announced_member_retracts_and_rewalks() {
    for version in [Version::Rar5, Version::Rar4] {
        let real = write(&items(), &Options::new(version)).remove(0);
        // Wrong bytes whose header checksums hold: the same set with its
        // first file under another name, so only a repair can tell.
        let mut other = items();
        other[0] = Item::File("alphz.bin", files()[0].1.clone());
        let wrong = write(&other, &Options::new(version)).remove(0);
        assert_eq!(real.len(), wrong.len());
        let length = real.len() as u64;
        let mut owner = Owner::named(store(), vec![real.clone()]);
        owner.deliver_as(
            0,
            ByteRange::new(0, length),
            Provenance::Damaged,
            Some(&wrong),
        );
        // Damaged bytes stay out of the walk until their volume closes.
        owner.close(0);
        owner.member("alphz.bin");
        let at = find(&real, b"alpha.bin") as u64;
        let header = ByteRange::new(at.saturating_sub(40), at + 9);
        owner.deliver_as(0, header, Provenance::Repaired, None);
        assert!(
            owner
                .said
                .contains(&Emit::Retract(weaver_graph::MemberId(0))),
            "{version:?} {:?}",
            owner.said
        );
        // The rest of the volume comes again from a second source.
        owner.deliver_as(0, ByteRange::new(0, length), Provenance::Repaired, None);
        owner.close(0);
        streamed(&owner);
        posted_whole(&owner);
    }
}

#[test]
fn a_damaged_member_byte_is_reported_at_its_volume_range() {
    for version in [Version::Rar5, Version::Rar4] {
        let volumes = write(&items(), &Options::new(version));
        let real = volumes[0].clone();
        let at = find(&real, &files()[1].1[..64]) + 1000;
        let mut bad = real.clone();
        bad[at] ^= 0xff;
        let mut owner = Owner::named(store(), volumes);
        let length = real.len() as u64;
        owner.deliver_as(0, ByteRange::new(0, length), Provenance::Fresh, Some(&bad));
        owner.close(0);
        let damaged = owner.said.iter().any(|emit| {
            matches!(emit, Emit::Damaged { volume: VolumeIndex(0), range } if range.contains(at as u64))
        });
        assert!(damaged, "{version:?} {:?}", owner.said);
        assert!(owner.member("beta.bin").closed);
    }
}

#[test]
fn the_recovery_record_is_announced() {
    for version in [Version::Rar5, Version::Rar4] {
        let mut options = split(version);
        options.recovery = true;
        let volumes = write(&items(), &options);
        let count = volumes.len();
        let mut owner = Owner::named(store(), volumes);
        owner.whole(4096);
        no_trouble(&owner);
        streamed(&owner);
        posted_whole(&owner);
        for volume in 0..count {
            let record = vec![0xa5; 40 + volume];
            let at = find(&owner.volumes[volume].1, &record) as u64;
            let announced = owner.said.iter().any(|emit| {
                matches!(
                    emit,
                    Emit::RecoveryAt { volume: found, range, kind: RecoveryKind::Rar }
                        if found.0 as usize == volume && range.contains(at)
                )
            });
            assert!(announced, "{version:?} volume {volume} {:?}", owner.said);
        }
    }
}

#[test]
fn unlock_bids_run_ahead_of_the_volume_being_read() {
    let mut options = Options::new(Version::Rar5);
    options.split = Some(1_500);
    let volumes = write(&items(), &options);
    assert!(volumes.len() > 9);
    let mut owner = Owner::named(store(), volumes);
    for volume in 1..owner.volumes.len() {
        owner.bind(volume);
    }
    let first: Vec<_> = owner
        .seeks
        .iter()
        .map(|seek| (seek.volume.0, seek.weight))
        .collect();
    let expected: Vec<_> = (1..=8).map(|index| (index, Weight(index))).collect();
    assert_eq!(first, expected);
    // Reading on moves the window: each bid once, the volume being read and
    // those in hand never.
    owner.frames(0, 4096);
    owner.close(0);
    owner.frames(1, 4096);
    owner.close(1);
    let later: Vec<_> = owner.seeks[8..]
        .iter()
        .map(|seek| (seek.volume.0, seek.weight))
        .collect();
    assert_eq!(later, vec![(9, Weight(8)), (10, Weight(8))]);
}

#[test]
fn a_set_without_its_first_volume_is_refused() {
    let volumes = write(&items(), &split(Version::Rar5));
    let mut owner = Owner::new(
        store(),
        vec![("alpha.part2.rar".to_owned(), volumes[1].clone())],
    );
    owner.frames(0, 4096);
    assert!(owner.said.contains(&Emit::Refuse(Why::FirstBytesLost)));
    assert!(owner.members.is_empty());
}

#[test]
fn a_lost_first_header_is_first_bytes_lost() {
    let volumes = write(&items(), &Options::new(Version::Rar5));
    let mut owner = Owner::named(store(), volumes);
    let mut out = Vec::new();
    owner
        .object
        .volume_lost(VolumeIndex(0), ByteRange::new(0, 64), &mut out);
    assert_eq!(out, vec![Emit::Refuse(Why::FirstBytesLost)]);
}

#[test]
fn a_missing_middle_volume_is_damaged_whole() {
    let volumes = write(&items(), &split(Version::Rar5));
    let length = volumes[0].len() as u64;
    let mut owner = Owner::named(store(), volumes);
    owner.frames(0, 4096);
    owner.frames(2, 4096);
    owner.close(0);
    owner.close(2);
    assert!(owner.said.contains(&Emit::Damaged {
        volume: VolumeIndex(1),
        range: ByteRange::new(0, length),
    }));
    // Nothing past the gap was announced: the file crossing it waits.
    assert!(!owner.member("alpha.bin").closed);
}

#[test]
fn a_short_volume_is_damaged_for_its_missing_tail() {
    let mut volumes = write(&items(), &Options::new(Version::Rar5));
    let real = volumes[0].len() as u64;
    let cut = find(&volumes[0], &files()[1].1[..64]) + 200;
    volumes[0].truncate(cut);
    let mut owner = Owner::named(store(), volumes);
    owner.whole(4096);
    let damaged = owner.said.iter().any(|emit| {
        matches!(emit, Emit::Damaged { volume: VolumeIndex(0), range }
            if range.start == cut as u64 && range.end <= real)
    });
    assert!(damaged, "{:?}", owner.said);
    assert!(owner.member("alpha.bin").closed);
}

fn fixture(name: &str) -> Vec<u8> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../server/crates/weaver-server-core/tests/fixtures")
        .join(name);
    std::fs::read(&path).unwrap_or_else(|error| panic!("{}: {error}", path.display()))
}

// A fixture written by RAR itself streams to what unrar-rs extracts from it.
fn real_fixture(name: &str, password: &str) -> Owner {
    let bytes = fixture(&format!("rar5/{name}"));
    let expected = unrar(std::slice::from_ref(&bytes), Some(password));
    assert!(!expected.is_empty());
    let mut owner = Owner::new(store(), vec![(name.to_owned(), bytes)]);
    owner.frames(0, 333);
    owner.offer(password);
    owner.close(0);
    owner.said.retain(|emit| *emit != Emit::NeedsPassword);
    no_trouble(&owner);
    for (name, bytes) in &expected {
        let got = owner.member(name);
        assert!(got.closed, "{name}");
        assert_eq!(&got.bytes, bytes, "{name}");
        assert!(
            got.coverage.verified(ByteRange::new(0, bytes.len() as u64)),
            "{name} verified"
        );
    }
    posted_whole(&owner);
    owner
}

#[test]
fn a_real_rar5_stored() {
    real_fixture("rar5_store.rar", "unused");
}

#[test]
fn a_real_rar5_with_encrypted_files() {
    let owner = real_fixture("rar5_enc_store.rar", "testpass123");
    let original = fixture("originals/small.txt");
    assert!(owner.members.values().any(|got| got.bytes == original));
}

#[test]
fn a_real_rar5_with_encrypted_headers() {
    real_fixture("rar5_hp_store.rar", "secretpass");
}

// The writer's archives are what unrar-rs reads: the cross-check that the
// store and its writer do not share one misreading of the format.
struct Memory(Vec<Vec<u8>>);

impl unrar_rs::VolumeProvider for Memory {
    fn get_volume(
        &self,
        index: usize,
    ) -> Result<Box<dyn unrar_rs::ReadSeek>, unrar_rs::VolumeProviderError> {
        self.0
            .get(index)
            .map(|bytes| Box::new(Cursor::new(bytes.clone())) as Box<dyn unrar_rs::ReadSeek>)
            .ok_or(unrar_rs::VolumeProviderError::Unavailable {
                volume: index,
                reason: "no such volume".into(),
            })
    }
}

fn unrar(volumes: &[Vec<u8>], password: Option<&str>) -> Vec<(String, Vec<u8>)> {
    let mut archive = unrar_rs::RarArchive::open_prefix(
        Cursor::new(volumes[0].clone()),
        password,
        NonZeroUsize::MIN,
    )
    .unwrap();
    archive.set_verify(true);
    for (volume, bytes) in volumes.iter().enumerate() {
        let mut headers = NonZeroUsize::MIN;
        loop {
            let found = archive
                .extend_volume_prefix(volume, Box::new(Cursor::new(bytes.clone())), headers)
                .unwrap();
            if found < headers.get() {
                break;
            }
            headers = headers.checked_add(1).unwrap();
        }
    }
    let provider = Memory(volumes.to_vec());
    let mut out = Vec::new();
    for index in 0..archive.len() {
        let info = archive.member_info(index).unwrap();
        if info.is_directory {
            continue;
        }
        let mut bytes = Vec::new();
        archive
            .by_index_via(index, &provider)
            .unwrap()
            .copy_to(&mut bytes)
            .unwrap();
        out.push((info.raw_name, bytes));
    }
    out
}

#[test]
fn unrar_reads_what_the_writer_writes() {
    let expected: Vec<_> = files()
        .into_iter()
        .map(|(name, bytes)| (name.to_owned(), bytes))
        .collect();
    for version in [Version::Rar5, Version::Rar4] {
        for (options, password) in [
            (Options::new(version), None),
            (split(version), None),
            (encrypted(version, false), Some(PASSWORD)),
            (encrypted(version, true), Some(PASSWORD)),
        ] {
            let volumes = write(&items(), &options);
            assert_eq!(unrar(&volumes, password), expected, "{options:?}");
        }
    }
}
