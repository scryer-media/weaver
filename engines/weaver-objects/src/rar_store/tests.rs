// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

mod facts;
mod owner;
mod writer;

use std::io::Cursor;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;

use weaver_graph::{
    ByteRange, Emit, Located, MemberId, Object, Placement, Provenance, RecoveryKind, VolumeIndex,
    Weight, Why,
};

use super::RarStore;
use crate::keys::{CAPACITY, Keys};
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
fn a_nameless_volume_without_its_predecessor_is_placed_where_it_states() {
    let volumes = write(&items(), &split(Version::Rar5));
    let mut object = store();
    object.bound(VolumeIndex(0), "c0ffee", &[], None);
    // Volume 2 of the set states its number; volume 1, which nobody holds
    // yet, does not decide where it goes.
    let front = &volumes[2][..4096.min(volumes[2].len())];
    assert_eq!(object.claim("d00d", front, None), Some(VolumeIndex(2)));
}

// A store holding only a set's second volume takes the first volume that
// carries on into it, never another set's first, which states the same
// number but ends on a member the second volume does not open on.
#[test]
fn a_first_volume_joins_only_the_set_it_carries_on_into() {
    let ours = write(&items(), &split(Version::Rar5));
    let theirs = write(
        &[Item::File("delta.bin", payload(4, 10_000))],
        &split(Version::Rar5),
    );
    let front = |volume: &Vec<u8>| volume[..4096.min(volume.len())].to_vec();
    let mut object = store();
    object.bound(VolumeIndex(1), "c0ffee", &front(&ours[1]), None);
    assert_eq!(object.claim("d00d", &front(&theirs[0]), None), None);
    assert_eq!(
        object.claim("beef", &front(&ours[0]), None),
        Some(VolumeIndex(0))
    );
}

// A one-volume set beside a numbered set of the same kind: every volume of
// either states a number, and the sets stay apart. A far volume of the
// numbered set never joins the one-volume set by its number alone: not
// where the names are two sets', and not past the volume whose end record
// says no volume follows. Held past that volume, as the one-volume set's
// first was claimed beside the other set's second, it goes back at its
// close.
#[test]
fn a_one_volume_set_and_a_numbered_set_keep_apart() {
    let mut whole = Options::new(Version::Rar5);
    whole.split = Some(1 << 20);
    let sample = write(
        &[Item::File("lantern.sample.bin", payload(6, 3_000))],
        &whole,
    )
    .remove(0);
    let set = write(&items(), &split(Version::Rar5));
    let front = |volume: &Vec<u8>| volume[..4096.min(volume.len())].to_vec();

    let mut named = store();
    named.bound(VolumeIndex(0), "lantern.sample.rar", &sample, None);
    assert_eq!(named.claim("alpha.part3.rar", &front(&set[2]), None), None);

    let mut ended = Owner::new(store(), vec![("c0ffee".to_owned(), sample.clone())]);
    ended.frames(0, 4096);
    ended.close(0);
    assert_eq!(ended.object.claim("d00d", &front(&set[2]), None), None);

    let posted = vec![
        ("c0ffee".to_owned(), sample),
        ("d00d".to_owned(), set[1].clone()),
    ];
    let mut beside = Owner::founded(store(), posted, 1);
    beside.frames(1, 4096);
    beside.close(1);
    beside.frames(0, 4096);
    beside.close(0);
    assert!(
        beside.said.contains(&Emit::GiveBack(VolumeIndex(1))),
        "{:?}",
        beside.said
    );
    assert_eq!(beside.member("lantern.sample.bin").bytes, payload(6, 3_000));
}

// Two numbered sets of one kind, posted together. A far volume of one never
// joins the other by the number its headers state when no held volume
// beside that number judges it and its name is the other set's; a far
// volume of the held set still joins by its name.
#[test]
fn a_far_volume_joins_only_the_set_its_name_numbers() {
    let ours = write(&items(), &split(Version::Rar5));
    let theirs = write(
        &[Item::File("delta.bin", payload(4, 20_000))],
        &split(Version::Rar5),
    );
    assert_eq!(theirs.len(), 4);
    let front = |volume: &Vec<u8>| volume[..4096.min(volume.len())].to_vec();

    let mut first = store();
    first.bound(VolumeIndex(0), "alpha.part1.rar", &front(&ours[0]), None);
    assert_eq!(
        first.claim("delta.part4.rar", &front(&theirs[3]), None),
        None
    );
    assert_eq!(
        first.claim("alpha.part3.rar", &front(&ours[2]), None),
        Some(VolumeIndex(2))
    );

    let mut last = store();
    last.bound(VolumeIndex(2), "alpha.part3.rar", &front(&ours[2]), None);
    assert_eq!(
        last.claim("delta.part1.rar", &front(&theirs[0]), None),
        None
    );
    assert_eq!(
        last.claim("alpha.part1.rar", &front(&ours[0]), None),
        Some(VolumeIndex(0))
    );
}

// A nameless RAR4 volume posted again goes where no free place takes it:
// the one held place its neighbours link it to.
#[test]
fn a_nameless_volume_posted_again_is_the_held_volume_its_neighbours_link_it_to() {
    // Each front is the whole volume, so it states the member it ends on.
    let volumes = write(&items(), &split(Version::Rar4));
    let mut object = store();
    for (index, volume) in volumes.iter().enumerate() {
        object.bound(
            VolumeIndex(index as u32),
            &format!("{:x}3c9e1f", index * 7 + 11),
            volume,
            None,
        );
    }
    assert_eq!(
        object.claim("d00d", &volumes[1], None),
        Some(VolumeIndex(1))
    );
}

// One member spans every volume of a nameless RAR4 set, so a middle
// volume's front links it to every free place beside the first and the
// last and states no number. It takes the lower, by links alone, and once
// its walk reads its end record, which numbers it elsewhere, it is given
// back.
#[test]
fn a_nameless_middle_volume_fitting_two_places_takes_the_lower_and_moves_when_its_end_says_otherwise()
 {
    let volumes = write(
        &[Item::File("feature.bin", payload(5, 23_000))],
        &split(Version::Rar4),
    );
    assert_eq!(volumes.len(), 4);
    // Each volume's end record states its number: more volumes follow
    // (0x0001), the number is present (0x0004).
    let numbered = |index: usize| {
        let mut bytes = volumes[index].clone();
        bytes.truncate(bytes.len() - 7);
        let flags = u16::from(index + 1 < volumes.len()) | 0x0004;
        let mut rest = vec![0x7b];
        rest.extend(flags.to_le_bytes());
        rest.extend(9u16.to_le_bytes());
        rest.extend((index as u16).to_le_bytes());
        bytes.extend((crate::grid::crc32(&rest) as u16).to_le_bytes());
        bytes.extend(rest);
        bytes
    };
    let name = |index: usize| format!("{:x}3c9e1f", index * 7 + 11);
    // The third volume arrives where the second would go.
    let posted = [0, 2, 1, 3]
        .map(|index| (name(index), numbered(index)))
        .into();
    let mut owner = Owner::new(store(), posted);
    owner
        .object
        .bound(VolumeIndex(3), &name(3), &numbered(3), None);
    owner.frames(0, 4096);
    owner.close(0);
    owner.frames(1, 4096);
    owner.close(1);
    assert!(
        owner.said.contains(&Emit::GiveBack(VolumeIndex(1))),
        "{:?}",
        owner.said
    );
}

// One stored member spans every volume of a nameless RAR4 set, so the last
// volume, arriving before the third, links to the free place after the
// second and is held there. Only its end record states its number: until
// it is read, the set is never judged by a chain that volume closes.
#[test]
fn a_nameless_volume_its_links_misplace_never_refuses_the_set() {
    let volumes = write(
        &[Item::File("feature.bin", payload(5, 23_000))],
        &split(Version::Rar4),
    );
    assert_eq!(volumes.len(), 4);
    // Each volume's end record states its number, as RAR 3 and later write
    // it: more volumes follow (0x0001), the number is present (0x0004).
    let numbered = |index: usize| {
        let mut bytes = volumes[index].clone();
        bytes.truncate(bytes.len() - 7);
        let flags = u16::from(index + 1 < volumes.len()) | 0x0004;
        let mut rest = vec![0x7b];
        rest.extend(flags.to_le_bytes());
        rest.extend(9u16.to_le_bytes());
        rest.extend((index as u16).to_le_bytes());
        bytes.extend((crate::grid::crc32(&rest) as u16).to_le_bytes());
        bytes.extend(rest);
        bytes
    };
    let named = [0, 1, 3]
        .map(|index| (format!("{:x}3c9e1f", index * 7 + 11), numbered(index)))
        .into();
    let mut owner = Owner::new(store(), named);
    for volume in 0..2 {
        owner.frames(volume, 4096);
        owner.close(volume);
    }
    owner.frames(2, 4096);
    assert!(
        !owner
            .said
            .iter()
            .any(|emit| matches!(emit, Emit::NeedsEngine(_) | Emit::Refuse(_))),
        "{:?}",
        owner.said
    );
}

// A nameless RAR4 volume a founding put first holds that place by links
// alone: a claim whose front states the place, the set's real first
// volume, takes it. A claim that only links, or a holder its own headers
// place, moves nothing.
#[test]
fn a_volume_links_alone_hold_yields_to_a_claim_that_states_its_place() {
    let volumes = write(
        &[Item::File("feature.bin", payload(5, 23_000))],
        &split(Version::Rar4),
    );
    let front = |index: usize| volumes[index][..4096].to_vec();
    let mut held = store();
    held.bound(VolumeIndex(0), "c0ffee", &front(2), None);
    assert!(held.provisional(VolumeIndex(0), &front(0), None));
    assert!(!held.provisional(VolumeIndex(0), &front(1), None));
    let mut first = store();
    first.bound(VolumeIndex(0), "d00d", &front(0), None);
    assert!(!first.provisional(VolumeIndex(0), &front(0), None));
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

// A set posted with its `.rar` and `.r00` names swapped: the real second
// volume, named as the first, founds the store at 0, where its own front
// says it is not, since it opens on a member begun before it. Its name
// settles it nowhere its headers rule out, so when the real first volume
// states position 0 the holder yields the place, solid or not.
#[test]
fn a_volume_named_first_yields_the_place_its_own_front_rules_out() {
    for solid in [false, true] {
        let mut options = split(Version::Rar4);
        options.solid = solid;
        let volumes = write(&items(), &options);
        let front = |volume: &Vec<u8>| volume[..4096.min(volume.len())].to_vec();
        let mut object = store();
        object.bound(VolumeIndex(0), "3c9e1f.rar", &front(&volumes[1]), None);
        assert!(
            object.provisional(VolumeIndex(0), &front(&volumes[0]), None),
            "solid={solid}: a holder its front misplaces yields"
        );
        assert_eq!(
            object.claim("3c9e1f.r00", &front(&volumes[0]), None),
            Some(VolumeIndex(0)),
            "solid={solid}"
        );
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

// The member a file was announced as, by its name.
fn member_id(owner: &Owner, name: &str) -> MemberId {
    owner
        .members
        .iter()
        .find(|(_, got)| got.name == name)
        .map(|(id, _)| MemberId(*id))
        .unwrap_or_else(|| panic!("no member {name}"))
}

// A stored file's bytes in a volume whose headers the store has not read
// are not located yet, never unseekable: every byte of the file is a raw
// byte of some volume. Once that volume's headers are read, the same range
// answers where it lies.
#[test]
fn a_seek_into_a_volume_not_walked_yet_is_located_once_its_headers_are_read() {
    for version in [Version::Rar5, Version::Rar4] {
        let volumes = write(&items(), &split(version));
        let mut owner = Owner::named(store(), volumes);
        owner.frames(0, 1000);
        owner.close(0);
        let alpha = member_id(&owner, "alpha.bin");
        let bytes = &files()[0].1;
        let length = bytes.len() as u64;
        let tail = ByteRange::new(length - 100, length);
        let whole = ByteRange::new(0, length);

        let mut extents = Vec::new();
        assert_eq!(
            owner.object.extent(alpha, tail, &mut extents),
            Located::NotYet,
            "{version:?}"
        );
        assert!(extents.is_empty(), "{version:?}");
        // What the first volume places is located; the rest is not yet.
        assert_eq!(
            owner.object.extent(alpha, whole, &mut extents),
            Located::NotYet,
            "{version:?}"
        );
        assert!(!extents.is_empty(), "{version:?}");
        assert!(
            extents.iter().all(|extent| extent.volume == VolumeIndex(0)),
            "{version:?}"
        );

        owner.frames(1, 1000);
        extents.clear();
        assert_eq!(
            owner.object.extent(alpha, tail, &mut extents),
            Located::Whole,
            "{version:?}"
        );
        assert_eq!(extents.len(), 1, "{version:?}");
        assert_eq!(extents[0].volume, VolumeIndex(1), "{version:?}");
        let at = extents[0].range;
        assert_eq!(
            &owner.volumes[1].1[at.start as usize..at.end as usize],
            &bytes[(length - 100) as usize..],
            "{version:?}"
        );
        extents.clear();
        assert_eq!(
            owner.object.extent(alpha, whole, &mut extents),
            Located::Whole,
            "{version:?}"
        );
        assert_eq!(
            extents.iter().map(|extent| extent.range.len()).sum::<u64>(),
            length,
            "{version:?}"
        );
    }
}

// Under a compressed set there is nothing to seek into: the store gives the
// set to an engine, whose members are no run of the volumes' raw bytes, so
// a seek into any of them is unseekable.
#[test]
fn a_seek_under_a_compressed_set_is_unseekable() {
    for version in [Version::Rar5, Version::Rar4] {
        let mut options = Options::new(version);
        options.solid = true;
        let volumes = write(&items(), &options);
        let mut owner = Owner::named(store(), volumes);
        owner.whole(4096);
        assert!(owner.said.contains(&Emit::NeedsEngine(Why::Compressed)));
        let engine = crate::rar_engine::RarEngine::new();
        let mut extents = Vec::new();
        assert_eq!(
            engine.extent(MemberId(0), ByteRange::new(0, 100), &mut extents),
            Located::Never,
            "{version:?}"
        );
        assert!(extents.is_empty(), "{version:?}");
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

// A RAR4 file key has no check of its own, so a candidate that did not open
// the headers keys the files on trial: a whole-file checksum that fails on
// bytes as they were posted condemns it, the store asks for every part it
// read again, and the next candidate opens the same bytes.
#[test]
fn a_wrong_rar4_key_on_verified_bytes_is_condemned_and_the_right_one_opens_the_same_bytes() {
    let volumes = write(&items(), &encrypted(Version::Rar4, false));
    let mut owner = Owner::named(store(), volumes);
    owner.frames(0, 4096);
    owner.offer("not-the-passphrase");
    let asked_again = owner
        .said
        .iter()
        .filter(|emit| matches!(emit, Emit::Again { .. }))
        .count();
    assert_eq!(asked_again, files().len(), "one part of each file, once");
    assert!(owner.members.values().all(|got| !got.closed));
    owner.offer(PASSWORD);
    owner.close(0);
    owner
        .said
        .retain(|emit| !matches!(emit, Emit::NeedsPassword | Emit::Again { .. }));
    no_trouble(&owner);
    streamed(&owner);
    posted_whole(&owner);
}

// The same failure on bytes not verified is damage: nothing is condemned
// while the files' bytes stand as they came, and once a repair verifies
// them and they fail again, the candidate is condemned and the next opens
// them. Only file data comes damaged; where it lies is what the store
// places at members once it has walked the volume.
#[test]
fn a_wrong_rar4_key_on_unverified_bytes_is_damage_until_repaired() {
    let volumes = write(&items(), &encrypted(Version::Rar4, false));
    let length = volumes[0].len() as u64;
    let whole = ByteRange::new(0, length);
    let mut walked = Owner::named(store(), volumes.clone());
    walked.offer(PASSWORD);
    walked.deliver(0, whole);
    let mut placements = Vec::new();
    walked.object.locate(VolumeIndex(0), whole, &mut placements);
    let mut at = 0;
    let mut data = Vec::new();
    for placement in &placements {
        let range = ByteRange::at(at, placement.len());
        data.push((range, matches!(placement, Placement::Member { .. })));
        at = range.end;
    }
    assert!(data.iter().filter(|(_, member)| *member).count() >= files().len());

    let mut owner = Owner::named(store(), volumes);
    owner.offer("not-the-passphrase");
    owner.offer(PASSWORD);
    for (range, member) in &data {
        let provenance = match member {
            true => Provenance::Damaged,
            false => Provenance::Fresh,
        };
        owner.deliver_as(0, *range, provenance, None);
    }
    let asked_again = |owner: &Owner| {
        owner
            .said
            .iter()
            .filter(|emit| matches!(emit, Emit::Again { .. }))
            .count()
    };
    assert_eq!(asked_again(&owner), 0, "{:?}", owner.said);
    assert_eq!(owner.members.len(), files().len());
    assert!(owner.members.values().all(|got| {
        got.coverage
            .runs()
            .all(|run| run.provenance == Provenance::Damaged)
    }));
    for (range, member) in &data {
        if *member {
            owner.deliver_as(0, *range, Provenance::Repaired, None);
        }
    }
    owner.close(0);
    assert_eq!(asked_again(&owner), files().len(), "one part of each file");
    owner
        .said
        .retain(|emit| !matches!(emit, Emit::NeedsPassword | Emit::Again { .. }));
    no_trouble(&owner);
    streamed(&owner);
    posted_whole(&owner);
}

#[test]
fn every_rar4_candidate_wrong_on_verified_bytes_needs_a_password() {
    let volumes = write(&items(), &encrypted(Version::Rar4, false));
    let mut owner = Owner::named(store(), volumes);
    owner.frames(0, 4096);
    owner.offer("not-the-passphrase");
    owner.offer("not-that-either");
    owner.close(0);
    assert!(owner.members.values().all(|got| !got.closed));
    assert!(!owner.said.contains(&Emit::Refuse(Why::Encrypted)));
    owner.quiet();
    assert!(owner.said.contains(&Emit::Refuse(Why::Encrypted)));
}

// RAR5 proves a candidate by its password check; RAR4 lets it through to
// the walk, whose first header refutes a wrong one by its CRC.
#[test]
fn encrypted_headers_open_with_the_password() {
    for version in [Version::Rar5, Version::Rar4] {
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
        assert!(owner.members.is_empty(), "{version:?}");
        owner.offer(PASSWORD);
        owner.whole(4096);
        owner.said.retain(|emit| *emit != Emit::NeedsPassword);
        no_trouble(&owner);
        streamed(&owner);
        posted_whole(&owner);
    }
}

#[test]
fn encrypted_headers_without_the_password_are_refused_at_quiet() {
    for version in [Version::Rar5, Version::Rar4] {
        let volumes = write(&items(), &encrypted(version, true));
        let mut owner = Owner::named(store(), volumes);
        owner.frames(0, 4096);
        owner.offer("not-the-passphrase");
        owner.close(0);
        // The question is still open: a candidate may yet answer it.
        assert!(
            !owner.said.contains(&Emit::Refuse(Why::EncryptedHeader)),
            "{version:?}"
        );
        assert!(owner.members.is_empty(), "{version:?}");
        owner.quiet();
        assert!(
            owner.said.contains(&Emit::Refuse(Why::EncryptedHeader)),
            "{version:?} {:?}",
            owner.said
        );
    }
}

#[test]
fn encrypted_files_without_the_password_are_refused_at_quiet() {
    let volumes = write(&items(), &encrypted(Version::Rar5, false));
    let mut owner = Owner::named(store(), volumes);
    owner.frames(0, 4096);
    owner.offer("not-the-passphrase");
    owner.close(0);
    assert!(!owner.said.contains(&Emit::Refuse(Why::Encrypted)));
    owner.quiet();
    assert!(owner.said.contains(&Emit::Refuse(Why::Encrypted)));
}

// One key cache per process: a second store offered the same password over
// the same salts derives nothing, neither a file key nor a header check.
#[test]
fn two_stores_with_one_password_and_salt_derive_once() {
    for (version, headers) in [
        (Version::Rar5, false),
        (Version::Rar4, false),
        (Version::Rar5, true),
    ] {
        let keys = Arc::new(Keys::new(CAPACITY));
        let volumes = write(&items(), &encrypted(version, headers));
        let mut counts = Vec::new();
        for _ in 0..2 {
            let object = RarStore::with_keys(Arc::clone(&keys));
            let mut owner = Owner::named(object, volumes.clone());
            owner.offer(PASSWORD);
            owner.whole(4096);
            owner.said.retain(|emit| *emit != Emit::NeedsPassword);
            no_trouble(&owner);
            streamed(&owner);
            let kdf = keys.kdf();
            let derived = kdf.rar5_derivation_count() + kdf.rar4_derivation_count();
            counts.push((keys.derivations(), derived));
        }
        assert!(counts[0].1 > 0, "{version:?} {headers}");
        assert_eq!(counts[0], counts[1], "{version:?} {headers}");
    }
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

// The flags of the end record the writer closes a volume with: where it
// says whether a volume follows.
fn end_flags(version: Version, volume: &[u8]) -> ByteRange {
    let length = volume.len() as u64;
    match version {
        Version::Rar5 => ByteRange::new(length - 2, length),
        Version::Rar4 => ByteRange::new(length - 4, length - 2),
    }
}

// A volume's end record arrives damaged, its flags zeroes, so read as it
// stands it says no volume follows. The set does not end there: the volumes
// after it stay held, and the repair that brings the real bytes reads the
// set whole.
#[test]
fn a_damaged_end_record_never_ends_the_set() {
    for version in [Version::Rar5, Version::Rar4] {
        let volumes = write(&items(), &split(version));
        let flags = end_flags(version, &volumes[0]);
        let length = volumes[0].len() as u64;
        let mut owner = Owner::named(store(), volumes);
        for volume in [1, 2] {
            owner.frames(volume, 1000);
            owner.close(volume);
        }
        owner.deliver(0, ByteRange::new(0, flags.start));
        owner.deliver_as(
            0,
            flags,
            Provenance::Damaged,
            Some(&vec![0; flags.len() as usize]),
        );
        owner.deliver(0, ByteRange::new(flags.end, length));
        owner.close(0);
        assert!(
            !owner
                .said
                .iter()
                .any(|emit| matches!(emit, Emit::GiveBack(_))),
            "{version:?} {:?}",
            owner.said
        );
        owner.deliver_as(0, flags, Provenance::Repaired, None);
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

// An article delivered damaged runs from the first file's data over the
// next file's header, intact, into that file's data. The walk reads damaged
// bytes only once the volume closes, so until then the store keeps the
// article from that header on as its own. The close names the file whose
// first bytes it kept, with no byte arriving: the store says they are no
// longer its own, and the file gets them, as they came, from where the
// owner kept them.
#[test]
fn bytes_kept_behind_a_header_read_at_close_reach_the_file_it_names() {
    for version in [Version::Rar5, Version::Rar4] {
        let real = write(&items(), &Options::new(version)).remove(0);
        let [(_, alpha), (_, beta), _] = &files()[..] else {
            unreachable!("three files")
        };
        let alpha_end = (find(&real, &alpha[..64]) + alpha.len()) as u64;
        let beta_at = find(&real, &beta[..64]) as u64;
        let article = ByteRange::new(alpha_end - 100, beta_at + 100);
        let length = real.len() as u64;
        let mut owner = Owner::named(store(), vec![real]);
        owner.deliver(0, ByteRange::new(0, article.start));
        owner.deliver_as(0, article, Provenance::Damaged, None);
        owner.deliver(0, ByteRange::new(article.end, length));
        assert_eq!(
            owner.members.len(),
            1,
            "{version:?} only the file before it"
        );
        owner.close(0);
        let got = owner.member("beta.bin");
        assert!(got.bytes == *beta, "{version:?}");
        assert!(
            got.coverage.covered(ByteRange::new(0, beta.len() as u64)),
            "{version:?}"
        );
        assert!(
            got.coverage.damaged(ByteRange::new(0, 100)),
            "{version:?} the kept bytes came damaged"
        );
        assert!(
            got.coverage
                .verified(ByteRange::new(100, beta.len() as u64)),
            "{version:?}"
        );
        assert!(
            owner.said.iter().any(|emit| matches!(
                emit,
                Emit::Unkept { volume: VolumeIndex(0), range, .. } if range.start == beta_at
            )),
            "{version:?} {:?}",
            owner.said
        );
    }
}

// A damaged article inside the first part of a split file: the namespace
// already marks it where it lies, so neither the part's own CRC nor the
// file's blames anything, and the repair is what they judge.
#[test]
fn bytes_that_came_damaged_are_never_blamed_on_their_part() {
    for version in [Version::Rar5, Version::Rar4] {
        let volumes = write(&items(), &split(version));
        let real = volumes[0].clone();
        let at = (find(&real, &files()[0].1[..64]) + 1000) as u64;
        let article = ByteRange::new(at, at + 200);
        let mut bad = real[article.start as usize..article.end as usize].to_vec();
        bad[17] ^= 0xff;
        let mut owner = Owner::named(store(), volumes);
        let length = real.len() as u64;
        owner.deliver(0, ByteRange::new(0, article.start));
        owner.deliver_as(0, article, Provenance::Damaged, Some(&bad));
        owner.deliver(0, ByteRange::new(article.end, length));
        for volume in 1..owner.volumes.len() {
            owner.deliver(
                volume,
                ByteRange::new(0, owner.volumes[volume].1.len() as u64),
            );
        }
        for volume in 0..owner.volumes.len() {
            owner.close(volume);
        }
        no_trouble(&owner);
        owner.deliver_as(0, article, Provenance::Repaired, None);
        no_trouble(&owner);
        streamed(&owner);
        posted_whole(&owner);
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
                    Emit::RecoveryAt { volume: found, range, kind: RecoveryKind::Rar, .. }
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
    // Reading on moves the window: the bids are restated from where the
    // read stands, the volume being read and those in hand never.
    owner.frames(0, 4096);
    owner.close(0);
    owner.frames(1, 4096);
    owner.close(1);
    let later: Vec<_> = owner
        .seeks
        .iter()
        .map(|seek| (seek.volume.0, seek.weight))
        .collect();
    let expected: Vec<_> = (3..=10).map(|index| (index, Weight(index - 2))).collect();
    assert_eq!(later, expected);
}

// The store is built on the second volume, and the first never comes: it
// is a volume missing like any other, damaged whole once the rest are in,
// and nothing is announced without the headers that name the members.
#[test]
fn a_set_without_its_first_volume_says_it_damaged_whole() {
    let volumes = write(&items(), &split(Version::Rar5));
    let length = volumes[1].len() as u64;
    let named = volumes
        .into_iter()
        .enumerate()
        .map(|(index, bytes)| (format!("alpha.part{}.rar", index + 1), bytes))
        .collect();
    let mut owner = Owner::founded(store(), named, 1);
    for volume in [1, 2] {
        owner.frames(volume, 4096);
    }
    for volume in [1, 2] {
        owner.close(volume);
    }
    assert!(owner.said.contains(&Emit::Damaged {
        volume: VolumeIndex(0),
        range: ByteRange::new(0, length),
    }));
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

#[test]
fn a_volume_numbered_last_of_all_numbers_is_placed_nowhere() {
    use super::claim::place;
    use crate::quiet::Placed;
    let mut facts = unrar_rs::RarArchive::parse_volume_facts_walk_with_shared_kdf_cache(
        Cursor::new(fixture("rar5/rar5_store.rar")),
        None,
        Arc::new(unrar_rs::KdfCache::new()),
    )
    .unwrap()
    .facts;
    facts.is_volume = true;
    let held = std::collections::BTreeMap::new();
    facts.volume_number = Some(5);
    assert_eq!(place(&facts, &held), Some(Placed::At(VolumeIndex(5))));
    facts.volume_number = Some(u32::MAX);
    assert_eq!(place(&facts, &held), None);
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
