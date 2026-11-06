// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What one event costs the store in a large set: it looks again only at the
// volume the event was about, extends the layout it keeps past its tail,
// and reads each bound name once, when it is bound.

use weaver_graph::{ByteRange, Delivery, Object, Provenance, VolumeIndex};

use super::super::counts;
use super::owner::Owner;
use super::writer::{Item, Options, Version, write};
use super::{no_trouble, posted_whole, store};
use crate::testing::payload;

const VOLUMES: usize = 50;

// One file split across fifty volumes.
fn fifty(version: Version) -> (Vec<u8>, Vec<Vec<u8>>) {
    let bytes = payload(7, VOLUMES * 1_000);
    let mut options = Options::new(version);
    options.split = Some(1_000);
    let volumes = write(&[Item::File("alpha.bin", bytes.clone())], &options);
    assert_eq!(volumes.len(), VOLUMES);
    (bytes, volumes)
}

#[test]
fn settle_walks_only_the_volumes_whose_bytes_came() {
    let (bytes, volumes) = fifty(Version::Rar5);
    let last = VOLUMES - 1;
    let length = volumes[last].len() as u64;
    let mut owner = Owner::named(store(), volumes);
    for volume in 0..VOLUMES {
        owner.bind(volume);
    }
    for volume in 0..last {
        owner.frames(volume, usize::MAX >> 1);
        owner.close(volume);
    }
    counts::take();
    // One article of the last volume: its headers. The delivery is taken
    // again as each header names how far the next reaches, and every time
    // only that volume is walked.
    owner.deliver(last, ByteRange::new(0, 256));
    let counted = counts::take();
    assert_eq!(counted.walked, [last as u32].into(), "{counted:?}");
    assert_eq!(counted.layouts, 0, "{counted:?}");
    // The rest of it, and its close: the layout takes the set's last
    // volume past its tail.
    owner.deliver(last, ByteRange::new(256, length));
    owner.close(last);
    let counted = counts::take();
    assert_eq!(counted.layouts, 0, "{counted:?}");
    let layout = owner.object.layout.as_ref().expect("a layout");
    assert!(layout.has_volume(last as u32));
    no_trouble(&owner);
    let got = owner.member("alpha.bin");
    assert!(got.closed);
    assert_eq!(got.bytes, bytes);
    assert_eq!(owner.held(), 0);
    posted_whole(&owner);
}

// A store judges a claim's format by the volumes it holds now: a stray
// volume of another format it walked and gave back refuses nothing after.
#[test]
fn a_volume_given_back_takes_its_format_with_it() {
    let stray = fifty(Version::Rar4).1.swap_remove(1);
    let (_, volumes) = fifty(Version::Rar5);
    let mut object = store();
    let front = &stray[..stray.len().min(4096)];
    let at = *object
        .claim("stray.part2.rar", front, None)
        .first()
        .expect("the stray is claimed by its name");
    object.bound(at, "stray.part2.rar", &[], None);
    object.sized(at, stray.len() as u64, &mut Vec::new());
    object.deliver(
        Delivery {
            volume: at,
            offset: 0,
            bytes: &weaver_graph::Evidenced::from(stray.clone()),
            provenance: Provenance::Fresh,
        },
        &mut |_, _| false,
        &mut Vec::new(),
    );
    assert!(
        object.volumes[&at].walk.facts.is_some(),
        "the stray is walked"
    );
    assert!(object.give_back(at).is_some(), "the stray goes back");
    let mut placements = Vec::new();
    for (index, bytes) in volumes.iter().enumerate() {
        let name = format!("alpha.part{}.rar", index + 1);
        let front = &bytes[..bytes.len().min(4096)];
        let placed = object.claim(&name, front, None);
        let Some(&at) = placed.first() else {
            break;
        };
        placements.push(placed);
        object.bound(at, &name, &[], None);
        object.sized(at, bytes.len() as u64, &mut Vec::new());
    }
    let listed: Vec<Vec<VolumeIndex>> = (0..VOLUMES as u32)
        .map(|index| vec![VolumeIndex(index)])
        .collect();
    assert_eq!(placements, listed);
}

#[test]
fn a_claim_parses_the_set_numbering_once() {
    let part = |index: usize| format!("alpha.part{}.rar", index + 1);
    let old = |index: usize| match index {
        0 => "alpha.rar".to_owned(),
        _ => format!("alpha.r{:02}", index - 1),
    };
    let shapes: [(Version, &dyn Fn(usize) -> String); 2] =
        [(Version::Rar5, &part), (Version::Rar4, &old)];
    for (version, name) in shapes {
        let (_, volumes) = fifty(version);
        let mut object = store();
        counts::take();
        let mut placements = Vec::new();
        for (index, bytes) in volumes.iter().enumerate() {
            let name = name(index);
            let front = &bytes[..bytes.len().min(4096)];
            let placed = object.claim(&name, front, None);
            let at = *placed.first().expect("claimed");
            placements.push(placed);
            object.bound(at, &name, &[], None);
            object.sized(at, bytes.len() as u64, &mut Vec::new());
        }
        // Each volume where its name numbers it, as before.
        let listed: Vec<Vec<VolumeIndex>> = (0..VOLUMES as u32)
            .map(|index| vec![VolumeIndex(index)])
            .collect();
        assert_eq!(placements, listed, "{version:?}");
        assert_eq!(counts::take().names, VOLUMES, "{version:?}");
    }
}
