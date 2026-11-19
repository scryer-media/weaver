// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What one event costs the store in a large set: it looks again only at the
// volume the event was about, extends the layout it keeps past its tail,
// and reads each bound name once, when it is bound.

use weaver_graph::{ByteRange, Delivery, Object, Provenance, VolumeIndex};

use super::super::counts;
use super::owner::Owner;
use super::writer::{Item, Options, Version, write};
use super::{PASSWORD, find, no_trouble, posted_whole, store};
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
        .claim("stray.part2.rar", (front).into(), None)
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
        let placed = object.claim(&name, (front).into(), None);
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
            let placed = object.claim(&name, (front).into(), None);
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

// A stored RAR5 set of `members` files of `size` bytes each, split at
// `split` data bytes a volume or in one, streamed in articles of `article`
// bytes and closed: what the store counted doing it.
fn streamed_many(members: usize, size: usize, split: Option<usize>) -> counts::Counts {
    const ARTICLE: usize = 16 * 1024;
    let items: Vec<Item> = (0..members)
        .map(|at| {
            let name: &'static str = Box::leak(format!("m{at:05}.bin").into_boxed_str());
            Item::File(name, payload(at as u64, size))
        })
        .collect();
    let mut options = Options::new(Version::Rar5);
    options.split = split;
    let volumes = write(&items, &options);
    let count = volumes.len();
    eprintln!("{members} members of {size} bytes in {count} volumes");
    let mut owner = Owner::named(store(), volumes);
    counts::take();
    for volume in 0..count {
        owner.bind(volume);
        owner.frames(volume, ARTICLE);
        owner.close(volume);
    }
    let counted = counts::take();
    no_trouble(&owner);
    assert_eq!(owner.members.len(), members);
    assert_eq!(owner.held(), 0);
    counted
}

#[test]
fn many_small_members_one_volume_count() {
    many_members_count("small, one volume", 200, None);
}

#[test]
fn many_large_members_one_volume_count() {
    many_members_count("large, one volume", 20_000, None);
}

#[test]
fn many_small_members_many_volumes_count() {
    many_members_count("small, many volumes", 200, Some(50_000));
}

#[test]
fn many_large_members_many_volumes_count() {
    many_members_count("large, many volumes", 20_000, Some(1_000_000));
}

// Four times the members costs about four times the work in every count
// that grows with the set alone. What still grows with its square is what
// the layouts laid afresh as the volume being read lists another member:
// each is compared again from the first member, and no more than that.
// The two set sizes the counts are compared at.
const SIZES: [usize; 2] = [100, 400];

fn many_members_count(shape: &str, size: usize, split: Option<usize>) {
    let linear = |small: usize, large: usize, what: &str| {
        assert!(
            large <= 5 * small.max(1),
            "{shape}: {what} {small} -> {large} grows faster than the set"
        );
    };
    let mut runs = Vec::new();
    {
        for members in SIZES {
            let c = streamed_many(members, size, split);
            assert!(c.compared <= c.laid_members, "{shape} M={members}: {c:?}");
            assert!(
                c.visited <= c.laid_members + members,
                "{shape} M={members}: {c:?}"
            );
            eprintln!(
                "{shape} M={members}: syncs {} layouts {} compared {} visited {} remaps {} \
                 keyed {} walks {} read_members {} laid_members {} recovery_looked {} known {}",
                c.syncs,
                c.layouts,
                c.compared,
                c.visited,
                c.remaps,
                c.keyed,
                c.walks,
                c.read_members,
                c.laid_members,
                c.recovery_looked,
                c.known,
            );
            runs.push(c);
        }
    }
    let [small, large] = &runs[..] else {
        unreachable!("two sizes")
    };
    linear(small.syncs, large.syncs, "syncs");
    linear(small.walks, large.walks, "walks");
    linear(small.keyed, large.keyed, "keyed");
    linear(small.remaps, large.remaps, "remaps");
    linear(
        small.recovery_looked,
        large.recovery_looked,
        "recovery_looked",
    );
}

// A layout is asked to take a volume again only on facts it was not asked
// on before: once on the very facts it took a volume on, its answer is
// known, and every such answer is the one it gives when asked.
#[test]
fn a_layout_is_asked_to_take_a_volume_only_on_facts_it_was_not_asked_on() {
    for version in [Version::Rar5, Version::Rar4] {
        let (bytes, volumes) = fifty(version);
        let mut owner = Owner::named(store(), volumes);
        counts::take();
        for volume in 0..VOLUMES {
            owner.bind(volume);
            owner.frames(volume, 64);
            owner.close(volume);
        }
        let counted = counts::take();
        let mut asked = counted.asked.clone();
        asked.sort_unstable();
        asked.dedup();
        assert_eq!(asked.len(), counted.asked.len(), "{version:?} {counted:?}");
        assert!(counted.known > 0, "{version:?} {counted:?}");
        no_trouble(&owner);
        let got = owner.member("alpha.bin");
        assert!(got.closed);
        assert_eq!(got.bytes, bytes);
        posted_whole(&owner);
    }
}

// `count` stored files of `size` bytes each, the one at `renamed` under a
// name of the same length when it is given.
fn many_items(count: usize, size: usize, renamed: Option<usize>) -> Vec<Item> {
    (0..count)
        .map(|at| {
            let name = match renamed == Some(at) {
                true => format!("m{at:04}z.bin"),
                false => format!("m{at:05}.bin"),
            };
            let name: &'static str = Box::leak(name.into_boxed_str());
            Item::File(name, payload(at as u64, size))
        })
        .collect()
}

fn options(split: Option<usize>, password: Option<&'static str>) -> Options {
    let mut options = Options::new(Version::Rar5);
    options.split = split;
    options.password = password;
    options
}

// Everything a run said, run once the way the store looked before it kept
// track of what changed and once the way it looks now.
// What each run cost comes with it.
type Run = (Vec<weaver_graph::Emit>, counts::Counts);

fn both_ways(scenario: &dyn Fn() -> Owner) -> (Run, Run) {
    counts::set_old(true);
    counts::take();
    let old = scenario().log;
    let old = (old, counts::take());
    counts::set_old(false);
    let new = scenario().log;
    let new = (new, counts::take());
    (old, new)
}

#[test]
fn looking_only_at_what_changed_says_what_looking_at_everything_said() {
    let one_volume = || {
        let volumes = write(&many_items(40, 300, None), &options(None, None));
        let mut owner = Owner::named(store(), volumes);
        owner.whole(97);
        no_trouble(&owner);
        owner
    };
    let spanning = || {
        let volumes = write(&many_items(30, 3_000, None), &options(Some(7_000), None));
        assert!(volumes.len() > 5);
        let mut owner = Owner::named(store(), volumes);
        owner.whole(1_000);
        no_trouble(&owner);
        owner
    };
    let encrypted = || {
        let volumes = write(
            &many_items(20, 2_000, None),
            &options(Some(9_000), Some(PASSWORD)),
        );
        let count = volumes.len();
        let mut owner = Owner::named(store(), volumes);
        owner.frames(0, 700);
        owner.frames(1, 700);
        owner.offer(PASSWORD);
        for volume in 2..count {
            owner.frames(volume, 700);
        }
        for volume in 0..count {
            owner.close(volume);
        }
        owner
    };
    let out_of_order = || {
        let volumes = write(&many_items(30, 2_500, None), &options(Some(8_000), None));
        let count = volumes.len();
        let mut owner = Owner::named(store(), volumes);
        // The odd volumes from the last, then the even ones from the first.
        let order: Vec<usize> = (0..count)
            .rev()
            .filter(|at| at % 2 == 1)
            .chain((0..count).filter(|at| at % 2 == 0))
            .collect();
        for volume in order {
            owner.backwards(volume, 900);
        }
        for volume in (0..count).rev() {
            owner.close(volume);
        }
        no_trouble(&owner);
        assert_eq!(owner.members.len(), 30);
        owner
    };
    // A volume whose headers change: its first copy names a file otherwise,
    // and a repair brings the real headers.
    let retracted = || {
        let real = write(&many_items(25, 400, None), &options(None, None)).remove(0);
        let wrong = write(&many_items(25, 400, Some(9)), &options(None, None)).remove(0);
        assert_eq!(real.len(), wrong.len());
        let length = real.len() as u64;
        let mut owner = Owner::named(store(), vec![real.clone()]);
        owner.deliver_as(
            0,
            ByteRange::new(0, length),
            Provenance::Damaged,
            Some(&wrong),
        );
        owner.close(0);
        let at = find(&real, b"m00009.bin") as u64;
        owner.deliver_as(
            0,
            ByteRange::new(at - 40, at + 10),
            Provenance::Repaired,
            None,
        );
        owner.deliver_as(0, ByteRange::new(0, length), Provenance::Repaired, None);
        owner.close(0);
        assert!(
            owner
                .said
                .iter()
                .any(|emit| matches!(emit, weaver_graph::Emit::Retract(_))),
            "the wrong headers are retracted"
        );
        owner
    };
    // The same in a set of many volumes: a middle one comes wrong first.
    let retracted_in_a_set = || {
        let split = Some(6_000);
        let real = write(&many_items(24, 1_500, None), &options(split, None));
        let wrong = write(&many_items(24, 1_500, Some(12)), &options(split, None));
        let count = real.len();
        let at = (0..count)
            .find(|&volume| real[volume] != wrong[volume])
            .expect("a volume differs");
        let mut owner = Owner::named(store(), real.clone());
        for volume in 0..count {
            let length = real[volume].len() as u64;
            match volume == at {
                true => owner.deliver_as(
                    volume,
                    ByteRange::new(0, length),
                    Provenance::Damaged,
                    Some(&wrong[volume]),
                ),
                false => owner.frames(volume, 1_100),
            }
        }
        for volume in 0..count {
            owner.close(volume);
        }
        let length = real[at].len() as u64;
        owner.deliver_as(at, ByteRange::new(0, length), Provenance::Repaired, None);
        owner.close(at);
        owner
    };
    let scenarios: [(&str, &dyn Fn() -> Owner); 6] = [
        ("one volume", &one_volume),
        ("members spanning volumes", &spanning),
        ("encrypted", &encrypted),
        ("out of order", &out_of_order),
        ("retracted", &retracted),
        ("retracted in a set", &retracted_in_a_set),
    ];
    for (name, scenario) in scenarios {
        let ((old, before), (new, after)) = both_ways(scenario);
        eprintln!(
            "{name}: compared {} -> {}, visited {} -> {}, keyed {} -> {}, retracts {}",
            before.compared,
            after.compared,
            before.visited,
            after.visited,
            before.keyed,
            after.keyed,
            old.iter()
                .filter(|emit| matches!(emit, weaver_graph::Emit::Retract(_)))
                .count(),
        );
        assert!(after.visited < before.visited, "{name} passes members over");
        assert!(!old.is_empty(), "{name}");
        assert_eq!(old.len(), new.len(), "{name}");
        for (at, (old, new)) in old.iter().zip(&new).enumerate() {
            assert_eq!(old, new, "{name}: emit {at}");
        }
    }
}
