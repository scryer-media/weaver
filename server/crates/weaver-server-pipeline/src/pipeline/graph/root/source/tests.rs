// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A seek visits only the articles it can move: the same answers and moves as
// testing every article of its run, at the cost of the articles it moves.

use std::collections::{BTreeMap, BTreeSet};

use weaver_graph::{ByteRange, Pull, VolumeIndex};

use super::{ArticleMap, Class, State, WITNESSES, is_raised};
use crate::pipeline::graph::root::facts::{CHUNK, VolumeFacts};
use crate::pipeline::graph::root::hooks::{
    LOST_NOT_FOUND, LOST_WITNESSED, SEEK_EXAMINED, SEEK_MAPPED,
};

// A seeded source of numbers, the same on every run.
struct Seeded(u64);

impl Seeded {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound.max(1)
    }

    fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }
}

const CLASSES: [Class; 9] = [
    Class::Critical,
    Class::Recovery,
    Class::FirstArticle,
    Class::Window(0),
    Class::Window(1),
    Class::Window(7),
    Class::Ordinary,
    Class::Parked,
    Class::Window(u32::MAX),
];

fn class(rng: &mut Seeded) -> Class {
    CLASSES[rng.below(CLASSES.len() as u64) as usize]
}

// The posted sizes of a seeded post: three volumes, one of them empty at
// times.
fn sizes(rng: &mut Seeded) -> Vec<Vec<(u32, u32)>> {
    (0..3)
        .map(|_| {
            let count = rng.below(40);
            (0..count)
                .map(|ordinal| (ordinal as u32 + 1, 1 + rng.below(1_000) as u32))
                .collect()
        })
        .collect()
}

// Lay seeded states, classes and boundaries over a map made from the post.
// The same seed lays the same map.
fn seeded(seed: u64) -> ArticleMap {
    let mut rng = Seeded(seed);
    let mut map = ArticleMap::new(&sizes(&mut rng));
    let falling = rng.chance(50);
    for found in &mut map.volumes {
        let len = found.articles.len();
        let mut decoded = 0;
        for index in 0..len {
            let encoded = found.encoded_at(index + 1) - found.encoded_at(index);
            let start = decoded;
            decoded += encoded - rng.below(encoded / 4 + 1);
            let state = match rng.below(10) {
                0..=4 => State::Waiting,
                5 | 6 => State::Arrived(Some(ByteRange::new(start, decoded))),
                7 => State::Arrived(None),
                _ => State::Lost {
                    damaged: rng.chance(50),
                },
            };
            found.set_state(&mut map.open, index, state);
            if rng.chance(60) {
                found.set_class(&mut map.open, index, class(&mut rng));
            }
            if rng.chance(30) {
                let offset = if falling && rng.chance(40) {
                    rng.below(decoded + 1)
                } else {
                    start
                };
                found.know(index, offset);
            }
        }
        if rng.chance(30) {
            found.know(len, decoded);
        }
        found.set_ordered_only(rng.chance(10));
    }
    map
}

// A seeded seek: a point, part of the volume, the whole of it, to the end
// of the address space, empty, or past the volume's end.
fn seek(rng: &mut Seeded, map: &ArticleMap) -> (VolumeIndex, ByteRange, Option<Class>) {
    let volume = rng.below(map.volumes.len() as u64 + 1) as usize;
    let end = map
        .volumes
        .get(volume)
        .map_or(1_000, |found| found.bounds(found.articles.len()).1);
    let at = rng.below(end + 1);
    let range = match rng.below(6) {
        0 => ByteRange::new(at, at + 1),
        1 => ByteRange::new(at, at + rng.below(end - at + 1)),
        2 => ByteRange::new(0, end),
        3 => ByteRange::new(at, u64::MAX),
        4 => ByteRange::new(at, at),
        _ => ByteRange::new(end + 1, end + 1 + rng.below(100)),
    };
    let class = (!rng.chance(12)).then(|| class(rng));
    (VolumeIndex(volume as u32), range, class)
}

fn classes(map: &ArticleMap) -> Vec<Vec<Class>> {
    map.volumes
        .iter()
        .map(|found| found.articles.iter().map(|article| article.class).collect())
        .collect()
}

// Every waiting article is filed once, under its class.
fn assert_filed(map: &ArticleMap) {
    for found in &map.volumes {
        let mut raised: BTreeMap<Class, BTreeSet<usize>> = BTreeMap::new();
        for (index, article) in found.articles.iter().enumerate() {
            let waiting = article.state == State::Waiting;
            assert_eq!(
                found.parkable.contains(&index),
                waiting && article.class == Class::Ordinary
            );
            assert_eq!(
                found.parked.contains(&index),
                waiting && article.class == Class::Parked
            );
            if waiting && is_raised(article.class) {
                raised.entry(article.class).or_default().insert(index);
            }
        }
        assert_eq!(found.raised, raised);
    }
}

// Whether the seek runs to the volume's end over placements that disagree,
// with a waiting article it can move whose bounds miss the range: the
// mapping then takes it by the end of the volume alone.
fn extends(map: &ArticleMap, volume: VolumeIndex, range: ByteRange, class: Option<Class>) -> bool {
    let Some(found) = map.volumes.get(volume.0 as usize) else {
        return false;
    };
    let Some(class) = class.filter(|class| *class != Class::Parked) else {
        return false;
    };
    let end = found.bounds(found.articles.len()).1;
    let range = ByteRange::new(range.start.min(end), range.end.min(end));
    !found.ordered_only
        && found.disorder > 0
        && range.end == end
        && found.articles.iter().enumerate().any(|(index, article)| {
            article.state == State::Waiting
                && class < article.class
                && !found.may_carry(index, range)
        })
}

#[test]
fn a_seek_answers_and_moves_as_a_scan_of_its_run_did() {
    let (mut lost, mut moved, mut still, mut extended, mut falling, mut lost_runs) =
        (0, 0, 0, 0, 0, 0);
    for seed in 0..600 {
        let mut map = seeded(seed);
        let mut scanned = seeded(seed);
        assert_filed(&map);
        assert_undecided(&map);
        let mut rng = Seeded(seed ^ 0xA5A5);
        for _ in 0..24 {
            let (volume, range, class) = seek(&mut rng, &map);
            if map
                .volumes
                .get(volume.0 as usize)
                .is_some_and(|found| found.disorder > 0)
            {
                falling += 1;
            }
            if extends(&map, volume, range, class) {
                extended += 1;
            }
            if lost_in_run(&map, volume, range) {
                lost_runs += 1;
            }
            // The same seek twice: the second finds moved what the first
            // moved, and must answer as the scan does over that too.
            for _ in 0..2 {
                let pull = map.map_seek(volume, range, class);
                let by_scan = scanned.map_seek_by_scan(volume, range, class);
                assert_eq!(pull, by_scan, "seed {seed}: {volume:?} {range:?} {class:?}");
                assert_eq!(map.moves, scanned.moves, "seed {seed}");
                assert_eq!(classes(&map), classes(&scanned), "seed {seed}");
                assert_filed(&map);
                assert_undecided(&map);
                match pull {
                    Pull::Lost => lost += 1,
                    Pull::Moved(0) => still += 1,
                    Pull::Moved(_) => moved += 1,
                    _ => {}
                }
            }
        }
    }
    assert!(lost > 0 && moved > 0 && still > 0, "{lost} {moved} {still}");
    assert!(falling > 0 && extended > 0, "{falling} {extended}");
    assert!(lost_runs > 0, "{lost_runs}");
}

// Whether the run of articles that may carry the range holds a lost one.
fn lost_in_run(map: &ArticleMap, volume: VolumeIndex, range: ByteRange) -> bool {
    let Some(found) = map.volumes.get(volume.0 as usize) else {
        return false;
    };
    let end = found.bounds(found.articles.len()).1;
    let range = ByteRange::new(range.start.min(end), range.end.min(end));
    !found.ordered_only
        && found.run(range).any(|index| {
            matches!(found.articles[index].state, State::Lost { .. })
                && found.may_carry(index, range)
        })
}

// Every volume answers `undecided` from its counts as from its articles:
// `Volume::undecided` asserts the two agree.
fn assert_undecided(map: &ArticleMap) {
    for index in 0..map.volumes.len() {
        let _ = map.undecided(VolumeIndex(index as u32));
    }
}

// A seek left standing over a volume whose articles arrive one by one, mapped
// again after each arrival.
fn standing(range: impl Fn(u64) -> ByteRange, class: Class) -> (usize, usize, usize) {
    const ARTICLES: u32 = 300;
    let sizes: Vec<(u32, u32)> = (1..=ARTICLES).map(|ordinal| (ordinal, 100)).collect();
    let mut map = ArticleMap::new(&[sizes]);
    let volume = VolumeIndex(0);
    let range = range(u64::from(ARTICLES) * 100);
    SEEK_EXAMINED.with(|count| count.set(0));
    SEEK_MAPPED.with(|count| count.set(0));
    for ordinal in 1..=ARTICLES {
        map.seek(volume, range, Some(class));
        let start = u64::from(ordinal - 1) * 90;
        map.arrived(
            volume,
            ordinal,
            ByteRange::new(start, start + 90),
            Some(start + 90),
            true,
        );
    }
    map.seek(volume, range, Some(class));
    let moves = map.moves.len();
    let examined = SEEK_EXAMINED.with(|count| count.get());
    let mapped = SEEK_MAPPED.with(|count| count.get());
    assert_eq!(
        mapped,
        ARTICLES as usize + 1,
        "each arrival maps the seek again"
    );
    assert!(moves > 0 && moves <= ARTICLES as usize);
    (ARTICLES as usize, examined, moves)
}

#[test]
fn a_standing_seek_examines_only_the_articles_it_moves() {
    // The whole volume, to its exact end.
    let (articles, examined, moves) = standing(|end| ByteRange::new(0, end), Class::Critical);
    assert_eq!(moves, articles);
    assert_eq!(examined, moves);
    // A recovery seek to the end of the address space, from part way in.
    let (_, examined, moves) = standing(|end| ByteRange::new(end / 2, u64::MAX), Class::Recovery);
    assert_eq!(examined, moves);
    // The root's bid on an index volume: the whole address space.
    let (articles, examined, moves) = standing(|_| ByteRange::new(0, u64::MAX), Class::Window(0));
    assert_eq!(moves, articles);
    assert_eq!(examined, moves);
}

// A raise over a volume placed exactly, three of its articles lost, and
// over one whose placements disagree, to its end: each examines the
// articles it moves and, for its answer, the first lost one surely in its
// range. Raised again, the volume has nothing left to move, and that lost
// one, its witness, answers without a walk.
#[test]
fn a_raise_over_lost_or_disordered_articles_examines_only_what_it_can_move() {
    const ARTICLES: usize = 100;
    let sizes: Vec<(u32, u32)> = (1..=ARTICLES as u32)
        .map(|ordinal| (ordinal, 100))
        .collect();
    let volume = VolumeIndex(0);
    let examined = |map: &mut ArticleMap, range: ByteRange| {
        SEEK_EXAMINED.with(|count| count.set(0));
        let moves = map.moves.len();
        let pull = map.map_seek(volume, range, Some(Class::Critical));
        (
            pull,
            SEEK_EXAMINED.with(|count| count.get()),
            map.moves.len() - moves,
        )
    };

    let mut map = ArticleMap::new(std::slice::from_ref(&sizes));
    let found = &mut map.volumes[0];
    for boundary in 0..=ARTICLES {
        found.know(boundary, boundary as u64 * 100);
    }
    for index in [10, 11, 12] {
        found.set_state(&mut map.open, index, State::Lost { damaged: false });
    }
    let whole = ByteRange::new(0, ARTICLES as u64 * 100);
    let (pull, seen, moves) = examined(&mut map, whole);
    assert_eq!(pull, Pull::Lost);
    assert_eq!(moves, ARTICLES - 3);
    assert_eq!(seen, moves + 1);
    let (pull, seen, moves) = examined(&mut map, whole);
    assert_eq!((pull, seen, moves), (Pull::Lost, 0, 0));

    let mut map = ArticleMap::new(std::slice::from_ref(&sizes));
    let found = &mut map.volumes[0];
    for boundary in 0..=ARTICLES {
        found.know(boundary, boundary as u64 * 100);
    }
    // Article 50 is placed back at the volume's front.
    found.know(50, 10);
    assert!(found.disorder > 0);
    let to_end = ByteRange::new(4_000, u64::MAX);
    let (pull, seen, moves) = examined(&mut map, to_end);
    assert_eq!(pull, Pull::Moved(ARTICLES as u64));
    assert_eq!(moves, ARTICLES);
    assert_eq!(
        seen, moves,
        "each article the raise can move is examined once"
    );
    let (pull, seen, moves) = examined(&mut map, to_end);
    assert_eq!((pull, seen, moves), (Pull::Moved(0), 0, 0));
}

// What a change to a seeded map does to the seeks left standing on it.
#[derive(Clone, Copy, Debug)]
enum Change {
    // A waiting article arrives.
    Arrival(usize, usize),
    // A lost article arrives after all.
    Found(usize, usize),
    // A boundary is placed, or placed again elsewhere.
    Know(usize, usize, u64),
    // A boundary's place is forgotten.
    Unknow(usize, usize),
}

fn change(rng: &mut Seeded, map: &ArticleMap) -> Option<Change> {
    let volume = rng.below(map.volumes.len() as u64) as usize;
    let found = &map.volumes[volume];
    let len = found.articles.len();
    let with = |wanted: fn(State) -> bool| {
        (0..len)
            .filter(|index| wanted(found.articles[*index].state))
            .collect::<Vec<_>>()
    };
    let pick = |rng: &mut Seeded, of: Vec<usize>| {
        (!of.is_empty()).then(|| of[rng.below(of.len() as u64) as usize])
    };
    match rng.below(4) {
        0 => pick(rng, with(|state| state == State::Waiting))
            .map(|index| Change::Arrival(volume, index)),
        1 => pick(rng, with(|state| matches!(state, State::Lost { .. })))
            .map(|index| Change::Found(volume, index)),
        2 => {
            let boundary = rng.below(len as u64 + 1) as usize;
            let end = found.encoded_at(len);
            Some(Change::Know(volume, boundary, rng.below(end + 1)))
        }
        _ => pick(rng, found.known.keys().copied().collect())
            .map(|boundary| Change::Unknow(volume, boundary)),
    }
}

fn apply(map: &mut ArticleMap, change: Change) {
    match change {
        Change::Arrival(volume, index) | Change::Found(volume, index) => {
            map.volumes[volume].set_state(&mut map.open, index, State::Arrived(None));
        }
        Change::Know(volume, boundary, offset) => map.volumes[volume].know(boundary, offset),
        Change::Unknow(volume, boundary) => map.volumes[volume].unknow(boundary),
    }
}

// Seeks answered Lost are left standing and mapped again after each change:
// arrivals, which leave a witness standing, a lost article that arrives, and
// boundaries placed again, which can take it away. Each answer is the scan's.
#[test]
fn a_standing_lost_seek_answers_as_a_scan_across_changes() {
    LOST_WITNESSED.with(|count| count.set(0));
    // Seeks answered Lost before a change and otherwise after it, by the
    // change that turned them.
    let (mut lost_after, mut found_turned, mut boundary_turned) = (0, 0, 0);
    for seed in 0..600 {
        let mut map = seeded(seed);
        let mut scanned = seeded(seed);
        let mut rng = Seeded(seed ^ 0x5A5A);
        let mut standing = Vec::new();
        for _ in 0..24 {
            let (volume, range, class) = seek(&mut rng, &map);
            let pull = map.map_seek(volume, range, class);
            assert_eq!(pull, scanned.map_seek_by_scan(volume, range, class));
            if pull == Pull::Lost && map.volumes.get(volume.0 as usize).is_some() {
                standing.push((volume, range, class, pull));
            }
        }
        for step in 0..16 {
            let Some(change) = change(&mut rng, &map) else {
                continue;
            };
            apply(&mut map, change);
            apply(&mut scanned, change);
            for (volume, range, class, last) in &mut standing {
                let was = std::mem::replace(last, Pull::Moved(0));
                let pull = map.map_seek(*volume, *range, *class);
                let by_scan = scanned.map_seek_by_scan(*volume, *range, *class);
                assert_eq!(
                    pull, by_scan,
                    "seed {seed} step {step} {change:?}: {volume:?} {range:?} {class:?}"
                );
                assert_eq!(map.moves, scanned.moves, "seed {seed}");
                assert_eq!(classes(&map), classes(&scanned), "seed {seed}");
                assert_filed(&map);
                *last = pull;
                match (was, pull, change) {
                    (Pull::Lost, Pull::Lost, _) => lost_after += 1,
                    (Pull::Lost, _, Change::Found(..)) => found_turned += 1,
                    (Pull::Lost, _, Change::Know(..) | Change::Unknow(..)) => {
                        boundary_turned += 1;
                    }
                    _ => {}
                }
            }
        }
    }
    let witnessed = LOST_WITNESSED.with(|count| count.get());
    assert!(
        lost_after > 0 && found_turned > 0 && boundary_turned > 0 && witnessed > 0,
        "{lost_after} {found_turned} {boundary_turned} {witnessed}"
    );
}

// One lost article, 90, surely carries the seek; 41 lost ones before it in a
// volume whose placements disagree do not. The first mapping walks the 42;
// each later one finds 90 still lost and answers from it. When 90 arrives,
// the walk looks at the 41 and answers otherwise; when its boundaries are
// placed so that it may lie before the range, the walk looks at all 42.
#[test]
fn a_lost_seek_mapped_again_walks_only_when_its_witness_fails() {
    const ARTICLES: usize = 100;
    const STEPS: usize = 5;
    let sizes: Vec<(u32, u32)> = (1..=ARTICLES as u32)
        .map(|ordinal| (ordinal, 100))
        .collect();
    let volume = VolumeIndex(0);
    let range = ByteRange::new(9_000, 9_100);
    let lost: Vec<usize> = (0..=40).chain([90]).collect();
    let build = || {
        let mut map = ArticleMap::new(std::slice::from_ref(&sizes));
        let found = &mut map.volumes[0];
        for boundary in 0..=ARTICLES {
            found.know(boundary, boundary as u64 * 100);
        }
        found.know(50, 10);
        assert!(found.disorder > 0);
        for index in &lost {
            found.set_state(&mut map.open, *index, State::Lost { damaged: false });
        }
        map
    };
    let mapped = |map: &mut ArticleMap| {
        SEEK_EXAMINED.with(|count| count.set(0));
        LOST_WITNESSED.with(|count| count.set(0));
        LOST_NOT_FOUND.with(|count| count.set((0, 0)));
        let pull = map.map_seek(volume, range, None);
        (
            pull,
            SEEK_EXAMINED.with(|count| count.get()),
            LOST_WITNESSED.with(|count| count.get()),
            LOST_NOT_FOUND.with(|count| count.get()),
        )
    };

    let mut map = build();
    assert_eq!(mapped(&mut map), (Pull::Lost, lost.len(), 0, (0, 0)));
    for index in 60..60 + STEPS {
        map.volumes[0].set_state(&mut map.open, index, State::Arrived(None));
        assert_eq!(mapped(&mut map), (Pull::Lost, 0, 1, (0, 0)));
    }
    map.volumes[0].set_state(&mut map.open, 90, State::Arrived(None));
    assert_eq!(
        mapped(&mut map),
        (Pull::Moved(0), lost.len() - 1, 0, (1, lost.len() - 1))
    );

    let mut map = build();
    assert_eq!(mapped(&mut map).0, Pull::Lost);
    let found = &mut map.volumes[0];
    found.unknow(90);
    found.unknow(91);
    found.know(89, 8_900);
    found.know(92, 9_000);
    assert!(!found.may_carry(90, range) && !found.surely_in(90, range));
    assert_eq!(
        mapped(&mut map),
        (Pull::Moved(0), lost.len(), 0, (1, lost.len()))
    );
}

// One volume asked over and over with more distinct ranges answered lost
// than it keeps witnesses for: the map never holds more than its bound, and
// every answer is the scan's.
#[test]
fn a_volume_keeps_a_bounded_number_of_lost_witnesses() {
    const ARTICLES: usize = 100;
    const LOST: std::ops::Range<usize> = 10..30;
    let sizes: Vec<(u32, u32)> = (1..=ARTICLES as u32)
        .map(|ordinal| (ordinal, 100))
        .collect();
    let laid = || {
        let mut map = ArticleMap::new(std::slice::from_ref(&sizes));
        let found = &mut map.volumes[0];
        for boundary in 0..=ARTICLES {
            found.know(boundary, boundary as u64 * 100);
        }
        for index in LOST {
            found.set_state(&mut map.open, index, State::Lost { damaged: false });
        }
        map
    };
    let (mut map, mut scanned) = (laid(), laid());
    let volume = VolumeIndex(0);
    let seeks: Vec<(ByteRange, Option<Class>)> = LOST
        .flat_map(|index| {
            let start = index as u64 * 100;
            [
                (ByteRange::new(start, start + 50), None),
                (
                    ByteRange::new(start + 25, start + 75),
                    Some(Class::Critical),
                ),
            ]
        })
        .collect();
    assert!(seeks.len() > WITNESSES);
    let (mut most, mut lost) = (0, 0);
    // Rounds of every seek, an article arriving between them.
    for (round, arriving) in (40..46).enumerate() {
        for (range, class) in &seeks {
            let pull = map.map_seek(volume, *range, *class);
            let by_scan = scanned.map_seek_by_scan(volume, *range, *class);
            assert_eq!(pull, by_scan, "round {round}: {range:?} {class:?}");
            assert_eq!(map.moves, scanned.moves);
            assert_eq!(classes(&map), classes(&scanned));
            let held = map.volumes[0].lost_witness.len();
            assert!(held <= WITNESSES, "{held} witnesses held");
            most = most.max(held);
            lost += usize::from(pull == Pull::Lost);
        }
        for map in [&mut map, &mut scanned] {
            map.volumes[0].set_state(&mut map.open, arriving, State::Arrived(None));
        }
    }
    assert_eq!(most, WITNESSES, "the bound is reached");
    assert_eq!(lost, 6 * seeks.len(), "every seek answers lost");
}

// `SEEKS` lost seeks of one volume, each over its own lost article, asked
// through `seek` twice. The first round maps each: every one answers Lost
// and records its witness, the map emptied each time it holds `WITNESSES`,
// so `SEEKS - 2 * WITNESSES` stay held. The second round is answered from
// the memo, and the fresh mapping that checks each answer leaves the
// witnesses and the counters as the first round left them.
#[test]
fn a_memo_answer_checked_again_changes_no_witness_and_no_counter() {
    const ARTICLES: usize = 100;
    const SEEKS: usize = 20;
    let sizes: Vec<(u32, u32)> = (1..=ARTICLES as u32)
        .map(|ordinal| (ordinal, 100))
        .collect();
    let mut map = ArticleMap::new(std::slice::from_ref(&sizes));
    let found = &mut map.volumes[0];
    for boundary in 0..=ARTICLES {
        found.know(boundary, boundary as u64 * 100);
    }
    for index in 10..10 + SEEKS {
        found.set_state(&mut map.open, index, State::Lost { damaged: false });
    }
    let volume = VolumeIndex(0);
    let ranges: Vec<ByteRange> = (10..10 + SEEKS as u64)
        .map(|index| ByteRange::new(index * 100, index * 100 + 50))
        .collect();
    let counters = || {
        (
            SEEK_MAPPED.with(|count| count.get()),
            SEEK_EXAMINED.with(|count| count.get()),
            LOST_WITNESSED.with(|count| count.get()),
            LOST_NOT_FOUND.with(|count| count.get()),
        )
    };
    SEEK_MAPPED.with(|count| count.set(0));
    SEEK_EXAMINED.with(|count| count.set(0));
    LOST_WITNESSED.with(|count| count.set(0));
    LOST_NOT_FOUND.with(|count| count.set((0, 0)));
    for range in &ranges {
        assert_eq!(map.seek(volume, *range, None), Pull::Lost);
    }
    // Each mapping walked the one lost article in its run and found it.
    assert_eq!(counters(), (SEEKS, SEEKS, 0, (0, 0)));
    let witnesses = map.volumes[0].lost_witness.clone();
    assert_eq!(witnesses.len(), SEEKS - 2 * WITNESSES);
    for range in &ranges {
        assert_eq!(map.seek(volume, *range, None), Pull::Lost);
    }
    assert_eq!(counters(), (SEEKS, SEEKS, 0, (0, 0)));
    assert_eq!(map.volumes[0].lost_witness, witnesses);
}

// A volume decides as its articles change state and class, and a restore
// rebuilds what it counts: `undecided` asserts its counts against its
// articles on every call.
#[test]
fn a_volume_decides_from_its_counts_as_from_its_articles() {
    let mut decided = 0;
    let mut undecided = 0;
    for seed in 0..200 {
        let mut rng = Seeded(seed);
        let sizes = sizes(&mut rng);
        let mut map = ArticleMap::new(&sizes);
        for step in 0..120 {
            let volume = rng.below(map.volumes.len() as u64) as usize;
            let found = &mut map.volumes[volume];
            if found.articles.is_empty() {
                continue;
            }
            let index = rng.below(found.articles.len() as u64) as usize;
            if rng.chance(50) {
                let state = match rng.below(4) {
                    0 | 1 => State::Waiting,
                    2 => State::Arrived(None),
                    _ => State::Lost { damaged: false },
                };
                found.set_state(&mut map.open, index, state);
            } else {
                found.set_class(&mut map.open, index, class(&mut rng));
            }
            match map.undecided(VolumeIndex(volume as u32)) {
                Some(_) => undecided += 1,
                None => decided += 1,
            }
            if step % 40 == 39 {
                let mut restored = ArticleMap::new(&sizes);
                restored
                    .restore(&facts(&map))
                    .expect("the facts of the same post");
                assert_undecided(&restored);
                for (live, restored) in map.volumes.iter().zip(&restored.volumes) {
                    assert_eq!(
                        live.settled_past_first, restored.settled_past_first,
                        "seed {seed}"
                    );
                }
            }
        }
    }
    assert!(decided > 0 && undecided > 0, "{decided} {undecided}");
}

// The volumes to come are filed as articles change state and class, as a
// walk of every volume still waited on finds them, and each flip into or
// out of them is marked once, under a later generation; a restore files
// them again from the states.
#[test]
fn the_volumes_to_come_are_filed_and_each_flip_is_marked() {
    let mut flips = 0;
    for seed in 0..200 {
        let mut rng = Seeded(seed);
        let sizes = sizes(&mut rng);
        let mut map = ArticleMap::new(&sizes);
        for _ in 0..120 {
            let before: BTreeSet<VolumeIndex> = map.coming().collect();
            let at = map.come_generation();
            let volume = rng.below(map.volumes.len() as u64) as usize;
            let found = &mut map.volumes[volume];
            if found.articles.is_empty() {
                continue;
            }
            let index = rng.below(found.articles.len() as u64) as usize;
            if rng.chance(50) {
                let state = match rng.below(4) {
                    0 | 1 => State::Waiting,
                    2 => State::Arrived(None),
                    _ => State::Lost { damaged: false },
                };
                found.set_state(&mut map.open, index, state);
            } else {
                found.set_class(&mut map.open, index, class(&mut rng));
            }
            let after: BTreeSet<VolumeIndex> = map.coming().collect();
            let flipped: BTreeSet<VolumeIndex> =
                before.symmetric_difference(&after).copied().collect();
            let marked: BTreeSet<VolumeIndex> = map.come_flips_since(at).collect();
            assert_eq!(marked, flipped, "seed {seed}");
            assert_eq!(
                map.come_generation() > at,
                !flipped.is_empty(),
                "seed {seed}"
            );
            flips += flipped.len();
        }
        let mut restored = ArticleMap::new(&sizes);
        restored
            .restore(&facts(&map))
            .expect("the facts of the same post");
        assert_eq!(
            restored.coming().collect::<Vec<_>>(),
            restored.coming_by_walk(),
            "seed {seed}"
        );
    }
    assert!(flips > 0);
}

// The facts a checkpoint takes of the map, as the root encodes them.
fn facts(map: &ArticleMap) -> Vec<VolumeFacts> {
    (0..map.volumes.len())
        .map(|volume| {
            let found = &map.volumes[volume];
            let chunks = found.articles.len() as u32 / CHUNK + 1;
            let mut articles = Vec::new();
            let mut raised = Vec::new();
            let mut known = Vec::new();
            for chunk in 0..chunks + 1 {
                articles.extend(map.articles_in(volume, chunk));
                raised.extend(map.raised_in(volume, chunk));
                known.extend(map.known_in(volume, chunk));
            }
            VolumeFacts {
                articles,
                known,
                ordered_only: found.ordered_only,
                segments: Vec::new(),
                raised,
            }
        })
        .collect()
}

#[test]
fn a_restored_map_files_its_waiting_articles_by_class_as_the_live_one() {
    for seed in 0..200 {
        let mut rng = Seeded(seed);
        let sizes = sizes(&mut rng);
        let mut live = ArticleMap::new(&sizes);
        for found in &mut live.volumes {
            for index in 0..found.articles.len() {
                let state = match rng.below(4) {
                    0 | 1 => State::Waiting,
                    2 => State::Arrived(None),
                    _ => State::Lost { damaged: false },
                };
                found.set_state(&mut live.open, index, state);
                // A parked article is held back again by its recovery, never
                // by the facts.
                let raised = class(&mut rng);
                if raised != Class::Parked {
                    found.set_class(&mut live.open, index, raised);
                }
            }
        }
        let mut restored = ArticleMap::new(&sizes);
        restored
            .restore(&facts(&live))
            .expect("the facts of the same post");
        assert_filed(&restored);
        for (live, restored) in live.volumes.iter().zip(&restored.volumes) {
            assert_eq!(live.raised, restored.raised, "seed {seed}");
            assert_eq!(live.parkable, restored.parkable, "seed {seed}");
            assert_eq!(live.parked, restored.parked, "seed {seed}");
        }
    }
}

// A volume placed only in order keeps its front as its articles arrive: an
// arrival at the article short of the front looks at it and the one after,
// one past it looks at nothing, and a change among the articles that reach
// the front reads them again. Every seek reads the kept front, which the
// tests hold to the walk from the first article.
#[test]
fn an_ordered_volume_keeps_its_front_at_the_cost_of_the_articles_it_reaches() {
    use crate::pipeline::graph::root::hooks::FRONT_EXAMINED;
    const ARTICLES: u32 = 300;
    let sizes: Vec<(u32, u32)> = (1..=ARTICLES).map(|ordinal| (ordinal, 100)).collect();
    let volume = VolumeIndex(0);
    let place = |map: &mut ArticleMap, ordinal: u32| {
        let start = u64::from(ordinal - 1) * 90;
        map.arrived(
            volume,
            ordinal,
            ByteRange::new(start, start + 90),
            Some(start + 90),
            true,
        );
    };
    let front = |ordinal: u32| u64::from(ordinal) * 90;

    // In order, each arrival seeked to its front and just past it.
    let mut map = ArticleMap::new(std::slice::from_ref(&sizes));
    map.set_ordered_only(volume);
    FRONT_EXAMINED.with(|count| count.set(0));
    for ordinal in 1..=ARTICLES {
        place(&mut map, ordinal);
        let reached = ByteRange::new(0, front(ordinal));
        assert_eq!(map.seek(volume, reached, None), Pull::Moved(0));
        let past = ByteRange::new(0, front(ordinal) + 1);
        let pull = map.seek(volume, past, Some(Class::Critical));
        if ordinal < ARTICLES {
            assert_eq!(pull, Pull::Unseekable, "{ordinal}");
        }
    }
    assert_eq!(
        FRONT_EXAMINED.with(|count| count.get()),
        2 * ARTICLES as usize - 1
    );

    // The first article last: the others look at nothing, and it reaches
    // every one of them.
    let mut map = ArticleMap::new(std::slice::from_ref(&sizes));
    map.set_ordered_only(volume);
    FRONT_EXAMINED.with(|count| count.set(0));
    for ordinal in 2..=ARTICLES {
        place(&mut map, ordinal);
        let past = ByteRange::new(0, 1);
        assert_eq!(map.seek(volume, past, None), Pull::Unseekable);
    }
    assert_eq!(FRONT_EXAMINED.with(|count| count.get()), 0);
    place(&mut map, 1);
    let whole = ByteRange::new(0, front(ARTICLES));
    assert_eq!(map.seek(volume, whole, None), Pull::Moved(0));
    assert_eq!(FRONT_EXAMINED.with(|count| count.get()), ARTICLES as usize);

    // Lost among them, the sixth cuts the front back to the five before it.
    FRONT_EXAMINED.with(|count| count.set(0));
    let found = &mut map.volumes[0];
    found.set_state(&mut map.open, 5, State::Lost { damaged: false });
    assert_eq!(FRONT_EXAMINED.with(|count| count.get()), 6);
    let cut = ByteRange::new(0, front(5) + 1);
    assert_eq!(map.seek(volume, cut, None), Pull::Unseekable);
    assert_eq!(
        map.seek(volume, ByteRange::new(0, front(5)), None),
        Pull::Moved(0)
    );
}
