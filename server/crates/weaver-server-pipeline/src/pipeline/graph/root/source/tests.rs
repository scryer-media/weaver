// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A seek visits only the articles it can move: the same answers and moves as
// testing every article of its run, at the cost of the articles it moves.

use std::collections::{BTreeMap, BTreeSet};

use weaver_graph::{ByteRange, Pull, VolumeIndex};

use super::{ArticleMap, Class, State, is_raised};
use crate::pipeline::graph::root::facts::{CHUNK, VolumeFacts};
use crate::pipeline::graph::root::hooks::{SEEK_EXAMINED, SEEK_MAPPED};

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
                found.set_class(index, class(&mut rng));
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
    let (mut lost, mut moved, mut still, mut extended, mut falling) = (0, 0, 0, 0, 0);
    for seed in 0..600 {
        let mut map = seeded(seed);
        let mut scanned = seeded(seed);
        assert_filed(&map);
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
            let pull = map.map_seek(volume, range, class);
            let by_scan = scanned.map_seek_by_scan(volume, range, class);
            assert_eq!(pull, by_scan, "seed {seed}: {volume:?} {range:?} {class:?}");
            assert_eq!(map.moves, scanned.moves, "seed {seed}");
            assert_eq!(classes(&map), classes(&scanned), "seed {seed}");
            assert_filed(&map);
            match pull {
                Pull::Lost => lost += 1,
                Pull::Moved(0) => still += 1,
                Pull::Moved(_) => moved += 1,
                _ => {}
            }
        }
    }
    assert!(lost > 0 && moved > 0 && still > 0, "{lost} {moved} {still}");
    assert!(falling > 0 && extended > 0, "{falling} {extended}");
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
                    found.set_class(index, raised);
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
