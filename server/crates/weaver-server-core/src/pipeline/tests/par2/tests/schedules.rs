// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Replayable arrival schedules of a PAR2 set's source and recovery articles,
// run through the graph engine. Fixtures carry real GF(2^16) recovery from
// the set's creator, never placeholder recovery bytes.
//
// A schedule serves the articles the job asks for in its own order and takes
// the pipeline down where it says. The job must finish as a run served in
// the posting order finishes, ask again for nothing it already had on disk,
// and come to what its damage and its recovery say: every described file
// delivered byte for byte when the set holds the blocks, and never a file
// claimed whole that is not when it does not.

use std::collections::BTreeMap;

use crate::history::HistoryFailureKind;
use crate::jobs::model::JobStatus;
use crate::pipeline::tests::engines::harness::{
    ARTICLE, Outcome, Post, Scenario, par2_volumes_sliced, recovery_blocks, recovery_packet, run,
};
use crate::pipeline::tests::engines::restart::{
    Down, Restarted, run_restarted_while_repairing, run_restarting_at,
};

#[derive(Clone, Copy, Debug)]
enum Shape {
    ShortTail,
    MultipleFiles,
    Nested,
    DuplicateContents,
    BeyondQuickHash,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Damage {
    Clean,
    Corrupt,
    Truncated,
    Missing,
    Shifted,
    Renamed,
    Exhausted,
}

const DAMAGES: [Damage; 7] = [
    Damage::Clean,
    Damage::Corrupt,
    Damage::Truncated,
    Damage::Missing,
    Damage::Shifted,
    Damage::Renamed,
    Damage::Exhausted,
];

// One step of a schedule, before it is laid out as articles.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Event {
    // The first half of a source file's articles, or the rest of them.
    Source(usize, u8),
    // A recovery volume's first article, or the rest of it.
    Recovery(usize, u8),
    // The pipeline goes down and the job is restored.
    Restart,
}

struct Random(u64);

impl Random {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e3779b97f4a7c15);
        let mut n = self.0;
        n = (n ^ (n >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        n = (n ^ (n >> 27)).wrapping_mul(0x94d049bb133111eb);
        n ^ (n >> 31)
    }
}

struct Fixture {
    // The files the set describes, by the names it gives them.
    files: Vec<(String, Vec<u8>)>,
    block: usize,
    // How many recovery blocks the set holds, and in how many volumes.
    recovery: u32,
    volumes: u32,
}

impl Fixture {
    fn new(shape: Shape, seed: u64) -> Self {
        Self::with_recovery(shape, seed, 12, 4)
    }

    fn with_recovery(shape: Shape, seed: u64, recovery: u32, volumes: u32) -> Self {
        let mut random = Random(seed);
        let block = if matches!(shape, Shape::BeyondQuickHash) {
            4096
        } else {
            ARTICLE
        };
        let count = if matches!(shape, Shape::MultipleFiles | Shape::DuplicateContents) {
            3
        } else {
            1
        };
        let mut files = Vec::new();
        for i in 0..count {
            let len = if matches!(shape, Shape::BeyondQuickHash) {
                block * 5 + 13
            } else {
                block * 2 + 13 + i
            };
            let name = if matches!(shape, Shape::Nested) {
                format!("nested/level/source-{i}.bin")
            } else {
                format!("source-{i}.bin")
            };
            let bytes: Vec<u8> = (0..len).map(|_| random.next() as u8).collect();
            files.push((name, bytes));
        }
        if matches!(shape, Shape::DuplicateContents) {
            for i in 1..files.len() {
                files[i].1 = files[0].1.clone();
            }
        }
        if matches!(shape, Shape::MultipleFiles) {
            files.push(("empty.bin".into(), Vec::new()));
        }
        Self {
            files,
            block,
            recovery,
            volumes,
        }
    }

    // The set over the files: its index first, then its volumes in block
    // order.
    fn set(&self, recovery: u32, volumes: u32) -> Vec<Post> {
        let described: Vec<(&str, &[u8])> = self
            .files
            .iter()
            .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
            .collect();
        par2_volumes_sliced("lantern", &described, self.block as u64, recovery, volumes)
    }

    // The articles of the posted file at `post`.
    fn articles(&self, post: usize) -> u32 {
        self.posted()
            .nth(post)
            .map_or(0, |(_, (_, bytes))| bytes.len().div_ceil(ARTICLE) as u32)
    }

    // The described files the job posts, by their place in the job: a file
    // with no bytes has no article, so no post carries it.
    fn posted(&self) -> impl Iterator<Item = (usize, &(String, Vec<u8>))> {
        self.files
            .iter()
            .filter(|(_, bytes)| !bytes.is_empty())
            .enumerate()
    }

    // The job as posted under `damage`: the source files, then the set. An
    // exhausted set keeps one recovery block in one volume.
    fn scenario(&self, damage: Damage) -> Scenario {
        let (recovery, volumes) = if damage == Damage::Exhausted {
            (1, 1)
        } else {
            (self.recovery, self.volumes)
        };
        let mut posts: Vec<Post> = self
            .posted()
            .map(|(i, (name, original))| {
                let posted = name.rsplit('/').next().unwrap();
                match damage {
                    Damage::Shifted => {
                        let mut bytes = vec![0x9b, 0x7d, 0x1f];
                        bytes.extend_from_slice(original);
                        Post::plain(posted, bytes)
                    }
                    Damage::Renamed => Post::plain(&format!("renamed-{i}.bin"), original.clone()),
                    Damage::Exhausted => Post::plain(posted, vec![0x5a; original.len()]),
                    _ => Post::plain(posted, original.clone()),
                }
            })
            .collect();
        posts.extend(self.set(recovery, volumes));
        let mut scenario = Scenario::new(posts);
        for (i, (_, original)) in self.posted() {
            let file = i as u32;
            let articles = self.articles(i);
            match damage {
                Damage::Corrupt => {
                    // The wide fixture damages bytes beyond the 16 KiB quick
                    // hash.
                    let at = if original.len() > 16 * 1024 {
                        16 * 1024 + 1
                    } else {
                        1
                    };
                    scenario = scenario.damaged(file, (at / ARTICLE) as u32);
                }
                Damage::Truncated => {
                    for ordinal in articles / 2..articles {
                        scenario = scenario.lost(file, ordinal);
                    }
                }
                Damage::Missing => {
                    for ordinal in 0..articles {
                        scenario = scenario.lost(file, ordinal);
                    }
                }
                _ => {}
            }
        }
        scenario
    }

    // The posts the set's volumes are, by their place in the job.
    fn volume_posts(&self, damage: Damage) -> std::ops::Range<usize> {
        let volumes = if damage == Damage::Exhausted {
            1
        } else {
            self.volumes as usize
        };
        let first = self.posted().count() + 1;
        first..first + volumes
    }

    // A seeded schedule: each source file in two halves, each volume's
    // first article then the rest of it, and two restarts, interleaved.
    fn schedule(&self, seed: u64, damage: Damage) -> Vec<Event> {
        let mut chains: Vec<Vec<Event>> = self
            .posted()
            .map(|(i, _)| vec![Event::Source(i, 1), Event::Source(i, 2)])
            .collect();
        for i in 0..self.volume_posts(damage).len() {
            chains.push(vec![Event::Recovery(i, 0), Event::Recovery(i, 1)]);
        }
        chains.push(vec![Event::Restart, Event::Restart]);
        let mut random = Random(seed);
        let mut result = vec![];
        while !chains.is_empty() {
            let i = random.next() as usize % chains.len();
            result.push(chains[i].remove(0));
            if chains[i].is_empty() {
                chains.remove(i);
            }
        }
        result
    }

    // The schedule laid out as articles in the order they are served, and
    // after how many distinct articles each restart falls.
    fn lay_out(
        &self,
        scenario: &Scenario,
        damage: Damage,
        events: &[Event],
    ) -> (Vec<(u32, u32)>, Vec<usize>) {
        let mut order = Vec::new();
        let mut restarts = Vec::new();
        let volumes = self.volume_posts(damage);
        for event in events {
            match *event {
                Event::Source(i, half) => {
                    let articles = self.articles(i);
                    let range = if half == 1 {
                        0..articles / 2
                    } else {
                        articles / 2..articles
                    };
                    order.extend(range.map(|ordinal| (i as u32, ordinal)));
                }
                Event::Recovery(i, step) => {
                    let file = volumes.start + i;
                    let articles = scenario.posts[file].bytes.len().div_ceil(ARTICLE) as u32;
                    let range = if step == 0 { 0..1 } else { 1..articles };
                    order.extend(range.map(|ordinal| (file as u32, ordinal)));
                }
                Event::Restart => restarts.push(order.len().max(1)),
            }
        }
        (order, restarts)
    }
}

// The delivered file that carries a described name, wherever the job put it.
fn delivered<'a>(files: &'a BTreeMap<String, Vec<u8>>, name: &str) -> Option<&'a Vec<u8>> {
    let base = name.rsplit('/').next().unwrap();
    files
        .iter()
        .find(|(path, _)| path.rsplit('/').next() == Some(base))
        .map(|(_, bytes)| bytes)
}

// The run in posting order came to what the damage and the recovery say:
// every described file whole when the set can mend it, else a job that is
// not complete, counts whole only described files delivered byte for byte,
// and fails as a repair when it counts none. A file listed with no bytes
// was never a file of the job, and is never delivered.
fn judge(fixture: &Fixture, whole: &Outcome, sufficient: bool, context: &str) {
    for (name, _) in fixture.files.iter().filter(|(_, bytes)| bytes.is_empty()) {
        assert!(
            delivered(&whole.files, name).is_none(),
            "{context}: {name} was listed with no article and delivered"
        );
    }
    if sufficient {
        assert_eq!(
            whole.status,
            Some(JobStatus::Complete),
            "{context}: {:?} {:?}",
            whole.failure,
            whole.files.keys().collect::<Vec<_>>()
        );
        for (_, (name, original)) in fixture.posted() {
            let got = delivered(&whole.files, name);
            assert!(
                got == Some(original),
                "{context}: {name} not delivered whole: {} bytes delivered, {} described, \
                 first difference at {:?}, in {:?}",
                got.map_or(0, Vec::len),
                original.len(),
                got.and_then(|bytes| (0..bytes.len().max(original.len()))
                    .find(|at| bytes.get(*at) != original.get(*at))),
                whole.files.keys().collect::<Vec<_>>()
            );
        }
        return;
    }
    // A file the job counts whole is byte for byte the described one; a
    // file it does not is delivered as it stands, short of its bytes.
    let counted = &whole.graph().delivery.whole;
    let mut any = false;
    for (_, (name, original)) in fixture.posted() {
        let Some(bytes) = delivered(&whole.files, name) else {
            continue;
        };
        if counted
            .iter()
            .any(|counted| counted.rsplit('/').next() == Some(name))
        {
            assert!(
                bytes == original,
                "{context}: {name} counted whole, delivered damaged"
            );
            any = true;
        } else {
            assert!(bytes != original, "{context}: {name} whole, not counted");
        }
    }
    match &whole.status {
        Some(JobStatus::Failed { .. }) => {
            assert!(!any, "{context}: a failed job delivered a file");
            assert_eq!(
                whole.failure_kind,
                Some(HistoryFailureKind::Repair),
                "{context}: {:?}",
                whole.failure
            );
        }
        Some(JobStatus::Partial) => assert!(any, "{context}: partial with no file whole"),
        other => panic!("{context}: a set short of blocks came to {other:?}"),
    }
}

// A scheduled run finished as the run in posting order did, and asked again
// only for what a restart found in memory.
fn agree(scheduled: &Restarted, whole: &Outcome, context: &str) {
    assert_eq!(scheduled.status, whole.status, "{context}: status");
    assert_eq!(
        scheduled.files.keys().collect::<Vec<_>>(),
        whole.files.keys().collect::<Vec<_>>(),
        "{context}: delivered names"
    );
    for (name, bytes) in &whole.files {
        assert!(
            scheduled.files[name] == *bytes,
            "{context}: {name}: delivered bytes"
        );
    }
    assert!(
        scheduled.from_disk.is_empty(),
        "{context}: fetched again though on disk: {:?}",
        scheduled.from_disk
    );
}

// Serves `order` first, restarting after each count of distinct articles in
// `restarts`.
async fn scheduled(scenario: Scenario, order: Vec<(u32, u32)>, restarts: &[usize]) -> Restarted {
    let scenario = scenario.first(&order);
    run_restarting_at(&scenario, &mut |_| {}, Down::Crash, &|asked| {
        restarts.contains(&asked)
    })
    .await
}

fn sufficient(damage: Damage) -> bool {
    damage != Damage::Exhausted
}

async fn replay(fixture: &Fixture, damage: Damage, events: &[Event], context: &str) {
    let whole = run(&fixture.scenario(damage)).await;
    judge(fixture, &whole, sufficient(damage), context);
    let scenario = fixture.scenario(damage);
    let (order, restarts) = fixture.lay_out(&scenario, damage, events);
    // A malformed copy of a volume's first article reaches the job before
    // its valid one: it must not take that block's place.
    let first_volume = fixture.volume_posts(damage).start as u32;
    let scenario = scenario.corrupt_on_primary(first_volume, 0);
    let ran = scheduled(scenario, order, &restarts).await;
    agree(&ran, &whole, &format!("{context} trace={events:?}"));
}

async fn campaign(shape: Shape) {
    let cases = std::env::var("WEAVER_PAR2_SCHEDULE_CASES")
        .map(|s| s.parse::<u64>().expect("positive schedule count"))
        .unwrap_or(4);
    assert!(cases > 0);
    let selected = std::env::var("WEAVER_PAR2_SCHEDULE_SEED")
        .ok()
        .map(|s| s.parse::<u64>().expect("decimal replay seed"));
    for seed in selected.map_or_else(|| (0..cases).collect(), |seed| vec![seed]) {
        let fixture = Fixture::new(shape, seed);
        for damage in DAMAGES {
            let events = fixture.schedule(seed, damage);
            replay(
                &fixture,
                damage,
                &events,
                &format!("{shape:?} {damage:?} seed={seed}"),
            )
            .await;
        }
    }
}

#[tokio::test]
async fn short_tail_schedules() {
    campaign(Shape::ShortTail).await;
}

#[tokio::test]
async fn multiple_files_and_empty_schedules() {
    campaign(Shape::MultipleFiles).await;
}

#[tokio::test]
async fn nested_path_schedules() {
    campaign(Shape::Nested).await;
}

#[tokio::test]
async fn duplicate_content_schedules() {
    campaign(Shape::DuplicateContents).await;
}

#[tokio::test]
async fn beyond_quick_hash_schedules() {
    campaign(Shape::BeyondQuickHash).await;
}

// Every order in which the chains' heads can be taken, each chain keeping
// its own order.
fn interleavings(chains: &mut [Vec<Event>], prefix: &mut Vec<Event>, output: &mut Vec<Vec<Event>>) {
    if chains.iter().all(Vec::is_empty) {
        output.push(prefix.clone());
        return;
    }
    for i in 0..chains.len() {
        if chains[i].is_empty() {
            continue;
        }
        let event = chains[i].remove(0);
        prefix.push(event);
        interleavings(chains, prefix, output);
        prefix.pop();
        chains[i].insert(0, event);
    }
}

// The seeded campaigns sample a large event space. This one is exhaustive
// over a small one: one source's two halves, one volume's first article
// (served malformed first) and the rest of it, and two restarts, in every
// one of their 90 interleavings. The rest of the recovery the repair needs
// arrives after the interleaving, in a fixed order, so each damage finishes
// against the same recovery whichever order came first.
async fn exhaustive_campaign(damage: Damage) {
    let fixture = Fixture::with_recovery(Shape::ShortTail, 0x45584841, 4, 4);
    let mut orders = Vec::new();
    interleavings(
        &mut [
            vec![Event::Source(0, 1), Event::Source(0, 2)],
            vec![Event::Recovery(0, 0), Event::Recovery(0, 1)],
            vec![Event::Restart, Event::Restart],
        ],
        &mut Vec::new(),
        &mut orders,
    );
    assert_eq!(orders.len(), 90);
    let rest = fixture.volume_posts(damage).len();
    for (case, order) in orders.iter().enumerate() {
        let mut events = order.clone();
        events.extend((1..rest).flat_map(|i| [Event::Recovery(i, 0), Event::Recovery(i, 1)]));
        replay(
            &fixture,
            damage,
            &events,
            &format!("{damage:?} case={case}"),
        )
        .await;
    }
}

#[tokio::test]
async fn every_interleaving_clean() {
    exhaustive_campaign(Damage::Clean).await;
}

#[tokio::test]
async fn every_interleaving_corrupt() {
    exhaustive_campaign(Damage::Corrupt).await;
}

#[tokio::test]
async fn every_interleaving_truncated() {
    exhaustive_campaign(Damage::Truncated).await;
}

#[tokio::test]
async fn every_interleaving_missing() {
    exhaustive_campaign(Damage::Missing).await;
}

#[tokio::test]
async fn every_interleaving_shifted() {
    exhaustive_campaign(Damage::Shifted).await;
}

#[tokio::test]
async fn every_interleaving_renamed() {
    exhaustive_campaign(Damage::Renamed).await;
}

#[tokio::test]
async fn every_interleaving_exhausted() {
    exhaustive_campaign(Damage::Exhausted).await;
}

// Two files of three slices each and a set of six blocks in six volumes of
// one block: each damage mask over the six slices against each subset of the
// six blocks, a missing block being a volume every server lacks. The masks
// are shared out over sixteen tests by the damage mask's top bits.
async fn six_block_subsets(shard: u32) {
    let mut random = Random(0x50415232);
    let files: Vec<(String, Vec<u8>)> = (0..2)
        .map(|i| {
            (
                format!("file-{i}.bin"),
                (0..192).map(|_| random.next() as u8).collect(),
            )
        })
        .collect();
    let fixture = Fixture {
        files,
        block: ARTICLE,
        recovery: 6,
        volumes: 6,
    };
    let set = fixture.set(6, 6);
    for (exponent, volume) in set[1..].iter().enumerate() {
        assert_eq!(
            recovery_blocks(volume).len(),
            1,
            "volume {exponent} holds one block"
        );
    }
    for damaged in shard * 4..shard * 4 + 4 {
        for recovery in 0u32..64 {
            let mut scenario = fixture.scenario(Damage::Clean);
            for (i, _) in fixture.files.iter().enumerate() {
                for slice in 0..3 {
                    if damaged & (1 << (i * 3 + slice)) != 0 {
                        scenario = scenario.damaged(i as u32, slice as u32);
                    }
                }
            }
            for exponent in 0..6u32 {
                if recovery & (1 << exponent) == 0 {
                    let file = fixture.files.len() as u32 + 1 + exponent;
                    let articles =
                        scenario.posts[file as usize].bytes.len().div_ceil(ARTICLE) as u32;
                    for ordinal in 0..articles {
                        scenario = scenario.lost(file, ordinal);
                    }
                }
            }
            let whole = run(&scenario).await;
            let context = format!("damage={damaged:06b} recovery={recovery:06b}");
            judge(
                &fixture,
                &whole,
                recovery.count_ones() >= damaged.count_ones(),
                &context,
            );
            // Nothing needed repair: no member was read back.
            if damaged == 0 {
                assert_eq!(whole.reads, 0, "{context}: a clean set read its files");
            }
        }
    }
}

macro_rules! six_block_shards {
    ($($name:ident $shard:literal)+) => {
        $(
            #[tokio::test]
            async fn $name() {
                six_block_subsets($shard).await;
            }
        )+
    };
}

six_block_shards! {
    every_damage_and_recovery_subset_of_six_blocks_00 0
    every_damage_and_recovery_subset_of_six_blocks_01 1
    every_damage_and_recovery_subset_of_six_blocks_02 2
    every_damage_and_recovery_subset_of_six_blocks_03 3
    every_damage_and_recovery_subset_of_six_blocks_04 4
    every_damage_and_recovery_subset_of_six_blocks_05 5
    every_damage_and_recovery_subset_of_six_blocks_06 6
    every_damage_and_recovery_subset_of_six_blocks_07 7
    every_damage_and_recovery_subset_of_six_blocks_08 8
    every_damage_and_recovery_subset_of_six_blocks_09 9
    every_damage_and_recovery_subset_of_six_blocks_10 10
    every_damage_and_recovery_subset_of_six_blocks_11 11
    every_damage_and_recovery_subset_of_six_blocks_12 12
    every_damage_and_recovery_subset_of_six_blocks_13 13
    every_damage_and_recovery_subset_of_six_blocks_14 14
    every_damage_and_recovery_subset_of_six_blocks_15 15
}

// A damaged set taken down at each boundary its run passes: after every
// article asked, shut down as an orderly stop takes it down, and once while
// its repair is out on a thread. The restored job repairs from what reached
// disk, and the damaged file is never delivered as it arrived.
#[tokio::test]
async fn a_shutdown_at_each_boundary_preserves_the_repair() {
    let fixture = Fixture::new(Shape::ShortTail, 91);
    let scenario = || fixture.scenario(Damage::Corrupt);
    let whole = run(&scenario()).await;
    judge(&fixture, &whole, true, "posting order");
    let asked = whole.requests.len();
    for at in 1..=asked {
        let ran = run_restarting_at(&scenario(), &mut |_| {}, Down::Shutdown, &|n| n == at).await;
        agree(&ran, &whole, &format!("shut down after {at} of {asked}"));
    }
    let (done, _) = run_restarted_while_repairing(&scenario()).await;
    assert_eq!(done.status, whole.status, "shut down while repairing");
    assert_eq!(done.files, whole.files, "shut down while repairing");
}

// Each bit of a recovery packet flipped, and each truncation of it, posted
// in the job's only volume before and after a valid copy of the same packet:
// the walk past the bad copy must keep the valid one, and the one damaged
// slice is repaired with it. The mutations are shared out over eight tests.
async fn mutated_recovery_packets(shard: usize) {
    let fixture = Fixture::with_recovery(Shape::ShortTail, 0x5041434b4554, 1, 1);
    let set = fixture.set(1, 1);
    let valid = recovery_packet(&set[1]);
    let mutations = valid.len() * 9;
    let per = mutations.div_ceil(8);
    for mutation in shard * per..((shard + 1) * per).min(mutations) {
        let mut bad = valid.clone();
        if mutation < valid.len() * 8 {
            bad[mutation / 8] ^= 1 << (mutation % 8);
        } else {
            bad.truncate(mutation - valid.len() * 8);
        }
        for bad_first in [false, true] {
            let mut volume = set[0].bytes.clone();
            if bad_first {
                volume.extend_from_slice(&bad);
                volume.extend_from_slice(&valid);
            } else {
                volume.extend_from_slice(&valid);
                volume.extend_from_slice(&bad);
            }
            let (name, original) = &fixture.files[0];
            let posts = vec![
                Post::plain(name, original.clone()),
                Post::plain(&set[0].name, set[0].bytes.clone()),
                Post::plain(&set[1].name, volume),
            ];
            let whole = run(&Scenario::new(posts).damaged(0, 1)).await;
            judge(
                &fixture,
                &whole,
                true,
                &format!("mutation={mutation} bad_first={bad_first}"),
            );
        }
    }
}

macro_rules! mutation_shards {
    ($($name:ident $shard:literal)+) => {
        $(
            #[tokio::test]
            async fn $name() {
                mutated_recovery_packets($shard).await;
            }
        )+
    };
}

mutation_shards! {
    every_recovery_packet_byte_and_truncation_preserves_the_valid_copy_0 0
    every_recovery_packet_byte_and_truncation_preserves_the_valid_copy_1 1
    every_recovery_packet_byte_and_truncation_preserves_the_valid_copy_2 2
    every_recovery_packet_byte_and_truncation_preserves_the_valid_copy_3 3
    every_recovery_packet_byte_and_truncation_preserves_the_valid_copy_4 4
    every_recovery_packet_byte_and_truncation_preserves_the_valid_copy_5 5
    every_recovery_packet_byte_and_truncation_preserves_the_valid_copy_6 6
    every_recovery_packet_byte_and_truncation_preserves_the_valid_copy_7 7
}
