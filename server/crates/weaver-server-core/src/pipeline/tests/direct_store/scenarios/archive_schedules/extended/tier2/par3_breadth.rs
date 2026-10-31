// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// PAR3 across its surface: a sidecar set posted beside the archive; Cauchy
// and FFT codes; recovery with margin, exact and one block short; three
// block sizes; one member and two; one archive set in the job and two; the
// index present, absent and damaged.
use super::super::super::super::sevenz_store::{Entry, build_7z_shaped, split_volumes};
use super::fixtures::{Container, MEMBER, SEVENZ_MEMBER, payload};
use super::post::{Damage, Post, Posted, Role, Wire};
use super::recovery::{self, Code, Geometry, MARGINS, Margin, recovery_packets};
use super::*;

const ARTICLE: usize = 768;
const VOLUMES: usize = 4;
const ARTICLES_PER_VOLUME: usize = 32;

const BLOCKS: [usize; 3] = [512, 1024, 4096];

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum Entries {
    One,
    Two,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum Sets {
    One,
    // Two archive sets in one job, each with its own recovery.
    Two,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum Index {
    Present,
    Absent,
    // Every index article arrives CRC-damaged.
    Damaged,
}

const INDICES: [Index; 3] = [Index::Present, Index::Absent, Index::Damaged];

const CONTAINERS: [Container; 2] = [Container::Rar5, Container::SevenZip];

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct Par3Cell {
    pub code: Code,
    pub margin: Margin,
    pub block: usize,
    pub entries: Entries,
    pub sets: Sets,
    pub index: Index,
    pub container: Container,
}

pub(super) fn cells() -> Vec<Par3Cell> {
    let mut cells = Vec::new();
    for code in [Code::Cauchy, Code::Fft] {
        for margin in MARGINS {
            for block in BLOCKS {
                for entries in [Entries::One, Entries::Two] {
                    for sets in [Sets::One, Sets::Two] {
                        for index in INDICES {
                            for container in CONTAINERS {
                                cells.push(Par3Cell {
                                    code,
                                    margin,
                                    block,
                                    entries,
                                    sets,
                                    index,
                                    container,
                                });
                            }
                        }
                    }
                }
            }
        }
    }
    cells
}

// Schedules each cell samples from the matrix's smoke schedules.
pub(super) const PER_UNIT: usize = 231;

// 432 cells, 231 schedules each.
pub(super) const TOTAL: usize = 99_792;

pub(super) fn family() -> Family<Par3Cell> {
    let cells = cells();
    assert_eq!(cells.len(), 432);
    Family {
        units: cells
            .into_iter()
            .enumerate()
            .map(|(index, cell)| (cell, Pool::Smoke, PER_UNIT, index))
            .collect(),
    }
}

// One archive set of the post: its prefix, its members and their bytes.
struct ArchiveSet {
    prefix: &'static str,
    members: Vec<(String, Vec<u8>)>,
}

const PREFIXES: [&str; 2] = ["silver.horizon", "amber.lantern"];

impl Par3Cell {
    fn archive_sets(self) -> Vec<ArchiveSet> {
        let count = if self.sets == Sets::Two { 2 } else { 1 };
        (0..count)
            .map(|set| {
                let len = VOLUMES * ARTICLES_PER_VOLUME * ARTICLE - 512;
                let first = payload(41 + set as u64 * 7, len);
                let mut members = vec![(self.member(set, 0), first)];
                if self.entries == Entries::Two {
                    members.push((self.member(set, 1), payload(53 + set as u64 * 7, len / 3)));
                }
                ArchiveSet {
                    prefix: PREFIXES[set],
                    members,
                }
            })
            .collect()
    }

    fn member(self, set: usize, entry: usize) -> String {
        let base = if self.container == Container::SevenZip {
            SEVENZ_MEMBER
        } else {
            MEMBER
        };
        match (set, entry) {
            (0, 0) => base.to_string(),
            (0, _) => base.replace("lantern", "beacon"),
            (_, 0) => base.replace("lantern", "amber"),
            (_, _) => base.replace("lantern", "ember"),
        }
    }

    // The posted volumes of one archive set.
    fn volumes(self, set: &ArchiveSet) -> Vec<(String, Vec<u8>)> {
        let mut volumes = match self.container {
            Container::SevenZip => {
                let entries: Vec<Entry> = set
                    .members
                    .iter()
                    .map(|(name, bytes)| Entry::file(leak(name), bytes.clone()))
                    .collect();
                let archive =
                    build_7z_shaped(&entries, sevenz_turbo::EncoderMethod::COPY, None, false);
                split_volumes(&archive, VOLUMES)
            }
            _ => {
                let members: Vec<(&str, Vec<u8>)> = set
                    .members
                    .iter()
                    .map(|(name, bytes)| (name.as_str(), bytes.clone()))
                    .collect();
                multi_member_store_set(&members, VOLUMES)
            }
        };
        for (name, _) in &mut volumes {
            *name = name.replacen("silver.horizon", set.prefix, 1);
        }
        volumes
    }

    fn mark_index(self, post: &mut Post) {
        for file in post.index_of(Role::Index) {
            let posted = &mut post.files[file];
            for article in 0..posted.articles() {
                posted.wire.insert(
                    article,
                    match self.index {
                        Index::Present => continue,
                        Index::Absent => Wire::Absent,
                        Index::Damaged => Wire::Damaged(Damage::CrcWrong),
                    },
                );
            }
        }
    }

    // The post with its sidecar sets authored to the margin over the data as
    // posted.
    fn build(self, interruption: Interruption) -> (Post, Option<Geometry>, Option<Verdict>) {
        let sets = self.archive_sets();
        let geometry = Geometry {
            block: self.block,
            packed: false,
        };
        let lost = move |post: &Post| run::lost_articles(post, interruption);
        // Sidecars, one per archive set, each authored over its own volumes.
        let data = self.data(&sets);
        let mut post = data.clone();
        let per_set = self.data_files(&data);
        let mut killed = vec![0usize; sets.len()];
        let mut blocks = vec![0usize; sets.len()];
        for _ in 0..8 {
            post = data.clone();
            let lost_now = lost(&data);
            for (set, files) in per_set.iter().enumerate() {
                let sources: Vec<(String, Vec<u8>)> = files
                    .iter()
                    .map(|&file| {
                        (
                            data.files[file].name.clone(),
                            data.files[file].bytes.clone(),
                        )
                    })
                    .collect();
                let needed = needed_over(&data, files, &lost_now, self.block);
                blocks[set] = recovery::blocks_for(self.margin, needed, killed[set], 2);
                let mut authored = recovery::par3_set(
                    &sources,
                    self.block,
                    blocks[set],
                    self.code,
                    blocks[set].div_ceil(4) as u64,
                );
                for (name, _) in &mut authored {
                    *name = name.replacen("silver.horizon", sets[set].prefix, 1);
                }
                recovery::post_set(&mut post, authored, ARTICLE);
            }
            self.mark_index(&mut post);
            let lost_now = lost(&post);
            let mut settled = true;
            for (set, _) in per_set.iter().enumerate() {
                let (least, most) = recovery_surviving(&post, sets[set].prefix, &lost_now);
                let after = if self.margin == Margin::OneShort {
                    blocks[set] - most.min(blocks[set])
                } else {
                    blocks[set] - least.min(blocks[set])
                };
                if after != killed[set] {
                    settled = false;
                }
                killed[set] = after;
            }
            if settled {
                break;
            }
        }
        let lost_now = lost(&post);
        let mut verdict = Verdict::Completes;
        for (set, files) in per_set.iter().enumerate() {
            let (upper, lower) = needed_over(&post, files, &lost_now, self.block);
            let (least, most) = recovery_surviving(&post, sets[set].prefix, &lost_now);
            let starved = interruption.fails();
            let (least, most) = if starved { (0, 0) } else { (least, most) };
            let mut sidecar = if upper <= least {
                Verdict::Completes
            } else if lower > most {
                Verdict::Fails
            } else {
                Verdict::Either
            };
            // Without its index the set's description must come from the
            // volumes' copies: the job may still end either way, named.
            if self.index != Index::Present && upper > 0 && sidecar != Verdict::Fails {
                sidecar = Verdict::Either;
            }
            verdict = combine(verdict, sidecar);
        }
        (post, Some(geometry), Some(verdict))
    }

    // The data files of each archive set, by set.
    fn data_files(self, post: &Post) -> Vec<Vec<usize>> {
        let count = if self.sets == Sets::Two { 2 } else { 1 };
        (0..count)
            .map(|set| {
                post.index_of(Role::Data)
                    .into_iter()
                    .filter(|&file| post.files[file].name.starts_with(PREFIXES[set]))
                    .collect()
            })
            .collect()
    }

    fn data(self, sets: &[ArchiveSet]) -> Post {
        let mut files = Vec::new();
        let mut expected = Vec::new();
        for set in sets {
            for (name, bytes) in self.volumes(set) {
                files.push(Posted::new(name, bytes, ARTICLE, Role::Data));
            }
            expected.extend(set.members.iter().cloned());
        }
        Post {
            files,
            password: None,
            expected,
            allowed: Vec::new(),
        }
    }
}

// The worse of two verdicts for a job that must publish every set.
fn combine(a: Verdict, b: Verdict) -> Verdict {
    match (a, b) {
        (Verdict::Fails, _) | (_, Verdict::Fails) => Verdict::Fails,
        (Verdict::Either, _) | (_, Verdict::Either) => Verdict::Either,
        _ => Verdict::Completes,
    }
}

// Entry names must outlive the builder; a handful of leaked strings per
// fixture is the price of the 7z writer's `&'static str` entries.
fn leak(name: &str) -> &'static str {
    Box::leak(name.to_string().into_boxed_str())
}

fn blocks_touched(ranges: &[Range<usize>], block: usize) -> usize {
    let mut touched = BTreeSet::new();
    for range in ranges {
        if range.is_empty() {
            continue;
        }
        for index in range.start / block..=(range.end - 1) / block {
            touched.insert(index);
        }
    }
    touched.len()
}

// Source blocks a sidecar over `files` may and must mend.
fn needed_over(
    post: &Post,
    files: &[usize],
    lost: &BTreeMap<usize, BTreeSet<u32>>,
    block: usize,
) -> (usize, usize) {
    let none = BTreeSet::new();
    let mut upper = 0;
    let mut lower = 0;
    for &file in files {
        let (may, must) = post.files[file].wrong_ranges(lost.get(&file).unwrap_or(&none));
        upper += blocks_touched(&may, block);
        lower += blocks_touched(&must, block);
    }
    (upper, lower)
}

// Recovery packets of the sidecar set named `prefix` that survive.
fn recovery_surviving(
    post: &Post,
    prefix: &str,
    lost: &BTreeMap<usize, BTreeSet<u32>>,
) -> (usize, usize) {
    let none = BTreeSet::new();
    let mut least = 0;
    let mut most = 0;
    for file in post.index_of(Role::Recovery) {
        let posted = &post.files[file];
        if !posted.name.starts_with(prefix) {
            continue;
        }
        let packets = recovery_packets(&posted.bytes);
        let (may, must) = posted.wrong_ranges(lost.get(&file).unwrap_or(&none));
        let hit = |ranges: &[Range<usize>], packet: &Range<usize>| {
            ranges
                .iter()
                .any(|range| range.start < packet.end && packet.start < range.end)
        };
        least += packets.iter().filter(|packet| !hit(&may, packet)).count();
        most += packets.iter().filter(|packet| !hit(&must, packet)).count();
    }
    (least, most)
}

impl Cell for Par3Cell {
    fn post(self, interruption: Interruption) -> Built {
        let (post, geometry, ruling) = self.build(interruption);
        Built {
            post,
            geometry,
            ruling,
        }
    }

    fn par2(self) -> bool {
        false
    }
}

macro_rules! par3_smokes {
    ($($name:ident $code:ident $margin:ident $block:literal $entries:ident $sets:ident $index:ident $container:ident;)+) => {
        mod combined_par3_breadth_smoke {
            use super::*;
            $(
                #[tokio::test]
                async fn $name() {
                    smoke(Par3Cell {
                        code: Code::$code,
                        margin: Margin::$margin,
                        block: $block,
                        entries: Entries::$entries,
                        sets: Sets::$sets,
                        index: Index::$index,
                        container: Container::$container,
                    })
                    .await;
                }
            )+
        }
    };
}

par3_smokes! {
    sidecar_cauchy_rar5 Cauchy With 1024 One One Present Rar5;
    sidecar_fft_sevenz_two_entries Fft Exact 512 Two One Present SevenZip;
    sidecar_two_sets_index_absent Cauchy With 4096 One Two Absent Rar5;
    sidecar_fft_two_sets_present Fft With 4096 One Two Present Rar5;
    sidecar_index_damaged_short Fft OneShort 1024 One One Damaged SevenZip;
}

mod combined_par3_breadth_loss_smoke {
    use super::*;

    #[tokio::test]
    async fn sidecar_fft_rar5() {
        loss_smoke(Par3Cell {
            code: Code::Fft,
            margin: Margin::With,
            block: 1024,
            entries: Entries::One,
            sets: Sets::One,
            index: Index::Present,
            container: Container::Rar5,
        })
        .await;
    }
}

// The campaign: 500 shards of about two hundred cases.
mod combined_par3_breadth {
    use super::*;

    tier2_shards!(family, 500; h0 0 h1 1 h2 2 h3 3 h4 4);
}
