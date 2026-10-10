// PAR3 across its surface: a sidecar set, a set inserted inside a 7z after
// its end header, and both at once; Cauchy and FFT codes; recovery with
// margin, exact and one block short; three block sizes; one member and two;
// one archive set in the job and two; the index present, absent and damaged.
use super::super::super::super::sevenz_store::embedded_par3::with_embedded_par3;
use super::super::super::super::sevenz_store::{Entry, build_7z_shaped, split_volumes};
use super::fixtures::{Container, MEMBER, SEVENZ_MEMBER, payload};
use super::post::{Damage, Post, Posted, Role, Wire};
use super::recovery::{self, Code, Geometry, MARGINS, Margin, recovery_packets};
use super::*;

const ARTICLE: usize = 768;
const VOLUMES: usize = 4;
const ARTICLES_PER_VOLUME: usize = 32;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum Placement {
    // A PAR3 set posted beside the archive.
    Sidecar,
    // A PAR3 recovery tail inside the 7z, after its end header.
    Embedded,
    // Both: the sidecar protects the container, tail and all.
    Both,
}

const PLACEMENTS: [Placement; 3] = [Placement::Sidecar, Placement::Embedded, Placement::Both];

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
    pub placement: Placement,
    pub code: Code,
    pub margin: Margin,
    pub block: usize,
    pub entries: Entries,
    pub sets: Sets,
    pub index: Index,
    pub container: Container,
}

// Whether the cell can be posted: a tail is inserted into a 7z only, and a
// post with no sidecar has no index to lose.
fn possible(cell: Par3Cell) -> bool {
    if cell.placement != Placement::Sidecar && cell.container != Container::SevenZip {
        return false;
    }
    !(cell.placement == Placement::Embedded && cell.index != Index::Present)
}

pub(super) fn cells() -> Vec<Par3Cell> {
    let mut cells = Vec::new();
    for placement in PLACEMENTS {
        for code in [Code::Cauchy, Code::Fft] {
            for margin in MARGINS {
                for block in BLOCKS {
                    for entries in [Entries::One, Entries::Two] {
                        for sets in [Sets::One, Sets::Two] {
                            for index in INDICES {
                                for container in CONTAINERS {
                                    let cell = Par3Cell {
                                        placement,
                                        code,
                                        margin,
                                        block,
                                        entries,
                                        sets,
                                        index,
                                        container,
                                    };
                                    if possible(cell) {
                                        cells.push(cell);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    cells
}

// Schedules each (cell, profile) unit samples from the matrix's smoke
// schedules.
pub(super) const PER_UNIT: usize = 231;

// 720 possible cells under three profiles, 231 schedules each.
pub(super) const TOTAL: usize = 498_960;

pub(super) fn family() -> Family<Par3Cell> {
    let cells = cells();
    assert_eq!(cells.len(), 720);
    Family {
        units: cells
            .into_iter()
            .enumerate()
            .flat_map(|(index, cell)| {
                PROFILES
                    .into_iter()
                    .map(move |profile| (cell, profile, Pool::Smoke, PER_UNIT, index))
            })
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
        let base = match (set, entry) {
            (0, 0) => base.to_string(),
            (0, _) => base.replace("lantern", "beacon"),
            (_, 0) => base.replace("lantern", "amber"),
            (_, _) => base.replace("lantern", "ember"),
        };
        base
    }

    // The posted volumes of one archive set, with the tail inserted where
    // the placement says.
    fn volumes(self, set: &ArchiveSet, tail_blocks: usize) -> Vec<(String, Vec<u8>)> {
        let mut volumes = match self.container {
            Container::SevenZip => {
                let entries: Vec<Entry> = set
                    .members
                    .iter()
                    .map(|(name, bytes)| Entry::file(leak(name), bytes.clone()))
                    .collect();
                let mut archive =
                    build_7z_shaped(&entries, sevenz_turbo::EncoderMethod::COPY, None, false);
                if self.placement != Placement::Sidecar {
                    archive =
                        with_embedded_par3(&archive, self.block as u64, tail_blocks.max(1) as u64);
                }
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
    // posted (tail included), and the tail sized to the margin over the
    // container's own bytes.
    fn build(self, interruption: Interruption) -> (Post, Option<Geometry>, Option<Verdict>) {
        let sets = self.archive_sets();
        let geometry = Geometry {
            block: self.block,
            packed: false,
        };
        let lost = move |post: &Post| run::lost_articles(post, interruption);
        // The tail first: it changes the data bytes the sidecar protects.
        let mut tail_blocks = vec![0usize; sets.len()];
        let mut post = self.data(&sets, &tail_blocks);
        if self.placement != Placement::Sidecar {
            let mut killed = vec![0usize; sets.len()];
            for _ in 0..8 {
                let lost_now = lost(&post);
                let mut settled = true;
                for (set, files) in self.data_files(&post).into_iter().enumerate() {
                    let needed = tail_needed(&post, &files, &lost_now, self.block);
                    let blocks = recovery::blocks_for(self.margin, needed, killed[set], 2);
                    let (least, most) = tail_surviving(&post, &files, &lost_now);
                    let after = if self.margin == Margin::OneShort {
                        tail_blocks[set].saturating_sub(most)
                    } else {
                        tail_blocks[set].saturating_sub(least)
                    };
                    if blocks != tail_blocks[set] || after != killed[set] {
                        settled = false;
                    }
                    tail_blocks[set] = blocks;
                    killed[set] = after;
                }
                post = self.data(&sets, &tail_blocks);
                if settled {
                    break;
                }
            }
        }
        if self.placement == Placement::Embedded {
            let lost_now = lost(&post);
            let mut verdict = Verdict::Completes;
            for files in self.data_files(&post) {
                verdict = combine(verdict, tail_verdict(&post, &files, &lost_now, self.block));
            }
            return (post, None, Some(verdict));
        }
        // Sidecars, one per archive set, each authored over its own volumes.
        let data = post.clone();
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
            let mut sidecar = if upper == 0 {
                Verdict::Completes
            } else if upper <= least {
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
            if self.placement == Placement::Both {
                let tail = tail_verdict(&post, files, &lost_now, self.block);
                sidecar = match (sidecar, tail) {
                    (Verdict::Completes, _) | (_, Verdict::Completes) => Verdict::Completes,
                    (Verdict::Fails, Verdict::Fails) => Verdict::Fails,
                    _ => Verdict::Either,
                };
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

    fn data(self, sets: &[ArchiveSet], tail_blocks: &[usize]) -> Post {
        let mut files = Vec::new();
        let mut expected = Vec::new();
        for (index, set) in sets.iter().enumerate() {
            for (name, bytes) in self.volumes(set, tail_blocks[index]) {
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

// The container bytes of a set with a tail, as one span across its volumes,
// and the posted offset of each file in that span.
fn container_span(post: &Post, files: &[usize]) -> (usize, Vec<usize>) {
    let mut offsets = Vec::new();
    let mut at = 0;
    for &file in files {
        offsets.push(at);
        at += post.files[file].bytes.len();
    }
    let whole: Vec<u8> = files
        .iter()
        .flat_map(|&file| post.files[file].bytes.clone())
        .collect();
    let tail = recovery_packets(&whole)
        .first()
        .map_or(whole.len(), |first| first.start);
    (tail, offsets)
}

// Container blocks a tail may and must mend: the wrong ranges that fall
// before the tail, in the container's own block grid.
fn tail_needed(
    post: &Post,
    files: &[usize],
    lost: &BTreeMap<usize, BTreeSet<u32>>,
    block: usize,
) -> (usize, usize) {
    let none = BTreeSet::new();
    let (tail, offsets) = container_span(post, files);
    let mut may_all = Vec::new();
    let mut must_all = Vec::new();
    for (at, &file) in files.iter().enumerate() {
        let (may, must) = post.files[file].wrong_ranges(lost.get(&file).unwrap_or(&none));
        let shift = |range: Range<usize>| {
            (range.start + offsets[at]).min(tail)..(range.end + offsets[at]).min(tail)
        };
        may_all.extend(may.into_iter().map(shift));
        must_all.extend(must.into_iter().map(shift));
    }
    (
        blocks_touched(&may_all, block),
        blocks_touched(&must_all, block),
    )
}

// Recovery packets of a tail that survive what the post loses.
fn tail_surviving(
    post: &Post,
    files: &[usize],
    lost: &BTreeMap<usize, BTreeSet<u32>>,
) -> (usize, usize) {
    let none = BTreeSet::new();
    let (_, offsets) = container_span(post, files);
    let whole: Vec<u8> = files
        .iter()
        .flat_map(|&file| post.files[file].bytes.clone())
        .collect();
    let packets = recovery_packets(&whole);
    let mut may_all = Vec::new();
    let mut must_all = Vec::new();
    for (at, &file) in files.iter().enumerate() {
        let (may, must) = post.files[file].wrong_ranges(lost.get(&file).unwrap_or(&none));
        may_all.extend(
            may.into_iter()
                .map(|range| range.start + offsets[at]..range.end + offsets[at]),
        );
        must_all.extend(
            must.into_iter()
                .map(|range| range.start + offsets[at]..range.end + offsets[at]),
        );
    }
    let hit = |ranges: &[Range<usize>], packet: &Range<usize>| {
        ranges
            .iter()
            .any(|range| range.start < packet.end && packet.start < range.end)
    };
    (
        packets
            .iter()
            .filter(|packet| !hit(&may_all, packet))
            .count(),
        packets
            .iter()
            .filter(|packet| !hit(&must_all, packet))
            .count(),
    )
}

// What a 7z with a tail must end in under the loss: the tail mends the
// container when enough of it survives; a lost start header leaves no map,
// so the set demotes and the conventional path repairs from the tail, which
// is still a completion.
fn tail_verdict(
    post: &Post,
    files: &[usize],
    lost: &BTreeMap<usize, BTreeSet<u32>>,
    block: usize,
) -> Verdict {
    let (upper, lower) = tail_needed(post, files, lost, block);
    if upper == 0 {
        return Verdict::Completes;
    }
    let (least, most) = tail_surviving(post, files, lost);
    if upper <= least {
        Verdict::Completes
    } else if lower > most {
        Verdict::Fails
    } else {
        Verdict::Either
    }
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

    fn defect(self, profile: ExtractionProfile) -> Option<Defect> {
        open_defect(self, profile)
    }

    fn par2(self) -> bool {
        false
    }
}

// The defects each cell and profile is held open for.
fn open_defect(cell: Par3Cell, profile: ExtractionProfile) -> Option<Defect> {
    let _ = profile;
    (cell.placement == Placement::Embedded)
        .then_some(Defect::Diverges(EMBEDDED_PAR3_SPLIT_NOT_REPAIRED))
}

// A split 7z whose only recovery is the PAR3 embedded in its tail is demoted
// for volume size when an article is lost, and the conventional path then
// fails "no PAR2 metadata is available for repair" without reading the tail.
// The product repairs lost articles from an embedded tail only in a
// single-volume archive. Not PAR2, so not release-blocking.
const EMBEDDED_PAR3_SPLIT_NOT_REPAIRED: &str =
    "embedded PAR3 in a split 7z is never used to repair a lost article; the set demotes and fails";

macro_rules! par3_smokes {
    ($($name:ident $placement:ident $code:ident $margin:ident $block:literal $entries:ident $sets:ident $index:ident $container:ident;)+) => {
        mod par3_breadth_smoke {
            use super::*;
            $(
                #[tokio::test]
                async fn $name() {
                    smoke(Par3Cell {
                        placement: Placement::$placement,
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
    sidecar_cauchy_rar5 Sidecar Cauchy With 1024 One One Present Rar5;
    sidecar_fft_sevenz_two_entries Sidecar Fft Exact 512 Two One Present SevenZip;
    sidecar_two_sets_index_absent Sidecar Cauchy With 4096 One Two Absent Rar5;
    sidecar_index_damaged_short Sidecar Fft OneShort 1024 One One Damaged SevenZip;
    embedded_cauchy Embedded Cauchy With 1024 One One Present SevenZip;
    embedded_fft_two_sets Embedded Fft Exact 512 Two Two Present SevenZip;
    both_cauchy_index_damaged Both Cauchy With 4096 One One Damaged SevenZip;
    both_fft_two_entries Both Fft OneShort 1024 Two One Present SevenZip;
}

mod par3_breadth_loss_smoke {
    use super::*;

    #[tokio::test]
    async fn sidecar_fft_rar5() {
        loss_smoke(Par3Cell {
            placement: Placement::Sidecar,
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

    #[tokio::test]
    async fn embedded_cauchy_sevenz() {
        loss_smoke(Par3Cell {
            placement: Placement::Embedded,
            code: Code::Cauchy,
            margin: Margin::With,
            block: 1024,
            entries: Entries::One,
            sets: Sets::One,
            index: Index::Present,
            container: Container::SevenZip,
        })
        .await;
    }
}

// The campaign: 500 shards of about a thousand cases.
mod combined_par3_breadth {
    use super::*;

    tier2_shards!(family, 500; h0 0 h1 1 h2 2 h3 3 h4 4);
}
