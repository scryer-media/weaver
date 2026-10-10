//! The damage axis: articles that arrive wrong rather than not at all.
//!
//! A cell is a damage kind, where it lands, how many articles carry it, the
//! recovery posted beside the set, and the container. The fixtures are the
//! field's shape at one thousandth of its size: 768-byte articles over
//! 1,048-byte PAR2 slices (768,000 over 1 MiB), so an article straddles a
//! slice boundary, and four volumes of 32 articles, so the first 16 KiB of a
//! volume is a run of articles of its own.
use super::fixtures::{Container, payload};
use super::post::{DAMAGES, Damage, Post, Posted, Role, Wire};
use super::recovery::{self, Code, Geometry, Margin, Par2Volumes};
use super::*;

/// Decoded bytes per article.
pub(in super::super) const ARTICLE: usize = 768;
/// PAR2 slice: 1 MiB at a thousandth, rounded to a multiple of four.
const SLICE: usize = 1048;
/// PAR3 block.
const BLOCK: usize = 1024;
const VOLUMES: usize = 4;
const ARTICLES_PER_VOLUME: usize = 32;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum Location {
    /// The first 16 KiB of volume 0, where the archive's headers live.
    FirstVolumeHead,
    /// The first 16 KiB of a middle volume.
    MiddleVolumeHead,
    /// The middle of a volume's data.
    MidVolume,
    /// Articles that straddle a recovery-slice boundary.
    SliceStraddle,
    /// The tail of the last volume, where the end header lives.
    LastVolumeTail,
    /// The recovery index.
    RecoveryIndex,
    /// A recovery volume.
    RecoveryVolume,
}

const LOCATIONS: [Location; 7] = [
    Location::FirstVolumeHead,
    Location::MiddleVolumeHead,
    Location::MidVolume,
    Location::SliceStraddle,
    Location::LastVolumeTail,
    Location::RecoveryIndex,
    Location::RecoveryVolume,
];

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(in super::super) enum Pattern {
    /// Two articles in one volume.
    Two,
    /// Twelve articles, four in each of three volumes.
    TwelveAcrossThree,
    /// A hundred contiguous articles.
    Hundred,
    /// Every article of one volume but its first and last.
    MostOfOne,
    /// A whole file absent, and one article damaged elsewhere.
    MemberAbsent,
}

pub(in super::super) const PATTERNS: [Pattern; 5] = [
    Pattern::Two,
    Pattern::TwelveAcrossThree,
    Pattern::Hundred,
    Pattern::MostOfOne,
    Pattern::MemberAbsent,
];

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum Recovery {
    Par2(Margin),
    /// PAR3 with margin, its code alternating between cells.
    Par3,
    None,
}

const RECOVERIES: [Recovery; 5] = [
    Recovery::Par2(Margin::With),
    Recovery::Par2(Margin::Exact),
    Recovery::Par2(Margin::OneShort),
    Recovery::Par3,
    Recovery::None,
];

const CONTAINERS: [Container; 4] = [
    Container::Rar5,
    Container::Rar5Encrypted,
    Container::Rar4,
    Container::SevenZip,
];

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct DamageCell {
    pub kind: Damage,
    pub location: Location,
    pub pattern: Pattern,
    pub recovery: Recovery,
    pub container: Container,
}

/// Whether a single job can present the cell: damage to a recovery file
/// needs a recovery set to be posted.
fn possible(cell: DamageCell) -> bool {
    !(cell.recovery == Recovery::None
        && matches!(cell.location, Location::RecoveryIndex | Location::RecoveryVolume))
}

pub(super) fn cells() -> Vec<DamageCell> {
    let mut cells = Vec::new();
    for kind in DAMAGES {
        for location in LOCATIONS {
            for pattern in PATTERNS {
                for recovery in RECOVERIES {
                    for container in CONTAINERS {
                        let cell = DamageCell {
                            kind,
                            location,
                            pattern,
                            recovery,
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
    cells
}

/// Schedules each (cell, profile) unit samples from the matrix's smoke
/// schedules.
const PER_UNIT: usize = 283;

/// 15,840 possible cells under three profiles, 283 schedules each.
pub(super) const TOTAL: usize = 4_482_720;

pub(super) fn family() -> Family<DamageCell> {
    let cells = cells();
    assert_eq!(cells.len(), 5_280);
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

/// The articles a pattern names, starting in `files` (the files of one
/// role, in posted order) at `first` and article `start`, or counted back
/// from the end of the last one.
pub(in super::super) fn place(
    post: &Post,
    files: &[usize],
    first: usize,
    start: u32,
    pattern: Pattern,
    backward: bool,
) -> Vec<(usize, u32)> {
    let articles = |file: usize| post.files[file].articles();
    let sequence: Vec<(usize, u32)> = {
        let mut sequence: Vec<(usize, u32)> = files[first..]
            .iter()
            .chain(&files[..first])
            .flat_map(|&file| (0..articles(file)).map(move |article| (file, article)))
            .collect();
        if backward {
            sequence.reverse();
        }
        sequence
    };
    let target = if backward { *files.last().unwrap() } else { files[first] };
    let at = |file: usize| -> u32 {
        if backward {
            0
        } else {
            start.min(articles(file).saturating_sub(1))
        }
    };
    match pattern {
        Pattern::Two => sequence
            .iter()
            .copied()
            .filter(|&(file, article)| file == target && (backward || article >= at(file)))
            .take(2)
            .collect(),
        Pattern::TwelveAcrossThree => {
            let mut chosen = Vec::new();
            let order: Vec<usize> = if backward {
                files.iter().rev().copied().collect()
            } else {
                files[first..].iter().chain(&files[..first]).copied().collect()
            };
            for &file in order.iter().take(3) {
                let count = articles(file);
                let begin = if backward {
                    count.saturating_sub(4)
                } else {
                    at(file).min(count.saturating_sub(4))
                };
                chosen.extend((begin..(begin + 4).min(count)).map(|article| (file, article)));
            }
            chosen
        }
        Pattern::Hundred => sequence
            .iter()
            .copied()
            .skip_while(|&(file, article)| !backward && file == target && article < at(file))
            .take(100)
            .collect(),
        Pattern::MostOfOne => {
            let count = articles(target);
            (1..count.saturating_sub(1)).map(|article| (target, article)).collect()
        }
        Pattern::MemberAbsent => (0..articles(target)).map(|article| (target, article)).collect(),
    }
}

/// The first article of `file` at or after its middle third whose bytes
/// cross a `slice` boundary.
fn straddling(posted: &Posted, slice: usize) -> u32 {
    (posted.articles() / 3..posted.articles())
        .find(|&article| {
            let range = posted.extent(article);
            !range.is_empty() && range.start / slice != (range.end - 1) / slice
        })
        .unwrap_or(0)
}

/// Marks the cell's damage on the files its location names.
fn damage(post: &mut Post, cell: DamageCell, recovery_files: bool) {
    let data = post.index_of(Role::Data);
    let (files, first, start, backward) = match cell.location {
        Location::FirstVolumeHead => (data.clone(), 0, 0, false),
        Location::MiddleVolumeHead => (data.clone(), 1, 0, false),
        Location::MidVolume => {
            let start = post.files[data[2]].articles() / 2;
            (data.clone(), 2, start, false)
        }
        Location::SliceStraddle => {
            let start = straddling(&post.files[data[1]], SLICE);
            (data.clone(), 1, start, false)
        }
        Location::LastVolumeTail => (data.clone(), data.len() - 1, 0, true),
        Location::RecoveryIndex => (post.index_of(Role::Index), 0, 0, false),
        Location::RecoveryVolume => (post.index_of(Role::Recovery), 0, 0, false),
    };
    let on_recovery = matches!(cell.location, Location::RecoveryIndex | Location::RecoveryVolume);
    if on_recovery != recovery_files || files.is_empty() {
        // A pattern's second half lands on data even when its first lands on
        // a recovery file.
        if cell.pattern == Pattern::MemberAbsent && on_recovery && !recovery_files {
            let middle = &mut post.files[data[1]];
            let article = middle.articles() / 2;
            middle.wire.insert(article, Wire::Damaged(cell.kind));
        }
        return;
    }
    let wire = if cell.pattern == Pattern::MemberAbsent {
        Wire::Absent
    } else {
        Wire::Damaged(cell.kind)
    };
    for (file, article) in place(post, &files, first, start, cell.pattern, backward) {
        post.files[file].wire.insert(article, wire);
    }
    if cell.pattern == Pattern::MemberAbsent && !on_recovery {
        let target = if backward { *files.last().unwrap() } else { files[first] };
        let elsewhere = data[(data.iter().position(|&file| file == target).unwrap() + 2) % data.len()];
        let other = &mut post.files[elsewhere];
        let article = other.articles() / 2;
        other.wire.insert(article, Wire::Damaged(cell.kind));
    }
}

impl DamageCell {
    fn code(self) -> Code {
        let seed = self.kind as usize + self.location as usize + self.pattern as usize + self.container as usize;
        if seed.is_multiple_of(2) { Code::Cauchy } else { Code::Fft }
    }

    fn data(self) -> Post {
        let payload_len = VOLUMES * ARTICLES_PER_VOLUME * ARTICLE - 512;
        let payload = payload(29, payload_len);
        let volumes = self.container.volumes(&payload, VOLUMES);
        Post {
            files: volumes
                .into_iter()
                .map(|(name, bytes)| Posted::new(name, bytes, ARTICLE, Role::Data))
                .collect(),
            password: self.container.password(),
            expected: vec![(self.container.member().to_string(), payload)],
            allowed: Vec::new(),
        }
    }
}

/// Authors the recovery set for `post` so the blocks that survive its own
/// damage meet `margin`, rebuilding until the set's layout settles.
pub(in super::super) fn with_recovery(
    mut post: Post,
    lost: impl Fn(&Post) -> BTreeMap<usize, BTreeSet<u32>>,
    margin: Margin,
    floor: usize,
    geometry: Geometry,
    author: impl Fn(&[(String, Vec<u8>)], usize) -> Vec<(String, Vec<u8>)>,
    mark: impl Fn(&mut Post),
) -> Post {
    let data = post.clone();
    let sources: Vec<(String, Vec<u8>)> = data
        .index_of(Role::Data)
        .into_iter()
        .map(|file| (data.files[file].name.clone(), data.files[file].bytes.clone()))
        .collect();
    let lost_now = lost(&data);
    let needed = recovery::needed(&data, geometry, &lost_now);
    let mut killed = 0;
    for _ in 0..8 {
        let blocks = recovery::blocks_for(margin, needed, killed, floor);
        post = data.clone();
        recovery::post_set(&mut post, author(&sources, blocks), ARTICLE);
        mark(&mut post);
        let lost_now = lost(&post);
        let (least, most) = recovery::surviving(&post, &lost_now, false);
        let after = if margin == Margin::OneShort {
            blocks - most
        } else {
            blocks - least
        };
        if after == killed {
            break;
        }
        killed = after;
    }
    post
}

impl Cell for DamageCell {
    fn post(self, interruption: Interruption) -> Built {
        let mut post = self.data();
        damage(&mut post, self, false);
        let lost = move |post: &Post| run::lost_articles(post, interruption);
        let source_blocks: usize = post
            .index_of(Role::Data)
            .into_iter()
            .map(|file| post.files[file].bytes.len().div_ceil(SLICE))
            .sum();
        let mark = |post: &mut Post| damage(post, self, true);
        let (post, geometry) = match self.recovery {
            Recovery::None => (post, None),
            Recovery::Par2(margin) => {
                let geometry = Geometry {
                    block: SLICE,
                    packed: false,
                };
                let post = with_recovery(
                    post,
                    lost,
                    margin,
                    source_blocks / 10,
                    geometry,
                    |sources, blocks| recovery::par2_set(sources, SLICE, blocks, Par2Volumes::Uniform),
                    mark,
                );
                (post, Some(geometry))
            }
            Recovery::Par3 => {
                let geometry = Geometry {
                    block: BLOCK,
                    packed: false,
                };
                let code = self.code();
                let post = with_recovery(
                    post,
                    lost,
                    Margin::With,
                    source_blocks / 10,
                    geometry,
                    |sources, blocks| {
                        recovery::par3_set(sources, BLOCK, blocks, code, blocks.div_ceil(4) as u64)
                    },
                    mark,
                );
                (post, Some(geometry))
            }
        };
        Built {
            post,
            geometry,
            ruling: None,
        }
    }

    fn defect(self, _profile: ExtractionProfile) -> Option<Defect> {
        None
    }

    fn par2(self) -> bool {
        matches!(self.recovery, Recovery::Par2(_))
    }
}

macro_rules! damage_smokes {
    ($($name:ident $kind:ident $location:ident $pattern:ident $recovery:expr, $container:ident;)+) => {
        mod damage_smoke {
            use super::*;
            $(
                #[tokio::test]
                async fn $name() {
                    smoke(DamageCell {
                        kind: Damage::$kind,
                        location: Location::$location,
                        pattern: Pattern::$pattern,
                        recovery: $recovery,
                        container: Container::$container,
                    })
                    .await;
                }
            )+
        }
    };
}

damage_smokes! {
    crc_wrong_mid_volume_par3 CrcWrong MidVolume MostOfOne Recovery::Par3, Rar5;
    no_checksum_middle_head_par2 NoChecksum MiddleVolumeHead Hundred Recovery::Par2(Margin::With), SevenZip;
    truncated_index_exact Truncated RecoveryIndex MostOfOne Recovery::Par2(Margin::Exact), Rar5;
    recomputed_mid_volume_par2 Recomputed MidVolume Two Recovery::Par2(Margin::With), Rar5;
    truncated_first_head_exact Truncated FirstVolumeHead TwelveAcrossThree Recovery::Par2(Margin::Exact), Rar5Encrypted;
    crc_wrong_straddle_unprotected CrcWrong SliceStraddle Two Recovery::None, Rar4;
    swapped_last_tail_par3 Swapped LastVolumeTail Two Recovery::Par3, SevenZip;
    duplicate_recovery_volume DifferingDuplicate RecoveryVolume Two Recovery::Par2(Margin::With), Rar5;
    no_checksum_index_absent NoChecksum RecoveryIndex MemberAbsent Recovery::Par2(Margin::With), SevenZip;
    overlong_mid_volume_short Overlong MidVolume Two Recovery::Par2(Margin::OneShort), Rar5;
    trailing_junk_middle_hundred TrailingJunk MiddleVolumeHead Hundred Recovery::Par2(Margin::With), Rar4;
}

mod damage_loss_smoke {
    use super::*;

    #[tokio::test]
    async fn crc_wrong_mid_volume_par2() {
        loss_smoke(DamageCell {
            kind: Damage::CrcWrong,
            location: Location::MidVolume,
            pattern: Pattern::TwelveAcrossThree,
            recovery: Recovery::Par2(Margin::With),
            container: Container::Rar5,
        })
        .await;
    }

    #[tokio::test]
    async fn truncated_straddle_par3() {
        loss_smoke(DamageCell {
            kind: Damage::Truncated,
            location: Location::SliceStraddle,
            pattern: Pattern::Two,
            recovery: Recovery::Par3,
            container: Container::SevenZip,
        })
        .await;
    }
}

/// The campaign: 4,500 shards of about a thousand cases.
mod combined_damage {
    use super::*;

    tier2_shards!(family, 4500;
        h00 0 h01 1 h02 2 h03 3 h04 4 h05 5 h06 6 h07 7 h08 8 h09 9 h10 10 h11 11 h12 12 h13 13 h14 14
        h15 15 h16 16 h17 17 h18 18 h19 19 h20 20 h21 21 h22 22 h23 23 h24 24 h25 25 h26 26 h27 27 h28 28
        h29 29 h30 30 h31 31 h32 32 h33 33 h34 34 h35 35 h36 36 h37 37 h38 38 h39 39 h40 40 h41 41 h42 42
        h43 43 h44 44);
}
