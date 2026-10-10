//! PAR2 as the field posts it: the slice sizes posters choose, their
//! redundancy, recovery that is ample, exact or one block short, block counts
//! in the hundreds and the thousands, articles that line up with slices and
//! articles that straddle them, and the packet layouts the common tools write.
//!
//! Sizes are the field's at a thousandth: a 768,000-byte slice is 768 bytes.
use super::damage::{PATTERNS, Pattern, place, with_recovery};
use super::fixtures::{Container, payload};
use super::post::{Damage, Post, Posted, Role, Wire};
use super::recovery::{
    self, Geometry, Margin, PAR2_MARGINS, Par2Volumes, par2_packet, par2_packets, par2_set_id,
    par2_type,
};
use super::*;

/// 716,800; 768,000; 1 MiB; 1,536,000; 5 MiB, at a thousandth and rounded
/// to the four-byte multiple PAR2 requires.
const SLICES: [usize; 5] = [716, 768, 1048, 1536, 5240];
const VOLUMES: usize = 4;
const ARTICLES_PER_VOLUME: usize = 32;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum Redundancy {
    Eight,
    Ten,
    Twenty,
    /// A hundred percent, and none of the data arrives: the job is rebuilt
    /// from the recovery volumes alone.
    ParsOnly,
}

const REDUNDANCIES: [Redundancy; 4] = [
    Redundancy::Eight,
    Redundancy::Ten,
    Redundancy::Twenty,
    Redundancy::ParsOnly,
];

impl Redundancy {
    fn percent(self) -> usize {
        match self {
            Self::Eight => 8,
            Self::Ten => 10,
            Self::Twenty => 20,
            Self::ParsOnly => 100,
        }
    }
}

/// How many source blocks the set describes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum Band {
    Hundreds,
    Thousands,
}

const BANDS: [Band; 2] = [Band::Hundreds, Band::Thousands];

impl Band {
    fn blocks(self) -> usize {
        match self {
            Self::Hundreds => 300,
            Self::Thousands => 2048,
        }
    }
}

/// Whether article boundaries fall on slice boundaries.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum Alignment {
    Aligned,
    Straddling,
}

const ALIGNMENTS: [Alignment; 2] = [Alignment::Aligned, Alignment::Straddling];

/// How the set's packets are laid out, sampled across cells.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum Structure {
    /// As the writer lays it out.
    Plain,
    /// Every article of the index damaged; the recovery volumes are intact,
    /// so the file list must come from their packets.
    IndexDamaged,
    /// No index at all: the recovery volumes alone.
    IndexAbsent,
    /// Volumes that double, named `vol00+01`, `vol01+02`, `vol03+04`.
    ExponentNaming,
    /// Every FileDesc packet posted twice in the index.
    DuplicateFileDesc,
    /// Each file's description and checksums in exactly one file of the set.
    SplitAcrossVolumes,
    /// A Unicode name packet beside every description.
    UnicodeNames,
    /// A creator packet as other common tools write one.
    Creator,
    /// A set of two files: the archive in two volumes.
    TwoFiles,
}

const STRUCTURES: [Structure; 9] = [
    Structure::Plain,
    Structure::IndexDamaged,
    Structure::IndexAbsent,
    Structure::ExponentNaming,
    Structure::DuplicateFileDesc,
    Structure::SplitAcrossVolumes,
    Structure::UnicodeNames,
    Structure::Creator,
    Structure::TwoFiles,
];

const CONTAINERS: [Container; 5] = [
    Container::Rar5,
    Container::Rar5Encrypted,
    Container::Rar5EncryptedHeaders,
    Container::Rar4,
    Container::SevenZip,
];

/// Neutral creator strings in the shapes common tools write.
const CREATORS: [&str; 3] = [
    "Parity Builder version 0.9.1",
    "Created by harbour-par v2.3.0 (lantern build)",
    "",
];

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct Par2Cell {
    pub slice: usize,
    pub redundancy: Redundancy,
    pub margin: Margin,
    pub band: Band,
    pub alignment: Alignment,
    pub pattern: Pattern,
    pub container: Container,
    /// Sampled across cells by a fixed stride, not crossed.
    pub structure: Structure,
    pub creator: usize,
}

pub(super) fn cells() -> Vec<Par2Cell> {
    let mut cells = Vec::new();
    for slice in SLICES {
        for redundancy in REDUNDANCIES {
            for margin in PAR2_MARGINS {
                for band in BANDS {
                    for alignment in ALIGNMENTS {
                        for pattern in PATTERNS {
                            for container in CONTAINERS {
                                let index = cells.len();
                                cells.push(Par2Cell {
                                    slice,
                                    redundancy,
                                    margin,
                                    band,
                                    alignment,
                                    pattern,
                                    container,
                                    structure: STRUCTURES[index % STRUCTURES.len()],
                                    creator: (index / STRUCTURES.len()) % CREATORS.len(),
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

/// Schedules each (cell, profile) unit samples from the archive matrix's
/// combined cases.
pub(super) const PER_UNIT: usize = 46;

/// 8,000 cells under three profiles, 46 schedules each.
pub(super) const TOTAL: usize = 1_104_000;

pub(super) fn family() -> Family<Par2Cell> {
    let cells = cells();
    assert_eq!(cells.len(), 8_000);
    Family {
        units: cells
            .into_iter()
            .enumerate()
            .flat_map(|(index, cell)| {
                PROFILES
                    .into_iter()
                    .map(move |profile| (cell, profile, Pool::Combined, PER_UNIT, index))
            })
            .collect(),
    }
}

impl Par2Cell {
    fn volumes(self) -> usize {
        if self.structure == Structure::TwoFiles {
            2
        } else {
            VOLUMES
        }
    }

    /// The article size: a whole number of slices, or that and half a slice.
    fn article(self) -> usize {
        let volume = self.band.blocks() * self.slice / self.volumes();
        let slices = (volume / ARTICLES_PER_VOLUME / self.slice).max(1);
        match self.alignment {
            Alignment::Aligned => slices * self.slice,
            Alignment::Straddling => slices * self.slice + self.slice / 2,
        }
    }

    fn data(self) -> Post {
        // The archive's own headers take a little of the last block.
        let payload = payload(31, self.band.blocks() * self.slice - 256);
        let article = self.article();
        let volumes = self.container.volumes(&payload, self.volumes());
        Post {
            files: volumes
                .into_iter()
                .map(|(name, bytes)| Posted::new(name, bytes, article, Role::Data))
                .collect(),
            password: self.container.password(),
            expected: vec![(self.container.member().to_string(), payload)],
            allowed: Vec::new(),
        }
    }

    /// Marks the cell's losses on the data, or on the recovery volumes when
    /// no data arrives at all.
    fn mark(self, post: &mut Post, recovery_files: bool) {
        let pars_only = self.redundancy == Redundancy::ParsOnly;
        if !recovery_files {
            let data = post.index_of(Role::Data);
            if pars_only {
                for file in data {
                    let articles = post.files[file].articles();
                    for article in 0..articles {
                        post.files[file].wire.insert(article, Wire::Absent);
                    }
                }
                return;
            }
            pattern(post, &data, self.pattern);
            return;
        }
        let index = post.index_of(Role::Index);
        match self.structure {
            Structure::IndexDamaged => {
                for file in &index {
                    for article in 0..post.files[*file].articles() {
                        post.files[*file]
                            .wire
                            .insert(article, Wire::Damaged(Damage::CrcWrong));
                    }
                }
            }
            Structure::IndexAbsent => {
                for file in &index {
                    for article in 0..post.files[*file].articles() {
                        post.files[*file].wire.insert(article, Wire::Absent);
                    }
                }
            }
            _ => {}
        }
        if pars_only {
            let volumes = post.index_of(Role::Recovery);
            if volumes.len() > 1 {
                // The data is gone; the pattern lands on what recovery is
                // left, never on all of it.
                pattern(post, &volumes[1..], self.pattern);
            }
        }
    }

    /// Rewrites the writer's set into the cell's packet layout.
    fn restructure(self, mut set: Vec<(String, Vec<u8>)>) -> Vec<(String, Vec<u8>)> {
        let id = par2_set_id(&set[0].1);
        match self.structure {
            Structure::DuplicateFileDesc => {
                let index = &mut set[0].1;
                let copies: Vec<u8> = par2_packets(index)
                    .into_iter()
                    .filter(|packet| &packet.kind == par2_type::FILE_DESC)
                    .flat_map(|packet| index[packet.range].to_vec())
                    .collect();
                index.extend(copies);
            }
            Structure::SplitAcrossVolumes => {
                let mut moved = Vec::new();
                for (_, bytes) in &mut set {
                    let mut kept = Vec::new();
                    for packet in par2_packets(bytes) {
                        let body = bytes[packet.range.clone()].to_vec();
                        if &packet.kind == par2_type::FILE_DESC || &packet.kind == par2_type::IFSC {
                            if !moved.contains(&body) {
                                moved.push(body);
                            }
                        } else {
                            kept.extend(body);
                        }
                    }
                    *bytes = kept;
                }
                // Each description and its checksums travel together, to
                // one file of the set in turn.
                let mut pairs: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
                for packet in moved {
                    pairs
                        .entry(packet[64..80].to_vec())
                        .or_default()
                        .extend(packet);
                }
                let files = set.len();
                for (at, (_, packets)) in pairs.into_iter().enumerate() {
                    set[at % files].1.extend(packets);
                }
            }
            Structure::UnicodeNames => {
                for (_, bytes) in &mut set {
                    let names: Vec<u8> = par2_packets(bytes)
                        .into_iter()
                        .filter(|packet| &packet.kind == par2_type::FILE_DESC)
                        .flat_map(|packet| {
                            let body = &bytes[packet.range.start + 64..packet.range.end];
                            let name: Vec<u8> = body[56..]
                                .iter()
                                .copied()
                                .take_while(|&byte| byte != 0)
                                .collect();
                            let mut unicode = body[..16].to_vec();
                            for unit in String::from_utf8_lossy(&name).encode_utf16() {
                                unicode.extend_from_slice(&unit.to_le_bytes());
                            }
                            par2_packet(&id, par2_type::UNICODE_NAME, &unicode)
                        })
                        .collect();
                    bytes.extend(names);
                }
            }
            Structure::Creator => {
                let creator =
                    par2_packet(&id, par2_type::CREATOR, CREATORS[self.creator].as_bytes());
                for (_, bytes) in &mut set {
                    let mut rebuilt = Vec::new();
                    let mut replaced = false;
                    for packet in par2_packets(bytes) {
                        if &packet.kind == par2_type::CREATOR {
                            rebuilt.extend_from_slice(&creator);
                            replaced = true;
                        } else {
                            rebuilt.extend_from_slice(&bytes[packet.range]);
                        }
                    }
                    if !replaced {
                        rebuilt.extend_from_slice(&creator);
                    }
                    *bytes = rebuilt;
                }
            }
            Structure::Plain
            | Structure::TwoFiles
            | Structure::IndexDamaged
            | Structure::IndexAbsent
            | Structure::ExponentNaming => {}
        }
        set
    }
}

/// Loses a pattern's articles across `files`, starting a few articles into
/// the second of them so a pattern crosses slice boundaries.
fn pattern(post: &mut Post, files: &[usize], pattern: Pattern) {
    let first = usize::from(files.len() > 1);
    let start = 3.min(post.files[files[first]].articles().saturating_sub(1));
    let wire = Wire::Absent;
    for (file, article) in place(post, files, first, start, pattern, false) {
        post.files[file].wire.insert(article, wire);
    }
    if pattern == Pattern::MemberAbsent && files.len() > 2 {
        let other = files[(first + 2) % files.len()];
        let article = post.files[other].articles() / 2;
        post.files[other]
            .wire
            .insert(article, Wire::Damaged(Damage::CrcWrong));
    }
}

impl Cell for Par2Cell {
    fn post(self, interruption: Interruption) -> Built {
        let mut post = self.data();
        self.mark(&mut post, false);
        let lost = move |post: &Post| run::lost_articles(post, interruption);
        let source_blocks: usize = post
            .index_of(Role::Data)
            .into_iter()
            .map(|file| post.files[file].bytes.len().div_ceil(self.slice))
            .sum();
        let geometry = Geometry {
            block: self.slice,
            packed: false,
        };
        let volumes = if self.structure == Structure::ExponentNaming {
            Par2Volumes::Exponent
        } else {
            Par2Volumes::Uniform
        };
        let post = with_recovery(
            post,
            lost,
            self.margin,
            (source_blocks * self.redundancy.percent()).div_ceil(100),
            geometry,
            |sources, blocks| {
                self.restructure(recovery::par2_set(sources, self.slice, blocks, volumes))
            },
            |post| self.mark(post, true),
        );
        let lost = run::lost_articles(&post, interruption);
        // Without every file's description the set cannot say what to
        // rebuild: a job that needed it may still end either way, named.
        let ruling = (!recovery::descriptions_survive(&post, &lost, interruption.fails())
            && recovery::needed(&post, geometry, &lost).0 > 0)
            .then_some(Verdict::Either);
        Built {
            post,
            geometry: Some(geometry),
            ruling,
        }
    }

    fn defect(self, _profile: ExtractionProfile) -> Option<Defect> {
        None
    }

    fn par2(self) -> bool {
        true
    }
}

macro_rules! par2_smokes {
    ($($name:ident $slice:literal $redundancy:ident $margin:ident $band:ident $alignment:ident $pattern:ident $structure:ident $container:ident;)+) => {
        mod par2_realism_smoke {
            use super::*;
            #[tokio::test]
            async fn repaired_encrypted_restart_finishes_member_edges() {
                run_cell(
                    Par2Cell {
                        slice: 5240,
                        redundancy: Redundancy::Ten,
                        margin: Margin::With,
                        band: Band::Hundreds,
                        alignment: Alignment::Aligned,
                        pattern: Pattern::MemberAbsent,
                        structure: Structure::Plain,
                        container: Container::Rar5EncryptedHeaders,
                        creator: 2,
                    },
                    ExtractionProfile::DirectStore,
                    vec![(941437, (vec![(0, 0), (0, 1), (0, 1)], Interruption::Combined {
                        mask: 12,
                        index_first: false,
                        action: BoundaryAction::Restart,
                        at: 1,
                    }))],
                ).await;
            }
            #[tokio::test]
            async fn index_absent_restart_fetches_remaining_recovery() {
                run_cell(
                    Par2Cell {
                        slice: 5240,
                        redundancy: Redundancy::Ten,
                        margin: Margin::With,
                        band: Band::Hundreds,
                        alignment: Alignment::Aligned,
                        pattern: Pattern::MemberAbsent,
                        structure: Structure::IndexAbsent,
                        container: Container::SevenZip,
                        creator: 2,
                    },
                    ExtractionProfile::DirectStore,
                    vec![(941713, (vec![(0, 0), (0, 1), (0, 1)], Interruption::Combined {
                        mask: 12,
                        index_first: false,
                        action: BoundaryAction::Restart,
                        at: 3,
                    }))],
                ).await;
            }
            $(
                #[tokio::test]
                async fn $name() {
                    smoke(Par2Cell {
                        slice: $slice,
                        redundancy: Redundancy::$redundancy,
                        margin: Margin::$margin,
                        band: Band::$band,
                        alignment: Alignment::$alignment,
                        pattern: Pattern::$pattern,
                        structure: Structure::$structure,
                        container: Container::$container,
                        creator: 1,
                    })
                    .await;
                }
            )+
        }
    };
}

par2_smokes! {
    plain_two_with 768 Ten With Hundreds Straddling Two Plain Rar5;
    index_damaged_exact 1048 Eight Exact Hundreds Aligned TwelveAcrossThree IndexDamaged Rar5;
    index_absent_most 716 Twenty With Hundreds Straddling MostOfOne IndexAbsent Rar4;
    exponent_short 1536 Ten OneShort Hundreds Aligned Two ExponentNaming Rar5;
    duplicate_desc_member 5240 Twenty With Hundreds Straddling MemberAbsent DuplicateFileDesc Rar5;
    split_packets_two 768 Ten With Hundreds Aligned Two SplitAcrossVolumes Rar4;
    unicode_names_hundred 1048 Twenty With Hundreds Straddling Hundred UnicodeNames Rar5;
    creator_pars_only 716 ParsOnly With Hundreds Aligned Two Creator Rar5;
    thousands_band_two 1048 Ten Exact Thousands Straddling Two Plain Rar5;
    two_files_one_over 768 Ten OneOver Hundreds Straddling Two TwoFiles SevenZip;
    headers_encrypted_with 1048 Twenty With Hundreds Aligned TwelveAcrossThree Plain Rar5EncryptedHeaders;
}

/// The campaign: 2,200 shards of about 500 cases; half the cells carry a
/// set of thousands of blocks, whose cases run several times longer.
mod combined_par2_realism {
    use super::*;

    tier2_shards!(family, 2200; h00 0 h01 1 h02 2 h03 3 h04 4 h05 5 h06 6 h07 7 h08 8 h09 9 h10 10 h11 11 h12 12 h13 13 h14 14 h15 15 h16 16 h17 17 h18 18 h19 19 h20 20 h21 21);
}
