// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The grouping-information axis of the archive matrix.
//
// A cell is a container, the names its volumes are posted under, and what
// else the post carries that could group or order them. [`rule`] states, for
// every cell, whether a single job can present it at all.
//
// The ruling is an exhaustive match over the axes, so a value added to any
// axis does not compile until every cell it opens is ruled.
//
// The cells reuse the matrix's synthetic stored-volume builders and the 7z
// writer; nothing here is a captured post.
use super::super::sevenz_store::schedules::unrepeated_payload;
use super::super::sevenz_store::{Entry, build_7z_shaped, sevenz_job_spec, split_volumes};
use super::*;
use crate::pipeline::tests::single_member_rar4_old_numbering_set;

// What the volumes are.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::pipeline::tests) enum Container {
    Rar4,
    Rar5,
    // Encrypted data under plain headers: the headers still chain the set.
    Rar5Encrypted,
    // Encrypted headers: nothing in a volume is readable before the set is
    // known, so only a name or a timely index can group it.
    Rar5EncryptedHeaders,
    SevenZip,
    SevenZipSolid,
    // Solid compressed sets a real archiver wrote, with the data encrypted
    // under plain headers or not.
    Rar4Solid,
    Rar4SolidEncrypted,
    Rar5Solid,
    Rar5SolidEncrypted,
}

// The names the volumes are posted under.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::pipeline::tests) enum Naming {
    // `silver.horizon.partNN.rar`, `silver.horizon.7z.NNN`.
    Conventional,
    // [`Naming::Conventional`] over four volumes, so two are middle volumes.
    ConventionalFour,
    // One volume, `silver.horizon.rar` or `silver.horizon.7z`.
    Single,
    // The first volume's stem in a different case from the second's.
    MixedCase,
    // Old-style numbering, `silver.horizon.rar` then `silver.horizon.r00`:
    // the first two volumes of a set written that way.
    OldStyle,
    // 32-hex extensionless names with no shared stem.
    HexBare,
    // One volume under a bare 32-hex name.
    HexSingle,
    // A hex stem whose numeric suffixes misstate the order: `.002`, `.001`.
    HexMisnumbered,
    // A hex stem whose `.rar` and `.r00` are swapped.
    HexSwappedRar,
    // Four unrelated hex names that do not sort into volume order.
    HexScattered,
    // Parts one and three of a three-part set under a hex stem, `.001` and
    // `.003`: part two was never posted.
    HexNumberedGap,
    // The first volume under a bare hex name, the second conventionally.
    FirstVolumeHex,
    // Every volume posted twice under two names that number it alike.
    Reposted,
    // Every volume posted twice, each copy under a hex name of its own.
    HexReposted,
    // The first part as a whole `silver.horizon.7z`, the second as `.7z.002`.
    BareFirstPart,
}

// What, besides the volumes, could group or order them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::pipeline::tests) enum Binding {
    // A PAR2 index carrying the real names arrives before any volume.
    Par2RealFirst,
    // A PAR2 index carrying the real names arrives after every volume.
    Par2RealLast,
    // A PAR2 index describes the volumes under the names they are posted
    // under, arriving where the matrix's index does.
    Par2Posted,
    // An `.sfv` lists the real names; no PAR2 is posted.
    Sfv,
    // Nothing besides the volumes.
    Nothing,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::pipeline::tests) struct Cell {
    pub container: Container,
    pub naming: Naming,
    pub binding: Binding,
}

#[derive(Clone, Copy, Debug)]
pub(super) enum Ruling {
    // No single job can present this cell, for the stated reason.
    Impossible(&'static str),
    Possible,
}

macro_rules! any_naming {
    () => {
        Naming::Conventional
            | Naming::ConventionalFour
            | Naming::Single
            | Naming::MixedCase
            | Naming::OldStyle
            | Naming::HexBare
            | Naming::HexSingle
            | Naming::HexMisnumbered
            | Naming::HexSwappedRar
            | Naming::HexScattered
            | Naming::HexNumberedGap
            | Naming::FirstVolumeHex
            | Naming::Reposted
            | Naming::HexReposted
            | Naming::BareFirstPart
    };
}

macro_rules! any_binding {
    () => {
        Binding::Par2RealFirst
            | Binding::Par2RealLast
            | Binding::Par2Posted
            | Binding::Sfv
            | Binding::Nothing
    };
}

impl Container {
    pub(super) fn is_sevenz(self) -> bool {
        matches!(self, Self::SevenZip | Self::SevenZipSolid)
    }

    fn is_rar5(self) -> bool {
        matches!(
            self,
            Self::Rar5
                | Self::Rar5Encrypted
                | Self::Rar5EncryptedHeaders
                | Self::Rar5Solid
                | Self::Rar5SolidEncrypted
        )
    }

    fn is_solid_rar(self) -> bool {
        matches!(
            self,
            Self::Rar4Solid | Self::Rar4SolidEncrypted | Self::Rar5Solid | Self::Rar5SolidEncrypted
        )
    }

    // The checked-in archive written as `volumes` volumes, if there is one.
    fn solid_format(self, volumes: usize) -> Option<CompressedFormat> {
        use CompressedFormat as F;
        Some(match (self, volumes) {
            (Self::Rar4Solid, 1) => F::Rar4Solid,
            (Self::Rar4Solid, 2) => F::Rar4SolidTwoVolumes,
            (Self::Rar4Solid, 4) => F::Rar4SolidFourVolumes,
            (Self::Rar4SolidEncrypted, 1) => F::Rar4SolidEncrypted,
            (Self::Rar4SolidEncrypted, 2) => F::Rar4SolidEncryptedTwoVolumes,
            (Self::Rar4SolidEncrypted, 4) => F::Rar4SolidEncryptedFourVolumes,
            (Self::Rar5Solid, 1) => F::Rar5Solid,
            (Self::Rar5Solid, 2) => F::Rar5SolidTwoVolumes,
            (Self::Rar5Solid, 4) => F::Rar5SolidFourVolumes,
            (Self::Rar5SolidEncrypted, 1) => F::Rar5SolidEncrypted,
            (Self::Rar5SolidEncrypted, 2) => F::Rar5SolidEncryptedTwoVolumes,
            (Self::Rar5SolidEncrypted, 4) => F::Rar5SolidEncryptedFourVolumes,
            _ => return None,
        })
    }

    // Whether the volumes' own readable headers chain the set, so a set no
    // name or index places is still admitted by content.
    fn headers_chain(self) -> bool {
        matches!(
            self,
            Self::Rar5 | Self::Rar5Encrypted | Self::Rar5Solid | Self::Rar5SolidEncrypted
        )
    }
}

// Every cell's ruling. See the module documentation.
pub(super) fn rule(cell: Cell) -> Ruling {
    let Cell {
        container,
        naming,
        binding,
    } = cell;
    let sevenz = container.is_sevenz();
    match (naming, binding) {
        (Naming::OldStyle | Naming::HexSwappedRar, any_binding!()) if sevenz => {
            Ruling::Impossible("a RAR naming")
        }
        (Naming::OldStyle, any_binding!()) if container.is_rar5() => {
            Ruling::Impossible("rar writes old-style volume names only for RAR4")
        }
        (Naming::HexNumberedGap, any_binding!()) if container.is_solid_rar() => {
            Ruling::Impossible("no solid set was written as three volumes")
        }
        (Naming::BareFirstPart, any_binding!()) if !sevenz => Ruling::Impossible("a 7z naming"),
        (
            Naming::Conventional
            | Naming::ConventionalFour
            | Naming::Single
            | Naming::MixedCase
            | Naming::OldStyle,
            Binding::Par2Posted,
        ) => Ruling::Impossible(
            "the posted names are the real ones, so describing them is the Par2Real binding",
        ),
        (
            Naming::Reposted | Naming::HexReposted,
            Binding::Par2RealFirst | Binding::Par2RealLast | Binding::Sfv,
        ) => Ruling::Impossible("a real-name listing names each volume once, not each copy"),
        (any_naming!(), any_binding!()) => Ruling::Possible,
    }
}

impl Naming {
    // Volumes the archive is written as, and the indices of those posted,
    // in posted order.
    pub(super) fn layout(self) -> (usize, &'static [usize]) {
        match self {
            Self::Single | Self::HexSingle => (1, &[0]),
            Self::ConventionalFour | Self::HexScattered => (4, &[0, 1, 2, 3]),
            Self::HexNumberedGap => (3, &[0, 2]),
            Self::Reposted | Self::HexReposted => (2, &[0, 1, 0, 1]),
            Self::Conventional
            | Self::MixedCase
            | Self::OldStyle
            | Self::HexBare
            | Self::HexMisnumbered
            | Self::HexSwappedRar
            | Self::FirstVolumeHex
            | Self::BareFirstPart => (2, &[0, 1]),
        }
    }

    // Whether the posted names place the set, so a lost offset-zero article
    // leaves no volume without a set.
    pub(super) fn names_place(self) -> bool {
        matches!(
            self,
            Self::Conventional
                | Self::ConventionalFour
                | Self::Single
                | Self::MixedCase
                | Self::OldStyle
                | Self::HexSwappedRar
        )
    }

    // Whether leaving one posted volume out of the post says something the
    // naming does not already: a repost carries every volume twice, so one
    // copy left out withholds nothing, and a numbered gap is already a set
    // missing a part.
    fn withholds(self) -> bool {
        !matches!(
            self,
            Self::Reposted | Self::HexReposted | Self::HexNumberedGap
        )
    }

    // The names each posted volume goes under, given each one's real name.
    fn posted(self, real: &[String]) -> Vec<String> {
        const STEM: &str = "5f0c9e2ab1d74c6e8a3f1b0d9c2e7a41";
        let hex = |index: usize| format!("{:032x}", 0xd1c7_0000_u128 + index as u128);
        match self {
            Self::Conventional | Self::ConventionalFour | Self::Single | Self::OldStyle => {
                real.to_vec()
            }
            Self::MixedCase => {
                let mut names = real.to_vec();
                names[0] = names[0].replacen("silver.horizon", "Silver.Horizon", 1);
                names
            }
            Self::HexBare | Self::HexReposted => (0..real.len()).map(hex).collect(),
            Self::HexSingle => vec![STEM.to_string()],
            Self::HexMisnumbered => vec![format!("{STEM}.002"), format!("{STEM}.001")],
            Self::HexSwappedRar => vec![format!("{STEM}.r00"), format!("{STEM}.rar")],
            Self::HexScattered => [
                "e93a07c1d25b4f68a0c3e1f7b9d24c85",
                "1b7f3d9e05a24c6f8e1d0b3a7c9f2e64",
                "c40e8b2f6a1d4973b5e0f2c8d16a9b37",
                "7a2c5e9f0b3d4e81a6f9c2b07d4e1f58",
            ]
            .map(str::to_string)
            .to_vec(),
            Self::HexNumberedGap => vec![format!("{STEM}.001"), format!("{STEM}.003")],
            Self::FirstVolumeHex => vec![STEM.to_string(), real[1].clone()],
            // A repost numbers the copy alike but pads it differently.
            Self::Reposted => vec![
                real[0].clone(),
                real[1].clone(),
                real[2]
                    .replace(".part01.", ".part1.")
                    .replace(".7z.001", ".7z.0001"),
                real[3]
                    .replace(".part02.", ".part2.")
                    .replace(".7z.002", ".7z.0002"),
            ],
            Self::BareFirstPart => vec!["silver.horizon.7z".into(), real[1].clone()],
        }
    }
}

const PASSWORD: &str = "moonlit-harbour";

// One cell's posted job.
#[derive(Clone)]
pub(super) struct Fixture {
    pub volumes: Vec<(String, Vec<u8>)>,
    // The length of each volume the archive is written as.
    pub lengths: Vec<usize>,
    // What each posted volume is really called, in posted order.
    pub real: Vec<String>,
    pub spec: JobSpec,
    pub expected: Vec<(&'static str, Vec<u8>)>,
}

impl Fixture {
    pub(super) fn build(cell: Cell) -> Self {
        let (count, posted) = cell.naming.layout();
        let payload = unrepeated_payload(13, 70_001);
        let (mut written, expected) = if cell.container.is_sevenz() {
            let (archive, expected) = if cell.container == Container::SevenZipSolid {
                let archive = include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/tests/fixtures/extraction_profiles/sevenz_solid.7z"
                ))
                .to_vec();
                let expected = ["part0.bin", "part1.bin", "part2.bin"]
                    .into_iter()
                    .enumerate()
                    .map(|(index, name)| {
                        (
                            name,
                            (0..8193)
                                .map(|n| ((n * 7 + n / 251 + index * 13) % 253) as u8)
                                .collect(),
                        )
                    })
                    .collect();
                (archive, expected)
            } else {
                let archive = build_7z_shaped(
                    &[Entry::file("feature.mkv", payload.clone())],
                    sevenz_turbo::EncoderMethod::COPY,
                    None,
                    false,
                );
                (archive, vec![("feature.mkv", payload)])
            };
            (split_volumes(&archive, count), expected)
        } else if let Some(format) = cell.container.solid_format(count) {
            // The volumes go under the real names a stored set has; every
            // naming renames posted files and leaves the members alone.
            let parts: Vec<Vec<u8>> = match format.volumes() {
                Some((_, parts)) => parts.into_iter().map(<[u8]>::to_vec).collect(),
                None => vec![format.fixture().1.to_vec()],
            };
            assert_eq!(parts.len(), count, "{cell:?}");
            let written = parts
                .into_iter()
                .enumerate()
                .map(|(index, bytes)| {
                    let name = if count == 1 {
                        "silver.horizon.rar".to_string()
                    } else {
                        format!("silver.horizon.part{:02}.rar", index + 1)
                    };
                    (name, bytes)
                })
                .collect();
            let expected = compressed_members(format)
                .into_iter()
                .map(|(name, bytes)| (&*name.leak(), bytes))
                .collect();
            (written, expected)
        } else {
            let name = "nested/feature.mkv";
            let mut written = match cell.container {
                // Written with the new-numbering flag clear, as an old-style
                // set's main header is.
                Container::Rar4 if cell.naming == Naming::OldStyle => {
                    single_member_rar4_old_numbering_set("silver.horizon", name, &payload, count)
                }
                Container::Rar4 if count == 4 => {
                    single_member_rar4_store_set_numbered(name, &payload, count)
                }
                Container::Rar4 => single_member_rar4_store_set(name, &payload, count),
                Container::Rar5 => single_member_store_set(name, &payload, count),
                Container::Rar5Encrypted => {
                    encrypted_store_set(name, &payload, count, PASSWORD, Some(PASSWORD), false)
                }
                Container::Rar5EncryptedHeaders => header_encrypted_store_set(
                    name,
                    &payload,
                    count,
                    PASSWORD,
                    HeaderCheck::For(PASSWORD),
                ),
                other => unreachable!("{other:?} is outside the grouping cross"),
            };
            if count == 1 {
                written[0].0 = "silver.horizon.rar".to_string();
            }
            (written, vec![(name, payload)])
        };
        // An old-style set's real names are `.rar`, then `.r00` onwards.
        if cell.naming == Naming::OldStyle {
            for (index, (name, _)) in written.iter_mut().enumerate() {
                *name = match index.checked_sub(1) {
                    None => "silver.horizon.rar".to_string(),
                    Some(next) => format!("silver.horizon.r{next:02}"),
                };
            }
        }
        let real: Vec<String> = posted.iter().map(|&at| written[at].0.clone()).collect();
        let names = cell.naming.posted(&real);
        let volumes: Vec<_> = posted
            .iter()
            .zip(names)
            .map(|(&at, name)| (name, written[at].1.clone()))
            .collect();
        let articles = 4 / volumes.len();
        let mut spec = if cell.container.is_sevenz() {
            sevenz_job_spec(&volumes, articles)
        } else {
            direct_store_job_spec_with_articles("Archive schedules", &volumes, articles)
        };
        spec.password = matches!(
            cell.container,
            Container::Rar5Encrypted
                | Container::Rar5EncryptedHeaders
                | Container::Rar4SolidEncrypted
                | Container::Rar5SolidEncrypted
        )
        .then(|| PASSWORD.to_string());
        Self {
            volumes,
            lengths: written.iter().map(|(_, bytes)| bytes.len()).collect(),
            real,
            spec,
            expected,
        }
    }

    // Whether `mask` loses an article no other posted copy of its volume
    // still carries: the bytes are then gone from the post.
    pub(super) fn uncovered_loss(&self, mask: u8) -> bool {
        let articles = 4 / self.volumes.len();
        (0..4).any(|slot| {
            mask & (1 << slot) != 0
                && !self.volumes.iter().enumerate().any(|(copy, (_, bytes))| {
                    bytes == &self.volumes[slot / articles].1
                        && mask & (1 << (copy * articles + slot % articles)) == 0
                })
        })
    }

    // `interruption` as the post delivers it: a volume the post never
    // carried is that volume posted and lost whole.
    pub(super) fn posted_loss(&self, interruption: Interruption) -> Interruption {
        match interruption.withheld() {
            Some((volume, index_first)) => {
                let articles = 4 / self.volumes.len() as u32;
                Interruption::Loss {
                    mask: (((1u32 << articles) - 1) << (volume * articles)) as u8,
                    index_first,
                }
            }
            None => interruption,
        }
    }
}

impl Binding {
    // Whether an index gives the volumes their real names, which place them.
    pub(super) fn names_real(self) -> bool {
        matches!(self, Self::Par2RealFirst | Self::Par2RealLast)
    }

    // How the schedule is driven, and the names any listing carries.
    pub(super) fn drive(self, fixture: &Fixture) -> (ScheduleOptions, Option<Vec<String>>) {
        let matrix = ScheduleOptions::MATRIX;
        let real = Some(fixture.real.clone());
        match self {
            Self::Par2RealFirst => (
                ScheduleOptions {
                    index_first: true,
                    ..matrix
                },
                real,
            ),
            Self::Par2RealLast => (
                ScheduleOptions {
                    index_last: true,
                    ..matrix
                },
                real,
            ),
            Self::Par2Posted => (
                matrix,
                Some(
                    fixture
                        .volumes
                        .iter()
                        .map(|(name, _)| name.clone())
                        .collect(),
                ),
            ),
            Self::Sfv => (
                ScheduleOptions {
                    recovery: RecoveryFormat::Absent,
                    sfv: true,
                    ..matrix
                },
                real,
            ),
            Self::Nothing => (
                ScheduleOptions {
                    recovery: RecoveryFormat::Absent,
                    ..matrix
                },
                None,
            ),
        }
    }
}

impl Cell {
    // Whether the post orders a middle volume by more than the volumes held
    // beside it: names that number it, a recovery set that gives it its
    // real name, or a header in its front that numbers it. A 7z piece after
    // the first has no header, and a RAR4 volume numbers itself only in its
    // end record.
    pub(super) fn orders_middles(self) -> bool {
        self.naming.names_place()
            || matches!(self.binding, Binding::Par2RealFirst | Binding::Par2RealLast)
            || self.container.headers_chain()
    }

    // Whether the cell's post can be driven through `interruption`: a
    // withheld volume has to be one the post carries and leaving it out has
    // to withhold something.
    pub(super) fn presents(self, interruption: Interruption) -> bool {
        interruption.fits(self.naming.layout().1.len())
            && (interruption.withheld().is_none() || self.naming.withholds())
    }
}

// The recovery layouts a schedule is posted under: the binding's own, and
// for a repair a PAR2 set as posters lay it out too, so the volumes it needs
// are promoted one by one.
pub(super) fn recovery_layouts(
    options: ScheduleOptions,
    interruption: Interruption,
) -> Vec<ScheduleOptions> {
    let ladder = options.recovery == RecoveryFormat::Par2
        && (interruption.loss().is_some() || interruption.withheld().is_some());
    std::iter::once(options)
        .chain(ladder.then_some(ScheduleOptions {
            recovery: RecoveryFormat::Par2Ladder,
            ..options
        }))
        .collect()
}

pub(super) const CONTAINERS: [Container; 10] = [
    Container::Rar4,
    Container::Rar5,
    Container::Rar5Encrypted,
    Container::Rar5EncryptedHeaders,
    Container::SevenZip,
    Container::SevenZipSolid,
    Container::Rar4Solid,
    Container::Rar4SolidEncrypted,
    Container::Rar5Solid,
    Container::Rar5SolidEncrypted,
];

pub(super) const NAMINGS: [Naming; 15] = [
    Naming::Conventional,
    Naming::ConventionalFour,
    Naming::Single,
    Naming::MixedCase,
    Naming::OldStyle,
    Naming::HexBare,
    Naming::HexSingle,
    Naming::HexMisnumbered,
    Naming::HexSwappedRar,
    Naming::HexScattered,
    Naming::HexNumberedGap,
    Naming::FirstVolumeHex,
    Naming::Reposted,
    Naming::HexReposted,
    Naming::BareFirstPart,
];

// The bindings the grouping cross varies.
pub(super) const BINDINGS: [Binding; 5] = [
    Binding::Par2RealFirst,
    Binding::Par2RealLast,
    Binding::Par2Posted,
    Binding::Sfv,
    Binding::Nothing,
];

// Every possible cell of the cross, in axis order.
pub(super) fn possible_cells() -> Vec<Cell> {
    let mut cells = Vec::new();
    for container in CONTAINERS {
        for naming in NAMINGS {
            for binding in BINDINGS {
                let cell = Cell {
                    container,
                    naming,
                    binding,
                };
                if matches!(rule(cell), Ruling::Possible) {
                    cells.push(cell);
                }
            }
        }
    }
    cells
}

// Prints the generator's list for the possible cells, in the macro's form.
fn generator_listing() -> String {
    fn snake(name: String) -> String {
        let mut out = String::new();
        for (index, ch) in name.chars().enumerate() {
            if ch.is_ascii_uppercase() {
                if index > 0 {
                    out.push('_');
                }
                out.push(ch.to_ascii_lowercase());
            } else {
                out.push(ch);
            }
        }
        out.replace("rar_4", "rar4")
            .replace("rar_5", "rar5")
            .replace("par_2", "par2")
    }
    let cells = possible_cells();
    let mut listing = String::new();
    for container in CONTAINERS {
        listing.push_str(&format!(
            "smoke {} {container:?} {{\n",
            snake(format!("{container:?}"))
        ));
        for naming in NAMINGS {
            let bindings: Vec<_> = cells
                .iter()
                .filter(|cell| cell.container == container && cell.naming == naming)
                .map(|cell| {
                    format!(
                        "{} {:?}",
                        snake(format!("{:?}", cell.binding)),
                        cell.binding
                    )
                })
                .collect();
            if !bindings.is_empty() {
                listing.push_str(&format!(
                    "    {} {naming:?} [{}]\n",
                    snake(format!("{naming:?}")),
                    bindings.join(" ")
                ));
            }
        }
        listing.push_str("}\n");
    }
    listing
}

// The generator emits a campaign for exactly the possible cells.
pub(super) fn assert_generated(generated: &[Cell]) {
    assert!(
        generated == possible_cells().as_slice(),
        "the generator's list is stale; replace it with:\n{}",
        generator_listing()
    );
}
