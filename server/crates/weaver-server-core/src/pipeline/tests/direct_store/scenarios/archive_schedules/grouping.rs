// The grouping-information axis of the archive matrix.
//
// A cell is a container, the names its volumes are posted under, and what
// else the post carries that could group or order them. [`rule`] states, for
// every cell, whether a single job can present it at all and, if it can,
// what each extraction profile must make of it: the volume set direct store
// admits by name alone, and whether the job extracts direct, streams through
// chase, extracts after download, demotes for a named reason, or fails.
//
// The ruling is an exhaustive match over the axes with no fallback arm, so a
// value added to any axis does not compile until every cell it opens has an
// expectation. The hand-named [`Format`](super::Format) layouts are aliases
// of cells, and take their direct-store route from here.
//
// The cells reuse the matrix's synthetic stored-volume builders and the 7z
// writer; nothing here is a captured post.
use super::super::sevenz_store::schedules::{LOSES, map_slots, unrepeated_payload};
use super::super::sevenz_store::{Entry, build_7z_shaped, sevenz_job_spec, split_volumes};
use super::*;
use crate::pipeline::direct_store::plan::{AdmissionRefusal, DirectSetPlan};

// What the volumes are.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Container {
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
    // under plain headers or not: direct store refuses every member, so
    // they extract by the fallback route.
    Rar4Solid,
    Rar4SolidEncrypted,
    Rar5Solid,
    Rar5SolidEncrypted,
    // Codec and integrity variants the grouping cross does not vary; they
    // exist as cells only so their hand-named formats alias to one.
    Rar4Encrypted,
    Rar4Unsalted,
    Rar5KeyedChecksum,
    Rar5UncheckedHeaders,
    QuickOpen,
    Blake2,
}

// The names the volumes are posted under.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Naming {
    // `silver.horizon.partNN.rar`, `silver.horizon.7z.NNN`.
    Conventional,
    // [`Naming::Conventional`] over four volumes, so two are middle volumes.
    ConventionalFour,
    // One volume, `silver.horizon.rar` or `silver.horizon.7z`.
    Single,
    // The first volume's stem in a different case from the second's.
    MixedCase,
    // `.rar` then `.s00`: the second old-style series, which an archiver
    // starts after `.r99`.
    OldStyleS,
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
pub(super) enum Binding {
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
    // The matrix's own: a PAR2 index under the posted names, posted only
    // when the schedule loses something. The hand-named formats' binding.
    LegacyOnLoss,
    // The matrix's own obfuscated binding: a PAR2 index with the real names,
    // ahead of the volumes unless the schedule's loss puts it last.
    LegacyReal,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Cell {
    pub container: Container,
    pub naming: Naming,
    pub binding: Binding,
}

// What direct store's name-only discovery makes of the posted names.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Admission {
    // One set, admitted by name.
    Named,
    // No set admitted; this refusal named.
    Refused(AdmissionRefusal),
    // One set admitted by name, and another under a name that differs
    // only in case refused for this.
    Split(AdmissionRefusal),
    // Nothing the names say forms a set.
    Unnamed,
}

// What one extraction profile must make of a cell.
#[derive(Clone, Copy, Debug)]
pub(super) enum Expect {
    // Direct store finalizes the set from its own partials.
    Direct,
    // No direct set; chase streams the extraction while it downloads.
    Streams,
    // Direct store demotes the set for a reason this accepts; the job then
    // extracts by the fallback route.
    Demotes(fn(DemotionReason) -> bool),
    // Extracted after download, with neither direct store nor chase.
    Downloads,
    // The job fails with an error naming this, and publishes nothing.
    Fails(&'static str),
}

#[derive(Clone, Copy, Debug)]
pub(super) struct Expectation {
    pub admission: Admission,
    pub direct_store: Expect,
    pub chase: Expect,
    pub conventional: Expect,
}

#[derive(Clone, Copy, Debug)]
pub(super) enum Ruling {
    // No single job can present this cell, for the stated reason.
    Impossible(&'static str),
    Possible(Expectation),
}

macro_rules! any_naming {
    () => {
        Naming::Conventional
            | Naming::ConventionalFour
            | Naming::Single
            | Naming::MixedCase
            | Naming::OldStyleS
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
            | Binding::LegacyOnLoss
            | Binding::LegacyReal
    };
}

fn rar_ineligible(reason: MemberIneligibility) -> fn(DemotionReason) -> bool {
    match reason {
        MemberIneligibility::Blake2OnlyNoCrc32 => |reason| {
            matches!(
                reason,
                DemotionReason::MemberIneligible(MemberIneligibility::Blake2OnlyNoCrc32)
            )
        },
        MemberIneligibility::MalformedChain => |reason| {
            matches!(
                reason,
                DemotionReason::MemberIneligible(MemberIneligibility::MalformedChain)
            )
        },
        other => unreachable!("no cell demotes for {other:?}"),
    }
}

fn sevenz_coder(reason: DemotionReason) -> bool {
    matches!(reason, DemotionReason::SevenZip(SevenZipRefusal::Coder))
}

// The same expectation under every profile but direct store.
const fn fallback(admission: Admission, direct_store: Expect) -> Expectation {
    Expectation {
        admission,
        direct_store,
        chase: Expect::Streams,
        conventional: Expect::Downloads,
    }
}

impl Container {
    fn is_sevenz(self) -> bool {
        matches!(self, Self::SevenZip | Self::SevenZipSolid)
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

    // What direct store does with a set it has admitted.
    fn admitted(self) -> Expect {
        match self {
            Self::SevenZipSolid => Expect::Demotes(sevenz_coder),
            Self::Rar4Solid
            | Self::Rar4SolidEncrypted
            | Self::Rar5Solid
            | Self::Rar5SolidEncrypted => Expect::Demotes(|reason| {
                matches!(
                    reason,
                    DemotionReason::MemberIneligible(
                        MemberIneligibility::Compressed
                            | MemberIneligibility::Solid
                            | MemberIneligibility::Encrypted
                    )
                )
            }),
            Self::Rar5UncheckedHeaders => Expect::Demotes(|reason| {
                matches!(reason, DemotionReason::HeaderEncryptedRefused(_))
            }),
            Self::Blake2 => Expect::Demotes(rar_ineligible(MemberIneligibility::Blake2OnlyNoCrc32)),
            Self::Rar4
            | Self::Rar5
            | Self::Rar5Encrypted
            | Self::Rar5EncryptedHeaders
            | Self::SevenZip
            | Self::Rar4Encrypted
            | Self::Rar4Unsalted
            | Self::Rar5KeyedChecksum
            | Self::QuickOpen => Expect::Direct,
        }
    }
}

// Every cell's ruling. See the module documentation.
pub(super) fn rule(cell: Cell) -> Ruling {
    let Cell {
        container,
        naming,
        binding,
    } = cell;
    match container {
        Container::Rar4Encrypted
        | Container::Rar4Unsalted
        | Container::Rar5KeyedChecksum
        | Container::Rar5UncheckedHeaders
        | Container::QuickOpen
        | Container::Blake2 => match (naming, binding) {
            (Naming::Conventional, Binding::LegacyOnLoss) => {
                Ruling::Possible(fallback(Admission::Named, container.admitted()))
            }
            (Naming::ConventionalFour, Binding::LegacyOnLoss)
                if container == Container::Rar4Encrypted =>
            {
                Ruling::Possible(fallback(Admission::Named, container.admitted()))
            }
            (any_naming!(), any_binding!()) => {
                Ruling::Impossible("a codec or integrity variant the grouping cross does not vary")
            }
        },
        Container::Rar4
        | Container::Rar5
        | Container::Rar5Encrypted
        | Container::Rar5EncryptedHeaders
        | Container::SevenZip
        | Container::SevenZipSolid
        | Container::Rar4Solid
        | Container::Rar4SolidEncrypted
        | Container::Rar5Solid
        | Container::Rar5SolidEncrypted => crossed(cell),
    }
}

fn crossed(cell: Cell) -> Ruling {
    let Cell {
        container,
        naming,
        binding,
    } = cell;
    let sevenz = container.is_sevenz();
    // Whether a PAR2 index naming the real volumes is in hand before any
    // volume byte lands: the roster that admits a set no name or header does.
    let roster = matches!(binding, Binding::Par2RealFirst | Binding::LegacyReal);
    // A set nothing names: its own headers admit it, or an index in time
    // does, or nothing does and it extracts once downloaded.
    let unnamed = |single: bool| {
        if container.headers_chain() || (sevenz && single) || roster {
            container.admitted()
        } else {
            Expect::Streams
        }
    };
    // A 7z continuation volume carries no header of its own, so with no
    // real-name index nothing but the name orders it.
    let real = matches!(
        binding,
        Binding::Par2RealFirst | Binding::Par2RealLast | Binding::LegacyReal
    );
    let unordered = Expectation {
        admission: Admission::Unnamed,
        direct_store: Expect::Fails(UNORDERED_7Z),
        chase: Expect::Fails(UNORDERED_7Z),
        conventional: Expect::Fails(UNORDERED_7Z),
    };
    // Names discovery refuses: the volumes' own headers or an index in time
    // admit the set, else it extracts by the fallback route.
    let by_roster = |admission| {
        fallback(
            admission,
            if roster || container.headers_chain() {
                container.admitted()
            } else {
                Expect::Streams
            },
        )
    };
    match (naming, binding) {
        (Naming::OldStyleS | Naming::HexSwappedRar, any_binding!()) if sevenz => {
            Ruling::Impossible("a RAR naming")
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
            | Naming::OldStyleS,
            Binding::Par2Posted | Binding::LegacyReal,
        ) => Ruling::Impossible(
            "the posted names are the real ones, so describing them is the Par2Real binding",
        ),
        (
            Naming::Reposted | Naming::HexReposted,
            Binding::Par2RealFirst | Binding::Par2RealLast | Binding::Sfv | Binding::LegacyReal,
        ) => Ruling::Impossible("a real-name listing names each volume once, not each copy"),
        (
            Naming::Conventional
            | Naming::ConventionalFour
            | Naming::Single
            | Naming::MixedCase
            | Naming::OldStyleS,
            Binding::Par2RealFirst
            | Binding::Par2RealLast
            | Binding::Sfv
            | Binding::Nothing
            | Binding::LegacyOnLoss,
        ) => Ruling::Possible(fallback(Admission::Named, container.admitted())),
        (
            Naming::HexBare | Naming::HexMisnumbered | Naming::HexScattered,
            Binding::Par2Posted | Binding::Sfv | Binding::Nothing | Binding::LegacyOnLoss,
        ) if sevenz && !real => Ruling::Possible(unordered),
        (
            Naming::HexBare | Naming::HexMisnumbered | Naming::HexScattered,
            Binding::Par2RealFirst
            | Binding::Par2RealLast
            | Binding::Par2Posted
            | Binding::Sfv
            | Binding::Nothing
            | Binding::LegacyOnLoss
            | Binding::LegacyReal,
        ) => Ruling::Possible(fallback(Admission::Unnamed, unnamed(false))),
        (
            Naming::HexSingle,
            Binding::Par2RealFirst
            | Binding::Par2RealLast
            | Binding::Par2Posted
            | Binding::Sfv
            | Binding::Nothing
            | Binding::LegacyOnLoss
            | Binding::LegacyReal,
        ) => Ruling::Possible(fallback(Admission::Unnamed, unnamed(true))),
        // A solid set refuses its member from the first header it reads,
        // which may come before the chain is seen to run backwards.
        (Naming::HexSwappedRar, any_binding!()) if container.is_solid_rar() => {
            Ruling::Possible(fallback(
                Admission::Named,
                Expect::Demotes(|reason| {
                    matches!(
                        reason,
                        DemotionReason::MemberIneligible(
                            MemberIneligibility::Compressed
                                | MemberIneligibility::Solid
                                | MemberIneligibility::Encrypted
                                | MemberIneligibility::MalformedChain
                        )
                    )
                }),
            ))
        }
        // Names that admit a set whose headers then chain the other way
        // round: direct store demotes it as malformed.
        (
            Naming::HexSwappedRar,
            Binding::Par2RealFirst
            | Binding::Par2RealLast
            | Binding::Par2Posted
            | Binding::Sfv
            | Binding::Nothing
            | Binding::LegacyOnLoss
            | Binding::LegacyReal,
        ) => Ruling::Possible(fallback(
            Admission::Named,
            Expect::Demotes(rar_ineligible(MemberIneligibility::MalformedChain)),
        )),
        // The missing part is reported, never renumbered around.
        (
            Naming::HexNumberedGap,
            Binding::Par2RealFirst
            | Binding::Par2RealLast
            | Binding::Par2Posted
            | Binding::Sfv
            | Binding::Nothing
            | Binding::LegacyOnLoss
            | Binding::LegacyReal,
        ) => Ruling::Possible(Expectation {
            admission: Admission::Unnamed,
            direct_store: Expect::Fails(MISSING_PART),
            chase: Expect::Fails(MISSING_PART),
            conventional: Expect::Fails(MISSING_PART),
        }),
        (
            Naming::FirstVolumeHex,
            Binding::Par2RealFirst
            | Binding::Par2RealLast
            | Binding::Par2Posted
            | Binding::Sfv
            | Binding::Nothing
            | Binding::LegacyOnLoss
            | Binding::LegacyReal,
        ) => Ruling::Possible(by_roster(Admission::Refused(AdmissionRefusal::VolumeGap))),
        // Each volume posted twice under two numberings: refused, and
        // extracted once the copies are down.
        (Naming::Reposted, Binding::Par2Posted | Binding::Nothing | Binding::LegacyOnLoss) => {
            Ruling::Possible(Expectation {
                admission: Admission::Refused(AdmissionRefusal::DuplicateVolume),
                direct_store: Expect::Downloads,
                chase: Expect::Downloads,
                conventional: Expect::Downloads,
            })
        }
        (Naming::HexReposted, Binding::Par2Posted | Binding::Nothing | Binding::LegacyOnLoss)
            if sevenz =>
        {
            Ruling::Possible(unordered)
        }
        (Naming::HexReposted, Binding::Par2Posted | Binding::Nothing | Binding::LegacyOnLoss) => {
            Ruling::Possible(fallback(Admission::Unnamed, unnamed(false)))
        }
        (
            Naming::BareFirstPart,
            Binding::Par2RealFirst
            | Binding::Par2RealLast
            | Binding::Par2Posted
            | Binding::Sfv
            | Binding::Nothing
            | Binding::LegacyOnLoss
            | Binding::LegacyReal,
        ) => Ruling::Possible(by_roster(Admission::Refused(
            AdmissionRefusal::MixedSevenZipShape,
        ))),
    }
}

// What a job missing a volume says about it.
const MISSING_PART: &str = "missing";

// What a job whose 7z continuation volumes nothing orders says.
const UNORDERED_7Z: &str = "failed to read 7z archive";

impl Expectation {
    fn of(self, profile: ExtractionProfile) -> Expect {
        match profile {
            ExtractionProfile::DirectStore => self.direct_store,
            ExtractionProfile::Chase => self.chase,
            ExtractionProfile::Conventional => self.conventional,
        }
    }
}

impl Naming {
    // Volumes the archive is written as, and the indices of those posted,
    // in posted order.
    fn layout(self) -> (usize, &'static [usize]) {
        match self {
            Self::Single | Self::HexSingle => (1, &[0]),
            Self::ConventionalFour | Self::HexScattered => (4, &[0, 1, 2, 3]),
            Self::HexNumberedGap => (3, &[0, 2]),
            Self::Reposted | Self::HexReposted => (2, &[0, 1, 0, 1]),
            Self::Conventional
            | Self::MixedCase
            | Self::OldStyleS
            | Self::HexBare
            | Self::HexMisnumbered
            | Self::HexSwappedRar
            | Self::FirstVolumeHex
            | Self::BareFirstPart => (2, &[0, 1]),
        }
    }

    // Whether the posted names place the set, so a lost offset-zero article
    // leaves no volume without a set.
    fn names_place(self) -> bool {
        matches!(
            self,
            Self::Conventional
                | Self::ConventionalFour
                | Self::Single
                | Self::MixedCase
                | Self::OldStyleS
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
            Self::Conventional | Self::ConventionalFour | Self::Single => real.to_vec(),
            Self::MixedCase => {
                let mut names = real.to_vec();
                names[0] = names[0].replacen("silver.horizon", "Silver.Horizon", 1);
                names
            }
            Self::OldStyleS => vec!["silver.horizon.rar".into(), "silver.horizon.s00".into()],
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
struct Fixture {
    volumes: Vec<(String, Vec<u8>)>,
    // What each posted volume is really called, in posted order.
    real: Vec<String>,
    spec: JobSpec,
    expected: Vec<(&'static str, Vec<u8>)>,
    // Loss masks that leave a 7z set without its map.
    unmapped: fn(u8) -> bool,
}

impl Fixture {
    fn build(cell: Cell) -> Self {
        let (count, posted) = cell.naming.layout();
        let payload = unrepeated_payload(13, 70_001);
        let (written, expected, unmapped) = if cell.container.is_sevenz() {
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
            let unmapped = if posted.len() == count {
                LOSES[usize::from(map_slots(&archive, count, 4 / count))]
            } else {
                LOSES[0]
            };
            (split_volumes(&archive, count), expected, unmapped)
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
            (written, expected, LOSES[0])
        } else {
            let name = "nested/feature.mkv";
            let mut written = match cell.container {
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
            (written, vec![(name, payload)], LOSES[0])
        };
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
            real,
            spec,
            expected,
            unmapped,
        }
    }

    fn admission(&self) -> Admission {
        let (plans, refused) = DirectSetPlan::discover(
            &self.spec,
            Path::new("/grouping/work"),
            Path::new("/grouping/output"),
        );
        match (plans.len(), refused.as_slice()) {
            (1, []) => Admission::Named,
            (0, [(_, refusal)]) => Admission::Refused(*refusal),
            (0, []) => Admission::Unnamed,
            (1, [(_, refusal)]) => Admission::Split(*refusal),
            _ => panic!(
                "discovery admitted {} sets and refused {refused:?}",
                plans.len()
            ),
        }
    }

    fn wanted(&self) -> Vec<&'static str> {
        self.expected.iter().map(|(name, _)| *name).collect()
    }

    fn identity_prefix_slots(&self) -> u8 {
        let articles = 4 / self.volumes.len();
        let mut mask = 0;
        for (file, (_, bytes)) in self.volumes.iter().enumerate() {
            for article in 0..articles {
                let (start, _) = article_extent(bytes.len(), article as u32, articles);
                if start < bytes.len().min(crate::pipeline::PAR2_HASH_16K_BYTES) {
                    mask |= 1 << (file * articles + article);
                }
            }
        }
        mask
    }
}

impl Binding {
    // How the schedule is driven, and the names any listing carries.
    fn drive(self, fixture: &Fixture) -> (ScheduleOptions, Option<Vec<String>>) {
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
            Self::LegacyOnLoss => (matrix, None),
            Self::LegacyReal => (matrix, real),
        }
    }
}

// The direct-store route a cell's expectation allows, with `unmapped` the
// loss masks that leave its map unreadable.
fn route(cell: Cell, expect: Expect, unmapped: fn(u8) -> bool) -> Route {
    let (_, posted) = cell.naming.layout();
    // Identity loss precedes codec selection, including a solid archive that
    // would normally demote only after its map revealed an unsupported coder.
    let unnamed_loss: fn(u8) -> bool = if cell.naming.names_place() {
        |_| false
    } else {
        match posted.len() {
            1 => |mask| mask & 0b0001 != 0,
            2 => |mask| mask & 0b0101 != 0,
            _ => |mask| mask != 0,
        }
    };
    let shape = match expect {
        Expect::Direct => {
            // A set only the index's descriptions name is named in time only
            // when the index leads.
            let named_by_early_index = cell.binding == Binding::LegacyReal
                && !cell.naming.names_place()
                && !cell.container.headers_chain();
            Route {
                named_by_early_index,
                ..Route::DIRECT
            }
        }
        Expect::Demotes(reason) => Route::refused(reason),
        Expect::Streams | Expect::Downloads => match cell.container.admitted() {
            // A restart can put retained metadata ahead of refetched parts,
            // admitting a set that originally entered the conventional path.
            Expect::Demotes(reason) => Route::refused(reason),
            _ => Route::refused(|_| false),
        },
        Expect::Fails(_) => Route::refused(|_| false),
    };
    Route {
        unnamed_loss,
        unmapped_loss: unmapped,
        ..shape
    }
}

// The direct-store route of a hand-named format's cell.
pub(super) fn alias_route(cell: Cell) -> Route {
    match rule(cell) {
        Ruling::Possible(expectation) => route(cell, expectation.direct_store, |_| false),
        Ruling::Impossible(why) => panic!("{cell:?} aliases an impossible cell: {why}"),
    }
}

impl Expect {
    // Holds a clean schedule's outcome to the expectation.
    fn assert_clean(self, outcome: &Outcome, unique: bool, context: &str) {
        let trace = &outcome.trace;
        match self {
            Self::Direct => assert_eq!(outcome.finalized, 1, "{context}: {trace:?}"),
            Self::Streams => {
                assert_eq!(outcome.finalized, 0, "{context}: {trace:?}");
                assert!(outcome.chase_armed > 0, "{context}: {trace:?}");
                if unique {
                    assert_eq!(outcome.chase_consumed, 1, "{context}: {trace:?}");
                }
            }
            Self::Demotes(reason) => {
                assert_eq!(outcome.finalized, 0, "{context}: {trace:?}");
                assert!(
                    !outcome.demotions.is_empty()
                        && outcome.demotions.iter().all(|demotion| reason(*demotion)),
                    "{context}: demotions {:?}: {trace:?}",
                    outcome.demotions
                );
            }
            Self::Downloads => {
                assert_eq!(outcome.finalized, 0, "{context}: {trace:?}");
                assert_eq!(outcome.chase_consumed, 0, "{context}: {trace:?}");
            }
            Self::Fails(_) => unreachable!("a failing cell has no clean outcome"),
        }
    }
}

// Runs `cases` over `cell` under `profile` and holds each to the ruling.
async fn run_cell(cell: Cell, profile: ExtractionProfile, cases: Vec<(usize, Schedule)>) {
    let expectation = match rule(cell) {
        Ruling::Possible(expectation) => expectation,
        Ruling::Impossible(why) => panic!("{cell:?} is impossible: {why}"),
    };
    let fixture = Fixture::build(cell);
    assert_eq!(fixture.admission(), expectation.admission, "{cell:?}");
    let expect = expectation.of(profile);
    let mut route = route(cell, expect, fixture.unmapped);
    let identity_prefix_slots = fixture.identity_prefix_slots();
    if !cell.naming.names_place() {
        // A small part needs more than its opening article to prove the
        // complete PAR2 identity prefix.
        route.unnamed_loss = LOSES[usize::from(identity_prefix_slots)];
    }
    let (options, described) = cell.binding.drive(&fixture);
    let wanted = fixture.wanted();
    // A posted listing is kept beside the members, as any loose file is.
    let mut published = wanted.clone();
    if options.sfv {
        published.push("silver.horizon.sfv");
    }
    for (case, (order, interruption)) in cases {
        if !profile.includes(interruption)
            || !interruption.fits(fixture.volumes.len())
            || (interruption.withheld().is_some() && !cell.naming.withholds())
        {
            continue;
        }
        // A repair also runs against the recovery set as posters lay it out,
        // so the volumes it needs are promoted one by one.
        let ladder = options.recovery == RecoveryFormat::Par2
            && (interruption.loss().is_some() || interruption.withheld().is_some());
        let ladder = ladder.then_some(ScheduleOptions {
            recovery: RecoveryFormat::Par2Ladder,
            ..options
        });
        for options in std::iter::once(options).chain(ladder) {
            let context = format!(
                "{cell:?} profile={profile:?} case={case} order={order:?} interruption={interruption:?} recovery={:?}",
                options.recovery
            );
            eprintln!("{context}");
            let outcome = run_schedule_with(
                options,
                profile,
                fixture.spec.clone(),
                &fixture.volumes,
                described.as_deref(),
                &order,
                &wanted,
                interruption,
            )
            .await;
            let check = || {
                // An available article from a byte-identical repost supplies the
                // same bytes. Otherwise a listing cannot repair missing data.
                let unrepairable =
                    (options.recovery == RecoveryFormat::Absent || interruption.fails())
                        && interruption.loss().is_some_and(|(mask, _)| {
                            // Names or posted PAR2 descriptions can identify the
                            // absent copy. Parity is unnecessary when identical
                            // source bytes remain available in another posting.
                            let identifiable_copy = cell.naming == Naming::Reposted
                                || (cell.naming == Naming::HexReposted
                                    && cell.binding == Binding::Par2Posted);
                            if !identifiable_copy {
                                return mask != 0;
                            }
                            let articles_per_file = 4 / fixture.volumes.len();
                            (0..4).any(|slot| {
                                mask & (1 << slot) != 0
                                    && !fixture.volumes.iter().enumerate().any(
                                        |(copy, (_, bytes))| {
                                            bytes == &fixture.volumes[slot / articles_per_file].1
                                                && mask
                                                    & (1 << (copy * articles_per_file
                                                        + slot % articles_per_file))
                                                    == 0
                                        },
                                    )
                            })
                        });
                if unrepairable {
                    profile.assert_rejected(&outcome, &wanted);
                    return;
                }
                // A volume the post never carried comes back only from recovery
                // blocks. Without them the job fails; a set that names its
                // volumes says which part it is missing.
                let withheld = interruption.withheld().is_some();
                // A 7z set no name or index places, with the one volume that
                // carries its signature never posted and nothing describing
                // it: the job holds files it has no reason to read as an
                // archive, and completes them as posted.
                if cell.container.is_sevenz()
                    && matches!(
                        cell.naming,
                        Naming::HexBare | Naming::HexMisnumbered | Naming::HexScattered
                    )
                    && options.recovery == RecoveryFormat::Absent
                    && interruption
                        .withheld()
                        .is_some_and(|(volume, _)| volume == 0)
                {
                    profile.assert_route(&outcome);
                    assert_eq!(
                        outcome.status,
                        Some(JobStatus::Complete),
                        "{context}: {:?}",
                        outcome.trace
                    );
                    assert_eq!(outcome.finalized, 0, "{context}: {:?}", outcome.trace);
                    assert!(
                        outcome.files.values().all(Option::is_none)
                            && wanted.iter().all(|name| !outcome.published.contains(*name)),
                        "{context}: an unreadable set published a member {:?}: {:?}",
                        outcome.published,
                        outcome.trace
                    );
                    return;
                }
                if withheld
                    && (options.recovery == RecoveryFormat::Absent
                        || matches!(expect, Expect::Fails(_)))
                {
                    profile.assert_rejected(&outcome, &wanted);
                    let Some(JobStatus::Failed { error }) = &outcome.status else {
                        unreachable!("assert_rejected holds the job failed")
                    };
                    // A withheld first volume reads as a set with no start, which
                    // is its own diagnostic.
                    if options.recovery == RecoveryFormat::Absent
                        && cell.naming.names_place()
                        && interruption
                            .withheld()
                            .is_some_and(|(volume, _)| volume > 0)
                    {
                        assert!(
                            error.contains(MISSING_PART),
                            "{context}: {error}: {:?}",
                            outcome.trace
                        );
                    }
                    return;
                }
                if let Expect::Fails(why) = expect {
                    profile.assert_rejected(&outcome, &wanted);
                    let Some(JobStatus::Failed { error }) = &outcome.status else {
                        unreachable!("assert_rejected holds the job failed")
                    };
                    // A posted-name index authenticates the misleading numbering.
                    // After restart it can reach the bounded extraction retry;
                    // clean recovery data still cannot establish archive order.
                    let verified_but_unordered = why == UNORDERED_7Z
                        && cell.binding == Binding::Par2Posted
                        && error.contains(
                            "clean PAR2 verification but extraction still failing after retry",
                        );
                    assert!(
                        error.contains(why) || verified_but_unordered,
                        "{context}: {error}: {:?}",
                        outcome.trace
                    );
                    return;
                }
                assert_eq!(
                    outcome.status,
                    Some(JobStatus::Complete),
                    "{context}: {:?}",
                    outcome.trace
                );
                for (name, bytes) in &fixture.expected {
                    assert_eq!(
                        outcome.files[*name].as_deref(),
                        Some(bytes.as_slice()),
                        "{context}: {:?}",
                        outcome.trace
                    );
                }
                let mut route = route;
                if interruption.loss().is_some_and(|(mask, _)| mask == 15)
                    && !matches!(
                        cell.naming,
                        Naming::Conventional
                            | Naming::ConventionalFour
                            | Naming::Single
                            | Naming::MixedCase
                    )
                {
                    // Recognizable posted names retain their direct identity even
                    // with every payload article absent. Obfuscated, reordered or
                    // reposted names need content evidence and rebuild on disk, and
                    // so do old-style names, which differ from the real PAR2 names
                    // and bind only through a content prefix the loss took.
                    route.direct = false;
                }
                if matches!(
                    interruption,
                    Interruption::Restart(_) | Interruption::Crash(_)
                ) && matches!(expect, Expect::Streams | Expect::Downloads)
                    && matches!(cell.container.admitted(), Expect::Direct)
                {
                    // Retained index metadata can identify refetched parts before
                    // their arrivals after restart. Either route remains valid;
                    // direct admission still requires no unexpected demotion and
                    // no refetch of durable articles below.
                    route.direct = outcome.finalized == route.sets;
                }
                if cell.naming == Naming::HexSwappedRar
                    && matches!(cell.binding, Binding::Par2RealFirst | Binding::Par2RealLast)
                    && interruption
                        .loss()
                        .is_some_and(|(mask, _)| mask & identity_prefix_slots != 0)
                {
                    // A lost identity prefix can prevent binding before enough
                    // header evidence survives to prove the swapped chain. Solid
                    // members can be refused before that binding is attempted.
                    route.shape_demotion = if cell.container.is_solid_rar() {
                        |reason| {
                            matches!(
                                reason,
                                DemotionReason::Par2Unbindable
                                    | DemotionReason::MemberIneligible(
                                        MemberIneligibility::Compressed
                                            | MemberIneligibility::Solid
                                            | MemberIneligibility::Encrypted
                                            | MemberIneligibility::MalformedChain
                                    )
                            )
                        }
                    } else {
                        |reason| {
                            matches!(
                                reason,
                                DemotionReason::Par2Unbindable
                                    | DemotionReason::MemberIneligible(
                                        MemberIneligibility::MalformedChain
                                    )
                            )
                        }
                    };
                }
                if cell.naming == Naming::HexSwappedRar
                    && !cell.container.is_solid_rar()
                    && interruption.withheld().is_some()
                {
                    // These names number the pair the other way round from the
                    // recovery set, so they cannot place the volume the post
                    // left out, and with one volume of the pair the chain that
                    // would show it runs backwards is never read. The missing
                    // volume is damage no direct set owns, and the set goes
                    // conventional, which its shape already sends it to.
                    route.shape_demotion = |reason| {
                        matches!(
                            reason,
                            DemotionReason::Par2Damaged
                                | DemotionReason::MemberIneligible(
                                    MemberIneligibility::MalformedChain
                                )
                        )
                    };
                }
                if cell.naming == Naming::OldStyleS
                    && interruption
                        .loss()
                        .is_some_and(|(mask, _)| mask & identity_prefix_slots != 0)
                {
                    // These posted names admit a RAR set but differ from the real
                    // PAR2 names. Without its content prefix a volume cannot be
                    // bound safely; repair must use materialized source files.
                    // Solid members may already have refused direct admission.
                    route.shape_demotion = if cell.container.is_solid_rar() {
                        |reason| {
                            matches!(
                                reason,
                                DemotionReason::Par2Unbindable
                                    | DemotionReason::MemberIneligible(
                                        MemberIneligibility::Compressed
                                            | MemberIneligibility::Solid
                                            | MemberIneligibility::Encrypted
                                    )
                            )
                        }
                    } else {
                        |reason| reason == DemotionReason::Par2Unbindable
                    };
                }
                // A volume the post never carried is held to the route of the
                // same volume posted and then lost whole: the set rebuilds it in
                // place, except where losing every article of that volume
                // already leaves the set without its map or a volume without
                // a name, and those exceptions are the loss's own.
                let delivered = match interruption.withheld() {
                    Some((volume, index_first)) => {
                        let articles = 4 / fixture.volumes.len() as u32;
                        Interruption::Loss {
                            mask: (((1u32 << articles) - 1) << (volume * articles)) as u8,
                            index_first,
                        }
                    }
                    None => interruption,
                };
                profile.assert_delivery(&outcome, route, &published, delivered);
                if matches!(interruption, Interruption::None) {
                    expect.assert_clean(&outcome, order.len() == 4, &context);
                }
            };
            check();
        }
    }
}

// A cell's campaign: every profile over the matrix's smoke schedules.
pub(super) async fn cell_campaign(cell: Cell) {
    for profile in PROFILES {
        let cases = schedules().into_iter().enumerate().collect();
        run_cell(cell, profile, cases).await;
    }
}

// A cell's default-suite smoke: in order and reversed, under every profile.
pub(super) async fn cell_smoke(cell: Cell) {
    let forward = slot_arrivals(4);
    let mut backward = forward.clone();
    backward.reverse();
    for profile in PROFILES {
        let cases = vec![
            (0, (forward.clone(), Interruption::None)),
            (1, (backward.clone(), Interruption::None)),
        ];
        run_cell(cell, profile, cases).await;
    }
}

// A split part the post never carried, between two that it did: every listed
// file is whole, so only the hole in the numbering says the recovery set
// has work, and it rebuilds the part rather than the job failing on it.
#[tokio::test]
async fn a_withheld_middle_split_part_is_rebuilt_from_recovery() {
    let case = schedules().into_iter().nth(361).unwrap();
    assert_eq!(
        case.1,
        Interruption::Withheld {
            volume: 2,
            index_first: false,
        }
    );
    for profile in PROFILES {
        run_cell(
            Cell {
                container: Container::SevenZip,
                naming: Naming::ConventionalFour,
                binding: Binding::Par2RealLast,
            },
            profile,
            vec![(361, case.clone())],
        )
        .await;
    }
}

#[tokio::test]
async fn repaired_swapped_rar_retires_lost_posted_volumes() {
    run_cell(
        Cell {
            container: Container::Rar4,
            naming: Naming::HexSwappedRar,
            binding: Binding::Par2RealFirst,
        },
        ExtractionProfile::DirectStore,
        [240, 244, 252, 268]
            .into_iter()
            .map(|case| (case, schedules().into_iter().nth(case).unwrap()))
            .collect(),
    )
    .await;
}

#[tokio::test]
async fn repaired_swapped_rar_does_not_reextract_damaged_sources() {
    for (container, profile, case) in [
        (Container::Rar4, ExtractionProfile::Chase, 248),
        (
            Container::Rar5Encrypted,
            ExtractionProfile::DirectStore,
            266,
        ),
    ] {
        run_cell(
            Cell {
                container,
                naming: Naming::HexSwappedRar,
                binding: Binding::Par2RealFirst,
            },
            profile,
            vec![(case, schedules().into_iter().nth(case).unwrap())],
        )
        .await;
    }
}

#[tokio::test]
async fn restored_opaque_repost_keeps_observed_identity() {
    for (container, case) in [
        (Container::Rar5, 156),
        (Container::Rar5Encrypted, 108),
        (Container::Rar5, 277),
        (Container::Rar5Encrypted, 277),
    ] {
        run_cell(
            Cell {
                container,
                naming: Naming::HexReposted,
                binding: Binding::Nothing,
            },
            ExtractionProfile::DirectStore,
            vec![(case, schedules().into_iter().nth(case).unwrap())],
        )
        .await;
    }
}

#[tokio::test]
async fn opaque_repost_repairs_a_missing_original_article() {
    run_cell(
        Cell {
            container: Container::Rar5,
            naming: Naming::HexReposted,
            binding: Binding::Par2Posted,
        },
        ExtractionProfile::DirectStore,
        vec![(242, schedules().into_iter().nth(242).unwrap())],
    )
    .await;
}

#[tokio::test]
async fn opaque_repost_recovers_without_parity_when_an_identical_copy_survives() {
    for (container, profile) in [
        (Container::Rar5, ExtractionProfile::DirectStore),
        (Container::Rar4, ExtractionProfile::DirectStore),
        (Container::Rar5, ExtractionProfile::Chase),
    ] {
        run_cell(
            Cell {
                container,
                naming: Naming::HexReposted,
                binding: Binding::Par2Posted,
            },
            profile,
            vec![(342, schedules().into_iter().nth(342).unwrap())],
        )
        .await;
    }
}

#[tokio::test]
async fn reposted_rar_recovers_both_articles_of_one_copy() {
    run_cell(
        Cell {
            container: Container::Rar4,
            naming: Naming::HexReposted,
            binding: Binding::Par2Posted,
        },
        ExtractionProfile::DirectStore,
        vec![(244, schedules().into_iter().nth(244).unwrap())],
    )
    .await;
}

#[tokio::test]
async fn solid_rar_restart_keeps_late_volume_refresh() {
    run_cell(
        Cell {
            container: Container::Rar4Solid,
            naming: Naming::HexScattered,
            binding: Binding::Nothing,
        },
        ExtractionProfile::DirectStore,
        vec![(104, schedules().into_iter().nth(104).unwrap())],
    )
    .await;
}

#[tokio::test]
async fn repaired_unposted_parts_join_header_named_archives() {
    for container in [
        Container::Rar4,
        Container::Rar5EncryptedHeaders,
        Container::SevenZipSolid,
    ] {
        run_cell(
            Cell {
                container,
                naming: Naming::FirstVolumeHex,
                binding: Binding::Par2Posted,
            },
            ExtractionProfile::DirectStore,
            [357, 359]
                .into_iter()
                .map(|case| (case, schedules().into_iter().nth(case).unwrap()))
                .collect(),
        )
        .await;
    }
}

#[tokio::test]
async fn solid_sevenz_uses_complete_reposted_part_after_article_loss() {
    run_cell(
        Cell {
            container: Container::SevenZipSolid,
            naming: Naming::Reposted,
            binding: Binding::Nothing,
        },
        ExtractionProfile::DirectStore,
        vec![(240, schedules().into_iter().nth(240).unwrap())],
    )
    .await;
}

#[tokio::test]
async fn repaired_solid_sevenz_preserves_missing_part_diagnostic() {
    run_cell(
        Cell {
            container: Container::SevenZipSolid,
            naming: Naming::HexNumberedGap,
            binding: Binding::Par2RealFirst,
        },
        ExtractionProfile::DirectStore,
        vec![(242, schedules().into_iter().nth(242).unwrap())],
    )
    .await;
}

#[tokio::test]
async fn repaired_misnumbered_sevenz_updates_topology() {
    run_cell(
        Cell {
            container: Container::SevenZip,
            naming: Naming::HexMisnumbered,
            binding: Binding::Par2RealLast,
        },
        ExtractionProfile::DirectStore,
        vec![(262, schedules().into_iter().nth(262).unwrap())],
    )
    .await;
}

#[tokio::test]
async fn first_volume_hex_restart_keeps_durable_articles() {
    run_cell(
        Cell {
            container: Container::Rar5,
            naming: Naming::FirstVolumeHex,
            binding: Binding::Nothing,
        },
        ExtractionProfile::DirectStore,
        vec![(100, schedules().into_iter().nth(100).unwrap())],
    )
    .await;
}

#[tokio::test]
async fn encrypted_first_hex_demotion_keeps_conventional_ownership() {
    run_cell(
        Cell {
            container: Container::Rar5Encrypted,
            naming: Naming::FirstVolumeHex,
            binding: Binding::Par2RealLast,
        },
        ExtractionProfile::DirectStore,
        vec![(119, schedules().into_iter().nth(119).unwrap())],
    )
    .await;
}

#[tokio::test]
async fn repaired_scattered_rar_publishes_only_members() {
    run_cell(
        Cell {
            container: Container::Rar4,
            naming: Naming::HexScattered,
            binding: Binding::Par2RealFirst,
        },
        ExtractionProfile::DirectStore,
        vec![(254, schedules().into_iter().nth(254).unwrap())],
    )
    .await;
}

#[tokio::test]
async fn first_volume_hex_sevenz_restores_part_indices() {
    run_cell(
        Cell {
            container: Container::SevenZipSolid,
            naming: Naming::FirstVolumeHex,
            binding: Binding::Nothing,
        },
        ExtractionProfile::DirectStore,
        vec![(98, schedules().into_iter().nth(98).unwrap())],
    )
    .await;
}

#[tokio::test]
async fn misnumbered_sevenz_reports_its_unordered_parts() {
    run_cell(
        Cell {
            container: Container::SevenZip,
            naming: Naming::HexMisnumbered,
            binding: Binding::Nothing,
        },
        ExtractionProfile::DirectStore,
        vec![(0, schedules().into_iter().next().unwrap())],
    )
    .await;
}

#[tokio::test]
async fn repaired_old_style_rar_retains_direct_ownership() {
    run_cell(
        Cell {
            container: Container::Rar4,
            naming: Naming::OldStyleS,
            binding: Binding::Par2RealFirst,
        },
        ExtractionProfile::DirectStore,
        vec![(248, schedules().into_iter().nth(248).unwrap())],
    )
    .await;
}

#[tokio::test]
async fn all_lost_old_style_rar_can_recover_conventionally() {
    run_cell(
        Cell {
            container: Container::Rar4,
            naming: Naming::OldStyleS,
            binding: Binding::Par2RealFirst,
        },
        ExtractionProfile::DirectStore,
        vec![(268, schedules().into_iter().nth(268).unwrap())],
    )
    .await;
}

#[tokio::test]
async fn verified_repost_satisfies_missing_original() {
    run_cell(
        Cell {
            container: Container::Rar4,
            naming: Naming::Reposted,
            binding: Binding::Par2Posted,
        },
        ExtractionProfile::DirectStore,
        vec![(240, schedules().into_iter().nth(240).unwrap())],
    )
    .await;
}

#[tokio::test]
async fn rar4_late_par2_recovers_lost_first_article() {
    run_cell(
        Cell {
            container: Container::Rar4,
            naming: Naming::HexNumberedGap,
            binding: Binding::Par2RealLast,
        },
        ExtractionProfile::DirectStore,
        vec![(
            240,
            (
                slot_arrivals(4),
                Interruption::Loss {
                    mask: 1,
                    index_first: false,
                },
            ),
        )],
    )
    .await;
}

#[tokio::test]
async fn sfv_cannot_repair_an_unavailable_first_article() {
    for (container, naming) in [
        (Container::SevenZip, Naming::HexScattered),
        (Container::SevenZip, Naming::HexMisnumbered),
        (Container::Rar4, Naming::HexNumberedGap),
    ] {
        for profile in PROFILES {
            run_cell(
                Cell {
                    container,
                    naming,
                    binding: Binding::Sfv,
                },
                profile,
                vec![(
                    0,
                    (
                        slot_arrivals(4),
                        Interruption::Loss {
                            mask: 1,
                            index_first: false,
                        },
                    ),
                )],
            )
            .await;
        }
    }
}

#[tokio::test]
async fn solid_bare_first_part_keeps_repair_and_restart_fallbacks() {
    for binding in [Binding::Par2RealFirst, Binding::Par2RealLast] {
        for profile in PROFILES {
            run_cell(
                Cell {
                    container: Container::SevenZipSolid,
                    naming: Naming::BareFirstPart,
                    binding,
                },
                profile,
                vec![
                    (
                        240,
                        (
                            slot_arrivals(4),
                            Interruption::Loss {
                                mask: 1,
                                index_first: false,
                            },
                        ),
                    ),
                    (0, (slot_arrivals(4), Interruption::Restart(2))),
                ],
            )
            .await;
        }
    }
}

#[tokio::test]
async fn reposted_rar4_restores_both_volume_identities() {
    run_cell(
        Cell {
            container: Container::Rar4,
            naming: Naming::HexReposted,
            binding: Binding::Nothing,
        },
        ExtractionProfile::Conventional,
        vec![(
            104,
            (
                vec![(0, 0), (0, 1), (1, 1), (1, 0)],
                Interruption::Restart(2),
            ),
        )],
    )
    .await;
}

#[tokio::test]
async fn retained_index_can_admit_misnumbered_rar_after_restart() {
    run_cell(
        Cell {
            container: Container::Rar4,
            naming: Naming::HexMisnumbered,
            binding: Binding::Par2RealLast,
        },
        ExtractionProfile::DirectStore,
        vec![
            (
                110,
                (
                    vec![(0, 0), (1, 0), (0, 1), (1, 1)],
                    Interruption::Restart(2),
                ),
            ),
            (
                277,
                (vec![(0, 0), (1, 0), (0, 1), (1, 1)], Interruption::Crash(2)),
            ),
        ],
    )
    .await;
}

#[tokio::test]
async fn renamed_par2_sources_repair_missing_identity_prefixes() {
    for (container, naming, binding, mask) in [
        (Container::Rar4, Naming::OldStyleS, Binding::Par2RealLast, 1),
        (
            Container::SevenZipSolid,
            Naming::HexBare,
            Binding::Par2RealFirst,
            2,
        ),
        (
            Container::SevenZipSolid,
            Naming::HexMisnumbered,
            Binding::Par2RealFirst,
            2,
        ),
        (
            Container::SevenZipSolid,
            Naming::HexBare,
            Binding::Par2RealFirst,
            8,
        ),
        (
            Container::SevenZipSolid,
            Naming::HexMisnumbered,
            Binding::Par2RealFirst,
            8,
        ),
    ] {
        run_cell(
            Cell {
                container,
                naming,
                binding,
            },
            ExtractionProfile::DirectStore,
            vec![(
                0,
                (
                    slot_arrivals(4),
                    Interruption::Loss {
                        mask,
                        index_first: false,
                    },
                ),
            )],
        )
        .await;
    }
}

const PROFILES: [ExtractionProfile; 3] = [
    ExtractionProfile::DirectStore,
    ExtractionProfile::Chase,
    ExtractionProfile::Conventional,
];

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
    Naming::OldStyleS,
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

// The bindings the grouping cross varies. The two legacy bindings belong to
// the hand-named formats, whose own campaigns run them.
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
                if matches!(rule(cell), Ruling::Possible(_)) {
                    cells.push(cell);
                }
            }
        }
    }
    cells
}

// Emits one ignored campaign test per listed cell under
// `archive_schedules::combined_grouping`, inside the archive matrix's
// filter, and one default-suite smoke per listed cell of a `smoke` container
// under `archive_schedules::grouping_smoke`, outside it.
macro_rules! grouping_cells {
    ($($tag:ident $c:ident $C:ident {
        $($n:ident $N:ident [$($b:ident $B:ident)*])*
    })*) => {
        // Every cell the generator emits a campaign for.
        const GENERATED_CELLS: &[grouping::Cell] = &[$($($(grouping::Cell {
            container: grouping::Container::$C,
            naming: grouping::Naming::$N,
            binding: grouping::Binding::$B,
        },)*)*)*];
        mod combined_grouping {
            $(mod $c {
                $(mod $n {
                    use super::super::super::grouping::*;
                    $(
                        #[tokio::test]
                        #[ignore = "opt-in archive matrix; run with the archive-matrix Nextest profile and --run-ignored all"]
                        async fn $b() {
                            cell_campaign(Cell {
                                container: Container::$C,
                                naming: Naming::$N,
                                binding: Binding::$B,
                            })
                            .await;
                        }
                    )*
                })*
            })*
        }
        mod grouping_smoke {
            use super::grouping::grouping_cells;
            $(grouping_cells!(@smoke $tag $c $C { $($n $N [$($b $B)*])* });)*
        }
    };
    (@smoke smoke $c:ident $C:ident {
        $($n:ident $N:ident [$($b:ident $B:ident)*])*
    }) => {
        mod $c {
            $(mod $n {
                use super::super::super::grouping::*;
                $(
                    #[tokio::test]
                    async fn $b() {
                        cell_smoke(Cell {
                            container: Container::$C,
                            naming: Naming::$N,
                            binding: Binding::$B,
                        })
                        .await;
                    }
                )*
            })*
        }
    };
    (@smoke campaign $($rest:tt)*) => {};
}
pub(super) use grouping_cells;

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
