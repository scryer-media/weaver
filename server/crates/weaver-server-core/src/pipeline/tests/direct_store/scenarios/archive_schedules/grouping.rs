//! The grouping-information axis of the archive matrix.
//!
//! A cell is a container, the names its volumes are posted under, and what
//! else the post carries that could group or order them. [`rule`] states, for
//! every cell, whether a single job can present it at all and, if it can,
//! what each extraction profile must make of it: the volume set direct store
//! admits by name alone, and whether the job extracts direct, streams through
//! chase, extracts after download, demotes for a named reason, or fails.
//!
//! The ruling is an exhaustive match over the axes with no fallback arm, so a
//! value added to any axis does not compile until every cell it opens has an
//! expectation. The hand-named [`Format`](super::Format) layouts are aliases
//! of cells, and take their direct-store route from here.
//!
//! The cells reuse the matrix's synthetic stored-volume builders and the 7z
//! writer; nothing here is a captured post.
use super::super::sevenz_store::schedules::{LOSES, map_slots, unrepeated_payload};
use super::super::sevenz_store::{Entry, build_7z_shaped, sevenz_job_spec, split_volumes};
use super::*;
use crate::pipeline::direct_store::plan::{AdmissionRefusal, DirectSetPlan};

/// What the volumes are.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Container {
    Rar4,
    Rar5,
    /// Encrypted data under plain headers: the headers still chain the set.
    Rar5Encrypted,
    /// Encrypted headers: nothing in a volume is readable before the set is
    /// known, so only a name or a timely index can group it.
    Rar5EncryptedHeaders,
    SevenZip,
    SevenZipSolid,
    // Codec and integrity variants the grouping cross does not vary; they
    // exist as cells only so their hand-named formats alias to one.
    Rar4Encrypted,
    Rar4Unsalted,
    Rar5KeyedChecksum,
    Rar5UncheckedHeaders,
    QuickOpen,
    Blake2,
}

/// The names the volumes are posted under.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Naming {
    /// `silver.horizon.partNN.rar`, `silver.horizon.7z.NNN`.
    Conventional,
    /// [`Naming::Conventional`] over four volumes, so two are middle volumes.
    ConventionalFour,
    /// One volume, `silver.horizon.rar` or `silver.horizon.7z`.
    Single,
    /// The first volume's stem in a different case from the second's.
    MixedCase,
    /// `.rar` then `.s00`: the second old-style series, which an archiver
    /// starts after `.r99`.
    OldStyleS,
    /// 32-hex extensionless names with no shared stem.
    HexBare,
    /// One volume under a bare 32-hex name.
    HexSingle,
    /// A hex stem whose numeric suffixes misstate the order: `.002`, `.001`.
    HexMisnumbered,
    /// A hex stem whose `.rar` and `.r00` are swapped.
    HexSwappedRar,
    /// Four unrelated hex names that do not sort into volume order.
    HexScattered,
    /// Parts one and three of a three-part set under a hex stem, `.001` and
    /// `.003`: part two was never posted.
    HexNumberedGap,
    /// The first volume under a bare hex name, the second conventionally.
    FirstVolumeHex,
    /// Every volume posted twice under two names that number it alike.
    Reposted,
    /// Every volume posted twice, each copy under a hex name of its own.
    HexReposted,
    /// The first part as a whole `silver.horizon.7z`, the second as `.7z.002`.
    BareFirstPart,
}

/// What, besides the volumes, could group or order them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Binding {
    /// A PAR2 index carrying the real names arrives before any volume.
    Par2RealFirst,
    /// A PAR2 index carrying the real names arrives after every volume.
    Par2RealLast,
    /// A PAR2 index describes the volumes under the names they are posted
    /// under, arriving where the matrix's index does.
    Par2Posted,
    /// An `.sfv` lists the real names; no PAR2 is posted.
    Sfv,
    /// Nothing besides the volumes.
    Nothing,
    /// The matrix's own: a PAR2 index under the posted names, posted only
    /// when the schedule loses something. The hand-named formats' binding.
    LegacyOnLoss,
    /// The matrix's own obfuscated binding: a PAR2 index with the real names,
    /// ahead of the volumes unless the schedule's loss puts it last.
    LegacyReal,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Cell {
    pub container: Container,
    pub naming: Naming,
    pub binding: Binding,
}

/// What direct store's name-only discovery makes of the posted names.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Admission {
    /// One set, admitted by name.
    Named,
    /// No set admitted; this refusal named.
    Refused(AdmissionRefusal),
    /// One set admitted by name, and another under a name that differs
    /// only in case refused for this.
    Split(AdmissionRefusal),
    /// Nothing the names say forms a set.
    Unnamed,
}

/// What one extraction profile must make of a cell.
#[derive(Clone, Copy, Debug)]
pub(super) enum Expect {
    /// Direct store finalizes the set from its own partials.
    Direct,
    /// No direct set; chase streams the extraction while it downloads.
    Streams,
    /// Direct store demotes the set for a reason this accepts; the job then
    /// extracts by the fallback route.
    Demotes(fn(DemotionReason) -> bool),
    /// Extracted after download, with neither direct store nor chase.
    Downloads,
    /// The job fails with an error naming this, and publishes nothing.
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
    /// No single job can present this cell, for the stated reason.
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

/// The same expectation under every profile but direct store.
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

    /// Whether the volumes' own readable headers chain the set, so a set no
    /// name or index places is still admitted by content.
    fn headers_chain(self) -> bool {
        matches!(self, Self::Rar5 | Self::Rar5Encrypted)
    }

    /// What direct store does with a set it has admitted.
    fn admitted(self) -> Expect {
        match self {
            Self::SevenZipSolid => Expect::Demotes(sevenz_coder),
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

/// Every cell's ruling. See the module documentation.
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
        | Container::SevenZipSolid => crossed(cell),
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

/// What a job missing a volume says about it.
const MISSING_PART: &str = "missing";

/// What a job whose 7z continuation volumes nothing orders says.
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
    /// Volumes the archive is written as, and the indices of those posted,
    /// in posted order.
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

    /// Whether the posted names place the set, so a lost offset-zero article
    /// leaves no volume without a set.
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

    /// The names each posted volume goes under, given each one's real name.
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

/// One cell's posted job.
struct Fixture {
    volumes: Vec<(String, Vec<u8>)>,
    /// What each posted volume is really called, in posted order.
    real: Vec<String>,
    spec: JobSpec,
    expected: Vec<(&'static str, Vec<u8>)>,
    /// Loss masks that leave a 7z set without its map.
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
            Container::Rar5Encrypted | Container::Rar5EncryptedHeaders
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
}

impl Binding {
    /// How the schedule is driven, and the names any listing carries.
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

/// The direct-store route a cell's expectation allows, with `unmapped` the
/// loss masks that leave its map unreadable.
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

/// The direct-store route of a hand-named format's cell.
pub(super) fn alias_route(cell: Cell) -> Route {
    match rule(cell) {
        Ruling::Possible(expectation) => route(cell, expectation.direct_store, |_| false),
        Ruling::Impossible(why) => panic!("{cell:?} aliases an impossible cell: {why}"),
    }
}

impl Expect {
    /// Holds a clean schedule's outcome to the expectation.
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

/// Runs `cases` over `cell` under `profile` and holds each to the ruling.
async fn run_cell(cell: Cell, profile: ExtractionProfile, cases: Vec<(usize, Schedule)>) {
    let expectation = match rule(cell) {
        Ruling::Possible(expectation) => expectation,
        Ruling::Impossible(why) => panic!("{cell:?} is impossible: {why}"),
    };
    let fixture = Fixture::build(cell);
    assert_eq!(fixture.admission(), expectation.admission, "{cell:?}");
    let expect = expectation.of(profile);
    let route = route(cell, expect, fixture.unmapped);
    let (options, described) = cell.binding.drive(&fixture);
    let wanted = fixture.wanted();
    // A posted listing is kept beside the members, as any loose file is.
    let mut published = wanted.clone();
    if options.sfv {
        published.push("silver.horizon.sfv");
    }
    for (case, (order, interruption)) in cases {
        if !profile.includes(interruption) {
            continue;
        }
        let context = format!(
            "{cell:?} profile={profile:?} case={case} order={order:?} interruption={interruption:?}"
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
            // The schedule makes these articles unavailable on every source.
            // SFV can identify intact files but cannot repair missing bytes;
            // transport failure precedes the no-loss topology diagnostic.
            let unrepairable = options.recovery == RecoveryFormat::Absent
                && interruption.loss().is_some_and(|(mask, _)| mask != 0);
            if interruption.fails() || unrepairable {
                profile.assert_rejected(&outcome, &wanted);
                return;
            }
            if let Expect::Fails(why) = expect {
                profile.assert_rejected(&outcome, &wanted);
                let Some(JobStatus::Failed { error }) = &outcome.status else {
                    unreachable!("assert_rejected holds the job failed")
                };
                assert!(
                    error.contains(why),
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
            profile.assert_delivery(&outcome, route, &published, interruption);
            if matches!(interruption, Interruption::None) {
                expect.assert_clean(&outcome, order.len() == 4, &context);
            }
            for (name, bytes) in &fixture.expected {
                assert_eq!(
                    outcome.files[*name].as_deref(),
                    Some(bytes.as_slice()),
                    "{context}: {:?}",
                    outcome.trace
                );
            }
        };
        check();
    }
}

/// A cell's campaign: every profile over the matrix's smoke schedules.
pub(super) async fn cell_campaign(cell: Cell) {
    for profile in PROFILES {
        let cases = schedules().into_iter().enumerate().collect();
        run_cell(cell, profile, cases).await;
    }
}

/// A cell's default-suite smoke: in order and reversed, under every profile.
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

#[tokio::test]
async fn rar4_late_par2_recovers_lost_first_article() {
    run_cell(
        Cell {
            container: Container::Rar4,
            naming: Naming::HexNumberedGap,
            binding: Binding::Par2RealLast,
        },
        ExtractionProfile::DirectStore,
        vec![(240, (slot_arrivals(4), Interruption::Loss { mask: 1, index_first: false }))],
    ).await;
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
                Cell { container, naming, binding: Binding::Sfv },
                profile,
                vec![(0, (slot_arrivals(4), Interruption::Loss { mask: 1, index_first: false }))],
            ).await;
        }
    }
}

#[tokio::test]
async fn solid_bare_first_part_keeps_repair_and_restart_fallbacks() {
    for binding in [Binding::Par2RealFirst, Binding::Par2RealLast] {
        for profile in PROFILES {
            run_cell(
                Cell { container: Container::SevenZipSolid, naming: Naming::BareFirstPart, binding },
                profile,
                vec![
                    (240, (slot_arrivals(4), Interruption::Loss { mask: 1, index_first: false })),
                    (0, (slot_arrivals(4), Interruption::Restart(2))),
                ],
            ).await;
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

const PROFILES: [ExtractionProfile; 3] = [
    ExtractionProfile::DirectStore,
    ExtractionProfile::Chase,
    ExtractionProfile::Conventional,
];

pub(super) const CONTAINERS: [Container; 6] = [
    Container::Rar4,
    Container::Rar5,
    Container::Rar5Encrypted,
    Container::Rar5EncryptedHeaders,
    Container::SevenZip,
    Container::SevenZipSolid,
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

/// The bindings the grouping cross varies. The two legacy bindings belong to
/// the hand-named formats, whose own campaigns run them.
pub(super) const BINDINGS: [Binding; 5] = [
    Binding::Par2RealFirst,
    Binding::Par2RealLast,
    Binding::Par2Posted,
    Binding::Sfv,
    Binding::Nothing,
];

/// Every possible cell of the cross, in axis order.
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

/// Emits one ignored campaign test per listed cell under
/// `archive_schedules::combined_grouping`, inside the archive matrix's
/// filter, and one default-suite smoke per listed cell of a `smoke` container
/// under `archive_schedules::grouping_smoke`, outside it.
macro_rules! grouping_cells {
    ($($tag:ident $c:ident $C:ident {
        $($n:ident $N:ident [$($b:ident $B:ident)*])*
    })*) => {
        /// Every cell the generator emits a campaign for.
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

/// Prints the generator's list for the possible cells, in the macro's form.
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

/// The generator emits a campaign for exactly the possible cells.
pub(super) fn assert_generated(generated: &[Cell]) {
    assert!(
        generated == possible_cells().as_slice(),
        "the generator's list is stale; replace it with:\n{}",
        generator_listing()
    );
}
