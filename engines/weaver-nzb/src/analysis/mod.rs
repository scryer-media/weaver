// Offline analysis of an NZB into a report that is safe to paste in public.
//
// An NZB cannot be shared where support happens, yet nearly every question
// about a failed download is a question about the NZB: how it is laid out,
// what is missing from it, how much recovery data it carries. [`NzbReport`]
// answers those questions without carrying the NZB.
//
// Redaction is a property of the types, not of a filter run afterwards. No
// field in this module can hold a subject, message-ID, poster, group, file
// name, URL or password: every string in a report is either a compile-time
// constant or a token Weaver generates (`f03`, `a1`, `p1`). Anything that
// identifies the content is dropped, counted, bucketed, or replaced by such a
// token, and the fingerprint is a one-way digest of the message-IDs.

mod classify;
mod render;

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, HashMap, HashSet};

use serde::Serialize;
use serde::ser::Serializer;
use weaver_model::files::{FileRole, archive_base_name};

use crate::parser::ParseDiagnostics;
use crate::types::{Nzb, NzbFile};

pub use self::classify::{DomainSpread, PosterTool};
use self::classify::{
    FileExt, NameShape, NameSource, StemClass, SubjectNumbers, classify_name, file_ext,
    name_mentions_password, par_base_name, parse_subject_numbers, poster_tool, stem_class,
};

// Bumped whenever a field changes meaning or disappears.
pub const REPORT_SCHEMA_VERSION: u32 = 1;

// A recovery set below this share of its protected bytes is thin anywhere.
const THIN_RECOVERY_PERMILLE: u32 = 10;
// Below this share, a post old enough to have lost articles is thin too.
const STALE_RECOVERY_PERMILLE: u32 = 50;
// Age past which articles are routinely gone from some providers.
const STALE_AGE_DAYS: u64 = 365;
// Age past which a post is near the edge of the longest retention on offer.
const RETENTION_EDGE_DAYS: u64 = 4_000;
// The anomalous-file listing is for a human reading a paste, not a dump.
const MAX_LISTED_FILES: usize = 10;

// A report about one NZB. See the module documentation for what it may hold.
#[derive(Debug, Clone, Serialize)]
pub struct NzbReport {
    pub identity: Identity,
    pub red_flags: Vec<RedFlag>,
    pub shape: Shape,
    pub layout: Layout,
    pub poster: PosterSignals,
    pub files: Vec<FileSummary>,
}

// What lets two reports of the same NZB be matched, and what produced them.
#[derive(Debug, Clone, Serialize)]
pub struct Identity {
    pub schema_version: u32,
    // BLAKE3 over the sorted, de-duplicated message-IDs, truncated to 128 bits.
    pub fingerprint: Fingerprint,
    // Filled in by whoever runs the analysis inside Weaver.
    pub weaver_version: Option<&'static str>,
}

// A one-way digest that correlates reports of the same NZB.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Fingerprint([u8; 16]);

impl Fingerprint {
    fn of_message_ids<'a>(ids: impl Iterator<Item = &'a str>) -> Self {
        let mut sorted: Vec<&str> = ids.map(str::trim).collect();
        sorted.sort_unstable();
        sorted.dedup();
        let mut hasher = blake3::Hasher::new();
        for id in sorted {
            hasher.update(id.as_bytes());
            hasher.update(b"\n");
        }
        let digest = hasher.finalize();
        let mut out = [0u8; 16];
        out.copy_from_slice(&digest.as_bytes()[..16]);
        Self(out)
    }

    pub fn to_hex(self) -> String {
        self.0.iter().map(|byte| format!("{byte:02x}")).collect()
    }
}

impl Serialize for Fingerprint {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_hex())
    }
}

// A file's stand-in in the report: `f01` is the first file in the NZB.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FileToken(u32);

impl std::fmt::Display for FileToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "f{:02}", self.0)
    }
}

impl Serialize for FileToken {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

// An archive or recovery set's stand-in: `a1`, `p1`, `q1`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SetToken {
    prefix: SetPrefix,
    number: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SetPrefix {
    Archive,
    Par2,
    Par3,
}

impl std::fmt::Display for SetToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let prefix = match self.prefix {
            SetPrefix::Archive => 'a',
            SetPrefix::Par2 => 'p',
            SetPrefix::Par3 => 'q',
        };
        write!(f, "{prefix}{}", self.number)
    }
}

impl Serialize for SetToken {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

// Counts and sizes over the whole NZB.
#[derive(Debug, Clone, Serialize)]
pub struct Shape {
    pub file_count: u32,
    // `<file>` elements with no usable segment, which a download never sees.
    pub files_without_segments: u32,
    // The highest `[NN/MM]` file total any subject declares.
    pub declared_file_count: Option<u32>,
    pub segment_count: u64,
    pub summed_bytes: u64,
    pub declared_bytes: Option<DeclaredBytes>,
    pub segment_sizes: Option<SegmentSizes>,
    pub posted: Option<PostedRange>,
    pub invalid_dates: u32,
    pub group_count: u32,
    pub poster_count: u32,
    // Files whose subject declares a segment count other than the one listed.
    pub segment_count_mismatch_files: u32,
    // Files with holes in their segment numbering, and the holes in total.
    pub files_missing_segments: u32,
    pub missing_segments: u64,
    pub malformed_segments: u64,
    // Segments a file listed twice, by number or by message-ID.
    pub duplicate_segments: u64,
    // Message-IDs that more than one file lists.
    pub shared_message_ids: u64,
    // Files that repeat an earlier file's segments outright.
    pub duplicate_files: u32,
    pub out_of_order_files: u32,
}

// Sizes declared in subjects against the segment sizes summed for them.
#[derive(Debug, Clone, Serialize)]
pub struct DeclaredBytes {
    pub files: u32,
    pub declared: u64,
    pub summed: u64,
    // Files whose segments sum to less than the subject declares.
    pub short_files: u32,
}

#[derive(Debug, Clone, Serialize)]
pub struct SegmentSizes {
    pub min: u32,
    pub median: u32,
    pub max: u32,
    // The most frequent size, and its share of all segments in permille.
    pub common: u32,
    pub common_permille: u32,
}

// Posting dates, relative to the analysis time so no timestamp is kept.
#[derive(Debug, Clone, Serialize)]
pub struct PostedRange {
    pub oldest_age_days: u64,
    pub newest_age_days: u64,
    pub span_secs: u64,
}

// How the files fit together.
#[derive(Debug, Clone, Serialize)]
pub struct Layout {
    pub payload: PayloadKind,
    pub archive_sets: Vec<ArchiveSet>,
    pub par2_sets: Vec<RecoverySet>,
    pub par3_sets: Vec<RecoverySet>,
    pub extras: Extras,
    // Archive sets whose own name ends in another archive's extension.
    pub nested_archive_sets: u32,
    pub unclassified_files: u32,
    pub obfuscation: Obfuscation,
    pub password: PasswordSignals,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PayloadKind {
    Archive,
    Direct,
    Mixed,
    Unknown,
}

impl PayloadKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Archive => "archive",
            Self::Direct => "direct",
            Self::Mixed => "mixed",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ArchiveKind {
    Rar,
    SevenZip,
    Zip,
    Tar,
    Compressed,
    Split,
}

impl ArchiveKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Rar => "rar",
            Self::SevenZip => "7z",
            Self::Zip => "zip",
            Self::Tar => "tar",
            Self::Compressed => "compressed",
            Self::Split => "split",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ArchiveSet {
    pub token: SetToken,
    pub kind: ArchiveKind,
    pub scheme: NameShape,
    pub volumes: u32,
    // Lowest and highest zero-based volume number present.
    pub first_volume: u32,
    pub last_volume: u32,
    // Volume numbers absent between zero and the highest present.
    pub missing_volumes: u32,
    // Volume numbers listed by more than one file.
    pub repeated_volumes: u32,
    pub bytes: u64,
    pub stem: StemClass,
    pub files: Vec<FileToken>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RecoverySet {
    pub token: SetToken,
    pub has_index: bool,
    pub volumes: u32,
    // Recovery blocks the volume names declare. PAR3 names carry none.
    pub declared_blocks: u32,
    pub bytes: u64,
    pub recovery_bytes: u64,
    // The files this set appears to protect, matched by name.
    pub protected_files: u32,
    pub protected_bytes: u64,
    // Recovery bytes over protected bytes, in permille. An estimate from
    // posted sizes: block size and packet overhead are not visible here.
    pub estimated_recovery_permille: Option<u32>,
    pub files: Vec<FileToken>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct Extras {
    pub nfo: u32,
    pub sfv: u32,
    pub srr: u32,
    pub srs: u32,
    pub sample: u32,
    pub nzb: u32,
    pub image: u32,
    pub text: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ObfuscationLevel {
    // Readable names in every subject.
    None,
    // Some readable names, some hashed or missing.
    Partial,
    // Names are present but hashed or random.
    HashedNames,
    // No subject carries a name; it exists only in the yEnc header.
    NamesInYencOnly,
}

impl ObfuscationLevel {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Partial => "partial",
            Self::HashedNames => "hashed-names",
            Self::NamesInYencOnly => "names-in-yenc-only",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Obfuscation {
    pub level: ObfuscationLevel,
    pub readable_files: u32,
    pub hashed_files: u32,
    pub unnamed_files: u32,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct PasswordSignals {
    pub in_meta: bool,
    pub in_name: bool,
}

// What the message-IDs and posters say about the posting tool.
#[derive(Debug, Clone, Serialize)]
pub struct PosterSignals {
    pub tool: PosterTool,
    pub message_id_domains: DomainSpread,
}

// One file, by token.
#[derive(Debug, Clone, Serialize)]
pub struct FileSummary {
    pub token: FileToken,
    pub ext: FileExt,
    pub shape: NameShape,
    pub stem: StemClass,
    pub name_source: NameSource,
    pub set: Option<SetToken>,
    pub segments: u32,
    pub declared_segments: Option<u32>,
    pub missing_segments: u32,
    pub bytes: u64,
    pub declared_bytes: Option<u64>,
    pub duplicate_segments: u32,
    pub malformed_segments: u32,
    pub out_of_order: bool,
    pub repeats: Option<FileToken>,
}

impl FileSummary {
    fn is_anomalous(&self) -> bool {
        self.missing_segments > 0
            || self
                .declared_segments
                .is_some_and(|declared| declared != self.segments)
            || self.duplicate_segments > 0
            || self.malformed_segments > 0
            || self.out_of_order
            || self.repeats.is_some()
            || self
                .declared_bytes
                .is_some_and(|declared| self.bytes < declared)
    }
}

// One finding worth reading first.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RedFlag {
    MissingVolumes {
        set: SetToken,
        missing: u32,
    },
    MissingFiles {
        declared: u32,
        listed: u32,
    },
    MissingSegments {
        files: u32,
        segments: u64,
    },
    DroppedFiles {
        files: u32,
    },
    NoRecoveryData,
    ThinRecovery {
        set: SetToken,
        permille: u32,
        oldest_age_days: Option<u64>,
    },
    HashedWithoutRecovery,
    ShortDeclaredSize {
        files: u32,
    },
    SegmentCountMismatch {
        files: u32,
    },
    DuplicateFiles {
        files: u32,
    },
    SharedMessageIds {
        count: u64,
    },
    MalformedSegments {
        count: u64,
    },
    NearRetentionEdge {
        oldest_age_days: u64,
    },
    PasswordDeclared,
}

impl NzbReport {
    // Pretty JSON, for the download button and `--json`.
    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).expect("report types always serialize")
    }

    // The compact text form, meant to be pasted inside a fenced block.
    pub fn to_text(&self) -> String {
        render::render(self)
    }

    // Record which Weaver produced this report.
    pub fn with_weaver_version(mut self, version: &'static str) -> Self {
        self.identity.weaver_version = Some(version);
        self
    }
}

// Analyze a parsed NZB. `now_epoch_secs` is the instant posting ages are
// measured from; it is an argument so the analysis stays a pure function.
pub fn analyze(nzb: &Nzb, diagnostics: &ParseDiagnostics, now_epoch_secs: u64) -> NzbReport {
    let files = analyze_files(nzb, diagnostics);
    let shape = shape(nzb, diagnostics, &files, now_epoch_secs);
    let (mut layout, set_of_file) = layout(nzb, &files);
    let mut summaries = summaries(&files, &set_of_file);
    // The listing carries a set token per file, so it is built after the sets.
    summaries.sort_by_key(|summary| summary.token);
    layout.obfuscation = obfuscation(&files);
    let poster = PosterSignals {
        tool: poster_tool(nzb),
        message_id_domains: classify::domain_spread(nzb),
    };
    let red_flags = red_flags(&shape, &layout);
    NzbReport {
        identity: Identity {
            schema_version: REPORT_SCHEMA_VERSION,
            fingerprint: Fingerprint::of_message_ids(
                nzb.files
                    .iter()
                    .flat_map(|file| &file.segments)
                    .map(|segment| segment.message_id.as_str()),
            ),
            weaver_version: None,
        },
        red_flags,
        shape,
        layout,
        poster,
        files: summaries,
    }
}

// Everything known about one file, before it is reduced to a summary. This
// borrows from the NZB and never leaves the module.
struct AnalyzedFile<'a> {
    token: FileToken,
    file: &'a NzbFile,
    name: Option<String>,
    name_source: NameSource,
    role: FileRole,
    shape: NameShape,
    ext: FileExt,
    stem: StemClass,
    numbers: SubjectNumbers,
    missing_segments: u32,
    duplicate_segments: u32,
    malformed_segments: u32,
    out_of_order: bool,
    repeats: Option<FileToken>,
}

fn analyze_files<'a>(nzb: &'a Nzb, diagnostics: &ParseDiagnostics) -> Vec<AnalyzedFile<'a>> {
    let mut seen_segment_sets: HashMap<Vec<&str>, FileToken> = HashMap::new();
    nzb.files
        .iter()
        .enumerate()
        .map(|(index, file)| {
            let token = FileToken(u32::try_from(index + 1).unwrap_or(u32::MAX));
            let notes = diagnostics.files.get(index).cloned().unwrap_or_default();
            let (name, name_source) = classify::subject_name(&file.subject);
            let role = name
                .as_deref()
                .map_or(FileRole::Unknown, FileRole::from_filename);
            let shape = name.as_deref().map_or(NameShape::None, classify_name);
            let ext = name.as_deref().map_or(FileExt::NONE, file_ext);
            let stem = name
                .as_deref()
                .map_or(StemClass::Empty, |name| stem_class(&set_stem(name, &role)));
            let numbers = parse_subject_numbers(&file.subject);

            let highest = file.segments.iter().map(|s| s.number).max().unwrap_or(0);
            let expected = numbers.segment_total.unwrap_or(0).max(highest);
            let present = u32::try_from(file.segments.len()).unwrap_or(u32::MAX);
            let missing_segments = expected.saturating_sub(present);

            let mut ids: Vec<&str> = file
                .segments
                .iter()
                .map(|segment| segment.message_id.as_str())
                .collect();
            ids.sort_unstable();
            let repeats = match seen_segment_sets.get(&ids) {
                Some(original) => Some(*original),
                None => {
                    seen_segment_sets.insert(ids, token);
                    None
                }
            };

            AnalyzedFile {
                token,
                file,
                name,
                name_source,
                role,
                shape,
                ext,
                stem,
                numbers,
                missing_segments,
                duplicate_segments: notes
                    .duplicate_segment_numbers
                    .saturating_add(notes.duplicate_message_ids),
                malformed_segments: notes.malformed_segments,
                out_of_order: notes.out_of_order,
                repeats,
            }
        })
        .collect()
}

// The part of a name that identifies its set, without volume or archive
// suffixes. Only ever classified, never reported.
fn set_stem(name: &str, role: &FileRole) -> String {
    match role {
        FileRole::Par2 { .. } | FileRole::Par3 { .. } => par_base_name(name).to_string(),
        _ => {
            let base = archive_base_name(name, role).unwrap_or_else(|| name.to_string());
            match base.rfind('.') {
                Some(dot) if dot > 0 && base.len() - dot <= 5 => base[..dot].to_string(),
                _ => base,
            }
        }
    }
}

fn shape(
    nzb: &Nzb,
    diagnostics: &ParseDiagnostics,
    files: &[AnalyzedFile<'_>],
    now_epoch_secs: u64,
) -> Shape {
    let mut sizes: Vec<u32> = nzb
        .files
        .iter()
        .flat_map(|file| file.segments.iter().map(|segment| segment.bytes))
        .collect();
    sizes.sort_unstable();
    let segment_sizes = segment_sizes(&sizes);

    let dates: Vec<u64> = nzb
        .files
        .iter()
        .map(|file| file.date)
        .filter(|date| *date > 0)
        .collect();
    let posted = match (dates.iter().min(), dates.iter().max()) {
        (Some(oldest), Some(newest)) => Some(PostedRange {
            oldest_age_days: now_epoch_secs.saturating_sub(*oldest) / 86_400,
            newest_age_days: now_epoch_secs.saturating_sub(*newest) / 86_400,
            span_secs: newest - oldest,
        }),
        _ => None,
    };

    let groups: HashSet<&str> = nzb
        .files
        .iter()
        .flat_map(|file| file.groups.iter().map(String::as_str))
        .collect();
    let posters: HashSet<&str> = nzb.files.iter().map(|file| file.poster.as_str()).collect();

    let mut declared = DeclaredBytes {
        files: 0,
        declared: 0,
        summed: 0,
        short_files: 0,
    };
    for file in files {
        if let Some(bytes) = file.numbers.declared_bytes {
            let summed = file.file.total_bytes();
            declared.files += 1;
            declared.declared = declared.declared.saturating_add(bytes);
            declared.summed = declared.summed.saturating_add(summed);
            if summed < bytes {
                declared.short_files += 1;
            }
        }
    }

    let mut owners: HashMap<&str, usize> = HashMap::new();
    for (index, file) in nzb.files.iter().enumerate() {
        for segment in &file.segments {
            owners
                .entry(segment.message_id.as_str())
                .and_modify(|owner| {
                    if *owner != index {
                        *owner = usize::MAX;
                    }
                })
                .or_insert(index);
        }
    }
    let shared_message_ids = owners
        .values()
        .filter(|owner| **owner == usize::MAX)
        .count() as u64;

    Shape {
        file_count: u32::try_from(files.len()).unwrap_or(u32::MAX),
        files_without_segments: diagnostics.files_without_segments,
        declared_file_count: files.iter().filter_map(|f| f.numbers.file_total).max(),
        segment_count: sizes.len() as u64,
        summed_bytes: nzb.files.iter().map(NzbFile::total_bytes).sum(),
        declared_bytes: (declared.files > 0).then_some(declared),
        segment_sizes,
        posted,
        invalid_dates: diagnostics.invalid_dates,
        group_count: u32::try_from(groups.len()).unwrap_or(u32::MAX),
        poster_count: u32::try_from(posters.len()).unwrap_or(u32::MAX),
        segment_count_mismatch_files: count(files, |f| {
            f.numbers
                .segment_total
                .is_some_and(|total| total as usize != f.file.segments.len())
        }),
        files_missing_segments: count(files, |f| f.missing_segments > 0),
        missing_segments: files.iter().map(|f| u64::from(f.missing_segments)).sum(),
        malformed_segments: files
            .iter()
            .map(|f| u64::from(f.malformed_segments))
            .sum::<u64>()
            + u64::from(diagnostics.malformed_segments_in_dropped_files),
        duplicate_segments: files.iter().map(|f| u64::from(f.duplicate_segments)).sum(),
        shared_message_ids,
        duplicate_files: count(files, |f| f.repeats.is_some()),
        out_of_order_files: count(files, |f| f.out_of_order),
    }
}

fn count(files: &[AnalyzedFile<'_>], predicate: impl Fn(&AnalyzedFile<'_>) -> bool) -> u32 {
    u32::try_from(files.iter().filter(|file| predicate(file)).count()).unwrap_or(u32::MAX)
}

fn segment_sizes(sorted: &[u32]) -> Option<SegmentSizes> {
    let (&min, &max) = (sorted.first()?, sorted.last()?);
    let median = sorted[sorted.len() / 2];
    let mut frequency: BTreeMap<u32, u64> = BTreeMap::new();
    for size in sorted {
        *frequency.entry(*size).or_default() += 1;
    }
    let (common, hits) = frequency
        .iter()
        .max_by(|(left_size, left), (right_size, right)| {
            left.cmp(right).then(right_size.cmp(left_size))
        })
        .map(|(size, hits)| (*size, *hits))?;
    Some(SegmentSizes {
        min,
        median,
        max,
        common,
        common_permille: permille(hits, sorted.len() as u64),
    })
}

fn permille(part: u64, whole: u64) -> u32 {
    if whole == 0 {
        return 0;
    }
    u32::try_from((u128::from(part) * 1000 / u128::from(whole)).min(u128::from(u32::MAX)))
        .unwrap_or(u32::MAX)
}

fn layout(nzb: &Nzb, files: &[AnalyzedFile<'_>]) -> (Layout, HashMap<FileToken, SetToken>) {
    let mut set_of_file = HashMap::new();
    let mut extras = Extras::default();
    let mut unclassified_files = 0u32;

    // Archive sets keyed by their base name; the key never leaves this function.
    let mut archives: Vec<(String, Vec<&AnalyzedFile<'_>>)> = Vec::new();
    let mut par2: Vec<(String, Vec<&AnalyzedFile<'_>>)> = Vec::new();
    let mut par3: Vec<(String, Vec<&AnalyzedFile<'_>>)> = Vec::new();
    let mut direct_bytes = 0u64;
    let mut archive_bytes = 0u64;

    for file in files {
        let Some(name) = file.name.as_deref() else {
            unclassified_files += 1;
            continue;
        };
        let lower = name.to_ascii_lowercase();
        if classify::is_sample_name(&lower) {
            extras.sample += 1;
        }
        match &file.role {
            FileRole::Par2 { .. } => push_group(&mut par2, par_base_name(name), file),
            FileRole::Par3 { .. } => push_group(&mut par3, par_base_name(name), file),
            FileRole::Standalone => match file.ext.as_str() {
                "nfo" => extras.nfo += 1,
                "sfv" => extras.sfv += 1,
                "srr" => extras.srr += 1,
                "srs" => extras.srs += 1,
                "nzb" => extras.nzb += 1,
                "jpg" | "jpeg" | "png" | "gif" => extras.image += 1,
                _ => extras.text += 1,
            },
            FileRole::Unknown => {
                if file.ext == FileExt::NONE || file.ext == FileExt::OTHER {
                    unclassified_files += 1;
                } else {
                    direct_bytes += file.file.total_bytes();
                }
            }
            role => {
                let base = archive_base_name(name, role).unwrap_or_else(|| name.to_string());
                archive_bytes += file.file.total_bytes();
                push_group(&mut archives, &base, file);
            }
        }
    }

    let mut nested_archive_sets = 0u32;
    let archive_sets: Vec<ArchiveSet> = archives
        .iter()
        .enumerate()
        .map(|(index, (base, members))| {
            let token = SetToken {
                prefix: SetPrefix::Archive,
                number: u32::try_from(index + 1).unwrap_or(u32::MAX),
            };
            let suffix_removed = matches!(
                members[0].role,
                FileRole::RarVolume { .. } | FileRole::SplitFile { .. }
            );
            if classify::names_inner_archive(base, suffix_removed) {
                nested_archive_sets += 1;
            }
            for member in members {
                set_of_file.insert(member.token, token);
            }
            archive_set(token, members)
        })
        .collect();

    let non_recovery: Vec<&AnalyzedFile<'_>> = files
        .iter()
        .filter(|file| !matches!(file.role, FileRole::Par2 { .. } | FileRole::Par3 { .. }))
        .collect();
    let single_recovery_set = par2.len() + par3.len() == 1;
    let par2_sets = recovery_sets(
        SetPrefix::Par2,
        &par2,
        &non_recovery,
        single_recovery_set,
        &mut set_of_file,
    );
    let par3_sets = recovery_sets(
        SetPrefix::Par3,
        &par3,
        &non_recovery,
        single_recovery_set,
        &mut set_of_file,
    );

    let payload = match (archive_bytes > 0, direct_bytes > 0) {
        (true, false) => PayloadKind::Archive,
        (false, true) => PayloadKind::Direct,
        (true, true) => PayloadKind::Mixed,
        (false, false) => PayloadKind::Unknown,
    };

    let in_name = nzb
        .meta
        .title
        .as_deref()
        .is_some_and(name_mentions_password)
        || nzb
            .files
            .iter()
            .any(|file| name_mentions_password(&file.subject));
    let in_meta =
        nzb.meta
            .password
            .as_deref()
            .is_some_and(|password| !password.trim().is_empty())
            || nzb.meta.tags.iter().any(|(key, value)| {
                key.eq_ignore_ascii_case("password") && !value.trim().is_empty()
            });

    (
        Layout {
            payload,
            archive_sets,
            par2_sets,
            par3_sets,
            extras,
            nested_archive_sets,
            unclassified_files,
            obfuscation: Obfuscation {
                level: ObfuscationLevel::None,
                readable_files: 0,
                hashed_files: 0,
                unnamed_files: 0,
            },
            password: PasswordSignals { in_meta, in_name },
        },
        set_of_file,
    )
}

fn push_group<'f, 'a>(
    groups: &mut Vec<(String, Vec<&'f AnalyzedFile<'a>>)>,
    key: &str,
    file: &'f AnalyzedFile<'a>,
) {
    let key = key.to_ascii_lowercase();
    match groups.iter_mut().find(|(existing, _)| *existing == key) {
        Some((_, members)) => members.push(file),
        None => groups.push((key, vec![file])),
    }
}

fn volume_number(file: &AnalyzedFile<'_>) -> Option<u32> {
    match (&file.role, file.shape) {
        // `.s00` continues where `.r99` stops; the role alone folds them together.
        (FileRole::RarVolume { volume_number }, NameShape::SNn) => {
            Some(volume_number.saturating_add(100))
        }
        (FileRole::RarVolume { volume_number }, _) => Some(*volume_number),
        (FileRole::SevenZipSplit { number }, _) | (FileRole::SplitFile { number }, _) => {
            Some(*number)
        }
        (FileRole::SevenZipArchive, _) => Some(0),
        _ => None,
    }
}

fn archive_set(token: SetToken, members: &[&AnalyzedFile<'_>]) -> ArchiveSet {
    let kind = match &members[0].role {
        FileRole::RarVolume { .. } => ArchiveKind::Rar,
        FileRole::SevenZipArchive | FileRole::SevenZipSplit { .. } => ArchiveKind::SevenZip,
        FileRole::ZipArchive => ArchiveKind::Zip,
        FileRole::TarArchive
        | FileRole::TarGzArchive
        | FileRole::TarBz2Archive
        | FileRole::TarXzArchive => ArchiveKind::Tar,
        FileRole::SplitFile { .. } => ArchiveKind::Split,
        _ => ArchiveKind::Compressed,
    };
    // The scheme is the one most members follow: a `.rar` first volume beside
    // `.r00` continuations is an `rNN` set, not a single archive.
    let mut schemes: BTreeMap<NameShape, u32> = BTreeMap::new();
    for member in members {
        *schemes.entry(member.shape).or_default() += 1;
    }
    let scheme = if schemes.contains_key(&NameShape::RNn) || schemes.contains_key(&NameShape::SNn) {
        NameShape::RNn
    } else {
        schemes
            .iter()
            .max_by_key(|(_, hits)| **hits)
            .map_or(NameShape::None, |(shape, _)| *shape)
    };

    let mut numbers: Vec<u32> = members.iter().filter_map(|m| volume_number(m)).collect();
    numbers.sort_unstable();
    let listed = numbers.len();
    numbers.dedup();
    let repeated_volumes = u32::try_from(listed - numbers.len()).unwrap_or(u32::MAX);
    let first_volume = numbers.first().copied().unwrap_or(0);
    let last_volume = numbers.last().copied().unwrap_or(0);
    let missing_volumes = if numbers.is_empty() {
        0
    } else {
        (last_volume + 1).saturating_sub(u32::try_from(numbers.len()).unwrap_or(u32::MAX))
    };

    ArchiveSet {
        token,
        kind,
        scheme,
        volumes: u32::try_from(members.len()).unwrap_or(u32::MAX),
        first_volume,
        last_volume,
        missing_volumes,
        repeated_volumes,
        bytes: members.iter().map(|m| m.file.total_bytes()).sum(),
        stem: members[0].stem,
        files: members.iter().map(|m| m.token).collect(),
    }
}

fn recovery_sets(
    prefix: SetPrefix,
    groups: &[(String, Vec<&AnalyzedFile<'_>>)],
    non_recovery: &[&AnalyzedFile<'_>],
    single_recovery_set: bool,
    set_of_file: &mut HashMap<FileToken, SetToken>,
) -> Vec<RecoverySet> {
    groups
        .iter()
        .enumerate()
        .map(|(index, (base, members))| {
            let token = SetToken {
                prefix,
                number: u32::try_from(index + 1).unwrap_or(u32::MAX),
            };
            for member in members {
                set_of_file.insert(member.token, token);
            }
            let is_index = |file: &AnalyzedFile<'_>| {
                matches!(
                    file.role,
                    FileRole::Par2 { is_index: true, .. } | FileRole::Par3 { is_index: true }
                )
            };
            let declared_blocks = members
                .iter()
                .map(|member| match member.role {
                    FileRole::Par2 {
                        recovery_block_count,
                        ..
                    } => recovery_block_count,
                    _ => 0,
                })
                .sum();
            let recovery_bytes: u64 = members
                .iter()
                .filter(|member| !is_index(member))
                .map(|member| member.file.total_bytes())
                .sum();

            let by_name: Vec<&&AnalyzedFile<'_>> = non_recovery
                .iter()
                .filter(|file| {
                    !base.is_empty()
                        && file
                            .name
                            .as_deref()
                            .is_some_and(|name| name.to_ascii_lowercase().starts_with(base))
                })
                .collect();
            let protected: Vec<&&AnalyzedFile<'_>> = if by_name.is_empty() && single_recovery_set {
                non_recovery.iter().collect()
            } else {
                by_name
            };
            let protected_bytes: u64 = protected.iter().map(|f| f.file.total_bytes()).sum();

            RecoverySet {
                token,
                has_index: members.iter().any(|member| is_index(member)),
                volumes: u32::try_from(members.iter().filter(|m| !is_index(m)).count())
                    .unwrap_or(u32::MAX),
                declared_blocks,
                bytes: members.iter().map(|m| m.file.total_bytes()).sum(),
                recovery_bytes,
                protected_files: u32::try_from(protected.len()).unwrap_or(u32::MAX),
                protected_bytes,
                estimated_recovery_permille: (protected_bytes > 0)
                    .then(|| permille(recovery_bytes, protected_bytes)),
                files: members.iter().map(|m| m.token).collect(),
            }
        })
        .collect()
}

fn obfuscation(files: &[AnalyzedFile<'_>]) -> Obfuscation {
    let unnamed_files = count(files, |f| f.name.is_none());
    let hashed_files = count(files, |f| f.name.is_some() && f.stem.is_hashed());
    let readable_files = count(files, |f| f.name.is_some() && !f.stem.is_hashed());
    let level = if files.is_empty() || (unnamed_files == 0 && hashed_files == 0) {
        ObfuscationLevel::None
    } else if readable_files > 0 {
        ObfuscationLevel::Partial
    } else if hashed_files > 0 {
        ObfuscationLevel::HashedNames
    } else {
        ObfuscationLevel::NamesInYencOnly
    };
    Obfuscation {
        level,
        readable_files,
        hashed_files,
        unnamed_files,
    }
}

fn summaries(
    files: &[AnalyzedFile<'_>],
    set_of_file: &HashMap<FileToken, SetToken>,
) -> Vec<FileSummary> {
    files
        .iter()
        .map(|file| FileSummary {
            token: file.token,
            ext: file.ext,
            shape: file.shape,
            stem: file.stem,
            name_source: file.name_source,
            set: set_of_file.get(&file.token).copied(),
            segments: u32::try_from(file.file.segments.len()).unwrap_or(u32::MAX),
            declared_segments: file.numbers.segment_total,
            missing_segments: file.missing_segments,
            bytes: file.file.total_bytes(),
            declared_bytes: file.numbers.declared_bytes,
            duplicate_segments: file.duplicate_segments,
            malformed_segments: file.malformed_segments,
            out_of_order: file.out_of_order,
            repeats: file.repeats,
        })
        .collect()
}

fn red_flags(shape: &Shape, layout: &Layout) -> Vec<RedFlag> {
    let mut flags = Vec::new();
    for set in &layout.archive_sets {
        if set.missing_volumes > 0 {
            flags.push(RedFlag::MissingVolumes {
                set: set.token,
                missing: set.missing_volumes,
            });
        }
    }
    if let Some(declared) = shape.declared_file_count
        && declared > shape.file_count
    {
        flags.push(RedFlag::MissingFiles {
            declared,
            listed: shape.file_count,
        });
    }
    if shape.missing_segments > 0 {
        flags.push(RedFlag::MissingSegments {
            files: shape.files_missing_segments,
            segments: shape.missing_segments,
        });
    }
    if shape.files_without_segments > 0 {
        flags.push(RedFlag::DroppedFiles {
            files: shape.files_without_segments,
        });
    }

    let oldest_age_days = shape.posted.as_ref().map(|posted| posted.oldest_age_days);
    let has_recovery = !layout.par2_sets.is_empty() || !layout.par3_sets.is_empty();
    if !has_recovery {
        flags.push(RedFlag::NoRecoveryData);
        if matches!(
            layout.obfuscation.level,
            ObfuscationLevel::HashedNames | ObfuscationLevel::NamesInYencOnly
        ) {
            flags.push(RedFlag::HashedWithoutRecovery);
        }
    }
    for set in layout.par2_sets.iter().chain(&layout.par3_sets) {
        let Some(permille) = set.estimated_recovery_permille else {
            continue;
        };
        let stale = oldest_age_days.is_some_and(|age| age > STALE_AGE_DAYS);
        if permille < THIN_RECOVERY_PERMILLE || (stale && permille < STALE_RECOVERY_PERMILLE) {
            flags.push(RedFlag::ThinRecovery {
                set: set.token,
                permille,
                oldest_age_days,
            });
        }
    }

    if let Some(declared) = &shape.declared_bytes
        && declared.short_files > 0
    {
        flags.push(RedFlag::ShortDeclaredSize {
            files: declared.short_files,
        });
    }
    if shape.segment_count_mismatch_files > 0 {
        flags.push(RedFlag::SegmentCountMismatch {
            files: shape.segment_count_mismatch_files,
        });
    }
    if shape.duplicate_files > 0 {
        flags.push(RedFlag::DuplicateFiles {
            files: shape.duplicate_files,
        });
    }
    if shape.shared_message_ids > 0 {
        flags.push(RedFlag::SharedMessageIds {
            count: shape.shared_message_ids,
        });
    }
    if shape.malformed_segments > 0 {
        flags.push(RedFlag::MalformedSegments {
            count: shape.malformed_segments,
        });
    }
    if let Some(age) = oldest_age_days
        && age > RETENTION_EDGE_DAYS
    {
        flags.push(RedFlag::NearRetentionEdge {
            oldest_age_days: age,
        });
    }
    if layout.password.in_meta || layout.password.in_name {
        flags.push(RedFlag::PasswordDeclared);
    }
    flags
}

// The files worth naming in the text form, by token.
fn anomalous_files(report: &NzbReport) -> impl Iterator<Item = &FileSummary> {
    report
        .files
        .iter()
        .filter(|file| file.is_anomalous())
        .take(MAX_LISTED_FILES)
}
