// Reduce names, subjects and message-IDs to closed classes.
//
// Every function here reads identifying text and returns either a constant
// from a fixed list or a number. Nothing it returns borrows from its input.

use std::collections::HashMap;

use serde::Serialize;
use serde::ser::Serializer;

use crate::deobfuscate::extract_filename;
use crate::types::Nzb;

// A file extension from a fixed list, or a shape class for numbered ones.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileExt(&'static str);

impl FileExt {
    pub const NONE: Self = Self("none");
    pub const OTHER: Self = Self("other");

    pub fn as_str(self) -> &'static str {
        self.0
    }
}

impl Serialize for FileExt {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.0)
    }
}

// Extensions a report may name. Anything else is reported as `other`.
const KNOWN_EXTENSIONS: &[&str] = &[
    "rar", "par2", "par3", "7z", "zip", "tar", "gz", "tgz", "bz2", "tbz", "tbz2", "xz", "txz",
    "zst", "zstd", "br", "deflate", "nfo", "sfv", "srr", "srs", "nzb", "txt", "md5", "url", "jpg",
    "jpeg", "png", "gif", "mkv", "mp4", "m4v", "avi", "wmv", "mov", "ts", "m2ts", "vob", "iso",
    "img", "bin", "cue", "flac", "mp3", "m4a", "m4b", "aac", "ogg", "opus", "wav", "epub", "mobi",
    "azw3", "pdf", "cbz", "cbr", "srt", "ass", "sub", "idx", "sup", "exe", "msi", "dmg", "apk",
];

// Classify the extension of a name.
pub fn file_ext(name: &str) -> FileExt {
    let lower = name.to_ascii_lowercase();
    let Some((_, ext)) = lower.rsplit_once('.') else {
        return FileExt::NONE;
    };
    if ext.is_empty() {
        return FileExt::NONE;
    }
    let bytes = ext.as_bytes();
    if bytes.len() == 3 && bytes.iter().all(u8::is_ascii_digit) {
        return FileExt("NNN");
    }
    if bytes.len() == 3 && bytes[1..].iter().all(u8::is_ascii_digit) {
        match bytes[0] {
            b'r' => return FileExt("rNN"),
            b's' => return FileExt("sNN"),
            _ => {}
        }
    }
    KNOWN_EXTENSIONS
        .iter()
        .find(|known| **known == ext)
        .map_or(FileExt::OTHER, |known| FileExt(known))
}

// How a name is numbered, without the name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub enum NameShape {
    // No name could be read from the subject.
    #[serde(rename = "none")]
    None,
    // A name with no extension at all.
    #[serde(rename = "bare")]
    Bare,
    // A name with an ordinary extension.
    #[serde(rename = "plain")]
    Plain,
    #[serde(rename = "rar")]
    Rar,
    #[serde(rename = "partNN.rar")]
    PartNn,
    #[serde(rename = "rNN")]
    RNn,
    #[serde(rename = "sNN")]
    SNn,
    #[serde(rename = "7z")]
    SevenZ,
    #[serde(rename = "7z.NNN")]
    SevenZNnn,
    #[serde(rename = "NNN")]
    Nnn,
    #[serde(rename = "par2")]
    Par2Index,
    #[serde(rename = "volN+M.par2")]
    Par2Volume,
    #[serde(rename = "par3")]
    Par3Index,
    #[serde(rename = "volN+M.par3")]
    Par3Volume,
}

impl NameShape {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Bare => "bare",
            Self::Plain => "plain",
            Self::Rar => "rar",
            Self::PartNn => "partNN.rar",
            Self::RNn => "rNN",
            Self::SNn => "sNN",
            Self::SevenZ => "7z",
            Self::SevenZNnn => "7z.NNN",
            Self::Nnn => "NNN",
            Self::Par2Index => "par2",
            Self::Par2Volume => "volN+M.par2",
            Self::Par3Index => "par3",
            Self::Par3Volume => "volN+M.par3",
        }
    }
}

fn all_digits(text: &str) -> bool {
    !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit())
}

fn has_volume_suffix(stem: &str) -> bool {
    stem.rsplit_once(".vol").is_some_and(|(_, suffix)| {
        suffix
            .split_once(['+', '-'])
            .is_some_and(|(start, count)| all_digits(start) && all_digits(count))
    })
}

// Classify how a name is numbered.
pub fn classify_name(name: &str) -> NameShape {
    let lower = name.to_ascii_lowercase();
    if let Some(stem) = lower.strip_suffix(".par2") {
        return if has_volume_suffix(stem) {
            NameShape::Par2Volume
        } else {
            NameShape::Par2Index
        };
    }
    if let Some(stem) = lower.strip_suffix(".par3") {
        return if has_volume_suffix(stem) {
            NameShape::Par3Volume
        } else {
            NameShape::Par3Index
        };
    }
    if let Some(stem) = lower.strip_suffix(".rar") {
        return match stem.rsplit_once(".part") {
            Some((_, number)) if all_digits(number) => NameShape::PartNn,
            _ => NameShape::Rar,
        };
    }
    if lower.ends_with(".7z") {
        return NameShape::SevenZ;
    }
    let Some((stem, ext)) = lower.rsplit_once('.') else {
        return NameShape::Bare;
    };
    if ext.len() == 3 && all_digits(ext) {
        return if stem.ends_with(".7z") {
            NameShape::SevenZNnn
        } else {
            NameShape::Nnn
        };
    }
    if ext.len() == 3 && all_digits(&ext[1..]) {
        match ext.as_bytes()[0] {
            b'r' => return NameShape::RNn,
            b's' => return NameShape::SNn,
            _ => {}
        }
    }
    if ext.is_empty() {
        NameShape::Bare
    } else {
        NameShape::Plain
    }
}

// What the part of a name before its set or volume suffix looks like.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StemClass {
    Empty,
    // Words a person would read, with separators.
    Readable,
    // Exactly 32 hex digits, the most common hashed form.
    Hex32,
    // Another run of 16 or more hex digits.
    Hex,
    // A long unbroken run of mixed letters and digits.
    Random,
    // Digits only.
    Numeric,
}

impl StemClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Empty => "empty",
            Self::Readable => "readable",
            Self::Hex32 => "hex32",
            Self::Hex => "hex",
            Self::Random => "random",
            Self::Numeric => "numeric",
        }
    }

    // Whether the stem hides what the file is.
    pub fn is_hashed(self) -> bool {
        matches!(self, Self::Hex32 | Self::Hex | Self::Random | Self::Numeric)
    }
}

// Classify a stem.
pub fn stem_class(stem: &str) -> StemClass {
    let stem = stem.trim();
    if stem.is_empty() {
        return StemClass::Empty;
    }
    if all_digits(stem) {
        return StemClass::Numeric;
    }
    let is_hex = stem.bytes().all(|byte| byte.is_ascii_hexdigit());
    if is_hex && stem.len() == 32 {
        return StemClass::Hex32;
    }
    if is_hex && stem.len() >= 16 {
        return StemClass::Hex;
    }
    let alnum = stem.bytes().all(|byte| byte.is_ascii_alphanumeric());
    let digits = stem.bytes().filter(u8::is_ascii_digit).count();
    let upper = stem.bytes().filter(u8::is_ascii_uppercase).count();
    let lower = stem.bytes().filter(u8::is_ascii_lowercase).count();
    // A long token with no separators that mixes digits into both cases is
    // generated, not typed. Shorter or single-case words stay readable.
    if alnum && stem.len() >= 16 && digits > 0 && upper > 0 && lower > 0 {
        return StemClass::Random;
    }
    StemClass::Readable
}

// Where in a subject the file name came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NameSource {
    Quoted,
    Bracketed,
    Unquoted,
    // The subject names no file; only the yEnc header can.
    Absent,
}

impl NameSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Quoted => "quoted",
            Self::Bracketed => "bracketed",
            Self::Unquoted => "unquoted",
            Self::Absent => "absent",
        }
    }
}

// Read the file name a subject carries. Unquoted guesses count only when
// they look like a file name, so a bare random token reads as no name.
pub fn subject_name(subject: &str) -> (Option<String>, NameSource) {
    match extract_filename(subject) {
        Some((name, confidence)) if confidence >= 1.0 => (Some(name), NameSource::Quoted),
        Some((name, confidence)) if confidence >= 0.8 && !name.trim().is_empty() => {
            (Some(name), NameSource::Bracketed)
        }
        Some((name, _)) if looks_like_file_name(&name) => (Some(name), NameSource::Unquoted),
        _ => (None, NameSource::Absent),
    }
}

fn looks_like_file_name(name: &str) -> bool {
    match name.rsplit_once('.') {
        Some((stem, ext)) => {
            !stem.is_empty()
                && (1..=5).contains(&ext.len())
                && ext.bytes().all(|byte| byte.is_ascii_alphanumeric())
        }
        None => false,
    }
}

// Numbers a subject declares about its post.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SubjectNumbers {
    // `MM` from `[NN/MM]`: how many files the poster says there are.
    pub file_total: Option<u32>,
    // `N` from `yEnc (1/N)`: how many segments this file should have.
    pub segment_total: Option<u32>,
    // The size after the yEnc part marker, when the poster wrote one.
    pub declared_bytes: Option<u64>,
}

// Parse the counts out of a subject.
pub fn parse_subject_numbers(subject: &str) -> SubjectNumbers {
    let mut numbers = SubjectNumbers::default();
    let lower = subject.to_ascii_lowercase();

    if let Some(yenc) = lower.rfind("yenc") {
        let after = &subject[yenc + 4..];
        let trimmed = after.trim_start();
        if let Some(inner) = trimmed.strip_prefix('(')
            && let Some(close) = inner.find(')')
        {
            if let Some((_, total)) = parse_fraction(&inner[..close]) {
                numbers.segment_total = Some(total);
            }
            let tail = inner[close + 1..].trim_start();
            let digits: &str = &tail[..tail
                .find(|c: char| !c.is_ascii_digit())
                .unwrap_or(tail.len())];
            numbers.declared_bytes = digits.parse::<u64>().ok().filter(|bytes| *bytes > 0);
        }
    }

    // The file count is the first bracketed fraction, which posters put
    // before the name.
    let mut rest = subject;
    while let Some(open) = rest.find('[') {
        let after = &rest[open + 1..];
        let Some(close) = after.find(']') else {
            break;
        };
        if let Some((_, total)) = parse_fraction(&after[..close]) {
            numbers.file_total = Some(total);
            break;
        }
        rest = &after[close + 1..];
    }
    numbers
}

fn parse_fraction(text: &str) -> Option<(u32, u32)> {
    let (left, right) = text.trim().split_once('/')?;
    let (left, right) = (left.trim(), right.trim());
    if !all_digits(left) || !all_digits(right) {
        return None;
    }
    let total = right.parse::<u32>().ok()?;
    (total > 0).then_some((left.parse().ok()?, total))
}

// The name of a PAR2 or PAR3 set, without its volume or extension.
pub fn par_base_name(name: &str) -> &str {
    let lower = name.to_ascii_lowercase();
    let stem_len = if lower.ends_with(".par2") || lower.ends_with(".par3") {
        name.len() - 5
    } else {
        name.len()
    };
    let stem = &name[..stem_len];
    if has_volume_suffix(&stem.to_ascii_lowercase())
        && let Some(position) = stem.to_ascii_lowercase().rfind(".vol")
    {
        return &stem[..position];
    }
    stem
}

// Whether a lowercased name marks itself as a sample.
pub fn is_sample_name(lower: &str) -> bool {
    lower
        .split(|c: char| !c.is_ascii_alphanumeric())
        .any(|token| token == "sample")
}

// Whether an archive set's name, with the set's own suffix removed, still
// ends in an archive extension: an archive packed inside another.
pub fn names_inner_archive(base: &str, suffix_already_removed: bool) -> bool {
    const OUTER: &[&str] = &[
        ".tar.gz", ".tar.bz2", ".tar.xz", ".tgz", ".tbz2", ".tbz", ".txz", ".7z", ".zip", ".tar",
        ".gz", ".bz2", ".xz", ".zst", ".zstd", ".br",
    ];
    const INNER: &[&str] = &[".rar", ".zip", ".7z", ".tar", ".iso"];
    let lower = base.to_ascii_lowercase();
    let inner = if suffix_already_removed {
        lower.as_str()
    } else {
        OUTER
            .iter()
            .find_map(|outer| lower.strip_suffix(outer))
            .unwrap_or(lower.as_str())
    };
    INNER.iter().any(|ext| inner.ends_with(ext))
}

// Whether a title or subject says the post needs a password.
pub fn name_mentions_password(text: &str) -> bool {
    if let Some(open) = text.find("{{")
        && let Some(close) = text[open + 2..].find("}}")
        && close > 0
    {
        return true;
    }
    let lower = text.to_ascii_lowercase();
    lower.contains("password") || lower.contains("passwort")
}

// A guess at the posting tool, from the message-ID domain it leaves behind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum PosterTool {
    NyuuLike,
    NgpostLike,
    NewsmanglerLike,
    JbinupLike,
    PowerpostLike,
    GopoststuffLike,
    Unknown,
}

impl PosterTool {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NyuuLike => "nyuu-like",
            Self::NgpostLike => "ngpost-like",
            Self::NewsmanglerLike => "newsmangler-like",
            Self::JbinupLike => "jbinup-like",
            Self::PowerpostLike => "powerpost-like",
            Self::GopoststuffLike => "gopoststuff-like",
            Self::Unknown => "unknown",
        }
    }
}

const TOOL_MARKERS: &[(&str, PosterTool)] = &[
    ("nyuu", PosterTool::NyuuLike),
    ("ngpost", PosterTool::NgpostLike),
    ("newsmangler", PosterTool::NewsmanglerLike),
    ("jbinup", PosterTool::JbinupLike),
    ("powerpost", PosterTool::PowerpostLike),
    ("gopoststuff", PosterTool::GopoststuffLike),
];

// A share of message-IDs below this is a coincidence, not a tool's mark.
const TOOL_MARKER_PERMILLE: u64 = 900;

fn message_id_domain(message_id: &str) -> Option<String> {
    let trimmed = message_id
        .trim()
        .trim_start_matches('<')
        .trim_end_matches('>');
    let (_, domain) = trimmed.rsplit_once('@')?;
    (!domain.is_empty()).then(|| domain.to_ascii_lowercase())
}

// Guess the posting tool. Only a marker nearly every message-ID carries
// counts; anything less reads as `unknown`.
pub fn poster_tool(nzb: &Nzb) -> PosterTool {
    let mut total = 0u64;
    let mut hits: HashMap<PosterTool, u64> = HashMap::new();
    for segment in nzb.files.iter().flat_map(|file| &file.segments) {
        total += 1;
        let Some(domain) = message_id_domain(&segment.message_id) else {
            continue;
        };
        if let Some((_, tool)) = TOOL_MARKERS
            .iter()
            .find(|(marker, _)| domain.contains(marker))
        {
            *hits.entry(*tool).or_default() += 1;
        }
    }
    if total == 0 {
        return PosterTool::Unknown;
    }
    TOOL_MARKERS
        .iter()
        .map(|(_, tool)| *tool)
        .find(|tool| hits.get(tool).copied().unwrap_or(0) * 1000 >= total * TOOL_MARKER_PERMILLE)
        .unwrap_or(PosterTool::Unknown)
}

// How many distinct message-ID domains a post uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DomainSpread {
    None,
    Single,
    Few,
    // So many that each message-ID likely carries its own random domain.
    Many,
}

impl DomainSpread {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Single => "single",
            Self::Few => "few",
            Self::Many => "many",
        }
    }
}

pub fn domain_spread(nzb: &Nzb) -> DomainSpread {
    let mut domains = std::collections::HashSet::new();
    for segment in nzb.files.iter().flat_map(|file| &file.segments) {
        if let Some(domain) = message_id_domain(&segment.message_id) {
            domains.insert(domain);
        }
    }
    match domains.len() {
        0 => DomainSpread::None,
        1 => DomainSpread::Single,
        2..=8 => DomainSpread::Few,
        _ => DomainSpread::Many,
    }
}
