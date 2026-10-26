// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Offline analysis of a torrent into a report that is safe to paste in
// public.
//
// A .torrent file or a magnet link names its content, and a private
// tracker's announce URL carries the passkey that is the operator's account,
// yet nearly every question about a torrent that will not download is a
// question about the metainfo: how its pieces are laid out, which trackers
// it names, what the private flag turns off. [`TorrentReport`] answers those
// questions without carrying the metainfo.
//
// Redaction is a property of the types, as in the NZB report. No field can
// hold the torrent's name, a file name or path, its comment, an info hash,
// or a tracker's path, query or user: every string is a compile-time
// constant, a token Weaver generates (`f03`), a tracker or web seed's host,
// or the creating tool's name filtered to a short run of plain characters.
// The fingerprint is a one-way digest of the hash the torrent goes by, so
// two reports of one torrent match without either naming it.

mod render;

#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::net::IpAddr;

use serde::Serialize;
use serde::ser::Serializer;
use url::Url;
use weaver_model::files::{FileExt, FileRole, file_ext, is_executable_name};

use crate::core::hash_id::{Id20, InfoHashes};
use crate::core::magnet::Magnet;
use crate::core::torrent_metainfo::{
    FileDetails, TorrentMetaV1, TorrentMetaV1Info, TorrentVersion, torrent_from_bytes,
};
use crate::sha1w::{ISha256, Sha256};

// Bumped whenever a field changes meaning or disappears.
pub const REPORT_SCHEMA_VERSION: u32 = 1;

// The anomalous-file listing is for a human reading a paste, not a dump.
const MAX_LISTED_FILES: usize = 10;
// The longest path component every common filesystem takes, in bytes.
const MAX_COMPONENT_BYTES: usize = 255;
// A creation date further ahead than this is not clock drift.
const FUTURE_SLACK_SECS: u64 = 86_400;
// The smallest piece the protocol's blocks fill.
const BLOCK_BYTES: u32 = 16 * 1024;

// A report about one torrent. See the module documentation for what it may
// hold.
#[derive(Debug, Clone, Serialize)]
pub struct TorrentReport {
    pub identity: Identity,
    pub red_flags: Vec<RedFlag>,
    // What a magnet does not carry is none.
    pub shape: Option<Shape>,
    pub layout: Option<Layout>,
    pub about: About,
    pub trackers: Trackers,
    pub files: Vec<FileSummary>,
}

// What lets two reports of the same torrent be matched, and what produced
// them.
#[derive(Debug, Clone, Serialize)]
pub struct Identity {
    pub schema_version: u32,
    // A digest of the 20-byte hash the torrent goes by, truncated to 128
    // bits: the same for its metainfo and for any magnet naming it.
    pub fingerprint: Fingerprint,
    pub source: Source,
    // Which hashes verify its pieces; none from a magnet.
    pub version: Option<Version>,
    pub has_v1_hash: bool,
    pub has_v2_hash: bool,
    // Filled in by whoever runs the analysis inside Weaver.
    pub weaver_version: Option<&'static str>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Source {
    Metainfo,
    Magnet,
}

impl Source {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Metainfo => "metainfo",
            Self::Magnet => "magnet",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Version {
    V1,
    V2,
    Hybrid,
}

impl Version {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::V1 => "v1",
            Self::V2 => "v2",
            Self::Hybrid => "hybrid",
        }
    }
}

impl From<TorrentVersion> for Version {
    fn from(version: TorrentVersion) -> Self {
        match version {
            TorrentVersion::V1 => Self::V1,
            TorrentVersion::V2 => Self::V2,
            TorrentVersion::Hybrid => Self::Hybrid,
        }
    }
}

// A one-way digest that correlates reports of the same torrent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Fingerprint([u8; 16]);

impl Fingerprint {
    pub fn of_hash(id: Id20) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(b"weaver torrent report\n");
        hasher.update(&id.0);
        let digest = hasher.finish();
        let mut out = [0u8; 16];
        out.copy_from_slice(&digest[..16]);
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

// A file's stand-in in the report: `f01` is the first file the torrent
// lists, padding included.
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

// Counts and sizes over the metainfo's pieces and files.
#[derive(Debug, Clone, Serialize)]
pub struct Shape {
    pub piece_length: u32,
    pub piece_count: u32,
    pub last_piece_bytes: u64,
    pub total_bytes: u64,
    // Files that are written, padding left out.
    pub file_count: u32,
    pub padding_files: u32,
    pub padding_bytes: u64,
    pub empty_files: u32,
    pub largest: Option<FileExtent>,
    pub smallest: Option<FileExtent>,
    // The v1 piece hashes the pieces need, in bytes, and those listed.
    pub piece_hashes: Option<PieceHashes>,
    // v2 files longer than a piece, and those whose piece layer the file
    // carries; a file without one is verified only once its layer arrives
    // from a peer.
    pub piece_layers: Option<PieceLayers>,
    // Whether Weaver takes the metainfo, and why not.
    pub loads: bool,
    pub load_error: Option<&'static str>,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct FileExtent {
    pub file: FileToken,
    pub bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct PieceHashes {
    pub expected: u64,
    pub listed: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct PieceLayers {
    pub needed: u32,
    pub carried: u32,
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

    // The archive a file's name makes it part of, as the NZB report reads
    // names; recovery files and plain files are none.
    fn of(role: &FileRole) -> Option<Self> {
        Some(match role {
            FileRole::RarVolume { .. } => Self::Rar,
            FileRole::SevenZipArchive | FileRole::SevenZipSplit { .. } => Self::SevenZip,
            FileRole::ZipArchive => Self::Zip,
            FileRole::TarArchive
            | FileRole::TarGzArchive
            | FileRole::TarBz2Archive
            | FileRole::TarXzArchive => Self::Tar,
            FileRole::GzArchive
            | FileRole::DeflateArchive
            | FileRole::BrotliArchive
            | FileRole::ZstdArchive
            | FileRole::Bzip2Archive
            | FileRole::XzArchive => Self::Compressed,
            FileRole::SplitFile { .. } => Self::Split,
            FileRole::Par2 { .. }
            | FileRole::Par3 { .. }
            | FileRole::Standalone
            | FileRole::Unknown => return None,
        })
    }
}

// How the files fit together.
#[derive(Debug, Clone, Serialize)]
pub struct Layout {
    pub single_file: bool,
    pub payload: PayloadKind,
    // The deepest file, in directories below the torrent's own.
    pub max_depth: u32,
    pub archive_files: u32,
    pub recovery_files: u32,
    pub executable_files: u32,
    pub hidden_files: u32,
    pub symlinks: u32,
    pub names: NameOddities,
}

// Files whose path is odd, by kind. Each count is of files.
#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct NameOddities {
    // A `..` component.
    pub traversal: u32,
    // A path that starts at a root or a drive.
    pub absolute: u32,
    // A component holding a path separator.
    pub separator: u32,
    // A path another file has too, or has but for letter case.
    pub duplicate: u32,
    pub case_duplicate: u32,
    // A component longer than a filesystem takes.
    pub overlong: u32,
    // A component that is not UTF-8.
    pub non_utf8: u32,
    pub empty_component: u32,
}

// Where the torrent came from, as far as it says.
#[derive(Debug, Clone, Serialize)]
pub struct About {
    // From the metainfo: none from a magnet, or when it does not say.
    pub created_age_days: Option<u64>,
    pub created_in_future: bool,
    pub created_by: Option<String>,
    pub comment: Option<TextShape>,
    pub name: Option<TextShape>,
    // The metainfo names a CA its peers' certificates are signed by.
    pub ssl: bool,
    pub magnet: Option<MagnetFacts>,
}

// A piece of free text, without the text.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct TextShape {
    pub bytes: u32,
    pub utf8: bool,
    pub has_url: bool,
}

impl TextShape {
    fn of(bytes: &[u8]) -> Self {
        let lower = bytes.to_ascii_lowercase();
        Self {
            bytes: u32::try_from(bytes.len()).unwrap_or(u32::MAX),
            utf8: std::str::from_utf8(bytes).is_ok(),
            has_url: memchr::memmem::find(&lower, b"://").is_some()
                || memchr::memmem::find(&lower, b"www.").is_some(),
        }
    }
}

// What a magnet link carries beyond its hashes and trackers.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct MagnetFacts {
    pub exact_length: Option<u64>,
    pub peers: u32,
    pub selects_files: bool,
}

// The trackers and web seeds, and what the private flag leaves on.
#[derive(Debug, Clone, Serialize)]
pub struct Trackers {
    // None from a magnet: only the metainfo says.
    pub private: Option<bool>,
    pub discovery: Discovery,
    pub tiers: u32,
    pub trackers: Vec<Endpoint>,
    pub web_seeds: Vec<Endpoint>,
}

// Peer discovery past the trackers. A private torrent finds peers through
// its trackers alone (BEP 27); a magnet's privacy is known only once its
// metadata arrives, so its discovery is unknown.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct Discovery {
    pub dht: Option<bool>,
    pub pex: Option<bool>,
    pub lsd: Option<bool>,
}

impl Discovery {
    fn of(private: Option<bool>) -> Self {
        let on = private.map(|private| !private);
        Self {
            dht: on,
            pex: on,
            lsd: on,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Scheme {
    Http,
    Https,
    Udp,
    Ws,
    Wss,
    Other,
    // Not a URL at all.
    Invalid,
}

impl Scheme {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Https => "https",
            Self::Udp => "udp",
            Self::Ws => "ws",
            Self::Wss => "wss",
            Self::Other => "other",
            Self::Invalid => "invalid",
        }
    }

    // Whether Weaver announces to a tracker of this scheme.
    pub fn announces(self) -> bool {
        matches!(self, Self::Http | Self::Https | Self::Udp)
    }
}

// What a tracker's or web seed's host is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HostKind {
    // A name, resolved when announced to.
    Name,
    // An address written into the URL, reachable from anywhere.
    PublicAddress,
    // An address on a private or link-local network.
    LocalAddress,
    Loopback,
    // No host.
    None,
}

impl HostKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Name => "name",
            Self::PublicAddress => "address",
            Self::LocalAddress => "lan address",
            Self::Loopback => "loopback",
            Self::None => "no host",
        }
    }

    pub fn is_literal(self) -> bool {
        matches!(
            self,
            Self::PublicAddress | Self::LocalAddress | Self::Loopback
        )
    }
}

// A tracker or web seed by its scheme and host. The path, query and user,
// where a private tracker puts the passkey, are never read into it.
#[derive(Debug, Clone, Serialize)]
pub struct Endpoint {
    pub tier: u32,
    pub scheme: Scheme,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub host_kind: HostKind,
    // The URL carries a path, query or user past its host.
    pub has_path: bool,
}

impl Endpoint {
    // A tracker or web seed by its URL as written; `tier` is its place in
    // the announce list.
    pub fn of(tier: u32, url: &[u8]) -> Self {
        let parsed = std::str::from_utf8(url)
            .ok()
            .and_then(|url| Url::parse(url.trim()).ok());
        let Some(url) = parsed else {
            return Self {
                tier,
                scheme: Scheme::Invalid,
                host: None,
                port: None,
                host_kind: HostKind::None,
                has_path: false,
            };
        };
        let scheme = match url.scheme() {
            "http" => Scheme::Http,
            "https" => Scheme::Https,
            "udp" => Scheme::Udp,
            "ws" => Scheme::Ws,
            "wss" => Scheme::Wss,
            _ => Scheme::Other,
        };
        // A scheme the URL standard does not know, such as udp, keeps its
        // host as written, so an address is read from the host's text.
        let host_kind = match url.host_str() {
            None | Some("") => HostKind::None,
            Some(host) => host
                .trim_start_matches('[')
                .trim_end_matches(']')
                .parse::<IpAddr>()
                .map_or(HostKind::Name, address_kind),
        };
        Self {
            tier,
            scheme,
            host: url.host_str().map(|host| host.to_ascii_lowercase()),
            port: url.port(),
            host_kind,
            has_path: !matches!(url.path(), "" | "/")
                || url.query().is_some()
                || !url.username().is_empty()
                || url.password().is_some(),
        }
    }
}

fn address_kind(ip: IpAddr) -> HostKind {
    let ip = match ip {
        IpAddr::V6(v6) => v6.to_ipv4_mapped().map_or(IpAddr::V6(v6), IpAddr::V4),
        ip => ip,
    };
    match ip {
        ip if ip.is_loopback() || ip.is_unspecified() => HostKind::Loopback,
        IpAddr::V4(v4) if v4.is_private() || v4.is_link_local() => HostKind::LocalAddress,
        IpAddr::V6(v6) if v6.is_unique_local() || v6.is_unicast_link_local() => {
            HostKind::LocalAddress
        }
        _ => HostKind::PublicAddress,
    }
}

// One file, by token.
#[derive(Debug, Clone, Serialize)]
pub struct FileSummary {
    pub token: FileToken,
    pub ext: FileExt,
    pub bytes: u64,
    pub depth: u32,
    pub padding: bool,
    pub hidden: bool,
    pub symlink: bool,
    // Marked executable by its attributes, or named as one that runs.
    pub executable_attr: bool,
    pub executable_name: bool,
    pub archive: Option<ArchiveKind>,
    pub recovery: bool,
    pub traversal: bool,
    pub absolute: bool,
    pub separator: bool,
    pub duplicate: bool,
    pub case_duplicate: bool,
    pub overlong: bool,
    pub non_utf8: bool,
    pub empty_component: bool,
}

impl FileSummary {
    fn odd_name(&self) -> bool {
        self.traversal
            || self.absolute
            || self.separator
            || self.duplicate
            || self.case_duplicate
            || self.overlong
            || self.non_utf8
            || self.empty_component
    }

    fn is_anomalous(&self) -> bool {
        !self.padding && (self.odd_name() || self.executable_name || self.bytes == 0)
    }
}

// One finding worth reading first.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RedFlag {
    DoesNotLoad { reason: &'static str },
    PrivateWithoutTrackers,
    NoTrackers,
    UnusableTrackers { trackers: u32 },
    LocalTrackers { trackers: u32 },
    PieceHashMismatch { expected: u64, listed: u64 },
    OddPieceLength { piece_length: u32 },
    PieceLayersMissing { files: u32 },
    EmptyFiles { files: u32 },
    PathTraversal { files: u32 },
    AbsolutePaths { files: u32 },
    SeparatorInNames { files: u32 },
    DuplicatePaths { files: u32 },
    CaseDuplicatePaths { files: u32 },
    OverlongNames { files: u32 },
    NonUtf8Names { files: u32 },
    ExecutableFiles { files: u32 },
    SingleFileArchive { archive: ArchiveKind },
    CreatedInFuture { days: u64 },
}

impl TorrentReport {
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

// Why bytes could not be analyzed: they are not torrent metainfo, or not a
// magnet link. The message names no part of the input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnalysisError(String);

impl std::fmt::Display for AnalysisError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for AnalysisError {}

// Analyze a .torrent file. `now_epoch_secs` is the instant the creation
// date's age is measured from; it is an argument so the analysis stays a
// pure function.
pub fn analyze_metainfo(bytes: &[u8], now_epoch_secs: u64) -> Result<TorrentReport, AnalysisError> {
    let meta = torrent_from_bytes(bytes)
        .map_err(|error| AnalysisError(format!("not torrent metainfo: {}", load_reason(&error))))?;
    Ok(analyze_parsed(&meta, now_epoch_secs))
}

// Analyze a magnet link, or a bare 40-digit info hash.
pub fn analyze_magnet_link(
    link: &str,
    now_epoch_secs: u64,
) -> Result<TorrentReport, AnalysisError> {
    let magnet = Magnet::parse(link.trim())
        .map_err(|_| AnalysisError("not a magnet link with an info hash".to_owned()))?;
    Ok(analyze_magnet(&magnet, now_epoch_secs))
}

// Analyze what a magnet link carries: its hashes, trackers, web seeds and
// length. Everything only the metainfo says is reported as unknown.
pub fn analyze_magnet(magnet: &Magnet, _now_epoch_secs: u64) -> TorrentReport {
    let hashes = magnet.info_hashes();
    let trackers: Vec<Endpoint> = magnet
        .trackers
        .iter()
        .enumerate()
        .map(|(index, url)| Endpoint::of(u32::try_from(index).unwrap_or(u32::MAX), url.as_bytes()))
        .collect();
    let web_seeds = magnet
        .web_seeds
        .iter()
        .map(|url| Endpoint::of(0, url.as_bytes()))
        .collect();
    let trackers = Trackers {
        private: None,
        discovery: Discovery::of(None),
        tiers: u32::try_from(trackers.len()).unwrap_or(u32::MAX),
        trackers,
        web_seeds,
    };
    let about = About {
        created_age_days: None,
        created_in_future: false,
        created_by: None,
        comment: None,
        name: magnet
            .name
            .as_deref()
            .map(|name| TextShape::of(name.as_bytes())),
        ssl: false,
        magnet: Some(MagnetFacts {
            exact_length: magnet.exact_length,
            peers: u32::try_from(magnet.peers.len()).unwrap_or(u32::MAX),
            selects_files: magnet.get_select_only().is_some(),
        }),
    };
    let red_flags = red_flags(None, None, &trackers, &[]);
    TorrentReport {
        identity: identity(Source::Magnet, None, hashes),
        red_flags,
        shape: None,
        layout: None,
        about,
        trackers,
        files: Vec::new(),
    }
}

fn identity(source: Source, version: Option<Version>, hashes: InfoHashes) -> Identity {
    Identity {
        schema_version: REPORT_SCHEMA_VERSION,
        fingerprint: Fingerprint::of_hash(hashes.best()),
        source,
        version,
        has_v1_hash: hashes.v1.is_some(),
        has_v2_hash: hashes.v2.is_some(),
        weaver_version: None,
    }
}

// A stable reason for metainfo Weaver does not take; the error's own words
// can quote the input.
fn load_reason(error: &crate::core::Error) -> &'static str {
    use crate::core::Error;
    match error {
        Error::BadTorrentMultiFileEmpty | Error::BadTorrentNoFiles => "no_files",
        Error::BadTorrentFileNoName | Error::BadTorrentEmptyFilename => "file_without_name",
        Error::BadTorrentBothSingleAndMultiFile => "single_and_multi_file",
        Error::BadTorrentPathTraversal => "path_traversal",
        Error::BadTorrentSeparatorInName => "separator_in_name",
        Error::BadTorrentZeroLength => "zero_length",
        Error::BadTorrentDuplicateFilenames => "duplicate_paths",
        Error::InvalidPieceIndex(_) => "invalid_piece_index",
        Error::Bencode(_) | Error::InvalidBencoding => "invalid_bencoding",
        Error::V2UnknownVersion(_) => "unknown_meta_version",
        Error::V2MissingFileTree => "v2_without_file_tree",
        Error::V2MissingMetaVersion => "file_tree_without_v2",
        Error::V2InvalidPieceLength(_) | Error::InvalidPieceLength(_) => "invalid_piece_length",
        Error::MissingPieces => "missing_piece_hashes",
        Error::V2InvalidPadFile => "v2_pad_file",
        Error::V2InvalidLength | Error::InvalidFileLength => "invalid_file_length",
        Error::V2MissingPiecesRoot => "v2_missing_pieces_root",
        Error::V2InconsistentFiles => "hybrid_listings_differ",
        Error::V2InvalidPieceLayer => "invalid_piece_layer",
        Error::TooManyPieces => "too_many_pieces",
        Error::InvalidPieceHashes { .. } => "piece_hash_count",
        Error::TooManyChunks => "too_many_blocks",
    }
}

// Everything known about one file, before it is reduced to a summary.
struct AnalyzedFile {
    summary: FileSummary,
    // Only ever compared, never reported.
    path_key: Vec<u8>,
}

fn analyze_parsed<B: AsRef<[u8]> + Clone>(
    meta: &TorrentMetaV1<B>,
    now_epoch_secs: u64,
) -> TorrentReport {
    let info: &TorrentMetaV1Info<B> = &meta.info.data;
    let validated = info.clone().validate();
    let load_error = validated.as_ref().err().map(load_reason);
    let version = match &validated {
        Ok(validated) => Some(Version::from(validated.version())),
        Err(_) => Some(match (info.meta_version() >= 2, info.has_v1_listing()) {
            (false, _) => Version::V1,
            (true, false) => Version::V2,
            (true, true) => Version::Hybrid,
        }),
    };

    // The listing the torrent's pieces follow: the v2 tree laid out with
    // its pads when it has no v1 listing, else the v1 listing as written.
    let mut files: Vec<AnalyzedFile> = Vec::new();
    match (&validated, info.has_v1_listing()) {
        (Ok(validated), false) => {
            for details in validated.iter_file_details() {
                push_file(&mut files, &details);
            }
        }
        _ => {
            if let Ok(listing) = info.iter_file_details_raw(info.detect_encoding()) {
                for details in listing {
                    push_file(&mut files, &details);
                }
            }
        }
    }
    mark_duplicates(&mut files);
    let summaries: Vec<FileSummary> = files.into_iter().map(|file| file.summary).collect();

    let shape = shape(info, &summaries, validated.as_ref().ok(), meta, load_error);
    let layout = layout(info, &summaries);
    let private = info.private;

    let mut tiers: Vec<Vec<&B>> = meta
        .announce_list
        .iter()
        .filter(|tier| !tier.is_empty())
        .map(|tier| tier.iter().collect())
        .collect();
    if tiers.is_empty()
        && let Some(announce) = &meta.announce
    {
        tiers.push(vec![announce]);
    }
    let trackers: Vec<Endpoint> = tiers
        .iter()
        .enumerate()
        .flat_map(|(tier, urls)| {
            urls.iter()
                .map(move |url| Endpoint::of(u32::try_from(tier).unwrap_or(u32::MAX), url.as_ref()))
        })
        .collect();
    let web_seeds = meta
        .web_seeds(!layout.single_file)
        .iter()
        .map(|seed| Endpoint::of(0, seed.url.as_bytes()))
        .collect();
    let trackers = Trackers {
        private: Some(private),
        discovery: Discovery::of(Some(private)),
        tiers: u32::try_from(tiers.len()).unwrap_or(u32::MAX),
        trackers,
        web_seeds,
    };

    let created = meta.creation_date.map(|date| date as u64);
    let about = About {
        created_age_days: created.map(|date| now_epoch_secs.saturating_sub(date) / 86_400),
        created_in_future: created.is_some_and(|date| date > now_epoch_secs + FUTURE_SLACK_SECS),
        created_by: meta
            .created_by
            .as_ref()
            .and_then(|tool| created_by(tool.as_ref())),
        comment: meta
            .comment
            .as_ref()
            .map(|comment| TextShape::of(comment.as_ref()))
            .filter(|shape| shape.bytes > 0),
        name: info.name.as_ref().map(|name| TextShape::of(name.as_ref())),
        ssl: info.ssl_cert.is_some(),
        magnet: None,
    };
    let future_days = created
        .filter(|_| about.created_in_future)
        .map(|date| (date - now_epoch_secs) / 86_400);

    let red_flags = red_flags(Some(&shape), Some(&layout), &trackers, &summaries)
        .into_iter()
        .chain(future_days.map(|days| RedFlag::CreatedInFuture { days }))
        .collect();
    TorrentReport {
        identity: identity(Source::Metainfo, version, meta.info_hashes),
        red_flags,
        shape: Some(shape),
        layout: Some(layout),
        about,
        trackers,
        files: summaries,
    }
}

// The creating tool, kept only as a tool's name and its version (`name/1.2`
// or `name 1.2`): anything else could be a sentence, a link or a tag that
// names whoever made the torrent.
fn created_by(bytes: &[u8]) -> Option<String> {
    let text = String::from_utf8_lossy(bytes);
    let text = text.trim();
    if text.is_empty() {
        return None;
    }
    let other = || Some("other".to_owned());
    let Some((name, rest)) = text.split_once(['/', ' ']) else {
        return other();
    };
    let version = rest.split(' ').next().unwrap_or_default();
    let version_digits = version.strip_prefix(['v', 'V']).unwrap_or(version);
    let name_ok = (1..=24).contains(&name.len())
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        && name.bytes().any(|b| b.is_ascii_alphabetic());
    let version_ok = (1..=16).contains(&version_digits.len())
        && version_digits.starts_with(|c: char| c.is_ascii_digit())
        && version_digits
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'-');
    if !name_ok || !version_ok {
        return other();
    }
    Some(text[..name.len() + 1 + version.len()].to_owned())
}

fn push_file<B: AsRef<[u8]>>(files: &mut Vec<AnalyzedFile>, details: &FileDetails<'_, B>) {
    let token = FileToken(u32::try_from(files.len() + 1).unwrap_or(u32::MAX));
    let attrs = details.attrs();
    let components: Vec<&[u8]> = details.filename.iter_components_bytes().collect();
    let last = components.last().copied().unwrap_or_default();
    let last_text = String::from_utf8_lossy(last);
    let role = FileRole::from_filename(&last_text);
    let archive = ArchiveKind::of(&role);
    let absolute = components.first().is_some_and(|first| {
        first.is_empty()
            || first.starts_with(b"/")
            || first.starts_with(b"\\")
            || (first.len() >= 2 && first[0].is_ascii_alphabetic() && first[1] == b':')
    });
    let mut path_key = Vec::new();
    for component in &components {
        path_key.extend_from_slice(component);
        path_key.push(0);
    }
    let summary = FileSummary {
        token,
        ext: if last.is_empty() {
            FileExt::NONE
        } else {
            file_ext(&last_text)
        },
        bytes: details.len,
        depth: u32::try_from(components.len().saturating_sub(1)).unwrap_or(u32::MAX),
        padding: attrs.padding,
        hidden: attrs.hidden,
        symlink: attrs.symlink,
        executable_attr: attrs.executable,
        executable_name: !attrs.padding && is_executable_name(&last_text),
        archive: archive.filter(|_| !attrs.padding),
        recovery: matches!(role, FileRole::Par2 { .. } | FileRole::Par3 { .. }),
        traversal: components.iter().any(|c| *c == b".." || *c == b"."),
        absolute,
        separator: components
            .iter()
            .any(|c| memchr::memchr2(b'/', b'\\', c).is_some()),
        duplicate: false,
        case_duplicate: false,
        overlong: components.iter().any(|c| c.len() > MAX_COMPONENT_BYTES),
        non_utf8: components.iter().any(|c| std::str::from_utf8(c).is_err()),
        empty_component: components.is_empty() || components.iter().skip(1).any(|c| c.is_empty()),
    };
    files.push(AnalyzedFile { summary, path_key });
}

// Mark files whose path another file has too: exactly, or but for letter
// case, which a case-insensitive filesystem cannot hold apart. Padding files
// share paths by design and are never written, so they are left out.
fn mark_duplicates(files: &mut [AnalyzedFile]) {
    let mut exact: HashMap<Vec<u8>, usize> = HashMap::new();
    let mut folded: HashMap<Vec<u8>, usize> = HashMap::new();
    for file in files.iter().filter(|file| !file.summary.padding) {
        *exact.entry(file.path_key.clone()).or_default() += 1;
        *folded
            .entry(file.path_key.to_ascii_lowercase())
            .or_default() += 1;
    }
    for file in files.iter_mut().filter(|file| !file.summary.padding) {
        let duplicate = exact.get(&file.path_key).is_some_and(|count| *count > 1);
        file.summary.duplicate = duplicate;
        file.summary.case_duplicate = !duplicate
            && folded
                .get(&file.path_key.to_ascii_lowercase())
                .is_some_and(|count| *count > 1);
    }
}

fn shape<B: AsRef<[u8]>>(
    info: &TorrentMetaV1Info<B>,
    files: &[FileSummary],
    validated: Option<&crate::core::torrent_metainfo::ValidatedTorrentMetaV1Info<B>>,
    meta: &TorrentMetaV1<B>,
    load_error: Option<&'static str>,
) -> Shape {
    let total_bytes: u64 = files.iter().map(|file| file.bytes).sum();
    let piece_length = info.piece_length;
    let piece_count = if piece_length == 0 {
        0
    } else {
        total_bytes.div_ceil(u64::from(piece_length))
    };
    let last_piece_bytes = match (piece_count, piece_length) {
        (0, _) | (_, 0) => 0,
        (count, length) => total_bytes - (count - 1) * u64::from(length),
    };
    let written: Vec<&FileSummary> = files.iter().filter(|file| !file.padding).collect();
    let largest = written
        .iter()
        .max_by(|a, b| a.bytes.cmp(&b.bytes).then(b.token.cmp(&a.token)))
        .map(|file| FileExtent {
            file: file.token,
            bytes: file.bytes,
        });
    let smallest = written
        .iter()
        .min_by(|a, b| a.bytes.cmp(&b.bytes).then(a.token.cmp(&b.token)))
        .map(|file| FileExtent {
            file: file.token,
            bytes: file.bytes,
        });
    let piece_hashes = (info.has_v1_listing()).then(|| PieceHashes {
        expected: piece_count.saturating_mul(20),
        listed: info
            .pieces
            .as_ref()
            .map_or(0, |pieces| pieces.as_ref().len() as u64),
    });
    let piece_layers = validated
        .and_then(|validated| validated.v2_files())
        .map(|v2| {
            let needed = v2
                .iter()
                .filter(|file| !file.is_pad() && file.length > u64::from(piece_length))
                .count();
            let carried = meta
                .piece_layers
                .as_ref()
                .and_then(|layers| validated?.piece_layers(layers).ok())
                .map_or(0, |layers| {
                    layers.iter().filter(|layer| layer.is_some()).count()
                });
            PieceLayers {
                needed: u32::try_from(needed).unwrap_or(u32::MAX),
                carried: u32::try_from(carried).unwrap_or(u32::MAX),
            }
        });
    Shape {
        piece_length,
        piece_count: u32::try_from(piece_count).unwrap_or(u32::MAX),
        last_piece_bytes,
        total_bytes,
        file_count: u32::try_from(written.len()).unwrap_or(u32::MAX),
        padding_files: count(files, |file| file.padding),
        padding_bytes: files
            .iter()
            .filter(|file| file.padding)
            .map(|file| file.bytes)
            .sum(),
        empty_files: count(files, |file| !file.padding && file.bytes == 0),
        largest,
        smallest,
        piece_hashes,
        piece_layers,
        loads: load_error.is_none(),
        load_error,
    }
}

fn count(files: &[FileSummary], predicate: impl Fn(&FileSummary) -> bool) -> u32 {
    u32::try_from(files.iter().filter(|file| predicate(file)).count()).unwrap_or(u32::MAX)
}

fn layout<B: AsRef<[u8]>>(info: &TorrentMetaV1Info<B>, files: &[FileSummary]) -> Layout {
    let single_file =
        info.files.is_none() && info.file_tree.as_ref().is_none_or(|tree| tree.single_file);
    let written =
        |predicate: fn(&FileSummary) -> bool| count(files, |file| !file.padding && predicate(file));
    let archive_bytes: u64 = files
        .iter()
        .filter(|file| !file.padding && file.archive.is_some())
        .map(|file| file.bytes)
        .sum();
    let direct_bytes: u64 = files
        .iter()
        .filter(|file| {
            !file.padding
                && file.archive.is_none()
                && !file.recovery
                && file.ext != FileExt::NONE
                && file.ext != FileExt::OTHER
        })
        .map(|file| file.bytes)
        .sum();
    let payload = match (archive_bytes > 0, direct_bytes > 0) {
        (true, false) => PayloadKind::Archive,
        (false, true) => PayloadKind::Direct,
        (true, true) => PayloadKind::Mixed,
        (false, false) => PayloadKind::Unknown,
    };
    Layout {
        single_file,
        payload,
        max_depth: files.iter().map(|file| file.depth).max().unwrap_or(0),
        archive_files: written(|file| file.archive.is_some()),
        recovery_files: written(|file| file.recovery),
        executable_files: written(|file| file.executable_name),
        hidden_files: written(|file| file.hidden),
        symlinks: written(|file| file.symlink),
        names: NameOddities {
            traversal: written(|file| file.traversal),
            absolute: written(|file| file.absolute),
            separator: written(|file| file.separator),
            duplicate: written(|file| file.duplicate),
            case_duplicate: written(|file| file.case_duplicate),
            overlong: written(|file| file.overlong),
            non_utf8: written(|file| file.non_utf8),
            empty_component: written(|file| file.empty_component),
        },
    }
}

fn red_flags(
    shape: Option<&Shape>,
    layout: Option<&Layout>,
    trackers: &Trackers,
    files: &[FileSummary],
) -> Vec<RedFlag> {
    let mut flags = Vec::new();
    if let Some(reason) = shape.and_then(|shape| shape.load_error) {
        flags.push(RedFlag::DoesNotLoad { reason });
    }
    let announcing = trackers
        .trackers
        .iter()
        .filter(|tracker| tracker.scheme.announces())
        .count();
    match (trackers.private, announcing) {
        (Some(true), 0) => flags.push(RedFlag::PrivateWithoutTrackers),
        (_, 0) => flags.push(RedFlag::NoTrackers),
        _ => {}
    }
    let unusable = trackers.trackers.len() - announcing;
    if unusable > 0 {
        flags.push(RedFlag::UnusableTrackers {
            trackers: u32::try_from(unusable).unwrap_or(u32::MAX),
        });
    }
    let local = trackers
        .trackers
        .iter()
        .filter(|tracker| {
            matches!(
                tracker.host_kind,
                HostKind::LocalAddress | HostKind::Loopback
            )
        })
        .count();
    if local > 0 {
        flags.push(RedFlag::LocalTrackers {
            trackers: u32::try_from(local).unwrap_or(u32::MAX),
        });
    }
    if let Some(shape) = shape {
        if let Some(hashes) = shape.piece_hashes
            && hashes.expected != hashes.listed
        {
            flags.push(RedFlag::PieceHashMismatch {
                expected: hashes.expected,
                listed: hashes.listed,
            });
        }
        if !shape.piece_length.is_power_of_two() || shape.piece_length < BLOCK_BYTES {
            flags.push(RedFlag::OddPieceLength {
                piece_length: shape.piece_length,
            });
        }
        if let Some(layers) = shape.piece_layers
            && layers.carried < layers.needed
        {
            flags.push(RedFlag::PieceLayersMissing {
                files: layers.needed - layers.carried,
            });
        }
        if shape.empty_files > 0 {
            flags.push(RedFlag::EmptyFiles {
                files: shape.empty_files,
            });
        }
    }
    if let Some(layout) = layout {
        let names = &layout.names;
        for (files, flag) in [
            (
                names.traversal,
                (|files| RedFlag::PathTraversal { files }) as fn(u32) -> RedFlag,
            ),
            (names.absolute, |files| RedFlag::AbsolutePaths { files }),
            (names.separator, |files| RedFlag::SeparatorInNames { files }),
            (names.duplicate, |files| RedFlag::DuplicatePaths { files }),
            (names.case_duplicate, |files| RedFlag::CaseDuplicatePaths {
                files,
            }),
            (names.overlong, |files| RedFlag::OverlongNames { files }),
            (names.non_utf8, |files| RedFlag::NonUtf8Names { files }),
        ] {
            if files > 0 {
                flags.push(flag(files));
            }
        }
        if layout.executable_files > 0 {
            flags.push(RedFlag::ExecutableFiles {
                files: layout.executable_files,
            });
        }
        if layout.single_file
            && let Some(archive) = files
                .iter()
                .find(|file| !file.padding)
                .and_then(|file| file.archive)
        {
            flags.push(RedFlag::SingleFileArchive { archive });
        }
    }
    flags
}

// The files worth naming in the text form, by token.
fn anomalous_files(report: &TorrentReport) -> impl Iterator<Item = &FileSummary> {
    report
        .files
        .iter()
        .filter(|file| file.is_anomalous())
        .take(MAX_LISTED_FILES)
}
