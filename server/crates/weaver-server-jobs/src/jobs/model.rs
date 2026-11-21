// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use serde::{Deserialize, Serialize};
use weaver_model::files::FileRole;

// Why a payload file left the delivery accounting entirely.
//
// A discard is not damage. The file's bytes are neither delivered nor missing
// from the delivery — they were never part of it, because the settlement
// established that nothing was ever going to be assembled from them and that
// nothing depended on them. Health measures what the user received against
// what the job set out to deliver, so a discard leaves both sides of that
// fraction rather than counting against it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminalDiscardKind {
    // A repost that collided with this job's message ids. Every article the
    // servers hold under them belongs to a different, coherent file, so the
    // file this NZB declared could not have arrived from any server.
    UnfetchableDuplicate,
    // A posted file whose bytes the recovery set already folded into an output
    // the verdict vouched for: a split part a join consumed, or the damaged
    // copy of a file the repair rebuilt. A spent input, not an outstanding
    // file.
    RepairLeftover,
    // Recovery capacity the job never needed. Recovery volumes have never
    // counted toward health, so nothing has to move them out of an accounting
    // they were never in; the variant exists so a surface that reports
    // discards can name one when it has to.
    UnneededRecoveryVolume,
}

impl TerminalDiscardKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::UnfetchableDuplicate => "unfetchable_duplicate",
            Self::RepairLeftover => "repair_leftover",
            Self::UnneededRecoveryVolume => "unneeded_recovery_volume",
        }
    }
}

// One file the settlement dropped, and what dropping it cost.
//
// Structured on purpose: this is the record a UI renders as "discarded
// unfetchable duplicate, 1.09 GB", and a sentence assembled here would only
// have to be taken apart again by whoever displays it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TerminalDiscard {
    // The NZB file index, which is the file's identity for this job.
    pub file_index: u32,
    pub filename: String,
    pub kind: TerminalDiscardKind,
    // Declared bytes that left both the health denominator and the failed-byte
    // ledger.
    pub bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ArchivePasswordSource {
    Explicit,
    // A password a calling integration handed over beside the job's own one,
    // such as GraphQL submit's `passwordCandidates` list.
    Integration,
    // An `X-DNZB-Password` response header weaver read on its own NZB fetch.
    DnzbHeader,
    NzbMeta,
    FilenameConvention,
    Settings,
    PasswordFile,
    // A password the tracker rule a torrent is under holds. Only the torrent
    // service makes one, and only for that rule's torrents; no label reads
    // back as one, so no job row can carry it.
    Tracker,
}

impl ArchivePasswordSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Explicit => "explicit",
            Self::Integration => "integration",
            Self::DnzbHeader => "dnzb_header",
            Self::NzbMeta => "nzb_meta",
            Self::FilenameConvention => "filename_convention",
            Self::Settings => "settings",
            Self::PasswordFile => "password_file",
            Self::Tracker => "tracker",
        }
    }

    pub fn from_label(label: &str) -> Option<Self> {
        [
            Self::Explicit,
            Self::Integration,
            Self::DnzbHeader,
            Self::NzbMeta,
            Self::FilenameConvention,
            Self::Settings,
            Self::PasswordFile,
        ]
        .into_iter()
        .find(|source| source.as_str() == label)
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct ArchivePasswordCandidate {
    source: ArchivePasswordSource,
    value: String,
}

impl ArchivePasswordCandidate {
    pub fn new(source: ArchivePasswordSource, value: String) -> Self {
        Self { source, value }
    }

    pub fn source(&self) -> ArchivePasswordSource {
        self.source
    }

    pub fn value(&self) -> &str {
        &self.value
    }
}

impl std::fmt::Debug for ArchivePasswordCandidate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArchivePasswordCandidate")
            .field("source", &self.source)
            .field("value", &"[REDACTED]")
            .finish()
    }
}

// A job definition submitted to the scheduler.
// Contains everything needed to create and run a download job.
#[derive(Clone)]
pub struct JobSpec {
    // Human-readable name for the job.
    pub name: String,
    // Password for encrypted archives (from NZB meta or filename {{password}} convention).
    pub password: Option<String>,
    // Files to download, each with their segments.
    pub files: Vec<FileSpec>,
    // Total expected bytes across all files.
    pub total_bytes: u64,
    // Optional category (e.g. "tv", "movies") for organizing downloads.
    pub category: Option<String>,
    // Arbitrary key-value metadata attached by the submitting client.
    pub metadata: Vec<(String, String)>,
}

impl JobSpec {
    // The declared bytes a run starts out to bring: every file but the
    // recovery volumes, which a run parks unless a loss asks for them.
    pub fn wanted_bytes(&self) -> u64 {
        self.files
            .iter()
            .filter(|file| !file.role.is_recovery())
            .map(|file| FileSummary::of(file).bytes)
            .sum()
    }

    // Whether a file's article list was let go: a restart keeps only the
    // files of a job it restores, and a job that runs reads them back on
    // install.
    pub fn lacks_article_lists(&self) -> bool {
        self.files.iter().any(|file| file.segments.is_empty())
    }

    // Takes every file's article list out of the spec, leaving what a job
    // waiting its turn keeps, and returns what the lists held, with none of
    // their articles in hand.
    pub fn drop_article_lists(&mut self) -> FileSummaries {
        self.drop_article_lists_with_in_hand(|_, _| false)
    }

    // As `drop_article_lists`, counting as still to fetch only the articles
    // `in_hand` does not hold, by file index and ordinal.
    pub fn drop_article_lists_with_in_hand(
        &mut self,
        in_hand: impl Fn(u32, u32) -> bool,
    ) -> FileSummaries {
        FileSummaries(
            self.files
                .iter_mut()
                .zip(0u32..)
                .map(|(file, file_index)| {
                    let summary = FileSummary::left(file, |ordinal| in_hand(file_index, ordinal));
                    file.segments = Vec::new();
                    summary
                })
                .collect(),
        )
    }

    // Total bytes of PAR2 recovery files in this spec.
    pub fn par2_bytes(&self) -> u64 {
        self.files
            .iter()
            .filter(|file| matches!(file.role, FileRole::Par2 { .. }))
            .flat_map(|file| file.segments.iter())
            .map(|segment| segment.bytes as u64)
            .sum()
    }

    // Number of PAR2 recovery volumes (excludes the tiny always-fetched
    // index files).
    pub fn par2_volume_count(&self) -> usize {
        self.files
            .iter()
            .filter(|file| {
                matches!(
                    file.role,
                    FileRole::Par2 {
                        is_index: false,
                        ..
                    }
                )
            })
            .count()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum FieldUpdate<T> {
    #[default]
    Unchanged,
    Clear,
    Set(T),
}

impl<T> FieldUpdate<T> {
    pub fn is_unchanged(&self) -> bool {
        matches!(self, Self::Unchanged)
    }
}

// The metadata key a job's priority is kept under.
pub const PRIORITY_ATTRIBUTE_KEY: &str = "priority";

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct JobUpdate {
    pub category: FieldUpdate<String>,
    pub metadata: FieldUpdate<Vec<(String, String)>>,
    // Ordered per-key edits preserve unrelated script and client updates.
    // A missing value removes the key; matching is ASCII case-insensitive.
    pub metadata_edits: Vec<(String, Option<String>)>,
    // Archive/unpack password override. `Clear` removes a password even when
    // the NZB itself carries one; the override persists across restarts.
    pub password: FieldUpdate<String>,
    // A new job name. It is kept in the metadata so a restore, which parses
    // the NZB again, keeps it too.
    pub name: Option<String>,
}

impl JobUpdate {
    pub fn is_empty(&self) -> bool {
        self.category.is_unchanged()
            && self.metadata.is_unchanged()
            && self.metadata_edits.is_empty()
            && self.password.is_unchanged()
            && self.name.is_none()
    }

    pub fn apply_to_spec(&self, spec: &mut JobSpec) {
        match &self.category {
            FieldUpdate::Unchanged => {}
            FieldUpdate::Clear => spec.category = None,
            FieldUpdate::Set(category) => spec.category = Some(category.clone()),
        }

        self.apply_to_metadata(&mut spec.metadata);

        match &self.password {
            FieldUpdate::Unchanged => {}
            FieldUpdate::Clear => spec.password = None,
            FieldUpdate::Set(password) => spec.password = Some(password.clone()),
        }

        if let Some(name) = &self.name {
            spec.name = name.clone();
            weaver_server_base::ingest::metadata::set_renamed_title(&mut spec.metadata, name);
        }
    }

    pub fn apply_to_metadata(&self, metadata: &mut Vec<(String, String)>) {
        match &self.metadata {
            FieldUpdate::Unchanged => {}
            FieldUpdate::Clear => metadata.clear(),
            FieldUpdate::Set(values) => *metadata = values.clone(),
        }
        for (key, value) in &self.metadata_edits {
            metadata.retain(|(existing, _)| !existing.eq_ignore_ascii_case(key));
            if let Some(value) = value {
                metadata.push((key.clone(), value.clone()));
            }
        }
    }
}

// Specification for a single file within a job.
#[derive(Clone)]
pub struct FileSpec {
    // Filename (extracted from NZB subject).
    pub filename: String,
    // Inferred file role.
    pub role: FileRole,
    // Newsgroups to try.
    pub groups: Vec<String>,
    // Unix epoch seconds from the NZB `<file date="…">` attribute; `None`
    // when absent or unparseable. Drives per-server retention skipping.
    pub posted_at_epoch: Option<u64>,
    // Segments (articles) that make up this file.
    pub segments: Vec<SegmentSpec>,
}

impl FileSpec {
    // The segment with `ordinal`. Ingest gives each segment its index as its
    // ordinal, so the index is tried first, then a binary search over the
    // sorted list, then a scan for a list that is not sorted.
    pub fn segment(&self, ordinal: u32) -> Option<&SegmentSpec> {
        segment_position(&self.segments, ordinal).map(|index| &self.segments[index])
    }

    pub fn segment_mut(&mut self, ordinal: u32) -> Option<&mut SegmentSpec> {
        segment_position(&self.segments, ordinal).map(|index| &mut self.segments[index])
    }
}

fn segment_position(segments: &[SegmentSpec], ordinal: u32) -> Option<usize> {
    if segments
        .get(ordinal as usize)
        .is_some_and(|segment| segment.ordinal == ordinal)
    {
        return Some(ordinal as usize);
    }
    if let Ok(index) = segments.binary_search_by_key(&ordinal, |segment| segment.ordinal) {
        return Some(index);
    }
    segments
        .iter()
        .position(|segment| segment.ordinal == ordinal)
}

// Specification for a single segment to download.
#[derive(Clone)]
pub struct SegmentSpec {
    // Dense 0-indexed position used for all internal indexing.
    pub ordinal: u32,
    // Raw 1-indexed NZB article number preserved for diagnostics.
    pub article_number: u32,
    // Expected decoded size in bytes.
    pub bytes: u32,
    // Every NNTP message-id the NZB gave this part.
    pub message_ids: MessageIds,
}

// The message-ids (without angle brackets) the NZB gave one part, in NZB
// order, and the one the article is fetched by. The article moves to the next
// id only when the one in use is spent; a part listed once holds one id.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MessageIds {
    ids: Vec<String>,
    in_use: usize,
}

impl MessageIds {
    // `ids` must not be empty.
    pub fn new(ids: Vec<String>) -> Self {
        assert!(!ids.is_empty(), "a part has at least one message-id");
        Self { ids, in_use: 0 }
    }

    // The id the article is fetched by.
    pub fn in_use(&self) -> &str {
        &self.ids[self.in_use]
    }

    // Every id, in NZB order.
    pub fn all(&self) -> &[String] {
        &self.ids
    }

    // Whether an id remains behind the one in use.
    pub fn has_next(&self) -> bool {
        self.in_use + 1 < self.ids.len()
    }

    // Spend the id in use: move to the next one, or answer false when every
    // id is spent.
    pub fn spend(&mut self) -> bool {
        let next = self.has_next();
        self.in_use += usize::from(next);
        next
    }
}

impl From<&str> for MessageIds {
    fn from(id: &str) -> Self {
        Self::new(vec![id.to_string()])
    }
}

// One file of a job as a job waiting its turn knows it: how many articles
// it has, what they add up to, and how many of them are still to fetch.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FileSummary {
    pub articles: u32,
    pub bytes: u64,
    pub remaining: u32,
}

impl FileSummary {
    // A file with none of its articles in hand.
    pub fn of(file: &FileSpec) -> Self {
        Self::left(file, |_| false)
    }

    // A file holding in hand the articles `in_hand` names by ordinal.
    pub fn left(file: &FileSpec, in_hand: impl Fn(u32) -> bool) -> Self {
        Self {
            articles: file.segments.len() as u32,
            bytes: file
                .segments
                .iter()
                .map(|segment| u64::from(segment.bytes))
                .sum(),
            remaining: file
                .segments
                .iter()
                .filter(|segment| !in_hand(segment.ordinal))
                .count() as u32,
        }
    }

    // Whether two summaries describe the same article list, whatever of it
    // is in hand.
    pub fn same_list(&self, other: &Self) -> bool {
        self.articles == other.articles && self.bytes == other.bytes
    }
}

// The summary of every file of a job, in the spec's file order.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FileSummaries(pub Vec<FileSummary>);

impl FileSummaries {
    // As `JobSpec::par2_bytes`, for the spec whose lists these summarise.
    pub fn par2_bytes(&self, spec: &JobSpec) -> u64 {
        spec.files
            .iter()
            .zip(&self.0)
            .filter(|(file, _)| matches!(file.role, FileRole::Par2 { .. }))
            .map(|(_, summary)| summary.bytes)
            .sum()
    }

    // As `JobSpec::wanted_bytes`, for the spec whose lists these summarise.
    pub fn wanted_bytes(&self, spec: &JobSpec) -> u64 {
        spec.files
            .iter()
            .zip(&self.0)
            .filter(|(file, _)| !file.role.is_recovery())
            .map(|(_, summary)| summary.bytes)
            .sum()
    }
}

// The protocols, from one list: the enum, `ALL` in the same order, and the
// exhaustive `index` every per-protocol entry is read through. A variant is
// added to the list and nowhere else; `names` then fails to compile until
// its row is written.
macro_rules! protocols {
    ($(#[$meta:meta])* pub enum Protocol { $($(#[$attr:meta])* $variant:ident => $index:literal),+ $(,)? }) => {
        $(#[$meta])*
        pub enum Protocol {
            $($(#[$attr])* $variant),+
        }

        impl Protocol {
            /// Every one, in the order their entries are indexed.
            pub const ALL: &'static [Self] = &[$(Self::$variant),+];
            pub const COUNT: usize = Self::ALL.len();

            /// Where the protocol's entry sits in every per-protocol table.
            pub const fn index(self) -> usize {
                match self {
                    $(Self::$variant => $index),+
                }
            }
        }
    };
}

protocols! {
    // What a job downloads over: its class of job.
    #[derive(
        Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
    )]
    pub enum Protocol {
        #[default]
        Nzb => 0,
        Torrent => 1,
    }
}

// Every protocol's index is its place in `ALL`, and `ALL` holds each index
// once: an index written out of order, twice or past the end fails the build.
const _: () = {
    let mut at = 0;
    while at < Protocol::COUNT {
        assert!(Protocol::ALL[at].index() == at);
        at += 1;
    }
};

/// Every name a class of job goes by, and what an operator's hold over it
/// does: one row per protocol, read in place of a match at each caller.
#[derive(Debug)]
pub struct ClassNames {
    /// Lower case, stable: stored values, schedule actions, GraphQL.
    pub key: &'static str,
    /// What runs the class's jobs, as a refusal names it while it is not
    /// running.
    pub runner: &'static str,
    /// The GraphQL error code of an add the class cannot take because it is
    /// not running or is turned off.
    pub disabled_code: &'static str,
    /// The operator's hold over the class alone, when it has one.
    pub hold: Option<ClassHold>,
}

/// An operator's hold over one class, and where it is kept.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClassHold {
    /// What the hold stops of the class's jobs.
    pub scope: HoldScope,
    /// The setting it is kept in across a restart.
    pub setting: &'static str,
}

const NZB_CLASS: ClassNames = ClassNames {
    key: "nzb",
    runner: "download pipeline",
    disabled_code: "NZB_DISABLED",
    hold: None,
};

const TORRENT_CLASS: ClassNames = ClassNames {
    key: "torrent",
    runner: "torrent client",
    disabled_code: "TORRENTS_DISABLED",
    // The operator's pause of the torrents stops them whole, seeding too.
    hold: Some(ClassHold {
        scope: HoldScope::Everything,
        setting: "torrent_paused",
    }),
};

impl Protocol {
    pub const fn names(self) -> &'static ClassNames {
        match self {
            Self::Nzb => &NZB_CLASS,
            Self::Torrent => &TORRENT_CLASS,
        }
    }

    pub const fn as_str(self) -> &'static str {
        self.names().key
    }

    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL
            .iter()
            .copied()
            .find(|protocol| protocol.as_str() == value)
    }

    /// The class a classified payload belongs to; `None` for a URL whose
    /// bytes are not fetched yet.
    pub const fn of_kind(kind: &Kind) -> Option<Self> {
        match kind {
            Kind::Nzb => Some(Self::Nzb),
            Kind::Metainfo | Kind::Magnet(_) => Some(Self::Torrent),
            Kind::Url(_) => None,
        }
    }
}

/// A class an operator can hold apart from the rest: only a protocol whose
/// row has a hold makes one, so no hold can name a class it would not stop.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HeldClass(Protocol);

impl HeldClass {
    /// The held class of `protocol`, when its row has a hold.
    pub const fn of(protocol: Protocol) -> Option<Self> {
        match protocol.names().hold {
            Some(_) => Some(Self(protocol)),
            None => None,
        }
    }

    /// The held class of `protocol`, for a constant: fails the build when
    /// its row has no hold.
    pub const fn must(protocol: Protocol) -> Self {
        match Self::of(protocol) {
            Some(class) => class,
            None => panic!("the protocol has no hold of its own"),
        }
    }

    pub const fn protocol(self) -> Protocol {
        self.0
    }

    pub const fn hold(self) -> &'static ClassHold {
        match &self.0.names().hold {
            Some(hold) => hold,
            None => unreachable!(),
        }
    }

    /// Every class an operator can hold apart, in the table's order.
    pub fn all() -> impl Iterator<Item = Self> {
        Protocol::ALL.iter().copied().filter_map(Self::of)
    }
}

// A held class is stored and sent by its protocol's key; a key that names no
// class, or a class with no hold, is refused.
impl Serialize for HeldClass {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.0.as_str())
    }
}

impl<'de> Deserialize<'de> for HeldClass {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let key = String::deserialize(deserializer)?;
        let protocol = Protocol::parse(&key)
            .ok_or_else(|| serde::de::Error::custom(format!("no class of job is named {key}")))?;
        Self::of(protocol)
            .ok_or_else(|| serde::de::Error::custom(format!("the {key} class has no hold")))
    }
}

// What a hold stops of the items it covers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum HoldScope {
    // Downloading stops; what is complete keeps seeding.
    Downloads,
    // Everything stops, seeding too.
    Everything,
}

/// What a submission is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Kind {
    /// An NZB, or anything else: the NZB path refuses what it cannot read,
    /// in its own words.
    Nzb,
    /// A bencoded torrent metainfo.
    Metainfo,
    /// A magnet URI. A bare infohash is given as its magnet.
    Magnet(String),
    /// An http(s) URL, whose bytes say what it is once fetched.
    Url(url::Url),
}

impl Kind {
    /// The class of job these bytes belong to; `None` for a URL whose bytes
    /// are not fetched yet.
    pub const fn protocol(&self) -> Option<Protocol> {
        Protocol::of_kind(self)
    }
}

// Status of a job in the scheduler.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobStatus {
    AwaitingQueueScripts,
    Queued,
    Downloading,
    // Health probe in progress — checking article availability.
    Checking,
    Verifying,
    QueuedRepair,
    Repairing,
    QueuedExtract,
    Extracting,
    // Moving extracted files to the final destination directory.
    Moving,
    QueuedPostProcessing,
    PostProcessing,
    Complete,
    // Delivered with what extracted; at least one member is damaged or
    // missing. Terminal like Complete; clients see it as failed.
    Partial,
    Failed { error: String },
    Paused,
    // A magnet link is asking its peers for the torrent's metainfo.
    FetchingMetadata,
    // A torrent's pieces already on disk are being hashed.
    CheckingFiles,
    // A finished torrent is uploading to its peers.
    Seeding,
    // A torrent that is downloading has no peer sending it data.
    Stalled,
}

impl JobStatus {
    // Whether the job is still being worked through: queued, downloading,
    // or verifying, repairing or extracting what came. A pause, a failure or
    // anything after that work is done belongs to whoever set it.
    pub fn in_progress(&self) -> bool {
        matches!(
            self,
            Self::Queued | Self::Downloading | Self::Verifying | Self::Repairing | Self::Extracting
        )
    }

    // Whether the download is over and the job is in verify, repair,
    // extraction, the move or post-processing, or waits for one of them.
    pub fn past_download(&self) -> bool {
        matches!(
            self,
            Self::Verifying
                | Self::QueuedRepair
                | Self::Repairing
                | Self::QueuedExtract
                | Self::Extracting
                | Self::Moving
                | Self::QueuedPostProcessing
                | Self::PostProcessing
        )
    }

    // Whether the job has its outcome: complete, partial or failed.
    pub fn finished(&self) -> bool {
        matches!(self, Self::Complete | Self::Partial | Self::Failed { .. })
    }

    pub fn persisted_status(&self) -> &'static str {
        match self {
            Self::AwaitingQueueScripts => "awaiting_queue_scripts",
            Self::Queued => "queued",
            Self::Downloading => "downloading",
            Self::Checking => "checking",
            Self::Verifying => "verifying",
            Self::QueuedRepair => "queued_repair",
            Self::Repairing => "repairing",
            Self::QueuedExtract => "queued_extract",
            Self::Extracting => "extracting",
            Self::Moving => "moving",
            Self::QueuedPostProcessing => "queued_post_processing",
            Self::PostProcessing => "post_processing",
            Self::Complete => "complete",
            Self::Partial => "partial",
            Self::Failed { .. } => "failed",
            Self::Paused => "paused",
            Self::FetchingMetadata => "fetching_metadata",
            Self::CheckingFiles => "checking_files",
            Self::Seeding => "seeding",
            Self::Stalled => "stalled",
        }
    }

    // The stage a terminal status names as failed by itself, when no step
    // recorded one: a partial delivery left members short, which every
    // reader outside the engine takes for a failed unpack.
    pub fn shortfall_stage(&self) -> Option<crate::jobs::failure_stage::PipelineFailureStage> {
        matches!(self, Self::Partial)
            .then_some(crate::jobs::failure_stage::PipelineFailureStage::Extract)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DownloadState {
    Queued,
    Downloading,
    // Health probe in progress — checking article availability.
    Checking,
    Complete,
    Failed,
}

impl DownloadState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Downloading => "downloading",
            Self::Checking => "checking",
            Self::Complete => "complete",
            Self::Failed => "failed",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "queued" => Some(Self::Queued),
            "downloading" => Some(Self::Downloading),
            "checking" => Some(Self::Checking),
            "complete" => Some(Self::Complete),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }
}

pub use weaver_server_base::jobs::model::PostState;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RunState {
    Active,
    Paused,
}

impl RunState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Paused => "paused",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "active" => Some(Self::Active),
            "paused" => Some(Self::Paused),
            _ => None,
        }
    }
}

pub fn derive_legacy_job_status(
    download_state: DownloadState,
    post_state: PostState,
    run_state: RunState,
    failure_error: Option<&str>,
) -> JobStatus {
    if matches!(run_state, RunState::Paused) {
        return JobStatus::Paused;
    }

    if matches!(download_state, DownloadState::Failed) || matches!(post_state, PostState::Failed) {
        return JobStatus::Failed {
            error: failure_error.unwrap_or("unknown error").to_string(),
        };
    }

    if matches!(post_state, PostState::Completed)
        && matches!(download_state, DownloadState::Complete)
    {
        return JobStatus::Complete;
    }

    match post_state {
        PostState::AwaitingQueueScripts => JobStatus::AwaitingQueueScripts,
        PostState::Finalizing => JobStatus::Moving,
        PostState::QueuedPostProcessing => JobStatus::QueuedPostProcessing,
        PostState::PostProcessing => JobStatus::PostProcessing,
        PostState::Repairing => JobStatus::Repairing,
        PostState::QueuedRepair => JobStatus::QueuedRepair,
        PostState::Verifying | PostState::AwaitingRepair => JobStatus::Verifying,
        PostState::Extracting => JobStatus::Extracting,
        PostState::QueuedExtract => JobStatus::QueuedExtract,
        PostState::Idle
        | PostState::WaitingForVolumes
        | PostState::Completed
        | PostState::Failed => match download_state {
            DownloadState::Checking => JobStatus::Checking,
            DownloadState::Queued => JobStatus::Queued,
            DownloadState::Downloading | DownloadState::Complete | DownloadState::Failed => {
                JobStatus::Downloading
            }
        },
    }
}

// The error a job the operator cancelled carries in history.
pub const OPERATOR_CANCELLED_ERROR: &str = "cancelled by operator";

pub fn job_status_from_persisted_str(status: &str, error: Option<&str>) -> JobStatus {
    match status {
        "awaiting_queue_scripts" => JobStatus::AwaitingQueueScripts,
        "queued" => JobStatus::Queued,
        "downloading" => JobStatus::Downloading,
        "checking" => JobStatus::Checking,
        "verifying" => JobStatus::Verifying,
        "queued_repair" => JobStatus::QueuedRepair,
        "repairing" => JobStatus::Repairing,
        "queued_extract" => JobStatus::QueuedExtract,
        "extracting" => JobStatus::Extracting,
        "moving" => JobStatus::Moving,
        "queued_post_processing" => JobStatus::QueuedPostProcessing,
        "post_processing" => JobStatus::PostProcessing,
        "complete" => JobStatus::Complete,
        "partial" => JobStatus::Partial,
        "failed" => JobStatus::Failed {
            error: error.unwrap_or("unknown error").to_string(),
        },
        "paused" => JobStatus::Paused,
        "fetching_metadata" => JobStatus::FetchingMetadata,
        "checking_files" => JobStatus::CheckingFiles,
        "seeding" => JobStatus::Seeding,
        "stalled" => JobStatus::Stalled,
        "cancelled" => JobStatus::Failed {
            error: error.unwrap_or("cancelled").to_string(),
        },
        other => JobStatus::Failed {
            error: format!("unknown status: {other}"),
        },
    }
}

pub fn runtime_lanes_from_status_snapshot(
    status: &JobStatus,
) -> (DownloadState, PostState, RunState) {
    match status {
        JobStatus::AwaitingQueueScripts => (
            DownloadState::Complete,
            PostState::AwaitingQueueScripts,
            RunState::Active,
        ),
        JobStatus::Queued => (DownloadState::Queued, PostState::Idle, RunState::Active),
        JobStatus::Downloading => (
            DownloadState::Downloading,
            PostState::Idle,
            RunState::Active,
        ),
        JobStatus::Checking => (DownloadState::Checking, PostState::Idle, RunState::Active),
        JobStatus::Verifying => (
            DownloadState::Complete,
            PostState::Verifying,
            RunState::Active,
        ),
        JobStatus::QueuedRepair => (
            DownloadState::Complete,
            PostState::QueuedRepair,
            RunState::Active,
        ),
        JobStatus::Repairing => (
            DownloadState::Complete,
            PostState::Repairing,
            RunState::Active,
        ),
        JobStatus::QueuedExtract => (
            DownloadState::Complete,
            PostState::QueuedExtract,
            RunState::Active,
        ),
        JobStatus::Extracting => (
            DownloadState::Complete,
            PostState::Extracting,
            RunState::Active,
        ),
        JobStatus::Moving => (
            DownloadState::Complete,
            PostState::Finalizing,
            RunState::Active,
        ),
        JobStatus::QueuedPostProcessing => (
            DownloadState::Complete,
            PostState::QueuedPostProcessing,
            RunState::Active,
        ),
        JobStatus::PostProcessing => (
            DownloadState::Complete,
            PostState::PostProcessing,
            RunState::Active,
        ),
        JobStatus::Complete | JobStatus::Partial => (
            DownloadState::Complete,
            PostState::Completed,
            RunState::Active,
        ),
        JobStatus::Failed { .. } => (DownloadState::Failed, PostState::Failed, RunState::Active),
        JobStatus::Paused => (DownloadState::Queued, PostState::Idle, RunState::Paused),
        JobStatus::FetchingMetadata | JobStatus::Stalled => (
            DownloadState::Downloading,
            PostState::Idle,
            RunState::Active,
        ),
        JobStatus::CheckingFiles => (DownloadState::Checking, PostState::Idle, RunState::Active),
        JobStatus::Seeding => (
            DownloadState::Complete,
            PostState::Completed,
            RunState::Active,
        ),
    }
}

// Current wall-clock time as Unix epoch milliseconds.
pub fn epoch_ms_now() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as f64
}
