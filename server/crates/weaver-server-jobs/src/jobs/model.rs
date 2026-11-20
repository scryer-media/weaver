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
