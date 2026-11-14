// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A job's NZB outline: what a job waiting its turn holds of its NZB, with
// nothing per article. Each posted file's full name, groups, date, article
// count and size, and the NZB's title and first password. The outline is
// stored beside the job's row, so a restart rebuilds every waiting job from
// it without reading the stored NZB; the article lists are read from the
// stored NZB only when a job is about to run.
//
// A job built from an outline is the job `nzb_to_spec` builds from the whole
// NZB, less its article lists: the same name, password, files, file names,
// roles, groups, dates, sizes and metadata.

use std::io::{self, BufReader, Read};
use std::path::Path;

use weaver_model::files::{FileRole, unique_download_filenames};
use weaver_nzb::{Nzb, NzbFile, NzbMeta, NzbSegment};

use super::PersistedNzbError;
use crate::jobs::model::{FileSummaries, FileSummary};
use crate::jobs::{ArchivePasswordCandidate, FileSpec, JobSpec, MessageIds, SegmentSpec};

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct NzbOutline {
    pub(crate) title: Option<String>,
    // The first password the NZB's meta gives, as the spec password is
    // taken from it.
    pub(crate) password: Option<String>,
    pub(crate) files: Vec<OutlineFile>,
}

impl std::fmt::Debug for NzbOutline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NzbOutline")
            .field("title", &self.title)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .field("files", &self.files)
            .finish()
    }
}

// One posted file of the outline, in the NZB's file order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OutlineFile {
    // The name the file was posted under, before Weaver keeps two names
    // apart.
    pub(crate) full_name: String,
    pub(crate) groups: Vec<String>,
    // The NZB's date for the file; 0 when it gives none.
    pub(crate) date: u64,
    pub(crate) articles: u32,
    pub(crate) bytes: u64,
}

impl OutlineFile {
    // The outline of a posted file: one with at least one article.
    fn of(file: &NzbFile) -> Option<Self> {
        if file.segments.is_empty() {
            return None;
        }
        Some(Self {
            full_name: posted_name(file),
            groups: file.groups.clone(),
            date: file.date,
            articles: file.segments.len() as u32,
            bytes: file.total_bytes(),
        })
    }
}

// The name a file is written under before names are kept apart, as
// `nzb_file_specs` takes it.
fn posted_name(file: &NzbFile) -> String {
    file.extract_filename()
        .map(|(name, _confidence)| name)
        .or_else(|| file.filename().map(str::to_string))
        .unwrap_or_else(|| "unknown".to_string())
}

impl NzbOutline {
    #[cfg(test)]
    pub(crate) fn of(nzb: &Nzb) -> Self {
        let mut outline = Self::with_meta(&nzb.meta);
        outline.files = nzb.files.iter().filter_map(OutlineFile::of).collect();
        outline
    }

    fn with_meta(meta: &NzbMeta) -> Self {
        Self {
            title: meta.title.clone(),
            password: std::iter::once(meta.password.as_deref())
                .chain(
                    meta.passwords
                        .iter()
                        .map(|password| Some(password.as_str())),
                )
                .find_map(super::normalize_archive_password_candidate),
            files: Vec::new(),
        }
    }

    // An NZB holding the outline's meta and no file: what the password
    // candidates are read from.
    fn meta_nzb(&self) -> Nzb {
        Nzb {
            meta: NzbMeta {
                title: self.title.clone(),
                password: self.password.clone(),
                passwords: Vec::new(),
                tags: Vec::new(),
            },
            files: Vec::new(),
        }
    }

    // The job a waiting job is, built from its outline as `nzb_to_spec`
    // builds it, with no article list, and the summary of each file's list.
    pub(crate) fn spec(
        &self,
        nzb_path: &Path,
        category: Option<String>,
        metadata: Vec<(String, String)>,
    ) -> (JobSpec, FileSummaries) {
        let stem = nzb_path.file_stem().and_then(|stem| stem.to_str());
        let metadata = super::append_original_title_metadata(metadata, stem, self.title.as_deref());
        let name = super::renamed_title(&metadata)
            .map(str::to_string)
            .unwrap_or_else(|| super::derive_release_name(stem, self.title.as_deref()));
        let password = super::nzb_password_candidates(&self.meta_nzb(), nzb_path, None)
            .into_iter()
            .next()
            .map(|candidate| candidate.value().to_string());
        let filenames =
            unique_download_filenames(self.files.iter().map(|file| file.full_name.as_str()));
        let metadata = super::append_full_file_names_metadata(
            metadata,
            self.files
                .iter()
                .map(|file| file.full_name.as_str())
                .zip(filenames.iter().map(String::as_str)),
        );
        let mut files = Vec::with_capacity(self.files.len());
        let mut summaries = Vec::with_capacity(self.files.len());
        let mut total_bytes = 0u64;
        for (file, filename) in self.files.iter().zip(filenames) {
            total_bytes += file.bytes;
            summaries.push(FileSummary {
                articles: file.articles,
                bytes: file.bytes,
                remaining: file.articles,
            });
            files.push(FileSpec {
                role: FileRole::from_filename(&filename),
                filename,
                groups: file.groups.clone(),
                posted_at_epoch: (file.date > 0).then_some(file.date),
                segments: Vec::new(),
            });
        }
        (
            JobSpec {
                name,
                password,
                files,
                total_bytes,
                category,
                metadata,
            },
            FileSummaries(summaries),
        )
    }
}

// What a read of a stored NZB keeps of each file: its outline alone, or its
// outline and its article list.
pub(crate) struct StoredRead {
    pub(crate) outline: NzbOutline,
    // Every posted file's article list, in the outline's file order; empty
    // for an outline-only read.
    pub(crate) lists: Vec<Vec<SegmentSpec>>,
    // Every password candidate the NZB gives for its path.
    pub(crate) passwords: Vec<ArchivePasswordCandidate>,
}

impl StoredRead {
    // The job read, as `nzb_to_spec` builds it from the whole NZB.
    pub(crate) fn spec(
        &mut self,
        nzb_path: &Path,
        category: Option<String>,
        metadata: Vec<(String, String)>,
    ) -> JobSpec {
        let (mut spec, _) = self.outline.spec(nzb_path, category, metadata);
        for (file, list) in spec.files.iter_mut().zip(std::mem::take(&mut self.lists)) {
            file.segments = list;
        }
        spec
    }
}

// Keeps an error the zstd stream gave, so a stored NZB that does not decode
// is reported as unreadable, not as bad XML.
struct ErrorTap<R> {
    inner: R,
    error: Option<io::Error>,
}

impl<R: Read> Read for ErrorTap<R> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.inner.read(buffer).inspect_err(|error| {
            self.error = Some(io::Error::new(error.kind(), error.to_string()));
        })
    }
}

const READ_BUFFER: usize = 64 * 1024;

// Read a stored NZB in one pass, from its zstd stream through the XML parse,
// keeping of each file what `keep` takes as the file closes. No whole
// document, decoded or parsed, is ever held.
fn stream_stored_nzb(
    bytes: &[u8],
    mut keep: impl FnMut(NzbFile),
) -> Result<NzbMeta, PersistedNzbError> {
    #[cfg(test)]
    super::NZB_PARSES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let on_file = |file| {
        keep(file);
        Ok(())
    };
    if !bytes.starts_with(&super::persisted_nzb::ZSTD_MAGIC) {
        return weaver_nzb::parse_nzb_reader_files(bytes, on_file)
            .map_err(PersistedNzbError::Parse);
    }
    let decoder = zstd::stream::read::Decoder::with_buffer(bytes).map_err(PersistedNzbError::Io)?;
    let mut reader = BufReader::with_capacity(
        READ_BUFFER,
        ErrorTap {
            inner: decoder,
            error: None,
        },
    );
    let parsed = weaver_nzb::parse_nzb_reader_files(&mut reader, on_file);
    if let Some(error) = reader.into_inner().error {
        return Err(PersistedNzbError::Io(error));
    }
    parsed.map_err(PersistedNzbError::Parse)
}

// The outline of a stored NZB: each file's article list is dropped as the
// file closes.
pub(crate) fn read_stored_outline(
    bytes: &[u8],
    nzb_path: &Path,
) -> Result<StoredRead, PersistedNzbError> {
    let mut files = Vec::new();
    let meta = stream_stored_nzb(bytes, |file| files.extend(OutlineFile::of(&file)))?;
    Ok(finish(meta, files, Vec::new(), nzb_path))
}

// The outline of a stored NZB and every article list in it. Each message-ID
// is moved from the parse into the list, never copied.
pub(crate) fn read_stored_lists(
    bytes: &[u8],
    nzb_path: &Path,
) -> Result<StoredRead, PersistedNzbError> {
    let mut files = Vec::new();
    let mut lists = Vec::new();
    let meta = stream_stored_nzb(bytes, |file| {
        if let Some(outline) = OutlineFile::of(&file) {
            files.push(outline);
            lists.push(segment_specs(file.segments));
        }
    })?;
    Ok(finish(meta, files, lists, nzb_path))
}

fn finish(
    meta: NzbMeta,
    files: Vec<OutlineFile>,
    lists: Vec<Vec<SegmentSpec>>,
    nzb_path: &Path,
) -> StoredRead {
    let mut outline = NzbOutline::with_meta(&meta);
    outline.files = files;
    let passwords = super::nzb_password_candidates(
        &Nzb {
            meta,
            files: Vec::new(),
        },
        nzb_path,
        None,
    );
    StoredRead {
        outline,
        lists,
        passwords,
    }
}

fn segment_specs(segments: Vec<NzbSegment>) -> Vec<SegmentSpec> {
    segments
        .into_iter()
        .enumerate()
        .map(|(ordinal, segment)| SegmentSpec {
            ordinal: ordinal as u32,
            article_number: segment.number,
            bytes: segment.bytes,
            message_ids: MessageIds::new(segment.message_ids),
        })
        .collect()
}

#[cfg(test)]
mod tests;
