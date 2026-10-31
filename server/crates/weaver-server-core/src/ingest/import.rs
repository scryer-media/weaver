// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use tracing::info;

use crate::ingest::{append_original_title_metadata, derive_release_name};
use crate::jobs::ids::JobId;
use crate::jobs::{
    ArchivePasswordCandidate, ArchivePasswordSource, FileSpec, JobSpec, MessageIds, SegmentSpec,
};
use crate::settings::archive_passwords;
use weaver_model::files::{FileRole, unique_download_filenames};
use weaver_nzb::{Nzb, NzbFile, parse_nzb};

// Global counter for generating unique job IDs.
static NEXT_JOB_ID: AtomicU64 = AtomicU64::new(1);

// Import an NZB file and return a JobId + JobSpec ready for the scheduler.
pub fn import_nzb(nzb_bytes: &[u8], nzb_path: &Path) -> Result<(JobId, JobSpec), ImportError> {
    let nzb = parse_nzb(nzb_bytes).map_err(ImportError::Parse)?;

    if nzb.files.is_empty() {
        return Err(ImportError::Empty);
    }

    let job_id = JobId(NEXT_JOB_ID.fetch_add(1, Ordering::Relaxed));
    let spec = nzb_to_spec(&nzb, nzb_path, None, vec![]);

    info!(
        job_id = job_id.0,
        name = %spec.name,
        files = spec.files.len(),
        total_bytes = spec.total_bytes,
        password = spec.password.is_some(),
        "imported NZB"
    );

    Ok((job_id, spec))
}

// Convert a parsed NZB into a JobSpec. Reused by both fresh imports and
// recovery (re-parsing an NZB to rebuild the spec for a recovered job).
pub fn nzb_to_spec(
    nzb: &Nzb,
    nzb_path: &Path,
    category: Option<String>,
    metadata: Vec<(String, String)>,
) -> JobSpec {
    let metadata = append_original_title_metadata(
        metadata,
        nzb_path.file_stem().and_then(|stem| stem.to_str()),
        nzb.meta.title.as_deref(),
    );

    let name = crate::ingest::renamed_title(&metadata)
        .map(str::to_string)
        .unwrap_or_else(|| {
            derive_release_name(
                nzb_path.file_stem().and_then(|stem| stem.to_str()),
                nzb.meta.title.as_deref(),
            )
        });

    let password = first_password_candidate(nzb, nzb_path, None);

    let (files, total_bytes, metadata) = nzb_file_specs(nzb, metadata);

    JobSpec {
        name,
        password,
        files,
        total_bytes,
        category,
        metadata,
    }
}

// The files of a job built from `nzb`, their total size, and `metadata` with
// the record of every file written under a name other than its full one.
// Every job built from an NZB, first or again, names its files here, so a
// rebuilt job finds them under the names the first run gave them. A file the
// NZB lists with no article is not a file of the job: nothing could ever be
// fetched for it, so no file of the job has fewer than one article.
pub(super) fn nzb_file_specs(
    nzb: &Nzb,
    metadata: Vec<(String, String)>,
) -> (Vec<FileSpec>, u64, Vec<(String, String)>) {
    let posted: Vec<&NzbFile> = nzb
        .files
        .iter()
        .filter(|nzb_file| !nzb_file.segments.is_empty())
        .collect();
    let full_names = posted
        .iter()
        .map(|nzb_file| {
            // Prefer the enhanced extraction (handles PRiVATE format, higher confidence).
            // If the extracted name is obfuscated, keep it - PAR2 deobfuscation will
            // rename it post-repair using 16KB hash matching.
            nzb_file
                .extract_filename()
                .map(|(name, _confidence)| name)
                .or_else(|| nzb_file.filename().map(str::to_string))
                .unwrap_or_else(|| "unknown".to_string())
        })
        .collect::<Vec<_>>();
    let filenames = unique_download_filenames(full_names.iter().map(String::as_str));
    let metadata = super::append_full_file_names_metadata(
        metadata,
        full_names
            .iter()
            .map(String::as_str)
            .zip(filenames.iter().map(String::as_str)),
    );

    let mut files = Vec::with_capacity(posted.len());
    let mut total_bytes: u64 = 0;
    for (nzb_file, filename) in posted.into_iter().zip(filenames) {
        let role = FileRole::from_filename(&filename);

        let segments: Vec<SegmentSpec> = nzb_file
            .segments
            .iter()
            .enumerate()
            .map(|(ordinal, segment)| SegmentSpec {
                ordinal: ordinal as u32,
                article_number: segment.number,
                bytes: segment.bytes,
                message_ids: MessageIds::new(segment.message_ids.clone()),
            })
            .collect();

        let file_bytes: u64 = nzb_file.total_bytes();
        total_bytes += file_bytes;

        files.push(FileSpec {
            filename,
            role,
            groups: nzb_file.groups.clone(),
            posted_at_epoch: (nzb_file.date > 0).then_some(nzb_file.date),
            segments,
        });
    }

    (files, total_bytes, metadata)
}

pub fn normalize_archive_password_candidate(raw: Option<&str>) -> Option<String> {
    raw.filter(|value| !value.trim().is_empty())
        .map(str::to_string)
}

// Passwords a calling integration handed over beside the job's own one, in
// the order given: normalized the way the spec password is, blanks and repeats
// dropped, at most the integration bucket's cap kept.
pub fn integration_archive_password_candidates<'a>(
    values: impl IntoIterator<Item = &'a str>,
) -> Vec<ArchivePasswordCandidate> {
    bounded_password_candidates(
        ArchivePasswordSource::Integration,
        values,
        archive_passwords::MAX_INTEGRATION_ARCHIVE_PASSWORDS,
    )
}

// `X-DNZB-Password` values from weaver's own NZB fetch, in the order received,
// normalized and de-duplicated the same way. It keeps the header bucket's cap
// plus the integration bucket's, so the bucket still fills when the harvest
// drops values the integration already handed over.
pub fn header_archive_password_candidates<'a>(
    values: impl IntoIterator<Item = &'a str>,
) -> Vec<ArchivePasswordCandidate> {
    bounded_password_candidates(
        ArchivePasswordSource::DnzbHeader,
        values,
        archive_passwords::MAX_HEADER_ARCHIVE_PASSWORDS
            + archive_passwords::MAX_INTEGRATION_ARCHIVE_PASSWORDS,
    )
}

fn bounded_password_candidates<'a>(
    source: ArchivePasswordSource,
    values: impl IntoIterator<Item = &'a str>,
    keep: usize,
) -> Vec<ArchivePasswordCandidate> {
    let mut candidates: Vec<ArchivePasswordCandidate> = Vec::new();
    for value in values {
        if candidates.len() >= keep {
            break;
        }
        push_password_candidate(&mut candidates, source, Some(value));
    }
    candidates
}

// Every `X-DNZB-Password` value on an indexer's NZB response, in the order
// received. The header is repeatable and each occurrence is one password; a
// value is never split on commas, because a password may contain one. Header
// names are case-insensitive, which `HeaderMap` already guarantees.
pub fn dnzb_header_passwords(headers: &reqwest::header::HeaderMap) -> Vec<String> {
    headers
        .get_all("x-dnzb-password")
        .iter()
        .filter_map(|value| std::str::from_utf8(value.as_bytes()).ok())
        .map(str::to_string)
        .collect()
}

pub fn nzb_password_candidates(
    nzb: &Nzb,
    nzb_path: &Path,
    explicit_password: Option<&str>,
) -> Vec<ArchivePasswordCandidate> {
    let mut candidates = Vec::new();
    push_password_candidate(
        &mut candidates,
        ArchivePasswordSource::Explicit,
        explicit_password,
    );
    // The parsed NZB stays faithful; the bound applies here, so a restart that
    // re-harvests the persisted NZB keeps the same entries. It is the NZB
    // bucket's cap plus the two buckets ahead of it, so the bucket still fills
    // when the harvest drops values those buckets already hold.
    let keep = archive_passwords::MAX_NZB_ARCHIVE_PASSWORDS
        + archive_passwords::MAX_INTEGRATION_ARCHIVE_PASSWORDS
        + archive_passwords::MAX_HEADER_ARCHIVE_PASSWORDS;
    let meta_passwords = std::iter::once(nzb.meta.password.as_deref()).chain(
        nzb.meta
            .passwords
            .iter()
            .map(|password| Some(password.as_str())),
    );
    for password in meta_passwords {
        let kept = candidates
            .iter()
            .filter(|candidate| candidate.source() == ArchivePasswordSource::NzbMeta)
            .count();
        if kept >= keep {
            break;
        }
        push_password_candidate(&mut candidates, ArchivePasswordSource::NzbMeta, password);
    }

    if let Some(stem) = nzb_path.file_stem().and_then(|segment| segment.to_str())
        && let Some(start) = stem.find("{{")
        && let Some(end) = stem[start..].find("}}")
    {
        push_password_candidate(
            &mut candidates,
            ArchivePasswordSource::FilenameConvention,
            Some(&stem[start + 2..start + end]),
        );
    }

    candidates
}

fn first_password_candidate(
    nzb: &Nzb,
    nzb_path: &Path,
    explicit_password: Option<&str>,
) -> Option<String> {
    nzb_password_candidates(nzb, nzb_path, explicit_password)
        .into_iter()
        .next()
        .map(|candidate| candidate.value().to_string())
}

fn push_password_candidate(
    candidates: &mut Vec<ArchivePasswordCandidate>,
    source: ArchivePasswordSource,
    raw: Option<&str>,
) {
    let Some(value) = normalize_archive_password_candidate(raw) else {
        return;
    };
    if candidates
        .iter()
        .any(|candidate| candidate.value() == value.as_str())
    {
        return;
    }
    candidates.push(ArchivePasswordCandidate::new(source, value));
}

#[derive(Debug)]
pub enum ImportError {
    Parse(weaver_nzb::NzbError),
    Empty,
}

impl std::fmt::Display for ImportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Parse(error) => write!(f, "NZB parse error: {error}"),
            Self::Empty => write!(f, "NZB contains no files"),
        }
    }
}

impl std::error::Error for ImportError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec_of(segments: &str) -> JobSpec {
        let xml = format!(
            r#"<?xml version="1.0"?>
<nzb>
  <file poster="p" date="0" subject="&quot;slot.bin&quot; yEnc (1/2)">
    <groups><group>alt.binaries.test</group></groups>
    <segments>{segments}</segments>
  </file>
</nzb>"#
        );
        import_nzb(xml.as_bytes(), Path::new("slot.nzb")).unwrap().1
    }

    // A part listed twice is one article holding both ids in NZB order; the
    // first is the one it is fetched by.
    #[test]
    fn a_part_listed_twice_is_one_article_with_both_ids() {
        let spec = spec_of(
            r#"<segment bytes="10" number="1">first@example.invalid</segment>
               <segment bytes="10" number="2">two@example.invalid</segment>
               <segment bytes="10" number="1">again@example.invalid</segment>"#,
        );
        let segments = &spec.files[0].segments;
        assert_eq!(segments.len(), 2);
        assert_eq!(
            segments[0].message_ids.all(),
            ["first@example.invalid", "again@example.invalid"]
        );
        assert_eq!(segments[0].message_ids.in_use(), "first@example.invalid");
        assert_eq!(segments[0].article_number, 1);
        assert_eq!(spec.total_bytes, 20);
    }

    // A file listed with no article is no file of the job: the one after
    // it is the job's first.
    #[test]
    fn a_file_with_no_article_is_not_a_file_of_the_job() {
        let xml = r#"<?xml version="1.0"?>
<nzb>
  <file poster="p" date="0" subject="&quot;empty.bin&quot; yEnc (1/1)">
    <groups><group>alt.binaries.test</group></groups>
    <segments></segments>
  </file>
  <file poster="p" date="0" subject="&quot;slot.bin&quot; yEnc (1/1)">
    <groups><group>alt.binaries.test</group></groups>
    <segments><segment bytes="10" number="1">only@example.invalid</segment></segments>
  </file>
</nzb>"#;
        let spec = import_nzb(xml.as_bytes(), Path::new("slot.nzb")).unwrap().1;
        assert_eq!(
            spec.files
                .iter()
                .map(|file| file.filename.as_str())
                .collect::<Vec<_>>(),
            ["slot.bin"]
        );
        assert_eq!(spec.files[0].segments.len(), 1);
        assert_eq!(spec.total_bytes, 10);
    }

    #[test]
    fn a_part_listed_once_holds_one_id() {
        let spec = spec_of(r#"<segment bytes="10" number="1">only@example.invalid</segment>"#);
        let ids = &spec.files[0].segments[0].message_ids;
        assert_eq!(ids.all(), ["only@example.invalid"]);
        assert_eq!(ids.in_use(), "only@example.invalid");
        assert!(!ids.has_next());
    }
}
