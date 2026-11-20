// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Direct routing of 7z containers whose members are stored with the Copy
// method.
//
// A 7z states its map at the *tail*, so these fixtures exercise the one thing
// a RAR set never does: a layout that cannot be known until the last volume's
// last article has landed, whatever order the articles arrive in.

use super::*;

pub mod schedules;

use sevenz_turbo::encoder_options::AesEncoderOptions;
use sevenz_turbo::{ArchiveEntry, ArchiveWriter, EncoderConfiguration, EncoderMethod, Password};

// One entry of a fixture archive.
pub struct Entry {
    name: &'static str,
    // `None` for an entry the archive names but stores no bytes for. Such an
    // entry has no stream at all — a zero-length stream is a different thing,
    // and not one a 7z writer produces.
    bytes: Option<Vec<u8>>,
    // The deletion marker an incremental archive carries.
    anti: bool,
    // The entry's stored attribute word, when the fixture sets one.
    attributes: Option<u32>,
}

impl Entry {
    pub fn file(name: &'static str, bytes: Vec<u8>) -> Self {
        Self {
            name,
            bytes: Some(bytes),
            anti: false,
            attributes: None,
        }
    }

    pub fn empty_file(name: &'static str) -> Self {
        Self {
            name,
            bytes: None,
            anti: false,
            attributes: None,
        }
    }

    // An entry the header marks for deletion rather than for writing.
    pub fn anti_item(name: &'static str) -> Self {
        Self {
            name,
            bytes: None,
            anti: true,
            attributes: None,
        }
    }

    // An entry whose attributes say the name is a redirection, not a file.
    pub fn symlink(name: &'static str, target: &str) -> Self {
        const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x400;
        Self {
            name,
            bytes: Some(target.as_bytes().to_vec()),
            anti: false,
            attributes: Some(FILE_ATTRIBUTE_REPARSE_POINT),
        }
    }
}

// Deterministic member payload: compressible enough that a non-Copy fixture
// really does shrink, and varied enough that a misrouted byte shows up.
pub fn payload(seed: u8, len: usize) -> Vec<u8> {
    (0..len)
        .map(|index| {
            let index = index as u64;
            (seed as u64)
                .wrapping_mul(0x9e37_79b9)
                .wrapping_add(index.wrapping_mul(31))
                .rotate_left((index % 13) as u32) as u8
        })
        .collect()
}

pub fn build_7z_shaped(
    entries: &[Entry],
    method: EncoderMethod,
    password: Option<&str>,
    encrypt_header: bool,
) -> Vec<u8> {
    let mut writer =
        ArchiveWriter::new(std::io::Cursor::new(Vec::new())).expect("create the 7z writer");
    let mut methods = vec![EncoderConfiguration::new(method)];
    if let Some(password) = password {
        // Encryption is applied to already-coded bytes, so it sits at the
        // output end of the chain.
        methods.insert(
            0,
            EncoderConfiguration::from(AesEncoderOptions::new(Password::new(password))),
        );
    }
    writer.set_content_methods(methods);
    writer.set_encrypt_header(encrypt_header);
    for entry in entries {
        let mut written = ArchiveEntry::new_file(entry.name);
        written.size = entry.bytes.as_ref().map_or(0, |bytes| bytes.len() as u64);
        written.is_anti_item = entry.anti;
        if let Some(attributes) = entry.attributes {
            written.has_windows_attributes = true;
            written.windows_attributes = attributes;
        }
        match &entry.bytes {
            Some(bytes) => writer
                .push_archive_entry(written, Some(std::io::Cursor::new(bytes.clone())))
                .expect("write a file entry"),
            None => writer
                .push_archive_entry::<std::io::Cursor<Vec<u8>>>(written, None)
                .expect("write an entry with no stream"),
        };
    }

    writer.finish().expect("finish the archive").into_inner()
}

// Cuts one container into `count` posted volumes, the way a 7z set is made:
// a pure byte split at a fixed size, with no per-volume header. The last
// volume is whatever is left, so an `archive` that does not divide evenly
// produces the short tail real sets have.
pub fn split_volumes(archive: &[u8], count: usize) -> Vec<(String, Vec<u8>)> {
    assert!(count >= 1);
    if count == 1 {
        return vec![("silver.horizon.7z".to_string(), archive.to_vec())];
    }
    let chunk = archive.len().div_ceil(count);
    (0..count)
        .map(|index| {
            let start = index * chunk;
            let end = ((index + 1) * chunk).min(archive.len());
            (
                format!("silver.horizon.7z.{:03}", index + 1),
                archive[start..end].to_vec(),
            )
        })
        .collect()
}

// [`direct_store_job_spec`] with the volumes' lengths stated **exactly**.
//
// The shared helper pads each segment's `bytes=` with yEnc overhead, which is
// what an NZB carries; a 7z set instead needs the decoded length its articles'
// yEnc headers state, because the volumes are a byte split of one container
// and their lengths are the only way a container offset becomes a (volume,
// offset) pair. The test harness drives the yEnc layout from the assembly's
// total, so the spec is where the true length has to be put.
pub(super) fn sevenz_job_spec(volumes: &[(String, Vec<u8>)], articles: usize) -> JobSpec {
    sevenz_job_spec_stating(volumes, articles, |decoded| decoded)
}

// [`sevenz_job_spec`] with each segment's `bytes=` passed through `stated`,
// for a test that needs the NZB to say something other than the exact decoded
// length — an encoded size, the way a real NZB states it.
fn sevenz_job_spec_stating(
    volumes: &[(String, Vec<u8>)],
    articles: usize,
    stated: impl Fn(u32) -> u32,
) -> JobSpec {
    let files: Vec<FileSpec> = volumes
        .iter()
        .enumerate()
        .map(|(index, (filename, bytes))| FileSpec {
            filename: filename.clone(),
            role: FileRole::from_filename(filename),
            groups: vec!["alt.binaries.test".to_string()],
            posted_at_epoch: None,
            segments: (0..articles as u32)
                .map(|segment_number| {
                    let (start, end) = article_extent(bytes.len(), segment_number, articles);
                    segment_spec! {
                        number: segment_number,
                        bytes: stated((end - start) as u32),
                        message_id: format!("sevenz-{index}-{segment_number}@example.com"),
                    }
                })
                .collect(),
        })
        .collect();
    JobSpec {
        name: "Silver Horizon".to_string(),
        password: None,
        // Ingest's rule: a job's total is its files' declared segment bytes.
        total_bytes: files
            .iter()
            .flat_map(|file| &file.segments)
            .map(|segment| u64::from(segment.bytes))
            .sum(),
        category: None,
        metadata: vec![],
        files,
    }
}
