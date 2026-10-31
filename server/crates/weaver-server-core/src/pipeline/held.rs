// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The parts that arrived and wait to be placed, as the job's checkpoint
// keeps them: a uuencode part ahead of its file's cursor, or an article that
// declared no usable start ahead of the ordinal it follows. A part spooled to
// disk lies under the job's working directory, in the directory the job's own
// files live in, and the job's checkpoint names it with the facts its decode
// found, so a restart places it as it would have been placed and no
// delivered article is fetched again. A part still in memory has no row: a
// restart fetches it again. The spool files go with the working directory
// when the job is removed; each one is removed as its part is placed or let
// go.
//
// This is the one place held parts turn into rows and back.

use std::path::Component;

use super::*;
use crate::jobs::graph_checkpoint::{HeldArticleRow, HeldPartRow};

// The name every held part's spool file starts with.
pub(super) const HELD_SPOOL_PREFIX: &str = "held-";

// The bytes of one CRC32 segment as a row keeps them: its offset and length,
// then its checksum, little-endian.
const SEGMENT_BYTES: usize = 8 + 8 + 4;

// The directory a job's held parts are spooled to.
pub(super) fn held_spool_dir(working_dir: &Path) -> PathBuf {
    working_dir.join(weaver_graph::names::INTERNAL_DIR)
}

// Whether `dir` may hold a job's held parts: a directory of its own, not a
// link to one. The one check a spill makes before it writes and a restore
// makes before it reads.
fn check_held_spool_dir(dir: &Path) -> std::io::Result<()> {
    let metadata = std::fs::symlink_metadata(dir)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(std::io::Error::other(format!(
            "held part spool directory is not a directory: {}",
            dir.display()
        )));
    }
    Ok(())
}

// Make `dir` ready for a spill: checked when it is there, created when it
// is not.
pub(super) fn prepare_held_spool_dir(dir: &Path) -> std::io::Result<()> {
    match check_held_spool_dir(dir) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => std::fs::create_dir_all(dir),
        checked => checked,
    }
}

// A held part a checkpoint names whose spool file is on disk, as a restore
// gives it to the job's activation.
pub(crate) struct HeldPart {
    pub(super) segment_id: SegmentId,
    pub(super) path: PathBuf,
    pub(super) decoded_bytes: usize,
    // What a held article's decode found, with no data of its own; `None`
    // for a uuencode part.
    pub(super) article: Option<Box<DecodeResult>>,
}

impl HeldPart {
    pub(crate) fn segment_id(&self) -> SegmentId {
        self.segment_id
    }
}

// The held parts `rows` name whose spool files hold exactly the bytes they
// recorded. A row whose file is gone, is not a file of its own directly in
// the job's spool directory, or holds other than its length, or whose facts
// no decode could have found, names a part that is fetched again; so does
// every row when the spool directory is not one a spill would write to.
// Blocks: it reads each file's length.
pub(crate) fn held_parts_from_rows(
    job_id: JobId,
    working_dir: &Path,
    rows: &[HeldPartRow],
) -> Vec<HeldPart> {
    if rows.is_empty() || check_held_spool_dir(&held_spool_dir(working_dir)).is_err() {
        return Vec::new();
    }
    rows.iter()
        .filter_map(|row| {
            let part = held_part(job_id, working_dir, row)?;
            let on_disk = std::fs::symlink_metadata(&part.path).ok()?;
            (on_disk.is_file() && on_disk.len() == part.decoded_bytes as u64).then_some(part)
        })
        .collect()
}

fn held_part(job_id: JobId, working_dir: &Path, row: &HeldPartRow) -> Option<HeldPart> {
    // The row names a file directly in the spool directory, and nothing
    // else: no parent, current or root component, and no deeper path.
    let mut components = Path::new(&row.spool_path).components();
    let (Some(Component::Normal(dir)), Some(Component::Normal(name)), None) =
        (components.next(), components.next(), components.next())
    else {
        return None;
    };
    if dir != std::ffi::OsStr::new(weaver_graph::names::INTERNAL_DIR) {
        return None;
    }
    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: u32::try_from(row.file).ok()?,
        },
        segment_number: u32::try_from(row.ordinal).ok()?,
    };
    let decoded_bytes = usize::try_from(row.decoded_len).ok()?;
    let article = match &row.article {
        None => None,
        Some(article) => Some(Box::new(held_article(segment_id, article)?)),
    };
    Some(HeldPart {
        segment_id,
        path: held_spool_dir(working_dir).join(name),
        decoded_bytes,
        article,
    })
}

fn held_article(segment_id: SegmentId, row: &HeldArticleRow) -> Option<DecodeResult> {
    let opt_u32 = |value: Option<i64>| value.map(u32::try_from).transpose().ok();
    let opt_u64 = |value: Option<i64>| value.map(u64::try_from).transpose().ok();
    if !row.crc_segments.len().is_multiple_of(SEGMENT_BYTES) {
        return None;
    }
    let segments = row
        .crc_segments
        .chunks_exact(SEGMENT_BYTES)
        .map(|bytes| {
            let (offset, rest) = bytes.split_at(8);
            let (len, crc32) = rest.split_at(8);
            weaver_yenc::Segment {
                file_offset: u64::from_le_bytes(offset.try_into().unwrap_or_default()),
                len: u64::from_le_bytes(len.try_into().unwrap_or_default()),
                crc32: u32::from_le_bytes(crc32.try_into().unwrap_or_default()),
            }
        })
        .collect();
    Some(DecodeResult {
        segment_id,
        raw_size: u64::try_from(row.raw_size).ok()?,
        encoding: SegmentEncoding::Yenc,
        yenc_layout: YencLayoutAssertions {
            file_size: u64::try_from(row.file_size).ok()?,
            part: opt_u32(row.part)?,
            total: opt_u32(row.total)?,
            begin: opt_u64(row.begin)?,
            end: opt_u64(row.end)?,
        },
        crc_valid: row.crc_valid,
        part_crc_verified: row.part_crc_verified,
        part_crc: u32::try_from(row.part_crc).ok()?,
        truncation_suspected: row.truncation_suspected,
        expected_file_crc: opt_u32(row.expected_file_crc)?,
        data: DecodedChunk::from(Vec::<u8>::new()),
        yenc_name: row.name.clone(),
        // The decoder's geometry is not read when a held article is placed.
        checkpoint_plan: weaver_yenc::CheckpointPlan::None,
        segments,
    })
}

fn article_row(result: &DecodeResult) -> HeldArticleRow {
    let layout = &result.yenc_layout;
    let mut crc_segments = Vec::with_capacity(result.segments.len() * SEGMENT_BYTES);
    for segment in &result.segments {
        crc_segments.extend_from_slice(&segment.file_offset.to_le_bytes());
        crc_segments.extend_from_slice(&segment.len.to_le_bytes());
        crc_segments.extend_from_slice(&segment.crc32.to_le_bytes());
    }
    HeldArticleRow {
        raw_size: result.raw_size as i64,
        crc_valid: result.crc_valid,
        truncation_suspected: result.truncation_suspected,
        part_crc_verified: result.part_crc_verified,
        part_crc: i64::from(result.part_crc),
        expected_file_crc: result.expected_file_crc.map(i64::from),
        file_size: layout.file_size as i64,
        part: layout.part.map(i64::from),
        total: layout.total.map(i64::from),
        begin: layout.begin.map(|begin| begin as i64),
        end: layout.end.map(|end| end as i64),
        name: result.yenc_name.clone(),
        crc_segments,
    }
}

impl Pipeline {
    // The job's spooled held parts as its checkpoint keeps them, in key
    // order.
    pub(crate) fn held_part_rows(&self, job_id: JobId) -> Vec<HeldPartRow> {
        let Some(working_dir) = self.jobs.get(&job_id).map(|state| &state.working_dir) else {
            return Vec::new();
        };
        let row = |file_id: NzbFileId, ordinal: u32, bytes: &ParkedBytes, article| {
            let ParkedBytes::Spilled {
                path,
                decoded_bytes,
            } = bytes
            else {
                return None;
            };
            Some(HeldPartRow {
                file: i64::from(file_id.file_index),
                ordinal: i64::from(ordinal),
                decoded_len: *decoded_bytes as i64,
                spool_path: path.strip_prefix(working_dir).ok()?.to_str()?.to_owned(),
                article,
            })
        };
        let uu = self
            .uu_files
            .iter()
            .filter(|(file_id, _)| file_id.job_id == job_id)
            .flat_map(|(file_id, uu)| {
                uu.parked
                    .iter()
                    .filter_map(move |(ordinal, part)| row(*file_id, *ordinal, &part.bytes, None))
            });
        let unanchored = self
            .unanchored_parked
            .iter()
            .filter(|(file_id, _)| file_id.job_id == job_id)
            .flat_map(|(file_id, parked)| {
                parked.iter().filter_map(move |(ordinal, part)| {
                    part.bytes
                        .is_spilled()
                        .then(|| {
                            row(
                                *file_id,
                                *ordinal,
                                &part.bytes,
                                Some(article_row(&part.result)),
                            )
                        })
                        .flatten()
                })
            });
        let mut rows: Vec<HeldPartRow> = uu.chain(unanchored).collect();
        rows.sort_unstable_by_key(|row| (row.file, row.ordinal));
        rows
    }

    // Take back the held parts a checkpoint names, charging the spool
    // ledger for each as a spill charges it. A part the pipeline still
    // holds, as it does across a pause, is the one the row names and is
    // left as it is. Called once the job's graph is in place, so a uuencode
    // file's cursor starts where its restored root ends.
    pub(crate) fn adopt_held_parts(&mut self, parts: Vec<HeldPart>) {
        for part in parts {
            let file_id = part.segment_id.file_id;
            let ordinal = part.segment_id.segment_number;
            let bytes = ParkedBytes::Spilled {
                path: part.path,
                decoded_bytes: part.decoded_bytes,
            };
            match part.article {
                None => {
                    let uu = self.uu_file(file_id);
                    if uu.parked.contains_key(&ordinal) {
                        continue;
                    }
                    // The spool keeps a part's bytes, not what its decode
                    // found or where it came from: a part taken back is
                    // placed as clean.
                    uu.parked.insert(
                        ordinal,
                        ParkedUu {
                            bytes,
                            facts: crate::pipeline::UuSegmentFacts {
                                damaged: false,
                                ended: false,
                            },
                            source: SegmentSource {
                                source_server_idx: None,
                                exclude_servers: Vec::new(),
                            },
                        },
                    );
                    self.note_uu_spooled(part.decoded_bytes, 1);
                    self.note_uu_parked_segment();
                }
                Some(result) => {
                    let parked = self.unanchored_parked.entry(file_id).or_default();
                    if parked.contains_key(&ordinal) {
                        continue;
                    }
                    parked.insert(
                        ordinal,
                        UnanchoredPart {
                            result: *result,
                            bytes,
                            source: SegmentSource {
                                source_server_idx: None,
                                exclude_servers: Vec::new(),
                            },
                        },
                    );
                    self.note_uu_spooled(part.decoded_bytes, 1);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
