// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The blob is a format byte, then one zstd frame (level 3) of a MessagePack
// body: the job's distinct group lists once, then one entry per file, in the
// NZB's file order, naming its group list by index:
//
//   ([groups, ...], [(full_name, groups_index, date, articles, bytes), ...])

use std::collections::HashMap;
use weaver_server_db::persistence::{StateError, sql_runtime::db_err};

// One posted file of the outline, in the NZB's file order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutlineFile {
    // The name the file was posted under, before Weaver keeps two names
    // apart.
    pub full_name: String,
    pub groups: Vec<String>,
    // The NZB's date for the file; 0 when it gives none.
    pub date: u64,
    pub articles: u32,
    pub bytes: u64,
}

// The one format this build writes and reads.
pub const OUTLINE_FORMAT: u8 = 1;
const OUTLINE_ZSTD_LEVEL: i32 = 3;

type StoredFile<'a> = (&'a str, u32, u64, u32, u64);
type ReadFile = (String, u32, u64, u32, u64);

// The stored blob of an outline's files.
pub fn encode_outline_files(files: &[OutlineFile]) -> Result<Vec<u8>, StateError> {
    let mut lists: Vec<&[String]> = Vec::new();
    let mut index: HashMap<&[String], u32> = HashMap::new();
    let rows: Vec<StoredFile<'_>> = files
        .iter()
        .map(|file| {
            let next = lists.len() as u32;
            let at = *index.entry(file.groups.as_slice()).or_insert_with(|| {
                lists.push(file.groups.as_slice());
                next
            });
            (
                file.full_name.as_str(),
                at,
                file.date,
                file.articles,
                file.bytes,
            )
        })
        .collect();
    let body = rmp_serde::to_vec(&(lists, rows)).map_err(db_err)?;
    let packed = zstd::bulk::compress(&body, OUTLINE_ZSTD_LEVEL).map_err(db_err)?;
    let mut blob = Vec::with_capacity(1 + packed.len());
    blob.push(OUTLINE_FORMAT);
    blob.extend_from_slice(&packed);
    Ok(blob)
}

// The files of a stored blob. Every failure is total: a blob that is not
// exactly one outline of this format holds no file.
pub fn decode_outline_files(blob: &[u8]) -> Result<Vec<OutlineFile>, String> {
    let (&format, packed) = blob.split_first().ok_or("empty outline")?;
    if format != OUTLINE_FORMAT {
        return Err(format!("outline format {format} is not {OUTLINE_FORMAT}"));
    }
    let body = zstd::stream::decode_all(packed).map_err(|error| error.to_string())?;
    let (lists, rows): (Vec<Vec<String>>, Vec<ReadFile>) =
        rmp_serde::from_slice(&body).map_err(|error| error.to_string())?;
    rows.into_iter()
        .map(|(full_name, at, date, articles, bytes)| {
            let groups = lists
                .get(at as usize)
                .cloned()
                .ok_or_else(|| format!("outline file names group list {at}, which it lacks"))?;
            Ok(OutlineFile {
                full_name,
                groups,
                date,
                articles,
                bytes,
            })
        })
        .collect()
}
