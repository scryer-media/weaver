// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use super::role_filename_view;

// What role a file plays in the context of a Usenet post.
//
// Used by engines to understand archive layout and by the server to determine
// download priority and extraction readiness.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileRole {
    // RAR volume: part of an archive set.
    RarVolume {
        volume_number: u32,
    },
    // PAR2 file: either index (`.par2`) or recovery (`.vol0+1.par2`).
    Par2 {
        is_index: bool,
        recovery_block_count: u32,
    },
    // Standalone PAR3 carrier. The name selects discovery priority only;
    // authenticated packets determine sets, matrices, and recovery capacity.
    // Embedded protection keeps its archive role.
    Par3 {
        is_index: bool,
    },
    // 7z archive (single file, not split).
    SevenZipArchive,
    // 7z split file: `.7z.001`, `.7z.002`, etc. (0-indexed).
    SevenZipSplit {
        number: u32,
    },
    // ZIP archive (`.zip`).
    ZipArchive,
    // tar archive (`.tar`).
    TarArchive,
    // Gzipped tar archive (`.tar.gz`, `.tgz`, `.tar.gzip`).
    TarGzArchive,
    // Bzip2-compressed tar archive (`.tar.bz2`, `.tbz`, `.tbz2`, `.tar.bzip2`).
    TarBz2Archive,
    // XZ-compressed tar archive (`.tar.xz`, `.txz`).
    TarXzArchive,
    // Gzipped single file (`.gz`, but not `.tar.gz`).
    GzArchive,
    // DEFLATE-compressed single file (`.deflate`).
    DeflateArchive,
    // Brotli-compressed single file (`.br`).
    BrotliArchive,
    // Zstandard-compressed single file (`.zst`, `.zstd`).
    ZstdArchive,
    // Bzip2-compressed single file (`.bz2`).
    Bzip2Archive,
    // XZ-compressed single file (`.xz`).
    XzArchive,
    // Plain split file (`.001`, `.002`, etc.). 0-indexed: `.001` = number 0.
    SplitFile {
        number: u32,
    },
    // Standalone file (not part of an archive).
    Standalone,
    // Could not determine role from filename.
    Unknown,
}

impl FileRole {
    // Infer the file role from a filename using standard Usenet naming conventions.
    pub fn from_filename(name: &str) -> Self {
        let lower = role_filename_view(name).to_ascii_lowercase();
        let lower = remove_duplicate_marker(&lower);

        if lower.ends_with(".par2") {
            if let Some(recovery_blocks) = parse_par2_vol_blocks(&lower) {
                return FileRole::Par2 {
                    is_index: false,
                    recovery_block_count: recovery_blocks,
                };
            }
            return FileRole::Par2 {
                is_index: true,
                recovery_block_count: 0,
            };
        }

        if lower.ends_with(".par3") {
            return FileRole::Par3 {
                is_index: !is_par3_volume_name(&lower),
            };
        }

        if let Some(vol) = parse_rar_volume(&lower) {
            return FileRole::RarVolume { volume_number: vol };
        }

        if let Some(role) = parse_7z_file(&lower) {
            return role;
        }

        if lower.ends_with(".zip") {
            return FileRole::ZipArchive;
        }

        if lower.ends_with(".tar.gz") || lower.ends_with(".tgz") || lower.ends_with(".tar.gzip") {
            return FileRole::TarGzArchive;
        }

        if lower.ends_with(".tar.bz2")
            || lower.ends_with(".tbz")
            || lower.ends_with(".tbz2")
            || lower.ends_with(".tar.bzip2")
        {
            return FileRole::TarBz2Archive;
        }

        if lower.ends_with(".tar.xz") || lower.ends_with(".txz") {
            return FileRole::TarXzArchive;
        }

        if lower.ends_with(".tar") {
            return FileRole::TarArchive;
        }

        if lower.ends_with(".gz") {
            return FileRole::GzArchive;
        }

        if lower.ends_with(".deflate") {
            return FileRole::DeflateArchive;
        }

        if lower.ends_with(".br") {
            return FileRole::BrotliArchive;
        }

        if lower.ends_with(".zst") || lower.ends_with(".zstd") {
            return FileRole::ZstdArchive;
        }

        if lower.ends_with(".bz2") {
            return FileRole::Bzip2Archive;
        }

        if lower.ends_with(".xz") {
            return FileRole::XzArchive;
        }

        if let Some(role) = parse_split_file(&lower) {
            return role;
        }

        if is_standalone_extension(&lower) {
            return FileRole::Standalone;
        }

        FileRole::Unknown
    }

    // The roles of a post's files, from their names in order. A name alone
    // makes `.rar`, `.rNN` and `.sNN` RAR volumes. RAR numbers on, a hundred
    // to a letter, through `.tNN` … `.zNN`, but by name alone those are
    // ordinary files (`map.t01`, `setup.x64`), and `.zNN` a split zip's part.
    // Such a name is a RAR volume when the post holds a RAR volume of its
    // stem, and for a `.zNN` no `name.zip`; otherwise its role stays the one
    // its name gives.
    pub fn of_post<'a>(names: impl IntoIterator<Item = Option<&'a str>>) -> Vec<Self> {
        let names: Vec<Option<&str>> = names.into_iter().collect();
        let mut roles: Vec<Self> = names
            .iter()
            .map(|name| name.map_or(FileRole::Unknown, FileRole::from_filename))
            .collect();
        let mut later = Vec::new();
        let mut rar_stems = HashSet::new();
        let mut zip_stems = HashSet::new();
        for (at, (name, role)) in names.iter().zip(&roles).enumerate() {
            let Some(name) = name else { continue };
            match role {
                FileRole::RarVolume { .. } => {
                    if let Some(stem) = super::archive_base_name(name, role) {
                        rar_stems.insert(stem);
                    }
                }
                FileRole::ZipArchive => {
                    let lower = role_filename_view(name).to_ascii_lowercase();
                    let lower = remove_duplicate_marker(&lower);
                    zip_stems.insert(lower[..lower.len() - 4].to_string());
                }
                FileRole::Unknown => {
                    if let Some(volume) = later_volume(name) {
                        later.push((at, volume));
                    }
                }
                _ => {}
            }
        }
        for (at, (stem, letter, volume_number)) in later {
            if rar_stems.contains(&stem) && (letter != b'z' || !zip_stems.contains(&stem)) {
                roles[at] = FileRole::RarVolume { volume_number };
            }
        }
        roles
    }

    // Download priority: lower number = download first.
    pub fn download_priority(&self) -> u32 {
        match self {
            FileRole::Par2 { is_index: true, .. } | FileRole::Par3 { is_index: true } => 0,
            FileRole::RarVolume { volume_number: 0 } => 1,
            FileRole::SevenZipArchive => 2,
            FileRole::ZipArchive => 3,
            FileRole::TarArchive => 3,
            FileRole::TarGzArchive => 3,
            FileRole::TarBz2Archive => 3,
            FileRole::TarXzArchive => 3,
            FileRole::GzArchive => 3,
            FileRole::DeflateArchive => 3,
            FileRole::BrotliArchive => 3,
            FileRole::ZstdArchive => 3,
            FileRole::Bzip2Archive => 3,
            FileRole::XzArchive => 3,
            FileRole::Standalone => 5,
            FileRole::RarVolume { volume_number } => 10 + *volume_number,
            FileRole::SevenZipSplit { number } => number.saturating_add(10),
            FileRole::SplitFile { number } => number.saturating_add(10),
            FileRole::Unknown => 50,
            FileRole::Par2 {
                is_index: false, ..
            }
            | FileRole::Par3 { is_index: false } => 1000,
        }
    }

    // Whether this file role is a recovery/repair file that should be
    // routed to the recovery queue.
    pub fn is_recovery(&self) -> bool {
        matches!(
            self,
            FileRole::Par2 {
                is_index: false,
                ..
            } | FileRole::Par3 { is_index: false }
        )
    }

    // Whether missing segments for this file should reduce computed job health.
    pub fn counts_toward_health(&self) -> bool {
        !matches!(self, FileRole::Par2 { .. } | FileRole::Par3 { .. })
    }
}

fn remove_duplicate_marker(lower: &str) -> String {
    let Some(marker_pos) = lower.rfind(".duplicate") else {
        return lower.to_string();
    };
    let digits_start = marker_pos + ".duplicate".len();
    let bytes = lower.as_bytes();
    let mut digits_end = digits_start;
    while digits_end < bytes.len() && bytes[digits_end].is_ascii_digit() {
        digits_end += 1;
    }
    if digits_end == digits_start {
        return lower.to_string();
    }
    if digits_end != bytes.len() && bytes[digits_end] != b'.' {
        return lower.to_string();
    }

    let mut normalized = String::with_capacity(lower.len() - (digits_end - marker_pos));
    normalized.push_str(&lower[..marker_pos]);
    normalized.push_str(&lower[digits_end..]);
    normalized
}

// The recovery blocks a PAR2 volume name declares, `None` when it is no
// volume name. `.vol<start>+<count>` names its count; `.vol<start>-<end>`
// names its first block and the one past its last, so it declares the
// blocks between, and none when the end is not past the start. A start
// that is no number counts from zero.
fn parse_par2_vol_blocks(lower: &str) -> Option<u32> {
    let vol_pos = lower.rfind(".vol")?;
    let between = &lower[vol_pos + 4..lower.len() - 5];
    if let Some((_, count)) = between.split_once('+') {
        return count.parse().ok();
    }
    let (start, end) = between.split_once('-')?;
    let end: u32 = end.parse().ok()?;
    Some(end.saturating_sub(start.parse().unwrap_or(0)))
}

fn is_par3_volume_name(lower: &str) -> bool {
    let Some((_, suffix)) = lower
        .strip_suffix(".par3")
        .and_then(|name| name.rsplit_once(".vol"))
    else {
        return false;
    };
    let Some((start, count)) = suffix.split_once('+') else {
        return false;
    };
    !start.is_empty()
        && !count.is_empty()
        && start.bytes().all(|byte| byte.is_ascii_digit())
        && count.bytes().all(|byte| byte.is_ascii_digit())
}

// Whether a name numbers a file `.z00` … `.z99`: a split zip's part, or a
// RAR volume after `.y99`. Either way it is a piece of something else.
pub fn names_a_z_part(name: &str) -> bool {
    later_volume(name).is_some_and(|(_, letter, _)| letter == b'z')
}

// The stem, letter and volume number of a `.tNN` … `.zNN` name,
// lower-cased: what it would be as a RAR volume.
fn later_volume(name: &str) -> Option<(String, u8, u32)> {
    let lower = role_filename_view(name).to_ascii_lowercase();
    let lower = remove_duplicate_marker(&lower);
    let (stem, letter, volume) = old_volume(&lower)?;
    (b't'..=b'z')
        .contains(&letter)
        .then(|| (stem.to_string(), letter, volume))
}

// The stem, letter and volume number of an old-numbered name
// `stem.<letter>NN`, `r` to `z`: `.r00` is volume 1, a hundred to a letter.
fn old_volume(lower: &str) -> Option<(&str, u8, u32)> {
    let at = lower.len().checked_sub(4)?;
    let ext = lower.get(at..)?.as_bytes();
    if ext[0] != b'.'
        || !(b'r'..=b'z').contains(&ext[1])
        || !ext[2..].iter().all(u8::is_ascii_digit)
    {
        return None;
    }
    let number = u32::from(ext[2] - b'0') * 10 + u32::from(ext[3] - b'0');
    Some((
        &lower[..at],
        ext[1],
        u32::from(ext[1] - b'r') * 100 + number + 1,
    ))
}

fn parse_rar_volume(lower: &str) -> Option<u32> {
    if let Some(stem) = lower.strip_suffix(".rar") {
        if let Some(part_pos) = stem.rfind(".part") {
            let num_str = &stem[part_pos + 5..];
            if let Ok(n) = num_str.parse::<u32>() {
                return Some(n.saturating_sub(1));
            }
        }
        return Some(0);
    }

    // The old numbering by name alone: `.rar`, then `.r00` … `.r99`, then
    // `.s00` … `.s99`. The letters after are a RAR volume's only beside its
    // set: see `FileRole::of_post`.
    old_volume(lower)
        .filter(|(_, letter, _)| matches!(letter, b'r' | b's'))
        .map(|(_, _, volume)| volume)
}

fn parse_7z_file(lower: &str) -> Option<FileRole> {
    if let Some(pos) = lower.rfind(".7z.") {
        let num_str = &lower[pos + 4..];
        if let Ok(n) = num_str.parse::<u32>() {
            return Some(FileRole::SevenZipSplit {
                number: n.saturating_sub(1),
            });
        }
    }

    if lower.ends_with(".7z") {
        return Some(FileRole::SevenZipArchive);
    }

    None
}

// The digits a split piece's number has at the least: `.001`.
const SPLIT_PIECE_DIGITS: usize = 3;

// A piece of a file split into numbered pieces, `name.001`, `name.002`, …:
// a stem, then a last extension of three or more digits. `lower` has had
// the quotes, underscores and dots a posted name trails taken off
// (`role_filename_view`). The same rule as `split_piece` in
// `engines/weaver-graph/src/sniff.rs`, which the graph founds its split
// store by; the two cannot share code, since neither crate depends on the
// other, so both tests pin one table of names.
fn parse_split_file(lower: &str) -> Option<FileRole> {
    let (stem, digits) = lower.rsplit_once('.')?;
    if stem.is_empty()
        || digits.len() < SPLIT_PIECE_DIGITS
        || !digits.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let number = digits
        .parse::<u32>()
        .map_or(u32::MAX, |n| n.saturating_sub(1));
    Some(FileRole::SplitFile { number })
}

fn is_standalone_extension(lower: &str) -> bool {
    let standalone = [
        ".nfo", ".txt", ".jpg", ".jpeg", ".png", ".gif", ".nzb", ".sfv", ".md5", ".url", ".srr",
        ".srs",
    ];
    standalone.iter().any(|ext| lower.ends_with(ext))
}
