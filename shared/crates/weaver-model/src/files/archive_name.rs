use super::{FileRole, role_filename_view};

// Extract the archive set base name from a filename and its role.
//
// Groups files that belong to the same archive set. For example:
// - `Show.S01E01.7z.003` (SevenZipSplit) -> `"show.s01e01.7z"`
// - `archive.7z` (SevenZipArchive) -> `"archive.7z"`
// - `movie.part01.rar` (RarVolume) -> `"movie"`
//
// RAR and 7z keys fold ASCII case; the original filenames remain unchanged.
// Returns `None` for non-archive roles.
pub fn archive_base_name(filename: &str, role: &FileRole) -> Option<String> {
    let filename = role_filename_view(filename);
    match role {
        FileRole::SevenZipArchive => Some(filename.to_ascii_lowercase()),
        FileRole::SevenZipSplit { .. } => {
            let lower = filename.to_ascii_lowercase();
            if let Some(pos) = lower.rfind(".7z.") {
                Some(lower[..pos + 3].to_string())
            } else {
                Some(lower)
            }
        }
        FileRole::RarVolume { .. } => {
            let lower = filename.to_ascii_lowercase();
            if lower.ends_with(".rar") {
                let lower_stem = &lower[..lower.len() - 4];
                if let Some(part_pos) = lower_stem.rfind(".part") {
                    Some(lower_stem[..part_pos].to_string())
                } else {
                    Some(lower_stem.to_string())
                }
            } else if lower.len() >= 4 {
                Some(lower[..lower.len() - 4].to_string())
            } else {
                Some(lower)
            }
        }
        FileRole::ZipArchive
        | FileRole::TarArchive
        | FileRole::TarGzArchive
        | FileRole::TarBz2Archive
        | FileRole::TarXzArchive
        | FileRole::GzArchive
        | FileRole::DeflateArchive
        | FileRole::BrotliArchive
        | FileRole::ZstdArchive
        | FileRole::Bzip2Archive
        | FileRole::XzArchive => Some(filename.to_string()),
        FileRole::SplitFile { .. } => {
            if let Some(dot_pos) = filename.rfind('.') {
                Some(filename[..dot_pos].to_string())
            } else {
                Some(filename.to_string())
            }
        }
        _ => None,
    }
}
