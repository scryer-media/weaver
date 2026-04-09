use super::FileRole;

/// Extract the archive set base name from a filename and its role.
///
/// Groups files that belong to the same archive set. For example:
/// - `Show.S01E01.7z.003` (SevenZipSplit) -> `"Show.S01E01.7z"`
/// - `archive.7z` (SevenZipArchive) -> `"archive.7z"`
/// - `movie.part01.rar` (RarVolume) -> `"movie"`
///
/// Returns `None` for non-archive roles.
pub fn archive_base_name(filename: &str, role: &FileRole) -> Option<String> {
    match role {
        FileRole::SevenZipArchive => Some(filename.to_string()),
        FileRole::SevenZipSplit { .. } => {
            let lower = filename.to_ascii_lowercase();
            if let Some(pos) = lower.rfind(".7z.") {
                Some(filename[..pos + 3].to_string())
            } else {
                Some(filename.to_string())
            }
        }
        FileRole::RarVolume { .. } => {
            let lower = filename.to_ascii_lowercase();
            if lower.ends_with(".rar") {
                let stem = &filename[..filename.len() - 4];
                let lower_stem = &lower[..lower.len() - 4];
                if let Some(part_pos) = lower_stem.rfind(".part") {
                    Some(stem[..part_pos].to_string())
                } else {
                    Some(stem.to_string())
                }
            } else if lower.len() >= 4 {
                Some(filename[..filename.len() - 4].to_string())
            } else {
                Some(filename.to_string())
            }
        }
        FileRole::ZipArchive
        | FileRole::TarArchive
        | FileRole::TarGzArchive
        | FileRole::TarBz2Archive
        | FileRole::GzArchive
        | FileRole::DeflateArchive
        | FileRole::BrotliArchive
        | FileRole::ZstdArchive
        | FileRole::Bzip2Archive => Some(filename.to_string()),
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
