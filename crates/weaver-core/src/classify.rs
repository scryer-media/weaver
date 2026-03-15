use serde::{Deserialize, Serialize};

/// What role a file plays in the context of a Usenet post.
///
/// Used by the scheduler to determine download priority and by the assembly
/// layer to track archive topology and extraction readiness.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileRole {
    /// RAR volume: part of an archive set.
    RarVolume { volume_number: u32 },
    /// PAR2 file: either index (`.par2`) or recovery (`.vol0+1.par2`).
    Par2 {
        is_index: bool,
        recovery_block_count: u32,
    },
    /// 7z archive (single file, not split).
    SevenZipArchive,
    /// 7z split file: `.7z.001`, `.7z.002`, etc. (0-indexed).
    SevenZipSplit { number: u32 },
    /// ZIP archive (`.zip`).
    ZipArchive,
    /// tar archive (`.tar`).
    TarArchive,
    /// Gzipped tar archive (`.tar.gz`, `.tgz`).
    TarGzArchive,
    /// Gzipped single file (`.gz`, but not `.tar.gz`).
    GzArchive,
    /// Plain split file (`.001`, `.002`, etc.). 0-indexed: `.001` = number 0.
    SplitFile { number: u32 },
    /// Standalone file (not part of an archive).
    Standalone,
    /// Could not determine role from filename.
    Unknown,
}

impl FileRole {
    /// Infer the file role from a filename using standard Usenet naming conventions.
    pub fn from_filename(name: &str) -> Self {
        let lower = name.to_ascii_lowercase();

        // PAR2 detection
        if lower.ends_with(".par2") {
            // Recovery volumes: name.vol00+01.par2, name.vol001+002.par2, etc.
            if let Some(recovery_blocks) = parse_par2_vol_blocks(&lower) {
                return FileRole::Par2 {
                    is_index: false,
                    recovery_block_count: recovery_blocks,
                };
            }
            // Index file: name.par2
            return FileRole::Par2 {
                is_index: true,
                recovery_block_count: 0,
            };
        }

        // RAR volume detection (multiple naming schemes)
        if let Some(vol) = parse_rar_volume(&lower) {
            return FileRole::RarVolume { volume_number: vol };
        }

        // 7z detection
        if let Some(role) = parse_7z_file(&lower) {
            return role;
        }

        // ZIP detection
        if lower.ends_with(".zip") {
            return FileRole::ZipArchive;
        }

        // tar.gz / tgz detection (must be before .gz and .tar checks)
        if lower.ends_with(".tar.gz") || lower.ends_with(".tgz") {
            return FileRole::TarGzArchive;
        }

        // tar detection
        if lower.ends_with(".tar") {
            return FileRole::TarArchive;
        }

        // gz detection (single-file gzip, not tar.gz — already caught above)
        if lower.ends_with(".gz") {
            return FileRole::GzArchive;
        }

        // Plain split files: .001, .002, ... .999
        // Must not match .7z.001 (already caught by parse_7z_file above)
        if let Some(role) = parse_split_file(&lower) {
            return role;
        }

        // Known non-archive extensions -> standalone
        if is_standalone_extension(&lower) {
            return FileRole::Standalone;
        }

        FileRole::Unknown
    }

    /// Download priority: lower number = download first.
    ///
    /// - PAR2 index files first (small, give us verification metadata)
    /// - First RAR volume next (gives us archive topology/headers)
    /// - Remaining RAR volumes in order
    /// - Standalone files
    /// - PAR2 recovery blocks last (only needed if damage detected)
    pub fn download_priority(&self) -> u32 {
        match self {
            FileRole::Par2 { is_index: true, .. } => 0,
            FileRole::RarVolume { volume_number: 0 } => 1,
            FileRole::SevenZipArchive => 2,
            FileRole::ZipArchive => 3,
            FileRole::TarArchive => 3,
            FileRole::TarGzArchive => 3,
            FileRole::GzArchive => 3,
            FileRole::Standalone => 5,
            FileRole::RarVolume { volume_number } => 10 + *volume_number,
            FileRole::SevenZipSplit { number } => 10 + *number,
            FileRole::SplitFile { number } => 10 + *number,
            FileRole::Unknown => 50,
            FileRole::Par2 {
                is_index: false, ..
            } => 1000,
        }
    }

    /// Whether this file role is a recovery/repair file that should be
    /// routed to the recovery queue (downloaded only with spare bandwidth).
    pub fn is_recovery(&self) -> bool {
        matches!(
            self,
            FileRole::Par2 {
                is_index: false,
                ..
            }
        )
    }
}

/// Parse PAR2 recovery block count from filenames like `name.vol00+05.par2`.
///
/// Supports both `+` separator (standard: `.vol00+05.par2`) and `-` separator
/// (alternate: `.vol-01.par2`). NZBGet handles both formats.
fn parse_par2_vol_blocks(lower: &str) -> Option<u32> {
    let vol_pos = lower.rfind(".vol")?;
    let between = &lower[vol_pos + 4..lower.len() - 5]; // strip ".vol" and ".par2"
    let sep_pos = between.find('+').or_else(|| between.find('-'))?;
    let count_str = &between[sep_pos + 1..];
    count_str.parse().ok()
}

/// Parse RAR volume number from filename.
///
/// Supports:
/// - New style: `name.part01.rar`, `name.part02.rar` (0-indexed: part01 = volume 0)
/// - Old style: `name.rar` (volume 0), `name.r00` (volume 1), `name.r01` (volume 2)
/// - New RAR5 multi-volume: `name.rar`, `name.part2.rar`, etc.
fn parse_rar_volume(lower: &str) -> Option<u32> {
    // New style: .part01.rar, .part001.rar
    if let Some(stem) = lower.strip_suffix(".rar") {
        if let Some(part_pos) = stem.rfind(".part") {
            let num_str = &stem[part_pos + 5..];
            if let Ok(n) = num_str.parse::<u32>() {
                // .part01 = volume 0, .part02 = volume 1
                return Some(n.saturating_sub(1));
            }
        }
        // Plain .rar with no .partNN -> volume 0
        return Some(0);
    }

    // Old style: .r00, .r01, .s00, .s01, etc.
    if lower.len() >= 4 {
        let ext = &lower[lower.len() - 4..];
        if (ext.starts_with(".r") || ext.starts_with(".s"))
            && let Ok(n) = ext[2..].parse::<u32>()
        {
            // .r00 = volume 1, .r01 = volume 2 (since .rar = volume 0)
            return Some(n + 1);
        }
    }

    None
}

/// Parse 7z archive or split file from filename.
///
/// Supports:
/// - Single archive: `name.7z`
/// - Split files: `name.7z.001`, `name.7z.002`, etc. (0-indexed: .001 = number 0)
fn parse_7z_file(lower: &str) -> Option<FileRole> {
    // Split files: .7z.001, .7z.002, etc.
    if let Some(pos) = lower.rfind(".7z.") {
        let num_str = &lower[pos + 4..];
        if let Ok(n) = num_str.parse::<u32>() {
            return Some(FileRole::SevenZipSplit {
                number: n.saturating_sub(1),
            });
        }
    }

    // Single archive: .7z
    if lower.ends_with(".7z") {
        return Some(FileRole::SevenZipArchive);
    }

    None
}

/// Parse plain split file from filename (.001, .002, etc.).
///
/// Only matches purely numeric 3-digit extensions. Must be called after
/// `parse_7z_file` to avoid matching `.7z.001` files.
fn parse_split_file(lower: &str) -> Option<FileRole> {
    if let Some(dot_pos) = lower.rfind('.') {
        let ext = &lower[dot_pos + 1..];
        if ext.len() == 3 && ext.bytes().all(|b| b.is_ascii_digit()) {
            if let Ok(n) = ext.parse::<u32>() {
                // .001 = number 0, .002 = number 1
                return Some(FileRole::SplitFile {
                    number: n.saturating_sub(1),
                });
            }
        }
    }
    None
}

/// Extract the archive set base name from a filename and its role.
///
/// Groups files that belong to the same archive set. For example:
/// - `Show.S01E01.7z.003` (SevenZipSplit) → `"Show.S01E01.7z"`
/// - `archive.7z` (SevenZipArchive) → `"archive.7z"`
/// - `movie.part01.rar` (RarVolume) → `"movie"`
///
/// Returns `None` for non-archive roles.
pub fn archive_base_name(filename: &str, role: &FileRole) -> Option<String> {
    match role {
        FileRole::SevenZipArchive => Some(filename.to_string()),
        FileRole::SevenZipSplit { .. } => {
            // Strip the `.NNN` suffix to get the base `.7z` name.
            let lower = filename.to_ascii_lowercase();
            if let Some(pos) = lower.rfind(".7z.") {
                Some(filename[..pos + 3].to_string())
            } else {
                Some(filename.to_string())
            }
        }
        FileRole::RarVolume { .. } => {
            let lower = filename.to_ascii_lowercase();
            // New style: strip .partNN.rar
            if lower.ends_with(".rar") {
                let stem = &filename[..filename.len() - 4];
                let lower_stem = &lower[..lower.len() - 4];
                if let Some(part_pos) = lower_stem.rfind(".part") {
                    Some(stem[..part_pos].to_string())
                } else {
                    // Plain .rar — stem is the base
                    Some(stem.to_string())
                }
            } else if lower.len() >= 4 {
                // Old style: .r00, .s00
                Some(filename[..filename.len() - 4].to_string())
            } else {
                Some(filename.to_string())
            }
        }
        // Single-file archives: the filename IS the base name.
        FileRole::ZipArchive
        | FileRole::TarArchive
        | FileRole::TarGzArchive
        | FileRole::GzArchive => Some(filename.to_string()),
        // Split files: strip the .NNN extension.
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

/// Check if the file extension indicates a standalone (non-archive) file.
fn is_standalone_extension(lower: &str) -> bool {
    let standalone = [
        ".nfo", ".txt", ".jpg", ".jpeg", ".png", ".gif", ".nzb", ".sfv", ".md5", ".url", ".srr",
        ".srs",
    ];
    standalone.iter().any(|ext| lower.ends_with(ext))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn par2_index() {
        assert_eq!(
            FileRole::from_filename("movie.par2"),
            FileRole::Par2 {
                is_index: true,
                recovery_block_count: 0
            }
        );
    }

    #[test]
    fn par2_recovery() {
        assert_eq!(
            FileRole::from_filename("movie.vol00+01.par2"),
            FileRole::Par2 {
                is_index: false,
                recovery_block_count: 1
            }
        );
        assert_eq!(
            FileRole::from_filename("movie.vol03+05.par2"),
            FileRole::Par2 {
                is_index: false,
                recovery_block_count: 5
            }
        );
    }

    #[test]
    fn par2_recovery_dash_format() {
        assert_eq!(
            FileRole::from_filename("movie.vol-01.par2"),
            FileRole::Par2 {
                is_index: false,
                recovery_block_count: 1
            }
        );
        assert_eq!(
            FileRole::from_filename("movie.vol-05.par2"),
            FileRole::Par2 {
                is_index: false,
                recovery_block_count: 5
            }
        );
    }

    #[test]
    fn par2_case_insensitive() {
        assert_eq!(
            FileRole::from_filename("Movie.PAR2"),
            FileRole::Par2 {
                is_index: true,
                recovery_block_count: 0
            }
        );
        assert_eq!(
            FileRole::from_filename("Movie.Vol00+02.PAR2"),
            FileRole::Par2 {
                is_index: false,
                recovery_block_count: 2
            }
        );
    }

    #[test]
    fn rar_new_style() {
        assert_eq!(
            FileRole::from_filename("movie.part01.rar"),
            FileRole::RarVolume { volume_number: 0 }
        );
        assert_eq!(
            FileRole::from_filename("movie.part02.rar"),
            FileRole::RarVolume { volume_number: 1 }
        );
        assert_eq!(
            FileRole::from_filename("movie.part10.rar"),
            FileRole::RarVolume { volume_number: 9 }
        );
    }

    #[test]
    fn rar_plain() {
        assert_eq!(
            FileRole::from_filename("movie.rar"),
            FileRole::RarVolume { volume_number: 0 }
        );
    }

    #[test]
    fn rar_old_style() {
        assert_eq!(
            FileRole::from_filename("movie.r00"),
            FileRole::RarVolume { volume_number: 1 }
        );
        assert_eq!(
            FileRole::from_filename("movie.r01"),
            FileRole::RarVolume { volume_number: 2 }
        );
        assert_eq!(
            FileRole::from_filename("movie.s00"),
            FileRole::RarVolume { volume_number: 1 }
        );
    }

    #[test]
    fn standalone() {
        assert_eq!(FileRole::from_filename("info.nfo"), FileRole::Standalone);
        assert_eq!(FileRole::from_filename("readme.txt"), FileRole::Standalone);
        assert_eq!(FileRole::from_filename("cover.jpg"), FileRole::Standalone);
    }

    #[test]
    fn sevenz_single() {
        assert_eq!(
            FileRole::from_filename("archive.7z"),
            FileRole::SevenZipArchive
        );
        assert_eq!(
            FileRole::from_filename("Movie.7Z"),
            FileRole::SevenZipArchive
        );
    }

    #[test]
    fn sevenz_split() {
        assert_eq!(
            FileRole::from_filename("archive.7z.001"),
            FileRole::SevenZipSplit { number: 0 }
        );
        assert_eq!(
            FileRole::from_filename("archive.7z.002"),
            FileRole::SevenZipSplit { number: 1 }
        );
        assert_eq!(
            FileRole::from_filename("archive.7z.010"),
            FileRole::SevenZipSplit { number: 9 }
        );
        assert_eq!(
            FileRole::from_filename("Movie.7z.003"),
            FileRole::SevenZipSplit { number: 2 }
        );
    }

    #[test]
    fn unknown() {
        assert_eq!(FileRole::from_filename("data.bin"), FileRole::Unknown);
    }

    #[test]
    fn archive_base_name_7z() {
        assert_eq!(
            archive_base_name("archive.7z", &FileRole::SevenZipArchive),
            Some("archive.7z".into())
        );
        assert_eq!(
            archive_base_name("Show.S01E01.7z.001", &FileRole::SevenZipSplit { number: 0 }),
            Some("Show.S01E01.7z".into())
        );
        assert_eq!(
            archive_base_name("Show.S01E01.7z.003", &FileRole::SevenZipSplit { number: 2 }),
            Some("Show.S01E01.7z".into())
        );
        // Different episodes produce different base names.
        assert_ne!(
            archive_base_name("Show.S01E01.7z.001", &FileRole::SevenZipSplit { number: 0 }),
            archive_base_name("Show.S01E02.7z.001", &FileRole::SevenZipSplit { number: 0 }),
        );
    }

    #[test]
    fn archive_base_name_rar() {
        assert_eq!(
            archive_base_name(
                "movie.part01.rar",
                &FileRole::RarVolume { volume_number: 0 }
            ),
            Some("movie".into())
        );
        assert_eq!(
            archive_base_name("movie.rar", &FileRole::RarVolume { volume_number: 0 }),
            Some("movie".into())
        );
        assert_eq!(
            archive_base_name("movie.r00", &FileRole::RarVolume { volume_number: 1 }),
            Some("movie".into())
        );
    }

    #[test]
    fn zip_archive() {
        assert_eq!(FileRole::from_filename("archive.zip"), FileRole::ZipArchive);
        assert_eq!(FileRole::from_filename("Movie.ZIP"), FileRole::ZipArchive);
    }

    #[test]
    fn tar_archive() {
        assert_eq!(FileRole::from_filename("backup.tar"), FileRole::TarArchive);
        assert_eq!(FileRole::from_filename("data.TAR"), FileRole::TarArchive);
    }

    #[test]
    fn tar_gz_archive() {
        assert_eq!(
            FileRole::from_filename("backup.tar.gz"),
            FileRole::TarGzArchive
        );
        assert_eq!(FileRole::from_filename("data.tgz"), FileRole::TarGzArchive);
        assert_eq!(
            FileRole::from_filename("Archive.TAR.GZ"),
            FileRole::TarGzArchive
        );
    }

    #[test]
    fn gz_archive() {
        assert_eq!(FileRole::from_filename("file.gz"), FileRole::GzArchive);
        assert_eq!(FileRole::from_filename("data.GZ"), FileRole::GzArchive);
        // .tar.gz should NOT match GzArchive
        assert_ne!(
            FileRole::from_filename("backup.tar.gz"),
            FileRole::GzArchive
        );
    }

    #[test]
    fn split_files() {
        assert_eq!(
            FileRole::from_filename("movie.mkv.001"),
            FileRole::SplitFile { number: 0 }
        );
        assert_eq!(
            FileRole::from_filename("movie.mkv.002"),
            FileRole::SplitFile { number: 1 }
        );
        assert_eq!(
            FileRole::from_filename("movie.mkv.010"),
            FileRole::SplitFile { number: 9 }
        );
        // .7z.001 should NOT match SplitFile (caught by parse_7z_file first)
        assert_eq!(
            FileRole::from_filename("archive.7z.001"),
            FileRole::SevenZipSplit { number: 0 }
        );
    }

    #[test]
    fn archive_base_name_zip() {
        assert_eq!(
            archive_base_name("archive.zip", &FileRole::ZipArchive),
            Some("archive.zip".into())
        );
    }

    #[test]
    fn archive_base_name_split() {
        assert_eq!(
            archive_base_name("movie.mkv.001", &FileRole::SplitFile { number: 0 }),
            Some("movie.mkv".into())
        );
        assert_eq!(
            archive_base_name("movie.mkv.003", &FileRole::SplitFile { number: 2 }),
            Some("movie.mkv".into())
        );
        // Same base name for different parts
        assert_eq!(
            archive_base_name("movie.mkv.001", &FileRole::SplitFile { number: 0 }),
            archive_base_name("movie.mkv.002", &FileRole::SplitFile { number: 1 }),
        );
    }

    #[test]
    fn archive_base_name_non_archive() {
        assert_eq!(archive_base_name("info.nfo", &FileRole::Standalone), None);
        assert_eq!(archive_base_name("data.bin", &FileRole::Unknown), None);
    }

    #[test]
    fn priority_ordering() {
        let par2_index = FileRole::Par2 {
            is_index: true,
            recovery_block_count: 0,
        };
        let rar_first = FileRole::RarVolume { volume_number: 0 };
        let rar_second = FileRole::RarVolume { volume_number: 1 };
        let standalone = FileRole::Standalone;
        let par2_recovery = FileRole::Par2 {
            is_index: false,
            recovery_block_count: 5,
        };

        assert!(par2_index.download_priority() < rar_first.download_priority());
        assert!(rar_first.download_priority() < standalone.download_priority());
        assert!(standalone.download_priority() < rar_second.download_priority());
        assert!(rar_second.download_priority() < par2_recovery.download_priority());
    }
}
