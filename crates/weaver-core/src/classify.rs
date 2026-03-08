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
            return FileRole::RarVolume {
                volume_number: vol,
            };
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
            FileRole::Par2 {
                is_index: true, ..
            } => 0,
            FileRole::RarVolume { volume_number: 0 } => 1,
            FileRole::RarVolume { volume_number } => 10 + *volume_number,
            FileRole::Standalone => 5,
            FileRole::Unknown => 50,
            FileRole::Par2 {
                is_index: false, ..
            } => 1000,
        }
    }

    /// Whether this file role is a recovery/repair file that should be
    /// routed to the recovery queue (downloaded only with spare bandwidth).
    pub fn is_recovery(&self) -> bool {
        matches!(self, FileRole::Par2 { is_index: false, .. })
    }
}

/// Parse PAR2 recovery block count from filenames like `name.vol00+05.par2`.
fn parse_par2_vol_blocks(lower: &str) -> Option<u32> {
    // Pattern: .vol<start>+<count>.par2
    let vol_pos = lower.rfind(".vol")?;
    let between = &lower[vol_pos + 4..lower.len() - 5]; // strip ".vol" and ".par2"
    let plus_pos = between.find('+')?;
    let count_str = &between[plus_pos + 1..];
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
    if lower.ends_with(".rar") {
        let stem = &lower[..lower.len() - 4];
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
        if ext.starts_with(".r") || ext.starts_with(".s") {
            if let Ok(n) = ext[2..].parse::<u32>() {
                // .r00 = volume 1, .r01 = volume 2 (since .rar = volume 0)
                return Some(n + 1);
            }
        }
    }

    None
}

/// Check if the file extension indicates a standalone (non-archive) file.
fn is_standalone_extension(lower: &str) -> bool {
    let standalone = [
        ".nfo", ".txt", ".jpg", ".jpeg", ".png", ".gif", ".nzb", ".sfv", ".md5", ".url",
        ".srr", ".srs",
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
        assert_eq!(
            FileRole::from_filename("cover.jpg"),
            FileRole::Standalone
        );
    }

    #[test]
    fn unknown() {
        assert_eq!(FileRole::from_filename("data.bin"), FileRole::Unknown);
        assert_eq!(FileRole::from_filename("archive.7z"), FileRole::Unknown);
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
