use serde::{Deserialize, Serialize};

/// What role a file plays in the context of a Usenet post.
///
/// Used by engines to understand archive layout and by the server to determine
/// download priority and extraction readiness.
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
    /// Gzipped tar archive (`.tar.gz`, `.tgz`, `.tar.gzip`).
    TarGzArchive,
    /// Bzip2-compressed tar archive (`.tar.bz2`, `.tbz`, `.tbz2`, `.tar.bzip2`).
    TarBz2Archive,
    /// Gzipped single file (`.gz`, but not `.tar.gz`).
    GzArchive,
    /// DEFLATE-compressed single file (`.deflate`).
    DeflateArchive,
    /// Brotli-compressed single file (`.br`).
    BrotliArchive,
    /// Zstandard-compressed single file (`.zst`, `.zstd`).
    ZstdArchive,
    /// Bzip2-compressed single file (`.bz2`).
    Bzip2Archive,
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

        if let Some(role) = parse_split_file(&lower) {
            return role;
        }

        if is_standalone_extension(&lower) {
            return FileRole::Standalone;
        }

        FileRole::Unknown
    }

    /// Download priority: lower number = download first.
    pub fn download_priority(&self) -> u32 {
        match self {
            FileRole::Par2 { is_index: true, .. } => 0,
            FileRole::RarVolume { volume_number: 0 } => 1,
            FileRole::SevenZipArchive => 2,
            FileRole::ZipArchive => 3,
            FileRole::TarArchive => 3,
            FileRole::TarGzArchive => 3,
            FileRole::TarBz2Archive => 3,
            FileRole::GzArchive => 3,
            FileRole::DeflateArchive => 3,
            FileRole::BrotliArchive => 3,
            FileRole::ZstdArchive => 3,
            FileRole::Bzip2Archive => 3,
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
    /// routed to the recovery queue.
    pub fn is_recovery(&self) -> bool {
        matches!(
            self,
            FileRole::Par2 {
                is_index: false,
                ..
            }
        )
    }

    /// Whether missing segments for this file should reduce computed job health.
    pub fn counts_toward_health(&self) -> bool {
        !matches!(self, FileRole::Par2 { .. })
    }
}

fn parse_par2_vol_blocks(lower: &str) -> Option<u32> {
    let vol_pos = lower.rfind(".vol")?;
    let between = &lower[vol_pos + 4..lower.len() - 5];
    let sep_pos = between.find('+').or_else(|| between.find('-'))?;
    let count_str = &between[sep_pos + 1..];
    count_str.parse().ok()
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

    if lower.len() >= 4 {
        let ext = &lower[lower.len() - 4..];
        if (ext.starts_with(".r") || ext.starts_with(".s"))
            && let Ok(n) = ext[2..].parse::<u32>()
        {
            return Some(n + 1);
        }
    }

    None
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

fn parse_split_file(lower: &str) -> Option<FileRole> {
    if let Some(dot_pos) = lower.rfind('.') {
        let ext = &lower[dot_pos + 1..];
        if ext.len() == 3
            && ext.bytes().all(|b| b.is_ascii_digit())
            && let Ok(n) = ext.parse::<u32>()
        {
            return Some(FileRole::SplitFile {
                number: n.saturating_sub(1),
            });
        }
    }
    None
}

fn is_standalone_extension(lower: &str) -> bool {
    let standalone = [
        ".nfo", ".txt", ".jpg", ".jpeg", ".png", ".gif", ".nzb", ".sfv", ".md5", ".url", ".srr",
        ".srs",
    ];
    standalone.iter().any(|ext| lower.ends_with(ext))
}
