use super::*;

const MAX_NESTED_EXTRACTION_DEPTH: u32 = 3;
const MAX_CONCURRENT_REPAIRS: usize = 1;

#[derive(Debug, Clone)]
struct ScannedExtractionFile {
    relative_path: String,
    role: weaver_model::files::FileRole,
    size: u64,
}

#[derive(Debug, Clone)]
struct NestedArchiveFile {
    relative_path: String,
    role: weaver_model::files::FileRole,
    archive_type: crate::jobs::assembly::ArchiveType,
    set_name: String,
    volume_number: u32,
    size: u64,
}

/// Simple archive type for non-RAR, non-7z extraction.
#[derive(Debug, Clone, Copy)]
pub(crate) enum SimpleArchiveKind {
    Zip,
    Tar,
    TarGz,
    TarBz2,
    Gz,
    Deflate,
    Brotli,
    Zstd,
    Bzip2,
    Split,
}

mod check;
mod extract;
mod nested;
mod output;
mod rar;
mod status;
