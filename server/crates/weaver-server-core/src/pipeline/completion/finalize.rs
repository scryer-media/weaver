use super::*;

const MAX_NESTED_EXTRACTION_DEPTH: u32 = 3;
const MAX_CONCURRENT_REPAIRS: usize = 1;

#[derive(Debug, Clone)]
struct ScannedExtractionFile {
    relative_path: String,
    declared_role: weaver_model::files::FileRole,
    detected_archive: Option<crate::jobs::assembly::DetectedArchiveIdentity>,
    size: u64,
}

#[derive(Debug, Clone)]
struct NestedArchiveFile {
    relative_path: String,
    declared_role: weaver_model::files::FileRole,
    detected_archive: Option<crate::jobs::assembly::DetectedArchiveIdentity>,
    archive_type: crate::jobs::assembly::ArchiveType,
    set_name: String,
    volume_number: u32,
    size: u64,
}

impl ScannedExtractionFile {
    fn effective_role(&self) -> weaver_model::files::FileRole {
        self.detected_archive
            .as_ref()
            .map(crate::jobs::assembly::DetectedArchiveIdentity::effective_role)
            .unwrap_or_else(|| self.declared_role.clone())
    }

    fn archive_set_name(&self) -> Option<String> {
        self.detected_archive
            .as_ref()
            .map(|detected| detected.set_name.clone())
            .or_else(|| {
                weaver_model::files::archive_base_name(&self.relative_path, &self.declared_role)
            })
    }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NestedExtractionDecision {
    Started,
    NoNestedArchives,
    PreserveOutputsAtDepthLimit,
}

mod check;
mod extract;
mod nested;
mod output;
mod rar;
mod status;
