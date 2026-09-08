//! Sequential archive adapters shared by download-time and completed-file extraction.

use super::*;
use std::io::Read;
use weaver_model::files::FileRole;

impl SimpleArchiveKind {
    pub(in crate::pipeline) fn from_role(role: &FileRole) -> Option<Self> {
        Some(match role {
            FileRole::ZipArchive => Self::Zip,
            FileRole::TarArchive => Self::Tar,
            FileRole::TarGzArchive => Self::TarGz,
            FileRole::TarBz2Archive => Self::TarBz2,
            FileRole::TarXzArchive => Self::TarXz,
            FileRole::GzArchive => Self::Gz,
            FileRole::DeflateArchive => Self::Deflate,
            FileRole::BrotliArchive => Self::Brotli,
            FileRole::ZstdArchive => Self::Zstd,
            FileRole::Bzip2Archive => Self::Bzip2,
            FileRole::XzArchive => Self::Xz,
            FileRole::SplitFile { .. } => Self::Split,
            _ => return None,
        })
    }
}

pub(in crate::pipeline) struct SequentialExtractionContext<'a> {
    pub kind: SimpleArchiveKind,
    pub archive_path: &'a Path,
    pub root: &'a ExtractionRoot,
    pub budget: &'a Arc<JobExtractionBudget>,
    pub event_tx: &'a broadcast::Sender<PipelineEvent>,
    pub job_id: JobId,
    pub set_name: &'a str,
}

pub(super) fn extract_file(
    context: &SequentialExtractionContext<'_>,
) -> Result<Vec<String>, String> {
    let file = std::fs::File::open(context.archive_path)
        .map_err(|error| format!("failed to open {:?}: {error}", context.kind))?;
    extract_sequential_stream(file, context)
}

/// The caller owns decoder memory. Input may block on download coverage; it
/// must report EOF only when the entire source has actually been committed.
pub(in crate::pipeline) fn extract_sequential_stream<R: Read>(
    reader: R,
    context: &SequentialExtractionContext<'_>,
) -> Result<Vec<String>, String> {
    let SequentialExtractionContext {
        kind,
        archive_path,
        root,
        budget,
        event_tx,
        job_id,
        set_name,
    } = context;
    let reader = BudgetedReader::new(reader, Arc::clone(budget));
    let mut decoded: Box<dyn Read + '_> = match kind {
        SimpleArchiveKind::Tar | SimpleArchiveKind::Split => Box::new(reader),
        SimpleArchiveKind::TarGz | SimpleArchiveKind::Gz => {
            Box::new(flate2::read::MultiGzDecoder::new(reader))
        }
        SimpleArchiveKind::TarBz2 | SimpleArchiveKind::Bzip2 => {
            Box::new(bzip2::read::MultiBzDecoder::new(reader))
        }
        SimpleArchiveKind::TarXz | SimpleArchiveKind::Xz => {
            let limit = crate::ingest::XZ_DECODER_MEMORY_LIMIT_BYTES.min(budget.max_memory_bytes());
            Box::new(
                crate::ingest::xz_multistream_decoder(reader, limit)
                    .map_err(|error| format!("failed to open xz decoder: {error}"))?,
            )
        }
        SimpleArchiveKind::Deflate => Box::new(flate2::read::DeflateDecoder::new(reader)),
        SimpleArchiveKind::Brotli => Box::new(brotli::Decompressor::new(reader, 4096)),
        SimpleArchiveKind::Zstd => {
            let mut decoder = zstd::stream::read::Decoder::new(reader)
                .map_err(|error| format!("failed to open zstd: {error}"))?;
            let window_log =
                (63 - budget.max_memory_bytes().max(1024).leading_zeros()).clamp(10, 31);
            decoder
                .window_log_max(window_log)
                .map_err(|error| format!("failed to apply zstd memory limit: {error}"))?;
            Box::new(decoder)
        }
        SimpleArchiveKind::Zip => return Err("ZIP requires a seekable reader".to_string()),
    };
    if matches!(
        kind,
        SimpleArchiveKind::Tar
            | SimpleArchiveKind::TarGz
            | SimpleArchiveKind::TarBz2
            | SimpleArchiveKind::TarXz
    ) {
        let extracted =
            extract_tar_from_reader(&mut decoded, root, budget, event_tx, *job_id, set_name)?;
        // TAR stops at its end marker, before the outer codec necessarily
        // validates its trailer. Verify that trailer before accepting output.
        let mut trailing_bytes = 0u64;
        let mut buffer = [0u8; 8192];
        loop {
            budget
                .check_active_io()
                .map_err(|error| error.to_string())?;
            let read = decoded
                .read(&mut buffer)
                .map_err(|error| format!("failed to finish compressed tar stream: {error}"))?;
            if read == 0 {
                break;
            }
            trailing_bytes = trailing_bytes
                .checked_add(read as u64)
                .ok_or_else(|| "tar trailing length overflow".to_string())?;
            budget.check_member_metadata("tar trailing data", trailing_bytes)?;
        }
        return Ok(extracted);
    }
    let suffixes: &[&str] = match kind {
        SimpleArchiveKind::Gz => &[".gz"],
        SimpleArchiveKind::Bzip2 => &[".bz2"],
        SimpleArchiveKind::Xz => &[".xz"],
        SimpleArchiveKind::Deflate => &[".deflate"],
        SimpleArchiveKind::Brotli => &[".br"],
        SimpleArchiveKind::Zstd => &[".zstd", ".zst"],
        SimpleArchiveKind::Split => &[],
        _ => unreachable!("tar and zip handled above"),
    };
    // Plain parts are presented as one stream, with the topology's base name
    // as the output name. No source part is deleted or overwritten here.
    let output_path = if matches!(kind, SimpleArchiveKind::Split) {
        Path::new(set_name)
    } else {
        archive_path
    };
    extract_single_stream_to_file(
        decoded,
        output_path,
        root,
        budget,
        suffixes,
        &format!("{kind:?}"),
        event_tx,
        *job_id,
        set_name,
    )
}

pub(in crate::pipeline) fn decoder_memory_bytes(kind: SimpleArchiveKind, ceiling: u64) -> u64 {
    simple_decoder_memory_bytes(kind, ceiling)
}
