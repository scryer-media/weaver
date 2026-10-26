// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// GraphQL surface for support reports. The reports themselves, and the
// redaction they guarantee, live in `weaver_server_core::support`.

use std::io::Read;

use async_graphql::{Context, InputObject, Result, SimpleObject, Upload};
use base64::Engine;
use weaver_server_core::support::{
    ReportEnvironment, SupportReportError, analyze_magnet, analyze_nzb_bytes,
    analyze_torrent_bytes, job_support_report, now_epoch_secs,
};
use weaver_server_core::torrents::service::TorrentService;
use weaver_server_core::{Database, SchedulerHandle};

use crate::auth::graphql_error;
use crate::jobs::staging::{
    inline_nzb_base64_fits, normalize_uploaded_nzb_reader, read_inline_nzb_bytes,
};

/// A report safe to paste in public: no names, paths, hosts or passwords.
#[derive(Debug, Clone, SimpleObject)]
pub struct SupportReport {
    /// Compact plain text, at most 80 columns, for a fenced block.
    pub text: String,
    /// The same report with every field, as pretty-printed JSON.
    pub json: String,
}

impl From<weaver_server_core::support::SupportReport> for SupportReport {
    fn from(report: weaver_server_core::support::SupportReport) -> Self {
        Self {
            text: report.text,
            json: report.json,
        }
    }
}

/// An NZB to analyze without submitting it. Exactly one source is required.
#[derive(Debug, InputObject)]
pub struct AnalyzeNzbInput {
    pub nzb_upload: Option<Upload>,
    /// The NZB, or the NZB gzip-, zstd- or xz-compressed, as base64.
    pub nzb_base64: Option<String>,
}

/// A torrent to analyze without adding it: a .torrent file, uploaded or
/// as base64, or a magnet link. Exactly one source is required.
#[derive(Debug, InputObject)]
pub struct AnalyzeTorrentInput {
    pub torrent_upload: Option<Upload>,
    pub torrent_base64: Option<String>,
    pub magnet: Option<String>,
}

// The largest .torrent file the analyzer reads.
const MAX_TORRENT_BYTES: u64 = 64 * 1024 * 1024;

fn report_error(error: SupportReportError) -> async_graphql::Error {
    match error {
        SupportReportError::NotFound(_) => graphql_error("NOT_FOUND", error.to_string()),
        SupportReportError::NzbUnavailable(_) => graphql_error("NOT_FOUND", error.to_string()),
        SupportReportError::TorrentUnavailable(_) => graphql_error("NOT_FOUND", error.to_string()),
        SupportReportError::Parse(_) | SupportReportError::TorrentParse(_) => {
            graphql_error("INVALID_INPUT", error.to_string())
        }
        SupportReportError::State(_) | SupportReportError::Task(_) => {
            graphql_error("INTERNAL", error.to_string())
        }
    }
}

pub(crate) async fn resolve_job_support_report(
    ctx: &Context<'_>,
    job_id: u64,
) -> Result<SupportReport> {
    let db = ctx.data::<Database>()?;
    let handle = ctx.data::<SchedulerHandle>()?;
    job_support_report(db, handle, ctx.data_opt::<TorrentService>(), job_id)
        .await
        .map(SupportReport::from)
        .map_err(report_error)
}

pub(crate) async fn resolve_analyze_nzb(
    ctx: &Context<'_>,
    input: AnalyzeNzbInput,
) -> Result<SupportReport> {
    let environment =
        ReportEnvironment::current(ctx.data::<Database>()?, ctx.data::<SchedulerHandle>()?);
    let source = match (input.nzb_upload, input.nzb_base64) {
        (Some(upload), None) => {
            let upload = upload
                .value(ctx)
                .map_err(|e| graphql_error("INVALID_INPUT", format!("invalid upload: {e}")))?;
            Source::Upload(upload)
        }
        (None, Some(encoded)) => {
            let encoded = encoded.trim();
            inline_nzb_base64_fits(encoded.len())
                .map_err(|e| graphql_error("INVALID_INPUT", e.to_string()))?;
            Source::Inline(
                base64::engine::general_purpose::STANDARD
                    .decode(encoded)
                    .map_err(|e| graphql_error("INVALID_INPUT", format!("invalid base64: {e}")))?,
            )
        }
        _ => {
            return Err(graphql_error(
                "INVALID_INPUT",
                "analyzeNzb requires exactly one of nzbUpload or nzbBase64",
            ));
        }
    };

    tokio::task::spawn_blocking(move || {
        let xml = match source {
            Source::Upload(upload) => {
                let mut reader = normalize_uploaded_nzb_reader(upload)
                    .map_err(|e| graphql_error("INVALID_INPUT", e.to_string()))?;
                let mut xml = Vec::new();
                reader
                    .read_to_end(&mut xml)
                    .map_err(|e| graphql_error("INVALID_INPUT", format!("invalid upload: {e}")))?;
                xml
            }
            Source::Inline(bytes) => read_inline_nzb_bytes(bytes)
                .map_err(|e| graphql_error("INVALID_INPUT", e.to_string()))?,
        };
        analyze_nzb_bytes(&xml, &environment, now_epoch_secs())
            .map(SupportReport::from)
            .map_err(report_error)
    })
    .await
    .map_err(|e| graphql_error("INTERNAL", e.to_string()))?
}

pub(crate) async fn resolve_analyze_torrent(
    ctx: &Context<'_>,
    input: AnalyzeTorrentInput,
) -> Result<SupportReport> {
    let environment =
        ReportEnvironment::current(ctx.data::<Database>()?, ctx.data::<SchedulerHandle>()?);
    let source = match (input.torrent_upload, input.torrent_base64, input.magnet) {
        (Some(upload), None, None) => {
            let upload = upload
                .value(ctx)
                .map_err(|e| graphql_error("INVALID_INPUT", format!("invalid upload: {e}")))?;
            TorrentSource::Upload(upload)
        }
        (None, Some(encoded), None) => {
            let encoded = encoded.trim();
            if encoded.len() as u64 > MAX_TORRENT_BYTES.div_ceil(3) * 4 {
                return Err(graphql_error("INVALID_INPUT", "the torrent is too large"));
            }
            TorrentSource::Bytes(
                base64::engine::general_purpose::STANDARD
                    .decode(encoded)
                    .map_err(|e| graphql_error("INVALID_INPUT", format!("invalid base64: {e}")))?,
            )
        }
        (None, None, Some(link)) => TorrentSource::Magnet(link),
        _ => {
            return Err(graphql_error(
                "INVALID_INPUT",
                "analyzeTorrent requires exactly one of torrentUpload, torrentBase64 or magnet",
            ));
        }
    };

    tokio::task::spawn_blocking(move || {
        let report = match source {
            TorrentSource::Magnet(link) => analyze_magnet(&link, &environment, now_epoch_secs()),
            TorrentSource::Bytes(bytes) => {
                analyze_torrent_bytes(&bytes, &environment, now_epoch_secs())
            }
            TorrentSource::Upload(upload) => {
                let mut bytes = Vec::new();
                upload
                    .content
                    .take(MAX_TORRENT_BYTES + 1)
                    .read_to_end(&mut bytes)
                    .map_err(|e| graphql_error("INVALID_INPUT", format!("invalid upload: {e}")))?;
                if bytes.len() as u64 > MAX_TORRENT_BYTES {
                    return Err(graphql_error("INVALID_INPUT", "the torrent is too large"));
                }
                analyze_torrent_bytes(&bytes, &environment, now_epoch_secs())
            }
        };
        report.map(SupportReport::from).map_err(report_error)
    })
    .await
    .map_err(|e| graphql_error("INTERNAL", e.to_string()))?
}

enum TorrentSource {
    Upload(async_graphql::UploadValue),
    Bytes(Vec<u8>),
    Magnet(String),
}

enum Source {
    Upload(async_graphql::UploadValue),
    Inline(Vec<u8>),
}
