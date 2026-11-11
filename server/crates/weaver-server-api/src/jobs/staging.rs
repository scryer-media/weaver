// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::HashMap;
use std::io::{Chain, Cursor, Error, ErrorKind, Read, Result as IoResult, Take};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use async_graphql::UploadValue;
use weaver_nzb::Nzb;

use crate::auth::CallerIdentity;
use weaver_server_core::auth::generate_api_key;
use weaver_server_core::ingest::{
    PreparedPersistedNzb, StagedSubmissionPreparation, SubmitNzbError,
    XZ_DECODER_MEMORY_LIMIT_BYTES, bzip2_multistream_decoder, nzb_to_submission_spec,
    parse_and_hash_persisted_nzb_bytes, persist_decoded_nzb_reader_to_zstd, xz_multistream_decoder,
};
use weaver_server_core::jobs::FingerprintEvidence;
use weaver_server_core::security::RuntimeSecurityConfig;

const DEFAULT_STAGED_UPLOAD_TTL: Duration = Duration::from_secs(15 * 60);
const DEFAULT_CLEANUP_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Clone)]
pub(crate) struct StagedUploadEntry {
    pub(crate) id: String,
    pub(crate) owner: CallerIdentity,
    pub(crate) filename: String,
    pub(crate) nzb_zstd: Vec<u8>,
    pub(crate) preparation: Option<StagedSubmissionPreparation>,
    pub(crate) scan_before_parse: bool,
    created_at: Instant,
    last_touched_at: Instant,
}

fn staged_preparation_from_nzb(
    nzb: &Nzb,
    filename: &str,
    job_hash: [u8; 32],
) -> StagedSubmissionPreparation {
    let spec = nzb_to_submission_spec(nzb, Some(filename), None, None, Vec::new());
    let evidence = FingerprintEvidence::from_validated_spec(&spec, job_hash);
    StagedSubmissionPreparation { spec, evidence }
}

impl StagedUploadEntry {
    pub(crate) async fn rehydrate_preparation(&mut self) -> Result<(), SubmitNzbError> {
        if self.scan_before_parse {
            return Ok(());
        }
        let filename = self.filename.clone();
        let nzb_zstd = self.nzb_zstd.clone();
        let preparation = tokio::task::spawn_blocking(move || {
            let (nzb, raw_job_hash) = match parse_and_hash_persisted_nzb_bytes(&nzb_zstd) {
                Ok(parsed) => parsed,
                Err(weaver_server_core::ingest::PersistedNzbError::Io(error)) => {
                    return Err(SubmitNzbError::Save(error));
                }
                Err(weaver_server_core::ingest::PersistedNzbError::Parse(error)) => {
                    return Err(SubmitNzbError::Parse(error));
                }
            };
            if nzb.files.is_empty() {
                return Err(SubmitNzbError::Empty);
            }
            Ok(staged_preparation_from_nzb(&nzb, &filename, raw_job_hash))
        })
        .await
        .map_err(|error| {
            SubmitNzbError::State(weaver_server_core::StateError::Database(format!(
                "staged submission preparation worker panicked: {error}"
            )))
        })??;
        self.preparation = Some(preparation);
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StagedUploadSummary {
    pub(crate) staged_upload_id: String,
    pub(crate) filename: String,
    pub(crate) display_name: String,
    pub(crate) total_files: Option<u32>,
    pub(crate) total_bytes: Option<u64>,
}

#[derive(Clone)]
pub(crate) struct StagedUploadManager {
    inner: Arc<RwLock<HashMap<String, StagedUploadEntry>>>,
    ttl: Duration,
    cleanup_interval: Duration,
}

impl Default for StagedUploadManager {
    fn default() -> Self {
        Self::new()
    }
}

impl StagedUploadManager {
    pub(crate) fn new() -> Self {
        Self::with_timing(DEFAULT_STAGED_UPLOAD_TTL, DEFAULT_CLEANUP_INTERVAL)
    }

    pub(crate) fn with_timing(ttl: Duration, cleanup_interval: Duration) -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
            ttl,
            cleanup_interval,
        }
    }

    pub(crate) fn spawn_cleanup_worker(&self) {
        let this = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(this.cleanup_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                let _ = this.purge_expired();
            }
        });
    }

    pub(crate) async fn stage_upload(
        &self,
        owner: CallerIdentity,
        upload: UploadValue,
        filename_override: Option<String>,
        scan_before_parse: bool,
    ) -> Result<StagedUploadSummary, SubmitNzbError> {
        let filename = filename_override
            .filter(|value| !value.trim().is_empty())
            .or_else(|| (!upload.filename.trim().is_empty()).then_some(upload.filename.clone()))
            .unwrap_or_else(|| "upload.nzb".to_string());
        let source = normalize_uploaded_nzb_reader(upload)?;
        if scan_before_parse {
            // Selection depends on the category and parameters chosen at submit
            // time. Keep the bounded decoded input intact until that scan runs.
            let nzb_zstd = tokio::task::spawn_blocking(move || {
                zstd::stream::encode_all(source, 3).map_err(SubmitNzbError::Save)
            })
            .await
            .map_err(|error| SubmitNzbError::Upload(std::io::Error::other(error)))??;
            let staged_upload_id = generate_api_key();
            let now = Instant::now();
            self.restore_entry(StagedUploadEntry {
                id: staged_upload_id.clone(),
                owner,
                filename: filename.clone(),
                nzb_zstd,
                preparation: None,
                scan_before_parse,
                created_at: now,
                last_touched_at: now,
            });
            return Ok(StagedUploadSummary {
                staged_upload_id,
                display_name: filename.clone(),
                filename,
                total_files: None,
                total_bytes: None,
            });
        }
        let persist_result = tokio::task::spawn_blocking(move || {
            let mut source = source;
            persist_decoded_nzb_reader_to_zstd(&mut source)
        })
        .await
        .map_err(|error| SubmitNzbError::Upload(std::io::Error::other(error.to_string())))?;
        let prepared = match persist_result {
            Ok(values) => values,
            Err(weaver_server_core::ingest::PersistedNzbError::Io(error)) => {
                return Err(SubmitNzbError::Save(error));
            }
            Err(weaver_server_core::ingest::PersistedNzbError::Parse(error)) => {
                return Err(SubmitNzbError::Parse(error));
            }
        };
        if prepared.nzb.files.is_empty() {
            return Err(SubmitNzbError::Empty);
        }

        let PreparedPersistedNzb {
            nzb_zstd,
            nzb,
            raw_job_hash,
        } = prepared;
        let filename_for_preparation = filename.clone();
        let preparation = tokio::task::spawn_blocking(move || {
            staged_preparation_from_nzb(&nzb, &filename_for_preparation, raw_job_hash)
        })
        .await
        .map_err(|error| {
            SubmitNzbError::State(weaver_server_core::StateError::Database(format!(
                "staged submission preparation worker panicked: {error}"
            )))
        })?;
        let display_name = preparation.spec.name.clone();
        let total_files = preparation.spec.files.len() as u32;
        let total_bytes = preparation.spec.total_bytes;
        let staged_upload_id = generate_api_key();
        let now = Instant::now();
        let entry = StagedUploadEntry {
            id: staged_upload_id.clone(),
            owner,
            filename: filename.clone(),
            nzb_zstd,
            preparation: Some(preparation),
            scan_before_parse,
            created_at: now,
            last_touched_at: now,
        };

        let mut guard = self
            .inner
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        Self::purge_expired_locked(&mut guard, self.ttl);
        guard.insert(staged_upload_id.clone(), entry);

        Ok(StagedUploadSummary {
            staged_upload_id,
            filename,
            display_name,
            total_files: Some(total_files),
            total_bytes: Some(total_bytes),
        })
    }

    pub(crate) fn discard_owned(&self, owner: &CallerIdentity, ids: &[String]) -> usize {
        let mut guard = self
            .inner
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        Self::purge_expired_locked(&mut guard, self.ttl);
        let mut removed = 0usize;
        for id in ids {
            let should_remove = guard
                .get(id)
                .map(|entry| &entry.owner == owner)
                .unwrap_or(false);
            if should_remove && guard.remove(id).is_some() {
                removed += 1;
            }
        }
        removed
    }

    pub(crate) fn take_for_submit(
        &self,
        owner: &CallerIdentity,
        ids: &[String],
    ) -> (Vec<StagedUploadEntry>, Vec<String>) {
        let mut guard = self
            .inner
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        Self::purge_expired_locked(&mut guard, self.ttl);

        let mut found = Vec::with_capacity(ids.len());
        let mut missing = Vec::new();

        for id in ids {
            let Some(entry) = guard.get(id) else {
                missing.push(id.clone());
                continue;
            };
            if &entry.owner != owner {
                missing.push(id.clone());
                continue;
            }
            if let Some(mut taken) = guard.remove(id) {
                taken.last_touched_at = Instant::now();
                found.push(taken);
            } else {
                missing.push(id.clone());
            }
        }

        (found, missing)
    }

    pub(crate) fn restore_entry(&self, entry: StagedUploadEntry) {
        let mut guard = self
            .inner
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.insert(entry.id.clone(), entry);
    }

    pub(crate) fn purge_expired(&self) -> usize {
        let mut guard = self
            .inner
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        Self::purge_expired_locked(&mut guard, self.ttl)
    }

    fn purge_expired_locked(
        guard: &mut HashMap<String, StagedUploadEntry>,
        ttl: Duration,
    ) -> usize {
        let before = guard.len();
        guard.retain(|_, entry| {
            entry.created_at.elapsed() < ttl && entry.last_touched_at.elapsed() < ttl
        });
        before.saturating_sub(guard.len())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum UploadEncoding {
    Plain,
    Zstd,
    Gzip,
    Brotli,
    Deflate,
    Xz,
    Bzip2,
}

// The longest magic number looked for, xz's.
const UPLOAD_MAGIC_LEN: usize = 6;

// The compression the first bytes of an NZB announce. An NZB is XML, so none
// of these can open a plain one. Brotli and raw deflate have no magic number.
fn encoding_from_magic(bytes: &[u8]) -> UploadEncoding {
    const GZIP_MAGIC: &[u8] = &[0x1f, 0x8b];
    const ZSTD_MAGIC: &[u8] = &[0x28, 0xb5, 0x2f, 0xfd];
    const XZ_MAGIC: &[u8] = &[0xfd, b'7', b'z', b'X', b'Z', 0x00];
    const BZIP2_MAGIC: &[u8] = b"BZh";

    if bytes.starts_with(GZIP_MAGIC) {
        UploadEncoding::Gzip
    } else if bytes.starts_with(ZSTD_MAGIC) {
        UploadEncoding::Zstd
    } else if bytes.starts_with(XZ_MAGIC) {
        UploadEncoding::Xz
    } else if bytes.starts_with(BZIP2_MAGIC) {
        UploadEncoding::Bzip2
    } else {
        UploadEncoding::Plain
    }
}

// An upload whose first bytes were read and are put back in front of it.
type Sniffed<R> = Chain<Take<Cursor<[u8; UPLOAD_MAGIC_LEN]>>, R>;

// Reads the first bytes of an upload to learn its compression, and hands back
// a reader that still starts at the first byte.
fn sniff_upload_encoding<R: Read>(mut source: R) -> IoResult<(UploadEncoding, Sniffed<R>)> {
    let mut prefix = [0_u8; UPLOAD_MAGIC_LEN];
    let mut filled = 0;
    while filled < prefix.len() {
        match source.read(&mut prefix[filled..]) {
            Ok(0) => break,
            Ok(read) => filled += read,
            Err(error) if error.kind() == ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    let encoding = encoding_from_magic(&prefix[..filled]);
    Ok((
        encoding,
        Cursor::new(prefix).take(filled as u64).chain(source),
    ))
}

fn detect_upload_encoding(upload: &UploadValue) -> UploadEncoding {
    let filename = upload.filename.trim().to_ascii_lowercase();
    if filename.ends_with(".zst") {
        return UploadEncoding::Zstd;
    }
    if filename.ends_with(".gz") || filename.ends_with(".gzip") {
        return UploadEncoding::Gzip;
    }
    if filename.ends_with(".br") {
        return UploadEncoding::Brotli;
    }
    if filename.ends_with(".deflate") {
        return UploadEncoding::Deflate;
    }
    if filename.ends_with(".xz") {
        return UploadEncoding::Xz;
    }
    if filename.ends_with(".bz2") {
        return UploadEncoding::Bzip2;
    }

    let Some(content_type) = upload.content_type.as_deref() else {
        return UploadEncoding::Plain;
    };
    let normalized = content_type.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "application/zstd" | "application/x-zstd" | "application/octet-stream+zstd" => {
            UploadEncoding::Zstd
        }
        "application/gzip" | "application/x-gzip" | "application/octet-stream+gzip" => {
            UploadEncoding::Gzip
        }
        "application/brotli" | "application/x-brotli" | "application/octet-stream+brotli" => {
            UploadEncoding::Brotli
        }
        "application/deflate" | "application/x-deflate" | "application/octet-stream+deflate" => {
            UploadEncoding::Deflate
        }
        "application/x-xz" | "application/xz" | "application/octet-stream+xz" => UploadEncoding::Xz,
        "application/x-bzip2" | "application/x-bzip" | "application/octet-stream+bzip2" => {
            UploadEncoding::Bzip2
        }
        _ => UploadEncoding::Plain,
    }
}

struct LimitedReader<R> {
    inner: R,
    remaining: u64,
    limit: u64,
    label: &'static str,
}

impl<R> LimitedReader<R> {
    fn new(inner: R, limit: u64, label: &'static str) -> Self {
        Self {
            inner,
            remaining: limit,
            limit,
            label,
        }
    }
}

impl<R: Read> Read for LimitedReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let cap = usize::try_from(self.remaining.saturating_add(1))
            .unwrap_or(usize::MAX)
            .min(buf.len());
        let read = self.inner.read(&mut buf[..cap])?;
        if read as u64 > self.remaining {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("{} exceeds {} bytes", self.label, self.limit),
            ));
        }
        self.remaining -= read as u64;
        Ok(read)
    }
}

pub(crate) fn normalize_uploaded_nzb_reader(
    upload: UploadValue,
) -> Result<Box<dyn Read + Send>, SubmitNzbError> {
    let limits = RuntimeSecurityConfig::from_env_or_default_for_tests();
    let named = detect_upload_encoding(&upload);
    let source = LimitedReader::new(
        upload.into_read(),
        limits.nzb_upload_limit_bytes,
        "NZB upload",
    );
    // A compressed NZB under a plain name, SABnzbd's "gzip in disguise", is
    // known by its first bytes. A name or content type that names an encoding
    // is taken at its word.
    let (sniffed, source) = sniff_upload_encoding(source).map_err(SubmitNzbError::Upload)?;
    let encoding = if named == UploadEncoding::Plain {
        sniffed
    } else {
        named
    };

    let decoded: Box<dyn Read + Send> = match encoding {
        UploadEncoding::Plain => Box::new(source),
        UploadEncoding::Zstd => {
            let decoder =
                zstd::stream::read::Decoder::new(source).map_err(SubmitNzbError::Upload)?;
            Box::new(decoder)
        }
        UploadEncoding::Gzip => Box::new(flate2::read::GzDecoder::new(source)),
        UploadEncoding::Brotli => Box::new(brotli::Decompressor::new(source, 64 * 1024)),
        UploadEncoding::Deflate => Box::new(flate2::read::DeflateDecoder::new(source)),
        UploadEncoding::Xz => Box::new(
            xz_multistream_decoder(source, XZ_DECODER_MEMORY_LIMIT_BYTES)
                .map_err(SubmitNzbError::Upload)?,
        ),
        UploadEncoding::Bzip2 => Box::new(bzip2_multistream_decoder(source)),
    };

    Ok(Box::new(LimitedReader::new(
        decoded,
        limits.nzb_decompressed_limit_bytes,
        "decompressed NZB",
    )))
}

// Whether base64 text of `encoded_len` characters can decode to an NZB within
// the inline limit. Checked before decoding so an oversized string is refused
// without first being decoded in full.
pub(crate) fn inline_nzb_base64_fits(encoded_len: usize) -> Result<(), SubmitNzbError> {
    let limit = RuntimeSecurityConfig::from_env_or_default_for_tests().nzb_upload_limit_bytes;
    if base64_len_within(encoded_len, limit) {
        return Ok(());
    }
    Err(SubmitNzbError::Upload(Error::new(
        ErrorKind::InvalidData,
        format!("NZB upload exceeds {limit} bytes"),
    )))
}

// Padded base64 spends four characters on every three bytes.
fn base64_len_within(encoded_len: usize, limit: u64) -> bool {
    (encoded_len as u64) <= limit.div_ceil(3).saturating_mul(4)
}

// Read NZB bytes that arrived inline, decompressing them when their magic
// says gzip, zstd, xz or bzip2. Inline bytes carry no filename or content type, so
// the bytes themselves are the only evidence; the upload limits still apply.
pub(crate) fn read_inline_nzb_bytes(bytes: Vec<u8>) -> Result<Vec<u8>, SubmitNzbError> {
    let limits = RuntimeSecurityConfig::from_env_or_default_for_tests();
    if bytes.len() as u64 > limits.nzb_upload_limit_bytes {
        return Err(SubmitNzbError::Upload(Error::new(
            ErrorKind::InvalidData,
            format!("NZB upload exceeds {} bytes", limits.nzb_upload_limit_bytes),
        )));
    }
    let encoding = encoding_from_magic(&bytes);
    let source = Cursor::new(bytes);
    let decoded: Box<dyn Read + Send> = match encoding {
        UploadEncoding::Gzip => Box::new(flate2::read::GzDecoder::new(source)),
        UploadEncoding::Zstd => {
            Box::new(zstd::stream::read::Decoder::new(source).map_err(SubmitNzbError::Upload)?)
        }
        UploadEncoding::Xz => Box::new(
            xz_multistream_decoder(source, XZ_DECODER_MEMORY_LIMIT_BYTES)
                .map_err(SubmitNzbError::Upload)?,
        ),
        UploadEncoding::Bzip2 => Box::new(bzip2_multistream_decoder(source)),
        UploadEncoding::Plain | UploadEncoding::Brotli | UploadEncoding::Deflate => {
            Box::new(source)
        }
    };
    let mut xml = Vec::new();
    LimitedReader::new(
        decoded,
        limits.nzb_decompressed_limit_bytes,
        "decompressed NZB",
    )
    .read_to_end(&mut xml)
    .map_err(SubmitNzbError::Upload)?;
    Ok(xml)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fmt::Write as _;
    use std::io::Write;

    use lzma_turbo::{LzmaEncProps, XzWriter};

    #[test]
    fn inline_base64_bound_matches_the_decoded_limit() {
        use base64::Engine;
        for limit in [0_u64, 1, 2, 3, 4, 5, 6, 7, 1024] {
            let largest =
                base64::engine::general_purpose::STANDARD.encode(vec![0_u8; limit as usize]);
            assert!(base64_len_within(largest.len(), limit), "limit {limit}");
            // Base64 length resolves bytes only to whole groups of three; the
            // exact limit is applied to the decoded bytes.
            let over = base64::engine::general_purpose::STANDARD.encode(vec![
                0_u8;
                limit.div_ceil(3)
                    as usize
                    * 3
                    + 1
            ]);
            assert!(!base64_len_within(over.len(), limit), "limit {limit}");
        }
        assert!(inline_nzb_base64_fits(0).is_ok());
        assert!(inline_nzb_base64_fits(usize::MAX).is_err());
    }

    fn minimal_nzb(name: &str) -> String {
        format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
  <file poster="test@test.com" date="1234567890" subject="{name} - &quot;file.rar&quot; yEnc (1/1)">
    <groups><group>alt.binaries.test</group></groups>
    <segments><segment bytes="500000" number="1">{name}-seg1@test.com</segment></segments>
  </file>
</nzb>"#
        )
    }

    fn make_upload(name: &str) -> UploadValue {
        UploadValue {
            filename: format!("{name}.nzb"),
            content_type: Some("application/x-nzb".to_string()),
            content: minimal_nzb(name).into_bytes().into(),
        }
    }

    fn make_large_single_file_upload(segment_count: usize) -> UploadValue {
        let mut xml = String::with_capacity(segment_count * 65);
        xml.push_str(r#"<nzb><file poster="p" date="0" subject="large"><segments>"#);
        for number in 1..=segment_count {
            write!(
                xml,
                r#"<segment bytes="1" number="{number}">{number}@test</segment>"#
            )
            .unwrap();
        }
        xml.push_str("</segments></file></nzb>");
        UploadValue {
            filename: "large.nzb".to_string(),
            content_type: Some("application/x-nzb".to_string()),
            content: xml.into_bytes().into(),
        }
    }

    fn make_xz_upload(filename: &str, content_type: &str, name: &str) -> UploadValue {
        let props = LzmaEncProps::xz_preset(0, false).unwrap();
        let mut writer = XzWriter::new(Vec::new(), &props).unwrap();
        writer.write_all(minimal_nzb(name).as_bytes()).unwrap();
        UploadValue {
            filename: filename.to_string(),
            content_type: Some(content_type.to_string()),
            content: writer.finish().unwrap().into(),
        }
    }

    #[test]
    fn limited_reader_errors_after_limit() {
        let mut reader = LimitedReader::new(std::io::Cursor::new(b"abcdef"), 3, "test payload");
        let mut buf = Vec::new();
        let error = reader.read_to_end(&mut buf).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidData);
        assert!(error.to_string().contains("test payload exceeds 3 bytes"));
    }

    #[tokio::test]
    async fn take_for_submit_is_scoped_to_owner() {
        let manager =
            StagedUploadManager::with_timing(Duration::from_secs(60), Duration::from_secs(60));
        let owner_a = CallerIdentity::Local([1; 32]);
        let owner_b = CallerIdentity::Local([2; 32]);
        let staged = manager
            .stage_upload(owner_a.clone(), make_upload("owned"), None, false)
            .await
            .unwrap();

        let (found, missing) =
            manager.take_for_submit(&owner_b, std::slice::from_ref(&staged.staged_upload_id));
        assert!(found.is_empty());
        assert_eq!(missing, vec![staged.staged_upload_id]);
    }

    #[tokio::test]
    async fn stages_xz_uploads_by_filename_or_mime_type() {
        let manager =
            StagedUploadManager::with_timing(Duration::from_secs(60), Duration::from_secs(60));
        let owner = CallerIdentity::Local([4; 32]);

        for (filename, content_type) in [
            ("filename.nzb.xz", "application/octet-stream"),
            ("mime.nzb", "application/x-xz"),
        ] {
            let staged = manager
                .stage_upload(
                    owner.clone(),
                    make_xz_upload(filename, content_type, "xz-upload"),
                    None,
                    false,
                )
                .await
                .unwrap();
            assert_eq!(staged.filename, filename);
            assert_eq!(staged.total_files, Some(1));
        }
    }

    // `minimal_nzb("bz2-upload")` as two bzip2 streams, one after the other.
    #[rustfmt::skip]
    const BZIP2_NZB: &[u8] = &[
        0x42, 0x5a, 0x68, 0x39, 0x31, 0x41, 0x59, 0x26, 0x53, 0x59, 0x3a, 0xb4, 0xe5, 0x35, 0x00, 0x00,
        0x1d, 0x5f, 0x80, 0x00, 0x10, 0x51, 0x03, 0xff, 0xff, 0xc5, 0x00, 0x06, 0x00, 0x3f, 0xf7, 0xff,
        0xd0, 0x20, 0x00, 0x84, 0x1a, 0xa9, 0xfa, 0x93, 0xda, 0x93, 0x40, 0x7a, 0x47, 0xa9, 0xe8, 0x6a,
        0x3d, 0x32, 0x41, 0xea, 0x7a, 0x8d, 0x0d, 0x32, 0x60, 0xd5, 0x4f, 0x6a, 0x9e, 0xa3, 0x4c, 0x65,
        0x34, 0xf4, 0xf5, 0x4f, 0x42, 0x68, 0xd1, 0x90, 0xc2, 0x06, 0x65, 0x31, 0x60, 0x8d, 0xe8, 0x31,
        0x61, 0x78, 0xb2, 0x3b, 0x47, 0x21, 0x9e, 0x89, 0x00, 0x9b, 0xe1, 0x3d, 0x4a, 0xba, 0xdc, 0x13,
        0x5f, 0x54, 0xb1, 0x8c, 0x76, 0xc5, 0x2c, 0x63, 0x2a, 0x50, 0x73, 0x36, 0xb7, 0x73, 0x7b, 0x2e,
        0xa0, 0x54, 0xea, 0xa9, 0xb4, 0xef, 0x6a, 0xe0, 0x50, 0x38, 0xd2, 0x4d, 0xf3, 0x2f, 0xed, 0x30,
        0x6e, 0x6c, 0x0e, 0x77, 0x8d, 0xfe, 0xf8, 0xea, 0x88, 0x9e, 0xf4, 0x49, 0xfc, 0x84, 0x27, 0xf4,
        0x95, 0x90, 0xf8, 0xca, 0x0a, 0xf7, 0x84, 0x55, 0xc0, 0x66, 0x12, 0x47, 0xc7, 0x59, 0xc9, 0xcc,
        0x9f, 0xa9, 0x30, 0xce, 0x0d, 0x0d, 0x70, 0xa1, 0x4a, 0x43, 0x69, 0x21, 0x24, 0x06, 0x41, 0x65,
        0xb9, 0x0f, 0x9d, 0x7f, 0xd3, 0x98, 0xba, 0x27, 0x06, 0xaf, 0xc2, 0x69, 0x60, 0xdb, 0xa0, 0xbb,
        0x92, 0x29, 0xc2, 0x84, 0x81, 0xd5, 0xa7, 0x29, 0xa8, 0x42, 0x5a, 0x68, 0x39, 0x31, 0x41, 0x59,
        0x26, 0x53, 0x59, 0x3d, 0xfe, 0x19, 0x72, 0x00, 0x00, 0x19, 0x5d, 0x80, 0x60, 0x10, 0x50, 0x63,
        0xf2, 0x0f, 0x42, 0x00, 0x3f, 0xa7, 0xde, 0x30, 0x20, 0x00, 0x80, 0x34, 0x29, 0x94, 0xf4, 0x69,
        0xa1, 0x89, 0x90, 0x83, 0x20, 0xcc, 0xa1, 0x83, 0x4d, 0x1a, 0x69, 0x84, 0xc4, 0xc9, 0x80, 0x81,
        0xa6, 0x58, 0x1b, 0x00, 0x10, 0x12, 0xcb, 0x44, 0xe6, 0xd8, 0x5d, 0x98, 0x73, 0xd8, 0x7b, 0x46,
        0xe1, 0xca, 0xa6, 0x0b, 0x22, 0x9e, 0x13, 0xcf, 0xd5, 0x55, 0x2c, 0x5c, 0x92, 0xf8, 0x83, 0x43,
        0x3e, 0x9f, 0x69, 0x8e, 0x38, 0xe9, 0xaa, 0x08, 0xbc, 0xeb, 0x4b, 0xe8, 0xfc, 0x7d, 0xc1, 0x6b,
        0xb3, 0xf5, 0x30, 0x20, 0xc0, 0x32, 0xa3, 0xcb, 0x58, 0x90, 0x3f, 0x06, 0x72, 0xa3, 0x19, 0x37,
        0x46, 0xbd, 0xc0, 0x5d, 0x5c, 0x04, 0xb0, 0x87, 0x58, 0x81, 0x71, 0x0e, 0xd7, 0xb4, 0x04, 0x0e,
        0x79, 0x9d, 0x21, 0x78, 0x35, 0x89, 0xba, 0x41, 0x36, 0xae, 0xb6, 0x14, 0x83, 0x4c, 0x46, 0x61,
        0x5f, 0xc5, 0xdc, 0x91, 0x4e, 0x14, 0x24, 0x0f, 0x7f, 0x86, 0x5c, 0x80,
    ];

    fn decoded_upload(filename: &str, content_type: &str, content: Vec<u8>) -> String {
        let upload = UploadValue {
            filename: filename.to_string(),
            content_type: Some(content_type.to_string()),
            content: content.into(),
        };
        let mut xml = String::new();
        normalize_uploaded_nzb_reader(upload)
            .unwrap()
            .read_to_string(&mut xml)
            .unwrap();
        xml
    }

    #[test]
    fn bzip2_uploads_decode_by_name_mime_type_or_magic() {
        for (filename, content_type) in [
            ("named.nzb.bz2", "application/octet-stream"),
            ("mime.nzb", "application/x-bzip2"),
            ("disguised.nzb", "application/x-nzb"),
        ] {
            assert_eq!(
                decoded_upload(filename, content_type, BZIP2_NZB.to_vec()),
                minimal_nzb("bz2-upload"),
                "{filename}"
            );
        }
        assert_eq!(
            read_inline_nzb_bytes(BZIP2_NZB.to_vec()).unwrap(),
            minimal_nzb("bz2-upload").into_bytes()
        );
    }

    // SABnzbd's "gzip in disguise": compressed bytes under a plain `.nzb` name.
    #[test]
    fn compressed_uploads_under_a_plain_name_decode_by_magic() {
        let nzb = minimal_nzb("disguised");

        let mut gzip = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        gzip.write_all(nzb.as_bytes()).unwrap();
        let gzip = gzip.finish().unwrap();
        let zstd = zstd::stream::encode_all(nzb.as_bytes(), 1).unwrap();
        let xz = make_xz_upload("x.nzb", "application/x-nzb", "disguised")
            .content
            .to_vec();

        for content in [gzip, zstd, xz] {
            assert_eq!(
                decoded_upload("plain.nzb", "application/x-nzb", content),
                nzb
            );
        }
    }

    #[test]
    fn plain_and_short_uploads_pass_through_the_magic_check() {
        let nzb = minimal_nzb("plain");
        assert_eq!(
            decoded_upload("plain.nzb", "application/x-nzb", nzb.clone().into_bytes()),
            nzb
        );
        for short in ["", "<", "<nzb/"] {
            assert_eq!(
                decoded_upload("short.nzb", "application/x-nzb", short.as_bytes().to_vec()),
                short
            );
        }
    }

    #[tokio::test]
    async fn stages_a_file_above_the_former_per_file_segment_cap() {
        const SEGMENTS: usize = 100_001;
        let manager =
            StagedUploadManager::with_timing(Duration::from_secs(60), Duration::from_secs(60));
        let owner = CallerIdentity::Local([5; 32]);

        let staged = manager
            .stage_upload(owner, make_large_single_file_upload(SEGMENTS), None, false)
            .await
            .unwrap();

        assert_eq!(staged.total_files, Some(1));
        assert_eq!(staged.total_bytes, Some(SEGMENTS as u64));
        let entries = manager
            .inner
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let preparation = entries[&staged.staged_upload_id]
            .preparation
            .as_ref()
            .unwrap();
        assert_eq!(preparation.spec.files[0].segments.len(), SEGMENTS);
    }

    #[tokio::test]
    async fn purge_expired_removes_stale_entries() {
        let manager = StagedUploadManager::with_timing(Duration::ZERO, Duration::from_secs(60));
        let owner = CallerIdentity::Local([3; 32]);
        let staged = manager
            .stage_upload(owner.clone(), make_upload("expired"), None, false)
            .await
            .unwrap();

        assert_eq!(manager.purge_expired(), 1);
        let (found, missing) =
            manager.take_for_submit(&owner, std::slice::from_ref(&staged.staged_upload_id));
        assert!(found.is_empty());
        assert_eq!(missing, vec![staged.staged_upload_id]);
    }

    #[tokio::test]
    async fn scan_staging_keeps_unparsed_input_and_retry_does_not_parse_it() {
        let manager = StagedUploadManager::new();
        let owner = CallerIdentity::Local([6; 32]);
        let source = b"incomplete NZB for a scan script to repair";
        let upload = || UploadValue {
            filename: "repairable.nzb".into(),
            content_type: None,
            content: source.to_vec().into(),
        };
        assert!(
            manager
                .stage_upload(owner.clone(), upload(), None, false)
                .await
                .is_err()
        );
        let staged = manager
            .stage_upload(owner.clone(), upload(), None, true)
            .await
            .unwrap();
        assert_eq!(staged.total_files, None);
        assert_eq!(staged.total_bytes, None);
        let (mut found, missing) = manager.take_for_submit(&owner, &[staged.staged_upload_id]);
        assert!(missing.is_empty());
        let mut entry = found.pop().unwrap();
        assert!(entry.preparation.is_none());
        assert_eq!(
            zstd::stream::decode_all(entry.nzb_zstd.as_slice()).unwrap(),
            source
        );
        entry.rehydrate_preparation().await.unwrap();
        assert!(entry.preparation.is_none());
        let id = entry.id.clone();
        manager.restore_entry(entry);
        let (found, missing) = manager.take_for_submit(&owner, &[id]);
        assert!(missing.is_empty());
        assert!(found[0].scan_before_parse);
    }
}
