use std::collections::HashMap;
use std::io::{Error, ErrorKind, Read, Result as IoResult};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use async_graphql::UploadValue;
use weaver_nzb::Nzb;

use crate::auth::CallerIdentity;
use weaver_server_core::auth::generate_api_key;
use weaver_server_core::ingest::{
    StagedSubmissionPreparation, SubmitNzbError, XZ_DECODER_MEMORY_LIMIT_BYTES,
    hash_persisted_nzb_bytes, nzb_to_submission_spec, parse_persisted_nzb_bytes,
    persist_decoded_nzb_reader_to_zstd, xz_multistream_decoder,
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
        let filename = self.filename.clone();
        let nzb_zstd = self.nzb_zstd.clone();
        let preparation = tokio::task::spawn_blocking(move || {
            let nzb = match parse_persisted_nzb_bytes(&nzb_zstd) {
                Ok(nzb) => nzb,
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
            let job_hash = hash_persisted_nzb_bytes(&nzb_zstd);
            Ok(staged_preparation_from_nzb(&nzb, &filename, job_hash))
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
    pub(crate) total_files: u32,
    pub(crate) total_bytes: u64,
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
    ) -> Result<StagedUploadSummary, SubmitNzbError> {
        let filename = filename_override
            .filter(|value| !value.trim().is_empty())
            .or_else(|| (!upload.filename.trim().is_empty()).then_some(upload.filename.clone()))
            .unwrap_or_else(|| "upload.nzb".to_string());
        let source = normalize_uploaded_nzb_reader(upload)?;
        let persist_result = tokio::task::spawn_blocking(move || {
            let mut source = source;
            persist_decoded_nzb_reader_to_zstd(&mut source)
        })
        .await
        .map_err(|error| SubmitNzbError::Upload(std::io::Error::other(error.to_string())))?;
        let (nzb_zstd, nzb) = match persist_result {
            Ok(values) => values,
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

        let filename_for_preparation = filename.clone();
        let job_hash = hash_persisted_nzb_bytes(&nzb_zstd);
        let preparation = tokio::task::spawn_blocking(move || {
            staged_preparation_from_nzb(&nzb, &filename_for_preparation, job_hash)
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
            total_files,
            total_bytes,
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
    let encoding = detect_upload_encoding(&upload);
    let source = LimitedReader::new(
        upload.into_read(),
        limits.nzb_upload_limit_bytes,
        "NZB upload",
    );

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
    };

    Ok(Box::new(LimitedReader::new(
        decoded,
        limits.nzb_decompressed_limit_bytes,
        "decompressed NZB",
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    use lzma_rust2::{XzOptions, XzWriter};

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

    fn make_xz_upload(filename: &str, content_type: &str, name: &str) -> UploadValue {
        let mut writer = XzWriter::new(Vec::new(), XzOptions::with_preset(0)).unwrap();
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
            .stage_upload(owner_a.clone(), make_upload("owned"), None)
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
                )
                .await
                .unwrap();
            assert_eq!(staged.filename, filename);
            assert_eq!(staged.total_files, 1);
        }
    }

    #[tokio::test]
    async fn purge_expired_removes_stale_entries() {
        let manager = StagedUploadManager::with_timing(Duration::ZERO, Duration::from_secs(60));
        let owner = CallerIdentity::Local([3; 32]);
        let staged = manager
            .stage_upload(owner.clone(), make_upload("expired"), None)
            .await
            .unwrap();

        assert_eq!(manager.purge_expired(), 1);
        let (found, missing) =
            manager.take_for_submit(&owner, std::slice::from_ref(&staged.staged_upload_id));
        assert!(found.is_empty());
        assert_eq!(missing, vec![staged.staged_upload_id]);
    }
}
