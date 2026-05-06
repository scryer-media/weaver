use std::collections::HashMap;
use std::io::Read;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use async_graphql::UploadValue;

use crate::auth::CallerIdentity;
use weaver_server_core::auth::generate_api_key;
use weaver_server_core::ingest::{
    SubmitNzbError, nzb_to_submission_spec, persist_decoded_nzb_reader_to_zstd,
};

const DEFAULT_STAGED_UPLOAD_TTL: Duration = Duration::from_secs(15 * 60);
const DEFAULT_CLEANUP_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Clone, Debug)]
pub(crate) struct StagedUploadEntry {
    pub(crate) id: String,
    pub(crate) owner: CallerIdentity,
    pub(crate) filename: String,
    pub(crate) nzb_zstd: Vec<u8>,
    created_at: Instant,
    last_touched_at: Instant,
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

        let spec = nzb_to_submission_spec(&nzb, Some(filename.as_str()), None, None, Vec::new());
        let staged_upload_id = generate_api_key();
        let now = Instant::now();
        let entry = StagedUploadEntry {
            id: staged_upload_id.clone(),
            owner,
            filename: filename.clone(),
            nzb_zstd,
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
            display_name: spec.name,
            total_files: spec.files.len() as u32,
            total_bytes: spec.total_bytes,
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
        _ => UploadEncoding::Plain,
    }
}

pub(crate) fn normalize_uploaded_nzb_reader(
    upload: UploadValue,
) -> Result<Box<dyn Read + Send>, SubmitNzbError> {
    let encoding = detect_upload_encoding(&upload);
    let source = upload.into_read();

    match encoding {
        UploadEncoding::Plain => Ok(Box::new(source)),
        UploadEncoding::Zstd => {
            let decoder =
                zstd::stream::read::Decoder::new(source).map_err(SubmitNzbError::Upload)?;
            Ok(Box::new(decoder))
        }
        UploadEncoding::Gzip => Ok(Box::new(flate2::read::GzDecoder::new(source))),
        UploadEncoding::Brotli => Ok(Box::new(brotli::Decompressor::new(source, 64 * 1024))),
        UploadEncoding::Deflate => Ok(Box::new(flate2::read::DeflateDecoder::new(source))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
