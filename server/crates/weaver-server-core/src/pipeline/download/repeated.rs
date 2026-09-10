//! Per-job reuse for repeated message IDs. Ordinary batches do not enter here.
//! Cache files are disposable; restart reestablishes article evidence normally.

use super::*;
use crate::MessageId;
use std::io::{Read, Write};
use std::sync::Weak;

pub(crate) struct SharedArticle {
    pub(in crate::pipeline) data: DecodedChunk,
    _memory: ProcessMemoryPermit,
}

struct Template {
    encoding: SegmentEncoding,
    layout: YencLayoutAssertions,
    crc_valid: bool,
    part_crc_verified: bool,
    part_crc: u32,
    expected_file_crc: Option<u32>,
    name: String,
    plan: weaver_yenc::CheckpointPlan,
    segments: Vec<weaver_yenc::Segment>,
}

impl Template {
    fn replay(&self, id: SegmentId, data: Arc<SharedArticle>) -> DecodeResult {
        DecodeResult {
            segment_id: id,
            raw_size: 0,
            encoding: self.encoding,
            yenc_layout: self.layout,
            crc_valid: self.crc_valid,
            part_crc_verified: self.part_crc_verified,
            part_crc: self.part_crc,
            expected_file_crc: self.expected_file_crc,
            data: DecodedChunk::Shared(data),
            yenc_name: self.name.clone(),
            checkpoint_plan: self.plan.clone(),
            segments: self.segments.clone(),
        }
    }
}

struct CachedBody {
    template: Template,
    path: tempfile::TempPath,
    bytes: usize,
    hash: blake3::Hash,
    resident: Weak<SharedArticle>,
    source: Option<usize>,
    generation: u64,
    _metadata: ProcessMemoryPermit,
}

impl CachedBody {
    fn store(
        mut decoded: DecodeResult,
        source: Option<usize>,
        root: &Path,
        memory: &Arc<ProcessMemoryBudget>,
        generation: u64,
    ) -> Result<(Self, DecodeResult), String> {
        let bytes = decoded.data.len_bytes();
        let metadata_bytes = std::mem::size_of::<Self>()
            + decoded.yenc_name.len() * 2
            + decoded.segments.len() * std::mem::size_of::<weaver_yenc::Segment>() * 2
            + 1024;
        let metadata = memory.try_reserve_retained(metadata_bytes as u64)?;
        let payload_memory = memory.try_reserve_retained(bytes as u64)?;
        let mut file = tempfile::NamedTempFile::new_in(root)
            .map_err(|error| format!("article cache creation failed: {error}"))?;
        let mut hash = blake3::Hasher::new();
        let mut written = Ok(());
        decoded.data.for_each_slice(|bytes| {
            if written.is_ok() {
                written = file.write_all(bytes);
            }
            hash.update(bytes);
        });
        written.map_err(|error| format!("article cache write failed: {error}"))?;
        let body = Arc::new(SharedArticle {
            data: std::mem::replace(&mut decoded.data, DecodedChunk::from(Vec::<u8>::new())),
            _memory: payload_memory,
        });
        let cached = Self {
            template: Template {
                encoding: decoded.encoding,
                layout: decoded.yenc_layout,
                crc_valid: decoded.crc_valid,
                part_crc_verified: decoded.part_crc_verified,
                part_crc: decoded.part_crc,
                expected_file_crc: decoded.expected_file_crc,
                name: decoded.yenc_name.clone(),
                plan: decoded.checkpoint_plan.clone(),
                segments: decoded.segments.clone(),
            },
            path: file.into_temp_path(),
            bytes,
            hash: hash.finalize(),
            resident: Arc::downgrade(&body),
            source,
            generation,
            _metadata: metadata,
        };
        decoded.data = DecodedChunk::Shared(body);
        Ok((cached, decoded))
    }

    fn read(
        &mut self,
        id: SegmentId,
        memory: &Arc<ProcessMemoryBudget>,
    ) -> Result<DecodeResult, String> {
        let data = if let Some(body) = self.resident.upgrade() {
            body
        } else {
            let permit = memory.try_reserve_retained(self.bytes as u64)?;
            let mut bytes = Vec::new();
            bytes
                .try_reserve_exact(self.bytes)
                .map_err(|error| format!("article cache allocation failed: {error}"))?;
            bytes.resize(self.bytes, 0);
            let mut file = std::fs::File::open(&self.path)
                .map_err(|error| format!("article cache open failed: {error}"))?;
            file.read_exact(&mut bytes)
                .map_err(|error| format!("article cache read failed: {error}"))?;
            if blake3::hash(&bytes) != self.hash {
                return Err("article cache contents changed".to_string());
            }
            let body = Arc::new(SharedArticle {
                data: DecodedChunk::from(bytes),
                _memory: permit,
            });
            self.resident = Arc::downgrade(&body);
            body
        };
        Ok(self.template.replay(id, data))
    }
}

struct CachedFailure {
    error: DownloadError,
    generation: u64,
    source: Option<usize>,
    groups: Arc<[String]>,
    requested_excludes: Vec<usize>,
    proved_excludes: Vec<usize>,
    expires: Option<Instant>,
    _memory: Option<ProcessMemoryPermit>,
}

enum Entry {
    Body(CachedBody),
    Failure(CachedFailure),
    Fatal(String),
}

pub(crate) struct RepeatedArticles {
    entries: HashMap<MessageId, Arc<tokio::sync::Mutex<Option<Entry>>>>,
    root: tempfile::TempDir,
    memory: Arc<ProcessMemoryBudget>,
    _index_memory: ProcessMemoryPermit,
}

struct Reply {
    data: Result<DownloadPayload, DownloadError>,
    attempts: Vec<weaver_nntp::client::FetchAttemptTrace>,
    source: Option<usize>,
    excludes: Vec<usize>,
}

impl RepeatedArticles {
    pub(crate) fn new(
        spec: &crate::jobs::JobSpec,
        root: &Path,
        memory: Arc<ProcessMemoryBudget>,
    ) -> Result<Option<Arc<Self>>, String> {
        let count: usize = spec.files.iter().map(|file| file.segments.len()).sum();
        let scratch = memory.try_reserve_retained((count as u64).saturating_mul(48))?;
        let mut counts = HashMap::<&str, bool>::new();
        for file in &spec.files {
            for segment in &file.segments {
                counts
                    .entry(&segment.message_id)
                    .and_modify(|repeated| *repeated = true)
                    .or_insert(false);
            }
        }
        let duplicates = counts.values().filter(|repeated| **repeated).count();
        if duplicates == 0 {
            return Ok(None);
        }
        let bytes = counts
            .iter()
            .filter(|(_, repeated)| **repeated)
            .map(|(id, _)| id.len() as u64 + 512)
            .sum();
        let index_memory = memory.try_reserve_retained(bytes)?;
        let entries = counts
            .into_iter()
            .filter(|(_, repeated)| *repeated)
            .map(|(id, _)| (MessageId::new(id), Arc::new(tokio::sync::Mutex::new(None))))
            .collect();
        drop(scratch);
        let scratch_root = root.join(".weaver-chunks");
        match std::fs::create_dir(&scratch_root) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                if !std::fs::symlink_metadata(&scratch_root)
                    .map_err(|error| error.to_string())?
                    .file_type()
                    .is_dir()
                {
                    return Err("article cache scratch path is not a directory".to_string());
                }
            }
            Err(error) => return Err(format!("article cache scratch creation failed: {error}")),
        }
        let root = tempfile::Builder::new()
            .prefix("article-cache-")
            .tempdir_in(scratch_root)
            .map_err(|error| format!("article cache directory creation failed: {error}"))?;
        Ok(Some(Arc::new(Self {
            entries,
            root,
            memory,
            _index_memory: index_memory,
        })))
    }

    async fn fetch(
        self: &Arc<Self>,
        work: &DownloadWork,
        excludes: &[usize],
        nntp: &NntpClient,
        generation: u64,
    ) -> Reply {
        let Some(entry) = self.entries.get(&work.message_id) else {
            return Self::fetch_uncached(work, excludes, nntp).await;
        };
        let mut state = entry.clone().lock_owned().await;
        match state.as_mut() {
            Some(Entry::Body(body))
                if body.generation == generation
                    && body
                        .source
                        .is_none_or(|source| !work.exclude_servers.contains(&source)) =>
            {
                let memory = self.memory.clone();
                let id = work.segment_id;
                let excludes = work.exclude_servers.clone();
                return tokio::task::spawn_blocking(move || {
                    let Some(Entry::Body(body)) = state.as_mut() else {
                        unreachable!()
                    };
                    Reply {
                        data: body
                            .read(id, &memory)
                            .map(DownloadPayload::Decoded)
                            .map_err(DownloadError::local),
                        attempts: Vec::new(),
                        source: body.source,
                        excludes: excludes.to_vec(),
                    }
                })
                .await
                .unwrap_or_else(|error| Reply {
                    data: Err(DownloadError::local(format!(
                        "article cache task failed: {error}"
                    ))),
                    attempts: Vec::new(),
                    source: None,
                    excludes: Vec::new(),
                });
            }
            Some(Entry::Failure(failure))
                if failure.generation == generation
                    && failure.groups == work.groups
                    && failure.requested_excludes == excludes
                    && failure.expires.is_none_or(|until| Instant::now() < until) =>
            {
                return Reply {
                    data: Err(failure.error.clone()),
                    attempts: Vec::new(),
                    source: failure.source,
                    excludes: failure.proved_excludes.clone(),
                };
            }
            Some(Entry::Fatal(error)) => {
                return Reply {
                    data: Err(DownloadError::local(error.clone())),
                    attempts: Vec::new(),
                    source: None,
                    excludes: work.exclude_servers.clone(),
                };
            }
            _ => {}
        }
        *state = None;
        let mut reply = Self::fetch_uncached(work, excludes, nntp).await;
        match reply.data {
            Ok(DownloadPayload::Decoded(decoded)) => {
                let cache = self.clone();
                let source = reply.source;
                let raw_size = decoded.raw_size;
                let result = tokio::task::spawn_blocking(move || {
                    match CachedBody::store(
                        decoded,
                        source,
                        cache.root.path(),
                        &cache.memory,
                        generation,
                    ) {
                        Ok((cached, decoded)) => {
                            *state = Some(Entry::Body(cached));
                            Ok(decoded)
                        }
                        Err(error) => {
                            *state = Some(Entry::Fatal(error.clone()));
                            Err(error)
                        }
                    }
                })
                .await;
                reply.data = result
                    .map_err(|error| format!("article cache task failed: {error}"))
                    .and_then(|result| result)
                    .map(DownloadPayload::Decoded)
                    .map_err(|error| DownloadError::Local { raw_size, error });
            }
            Err(ref error) => {
                let (message_bytes, raw_size) = match error {
                    DownloadError::Fetch(failure) => (failure.message.len(), 0),
                    DownloadError::Decode {
                        error, raw_size, ..
                    }
                    | DownloadError::Local { error, raw_size } => (error.len(), *raw_size),
                };
                let cost = std::mem::size_of::<CachedFailure>()
                    + message_bytes * 2
                    + (excludes.len() + reply.attempts.len() + work.exclude_servers.len()) * 32;
                let reservation = match self.memory.try_reserve_retained(cost as u64) {
                    Ok(reservation) => reservation,
                    Err(error) => {
                        *state = Some(Entry::Fatal(error.clone()));
                        reply.data = Err(DownloadError::Local { raw_size, error });
                        return reply;
                    }
                };
                let mut cached_error = error.clone();
                if let DownloadError::Decode { raw_size, .. } = &mut cached_error {
                    *raw_size = 0;
                }
                let expires = match error {
                    DownloadError::Fetch(failure)
                        if failure.kind.preserves_article_retry_budget() =>
                    {
                        Some(Instant::now() + Duration::from_millis(500))
                    }
                    _ => None,
                };
                let mut proved_excludes = work.exclude_servers.clone();
                for attempt in &reply.attempts {
                    if matches!(
                        attempt.outcome,
                        weaver_nntp::client::FetchAttemptOutcome::NotFound
                    ) && !proved_excludes.contains(&attempt.server_idx)
                    {
                        proved_excludes.push(attempt.server_idx);
                    }
                }
                *state = Some(Entry::Failure(CachedFailure {
                    error: cached_error,
                    generation,
                    source: reply.source,
                    groups: work.groups.clone(),
                    requested_excludes: excludes.to_vec(),
                    proved_excludes,
                    expires,
                    _memory: Some(reservation),
                }));
            }
            Ok(DownloadPayload::Raw(_)) => unreachable!("fused BODY fetch returns decoded data"),
        }
        reply
    }

    async fn fetch_uncached(work: &DownloadWork, excludes: &[usize], nntp: &NntpClient) -> Reply {
        let trace = nntp
            .fetch_body_decoded_with_groups_excluding_traced(
                &work.message_id.wire_form(),
                &work.groups,
                excludes,
            )
            .await;
        let (data, attempts, source) =
            Pipeline::download_data_from_decoded_trace(work.segment_id, trace);
        Reply {
            data,
            attempts,
            source,
            excludes: work.exclude_servers.clone(),
        }
    }
}

impl Pipeline {
    pub(crate) fn install_repeated_articles(
        &mut self,
        state: &crate::jobs::JobState,
    ) -> Result<(), crate::SchedulerError> {
        if let Some(cache) = RepeatedArticles::new(
            &state.spec,
            &state.working_dir,
            self.process_memory_budget.clone(),
        )
        .map_err(crate::SchedulerError::InvalidInput)?
        {
            self.repeated_articles.insert(state.job_id, cache);
        }
        Ok(())
    }

    pub(in crate::pipeline::download) fn spawn_repeated_download_batch(
        &self,
        lease: DownloadBatchLease,
        cache: Arc<RepeatedArticles>,
    ) {
        let nntp = self.nntp.clone();
        let tx = self.download_done_tx.clone();
        let parked = self.download_lane_parked_tx.clone();
        tokio::spawn(async move {
            for work in lease.works {
                let reply = cache
                    .fetch(
                        &work,
                        &lease.effective_exclude_servers,
                        &nntp,
                        lease.runtime_generation,
                    )
                    .await;
                let _ = tx
                    .send(DownloadResult {
                        segment_id: work.segment_id,
                        runtime_generation: lease.runtime_generation,
                        data: reply.data,
                        attempts: reply.attempts,
                        lane_observation: None,
                        source_server_idx: reply.source,
                        origin: DownloadResultOrigin::from_work(
                            work.is_recovery,
                            work.completion_critical,
                        ),
                        retry_count: work.retry_count,
                        exclude_servers: reply.excludes,
                        release_connection_slot: false,
                    })
                    .await;
            }
            let _ = parked
                .send(DownloadLaneParked {
                    job_id: lease.job_id,
                    mode: lease.lane_mode,
                    spillover_loan_kind: lease.spillover_loan_kind,
                    completion_critical: lease.compatibility.completion_critical,
                    reason: LaneParkReason::NoWork,
                    release_connection_slot: true,
                    release_ip_replacement_burst: false,
                })
                .await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decoded(id: SegmentId, bytes: &[u8]) -> DecodeResult {
        DecodeResult {
            segment_id: id,
            raw_size: bytes.len() as u64 + 80,
            encoding: SegmentEncoding::Yenc,
            yenc_layout: YencLayoutAssertions {
                file_size: bytes.len() as u64,
                part: None,
                total: None,
                begin: None,
                end: None,
            },
            crc_valid: true,
            part_crc_verified: true,
            part_crc: par2_rs::checksum::crc32(bytes),
            expected_file_crc: None,
            data: bytes.to_vec().into(),
            yenc_name: "payload.bin".into(),
            checkpoint_plan: weaver_yenc::CheckpointPlan::None,
            segments: Vec::new(),
        }
    }

    fn work() -> DownloadWork {
        DownloadWork {
            segment_id: SegmentId {
                file_id: NzbFileId {
                    job_id: JobId(1),
                    file_index: 0,
                },
                segment_number: 0,
            },
            message_id: MessageId::new("repeated@example.test"),
            groups: Arc::from(["alt.binaries.test".into()]),
            priority: 3,
            byte_estimate: 64,
            retry_count: 0,
            is_recovery: false,
            completion_critical: false,
            exclude_servers: Vec::new(),
            avoid_server: None,
        }
    }

    #[test]
    fn cached_body_shares_memory_then_reloads_verified_disk_bytes() {
        let root = tempfile::tempdir().unwrap();
        let memory = Arc::new(ProcessMemoryBudget::new(1 << 20));
        let id = work().segment_id;
        let bytes = [73; 4096];
        let (mut cache, first) =
            CachedBody::store(decoded(id, &bytes), Some(0), root.path(), &memory, 7).unwrap();
        let charged = memory.reserved_bytes();
        let next_id = SegmentId {
            file_id: NzbFileId {
                file_index: 1,
                ..id.file_id
            },
            ..id
        };
        let second = cache.read(next_id, &memory).unwrap();
        let (DecodedChunk::Shared(a), DecodedChunk::Shared(b)) = (&first.data, &second.data) else {
            panic!("shared payloads")
        };
        assert!(Arc::ptr_eq(a, b));
        assert_eq!(second.segment_id, next_id);
        assert_eq!(second.raw_size, 0);
        assert_eq!(memory.reserved_bytes(), charged);
        drop(first);
        drop(second);
        assert_eq!(memory.reserved_bytes(), charged - bytes.len() as u64);
        let restored = cache.read(next_id, &memory).unwrap();
        let mut actual = Vec::new();
        restored.data.write_to(&mut actual).unwrap();
        assert_eq!(actual, bytes);
        drop(cache);
        assert_eq!(memory.reserved_bytes(), bytes.len() as u64);
        drop(restored);
        assert_eq!(memory.reserved_bytes(), 0);
    }

    #[test]
    fn changed_cache_bytes_are_never_replayed_as_verified() {
        let root = tempfile::tempdir().unwrap();
        let memory = Arc::new(ProcessMemoryBudget::new(1 << 20));
        let id = work().segment_id;
        let (mut cache, first) =
            CachedBody::store(decoded(id, b"original"), Some(0), root.path(), &memory, 7).unwrap();
        drop(first);
        let metadata = memory.reserved_bytes();
        std::fs::write(&cache.path, b"modified").unwrap();
        assert!(
            cache
                .read(id, &memory)
                .err()
                .unwrap()
                .contains("contents changed")
        );
        assert_eq!(memory.reserved_bytes(), metadata);
        drop(cache);
        assert_eq!(memory.reserved_bytes(), 0);
    }

    #[test]
    fn cache_reservation_failure_releases_partial_charges() {
        let root = tempfile::tempdir().unwrap();
        let memory = Arc::new(ProcessMemoryBudget::new(4096));
        assert!(
            CachedBody::store(
                decoded(work().segment_id, &[0; 4096]),
                Some(0),
                root.path(),
                &memory,
                7
            )
            .is_err()
        );
        assert_eq!(memory.reserved_bytes(), 0);
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn shared_missing_evidence_keeps_its_provider_scope_and_generation() {
        let root = tempfile::tempdir().unwrap();
        let memory = Arc::new(ProcessMemoryBudget::new(1 << 20));
        let work = work();
        let failure = CachedFailure {
            error: DownloadError::from_nntp(weaver_nntp::NntpError::ArticleNotFound),
            generation: 7,
            source: Some(0),
            groups: work.groups.clone(),
            requested_excludes: Vec::new(),
            proved_excludes: vec![0],
            expires: None,
            _memory: None,
        };
        let cache = Arc::new(RepeatedArticles {
            entries: HashMap::from([(
                work.message_id.clone(),
                Arc::new(tokio::sync::Mutex::new(Some(Entry::Failure(failure)))),
            )]),
            root,
            _index_memory: memory.try_reserve_retained(0).unwrap(),
            memory,
        });
        let client = NntpClient::new(weaver_nntp::client::NntpClientConfig {
            servers: Vec::new(),
            max_idle_age: Duration::from_secs(1),
            max_retries_per_server: 1,
            soft_timeout: Duration::from_secs(1),
        });
        let reply = cache.fetch(&work, &[], &client, 7).await;
        assert!(matches!(
            reply.data,
            Err(DownloadError::Fetch(DownloadFailure {
                kind: DownloadFailureKind::ArticleNotFound,
                ..
            }))
        ));
        assert_eq!(reply.excludes, vec![0]);
        assert!(reply.attempts.is_empty());
        {
            let mut state = cache.entries[&work.message_id].lock().await;
            let Some(Entry::Failure(failure)) = state.as_mut() else {
                panic!("cached failure")
            };
            failure.requested_excludes = vec![1];
        }
        let reply = cache.fetch(&work, &[1], &client, 7).await;
        assert_eq!(
            reply.excludes,
            vec![0],
            "a transport avoidance hint is not a missing-article proof"
        );
        let reply = cache.fetch(&work, &[], &client, 8).await;
        assert!(matches!(
            reply.data,
            Err(DownloadError::Fetch(DownloadFailure {
                kind: DownloadFailureKind::CapacityUnavailable,
                ..
            }))
        ));
    }
}
