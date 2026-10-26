// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Per-job reuse for repeated message IDs. Ordinary batches do not enter here.
// Cache files are disposable; restart reestablishes article evidence normally.

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
    truncation_suspected: bool,
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
            truncation_suspected: self.truncation_suspected,
            expected_file_crc: self.expected_file_crc,
            data: DecodedChunk::Shared(data),
            yenc_name: self.name.clone(),
            checkpoint_plan: self.plan.clone(),
            segments: self.segments.clone(),
        }
    }
}

// The cache's directory. A job rename moves it with the working directory,
// so the live path is held here, behind one lock, rather than in a
// `tempfile::TempDir`: those paths are immutable, and a stale one would
// both miss the moved files and, on drop, remove whatever later took the
// old path. Every cached body names its file relative to this root, so a
// relocation is the one rewrite below and no entry has to be visited.
struct CacheRoot {
    path: std::sync::RwLock<PathBuf>,
}

impl CacheRoot {
    fn new(path: PathBuf) -> Arc<Self> {
        Arc::new(Self {
            path: std::sync::RwLock::new(path),
        })
    }

    fn path(&self) -> PathBuf {
        self.path
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    fn relocate(&self, old: &Path, new: &Path) {
        let mut path = self
            .path
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Ok(relative) = path.strip_prefix(old) {
            *path = new.join(relative);
        }
    }
}

impl Drop for CacheRoot {
    fn drop(&mut self) {
        let path = self
            .path
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _ = std::fs::remove_dir_all(path);
    }
}

struct CachedBody {
    template: Template,
    root: Arc<CacheRoot>,
    name: std::ffi::OsString,
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
        root: &Arc<CacheRoot>,
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
        let mut file = tempfile::NamedTempFile::new_in(root.path())
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
        let path = file
            .into_temp_path()
            .keep()
            .map_err(|error| format!("article cache keep failed: {error}"))?;
        let name = path
            .file_name()
            .expect("a cache file always has a name")
            .to_os_string();
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
                truncation_suspected: decoded.truncation_suspected,
                expected_file_crc: decoded.expected_file_crc,
                name: decoded.yenc_name.clone(),
                plan: decoded.checkpoint_plan.clone(),
                segments: decoded.segments.clone(),
            },
            root: root.clone(),
            name,
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
            let mut file = std::fs::File::open(self.path())
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

    fn path(&self) -> PathBuf {
        self.root.path().join(&self.name)
    }
}

impl Drop for CachedBody {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(self.path());
    }
}

struct CachedFailure {
    error: DownloadError,
    generation: u64,
    source: Option<usize>,
    groups: Arc<[String]>,
    requested_excludes: Vec<usize>,
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
    root: Arc<CacheRoot>,
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
            for id in file
                .segments
                .iter()
                .flat_map(|segment| segment.message_ids.all())
            {
                counts
                    .entry(id)
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
            .map_err(|error| format!("article cache directory creation failed: {error}"))?
            .keep();
        let root = CacheRoot::new(root);
        Ok(Some(Arc::new(Self {
            entries,
            root,
            memory,
            _index_memory: index_memory,
        })))
    }

    // The job's working directory moved from `old` to `new` with this cache
    // inside it. The caller has proven the job idle, so no read or store is
    // between resolving a path and using it.
    pub(crate) fn relocate_working_dir(&self, old: &Path, new: &Path) {
        self.root.relocate(old, new);
    }

    #[cfg(test)]
    async fn fetch(
        self: &Arc<Self>,
        work: &DownloadWork,
        excludes: &[usize],
        nntp: &NntpClient,
        generation: u64,
    ) -> Reply {
        self.fetch_guarded(work, excludes, nntp, generation, || true)
            .await
    }

    async fn fetch_guarded(
        self: &Arc<Self>,
        work: &DownloadWork,
        excludes: &[usize],
        nntp: &NntpClient,
        generation: u64,
        ready: impl Fn() -> bool,
    ) -> Reply {
        let obsolete = || Reply {
            data: Err(DownloadError::Fetch(DownloadFailure::new(
                DownloadFailureKind::Unrequested,
                "duplicate article reservation became obsolete",
            ))),
            attempts: Vec::new(),
            source: None,
            excludes: work.exclude_servers.clone(),
        };
        if !ready() {
            return obsolete();
        }
        let Some(entry) = self.entries.get(&work.message_id) else {
            return Self::fetch_uncached(work, excludes, nntp).await;
        };
        let mut state = entry.clone().lock_owned().await;
        if !ready() {
            return obsolete();
        }
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
                // The replay reports the article's own ledger, exactly as
                // the fetch that produced the entry did, and carries the
                // proof as `source`. Folding the source into the ledger here
                // would hand completion a set that already names the refusing
                // server, which reads as a retry that learned nothing and
                // books the segment after a single server's 430.
                return Reply {
                    data: Err(failure.error.clone()),
                    attempts: Vec::new(),
                    source: failure.source,
                    excludes: work.exclude_servers.clone(),
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
                    match CachedBody::store(decoded, source, &cache.root, &cache.memory, generation)
                    {
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
                *state = Some(Entry::Failure(CachedFailure {
                    error: cached_error,
                    generation,
                    source: reply.source,
                    groups: work.groups.clone(),
                    requested_excludes: excludes.to_vec(),
                    expires,
                    _memory: Some(reservation),
                }));
            }
            Ok(DownloadPayload::Raw(_)) => unreachable!("fused BODY fetch returns decoded data"),
        }
        reply
    }

    // The one fetch this cache cannot replay, and the last request in the
    // pipeline still issued through an async connection.
    //
    // On a server the owned download lanes serve, this acquire refuses to
    // queue behind them: it comes straight back as a capacity refusal rather
    // than spending the whole soft-timeout budget waiting for an article
    // transfer to end. That refusal is local capacity, never article
    // evidence, so the work requeues without spending the segment's retry
    // budget and the short-lived cached failure expires on its own. No
    // article is lost; the duplicates simply wait for a lane to free up.
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
        // The one wire fetch here, credited exactly as a lane credits one.
        if let Some(counters) = &work.counters {
            super::owned_lane::credit_transfer(counters, &data);
        }
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
            self.process_memory_budget.for_job(state.job_id.0),
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
        let state = self.shared_state.clone();
        let tx = self.download_done_tx.clone();
        let parked = self.download_lane_parked_tx.clone();
        tokio::spawn(async move {
            for work in lease.works {
                // The article's own failure ledger, unioned with the job's
                // retention exclusions from the lease. A batch is cut for one
                // job, not for one exclusion set, so the ledger has to come
                // from the work: fetching on the lease's set alone re-asks a
                // server this article has already been refused by, and the
                // result then reports a set that never grows.
                let article_excludes = Pipeline::union_exclude_servers(
                    &work.exclude_servers,
                    &lease.effective_exclude_servers,
                );
                let excludes =
                    Pipeline::union_exclude_servers(&article_excludes, &lease.dial_exclude_servers);
                let guard_state = state.clone();
                let guard_pool = Arc::clone(nntp.pool());
                let guard_excludes = article_excludes.clone();
                let job_id = lease.job_id;
                let generation = lease.runtime_generation;
                let estimate = Pipeline::bandwidth_reservation_estimate(work.byte_estimate);
                let guarded = nntp.with_work_guard(move |server| {
                    super::owned_lane::job_work_is_current(&guard_state, job_id, generation)
                        && matches!(
                            guard_pool.provider_policy().eligibility(
                                server.0,
                                &guard_excludes,
                                &[],
                                estimate
                            ),
                            weaver_nntp::provider_policy::ArticleEligibility::Eligible(_)
                        )
                });
                let reply = cache
                    .fetch_guarded(&work, &excludes, &guarded, lease.runtime_generation, || {
                        super::owned_lane::job_work_is_current(
                            &state,
                            lease.job_id,
                            lease.runtime_generation,
                        ) && {
                            let policy = nntp.pool().provider_policy();
                            (0..nntp.pool().server_count()).any(|server| {
                                !lease.dial_exclude_servers.contains(&server)
                                    && matches!(
                                        policy.eligibility(
                                            server,
                                            &article_excludes,
                                            &[],
                                            Pipeline::bandwidth_reservation_estimate(
                                                work.byte_estimate
                                            )
                                        ),
                                        weaver_nntp::provider_policy::ArticleEligibility::Eligible(
                                            _
                                        )
                                    )
                            })
                        }
                    })
                    .await;
                let _ = tx
                    .send(DownloadResult {
                        lane_id: lease.lane_id,
                        job_id: lease.job_id,
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
                        exclude_servers: Pipeline::union_exclude_servers(
                            &reply.excludes,
                            &lease.effective_exclude_servers,
                        ),
                        release_connection_slot: false,
                    })
                    .await;
            }
            let _ = parked
                .send(DownloadLaneParked {
                    lane_id: lease.lane_id,
                    job_id: lease.job_id,
                    mode: lease.lane_mode,
                    completion_critical: lease.completion_critical,
                    reason: LaneParkReason::NoWork,
                    release_connection_slot: true,
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
            truncation_suspected: false,
            expected_file_crc: None,
            data: bytes.to_vec().into(),
            yenc_name: "payload.bin".into(),
            checkpoint_plan: weaver_yenc::CheckpointPlan::None,
            segments: Vec::new(),
        }
    }

    fn work() -> DownloadWork {
        DownloadWork {
            counters: None,
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
        let root = CacheRoot::new(tempfile::tempdir().unwrap().keep());
        let memory = Arc::new(ProcessMemoryBudget::new(1 << 20));
        let id = work().segment_id;
        let bytes = [73; 4096];
        let (mut cache, first) =
            CachedBody::store(decoded(id, &bytes), Some(0), &root, &memory, 7).unwrap();
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

    // A rename of an idle job moves its working directory, the cache with
    // it. The bodies already cached must read back from the new place and
    // the next store must land there too; nothing may still name the old
    // directory, and dropping the cache cleans the new one up.
    #[test]
    fn a_renamed_working_dir_carries_the_cache_and_every_cached_body() {
        let base = tempfile::tempdir().unwrap();
        let old = base.path().join("old name");
        let new = base.path().join("new name");
        let dir = old.join(".weaver-chunks").join("article-cache-test");
        std::fs::create_dir_all(&dir).unwrap();
        let memory = Arc::new(ProcessMemoryBudget::new(1 << 20));
        let cache = RepeatedArticles {
            entries: HashMap::new(),
            root: CacheRoot::new(dir),
            _index_memory: memory.try_reserve_retained(0).unwrap(),
            memory: memory.clone(),
        };
        let id = work().segment_id;
        let (mut body, first) =
            CachedBody::store(decoded(id, b"before"), Some(0), &cache.root, &memory, 7).unwrap();
        drop(first);

        std::fs::rename(&old, &new).unwrap();
        cache.relocate_working_dir(&old, &new);

        let moved = new.join(".weaver-chunks").join("article-cache-test");
        assert_eq!(cache.root.path(), moved);
        assert_eq!(body.path().parent(), Some(moved.as_path()));
        let restored = body.read(id, &memory).unwrap();
        let mut actual = Vec::new();
        restored.data.write_to(&mut actual).unwrap();
        assert_eq!(actual, b"before");
        let (after, _replayed) =
            CachedBody::store(decoded(id, b"after"), Some(0), &cache.root, &memory, 7).unwrap();
        assert_eq!(after.path().parent(), Some(moved.as_path()));
        assert_eq!(std::fs::read(after.path()).unwrap(), b"after");

        drop((body, after, restored, _replayed));
        drop(cache);
        assert!(!moved.exists());
        assert!(!old.exists());
    }

    #[test]
    fn a_shared_article_hands_over_its_payload_as_views_of_the_one_body() {
        let root = CacheRoot::new(tempfile::tempdir().unwrap().keep());
        let memory = Arc::new(ProcessMemoryBudget::new(1 << 20));
        let id = work().segment_id;
        let bytes = [91u8; 2048];
        let (_cache, replayed) =
            CachedBody::store(decoded(id, &bytes), Some(0), &root, &memory, 7).unwrap();
        let DecodedChunk::Shared(body) = &replayed.data else {
            panic!("a stored body replays shared")
        };
        let pieces = replayed.data.pieces();
        let inner = body.data.pieces();
        assert_eq!(pieces.len(), 1);
        assert_eq!(pieces[0].as_ref(), bytes.as_slice());
        assert_eq!(
            pieces[0].as_ptr(),
            inner[0].as_ptr(),
            "a shared article hands out the body's own buffer, never a copy"
        );
    }

    #[test]
    fn changed_cache_bytes_are_never_replayed_as_verified() {
        let root = CacheRoot::new(tempfile::tempdir().unwrap().keep());
        let memory = Arc::new(ProcessMemoryBudget::new(1 << 20));
        let id = work().segment_id;
        let (mut cache, first) =
            CachedBody::store(decoded(id, b"original"), Some(0), &root, &memory, 7).unwrap();
        drop(first);
        let metadata = memory.reserved_bytes();
        std::fs::write(cache.path(), b"modified").unwrap();
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
        let root = CacheRoot::new(tempfile::tempdir().unwrap().keep());
        let memory = Arc::new(ProcessMemoryBudget::new(4096));
        assert!(
            CachedBody::store(
                decoded(work().segment_id, &[0; 4096]),
                Some(0),
                &root,
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
        let root = CacheRoot::new(tempfile::tempdir().unwrap().keep());
        let memory = Arc::new(ProcessMemoryBudget::new(1 << 20));
        let work = work();
        let failure = CachedFailure {
            error: DownloadError::from_nntp(weaver_nntp::NntpError::ArticleNotFound),
            generation: 7,
            source: Some(0),
            groups: work.groups.clone(),
            requested_excludes: Vec::new(),
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
        assert_eq!(
            reply.excludes,
            Vec::<usize>::new(),
            "the replay reports the article's own ledger; the proof rides as the source"
        );
        assert_eq!(reply.source, Some(0));
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
            Vec::<usize>::new(),
            "a transport avoidance hint is not a missing-article proof"
        );
        assert_eq!(reply.source, Some(0));
        let reply = cache.fetch(&work, &[], &client, 8).await;
        assert!(matches!(
            reply.data,
            Err(DownloadError::Fetch(DownloadFailure {
                kind: DownloadFailureKind::CapacityUnavailable,
                ..
            }))
        ));
    }

    // "Every connection is busy" is local capacity, not article evidence.
    // A refusal that arrives without waiting must never latch as this
    // message id's answer: it has to expire so the duplicates come back, and
    // it has to leave the segment's retry budget alone.
    #[tokio::test]
    async fn a_capacity_refusal_expires_and_keeps_the_retry_budget() {
        let root = CacheRoot::new(tempfile::tempdir().unwrap().keep());
        let memory = Arc::new(ProcessMemoryBudget::new(1 << 20));
        let work = work();
        let stale = CachedFailure {
            error: DownloadError::from_nntp(weaver_nntp::NntpError::AcquireTimeout(0)),
            generation: 7,
            source: None,
            groups: work.groups.clone(),
            requested_excludes: Vec::new(),
            expires: Some(Instant::now() - Duration::from_millis(1)),
            _memory: None,
        };
        let cache = Arc::new(RepeatedArticles {
            entries: HashMap::from([(
                work.message_id.clone(),
                Arc::new(tokio::sync::Mutex::new(Some(Entry::Failure(stale)))),
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
        // The expired entry is discarded rather than replayed, so this reply
        // is the fresh refusal from the fetch path.
        let reply = cache.fetch(&work, &[], &client, 7).await;
        let Err(DownloadError::Fetch(failure)) = &reply.data else {
            panic!("the expired entry must be refetched into a capacity refusal")
        };
        assert_eq!(failure.kind, DownloadFailureKind::CapacityUnavailable);
        assert!(
            failure.kind.preserves_article_retry_budget(),
            "a busy-lane refusal must requeue without spending the segment's retries"
        );
        let mut state = cache.entries[&work.message_id].lock().await;
        let Some(Entry::Failure(cached)) = state.as_mut() else {
            panic!("cached failure")
        };
        assert!(
            cached.expires.is_some_and(|until| until > Instant::now()),
            "a capacity refusal must be cached only briefly, never latched"
        );
    }
}
