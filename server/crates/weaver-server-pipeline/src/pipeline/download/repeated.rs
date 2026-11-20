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

// The cache's directory, removed once the cache and every body cut from it
// are gone. Every cached body names its file relative to this root.
struct CacheRoot {
    path: PathBuf,
}

impl CacheRoot {
    fn new(path: PathBuf) -> Arc<Self> {
        Arc::new(Self { path })
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for CacheRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
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

// A fetch's failure, replayed only to a reference asking the same server
// with the same failure ledger: the server the lane fetching it was pinned
// to, and the article's own ledger with the job's retention exclusions.
struct CachedFailure {
    error: DownloadError,
    generation: u64,
    source: Option<usize>,
    groups: Arc<[String]>,
    server: usize,
    requested_excludes: weaver_nntp::ServerSet,
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
    // How many lookups found the claim held and waited for it.
    #[cfg(test)]
    waits: (std::sync::Mutex<usize>, std::sync::Condvar),
}

// A reference answered from the cache: no request went out for it, so it
// carries no attempts, and its source is the server whose answer it repeats.
pub(in crate::pipeline::download) struct Reply {
    pub(in crate::pipeline::download) data: Result<DownloadPayload, DownloadError>,
    pub(in crate::pipeline::download) source: Option<usize>,
}

// What the cache says about one reference to a shared article.
pub(in crate::pipeline::download) enum SharedLookup {
    Replay(Reply),
    // Nothing to replay: the caller fetches the article and settles the
    // claim with the result. Every other reference to it waits on the claim.
    Fetch(SharedClaim),
    // The work stopped being current while the lookup waited: nothing is
    // replayed or fetched for it.
    Obsolete,
    // Asked without waiting: the article's answer is not in the cache yet,
    // or another fetch of it is in flight.
    NotReady,
}

// The one fetch of a shared article that is in flight. Holding it holds the
// article's entry, so it is settled, or dropped, before its holder waits on
// anything else.
pub(in crate::pipeline::download) struct SharedClaim {
    state: tokio::sync::OwnedMutexGuard<Option<Entry>>,
    cache: Arc<RepeatedArticles>,
    segment_id: SegmentId,
    generation: u64,
    groups: Arc<[String]>,
    server: usize,
    requested_excludes: weaver_nntp::ServerSet,
}

impl RepeatedArticles {
    // The index of a job whose articles are being built, charged as it
    // stands and never refused, like everything else built then.
    pub(crate) fn new(
        spec: &crate::jobs::JobSpec,
        root: &Path,
        memory: Arc<ProcessMemoryBudget>,
    ) -> Result<Option<Arc<Self>>, String> {
        let count: usize = spec.files.iter().map(|file| file.segments.len()).sum();
        let scratch = memory.charge_retained((count as u64).saturating_mul(48));
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
        let index_memory = memory.charge_retained(bytes);
        let entries = counts
            .into_iter()
            .filter(|(_, repeated)| *repeated)
            .map(|(id, _)| (MessageId::new(id), Arc::new(tokio::sync::Mutex::new(None))))
            .collect();
        drop(scratch);
        // Beside the job's own files, not among them: the cache is this
        // process's, and no checkpoint names it.
        let scratch_root = root.join(weaver_graph::names::CACHE_DIR);
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
            #[cfg(test)]
            waits: Default::default(),
        })))
    }

    // Whether `id` is one of the articles several references share.
    pub(in crate::pipeline::download) fn shares(&self, id: &MessageId) -> bool {
        self.entries.contains_key(id)
    }

    // Answer one reference from the cache, or hand back the claim on its one
    // fetch. `server` is the server the caller would fetch it from, and
    // `requested_excludes` the article's failure ledger with the job's
    // retention exclusions; a cached failure is replayed only to the same
    // request. `None` for an article no other reference shares.
    //
    // With `wait`, blocks while another caller holds the claim, so it is
    // asked so only on a thread that may wait, holding no other claim: a
    // download lane, never the async runtime or the pipeline loop. Without
    // it, only a replay is answered, and anything else is `NotReady`.
    // `current` is asked once the entry is held, before anything is replayed
    // or claimed: a wait can outlast the work.
    pub(in crate::pipeline::download) fn lookup(
        self: &Arc<Self>,
        work: &DownloadWork,
        server: usize,
        requested_excludes: weaver_nntp::ServerSet,
        generation: u64,
        wait: bool,
        current: impl FnOnce() -> bool,
    ) -> Option<SharedLookup> {
        let entry = self.entries.get(&work.message_id)?;
        let mut state = match Arc::clone(entry).try_lock_owned() {
            Ok(state) => state,
            Err(_) if !wait => return Some(SharedLookup::NotReady),
            Err(_) => {
                #[cfg(test)]
                {
                    let (waits, changed) = &self.waits;
                    *waits.lock().unwrap() += 1;
                    changed.notify_all();
                }
                Arc::clone(entry).blocking_lock_owned()
            }
        };
        if !current() {
            return Some(SharedLookup::Obsolete);
        }
        match state.as_mut() {
            Some(Entry::Body(body))
                if body.generation == generation
                    && body
                        .source
                        .is_none_or(|source| !work.exclude_servers.contains(source)) =>
            {
                return Some(SharedLookup::Replay(Reply {
                    data: body
                        .read(work.segment_id, &self.memory)
                        .map(DownloadPayload::Decoded)
                        .map_err(DownloadError::local),
                    source: body.source,
                }));
            }
            Some(Entry::Failure(failure))
                if failure.generation == generation
                    && failure.groups == work.groups
                    && failure.server == server
                    && failure.requested_excludes == requested_excludes
                    && failure.expires.is_none_or(|until| Instant::now() < until) =>
            {
                // The replay reports the article's own ledger, exactly as
                // the fetch that produced the entry did, and carries the
                // proof as `source`. Folding the source into the ledger here
                // would hand completion a set that already names the refusing
                // server, which reads as a retry that learned nothing and
                // books the segment after a single server's 430.
                return Some(SharedLookup::Replay(Reply {
                    data: Err(failure.error.clone()),
                    source: failure.source,
                }));
            }
            Some(Entry::Fatal(error)) => {
                return Some(SharedLookup::Replay(Reply {
                    data: Err(DownloadError::local(error.clone())),
                    source: None,
                }));
            }
            _ => {}
        }
        if !wait {
            return Some(SharedLookup::NotReady);
        }
        *state = None;
        Some(SharedLookup::Fetch(SharedClaim {
            state,
            cache: Arc::clone(self),
            segment_id: work.segment_id,
            generation,
            groups: work.groups.clone(),
            server,
            requested_excludes,
        }))
    }

    // Wait until `count` lookups have found the claim held.
    #[cfg(test)]
    fn wait_for_waiters(&self, count: usize) {
        let (waits, changed) = &self.waits;
        let mut seen = waits.lock().unwrap();
        while *seen < count {
            seen = changed.wait(seen).unwrap();
        }
    }
}

// Whether a failed fetch is the server's answer about the article, or about
// the account on that server, and so holds for every reference asking the
// same server: the article missing, its body refused or undecodable, the
// login refused. A fault of the one connection that asked (the socket lost
// or timed out, a status line never answered, a 412 the next connection
// answers by selecting a group) or a local refusal (capacity, quota, policy,
// memory) is not kept: the next reference fetches for itself.
fn is_the_servers_answer(error: &DownloadError) -> bool {
    match error {
        DownloadError::Fetch(failure) => match failure.kind {
            DownloadFailureKind::ArticleNotFound
            | DownloadFailureKind::Auth
            | DownloadFailureKind::ContentOrProtocol => true,
            // A 412 the work cannot answer by selecting a group is the
            // server's answer; one the next connection answers is not.
            DownloadFailureKind::ConnectionEstablishment => {
                failure.group_refusal == GroupRefusal::Settled
            }
            DownloadFailureKind::CapacityUnavailable
            | DownloadFailureKind::ServerQuota
            | DownloadFailureKind::LaneUnavailable
            | DownloadFailureKind::RouteCapacity
            | DownloadFailureKind::Unrequested
            | DownloadFailureKind::EstablishedTransport
            | DownloadFailureKind::Unanswered => false,
        },
        DownloadError::Decode { .. } => true,
        DownloadError::Local { .. } => false,
    }
}

impl SharedClaim {
    pub(in crate::pipeline::download) fn segment_id(&self) -> SegmentId {
        self.segment_id
    }

    // Keep what the fetch answered for the references still to come. A
    // decoded body is stored and handed back as a view of the stored copy; a
    // body the cache cannot keep fails this reference locally, and every
    // later one with it. A failure that is the server's answer is kept for
    // the same request, a refused login only briefly; any other failure
    // leaves the claim given up, as if unsettled.
    pub(in crate::pipeline::download) fn settle(
        mut self,
        data: &mut Result<DownloadPayload, DownloadError>,
        source: Option<usize>,
        attempts: usize,
    ) {
        let cache = &self.cache;
        match std::mem::replace(data, Err(DownloadError::local(String::new()))) {
            Ok(DownloadPayload::Decoded(decoded)) => {
                let raw_size = decoded.raw_size;
                *data = match CachedBody::store(
                    decoded,
                    source,
                    &cache.root,
                    &cache.memory,
                    self.generation,
                ) {
                    Ok((cached, decoded)) => {
                        *self.state = Some(Entry::Body(cached));
                        Ok(DownloadPayload::Decoded(decoded))
                    }
                    Err(error) => {
                        *self.state = Some(Entry::Fatal(error.clone()));
                        Err(DownloadError::Local { raw_size, error })
                    }
                };
            }
            Err(error) if !is_the_servers_answer(&error) => *data = Err(error),
            Err(error) => {
                let (message_bytes, raw_size) = match &error {
                    DownloadError::Fetch(failure) => (failure.message.len(), 0),
                    DownloadError::Decode {
                        error, raw_size, ..
                    }
                    | DownloadError::Local { error, raw_size } => (error.len(), *raw_size),
                };
                let cost = std::mem::size_of::<CachedFailure>()
                    + message_bytes * 2
                    + (self.requested_excludes.len() + attempts + 1) * 32;
                let reservation = match cache.memory.try_reserve_retained(cost as u64) {
                    Ok(reservation) => reservation,
                    Err(reserve) => {
                        *self.state = Some(Entry::Fatal(reserve.clone()));
                        *data = Err(DownloadError::Local {
                            raw_size,
                            error: reserve,
                        });
                        return;
                    }
                };
                let mut cached_error = error.clone();
                if let DownloadError::Decode { raw_size, .. } = &mut cached_error {
                    *raw_size = 0;
                }
                let expires = match &error {
                    DownloadError::Fetch(failure)
                        if failure.kind.preserves_article_retry_budget() =>
                    {
                        Some(Instant::now() + Duration::from_millis(500))
                    }
                    _ => None,
                };
                *self.state = Some(Entry::Failure(CachedFailure {
                    error: cached_error,
                    generation: self.generation,
                    source,
                    groups: self.groups.clone(),
                    server: self.server,
                    requested_excludes: self.requested_excludes,
                    expires,
                    _memory: Some(reservation),
                }));
                *data = Err(error);
            }
            #[cfg(test)]
            Ok(raw @ DownloadPayload::Raw(_)) => *data = Ok(raw),
        }
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
            exclude_servers: weaver_nntp::ServerSet::EMPTY,
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
        let mut pieces = Vec::new();
        replayed
            .data
            .for_each_slice(|piece| pieces.push((piece.as_ptr(), piece.to_vec())));
        let mut inner = Vec::new();
        body.data.for_each_slice(|piece| inner.push(piece.as_ptr()));
        assert_eq!(pieces.len(), 1);
        assert_eq!(pieces[0].1, bytes);
        assert_eq!(
            pieces[0].0, inner[0],
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

    impl RepeatedArticles {
        // A lookup by a lane that may wait, for work that stays current.
        fn ask(
            self: &Arc<Self>,
            work: &DownloadWork,
            server: usize,
            requested_excludes: weaver_nntp::ServerSet,
            generation: u64,
        ) -> Option<SharedLookup> {
            self.lookup(work, server, requested_excludes, generation, true, || true)
        }
    }

    fn cache_holding(work: &DownloadWork, entry: Option<Entry>) -> Arc<RepeatedArticles> {
        let root = CacheRoot::new(tempfile::tempdir().unwrap().keep());
        let memory = Arc::new(ProcessMemoryBudget::new(1 << 20));
        Arc::new(RepeatedArticles {
            entries: HashMap::from([(
                work.message_id.clone(),
                Arc::new(tokio::sync::Mutex::new(entry)),
            )]),
            root,
            _index_memory: memory.try_reserve_retained(0).unwrap(),
            memory,
            waits: Default::default(),
        })
    }

    fn reference(work: &DownloadWork, file_index: u32) -> DownloadWork {
        DownloadWork {
            segment_id: SegmentId {
                file_id: NzbFileId {
                    file_index,
                    ..work.segment_id.file_id
                },
                ..work.segment_id
            },
            ..work.clone()
        }
    }

    fn replay(lookup: Option<SharedLookup>) -> Reply {
        match lookup {
            Some(SharedLookup::Replay(reply)) => reply,
            _ => panic!("expected a replay of a shared article"),
        }
    }

    fn claim(lookup: Option<SharedLookup>) -> SharedClaim {
        match lookup {
            Some(SharedLookup::Fetch(claim)) => claim,
            _ => panic!("expected a claim on a shared article"),
        }
    }

    #[test]
    fn an_article_no_other_reference_shares_is_not_looked_up() {
        let work = work();
        let cache = cache_holding(&work, None);
        let other = DownloadWork {
            message_id: MessageId::new("alone@example.test"),
            ..work.clone()
        };
        assert!(cache.shares(&work.message_id));
        assert!(!cache.shares(&other.message_id));
        assert!(
            cache
                .ask(&other, 0, weaver_nntp::ServerSet::EMPTY, 7)
                .is_none()
        );
    }

    // One fetch answers every reference: the first takes the claim, a second
    // asked while the fetch is out waits for it, and each one after is a
    // view of the one stored body, crediting no wire bytes.
    #[test]
    fn one_fetch_serves_every_reference() {
        let work = work();
        let cache = cache_holding(&work, None);
        let first = claim(cache.ask(&work, 0, weaver_nntp::ServerSet::EMPTY, 7));
        assert_eq!(first.segment_id(), work.segment_id);
        let waiting = {
            let cache = Arc::clone(&cache);
            let second = reference(&work, 1);
            std::thread::spawn(move || {
                replay(cache.ask(&second, 0, weaver_nntp::ServerSet::EMPTY, 7))
            })
        };
        // The second reference found the claim held and waits on it.
        cache.wait_for_waiters(1);
        assert!(
            !waiting.is_finished(),
            "a waiter is answered only by the settle"
        );
        let bytes = [42u8; 1024];
        let mut data = Ok(DownloadPayload::Decoded(decoded(work.segment_id, &bytes)));
        first.settle(&mut data, Some(0), 1);
        let Ok(DownloadPayload::Decoded(fetched)) = &data else {
            panic!("the fetched reference keeps its body")
        };
        assert_eq!(fetched.raw_size, bytes.len() as u64 + 80);
        let DecodedChunk::Shared(body) = &fetched.data else {
            panic!("the fetched reference is a view of the stored body")
        };
        let second = waiting.join().unwrap();
        let mut replies = vec![second];
        for file_index in 2..4 {
            replies.push(replay(cache.ask(
                &reference(&work, file_index),
                1,
                weaver_nntp::ServerSet::EMPTY,
                7,
            )));
        }
        for (index, reply) in replies.into_iter().enumerate() {
            assert_eq!(reply.source, Some(0));
            let Ok(DownloadPayload::Decoded(replayed)) = reply.data else {
                panic!("every reference replays the body")
            };
            assert_eq!(replayed.segment_id.file_id.file_index, index as u32 + 1);
            assert_eq!(replayed.raw_size, 0, "a replay is no new transfer");
            let DecodedChunk::Shared(shared) = &replayed.data else {
                panic!("a replay is shared")
            };
            assert!(Arc::ptr_eq(body, shared));
        }
        // A reference that has already been refused by the body's source,
        // or a new generation, fetches again.
        let refused = DownloadWork {
            exclude_servers: weaver_nntp::ServerSet::from([0]),
            ..reference(&work, 1)
        };
        drop(claim(cache.ask(&refused, 1, refused.exclude_servers, 7)));
        drop(claim(cache.ask(&work, 0, weaver_nntp::ServerSet::EMPTY, 8)));
    }

    // A claim dropped without a result leaves nothing behind: the next
    // reference fetches.
    #[test]
    fn a_claim_given_up_unsettled_leaves_the_next_reference_to_fetch() {
        let work = work();
        let cache = cache_holding(&work, None);
        drop(claim(cache.ask(&work, 0, weaver_nntp::ServerSet::EMPTY, 7)));
        drop(claim(cache.ask(
            &reference(&work, 1),
            0,
            weaver_nntp::ServerSet::EMPTY,
            7,
        )));
    }

    #[test]
    fn shared_missing_evidence_keeps_its_provider_scope_and_generation() {
        let work = work();
        let cache = cache_holding(&work, None);
        let fetch = claim(cache.ask(&work, 0, weaver_nntp::ServerSet::EMPTY, 7));
        let mut data = Err(DownloadError::from_nntp(
            weaver_nntp::NntpError::ArticleNotFound,
        ));
        fetch.settle(&mut data, Some(0), 1);
        assert!(matches!(
            data,
            Err(DownloadError::Fetch(DownloadFailure {
                kind: DownloadFailureKind::ArticleNotFound,
                ..
            }))
        ));
        let reply = replay(cache.ask(&reference(&work, 1), 0, weaver_nntp::ServerSet::EMPTY, 7));
        assert!(matches!(
            reply.data,
            Err(DownloadError::Fetch(DownloadFailure {
                kind: DownloadFailureKind::ArticleNotFound,
                ..
            }))
        ));
        assert_eq!(
            reply.source,
            Some(0),
            "the replay carries the proof as its source"
        );
        // Another server, another ledger or another generation is another
        // request: it fetches, and its claim replaces the evidence.
        drop(claim(cache.ask(&work, 1, weaver_nntp::ServerSet::EMPTY, 7)));
        let fetch = claim(cache.ask(&work, 0, weaver_nntp::ServerSet::EMPTY, 7));
        let mut data = Err(DownloadError::from_nntp(
            weaver_nntp::NntpError::ArticleNotFound,
        ));
        fetch.settle(&mut data, Some(0), 1);
        drop(claim(cache.ask(
            &work,
            0,
            weaver_nntp::ServerSet::from([1]),
            7,
        )));
        let fetch = claim(cache.ask(&work, 0, weaver_nntp::ServerSet::EMPTY, 7));
        let mut data = Err(DownloadError::from_nntp(
            weaver_nntp::NntpError::ArticleNotFound,
        ));
        fetch.settle(&mut data, Some(0), 1);
        drop(claim(cache.ask(&work, 0, weaver_nntp::ServerSet::EMPTY, 8)));
    }

    fn entry_is_empty(cache: &RepeatedArticles, work: &DownloadWork) -> bool {
        cache.entries[&work.message_id].blocking_lock().is_none()
    }

    // "Every connection is busy" is local capacity, not article evidence:
    // it is never kept, the next reference asks for itself, and the
    // reference that met it keeps the segment's retry budget.
    #[test]
    fn a_capacity_refusal_is_never_kept_and_keeps_the_retry_budget() {
        let work = work();
        let cache = cache_holding(&work, None);
        let fetch = claim(cache.ask(&work, 0, weaver_nntp::ServerSet::EMPTY, 7));
        let mut data = Err(DownloadError::from_nntp(
            weaver_nntp::NntpError::AcquireTimeout(0),
        ));
        fetch.settle(&mut data, None, 0);
        let Err(DownloadError::Fetch(failure)) = &data else {
            panic!("the refusal stays this reference's answer")
        };
        assert_eq!(failure.kind, DownloadFailureKind::CapacityUnavailable);
        assert!(
            failure.kind.preserves_article_retry_budget(),
            "a busy-lane refusal must requeue without spending the segment's retries"
        );
        assert!(entry_is_empty(&cache, &work));
        drop(claim(cache.ask(
            &reference(&work, 1),
            0,
            weaver_nntp::ServerSet::EMPTY,
            7,
        )));
    }

    // A fault of the one connection that fetched is that reference's alone:
    // nothing is kept, and the next reference on the same server fetches on
    // its own connection, so it is never handed a failure it did not meet.
    #[test]
    fn a_connection_fault_is_never_replayed_to_another_reference() {
        let faults = [
            DownloadError::from_nntp(weaver_nntp::NntpError::ServerDisconnectedMidBody),
            DownloadError::from_nntp(weaver_nntp::NntpError::ConnectionClosed),
            DownloadError::from_nntp(weaver_nntp::NntpError::Timeout),
            DownloadError::from_nntp(weaver_nntp::NntpError::SessionExpired),
            DownloadError::from_nntp(weaver_nntp::NntpError::NoGroupSelected),
            DownloadError::local("decode buffer refused".to_string()),
        ];
        for fault in faults {
            let work = work();
            let cache = cache_holding(&work, None);
            let fetch = claim(cache.ask(&work, 0, weaver_nntp::ServerSet::EMPTY, 7));
            let mut data = Err(fault.clone());
            fetch.settle(&mut data, Some(0), 1);
            let Err(kept) = &data else {
                panic!("the fault stays this reference's answer")
            };
            assert_eq!(format!("{kept:?}"), format!("{fault:?}"));
            assert!(entry_is_empty(&cache, &work), "{fault:?} is not kept");
            // The next reference on the same server fetches for itself, and
            // its body serves the rest.
            let second = reference(&work, 1);
            let fetch = claim(cache.ask(&second, 0, weaver_nntp::ServerSet::EMPTY, 7));
            let mut data = Ok(DownloadPayload::Decoded(decoded(
                second.segment_id,
                b"body",
            )));
            fetch.settle(&mut data, Some(0), 1);
            let reply =
                replay(cache.ask(&reference(&work, 2), 0, weaver_nntp::ServerSet::EMPTY, 7));
            assert!(reply.data.is_ok(), "{fault:?}");
        }
        // A 412 the work cannot answer by selecting a group is the server's
        // answer about the article, and is kept.
        let work = work();
        let cache = cache_holding(&work, None);
        let fetch = claim(cache.ask(&work, 0, weaver_nntp::ServerSet::EMPTY, 7));
        let mut refusal = DownloadFailure::from_nntp(weaver_nntp::NntpError::NoGroupSelected);
        refusal.settle_group_refusal(false, true);
        let mut data = Err(DownloadError::Fetch(refusal));
        fetch.settle(&mut data, Some(0), 1);
        assert!(
            replay(cache.ask(&reference(&work, 1), 0, weaver_nntp::ServerSet::EMPTY, 7))
                .data
                .is_err()
        );
    }

    // A refused login is the account's answer on that server: it is kept,
    // only briefly, and the reference keeps the segment's retry budget. An
    // expired one is discarded rather than replayed.
    #[test]
    fn a_refused_login_is_kept_briefly_and_keeps_the_retry_budget() {
        let work = work();
        let stale = CachedFailure {
            error: DownloadError::from_nntp(weaver_nntp::NntpError::AuthenticationRejected),
            generation: 7,
            source: Some(0),
            groups: work.groups.clone(),
            server: 0,
            requested_excludes: weaver_nntp::ServerSet::EMPTY,
            expires: Some(Instant::now() - Duration::from_millis(1)),
            _memory: None,
        };
        let cache = cache_holding(&work, Some(Entry::Failure(stale)));
        let fetch = claim(cache.ask(&work, 0, weaver_nntp::ServerSet::EMPTY, 7));
        let mut data = Err(DownloadError::from_nntp(
            weaver_nntp::NntpError::AuthenticationRejected,
        ));
        let before = Instant::now();
        fetch.settle(&mut data, Some(0), 1);
        let Err(DownloadError::Fetch(failure)) = &data else {
            panic!("the refusal stays this reference's answer")
        };
        assert_eq!(failure.kind, DownloadFailureKind::Auth);
        assert!(failure.kind.preserves_article_retry_budget());
        let state = cache.entries[&work.message_id].blocking_lock();
        let Some(Entry::Failure(cached)) = state.as_ref() else {
            panic!("a refused login is kept")
        };
        assert!(
            cached
                .expires
                .is_some_and(|until| until >= before + Duration::from_millis(500)),
            "a refused login is kept only briefly, never latched"
        );
    }

    // A lookup that waited out another lane's fetch replays nothing to work
    // that stopped being current meanwhile, and leaves the entry for others.
    #[test]
    fn a_waiter_whose_work_went_stale_replays_nothing() {
        let work = work();
        let cache = cache_holding(&work, None);
        let first = claim(cache.ask(&work, 0, weaver_nntp::ServerSet::EMPTY, 7));
        let current = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let waiting = {
            let cache = Arc::clone(&cache);
            let current = Arc::clone(&current);
            let second = reference(&work, 1);
            std::thread::spawn(move || {
                cache.lookup(&second, 0, weaver_nntp::ServerSet::EMPTY, 7, true, || {
                    current.load(std::sync::atomic::Ordering::SeqCst)
                })
            })
        };
        cache.wait_for_waiters(1);
        // Paused, cancelled or regenerated while the fetch was out.
        current.store(false, std::sync::atomic::Ordering::SeqCst);
        let mut data = Ok(DownloadPayload::Decoded(decoded(work.segment_id, b"body")));
        first.settle(&mut data, Some(0), 1);
        assert!(matches!(
            waiting.join().unwrap(),
            Some(SharedLookup::Obsolete)
        ));
        assert!(
            replay(cache.ask(&reference(&work, 2), 0, weaver_nntp::ServerSet::EMPTY, 7))
                .data
                .is_ok()
        );
    }

    // Asked without waiting, a lookup answers only a replay: a fetch in
    // flight or an article not fetched yet is not ready, and claims nothing.
    #[test]
    fn a_lookup_that_may_not_wait_answers_only_a_replay() {
        let work = work();
        let cache = cache_holding(&work, None);
        let second = reference(&work, 1);
        let no_wait = |cache: &Arc<RepeatedArticles>| {
            cache.lookup(&second, 0, weaver_nntp::ServerSet::EMPTY, 7, false, || true)
        };
        assert!(matches!(no_wait(&cache), Some(SharedLookup::NotReady)));
        let first = claim(cache.ask(&work, 0, weaver_nntp::ServerSet::EMPTY, 7));
        assert!(matches!(no_wait(&cache), Some(SharedLookup::NotReady)));
        let mut data = Ok(DownloadPayload::Decoded(decoded(work.segment_id, b"body")));
        first.settle(&mut data, Some(0), 1);
        assert!(replay(no_wait(&cache)).data.is_ok());
    }
}
