//! PAR3 reads the posted image of a direct volume, including encrypted members.
//! Snapshot checks run on blocking workers and include every backing file.

use super::*;
use crate::pipeline::direct_store::provider::{
    CipherOverlayCounters, VirtualVolume, VirtualVolumeReader, is_hole,
};
use par3_rs::runtime::HandleLease;
use std::collections::VecDeque;
use std::io::{self, Read, Seek, SeekFrom};
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, Weak};

/// Idle readers retain each publication's cipher frontier under shared budgets.
#[derive(Default)]
pub(in crate::pipeline) struct ReaderCache {
    inner: Mutex<IdleReaders>,
    hits: AtomicU64,
    evictions: AtomicU64,
}

#[derive(Default)]
struct IdleReaders {
    epoch: u64,
    readers: VecDeque<(SourceId, Weak<()>, Reader)>,
}

const MAX_IDLE_READERS: usize = 16;

/// Whether an idle reader's budget could satisfy the request that failed.
fn reclaimable(error: &io::Error) -> bool {
    budget::pressure_source(error).is_some()
        || error.get_ref().is_some_and(|inner| {
            inner.downcast_ref::<EngineError>().is_some_and(|error| {
                matches!(error, EngineError::ResourceLimit(_))
                    || budget::error_pressure_source(error).is_some()
            })
        })
}

impl ReaderCache {
    /// Cumulative reader reuses and evictions, read once per work handback.
    pub(super) fn counters(&self) -> (u64, u64) {
        (
            self.hits.load(Ordering::Relaxed),
            self.evictions.load(Ordering::Relaxed),
        )
    }

    pub(super) fn clear(&self) -> EngineResult<()> {
        let mut cache = self
            .inner
            .lock()
            .map_err(|_| EngineError::InvalidState("PAR3 reader cache poisoned"))?;
        cache.epoch = cache.epoch.wrapping_add(1);
        cache.readers.clear();
        Ok(())
    }

    fn evict(&self) -> io::Result<bool> {
        let evicted = self
            .inner
            .lock()
            .map_err(|_| io::Error::other("PAR3 reader cache poisoned"))?
            .readers
            .pop_front()
            .is_some();
        if evicted {
            self.evictions.fetch_add(1, Ordering::Relaxed);
        }
        Ok(evicted)
    }
}

struct Backing {
    access: Arc<dyn SourceAccess>,
    snapshot: Option<SourceSnapshot>,
}

pub(in crate::pipeline) struct VirtualInput {
    pub volume: VirtualVolume,
    memory: assessment::ViewReservation,
    payloads: Arc<Vec<Arc<budget::PayloadLease>>>,
    pins: Vec<HandleLease>,
}

impl VirtualInput {
    pub fn new(volume: VirtualVolume, options: &ExecutionOptions) -> EngineResult<Self> {
        let memory = assessment::ViewReservation::acquire(volume.retained_metadata_bytes())?;
        let payloads = Arc::new(
            volume
                .retained_payloads()
                .map(|bytes| budget::budgets().retain(bytes))
                .collect::<EngineResult<Vec<_>>>()?,
        );
        let pins = (0..volume.retained_handles())
            .map(|_| options.handles.acquire())
            .collect::<EngineResult<Vec<_>>>()?;
        Ok(Self {
            volume,
            memory,
            payloads,
            pins,
        })
    }
}

pub(in crate::pipeline) struct VirtualSource {
    source: SourceId,
    volume: VirtualVolume,
    ranges: Vec<Range<u64>>,
    backing: Vec<Backing>,
    options: ExecutionOptions,
    identity: Arc<()>,
    cache: Arc<ReaderCache>,
    counters: Arc<CipherOverlayCounters>,
    requested_bytes: Arc<AtomicU64>,
    snapshot_checks: AtomicU64,
    _memory: assessment::ViewReservation,
    payloads: Arc<Vec<Arc<budget::PayloadLease>>>,
    _pins: Vec<HandleLease>,
}

impl VirtualSource {
    pub(in crate::pipeline) fn new(
        source: SourceId,
        image: VirtualInput,
        options: ExecutionOptions,
        cache: Arc<ReaderCache>,
    ) -> EngineResult<Self> {
        options.cancel.check()?;
        let VirtualInput {
            volume,
            memory,
            payloads,
            pins,
        } = image;
        let mut paths = std::collections::BTreeSet::new();
        if !volume.envelope_covered.ranges().is_empty() {
            paths.insert(volume.envelope.clone());
        }
        for extent in &volume.extents {
            if let Some(path) = volume.partials.get(&extent.member_id) {
                paths.insert(path.clone());
            }
        }
        let mut backing = Vec::with_capacity(paths.len());
        for path in paths {
            options.cancel.check()?;
            // Missing backings remain holes. If one appears after publication,
            // a new source generation must admit it before cached evidence can
            // describe that image. Existing Windows files use the cached strong
            // snapshot adapter so every read does not hash the entire partial.
            let mut disk = DiskSourceAccess::with_options(options.clone());
            disk.insert(source, path.clone());
            let snapshot = disk.snapshot(source)?;
            let access = if snapshot.is_some() {
                disk_source(source, path, &options)?
            } else {
                Arc::new(disk)
            };
            let snapshot = access.snapshot(source)?;
            backing.push(Backing { access, snapshot });
        }
        let ranges = volume
            .readable_ranges()
            .into_iter()
            .map(|(start, end)| start..end)
            .collect();
        Ok(Self {
            source,
            volume,
            ranges,
            backing,
            options,
            cache,
            identity: Arc::new(()),
            counters: Arc::new(CipherOverlayCounters::default()),
            requested_bytes: Arc::new(AtomicU64::new(0)),
            snapshot_checks: AtomicU64::new(0),
            _memory: memory,
            payloads,
            _pins: pins,
        })
    }

    #[cfg(test)]
    pub(in crate::pipeline) fn cipher_counters(&self) -> Arc<CipherOverlayCounters> {
        Arc::clone(&self.counters)
    }

    fn check(&self) -> io::Result<()> {
        self.options.cancel.check().map_err(io::Error::other)?;
        self.snapshot_checks.fetch_add(1, Ordering::Relaxed);
        for backing in &self.backing {
            // A Windows snapshot opens the file to read its fence, so a spent
            // handle budget reclaims idle readers here just as a new reader does.
            let snapshot = loop {
                match backing.access.snapshot(self.source) {
                    Err(error) if reclaimable(&error) => {
                        if !self.cache.evict()? {
                            return Err(error);
                        }
                    }
                    result => break result?,
                }
            };
            if snapshot != backing.snapshot {
                return Err(io::Error::other(EngineError::SourceChanged(self.source)));
            }
        }
        Ok(())
    }

    fn reader(&self) -> io::Result<(Reader, u64)> {
        let epoch = {
            let mut cache = self
                .cache
                .inner
                .lock()
                .map_err(|_| io::Error::other("PAR3 reader cache poisoned"))?;
            cache
                .readers
                .retain(|(_, identity, _)| identity.strong_count() != 0);
            if let Some(index) = cache.readers.iter().position(|(source, identity, _)| {
                *source == self.source && identity.ptr_eq(&Arc::downgrade(&self.identity))
            }) {
                let (_, _, reader) = cache.readers.remove(index).expect("cached reader");
                self.cache.hits.fetch_add(1, Ordering::Relaxed);
                return Ok((reader, cache.epoch));
            }
            cache.epoch
        };
        loop {
            match self.new_reader() {
                Ok(reader) => return Ok((reader, epoch)),
                Err(error) if reclaimable(&error) => {
                    if !self.cache.evict()? {
                        return Err(error);
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn new_reader(&self) -> io::Result<Reader> {
        // Cover one bounded cipher temporary, the chain-to-seed buffer and the
        // owned metadata clone. Handles are leased before either can open.
        let memory = assessment::ViewReservation::acquire(
            (512usize << 10).saturating_add(self.volume.retained_metadata_bytes()),
        )
        .map_err(|error| budget::source_pressure(self.source, error))?;
        let handles = [
            self.options.handles.acquire().map_err(io::Error::other)?,
            self.options.handles.acquire().map_err(io::Error::other)?,
        ];
        let pins = (0..self.volume.retained_handles())
            .map(|_| self.options.handles.acquire())
            .collect::<EngineResult<Vec<_>>>()
            .map_err(io::Error::other)?;
        Ok(Reader {
            cancel: self.options.cancel.clone(),
            requested_bytes: Arc::clone(&self.requested_bytes),
            inner: VirtualVolumeReader::<true>::new(
                self.volume.clone(),
                Arc::clone(&self.counters),
            ),
            _memory: memory,
            _payloads: Arc::clone(&self.payloads),
            _handles: handles,
            _pins: pins,
        })
    }

    fn return_reader(&self, reader: Reader, epoch: u64) -> io::Result<()> {
        let mut cache = self
            .cache
            .inner
            .lock()
            .map_err(|_| io::Error::other("PAR3 reader cache poisoned"))?;
        if cache.epoch != epoch || self.options.cancel.check().is_err() {
            return Ok(());
        }
        // Concurrent reads own distinct readers. Retain only the last returned
        // frontier for a publication; no checked-out reader is evicted here.
        cache
            .readers
            .retain(|(source, _, _)| *source != self.source);
        if cache.readers.len() == MAX_IDLE_READERS {
            cache.readers.pop_front();
            self.cache.evictions.fetch_add(1, Ordering::Relaxed);
        }
        cache
            .readers
            .push_back((self.source, Arc::downgrade(&self.identity), reader));
        Ok(())
    }
}

impl Drop for VirtualSource {
    fn drop(&mut self) {
        if let Ok(mut cache) = self.cache.inner.lock() {
            cache
                .readers
                .retain(|(_, identity, _)| !identity.ptr_eq(&Arc::downgrade(&self.identity)));
        }
        tracing::info!(
            source = self.source.0,
            requested_bytes = self.requested_bytes.load(Ordering::Relaxed),
            snapshot_checks = self.snapshot_checks.load(Ordering::Relaxed),
            reencrypted_bytes = self.counters.reencrypted_bytes(),
            chained_bytes = self.counters.chained_bytes(),
            reader_reuses = self.cache.hits.load(Ordering::Relaxed),
            reader_evictions = self.cache.evictions.load(Ordering::Relaxed),
            "PAR3 virtual source retired"
        );
    }
}

impl SourceAccess for VirtualSource {
    fn snapshot(&self, source: SourceId) -> io::Result<Option<SourceSnapshot>> {
        if source != self.source {
            return Ok(None);
        }
        self.check()?;
        // PublishedSources owns the logical generation across publications.
        Ok(Some(SourceSnapshot {
            len: self.volume.len,
            generation: 1,
        }))
    }

    fn next_available(&self, source: SourceId, offset: u64) -> io::Result<Option<Range<u64>>> {
        if source != self.source {
            return Ok(None);
        }
        self.check()?;
        let index = self.ranges.partition_point(|range| range.end <= offset);
        Ok(self
            .ranges
            .get(index)
            .map(|range| range.start.max(offset)..range.end))
    }

    fn read_at(&self, source: SourceId, offset: u64, out: &mut [u8]) -> io::Result<usize> {
        if source != self.source || out.is_empty() {
            return Ok(0);
        }
        self.check()?;
        let (mut reader, epoch) = self.reader()?;
        reader.inner.seek(SeekFrom::Start(offset))?;
        let count = reader.read(out)?;
        // Idle before the closing check: that check may need a handle, and
        // under a spent budget this reader's may be the only ones to reclaim.
        // A source found changed fails every later check before any read, so
        // the reader it leaves behind never serves another byte.
        self.return_reader(reader, epoch)?;
        self.check()?;
        Ok(count)
    }

    fn open_sequential(&self, source: SourceId) -> io::Result<Option<Box<dyn Read + Send>>> {
        if source != self.source {
            return Ok(None);
        }
        self.check()?;
        let Some(prefix) = self.ranges.first().filter(|range| range.start == 0) else {
            return Ok(None);
        };
        let (mut reader, _) = self.reader()?;
        reader.inner.seek(SeekFrom::Start(0))?;
        // PAR3 can use an honest prefix even when later readable islands exist.
        // PublishedSources checks freshness around sequential reads as well.
        Ok(Some(Box::new(reader.take(prefix.end))))
    }
}

struct Reader {
    cancel: par3_rs::runtime::CancellationToken,
    requested_bytes: Arc<AtomicU64>,
    inner: VirtualVolumeReader<true>,
    _memory: assessment::ViewReservation,
    _payloads: Arc<Vec<Arc<budget::PayloadLease>>>,
    _handles: [HandleLease; 2],
    _pins: Vec<HandleLease>,
}

impl Read for Reader {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        self.cancel.check().map_err(io::Error::other)?;
        self.requested_bytes
            .fetch_add(out.len() as u64, Ordering::Relaxed);
        match self.inner.read(out) {
            Err(error) if is_hole(&error) => Ok(0),
            result => result,
        }
    }
}
