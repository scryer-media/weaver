//! PAR3 reads the posted image of a direct volume, including encrypted members.
//! Snapshot checks run on blocking workers and include every backing file.

use super::*;
use crate::pipeline::direct_store::provider::{
    CipherOverlayCounters, VirtualVolume, VirtualVolumeReader, is_hole,
};
use par3_rs::runtime::HandleLease;
use std::io::{self, Read, Seek, SeekFrom};
use std::ops::Range;
use std::sync::Mutex;

/// One cached ranged reader across a job, independent of its source count.
#[derive(Default)]
pub(in crate::pipeline) struct ReaderCache(Mutex<Option<(Arc<()>, Reader)>>);

struct Backing {
    access: Arc<dyn SourceAccess>,
    snapshot: Option<SourceSnapshot>,
}

pub(in crate::pipeline) struct VirtualInput {
    pub volume: VirtualVolume,
    memory: assessment::ViewReservation,
    pins: Vec<HandleLease>,
}

impl VirtualInput {
    pub fn new(volume: VirtualVolume, options: &ExecutionOptions) -> EngineResult<Self> {
        let memory = assessment::ViewReservation::acquire(volume.retained_bytes())?;
        let pins = (0..volume.retained_handles())
            .map(|_| options.handles.acquire())
            .collect::<EngineResult<Vec<_>>>()?;
        Ok(Self {
            volume,
            memory,
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
    _memory: assessment::ViewReservation,
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
            _memory: memory,
            _pins: pins,
        })
    }

    #[cfg(test)]
    pub(in crate::pipeline) fn cipher_counters(&self) -> Arc<CipherOverlayCounters> {
        Arc::clone(&self.counters)
    }

    fn check(&self) -> io::Result<()> {
        self.options.cancel.check().map_err(io::Error::other)?;
        for backing in &self.backing {
            if backing.access.snapshot(self.source)? != backing.snapshot {
                return Err(io::Error::other(EngineError::SourceChanged(self.source)));
            }
        }
        Ok(())
    }

    fn reader(&self) -> io::Result<Reader> {
        let cached = self
            .cache
            .0
            .lock()
            .map_err(|_| io::Error::other("PAR3 reader cache poisoned"))?
            .take();
        if let Some((identity, reader)) = cached
            && Arc::ptr_eq(&identity, &self.identity)
        {
            return Ok(reader);
        }
        // Cover one bounded cipher temporary, the chain-to-seed buffer and the
        // owned metadata clone. Handles are leased before either can open.
        let memory = assessment::ViewReservation::acquire(
            (512usize << 10).saturating_add(self.volume.retained_bytes()),
        )
        .map_err(io::Error::other)?;
        let handles = [
            self.options.handles.acquire().map_err(io::Error::other)?,
            self.options.handles.acquire().map_err(io::Error::other)?,
        ];
        let pins = (0..self.volume.retained_handles())
            .map(|_| self.options.handles.acquire())
            .collect::<EngineResult<Vec<_>>>()
            .map_err(io::Error::other)?;
        Ok(Reader {
            inner: VirtualVolumeReader::<true>::new(
                self.volume.clone(),
                Arc::clone(&self.counters),
            ),
            _memory: memory,
            _handles: handles,
            _pins: pins,
        })
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
        let mut reader = self.reader()?;
        reader.inner.seek(SeekFrom::Start(offset))?;
        let count = reader.read(out)?;
        self.check()?;
        *self
            .cache
            .0
            .lock()
            .map_err(|_| io::Error::other("PAR3 reader cache poisoned"))? =
            Some((Arc::clone(&self.identity), reader));
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
        let mut reader = self.reader()?;
        reader.inner.seek(SeekFrom::Start(0))?;
        // PAR3 can use an honest prefix even when later readable islands exist.
        // PublishedSources checks freshness around sequential reads as well.
        Ok(Some(Box::new(reader.take(prefix.end))))
    }
}

struct Reader {
    inner: VirtualVolumeReader<true>,
    _memory: assessment::ViewReservation,
    _handles: [HandleLease; 2],
    _pins: Vec<HandleLease>,
}

impl Read for Reader {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        match self.inner.read(out) {
            Err(error) if is_hole(&error) => Ok(0),
            result => result,
        }
    }
}
