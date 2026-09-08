//! Published byte coverage for the PAR3 engine. Filesystem sparse extents are
//! never evidence of arrival; only committed assembly/direct-store ranges are.

use par3_rs::runtime::{EngineError, EngineResult};
use par3_rs::source::{SourceAccess, SourceId, SourceSnapshot};
use std::collections::BTreeMap;
use std::io::{self, Read};
use std::ops::Range;
use std::sync::{Arc, RwLock};

const MAX_SOURCES: usize = 16_384;
const MAX_RANGES: usize = 262_144;

struct Publication {
    access: Arc<dyn SourceAccess>,
    backing: SourceSnapshot,
    snapshot: SourceSnapshot,
    ranges: Vec<Range<u64>>,
    revision: u64,
}

#[derive(Default)]
struct Registry {
    sources: BTreeMap<SourceId, Arc<Publication>>,
    ranges: usize,
}

/// A clone shares publication state, so a retained session sees later arrivals
/// and changed generations without reopening clean sources. Publication belongs
/// after the writer/direct-store barrier. Replacing existing bytes MUST use
/// `replace`; `arrive` asserts that every previously published byte is unchanged.
#[derive(Clone, Default)]
pub(in crate::pipeline) struct PublishedSources(Arc<RwLock<Registry>>);

impl PublishedSources {
    pub(in crate::pipeline) fn replace(
        &self,
        source: SourceId,
        access: Arc<dyn SourceAccess>,
        len: u64,
        ranges: Vec<Range<u64>>,
    ) -> EngineResult<SourceSnapshot> {
        self.publish(source, access, len, ranges, false)
    }

    pub(in crate::pipeline) fn arrive(
        &self,
        source: SourceId,
        access: Arc<dyn SourceAccess>,
        len: u64,
        ranges: Vec<Range<u64>>,
    ) -> EngineResult<SourceSnapshot> {
        self.publish(source, access, len, ranges, true)
    }

    fn publish(
        &self,
        source: SourceId,
        access: Arc<dyn SourceAccess>,
        len: u64,
        ranges: Vec<Range<u64>>,
        arrival: bool,
    ) -> EngineResult<SourceSnapshot> {
        if ranges.len() > MAX_RANGES {
            return Err(EngineError::ResourceLimit("published source ranges"));
        }
        let mut end = 0;
        for range in &ranges {
            if range.start < end || range.start >= range.end || range.end > len {
                return Err(EngineError::InvalidState("invalid published coverage"));
            }
            end = range.end;
        }
        let backing = access.snapshot(source)?.ok_or(EngineError::Unavailable {
            source_id: source,
            offset: 0,
        })?;
        if ranges.last().is_some_and(|range| range.end > backing.len) {
            return Err(EngineError::InvalidState("coverage exceeds backing source"));
        }
        let mut registry = self
            .0
            .write()
            .map_err(|_| io::Error::other("source registry poisoned"))?;
        let old = registry.sources.get(&source);
        if old.is_none() && registry.sources.len() >= MAX_SOURCES {
            return Err(EngineError::ResourceLimit("published source count"));
        }
        let total = registry.ranges - old.map_or(0, |old| old.ranges.len()) + ranges.len();
        if total > MAX_RANGES {
            return Err(EngineError::ResourceLimit("published source ranges"));
        }
        if arrival
            && let Some(old) = old
            && (len != old.snapshot.len || !covers(&ranges, &old.ranges))
        {
            return Err(EngineError::SourceChanged(source));
        }
        let generation = match old {
            Some(old) if arrival => old.snapshot.generation,
            Some(old) => old
                .snapshot
                .generation
                .checked_add(1)
                .ok_or(EngineError::ResourceLimit("source generations"))?,
            None => 1,
        };
        let revision = old
            .map_or(0, |old| old.revision)
            .checked_add(1)
            .ok_or(EngineError::ResourceLimit("source revisions"))?;
        let snapshot = SourceSnapshot { len, generation };
        registry.sources.insert(
            source,
            Arc::new(Publication {
                access,
                backing,
                snapshot,
                ranges,
                revision,
            }),
        );
        registry.ranges = total;
        Ok(snapshot)
    }

    pub(in crate::pipeline) fn revision(&self, source: SourceId) -> io::Result<Option<u64>> {
        Ok(self.entry(source)?.map(|entry| entry.revision))
    }

    fn entry(&self, source: SourceId) -> io::Result<Option<Arc<Publication>>> {
        Ok(self
            .0
            .read()
            .map_err(|_| io::Error::other("source registry poisoned"))?
            .sources
            .get(&source)
            .cloned())
    }
}

fn covers(new: &[Range<u64>], old: &[Range<u64>]) -> bool {
    let mut index = 0;
    for range in old {
        let mut position = range.start;
        while position < range.end {
            while index < new.len() && new[index].end <= position {
                index += 1;
            }
            let Some(cover) = new.get(index) else {
                return false;
            };
            if cover.start > position {
                return false;
            }
            position = cover.end;
        }
    }
    true
}

impl Publication {
    fn check(&self, source: SourceId) -> io::Result<()> {
        if self.access.snapshot(source)? != Some(self.backing) {
            return Err(io::Error::other(EngineError::SourceChanged(source)));
        }
        Ok(())
    }

    fn available(&self, offset: u64) -> Option<Range<u64>> {
        let index = self.ranges.partition_point(|range| range.end <= offset);
        self.ranges
            .get(index)
            .map(|range| range.start.max(offset)..range.end)
    }
}

impl SourceAccess for PublishedSources {
    fn snapshot(&self, source: SourceId) -> io::Result<Option<SourceSnapshot>> {
        self.entry(source)?
            .map(|entry| {
                entry.check(source)?;
                Ok(entry.snapshot)
            })
            .transpose()
    }

    fn read_at(&self, source: SourceId, offset: u64, out: &mut [u8]) -> io::Result<usize> {
        let Some(entry) = self.entry(source)? else {
            return Ok(0);
        };
        entry.check(source)?;
        let Some(range) = entry
            .available(offset)
            .filter(|range| range.start == offset)
        else {
            return Ok(0);
        };
        let count = out
            .len()
            .min(usize::try_from(range.end - offset).unwrap_or(usize::MAX));
        let read = entry.access.read_at(source, offset, &mut out[..count])?;
        entry.check(source)?;
        if self.snapshot(source)? != Some(entry.snapshot) {
            return Err(io::Error::other(EngineError::SourceChanged(source)));
        }
        Ok(read)
    }

    fn next_available(&self, source: SourceId, offset: u64) -> io::Result<Option<Range<u64>>> {
        let Some(entry) = self.entry(source)? else {
            return Ok(None);
        };
        entry.check(source)?;
        Ok(entry.available(offset))
    }

    fn open_sequential(&self, source: SourceId) -> io::Result<Option<Box<dyn Read + Send>>> {
        let Some(entry) = self.entry(source)? else {
            return Ok(None);
        };
        entry.check(source)?;
        let Some(prefix) = entry.available(0).filter(|range| range.start == 0) else {
            return Ok(None);
        };
        let Some(reader) = entry.access.open_sequential(source)? else {
            return Ok(None);
        };
        Ok(Some(Box::new(PublishedReader {
            registry: self.clone(),
            source,
            entry,
            inner: reader.take(prefix.end),
        })))
    }
}

struct PublishedReader {
    registry: PublishedSources,
    source: SourceId,
    entry: Arc<Publication>,
    inner: io::Take<Box<dyn Read + Send>>,
}

impl Read for PublishedReader {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if self.registry.snapshot(self.source)? != Some(self.entry.snapshot) {
            return Err(io::Error::other(EngineError::SourceChanged(self.source)));
        }
        self.entry.check(self.source)?;
        let count = self.inner.read(out)?;
        self.entry.check(self.source)?;
        if self.registry.snapshot(self.source)? != Some(self.entry.snapshot) {
            return Err(io::Error::other(EngineError::SourceChanged(self.source)));
        }
        Ok(count)
    }
}

#[cfg(test)]
mod tests;
