//! Strong extent donation from explicitly published sources.

use super::*;
use par3_rs::layout::{BlockLayout, ExtentKind};
use par3_rs::placement::{PlacementOptions, search_extent};
use par3_rs::session::RepairStatus;
use std::ops::Range;

const READ_LIMIT: u64 = 1 << 30;
type Key = (par3_rs::Fingerprint, usize, SourceId);

struct Attempt {
    snapshot: SourceSnapshot,
    revision: u64,
    checked: Vec<u8>,
    shift: Option<i128>,
    _reservation: assessment::ViewReservation,
}

#[derive(Default)]
pub(super) struct Cache {
    attempts: BTreeMap<Key, Attempt>,
    read_bytes: u64,
}

impl Par3Job {
    pub(super) fn discover_donors(&mut self) -> EngineResult<()> {
        if !self.sets.values().any(|set| {
            set.view.as_ref().is_some_and(|view| {
                matches!(
                    view.status,
                    RepairStatus::Ready | RepairStatus::NeedRecovery
                )
            })
        }) {
            return Ok(());
        }
        let _candidates = assessment::ViewReservation::acquire(self.bindings.len() * 64)?;
        let mut candidates = Vec::new();
        for &source in self.bindings.values() {
            if self.carriers.contains_key(&source) {
                continue;
            }
            if let Some(snapshot) = self.sources.snapshot(source)? {
                let revision = self
                    .sources
                    .revision(source)?
                    .ok_or(EngineError::SourceChanged(source))?;
                candidates.push((source, snapshot, revision));
            }
        }
        self.donor_search
            .attempts
            .retain(|(_, _, source), _| candidates.iter().any(|(id, _, _)| id == source));
        for set in self.sets.values_mut() {
            let Some(layout) = set.native.layout()? else {
                continue;
            };
            // Embedded self-repair has its own protected-stream identity and
            // container reconstruction rules; ordinary donor placement cannot
            // grant ownership of the unprotected packet region.
            if layout.files().iter().any(|file| {
                file.extents
                    .iter()
                    .any(|extent| matches!(extent.kind, ExtentKind::Unprotected))
            }) {
                continue;
            }
            for sliding in [false, true] {
                let Some(view) = set.view.as_ref() else {
                    continue;
                };
                if !matches!(
                    view.status,
                    RepairStatus::Ready | RepairStatus::NeedRecovery
                ) || (sliding && view.status != RepairStatus::NeedRecovery)
                {
                    continue;
                }
                if search_pass(
                    set,
                    &layout,
                    &candidates,
                    &self.sources,
                    &self.options,
                    &mut self.donor_search,
                    sliding,
                )? {
                    set.assess()?;
                }
            }
        }
        Ok(())
    }
}

fn search_pass(
    set: &mut assessment::SetSession,
    layout: &BlockLayout,
    candidates: &[(SourceId, SourceSnapshot, u64)],
    sources: &PublishedSources,
    options: &ExecutionOptions,
    cache: &mut Cache,
    sliding: bool,
) -> EngineResult<bool> {
    let mut changed = false;
    let view = set.view.as_ref().expect("assessed set");
    for (file_index, file) in layout.files().iter().enumerate() {
        let Some(state) = view.files.iter().find(|state| state.path == file.path) else {
            continue;
        };
        if state.complete {
            continue;
        }
        for (extent_index, extent) in file.extents.iter().enumerate() {
            if !matches!(
                extent.kind,
                ExtentKind::Block {
                    fingerprint: Some(_),
                    rolling_hash: Some(_),
                    ..
                }
            ) || !state
                .unresolved
                .iter()
                .any(|range| range.start < extent.range.end && extent.range.start < range.end)
            {
                continue;
            }
            if candidates.iter().any(|&(source, snapshot, _)| {
                cache
                    .attempts
                    .get(&(layout.identity(), file_index, source))
                    .is_some_and(|attempt| {
                        attempt.snapshot == snapshot && attempt.checked[extent_index] & 4 != 0
                    })
            }) {
                continue;
            }
            for &(source, snapshot, revision) in candidates {
                options.cancel.check()?;
                if !sliding && state.source == Some(source) {
                    continue;
                }
                let key = (layout.identity(), file_index, source);
                if let Some(attempt) = cache.attempts.get_mut(&key)
                    && attempt.snapshot == snapshot
                    && attempt.revision != revision
                {
                    // New coverage retries negative searches; confirmations
                    // from the same immutable generation remain admitted.
                    for flags in &mut attempt.checked {
                        *flags = if *flags & 4 != 0 { 7 } else { 0 };
                    }
                    attempt.revision = revision;
                }
                if !cache.attempts.get(&key).is_some_and(|attempt| {
                    attempt.snapshot == snapshot && attempt.revision == revision
                }) {
                    let reservation =
                        assessment::ViewReservation::acquire(256 + file.extents.len())?;
                    cache.attempts.insert(
                        key,
                        Attempt {
                            snapshot,
                            revision,
                            checked: vec![0; file.extents.len()],
                            shift: None,
                            _reservation: reservation,
                        },
                    );
                }
                let flag = if sliding { 2 } else { 1 };
                let attempt = cache.attempts.get_mut(&key).expect("candidate");
                if attempt.checked[extent_index] & flag != 0 {
                    continue;
                }
                let length = extent.range.end - extent.range.start;
                let preferred = if sliding {
                    attempt.shift.filter(|shift| *shift != 0).and_then(|shift| {
                        let start = u64::try_from(i128::from(extent.range.start) + shift).ok()?;
                        Some(start..start.checked_add(length)?)
                    })
                } else {
                    Some(extent.range.clone())
                };
                let mut found = None;
                if let Some(range) = preferred {
                    found = locate(
                        layout,
                        file_index,
                        extent_index,
                        Window {
                            sources,
                            source,
                            range,
                        },
                        options,
                        &mut cache.read_bytes,
                    )?;
                }
                if sliding && found.is_none() {
                    found = locate(
                        layout,
                        file_index,
                        extent_index,
                        Window {
                            sources,
                            source,
                            range: 0..snapshot.len,
                        },
                        options,
                        &mut cache.read_bytes,
                    )?;
                }
                if let Some(found) = found {
                    let shift = i128::from(found.offset()) - i128::from(extent.range.start);
                    set.native.add_placement(found)?;
                    attempt.checked[extent_index] |= flag | 4;
                    attempt.shift = Some(shift);
                    changed = true;
                    break;
                }
                attempt.checked[extent_index] |= flag;
            }
        }
    }
    Ok(changed)
}

fn locate(
    layout: &BlockLayout,
    file: usize,
    extent: usize,
    access: Window<'_>,
    options: &ExecutionOptions,
    read_bytes: &mut u64,
) -> EngineResult<Option<par3_rs::placement::PlacedExtent>> {
    let limits = PlacementOptions {
        max_read_bytes: READ_LIMIT.saturating_sub(*read_bytes),
        max_candidates: 1,
        max_matches: 4096,
        ..PlacementOptions::default()
    };
    let before = options.diagnostics.source_io().read_bytes;
    let result = search_extent(
        layout,
        file,
        extent,
        &access,
        &[access.source],
        &limits,
        options,
    );
    *read_bytes = read_bytes.saturating_add(
        options
            .diagnostics
            .source_io()
            .read_bytes
            .saturating_sub(before),
    );
    Ok(result?.matches.into_iter().next())
}

/// Limit the search window without changing identity, generation, or holes.
struct Window<'a> {
    sources: &'a PublishedSources,
    source: SourceId,
    range: Range<u64>,
}
impl SourceAccess for Window<'_> {
    fn snapshot(&self, source: SourceId) -> std::io::Result<Option<SourceSnapshot>> {
        if source != self.source {
            return Ok(None);
        }
        self.sources.snapshot(source)
    }
    fn read_at(&self, source: SourceId, offset: u64, buffer: &mut [u8]) -> std::io::Result<usize> {
        if source != self.source || offset < self.range.start || offset >= self.range.end {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "read outside donor window",
            ));
        }
        let count = (self.range.end - offset).min(buffer.len() as u64) as usize;
        self.sources.read_at(source, offset, &mut buffer[..count])
    }
    fn next_available(&self, source: SourceId, offset: u64) -> std::io::Result<Option<Range<u64>>> {
        if source != self.source || offset >= self.range.end {
            return Ok(None);
        }
        Ok(self
            .sources
            .next_available(source, offset.max(self.range.start))?
            .and_then(|range| {
                let range = range.start.max(self.range.start)..range.end.min(self.range.end);
                (range.start < range.end).then_some(range)
            }))
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{INDEX, RECOVERY, inputs};
    use super::*;

    fn publish(job: &mut Par3Job, source: SourceId, generation: u64, name: &str, bytes: Vec<u8>) {
        let len = bytes.len() as u64;
        let mut access = par3_rs::source::MemorySourceAccess::default();
        access.insert(source, generation, bytes.into());
        job.publish_access(
            source,
            Arc::new(access),
            name.into(),
            std::iter::once(0..len).collect(),
            None,
        )
        .unwrap();
    }

    #[test]
    fn shifted_virtual_donors_are_cached_and_generation_bound() {
        let root = tempfile::tempdir().unwrap();
        let mut job = Par3Job::default();
        for (index, (name, bytes)) in inputs().into_iter().enumerate() {
            let (name, bytes) = if index == 0 {
                (
                    "opaque.dat".to_string(),
                    [b"prefix13bytes".as_slice(), bytes.as_slice()].concat(),
                )
            } else {
                (name, bytes)
            };
            publish(&mut job, SourceId(index as u64 + 1), 1, &name, bytes);
        }
        let index = root.path().join("set.par3");
        std::fs::write(&index, INDEX).unwrap();
        job.scan_file(SourceId(99), index, None).unwrap();
        job.assess().unwrap();
        assert!(job.name_search.source().is_none());
        assert_eq!(
            job.sets
                .values()
                .next()
                .unwrap()
                .view
                .as_ref()
                .unwrap()
                .status,
            RepairStatus::Ready
        );
        assert!(job.donor_search.read_bytes > 0);
        let read = job.options.diagnostics.source_io().read_bytes;
        for _ in 0..3 {
            job.assess().unwrap();
        }
        assert_eq!(job.options.diagnostics.source_io().read_bytes, read);
        let recovery = root.path().join("set.vol0+1.par3");
        std::fs::write(&recovery, RECOVERY).unwrap();
        job.scan_file(SourceId(98), recovery, None).unwrap();
        let read = job.options.diagnostics.source_io().read_bytes;
        job.assess().unwrap();
        assert_eq!(job.options.diagnostics.source_io().read_bytes, read);
        publish(&mut job, SourceId(1), 2, "opaque.dat", vec![0; 5013]);
        job.assess().unwrap();
        assert_eq!(
            job.sets
                .values()
                .next()
                .unwrap()
                .view
                .as_ref()
                .unwrap()
                .status,
            RepairStatus::NeedRecovery
        );
        let read = job.options.diagnostics.source_io().read_bytes;
        job.assess().unwrap();
        assert_eq!(
            job.options.diagnostics.source_io().read_bytes,
            read,
            "negative donor searches are cached too"
        );
    }

    #[test]
    fn donor_window_keeps_holes_and_source_identity() {
        let mut job = Par3Job::default();
        let mut memory = par3_rs::source::MemorySourceAccess::default();
        memory.insert(SourceId(1), 1, Arc::from(vec![7; 3000]));
        job.publish_access(
            SourceId(1),
            Arc::new(memory),
            "opaque.dat".into(),
            vec![0..1000, 1500..3000],
            None,
        )
        .unwrap();
        let window = Window {
            sources: &job.sources,
            source: SourceId(1),
            range: 500..2000,
        };
        assert_eq!(
            window.snapshot(SourceId(1)).unwrap(),
            job.sources.snapshot(SourceId(1)).unwrap()
        );
        assert_eq!(
            window.next_available(SourceId(1), 0).unwrap(),
            Some(500..1000)
        );
        assert_eq!(
            window.next_available(SourceId(1), 1000).unwrap(),
            Some(1500..2000)
        );
        assert_eq!(window.next_available(SourceId(1), 2000).unwrap(), None);
        let mut untouched = [123];
        assert_eq!(
            window.read_at(SourceId(1), 1000, &mut untouched).unwrap(),
            0
        );
        assert_eq!(untouched, [123], "holes never supply synthetic bytes");
    }
}
