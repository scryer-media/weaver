//! Published direct-store source views for a RAR chase.

use std::collections::HashSet;
use std::io::{self, Read, Seek, SeekFrom};
use std::sync::{Arc, Mutex};

use super::coverage::SetCoverage;
use crate::pipeline::direct_store::provider::{
    HybridVolumeProvider, VirtualVolume, VirtualVolumeReader,
};
use crate::pipeline::extraction::ProcessMemoryBudget;
use crate::pipeline::extraction::safety::ProcessMemoryPermit;

struct View {
    provider: HybridVolumeProvider,
    extracted: HashSet<String>,
    known: HashSet<String>,
    _memory: ProcessMemoryPermit,
}

#[derive(Clone)]
pub(crate) struct VirtualRarInput {
    pub coverage: Arc<SetCoverage>,
    view: Arc<Mutex<Option<Arc<View>>>>,
}

impl VirtualRarInput {
    pub fn new(coverage: Arc<SetCoverage>) -> Self {
        Self {
            coverage,
            view: Arc::new(Mutex::new(None)),
        }
    }

    pub fn publish(
        &self,
        mut volumes: Vec<VirtualVolume>,
        extracted: HashSet<String>,
        known: HashSet<String>,
        complete: &[(usize, u64)],
        memory: &Arc<ProcessMemoryBudget>,
    ) -> Result<(), String> {
        // Held articles are not committed source bytes. Do not pin them or
        // advertise them as readable to speculative decompression.
        for volume in &mut volumes {
            volume.held = Arc::new(Vec::new());
        }
        let bytes = volumes
            .iter()
            .map(VirtualVolume::retained_bytes)
            .try_fold(0u64, |sum, bytes| sum.checked_add(bytes as u64))
            .ok_or("RAR source view size overflow")?;
        let names = extracted
            .iter()
            .chain(known.iter())
            .map(|name| name.capacity() as u64 + 128)
            .sum::<u64>();
        let permit = memory.try_reserve_retained(bytes.saturating_add(names))?;
        let ranges: Vec<_> = volumes
            .iter()
            .map(|volume| (volume.volume_index as usize, volume.readable_ranges()))
            .collect();
        let view = Arc::new(View {
            provider: HybridVolumeProvider::new(volumes),
            extracted,
            known,
            _memory: permit,
        });
        *self.view.lock().expect("RAR source view poisoned") = Some(view);
        // Readers must see the backing view before its coverage is advertised.
        for (index, ranges) in ranges {
            for (start, end) in ranges {
                self.coverage.note_committed_range(index, start, end);
            }
        }
        for &(index, len) in complete {
            self.coverage.finish_ranged_part(index, len);
        }
        Ok(())
    }

    pub fn should_extract(&self, name: &str) -> Result<bool, String> {
        let view = self.snapshot().map_err(|error| error.to_string())?;
        if !view.known.contains(name) {
            return Err(format!("direct-store has not classified RAR member {name}"));
        }
        Ok(view.extracted.contains(name))
    }

    fn snapshot(&self) -> io::Result<Arc<View>> {
        self.view
            .lock()
            .expect("RAR source view poisoned")
            .clone()
            .ok_or_else(|| io::Error::other("RAR source view has not been published"))
    }
}

impl unrar_rs::VolumeProvider for VirtualRarInput {
    fn get_volume(
        &self,
        index: usize,
    ) -> Result<Box<dyn unrar_rs::ReadSeek>, unrar_rs::VolumeProviderError> {
        if index >= self.coverage.part_count() {
            return Err(unrar_rs::VolumeProviderError::Unavailable {
                volume: index,
                reason: "volume is outside the direct set".into(),
            });
        }
        Ok(Box::new(Reader {
            input: self.clone(),
            index,
            position: 0,
            source: None,
        }))
    }
}

struct Reader {
    input: VirtualRarInput,
    index: usize,
    position: u64,
    // Keep file handles and CBC checkpoints across reads of the same view.
    // The Arc also keeps that snapshot's memory reservation alive.
    source: Option<(Arc<View>, VirtualVolumeReader)>,
}

impl Read for Reader {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        loop {
            self.input.coverage.wait_for_read()?;
            let available = self.input.coverage.readable_at(self.index, self.position)?;
            if available == 0 {
                return if self
                    .input
                    .coverage
                    .part_progress(self.index)?
                    .len
                    .is_some_and(|len| self.position >= len)
                {
                    Ok(0)
                } else {
                    Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "completed virtual RAR contains an input hole",
                    ))
                };
            }
            let generation = self.input.coverage.part_progress(self.index)?.rewritten;
            let view = self.input.snapshot()?;
            if self
                .source
                .as_ref()
                .is_none_or(|(old, _)| !Arc::ptr_eq(old, &view))
            {
                let reader = view
                    .provider
                    .open(self.index as u32)
                    .ok_or_else(|| io::Error::other("RAR virtual volume disappeared"))?;
                self.source = Some((view, reader));
            }
            let (_, reader) = self.source.as_mut().expect("source opened above");
            reader.seek(SeekFrom::Start(self.position))?;
            let wanted = out
                .len()
                .min(usize::try_from(available).unwrap_or(usize::MAX));
            let result = reader.read(&mut out[..wanted]);
            let read = result.as_ref().copied().unwrap_or(0);
            if !self.input.coverage.commit_read(
                self.index,
                self.position,
                read as u64,
                generation,
            )? {
                continue;
            }
            let read = result?;
            if read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "virtual RAR ended inside committed coverage",
                ));
            }
            self.position += read as u64;
            return Ok(read);
        }
    }
}

impl Seek for Reader {
    fn seek(&mut self, from: SeekFrom) -> io::Result<u64> {
        self.input.coverage.wait_for_read()?;
        let position = match from {
            SeekFrom::Start(offset) => Some(offset),
            SeekFrom::Current(offset) => self.position.checked_add_signed(offset),
            SeekFrom::End(offset) => self
                .input
                .coverage
                .part_progress(self.index)?
                .len
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::Unsupported, "virtual RAR length not settled")
                })?
                .checked_add_signed(offset),
        }
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "RAR seek overflow"))?;
        self.position = position;
        Ok(position)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::direct_store::ByteRanges;
    use unrar_rs::VolumeProvider;

    fn volume(path: &std::path::Path, committed: u64) -> VirtualVolume {
        let mut covered = ByteRanges::new();
        covered.insert(0, committed);
        VirtualVolume {
            volume_index: 0,
            envelope: path.to_path_buf(),
            extents: Vec::new(),
            partials: Arc::default(),
            envelope_covered: covered.clone(),
            covered,
            held: Arc::default(),
            len: 8,
            ciphers: Arc::default(),
        }
    }

    #[test]
    fn an_existing_reader_observes_new_committed_source_views() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source.envelope");
        std::fs::write(&path, b"abcdefgh").unwrap();
        let input = VirtualRarInput::new(Arc::new(SetCoverage::new(1)));
        let memory = Arc::new(ProcessMemoryBudget::new(1024 * 1024));
        let names = HashSet::from(["subtitle.txt".to_string()]);
        input
            .publish(
                vec![volume(&path, 3)],
                names.clone(),
                names.clone(),
                &[],
                &memory,
            )
            .unwrap();
        let mut reader = input.get_volume(0).unwrap();
        assert_eq!(
            reader.seek(SeekFrom::End(0)).unwrap_err().kind(),
            io::ErrorKind::Unsupported
        );
        let mut head = [0; 3];
        reader.read_exact(&mut head).unwrap();
        assert_eq!(head, *b"abc");
        assert!(input.should_extract("subtitle.txt").unwrap());
        assert!(input.should_extract("unknown.txt").is_err());
        input
            .publish(
                vec![volume(&path, 8)],
                names.clone(),
                names,
                &[(0, 8)],
                &memory,
            )
            .unwrap();
        let mut tail = Vec::new();
        reader.read_to_end(&mut tail).unwrap();
        assert_eq!(tail, b"defgh");
        input.coverage.abort("direct sources invalidated");
        assert!(reader.seek(SeekFrom::Start(0)).is_err());
    }

    #[test]
    fn aggregate_coverage_cannot_admit_an_envelope_hole() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source.envelope");
        std::fs::write(&path, b"abc\0\0\0\0\0").unwrap();
        let mut view = volume(&path, 3);
        view.covered.insert(0, 8);
        let input = VirtualRarInput::new(Arc::new(SetCoverage::new(1)));
        let memory = Arc::new(ProcessMemoryBudget::new(1024 * 1024));
        input
            .publish(
                vec![view],
                HashSet::new(),
                HashSet::new(),
                &[(0, 8)],
                &memory,
            )
            .unwrap();
        let mut reader = input.get_volume(0).unwrap();
        reader.seek(SeekFrom::Start(3)).unwrap();
        assert_eq!(
            reader.read(&mut [0; 1]).unwrap_err().kind(),
            io::ErrorKind::UnexpectedEof
        );
    }

    #[test]
    fn source_view_memory_refusal_does_not_publish_coverage() {
        let input = VirtualRarInput::new(Arc::new(SetCoverage::new(1)));
        let memory = Arc::new(ProcessMemoryBudget::new(1));
        assert!(
            input
                .publish(
                    vec![volume(std::path::Path::new("unopened.envelope"), 8)],
                    HashSet::new(),
                    HashSet::new(),
                    &[(0, 8)],
                    &memory,
                )
                .is_err()
        );
        assert!(input.snapshot().is_err());
        assert!(!input.coverage.part_is_complete(0));
        assert_eq!(input.coverage.consumed_high_water(0), 0);
    }
}
