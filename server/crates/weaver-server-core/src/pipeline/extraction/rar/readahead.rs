use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::Mutex;
use std::thread::JoinHandle;

/// How much of the next volume the readahead worker pulls through the page
/// cache before handing the open file over.
const PREFETCH_BYTES: u64 = 4 * 1024 * 1024;

/// Volume provider for on-disk RAR volumes that overlaps the open and first
/// read of volume N+1 with the consumption of volume N.
///
/// A single slot holds the in-flight prefetch: `get_volume(n)` consumes the
/// slot when it holds volume `n` and then schedules volume `n + 1`. Any
/// prefetch failure falls back to a synchronous open, so observable behavior
/// matches a provider without readahead.
pub(super) struct ReadaheadVolumeProvider {
    paths: HashMap<usize, PathBuf>,
    slot: Mutex<Option<PendingVolume>>,
    #[cfg(test)]
    prefetch_hits: std::sync::atomic::AtomicUsize,
}

struct PendingVolume {
    volume: usize,
    handle: JoinHandle<std::io::Result<File>>,
}

impl ReadaheadVolumeProvider {
    pub(super) fn new(paths: HashMap<usize, PathBuf>) -> Self {
        Self {
            paths,
            slot: Mutex::new(None),
            #[cfg(test)]
            prefetch_hits: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    fn take_prefetched(&self, volume: usize) -> Option<File> {
        let pending = {
            let mut slot = self.slot.lock().expect("RAR readahead slot poisoned");
            if slot
                .as_ref()
                .is_some_and(|pending| pending.volume == volume)
            {
                slot.take()
            } else {
                None
            }
        };
        let file = pending.and_then(|pending| pending.handle.join().ok()?.ok());
        #[cfg(test)]
        if file.is_some() {
            self.prefetch_hits
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        file
    }

    fn schedule(&self, volume: usize) {
        let Some(path) = self.paths.get(&volume) else {
            return;
        };
        let mut slot = self.slot.lock().expect("RAR readahead slot poisoned");
        // The engine fetches a volume twice when it discovers continuation
        // headers (header parse, then segment read); the second fetch of
        // volume N must not clobber the prefetch of N+1 already in flight.
        if slot
            .as_ref()
            .is_some_and(|pending| pending.volume == volume)
        {
            return;
        }
        *slot = Some(PendingVolume {
            volume,
            handle: spawn_prefetch(path.clone()),
        });
    }

    #[cfg(test)]
    fn prefetch_hits(&self) -> usize {
        self.prefetch_hits
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

fn spawn_prefetch(path: PathBuf) -> JoinHandle<std::io::Result<File>> {
    std::thread::spawn(move || {
        let mut file = File::open(path)?;
        let mut buf = vec![0u8; 128 * 1024];
        let mut remaining = PREFETCH_BYTES;
        while remaining > 0 {
            let want = buf.len().min(remaining as usize);
            match file.read(&mut buf[..want]) {
                Ok(0) => break,
                Ok(read) => remaining -= read as u64,
                // Genuine read errors resurface on the consumer side.
                Err(_) => break,
            }
        }
        file.seek(SeekFrom::Start(0))?;
        Ok(file)
    })
}

impl weaver_unrar::VolumeProvider for ReadaheadVolumeProvider {
    fn get_volume(
        &self,
        index: usize,
    ) -> Result<Box<dyn weaver_unrar::ReadSeek>, weaver_unrar::VolumeProviderError> {
        let path = self.paths.get(&index).ok_or_else(|| {
            weaver_unrar::VolumeProviderError::Unavailable {
                volume: index,
                reason: "not registered".into(),
            }
        })?;
        let file = match self.take_prefetched(index) {
            Some(file) => file,
            None => File::open(path).map_err(weaver_unrar::VolumeProviderError::Io)?,
        };
        self.schedule(index + 1);
        Ok(Box::new(file))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use weaver_unrar::VolumeProvider;

    fn write_volumes(temp: &tempfile::TempDir, contents: &[&[u8]]) -> HashMap<usize, PathBuf> {
        contents
            .iter()
            .enumerate()
            .map(|(index, bytes)| {
                let path = temp.path().join(format!("vol{index}.rar"));
                std::fs::write(&path, bytes).unwrap();
                (index, path)
            })
            .collect()
    }

    fn read_all(provider: &ReadaheadVolumeProvider, index: usize) -> Vec<u8> {
        let mut reader = provider.get_volume(index).unwrap();
        let mut data = Vec::new();
        reader.read_to_end(&mut data).unwrap();
        data
    }

    #[test]
    fn sequential_reads_use_the_prefetched_volume() {
        let temp = tempfile::tempdir().unwrap();
        let provider =
            ReadaheadVolumeProvider::new(write_volumes(&temp, &[b"first", b"second", b"third"]));

        assert_eq!(read_all(&provider, 0), b"first");
        assert_eq!(read_all(&provider, 1), b"second");
        assert_eq!(read_all(&provider, 2), b"third");
        assert_eq!(provider.prefetch_hits(), 2);
    }

    #[test]
    fn refetching_the_current_volume_keeps_the_next_prefetch_alive() {
        let temp = tempfile::tempdir().unwrap();
        let provider = ReadaheadVolumeProvider::new(write_volumes(&temp, &[b"first", b"second"]));

        // Continuation discovery fetches a volume once for headers and once
        // for the data segment.
        assert_eq!(read_all(&provider, 0), b"first");
        assert_eq!(read_all(&provider, 0), b"first");
        assert_eq!(read_all(&provider, 1), b"second");
        assert_eq!(read_all(&provider, 1), b"second");
        assert_eq!(provider.prefetch_hits(), 1);
    }

    #[test]
    fn prefetched_reader_supports_seeking_like_a_plain_file() {
        let temp = tempfile::tempdir().unwrap();
        let provider = ReadaheadVolumeProvider::new(write_volumes(&temp, &[b"abcdef", b"uvwxyz"]));

        assert_eq!(read_all(&provider, 0), b"abcdef");
        let mut reader = provider.get_volume(1).unwrap();
        reader.seek(SeekFrom::Start(2)).unwrap();
        let mut buf = [0u8; 3];
        reader.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"wxy");
        assert_eq!(provider.prefetch_hits(), 1);
    }

    #[test]
    fn unregistered_volume_is_unavailable() {
        let temp = tempfile::tempdir().unwrap();
        let provider = ReadaheadVolumeProvider::new(write_volumes(&temp, &[b"only"]));

        assert_eq!(read_all(&provider, 0), b"only");
        let Err(error) = provider.get_volume(1) else {
            panic!("expected unregistered volume to be unavailable");
        };
        assert!(matches!(
            error,
            weaver_unrar::VolumeProviderError::Unavailable { volume: 1, .. }
        ));
    }

    #[test]
    fn missing_file_reports_the_synchronous_open_error() {
        let temp = tempfile::tempdir().unwrap();
        let mut paths = write_volumes(&temp, &[b"first"]);
        paths.insert(1, temp.path().join("missing.rar"));
        let provider = ReadaheadVolumeProvider::new(paths);

        assert_eq!(read_all(&provider, 0), b"first");
        let Err(error) = provider.get_volume(1) else {
            panic!("expected missing volume file to fail the open");
        };
        assert!(matches!(error, weaver_unrar::VolumeProviderError::Io(_)));
        assert_eq!(provider.prefetch_hits(), 0);
    }
}
