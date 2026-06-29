use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

#[cfg(test)]
thread_local! {
    static GLOBAL_PEAK_OPEN_COUNT: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(super) fn reset_global_peak_open_count() {
    GLOBAL_PEAK_OPEN_COUNT.with(|peak| peak.set(0));
}

#[cfg(test)]
pub(super) fn global_peak_open_count() -> usize {
    GLOBAL_PEAK_OPEN_COUNT.with(std::cell::Cell::get)
}

#[derive(Clone)]
pub(super) struct BoundedRarSourcePool {
    inner: Arc<Mutex<BoundedRarSourceInner>>,
}

impl BoundedRarSourcePool {
    pub(super) fn single_fd() -> Self {
        Self {
            inner: Arc::new(Mutex::new(BoundedRarSourceInner::new())),
        }
    }

    pub(super) fn reader(&self, path: PathBuf) -> Box<dyn weaver_unrar::ReadSeek> {
        let id = {
            let mut inner = self.inner.lock().expect("bounded RAR source pool poisoned");
            inner.allocate_reader_id()
        };
        Box::new(BoundedRarSourceReader {
            id,
            path,
            position: 0,
            pool: self.inner.clone(),
        })
    }

    #[cfg(test)]
    pub(super) fn peak_open_count(&self) -> usize {
        self.inner
            .lock()
            .expect("bounded RAR source pool poisoned")
            .peak_open_count
    }
}

struct BoundedRarSourceInner {
    next_reader_id: usize,
    open: Option<OpenRarSource>,
    #[cfg(test)]
    peak_open_count: usize,
}

impl BoundedRarSourceInner {
    fn new() -> Self {
        Self {
            next_reader_id: 0,
            open: None,
            #[cfg(test)]
            peak_open_count: 0,
        }
    }

    fn allocate_reader_id(&mut self) -> usize {
        let id = self.next_reader_id;
        self.next_reader_id = self.next_reader_id.saturating_add(1);
        id
    }

    fn close_reader(&mut self, id: usize) {
        if self.open.as_ref().is_some_and(|open| open.id == id) {
            self.open = None;
        }
    }

    fn read_at(
        &mut self,
        id: usize,
        path: &Path,
        position: u64,
        buf: &mut [u8],
    ) -> io::Result<(usize, u64)> {
        self.ensure_open(id, path, position)?;
        let open = self.open.as_mut().expect("bounded RAR source must be open");
        let read = open.file.read(buf)?;
        open.position = open.position.saturating_add(read as u64);
        Ok((read, open.position))
    }

    fn ensure_open(&mut self, id: usize, path: &Path, position: u64) -> io::Result<()> {
        if !self.open.as_ref().is_some_and(|open| open.id == id) {
            self.open = None;
            let mut file = File::open(path).map_err(|error| {
                io::Error::new(
                    error.kind(),
                    crate::pipeline::capacity::format_fd_capacity_error(
                        &format!("failed to lazily open RAR source {}", path.display()),
                        &error,
                    ),
                )
            })?;
            file.seek(SeekFrom::Start(position))?;
            self.open = Some(OpenRarSource { id, file, position });
            #[cfg(test)]
            {
                self.peak_open_count = self.peak_open_count.max(1);
                GLOBAL_PEAK_OPEN_COUNT.with(|peak| peak.set(peak.get().max(1)));
            }
            return Ok(());
        }

        let open = self.open.as_mut().expect("bounded RAR source must be open");
        if open.position != position {
            open.file.seek(SeekFrom::Start(position))?;
            open.position = position;
        }
        Ok(())
    }
}

struct OpenRarSource {
    id: usize,
    file: File,
    position: u64,
}

struct BoundedRarSourceReader {
    id: usize,
    path: PathBuf,
    position: u64,
    pool: Arc<Mutex<BoundedRarSourceInner>>,
}

impl Drop for BoundedRarSourceReader {
    fn drop(&mut self) {
        if let Ok(mut pool) = self.pool.lock() {
            pool.close_reader(self.id);
        }
    }
}

impl Read for BoundedRarSourceReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let (read, position) = self
            .pool
            .lock()
            .expect("bounded RAR source pool poisoned")
            .read_at(self.id, &self.path, self.position, buf)?;
        self.position = position;
        Ok(read)
    }
}

impl Seek for BoundedRarSourceReader {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.position = match pos {
            SeekFrom::Start(offset) => offset,
            SeekFrom::Current(delta) => apply_seek_delta(self.position, delta)?,
            SeekFrom::End(delta) => {
                let len = std::fs::metadata(&self.path)?.len();
                apply_seek_delta(len, delta)?
            }
        };
        Ok(self.position)
    }
}

fn apply_seek_delta(base: u64, delta: i64) -> io::Result<u64> {
    let value = i128::from(base) + i128::from(delta);
    if !(0..=i128::from(u64::MAX)).contains(&value) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "seek resolved outside file bounds",
        ));
    }
    Ok(value as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_reader_reopens_sources_without_exceeding_single_fd_peak() {
        reset_global_peak_open_count();
        let temp = tempfile::tempdir().unwrap();
        let first = temp.path().join("first.bin");
        let second = temp.path().join("second.bin");
        std::fs::write(&first, b"abcdef").unwrap();
        std::fs::write(&second, b"uvwxyz").unwrap();

        let pool = BoundedRarSourcePool::single_fd();
        let mut first_reader = pool.reader(first);
        let mut second_reader = pool.reader(second);

        let mut buf = [0u8; 3];
        first_reader.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"abc");
        second_reader.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"uvw");
        first_reader.seek(SeekFrom::Start(3)).unwrap();
        first_reader.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"def");

        assert_eq!(pool.peak_open_count(), 1);
        assert_eq!(global_peak_open_count(), 1);
    }
}
