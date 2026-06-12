use std::fs::File;
use std::io::{self, Read};
use std::path::{Path, PathBuf};

const LARGE_FILE_CACHE_ADVICE_MIN_BYTES: u64 = 64 * 1024 * 1024;

pub(crate) struct CacheAdvisedReader {
    file: File,
    path: PathBuf,
    file_len: u64,
    touched: u64,
}

impl CacheAdvisedReader {
    pub(crate) fn open(path: &Path) -> io::Result<Self> {
        let file = File::open(path)?;
        let file_len = file.metadata()?.len();
        advise_sequential(&file, path, file_len);
        Ok(Self {
            file,
            path: path.to_path_buf(),
            file_len,
            touched: 0,
        })
    }
}

impl Read for CacheAdvisedReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let read = self.file.read(buf)?;
        self.touched = self.touched.saturating_add(read as u64);
        Ok(read)
    }
}

impl Drop for CacheAdvisedReader {
    fn drop(&mut self) {
        drop_touched_file_cache(&self.file, &self.path, self.file_len, 0, self.touched);
    }
}

pub(crate) fn read_to_vec(path: &Path) -> io::Result<Vec<u8>> {
    let mut reader = CacheAdvisedReader::open(path)?;
    let mut data = Vec::new();
    reader.read_to_end(&mut data)?;
    Ok(data)
}

pub(crate) fn advise_sequential(file: &File, path: &Path, len: u64) {
    advise_range_sequential(file, path, 0, len);
}

pub(crate) fn advise_range_sequential(file: &File, path: &Path, offset: u64, len: u64) {
    if len < LARGE_FILE_CACHE_ADVICE_MIN_BYTES {
        return;
    }
    log_cache_advice(
        "sequential",
        path,
        raw_cache_advice(file, offset, len, CacheAdvice::Sequential),
    );
}

pub(crate) fn drop_file_cache(file: &File, path: &Path, offset: u64, len: u64) {
    if len < LARGE_FILE_CACHE_ADVICE_MIN_BYTES {
        return;
    }
    log_cache_advice(
        "dontneed",
        path,
        raw_cache_advice(file, offset, len, CacheAdvice::DontNeed),
    );
}

pub(crate) fn drop_touched_file_cache(
    file: &File,
    path: &Path,
    file_len: u64,
    offset: u64,
    touched: u64,
) {
    if file_len < LARGE_FILE_CACHE_ADVICE_MIN_BYTES || touched == 0 {
        return;
    }
    log_cache_advice(
        "dontneed",
        path,
        raw_cache_advice(file, offset, touched, CacheAdvice::DontNeed),
    );
}

pub(crate) fn drop_path_cache(path: &Path) {
    let Ok(file) = File::open(path) else {
        return;
    };
    let len = file.metadata().ok().map_or(0, |metadata| metadata.len());
    drop_file_cache(&file, path, 0, len);
}

fn log_cache_advice(operation: &'static str, path: &Path, result: io::Result<()>) {
    match result {
        Ok(()) => tracing::trace!(operation, path = %path.display(), "file cache advice applied"),
        Err(error) => {
            tracing::debug!(operation, path = %path.display(), error = %error, "file cache advice failed")
        }
    }
}

#[derive(Clone, Copy)]
enum CacheAdvice {
    Sequential,
    DontNeed,
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn raw_cache_advice(file: &File, offset: u64, len: u64, advice: CacheAdvice) -> io::Result<()> {
    use std::os::fd::AsRawFd;

    let offset: libc::off_t = offset
        .try_into()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "cache advice offset overflow"))?;
    let len: libc::off_t = len
        .try_into()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "cache advice length overflow"))?;
    let advice = match advice {
        CacheAdvice::Sequential => libc::POSIX_FADV_SEQUENTIAL,
        CacheAdvice::DontNeed => libc::POSIX_FADV_DONTNEED,
    };
    let rc = unsafe { libc::posix_fadvise(file.as_raw_fd(), offset, len, advice) };
    if rc == 0 {
        Ok(())
    } else {
        Err(io::Error::from_raw_os_error(rc))
    }
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
fn raw_cache_advice(_file: &File, _offset: u64, _len: u64, _advice: CacheAdvice) -> io::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_to_vec_preserves_contents() {
        let temp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(temp.path(), b"cache-advised payload").unwrap();

        assert_eq!(read_to_vec(temp.path()).unwrap(), b"cache-advised payload");
    }

    #[test]
    fn path_drop_swallows_missing_file() {
        drop_path_cache(Path::new("/definitely/missing/weaver/par2-cache.bin"));
    }

    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    #[test]
    fn cache_advice_noops_on_unsupported_platforms() {
        let temp = tempfile::NamedTempFile::new().unwrap();
        let file = File::open(temp.path()).unwrap();

        assert!(raw_cache_advice(&file, 0, 0, CacheAdvice::Sequential).is_ok());
        assert!(raw_cache_advice(&file, 0, 0, CacheAdvice::DontNeed).is_ok());
    }
}
