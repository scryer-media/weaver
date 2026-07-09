use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::Path;

pub(crate) const LARGE_FILE_CACHE_ADVICE_MIN_BYTES: u64 = 64 * 1024 * 1024;

const COPY_BUFFER_BYTES: usize = 1024 * 1024;

pub(crate) fn copy_large_file(src: &Path, dst: &Path) -> io::Result<u64> {
    copy_large_file_with_progress(src, dst, |_| {})
}

pub(crate) fn copy_large_file_with_progress<F>(
    src: &Path,
    dst: &Path,
    mut on_copied: F,
) -> io::Result<u64>
where
    F: FnMut(u64),
{
    let metadata = fs::metadata(src)?;
    let mut input = File::open(src)?;
    advise_open_file_sequential(&input, src, 0, metadata.len());

    if let Some(parent) = dst.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut output = OpenOptions::new().create_new(true).write(true).open(dst)?;

    let mut copied = 0u64;
    let mut buf = vec![0u8; COPY_BUFFER_BYTES];
    loop {
        let read = input.read(&mut buf)?;
        if read == 0 {
            break;
        }
        output.write_all(&buf[..read])?;
        copied += read as u64;
        on_copied(read as u64);
    }
    output.flush()?;
    fs::set_permissions(dst, metadata.permissions())?;

    drop_open_file_cache(&input, src, 0, copied);
    drop_open_file_cache(&output, dst, 0, copied);

    Ok(copied)
}

#[allow(dead_code)]
pub(crate) fn advise_path_sequential(path: &Path) {
    let Ok(file) = File::open(path) else {
        return;
    };
    let len = file.metadata().ok().map_or(0, |metadata| metadata.len());
    advise_open_file_sequential(&file, path, 0, len);
}

#[allow(dead_code)]
pub(crate) fn drop_path_cache(path: &Path) {
    let Ok(file) = File::open(path) else {
        return;
    };
    let len = file.metadata().ok().map_or(0, |metadata| metadata.len());
    drop_open_file_cache(&file, path, 0, len);
}

pub(crate) fn advise_open_file_sequential(file: &File, path: &Path, offset: u64, len: u64) {
    if len < LARGE_FILE_CACHE_ADVICE_MIN_BYTES {
        return;
    }
    log_cache_advice(
        "sequential",
        path,
        raw_cache_advice(file, offset, len, CacheAdvice::Sequential),
    );
}

pub(crate) fn drop_open_file_cache(file: &File, path: &Path, offset: u64, len: u64) {
    if len < LARGE_FILE_CACHE_ADVICE_MIN_BYTES {
        return;
    }
    log_cache_advice(
        "dontneed",
        path,
        raw_cache_advice(file, offset, len, CacheAdvice::DontNeed),
    );
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
mod tests;
