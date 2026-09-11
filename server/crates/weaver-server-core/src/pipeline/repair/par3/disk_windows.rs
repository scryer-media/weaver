//! Mutable Windows files use identity and change-time fences between reads.
//! No retained sharing lock may prevent the next decoded article from writing.

use super::*;
use std::fs::File;
use std::io::{self, Read};
use std::ops::Range;
use std::os::windows::io::AsRawHandle;
use std::path::Path;
use windows_sys::Win32::Storage::FileSystem::{
    BY_HANDLE_FILE_INFORMATION, FILE_BASIC_INFO, FileBasicInfo, GetFileInformationByHandle,
    GetFileInformationByHandleEx,
};

#[derive(Clone, Copy, PartialEq, Eq)]
struct Stamp {
    volume: u32,
    index: u64,
    len: u64,
    created: i64,
    written: i64,
    changed: i64,
}

impl Stamp {
    fn read(path: &Path, options: &ExecutionOptions) -> io::Result<Self> {
        // Declare the lease first so the file closes before capacity is released.
        let _lease = options.handles.acquire().map_err(io::Error::other)?;
        // Opening a directory is refused outright here; report it the way the
        // engine's own source access does so callers see one error kind.
        if !std::fs::symlink_metadata(path)?.file_type().is_file() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "PAR3 source is not a regular file",
            ));
        }
        let file = File::open(path)?;
        let mut identity = BY_HANDLE_FILE_INFORMATION::default();
        let mut basic = FILE_BASIC_INFO::default();
        // SAFETY: both destinations are initialized, correctly sized native
        // records, and the borrowed handle stays open throughout these calls.
        let identity_ok =
            unsafe { GetFileInformationByHandle(file.as_raw_handle(), &mut identity) };
        if identity_ok == 0 {
            return Err(io::Error::last_os_error());
        }
        let basic_ok = unsafe {
            GetFileInformationByHandleEx(
                file.as_raw_handle(),
                FileBasicInfo,
                (&mut basic as *mut FILE_BASIC_INFO).cast(),
                std::mem::size_of::<FILE_BASIC_INFO>() as u32,
            )
        };
        if basic_ok == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            volume: identity.dwVolumeSerialNumber,
            index: (u64::from(identity.nFileIndexHigh) << 32) | u64::from(identity.nFileIndexLow),
            len: (u64::from(identity.nFileSizeHigh) << 32) | u64::from(identity.nFileSizeLow),
            created: basic.CreationTime,
            written: basic.LastWriteTime,
            changed: basic.ChangeTime,
        })
    }
}

struct MutableDisk {
    source: SourceId,
    path: PathBuf,
    inner: DiskSourceAccess,
    options: ExecutionOptions,
    stamp: Stamp,
    snapshot: SourceSnapshot,
}

pub(super) fn open(
    source: SourceId,
    path: PathBuf,
    options: &ExecutionOptions,
) -> EngineResult<Arc<dyn SourceAccess>> {
    let stamp = Stamp::read(&path, options)?;
    let mut inner = DiskSourceAccess::with_options(options.clone());
    inner.insert(source, path.clone());
    // Establish the engine's strong initial generation once. Later checks use
    // file identity plus change time, including same-length writes and renames.
    let snapshot = inner.snapshot(source)?.ok_or(EngineError::Unavailable {
        source_id: source,
        offset: 0,
    })?;
    if Stamp::read(&path, options)? != stamp {
        return Err(EngineError::SourceChanged(source));
    }
    Ok(Arc::new(MutableDisk {
        source,
        path,
        inner,
        options: options.clone(),
        stamp,
        snapshot,
    }))
}

impl SourceAccess for MutableDisk {
    fn snapshot(&self, source: SourceId) -> io::Result<Option<SourceSnapshot>> {
        if source != self.source {
            return Ok(None);
        }
        match Stamp::read(&self.path, &self.options) {
            Ok(stamp) if stamp == self.stamp => Ok(Some(self.snapshot)),
            Ok(_) => Err(io::Error::other(EngineError::SourceChanged(source))),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }

    fn read_at(&self, source: SourceId, offset: u64, out: &mut [u8]) -> io::Result<usize> {
        self.inner.read_at(source, offset, out)
    }

    fn next_available(&self, source: SourceId, offset: u64) -> io::Result<Option<Range<u64>>> {
        Ok(self
            .snapshot(source)?
            .filter(|s| offset < s.len)
            .map(|s| offset..s.len))
    }

    fn open_sequential(&self, source: SourceId) -> io::Result<Option<Box<dyn Read + Send>>> {
        self.inner.open_sequential(source)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn published_carrier_allows_writes_and_fences_changed_content() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("carrier.par3");
        std::fs::write(&path, b"first").unwrap();
        let options = execution_options();
        let source = SourceId(1);
        let access = open(source, path.clone(), &options).unwrap();
        let snapshot = access.snapshot(source).unwrap();
        assert_eq!(access.snapshot(source).unwrap(), snapshot);
        std::fs::write(&path, b"other").unwrap();
        assert!(matches!(
            EngineError::from(access.snapshot(source).unwrap_err()),
            EngineError::SourceChanged(id) if id == source
        ));
        let next = open(source, path, &options).unwrap();
        assert_ne!(next.snapshot(source).unwrap(), snapshot);
    }
}
