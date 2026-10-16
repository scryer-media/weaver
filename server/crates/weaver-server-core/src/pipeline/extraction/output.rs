//! Member output sessions shared by archive engines. Accepting bytes, finishing
//! their stream, and establishing integrity are separate events. None of them
//! alone grants publication or proves placement of an ancestor's source bytes.

use std::io::{self, Write};
use std::path::Path;
use std::sync::Arc;
use std::time::SystemTime;

use super::{ExtractionRoot, JobExtractionBudget};

pub(crate) mod channel;
pub(crate) mod select;

#[cfg(test)]
mod tests;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Integrity {
    Verified,
    NoChecksum,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Metadata {
    pub modified: Option<SystemTime>,
    pub accessed: Option<SystemTime>,
    pub unix_mode: Option<u32>,
}

impl Metadata {
    pub(crate) fn rar(member: &unrar_rs::MemberInfo) -> Self {
        Self {
            modified: member.mtime,
            accessed: member.atime,
            #[cfg(unix)]
            unix_mode: super::rar::rar_member_unix_output_mode(member),
            #[cfg(not(unix))]
            unix_mode: None,
        }
    }
}

// An implementation binds the occurrence to its namespace and generation.
// Paths are validated destination metadata, never member identities.
pub(crate) trait ArchiveOutput: Sync {
    type Member: OutputMember + Send;
    fn file(&self, occurrence: usize, path: &Path, length: Option<u64>)
    -> io::Result<Self::Member>;
    fn directory(&self, occurrence: usize, path: &Path) -> io::Result<()>;
    fn directory_metadata(
        &self,
        occurrence: usize,
        path: &Path,
        metadata: Metadata,
    ) -> io::Result<()>;
}

impl<T: ArchiveOutput> ArchiveOutput for &T {
    type Member = T::Member;

    fn file(
        &self,
        occurrence: usize,
        path: &Path,
        length: Option<u64>,
    ) -> io::Result<Self::Member> {
        T::file(self, occurrence, path, length)
    }

    fn directory(&self, occurrence: usize, path: &Path) -> io::Result<()> {
        T::directory(self, occurrence, path)
    }

    fn directory_metadata(
        &self,
        occurrence: usize,
        path: &Path,
        metadata: Metadata,
    ) -> io::Result<()> {
        T::directory_metadata(self, occurrence, path, metadata)
    }
}

pub(crate) trait OutputWriter: Write {
    fn metadata(&self, metadata: Metadata) -> io::Result<()>;
}

pub(crate) trait OutputMember: Write {
    fn metadata(&self, metadata: Metadata) -> io::Result<()>;
    fn finish_data(self) -> io::Result<PendingOutput>;
}

impl OutputWriter for cap_std::fs::File {
    fn metadata(&self, metadata: Metadata) -> io::Result<()> {
        if metadata.modified.is_none()
            && metadata.accessed.is_none()
            && metadata.unix_mode.is_none()
        {
            return Ok(());
        }
        #[cfg(unix)]
        let file = {
            use std::os::fd::AsFd;
            std::fs::File::from(self.as_fd().try_clone_to_owned()?)
        };
        #[cfg(windows)]
        let file = {
            use std::os::windows::io::AsHandle;
            std::fs::File::from(self.as_handle().try_clone_to_owned()?)
        };
        if metadata.modified.is_some() || metadata.accessed.is_some() {
            let mut times = std::fs::FileTimes::new();
            if let Some(modified) = metadata.modified {
                times = times.set_modified(modified);
            }
            if let Some(accessed) = metadata.accessed {
                times = times.set_accessed(accessed);
            }
            file.set_times(times)?;
        }
        #[cfg(unix)]
        if let Some(mode) = metadata.unix_mode {
            use std::os::unix::fs::PermissionsExt;
            file.set_permissions(std::fs::Permissions::from_mode(mode))?;
        }
        Ok(())
    }
}

// The coordinator's implementation owns generation fencing and candidate
// lifetime. Dropping a stream or proof before success revokes that candidate.
pub(crate) trait OutputCompletion: Send {
    fn data_complete(&mut self, length: u64) -> io::Result<()>;
    fn integrity(&mut self, integrity: Integrity) -> io::Result<()>;
    fn abort(&mut self);
}

pub(crate) struct MemberOutput<W> {
    writer: super::BudgetedWriter<W>,
    pending: PendingOutput,
    expected: Option<u64>,
    written: u64,
}

impl<W: Write> MemberOutput<W> {
    pub(crate) fn new(
        writer: super::BudgetedWriter<W>,
        completion: Option<Box<dyn OutputCompletion>>,
        expected: Option<u64>,
    ) -> Self {
        Self {
            writer,
            pending: PendingOutput { completion },
            expected,
            written: 0,
        }
    }

    pub(crate) fn finish_data(mut self) -> io::Result<PendingOutput> {
        if self
            .expected
            .is_some_and(|expected| self.written != expected)
        {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "archive member size mismatch",
            ));
        }
        self.writer.flush()?;
        if let Some(completion) = &mut self.pending.completion {
            completion.data_complete(self.written)?;
        }
        // The writer closes here, before a delayed integrity verdict. Waiting
        // for CRCs must not retain one open file handle per archive member.
        Ok(self.pending)
    }
}

impl<W: OutputWriter> MemberOutput<W> {
    pub(crate) fn metadata(&self, metadata: Metadata) -> io::Result<()> {
        self.writer.get_ref().metadata(metadata)
    }
}

impl<W: OutputWriter> OutputMember for MemberOutput<W> {
    fn metadata(&self, metadata: Metadata) -> io::Result<()> {
        MemberOutput::metadata(self, metadata)
    }

    fn finish_data(self) -> io::Result<PendingOutput> {
        MemberOutput::finish_data(self)
    }
}

impl<W: Write> Write for MemberOutput<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let end = self
            .written
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| io::Error::other("archive member size overflow"))?;
        if self.expected.is_some_and(|expected| end > expected) {
            return Err(io::Error::other("archive member exceeds declared size"));
        }
        let written = self.writer.write(bytes)?;
        self.written += written as u64;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

pub(crate) struct PendingOutput {
    completion: Option<Box<dyn OutputCompletion>>,
}

impl PendingOutput {
    pub(crate) fn integrity(mut self, integrity: Integrity) -> io::Result<()> {
        if let Some(completion) = &mut self.completion {
            completion.integrity(integrity)?;
        }
        self.completion = None;
        Ok(())
    }
}

impl Drop for PendingOutput {
    fn drop(&mut self) {
        if let Some(completion) = &mut self.completion {
            completion.abort();
        }
    }
}

pub(crate) struct FileOutput<'a> {
    pub root: &'a ExtractionRoot,
    pub budget: &'a Arc<JobExtractionBudget>,
}

impl ArchiveOutput for FileOutput<'_> {
    type Member = MemberOutput<cap_std::fs::File>;

    fn file(&self, _: usize, path: &Path, length: Option<u64>) -> io::Result<Self::Member> {
        let writer = self
            .root
            .create_file(path, self.budget)
            .map_err(io::Error::other)?;
        // The existing file backend already keeps its whole staging tree
        // private until manifest validation. It needs neither a callback
        // allocation nor dynamic dispatch on the ordinary write path.
        Ok(MemberOutput::new(writer, None, length))
    }

    fn directory(&self, _: usize, path: &Path) -> io::Result<()> {
        self.root
            .create_dir(path, self.budget)
            .map_err(io::Error::other)
    }

    fn directory_metadata(&self, _: usize, path: &Path, metadata: Metadata) -> io::Result<()> {
        self.root.set_directory_metadata(
            path,
            metadata.modified,
            metadata.accessed,
            metadata.unix_mode,
        )
    }
}
