// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::{
    fs::File,
    io::IoSlice,
    ops::{Deref, DerefMut},
    path::PathBuf,
};

use anyhow::Context;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::Error;

pub trait OurFileExt {
    fn pwrite_all_vectored(&self, offset: u64, bufs: [IoSlice<'_>; 2]) -> anyhow::Result<usize>;
    fn pread_exact(&self, offset: u64, buf: &mut [u8]) -> anyhow::Result<()>;
    fn pwrite_all(&self, offset: u64, buf: &[u8]) -> anyhow::Result<()>;
}

impl OurFileExt for File {
    #[cfg(unix)]
    fn pwrite_all_vectored(&self, offset: u64, bufs: [IoSlice<'_>; 2]) -> anyhow::Result<usize> {
        // off_t is 32 bits on 32-bit Android (and on 32-bit Linux built without
        // _FILE_OFFSET_BITS=64), so pwritev cannot address anything past 2 GiB there.
        // pwrite_all goes through pwrite64 and takes the u64 as is, so past that point
        // write the two buffers one after the other instead.
        match nix::libc::off_t::try_from(offset) {
            Ok(offset) => {
                nix::sys::uio::pwritev(self, &bufs, offset).context("error calling pwritev")
            }
            Err(_) => pwrite_all_unvectored(self, offset, bufs),
        }
    }

    #[cfg(not(unix))]
    fn pwrite_all_vectored(&self, offset: u64, bufs: [IoSlice<'_>; 2]) -> anyhow::Result<usize> {
        match (bufs[0].len(), bufs[1].len()) {
            (len, 0) if len > 0 => {
                self.pwrite_all(offset, &bufs[0])?;
                Ok(len)
            }
            (0, len) if len > 0 => {
                self.pwrite_all(offset, &bufs[1])?;
                Ok(len)
            }
            (0, 0) => Ok(0),
            (l0, l1) => {
                // concatenate the buffers in memory so that we issue one write call instead of 2
                // assumes the message is <= CHUNK_SIZE
                use crate::core::constants::CHUNK_SIZE;
                let mut buf = [0u8; CHUNK_SIZE as usize];

                buf.get_mut(..l0)
                    .context("buf too small")?
                    .copy_from_slice(&bufs[0]);
                buf.get_mut(l0..l0 + l1)
                    .context("buf too small")?
                    .copy_from_slice(&bufs[1]);
                self.pwrite_all(offset, &buf[..l0 + l1])?;
                Ok(l0 + l1)
            }
        }
    }

    #[cfg(unix)]
    fn pread_exact(&self, offset: u64, buf: &mut [u8]) -> anyhow::Result<()> {
        use std::os::unix::fs::FileExt;

        Ok(self.read_exact_at(buf, offset)?)
    }

    #[cfg(windows)]
    fn pread_exact(&self, mut offset: u64, mut buf: &mut [u8]) -> anyhow::Result<()> {
        use std::os::windows::fs::FileExt;
        while !buf.is_empty() {
            let n = self.seek_read(buf, offset)?;
            if n == 0 {
                return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "eof").into());
            }
            offset += n as u64;
            buf = &mut buf[n..];
        }
        Ok(())
    }

    #[cfg(not(any(windows, unix)))]
    fn pread_exact(&self, offset: u64, buf: &mut [u8]) -> anyhow::Result<()> {
        anyhow::bail!("pread_exact not implemented for your platform")
    }

    #[cfg(unix)]
    fn pwrite_all(&self, offset: u64, buf: &[u8]) -> anyhow::Result<()> {
        use std::os::unix::fs::FileExt;
        Ok(self.write_all_at(buf, offset)?)
    }

    #[cfg(windows)]
    fn pwrite_all(&self, offset: u64, buf: &[u8]) -> anyhow::Result<()> {
        use std::os::windows::fs::FileExt;

        let mut remaining = buf.len();
        let mut buf = buf;
        let mut offset = offset;
        while remaining > 0 {
            let written = self.seek_write(&buf[..remaining], offset)?;
            remaining -= written;
            offset += written as u64;
            buf = &buf[written..];
        }
        Ok(())
    }

    #[cfg(not(any(windows, unix)))]
    fn pwrite_all(&self, offset: u64, buf: &[u8]) -> anyhow::Result<()> {
        anyhow::bail!("pwrite_all not implemented for your platform")
    }
}

/// `bufs` filled whole, one after the other, from the file's bytes at
/// `offset`, in as few positional reads as the platform allows: one preadv
/// for them all where it has one (libtorrent `disk_io_thread::do_read` reads
/// a cache line of blocks with one readv). A file too short says so.
pub fn pread_exact_vectored(
    file: &File,
    offset: u64,
    bufs: &mut [&mut [u8]],
) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::io::IoSliceMut;
        let total: u64 = bufs.iter().map(|buf| buf.len() as u64).sum();
        // off_t is 32 bits on some 32-bit targets: past it, read the buffers
        // one after the other, as pwrite_all_vectored() writes them.
        if nix::libc::off_t::try_from(offset.saturating_add(total)).is_ok() {
            let mut slices: Vec<IoSliceMut<'_>> =
                bufs.iter_mut().map(|buf| IoSliceMut::new(buf)).collect();
            let mut left: &mut [IoSliceMut<'_>] = &mut slices;
            let mut at = offset;
            while !left.is_empty() {
                let off = nix::libc::off_t::try_from(at).map_err(std::io::Error::other)?;
                match nix::sys::uio::preadv(file, left, off) {
                    Ok(0) => return Err(std::io::ErrorKind::UnexpectedEof.into()),
                    Ok(read) => {
                        at += read as u64;
                        IoSliceMut::advance_slices(&mut left, read);
                    }
                    Err(nix::errno::Errno::EINTR) => {}
                    Err(e) => return Err(e.into()),
                }
            }
            return Ok(());
        }
    }
    let mut at = offset;
    for buf in bufs.iter_mut() {
        read_exact_at(file, at, buf)?;
        at += buf.len() as u64;
    }
    Ok(())
}

// `buf` filled whole from the file's bytes at `offset`.
fn read_exact_at(file: &File, offset: u64, buf: &mut [u8]) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.read_exact_at(buf, offset)
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt;
        let mut read = 0usize;
        while read < buf.len() {
            let progress = file.seek_read(&mut buf[read..], offset + read as u64)?;
            if progress == 0 {
                return Err(std::io::ErrorKind::UnexpectedEof.into());
            }
            read += progress;
        }
        Ok(())
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = (file, offset, buf);
        Err(std::io::ErrorKind::Unsupported.into())
    }
}

/// What pwrite_all_vectored() falls back to when the offset does not fit the platform's
/// pwritev: the same bytes, in two plain positional writes.
#[cfg(unix)]
fn pwrite_all_unvectored(
    file: &File,
    mut offset: u64,
    bufs: [IoSlice<'_>; 2],
) -> anyhow::Result<usize> {
    for buf in &bufs {
        file.pwrite_all(offset, buf)?;
        offset += buf.len() as u64;
    }
    Ok(bufs[0].len() + bufs[1].len())
}

#[derive(Default, Debug)]
struct OpenedFileLocked {
    #[allow(unused)]
    path: PathBuf,
    fd: Option<File>,
    #[cfg(windows)]
    tried_marking_sparse: bool,
}

impl Deref for OpenedFileLocked {
    type Target = Option<File>;

    fn deref(&self) -> &Self::Target {
        &self.fd
    }
}

impl DerefMut for OpenedFileLocked {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.fd
    }
}

#[derive(Debug)]
pub(crate) struct OpenedFile {
    file: RwLock<OpenedFileLocked>,
}

impl OpenedFile {
    pub fn new(path: PathBuf, f: File) -> Self {
        Self {
            file: RwLock::new(OpenedFileLocked {
                path,
                fd: Some(f),
                #[cfg(windows)]
                tried_marking_sparse: false,
            }),
        }
    }

    pub fn new_dummy() -> Self {
        Self {
            file: RwLock::new(Default::default()),
        }
    }

    pub fn take_clone(&self) -> anyhow::Result<Self> {
        let f = std::mem::take(&mut *self.file.write());
        Ok(Self {
            file: RwLock::new(f),
        })
    }

    pub fn lock_read(&self) -> crate::Result<impl Deref<Target = File>> {
        RwLockReadGuard::try_map(self.file.read(), |f| f.as_ref())
            .ok()
            .ok_or(Error::FsFileIsNone)
    }

    #[allow(dead_code)]
    pub fn lock_write(&self) -> crate::Result<impl DerefMut<Target = File>> {
        RwLockWriteGuard::try_map(self.file.write(), |f| f.as_mut())
            .ok()
            .ok_or(Error::FsFileIsNone)
    }

    #[cfg(windows)]
    pub fn try_mark_sparse(&self) -> crate::Result<impl Deref<Target = File>> {
        {
            let g = self.file.read();
            if g.tried_marking_sparse {
                return RwLockReadGuard::try_map(g, |f| f.fd.as_ref())
                    .ok()
                    .ok_or(Error::FsFileIsNone);
            }
        }
        let mut g = self.file.write();
        if !g.tried_marking_sparse {
            g.tried_marking_sparse = true;
            let f = g.fd.as_ref().ok_or(Error::FsFileIsNone)?;
            let marked = super::sparse::mark_file_sparse(f);
            tracing::debug!(path=?g.path, marked, "marking sparse");
        }
        let g = parking_lot::RwLockWriteGuard::downgrade(g);
        Ok(RwLockReadGuard::try_map(g, |f| f.fd.as_ref()).ok().unwrap())
    }
}

#[cfg(test)]
mod tests {
    use std::io::{IoSlice, Read};

    use crate::core::constants::CHUNK_SIZE;
    use crate::peer_protocol::DoubleBufHelper;
    use tempfile::TempDir;

    use crate::storage::filesystem::opened_file::OurFileExt;

    // Writes every split of a random buffer at `offset` with `write`, and checks the file
    // holds exactly those bytes there.
    fn check_pwrite_all_vectored(
        name: &str,
        offset: u64,
        write: impl Fn(&std::fs::File, u64, [IoSlice<'_>; 2]) -> anyhow::Result<usize>,
    ) {
        use std::io::Seek;

        let td = TempDir::with_prefix(name).unwrap();
        let mut tmp_buf = [0u8; CHUNK_SIZE as usize];
        for bufsize in [10000usize, CHUNK_SIZE as usize] {
            let mut buf = vec![0u8; bufsize];
            rand::fill(&mut buf[..]);
            for split_point in [0, bufsize / 2, bufsize] {
                let path = td.path().join(format!("file_{bufsize}_{split_point}"));
                let file = std::fs::OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(&path)
                    .unwrap();
                let (first, second) = buf.split_at(split_point);
                let bufs = DoubleBufHelper::new(first, second).as_ioslices(bufsize);
                assert_eq!(write(&file, offset, bufs).unwrap(), bufsize, "{path:?}");

                let mut file = std::fs::File::open(&path).unwrap();
                assert_eq!(
                    file.metadata().unwrap().len(),
                    offset + bufsize as u64,
                    "{path:?}"
                );
                file.seek(std::io::SeekFrom::Start(offset)).unwrap();
                file.read_exact(&mut tmp_buf[..bufsize]).unwrap();
                assert_eq!(&tmp_buf[..bufsize], buf);
            }
        }
    }

    #[test]
    fn test_pwrite_all_vectored() {
        check_pwrite_all_vectored("test_pwrite_all_vectored", 0, |f, offset, bufs| {
            f.pwrite_all_vectored(offset, bufs)
        });
    }

    // 2 GiB is where a 32-bit off_t runs out. On a 64-bit host this still goes through
    // pwritev, so it cannot reproduce the 32-bit failure; on a 32-bit target the same
    // test takes the fallback.
    //
    // Unix only, and not because the code under test is: writing at this offset leaves a
    // 2 GiB hole, which is free where files are sparse by default and is not on NTFS,
    // where Windows zero-fills it -- six files of it, more than a CI runner has room or
    // time for. Windows takes the seek_write path below, whose offset is a u64 with no
    // such boundary, so there is nothing there for this test to find.
    #[cfg(unix)]
    #[test]
    fn test_pwrite_all_vectored_past_2gib() {
        check_pwrite_all_vectored(
            "test_pwrite_all_vectored_past_2gib",
            1u64 << 31,
            |f, offset, bufs| f.pwrite_all_vectored(offset, bufs),
        );
    }

    // The fallback itself, at an offset a 32-bit off_t cannot hold: the path a 32-bit
    // Android device takes for every write past 2 GiB into a file.
    #[cfg(unix)]
    #[test]
    fn test_pwrite_all_unvectored_past_2gib() {
        check_pwrite_all_vectored(
            "test_pwrite_all_unvectored_past_2gib",
            (1u64 << 31) + 12345,
            super::pwrite_all_unvectored,
        );
    }

    // Each buffer is filled in turn from the bytes at the offset, an empty
    // one among them, and a read past the end of the file says so.
    #[test]
    fn pread_exact_vectored_fills_each_buffer_in_turn() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("f");
        let bytes: Vec<u8> = (0..50_000u32)
            .map(|i| (i * 7 + i / 251).to_le_bytes()[0])
            .collect();
        std::fs::write(&path, &bytes).unwrap();
        let file = std::fs::File::open(&path).unwrap();

        let (mut a, mut b, mut c, mut d) = (vec![0; 3], vec![0; 16384], vec![], vec![0; 9000]);
        let mut bufs: Vec<&mut [u8]> = vec![&mut a, &mut b, &mut c, &mut d];
        super::pread_exact_vectored(&file, 77, &mut bufs).unwrap();
        assert_eq!(a, bytes[77..80]);
        assert_eq!(b, bytes[80..16464]);
        assert_eq!(d, bytes[16464..25464]);

        let (mut e, mut f) = (vec![0; 100], vec![0; 100]);
        let mut past: Vec<&mut [u8]> = vec![&mut e, &mut f];
        let err = super::pread_exact_vectored(&file, 49_850, &mut past).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
    }
}
