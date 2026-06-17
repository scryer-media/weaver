use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;
use std::range::Range;

/// Presents multiple files as a single contiguous `Read + Seek` stream.
///
/// Used for split archives (e.g., `.7z.001`, `.7z.002`) where the archive
/// parser expects a single seekable source.
pub struct SplitFileReader {
    /// Ordered file parts and their byte spans in the virtual stream.
    parts: Vec<Part>,
    total_size: u64,
    position: u64,
}

struct Part {
    file: File,
    /// Half-open byte span owned by this part in the virtual stream.
    span: Range<u64>,
}

impl SplitFileReader {
    /// Create a `SplitFileReader` from an ordered list of file paths.
    ///
    /// The files are concatenated in the order provided. All files are opened
    /// immediately so that errors surface early.
    pub fn open(paths: &[impl AsRef<Path>]) -> io::Result<Self> {
        if paths.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "no files provided to SplitFileReader",
            ));
        }

        let mut parts = Vec::with_capacity(paths.len());
        let mut offset = 0u64;

        for path in paths {
            let file = File::open(path.as_ref())?;
            let size = file.metadata()?.len();
            let end = offset.checked_add(size).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "split file size overflow")
            })?;
            parts.push(Part {
                file,
                span: Range { start: offset, end },
            });
            offset = end;
        }

        Ok(Self {
            parts,
            total_size: offset,
            position: 0,
        })
    }

    /// Find the part index and local offset for the current position.
    fn find_part(&self) -> Option<(usize, u64)> {
        if self.position >= self.total_size {
            return None;
        }
        // Binary search: find the last part whose start <= position.
        let idx = self
            .parts
            .partition_point(|p| p.span.start <= self.position)
            .saturating_sub(1);
        let part = &self.parts[idx];
        debug_assert!(part.span.contains(&self.position));
        let local_offset = self.position - part.span.start;
        Some((idx, local_offset))
    }
}

impl Read for SplitFileReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() || self.position >= self.total_size {
            return Ok(0);
        }

        let mut total_read = 0;
        while total_read < buf.len() && self.position < self.total_size {
            let Some((idx, local_offset)) = self.find_part() else {
                break;
            };
            let part = &mut self.parts[idx];

            // Seek the underlying file to the right position.
            part.file.seek(SeekFrom::Start(local_offset))?;

            // Limit read to remaining bytes in this part.
            let remaining_in_part = part.span.end - self.position;
            let to_read = (buf.len() - total_read).min(remaining_in_part as usize);

            let n = part.file.read(&mut buf[total_read..total_read + to_read])?;
            if n == 0 {
                break;
            }
            total_read += n;
            self.position += n as u64;
        }

        Ok(total_read)
    }
}

impl Seek for SplitFileReader {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let new_pos = match pos {
            SeekFrom::Start(offset) => offset as i64,
            SeekFrom::End(offset) => self.total_size as i64 + offset,
            SeekFrom::Current(offset) => self.position as i64 + offset,
        };

        if new_pos < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "seek to negative position",
            ));
        }

        self.position = new_pos as u64;
        Ok(self.position)
    }
}

#[cfg(test)]
mod tests;
