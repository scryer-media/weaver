use std::path::Path;

use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;

use crate::entry::JournalEntry;
use crate::error::StateError;

/// Append-only binary journal writer.
///
/// Wire format per entry:
/// ```text
/// [4 bytes: payload length as u32 LE]
/// [N bytes: MessagePack-encoded JournalEntry]
/// [4 bytes: CRC32 of the payload bytes, as u32 LE]
/// ```
pub struct JournalWriter {
    file: tokio::fs::File,
    entries_written: u64,
}

impl JournalWriter {
    /// Open or create a journal file. Appends to existing content.
    pub async fn open(path: &Path) -> Result<Self, StateError> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await?;

        Ok(Self {
            file,
            entries_written: 0,
        })
    }

    /// Append a journal entry. Flushes to disk.
    pub async fn append(&mut self, entry: &JournalEntry) -> Result<(), StateError> {
        let payload =
            rmp_serde::to_vec(entry).map_err(|e| StateError::Serialize(e.to_string()))?;

        let len = payload.len() as u32;
        let crc = weaver_core::checksum::crc32(&payload);

        self.file.write_all(&len.to_le_bytes()).await?;
        self.file.write_all(&payload).await?;
        self.file.write_all(&crc.to_le_bytes()).await?;
        self.file.flush().await?;

        self.entries_written += 1;
        Ok(())
    }

    /// Number of entries written since open.
    pub fn entries_written(&self) -> u64 {
        self.entries_written
    }

    /// Sync all buffered data to disk.
    pub async fn sync(&mut self) -> Result<(), StateError> {
        self.file.sync_all().await?;
        Ok(())
    }
}
