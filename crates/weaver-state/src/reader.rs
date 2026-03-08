use std::path::Path;

use tokio::fs::File;
use tokio::io::AsyncReadExt;

use crate::entry::JournalEntry;
use crate::error::StateError;

/// Journal reader that tolerates truncation from crash mid-write.
pub struct JournalReader {
    file: File,
}

impl JournalReader {
    /// Open a journal file for reading.
    pub async fn open(path: &Path) -> Result<Self, StateError> {
        let file = File::open(path).await?;
        Ok(Self { file })
    }

    /// Read the next entry. Returns `None` at EOF.
    ///
    /// Truncated or corrupt entries at the tail of the file are silently
    /// skipped (they represent incomplete writes from a crash). A warning
    /// is emitted via `tracing`.
    pub async fn next_entry(&mut self) -> Result<Option<JournalEntry>, StateError> {
        loop {
            // Read the 4-byte length header.
            let mut len_buf = [0u8; 4];
            match self.file.read_exact(&mut len_buf).await {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
                Err(e) => return Err(StateError::Io(e)),
            }

            let payload_len = u32::from_le_bytes(len_buf) as usize;

            // Read the payload + CRC (payload_len + 4 bytes for CRC).
            let total = payload_len + 4;
            let mut buf = vec![0u8; total];
            match self.file.read_exact(&mut buf).await {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    tracing::warn!(
                        payload_len,
                        "truncated journal entry at EOF — skipping incomplete write"
                    );
                    return Ok(None);
                }
                Err(e) => return Err(StateError::Io(e)),
            }

            let payload = &buf[..payload_len];
            let stored_crc = u32::from_le_bytes(
                buf[payload_len..payload_len + 4]
                    .try_into()
                    .expect("4 bytes for CRC"),
            );

            let computed_crc = weaver_core::checksum::crc32(payload);
            if stored_crc != computed_crc {
                tracing::warn!(
                    expected = format!("{stored_crc:#010x}"),
                    actual = format!("{computed_crc:#010x}"),
                    "CRC mismatch in journal entry — skipping corrupt entry"
                );
                // Skip this entry and try the next one.
                continue;
            }

            let entry: JournalEntry = rmp_serde::from_slice(payload)
                .map_err(|e| StateError::Deserialize(e.to_string()))?;

            return Ok(Some(entry));
        }
    }

    /// Read all entries into a Vec.
    pub async fn read_all(&mut self) -> Result<Vec<JournalEntry>, StateError> {
        let mut entries = Vec::new();
        while let Some(entry) = self.next_entry().await? {
            entries.push(entry);
        }
        Ok(entries)
    }
}
