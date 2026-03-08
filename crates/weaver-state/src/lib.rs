mod entry;
mod error;
mod reader;
mod recovery;
mod writer;

pub use entry::{JournalEntry, PersistedJobStatus, PersistedVerifyStatus};
pub use error::StateError;
pub use reader::JournalReader;
pub use recovery::{recover, RecoveredJob, RecoveryState};
pub use writer::JournalWriter;

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use weaver_core::id::{JobId, NzbFileId, SegmentId};

    use super::*;

    /// Helper: create a temp file path for journal tests.
    fn temp_journal_path(name: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "weaver_state_test_{}_{}.journal",
            name,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        path
    }

    fn sample_job_created() -> JournalEntry {
        JournalEntry::JobCreated {
            job_id: JobId(1),
            nzb_hash: [0xAA; 32],
            nzb_path: PathBuf::from("/tmp/test.nzb"),
            output_dir: PathBuf::from("/tmp/output"),
            created_at: 1700000000,
            category: None,
            metadata: vec![],
        }
    }

    fn sample_segment_committed() -> JournalEntry {
        JournalEntry::SegmentCommitted {
            segment_id: SegmentId {
                file_id: NzbFileId {
                    job_id: JobId(1),
                    file_index: 0,
                },
                segment_number: 5,
            },
            file_offset: 1024,
            decoded_size: 768000,
            crc32: 0xDEADBEEF,
        }
    }

    fn sample_file_complete() -> JournalEntry {
        JournalEntry::FileComplete {
            file_id: NzbFileId {
                job_id: JobId(1),
                file_index: 0,
            },
            filename: "data.rar".to_string(),
            md5: [0x11; 16],
        }
    }

    /// Helper to create one of each variant for the multiple_entry_types test.
    fn all_variants() -> Vec<JournalEntry> {
        vec![
            sample_job_created(),
            JournalEntry::JobStatusChanged {
                job_id: JobId(1),
                status: PersistedJobStatus::Downloading,
                timestamp: 1700000001,
            },
            sample_segment_committed(),
            sample_file_complete(),
            JournalEntry::FileVerified {
                file_id: NzbFileId {
                    job_id: JobId(1),
                    file_index: 0,
                },
                status: PersistedVerifyStatus::Intact,
            },
            JournalEntry::FileVerified {
                file_id: NzbFileId {
                    job_id: JobId(1),
                    file_index: 1,
                },
                status: PersistedVerifyStatus::Damaged {
                    bad_slices: 2,
                    total_slices: 10,
                },
            },
            JournalEntry::FileVerified {
                file_id: NzbFileId {
                    job_id: JobId(1),
                    file_index: 2,
                },
                status: PersistedVerifyStatus::Missing,
            },
            JournalEntry::Par2MetadataLoaded {
                job_id: JobId(1),
                slice_size: 384000,
                recovery_block_count: 8,
            },
            JournalEntry::MemberExtracted {
                job_id: JobId(1),
                member_name: "movie.mkv".to_string(),
                output_path: PathBuf::from("/tmp/output/movie.mkv"),
            },
            JournalEntry::ExtractionComplete { job_id: JobId(1) },
            JournalEntry::JobStatusChanged {
                job_id: JobId(1),
                status: PersistedJobStatus::Complete,
                timestamp: 1700000099,
            },
            JournalEntry::JobStatusChanged {
                job_id: JobId(2),
                status: PersistedJobStatus::Failed {
                    error: "missing volumes".to_string(),
                },
                timestamp: 1700000100,
            },
            JournalEntry::JobStatusChanged {
                job_id: JobId(3),
                status: PersistedJobStatus::Paused,
                timestamp: 1700000101,
            },
            JournalEntry::Checkpoint {
                job_id: JobId(1),
                timestamp: 1700000200,
            },
        ]
    }

    #[tokio::test]
    async fn write_read_roundtrip() {
        let path = temp_journal_path("roundtrip");
        let _cleanup = TempFileGuard(&path);

        let entries = vec![
            sample_job_created(),
            sample_segment_committed(),
            sample_file_complete(),
        ];

        {
            let mut writer = JournalWriter::open(&path).await.unwrap();
            for entry in &entries {
                writer.append(entry).await.unwrap();
            }
            writer.sync().await.unwrap();
        }

        let mut reader = JournalReader::open(&path).await.unwrap();
        let read_back = reader.read_all().await.unwrap();

        assert_eq!(read_back.len(), entries.len());
        // Verify via JSON serialization for structural equality.
        for (original, recovered) in entries.iter().zip(read_back.iter()) {
            assert_eq!(
                serde_json::to_string(original).unwrap(),
                serde_json::to_string(recovered).unwrap(),
            );
        }
    }

    #[tokio::test]
    async fn empty_journal() {
        let path = temp_journal_path("empty");
        let _cleanup = TempFileGuard(&path);

        // Create an empty file.
        tokio::fs::write(&path, b"").await.unwrap();

        let mut reader = JournalReader::open(&path).await.unwrap();
        let entries = reader.read_all().await.unwrap();
        assert!(entries.is_empty());
    }

    #[tokio::test]
    async fn truncated_entry_recovery() {
        let path = temp_journal_path("truncated");
        let _cleanup = TempFileGuard(&path);

        // Write 3 entries.
        {
            let mut writer = JournalWriter::open(&path).await.unwrap();
            writer.append(&sample_job_created()).await.unwrap();
            writer.append(&sample_segment_committed()).await.unwrap();
            writer.append(&sample_file_complete()).await.unwrap();
            writer.sync().await.unwrap();
        }

        // Read the file and truncate it mid-entry (remove last few bytes).
        let data = tokio::fs::read(&path).await.unwrap();
        let truncated = &data[..data.len() - 5];
        tokio::fs::write(&path, truncated).await.unwrap();

        let mut reader = JournalReader::open(&path).await.unwrap();
        let entries = reader.read_all().await.unwrap();

        // Should recover the first 2 complete entries.
        assert_eq!(entries.len(), 2);
    }

    #[tokio::test]
    async fn crc_corruption() {
        let path = temp_journal_path("crc_corrupt");
        let _cleanup = TempFileGuard(&path);

        // Write 3 entries.
        {
            let mut writer = JournalWriter::open(&path).await.unwrap();
            writer.append(&sample_job_created()).await.unwrap();
            writer.append(&sample_segment_committed()).await.unwrap();
            writer.append(&sample_file_complete()).await.unwrap();
            writer.sync().await.unwrap();
        }

        // Corrupt a byte in the second entry's payload.
        let mut data = tokio::fs::read(&path).await.unwrap();

        // Find the start of the second entry: read the first entry's length.
        let first_len = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        let second_entry_start = 4 + first_len + 4; // header + payload + crc
        // Flip a byte in the second entry's payload (after the length header).
        let corrupt_offset = second_entry_start + 4 + 1; // into the payload
        data[corrupt_offset] ^= 0xFF;
        tokio::fs::write(&path, &data).await.unwrap();

        let mut reader = JournalReader::open(&path).await.unwrap();
        let entries = reader.read_all().await.unwrap();

        // Entry 1 and 3 should be intact; entry 2 is corrupt and skipped.
        assert_eq!(entries.len(), 2);
    }

    #[tokio::test]
    async fn multiple_entry_types() {
        let path = temp_journal_path("all_variants");
        let _cleanup = TempFileGuard(&path);

        let entries = all_variants();

        {
            let mut writer = JournalWriter::open(&path).await.unwrap();
            for entry in &entries {
                writer.append(entry).await.unwrap();
            }
            writer.sync().await.unwrap();
        }

        let mut reader = JournalReader::open(&path).await.unwrap();
        let read_back = reader.read_all().await.unwrap();

        assert_eq!(read_back.len(), entries.len());
        for (original, recovered) in entries.iter().zip(read_back.iter()) {
            assert_eq!(
                serde_json::to_string(original).unwrap(),
                serde_json::to_string(recovered).unwrap(),
            );
        }
    }

    #[tokio::test]
    async fn large_journal() {
        let path = temp_journal_path("large");
        let _cleanup = TempFileGuard(&path);

        let count = 1000u64;
        {
            let mut writer = JournalWriter::open(&path).await.unwrap();
            for i in 0..count {
                let entry = JournalEntry::SegmentCommitted {
                    segment_id: SegmentId {
                        file_id: NzbFileId {
                            job_id: JobId(1),
                            file_index: (i / 100) as u32,
                        },
                        segment_number: (i % 100) as u32,
                    },
                    file_offset: i * 768000,
                    decoded_size: 768000,
                    crc32: weaver_core::checksum::crc32(&i.to_le_bytes()),
                };
                writer.append(&entry).await.unwrap();
            }
            writer.sync().await.unwrap();
        }

        let mut reader = JournalReader::open(&path).await.unwrap();
        let entries = reader.read_all().await.unwrap();
        assert_eq!(entries.len(), count as usize);

        // Spot-check the first and last entries.
        match &entries[0] {
            JournalEntry::SegmentCommitted {
                segment_id,
                file_offset,
                ..
            } => {
                assert_eq!(segment_id.file_id.file_index, 0);
                assert_eq!(segment_id.segment_number, 0);
                assert_eq!(*file_offset, 0);
            }
            other => panic!("unexpected entry type: {other:?}"),
        }
        match &entries[999] {
            JournalEntry::SegmentCommitted {
                segment_id,
                file_offset,
                ..
            } => {
                assert_eq!(segment_id.file_id.file_index, 9);
                assert_eq!(segment_id.segment_number, 99);
                assert_eq!(*file_offset, 999 * 768000);
            }
            other => panic!("unexpected entry type: {other:?}"),
        }
    }

    #[tokio::test]
    async fn append_to_existing() {
        let path = temp_journal_path("append");
        let _cleanup = TempFileGuard(&path);

        // Write 2 entries, close.
        {
            let mut writer = JournalWriter::open(&path).await.unwrap();
            writer.append(&sample_job_created()).await.unwrap();
            writer.append(&sample_segment_committed()).await.unwrap();
            writer.sync().await.unwrap();
        }

        // Reopen, write 2 more.
        {
            let mut writer = JournalWriter::open(&path).await.unwrap();
            writer.append(&sample_file_complete()).await.unwrap();
            writer
                .append(&JournalEntry::ExtractionComplete {
                    job_id: JobId(1),
                })
                .await
                .unwrap();
            writer.sync().await.unwrap();
        }

        let mut reader = JournalReader::open(&path).await.unwrap();
        let entries = reader.read_all().await.unwrap();
        assert_eq!(entries.len(), 4);
    }

    #[tokio::test]
    async fn entries_written_counter() {
        let path = temp_journal_path("counter");
        let _cleanup = TempFileGuard(&path);

        let mut writer = JournalWriter::open(&path).await.unwrap();
        assert_eq!(writer.entries_written(), 0);

        writer.append(&sample_job_created()).await.unwrap();
        assert_eq!(writer.entries_written(), 1);

        writer.append(&sample_segment_committed()).await.unwrap();
        assert_eq!(writer.entries_written(), 2);

        writer.append(&sample_file_complete()).await.unwrap();
        assert_eq!(writer.entries_written(), 3);
    }

    /// RAII guard that removes the temp file on drop.
    struct TempFileGuard<'a>(&'a std::path::Path);

    impl Drop for TempFileGuard<'_> {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(self.0);
        }
    }
}
