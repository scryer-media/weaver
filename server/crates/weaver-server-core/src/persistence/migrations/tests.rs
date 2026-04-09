use super::*;
use std::io::Write;

#[test]
fn migrate_from_toml_file() {
    let dir = std::env::temp_dir().join(format!(
        "weaver_migration_test_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();

    let toml_path = dir.join("weaver.toml");
    let mut f = std::fs::File::create(&toml_path).unwrap();
    write!(
        f,
        r#"
data_dir = "/tmp/weaver-data"

[[servers]]
id = 1
host = "news.example.com"
port = 443
tls = true
username = "user"
password = "pass"
connections = 10
active = true
"#
    )
    .unwrap();

    let db = Database::open_in_memory().unwrap();
    assert!(db.migrate_from_toml(&toml_path).unwrap());

    // Verify config was imported.
    let config = db.load_config().unwrap();
    assert_eq!(config.data_dir, "/tmp/weaver-data");
    assert_eq!(config.servers.len(), 1);
    assert_eq!(config.servers[0].host, "news.example.com");

    // Verify TOML was renamed.
    assert!(!toml_path.exists());
    assert!(dir.join("weaver.toml.migrated").exists());

    // Running again should be a no-op (DB not empty).
    // Create a dummy toml to verify it's not read.
    std::fs::write(&toml_path, "data_dir = \"/other\"").unwrap();
    assert!(!db.migrate_from_toml(&toml_path).unwrap());

    // Cleanup.
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn migrate_no_toml_file() {
    let db = Database::open_in_memory().unwrap();
    assert!(
        !db.migrate_from_toml(Path::new("/nonexistent/weaver.toml"))
            .unwrap()
    );
}

#[test]
fn migrate_no_journal_file() {
    let db = Database::open_in_memory().unwrap();
    assert!(
        !db.migrate_from_journal(Path::new("/nonexistent/weaver.journal"))
            .unwrap()
    );
}

/// Write a journal entry in the binary format: [4-byte LE len][payload][4-byte LE CRC].
fn write_journal_entry(buf: &mut Vec<u8>, entry: &JournalEntry) {
    let payload = rmp_serde::to_vec(entry).unwrap();
    let len = payload.len() as u32;
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(&payload);
    let crc = weaver_par2::checksum::crc32(&payload);
    buf.extend_from_slice(&crc.to_le_bytes());
}

#[test]
fn migrate_journal_to_sqlite() {
    use crate::jobs::ids::{JobId, NzbFileId, SegmentId};
    use std::path::PathBuf;

    let dir = std::env::temp_dir().join(format!(
        "weaver_journal_migration_test_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let journal_path = dir.join("weaver.journal");

    // Build a journal with 1 job, 3 segments, 1 completed file.
    let mut data = Vec::new();
    write_journal_entry(
        &mut data,
        &JournalEntry::JobCreated {
            job_id: JobId(1),
            nzb_hash: [0xAA; 32],
            nzb_path: PathBuf::from("/tmp/test.nzb"),
            output_dir: PathBuf::from("/tmp/output"),
            created_at: 1700000000,
            category: Some("movies".to_string()),
            metadata: vec![],
        },
    );
    for i in 0..3u32 {
        write_journal_entry(
            &mut data,
            &JournalEntry::SegmentCommitted {
                segment_id: SegmentId {
                    file_id: NzbFileId {
                        job_id: JobId(1),
                        file_index: 0,
                    },
                    segment_number: i,
                },
                file_offset: i as u64 * 768000,
                decoded_size: 768000,
                crc32: 0xDEADBEEF,
            },
        );
    }
    write_journal_entry(
        &mut data,
        &JournalEntry::FileComplete {
            file_id: NzbFileId {
                job_id: JobId(1),
                file_index: 0,
            },
            filename: "data.rar".to_string(),
            md5: [0x11; 16],
        },
    );
    write_journal_entry(
        &mut data,
        &JournalEntry::JobStatusChanged {
            job_id: JobId(1),
            status: PersistedJobStatus::Verifying,
            timestamp: 1700000050,
        },
    );
    std::fs::write(&journal_path, &data).unwrap();

    let db = Database::open_in_memory().unwrap();
    assert!(db.migrate_from_journal(&journal_path).unwrap());

    // Verify active state.
    let jobs = db.load_active_jobs().unwrap();
    assert_eq!(jobs.len(), 1);
    let job = &jobs[&JobId(1)];
    assert_eq!(job.nzb_path, PathBuf::from("/tmp/test.nzb"));
    assert_eq!(job.status, "verifying");
    assert_eq!(job.committed_segments.len(), 3);
    assert_eq!(job.complete_files.len(), 1);
    assert_eq!(job.category, Some("movies".to_string()));

    // Journal should be renamed.
    assert!(!journal_path.exists());
    assert!(dir.join("weaver.journal.migrated").exists());

    // Running again should be a no-op.
    std::fs::write(&journal_path, &data).unwrap();
    assert!(!db.migrate_from_journal(&journal_path).unwrap());

    let _ = std::fs::remove_dir_all(&dir);
}
