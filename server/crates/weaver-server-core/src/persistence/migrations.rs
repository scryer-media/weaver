use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use crate::jobs::ids::{JobId, NzbFileId, SegmentId};
use crate::settings::Config;
use serde::{Deserialize, Serialize};

use crate::StateError;

use crate::jobs::{ActiveJob, CommittedSegment};
use crate::persistence::Database;

// --- Legacy journal types (kept for one-time migration from binary journal) ---

#[derive(Debug, Clone, Serialize, Deserialize)]
enum JournalEntry {
    JobCreated {
        job_id: JobId,
        #[allow(dead_code)]
        nzb_hash: [u8; 32],
        nzb_path: PathBuf,
        output_dir: PathBuf,
        created_at: u64,
        #[serde(default)]
        category: Option<String>,
        #[serde(default)]
        metadata: Vec<(String, String)>,
    },
    JobStatusChanged {
        job_id: JobId,
        status: PersistedJobStatus,
        #[allow(dead_code)]
        timestamp: u64,
    },
    SegmentCommitted {
        segment_id: SegmentId,
        #[allow(dead_code)]
        file_offset: u64,
        #[allow(dead_code)]
        decoded_size: u32,
        #[allow(dead_code)]
        crc32: u32,
    },
    FileComplete {
        file_id: NzbFileId,
        #[allow(dead_code)]
        filename: String,
        #[allow(dead_code)]
        md5: [u8; 16],
    },
    FileVerified {
        #[allow(dead_code)]
        file_id: NzbFileId,
        #[allow(dead_code)]
        status: PersistedVerifyStatus,
    },
    Par2MetadataLoaded {
        #[allow(dead_code)]
        job_id: JobId,
        #[allow(dead_code)]
        slice_size: u64,
        #[allow(dead_code)]
        recovery_block_count: u32,
    },
    MemberExtracted {
        #[allow(dead_code)]
        job_id: JobId,
        #[allow(dead_code)]
        member_name: String,
        #[allow(dead_code)]
        output_path: PathBuf,
    },
    ExtractionComplete {
        #[allow(dead_code)]
        job_id: JobId,
    },
    Checkpoint {
        #[allow(dead_code)]
        job_id: JobId,
        #[allow(dead_code)]
        timestamp: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum PersistedJobStatus {
    Downloading,
    Verifying,
    QueuedRepair,
    Repairing,
    QueuedExtract,
    Extracting,
    Complete,
    Failed { error: String },
    Paused,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum PersistedVerifyStatus {
    Intact,
    Damaged { bad_slices: u32, total_slices: u32 },
    Missing,
}

struct LegacyRecoveredJob {
    job_id: JobId,
    nzb_path: PathBuf,
    output_dir: PathBuf,
    committed_segments: HashSet<SegmentId>,
    complete_files: HashSet<NzbFileId>,
    status: PersistedJobStatus,
    created_at: u64,
    category: Option<String>,
    metadata: Vec<(String, String)>,
}

fn recover_legacy(entries: Vec<JournalEntry>) -> HashMap<JobId, LegacyRecoveredJob> {
    let mut jobs: HashMap<JobId, LegacyRecoveredJob> = HashMap::new();
    for entry in entries {
        match entry {
            JournalEntry::JobCreated {
                job_id,
                nzb_path,
                output_dir,
                created_at,
                category,
                metadata,
                ..
            } => {
                jobs.insert(
                    job_id,
                    LegacyRecoveredJob {
                        job_id,
                        nzb_path,
                        output_dir,
                        committed_segments: HashSet::new(),
                        complete_files: HashSet::new(),
                        status: PersistedJobStatus::Downloading,
                        created_at,
                        category,
                        metadata,
                    },
                );
            }
            JournalEntry::SegmentCommitted { segment_id, .. } => {
                if let Some(job) = jobs.get_mut(&segment_id.file_id.job_id) {
                    job.committed_segments.insert(segment_id);
                }
            }
            JournalEntry::FileComplete { file_id, .. } => {
                if let Some(job) = jobs.get_mut(&file_id.job_id) {
                    job.complete_files.insert(file_id);
                }
            }
            JournalEntry::JobStatusChanged { job_id, status, .. } => {
                if let Some(job) = jobs.get_mut(&job_id) {
                    job.status = status;
                }
            }
            _ => {}
        }
    }
    jobs
}

impl Database {
    fn already_migrated_toml_path(toml_path: &Path) -> bool {
        toml_path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.ends_with(".toml.migrated"))
    }

    /// If the database is empty and a `weaver.toml` file exists at the given path,
    /// import its settings and servers, then rename it to `weaver.toml.migrated`.
    pub fn migrate_from_toml(&self, toml_path: &Path) -> Result<bool, StateError> {
        if !self.is_empty()? {
            return Ok(false);
        }

        if !toml_path.exists() {
            return Ok(false);
        }

        let contents = std::fs::read_to_string(toml_path).map_err(StateError::Io)?;

        let mut config: Config = toml::from_str(&contents)
            .map_err(|e| StateError::Database(format!("failed to parse TOML: {e}")))?;

        // Assign IDs to any servers missing them.
        config.assign_server_ids();

        self.save_config(&config)?;

        if Self::already_migrated_toml_path(toml_path) {
            tracing::info!(
                from = %toml_path.display(),
                servers = config.servers.len(),
                "imported config from migrated TOML into SQLite"
            );
        } else {
            // Rename the TOML file so it's not re-imported.
            let migrated_path = toml_path.with_extension("toml.migrated");
            std::fs::rename(toml_path, &migrated_path).map_err(StateError::Io)?;

            tracing::info!(
                from = %toml_path.display(),
                to = %migrated_path.display(),
                servers = config.servers.len(),
                "migrated config from TOML to SQLite"
            );
        }

        Ok(true)
    }

    /// Migrate an existing binary journal into the active_* SQLite tables.
    ///
    /// Reads the journal, replays it via `recover()`, inserts the recovered
    /// state into SQLite, then renames the journal to `.journal.migrated`.
    /// Returns `true` if a migration was performed.
    pub fn migrate_from_journal(&self, journal_path: &Path) -> Result<bool, StateError> {
        if !journal_path.exists() {
            return Ok(false);
        }

        // Check if we already have active jobs (already migrated).
        {
            let conn = self.conn();
            let count: i64 = conn
                .query_row("SELECT COUNT(*) FROM active_jobs", [], |row| row.get(0))
                .map_err(|e| StateError::Database(e.to_string()))?;
            if count > 0 {
                return Ok(false);
            }
        }

        // Read the journal synchronously.
        let entries = read_journal_sync(journal_path)?;
        if entries.is_empty() {
            // Empty journal — just rename and move on.
            let migrated = journal_path.with_extension("journal.migrated");
            std::fs::rename(journal_path, &migrated).map_err(StateError::Io)?;
            tracing::info!("migrated empty journal to SQLite");
            return Ok(true);
        }

        // Replay to get recovered state.
        let recovered_jobs = recover_legacy(entries);

        // Insert into SQLite.
        for job in recovered_jobs.values() {
            self.create_active_job(&ActiveJob {
                job_id: job.job_id,
                nzb_hash: [0; 32], // hash not stored in journal entries after creation
                nzb_path: job.nzb_path.clone(),
                output_dir: job.output_dir.clone(),
                created_at: job.created_at,
                category: job.category.clone(),
                metadata: job.metadata.clone(),
            })?;

            // Map the PersistedJobStatus to a string for the active table.
            let (status_str, error_str) = match &job.status {
                PersistedJobStatus::Downloading => ("downloading", None),
                PersistedJobStatus::Verifying => ("verifying", None),
                PersistedJobStatus::QueuedRepair => ("queued_repair", None),
                PersistedJobStatus::Repairing => ("repairing", None),
                PersistedJobStatus::QueuedExtract => ("queued_extract", None),
                PersistedJobStatus::Extracting => ("extracting", None),
                PersistedJobStatus::Complete => ("complete", None),
                PersistedJobStatus::Failed { error } => ("failed", Some(error.as_str())),
                PersistedJobStatus::Paused => ("paused", None),
                PersistedJobStatus::Cancelled => ("cancelled", None),
            };
            self.set_active_job_status(job.job_id, status_str, error_str)?;

            // Batch-insert segments.
            let segments: Vec<CommittedSegment> = job
                .committed_segments
                .iter()
                .map(|seg| CommittedSegment {
                    job_id: seg.file_id.job_id,
                    file_index: seg.file_id.file_index,
                    segment_number: seg.segment_number,
                    file_offset: 0,  // offset not recoverable from HashSet
                    decoded_size: 0, // size not recoverable from HashSet
                    crc32: 0,        // crc not recoverable from HashSet
                })
                .collect();
            self.commit_segments(&segments)?;

            // Insert completed files.
            for file_id in &job.complete_files {
                self.complete_file(
                    file_id.job_id,
                    file_id.file_index,
                    "",       // filename not recoverable from HashSet
                    &[0; 16], // md5 not recoverable from HashSet
                )?;
            }
        }

        // Rename journal so it's not re-imported.
        let migrated = journal_path.with_extension("journal.migrated");
        std::fs::rename(journal_path, &migrated).map_err(StateError::Io)?;

        tracing::info!(
            jobs = recovered_jobs.len(),
            from = %journal_path.display(),
            to = %migrated.display(),
            "migrated journal to SQLite"
        );

        Ok(true)
    }
}

/// Read a binary journal file synchronously.
/// Format: repeated [4-byte LE length][payload][4-byte LE CRC32].
fn read_journal_sync(path: &Path) -> Result<Vec<JournalEntry>, StateError> {
    let data = std::fs::read(path).map_err(StateError::Io)?;
    let mut entries = Vec::new();
    let mut pos = 0;

    while pos + 4 <= data.len() {
        let payload_len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;

        if pos + payload_len + 4 > data.len() {
            // Truncated entry at tail — stop.
            break;
        }

        let payload = &data[pos..pos + payload_len];
        let stored_crc = u32::from_le_bytes(
            data[pos + payload_len..pos + payload_len + 4]
                .try_into()
                .unwrap(),
        );
        pos += payload_len + 4;

        let computed_crc = weaver_par2::checksum::crc32(payload);
        if stored_crc != computed_crc {
            tracing::warn!("CRC mismatch during journal migration — skipping entry");
            continue;
        }

        match rmp_serde::from_slice::<JournalEntry>(payload) {
            Ok(entry) => entries.push(entry),
            Err(e) => {
                tracing::warn!(%e, "failed to deserialize journal entry during migration");
            }
        }
    }

    Ok(entries)
}

#[cfg(test)]
mod tests;
