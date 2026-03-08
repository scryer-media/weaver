use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use weaver_core::id::{JobId, NzbFileId, SegmentId};

use crate::entry::{JournalEntry, PersistedJobStatus};

/// State recovered from replaying a journal.
#[derive(Debug)]
pub struct RecoveryState {
    pub jobs: HashMap<JobId, RecoveredJob>,
}

/// A single job's recovered state.
#[derive(Debug)]
pub struct RecoveredJob {
    pub job_id: JobId,
    pub nzb_path: PathBuf,
    pub output_dir: PathBuf,
    pub committed_segments: HashSet<SegmentId>,
    pub complete_files: HashSet<NzbFileId>,
    pub status: PersistedJobStatus,
    pub created_at: u64,
    pub category: Option<String>,
    pub metadata: Vec<(String, String)>,
}

/// Replay journal entries and build recovery state.
///
/// Processes entries in order. Each entry updates the corresponding job's
/// recovered state:
///
/// - `JobCreated` -- creates a new `RecoveredJob`
/// - `SegmentCommitted` -- adds the segment to `committed_segments`
/// - `FileComplete` -- adds the file to `complete_files`
/// - `JobStatusChanged` -- updates `status`
/// - `Checkpoint` and other variants -- no-op for recovery purposes
pub fn recover(entries: Vec<JournalEntry>) -> RecoveryState {
    let mut jobs: HashMap<JobId, RecoveredJob> = HashMap::new();

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
                    RecoveredJob {
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
                let job_id = segment_id.file_id.job_id;
                if let Some(job) = jobs.get_mut(&job_id) {
                    job.committed_segments.insert(segment_id);
                }
            }
            JournalEntry::FileComplete { file_id, .. } => {
                let job_id = file_id.job_id;
                if let Some(job) = jobs.get_mut(&job_id) {
                    job.complete_files.insert(file_id);
                }
            }
            JournalEntry::JobStatusChanged {
                job_id, status, ..
            } => {
                if let Some(job) = jobs.get_mut(&job_id) {
                    job.status = status;
                }
            }
            // Checkpoint, FileVerified, Par2MetadataLoaded, MemberExtracted,
            // ExtractionComplete are no-ops for recovery purposes.
            _ => {}
        }
    }

    RecoveryState { jobs }
}

#[cfg(test)]
mod tests {
    use super::*;
    use weaver_core::id::{JobId, NzbFileId, SegmentId};

    fn make_job_created(job_id: u64) -> JournalEntry {
        JournalEntry::JobCreated {
            job_id: JobId(job_id),
            nzb_hash: [0xAA; 32],
            nzb_path: PathBuf::from(format!("/tmp/test_{job_id}.nzb")),
            output_dir: PathBuf::from(format!("/tmp/output_{job_id}")),
            created_at: 1700000000 + job_id,
            category: None,
            metadata: vec![],
        }
    }

    fn make_segment_committed(job_id: u64, file_index: u32, segment_number: u32) -> JournalEntry {
        JournalEntry::SegmentCommitted {
            segment_id: SegmentId {
                file_id: NzbFileId {
                    job_id: JobId(job_id),
                    file_index,
                },
                segment_number,
            },
            file_offset: (segment_number as u64) * 768000,
            decoded_size: 768000,
            crc32: 0xDEADBEEF,
        }
    }

    fn make_file_complete(job_id: u64, file_index: u32) -> JournalEntry {
        JournalEntry::FileComplete {
            file_id: NzbFileId {
                job_id: JobId(job_id),
                file_index,
            },
            filename: format!("file_{file_index}.rar"),
            md5: [0x11; 16],
        }
    }

    fn make_status_changed(job_id: u64, status: PersistedJobStatus) -> JournalEntry {
        JournalEntry::JobStatusChanged {
            job_id: JobId(job_id),
            status,
            timestamp: 1700000050,
        }
    }

    #[test]
    fn empty_journal_recovery() {
        let state = recover(vec![]);
        assert!(state.jobs.is_empty());
    }

    #[test]
    fn single_job_recovery() {
        let entries = vec![
            make_job_created(1),
            make_segment_committed(1, 0, 0),
            make_segment_committed(1, 0, 1),
            make_segment_committed(1, 0, 2),
            make_file_complete(1, 0),
        ];

        let state = recover(entries);
        assert_eq!(state.jobs.len(), 1);

        let job = &state.jobs[&JobId(1)];
        assert_eq!(job.job_id, JobId(1));
        assert_eq!(job.nzb_path, PathBuf::from("/tmp/test_1.nzb"));
        assert_eq!(job.output_dir, PathBuf::from("/tmp/output_1"));
        assert_eq!(job.committed_segments.len(), 3);
        assert_eq!(job.complete_files.len(), 1);
        assert!(job.complete_files.contains(&NzbFileId {
            job_id: JobId(1),
            file_index: 0,
        }));
        assert_eq!(job.created_at, 1700000001);
        // Default status since no JobStatusChanged was sent
        assert_eq!(job.status, PersistedJobStatus::Downloading);
    }

    #[test]
    fn status_progression() {
        let entries = vec![
            make_job_created(1),
            make_status_changed(1, PersistedJobStatus::Downloading),
            make_segment_committed(1, 0, 0),
            make_status_changed(1, PersistedJobStatus::Verifying),
            make_status_changed(1, PersistedJobStatus::Complete),
        ];

        let state = recover(entries);
        let job = &state.jobs[&JobId(1)];
        assert_eq!(job.status, PersistedJobStatus::Complete);
    }

    #[test]
    fn multiple_jobs() {
        let entries = vec![
            make_job_created(1),
            make_job_created(2),
            make_segment_committed(1, 0, 0),
            make_segment_committed(2, 0, 0),
            make_segment_committed(1, 0, 1),
            make_segment_committed(2, 1, 0),
            make_file_complete(1, 0),
            make_file_complete(2, 0),
            make_file_complete(2, 1),
        ];

        let state = recover(entries);
        assert_eq!(state.jobs.len(), 2);

        let job1 = &state.jobs[&JobId(1)];
        assert_eq!(job1.committed_segments.len(), 2);
        assert_eq!(job1.complete_files.len(), 1);

        let job2 = &state.jobs[&JobId(2)];
        assert_eq!(job2.committed_segments.len(), 2);
        assert_eq!(job2.complete_files.len(), 2);
    }

    #[test]
    fn segments_not_duplicated() {
        let entries = vec![
            make_job_created(1),
            make_segment_committed(1, 0, 0),
            make_segment_committed(1, 0, 0), // duplicate
            make_segment_committed(1, 0, 0), // duplicate
        ];

        let state = recover(entries);
        let job = &state.jobs[&JobId(1)];
        assert_eq!(job.committed_segments.len(), 1);
    }

    #[test]
    fn incomplete_job() {
        let entries = vec![
            make_job_created(1),
            make_status_changed(1, PersistedJobStatus::Downloading),
            make_segment_committed(1, 0, 0),
            make_segment_committed(1, 0, 1),
            // No FileComplete, no status change to Complete
        ];

        let state = recover(entries);
        let job = &state.jobs[&JobId(1)];
        assert_eq!(job.status, PersistedJobStatus::Downloading);
        assert_eq!(job.committed_segments.len(), 2);
        assert!(job.complete_files.is_empty());
    }
}
