use crate::classify::FileRole;
use crate::id::{JobId, NzbFileId, SegmentId};

/// Pipeline events emitted by the scheduler and consumed by the state journal,
/// API subscriptions, and internal scheduler logic.
///
/// These events form the heartbeat of the byte pipeline. Each event represents
/// a state transition that downstream consumers can react to.
#[derive(Debug, Clone)]
pub enum PipelineEvent {
    // ---- Job lifecycle ----
    /// A new job was created from an NZB submission.
    JobCreated {
        job_id: JobId,
        name: String,
        total_files: u32,
        total_bytes: u64,
    },

    /// Job was paused by the user.
    JobPaused { job_id: JobId },

    /// Job was resumed by the user.
    JobResumed { job_id: JobId },

    /// Job was cancelled by the user.
    JobCancelled { job_id: JobId },

    /// All downloads paused globally.
    GlobalPaused,

    /// All downloads resumed globally.
    GlobalResumed,

    /// All files downloaded, verified, and extracted.
    JobCompleted { job_id: JobId },

    /// Job failed permanently.
    JobFailed { job_id: JobId, error: String },

    // ---- Download stage ----
    /// A segment was queued for download.
    SegmentQueued {
        segment_id: SegmentId,
        byte_estimate: u32,
    },

    /// Raw article data received from NNTP.
    ArticleDownloaded {
        segment_id: SegmentId,
        raw_size: u32,
    },

    /// A job started an active article download pass.
    DownloadStarted { job_id: JobId },

    /// A job finished an active article download pass.
    DownloadFinished { job_id: JobId },

    /// Article not found on any configured server.
    ArticleNotFound { segment_id: SegmentId },

    /// A failed segment was scheduled for retry with backoff.
    SegmentRetryScheduled {
        segment_id: SegmentId,
        attempt: u32,
        delay_secs: f64,
    },

    /// A segment permanently failed after exhausting all retries.
    SegmentFailedPermanent {
        segment_id: SegmentId,
        error: String,
    },

    // ---- Decode stage ----
    /// yEnc decode completed. Data is in a pooled buffer.
    SegmentDecoded {
        segment_id: SegmentId,
        decoded_size: u32,
        file_offset: u64,
        crc_valid: bool,
    },

    /// yEnc decode failed for a segment.
    SegmentDecodeFailed {
        segment_id: SegmentId,
        error: String,
    },

    // ---- Assembly stage ----
    /// Decoded segment data committed to disk.
    SegmentCommitted { segment_id: SegmentId },

    /// All segments for a file have been received and committed.
    FileComplete {
        file_id: NzbFileId,
        filename: String,
        total_bytes: u64,
    },

    /// Some segments for a file could not be retrieved.
    FileMissing {
        file_id: NzbFileId,
        filename: String,
        missing_segments: u32,
    },

    // ---- Verification stage ----
    /// File verification started.
    VerificationStarted { file_id: NzbFileId },

    /// File verification completed.
    VerificationComplete {
        file_id: NzbFileId,
        status: FileVerifyStatus,
    },

    /// PAR2 metadata was parsed, enabling verification.
    Par2MetadataLoaded { job_id: JobId },

    /// Job-level PAR2 verification started.
    JobVerificationStarted { job_id: JobId },

    /// Job-level PAR2 verification completed.
    JobVerificationComplete { job_id: JobId, passed: bool },

    /// Updated repair confidence after verification.
    RepairConfidenceUpdated {
        job_id: JobId,
        damaged_slices: u32,
        recovery_blocks_available: u32,
        repairable: bool,
    },

    // ---- Repair stage ----
    /// PAR2 repair started.
    RepairStarted { job_id: JobId },

    /// PAR2 repair completed.
    RepairComplete { job_id: JobId, slices_repaired: u32 },

    /// PAR2 repair failed.
    RepairFailed { job_id: JobId, error: String },

    // ---- Extraction stage ----
    /// Archive extraction is ready to begin.
    ExtractionReady { job_id: JobId },

    /// A specific archive member started extraction work.
    ExtractionMemberStarted {
        job_id: JobId,
        set_name: String,
        member: String,
    },

    /// A specific archive member is blocked waiting for another RAR volume.
    ExtractionMemberWaitingStarted {
        job_id: JobId,
        set_name: String,
        member: String,
        volume_index: usize,
    },

    /// A specific archive member resumed after a blocking volume wait.
    ExtractionMemberWaitingFinished {
        job_id: JobId,
        set_name: String,
        member: String,
        volume_index: usize,
    },

    /// A specific archive member started append/concat finalization.
    ExtractionMemberAppendStarted {
        job_id: JobId,
        set_name: String,
        member: String,
    },

    /// A specific archive member finished append/concat finalization.
    ExtractionMemberAppendFinished {
        job_id: JobId,
        set_name: String,
        member: String,
    },

    /// Progress on extracting a single archive member.
    ExtractionProgress {
        job_id: JobId,
        member: String,
        bytes_written: u64,
        total_bytes: u64,
    },

    /// A specific archive member finished extraction successfully.
    ExtractionMemberFinished {
        job_id: JobId,
        set_name: String,
        member: String,
    },

    /// A specific archive member failed extraction or CRC validation.
    ExtractionMemberFailed {
        job_id: JobId,
        set_name: String,
        member: String,
        error: String,
    },

    /// All archive members extracted.
    ExtractionComplete { job_id: JobId },

    /// Extraction failed.
    ExtractionFailed { job_id: JobId, error: String },

    // ---- File classification (discovered during download) ----
    /// A file's role was identified or updated.
    FileClassified { file_id: NzbFileId, role: FileRole },

    /// Final move from intermediate to complete has started.
    MoveToCompleteStarted { job_id: JobId },

    /// Final move from intermediate to complete finished successfully.
    MoveToCompleteFinished { job_id: JobId },
}

/// Result of verifying a file against PAR2 checksums.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileVerifyStatus {
    /// All slices verified successfully.
    Intact,
    /// Some slices failed verification.
    Damaged { bad_slices: u32, total_slices: u32 },
    /// File is completely missing.
    Missing,
    /// No PAR2 metadata available for this file.
    Unverifiable,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::NzbFileId;

    #[test]
    fn event_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<PipelineEvent>();
    }

    #[test]
    fn verify_status_equality() {
        assert_eq!(FileVerifyStatus::Intact, FileVerifyStatus::Intact);
        assert_ne!(
            FileVerifyStatus::Intact,
            FileVerifyStatus::Damaged {
                bad_slices: 1,
                total_slices: 10
            }
        );
    }

    #[test]
    fn event_debug_format() {
        let event = PipelineEvent::JobCreated {
            job_id: JobId(1),
            name: "test".into(),
            total_files: 5,
            total_bytes: 1024,
        };
        let debug = format!("{event:?}");
        assert!(debug.contains("JobCreated"));
        assert!(debug.contains("test"));
    }

    #[test]
    fn event_clone() {
        let event = PipelineEvent::FileComplete {
            file_id: NzbFileId {
                job_id: JobId(1),
                file_index: 0,
            },
            filename: "test.rar".into(),
            total_bytes: 1_000_000,
        };
        let cloned = event.clone();
        match cloned {
            PipelineEvent::FileComplete {
                filename,
                total_bytes,
                ..
            } => {
                assert_eq!(filename, "test.rar");
                assert_eq!(total_bytes, 1_000_000);
            }
            _ => panic!("wrong variant"),
        }
    }
}
