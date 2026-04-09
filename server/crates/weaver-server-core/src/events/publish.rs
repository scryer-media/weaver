use crate::events::model::PipelineEvent;

pub fn should_record_job_event(event: &PipelineEvent) -> bool {
    !matches!(
        event,
        PipelineEvent::ArticleDownloaded { .. }
            | PipelineEvent::ArticleNotFound { .. }
            | PipelineEvent::SegmentQueued { .. }
            | PipelineEvent::SegmentDecoded { .. }
            | PipelineEvent::SegmentDecodeFailed { .. }
            | PipelineEvent::SegmentCommitted { .. }
            | PipelineEvent::SegmentRetryScheduled { .. }
            | PipelineEvent::SegmentFailedPermanent { .. }
            | PipelineEvent::FileComplete { .. }
            | PipelineEvent::FileClassified { .. }
            | PipelineEvent::VerificationStarted { .. }
            | PipelineEvent::VerificationComplete { .. }
            | PipelineEvent::ExtractionProgress { .. }
            | PipelineEvent::RepairConfidenceUpdated { .. }
    )
}

pub fn pipeline_job_id(event: &PipelineEvent) -> Option<u64> {
    match event {
        PipelineEvent::JobCreated { job_id, .. }
        | PipelineEvent::JobPaused { job_id }
        | PipelineEvent::JobResumed { job_id }
        | PipelineEvent::JobCancelled { job_id }
        | PipelineEvent::JobCompleted { job_id }
        | PipelineEvent::JobFailed { job_id, .. }
        | PipelineEvent::DownloadStarted { job_id }
        | PipelineEvent::DownloadFinished { job_id }
        | PipelineEvent::Par2MetadataLoaded { job_id }
        | PipelineEvent::JobVerificationStarted { job_id }
        | PipelineEvent::JobVerificationComplete { job_id, .. }
        | PipelineEvent::RepairConfidenceUpdated { job_id, .. }
        | PipelineEvent::RepairStarted { job_id }
        | PipelineEvent::RepairComplete { job_id, .. }
        | PipelineEvent::RepairFailed { job_id, .. }
        | PipelineEvent::ExtractionReady { job_id }
        | PipelineEvent::ExtractionMemberStarted { job_id, .. }
        | PipelineEvent::ExtractionMemberWaitingStarted { job_id, .. }
        | PipelineEvent::ExtractionMemberWaitingFinished { job_id, .. }
        | PipelineEvent::ExtractionMemberAppendStarted { job_id, .. }
        | PipelineEvent::ExtractionMemberAppendFinished { job_id, .. }
        | PipelineEvent::ExtractionProgress { job_id, .. }
        | PipelineEvent::ExtractionMemberFinished { job_id, .. }
        | PipelineEvent::ExtractionMemberFailed { job_id, .. }
        | PipelineEvent::ExtractionComplete { job_id }
        | PipelineEvent::ExtractionFailed { job_id, .. }
        | PipelineEvent::MoveToCompleteStarted { job_id }
        | PipelineEvent::MoveToCompleteFinished { job_id } => Some(job_id.0),
        PipelineEvent::FileClassified { file_id, .. }
        | PipelineEvent::VerificationStarted { file_id }
        | PipelineEvent::VerificationComplete { file_id, .. }
        | PipelineEvent::FileComplete { file_id, .. }
        | PipelineEvent::FileMissing { file_id, .. } => Some(file_id.job_id.0),
        PipelineEvent::SegmentQueued { segment_id, .. }
        | PipelineEvent::ArticleDownloaded { segment_id, .. }
        | PipelineEvent::ArticleNotFound { segment_id }
        | PipelineEvent::SegmentRetryScheduled { segment_id, .. }
        | PipelineEvent::SegmentFailedPermanent { segment_id, .. }
        | PipelineEvent::SegmentDecoded { segment_id, .. }
        | PipelineEvent::SegmentDecodeFailed { segment_id, .. }
        | PipelineEvent::SegmentCommitted { segment_id } => Some(segment_id.file_id.job_id.0),
        PipelineEvent::GlobalPaused | PipelineEvent::GlobalResumed => None,
    }
}
