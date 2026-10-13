// Why a failed job failed, kept on its history row so clients that report
// per-stage results can tell a repair failure from a bad password, a full
// disk or a failed move.

use crate::jobs::{SemanticTerminalCause, classify_semantic_terminal_cause};
use crate::post_processing::model::PipelineFailureStage;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistoryFailureKind {
    // Verification or repair could not restore the files.
    Repair,
    // The archive password was wrong or missing.
    Password,
    // The disk filled up.
    DiskFull,
    // Any other unpack error.
    Unpack,
    // Moving or renaming the finished files failed.
    Move,
    // Too many articles were missing, or the download could not finish.
    Health,
    // Removed as a duplicate of another download.
    Duplicate,
}

impl HistoryFailureKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Repair => "repair",
            Self::Password => "password",
            Self::DiskFull => "disk_full",
            Self::Unpack => "unpack",
            Self::Move => "move",
            Self::Health => "health",
            Self::Duplicate => "duplicate",
        }
    }

    pub fn from_persisted(value: &str) -> Option<Self> {
        match value {
            "repair" => Some(Self::Repair),
            "password" => Some(Self::Password),
            "disk_full" => Some(Self::DiskFull),
            "unpack" => Some(Self::Unpack),
            "move" => Some(Self::Move),
            "health" => Some(Self::Health),
            "duplicate" => Some(Self::Duplicate),
            _ => None,
        }
    }

    // Classify a failed job from the stage it failed in, its typed terminal
    // cause and its error text. Each input is optional: a job restored after
    // a restart, or a row written before the kind was kept, has only text.
    pub fn derive(
        stage: Option<PipelineFailureStage>,
        cause: Option<SemanticTerminalCause>,
        error: Option<&str>,
        health: u32,
        failed_bytes: u64,
    ) -> Self {
        let text = error.unwrap_or_default().to_ascii_lowercase();
        let mentions = |needles: &[&str]| needles.iter().any(|needle| text.contains(needle));
        if mentions(&[
            "no space",
            "disk full",
            "not enough space",
            "storage full",
            "os error 28",
            "out of disk",
        ]) {
            return Self::DiskFull;
        }
        let cause = cause.unwrap_or_else(|| {
            classify_semantic_terminal_cause("failed", error, health, failed_bytes)
        });
        if cause == SemanticTerminalCause::PasswordFailure
            || mentions(&["password", "passphrase", "encrypted archive"])
        {
            return Self::Password;
        }
        match stage {
            Some(PipelineFailureStage::Move) => Self::Move,
            Some(PipelineFailureStage::Extract) => Self::Unpack,
            Some(PipelineFailureStage::Verify | PipelineFailureStage::Repair) => Self::Repair,
            Some(PipelineFailureStage::Download) | None => match cause {
                SemanticTerminalCause::RepairFailure => Self::Repair,
                SemanticTerminalCause::ArchiveOrUnpackFailure => Self::Unpack,
                SemanticTerminalCause::DiskOrPermissionFailure
                    if mentions(&["move error", "rename error", "failed to move", "rename"]) =>
                {
                    Self::Move
                }
                SemanticTerminalCause::DiskOrPermissionFailure if stage.is_none() => Self::Unpack,
                _ => Self::Health,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn derive(stage: Option<PipelineFailureStage>, error: &str) -> HistoryFailureKind {
        HistoryFailureKind::derive(stage, None, Some(error), 1_000, 0)
    }

    #[test]
    fn failure_kind_follows_the_failing_stage() {
        assert_eq!(
            derive(
                Some(PipelineFailureStage::Repair),
                "not enough recovery blocks"
            ),
            HistoryFailureKind::Repair
        );
        assert_eq!(
            derive(Some(PipelineFailureStage::Verify), "verification failed"),
            HistoryFailureKind::Repair
        );
        assert_eq!(
            derive(
                Some(PipelineFailureStage::Extract),
                "unexpected end of archive"
            ),
            HistoryFailureKind::Unpack
        );
        assert_eq!(
            derive(Some(PipelineFailureStage::Move), "permission denied"),
            HistoryFailureKind::Move
        );
        assert_eq!(
            derive(
                Some(PipelineFailureStage::Download),
                "aborted: 9 of 10 first articles are missing"
            ),
            HistoryFailureKind::Health
        );
    }

    #[test]
    fn password_and_full_disk_win_over_the_stage() {
        assert_eq!(
            derive(
                Some(PipelineFailureStage::Extract),
                "set 'a': invalid password for encrypted archive after 2 candidate(s)"
            ),
            HistoryFailureKind::Password
        );
        assert_eq!(
            HistoryFailureKind::derive(
                Some(PipelineFailureStage::Extract),
                Some(SemanticTerminalCause::PasswordFailure),
                Some("extraction failed"),
                1_000,
                0,
            ),
            HistoryFailureKind::Password
        );
        assert_eq!(
            derive(
                Some(PipelineFailureStage::Extract),
                "write failed: No space left on device (os error 28)"
            ),
            HistoryFailureKind::DiskFull
        );
        assert_eq!(
            derive(Some(PipelineFailureStage::Move), "No space left on device"),
            HistoryFailureKind::DiskFull
        );
    }

    #[test]
    fn failure_kind_without_a_stage_reads_the_cause() {
        assert_eq!(
            HistoryFailureKind::derive(
                None,
                Some(SemanticTerminalCause::MissingArticlesOrLowHealth),
                Some("payload file(s) were never delivered"),
                400,
                600,
            ),
            HistoryFailureKind::Health
        );
        assert_eq!(
            derive(None, "par2 repair failed"),
            HistoryFailureKind::Repair
        );
        assert_eq!(
            derive(None, "unrar reported a CRC error"),
            HistoryFailureKind::Unpack
        );
        assert_eq!(
            derive(None, "move error: target exists"),
            HistoryFailureKind::Move
        );
    }

    #[test]
    fn failure_kind_round_trips_through_storage() {
        for kind in [
            HistoryFailureKind::Repair,
            HistoryFailureKind::Password,
            HistoryFailureKind::DiskFull,
            HistoryFailureKind::Unpack,
            HistoryFailureKind::Move,
            HistoryFailureKind::Health,
            HistoryFailureKind::Duplicate,
        ] {
            assert_eq!(
                HistoryFailureKind::from_persisted(kind.as_str()),
                Some(kind)
            );
        }
        assert_eq!(HistoryFailureKind::from_persisted("other"), None);
    }
}
