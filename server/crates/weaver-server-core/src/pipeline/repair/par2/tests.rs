use super::{
    RecoveryCandidate, RecoveryCountSource, is_terminal_status, par2_recovery_packet_size,
    select_recovery_file_indices,
};
use crate::JobStatus;

#[test]
fn targeted_selection_prefers_minimum_bytes_then_file_count() {
    let selected = select_recovery_file_indices(
        &[
            RecoveryCandidate {
                file_index: 1,
                blocks: 1,
                total_bytes: 10,
                source: RecoveryCountSource::Exact,
            },
            RecoveryCandidate {
                file_index: 2,
                blocks: 2,
                total_bytes: 20,
                source: RecoveryCountSource::Exact,
            },
            RecoveryCandidate {
                file_index: 3,
                blocks: 4,
                total_bytes: 40,
                source: RecoveryCountSource::Exact,
            },
            RecoveryCandidate {
                file_index: 4,
                blocks: 8,
                total_bytes: 80,
                source: RecoveryCountSource::Exact,
            },
            RecoveryCandidate {
                file_index: 5,
                blocks: 16,
                total_bytes: 160,
                source: RecoveryCountSource::Exact,
            },
        ],
        20,
    );
    assert_eq!(selected, vec![3, 5]);
}

#[test]
fn targeted_selection_returns_empty_when_covered() {
    let selected = select_recovery_file_indices(&[], 0);
    assert!(selected.is_empty());
}

#[test]
fn recovery_packet_size_rounds_to_alignment() {
    assert_eq!(par2_recovery_packet_size(8), 76);
    assert_eq!(par2_recovery_packet_size(9), 80);
}

#[test]
fn terminal_status_detection_matches_history_contract() {
    assert!(is_terminal_status(&JobStatus::Complete));
    assert!(is_terminal_status(&JobStatus::Failed {
        error: "boom".to_string(),
    }));
    assert!(!is_terminal_status(&JobStatus::Downloading));
    assert!(!is_terminal_status(&JobStatus::Paused));
}
