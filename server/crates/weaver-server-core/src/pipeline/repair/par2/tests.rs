use super::{
    RecoveryCandidate, RecoveryCountSource, is_terminal_status, par2_prefix_set_ids,
    par2_recovery_packet_size, par2_set_base_name, parse_stateful_par2_session_enabled,
    select_par2_session_eviction, select_recovery_file_indices, unique_par2_binding_candidate,
};
use crate::{JobId, JobStatus};
use std::time::{Duration, Instant};

#[test]
fn par2_file_binding_requires_one_matching_description() {
    let first = par2_rs::FileId::from_bytes([1; 16]);
    let second = par2_rs::FileId::from_bytes([2; 16]);

    assert_eq!(unique_par2_binding_candidate(&[]), None);
    assert_eq!(unique_par2_binding_candidate(&[first]), Some(first));
    assert_eq!(unique_par2_binding_candidate(&[first, second]), None);
}

#[test]
fn metadata_prefix_scanning_reports_every_valid_set_id() {
    let first = crate::pipeline::tests::build_test_par2_index("first.bin", b"first", 4);
    let second = crate::pipeline::tests::build_test_par2_index("second.bin", b"second", 4);
    let first_ids = par2_prefix_set_ids(&first);
    let second_ids = par2_prefix_set_ids(&second);
    assert_eq!(first_ids.len(), 1);
    assert_eq!(second_ids.len(), 1);
    assert_ne!(first_ids, second_ids);

    let mut mixed = first;
    mixed.extend_from_slice(&second);
    let mut expected = [first_ids, second_ids].concat();
    expected.sort_by_key(|set_id| *set_id.as_bytes());
    assert_eq!(par2_prefix_set_ids(&mixed), expected);
    assert!(par2_prefix_set_ids(b"PAR2\0PKT invalid").is_empty());
}

#[test]
fn metadata_prefix_scanning_waits_for_the_rest_of_a_split_packet() {
    let index = crate::pipeline::tests::build_test_par2_index("split.bin", b"payload", 4);
    let split = par2_rs::packet::header::HEADER_SIZE;

    assert!(
        par2_prefix_set_ids(&index[..split]).is_empty(),
        "the packet header alone is not authenticated metadata"
    );
    assert_eq!(par2_prefix_set_ids(&index).len(), 1);
}

#[test]
fn retained_session_recovery_merge_does_not_rescan_sources() {
    let temp = tempfile::tempdir().unwrap();
    let payload_path = temp.path().join("payload.bin");
    let index_path = temp.path().join("repair.par2");
    let recovery_path = temp.path().join("repair.vol00+01.par2");
    let payload = b"verified payload";
    std::fs::write(&payload_path, payload).unwrap();
    std::fs::write(
        &index_path,
        crate::pipeline::tests::build_test_par2_index("payload.bin", payload, 8),
    )
    .unwrap();

    let mut session = par2_rs::Par2RepairSession::open(par2_rs::Par2RepairSessionOptions::new(
        temp.path().to_path_buf(),
        vec![index_path.clone()],
    ))
    .unwrap();
    assert_eq!(
        session.analyze().unwrap().status,
        par2_rs::Par2RepairStatus::Verified
    );

    // A volume path may contain only non-recovery packets; merging it still
    // invalidates the cached assessment but must retain verified sources.
    std::fs::copy(&index_path, &recovery_path).unwrap();
    session.merge_recovery_paths([&recovery_path]).unwrap();
    std::fs::remove_file(&payload_path).unwrap();
    assert_eq!(
        session.analyze().unwrap().status,
        par2_rs::Par2RepairStatus::Verified
    );
}

#[test]
fn retained_session_invalidation_drops_cached_assessment() {
    let temp = tempfile::tempdir().unwrap();
    let payload_path = temp.path().join("payload.bin");
    let index_path = temp.path().join("repair.par2");
    let payload = b"verified payload";
    std::fs::write(&payload_path, payload).unwrap();
    std::fs::write(
        &index_path,
        crate::pipeline::tests::build_test_par2_index("payload.bin", payload, 8),
    )
    .unwrap();

    let mut session = par2_rs::Par2RepairSession::open(par2_rs::Par2RepairSessionOptions::new(
        temp.path().to_path_buf(),
        vec![index_path],
    ))
    .unwrap();
    session.analyze().unwrap();
    session.invalidate_path(&payload_path);
    assert!(matches!(
        session.assessment(),
        Err(par2_rs::Par2SessionError::InvalidState { .. })
    ));
}

#[test]
fn stateful_par2_session_parser_defaults_to_enabled() {
    assert!(parse_stateful_par2_session_enabled(None));
    assert!(parse_stateful_par2_session_enabled(Some("true")));
    assert!(!parse_stateful_par2_session_enabled(Some("false")));
    assert!(!parse_stateful_par2_session_enabled(Some("0")));
    assert!(!parse_stateful_par2_session_enabled(Some("off")));
}

#[test]
fn retained_session_budget_evicts_other_job_before_protected_job() {
    let set_id = par2_rs::RecoverySetId::from_bytes([2; 16]);
    let other = (JobId(1), par2_rs::RecoverySetId::from_bytes([1; 16]));
    let protected = (JobId(2), set_id);
    assert_eq!(
        select_par2_session_eviction([(other, true, None), (protected, true, None)], protected,),
        Some(other)
    );
    assert_eq!(
        select_par2_session_eviction([(protected, true, None)], protected),
        Some(protected)
    );
}

#[test]
fn retained_session_budget_evicts_least_recently_used_session() {
    let now = Instant::now();
    let set_id = par2_rs::RecoverySetId::from_bytes([3; 16]);
    let first = (JobId(1), par2_rs::RecoverySetId::from_bytes([1; 16]));
    let second = (JobId(2), par2_rs::RecoverySetId::from_bytes([2; 16]));
    let protected = (JobId(3), set_id);
    assert_eq!(
        select_par2_session_eviction(
            [
                (first, true, Some(now - Duration::from_secs(1))),
                (second, true, Some(now - Duration::from_secs(2))),
                (protected, true, Some(now - Duration::from_secs(3))),
            ],
            protected,
        ),
        Some(second)
    );
}

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
fn targeted_selection_uses_standard_par2_volume_capacities() {
    // These are the exact counts declared by `.volNNN+CCC.par2` names.  A
    // missing 320-slice source must choose 335 packets in the first wave,
    // rather than an encoded-size estimate that can leave it short.
    let selected = select_recovery_file_indices(
        &[
            RecoveryCandidate {
                file_index: 8,
                blocks: 16,
                total_bytes: 16,
                source: RecoveryCountSource::FilenameFallback,
            },
            RecoveryCandidate {
                file_index: 9,
                blocks: 32,
                total_bytes: 32,
                source: RecoveryCountSource::FilenameFallback,
            },
            RecoveryCandidate {
                file_index: 10,
                blocks: 64,
                total_bytes: 64,
                source: RecoveryCountSource::FilenameFallback,
            },
            RecoveryCandidate {
                file_index: 11,
                blocks: 128,
                total_bytes: 128,
                source: RecoveryCountSource::FilenameFallback,
            },
            RecoveryCandidate {
                file_index: 12,
                blocks: 127,
                total_bytes: 127,
                source: RecoveryCountSource::FilenameFallback,
            },
        ],
        320,
    );

    assert_eq!(selected, vec![8, 10, 12, 11]);
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

#[test]
fn par2_collection_base_name_groups_an_index_with_its_volumes() {
    assert_eq!(
        par2_set_base_name("silver.horizon.par2").as_deref(),
        Some("silver.horizon")
    );
    assert_eq!(
        par2_set_base_name("silver.horizon.vol00+08.par2").as_deref(),
        Some("silver.horizon")
    );
    // The convention is not consistently cased on the wire, and the block
    // separator is written both ways.
    assert_eq!(
        par2_set_base_name("Silver.Horizon.VOL000-008.PAR2").as_deref(),
        Some("silver.horizon")
    );
    // A `.vol` part without a block count is part of the name, not a volume
    // marker, and a stray `.par2` inside the name does not start a new one.
    assert_eq!(
        par2_set_base_name("silver.volume.par2").as_deref(),
        Some("silver.volume")
    );
    assert_eq!(
        par2_set_base_name("silver.par2.vol01+02.par2").as_deref(),
        Some("silver.par2")
    );
}

#[test]
fn par2_collection_base_name_declines_names_outside_the_convention() {
    assert!(par2_set_base_name("silver.horizon.mkv").is_none());
    assert!(par2_set_base_name(".par2").is_none());
    assert!(par2_set_base_name("").is_none());
}
