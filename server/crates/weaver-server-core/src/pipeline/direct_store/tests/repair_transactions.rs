use super::super::set::DirectSet;
use super::*;

fn transaction_set() -> DirectSet {
    let plan = DirectSetPlan {
        set_name: SET.into(),
        volumes: [(0, 0), (1, 1)].into(),
        files: [(0, 0), (1, 1)].into(),
        identity: None,
        working_dir: "/nonexistent".into(),
        destination_dir: "/nonexistent-staging".into(),
    };
    let mut first = member_facts(REARM_MEMBER, 64, 400, 800);
    first.split_after = true;
    first.packed_crc32 = Some(par2_rs::checksum::crc32(&[42; 400]));
    let mut last = member_facts(REARM_MEMBER, 64, 400, 800);
    last.split_before = true;
    last.data_crc32 = Some(par2_rs::checksum::crc32(&[42; 800]));
    let mut set = DirectSet::new(JOB, plan);
    set.router
        .restore_layout(
            &[
                (0, volume_facts(0, true, vec![first])),
                (1, volume_facts(1, false, vec![last])),
            ]
            .into(),
        )
        .unwrap();
    set
}

fn checkpoint(set: &mut DirectSet, recorder: &Recorder) {
    assert!(
        set.run_barrier(
            BarrierTrigger::Demand(BarrierDemand::RepairRecreate),
            Instant::now(),
            &mut recorder.clone(),
            &mut recorder.clone(),
            &mut recorder.clone(),
        )
        .is_none()
    );
    assert!(recorder.ops().is_empty());
}

#[test]
fn multi_volume_replacement_defers_checks_and_checkpoints_until_all_placement() {
    transaction_checks(false);
}

#[test]
fn failed_multi_volume_integrity_keeps_the_checkpoint_fence() {
    transaction_checks(true);
}

fn transaction_checks(corrupt: bool) {
    let mut set = transaction_set();
    let recorder = Recorder::default();
    set.begin_repair_transaction(vec![0, 1]).unwrap();
    for volume in 0..2 {
        let mut data = vec![42; 400];
        if corrupt && volume == 0 {
            data[0] ^= 1;
        }
        let spans = set
            .router
            .route_repaired_batch(volume, &[(64, Arc::from(data))], &[], false, true)
            .expect("even a bad first volume defers its gate until the set is replaced");
        set.record_writes(&spans, Instant::now());
        assert!(set.router.repair_batch_in_progress());
        assert!(!set.router.all_members_verified());
        checkpoint(&mut set, &recorder);
    }
    let result = set.finish_repair_transaction();
    if corrupt {
        assert!(result.is_err());
        assert!(set.router.repair_batch_in_progress());
        checkpoint(&mut set, &recorder);
    } else {
        result.unwrap();
        assert!(!set.router.repair_batch_in_progress());
        assert!(set.router.all_members_verified());
        set.run_barrier(
            BarrierTrigger::Demand(BarrierDemand::RepairRecreate),
            Instant::now(),
            &mut recorder.clone(),
            &mut recorder.clone(),
            &mut recorder.clone(),
        )
        .unwrap()
        .unwrap();
        assert!(recorder.committed().is_some());
    }
}

#[test]
fn replacement_transactions_reject_invalid_plans_and_wrong_volume_order() {
    for volumes in [vec![], vec![0, 0], vec![1, 0], vec![2], vec![0, 1, 2]] {
        assert!(transaction_set().begin_repair_transaction(volumes).is_err());
    }
    let mut set = transaction_set();
    set.begin_repair_transaction(vec![0, 1]).unwrap();
    assert!(
        set.router
            .route_repaired_batch(1, &[(64, Arc::from([42; 400]))], &[], false, true)
            .is_err()
    );
    assert!(set.router.repair_batch_in_progress());
}

#[test]
fn incomplete_replacement_transactions_cannot_be_closed_or_reentered() {
    let mut set = transaction_set();
    set.begin_repair_transaction(vec![0, 1]).unwrap();
    assert!(set.finish_repair_transaction().is_err());
    assert!(set.router.repair_batch_in_progress());
    let mut set = transaction_set();
    set.begin_repair_transaction(vec![0, 1]).unwrap();
    assert!(set.begin_repair_transaction(vec![0, 1]).is_err());
    assert!(set.router.repair_batch_in_progress());
    let mut set = transaction_set();
    set.begin_repair_transaction(vec![0]).unwrap();
    assert!(
        set.router
            .route_repaired_batch(0, &[], &[], false, true)
            .is_err()
    );
    assert!(set.router.repair_batch_in_progress());
}

#[test]
fn par3_archive_checksum_deferral_requires_a_final_archive_verdict() {
    let (mut router, _) = straddle_router(&[42; 400], 64);
    router.note_par3_available(true);
    router.stage_for_test(0, 64, &[41; 400]);
    router
        .drain_for_test(0)
        .expect("PAR3 can still repair the member");
    assert!(!router.all_members_verified());
    assert!(router.damaged_volumes().contains(&0));
    assert_eq!(
        router.settle_par3_verification(),
        Err(DemotionReason::MemberChecksumMismatch)
    );
    assert!(!router.awaits_par3_verdict());
}

#[test]
fn complete_direct_set_waits_for_native_verdict_application() {
    let mut set = transaction_set();
    set.router.note_par3_available(true);
    set.note_repair_attempted();
    set.begin_repair_transaction(vec![0, 1]).unwrap();
    for volume in 0..2 {
        let spans = set
            .router
            .route_repaired_batch(volume, &[(64, Arc::from([42; 400]))], &[], false, true)
            .unwrap();
        set.record_writes(&spans, Instant::now());
        set.note_volume_complete(volume, 464).unwrap();
    }
    set.finish_repair_transaction().unwrap();
    assert!(set.all_volumes_complete());
    for volume in 0..2 {
        assert_eq!(
            set.virtual_volume_len(volume, 600),
            600,
            "the repair latch must not change PAR2 volume length policy"
        );
    }
    assert!(set.router.all_members_verified());
    assert!(!set.ready_to_finalize());
    set.router.settle_par3_verification().unwrap();
    assert!(set.ready_to_finalize());
}

#[test]
fn par3_archive_checksum_deferral_accepts_verified_replacement_bytes() {
    let (mut router, _) = straddle_router(&[42; 400], 64);
    router.note_par3_available(true);
    router.stage_for_test(0, 64, &[41; 400]);
    router.drain_for_test(0).unwrap();
    router
        .route_repaired(0, &[(64, Arc::from([42; 400]))], &[], false)
        .unwrap();
    router.settle_par3_verification().unwrap();
    assert!(router.all_members_verified());
}

#[test]
fn par2_whole_member_checksum_policy_does_not_gain_par3_deferral() {
    let (mut router, _) = straddle_router(&[42; 400], 64);
    router.note_par2_available(true);
    router.stage_for_test(0, 64, &[41; 400]);
    assert_eq!(
        router.drain_for_test(0).unwrap_err(),
        DemotionReason::MemberChecksumMismatch
    );
}

#[test]
fn native_verification_cannot_close_an_active_replacement_transaction() {
    let mut set = transaction_set();
    set.router.note_par3_available(true);
    set.begin_repair_transaction(vec![0, 1]).unwrap();
    assert!(set.router.settle_par3_verification().is_err());
    assert!(set.router.repair_batch_in_progress());
}
