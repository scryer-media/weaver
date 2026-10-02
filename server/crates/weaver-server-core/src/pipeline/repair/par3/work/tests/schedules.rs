//! Worker execution and actor handback are distinct events. Holding WorkDone
//! gives a deterministic gate after native work without a clock or a sleep.
use super::*;

async fn settle_all(coordinator: &mut Coordinator, job: JobId) {
    for _ in 0..32 {
        coordinator.dispatch().unwrap();
        if !coordinator.has_worker_in_flight(job) {
            return;
        }
        let done = next(coordinator).await;
        assert_eq!(done.job_id, job);
        coordinator.settle(done);
    }
    panic!("PAR3 exceeded the schedule's work-unit budget");
}

#[tokio::test]
async fn recovery_release_and_handback_schedules_preserve_available_and_missing_indices() {
    for release_first in [false, true] {
        for repeat_release in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let mut coordinator = Coordinator::default();
            let job = JobId(41996);
            coordinator
                .enqueue(job, SourceId(90), carrier(root.path()))
                .unwrap();
            settle_all(&mut coordinator, job).await;
            let (set, matrix) = {
                let (set, view) = coordinator.assessments(job).next().unwrap();
                (set, view.requirements[0].matrix)
            };
            coordinator
                .begin_recovery_batch(job, vec![], false)
                .unwrap();
            let slot = coordinator.jobs.get_mut(&job).unwrap();
            slot.runtime
                .as_mut()
                .unwrap()
                .sets
                .get_mut(&set)
                .unwrap()
                .native
                .note_recovery_in_flight(matrix, &[0, 1])
                .unwrap();
            slot.acquisition.batch.as_mut().unwrap().declared = vec![(set, matrix, vec![0, 1])];
            let recovery = root.path().join("set.vol0+1.par3");
            std::fs::write(
                &recovery,
                include_bytes!("../../../backend/fixtures/set.vol0+1.par3"),
            )
            .unwrap();
            coordinator.enqueue(job, SourceId(91), recovery).unwrap();
            coordinator.dispatch().unwrap();
            let done = next(&mut coordinator).await;
            assert!(done.result.is_ok());
            if release_first {
                coordinator.forget_recovery_in_flight(job);
            }
            coordinator.settle(done);
            if !release_first {
                coordinator.forget_recovery_in_flight(job);
            }
            if repeat_release {
                coordinator.forget_recovery_in_flight(job);
            }
            settle_all(&mut coordinator, job).await;
            let (_, view) = coordinator.assessments(job).next().unwrap();
            let requirement = &view.requirements[0];
            assert_eq!(
                requirement.available,
                vec![0],
                "received parity remains available"
            );
            assert_eq!(requirement.in_flight, 0, "every declaration was released");
            assert!(
                requirement.next_indices.contains(&1),
                "missing recovery becomes askable again: {requirement:?}"
            );
            assert!(
                !requirement.next_indices.contains(&0),
                "received parity is never requested again"
            );
            assert!(
                coordinator.jobs[&job]
                    .acquisition
                    .deferred_release
                    .is_empty()
            );
            assert!(coordinator.worker_allowances.is_empty());
        }
    }
}

#[tokio::test]
async fn source_invalidation_around_handback_never_exposes_the_old_assessment() {
    for invalidate_first in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let path = carrier(root.path());
        let job = JobId(41997);
        let source = SourceId(0);
        let mut coordinator = Coordinator::default();
        coordinator.enqueue(job, source, path.clone()).unwrap();
        settle_all(&mut coordinator, job).await;
        coordinator.queue_reassessment(job).unwrap();
        coordinator.dispatch().unwrap();
        let done = next(&mut coordinator).await;
        assert!(done.result.is_ok());
        if invalidate_first {
            coordinator.invalidate_source(job, source).unwrap();
        }
        coordinator.settle(done);
        if !invalidate_first {
            coordinator.invalidate_source(job, source).unwrap();
        }
        assert_eq!(
            coordinator.assessments(job).count(),
            0,
            "dirty generations cannot authorize completion"
        );
        assert!(coordinator.jobs[&job].dirty.contains(&source));
        coordinator.enqueue(job, source, path).unwrap();
        settle_all(&mut coordinator, job).await;
        assert!(!coordinator.jobs[&job].dirty.contains(&source));
        assert_eq!(coordinator.assessments(job).count(), 1);
        assert!(coordinator.worker_allowances.is_empty());
    }
}

#[tokio::test]
async fn cancellation_before_handback_cannot_resurrect_a_reused_job_id() {
    let root = tempfile::tempdir().unwrap();
    let mut coordinator = Coordinator::default();
    let job = JobId(41998);
    coordinator
        .enqueue(job, SourceId(0), carrier(root.path()))
        .unwrap();
    coordinator.dispatch().unwrap();
    let done = next(&mut coordinator).await;
    coordinator.forget(job);
    coordinator.admit(job).unwrap();
    assert_eq!(coordinator.settle(done), None);
    assert_eq!(coordinator.assessments(job).count(), 0);
    assert!(!coordinator.has_work(job));
    assert!(coordinator.worker_allowances.is_empty());
}
