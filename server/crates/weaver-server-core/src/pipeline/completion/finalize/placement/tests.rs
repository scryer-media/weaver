use super::*;

fn cycle_fixture() -> (tempfile::TempDir, par2_rs::PlacementPlan) {
    let dir = tempfile::tempdir().unwrap();
    let renames = [("a", "b"), ("b", "c"), ("c", "a")]
        .into_iter()
        .enumerate()
        .map(|(i, (source, destination))| {
            std::fs::write(dir.path().join(source), [i as u8; 32]).unwrap();
            par2_rs::PlacementEntry {
                file_id: par2_rs::FileId::from_bytes([i as u8; 16]),
                current_name: source.into(),
                correct_name: destination.into(),
            }
        })
        .collect();
    (
        dir,
        par2_rs::PlacementPlan {
            exact: vec![],
            swaps: vec![],
            renames,
            unresolved: vec![],
            conflicts: vec![],
        },
    )
}

#[test]
fn placement_io_failure_rolls_back_staging_and_partial_installation() {
    for fail_at in 0..6 {
        let (dir, plan) = cycle_fixture();
        let mut calls = 0;
        let error = apply_complete_plan_with_move(dir.path(), &plan, |source, destination| {
            let fail = calls == fail_at;
            calls += 1;
            if fail {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "injected move failure",
                ));
            }
            rename_no_overwrite(source, destination)
        })
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        for (i, name) in ["a", "b", "c"].iter().enumerate() {
            assert_eq!(
                std::fs::read(dir.path().join(name)).unwrap(),
                [i as u8; 32],
                "failure at move {fail_at} must restore {name}"
            );
        }
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 3);
    }
}

#[test]
fn placement_failed_rollback_retains_every_payload_and_reports_its_location() {
    let (dir, plan) = cycle_fixture();
    let mut calls = 0;
    let error = apply_complete_plan_with_move(dir.path(), &plan, |source, destination| {
        calls += 1;
        if calls >= 5 {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "persistent move failure",
            ));
        }
        rename_no_overwrite(source, destination)
    })
    .unwrap_err();
    let staging = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.is_dir())
        .unwrap();
    assert!(error.to_string().contains(staging.to_str().unwrap()));
    assert_eq!(std::fs::read(dir.path().join("b")).unwrap(), [0; 32]);
    assert_eq!(std::fs::read(staging.join("b")).unwrap(), [1; 32]);
    assert_eq!(std::fs::read(staging.join("c")).unwrap(), [2; 32]);
}

#[test]
fn placement_partial_move_error_reports_retained_staging() {
    let (dir, plan) = cycle_fixture();
    let error = apply_complete_plan_with_move(dir.path(), &plan, |source, destination| {
        std::fs::hard_link(source, destination)?;
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "source removal failed",
        ))
    })
    .unwrap_err();
    let staging = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.is_dir())
        .unwrap();
    assert!(error.to_string().contains(staging.to_str().unwrap()));
    for (i, name) in ["a", "b", "c"].iter().enumerate() {
        assert_eq!(std::fs::read(dir.path().join(name)).unwrap(), [i as u8; 32]);
    }
    assert_eq!(std::fs::read(staging.join("a")).unwrap(), [0; 32]);
}
