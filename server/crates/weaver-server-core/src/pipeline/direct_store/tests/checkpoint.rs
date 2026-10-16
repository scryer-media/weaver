use super::*;

#[test]
fn retained_layout_refuses_unknown_volumes_and_unsafe_member_paths() {
    let plan = rearm_router().plan().clone();
    let mut router = DirectSetRouter::new(plan.clone());
    let unknown = std::collections::BTreeMap::from([(
        9,
        volume_facts(9, false, vec![member_facts("leaf.bin", 64, 8, 8)]),
    )]);
    assert!(router.restore_retained_layout(&unknown).is_err());
    let mut router = DirectSetRouter::new(plan);
    let unsafe_path = std::collections::BTreeMap::from([(
        0,
        volume_facts(0, false, vec![member_facts("../escape.bin", 64, 8, 8)]),
    )]);
    assert!(router.restore_retained_layout(&unsafe_path).is_err());
}

#[test]
fn historical_member_backing_requires_exact_intrinsic_coordinates() {
    let mut router = rearm_router();
    let relative = router.member_partials()[0].2.to_owned();
    router
        .restore_backed_extent(0, 64, &relative, 0, REARM_PART)
        .unwrap();
    assert!(
        router
            .restore_backed_extent(0, 65, &relative, 0, REARM_PART - 1)
            .is_err()
    );
    assert!(
        router
            .restore_backed_extent(1, 64, &relative, 0, REARM_PART)
            .is_err()
    );
    assert!(
        router
            .restore_backed_extent(2, 64, &relative, 0, 1)
            .is_err()
    );
    assert!(
        router
            .restore_backed_extent(0, 64, "unowned.partial", 0, 1)
            .is_err()
    );
    assert!(
        router
            .restore_backed_extent(0, u64::MAX, &relative, 0, 2)
            .is_err()
    );
    assert!(
        router
            .restore_backed_extent(0, 64, &relative, u64::MAX, 2)
            .is_err()
    );
}
