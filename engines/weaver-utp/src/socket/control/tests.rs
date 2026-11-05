// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::BTreeMap;

use futures::FutureExt;

use super::*;

#[tokio::test]
async fn admission_permits_bound_all_pending_work_and_return_on_drop() {
    let admission = ConnectAdmission::default();
    let cancellation = CancellationToken::new();
    let mut held = Vec::new();
    for _ in 0..MAX_PENDING_CONNECTS {
        held.push(admission.acquire(&cancellation).await.unwrap());
    }
    assert_eq!(admission.permits.available_permits(), 0);
    let mut next = Box::pin(admission.acquire(&cancellation));
    assert!(next.as_mut().now_or_never().is_none());
    held.pop();
    held.push(next.await.unwrap());
    assert_eq!(admission.permits.available_permits(), 0);
    drop(held);
    assert_eq!(admission.permits.available_permits(), MAX_PENDING_CONNECTS);
}

#[tokio::test]
async fn canceled_admission_waits_leave_no_reserved_capacity() {
    let admission = ConnectAdmission::default();
    let cancellation = CancellationToken::new();
    let held = admission
        .permits
        .clone()
        .acquire_many_owned(MAX_PENDING_CONNECTS as u32)
        .await
        .unwrap();
    for _ in 0..128 {
        let mut next = Box::pin(admission.acquire(&cancellation));
        assert!(next.as_mut().now_or_never().is_none());
        drop(next);
    }
    let mut next = Box::pin(admission.acquire(&cancellation));
    assert!(next.as_mut().now_or_never().is_none());
    cancellation.cancel();
    assert!(matches!(next.await, Err(crate::Error::DispatcherDead)));
    drop(held);
    assert_eq!(admission.permits.available_permits(), MAX_PENDING_CONNECTS);
}

#[tokio::test]
async fn already_canceled_admission_cannot_take_an_available_permit() {
    let admission = ConnectAdmission::default();
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    assert!(matches!(
        admission.acquire(&cancellation).await,
        Err(crate::Error::DispatcherDead)
    ));
    assert_eq!(admission.permits.available_permits(), MAX_PENDING_CONNECTS);
}

#[tokio::test]
async fn retirement_after_an_armed_scan_wakes_the_next_cleanup_pass() {
    let signals = Arc::new(RetirementSignals::default());
    let (flag, guard) = signals.registration();
    let notified = signals.notifications().notified();
    tokio::pin!(notified);
    notified.as_mut().enable();
    assert!(!signals.take_dirty());
    assert!(!flag.is_retired());
    drop(guard);
    notified.await;
    assert!(signals.take_dirty());
    assert!(flag.is_retired());
    assert!(!signals.take_dirty());
}

#[tokio::test]
async fn retirements_coalesce_without_losing_their_individual_flags() {
    let signals = Arc::new(RetirementSignals::default());
    let mut flags = Vec::new();
    let mut guards = Vec::new();
    for _ in 0..128 {
        let (flag, guard) = signals.registration();
        flags.push(flag);
        guards.push(guard);
    }
    drop(guards);
    assert!(signals.take_dirty());
    assert!(flags.iter().all(|flag| flag.is_retired()));
    assert!(!signals.take_dirty());
    signals.notifications().notified().await;
    assert!(signals.notifications().notified().now_or_never().is_none());
}

#[test]
fn delayed_retirement_does_not_identify_a_replacement_by_its_reused_key() {
    let signals = Arc::new(RetirementSignals::default());
    let (old_flag, old_guard) = signals.registration();
    let (new_flag, _new_guard) = signals.registration();
    let mut current = BTreeMap::from([(7u16, Arc::clone(&old_flag))]);
    current.remove(&7);
    current.insert(7, Arc::clone(&new_flag));
    drop(old_guard);
    assert!(signals.take_dirty());
    current.retain(|_, flag| !flag.is_retired());
    assert!(Arc::ptr_eq(current.get(&7).unwrap(), &new_flag));
    assert!(old_flag.is_retired());
    assert!(!new_flag.is_retired());
}

#[test]
fn disarming_a_retirement_guard_preserves_the_current_owner() {
    let signals = Arc::new(RetirementSignals::default());
    let (flag, mut guard) = signals.registration();
    guard.disarm();
    drop(guard);
    assert!(!flag.is_retired());
    assert!(!signals.take_dirty());
}
