// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

#[test]
fn formats_settle_independently_and_missing_indexes_release_the_hold() {
    for (ready, pending) in [
        (RecoveryFormat::Par2, RecoveryFormat::Par3),
        (RecoveryFormat::Par3, RecoveryFormat::Par2),
    ] {
        let mut metadata = RecoveryMetadata::default();
        metadata.observe(MemberId(0), ready, MetadataState::Ready);
        metadata.observe(MemberId(1), pending, MetadataState::Unresolved);
        assert_eq!(metadata.bootstrap_files(), Some(vec![MemberId(1)]));
        let mut settled = RecoveryMetadata::default();
        settled.observe(MemberId(0), ready, MetadataState::Ready);
        settled.observe(MemberId(1), pending, MetadataState::Unavailable);
        assert_eq!(settled.bootstrap_files(), None);
    }
}

#[test]
fn indexless_discovery_does_not_hold_payload_for_recovery_volumes() {
    assert_eq!(
        RecoveryFormat::declared_index(&FileRole::Par3 { is_index: false }),
        None
    );
    assert_eq!(
        RecoveryFormat::declared_index(&FileRole::Par2 {
            is_index: false,
            recovery_block_count: 8
        }),
        None
    );
    assert_eq!(RecoveryMetadata::default().bootstrap_files(), None);
}
