// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The extraction caps: one budget per job, held where the namespace
// writes. A breach fails the job with the refusal's rendering and reason
// word, at the extraction stage; a name the operator's policy refuses fails
// it with the policy's sentence. The depth limit is no breach: the tree
// never builds an object past it, the member is delivered as the container
// it is, and the job goes on. The deadline has no row: it counts the time
// decoders execute, which a row here cannot hold still, so it is pinned
// beside the clock and the namespace instead.

pub use super::actor_drive::limits;
use crate::history::HistoryFailureKind;

pub const FLOOR: u64 = 256 * 1024;

pub const SETTINGS_KEY: &str = crate::post_processing::settings::SETTINGS_KEY;

pub fn refresh_admission_hint(db: &crate::Database) -> Result<bool, crate::StateError> {
    crate::post_processing::events::refresh_admission_hint(db)
}

pub fn before_first_move(
    pipeline: &crate::pipeline::Pipeline,
    hook: impl FnOnce() + Send + 'static,
) {
    crate::pipeline::completion::finalize::FIXTURE_MOVE_HOOKS
        .lock()
        .unwrap()
        .insert(pipeline.complete_dir.clone(), Box::new(move |_| hook()));
}

pub fn assert_refused(outcome: &super::harness::Outcome, reason: &str) {
    let marker = format!("WEAVER_EXTRACTION_REJECTED[{reason}]: ");
    assert!(
        outcome
            .failure
            .as_deref()
            .is_some_and(|failure| failure.starts_with(&marker)),
        "{:?}",
        outcome.status
    );
    assert_eq!(outcome.failure_kind, Some(HistoryFailureKind::Unpack));
    assert!(outcome.files.is_empty(), "{:?}", outcome.files.keys());
}

// A zstd frame of `blocks` RLE blocks of 128 KiB of zeros each: a few bytes
// posted for every 128 KiB it inflates to.
pub fn zeros_zstd(blocks: usize) -> Vec<u8> {
    const BLOCK: u32 = 128 * 1024;
    let mut frame = vec![0x28, 0xb5, 0x2f, 0xfd, 0x00, 0x38];
    for block in 0..blocks {
        let last = u32::from(block + 1 == blocks);
        let header = last | (1 << 1) | (BLOCK << 3);
        frame.extend_from_slice(&header.to_le_bytes()[..3]);
        frame.push(0);
    }
    frame
}
