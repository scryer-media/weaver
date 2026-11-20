// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

pub fn compute_decode_backlog_budget_bytes(
    profile: &SystemProfile,
    base: usize,
    write: usize,
) -> usize {
    crate::pipeline::orchestrator::compute_decode_backlog_budget_bytes(profile, base, write)
}
pub fn compute_write_backlog_budget_bytes(profile: &SystemProfile, base: usize) -> usize {
    crate::pipeline::orchestrator::compute_write_backlog_budget_bytes(profile, base)
}
pub fn system_profile(pipeline: &Pipeline) -> &SystemProfile {
    pipeline.tuner.system_profile()
}
pub fn write_limit(pipeline: &Pipeline) -> usize {
    pipeline.write_backlog_budget_bytes
}
pub fn spool_limit(pipeline: &Pipeline) -> usize {
    pipeline.uu_spool_max_bytes
}
pub fn decode_limit(pipeline: &Pipeline) -> usize {
    pipeline.decode_backlog_budget_bytes
}
pub async fn run_queue_intents(pipeline: &mut Pipeline) {
    pipeline.run_queue_intents().await;
}
pub fn set_storage_classification(
    pipeline: &Pipeline,
    storage: StorageClass,
    filesystem: FilesystemType,
) {
    pipeline.shared_state.queue.write_intent(|inner| {
        inner.globals.storage_classification = Some((storage, filesystem));
        crate::pipeline::queue::Change::changed(())
    });
}
