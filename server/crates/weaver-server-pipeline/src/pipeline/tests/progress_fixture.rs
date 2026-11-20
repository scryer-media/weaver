// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
pub fn set_driving(pipeline: &Pipeline, driving: bool) {
    pipeline.shared_state.queue.set_driving(driving);
}
pub fn driving(pipeline: &Pipeline) -> bool {
    pipeline.shared_state.queue.driving()
}
pub fn push_finished_row(pipeline: &Pipeline, id: JobId) {
    pipeline.shared_state.queue.write(|inner| {
        inner.push_job(crate::pipeline::queue::JobRow::new(finished_job_info(id)));
        crate::pipeline::queue::Change::changed(())
    });
}
pub fn phase_begin(
    pipeline: &mut Pipeline,
    id: JobId,
    phase: JobPhase,
    total: Option<u64>,
) -> Arc<PhaseCounters> {
    pipeline.phase_begin(id, phase, total)
}
pub fn phase_end(pipeline: &mut Pipeline, id: JobId, phase: JobPhase) {
    pipeline.phase_end(id, phase);
}
pub fn clear_phases(pipeline: &mut Pipeline, id: JobId) {
    pipeline.clear_job_phase_progress_runtime(id);
}
pub fn sample_phases(pipeline: &Pipeline) {
    pipeline.shared_state.queue.sample_counters(
        &pipeline.metrics,
        &pipeline.event_tx,
        std::time::Instant::now(),
    );
}

pub fn downloaded_snapshot(pipeline: &Pipeline) -> u64 {
    pipeline.shared_state.metrics_snapshot().bytes_downloaded
}
