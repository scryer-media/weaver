// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::jobs::model::TerminalDiscard;

impl Pipeline {
    // The failed bytes and health the terminal record should carry, and the
    // discards that explain them.
    //
    // Falls back to the live ledger for a job that never reached the census —
    // a failed job, where the raw counters are the explanation and must be
    // preserved exactly as the download layer left them.
    pub(in crate::pipeline) fn terminal_record_figures(
        &self,
        job_id: JobId,
        total_bytes: u64,
    ) -> (u64, u32, Vec<TerminalDiscard>) {
        if let Some(reconciliation) = self.terminal_reconciliations.get(&job_id) {
            return (
                reconciliation.failed_bytes,
                reconciliation.health,
                reconciliation.discards.clone(),
            );
        }
        let failed_bytes = self.settled_failed_bytes(job_id, total_bytes);
        (
            failed_bytes,
            health_milli(total_bytes, failed_bytes),
            Vec::new(),
        )
    }
}
