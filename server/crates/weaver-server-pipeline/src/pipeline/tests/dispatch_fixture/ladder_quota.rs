// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

// A quota of `limit_bytes` on the control of server `idx`.
pub fn metered(idx: u32, limit_bytes: u64) -> Arc<weaver_nntp::transfer::ServerTransferControl> {
    weaver_nntp::transfer::ServerTransferRegistry::new().configure(
        weaver_nntp::transfer::StableServerId(idx),
        quota_config(limit_bytes),
    )
}

pub fn quota_config(limit_bytes: u64) -> weaver_nntp::transfer::ServerTransferConfig {
    weaver_nntp::transfer::ServerTransferConfig {
        rate_bytes_per_sec: 0,
        quota: Some(weaver_nntp::transfer::QuotaRuntimeConfig {
            limit_bytes,
            generation: 1,
            retry_at: None,
        }),
        closed: false,
    }
}

// Charge `bytes` downloaded to `control`'s quota.
pub fn charge(control: &Arc<weaver_nntp::transfer::ServerTransferControl>, bytes: usize) {
    let mut permit = control.try_reserve(bytes as u64).unwrap();
    permit.record_unpaced(bytes);
    let _ = permit.finish();
}

impl DispatchFixture {
    pub fn meter_ladder_server(
        &mut self,
        levels: &[(u32, bool)],
        server: usize,
        control: Arc<weaver_nntp::transfer::ServerTransferControl>,
    ) {
        self.0.nntp = Arc::new(super::ladder::ladder_client_with_quota(
            levels,
            Some((server, control)),
        ));
    }
    pub fn checkpoint_admits_all(&self, job: JobId) -> bool {
        self.0.checkpoint_admission(job).admits_every_article()
    }
    pub fn set_scan_floors_off(off: bool) {
        crate::pipeline::download::scheduler::set_scan_floors_off(off);
    }
    pub fn take_servable_works(
        &mut self,
        job: JobId,
        server: usize,
        want: usize,
    ) -> Option<Vec<DownloadWork>> {
        let pressure = self.0.refresh_download_pressure();
        self.0
            .take_servable_works_or_none(job, server, want, pressure)
    }
    pub async fn answer_connection_failed(&mut self, server: usize, work: DownloadWork) {
        super::ladder::answer_connection_failed(&mut self.0, server, work).await;
    }
    pub fn wake_all_retries(&mut self) -> usize {
        self.0.wake_all_infrastructure_retries()
    }
}
