use super::*;

impl Pipeline {
    pub(crate) fn maybe_service_deferred_lane_refills(&mut self) {
        if self.deferred_lane_refills.is_empty()
            || self.refresh_download_pressure().state == DownloadPressureState::Hard
        {
            return;
        }
        let mut pending = std::mem::take(&mut self.deferred_lane_refills);
        while let Some(request) = pending.pop_front() {
            self.handle_download_lane_refill_request(request);
            if !self.deferred_lane_refills.is_empty() {
                break;
            }
        }
        self.deferred_lane_refills.append(&mut pending);
    }

    pub(crate) fn handle_download_lane_refill_request(
        &mut self,
        request: DownloadLaneRefillRequest,
    ) {
        let DownloadLaneRefillRequest {
            job_id,
            runtime_generation,
            server_idx,
            remote_ip,
            supports_pipelining,
            current_mode,
            compatibility,
            response_tx,
        } = request;
        let batch_class = DownloadBatchClass::from(&compatibility);
        if runtime_generation != self.pool_generation {
            let _ = response_tx.send(DownloadLaneRefillResponse {
                lease: None,
                park_reason: LaneParkReason::Error,
            });
            return;
        }

        let now = Instant::now();
        let mut allow_refill = !self.global_paused && !self.rate_limiter.should_wait();
        let mut park_reason = LaneParkReason::NoWork;
        let lane_ip_key = ServerIpKey {
            server_idx,
            ip: remote_ip,
        };
        if self.ip_replacement_retired_ips.contains(&lane_ip_key) {
            allow_refill = false;
            park_reason = LaneParkReason::IpReplacementRetired;
        }
        if !allow_refill && park_reason == LaneParkReason::NoWork {
            park_reason = LaneParkReason::ProbeYield;
        }
        if allow_refill && let Err(error) = self.refresh_bandwidth_cap_window() {
            error!(error = %error, "failed to refresh ISP bandwidth cap state for lane refill");
            allow_refill = false;
            park_reason = LaneParkReason::Error;
        }
        if allow_refill
            && self.bandwidth_cap.cap_enabled()
            && self.bandwidth_cap.remaining_bytes() == 0
        {
            allow_refill = false;
            park_reason = LaneParkReason::Pressure;
        }

        let pressure = self.refresh_download_pressure();
        if allow_refill && pressure.state == DownloadPressureState::Hard {
            self.deferred_lane_refills
                .push_back(DownloadLaneRefillRequest {
                    job_id,
                    runtime_generation,
                    server_idx,
                    remote_ip,
                    supports_pipelining,
                    current_mode,
                    compatibility,
                    response_tx,
                });
            self.metrics
                .download_lane_refill_deferred_total
                .fetch_add(1, Ordering::Relaxed);
            return;
        }

        if allow_refill {
            self.apply_rar_unlock_priorities_if_dirty(job_id);
            let mut eligible = self
                .job_order
                .iter()
                .enumerate()
                .filter_map(|(index, id)| {
                    let state = self.jobs.get(id)?;
                    (!state.download_queue.is_empty()
                        && Self::status_allows_download_dispatch(&state.status))
                    .then_some((Self::job_dispatch_priority(state), index, *id))
                })
                .collect::<Vec<_>>();
            eligible.sort_unstable();

            if self.select_hot_dispatch_job(&eligible) != Some(job_id) {
                allow_refill = false;
                park_reason = LaneParkReason::HotReclaim;
            }
        }

        let lease = if allow_refill {
            match self.try_lease_refill_download_batch(job_id, compatibility, pressure) {
                Ok(lease) => lease,
                Err(DispatchAttempt::StopAll) => {
                    park_reason = LaneParkReason::Pressure;
                    None
                }
                Err(DispatchAttempt::NoWork) => None,
                Err(DispatchAttempt::Dispatched) => unreachable!("refill lease never dispatches"),
            }
        } else {
            None
        };

        let Some(lease) = lease else {
            self.metrics
                .download_lane_refill_parked_total
                .fetch_add(1, Ordering::Relaxed);
            let _ = response_tx.send(DownloadLaneRefillResponse {
                lease: None,
                park_reason,
            });
            self.update_queue_metrics();
            self.publish_hot_dispatch_metrics(now);
            return;
        };

        let activation_items = Self::activation_items(&lease);
        let next_mode = Self::actual_download_lane_mode(
            lease.lane_mode,
            &lease.server_modes,
            server_idx,
            supports_pipelining,
        );
        let work_count = lease.works.len();
        match response_tx.send(DownloadLaneRefillResponse {
            lease: Some(lease),
            park_reason: LaneParkReason::NoWork,
        }) {
            Ok(()) => {
                self.metrics
                    .download_lane_refill_granted_total
                    .fetch_add(1, Ordering::Relaxed);
                self.activate_download_batch(
                    job_id,
                    batch_class,
                    next_mode,
                    work_count,
                    &activation_items,
                    false,
                );
                self.note_download_lane_mode_changed(current_mode, next_mode);
            }
            Err(response) => {
                if let Some(lease) = response.lease {
                    self.rollback_download_batch_lease(lease);
                }
            }
        }
        self.update_queue_metrics();
        self.publish_hot_dispatch_metrics(now);
    }
}
