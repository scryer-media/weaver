use super::*;

impl Pipeline {
    pub(crate) fn maybe_service_deferred_lane_refills(&mut self) {
        if self.deferred_lane_refills.is_empty() {
            return;
        }
        if self.refresh_download_pressure().state == DownloadPressureState::Hard {
            return;
        }
        let mut pending = std::mem::take(&mut self.deferred_lane_refills);
        while let Some(request) = pending.pop_front() {
            self.handle_download_lane_refill_request(request);
            if !self.deferred_lane_refills.is_empty() {
                // Granting re-latched hard pressure and the handler re-deferred
                // this request; stop and keep the rest queued in arrival order.
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
            server_idx,
            remote_ip,
            supports_pipelining,
            current_mode,
            spillover_loan_kind,
            compatibility,
            response_tx,
        } = request;
        let now = Instant::now();
        let mut park_reason = LaneParkReason::NoWork;
        let mut allow_refill = !self.global_paused && !self.rate_limiter.should_wait();
        if !allow_refill {
            self.hot_share_yield_signal.clear();
        }
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
            self.hot_share_yield_signal.clear();
            allow_refill = false;
            park_reason = LaneParkReason::Error;
        }
        if allow_refill
            && self.bandwidth_cap.cap_enabled()
            && self.bandwidth_cap.remaining_bytes() == 0
        {
            self.hot_share_yield_signal.clear();
            allow_refill = false;
            park_reason = LaneParkReason::Pressure;
        }

        let pressure = self.refresh_download_pressure();
        // Soft pressure shrinks the lease (see download_lane_lease_work_limit) instead of
        // idling the lane. Hard pressure holds the request until the backlog drains: the
        // lane blocks on its oneshot either way, and answering on the pressure transition
        // avoids a park/redispatch round-trip per stall.
        if allow_refill && pressure.state == DownloadPressureState::Hard {
            self.deferred_lane_refills
                .push_back(DownloadLaneRefillRequest {
                    job_id,
                    server_idx,
                    remote_ip,
                    supports_pipelining,
                    current_mode,
                    spillover_loan_kind,
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
                    if state.download_queue.is_empty()
                        || !Self::status_allows_download_dispatch(&state.status)
                    {
                        return None;
                    }
                    Some((Self::job_dispatch_priority(state), index, *id))
                })
                .collect::<Vec<_>>();
            eligible.sort_unstable();

            let requested_priority = eligible.iter().find_map(|(priority, _, eligible_job_id)| {
                (*eligible_job_id == job_id).then_some(*priority)
            });
            let effective_capacity = self.effective_download_connection_capacity(
                self.tuner.params().max_concurrent_downloads,
            );
            let bandwidth_cap_tight = self.bandwidth_cap.cap_enabled()
                && self.bandwidth_cap.remaining_bytes()
                    <= self.bandwidth_cap.limit_bytes() * 15 / 100;
            let selected_hot = self.select_hot_dispatch_job(&eligible, now);
            let bounded_share_plan = selected_hot.and_then(|(hot_priority, hot_job_id)| {
                self.bounded_same_band_share_plan(
                    &eligible,
                    hot_priority,
                    hot_job_id,
                    effective_capacity,
                    pressure,
                    bandwidth_cap_tight,
                )
            });
            self.update_hot_share_yield_signal(bounded_share_plan.as_ref());
            match selected_hot {
                Some((_hot_priority, hot_job_id)) if hot_job_id == job_id => {
                    if bounded_share_plan
                        .as_ref()
                        .is_some_and(|plan| self.hot_share_yield_unmet(plan))
                    {
                        allow_refill = false;
                        park_reason = LaneParkReason::HotShareYield;
                    }
                }
                Some((hot_priority, hot_job_id)) => {
                    let hot_speed_bps = self.hot_dispatch_speed_bps(now);
                    self.hot_dispatch_expansion_window
                        .refresh(now, hot_speed_bps);
                    let recent_expansion_helped = self
                        .hot_dispatch_expansion_window
                        .recent_improvement_pct(now)
                        >= HOT_DISPATCH_EXPANSION_HELPFUL_PERCENT;
                    let best_mode_block_reason = self.hot_best_mode_block_reason(
                        hot_job_id,
                        effective_capacity,
                        pressure,
                        recent_expansion_helped,
                    );
                    let can_keep_bounded_same_band = spillover_loan_kind
                        == Some(SpilloverLoanKind::BoundedSameBand)
                        && requested_priority == Some(hot_priority)
                        && bounded_share_plan.as_ref().is_some_and(|plan| {
                            plan.peer_jobs.contains(&job_id)
                                && self.hot_dispatch_spillover_loans.bounded_lent_connections()
                                    <= plan.share_target
                        });
                    let can_keep_lent_lane = requested_priority.is_some_and(|request_priority| {
                        spillover_loan_kind == Some(SpilloverLoanKind::MeasuredUnderfill)
                            && self.hot_dispatch_mode == DispatchShareMode::Shared
                            && request_priority >= hot_priority
                            && best_mode_block_reason == HotBestModeBlockReason::None
                            && !self.hot_spillover_reclaim_pending_for(job_id)
                    });
                    if spillover_loan_kind == Some(SpilloverLoanKind::MeasuredUnderfill)
                        && self.hot_spillover_reclaim_pending_for(job_id)
                    {
                        allow_refill = false;
                        park_reason = LaneParkReason::SpilloverSpeedHarm;
                        self.block_or_reclaim_spillover(SpilloverDecision::ReclaimedSpeedHarm);
                    } else if can_keep_bounded_same_band {
                        self.hot_dispatch_last_lend_at = Some(now);
                    } else if spillover_loan_kind == Some(SpilloverLoanKind::BoundedSameBand) {
                        allow_refill = false;
                        park_reason = LaneParkReason::SpilloverWithdraw;
                        self.record_spillover_decision(SpilloverDecision::Reclaimed);
                    } else if can_keep_lent_lane {
                        self.hot_dispatch_last_lend_at = Some(now);
                    } else {
                        allow_refill = false;
                        if best_mode_block_reason == HotBestModeBlockReason::HotHasQueuedPrimary {
                            self.set_hot_best_mode_block_reason(best_mode_block_reason);
                            self.block_or_reclaim_spillover(
                                SpilloverDecision::BlockedHotCanUseCapacity,
                            );
                            park_reason = LaneParkReason::SpilloverWithdraw;
                        } else if best_mode_block_reason
                            == HotBestModeBlockReason::RecentExpansionHelped
                        {
                            self.set_hot_best_mode_block_reason(best_mode_block_reason);
                            self.block_or_reclaim_spillover(
                                SpilloverDecision::BlockedRecentExpansionHelped,
                            );
                            park_reason = LaneParkReason::SpilloverWithdraw;
                        } else if matches!(
                            best_mode_block_reason,
                            HotBestModeBlockReason::LaneCapacityAvailable
                                | HotBestModeBlockReason::PipelinePromotionPending
                        ) {
                            self.set_hot_best_mode_block_reason(best_mode_block_reason);
                            self.block_or_reclaim_spillover(
                                SpilloverDecision::BlockedBestModePending,
                            );
                            park_reason = LaneParkReason::SpilloverWithdraw;
                        } else if requested_priority.is_none() {
                            self.set_hot_best_mode_block_reason(HotBestModeBlockReason::None);
                            park_reason = LaneParkReason::NoWork;
                        } else {
                            self.set_hot_best_mode_block_reason(HotBestModeBlockReason::None);
                            park_reason = LaneParkReason::HotReclaim;
                        }
                    }
                }
                None => {
                    allow_refill = false;
                    self.clear_hot_dispatch_period();
                    self.hot_share_yield_signal.clear();
                    park_reason = LaneParkReason::NoWork;
                }
            }
        }

        let mut lease = if allow_refill {
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
        if let Some(lease) = lease.as_mut() {
            lease.spillover_loan_kind = spillover_loan_kind;
        }

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
        let is_recovery = lease.compatibility.is_recovery;
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
                    is_recovery,
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
