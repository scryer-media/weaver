use super::*;
use crate::pipeline::download::scheduler::Handout;

pub(in crate::pipeline) fn is_ip_replacement_policy_stop(
    data: &std::result::Result<DownloadPayload, DownloadError>,
) -> bool {
    matches!(
        data,
        Err(DownloadError::Fetch(failure))
            if matches!(
                failure.kind,
                DownloadFailureKind::ServerQuota | DownloadFailureKind::Unrequested
            )
    )
}

pub(in crate::pipeline) fn should_neutrally_park_ip_replacement(
    policy_stopped: bool,
    prior_park_reason: LaneParkReason,
) -> bool {
    policy_stopped && prior_park_reason != LaneParkReason::Error
}

impl Pipeline {
    pub(in crate::pipeline::download::worker) fn observe_ip_rtt_attempts(
        &mut self,
        attempts: &[weaver_nntp::client::FetchAttemptTrace],
    ) {
        if self.ip_replacement_trial_extra_connections == 0 {
            return;
        }

        let now = Instant::now();
        let mut changed = false;
        for attempt in attempts {
            if attempt.outcome != FetchAttemptOutcome::Success {
                continue;
            }
            let Some(ip) = attempt.remote_ip else {
                continue;
            };
            let key = ServerIpKey {
                server_idx: attempt.server_idx,
                ip,
            };
            if !self.ip_rtt_ewma.contains_key(&key)
                && self.ip_rtt_ewma.len() >= MAX_IP_RTT_EWMA_ENTRIES
                && let Some(oldest_key) = self
                    .ip_rtt_ewma
                    .iter()
                    .min_by_key(|(_, state)| state.last_seen)
                    .map(|(key, _)| *key)
            {
                self.ip_rtt_ewma.remove(&oldest_key);
            }

            self.ip_rtt_ewma
                .entry(key)
                .and_modify(|state| state.observe(now, attempt.elapsed))
                .or_insert_with(|| IpRttEwma::new(now, attempt.elapsed));
            changed = true;
        }

        if changed {
            let slowest_ms = self
                .ip_rtt_ewma
                .values()
                .map(|state| state.ewma_ms.ceil() as u64)
                .max()
                .unwrap_or(0);
            self.metrics
                .set_ip_rtt_ewma_summary(self.ip_rtt_ewma.len(), slowest_ms);
        }
    }

    pub(in crate::pipeline::download::worker) fn ip_replacement_baseline_ms(
        &self,
        key: ServerIpKey,
        now: Instant,
    ) -> Option<f64> {
        self.ip_rtt_ewma
            .iter()
            .filter(|(candidate_key, state)| {
                candidate_key.server_idx == key.server_idx
                    && candidate_key.ip != key.ip
                    && state.samples >= IP_REPLACEMENT_BASELINE_MIN_SAMPLES
                    && now.saturating_duration_since(state.last_seen)
                        <= IP_REPLACEMENT_BASELINE_RECENT
            })
            .map(|(_, state)| state.ewma_ms)
            .min_by(|a, b| a.total_cmp(b))
            .or_else(|| {
                self.download_lane_runtime
                    .servers
                    .get(&key.server_idx)
                    .and_then(|explorer| explorer.latency())
                    .map(|duration| duration.as_secs_f64() * 1000.0)
            })
    }

    pub(in crate::pipeline::download::worker) fn select_ip_replacement_candidate(
        &self,
        now: Instant,
    ) -> Option<IpReplacementCandidate> {
        if self.ip_replacement_trial_extra_connections == 0 || self.ip_replacement_burst_active {
            return None;
        }

        self.ip_rtt_ewma
            .iter()
            .filter_map(|(key, state)| {
                if state.samples < IP_REPLACEMENT_MIN_OLD_SAMPLES
                    || now.saturating_duration_since(state.first_seen) < IP_REPLACEMENT_MIN_OLD_AGE
                {
                    return None;
                }
                let baseline_ms = self.ip_replacement_baseline_ms(*key, now)?;
                let old_is_slow = state.ewma_ms >= baseline_ms * IP_REPLACEMENT_OLD_SLOWER_RATIO
                    && state.ewma_ms - baseline_ms >= IP_REPLACEMENT_OLD_SLOWER_MS;
                old_is_slow.then_some(IpReplacementCandidate {
                    old_key: *key,
                    old_ewma_ms: state.ewma_ms,
                    baseline_ms,
                })
            })
            .max_by(|a, b| {
                let a_delta = a.old_ewma_ms - a.baseline_ms;
                let b_delta = b.old_ewma_ms - b.baseline_ms;
                a_delta.total_cmp(&b_delta)
            })
    }

    pub(in crate::pipeline::download::worker) fn ip_replacement_trial_groups(
        &self,
        job_id: JobId,
        server_idx: usize,
    ) -> Option<std::sync::Arc<[String]>> {
        self.jobs.get(&job_id).and_then(|state| {
            state
                .download_queue
                .peek_next_matching(|work| {
                    !work.is_recovery && !work.exclude_servers.contains(&server_idx)
                })
                .map(|work| work.groups.clone())
        })
    }

    pub(in crate::pipeline::download::worker) fn maybe_start_ip_replacement_trial(
        &mut self,
        hot_job_id: JobId,
        pressure: DownloadPressure,
        configured_download_capacity: usize,
    ) {
        if self.ip_replacement_trial_extra_connections == 0 || self.ip_replacement_burst_active {
            return;
        }
        // Cache replays cannot measure a provider IP and must not fetch the
        // same message again through an experimental lane.
        if self.repeated_articles.contains_key(&hot_job_id) {
            return;
        }
        // A trial is only worth an extra connection when the ordinary budget is
        // already fully committed — and never while recovery articles are on
        // the wire, since those are what a repair is waiting on.
        if pressure.suppresses_spillover()
            || configured_download_capacity == 0
            || self.active_download_connections != configured_download_capacity
            || self.active_recovery > 0
            || !self.job_has_dispatchable_work(hot_job_id)
        {
            self.metrics.note_ip_replacement_trial_blocked();
            return;
        }

        let now = Instant::now();
        let Some(candidate) = self.select_ip_replacement_candidate(now) else {
            self.metrics.note_ip_replacement_trial_blocked();
            return;
        };

        let Some(groups) =
            self.ip_replacement_trial_groups(hot_job_id, candidate.old_key.server_idx)
        else {
            self.metrics.note_ip_replacement_trial_blocked();
            return;
        };

        self.ip_replacement_burst_active = true;
        self.metrics.set_ip_replacement_burst_active(true);
        self.metrics.note_ip_replacement_trial_started();
        self.spawn_ip_replacement_candidate_acquire(hot_job_id, candidate, groups);
    }

    pub(in crate::pipeline::download::worker) fn spawn_ip_replacement_candidate_acquire(
        &self,
        job_id: JobId,
        candidate: IpReplacementCandidate,
        groups: std::sync::Arc<[String]>,
    ) {
        let nntp = Arc::clone(&self.nntp);
        let trial_tx = self.ip_replacement_trial_tx.clone();

        tokio::spawn(async move {
            let server = weaver_nntp::ServerId(candidate.old_key.server_idx);
            // An over-max trial lane is off the permit path entirely: it takes
            // the one dedicated replacement socket slot and never a connection
            // permit, so it neither queues behind the download lanes nor takes
            // a connection away from them. Nothing here is affected by the
            // rule that refuses to queue an async caller behind busy lanes.
            match nntp
                .acquire_extra_body_lane_excluding(server, &groups, &[candidate.old_key.ip])
                .await
            {
                Ok(lane) => {
                    let Some(candidate_ip) = lane.remote_ip() else {
                        lane.discard().await;
                        let _ = trial_tx.send(IpReplacementTrialEvent::AcquireFailed).await;
                        return;
                    };
                    if candidate_ip == candidate.old_key.ip {
                        lane.discard().await;
                        let _ = trial_tx.send(IpReplacementTrialEvent::SameIpRejected).await;
                    } else {
                        let _ = trial_tx
                            .send(IpReplacementTrialEvent::CandidateAcquired {
                                job_id,
                                candidate,
                                candidate_ip,
                                lane: Box::new(lane),
                            })
                            .await;
                    }
                }
                Err(_) => {
                    let _ = trial_tx.send(IpReplacementTrialEvent::AcquireFailed).await;
                }
            }
        });
    }

    /// Cut the sample batch a trial fetches through its candidate
    /// connection: payload articles only, from one job, and exactly the
    /// sample count or nothing. Anything the scheduler handed out that a
    /// trial cannot use goes straight back to its queue.
    fn try_lease_ip_replacement_trial_batch(
        &mut self,
        job_id: JobId,
        server_idx: usize,
    ) -> Option<DownloadBatchLease> {
        if self.repeated_articles.contains_key(&job_id) {
            return None;
        }
        let pressure = self.refresh_download_pressure();
        let spill_in_flight = self.spill_job_in_flight_on(server_idx);
        let works = match self.next_works(
            server_idx,
            IP_REPLACEMENT_TRIAL_SAMPLES,
            spill_in_flight,
            pressure,
        ) {
            Handout::Works(works) => works,
            Handout::Idle | Handout::Yield(_) | Handout::Saturated { .. } => return None,
        };
        let handout_job = works.first()?.segment_id.file_id.job_id;
        // Recovery articles are what a repair is waiting on, and critical
        // work is sized for the lane it was cut for: neither is a sample.
        let (samples, returned): (Vec<_>, Vec<_>) = works
            .into_iter()
            .partition(|work| !work.is_recovery && !work.completion_critical);
        let usable = samples.len() >= IP_REPLACEMENT_TRIAL_SAMPLES
            && !self.repeated_articles.contains_key(&handout_job);
        let returned = if usable {
            returned
        } else {
            returned.into_iter().chain(samples.clone()).collect()
        };
        if let Some(state) = self.jobs.get_mut(&handout_job) {
            for work in returned {
                state.download_queue.push(work);
            }
        }
        if !usable {
            self.update_queue_metrics();
            return None;
        }
        let lease = self.lease_for_handout(
            Self::next_download_lane_id(),
            server_idx,
            DownloadLaneMode::Sequential,
            pressure,
            samples,
        )?;
        if lease.works.len() < IP_REPLACEMENT_TRIAL_SAMPLES {
            self.rollback_download_batch_lease(lease);
            return None;
        }
        Some(lease)
    }

    pub(crate) fn handle_ip_replacement_trial_event(&mut self, event: IpReplacementTrialEvent) {
        match event {
            IpReplacementTrialEvent::CandidateAcquired {
                job_id,
                candidate,
                candidate_ip,
                lane,
            } => {
                if self.ip_replacement_trial_extra_connections == 0
                    || !self.ip_replacement_burst_active
                {
                    tokio::spawn(async move {
                        lane.discard().await;
                    });
                    self.metrics.note_ip_replacement_trial_rejected();
                    self.ip_replacement_burst_active = false;
                    self.metrics.set_ip_replacement_burst_active(false);
                    return;
                }

                let server_idx = candidate.old_key.server_idx;
                let lease = match self.try_lease_ip_replacement_trial_batch(job_id, server_idx) {
                    Some(lease) => lease,
                    None => {
                        let trial_tx = self.ip_replacement_trial_tx.clone();
                        tokio::spawn(async move {
                            let lane = lane;
                            lane.discard().await;
                            let _ = trial_tx
                                .send(IpReplacementTrialEvent::CandidateRejected)
                                .await;
                        });
                        return;
                    }
                };

                let activation_items = Self::activation_items(&lease);
                self.activate_download_batch_lease(&lease, &activation_items, false);
                if let Some(owner) = self.download_lane_owners.get_mut(&lease.lane_id) {
                    owner.server_idx = Some(server_idx);
                }
                self.spawn_ip_replacement_trial(lease, candidate, candidate_ip, *lane);
            }
            IpReplacementTrialEvent::AcquireFailed => {
                self.metrics.note_ip_replacement_trial_acquire_failed();
                self.metrics.note_ip_replacement_trial_rejected();
                self.ip_replacement_burst_active = false;
                self.metrics.set_ip_replacement_burst_active(false);
            }
            IpReplacementTrialEvent::SameIpRejected => {
                self.metrics.note_ip_replacement_trial_same_ip_rejected();
                self.metrics.note_ip_replacement_trial_rejected();
                self.ip_replacement_burst_active = false;
                self.metrics.set_ip_replacement_burst_active(false);
            }
            IpReplacementTrialEvent::CandidateRejected => {
                self.metrics.note_ip_replacement_trial_rejected();
                self.ip_replacement_burst_active = false;
                self.metrics.set_ip_replacement_burst_active(false);
            }
            IpReplacementTrialEvent::CandidateAccepted {
                lane_id,
                old_key,
                samples,
            } => {
                if !self.download_lane_is_live(lane_id) {
                    return;
                }
                if self.ip_replacement_trial_extra_connections == 0
                    || !self.ip_replacement_burst_active
                {
                    return;
                }
                self.ip_replacement_retired_ips.insert(old_key);
                self.observe_ip_rtt_attempts(&samples);
            }
        }
    }

    pub(in crate::pipeline::download::worker) fn spawn_ip_replacement_trial(
        &self,
        lease: DownloadBatchLease,
        candidate: IpReplacementCandidate,
        candidate_ip: IpAddr,
        mut lane: weaver_nntp::BodyLaneLease,
    ) {
        let nntp = Arc::clone(&self.nntp);
        let metrics = Arc::clone(&self.metrics);
        let tx = self.download_done_tx.clone();
        let parked_tx = self.download_lane_parked_tx.clone();
        let trial_tx = self.ip_replacement_trial_tx.clone();

        tokio::spawn(async move {
            let fetch_started = Instant::now();
            let server = weaver_nntp::ServerId(candidate.old_key.server_idx);
            let mode = DownloadLaneMode::Sequential;
            let job_id = lease.job_id;
            let runtime_generation = lease.runtime_generation;
            let lane_id = lease.lane_id;
            let mut trial_attempts = Vec::new();
            let mut park_reason = LaneParkReason::NoWork;
            let mut policy_stopped = false;

            let mut works = lease.works.into_iter();
            while let Some(work) = works.next() {
                let estimate = Self::bandwidth_reservation_estimate(work.byte_estimate);
                let trace = lane
                    .fetch_decoded_sequential_with_estimate(&work.message_id.to_string(), estimate)
                    .await;
                trial_attempts.extend(trace.attempts.iter().cloned());
                let segment_id = work.segment_id;
                let retry_count = work.retry_count;
                let exclude_servers = work.exclude_servers.clone();
                let (data, attempts, source_server_idx) =
                    Self::download_data_from_decoded_trace(segment_id, trace);
                let policy_stop = is_ip_replacement_policy_stop(&data);
                if data.is_err() && !policy_stop {
                    park_reason = LaneParkReason::Error;
                }
                let _ = tx
                    .send(DownloadResult {
                        lane_id,
                        job_id,
                        segment_id,
                        runtime_generation,
                        data,
                        attempts,
                        lane_observation: None,
                        source_server_idx,
                        origin: DownloadResultOrigin::IpReplacementTrial,
                        retry_count,
                        exclude_servers,
                        release_connection_slot: false,
                    })
                    .await;
                if policy_stop {
                    policy_stopped = true;
                    for unrequested in works.by_ref() {
                        let _ = tx
                            .send(DownloadResult {
                                lane_id,
                                job_id,
                                segment_id: unrequested.segment_id,
                                runtime_generation,
                                data: Err(DownloadError::Fetch(DownloadFailure::new(
                                    DownloadFailureKind::Unrequested,
                                    "BODY not requested because the IP replacement trial hit server quota",
                                ))),
                                attempts: Vec::new(),
                                lane_observation: None,
                                source_server_idx: None,
                                origin: DownloadResultOrigin::IpReplacementTrial,
                                retry_count: unrequested.retry_count,
                                exclude_servers: unrequested.exclude_servers.clone(),
                                release_connection_slot: false,
                            })
                            .await;
                    }
                    break;
                }
            }

            let policy_parked = should_neutrally_park_ip_replacement(policy_stopped, park_reason);
            let candidate_rtts = if policy_parked {
                Vec::new()
            } else {
                trial_attempts
                    .iter()
                    .filter(|attempt| {
                        attempt.outcome == FetchAttemptOutcome::Success
                            && attempt.server_idx == candidate.old_key.server_idx
                            && attempt.remote_ip == Some(candidate_ip)
                    })
                    .map(|attempt| attempt.elapsed.as_secs_f64() * 1000.0)
                    .collect::<Vec<_>>()
            };

            let accepted = if policy_parked {
                false
            } else if candidate_rtts.len() >= IP_REPLACEMENT_TRIAL_SAMPLES {
                let candidate_ewma_ms =
                    candidate_rtts.iter().sum::<f64>() / candidate_rtts.len() as f64;
                let better_by_ratio = candidate_ewma_ms
                    <= candidate.old_ewma_ms * IP_REPLACEMENT_CANDIDATE_BETTER_RATIO;
                let better_by_ms =
                    candidate.old_ewma_ms - candidate_ewma_ms >= IP_REPLACEMENT_CANDIDATE_BETTER_MS;
                if better_by_ratio && better_by_ms {
                    nntp.retire_server_ip(server, candidate.old_key.ip).await;
                    metrics.note_ip_replacement_trial_accepted();
                    metrics.note_ip_replacement_old_connection_retired();
                    debug!(
                        server = candidate.old_key.server_idx,
                        old_ip = %candidate.old_key.ip,
                        candidate_ip = %candidate_ip,
                        old_ewma_ms = candidate.old_ewma_ms,
                        candidate_ewma_ms,
                        baseline_ms = candidate.baseline_ms,
                        "accepted IP replacement trial"
                    );
                    true
                } else {
                    metrics.note_ip_replacement_trial_rejected();
                    debug!(
                        server = candidate.old_key.server_idx,
                        old_ip = %candidate.old_key.ip,
                        candidate_ip = %candidate_ip,
                        old_ewma_ms = candidate.old_ewma_ms,
                        candidate_ewma_ms,
                        baseline_ms = candidate.baseline_ms,
                        "rejected IP replacement trial"
                    );
                    false
                }
            } else {
                metrics.note_ip_replacement_trial_rejected();
                debug!(
                    server = candidate.old_key.server_idx,
                    old_ip = %candidate.old_key.ip,
                    candidate_ip = %candidate_ip,
                    successful_samples = candidate_rtts.len(),
                    "rejected IP replacement trial without enough distinct-IP proof"
                );
                false
            };

            if policy_parked {
                lane.park();
            } else if accepted {
                let accepted_samples = trial_attempts
                    .iter()
                    .filter(|attempt| {
                        attempt.outcome == FetchAttemptOutcome::Success
                            && attempt.server_idx == candidate.old_key.server_idx
                            && attempt.remote_ip == Some(candidate_ip)
                    })
                    .cloned()
                    .collect();
                let _ = trial_tx
                    .send(IpReplacementTrialEvent::CandidateAccepted {
                        lane_id,
                        old_key: candidate.old_key,
                        samples: accepted_samples,
                    })
                    .await;
                lane.park();
            } else {
                lane.discard().await;
            }
            let _ = parked_tx
                .send(DownloadLaneParked {
                    lane_id,
                    job_id,
                    mode,
                    completion_critical: false,
                    reason: if policy_parked {
                        LaneParkReason::ServerQuota
                    } else if accepted {
                        park_reason
                    } else {
                        LaneParkReason::ProofFailure
                    },
                    release_connection_slot: false,
                    release_ip_replacement_burst: true,
                })
                .await;
            crate::runtime::perf_probe::record(
                "download.ip_replacement_trial",
                fetch_started.elapsed(),
            );
        });
    }
}
