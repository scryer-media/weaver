use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use crate::JobSpec;

const TRANSPORT_SAMPLE_MAX_SEGMENTS: usize = 128;
const TRANSPORT_SAMPLE_MAX_FILES: usize = 32;
const JOB_BODY_WINDOW: usize = 64;
const LANE_RTT_WINDOW: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct DownloadLaneId(pub(super) u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum DownloadLaneMode {
    Sequential,
    PipelineDepth2,
    PipelineDepth4,
}

impl DownloadLaneMode {
    pub(super) fn max_depth(self) -> usize {
        match self {
            Self::Sequential => 1,
            Self::PipelineDepth2 => 2,
            Self::PipelineDepth4 => 4,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[allow(dead_code)]
pub(crate) enum DownloadLaneState {
    Idle,
    AwaitingWork,
    BindingServer,
    Acquired,
    Issuing,
    Draining,
    YieldAfterBatch,
    Parking,
    Recovering,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[allow(dead_code)]
pub(crate) enum LaneParkReason {
    NoWork,
    Pressure,
    ProbeYield,
    HotReclaim,
    SpilloverWithdraw,
    SpilloverSpeedHarm,
    IpReplacementRetired,
    ServerTierChanged,
    ProofFailure,
    Error,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum JobTransportClass {
    Bulk,
    Chatty,
    Mixed,
}

impl JobTransportClass {
    pub(super) fn eligible_for_depth2_trial(self) -> bool {
        matches!(self, Self::Chatty | Self::Mixed)
    }

    pub(super) fn eligible_for_depth4_trial(self) -> bool {
        matches!(self, Self::Chatty)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct JobTransportProfile {
    class: JobTransportClass,
    body_sizes: VecDeque<u64>,
}

impl JobTransportProfile {
    pub(super) fn classify(spec: &JobSpec) -> Self {
        let mut sizes = Vec::new();

        for file in spec
            .files
            .iter()
            .filter(|file| !file.role.is_recovery())
            .take(TRANSPORT_SAMPLE_MAX_FILES)
        {
            let remaining = TRANSPORT_SAMPLE_MAX_SEGMENTS.saturating_sub(sizes.len());
            if remaining == 0 {
                break;
            }
            sizes.extend(
                file.segments
                    .iter()
                    .take(remaining)
                    .map(|segment| u64::from(segment.bytes)),
            );
        }

        let sample_median_bytes = median(&mut sizes);
        let small_count = sizes.iter().filter(|bytes| **bytes < 64 * 1024).count();
        let sample_small_fraction_milli = if sizes.is_empty() {
            0
        } else {
            ((small_count as u64 * 1000) / sizes.len() as u64) as u32
        };

        let class = if sample_median_bytes < 96 * 1024 || sample_small_fraction_milli >= 400 {
            JobTransportClass::Chatty
        } else if sample_median_bytes >= 256 * 1024 && sample_small_fraction_milli < 100 {
            JobTransportClass::Bulk
        } else {
            JobTransportClass::Mixed
        };

        let mut body_sizes = VecDeque::with_capacity(JOB_BODY_WINDOW);
        for size in sizes.iter().take(JOB_BODY_WINDOW) {
            body_sizes.push_back(*size);
        }

        Self { class, body_sizes }
    }

    pub(super) fn class(&self) -> JobTransportClass {
        self.class
    }

    pub(super) fn median_body_bytes(&self) -> u64 {
        let mut sizes: Vec<u64> = self.body_sizes.iter().copied().collect();
        median(&mut sizes)
    }

    pub(super) fn note_body_size(&mut self, bytes: u64) {
        if self.body_sizes.len() == JOB_BODY_WINDOW {
            self.body_sizes.pop_front();
        }
        self.body_sizes.push_back(bytes);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ServerPipelineState {
    Unknown,
    SequentialOnly,
    PipelineTrialDepth2,
    PipelineProvenDepth2,
    PipelineTrialDepth4,
    PipelineProvenDepth4,
    PipelineBlocked,
}

#[derive(Debug, Clone)]
pub(crate) struct ServerPipelineProof {
    state: ServerPipelineState,
    sequential_successes_since_reset: u64,
    clean_depth2_responses: u64,
    clean_depth2_batches: u64,
    post_depth2_clean_responses: u64,
    clean_depth4_responses: u64,
    clean_depth4_batches: u64,
    failure_rung: u8,
    last_failed_at: Option<Instant>,
    cooldown_until: Option<Instant>,
}

impl Default for ServerPipelineProof {
    fn default() -> Self {
        Self {
            state: ServerPipelineState::Unknown,
            sequential_successes_since_reset: 0,
            clean_depth2_responses: 0,
            clean_depth2_batches: 0,
            post_depth2_clean_responses: 0,
            clean_depth4_responses: 0,
            clean_depth4_batches: 0,
            failure_rung: 0,
            last_failed_at: None,
            cooldown_until: None,
        }
    }
}

impl ServerPipelineProof {
    pub(super) fn state(&self, now: Instant) -> ServerPipelineState {
        if self
            .cooldown_until
            .is_some_and(|cooldown_until| now < cooldown_until)
        {
            ServerPipelineState::PipelineBlocked
        } else {
            self.state
        }
    }

    pub(super) fn choose_mode(
        &self,
        now: Instant,
        job_class: JobTransportClass,
        job_median_body_bytes: u64,
        lane_rtt: Option<Duration>,
        pressure_clear: bool,
    ) -> DownloadLaneMode {
        if !pressure_clear {
            return DownloadLaneMode::Sequential;
        }

        match self.state(now) {
            ServerPipelineState::PipelineProvenDepth4 => DownloadLaneMode::PipelineDepth4,
            ServerPipelineState::PipelineTrialDepth4 => DownloadLaneMode::PipelineDepth4,
            ServerPipelineState::PipelineProvenDepth2 => {
                if job_class.eligible_for_depth4_trial()
                    && self.post_depth2_clean_responses >= 64
                    && job_median_body_bytes < 64 * 1024
                    && lane_rtt.is_some_and(|rtt| rtt >= Duration::from_millis(25))
                {
                    DownloadLaneMode::PipelineDepth4
                } else {
                    DownloadLaneMode::PipelineDepth2
                }
            }
            ServerPipelineState::PipelineTrialDepth2 => DownloadLaneMode::PipelineDepth2,
            ServerPipelineState::Unknown | ServerPipelineState::SequentialOnly => {
                let high_latency_bulk = matches!(job_class, JobTransportClass::Bulk)
                    && lane_rtt.is_some_and(|rtt| rtt >= Duration::from_millis(100));
                if (job_class.eligible_for_depth2_trial() || high_latency_bulk)
                    && self.sequential_successes_since_reset >= 8
                    && self
                        .cooldown_until
                        .is_none_or(|cooldown_until| now >= cooldown_until)
                {
                    DownloadLaneMode::PipelineDepth2
                } else {
                    DownloadLaneMode::Sequential
                }
            }
            ServerPipelineState::PipelineBlocked => DownloadLaneMode::Sequential,
        }
    }

    pub(super) fn note_sequential_result(&mut self, success: bool, supports_pipelining: bool) {
        if success {
            self.sequential_successes_since_reset =
                self.sequential_successes_since_reset.saturating_add(1);
        }
        if !supports_pipelining {
            self.state = ServerPipelineState::SequentialOnly;
            return;
        }
        if matches!(self.state, ServerPipelineState::SequentialOnly) {
            self.state = ServerPipelineState::Unknown;
        }
    }

    pub(super) fn note_pipeline_batch(
        &mut self,
        now: Instant,
        mode: DownloadLaneMode,
        clean: bool,
        response_count: u64,
    ) -> Option<ServerPipelineState> {
        if !clean {
            let state = self.block(now);
            return Some(state);
        }

        match mode {
            DownloadLaneMode::Sequential => None,
            DownloadLaneMode::PipelineDepth2 => {
                if matches!(
                    self.state(now),
                    ServerPipelineState::Unknown | ServerPipelineState::SequentialOnly
                ) {
                    self.state = ServerPipelineState::PipelineTrialDepth2;
                }
                self.clean_depth2_responses =
                    self.clean_depth2_responses.saturating_add(response_count);
                self.clean_depth2_batches = self.clean_depth2_batches.saturating_add(1);
                if self.state == ServerPipelineState::PipelineTrialDepth2
                    && self.clean_depth2_responses >= 24
                    && self.clean_depth2_batches >= 2
                {
                    self.state = ServerPipelineState::PipelineProvenDepth2;
                    return Some(self.state);
                }
                if self.state == ServerPipelineState::PipelineProvenDepth2 {
                    self.post_depth2_clean_responses = self
                        .post_depth2_clean_responses
                        .saturating_add(response_count);
                }
                None
            }
            DownloadLaneMode::PipelineDepth4 => {
                if matches!(self.state(now), ServerPipelineState::PipelineProvenDepth2) {
                    self.state = ServerPipelineState::PipelineTrialDepth4;
                }
                self.clean_depth4_responses =
                    self.clean_depth4_responses.saturating_add(response_count);
                self.clean_depth4_batches = self.clean_depth4_batches.saturating_add(1);
                if self.state == ServerPipelineState::PipelineTrialDepth4
                    && self.clean_depth4_responses >= 32
                    && self.clean_depth4_batches >= 2
                {
                    self.state = ServerPipelineState::PipelineProvenDepth4;
                    return Some(self.state);
                }
                None
            }
        }
    }

    fn block(&mut self, now: Instant) -> ServerPipelineState {
        if self.last_failed_at.is_none_or(|failed_at| {
            now.saturating_duration_since(failed_at) >= Duration::from_secs(24 * 60 * 60)
        }) {
            self.failure_rung = 0;
        }
        self.failure_rung = self.failure_rung.saturating_add(1);
        self.last_failed_at = Some(now);
        let cooldown = match self.failure_rung {
            1 => Duration::from_secs(15 * 60),
            2 => Duration::from_secs(60 * 60),
            _ => Duration::from_secs(6 * 60 * 60),
        };
        self.cooldown_until = Some(now + cooldown);
        self.state = ServerPipelineState::Unknown;
        self.clean_depth2_responses = 0;
        self.clean_depth2_batches = 0;
        self.post_depth2_clean_responses = 0;
        self.clean_depth4_responses = 0;
        self.clean_depth4_batches = 0;
        ServerPipelineState::PipelineBlocked
    }
}

#[derive(Debug, Default, Clone)]
pub(crate) struct LaneRttWindow {
    ewma: Option<Duration>,
    samples: VecDeque<Duration>,
}

impl LaneRttWindow {
    pub(super) fn note(&mut self, sample: Duration) {
        self.ewma = Some(match self.ewma {
            Some(current) => {
                let current_us = current.as_micros() as u64;
                let sample_us = sample.as_micros() as u64;
                Duration::from_micros((current_us.saturating_mul(7) + sample_us) / 8)
            }
            None => sample,
        });
        if self.samples.len() == LANE_RTT_WINDOW {
            self.samples.pop_front();
        }
        self.samples.push_back(sample);
    }

    pub(crate) fn ewma(&self) -> Option<Duration> {
        self.ewma
    }
}

#[derive(Debug, Default)]
pub(crate) struct DownloadLaneRuntimeState {
    pub(super) next_lane_id: u64,
    pub(super) active_by_mode: HashMap<DownloadLaneMode, usize>,
    pub(super) active_by_state: HashMap<DownloadLaneState, usize>,
    pub(super) park_counts: HashMap<LaneParkReason, u64>,
    pub(super) server_proof: HashMap<usize, ServerPipelineProof>,
    pub(super) server_rtt: HashMap<usize, LaneRttWindow>,
}

fn median(values: &mut [u64]) -> u64 {
    if values.is_empty() {
        return 0;
    }
    values.sort_unstable();
    values[values.len() / 2]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FileSpec, SegmentSpec};
    use weaver_model::files::FileRole;

    fn segment(bytes: u32, ordinal: u32) -> SegmentSpec {
        SegmentSpec {
            ordinal,
            article_number: ordinal + 1,
            bytes,
            message_id: format!("segment-{ordinal}@example.test"),
        }
    }

    fn file(role: FileRole, sizes: &[u32]) -> FileSpec {
        FileSpec {
            filename: "sample.bin".to_string(),
            role,
            groups: vec!["alt.binaries.test".to_string()],
            segments: sizes
                .iter()
                .enumerate()
                .map(|(idx, bytes)| segment(*bytes, idx as u32))
                .collect(),
        }
    }

    fn spec(files: Vec<FileSpec>) -> JobSpec {
        let total_bytes = files
            .iter()
            .flat_map(|file| file.segments.iter())
            .map(|segment| u64::from(segment.bytes))
            .sum();
        JobSpec {
            name: "classifier-test".to_string(),
            password: None,
            files,
            total_bytes,
            category: None,
            metadata: Vec::new(),
        }
    }

    #[test]
    fn classifier_identifies_chatty_bulk_and_mixed_layouts() {
        let chatty = JobTransportProfile::classify(&spec(vec![file(
            FileRole::Standalone,
            &[32 * 1024, 48 * 1024, 80 * 1024],
        )]));
        assert_eq!(chatty.class(), JobTransportClass::Chatty);

        let bulk = JobTransportProfile::classify(&spec(vec![file(
            FileRole::RarVolume { volume_number: 0 },
            &[300 * 1024, 320 * 1024, 340 * 1024],
        )]));
        assert_eq!(bulk.class(), JobTransportClass::Bulk);

        let mixed = JobTransportProfile::classify(&spec(vec![file(
            FileRole::Standalone,
            &[120 * 1024, 180 * 1024, 220 * 1024],
        )]));
        assert_eq!(mixed.class(), JobTransportClass::Mixed);
    }

    #[test]
    fn par2_dominated_sample_still_uses_body_size_thresholds() {
        let profile = JobTransportProfile::classify(&spec(vec![file(
            FileRole::Par2 {
                is_index: true,
                recovery_block_count: 0,
            },
            &[300 * 1024, 320 * 1024, 340 * 1024],
        )]));

        assert_eq!(profile.class(), JobTransportClass::Bulk);
    }

    #[test]
    fn proof_promotes_depth2_only_after_sequential_warmup_and_clean_batches() {
        let mut proof = ServerPipelineProof::default();
        let now = Instant::now();
        for _ in 0..7 {
            proof.note_sequential_result(true, true);
        }
        assert_eq!(
            proof.choose_mode(now, JobTransportClass::Chatty, 32 * 1024, None, true),
            DownloadLaneMode::Sequential
        );

        proof.note_sequential_result(true, true);
        assert_eq!(
            proof.choose_mode(now, JobTransportClass::Chatty, 32 * 1024, None, true),
            DownloadLaneMode::PipelineDepth2
        );

        assert_eq!(
            proof.note_pipeline_batch(now, DownloadLaneMode::PipelineDepth2, true, 12),
            None
        );
        assert_eq!(
            proof.note_pipeline_batch(now, DownloadLaneMode::PipelineDepth2, true, 12),
            Some(ServerPipelineState::PipelineProvenDepth2)
        );
        assert_eq!(
            proof.choose_mode(now, JobTransportClass::Chatty, 32 * 1024, None, true),
            DownloadLaneMode::PipelineDepth2
        );
    }

    #[test]
    fn proof_blocks_pipeline_after_unclean_batch() {
        let mut proof = ServerPipelineProof::default();
        let now = Instant::now();
        for _ in 0..8 {
            proof.note_sequential_result(true, true);
        }

        assert_eq!(
            proof.note_pipeline_batch(now, DownloadLaneMode::PipelineDepth2, false, 1),
            Some(ServerPipelineState::PipelineBlocked)
        );
        assert_eq!(
            proof.choose_mode(now, JobTransportClass::Chatty, 32 * 1024, None, true),
            DownloadLaneMode::Sequential
        );
    }

    #[test]
    fn proof_allows_depth2_for_bulk_on_high_latency_servers() {
        let mut proof = ServerPipelineProof::default();
        let now = Instant::now();
        for _ in 0..8 {
            proof.note_sequential_result(true, true);
        }

        assert_eq!(
            proof.choose_mode(
                now,
                JobTransportClass::Bulk,
                320 * 1024,
                Some(Duration::from_millis(99)),
                true,
            ),
            DownloadLaneMode::Sequential
        );
        assert_eq!(
            proof.choose_mode(
                now,
                JobTransportClass::Bulk,
                320 * 1024,
                Some(Duration::from_millis(100)),
                true,
            ),
            DownloadLaneMode::PipelineDepth2
        );
    }

    #[test]
    fn proof_gates_depth4_on_depth2_history_rtt_and_chatty_body_size() {
        let mut proof = ServerPipelineProof::default();
        let now = Instant::now();
        for _ in 0..8 {
            proof.note_sequential_result(true, true);
        }
        proof.note_pipeline_batch(now, DownloadLaneMode::PipelineDepth2, true, 12);
        proof.note_pipeline_batch(now, DownloadLaneMode::PipelineDepth2, true, 12);
        proof.note_pipeline_batch(now, DownloadLaneMode::PipelineDepth2, true, 64);

        assert_eq!(
            proof.choose_mode(
                now,
                JobTransportClass::Chatty,
                48 * 1024,
                Some(Duration::from_millis(25)),
                true,
            ),
            DownloadLaneMode::PipelineDepth4
        );
        assert_eq!(
            proof.choose_mode(
                now,
                JobTransportClass::Mixed,
                48 * 1024,
                Some(Duration::from_millis(25)),
                true,
            ),
            DownloadLaneMode::PipelineDepth2
        );
    }
}
