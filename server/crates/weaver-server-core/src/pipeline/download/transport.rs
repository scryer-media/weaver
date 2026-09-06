use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Latency bands. A band picks the starting rung, labels a server in the UI,
/// and labels a connection test. It never gates a depth on its own: the depth
/// comes from the measured latency/transfer ratio.
pub(crate) const LATENCY_BAND_GOOD_MAX: Duration = Duration::from_millis(400);
pub(crate) const LATENCY_BAND_MODERATE_MAX: Duration = Duration::from_millis(800);

/// Depths the explorer will actually run. `1` is sequential; a failure drops
/// off the bottom of the pipelined rungs onto it.
const RUNGS: [u8; 4] = [1, 2, 4, 8];
const MIN_TARGET_DEPTH: u32 = 2;
const MAX_TARGET_DEPTH: u32 = 8;

/// Clean responses observed at one rung before the explorer acts on what it
/// measured there. Also bounds rung changes to one per window.
const RUNG_WINDOW_RESPONSES: u64 = 32;

/// A rung is only kept when it beats the rung below it by this much; anything
/// less is noise on a shared link.
const RUNG_KEEP_THROUGHPUT_RATIO: f64 = 1.05;

/// How long a server is left alone after a revert or an unclean batch.
const RUNG_HOLD: Duration = Duration::from_secs(10 * 60);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum DownloadLaneMode {
    Sequential,
    Pipelined { depth: u8 },
}

impl DownloadLaneMode {
    pub(super) fn max_depth(self) -> usize {
        match self {
            Self::Sequential => 1,
            Self::Pipelined { depth } => usize::from(depth).max(1),
        }
    }

    pub(crate) fn depth(self) -> u8 {
        match self {
            Self::Sequential => 1,
            Self::Pipelined { depth } => depth.max(1),
        }
    }

    pub(crate) fn from_depth(depth: u8) -> Self {
        if depth <= 1 {
            Self::Sequential
        } else {
            Self::Pipelined { depth }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum LatencyBand {
    Good,
    Moderate,
    Slow,
}

impl LatencyBand {
    pub(crate) fn from_latency(latency: Duration) -> Self {
        if latency < LATENCY_BAND_GOOD_MAX {
            Self::Good
        } else if latency < LATENCY_BAND_MODERATE_MAX {
            Self::Moderate
        } else {
            Self::Slow
        }
    }

    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Good => "good",
            Self::Moderate => "moderate",
            Self::Slow => "slow",
        }
    }

    /// Where a server with no measurement of its own starts. A further-away
    /// server has more round trip to hide, so it starts deeper.
    pub(crate) fn starting_depth(self) -> u8 {
        match self {
            Self::Good => 2,
            Self::Moderate => 4,
            Self::Slow => 8,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum LaneParkReason {
    NoWork,
    Pressure,
    ProbeYield,
    HotReclaim,
    HotShareYield,
    SpilloverWithdraw,
    SpilloverSpeedHarm,
    IpReplacementRetired,
    ProofFailure,
    Capacity,
    ServerQuota,
    Error,
}

/// What the explorer decided at the end of a window, for the one log line a
/// rung change is allowed to write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RungChange {
    /// Moved one rung toward the physical target.
    Stepped { from: u8, to: u8 },
    /// The step up paid for itself and is now the baseline.
    Kept { depth: u8 },
    /// The step up did not pay for itself; back one rung and hold.
    Reverted { from: u8, to: u8 },
    /// An unclean pipelined batch cost a rung.
    Dropped { from: u8, to: u8 },
    /// A second unclean batch inside the hold: this server does not pipeline.
    PinnedSequential,
}

fn rung_index(depth: u8) -> usize {
    RUNGS
        .iter()
        .position(|rung| *rung == depth)
        .unwrap_or_else(|| RUNGS.iter().filter(|rung| **rung <= depth).count().max(1) - 1)
}

/// The lowest rung that covers `depth`, so a physical target of 3 asks for 4.
fn rung_at_or_above(depth: u8) -> u8 {
    RUNGS
        .iter()
        .copied()
        .find(|rung| *rung >= depth)
        .unwrap_or(*RUNGS.last().expect("rung ladder is not empty"))
}

/// Per-server depth explorer.
///
/// It holds two measurements the lanes take apart for it — how long a request
/// waits for its status line, and how long the article itself takes on the
/// wire — and walks the rung ladder toward the depth that would keep the link
/// busy, keeping a rung only when it measurably paid for itself.
#[derive(Debug, Clone)]
pub(crate) struct ServerPipelineExplorer {
    supports_pipelining: bool,
    pinned_sequential: bool,
    latency: Option<Duration>,
    transfer: Option<Duration>,
    current_depth: u8,
    /// Rung the current window is being compared against, and its throughput.
    baseline_depth: Option<u8>,
    baseline_throughput_bps: Option<f64>,
    /// Set while the current rung is a step up still on trial.
    on_trial: bool,
    window_responses: u64,
    window_bytes: u64,
    window_elapsed: Duration,
    hold_until: Option<Instant>,
    last_unclean_at: Option<Instant>,
    /// Depth the servers table already holds, so a kept rung only writes once.
    persisted_depth: Option<u8>,
}

impl Default for ServerPipelineExplorer {
    fn default() -> Self {
        Self {
            supports_pipelining: false,
            pinned_sequential: false,
            latency: None,
            transfer: None,
            current_depth: 2,
            baseline_depth: None,
            baseline_throughput_bps: None,
            on_trial: false,
            window_responses: 0,
            window_bytes: 0,
            window_elapsed: Duration::ZERO,
            hold_until: None,
            last_unclean_at: None,
            persisted_depth: None,
        }
    }
}

impl ServerPipelineExplorer {
    /// Start from what the last run proved, or from the band of the connection
    /// test's first-byte latency, or from the shallowest pipelined rung.
    pub(crate) fn seeded(proven_depth: Option<u8>, probe_latency: Option<Duration>) -> Self {
        let start = proven_depth
            .map(rung_at_or_above)
            .or_else(|| {
                probe_latency.map(|latency| LatencyBand::from_latency(latency).starting_depth())
            })
            .unwrap_or(2);
        Self {
            current_depth: start.clamp(2, 8),
            latency: probe_latency,
            persisted_depth: proven_depth,
            ..Self::default()
        }
    }

    /// The depth to write through, if the proven rung has moved since the last
    /// write. Consumed, so a rung held across many windows writes once.
    pub(super) fn take_persist_request(&mut self) -> Option<u8> {
        if self.persisted_depth == Some(self.current_depth) {
            return None;
        }
        self.persisted_depth = Some(self.current_depth);
        Some(self.current_depth)
    }

    pub(crate) fn current_depth(&self) -> u8 {
        self.current_depth
    }

    pub(crate) fn latency(&self) -> Option<Duration> {
        self.latency
    }

    pub(crate) fn transfer(&self) -> Option<Duration> {
        self.transfer
    }

    pub(crate) fn latency_band(&self) -> Option<LatencyBand> {
        self.latency.map(LatencyBand::from_latency)
    }

    pub(crate) fn pinned_sequential(&self) -> bool {
        self.pinned_sequential
    }

    pub(super) fn note_latency(&mut self, sample: Duration) {
        self.latency = Some(blend(self.latency, sample));
    }

    pub(super) fn note_transfer(&mut self, sample: Duration) {
        self.transfer = Some(blend(self.transfer, sample));
    }

    pub(super) fn note_supports_pipelining(&mut self, supports_pipelining: bool) {
        self.supports_pipelining = supports_pipelining;
    }

    /// Depth that would keep one connection busy across the round trip:
    /// enough requests in flight to cover the wait, plus the one being served.
    pub(crate) fn physical_target_depth(&self) -> Option<u8> {
        let latency = self.latency?;
        let transfer = self.transfer?;
        if transfer.is_zero() {
            return Some(MAX_TARGET_DEPTH as u8);
        }
        let ratio = latency.as_secs_f64() / transfer.as_secs_f64();
        let depth = (ratio.ceil().max(0.0) as u32).saturating_add(1);
        Some(depth.clamp(MIN_TARGET_DEPTH, MAX_TARGET_DEPTH) as u8)
    }

    /// The rung the explorer is walking toward.
    pub(crate) fn target_rung(&self) -> u8 {
        self.physical_target_depth()
            .map_or(self.current_depth, rung_at_or_above)
    }

    pub(super) fn choose_mode(&self, pressure_clear: bool) -> DownloadLaneMode {
        if !pressure_clear || self.pinned_sequential || !self.supports_pipelining {
            return DownloadLaneMode::Sequential;
        }
        DownloadLaneMode::from_depth(self.current_depth)
    }

    /// Fold one clean response into the current window and, once the window is
    /// full, decide whether the rung stays, reverts, or advances.
    pub(super) fn note_response(
        &mut self,
        now: Instant,
        observed_depth: u8,
        payload_bytes: u64,
        policy_elapsed: Duration,
        pressure_clear: bool,
    ) -> Option<RungChange> {
        // A pinned server is out of the exploration entirely: nothing it does
        // afterwards may walk its depth back up.
        if self.pinned_sequential {
            return None;
        }
        // Pressure forces sequential and distorts throughput; such a sample
        // says nothing about the rung under test.
        if !pressure_clear || observed_depth != self.current_depth {
            return None;
        }
        self.window_responses = self.window_responses.saturating_add(1);
        self.window_bytes = self.window_bytes.saturating_add(payload_bytes);
        self.window_elapsed = self.window_elapsed.saturating_add(policy_elapsed);
        if self.window_responses < RUNG_WINDOW_RESPONSES {
            return None;
        }

        let throughput = self.window_throughput_bps();
        self.reset_window();

        if self.on_trial {
            let baseline = self.baseline_throughput_bps.unwrap_or(0.0);
            let previous = self.baseline_depth.unwrap_or(self.current_depth);
            if throughput >= baseline * RUNG_KEEP_THROUGHPUT_RATIO {
                self.on_trial = false;
                self.baseline_depth = Some(self.current_depth);
                self.baseline_throughput_bps = Some(throughput);
                return Some(RungChange::Kept {
                    depth: self.current_depth,
                });
            }
            let from = self.current_depth;
            self.current_depth = previous;
            self.on_trial = false;
            self.baseline_depth = Some(previous);
            self.baseline_throughput_bps = Some(throughput);
            self.hold_until = Some(now + RUNG_HOLD);
            return Some(RungChange::Reverted { from, to: previous });
        }

        self.baseline_depth = Some(self.current_depth);
        self.baseline_throughput_bps = Some(throughput);

        if self.hold_until.is_some_and(|until| now < until) {
            return None;
        }
        self.hold_until = None;

        let target = self.target_rung();
        if target == self.current_depth {
            return None;
        }
        let from = self.current_depth;
        let to = self.step_toward(target);
        if to == from {
            return None;
        }
        self.current_depth = to;
        self.on_trial = to > from;
        Some(RungChange::Stepped { from, to })
    }

    /// An unclean pipelined batch: one rung off, and hands off this server for
    /// ten minutes. A second one inside that window settles the question.
    pub(super) fn note_unclean_batch(&mut self, now: Instant) -> Option<RungChange> {
        if self.pinned_sequential {
            return None;
        }
        self.reset_window();
        self.on_trial = false;
        self.baseline_depth = None;
        self.baseline_throughput_bps = None;

        let within_hold = self
            .last_unclean_at
            .is_some_and(|at| now.saturating_duration_since(at) < RUNG_HOLD);
        self.last_unclean_at = Some(now);
        if within_hold {
            self.pinned_sequential = true;
            self.current_depth = 1;
            return Some(RungChange::PinnedSequential);
        }

        self.hold_until = Some(now + RUNG_HOLD);
        let from = self.current_depth;
        let to = RUNGS[rung_index(from).saturating_sub(1)];
        self.current_depth = to;
        if to == from {
            return None;
        }
        Some(RungChange::Dropped { from, to })
    }

    fn step_toward(&self, target: u8) -> u8 {
        let current = rung_index(self.current_depth);
        let target_idx = rung_index(target);
        if target_idx > current {
            RUNGS[(current + 1).min(RUNGS.len() - 1)]
        } else if target_idx < current {
            RUNGS[current.saturating_sub(1)]
        } else {
            self.current_depth
        }
    }

    fn window_throughput_bps(&self) -> f64 {
        let seconds = self.window_elapsed.as_secs_f64();
        if seconds <= 0.0 {
            return 0.0;
        }
        self.window_bytes as f64 / seconds
    }

    fn reset_window(&mut self) {
        self.window_responses = 0;
        self.window_bytes = 0;
        self.window_elapsed = Duration::ZERO;
    }
}

fn blend(current: Option<Duration>, sample: Duration) -> Duration {
    match current {
        Some(current) => Duration::from_micros(
            (current.as_micros() as u64).saturating_mul(3) / 4 + (sample.as_micros() as u64) / 4,
        ),
        None => sample,
    }
}

#[derive(Debug, Default)]
pub(crate) struct DownloadLaneRuntimeState {
    pub(super) servers: HashMap<usize, ServerPipelineExplorer>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn explorer(latency_ms: u64, transfer_ms: u64) -> ServerPipelineExplorer {
        let mut explorer = ServerPipelineExplorer::default();
        explorer.note_supports_pipelining(true);
        explorer.latency = Some(Duration::from_millis(latency_ms));
        explorer.transfer = Some(Duration::from_millis(transfer_ms));
        explorer
    }

    /// Feed one full window at the explorer's current rung.
    fn run_window(
        explorer: &mut ServerPipelineExplorer,
        now: Instant,
        bytes_per_response: u64,
        elapsed_per_response: Duration,
    ) -> Option<RungChange> {
        let depth = explorer.current_depth();
        let mut change = None;
        for _ in 0..RUNG_WINDOW_RESPONSES {
            change = explorer
                .note_response(now, depth, bytes_per_response, elapsed_per_response, true)
                .or(change);
        }
        change
    }

    #[test]
    fn target_depth_follows_the_latency_to_transfer_ratio() {
        // Good band, article costs about as much as the round trip.
        let good = explorer(200, 200);
        assert_eq!(good.physical_target_depth(), Some(2));
        assert_eq!(good.target_rung(), 2);

        // Good band, tiny articles: the link is idle most of the round trip.
        let good_small = explorer(300, 50);
        assert_eq!(good_small.physical_target_depth(), Some(7));
        assert_eq!(good_small.target_rung(), 8);

        // Moderate band with a large article still wants a shallow rung.
        let moderate = explorer(600, 400);
        assert_eq!(moderate.physical_target_depth(), Some(3));
        assert_eq!(moderate.target_rung(), 4);

        // Slow band, large articles.
        let slow = explorer(900, 900);
        assert_eq!(slow.physical_target_depth(), Some(2));
        assert_eq!(slow.target_rung(), 2);

        // Slow band, small articles: clamped at the deepest rung.
        let slow_small = explorer(1000, 20);
        assert_eq!(slow_small.physical_target_depth(), Some(8));
        assert_eq!(slow_small.target_rung(), 8);
    }

    #[test]
    fn latency_bands_split_at_the_documented_thresholds() {
        assert_eq!(
            LatencyBand::from_latency(Duration::from_millis(399)),
            LatencyBand::Good
        );
        assert_eq!(
            LatencyBand::from_latency(Duration::from_millis(400)),
            LatencyBand::Moderate
        );
        assert_eq!(
            LatencyBand::from_latency(Duration::from_millis(799)),
            LatencyBand::Moderate
        );
        assert_eq!(
            LatencyBand::from_latency(Duration::from_millis(800)),
            LatencyBand::Slow
        );
    }

    #[test]
    fn first_window_sets_a_baseline_then_steps_one_rung_toward_the_target() {
        let mut explorer = explorer(300, 50);
        let now = Instant::now();
        assert_eq!(explorer.current_depth(), 2);

        let change = run_window(&mut explorer, now, 100_000, Duration::from_millis(100));
        assert_eq!(change, Some(RungChange::Stepped { from: 2, to: 4 }));
        assert_eq!(explorer.current_depth(), 4);
        assert_eq!(
            explorer.choose_mode(true),
            DownloadLaneMode::Pipelined { depth: 4 }
        );
    }

    #[test]
    fn a_step_up_that_pays_for_itself_is_kept() {
        let mut explorer = explorer(300, 50);
        let now = Instant::now();
        run_window(&mut explorer, now, 100_000, Duration::from_millis(100));
        assert_eq!(explorer.current_depth(), 4);

        // Twice the bytes in the same time: comfortably past the 1.05 bar.
        let change = run_window(&mut explorer, now, 200_000, Duration::from_millis(100));
        assert_eq!(change, Some(RungChange::Kept { depth: 4 }));
        assert_eq!(explorer.current_depth(), 4);
    }

    #[test]
    fn a_step_up_that_does_not_pay_reverts_and_holds() {
        let mut explorer = explorer(300, 50);
        let now = Instant::now();
        run_window(&mut explorer, now, 100_000, Duration::from_millis(100));
        assert_eq!(explorer.current_depth(), 4);

        let change = run_window(&mut explorer, now, 101_000, Duration::from_millis(100));
        assert_eq!(change, Some(RungChange::Reverted { from: 4, to: 2 }));
        assert_eq!(explorer.current_depth(), 2);

        // Inside the hold the explorer leaves the server alone.
        let change = run_window(&mut explorer, now, 100_000, Duration::from_millis(100));
        assert_eq!(change, None);
        assert_eq!(explorer.current_depth(), 2);

        // Past the hold it is free to try again.
        let later = now + RUNG_HOLD + Duration::from_secs(1);
        let change = run_window(&mut explorer, later, 100_000, Duration::from_millis(100));
        assert_eq!(change, Some(RungChange::Stepped { from: 2, to: 4 }));
    }

    #[test]
    fn an_unclean_batch_drops_a_rung_and_a_second_one_pins_sequential() {
        let mut explorer = explorer(300, 50);
        let now = Instant::now();
        run_window(&mut explorer, now, 100_000, Duration::from_millis(100));
        assert_eq!(explorer.current_depth(), 4);

        assert_eq!(
            explorer.note_unclean_batch(now),
            Some(RungChange::Dropped { from: 4, to: 2 })
        );
        assert_eq!(explorer.current_depth(), 2);

        assert_eq!(
            explorer.note_unclean_batch(now + Duration::from_secs(60)),
            Some(RungChange::PinnedSequential)
        );
        assert!(explorer.pinned_sequential());
        assert_eq!(explorer.choose_mode(true), DownloadLaneMode::Sequential);

        // The pin outlives the hold: it is for the rest of the process, and a
        // full window of clean responses does not lift it.
        let later = now + RUNG_HOLD * 3;
        assert_eq!(
            run_window(&mut explorer, later, 100_000, Duration::from_millis(100)),
            None
        );
        assert_eq!(explorer.choose_mode(true), DownloadLaneMode::Sequential);
        // A third unclean batch is a no-op rather than a second warning.
        assert_eq!(explorer.note_unclean_batch(later), None);
    }

    #[test]
    fn an_unclean_batch_at_the_shallowest_rung_falls_back_to_sequential() {
        let mut explorer = explorer(300, 200);
        let now = Instant::now();
        assert_eq!(explorer.current_depth(), 2);
        assert_eq!(
            explorer.note_unclean_batch(now),
            Some(RungChange::Dropped { from: 2, to: 1 })
        );
        assert_eq!(explorer.choose_mode(true), DownloadLaneMode::Sequential);
    }

    #[test]
    fn samples_taken_under_byte_pressure_are_discarded() {
        let mut explorer = explorer(300, 50);
        let now = Instant::now();
        for _ in 0..RUNG_WINDOW_RESPONSES * 2 {
            assert_eq!(
                explorer.note_response(now, 2, 100_000, Duration::from_millis(100), false),
                None
            );
        }
        assert_eq!(explorer.current_depth(), 2);
    }

    #[test]
    fn at_most_one_rung_change_per_window() {
        let mut explorer = explorer(300, 20);
        let now = Instant::now();
        assert_eq!(explorer.target_rung(), 8);

        let change = run_window(&mut explorer, now, 100_000, Duration::from_millis(100));
        assert_eq!(change, Some(RungChange::Stepped { from: 2, to: 4 }));

        // One short of a window buys nothing more.
        for _ in 0..RUNG_WINDOW_RESPONSES - 1 {
            assert_eq!(
                explorer.note_response(now, 4, 400_000, Duration::from_millis(100), true),
                None
            );
        }
        assert_eq!(explorer.current_depth(), 4);
    }

    #[test]
    fn a_seeded_explorer_starts_at_the_proven_rung_or_the_probe_band() {
        assert_eq!(
            ServerPipelineExplorer::seeded(Some(8), None).current_depth(),
            8
        );
        assert_eq!(
            ServerPipelineExplorer::seeded(None, Some(Duration::from_millis(900))).current_depth(),
            8
        );
        assert_eq!(
            ServerPipelineExplorer::seeded(None, Some(Duration::from_millis(500))).current_depth(),
            4
        );
        assert_eq!(
            ServerPipelineExplorer::seeded(None, Some(Duration::from_millis(50))).current_depth(),
            2
        );
        assert_eq!(
            ServerPipelineExplorer::seeded(None, None).current_depth(),
            2
        );
        // A proven depth off the ladder rounds up to a rung that is actually run.
        assert_eq!(
            ServerPipelineExplorer::seeded(Some(3), None).current_depth(),
            4
        );
        // A proven rung outranks the probe band; the probe only fills the gap.
        assert_eq!(
            ServerPipelineExplorer::seeded(Some(2), Some(Duration::from_millis(900)))
                .current_depth(),
            2
        );
    }

    /// The persisted column must only ever hold a rung the explorer settled
    /// on, and a settled rung must only be written once.
    #[test]
    fn only_a_settled_rung_is_offered_for_persistence() {
        let mut explorer = explorer(300, 50);
        let now = Instant::now();
        // Seeded from nothing, so the shallowest rung is already worth saving.
        assert_eq!(explorer.take_persist_request(), Some(2));
        assert_eq!(explorer.take_persist_request(), None);

        run_window(&mut explorer, now, 100_000, Duration::from_millis(100));
        assert_eq!(explorer.current_depth(), 4);
        // The step up is on trial; the caller must not write it through yet,
        // but the value it would write is the one under test.
        run_window(&mut explorer, now, 200_000, Duration::from_millis(100));
        assert_eq!(explorer.take_persist_request(), Some(4));
        assert_eq!(explorer.take_persist_request(), None);

        // Losing the rung offers the lower value, so a restart cannot resume
        // at a depth this server has since failed.
        explorer.note_unclean_batch(now);
        assert_eq!(explorer.current_depth(), 2);
        assert_eq!(explorer.take_persist_request(), Some(2));
    }

    /// A server seeded deep from a previous run keeps that rung: the explorer
    /// has nothing to walk toward until it has measured the link itself.
    #[test]
    fn a_seeded_rung_is_held_until_the_explorer_has_its_own_measurements() {
        let mut explorer = ServerPipelineExplorer::seeded(Some(8), None);
        explorer.note_supports_pipelining(true);
        let now = Instant::now();
        assert_eq!(
            explorer.choose_mode(true),
            DownloadLaneMode::Pipelined { depth: 8 }
        );
        assert_eq!(explorer.physical_target_depth(), None);
        assert_eq!(
            run_window(&mut explorer, now, 100_000, Duration::from_millis(100)),
            None
        );
        assert_eq!(explorer.current_depth(), 8);
    }

    /// Responses that belong to another rung — a batch already in flight when
    /// the depth moved — must not be folded into the window under test.
    #[test]
    fn responses_from_a_stale_rung_are_discarded() {
        let mut explorer = explorer(300, 50);
        let now = Instant::now();
        for _ in 0..RUNG_WINDOW_RESPONSES * 2 {
            assert_eq!(
                explorer.note_response(now, 8, 100_000, Duration::from_millis(100), true),
                None
            );
        }
        assert_eq!(explorer.current_depth(), 2);
    }

    /// A step down toward a shallower target is a measured decision, not a
    /// trial: it becomes the baseline immediately rather than being re-judged.
    #[test]
    fn a_step_down_toward_a_shallower_target_is_not_put_on_trial() {
        let mut explorer = ServerPipelineExplorer::seeded(Some(8), None);
        explorer.note_supports_pipelining(true);
        let now = Instant::now();
        // Big articles on a near server: one request in flight covers the wait.
        explorer.note_latency(Duration::from_millis(100));
        explorer.note_transfer(Duration::from_millis(400));
        assert_eq!(explorer.target_rung(), 2);

        assert_eq!(
            run_window(&mut explorer, now, 100_000, Duration::from_millis(100)),
            Some(RungChange::Stepped { from: 8, to: 4 })
        );
        // Worse throughput at the shallower rung does not bounce it back up:
        // the target, not the comparison, is what moved it.
        assert_eq!(
            run_window(&mut explorer, now, 10_000, Duration::from_millis(100)),
            Some(RungChange::Stepped { from: 4, to: 2 })
        );
        assert_eq!(explorer.current_depth(), 2);
    }

    #[test]
    fn a_server_that_does_not_pipeline_stays_sequential() {
        let mut explorer = ServerPipelineExplorer::default();
        explorer.note_supports_pipelining(false);
        assert_eq!(explorer.choose_mode(true), DownloadLaneMode::Sequential);
        explorer.note_supports_pipelining(true);
        assert_eq!(
            explorer.choose_mode(true),
            DownloadLaneMode::Pipelined { depth: 2 }
        );
        assert_eq!(explorer.choose_mode(false), DownloadLaneMode::Sequential);
    }
}
