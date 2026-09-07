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

/// Responses in each of the first [`RUNG_WARMUP_WINDOWS`] windows.
///
/// A server the lanes have never measured has no article transfer time and so
/// no depth target at all, and thirty-two responses is a long time to wait for
/// one: a job of a few hundred articles can be most of the way finished before
/// the first window closes, which is the whole download running at the seeded
/// rung. Eight is enough for a median body size and a wire rate — the two
/// halves the depth model divides — while costing a fraction of the job.
const RUNG_WARMUP_WINDOW_RESPONSES: u64 = 8;

/// How many short windows a fresh explorer runs: one to measure the link and
/// pick a rung, one to judge the rung it picked. After that the full window
/// applies, because by then the depth is settled and the only question left is
/// whether the link has changed.
const RUNG_WARMUP_WINDOWS: u8 = 2;

/// A rung is only kept when it beats the rung below it by this much; anything
/// less is noise on a shared link.
const RUNG_KEEP_THROUGHPUT_RATIO: f64 = 1.05;

/// How long a server is left alone after a revert or an unclean batch.
const RUNG_HOLD: Duration = Duration::from_secs(10 * 60);

/// Body sizes kept for the median the depth formula divides by.
///
/// A median, not a mean: a file's last article is a fraction of the others and
/// a PAR2 index is a fraction again, and either one drags a mean far enough to
/// ask for a rung the link does not need. Sixteen samples is half a rung window
/// — enough to be steady, short enough to follow a job whose article size
/// genuinely changed.
const BODY_SIZE_SAMPLES: usize = 16;

/// The bandwidth-delay depth: how many BODY requests must be outstanding for a
/// lane to have something arriving for the whole round trip.
///
/// `1 + ceil(rtt / article_transfer)` — one request being served, plus enough
/// queued behind it to cover the wait for the next status line. At the bench's
/// 100 ms round trip with a 750 KiB article taking 25 ms on the wire, that is
/// `1 + ceil(100/25) = 5`, which the ladder rounds up to 8; with an article
/// that takes as long as the round trip it is 2, and pipelining buys almost
/// nothing.
///
/// Returned unclamped so the caller decides which ladder it is walking.
fn bandwidth_delay_depth(rtt: Duration, article_transfer: Duration) -> u32 {
    if article_transfer.is_zero() {
        // An article that costs no measurable time is all round trip.
        return MAX_TARGET_DEPTH;
    }
    let ratio = rtt.as_secs_f64() / article_transfer.as_secs_f64();
    if !ratio.is_finite() || ratio <= 0.0 {
        return MIN_TARGET_DEPTH;
    }
    (ratio.ceil() as u32).saturating_add(1)
}

/// The wire time one article costs at a measured per-lane rate.
///
/// This is the `median body bytes / per-lane rate` half of the depth formula.
/// The rate handed in must be bytes over wire time — the caller's
/// `window_wire_rate_bps` — so that the idle round trips the depth exists to
/// fill are not part of the divisor; deepening a lane then cannot inflate its
/// own target, and a shallow lane cannot talk itself out of the depth it needs.
fn article_transfer_time(median_body_bytes: u64, throughput_bps: f64) -> Option<Duration> {
    if median_body_bytes == 0 || !throughput_bps.is_finite() || throughput_bps <= 0.0 {
        return None;
    }
    let seconds = median_body_bytes as f64 / throughput_bps;
    if !seconds.is_finite() || seconds <= 0.0 {
        return None;
    }
    Some(Duration::from_secs_f64(seconds))
}

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
    /// Set until the explorer has moved to — or found itself already sitting
    /// on — the rung its own measurements ask for. The first move goes
    /// straight there; every later one walks the ladder a rung at a time.
    first_climb_pending: bool,
    /// Short windows left before the full window length applies.
    warmup_windows_left: u8,
    /// Every response folded into this window, at the current rung or below.
    window_responses: u64,
    /// Bytes and wire time of every response in the window, whatever rung it
    /// was issued at. These feed the link model — body size and wire rate —
    /// which describes the link rather than the depth, so a lane still
    /// draining a shallower lease is a perfectly good sample for it.
    window_model_bytes: u64,
    /// The window's wire time alone: the per-article transfer the lane
    /// reported with each response, summed. `window_rung_elapsed` also carries
    /// the status-line waits, which is what the rung trials compare on but
    /// exactly what the depth model must divide out.
    window_wire_elapsed: Duration,
    /// Responses in the window issued at exactly the current rung, and their
    /// bytes and wall clock. Only these may judge a rung against the one below
    /// it: a shallower lane is slower by construction and would argue every
    /// step up back out of existence.
    window_rung_responses: u64,
    window_rung_bytes: u64,
    window_rung_elapsed: Duration,
    /// Ring of recent decoded body sizes; the depth formula takes its median.
    body_bytes: [u64; BODY_SIZE_SAMPLES],
    body_bytes_len: usize,
    body_bytes_next: usize,
    /// `median body bytes / per-lane read rate`, taken as one pair at each
    /// window close and blended across windows. The depth formula's divisor.
    modelled_transfer: Option<Duration>,
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
            first_climb_pending: true,
            warmup_windows_left: RUNG_WARMUP_WINDOWS,
            window_responses: 0,
            window_model_bytes: 0,
            window_wire_elapsed: Duration::ZERO,
            window_rung_responses: 0,
            window_rung_bytes: 0,
            window_rung_elapsed: Duration::ZERO,
            body_bytes: [0; BODY_SIZE_SAMPLES],
            body_bytes_len: 0,
            body_bytes_next: 0,
            modelled_transfer: None,
            hold_until: None,
            last_unclean_at: None,
            persisted_depth: None,
        }
    }
}

impl ServerPipelineExplorer {
    /// Start from what the last run proved, or from what the round trip and a
    /// known article transfer time say the link needs, or from the band of the
    /// connection test's first-byte latency, or from the shallowest pipelined
    /// rung.
    ///
    /// The order is deliberate. A rung a previous run proved on this server is
    /// evidence and outranks any estimate. The bandwidth-delay estimate is the
    /// real answer whenever both of its halves are known — an article transfer
    /// time carried over from a pool that has already run, say — and the
    /// latency band is only the coarse stand-in for when they are not: it says
    /// 2 for everything nearer than 400 ms, which on a 100 ms link carrying
    /// ordinary articles is a quarter of the depth the link can use.
    pub(crate) fn seeded(
        proven_depth: Option<u8>,
        probe_latency: Option<Duration>,
        article_transfer: Option<Duration>,
    ) -> Self {
        let modelled = probe_latency
            .zip(article_transfer)
            .map(|(latency, transfer)| {
                rung_at_or_above(
                    bandwidth_delay_depth(latency, transfer)
                        .clamp(MIN_TARGET_DEPTH, MAX_TARGET_DEPTH) as u8,
                )
            });
        let start = proven_depth
            .map(rung_at_or_above)
            .or(modelled)
            .or_else(|| {
                probe_latency.map(|latency| LatencyBand::from_latency(latency).starting_depth())
            })
            .unwrap_or(2);
        Self {
            current_depth: start.clamp(2, 8),
            latency: probe_latency,
            transfer: article_transfer,
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

    pub(in crate::pipeline) fn note_latency(&mut self, sample: Duration) {
        self.latency = Some(blend(self.latency, sample));
    }

    pub(in crate::pipeline) fn note_transfer(&mut self, sample: Duration) {
        self.transfer = Some(blend(self.transfer, sample));
    }

    pub(super) fn note_supports_pipelining(&mut self, supports_pipelining: bool) {
        self.supports_pipelining = supports_pipelining;
    }

    /// Median of the recent decoded body sizes, or `None` before any landed.
    fn median_body_bytes(&self) -> Option<u64> {
        if self.body_bytes_len == 0 {
            return None;
        }
        let mut samples = self.body_bytes[..self.body_bytes_len].to_vec();
        samples.sort_unstable();
        Some(samples[self.body_bytes_len / 2])
    }

    /// What one article costs on this lane's wire, as the depth formula wants
    /// it: `median body bytes / measured per-lane wire rate`.
    ///
    /// Preferred over the raw per-article EWMA because both halves are taken
    /// from the same closed window of 32 responses, so it does not swing on one
    /// slow article the way a four-deep EWMA does. `None` until a window has
    /// closed.
    ///
    /// The rate is bytes over *wire* time, not over the window's wall clock.
    /// The wall clock includes every status-line wait, and on a shallow pipe
    /// that wait is most of the round trip: dividing by it would fold the
    /// round trip into the divisor and the formula would ask for less depth
    /// than the link needs — precisely at the rung where the answer matters.
    ///
    /// The two halves must come from the same window or the quotient is
    /// meaningless: a job whose articles got smaller and a link that got slower
    /// look identical to a median and a rate blended on different clocks, and
    /// the pair would ask for a deep rung on a link that had just halved.
    pub(crate) fn modelled_article_transfer(&self) -> Option<Duration> {
        self.modelled_transfer
    }

    /// The article transfer time the depth decision runs on: the modelled one
    /// once the link has been measured, and the directly sampled EWMA before
    /// then so a fresh lane is not left without a target for its first window.
    fn effective_article_transfer(&self) -> Option<Duration> {
        self.modelled_article_transfer().or(self.transfer)
    }

    /// Depth that would keep one connection busy across the round trip:
    /// enough requests in flight to cover the wait, plus the one being served.
    ///
    /// See [`bandwidth_delay_depth`]. Clamped to the rungs the ladder actually
    /// runs — `RUNGS` tops out at 8, and the lane code carries any depth the
    /// enum can hold, so nothing here needs to clamp lower.
    pub(crate) fn physical_target_depth(&self) -> Option<u8> {
        let latency = self.latency?;
        let transfer = self.effective_article_transfer()?;
        let depth = bandwidth_delay_depth(latency, transfer);
        Some(depth.clamp(MIN_TARGET_DEPTH, MAX_TARGET_DEPTH) as u8)
    }

    fn note_body_bytes(&mut self, payload_bytes: u64) {
        if payload_bytes == 0 {
            return;
        }
        self.body_bytes[self.body_bytes_next] = payload_bytes;
        self.body_bytes_next = (self.body_bytes_next + 1) % BODY_SIZE_SAMPLES;
        self.body_bytes_len = (self.body_bytes_len + 1).min(BODY_SIZE_SAMPLES);
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
        if !pressure_clear {
            return None;
        }
        // A response from a *deeper* batch than the explorer is running now
        // was issued against a rung that has since been left behind, and
        // nothing it measured describes the rung under test.
        //
        // A *shallower* one is kept. It cannot judge the rung — see
        // `window_rung_responses` — but its body size and wire time describe
        // the link, and throwing it away is what left a freshly started server
        // measuring nothing at all: every lane's first lease is booked before
        // the explorer has a depth, so every response it produces is shallower
        // than the rung the explorer later moved to, and the window that was
        // supposed to pick a depth never filled.
        if observed_depth > self.current_depth {
            return None;
        }
        self.window_responses = self.window_responses.saturating_add(1);
        self.window_model_bytes = self.window_model_bytes.saturating_add(payload_bytes);
        // `transfer` is the lane's status-line-to-terminator time, refreshed
        // just before this call; a lane that has not measured one yet has
        // nothing queued ahead of it, so its whole elapsed is wire time.
        self.window_wire_elapsed = self
            .window_wire_elapsed
            .saturating_add(self.transfer.unwrap_or(policy_elapsed));
        self.note_body_bytes(payload_bytes);
        if observed_depth == self.current_depth {
            self.window_rung_responses = self.window_rung_responses.saturating_add(1);
            self.window_rung_bytes = self.window_rung_bytes.saturating_add(payload_bytes);
            self.window_rung_elapsed = self.window_rung_elapsed.saturating_add(policy_elapsed);
        }
        if self.window_responses < self.window_target_responses() {
            return None;
        }

        // Wall-clock throughput is what the rung trials compare, since a
        // deeper rung's whole gain is the wait it removes; the depth model
        // divides by the wire rate for exactly the opposite reason.
        let throughput = self.window_throughput_bps();
        let wire_rate = self.window_wire_rate_bps();
        if let Some(median) = self.median_body_bytes()
            && let Some(modelled) = article_transfer_time(median, wire_rate)
        {
            self.modelled_transfer = Some(blend(self.modelled_transfer, modelled));
        }
        // A rung is judged only on a window at least half of which was
        // issued at that rung. A window is allowed to fill from shallower
        // leases so that the link model is never starved, but the trial
        // comparison then rests on however many rung responses were mixed
        // in — and one or two of them deciding a `Reverted`, which holds the
        // server down for `RUNG_HOLD`, is a verdict on noise. Below the bar
        // the window still feeds the model and the judgment waits for the
        // next one, by which time the shallower leases have drained.
        let rung_was_measured =
            self.window_rung_responses.saturating_mul(2) >= self.window_target_responses();
        self.reset_window();
        self.warmup_windows_left = self.warmup_windows_left.saturating_sub(1);

        if !rung_was_measured {
            // Most of this window came from leases still draining at a
            // shallower depth. The link model above is worth having either
            // way; the rung comparison has too little in it to compare.
            return None;
        }

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
            // Nothing to move toward. If that is because the measured link
            // already agrees with the rung, rather than because there is no
            // measurement yet, the first climb is over.
            if self.physical_target_depth().is_some() {
                self.first_climb_pending = false;
            }
            return None;
        }
        let from = self.current_depth;
        // The first move goes straight to the rung the bandwidth-delay
        // estimate asks for. Walking there a rung at a time costs a window per
        // rung plus a window to judge each one, and a download can be over
        // before the ladder arrives — which is the same as never pipelining at
        // all. Every later move is a single step, because by then the depth is
        // settled and a change means the link itself moved.
        let to = if self.first_climb_pending {
            target
        } else {
            self.step_toward(target)
        };
        self.first_climb_pending = false;
        if to == from {
            return None;
        }
        self.current_depth = to;
        self.on_trial = to > from;
        Some(RungChange::Stepped { from, to })
    }

    /// Responses this window closes on.
    fn window_target_responses(&self) -> u64 {
        if self.warmup_windows_left > 0 {
            RUNG_WARMUP_WINDOW_RESPONSES
        } else {
            RUNG_WINDOW_RESPONSES
        }
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
        let seconds = self.window_rung_elapsed.as_secs_f64();
        if seconds <= 0.0 {
            return 0.0;
        }
        self.window_rung_bytes as f64 / seconds
    }

    /// Bytes per second of wire time: the rate the depth model divides by.
    fn window_wire_rate_bps(&self) -> f64 {
        let seconds = self.window_wire_elapsed.as_secs_f64();
        if seconds <= 0.0 {
            return 0.0;
        }
        self.window_model_bytes as f64 / seconds
    }

    fn reset_window(&mut self) {
        self.window_responses = 0;
        self.window_model_bytes = 0;
        self.window_wire_elapsed = Duration::ZERO;
        self.window_rung_responses = 0;
        self.window_rung_bytes = 0;
        self.window_rung_elapsed = Duration::ZERO;
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
    pub(in crate::pipeline) servers: HashMap<usize, ServerPipelineExplorer>,
    /// Durable identity of each explorer's pool position, recorded when the
    /// explorer is created. A pool rebuild renumbers the positions, so the
    /// mapping has to be the one that was true while the measurements were
    /// taken — looking it up afterwards resolves against the new pool and
    /// silently throws every measurement away.
    pub(in crate::pipeline) stable_ids: HashMap<usize, u32>,
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

    /// `depth = 1 + ceil(rtt / per-lane article transfer time)`.
    ///
    /// One article's transfer covers one article's worth of the round trip, so
    /// the pipe needs one more request in flight than the number of article
    /// transfers that fit inside the round trip. A link whose articles cost
    /// more than the round trip needs no pipelining at all; a 100 ms link
    /// carrying ~10 ms articles needs eleven, which the ladder caps at eight.
    #[test]
    fn bandwidth_delay_depth_follows_the_ratio_of_rtt_to_article_time() {
        let cases = [
            // (rtt ms, article ms, depth)
            (100, 100, 2),
            (100, 50, 3),
            (100, 34, 4),
            (100, 25, 5),
            (100, 10, 11),
            (200, 100, 3),
            (10, 100, 2),
            (0, 100, 2),
        ];
        for (rtt_ms, article_ms, expected) in cases {
            assert_eq!(
                bandwidth_delay_depth(
                    Duration::from_millis(rtt_ms),
                    Duration::from_millis(article_ms)
                ),
                expected,
                "rtt {rtt_ms}ms over {article_ms}ms articles"
            );
        }
        // An article that costs no measurable time is all round trip.
        assert_eq!(
            bandwidth_delay_depth(Duration::from_millis(100), Duration::ZERO),
            MAX_TARGET_DEPTH
        );
    }

    /// The formula only chooses the trial target; the ladder still bounds it.
    #[test]
    fn physical_target_depth_clamps_the_formula_to_the_rung_ladder() {
        let mut fast_link = explorer(100, 10);
        fast_link.modelled_transfer = Some(Duration::from_millis(10));
        assert_eq!(fast_link.physical_target_depth(), Some(8));

        let mut slow_articles = explorer(100, 400);
        slow_articles.modelled_transfer = Some(Duration::from_millis(400));
        assert_eq!(slow_articles.physical_target_depth(), Some(2));

        let mut unmeasured = ServerPipelineExplorer::default();
        unmeasured.note_supports_pipelining(true);
        assert_eq!(
            unmeasured.physical_target_depth(),
            None,
            "no latency sample means no target at all"
        );
    }

    /// The two halves of the quotient have to come from the same window.
    #[test]
    fn article_transfer_time_divides_median_bytes_by_the_measured_rate() {
        assert_eq!(
            article_transfer_time(750_000, 7_500_000.0),
            Some(Duration::from_millis(100))
        );
        assert_eq!(article_transfer_time(0, 7_500_000.0), None);
        assert_eq!(article_transfer_time(750_000, 0.0), None);
        assert_eq!(article_transfer_time(750_000, f64::NAN), None);
        assert_eq!(article_transfer_time(750_000, -1.0), None);
    }

    /// A sequential lane's wall clock is one round trip plus one article per
    /// response. The model must divide by the article, not the sum: at 100 ms
    /// over 25 ms articles the link needs depth 5 (rung 8), and a divisor that
    /// kept the round trip would have said 2.
    #[test]
    fn modelled_transfer_excludes_the_status_line_wait() {
        let mut explorer = explorer(100, 25);
        let now = Instant::now();
        // 750 KB in 125 ms of wall clock per response, all at rung 1.
        run_window(&mut explorer, now, 750_000, Duration::from_millis(125));
        let modelled = explorer
            .modelled_article_transfer()
            .expect("a closed window models the article");
        assert!(
            (Duration::from_millis(24)..=Duration::from_millis(26)).contains(&modelled),
            "modelled {modelled:?} should be the wire time, not the wall clock"
        );
        assert_eq!(explorer.physical_target_depth(), Some(5));
        assert_eq!(explorer.target_rung(), 8);
    }

    /// The median is what keeps a file's short tail article and a PAR2 index
    /// from moving the modelled article size.
    #[test]
    fn median_body_bytes_ignores_the_odd_short_article() {
        let mut explorer = ServerPipelineExplorer::default();
        assert_eq!(explorer.median_body_bytes(), None);
        for bytes in [750_000, 750_000, 750_000, 12_000, 750_000] {
            explorer.note_body_bytes(bytes);
        }
        assert_eq!(explorer.median_body_bytes(), Some(750_000));
    }

    /// Feed exactly one window at the explorer's current rung, whichever
    /// length that window is.
    fn run_window(
        explorer: &mut ServerPipelineExplorer,
        now: Instant,
        bytes_per_response: u64,
        elapsed_per_response: Duration,
    ) -> Option<RungChange> {
        let depth = explorer.current_depth();
        let mut change = None;
        for _ in 0..explorer.window_target_responses() {
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

    /// The first decision goes straight to the rung the measured link asks
    /// for.
    ///
    /// This test used to assert a single rung of movement — `2 -> 4` on a link
    /// whose target is 8 — which is the behaviour that left a short download
    /// running most of its articles at the seeded depth: each further rung
    /// cost a window to step and another to judge. The ladder's caution is
    /// still there, in the trial that follows and in every later change; what
    /// moved is only how the explorer arrives at its first rung.
    #[test]
    fn the_first_window_moves_straight_to_the_bandwidth_delay_rung() {
        let mut explorer = explorer(300, 50);
        let now = Instant::now();
        assert_eq!(explorer.current_depth(), 2);
        assert_eq!(explorer.target_rung(), 8);

        let change = run_window(&mut explorer, now, 100_000, Duration::from_millis(100));
        assert_eq!(change, Some(RungChange::Stepped { from: 2, to: 8 }));
        assert_eq!(explorer.current_depth(), 8);
        assert_eq!(
            explorer.choose_mode(true),
            DownloadLaneMode::Pipelined { depth: 8 }
        );
    }

    /// Later changes still walk the ladder a rung at a time.
    #[test]
    fn a_later_change_steps_one_rung_at_a_time() {
        let mut explorer = explorer(300, 200);
        let now = Instant::now();
        // Articles as long as two thirds of the round trip: the link wants 4,
        // which is where the first decision puts it.
        assert_eq!(explorer.target_rung(), 4);
        assert_eq!(
            run_window(&mut explorer, now, 100_000, Duration::from_millis(100)),
            Some(RungChange::Stepped { from: 2, to: 4 })
        );
        // Keep the rung, then let the articles shrink so the target moves out
        // to 8. The climb is over, so the explorer steps rather than jumps.
        assert_eq!(
            run_window(&mut explorer, now, 200_000, Duration::from_millis(100)),
            Some(RungChange::Kept { depth: 4 })
        );
        explorer.modelled_transfer = Some(Duration::from_millis(20));
        assert_eq!(explorer.target_rung(), 8);
        assert_eq!(
            run_window(&mut explorer, now, 300_000, Duration::from_millis(100)),
            Some(RungChange::Stepped { from: 4, to: 8 })
        );
    }

    #[test]
    fn a_step_up_that_pays_for_itself_is_kept() {
        let mut explorer = explorer(300, 50);
        let now = Instant::now();
        run_window(&mut explorer, now, 100_000, Duration::from_millis(100));
        assert_eq!(explorer.current_depth(), 8);

        // Twice the bytes in the same time: comfortably past the 1.05 bar.
        let change = run_window(&mut explorer, now, 200_000, Duration::from_millis(100));
        assert_eq!(change, Some(RungChange::Kept { depth: 8 }));
        assert_eq!(explorer.current_depth(), 8);
    }

    #[test]
    fn a_step_up_that_does_not_pay_reverts_and_holds() {
        let mut explorer = explorer(300, 50);
        let now = Instant::now();
        run_window(&mut explorer, now, 100_000, Duration::from_millis(100));
        assert_eq!(explorer.current_depth(), 8);

        let change = run_window(&mut explorer, now, 101_000, Duration::from_millis(100));
        assert_eq!(change, Some(RungChange::Reverted { from: 8, to: 2 }));
        assert_eq!(explorer.current_depth(), 2);

        // Inside the hold the explorer leaves the server alone.
        let change = run_window(&mut explorer, now, 100_000, Duration::from_millis(100));
        assert_eq!(change, None);
        assert_eq!(explorer.current_depth(), 2);

        // Past the hold it is free to try again — a rung at a time now, since
        // the first climb is behind it.
        let later = now + RUNG_HOLD + Duration::from_secs(1);
        let change = run_window(&mut explorer, later, 100_000, Duration::from_millis(100));
        assert_eq!(change, Some(RungChange::Stepped { from: 2, to: 4 }));
    }

    #[test]
    fn an_unclean_batch_drops_a_rung_and_a_second_one_pins_sequential() {
        let mut explorer = explorer(300, 50);
        let now = Instant::now();
        run_window(&mut explorer, now, 100_000, Duration::from_millis(100));
        assert_eq!(explorer.current_depth(), 8);

        assert_eq!(
            explorer.note_unclean_batch(now),
            Some(RungChange::Dropped { from: 8, to: 4 })
        );
        assert_eq!(explorer.current_depth(), 4);

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
        assert_eq!(change, Some(RungChange::Stepped { from: 2, to: 8 }));

        // One short of a window buys nothing more.
        let short_of_a_window = explorer.window_target_responses() - 1;
        for _ in 0..short_of_a_window {
            assert_eq!(
                explorer.note_response(now, 8, 400_000, Duration::from_millis(100), true),
                None
            );
        }
        assert_eq!(explorer.current_depth(), 8);
    }

    #[test]
    fn a_seeded_explorer_starts_at_the_proven_rung_or_the_probe_band() {
        assert_eq!(
            ServerPipelineExplorer::seeded(Some(8), None, None).current_depth(),
            8
        );
        assert_eq!(
            ServerPipelineExplorer::seeded(None, Some(Duration::from_millis(900)), None)
                .current_depth(),
            8
        );
        assert_eq!(
            ServerPipelineExplorer::seeded(None, Some(Duration::from_millis(500)), None)
                .current_depth(),
            4
        );
        assert_eq!(
            ServerPipelineExplorer::seeded(None, Some(Duration::from_millis(50)), None)
                .current_depth(),
            2
        );
        assert_eq!(
            ServerPipelineExplorer::seeded(None, None, None).current_depth(),
            2
        );
        // A proven depth off the ladder rounds up to a rung that is actually run.
        assert_eq!(
            ServerPipelineExplorer::seeded(Some(3), None, None).current_depth(),
            4
        );
        // A proven rung outranks the probe band; the probe only fills the gap.
        assert_eq!(
            ServerPipelineExplorer::seeded(Some(2), Some(Duration::from_millis(900)), None)
                .current_depth(),
            2
        );
    }

    /// A carried-over article transfer time turns the seed into the real
    /// bandwidth-delay answer, which the coarse band cannot reach: 100 ms is
    /// deep inside the "good" band, and the band would start such a link two
    /// deep when it can use eight.
    #[test]
    fn a_seeded_explorer_prefers_the_bandwidth_delay_rung_over_the_band() {
        let seeded = ServerPipelineExplorer::seeded(
            None,
            Some(Duration::from_millis(100)),
            Some(Duration::from_millis(25)),
        );
        assert_eq!(seeded.current_depth(), 8);
        assert_eq!(seeded.physical_target_depth(), Some(5));

        // Articles that cost as much as the round trip need no depth, and the
        // band would have said the same thing for the wrong reason.
        assert_eq!(
            ServerPipelineExplorer::seeded(
                None,
                Some(Duration::from_millis(100)),
                Some(Duration::from_millis(100)),
            )
            .current_depth(),
            2
        );

        // A rung a previous run proved still outranks the estimate.
        assert_eq!(
            ServerPipelineExplorer::seeded(
                Some(2),
                Some(Duration::from_millis(100)),
                Some(Duration::from_millis(25)),
            )
            .current_depth(),
            2
        );
    }

    /// The first leases every lane takes are booked before any response has
    /// come back, so they run at whatever depth the seed chose. Discarding
    /// their responses — which is what an exact-depth match does the moment the
    /// explorer moves — left the window that is supposed to pick a depth
    /// unable to fill from the lanes still draining those leases.
    #[test]
    fn responses_from_a_shallower_lease_still_fill_the_window() {
        let mut deep = ServerPipelineExplorer::seeded(Some(8), None, None);
        deep.note_supports_pipelining(true);
        deep.note_latency(Duration::from_millis(100));
        deep.note_transfer(Duration::from_millis(25));
        let now = Instant::now();

        // Every response comes from a lease booked two deep while the explorer
        // is already running eight.
        for _ in 0..deep.window_target_responses() {
            assert_eq!(
                deep.note_response(now, 2, 750_000, Duration::from_millis(125), true),
                None,
                "a shallower lease may fill the window but may not judge the rung"
            );
        }
        // The link model is built from them all the same.
        assert_eq!(
            deep.modelled_article_transfer(),
            Some(Duration::from_millis(25))
        );
        assert_eq!(deep.current_depth(), 8);

        // A batch from a rung the explorer has already left behind is still
        // thrown away entirely.
        let mut shallow = explorer(300, 50);
        for _ in 0..RUNG_WINDOW_RESPONSES * 2 {
            assert_eq!(
                shallow.note_response(now, 8, 100_000, Duration::from_millis(100), true),
                None
            );
        }
        assert_eq!(shallow.current_depth(), 2);
    }

    /// The opening windows are short on purpose: a server with no measurement
    /// has no depth target at all, and a download of a few hundred articles
    /// would otherwise spend most of itself waiting for the first window.
    #[test]
    fn the_opening_windows_are_short_and_then_the_full_window_applies() {
        let mut explorer = explorer(300, 50);
        assert_eq!(explorer.window_target_responses(), 8);
        let now = Instant::now();

        run_window(&mut explorer, now, 100_000, Duration::from_millis(100));
        assert_eq!(explorer.window_target_responses(), 8);
        run_window(&mut explorer, now, 200_000, Duration::from_millis(100));
        assert_eq!(explorer.window_target_responses(), RUNG_WINDOW_RESPONSES);
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
        assert_eq!(explorer.current_depth(), 8);
        // The step up is on trial; the caller must not write it through yet,
        // but the value it would write is the one under test. One window that
        // keeps the rung is enough — a proven depth is worth saving as soon as
        // it is proven, not several windows later.
        run_window(&mut explorer, now, 200_000, Duration::from_millis(100));
        assert_eq!(explorer.take_persist_request(), Some(8));
        assert_eq!(explorer.take_persist_request(), None);

        // Losing the rung offers the lower value, so a restart cannot resume
        // at a depth this server has since failed.
        explorer.note_unclean_batch(now);
        assert_eq!(explorer.current_depth(), 4);
        assert_eq!(explorer.take_persist_request(), Some(4));
    }

    /// A server seeded deep from a previous run keeps that rung: the explorer
    /// has nothing to walk toward until it has measured the link itself.
    #[test]
    fn a_seeded_rung_is_held_until_the_explorer_has_its_own_measurements() {
        let mut explorer = ServerPipelineExplorer::seeded(Some(8), None, None);
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
        let mut explorer = ServerPipelineExplorer::seeded(Some(8), None, None);
        explorer.note_supports_pipelining(true);
        let now = Instant::now();
        // Big articles on a near server: one request in flight covers the wait.
        explorer.note_latency(Duration::from_millis(100));
        explorer.note_transfer(Duration::from_millis(400));
        assert_eq!(explorer.target_rung(), 2);

        // The first decision goes to the target in one move — down as well as
        // up. This used to walk 8 -> 4 -> 2 over two windows.
        assert_eq!(
            run_window(&mut explorer, now, 100_000, Duration::from_millis(100)),
            Some(RungChange::Stepped { from: 8, to: 2 })
        );
        assert_eq!(explorer.current_depth(), 2);
        // Worse throughput at the shallower rung does not bounce it back up:
        // the target, not the comparison, is what moved it, so there is no
        // trial to lose.
        assert_eq!(
            run_window(&mut explorer, now, 10_000, Duration::from_millis(100)),
            None
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
