// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::HashMap;
use std::time::{Duration, Instant};

// Latency bands. A band picks the starting rung, labels a server in the UI,
// and labels a connection test. It never gates a depth on its own: the depth
// comes from the measured latency/transfer ratio.
pub(crate) const LATENCY_BAND_GOOD_MAX: Duration = Duration::from_millis(400);
pub(crate) const LATENCY_BAND_MODERATE_MAX: Duration = Duration::from_millis(800);

// Probe latency from which a server with nothing proven opens four deep
// inside the good band, which otherwise opens two deep.
//
// A 100 ms link carrying ordinary articles needs 4: pinned at 4 it downloads
// as fast as pinned at 8, and an explorer that opens at 2 cannot move before
// its opening window closes, by which time connect, login and the first
// articles have all run at 2. Below this a link is near enough that 2 covers
// the round trip.
const OPEN_FOUR_DEEP_MIN_LATENCY: Duration = Duration::from_millis(50);

// Depths the explorer will actually run. `1` is sequential; a failure drops
// off the bottom of the pipelined rungs onto it.
const RUNGS: [u8; 4] = [1, 2, 4, 8];
const MIN_TARGET_DEPTH: u32 = 2;
const MAX_TARGET_DEPTH: u32 = 8;
// Requests added to the bandwidth-delay depth before the explorer picks the
// rung it walks toward; see `target_rung`.
const TARGET_HEADROOM_DEPTH: u8 = 1;

// Clean responses observed at one rung before the explorer acts on what it
// measured there. Also bounds rung changes to one per window.
const RUNG_WINDOW_RESPONSES: u64 = 32;

// Responses in each of the first [`RUNG_WARMUP_WINDOWS`] windows.
//
// A server the lanes have never measured has no article transfer time and so
// no depth target at all, and thirty-two responses is a long time to wait for
// one: a job of a few hundred articles can be most of the way finished before
// the first window closes, which is the whole download running at the seeded
// rung. Eight is enough for a median body size and a wire rate — the two
// halves the depth model divides — while costing a fraction of the job.
const RUNG_WARMUP_WINDOW_RESPONSES: u64 = 8;

// How many short windows a fresh explorer runs: one to measure the link and
// move to the rung it asks for, then two more for the rung to be settled or
// judged. After that the full window applies, because by then the depth is
// settled and the only question left is whether the link has changed.
const RUNG_WARMUP_WINDOWS: u8 = 3;

// Status-line waits held before their minimum folds into the round-trip
// estimate, independently of the rung window. Matches the short warm-up
// window, so a server whose rung window never closes refreshes its round
// trip about as often as one whose window is still short.
const LATENCY_FOLD_SAMPLES: u32 = RUNG_WARMUP_WINDOW_RESPONSES as u32;

// How far apart two windows' throughput may be and still count as the same
// on a shared link. A step up is reverted only when it falls short of the
// rung below it by more than this; a trial inside the band is no worse than
// its baseline and is kept. On a link-bound server every rung reads the same
// throughput, so a bar the trial had to clear upward would revert a rung that
// cost nothing.
const RUNG_THROUGHPUT_NOISE_RATIO: f64 = 1.05;

// How long a server is left alone after a revert or an unclean batch.
const RUNG_HOLD: Duration = Duration::from_secs(10 * 60);

// Unclean batches on one server that come this soon after the one that cost
// a rung are the same event and count once. A burst of connection resets
// tears down every lane of the server within a second or two, and each lane
// reports its own unclean batch.
pub(crate) const UNCLEAN_BURST_WINDOW: Duration = Duration::from_secs(30);

// How long a server stays pinned to sequential before it is given one trial
// at depth 2. Each unclean trial pins it again for twice as long, up to
// `PIN_HOLD_MAX`.
pub(crate) const PIN_HOLD: Duration = RUNG_HOLD;
pub(crate) const PIN_HOLD_MAX: Duration = Duration::from_secs(2 * 60 * 60);

// Clean, complete pipelined batches at depth 2 that end a lapsed pin's
// trial and lift the pin. Counted per batch, not per rung window, so a
// server that gets only a few articles a job still finishes its trial.
pub(crate) const PIN_TRIAL_CLEAN_BATCHES: u8 = 4;

// The depth the one trial lane runs at: the shallowest pipelined rung.
const PIN_TRIAL_DEPTH: u8 = RUNGS[1];

// A server that goes this long after its pin was lifted without being
// pinned again starts the back-off over at `PIN_HOLD`: it has run clean for
// as long as the longest pin would have held it. Until then the lift is
// provisional, and an unclean batch pins it again at the next step.
pub(crate) const PIN_BACKOFF_RESET_AFTER: Duration = PIN_HOLD_MAX;

// The hold of the pin that follows `strikes` earlier ones in the same
// back-off.
fn pin_hold(strikes: u32) -> Duration {
    PIN_HOLD
        .saturating_mul(1u32 << strikes.min(16))
        .min(PIN_HOLD_MAX)
}

// Body sizes kept for the median the depth formula divides by.
//
// A median, not a mean: a file's last article is a fraction of the others and
// a PAR2 index is a fraction again, and either one drags a mean far enough to
// ask for a rung the link does not need. Sixteen samples is half a rung window
// — enough to be steady, short enough to follow a job whose article size
// genuinely changed.
const BODY_SIZE_SAMPLES: usize = 16;

// The bandwidth-delay depth: how many BODY requests must be outstanding for a
// lane to have something arriving for the whole round trip.
//
// `1 + ceil(rtt / article_transfer)` — one request being served, plus enough
// queued behind it to cover the wait for the next status line. At the bench's
// 100 ms round trip with a 750 KiB article taking 25 ms on the wire, that is
// `1 + ceil(100/25) = 5`, which the ladder rounds up to 8; with an article
// that takes as long as the round trip it is 2, and pipelining buys almost
// nothing.
//
// Returned unclamped so the caller decides which ladder it is walking.
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

// The wire time one article costs at a measured per-lane rate.
//
// This is the `median body bytes / per-lane rate` half of the depth formula.
// The rate handed in must be bytes over wire time — the caller's
// `window_wire_rate_bps` — so that the idle round trips the depth exists to
// fill are not part of the divisor; deepening a lane then cannot inflate its
// own target, and a shallow lane cannot talk itself out of the depth it needs.
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

    // Where a server with no measurement of its own starts. A further-away
    // server has more round trip to hide, so it starts deeper. The good band
    // opens deeper from [`OPEN_FOUR_DEEP_MIN_LATENCY`]; see
    // [`guessed_opening_depth`].
    pub(crate) fn starting_depth(self) -> u8 {
        match self {
            Self::Good => 2,
            Self::Moderate => 4,
            Self::Slow => 8,
        }
    }
}

// The rung a server opens at when the probe's latency is all there is to go
// on: the band's starting depth, and 4 for a good-band link from
// [`OPEN_FOUR_DEEP_MIN_LATENCY`] up.
fn guessed_opening_depth(latency: Duration) -> u8 {
    let band_depth = LatencyBand::from_latency(latency).starting_depth();
    if latency >= OPEN_FOUR_DEEP_MIN_LATENCY {
        band_depth.max(4)
    } else {
        band_depth
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum LaneParkReason {
    NoWork,
    Pressure,
    ProbeYield,
    Capacity,
    ServerQuota,
    Error,
}

// What the explorer decided at the end of a window, for the one log line a
// rung change is allowed to write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RungChange {
    // Moved one rung toward the physical target.
    Stepped { from: u8, to: u8 },
    // The step up paid for itself and is now the baseline.
    Kept { depth: u8 },
    // The step up did not pay for itself; back one rung and hold.
    Reverted { from: u8, to: u8 },
    // An unclean pipelined batch cost a rung.
    Dropped { from: u8, to: u8 },
    // A second unclean batch inside the hold, or an unclean trial after a
    // pin lapsed: the server is sequential for `hold`.
    PinnedSequential { hold: Duration },
    // A clean trial after a pin lapsed: the server pipelines again.
    PinLifted { depth: u8 },
}

impl RungChange {
    // Whether the verdict leaves the server on a rung worth writing through
    // to the servers table. A step up is not: it is either on trial or, as a
    // first climb taken on the model alone, waiting for the window that
    // settles it, and a restart would resume at a depth nothing has yet paid
    // for. Every other outcome is a settled verdict, including the ones that
    // lower the rung — a persisted depth that over-promises is the one worth
    // correcting. A pin is one of those: it writes depth 1, so a restart
    // opens the server at the shallowest pipelined rung and not at the rung
    // that just failed twice.
    pub(crate) fn settles_the_rung(self) -> bool {
        !matches!(self, Self::Stepped { from, to } if to > from)
    }
}

fn rung_index(depth: u8) -> usize {
    RUNGS
        .iter()
        .position(|rung| *rung == depth)
        .unwrap_or_else(|| RUNGS.iter().filter(|rung| **rung <= depth).count().max(1) - 1)
}

// The lowest rung that covers `depth`, so a physical target of 3 asks for 4.
fn rung_at_or_above(depth: u8) -> u8 {
    RUNGS
        .iter()
        .copied()
        .find(|rung| *rung >= depth)
        .unwrap_or(*RUNGS.last().expect("rung ladder is not empty"))
}

// Per-server depth explorer.
//
// It holds two measurements the lanes take apart for it — how long a request
// waits for its status line, and how long the article itself takes on the
// wire — and walks the rung ladder toward the depth that would keep the link
// busy, keeping a rung only when it measurably paid for itself.
#[derive(Debug, Clone)]
pub(crate) struct ServerPipelineExplorer {
    supports_pipelining: bool,
    pinned_sequential: bool,
    latency: Option<Duration>,
    transfer: Option<Duration>,
    current_depth: u8,
    // Rung the current window is being compared against, and its throughput.
    baseline_depth: Option<u8>,
    baseline_throughput_bps: Option<f64>,
    // Set while the current rung is a step up still on trial.
    on_trial: bool,
    // Set until the explorer has moved to — or found itself already sitting
    // on — the rung its own measurements ask for. The first move goes
    // straight there; every later one walks the ladder a rung at a time.
    first_climb_pending: bool,
    // The first climb is judged by a trial like any later one rather than
    // taken on the model alone. Set when the servers table held a rung for
    // this server when the explorer was seeded, and when a guessed opening
    // rung was dropped: either way the server has a record the model alone
    // does not get to overrule.
    first_climb_is_a_trial: bool,
    // The opening rung came from the probe's latency band alone — no proven
    // rung, no complete model — and is deeper than 2. While set, an unclean
    // batch drops the rung without a hold and without counting toward a pin
    // to sequential: the explorer never measured that rung, so failing it
    // says nothing about the rung it would have chosen. Cleared by the first
    // measured, fed window that closes at the rung, or by the drop.
    opened_on_a_guess: bool,
    // Set after the first climb moved to the modelled rung without a trial,
    // until a clean window at that rung settles it as the baseline.
    settle_pending: bool,
    // Short windows left before the full window length applies.
    warmup_windows_left: u8,
    // Every response folded into this window, at the current rung or below.
    window_responses: u64,
    // Shortest status-line wait any lane measured on a request it issued
    // alone during this window, split by whether the connection had already
    // delivered an article before. The minimum is the round trip; the mean of
    // the same samples carries server think time, a re-authentication after
    // an idle spell, or whatever else queued ahead of the status line, and
    // each of those asks for depth the link cannot use.
    window_latency_warm_min: Option<Duration>,
    window_latency_cold_min: Option<Duration>,
    // Status-line waits held since the last fold, warm and cold.
    window_latency_samples: u32,
    // Bytes and wire time of every response in the window, whatever rung it
    // was issued at. These feed the link model — body size and wire rate —
    // which describes the link rather than the depth, so a lane still
    // draining a shallower lease is a perfectly good sample for it.
    window_model_bytes: u64,
    // The window's wire time alone: the per-article transfer the lane
    // reported with each response, summed. `window_rung_elapsed` also carries
    // the status-line waits, which is what the rung trials compare on but
    // exactly what the depth model must divide out.
    window_wire_elapsed: Duration,
    // Responses in the window issued at exactly the current rung, and their
    // bytes and wall clock. Only these may judge a rung against the one below
    // it: a shallower lane is slower by construction and would argue every
    // step up back out of existence.
    window_rung_responses: u64,
    window_rung_bytes: u64,
    window_rung_elapsed: Duration,
    // A response at the rung arrived with its lane holding less than a
    // full rung of articles: the lane waited on its next lease, and the
    // window timed that wait as well as the server.
    window_starved: bool,
    // Ring of recent decoded body sizes; the depth formula takes its median.
    body_bytes: [u64; BODY_SIZE_SAMPLES],
    body_bytes_len: usize,
    body_bytes_next: usize,
    // `median body bytes / per-lane read rate`, taken as one pair at each
    // window close and blended across windows. The depth formula's divisor.
    modelled_transfer: Option<Duration>,
    hold_until: Option<Instant>,
    // The last unclean batch that counted toward a pin: the one that cost a
    // held rung or pinned. A second one inside `RUNG_HOLD` of it pins.
    last_unclean_at: Option<Instant>,
    // The last unclean batch acted on in any way, a guessed rung's drop
    // included. Batches inside `UNCLEAN_BURST_WINDOW` after it do not count.
    unclean_burst_at: Option<Instant>,
    // When the current pin lapses into a trial. Kept through the trial, so a
    // pool rebuilt during one carries a pin that is already due.
    pinned_until: Option<Instant>,
    // Pins in the current back-off; the next pin holds `pin_hold(strikes)`.
    pin_strikes: u32,
    // The server is on the one trial a lapsed pin gets: one lane runs at
    // depth 2 and every other lane stays sequential, so `current_depth`
    // stays 1 until the trial is judged.
    pin_trial: bool,
    // The lane running the trial. `None` until a lane asks for its mode
    // after the lapse, and taken over by the next lane to ask once this one
    // is gone.
    pin_trial_lane: Option<u64>,
    // Clean batches at depth 2 counted toward `PIN_TRIAL_CLEAN_BATCHES` on
    // the current trial.
    pin_trial_clean_batches: u8,
    // When a clean trial last lifted a pin.
    pin_lifted_at: Option<Instant>,
    // Depth the servers table already holds, so a kept rung only writes once.
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
            first_climb_is_a_trial: false,
            opened_on_a_guess: false,
            settle_pending: false,
            warmup_windows_left: RUNG_WARMUP_WINDOWS,
            window_responses: 0,
            window_latency_warm_min: None,
            window_latency_cold_min: None,
            window_latency_samples: 0,
            window_model_bytes: 0,
            window_wire_elapsed: Duration::ZERO,
            window_rung_responses: 0,
            window_rung_bytes: 0,
            window_rung_elapsed: Duration::ZERO,
            window_starved: false,
            body_bytes: [0; BODY_SIZE_SAMPLES],
            body_bytes_len: 0,
            body_bytes_next: 0,
            modelled_transfer: None,
            hold_until: None,
            last_unclean_at: None,
            unclean_burst_at: None,
            pinned_until: None,
            pin_strikes: 0,
            pin_trial: false,
            pin_trial_lane: None,
            pin_trial_clean_batches: 0,
            pin_lifted_at: None,
            persisted_depth: None,
        }
    }
}

impl ServerPipelineExplorer {
    // Start from what the last run proved, or from what the round trip and a
    // known article transfer time say the link needs, or from the band of the
    // connection test's first-byte latency, or from the shallowest pipelined
    // rung.
    //
    // The order is deliberate. A rung a previous run proved on this server is
    // evidence and outranks any estimate. The bandwidth-delay estimate is the
    // real answer whenever both of its halves are known — an article transfer
    // time carried over from a pool that has already run, say — and the
    // latency band is only the coarse stand-in for when they are not: see
    // `guessed_opening_depth`. A rung opened on that guess alone is marked,
    // so that failing it costs no hold; see `opened_on_a_guess`.
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
        let measured = proven_depth.map(rung_at_or_above).or(modelled);
        let guessed = probe_latency.map(guessed_opening_depth);
        let start = measured.or(guessed).unwrap_or(2).clamp(2, 8);
        Self {
            current_depth: start,
            latency: probe_latency,
            transfer: article_transfer,
            persisted_depth: proven_depth,
            first_climb_is_a_trial: proven_depth.is_some(),
            opened_on_a_guess: measured.is_none() && guessed.is_some() && start > 2,
            ..Self::default()
        }
    }

    // Carry what `prior`, this server's explorer in the pool this one
    // replaces, had ruled about pipelining: a server pinned sequential stays
    // pinned and at depth 1 until the same lapse, with the same back-off, and
    // a hold after an unclean batch keeps running, so a rebuilt pool neither
    // pipelines to a server that failed it twice nor shows a depth the lanes
    // are held off. A server on its trial is carried pinned with its lapse
    // already due, so its next response starts the trial again, on whichever
    // lane of the new pool asks first.
    pub(super) fn carry_pipelining_verdict(&mut self, prior: &Self) {
        self.last_unclean_at = prior.last_unclean_at;
        self.unclean_burst_at = prior.unclean_burst_at;
        self.hold_until = prior.hold_until;
        self.pin_strikes = prior.pin_strikes;
        self.pin_lifted_at = prior.pin_lifted_at;
        if prior.pinned_sequential || prior.pin_trial {
            self.pinned_sequential = true;
            self.pinned_until = prior.pinned_until;
            self.current_depth = 1;
        }
    }

    // The depth to write through, if the proven rung has moved since the last
    // write. Consumed, so a rung held across many windows writes once.
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

    // The server is on the one trial a lapsed pin gets.
    #[cfg(test)]
    pub(crate) fn on_pin_trial(&self) -> bool {
        self.pin_trial
    }

    // The lane running the trial, if one has taken it.
    #[cfg(test)]
    pub(crate) fn pin_trial_lane(&self) -> Option<u64> {
        self.pin_trial_lane
    }

    // A pipelining server whose pin has lapsed into its trial.
    #[cfg(test)]
    pub(crate) fn on_a_lapsed_pin_trial(now: Instant) -> Self {
        let mut explorer = Self::seeded(Some(2), None, None);
        explorer.note_supports_pipelining(true);
        explorer.pin(now);
        explorer.begin_pin_trial();
        explorer
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

    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub(crate) fn pinned_sequential(&self) -> bool {
        self.pinned_sequential
    }

    // What the health view reports as the pin: held until a clean trial
    // lifts it, so a server on its trial still reads pinned.
    pub(crate) fn pin_held(&self) -> bool {
        self.pinned_sequential || self.pin_trial
    }

    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub(in crate::pipeline) fn note_latency(&mut self, sample: Duration) {
        self.latency = Some(blend(self.latency, sample));
    }

    // One status-line wait from a request a lane issued with nothing queued
    // ahead of it. `cold` marks the connection's first article: that wait
    // carries the handshake's tail — TLS session setup, authentication, the
    // group probe — and none of it recurs once the connection is warm.
    //
    // Samples are held and only their minimum reaches the round-trip
    // estimate; see `window_latency_warm_min`. They fold when the rung window
    // closes, and also every [`LATENCY_FOLD_SAMPLES`] samples on their own,
    // so a server pinned sequential or held under pressure — whose rung
    // window never closes — still keeps its round trip current.
    pub(in crate::pipeline) fn note_latency_sample(&mut self, sample: Duration, cold: bool) {
        let slot = if cold {
            &mut self.window_latency_cold_min
        } else {
            &mut self.window_latency_warm_min
        };
        *slot = Some(slot.map_or(sample, |current| current.min(sample)));
        self.window_latency_samples = self.window_latency_samples.saturating_add(1);
        if self.window_latency_samples >= LATENCY_FOLD_SAMPLES {
            self.fold_window_latency();
        }
    }

    // Fold the shortest status-line wait held so far into the round-trip
    // estimate and start holding afresh. A warm sample always wins; a cold
    // one is only ever a stand-in for a server that has not yet measured a
    // round trip at all, so a handshake never drags an estimate the warm
    // lanes have already settled.
    fn fold_window_latency(&mut self) {
        if let Some(warm) = self.window_latency_warm_min {
            self.latency = Some(blend(self.latency, warm));
        } else if self.latency.is_none() {
            self.latency = self.window_latency_cold_min;
        }
        self.window_latency_warm_min = None;
        self.window_latency_cold_min = None;
        self.window_latency_samples = 0;
    }

    pub(in crate::pipeline) fn note_transfer(&mut self, sample: Duration) {
        self.transfer = Some(blend(self.transfer, sample));
    }

    pub(super) fn note_supports_pipelining(&mut self, supports_pipelining: bool) {
        self.supports_pipelining = supports_pipelining;
    }

    // Median of the recent decoded body sizes, or `None` before any landed.
    fn median_body_bytes(&self) -> Option<u64> {
        if self.body_bytes_len == 0 {
            return None;
        }
        let mut samples = self.body_bytes[..self.body_bytes_len].to_vec();
        samples.sort_unstable();
        Some(samples[self.body_bytes_len / 2])
    }

    // What one article costs on this lane's wire, as the depth formula wants
    // it: `median body bytes / measured per-lane wire rate`.
    //
    // Preferred over the raw per-article EWMA because both halves are taken
    // from the same closed window of 32 responses, so it does not swing on one
    // slow article the way a four-deep EWMA does. `None` until a window has
    // closed.
    //
    // The rate is bytes over *wire* time, not over the window's wall clock.
    // The wall clock includes every status-line wait, and on a shallow pipe
    // that wait is most of the round trip: dividing by it would fold the
    // round trip into the divisor and the formula would ask for less depth
    // than the link needs — precisely at the rung where the answer matters.
    //
    // The two halves must come from the same window or the quotient is
    // meaningless: a job whose articles got smaller and a link that got slower
    // look identical to a median and a rate blended on different clocks, and
    // the pair would ask for a deep rung on a link that had just halved.
    pub(crate) fn modelled_article_transfer(&self) -> Option<Duration> {
        self.modelled_transfer
    }

    // The article transfer time the depth decision runs on: the modelled one
    // once the link has been measured, and the directly sampled EWMA before
    // then so a fresh lane is not left without a target for its first window.
    fn effective_article_transfer(&self) -> Option<Duration> {
        self.modelled_article_transfer().or(self.transfer)
    }

    // Depth that would keep one connection busy across the round trip:
    // enough requests in flight to cover the wait, plus the one being served.
    //
    // See [`bandwidth_delay_depth`]. Clamped to the rungs the ladder actually
    // runs — `RUNGS` tops out at 8, and the lane code carries any depth the
    // enum can hold, so nothing here needs to clamp lower.
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

    // The rung the explorer is walking toward: the bandwidth-delay depth plus
    // one request of headroom, rounded up to a rung, except when the depth
    // is exactly the current rung, which is then the target as it stands.
    //
    // The formula is exact only for the round trip and transfer it was given,
    // and both move window to window. At 100 ms over 48 ms articles it says
    // 4, and a window that reads the wire a little faster says 5; without
    // headroom the target flips between rungs 4 and 8 from run to run. One
    // more request covers the swing, both on the way up (2 reading 4 or 5
    // asks for 8) and once settled (8 reading 4 stays at 8). The explorer
    // only walks down when the depth with its headroom rounds below the
    // current rung: 8 reading 3 asks for 4. The trial still decides whether
    // a deeper rung is kept.
    //
    // A depth equal to the rung gets no headroom, because there it is not
    // slack but a climb nobody measured: a link-bound server at 2 whose
    // formula says 2 stays at 2 instead of jumping to 4 untried, and 4
    // reading 4 stays at 4.
    pub(crate) fn target_rung(&self) -> u8 {
        self.physical_target_depth()
            .map_or(self.current_depth, |depth| {
                let headroom = if depth != self.current_depth {
                    TARGET_HEADROOM_DEPTH
                } else {
                    0
                };
                rung_at_or_above(depth.saturating_add(headroom))
            })
    }

    pub(super) fn choose_mode(&self, pressure_clear: bool) -> DownloadLaneMode {
        if !pressure_clear || self.pinned_sequential || !self.supports_pipelining {
            return DownloadLaneMode::Sequential;
        }
        DownloadLaneMode::from_depth(self.current_depth)
    }

    // The mode for `lane_id`'s next lease. The same as `choose_mode` except
    // on the trial a lapsed pin gets: then exactly one lane runs at depth 2
    // and the rest stay sequential, so an unclean trial costs one connection
    // and not every lane of the server. The first lane to ask takes the
    // trial and keeps it for as long as `lane_is_live` says it exists; once
    // it is gone the next lane to ask takes the trial over, with the clean
    // batches already counted.
    pub(super) fn choose_mode_for_lane(
        &mut self,
        pressure_clear: bool,
        lane_id: u64,
        lane_is_live: impl FnOnce(u64) -> bool,
    ) -> DownloadLaneMode {
        if !self.pin_trial || !pressure_clear || !self.supports_pipelining {
            return self.choose_mode(pressure_clear);
        }
        match self.pin_trial_lane {
            Some(holder) if holder == lane_id => {}
            Some(holder) if lane_is_live(holder) => return DownloadLaneMode::Sequential,
            _ => self.pin_trial_lane = Some(lane_id),
        }
        DownloadLaneMode::from_depth(PIN_TRIAL_DEPTH)
    }

    // Fold one clean response into the current window and, once the window is
    // full, decide whether the rung stays, reverts, or advances. `fed` says
    // the response's lane held a full rung of articles when it was read.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn note_response(
        &mut self,
        now: Instant,
        observed_depth: u8,
        payload_bytes: u64,
        policy_elapsed: Duration,
        pressure_clear: bool,
        cold: bool,
        fed: bool,
    ) -> Option<RungChange> {
        // A pinned server is out of the exploration until its pin lapses:
        // nothing it does before then may walk its depth back up. The first
        // response after the lapse puts it on its one trial at depth 2.
        if self.pinned_sequential {
            if self.pinned_until.is_some_and(|until| now >= until) {
                self.begin_pin_trial();
            }
            return None;
        }
        // The trial a lapsed pin gets is judged by its lane's batches, in
        // `note_clean_batch`. One lane at depth 2 beside sequential ones
        // measures neither rung, so the windows wait for the verdict.
        if self.pin_trial {
            return None;
        }
        // Pressure forces sequential and distorts throughput; such a sample
        // says nothing about the rung under test.
        if !pressure_clear {
            return None;
        }
        // A connection's first article pays for the handshake's tail and is
        // read through a congestion window still opening; its wire time
        // describes neither the link nor the rung. Left in, a job's opening
        // window — every lane's first response — would model a link several
        // times slower than the one the download then runs on, and every
        // reconnect afterwards would nudge the model the same way.
        if cold {
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
            // Only a response at the rung can judge it, so only one of
            // those voids the judgment by having waited on its lease.
            self.window_starved |= !fed;
        }
        // A trial window counts only responses at the rung on trial, so it
        // opens with the first of them. A step reaches the wire only as the
        // lanes adopt leases booked after it: every lease already held keeps
        // the depth it was booked at, and at depth 2 with eight lanes that is
        // some seventy responses still to come at the old rung. Counted, they
        // closed window after window with nothing in them that could judge
        // the step, and a short job ended before any verdict. They still feed
        // the link model; they no longer close the trial.
        //
        // A first climb waiting to settle counts the same way, for the same
        // reason: only a response at the new rung can settle it.
        let counted = if self.on_trial || self.settle_pending {
            self.window_rung_responses
        } else {
            self.window_responses
        };
        if counted < self.window_target_responses() {
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
        // A rung is judged only on a window its lanes were fed for
        // throughout. A lane left without its next lease stops at the bottom
        // of its pipe, and the window then times the wait for work as if the
        // server had been slow: a verdict on that window reverts a rung for
        // the dispatcher's lateness.
        let rung_was_fed = !self.window_starved;
        // The explorer's first window is the link's alone. It is read while
        // the lanes are still coming up — some on their first article, the
        // rest sharing the link between fewer of them — so each lane moves
        // bytes faster than it will once all of them run. Taken as a baseline
        // it is a throughput no later window at any rung matches, and the
        // trial that follows reverts on a link that only got busier. It feeds
        // the model; the baseline comes from the next window.
        let opening_window = self.warmup_windows_left == RUNG_WARMUP_WINDOWS;
        self.fold_window_latency();
        self.reset_window();
        self.warmup_windows_left = self.warmup_windows_left.saturating_sub(1);

        if !rung_was_measured || !rung_was_fed {
            // Most of this window came from leases still draining at a
            // shallower depth, or a lane ran out of work inside it. The link
            // model above is worth having either way; the rung comparison has
            // nothing in it that describes the rung.
            return None;
        }
        // A clean, measured window at a guessed rung: the server has taken
        // that depth, and from here every rule applies as to a chosen one.
        self.opened_on_a_guess = false;
        if opening_window {
            return self.first_climb_from_the_model(now);
        }

        // The first climb moved without a trial; the first clean window at
        // its rung makes that rung the baseline and the depth worth saving.
        if self.settle_pending {
            self.settle_pending = false;
            self.baseline_depth = Some(self.current_depth);
            self.baseline_throughput_bps = Some(throughput);
            return Some(RungChange::Kept {
                depth: self.current_depth,
            });
        }

        if self.on_trial {
            let baseline = self.baseline_throughput_bps.unwrap_or(0.0);
            let previous = self.baseline_depth.unwrap_or(self.current_depth);
            // No worse than the rung below, within the noise band, keeps the
            // rung: a deeper rung that ties costs nothing, and only one that
            // measurably lost is worth a hold.
            if throughput * RUNG_THROUGHPUT_NOISE_RATIO >= baseline {
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

    // The first climb on a server with no proven rung and no dropped guess,
    // taken as the opening window closes.
    //
    // By then the window has measured the round trip and the article
    // transfer, and the two already name the rung the link needs. Waiting a
    // window to take a baseline at the seeded rung and another to judge the
    // climb as a trial runs most of a short job at that seeded rung, so the
    // explorer moves at once, as if the rung had been seeded, and the next
    // clean window at that rung settles it. Only a climb up moves here: a
    // model that asks for no more than the current rung, or one still missing
    // a half, leaves the second window to decide as it always has.
    fn first_climb_from_the_model(&mut self, now: Instant) -> Option<RungChange> {
        if !self.first_climb_pending
            || self.first_climb_is_a_trial
            || self.hold_until.is_some_and(|until| now < until)
            || self.physical_target_depth().is_none()
        {
            return None;
        }
        let from = self.current_depth;
        let to = self.target_rung();
        if to <= from {
            return None;
        }
        self.current_depth = to;
        self.on_trial = false;
        self.first_climb_pending = false;
        self.settle_pending = true;
        Some(RungChange::Stepped { from, to })
    }

    // Responses this window closes on.
    fn window_target_responses(&self) -> u64 {
        if self.warmup_windows_left > 0 {
            RUNG_WARMUP_WINDOW_RESPONSES
        } else {
            RUNG_WINDOW_RESPONSES
        }
    }

    // A clean, complete pipelined batch `lane_id` issued at `depth`. On the
    // trial a lapsed pin gets, `PIN_TRIAL_CLEAN_BATCHES` of them from the
    // trial's lane lift the pin for every lane, and the explorer goes on
    // from depth 2 as after any kept trial. Anywhere else it changes nothing.
    //
    // `pipelined` says the server answered on the batch's connection with a
    // later request already on the wire. A batch that ran with one request
    // at a time on the wire was sequential in all but name and proves
    // nothing about the server, so it does not count.
    pub(in crate::pipeline) fn note_clean_batch(
        &mut self,
        now: Instant,
        depth: u8,
        pipelined: bool,
        lane_id: u64,
    ) -> Option<RungChange> {
        if !self.pin_trial
            || !pipelined
            || depth != PIN_TRIAL_DEPTH
            || self.pin_trial_lane != Some(lane_id)
        {
            return None;
        }
        self.pin_trial_clean_batches = self.pin_trial_clean_batches.saturating_add(1);
        if self.pin_trial_clean_batches < PIN_TRIAL_CLEAN_BATCHES {
            return None;
        }
        self.pin_trial = false;
        self.pin_trial_lane = None;
        self.pin_trial_clean_batches = 0;
        self.on_trial = false;
        self.pinned_until = None;
        self.pin_lifted_at = Some(now);
        self.current_depth = PIN_TRIAL_DEPTH;
        self.baseline_depth = Some(self.current_depth);
        self.baseline_throughput_bps = None;
        self.reset_window();
        Some(RungChange::PinLifted {
            depth: self.current_depth,
        })
    }

    // The same, from lane 0 at the explorer's current rung, or at the
    // trial's on a trial.
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub(in crate::pipeline) fn note_unclean_batch(&mut self, now: Instant) -> Option<RungChange> {
        let depth = if self.pin_trial {
            PIN_TRIAL_DEPTH
        } else {
            self.current_depth
        };
        self.note_unclean_batch_at(now, depth, true, 0)
    }

    // An unclean pipelined batch issued at `depth`, `pipelined` when the
    // server had answered on its connection with a later request already on
    // the wire before the fault: one rung off, and hands
    // off this server for ten minutes. A second one inside that window pins
    // the server to sequential for `pin_hold`. Unclean batches inside
    // `UNCLEAN_BURST_WINDOW` of the last one acted on are the same event and
    // change nothing, and so does one issued deeper than the current rung:
    // the rung it failed at is already given up. On the trial after a pin
    // lapsed only the trial lane's batch counts: it pins again at the next
    // step of the back-off, and that lane's connection is the only one lost.
    // So does one at the trial's rung or below before a lifted pin has run
    // clean for `PIN_BACKOFF_RESET_AFTER`: the trial's own proof has failed.
    // A fault at a deeper rung in that time says only that the deeper rung
    // is too deep, and takes the ordinary drop and hold.
    //
    // A rung opened on the probe's guess and not yet measured is the
    // exception: it drops a rung with no hold and is not counted toward the
    // pin, and the first climb back is judged by a trial.
    pub(in crate::pipeline) fn note_unclean_batch_at(
        &mut self,
        now: Instant,
        depth: u8,
        pipelined: bool,
        lane_id: u64,
    ) -> Option<RungChange> {
        // A connection lost before the server answered anything on it with
        // a later request on the wire failed the way a sequential one does,
        // however many requests it had written: an outage, a server restart,
        // an idle drop. That says nothing about pipelining and is no strike.
        if self.pinned_sequential || !pipelined {
            return None;
        }
        if self.pin_trial {
            // Any other lane's unclean batch was issued before the pin.
            if self.pin_trial_lane != Some(lane_id) || depth != PIN_TRIAL_DEPTH {
                return None;
            }
            self.unclean_burst_at = Some(now);
            self.last_unclean_at = Some(now);
            self.reset_window();
            return Some(self.pin(now));
        }
        if depth > self.current_depth {
            return None;
        }
        if self
            .unclean_burst_at
            .is_some_and(|at| now.saturating_duration_since(at) < UNCLEAN_BURST_WINDOW)
        {
            return None;
        }
        self.unclean_burst_at = Some(now);
        let lift_is_provisional = self
            .pin_lifted_at
            .is_some_and(|at| now.saturating_duration_since(at) < PIN_BACKOFF_RESET_AFTER);
        if lift_is_provisional && depth <= PIN_TRIAL_DEPTH {
            self.reset_window();
            self.last_unclean_at = Some(now);
            return Some(self.pin(now));
        }
        self.reset_window();
        self.on_trial = false;
        self.settle_pending = false;
        self.baseline_depth = None;
        self.baseline_throughput_bps = None;

        if self.opened_on_a_guess {
            self.opened_on_a_guess = false;
            self.first_climb_is_a_trial = true;
            let from = self.current_depth;
            let to = RUNGS[rung_index(from).saturating_sub(1)];
            self.current_depth = to;
            return (to != from).then_some(RungChange::Dropped { from, to });
        }

        let within_hold = self
            .last_unclean_at
            .is_some_and(|at| now.saturating_duration_since(at) < RUNG_HOLD);
        self.last_unclean_at = Some(now);
        if within_hold {
            return Some(self.pin(now));
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

    // Pin the server to sequential until `pinned_until`. The hold doubles
    // with each pin of the back-off; a server that ran clean for
    // `PIN_BACKOFF_RESET_AFTER` since its last pin was lifted starts over.
    fn pin(&mut self, now: Instant) -> RungChange {
        if self
            .pin_lifted_at
            .is_some_and(|at| now.saturating_duration_since(at) >= PIN_BACKOFF_RESET_AFTER)
        {
            self.pin_strikes = 0;
        }
        self.pin_lifted_at = None;
        let hold = pin_hold(self.pin_strikes);
        self.pin_strikes = self.pin_strikes.saturating_add(1);
        self.pinned_sequential = true;
        self.pin_trial = false;
        self.pin_trial_lane = None;
        self.pin_trial_clean_batches = 0;
        self.on_trial = false;
        self.pinned_until = Some(now + hold);
        self.current_depth = 1;
        RungChange::PinnedSequential { hold }
    }

    // The pin has lapsed: one trial, on one lane at depth 2, ended by
    // `PIN_TRIAL_CLEAN_BATCHES` clean batches from that lane or by an
    // unclean one. Every other lane stays sequential, so the depth stays 1.
    fn begin_pin_trial(&mut self) {
        self.pinned_sequential = false;
        self.pin_trial = true;
        self.pin_trial_lane = None;
        self.pin_trial_clean_batches = 0;
        self.on_trial = false;
        self.settle_pending = false;
        // Every later move is a single step: the first climb is long past.
        self.first_climb_pending = false;
        // A pin can come before the opening window closed; what follows the
        // trial is not an opening window.
        self.warmup_windows_left = self.warmup_windows_left.min(RUNG_WARMUP_WINDOWS - 1);
        self.baseline_depth = None;
        self.baseline_throughput_bps = None;
        self.current_depth = 1;
        self.reset_window();
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

    // Bytes per second of wire time: the rate the depth model divides by.
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
        self.window_starved = false;
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
    // Durable identity of each explorer's pool position, recorded when the
    // explorer is created. A pool rebuild renumbers the positions, so the
    // mapping has to be the one that was true while the measurements were
    // taken — looking it up afterwards resolves against the new pool and
    // silently throws every measurement away.
    pub(in crate::pipeline) stable_ids: HashMap<usize, u32>,
    // What each position's server was when its explorer was seeded: stable
    // id, host, port and username, the identity a pool rebuild keeps a
    // server by. A pipelining verdict is carried only to the same server.
    pub(in crate::pipeline) identities: Vec<weaver_nntp::pool::ServerIdentity>,
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

    // `depth = 1 + ceil(rtt / per-lane article transfer time)`.
    //
    // One article's transfer covers one article's worth of the round trip, so
    // the pipe needs one more request in flight than the number of article
    // transfers that fit inside the round trip. A link whose articles cost
    // more than the round trip needs no pipelining at all; a 100 ms link
    // carrying ~10 ms articles needs eleven, which the ladder caps at eight.
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

    // The formula only chooses the trial target; the ladder still bounds it.
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

    // The two halves of the quotient have to come from the same window.
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

    // A sequential lane's wall clock is one round trip plus one article per
    // response. The model must divide by the article, not the sum: at 100 ms
    // over 25 ms articles the link needs depth 5 (rung 8), and a divisor that
    // kept the round trip would have said 2.
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

    // The median is what keeps a file's short tail article and a PAR2 index
    // from moving the modelled article size.
    #[test]
    fn median_body_bytes_ignores_the_odd_short_article() {
        let mut explorer = ServerPipelineExplorer::default();
        assert_eq!(explorer.median_body_bytes(), None);
        for bytes in [750_000, 750_000, 750_000, 12_000, 750_000] {
            explorer.note_body_bytes(bytes);
        }
        assert_eq!(explorer.median_body_bytes(), Some(750_000));
    }

    // Feed exactly one window at the explorer's current rung, whichever
    // length that window is.
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
                .note_response(
                    now,
                    depth,
                    bytes_per_response,
                    elapsed_per_response,
                    true,
                    false,
                    true,
                )
                .or(change);
        }
        change
    }

    // A server the servers table already holds a rung for, two deep. Its
    // first climb is a trial, judged like every later one.
    fn proven_at_two(latency_ms: u64, transfer_ms: u64) -> ServerPipelineExplorer {
        let mut explorer = ServerPipelineExplorer::seeded(
            Some(2),
            Some(Duration::from_millis(latency_ms)),
            Some(Duration::from_millis(transfer_ms)),
        );
        explorer.note_supports_pipelining(true);
        explorer
    }

    // Feed the opening window of a server with a proven rung, which measures
    // the link and decides nothing.
    fn open_window(explorer: &mut ServerPipelineExplorer, now: Instant) {
        let depth = explorer.current_depth();
        assert_eq!(
            run_window(explorer, now, 100_000, Duration::from_millis(100)),
            None,
            "the opening window moves no rung"
        );
        assert_eq!(explorer.current_depth(), depth);
        assert_eq!(explorer.baseline_throughput_bps, None);
    }

    // A connection's first article never counts toward the window, so a
    // job whose lanes have all just connected does not close its opening
    // window on handshake-era samples.
    #[test]
    fn cold_responses_do_not_fill_the_window() {
        let mut explorer = explorer(300, 50);
        let now = Instant::now();
        let depth = explorer.current_depth();
        for _ in 0..explorer.window_target_responses() {
            assert_eq!(
                explorer.note_response(
                    now,
                    depth,
                    100_000,
                    Duration::from_millis(400),
                    true,
                    true,
                    true
                ),
                None
            );
        }
        assert_eq!(explorer.window_responses, 0);
        assert_eq!(explorer.modelled_article_transfer(), None);
        assert_eq!(explorer.current_depth(), 2);
    }

    // The window's shortest warm status-line wait is the round trip; one
    // slow outlier in the same window, warm or cold, does not move it.
    #[test]
    fn the_window_folds_its_shortest_warm_latency() {
        let mut explorer = explorer(300, 50);
        let now = Instant::now();
        explorer.note_latency_sample(Duration::from_millis(900), true);
        explorer.note_latency_sample(Duration::from_millis(100), false);
        explorer.note_latency_sample(Duration::from_millis(700), false);
        explorer.note_latency_sample(Duration::from_millis(100), false);
        assert_eq!(explorer.latency(), Some(Duration::from_millis(300)));
        run_window(&mut explorer, now, 100_000, Duration::from_millis(100));
        // blend(300, 100): three quarters of the old estimate plus a quarter
        // of the minimum, not of the 700 ms outlier or the 900 ms cold wait.
        assert_eq!(explorer.latency(), Some(Duration::from_millis(250)));
        assert_eq!(explorer.window_latency_warm_min, None);
        assert_eq!(explorer.window_latency_cold_min, None);
    }

    // A server whose rung window never closes — pinned sequential here —
    // still folds its round trip every few samples.
    #[test]
    fn latency_folds_on_its_own_when_the_rung_window_never_closes() {
        let mut explorer = explorer(300, 50);
        explorer.pinned_sequential = true;
        let now = Instant::now();
        for _ in 0..LATENCY_FOLD_SAMPLES - 1 {
            explorer.note_latency_sample(Duration::from_millis(100), false);
            assert_eq!(
                explorer.note_response(
                    now,
                    1,
                    100_000,
                    Duration::from_millis(100),
                    true,
                    false,
                    true
                ),
                None
            );
        }
        assert_eq!(explorer.latency(), Some(Duration::from_millis(300)));
        explorer.note_latency_sample(Duration::from_millis(100), false);
        assert_eq!(explorer.latency(), Some(Duration::from_millis(250)));
        assert_eq!(explorer.window_latency_samples, 0);
    }

    // A cold sample stands in only while the server has no round trip at
    // all, and never once a warm one has been measured.
    #[test]
    fn a_cold_latency_only_seeds_an_unmeasured_server() {
        let mut explorer = ServerPipelineExplorer::default();
        explorer.note_supports_pipelining(true);
        let now = Instant::now();
        explorer.note_latency_sample(Duration::from_millis(600), true);
        run_window(&mut explorer, now, 100_000, Duration::from_millis(100));
        assert_eq!(explorer.latency(), Some(Duration::from_millis(600)));

        explorer.note_latency_sample(Duration::from_millis(200), false);
        run_window(&mut explorer, now, 100_000, Duration::from_millis(100));
        assert_eq!(explorer.latency(), Some(Duration::from_millis(500)));

        explorer.note_latency_sample(Duration::from_millis(2_000), true);
        run_window(&mut explorer, now, 100_000, Duration::from_millis(100));
        assert_eq!(explorer.latency(), Some(Duration::from_millis(500)));
    }

    #[test]
    fn target_depth_follows_the_latency_to_transfer_ratio() {
        // Good band, article costs about as much as the round trip. The
        // physics asks for 2, which is the rung the server is on, so no
        // headroom is added: a link-bound server stays at 2.
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

        // Slow band, large articles: link-bound at 2, like the good band.
        let slow = explorer(900, 900);
        assert_eq!(slow.physical_target_depth(), Some(2));
        assert_eq!(slow.target_rung(), 2);

        // Slow band, small articles: clamped at the deepest rung.
        let slow_small = explorer(1000, 20);
        assert_eq!(slow_small.physical_target_depth(), Some(8));
        assert_eq!(slow_small.target_rung(), 8);
    }

    // The headroom request is added unless the formula's depth is the
    // current rung. Each case is (current rung, formula depth, target rung).
    #[test]
    fn headroom_is_added_unless_the_formula_equals_the_current_rung() {
        // Round trip 100 ms; the article time picks the formula's depth.
        let article_ms_for_depth = |depth: u8| -> u64 {
            match depth {
                2 => 100,
                3 => 50,
                4 => 34,
                5 => 25,
                8 => 10,
                _ => unreachable!("no case uses depth {depth}"),
            }
        };
        let cases = [
            // The formula on the rung: link-bound servers stay; no untried
            // climb.
            (2, 2, 2),
            (4, 4, 4),
            (8, 8, 8),
            // Climbs keep their slack. The bench link reads 4 or 5 window to
            // window; from 2 both give 8.
            (2, 3, 4),
            (2, 4, 8),
            (2, 5, 8),
            (4, 5, 8),
            // A settled rung is not left on one low reading.
            (8, 4, 8),
            (8, 5, 8),
            (4, 2, 4),
            (4, 3, 4),
            // The walk down starts only when the depth with its headroom
            // rounds below the rung.
            (8, 3, 4),
            (8, 2, 4),
        ];
        for (rung, depth, target) in cases {
            let article_ms = article_ms_for_depth(depth);
            let mut explorer = explorer(100, article_ms);
            explorer.modelled_transfer = Some(Duration::from_millis(article_ms));
            explorer.current_depth = rung;
            assert_eq!(
                explorer.physical_target_depth(),
                Some(depth),
                "formula for {article_ms} ms articles"
            );
            assert_eq!(
                explorer.target_rung(),
                target,
                "rung {rung}, formula {depth}"
            );
        }
    }

    // A fresh server whose complete model says 2 (link-bound) makes no jump
    // at the opening window, and the next window agrees with the rung and
    // ends the first climb there. Nothing is saved.
    #[test]
    fn a_link_bound_fresh_server_makes_no_jump_at_the_opening_window() {
        let mut explorer = explorer(200, 200);
        let now = Instant::now();
        assert_eq!(explorer.target_rung(), 2);
        assert_eq!(
            run_window(&mut explorer, now, 768_000, Duration::from_millis(400)),
            None
        );
        assert_eq!(explorer.physical_target_depth(), Some(2));
        assert_eq!(explorer.current_depth(), 2);
        assert!(!explorer.settle_pending);
        assert!(explorer.first_climb_pending);

        assert_eq!(
            run_window(&mut explorer, now, 768_000, Duration::from_millis(400)),
            None
        );
        assert_eq!(explorer.current_depth(), 2);
        assert!(!explorer.first_climb_pending);
        assert_eq!(explorer.persisted_depth, None);
    }

    // The bench link sits on the 4/5 edge: a 102 ms round trip over 48 ms
    // articles is depth 4, and a window that reads the wire at 31 ms makes it
    // 5. Without headroom those were rungs 4 and 8 from run to run; with it
    // both ask for the next rung up.
    #[test]
    fn a_target_on_the_four_five_edge_asks_for_the_next_rung_up() {
        let mut slower = explorer(102, 48);
        slower.modelled_transfer = Some(Duration::from_millis(48));
        assert_eq!(slower.physical_target_depth(), Some(4));
        assert_eq!(slower.target_rung(), 8);

        let mut faster = explorer(102, 31);
        faster.modelled_transfer = Some(Duration::from_millis(31));
        assert_eq!(faster.physical_target_depth(), Some(5));
        assert_eq!(faster.target_rung(), 8);

        // A server with nothing proven goes there as its opening window
        // closes.
        let now = Instant::now();
        assert_eq!(
            run_window(&mut slower, now, 768_000, Duration::from_millis(50)),
            Some(RungChange::Stepped { from: 2, to: 8 })
        );
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

    // A server with nothing proven goes straight to the rung the measured
    // link asks for as soon as its opening window has measured it, with no
    // trial, and the next clean window at that rung settles it.
    //
    // A climb a rung at a time cost a window to step and another to judge
    // each rung, and a baseline window plus a trial window at the seeded rung
    // still ran most of a short download there. The ladder's caution stays in
    // every later change; what moved is only how a fresh server arrives at its
    // first rung.
    #[test]
    fn the_opening_window_moves_straight_to_the_bandwidth_delay_rung() {
        let mut explorer = explorer(300, 50);
        let now = Instant::now();
        assert_eq!(explorer.current_depth(), 2);
        assert_eq!(explorer.target_rung(), 8);

        let change = run_window(&mut explorer, now, 100_000, Duration::from_millis(100));
        assert_eq!(change, Some(RungChange::Stepped { from: 2, to: 8 }));
        assert_eq!(explorer.current_depth(), 8);
        assert!(!explorer.on_trial, "the first climb is not a trial");
        assert_eq!(explorer.baseline_throughput_bps, None);
        assert_eq!(
            explorer.choose_mode(true),
            DownloadLaneMode::Pipelined { depth: 8 }
        );

        // Any clean window at the rung settles it, whatever it reads: there
        // is no baseline below it to lose to.
        let change = run_window(&mut explorer, now, 10_000, Duration::from_millis(100));
        assert_eq!(change, Some(RungChange::Kept { depth: 8 }));
        assert_eq!(explorer.current_depth(), 8);
        assert_eq!(explorer.baseline_depth, Some(8));
    }

    // A server the servers table already holds a rung for keeps today's
    // first climb: the opening window decides nothing, the next one takes
    // the baseline and climbs, and that climb is a trial.
    #[test]
    fn a_proven_servers_first_climb_is_still_a_trial() {
        let mut explorer = proven_at_two(300, 50);
        let now = Instant::now();
        open_window(&mut explorer, now);
        let change = run_window(&mut explorer, now, 100_000, Duration::from_millis(100));
        assert_eq!(change, Some(RungChange::Stepped { from: 2, to: 8 }));
        assert!(explorer.on_trial);
        assert!(!explorer.settle_pending);
    }

    // A model still missing a half has no rung to jump to: the opening
    // window decides nothing, and the first climb waits for the model and
    // goes on trial as it did before.
    #[test]
    fn an_incomplete_model_makes_no_jump_at_the_opening_window() {
        let mut explorer = ServerPipelineExplorer::default();
        explorer.note_supports_pipelining(true);
        let now = Instant::now();
        assert_eq!(
            run_window(&mut explorer, now, 100_000, Duration::from_millis(100)),
            None
        );
        assert_eq!(explorer.physical_target_depth(), None);
        assert_eq!(explorer.current_depth(), 2);
        assert!(!explorer.settle_pending);
        assert!(explorer.first_climb_pending);

        // The round trip arrives; the next window climbs on trial.
        assert_eq!(
            run_window(&mut explorer, now, 100_000, Duration::from_millis(100)),
            None
        );
        explorer.note_latency(Duration::from_millis(300));
        assert_eq!(
            run_window(&mut explorer, now, 100_000, Duration::from_millis(100)),
            Some(RungChange::Stepped { from: 2, to: 8 })
        );
        assert!(explorer.on_trial);
        assert!(!explorer.settle_pending);
    }

    // The jump is a step up, which the lanes never write through. Responses
    // still arriving from leases booked at the old rung do not settle it, nor
    // does a window a lane ran short in; the first clean window at the new
    // rung returns `Kept`, and only that is saved.
    #[test]
    fn the_first_climb_is_not_saved_until_a_clean_window_at_its_rung_settles_it() {
        let mut explorer = explorer(300, 50);
        let now = Instant::now();
        let change = run_window(&mut explorer, now, 100_000, Duration::from_millis(100))
            .expect("the opening window jumps");
        assert_eq!(change, RungChange::Stepped { from: 2, to: 8 });
        assert!(!change.settles_the_rung());

        // Seventy-two responses from leases booked two deep, one of them from
        // a lane left short: none of them settles the rung or voids it.
        for response in 0..72 {
            assert_eq!(
                explorer.note_response(
                    now,
                    2,
                    100_000,
                    Duration::from_millis(100),
                    true,
                    false,
                    response != 40,
                ),
                None
            );
        }
        assert!(explorer.settle_pending);

        // A window at the rung that a lane ran short in settles nothing.
        let responses = explorer.window_target_responses();
        for response in 0..responses {
            assert_eq!(
                explorer.note_response(
                    now,
                    8,
                    100_000,
                    Duration::from_millis(100),
                    true,
                    false,
                    response != responses / 2,
                ),
                None
            );
        }
        assert!(explorer.settle_pending);

        let change = run_window(&mut explorer, now, 100_000, Duration::from_millis(100))
            .expect("a clean window at the rung settles it");
        assert_eq!(change, RungChange::Kept { depth: 8 });
        assert!(change.settles_the_rung());
        assert!(!explorer.settle_pending);
        assert_eq!(explorer.take_persist_request(), Some(8));
        assert_eq!(explorer.take_persist_request(), None);
    }

    // Later changes still walk the ladder a rung at a time.
    #[test]
    fn a_later_change_steps_one_rung_at_a_time() {
        let mut explorer = explorer(300, 200);
        let now = Instant::now();
        // Articles as long as two thirds of the round trip: the link wants 4,
        // which is where the opening window puts it.
        assert_eq!(explorer.target_rung(), 4);
        assert_eq!(
            run_window(&mut explorer, now, 100_000, Duration::from_millis(100)),
            Some(RungChange::Stepped { from: 2, to: 4 })
        );
        // Settle the rung, then let the articles shrink so the target moves out
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

    // The trial tests below run on a server with a proven rung, whose first
    // climb is a trial; every later climb on any server is one too.
    #[test]
    fn a_step_up_that_pays_for_itself_is_kept() {
        let mut explorer = proven_at_two(300, 50);
        let now = Instant::now();
        open_window(&mut explorer, now);
        run_window(&mut explorer, now, 100_000, Duration::from_millis(100));
        assert_eq!(explorer.current_depth(), 8);

        // Twice the bytes in the same time: comfortably past the 1.05 bar.
        let change = run_window(&mut explorer, now, 200_000, Duration::from_millis(100));
        assert_eq!(change, Some(RungChange::Kept { depth: 8 }));
        assert_eq!(explorer.current_depth(), 8);
    }

    // A deeper rung that reads the same throughput as the one below it, to
    // within the noise band, cost nothing and is kept. A link-bound server
    // reads the same at every rung, and a bar the trial had to clear upward
    // reverted it on every run.
    #[test]
    fn a_trial_that_ties_its_baseline_is_kept() {
        let mut explorer = proven_at_two(300, 50);
        let now = Instant::now();
        open_window(&mut explorer, now);
        run_window(&mut explorer, now, 100_000, Duration::from_millis(100));
        assert_eq!(explorer.current_depth(), 8);

        // Four percent below the baseline: inside the band.
        let change = run_window(&mut explorer, now, 96_000, Duration::from_millis(100));
        assert_eq!(change, Some(RungChange::Kept { depth: 8 }));
        assert_eq!(explorer.current_depth(), 8);
    }

    // The bench's link-bound server: 768 000-byte articles, a 102 ms round
    // trip, 48 ms on the wire. The opening window reads each lane at 18 MB/s
    // while the lanes are still coming up; every later window, at either
    // rung, reads 15.4 MB/s. With nothing proven, the opening window moves
    // the server to 8 and the first steady window settles it there; the
    // fast opening window is never the baseline.
    #[test]
    fn the_bench_link_jumps_at_the_opening_window_and_settles_on_the_next() {
        const ARTICLE: u64 = 768_000;
        let opening = Duration::from_micros(42_700);
        let steady = Duration::from_micros(50_000);
        let mut explorer = explorer(102, 48);
        let now = Instant::now();

        assert_eq!(
            run_window(&mut explorer, now, ARTICLE, opening),
            Some(RungChange::Stepped { from: 2, to: 8 })
        );
        assert_eq!(explorer.baseline_throughput_bps, None);
        assert_eq!(
            run_window(&mut explorer, now, ARTICLE, steady),
            Some(RungChange::Kept { depth: 8 })
        );
        assert_eq!(explorer.current_depth(), 8);
        let baseline = explorer
            .baseline_throughput_bps
            .expect("the settle window is the baseline");
        assert!((baseline - ARTICLE as f64 / steady.as_secs_f64()).abs() < 1.0);
    }

    // The bench link once settled at 8: a later window that reads the
    // formula at 4 rather than 5 leaves the rung where it is. Without the
    // headroom on that reading the explorer stepped down to 4, and the next
    // reading of 5 climbed back to 8 on trial.
    #[test]
    fn the_bench_link_settled_at_eight_stays_on_a_window_that_reads_four() {
        const ARTICLE: u64 = 768_000;
        let steady = Duration::from_millis(50);
        let mut explorer = explorer(102, 48);
        let now = Instant::now();
        assert_eq!(
            run_window(&mut explorer, now, ARTICLE, steady),
            Some(RungChange::Stepped { from: 2, to: 8 })
        );
        assert_eq!(
            run_window(&mut explorer, now, ARTICLE, steady),
            Some(RungChange::Kept { depth: 8 })
        );

        explorer.modelled_transfer = Some(Duration::from_millis(48));
        explorer.transfer = Some(Duration::from_millis(48));
        assert_eq!(
            run_window(&mut explorer, now, ARTICLE, steady),
            None,
            "a reading of 4 at a settled 8 moves nothing"
        );
        assert_eq!(explorer.physical_target_depth(), Some(4));
        assert_eq!(explorer.target_rung(), 8);
        assert_eq!(explorer.current_depth(), 8);
    }

    // The same link on a server with a proven rung, whose first climb is a
    // trial. The opening window is not the baseline, so the trial that
    // matches the steady state keeps its rung.
    #[test]
    fn a_fast_opening_window_does_not_revert_a_trial_at_the_steady_state() {
        const ARTICLE: u64 = 768_000;
        let opening = Duration::from_micros(42_700);
        let steady = Duration::from_micros(50_000);
        let mut explorer = proven_at_two(102, 48);
        let now = Instant::now();

        assert_eq!(run_window(&mut explorer, now, ARTICLE, opening), None);
        assert_eq!(explorer.current_depth(), 2);
        assert_eq!(explorer.baseline_throughput_bps, None);

        let change = run_window(&mut explorer, now, ARTICLE, steady);
        let Some(RungChange::Stepped { from: 2, to }) = change else {
            panic!("the baseline window steps toward the target, got {change:?}");
        };
        let change = run_window(&mut explorer, now, ARTICLE, steady);
        assert_eq!(change, Some(RungChange::Kept { depth: to }));
        assert_eq!(explorer.current_depth(), to);
    }

    // A trial window in which one response found its lane short of a full
    // rung is no verdict: the slow window timed the lane waiting on its next
    // lease. The trial stays open, and the next window, fed throughout,
    // judges it, and still reverts a rung that measurably lost.
    #[test]
    fn a_window_a_lane_ran_short_in_judges_no_rung() {
        let mut explorer = proven_at_two(300, 50);
        let now = Instant::now();
        open_window(&mut explorer, now);
        run_window(&mut explorer, now, 100_000, Duration::from_millis(100));
        assert_eq!(explorer.current_depth(), 8);

        let responses = explorer.window_target_responses();
        let mut change = None;
        for response in 0..responses {
            let fed = response != responses / 2;
            change = explorer
                .note_response(now, 8, 90_000, Duration::from_millis(100), true, false, fed)
                .or(change);
        }
        assert_eq!(change, None);
        assert_eq!(explorer.current_depth(), 8);
        assert_eq!(explorer.window_responses, 0);

        let change = run_window(&mut explorer, now, 90_000, Duration::from_millis(100));
        assert_eq!(change, Some(RungChange::Reverted { from: 8, to: 2 }));
    }

    #[test]
    fn a_step_up_that_does_not_pay_reverts_and_holds() {
        let mut explorer = proven_at_two(300, 50);
        let now = Instant::now();
        open_window(&mut explorer, now);
        run_window(&mut explorer, now, 100_000, Duration::from_millis(100));
        assert_eq!(explorer.current_depth(), 8);

        let change = run_window(&mut explorer, now, 90_000, Duration::from_millis(100));
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
        assert!(explorer.settle_pending);

        assert_eq!(
            explorer.note_unclean_batch(now),
            Some(RungChange::Dropped { from: 8, to: 4 })
        );
        assert_eq!(explorer.current_depth(), 4);
        // The rung the first climb was waiting to settle is gone.
        assert!(!explorer.settle_pending);

        let pinned_at = now + Duration::from_secs(60);
        assert_eq!(
            explorer.note_unclean_batch(pinned_at),
            Some(RungChange::PinnedSequential { hold: PIN_HOLD })
        );
        assert!(explorer.pinned_sequential());
        assert_eq!(explorer.choose_mode(true), DownloadLaneMode::Sequential);
        assert_eq!(explorer.pinned_until, Some(pinned_at + PIN_HOLD));

        // Until the pin lapses a full window of clean responses does not
        // lift it.
        let inside = pinned_at + PIN_HOLD - Duration::from_secs(1);
        assert_eq!(
            run_window(&mut explorer, inside, 100_000, Duration::from_millis(100)),
            None
        );
        assert!(explorer.pinned_sequential());
        assert_eq!(explorer.choose_mode(true), DownloadLaneMode::Sequential);
        // A third unclean batch is a no-op rather than a second warning.
        assert_eq!(explorer.note_unclean_batch(inside), None);
    }

    // A server pinned at `now + UNCLEAN_BURST_WINDOW`, from a first climb to
    // 8 and two counted unclean batches.
    fn pinned(now: Instant) -> (ServerPipelineExplorer, Instant) {
        let mut explorer = explorer(300, 50);
        run_window(&mut explorer, now, 100_000, Duration::from_millis(100));
        assert_eq!(
            explorer.note_unclean_batch(now),
            Some(RungChange::Dropped { from: 8, to: 4 })
        );
        let pinned_at = now + UNCLEAN_BURST_WINDOW;
        assert!(matches!(
            explorer.note_unclean_batch(pinned_at),
            Some(RungChange::PinnedSequential { .. })
        ));
        (explorer, pinned_at)
    }

    // One sequential response after the lapse starts the trial, and lane 0
    // takes it.
    fn lapse(explorer: &mut ServerPipelineExplorer, at: Instant) {
        assert_eq!(
            explorer.note_response(
                at,
                1,
                100_000,
                Duration::from_millis(100),
                true,
                false,
                true
            ),
            None
        );
        assert!(!explorer.pinned_sequential());
        assert!(explorer.pin_trial);
        // The server's depth stays 1; lane 0 asks first and takes the trial.
        assert_eq!(explorer.current_depth(), 1);
        assert_eq!(explorer.choose_mode(true), DownloadLaneMode::Sequential);
        assert_eq!(
            explorer.choose_mode_for_lane(true, 0, |_| true),
            DownloadLaneMode::Pipelined { depth: 2 }
        );
    }

    // Clean batches at the trial's rung, all at `at`.
    fn clean_batches(
        explorer: &mut ServerPipelineExplorer,
        at: Instant,
        count: u8,
    ) -> Option<RungChange> {
        let mut change = None;
        for _ in 0..count {
            change = explorer.note_clean_batch(at, 2, true, 0).or(change);
        }
        change
    }

    // A burst of resets makes every lane of the server report an unclean
    // batch within a second. That is one event: it costs one rung and
    // starts one hold, and does not pin. The lanes whose batches were issued
    // at the rung just given up are ignored for that alone; the rest by the
    // burst window.
    #[test]
    fn a_burst_of_unclean_batches_costs_one_rung_and_does_not_pin() {
        let mut explorer = explorer(300, 50);
        let now = Instant::now();
        run_window(&mut explorer, now, 100_000, Duration::from_millis(100));
        assert_eq!(explorer.current_depth(), 8);

        assert_eq!(
            explorer.note_unclean_batch_at(now, 8, true, 0),
            Some(RungChange::Dropped { from: 8, to: 4 })
        );
        for lane in 1..40u64 {
            let at = now + Duration::from_millis(25 * lane);
            let depth = if lane % 2 == 0 { 8 } else { 4 };
            assert_eq!(
                explorer.note_unclean_batch_at(at, depth, true, 0),
                None,
                "lane {lane}"
            );
        }
        // Issued at the rung already given up, it is ignored past the
        // burst window too.
        assert_eq!(
            explorer.note_unclean_batch_at(now + UNCLEAN_BURST_WINDOW * 2, 8, true, 0),
            None
        );
        assert!(!explorer.pinned_sequential());
        assert_eq!(explorer.current_depth(), 4);
        assert_eq!(explorer.hold_until, Some(now + RUNG_HOLD));
        assert_eq!(explorer.last_unclean_at, Some(now));

        // Just inside the burst window still changes nothing; the first one
        // past it, inside the hold, pins.
        let edge = now + UNCLEAN_BURST_WINDOW - Duration::from_millis(1);
        assert_eq!(explorer.note_unclean_batch(edge), None);
        assert_eq!(
            explorer.note_unclean_batch(now + UNCLEAN_BURST_WINDOW),
            Some(RungChange::PinnedSequential { hold: PIN_HOLD })
        );
    }

    // A pin lapses into one trial at depth 2. `PIN_TRIAL_CLEAN_BATCHES`
    // clean batches there lift it, and the explorer goes on from that rung.
    #[test]
    fn a_lapsed_pin_gets_one_trial_at_depth_two_and_a_clean_one_lifts_it() {
        let now = Instant::now();
        let (mut explorer, pinned_at) = pinned(now);
        let due = pinned_at + PIN_HOLD;
        // Just before the lapse a response leaves the pin alone.
        assert_eq!(
            explorer.note_response(
                due - Duration::from_secs(1),
                1,
                100_000,
                Duration::from_millis(100),
                true,
                false,
                true
            ),
            None
        );
        assert!(explorer.pinned_sequential());

        lapse(&mut explorer, due);
        // A full, fed window at the trial's rung does not judge it, and a
        // clean batch issued at another depth does not count.
        assert_eq!(
            run_window(&mut explorer, due, 100_000, Duration::from_millis(100)),
            None
        );
        assert_eq!(explorer.note_clean_batch(due, 4, true, 0), None);
        assert_eq!(
            clean_batches(&mut explorer, due, PIN_TRIAL_CLEAN_BATCHES - 1),
            None
        );
        assert!(explorer.pin_trial);
        // Neither does one that closed with a single request on the wire.
        assert_eq!(explorer.note_clean_batch(due, 2, false, 0), None);
        assert!(explorer.pin_trial);
        assert_eq!(
            explorer.note_clean_batch(due, 2, true, 0),
            Some(RungChange::PinLifted { depth: 2 })
        );
        assert!(!explorer.pin_trial);
        assert!(!explorer.on_trial);
        assert_eq!(explorer.pinned_until, None);
        assert_eq!(explorer.pin_lifted_at, Some(due));
        assert_eq!(
            explorer.choose_mode(true),
            DownloadLaneMode::Pipelined { depth: 2 }
        );
        // From here it explores as after any kept trial: a rung at a time.
        assert_eq!(
            run_window(&mut explorer, due, 100_000, Duration::from_millis(100)),
            Some(RungChange::Stepped { from: 2, to: 4 })
        );
    }

    // A server that gets a few articles a job never closes a rung window,
    // and its trial still ends: on its batches. An unclean batch before the
    // last clean one needed pins it again.
    #[test]
    fn a_starved_trial_lifts_on_its_clean_batches_and_an_unclean_one_before_then_pins() {
        let now = Instant::now();
        let (mut explorer, pinned_at) = pinned(now);
        let due = pinned_at + PIN_HOLD;
        lapse(&mut explorer, due);
        let mut unclean = explorer.clone();

        // Batches hours apart, with no window anywhere near full.
        let mut at = due;
        for _ in 0..PIN_TRIAL_CLEAN_BATCHES - 1 {
            at += Duration::from_secs(60 * 60);
            assert_eq!(explorer.note_clean_batch(at, 2, true, 0), None);
        }
        assert!(explorer.window_rung_responses < explorer.window_target_responses());
        at += Duration::from_secs(60 * 60);
        assert_eq!(
            explorer.note_clean_batch(at, 2, true, 0),
            Some(RungChange::PinLifted { depth: 2 })
        );
        assert_eq!(explorer.pin_lifted_at, Some(at));

        clean_batches(&mut unclean, due, PIN_TRIAL_CLEAN_BATCHES - 1);
        assert_eq!(
            unclean.note_unclean_batch(due + Duration::from_secs(60)),
            Some(RungChange::PinnedSequential { hold: PIN_HOLD * 2 })
        );
        // The next trial counts its clean batches from nothing.
        let again = due + Duration::from_secs(60) + PIN_HOLD * 2;
        lapse(&mut unclean, again);
        assert_eq!(unclean.pin_trial_clean_batches, 0);
    }

    // A lift is provisional until the server has run clean for
    // `PIN_BACKOFF_RESET_AFTER`. An unclean batch an hour after it pins again
    // at the next step; one after that time is an ordinary first strike.
    #[test]
    fn an_unclean_batch_soon_after_a_lift_pins_again_for_twice_as_long() {
        let now = Instant::now();
        let (mut explorer, pinned_at) = pinned(now);
        let due = pinned_at + PIN_HOLD;
        lapse(&mut explorer, due);
        clean_batches(&mut explorer, due, PIN_TRIAL_CLEAN_BATCHES);
        assert_eq!(explorer.pin_lifted_at, Some(due));
        let lifted = explorer.clone();

        assert_eq!(
            explorer.note_unclean_batch(due + Duration::from_secs(60 * 60)),
            Some(RungChange::PinnedSequential { hold: PIN_HOLD * 2 })
        );
        assert!(explorer.pinned_sequential());

        let mut explorer = lifted;
        let strike = due + PIN_BACKOFF_RESET_AFTER;
        assert_eq!(
            explorer.note_unclean_batch(strike),
            Some(RungChange::Dropped { from: 2, to: 1 })
        );
        assert!(!explorer.pinned_sequential());
        assert_eq!(explorer.hold_until, Some(strike + RUNG_HOLD));
    }

    // An unclean trial pins again for twice as long, up to the cap.
    #[test]
    fn an_unclean_trial_pins_again_for_twice_as_long_up_to_the_cap() {
        let now = Instant::now();
        let (mut explorer, pinned_at) = pinned(now);
        let mut due = pinned_at + PIN_HOLD;
        let mut holds = Vec::new();
        for _ in 0..6 {
            lapse(&mut explorer, due);
            let Some(RungChange::PinnedSequential { hold }) = explorer.note_unclean_batch(due)
            else {
                panic!("an unclean trial pins");
            };
            assert!(explorer.pinned_sequential());
            assert_eq!(explorer.current_depth(), 1);
            assert_eq!(explorer.pinned_until, Some(due + hold));
            holds.push(hold.as_secs() / 60);
            due += hold;
        }
        assert_eq!(holds, vec![20, 40, 80, 120, 120, 120]);
    }

    // The back-off starts over once the server has run as long as the
    // longest pin since a clean trial lifted its last one.
    #[test]
    fn a_clean_stretch_after_a_lifted_pin_resets_the_back_off() {
        let now = Instant::now();
        let (mut explorer, pinned_at) = pinned(now);
        let mut due = pinned_at + PIN_HOLD;
        lapse(&mut explorer, due);
        assert!(matches!(
            explorer.note_unclean_batch(due),
            Some(RungChange::PinnedSequential { hold }) if hold == PIN_HOLD * 2
        ));
        due += PIN_HOLD * 2;
        lapse(&mut explorer, due);
        clean_batches(&mut explorer, due, PIN_TRIAL_CLEAN_BATCHES);
        assert_eq!(explorer.pin_lifted_at, Some(due));

        // Pinned again before the stretch: the back-off goes on.
        let mut soon = explorer.clone();
        assert_eq!(
            soon.note_unclean_batch(due + Duration::from_secs(60)),
            Some(RungChange::PinnedSequential { hold: PIN_HOLD * 4 })
        );

        // Pinned again after it: back to the first hold.
        let strike = due + PIN_BACKOFF_RESET_AFTER;
        explorer.note_unclean_batch(strike);
        assert_eq!(
            explorer.note_unclean_batch(strike + UNCLEAN_BURST_WINDOW),
            Some(RungChange::PinnedSequential { hold: PIN_HOLD })
        );
    }

    // A pool rebuild carries the pin, when it lapses and how far its
    // back-off has gone. A server rebuilt during its trial is carried
    // pinned with the lapse already due, so its trial starts again.
    #[test]
    fn a_rebuild_carries_the_pin_its_lapse_and_its_back_off() {
        let now = Instant::now();
        let (mut prior, pinned_at) = pinned(now);
        let due = pinned_at + PIN_HOLD;
        lapse(&mut prior, due);
        prior.note_unclean_batch(due);
        assert_eq!(prior.pin_strikes, 2);

        let mut rebuilt = ServerPipelineExplorer::seeded(Some(4), None, None);
        rebuilt.note_supports_pipelining(true);
        rebuilt.carry_pipelining_verdict(&prior);
        assert!(rebuilt.pinned_sequential());
        assert_eq!(rebuilt.current_depth(), 1);
        assert_eq!(rebuilt.pinned_until, prior.pinned_until);
        assert_eq!(rebuilt.pin_strikes, 2);
        assert_eq!(rebuilt.last_unclean_at, prior.last_unclean_at);

        // Rebuilt on the trial.
        let again = due + PIN_HOLD * 2;
        lapse(&mut prior, again);
        let mut rebuilt = ServerPipelineExplorer::seeded(Some(4), None, None);
        rebuilt.note_supports_pipelining(true);
        rebuilt.carry_pipelining_verdict(&prior);
        assert!(rebuilt.pinned_sequential());
        assert_eq!(rebuilt.pinned_until, Some(again));
        lapse(&mut rebuilt, again);
        assert_eq!(
            rebuilt.note_unclean_batch(again),
            Some(RungChange::PinnedSequential { hold: PIN_HOLD * 4 })
        );
    }

    // An outage takes every connection down, and takes down each one the
    // lanes open for as long as it lasts. The lanes that were mid-pipeline
    // when it began lose connections the server had been answering with
    // later requests on the wire: one event, one strike. Every connection
    // opened during the outage dies before the server answers anything on
    // it, however many requests the lane wrote first, and is no strike, so
    // a 90 second outage does not pin the server. A fault mid-pipeline after
    // it is a strike as any other.
    #[test]
    fn an_outage_of_dead_connections_is_no_strike_and_a_fault_mid_pipeline_is() {
        let now = Instant::now();
        let mut explorer = ServerPipelineExplorer::seeded(Some(4), None, None);
        explorer.note_supports_pipelining(true);

        // Every lane was mid-pipeline when the server went away.
        let mut strikes = Vec::new();
        for lane in 0..8u64 {
            let at = now + Duration::from_millis(50 * lane);
            if let Some(change) = explorer.note_unclean_batch_at(at, 4, true, lane) {
                strikes.push(change);
            }
        }
        assert_eq!(strikes, vec![RungChange::Dropped { from: 4, to: 2 }]);

        // For the rest of the outage each connection the lanes open dies
        // before it answers anything.
        for step in 1..=18u64 {
            let at = now + Duration::from_secs(5 * step);
            let depth = explorer.current_depth();
            for lane in 0..8u64 {
                assert_eq!(
                    explorer.note_unclean_batch_at(at, depth, false, lane),
                    None,
                    "{}s lane {lane}",
                    5 * step
                );
            }
        }
        assert!(!explorer.pinned_sequential());
        assert_eq!(explorer.current_depth(), 2);
        assert_eq!(explorer.last_unclean_at, Some(now));
        assert_eq!(explorer.hold_until, Some(now + RUNG_HOLD));

        // Back up, a connection that pipelined and then failed mid-pipeline
        // inside the hold is the second strike, and pins.
        let fault = now + Duration::from_secs(120);
        assert_eq!(
            explorer.note_unclean_batch_at(fault, 2, true, 0),
            Some(RungChange::PinnedSequential { hold: PIN_HOLD })
        );
    }

    // A trial runs on one lane. The first lane to ask after the lapse is
    // handed depth 2; every other lane is handed sequential for as long as
    // that lane lives, and its batches alone are counted.
    #[test]
    fn a_pin_trial_runs_on_one_lane_and_the_rest_stay_sequential() {
        let now = Instant::now();
        let (mut explorer, pinned_at) = pinned(now);
        let due = pinned_at + PIN_HOLD;
        lapse(&mut explorer, due);
        assert_eq!(explorer.pin_trial_lane, Some(0));

        for lane in 1..8 {
            assert_eq!(
                explorer.choose_mode_for_lane(true, lane, |holder| holder == 0),
                DownloadLaneMode::Sequential,
                "lane {lane}"
            );
        }
        assert_eq!(
            explorer.choose_mode_for_lane(true, 0, |_| panic!("the holder is not looked up")),
            DownloadLaneMode::Pipelined { depth: 2 }
        );
        // Pressure holds the trial lane sequential too, and it keeps the trial.
        assert_eq!(
            explorer.choose_mode_for_lane(false, 0, |_| true),
            DownloadLaneMode::Sequential
        );
        assert_eq!(explorer.pin_trial_lane, Some(0));
        assert_eq!(explorer.current_depth(), 1);

        // Another lane's batches, clean or not, are not the trial's: they
        // were issued before the pin.
        for _ in 0..PIN_TRIAL_CLEAN_BATCHES * 2 {
            assert_eq!(explorer.note_clean_batch(due, 2, true, 3), None);
        }
        assert_eq!(explorer.note_unclean_batch_at(due, 2, true, 3), None);
        assert_eq!(explorer.note_unclean_batch_at(due, 4, true, 0), None);
        assert!(explorer.pin_trial);
        assert_eq!(explorer.pin_trial_clean_batches, 0);

        // The trial lane's unclean batch pins the server again.
        assert_eq!(
            explorer.note_unclean_batch_at(due, 2, true, 0),
            Some(RungChange::PinnedSequential { hold: PIN_HOLD * 2 })
        );
        assert_eq!(explorer.pin_trial_lane, None);
    }

    // The trial is not lost with its lane. Once the lane is gone the next
    // lane to ask takes the trial over, with the clean batches already
    // counted, and no second lane runs it while the first lives.
    #[test]
    fn a_pin_trial_moves_to_another_lane_when_its_lane_goes() {
        let now = Instant::now();
        let (mut explorer, pinned_at) = pinned(now);
        let due = pinned_at + PIN_HOLD;
        lapse(&mut explorer, due);
        clean_batches(&mut explorer, due, PIN_TRIAL_CLEAN_BATCHES - 2);

        // Lane 0 lives: lane 5 is refused.
        assert_eq!(
            explorer.choose_mode_for_lane(true, 5, |_| true),
            DownloadLaneMode::Sequential
        );
        assert_eq!(explorer.pin_trial_lane, Some(0));
        // Lane 0 is gone: lane 5 takes the trial, and lane 6 is refused.
        assert_eq!(
            explorer.choose_mode_for_lane(true, 5, |_| false),
            DownloadLaneMode::Pipelined { depth: 2 }
        );
        assert_eq!(explorer.pin_trial_lane, Some(5));
        assert_eq!(
            explorer.choose_mode_for_lane(true, 6, |holder| holder == 5),
            DownloadLaneMode::Sequential
        );
        assert!(explorer.pin_trial);

        // A late batch from the lane that went no longer counts.
        assert_eq!(explorer.note_clean_batch(due, 2, true, 0), None);
        assert_eq!(explorer.note_clean_batch(due, 2, true, 5), None);
        assert_eq!(
            explorer.note_clean_batch(due, 2, true, 5),
            Some(RungChange::PinLifted { depth: 2 })
        );
        // Lifted for every lane.
        assert_eq!(
            explorer.choose_mode_for_lane(true, 6, |_| panic!("no trial, no holder")),
            DownloadLaneMode::Pipelined { depth: 2 }
        );
    }

    // A pin writes depth 1 through to the servers table, so a restart opens
    // the server at the shallowest pipelined rung and not at the rung that
    // just failed. The trial moves nothing on disk until it is judged, and a
    // lift writes the rung it proved.
    #[test]
    fn a_pin_saves_depth_one_and_a_restart_opens_at_the_shallowest_rung() {
        let mut explorer = ServerPipelineExplorer::seeded(Some(8), None, None);
        explorer.note_supports_pipelining(true);
        let now = Instant::now();
        let dropped = explorer.note_unclean_batch(now).expect("dropped");
        assert_eq!(dropped, RungChange::Dropped { from: 8, to: 4 });
        assert!(dropped.settles_the_rung());
        assert_eq!(explorer.take_persist_request(), Some(4));

        let pinned_at = now + UNCLEAN_BURST_WINDOW;
        let pinned = explorer.note_unclean_batch(pinned_at).expect("pinned");
        assert_eq!(pinned, RungChange::PinnedSequential { hold: PIN_HOLD });
        assert!(pinned.settles_the_rung());
        assert_eq!(explorer.take_persist_request(), Some(1));
        assert_eq!(explorer.take_persist_request(), None);

        // A restart seeds from the saved depth.
        let mut restarted = ServerPipelineExplorer::seeded(Some(1), None, None);
        restarted.note_supports_pipelining(true);
        assert_eq!(restarted.current_depth(), 2);
        assert!(!restarted.pinned_sequential());
        assert_eq!(
            restarted.choose_mode(true),
            DownloadLaneMode::Pipelined { depth: 2 }
        );

        // A clean trial writes the rung it proved.
        let due = pinned_at + PIN_HOLD;
        lapse(&mut explorer, due);
        let lifted = clean_batches(&mut explorer, due, PIN_TRIAL_CLEAN_BATCHES).expect("lifted");
        assert_eq!(lifted, RungChange::PinLifted { depth: 2 });
        assert!(lifted.settles_the_rung());
        assert_eq!(explorer.take_persist_request(), Some(2));
    }

    // A clean trial proved depth 2 and nothing deeper. A fault at the next
    // rung up while the lift is still provisional gives that rung up and
    // holds, as any unclean batch does; it does not pin. A fault back at the
    // proven rung in that time does.
    #[test]
    fn a_fault_above_the_trial_rung_after_a_lift_drops_and_holds_without_pinning() {
        let now = Instant::now();
        let (mut explorer, pinned_at) = pinned(now);
        let due = pinned_at + PIN_HOLD;
        lapse(&mut explorer, due);
        clean_batches(&mut explorer, due, PIN_TRIAL_CLEAN_BATCHES);
        assert_eq!(
            run_window(&mut explorer, due, 100_000, Duration::from_millis(100)),
            Some(RungChange::Stepped { from: 2, to: 4 })
        );

        let fault = due + Duration::from_secs(60);
        assert_eq!(
            explorer.note_unclean_batch_at(fault, 4, true, 0),
            Some(RungChange::Dropped { from: 4, to: 2 })
        );
        assert!(!explorer.pinned_sequential());
        assert_eq!(explorer.hold_until, Some(fault + RUNG_HOLD));
        assert_eq!(explorer.pin_strikes, 1);
        assert_eq!(
            explorer.choose_mode(true),
            DownloadLaneMode::Pipelined { depth: 2 }
        );

        // The proven rung failing too is the trial's verdict undone.
        assert_eq!(
            explorer.note_unclean_batch_at(fault + UNCLEAN_BURST_WINDOW, 2, true, 0),
            Some(RungChange::PinnedSequential { hold: PIN_HOLD * 2 })
        );
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
                explorer.note_response(
                    now,
                    2,
                    100_000,
                    Duration::from_millis(100),
                    false,
                    false,
                    true
                ),
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
                explorer.note_response(
                    now,
                    8,
                    400_000,
                    Duration::from_millis(100),
                    true,
                    false,
                    true
                ),
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
            ServerPipelineExplorer::seeded(None, Some(Duration::from_millis(400)), None)
                .current_depth(),
            4
        );
        // The good band opens four deep from 50 ms, two deep below it.
        assert_eq!(
            ServerPipelineExplorer::seeded(None, Some(Duration::from_millis(399)), None)
                .current_depth(),
            4
        );
        assert_eq!(
            ServerPipelineExplorer::seeded(None, Some(Duration::from_millis(100)), None)
                .current_depth(),
            4
        );
        assert_eq!(
            ServerPipelineExplorer::seeded(None, Some(Duration::from_millis(50)), None)
                .current_depth(),
            4
        );
        assert_eq!(
            ServerPipelineExplorer::seeded(None, Some(Duration::from_millis(49)), None)
                .current_depth(),
            2
        );
        assert_eq!(
            ServerPipelineExplorer::seeded(None, Some(Duration::from_millis(30)), None)
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

    // A carried-over article transfer time turns the seed into the real
    // bandwidth-delay answer, which the coarse band cannot reach: 100 ms is
    // deep inside the "good" band, and the band would start such a link two
    // deep when it can use eight.
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

    // A server seeded from nothing but the probe's latency.
    fn guessed_from_probe(probe_ms: u64) -> ServerPipelineExplorer {
        let mut explorer =
            ServerPipelineExplorer::seeded(None, Some(Duration::from_millis(probe_ms)), None);
        explorer.note_supports_pipelining(true);
        explorer
    }

    // Only an opening rung the band guessed, deeper than 2, is marked. A
    // proven rung, a complete model, or a band that says 2 is not a guess.
    #[test]
    fn only_a_band_guessed_opening_deeper_than_two_is_marked_a_guess() {
        for (probe_ms, depth) in [(100, 4), (400, 4), (900, 8)] {
            let explorer = guessed_from_probe(probe_ms);
            assert_eq!(explorer.current_depth(), depth, "probe {probe_ms} ms");
            assert!(explorer.opened_on_a_guess, "probe {probe_ms} ms");
        }
        let near = guessed_from_probe(30);
        assert_eq!(near.current_depth(), 2);
        assert!(!near.opened_on_a_guess);

        let proven =
            ServerPipelineExplorer::seeded(Some(4), Some(Duration::from_millis(100)), None);
        assert!(!proven.opened_on_a_guess);
        let modelled = ServerPipelineExplorer::seeded(
            None,
            Some(Duration::from_millis(100)),
            Some(Duration::from_millis(25)),
        );
        assert_eq!(modelled.current_depth(), 8);
        assert!(!modelled.opened_on_a_guess);
        assert!(!ServerPipelineExplorer::seeded(None, None, None).opened_on_a_guess);
    }

    // A 100 ms probe opens at 4. The opening window takes that rung clean
    // and its model asks for 8: the explorer jumps there untried and the next
    // clean window at 8 settles it, which is the first depth worth saving.
    #[test]
    fn a_guessed_opening_taken_clean_jumps_on_the_model_and_settles() {
        let mut explorer = guessed_from_probe(100);
        let now = Instant::now();
        assert_eq!(explorer.current_depth(), 4);

        assert_eq!(
            run_window(&mut explorer, now, 768_000, Duration::from_millis(25)),
            Some(RungChange::Stepped { from: 4, to: 8 })
        );
        assert!(!explorer.opened_on_a_guess);
        assert!(!explorer.on_trial);
        assert!(explorer.settle_pending);
        assert_eq!(
            run_window(&mut explorer, now, 768_000, Duration::from_millis(25)),
            Some(RungChange::Kept { depth: 8 })
        );
        assert_eq!(explorer.take_persist_request(), Some(8));

        // A model that asks for the guessed rung keeps it, and the first
        // climb is over once the next window agrees; nothing is saved.
        let mut explorer = guessed_from_probe(100);
        assert_eq!(
            run_window(&mut explorer, now, 768_000, Duration::from_millis(60)),
            None
        );
        assert_eq!(explorer.target_rung(), 4);
        assert_eq!(
            run_window(&mut explorer, now, 768_000, Duration::from_millis(60)),
            None
        );
        assert_eq!(explorer.current_depth(), 4);
        assert!(!explorer.first_climb_pending);
        assert_eq!(explorer.persisted_depth, None);

        // A model that reads exactly 4 at the guessed 4 gets no headroom and
        // stays: it does not jump to 8 untried.
        let mut explorer = guessed_from_probe(100);
        assert_eq!(
            run_window(&mut explorer, now, 768_000, Duration::from_millis(34)),
            None
        );
        assert_eq!(explorer.physical_target_depth(), Some(4));
        assert_eq!(explorer.target_rung(), 4);
        assert!(!explorer.settle_pending);
        assert_eq!(
            run_window(&mut explorer, now, 768_000, Duration::from_millis(34)),
            None
        );
        assert_eq!(explorer.current_depth(), 4);
        assert!(!explorer.first_climb_pending);
        assert_eq!(explorer.persisted_depth, None);
    }

    // A guessed rung the server refuses before any window has measured it
    // costs one rung and nothing more: no hold, no count toward the pin, and
    // the climb back is a trial rather than a jump on the model.
    #[test]
    fn an_unclean_batch_at_a_guessed_rung_drops_it_without_a_hold() {
        let mut explorer = guessed_from_probe(100);
        let now = Instant::now();
        assert_eq!(
            explorer.note_unclean_batch(now),
            Some(RungChange::Dropped { from: 4, to: 2 })
        );
        assert_eq!(explorer.current_depth(), 2);
        assert_eq!(explorer.hold_until, None);
        assert_eq!(explorer.last_unclean_at, None);
        assert!(!explorer.opened_on_a_guess);

        // The opening window at 2 makes no jump, though its model says 8.
        assert_eq!(
            run_window(&mut explorer, now, 768_000, Duration::from_millis(25)),
            None
        );
        assert_eq!(explorer.target_rung(), 8);
        assert_eq!(explorer.current_depth(), 2);
        // The next window takes the baseline and climbs on trial.
        assert_eq!(
            run_window(&mut explorer, now, 768_000, Duration::from_millis(25)),
            Some(RungChange::Stepped { from: 2, to: 8 })
        );
        assert!(explorer.on_trial);
        assert!(!explorer.settle_pending);
    }

    // A burst at a guessed rung costs that one rung and nothing more: the
    // other lanes' batches, whether issued at the dropped rung or at the
    // rung below it, change nothing, and no depth is left to write through.
    // The first unclean batch past the burst window is the first one
    // counted: it drops and holds as any other, and only one after that
    // pins.
    #[test]
    fn a_second_unclean_batch_after_a_dropped_guess_does_not_pin() {
        let mut explorer = guessed_from_probe(100);
        let now = Instant::now();
        assert_eq!(
            explorer.note_unclean_batch_at(now, 4, true, 0),
            Some(RungChange::Dropped { from: 4, to: 2 })
        );
        for lane in 1..16u64 {
            let at = now + Duration::from_millis(100 * lane);
            let depth = if lane % 2 == 0 { 4 } else { 2 };
            assert_eq!(
                explorer.note_unclean_batch_at(at, depth, true, 0),
                None,
                "lane {lane}"
            );
        }
        assert_eq!(explorer.current_depth(), 2);
        assert_eq!(explorer.hold_until, None);
        assert_eq!(explorer.last_unclean_at, None);

        let next = now + UNCLEAN_BURST_WINDOW;
        assert_eq!(
            explorer.note_unclean_batch(next),
            Some(RungChange::Dropped { from: 2, to: 1 })
        );
        assert!(!explorer.pinned_sequential());
        assert_eq!(explorer.hold_until, Some(next + RUNG_HOLD));
        assert_eq!(explorer.last_unclean_at, Some(next));
        assert_eq!(
            explorer.note_unclean_batch(next + UNCLEAN_BURST_WINDOW),
            Some(RungChange::PinnedSequential { hold: PIN_HOLD })
        );
    }

    // Once a measured, fed window has closed clean at the guessed rung the
    // server has taken it, and an unclean batch is treated as at any rung:
    // dropped, held, and counted.
    #[test]
    fn an_unclean_batch_after_the_guessed_rung_was_taken_holds() {
        let mut explorer = guessed_from_probe(100);
        let now = Instant::now();
        assert_eq!(
            run_window(&mut explorer, now, 768_000, Duration::from_millis(60)),
            None
        );
        assert!(!explorer.opened_on_a_guess);
        assert_eq!(
            explorer.note_unclean_batch(now),
            Some(RungChange::Dropped { from: 4, to: 2 })
        );
        assert_eq!(explorer.hold_until, Some(now + RUNG_HOLD));
        assert_eq!(explorer.last_unclean_at, Some(now));
    }

    // The no-hold drop covers every band-guessed opening above 2, so the
    // moderate and slow bands get it too; a proven rung never does.
    #[test]
    fn every_band_guess_drops_without_a_hold_and_a_proven_rung_holds() {
        let now = Instant::now();
        for (probe_ms, from, to) in [(400, 4, 2), (900, 8, 4)] {
            let mut explorer = guessed_from_probe(probe_ms);
            assert_eq!(
                explorer.note_unclean_batch(now),
                Some(RungChange::Dropped { from, to }),
                "probe {probe_ms} ms"
            );
            assert_eq!(explorer.hold_until, None, "probe {probe_ms} ms");
            assert_eq!(explorer.last_unclean_at, None, "probe {probe_ms} ms");
        }

        let mut proven =
            ServerPipelineExplorer::seeded(Some(4), Some(Duration::from_millis(100)), None);
        proven.note_supports_pipelining(true);
        assert_eq!(
            proven.note_unclean_batch(now),
            Some(RungChange::Dropped { from: 4, to: 2 })
        );
        assert_eq!(proven.hold_until, Some(now + RUNG_HOLD));
        assert_eq!(proven.last_unclean_at, Some(now));
    }

    // The first leases every lane takes are booked before any response has
    // come back, so they run at whatever depth the seed chose. Discarding
    // their responses — which is what an exact-depth match does the moment the
    // explorer moves — left the window that is supposed to pick a depth
    // unable to fill from the lanes still draining those leases.
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
                deep.note_response(
                    now,
                    2,
                    750_000,
                    Duration::from_millis(125),
                    true,
                    false,
                    true
                ),
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
                shallow.note_response(
                    now,
                    8,
                    100_000,
                    Duration::from_millis(100),
                    true,
                    false,
                    true
                ),
                None
            );
        }
        assert_eq!(shallow.current_depth(), 2);
    }

    // After a step the lanes still hold leases booked at the old rung, and
    // their responses keep arriving at the old depth until each lane adopts
    // one booked after the step. The trial window opens with the first
    // response at the new rung and closes on a full window of them, however
    // many old-rung responses arrive first.
    #[test]
    fn the_trial_window_opens_with_the_first_response_at_the_new_rung() {
        let mut explorer = proven_at_two(300, 200);
        let now = Instant::now();
        open_window(&mut explorer, now);
        assert_eq!(
            run_window(&mut explorer, now, 100_000, Duration::from_millis(100)),
            Some(RungChange::Stepped { from: 2, to: 4 })
        );
        // The next refill is sized from the rung the explorer now runs.
        assert_eq!(
            explorer.choose_mode(true),
            DownloadLaneMode::Pipelined { depth: 4 }
        );

        // Seventy-two responses from the leases already booked two deep,
        // one of them from a lane left short: more than two full windows,
        // and none of them closes the trial or voids it.
        for response in 0..72 {
            assert_eq!(
                explorer.note_response(
                    now,
                    2,
                    100_000,
                    Duration::from_millis(100),
                    true,
                    false,
                    response != 40,
                ),
                None
            );
        }
        assert_eq!(explorer.current_depth(), 4);
        assert_eq!(explorer.window_rung_responses, 0);
        assert!(!explorer.window_starved);

        // The first window of responses at the new rung judges it.
        let responses = explorer.window_target_responses();
        for _ in 1..responses {
            assert_eq!(
                explorer.note_response(
                    now,
                    4,
                    100_000,
                    Duration::from_millis(100),
                    true,
                    false,
                    true
                ),
                None
            );
        }
        assert_eq!(
            explorer.note_response(
                now,
                4,
                100_000,
                Duration::from_millis(100),
                true,
                false,
                true
            ),
            Some(RungChange::Kept { depth: 4 })
        );
    }

    // The opening windows are short on purpose: a server with no measurement
    // has no depth target at all, and a download of a few hundred articles
    // would otherwise spend most of itself waiting for the first window.
    #[test]
    fn the_opening_windows_are_short_and_then_the_full_window_applies() {
        let mut explorer = explorer(300, 50);
        assert_eq!(explorer.window_target_responses(), 8);
        let now = Instant::now();

        run_window(&mut explorer, now, 100_000, Duration::from_millis(100));
        assert_eq!(explorer.window_target_responses(), 8);
        run_window(&mut explorer, now, 100_000, Duration::from_millis(100));
        assert_eq!(explorer.window_target_responses(), 8);
        run_window(&mut explorer, now, 200_000, Duration::from_millis(100));
        assert_eq!(explorer.window_target_responses(), RUNG_WINDOW_RESPONSES);
    }

    // The persisted column must only ever hold a rung the explorer settled
    // on, and a settled rung must only be written once.
    #[test]
    fn only_a_settled_rung_is_offered_for_persistence() {
        let mut explorer = explorer(300, 50);
        let now = Instant::now();
        // Seeded from nothing, so the shallowest rung is already worth saving.
        assert_eq!(explorer.take_persist_request(), Some(2));
        assert_eq!(explorer.take_persist_request(), None);

        let change = run_window(&mut explorer, now, 100_000, Duration::from_millis(100));
        assert_eq!(change, Some(RungChange::Stepped { from: 2, to: 8 }));
        assert_eq!(explorer.current_depth(), 8);
        // The step up waits for its settle window; the caller must not write
        // it through yet. One window that settles the rung is enough — a
        // proven depth is worth saving as soon as it is proven, not several
        // windows later.
        assert!(!change.expect("stepped").settles_the_rung());
        let change = run_window(&mut explorer, now, 200_000, Duration::from_millis(100));
        assert_eq!(change, Some(RungChange::Kept { depth: 8 }));
        assert_eq!(explorer.take_persist_request(), Some(8));
        assert_eq!(explorer.take_persist_request(), None);

        // Losing the rung offers the lower value, so a restart cannot resume
        // at a depth this server has since failed.
        explorer.note_unclean_batch(now);
        assert_eq!(explorer.current_depth(), 4);
        assert_eq!(explorer.take_persist_request(), Some(4));
    }

    // A server seeded deep from a previous run keeps that rung: the explorer
    // has nothing to walk toward until it has measured the link itself.
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

    // Responses that belong to another rung — a batch already in flight when
    // the depth moved — must not be folded into the window under test.
    #[test]
    fn responses_from_a_stale_rung_are_discarded() {
        let mut explorer = explorer(300, 50);
        let now = Instant::now();
        for _ in 0..RUNG_WINDOW_RESPONSES * 2 {
            assert_eq!(
                explorer.note_response(
                    now,
                    8,
                    100_000,
                    Duration::from_millis(100),
                    true,
                    false,
                    true
                ),
                None
            );
        }
        assert_eq!(explorer.current_depth(), 2);
    }

    // A step down toward a shallower target is a measured decision, not a
    // trial: it becomes the baseline immediately rather than being re-judged.
    #[test]
    fn a_step_down_toward_a_shallower_target_is_not_put_on_trial() {
        let mut explorer = ServerPipelineExplorer::seeded(Some(8), None, None);
        explorer.note_supports_pipelining(true);
        let now = Instant::now();
        // Big articles on a near server: one request in flight covers the
        // wait, and the headroom request asks for rung 4.
        explorer.note_latency(Duration::from_millis(100));
        explorer.note_transfer(Duration::from_millis(400));
        assert_eq!(explorer.target_rung(), 4);

        // The first decision goes to the target in one move — down as well as
        // up.
        open_window(&mut explorer, now);
        assert_eq!(
            run_window(&mut explorer, now, 100_000, Duration::from_millis(100)),
            Some(RungChange::Stepped { from: 8, to: 4 })
        );
        assert_eq!(explorer.current_depth(), 4);
        // Worse throughput at the shallower rung does not bounce it back up:
        // the target, not the comparison, is what moved it, so there is no
        // trial to lose.
        assert_eq!(
            run_window(&mut explorer, now, 10_000, Duration::from_millis(100)),
            None
        );
        assert_eq!(explorer.current_depth(), 4);
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
