// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::HashMap;
use std::time::{Duration, Instant};

pub(crate) use weaver_server_composition::servers::latency_band::LatencyBand;

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
pub const MAX_TARGET_DEPTH: u32 = 8;
// Requests added to the bandwidth-delay depth before the explorer picks the
// rung it walks toward; see `target_rung`.
const TARGET_HEADROOM_DEPTH: u8 = 1;

// Clean responses observed at one rung before the explorer acts on what it
// measured there. Also bounds rung changes to one per window.
pub const RUNG_WINDOW_RESPONSES: u64 = 32;

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
pub const LATENCY_FOLD_SAMPLES: u32 = RUNG_WARMUP_WINDOW_RESPONSES as u32;

// How far apart two windows' throughput may be and still count as the same
// on a shared link. A step up is reverted only when it falls short of the
// rung below it by more than this; a trial inside the band is no worse than
// its baseline and is kept. On a link-bound server every rung reads the same
// throughput, so a bar the trial had to clear upward would revert a rung that
// cost nothing.
const RUNG_THROUGHPUT_NOISE_RATIO: f64 = 1.05;

// How long a server is left alone after a revert or an unclean batch.
pub const RUNG_HOLD: Duration = Duration::from_secs(10 * 60);

// Unclean batches on one server that come this soon after the one that cost
// a rung are the same event and count once. A burst of connection resets
// tears down every lane of the server within a second or two, and each lane
// reports its own unclean batch.
pub const UNCLEAN_BURST_WINDOW: Duration = Duration::from_secs(30);

// How long a server stays pinned to sequential before it is given one trial
// at depth 2. Each unclean trial pins it again for twice as long, up to
// `PIN_HOLD_MAX`.
pub const PIN_HOLD: Duration = RUNG_HOLD;
pub(crate) const PIN_HOLD_MAX: Duration = Duration::from_secs(2 * 60 * 60);

// Clean, complete pipelined batches at depth 2 that end a lapsed pin's
// trial and lift the pin. Counted per batch, not per rung window, so a
// server that gets only a few articles a job still finishes its trial.
pub const PIN_TRIAL_CLEAN_BATCHES: u8 = 4;

// The depth the one trial lane runs at: the shallowest pipelined rung.
const PIN_TRIAL_DEPTH: u8 = RUNGS[1];

// A server that goes this long after its pin was lifted without being
// pinned again starts the back-off over at `PIN_HOLD`: it has run clean for
// as long as the longest pin would have held it. Until then the lift is
// provisional, and an unclean batch pins it again at the next step.
pub const PIN_BACKOFF_RESET_AFTER: Duration = PIN_HOLD_MAX;

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
pub fn bandwidth_delay_depth(rtt: Duration, article_transfer: Duration) -> u32 {
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
pub fn article_transfer_time(median_body_bytes: u64, throughput_bps: f64) -> Option<Duration> {
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
pub enum DownloadLaneMode {
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

    pub fn depth(self) -> u8 {
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
pub enum LaneParkReason {
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
pub enum RungChange {
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
    pub fn settles_the_rung(self) -> bool {
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
pub struct ServerPipelineExplorer {
    supports_pipelining: bool,
    pub pinned_sequential: bool,
    pub latency: Option<Duration>,
    pub transfer: Option<Duration>,
    pub current_depth: u8,
    // Rung the current window is being compared against, and its throughput.
    pub baseline_depth: Option<u8>,
    pub baseline_throughput_bps: Option<f64>,
    // Set while the current rung is a step up still on trial.
    pub on_trial: bool,
    // Set until the explorer has moved to — or found itself already sitting
    // on — the rung its own measurements ask for. The first move goes
    // straight there; every later one walks the ladder a rung at a time.
    pub first_climb_pending: bool,
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
    pub opened_on_a_guess: bool,
    // Set after the first climb moved to the modelled rung without a trial,
    // until a clean window at that rung settles it as the baseline.
    pub settle_pending: bool,
    // Short windows left before the full window length applies.
    warmup_windows_left: u8,
    // Every response folded into this window, at the current rung or below.
    pub window_responses: u64,
    // Shortest status-line wait any lane measured on a request it issued
    // alone during this window, split by whether the connection had already
    // delivered an article before. The minimum is the round trip; the mean of
    // the same samples carries server think time, a re-authentication after
    // an idle spell, or whatever else queued ahead of the status line, and
    // each of those asks for depth the link cannot use.
    pub window_latency_warm_min: Option<Duration>,
    pub window_latency_cold_min: Option<Duration>,
    // Status-line waits held since the last fold, warm and cold.
    pub window_latency_samples: u32,
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
    pub window_rung_responses: u64,
    window_rung_bytes: u64,
    window_rung_elapsed: Duration,
    // A response at the rung arrived with its lane holding less than a
    // full rung of articles: the lane waited on its next lease, and the
    // window timed that wait as well as the server.
    pub window_starved: bool,
    // Ring of recent decoded body sizes; the depth formula takes its median.
    body_bytes: [u64; BODY_SIZE_SAMPLES],
    body_bytes_len: usize,
    body_bytes_next: usize,
    // `median body bytes / per-lane read rate`, taken as one pair at each
    // window close and blended across windows. The depth formula's divisor.
    pub modelled_transfer: Option<Duration>,
    pub hold_until: Option<Instant>,
    // The last unclean batch that counted toward a pin: the one that cost a
    // held rung or pinned. A second one inside `RUNG_HOLD` of it pins.
    pub last_unclean_at: Option<Instant>,
    // The last unclean batch acted on in any way, a guessed rung's drop
    // included. Batches inside `UNCLEAN_BURST_WINDOW` after it do not count.
    unclean_burst_at: Option<Instant>,
    // When the current pin lapses into a trial. Kept through the trial, so a
    // pool rebuilt during one carries a pin that is already due.
    pub pinned_until: Option<Instant>,
    // Pins in the current back-off; the next pin holds `pin_hold(strikes)`.
    pub pin_strikes: u32,
    // The server is on the one trial a lapsed pin gets: one lane runs at
    // depth 2 and every other lane stays sequential, so `current_depth`
    // stays 1 until the trial is judged.
    pub pin_trial: bool,
    // The lane running the trial. `None` until a lane asks for its mode
    // after the lapse, and taken over by the next lane to ask once this one
    // is gone.
    pub pin_trial_lane: Option<u64>,
    // Clean batches at depth 2 counted toward `PIN_TRIAL_CLEAN_BATCHES` on
    // the current trial.
    pub pin_trial_clean_batches: u8,
    // When a clean trial last lifted a pin.
    pub pin_lifted_at: Option<Instant>,
    // Depth the servers table already holds, so a kept rung only writes once.
    pub persisted_depth: Option<u8>,
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
    pub fn seeded(
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
    pub fn carry_pipelining_verdict(&mut self, prior: &Self) {
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
    pub fn take_persist_request(&mut self) -> Option<u8> {
        if self.persisted_depth == Some(self.current_depth) {
            return None;
        }
        self.persisted_depth = Some(self.current_depth);
        Some(self.current_depth)
    }

    pub fn current_depth(&self) -> u8 {
        self.current_depth
    }

    // The server is on the one trial a lapsed pin gets.
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub(crate) fn on_pin_trial(&self) -> bool {
        self.pin_trial
    }

    // The lane running the trial, if one has taken it.
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub(crate) fn pin_trial_lane(&self) -> Option<u64> {
        self.pin_trial_lane
    }

    // A pipelining server whose pin has lapsed into its trial.
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub(crate) fn on_a_lapsed_pin_trial(now: Instant) -> Self {
        let mut explorer = Self::seeded(Some(2), None, None);
        explorer.note_supports_pipelining(true);
        explorer.pin(now);
        explorer.begin_pin_trial();
        explorer
    }

    pub fn latency(&self) -> Option<Duration> {
        self.latency
    }

    pub(crate) fn transfer(&self) -> Option<Duration> {
        self.transfer
    }

    pub(crate) fn latency_band(&self) -> Option<LatencyBand> {
        self.latency.map(LatencyBand::from_latency)
    }

    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub fn pinned_sequential(&self) -> bool {
        self.pinned_sequential
    }

    // What the health view reports as the pin: held until a clean trial
    // lifts it, so a server on its trial still reads pinned.
    pub(crate) fn pin_held(&self) -> bool {
        self.pinned_sequential || self.pin_trial
    }

    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub fn note_latency(&mut self, sample: Duration) {
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
    pub fn note_latency_sample(&mut self, sample: Duration, cold: bool) {
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

    pub fn note_transfer(&mut self, sample: Duration) {
        self.transfer = Some(blend(self.transfer, sample));
    }

    pub fn note_supports_pipelining(&mut self, supports_pipelining: bool) {
        self.supports_pipelining = supports_pipelining;
    }

    // Median of the recent decoded body sizes, or `None` before any landed.
    pub fn median_body_bytes(&self) -> Option<u64> {
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
    pub fn modelled_article_transfer(&self) -> Option<Duration> {
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
    pub fn physical_target_depth(&self) -> Option<u8> {
        let latency = self.latency?;
        let transfer = self.effective_article_transfer()?;
        let depth = bandwidth_delay_depth(latency, transfer);
        Some(depth.clamp(MIN_TARGET_DEPTH, MAX_TARGET_DEPTH) as u8)
    }

    pub fn note_body_bytes(&mut self, payload_bytes: u64) {
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
    pub fn target_rung(&self) -> u8 {
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

    pub fn choose_mode(&self, pressure_clear: bool) -> DownloadLaneMode {
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
    pub fn choose_mode_for_lane(
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
    pub fn note_response(
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
    pub fn window_target_responses(&self) -> u64 {
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
    pub fn note_clean_batch(
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
    pub fn note_unclean_batch(&mut self, now: Instant) -> Option<RungChange> {
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
    pub fn note_unclean_batch_at(
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
