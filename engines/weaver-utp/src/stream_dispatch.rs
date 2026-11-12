// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
// Lifted from librqbit-utp 0.7.0 (Apache-2.0), Copyright (c) the librqbit authors.
// Modified by the Weaver authors, 2026-11-10.

#[cfg(test)]
mod tests;

use std::{
    future::Future,
    io::IoSlice,
    net::SocketAddr,
    num::NonZeroUsize,
    pin::Pin,
    sync::{Arc, atomic::Ordering},
    task::Poll,
    time::Duration,
};

use tokio::time::{Instant, Sleep};
use tokio_util::sync::CancellationToken;
use tracing::{Level, debug, debug_span, event, trace, trace_span};

use crate::{
    DatagramSend, Error,
    congestion::{DelaySample, ledbat::Ledbat},
    constants::{
        ACK_DELAY, IMMEDIATE_ACK_EVERY_RMSS, RECOVERY_TRACING_LOG_LEVEL, RTTE_TRACING_LOG_LEVEL,
        SEQUENCE_HALF_SPACE, SYNACK_RESEND_INTERNAL, UTP_HEADER, calc_pipe_expiry,
    },
    delay::DelayTracker,
    ingress::{Event as IngressEvent, Receiver as IngressReceiver},
    mtu::{SegmentSizes, SegmentSizesConfig, packet_mtu},
    pacer::Pacer,
    raw::{Type, UtpHeader},
    recovery::Recovery,
    rtte::RttEstimator,
    seq_nr::SeqNr,
    socket::{RetirementGuard, SocketState, ValidatedSocketOpts},
    spawn_utils::task_with_cancel,
    stats::Counter,
    stream::UtpStream,
    stream_rx::{AssemblerAddRemoveResult, UserRx},
    stream_tx::{UserTx, UtpStreamWriteHalf},
    stream_tx_segments::{OnAckResult, PopExpiredProbe, Segments},
    traits::{Transport, UtpEnvironment},
    utils::update_optional_waker,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VirtualSocketState {
    SynReceived,
    SynAckSent { count: usize },

    Established,

    // The remote sent EOF; our writer remains open.
    CloseWait { remote_fin: SeqNr },

    // We sent FIN, not yet ACKed
    FinWait1 { our_fin: SeqNr },

    // Our fin was ACKed
    FinWait2,

    // Both sides closed, we are waiting for final ACK.
    // After this we just kill the socket.
    LastAck { our_fin: SeqNr, remote_fin: SeqNr },

    // We are fully done, no more packets to be sent or received.
    Closed,
}

impl VirtualSocketState {
    fn is_closed(&self, wait_for_last_ack: bool) -> bool {
        match self {
            VirtualSocketState::Closed => true,
            VirtualSocketState::LastAck { .. } if !wait_for_last_ack => true,
            _ => false,
        }
    }

    fn name(&self) -> &'static str {
        match self {
            VirtualSocketState::SynReceived => "syn-received",
            VirtualSocketState::SynAckSent { .. } => "syn-ack-sent",
            VirtualSocketState::Established => "established",
            VirtualSocketState::CloseWait { .. } => "close-wait",
            VirtualSocketState::FinWait1 { .. } => "fin-wait-1",
            VirtualSocketState::FinWait2 => "fin-wait-2",
            VirtualSocketState::LastAck { .. } => "last-ack",
            VirtualSocketState::Closed => "closed",
        }
    }

    fn transition_to_fin_wait_1(&mut self, our_fin: SeqNr) -> bool {
        match *self {
            VirtualSocketState::Established
            | VirtualSocketState::SynReceived
            | VirtualSocketState::SynAckSent { .. } => {
                trace!("state {} -> fin-wait-1", self.name());
                *self = VirtualSocketState::FinWait1 { our_fin }
            }
            VirtualSocketState::CloseWait { remote_fin } => {
                trace!("state close-wait -> last-ack");
                *self = VirtualSocketState::LastAck {
                    our_fin,
                    remote_fin,
                }
            }
            _ => return false,
        };
        true
    }

    // True if we are not sending any more data.
    fn is_local_fin_or_later(&self) -> bool {
        match self {
            VirtualSocketState::SynReceived
            | VirtualSocketState::SynAckSent { .. }
            | VirtualSocketState::Established
            | VirtualSocketState::CloseWait { .. } => false,
            VirtualSocketState::Closed
            | VirtualSocketState::FinWait1 { .. }
            | VirtualSocketState::FinWait2
            | VirtualSocketState::LastAck { .. } => true,
        }
    }

    fn our_fin_if_unacked(&self) -> Option<SeqNr> {
        match *self {
            VirtualSocketState::FinWait1 { our_fin }
            | VirtualSocketState::LastAck { our_fin, .. } => Some(our_fin),
            _ => None,
        }
    }

    fn is_remote_fin_or_later(&self) -> bool {
        matches!(
            self,
            VirtualSocketState::CloseWait { .. }
                | VirtualSocketState::LastAck { .. }
                | VirtualSocketState::Closed
        )
    }
}

// An equivalent of a TCP socket for uTP.
struct VirtualSocket<T, Env> {
    state: VirtualSocketState,
    socket: Arc<SocketState<T, Env>>,
    transport: Option<Arc<T>>,
    sender: Option<Box<dyn DatagramSend>>,
    socket_created: Instant,
    socket_opts: ValidatedSocketOpts,

    remote: SocketAddr,
    conn_id_send: SeqNr,

    // Triggers delay-based operations
    timers: Timers,

    // One-way delay both ways, and the gap reported back in every header.
    delay: DelayTracker,
    last_remote_window: u32,

    // The seq_nr to use in outgoing packets. It gets incremented AFTER the
    // following cases:
    // 1. Initial SYN is sent.
    // 2. ST_DATA is sent
    // ST_FIN takes this seq_nr without advancing it, so every ST_STATE after
    // our FIN carries the FIN's seq_nr.
    seq_nr: SeqNr,

    // How many consecutive RTO retransmissions happened.
    rto_retransmissions: usize,

    // No segment before this one is resent by recovery again: each packet is
    // fast-resent at most once, and the head the retransmission timer
    // resends is not fast-resent after it (libtorrent v2.0.15
    // utp_stream.cpp m_fast_resend_seq_nr: 1235, 1355, 2815-2827 and
    // 3550-3551). The
    // timer path alone may send a packet again after that, and gives up
    // once a packet went out `num_resends` times.
    fast_resend_seq_nr: SeqNr,

    // The remote has sent a packet other than its SYN: it is there, not a
    // spoofed address. A stream that is not fails on its first timeout.
    confirmed: bool,
    // When the remote's last valid packet arrived.
    last_remote_packet: Instant,
    // Sends of our FIN.
    fin_sends: usize,
    // Since when nothing of ours has been in flight, the window decaying
    // once per timeout that passes so.
    idle_since: Option<Instant>,

    // This is used to rewind state back for retransmission.
    // All packets after this number will get resent (ST_DATA, ST_FIN).
    last_sent_seq_nr: SeqNr,
    // The highest seq_nr we ever sent, kept through a rewind: an ACK past
    // it acks nothing we sent.
    highest_sent_seq_nr: SeqNr,
    // The packet resent as this flight's tail-loss probe, until it is
    // acknowledged. One probe per flight.
    tail_probe_seq_nr: Option<SeqNr>,

    // Last remote sequence number that we fully processed.
    last_consumed_remote_seq_nr: SeqNr,

    // Last ACK that we sent out. This is different from "last_consumed_remote_seq_nr" because we don't ACK
    // every packet. This must be <= last_consumed_remote_seq_nr.
    last_sent_ack_nr: SeqNr,

    last_sent_window: u32,

    // How many bytes we have consumed but not sent an ACK yet.
    // Used for immediate ACKs on 2 * RMSS threshold.
    consumed_but_unacked_bytes: usize,

    // Incoming queue. The main UDP socket writes here, and we need to consume these
    // as fast as possible.
    rx: IngressReceiver,
    user_rx: UserRx,

    // The user ppayload that we haven't yet segmented into uTP messages.
    // This is what "UtpStream::poll_write" writes to.
    user_tx: Arc<UserTx>,
    // Unacked segments. Ready to send or retransmit.
    user_tx_segments: Segments,

    segment_sizes: SegmentSizes,

    rtte: RttEstimator,
    congestion_controller: Ledbat,
    // When the data the window allows goes out.
    pacer: Pacer,

    recovery: Recovery,

    this_poll: ThisPoll,

    env: Env,

    drop_guard: RetirementGuard,
    parent_span: Option<tracing::Span>,
}

// Updated on every poll
struct ThisPoll {
    // Set here once per poll for consistent reproducible calculations.
    now: Instant,

    // Temp buffer used for writing messages to socket, so that we dno't alloc and zero
    // it every time.
    tmp_buf: Vec<u8>,

    // Set if the transport can't send anything this poll.
    transport_pending: bool,

    restart: bool,

    unsegmented_data: usize,
}

#[derive(Default)]
struct ProcessIncomingMessageResult {
    on_ack_result: OnAckResult,
}

impl ProcessIncomingMessageResult {
    pub(crate) fn update(&mut self, other: &ProcessIncomingMessageResult) {
        self.on_ack_result.update(&other.on_ack_result);
    }
}

// We can't make these methods as it borrows a bunch of fields, and this whole
// section is run while user_tx_segments.iter_mut() is already borrowed.
macro_rules! on_packet_sent {
    ($self:expr, $header:expr) => {{
        // Each packet sent can act as ACK so update related state.
        $self.last_sent_ack_nr = $header.ack_nr;
        $self.last_sent_window = $header.wnd_size;
        $self.consumed_but_unacked_bytes = 0;
        $self.timers.ack_delay_timer.turn_off("ACK sent");
    }};
}

// This macro exists to prevent borrow checker errors as the code inside uses a bunch of
// fields on "self" but the caller also uses a bunch of fields on self.
macro_rules! send_data {
    ($self:expr, $cx:expr, $header:expr, $segment_iter_item:expr) => {{
        // A closure so that we can use "?" inside the block.
        let mut tmp_closure = || {
            $header.set_type(Type::Data);
            $header.seq_nr = $segment_iter_item.seq_nr();
            $header.timestamp_microseconds =
                ($self.env.now() - $self.socket_created).as_micros() as u32;
            $header.timestamp_difference_microseconds = $self.delay.reply_us();

            let mut h = [0u8; UTP_HEADER as usize];
            let hlen = $header.serialize(&mut h)?;

            // Sent from the ring in place; the writer waits on the lock only
            // for the length of one send.
            {
                let g = $self.user_tx.ring.lock();
                let offset = $segment_iter_item.payload_offset();
                let plen = $segment_iter_item.payload_size();
                let (first, second) = g.as_slices();
                let [first, second] =
                    crate::utils::prepare_2_ioslices(first, second, offset, plen)?;
                let bufs = [
                    IoSlice::new(&h[..hlen]),
                    IoSlice::new(first),
                    IoSlice::new(second),
                ];
                $self.this_poll.transport_pending = $self
                    .socket
                    .try_poll_send_to_vectored(
                        $self.transport.as_deref().ok_or(Error::DispatcherDead)?,
                        $self.sender.as_deref_mut(),
                        $cx,
                        &bufs,
                        $self.remote,
                        hlen + plen,
                    )
                    .map_err(Error::Send)?;
            }

            if $self.this_poll.transport_pending {
                return Ok(false);
            }

            // This time better be as precise as possible as we are using it to
            // calculate recovery pipe (estimate of how many packets are in transit).
            $segment_iter_item.on_sent($self.env.now(), &$self.socket.stats);
            on_packet_sent!($self, $header);

            if $segment_iter_item.seq_nr().is_after($self.last_sent_seq_nr) {
                $self.last_sent_seq_nr = $segment_iter_item.seq_nr();
                $self.seq_nr = $segment_iter_item.seq_nr() + 1;
            }
            if $self.last_sent_seq_nr.is_after($self.highest_sent_seq_nr) {
                $self.highest_sent_seq_nr = $self.last_sent_seq_nr;
                // A new tail: its probe waits from now.
                $self.timers.tail_probe.turn_off("new tail sent");
            }

            // Data sent: start the timer if it is not running.
            $self.timers.retransmit.arm(
                $self.this_poll.now,
                $self.rtte.retransmission_timeout(),
                false,
                "data sent",
            );

            $self.timers.remote_inactivity_timer.arm(
                $self.this_poll.now,
                $self.socket_opts.remote_inactivity_timeout,
                false,
                "expecting reply on ST_DATA",
            );
            Ok(true)
        };
        tmp_closure()
    }};
}

impl<T: Transport, Env: UtpEnvironment> VirtualSocket<T, Env> {
    async fn run_forever(self) -> crate::Result<()> {
        self.await
    }

    fn timestamp_microseconds(&self) -> u32 {
        (self.this_poll.now - self.socket_created).as_micros() as u32
    }

    /// Create a stub header for a new outgoing message.
    pub(crate) fn outgoing_header(&self) -> UtpHeader {
        UtpHeader {
            connection_id: self.conn_id_send,
            timestamp_microseconds: self.timestamp_microseconds(),
            timestamp_difference_microseconds: self.delay.reply_us(),
            seq_nr: self.seq_nr,
            ack_nr: self.last_consumed_remote_seq_nr,
            wnd_size: self.rx_window(),
            ..Default::default()
        }
    }

    fn rx_window(&self) -> u32 {
        self.user_rx
            .remaining_rx_window()
            .try_into()
            .unwrap_or(u32::MAX)
    }

    fn send_tx_queue(&mut self, cx: &mut std::task::Context<'_>) -> crate::Result<()> {
        // No reason to send anything, we'll get polled next time.
        if self.this_poll.transport_pending {
            return Ok(());
        }

        let mut header = self.outgoing_header();

        // Each timeout that passed with nothing in flight decays the window,
        // as libtorrent's timer does while a direction is idle
        // (utp_stream.cpp:3461-3468). The pacer below fills to the decayed
        // window, so the first pass after idle sends no more than half of
        // it.
        if !self.user_tx_segments.is_empty()
            && let Some(since) = self.idle_since.take()
        {
            let rto = self
                .rtte
                .retransmission_timeout()
                .max(Duration::from_millis(1));
            let periods = self
                .this_poll
                .now
                .saturating_duration_since(since)
                .as_nanos()
                / rto.as_nanos();
            for _ in 0..periods.min(IDLE_DECAYS_MAX) {
                self.congestion_controller.on_idle_timeout();
            }
        }

        self.pacer.refill(
            self.this_poll.now,
            self.congestion_controller
                .window()
                .min(self.last_remote_window as usize),
            self.rtte.roundtrip_time(),
            usize::from(self.segment_sizes.mss()),
            self.congestion_controller.is_slow_start(),
        );
        // The payload of the packet the pacer held back, if one was.
        let mut paced = None;

        // Retransmit timer expired, rewind state backwards.
        if self.timers.retransmit.expired(self.this_poll.now) {
            trace!("retransmit timer expired");
            self.socket.stats.rto_timeouts_count.increment(1);

            let only_packet_outstanding = self.user_tx_segments.sole_outstanding_is_mtu_probe()
                && !(self.fin_sends > 0 && self.state.our_fin_if_unacked().is_some());
            let seg = self.user_tx_segments.iter_mut_for_sending(None).next();
            if let Some(mut seg) = seg {
                if self.timers.retransmit.report_expiry() {
                    self.socket.stats.timeouts.increment(1);
                }
                // A stream that dies on a packet larger than any it saw
                // delivered leaves later streams probing no higher.
                let mss = usize::from(self.segment_sizes.mss());
                let is_ipv4 = self.remote.is_ipv4();
                let restrict = |socket: &SocketState<T, Env>, payload: usize| {
                    if payload > mss {
                        socket.restrict_mtu(packet_mtu(payload, is_ipv4));
                    }
                };
                // Only a lone probe can attribute the timeout solely to its size.
                let sole_probe = seg.is_mtu_probe() && only_packet_outstanding;
                if !sole_probe {
                    let timeouts = self.rtte.timeouts() as usize + 1;
                    if timeouts > self.socket_opts.num_resends || !self.confirmed {
                        debug!(timeouts, confirmed = self.confirmed, "timed out");
                        restrict(&self.socket, seg.payload_size());
                        return Err(Error::TimedOut);
                    }
                }
                if seg.counted_sends() >= self.socket_opts.num_resends {
                    self.socket.stats.max_retransmissions_reached.increment(1);
                    let (seq_nr, sends, evidence_resends) =
                        (seg.seq_nr(), seg.send_count(), seg.evidence_resends());
                    restrict(&self.socket, seg.payload_size());
                    debug!(
                        %seq_nr,
                        sends,
                        evidence_resends,
                        rto = ?self.rtte.retransmission_timeout(),
                        cwnd = self.congestion_controller.window(),
                        in_flight = self.user_tx_segments.calc_flight_size(self.highest_sent_seq_nr),
                        peer_window = self.last_remote_window,
                        since_peer_packet = ?self
                            .this_poll
                            .now
                            .saturating_duration_since(self.last_remote_packet),
                        "giving up: retransmissions exhausted"
                    );
                    return Err(Error::MaxRetransmissionsReached);
                }
                if self.fast_resend_seq_nr == seg.seq_nr() {
                    self.fast_resend_seq_nr += 1;
                }
                if send_data!(self, cx, header, seg)? {
                    trace!(
                        %header.seq_nr,
                        %header.ack_nr,
                        payload_size = seg.payload_size(),
                        "RTO expired: sent ST_DATA"
                    );
                    self.pacer.charge(seg.payload_size());

                    // A lone probe can fail because it is too large. Other
                    // outstanding packets make this a congestion timeout.
                    if !sole_probe {
                        self.congestion_controller.on_retransmission_timeout(
                            self.this_poll.now,
                            self.highest_sent_seq_nr + 1,
                        );
                        self.rtte.on_rto_timeout();
                        self.recovery
                            .on_rto_timeout(self.last_sent_seq_nr, &self.socket.stats);
                    }

                    // Restart the timer.
                    self.timers.retransmit.arm(
                        self.this_poll.now,
                        self.rtte.retransmission_timeout(),
                        true,
                        "backed off",
                    );

                    // Rewind back last sent seq_nr so that normal sending resumes.
                    // Resend all outstanding data from here.
                    // If we don't do this, then all subsequent lost packets will trigger an RTO.
                    self.last_sent_seq_nr = seg.seq_nr();
                    // Increase the counter so that we don't send anything past it until it gets ACKed.
                    self.rto_retransmissions += 1;
                } else {
                    // This will retry on next poll.
                    return Ok(());
                }
            } else {
                // There's no data to send. Maybe we need to resend FIN? Check.
                match self.state.our_fin_if_unacked() {
                    Some(our_fin_seq_nr) if self.last_sent_seq_nr == our_fin_seq_nr => {
                        if self.timers.retransmit.report_expiry() {
                            self.socket.stats.timeouts.increment(1);
                        }
                        let timeouts = self.rtte.timeouts() as usize + 1;
                        if timeouts > self.socket_opts.num_resends
                            || self.fin_sends >= self.socket_opts.fin_resends
                        {
                            debug!(timeouts, self.fin_sends, "FIN unacknowledged, giving up");
                            return Err(Error::TimedOut);
                        }
                        trace!("RTO: rewinding self.last_sent_seq_nr to retransmit FIN");
                        self.last_sent_seq_nr -= 1;
                        if self.maybe_send_fin(cx)? {
                            // Nothing but the FIN is out: the window decays.
                            self.congestion_controller.on_idle_timeout();
                            self.rtte.on_rto_timeout();
                            self.recovery
                                .on_rto_timeout(self.last_sent_seq_nr, &self.socket.stats);
                            self.timers.retransmit.arm(
                                self.this_poll.now,
                                self.rtte.retransmission_timeout(),
                                true,
                                "backed off",
                            );
                        }
                    }
                    _ => {
                        trace!("retransmit timer expired, but nothing to send");
                        self.timers
                            .retransmit
                            .turn_off("RTO expired, but nothing to send");
                    }
                }
            }
        }

        // We are in RTO retransmission mode, don't send anything.
        if self.rto_retransmissions > 0 {
            trace!(
                rto_retransmissions = self.rto_retransmissions,
                "not sending anything while in RTO processing"
            );
            return Ok(());
        }

        if self.user_tx_segments.is_empty() {
            return Ok(());
        }

        // The flight's tail, unacknowledged for two round trips with
        // nothing new to send, is resent once so the ACK it draws shows
        // what was lost (RFC 8985 section 7). The probe is no judgement of
        // loss: the window and the count toward giving up stay as they are.
        if self.timers.tail_probe.expired(self.this_poll.now) {
            if self.tail_probe_due() {
                let tail = self.highest_sent_seq_nr;
                if let Some(mut seg) = self
                    .user_tx_segments
                    .iter_mut_for_sending(Some(tail))
                    .next()
                {
                    if !send_data!(self, cx, header, seg)? {
                        return Ok(());
                    }
                    seg.on_resent_as_probe();
                    self.pacer.charge(seg.payload_size());
                    self.tail_probe_seq_nr = Some(tail);
                    trace!(%header.seq_nr, payload_size = seg.payload_size(), "tail probe: sent ST_DATA");
                }
            }
            self.timers.tail_probe.turn_off("tail probe due");
        }

        let mut in_recovery = false;

        // In recovery, resend what the SACKs show lost, as far as the
        // recovery window and the pacer allow. Every hole a SACK reaches is
        // a candidate, not only those before the point recovery began at:
        // libtorrent resends from each SACK (utp_stream.cpp:1235-1355). A
        // packet resent once waits for evidence of its own, SACKs of packets
        // sent after the resend, before it counts lost again; libtorrent
        // leaves that to the retransmission timer.
        if let Some(rec) = self.recovery.recovering_mut() {
            in_recovery = true;
            let sack_depth = self.user_tx_segments.sack_depth();
            let last_sent_seq_nr = self.last_sent_seq_nr;
            let fast_resend_seq_nr = self.fast_resend_seq_nr;
            let mut it = self
                .user_tx_segments
                .iter_mut_for_sending(None)
                .take(sack_depth + 1)
                .take_while(|seg| seg.seq_nr().is_at_or_before(last_sent_seq_nr))
                .filter(|seg| seg.send_count() > 0);

            let mut cwnd = rec.cwnd();
            let mut sent = 0;
            let mss = self.segment_sizes.mss() as usize;
            let mut force_head = rec.total_retransmitted_segments() == 0;
            while force_head || cwnd > mss {
                let mut seg = match it.next() {
                    Some(seg) => seg,
                    None => break,
                };

                // The first packet recovery resends is the oldest, lost or
                // not, as the duplicates that began recovery point at it,
                // unless it was resent already (libtorrent v2.0.15
                // utp_stream.cpp:2814-2827).
                let forced = std::mem::take(&mut force_head)
                    && seg.seq_nr().is_at_or_after(fast_resend_seq_nr);
                if !forced {
                    if !seg.is_lost() {
                        continue;
                    }
                    if !self.pacer.allows(seg.payload_size()) {
                        paced = Some(seg.payload_size());
                        break;
                    }
                }

                if send_data!(self, cx, header, seg)? {
                    event!(
                        RECOVERY_TRACING_LOG_LEVEL,
                        %header.seq_nr,
                        %header.ack_nr,
                        payload_size = seg.payload_size(),
                        pipe = rec.pipe_estimate.pipe,
                        cwnd,
                        is_lost = seg.is_lost(),
                        is_expired = seg.is_expired(),
                        "RECOVERY: sent ST_DATA"
                    );
                    // Each loss may cut the window, once per window of
                    // packets and cut timer (libtorrent v2.0.15
                    // utp_stream.cpp:1346-1349, 2833 and 2116-2121).
                    if !seg.is_mtu_probe() {
                        self.congestion_controller.on_enter_recovery(
                            self.this_poll.now,
                            seg.seq_nr(),
                            self.highest_sent_seq_nr + 1,
                        );
                    }
                    seg.on_resent_as_lost();
                    if seg.seq_nr().is_after(rec.high_rxt) {
                        rec.high_rxt = seg.seq_nr();
                    }
                    if seg.seq_nr().is_at_or_after(self.fast_resend_seq_nr) {
                        self.fast_resend_seq_nr = seg.seq_nr() + 1;
                    }
                    rec.increment_total_transmitted_segments();
                    self.socket
                        .stats
                        .recovery_retransmitted_segments_count
                        .increment(1);
                    rec.pipe_estimate.pipe += seg.payload_size();
                    cwnd = cwnd.saturating_sub(seg.payload_size());
                    self.pacer.charge(seg.payload_size());
                    sent += 1;
                } else {
                    return Ok(());
                }
            }
            if let Some(at) = paced.and_then(|payload| self.pacer.ready_at(payload)) {
                self.timers.pace.set(at);
            }

            event!(
                RECOVERY_TRACING_LOG_LEVEL,
                ?sent,
                pipe = rec.pipe_estimate.pipe,
                remaining_cwnd = cwnd,
                "recovery sending"
            );

            if cwnd < mss {
                match rec.pipe_estimate.recalc_timer {
                    Some(t) => {
                        self.timers.recovery_pipe_expiry.set(t);
                    }
                    None if sent > 0 => {
                        self.timers.recovery_pipe_expiry.arm(
                            self.this_poll.now,
                            calc_pipe_expiry(self.rtte.roundtrip_time()),
                            true,
                            "cwnd < mss && sent > 0",
                        );
                    }
                    _ => {}
                }
            }

            // Retransmit FIN if needed.
            if let Some(our_fin) = self.state.our_fin_if_unacked()
                && rec.high_rxt == our_fin - 1
            {
                // Rewind last_sent_seq_nr to re-send FIN
                self.last_sent_seq_nr = our_fin - 1;
                rec.high_rxt = our_fin;
                rec.increment_total_transmitted_segments();
                self.socket
                    .stats
                    .recovery_retransmitted_segments_count
                    .increment(1);
                // No reason to do anything further.
                return Ok(());
            }
        }

        let mut remaining_cwnd = self
            .recovery
            .remaining_cwnd(self.last_remote_window)
            .unwrap_or_else(|| {
                self.congestion_controller
                    .window()
                    .min(self.last_remote_window as usize)
                    .saturating_sub(
                        self.user_tx_segments
                            .calc_flight_size(self.last_sent_seq_nr),
                    )
            });

        let mut sent_count = 0;

        let mut message_too_long = None;

        // Send the stuff we haven't sent yet, up to sender's window.
        for mut item in self
            .user_tx_segments
            .iter_mut_for_sending(Some(self.last_sent_seq_nr + 1))
        {
            if remaining_cwnd < item.payload_size() {
                self.socket.stats.send_window_exhausted.increment(1);
                trace_every_ms!(100, "remote recv window exhausted");
                break;
            }
            if !self.pacer.allows(item.payload_size()) {
                paced = Some(item.payload_size());
                break;
            }

            let go_back = item.send_count() > 0;
            match send_data!(self, cx, header, item) {
                Ok(true) => {
                    // The timer's go-back walks the fast-resend mark along, so
                    // a SACK does not send the same packet again (libtorrent
                    // v1.2.20 utp_stream.cpp:1800-1802).
                    if go_back && self.fast_resend_seq_nr == item.seq_nr() {
                        self.fast_resend_seq_nr += 1;
                    }
                    trace!(
                        %header.seq_nr,
                        %header.ack_nr,
                        payload_size = item.payload_size(),
                        remaining_cwnd,
                        "sent ST_DATA"
                    );
                    remaining_cwnd -= item.payload_size();
                    self.pacer.charge(item.payload_size());
                    sent_count += 1;
                }
                // Transport was pending, need to retry
                Ok(false) => {
                    break;
                }
                // If we couldn't send due to message size, we need to tweak MTU if possible and then retry.
                Err(Error::Send(source)) if crate::is_message_size_error(&source) => {
                    let seq_nr = item.seq_nr();
                    let size = item.payload_size();
                    debug!(
                        ?seq_nr,
                        payload_size = size,
                        "got message too long (EMSGSIZE): {source:#}"
                    );
                    message_too_long = Some((seq_nr, size));
                    break;
                }
                Err(e) => return Err(e),
            }
        }
        if let Some(at) = paced.and_then(|payload| self.pacer.ready_at(payload)) {
            self.timers.pace.set(at);
        }

        if let Some((seq_nr, size)) = message_too_long {
            if self.user_tx_segments.pop_mtu_probe(seq_nr) {
                debug!("popped too large MTU probe, will retry");
                self.segment_sizes.on_probe_failed(size);
                self.segment_sizes.disarm_cooldown();
                self.this_poll.restart = true;
            } else {
                return Err(Error::BugEmsgSizeNoProbe);
            }
        }

        if sent_count == 0 {
            trace!(remaining_cwnd, "did not send anything");
        } else if in_recovery {
            self.socket
                .stats
                .recovery_transmitted_new_segments_count
                .increment(sent_count);
            event!(
                RECOVERY_TRACING_LOG_LEVEL,
                sent_count,
                remaining_cwnd = remaining_cwnd,
                "sent in recovery"
            );
        } else {
            trace!(remaining_cwnd, "remaining recv_wnd after sending");
        }

        Ok(())
    }

    fn maybe_send_ack(&mut self, cx: &mut std::task::Context<'_>) -> crate::Result<bool> {
        if self.immediate_ack_to_transmit() {
            self.socket.stats.immediate_acks.increment(1);
            return self.send_ack(cx);
        }
        if self.should_send_window_update() {
            return self.send_ack(cx);
        }
        if self.timers.ack_delay_timer.expired(self.this_poll.now) {
            if self.ack_to_transmit() {
                self.socket.stats.delayed_acks.increment(1);
                trace!("delayed ack expired, sending ACK");
                return self.send_ack(cx);
            } else {
                self.timers
                    .ack_delay_timer
                    .turn_off("delayed ACK expired but nothing to send");
            }
        } else if self.consumed_but_unacked_bytes > 0 {
            self.timers
                .ack_delay_timer
                .arm(self.this_poll.now, ACK_DELAY, false, "delayed ACK");
        }
        Ok(false)
    }

    fn on_packet_sent(&mut self, header: &UtpHeader) {
        on_packet_sent!(self, header)
    }

    fn send_control_packet(
        &mut self,
        cx: &mut std::task::Context<'_>,
        header: &UtpHeader,
    ) -> crate::Result<bool> {
        if self.this_poll.transport_pending {
            return Ok(false);
        }

        trace!(
            seq_nr = %header.seq_nr,
            ack_nr = %header.ack_nr,
            wnd_size = %header.wnd_size,
            type = ?header.get_type(),
            "sending"
        );

        let len = header.serialize(&mut self.this_poll.tmp_buf)?;

        self.this_poll.transport_pending = self
            .socket
            .try_poll_send_to(
                self.transport.as_deref().ok_or(Error::DispatcherDead)?,
                self.sender.as_deref_mut(),
                cx,
                &self.this_poll.tmp_buf[..len],
                self.remote,
            )
            .map_err(Error::Send)?;

        let sent = !self.this_poll.transport_pending;

        if sent {
            self.socket.stats.sent_control_packets.increment(1);
            self.on_packet_sent(header);
        } else {
            self.socket.stats.unsent_control_packets.increment(1);
        }

        Ok(sent)
    }

    fn send_ack(&mut self, cx: &mut std::task::Context<'_>) -> crate::Result<bool> {
        let mut header = self.outgoing_header();
        header.extensions.selective_ack = self.user_rx.selective_ack();
        self.send_control_packet(cx, &header)
    }

    fn maybe_send_fin(&mut self, cx: &mut std::task::Context<'_>) -> crate::Result<bool> {
        if self.this_poll.transport_pending {
            return Ok(false);
        }

        let seq_nr = match self.state.our_fin_if_unacked() {
            Some(seq_nr) => seq_nr,
            None => return Ok(false),
        };

        // Only send fin after all the outstanding data was sent.
        if seq_nr != self.last_sent_seq_nr + 1 {
            return Ok(false);
        }

        let mut fin = self.outgoing_header();
        fin.set_type(Type::Fin);
        fin.seq_nr = seq_nr;
        if self.send_control_packet(cx, &fin)? {
            self.fin_sends += 1;
            self.timers.retransmit.arm(
                self.this_poll.now,
                self.rtte.retransmission_timeout(),
                false,
                "data sent",
            );
            self.last_sent_seq_nr = seq_nr;
            if seq_nr.is_after(self.highest_sent_seq_nr) {
                self.highest_sent_seq_nr = seq_nr;
            }
            return Ok(true);
        }
        Ok(false)
    }

    fn split_tx_queue_into_segments(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> crate::Result<()> {
        let tx_len = {
            let mut g = self.user_tx.locked.write();
            let c = self.user_tx.ring.lock();
            let tx_len = c.len();

            if tx_len == 0 {
                update_optional_waker(&mut g.dispatcher_waker, cx);
                return Ok(());
            }

            // Grow the send buffer if it's approaching limits.
            // The 0.9 value found empirically.
            //
            // The buffer holds what is in flight as well as what waits, and
            // the window grows only while the flight fills it, so a buffer
            // no larger than the window would pin the window where the
            // buffer stopped. It grows to a window in flight and a window
            // waiting.
            let grow_limit = self
                .congestion_controller
                .window()
                .min(self.last_remote_window as usize)
                .saturating_mul(2)
                .min(self.socket.opts().vsock_tx_bufsize_bytes_max.get());
            let cap = c.capacity();
            let full_ratio = tx_len as f64 / cap as f64;
            drop(g);
            drop(c);

            if cap < grow_limit && full_ratio > 0.9 {
                let new_cap = self
                    .user_tx
                    .grow(self.socket.opts().vsock_tx_bufsize_bytes_max);
                if let Some(new_cap) = new_cap {
                    tracing::debug!(new_cap, tx_len, grow_limit, "grew send buffer");
                    let w = self.user_tx.locked.write().writer_waker.take();
                    if let Some(w) = w {
                        w.wake();
                    }
                }
            }

            tx_len
        };

        if matches!(
            self.state,
            VirtualSocketState::LastAck { .. } | VirtualSocketState::Closed
        ) {
            trace!(?self.state, "both directions are closing, not segmenting further");
            return Ok(());
        }

        match self.user_tx_segments.pop_expired_mtu_probe(
            self.timers.retransmit.expired(self.this_poll.now)
                && self.user_tx_segments.sole_outstanding_is_mtu_probe()
                && !(self.fin_sends > 0 && self.state.our_fin_if_unacked().is_some()),
            self.socket_opts.mtu_probe_max_retransmissions,
        ) {
            PopExpiredProbe::Expired {
                rewind_to,
                payload_size,
            } => {
                debug!(payload_size, ?self.last_sent_seq_nr, ?rewind_to, "MTU probe expired");
                if self.timers.retransmit.report_expiry() {
                    self.socket.stats.timeouts.increment(1);
                }
                // In case the retransmit timer expired, this is not "real" expiry, but expiry due to us sending
                // too large segment. So ignore the retransmit timer, pretend it didn't fire.
                self.timers.retransmit.turn_off("MTU probe is not real RTO");
                self.rto_retransmissions = 0;

                if self.last_sent_seq_nr.is_after(rewind_to) {
                    self.last_sent_seq_nr = rewind_to;
                }
                self.segment_sizes.on_probe_failed(payload_size);
            }
            PopExpiredProbe::NotExpired => {
                trace!("MTU probe hasnt expired yet");
                return Ok(());
            }
            PopExpiredProbe::Empty => {}
        }

        let segmented_len = self.user_tx_segments.total_len_bytes();

        if tx_len < segmented_len {
            return Err(Error::BugInBufferComputations {
                user_tx_buflen: try_shrink_or_neg!(tx_len),
                segmented_len: try_shrink_or_neg!(segmented_len),
            });
        }

        let mut remaining = tx_len - segmented_len;

        let mut remote_window_remaining = self.last_remote_window as usize;

        trace!(
            remote_window_remaining,
            congestion_controller_window = self.congestion_controller.window(),
            remaining,
            self.last_remote_window
        );

        // Whether the loop stopped for want of written data.
        let mut starved = remaining == 0;
        while remaining > 0 && remote_window_remaining > 0 {
            let ss = self.segment_sizes.next_segment_size();
            let min_ss = self.segment_sizes.mss();
            let max_payload_size = (ss as usize).min(remote_window_remaining);
            let payload_size = max_payload_size.min(remaining);

            // Run Nagle algorithm to prevent sending too many small segments.
            {
                let can_send_full_payload = payload_size == max_payload_size;
                let data_in_flight = !self.user_tx_segments.is_empty();

                if self.socket_opts.nagle && !can_send_full_payload && data_in_flight {
                    trace!(max_payload_size, "nagle: buffering more data");
                    starved = true;
                    break;
                }
            }

            let is_mtu_probe = payload_size > min_ss as usize;

            if !self.user_tx_segments.enqueue(payload_size, is_mtu_probe) {
                break;
            }
            self.segment_sizes.on_segmented(is_mtu_probe);
            remaining -= payload_size;
            remote_window_remaining -= payload_size;
            starved = remaining == 0;
            trace!(bytes = payload_size, "segmented");

            if is_mtu_probe {
                trace!(payload_size, "MTU probing, not segmenting more data");
                break;
            }
        }

        trace!(
            remaining,
            remote_window_remaining,
            user_tx_segments_segments = self.user_tx_segments.total_len_packets(),
            user_tx_segments_bytes = self.user_tx_segments.total_len_bytes(),
            segment_sizes = ?self.segment_sizes.log_debug(),
            "split_tx_queue_into_segments finished",
        );

        self.this_poll.unsegmented_data = remaining;

        // Everything written is segmented: the next write wakes the stream,
        // so what the writer adds goes out without waiting for an ACK. A
        // send buffer smaller than the flight is otherwise refilled once per
        // ACK, and never seen full enough to grow.
        if starved {
            update_optional_waker(&mut self.user_tx.locked.write().dispatcher_waker, cx);
        }

        Ok(())
    }

    /// Before dying, ensure cleanup and notifications are done.
    fn just_before_death(&mut self, cx: &mut std::task::Context<'_>, error: Option<&crate::Error>) {
        if let Some(err) = error {
            trace!("just_before_death: {err:#}");
        } else {
            trace!("just_before_death: no error");
        }

        if let Some(e) = error {
            self.user_rx.enqueue_error(format!("{e:#}"));
        }

        // This will close the reader.
        self.user_rx.mark_vsock_closed();

        // This will close the writer.
        self.user_tx.mark_vsock_closed();

        // A stream that fails tells the remote with a reset, so the remote
        // ends its side now instead of retransmitting into its own timeout.
        // A reset the remote sent needs no answer, and a remote that went
        // quiet after our FIN is the ordinary end of a close.
        let closing = self.state.is_local_fin_or_later();
        if error.is_some_and(|error| match error {
            Error::StResetReceived => false,
            Error::RemoteInactiveForTooLong => !closing,
            _ => true,
        }) {
            let mut reset = self.outgoing_header();
            reset.set_type(Type::Reset);
            if let Err(e) = self.send_control_packet(cx, &reset) {
                trace!("error sending ST_RESET: {e:#}")
            }
        }
    }

    fn state_is_closed(&self) -> bool {
        self.state.is_closed(self.socket_opts.wait_for_last_ack)
    }

    fn process_all_incoming_messages(
        &mut self,
        cx: &mut std::task::Context<'_>,
        remaining: &mut usize,
    ) -> crate::Result<()> {
        let mut result = ProcessIncomingMessageResult::default();

        while *remaining > 0 {
            let Poll::Ready(msg) = self.rx.poll_recv(cx) else {
                break;
            };
            *remaining -= 1;
            let msg = match msg {
                Some(msg) => msg,
                None => {
                    // This could happen only when we were removed from socket.streams(), of if socket dispatcher is dead.
                    // This should not have happened!
                    // But if it did, we won't be able to receive any more messages.
                    // We need to send FIN in this case, but we can't wait for its ACK to arrive back, cause we can't
                    // receive any messages!

                    trace!("can't receive messages anymore. Transitioning to Closed");
                    self.transition_to_fin_wait_1();
                    self.maybe_send_fin(cx)?;
                    log_if_changed!(Level::DEBUG, "state", self, |s| s.state, |s| s.state =
                        VirtualSocketState::Closed);
                    return Ok(());
                }
            };
            result.update(&self.process_incoming_message(cx, msg)?);
            if self.state_is_closed() || self.this_poll.transport_pending {
                break;
            }
        }

        if result.on_ack_result.acked_segments_count > 0
            || result.on_ack_result.newly_sacked_segment_count > 0
        {
            // Exit RTO mode.
            self.rto_retransmissions = 0;

            // The watermark never falls behind the first unacknowledged
            // packet (libtorrent v2.0.15 utp_stream.cpp:2750-2751).
            let snd_una = self.user_tx_segments.next_seq_nr()
                - self.user_tx_segments.total_len_packets() as u16;
            if self.fast_resend_seq_nr.is_before(snd_una) {
                self.fast_resend_seq_nr = snd_una;
            }
            // The flight moved: a tail probe waits from now, and a probe
            // acknowledged ends its flight's.
            self.timers.tail_probe.turn_off("ACK received");
            if self
                .tail_probe_seq_nr
                .is_some_and(|probe| probe.is_before(snd_una))
            {
                self.tail_probe_seq_nr = None;
            }

            // Reset retransmit timer.
            if self.user_tx_segments.is_empty() && self.state.our_fin_if_unacked().is_none() {
                // If all outstanding data ACKed, turn off the timer.
                self.timers.retransmit.turn_off("all data acknowledged");
                self.idle_since = Some(self.this_poll.now);

                self.timers.remote_inactivity_timer.turn_off("TX is empty");
            } else {
                // When an ACK is received that acknowledges new data, restart the
                // retransmission timer.
                self.timers.retransmit.arm(
                    self.this_poll.now,
                    self.rtte.retransmission_timeout(),
                    true,
                    "new data acknowledged",
                );

                self.restart_remote_inactivity_timer();
            }
        }

        if result.on_ack_result.acked_segments_count > 0 {
            // Cleanup user side of TX queue, remove the ACKed bytes from the front of it,
            // and notify the writer.
            {
                let mut g = self.user_tx.locked.write();
                self.user_tx
                    .truncate_front(result.on_ack_result.acked_bytes)?;

                let waker = g.writer_waker.take();

                // Waking under lock may slow things down.
                drop(g);

                if let Some(w) = waker {
                    w.wake();
                }
            }

            trace!(?result.on_ack_result, "removed ACKed tx messages");
        }

        if let Some(rec) = self.recovery.recovering_mut() {
            rec.pipe_estimate = self.user_tx_segments.calc_pipe(
                rec.high_rxt,
                self.last_sent_seq_nr,
                self.rtte.roundtrip_time(),
                self.this_poll.now,
            );
        }

        #[cfg(test)]
        if self.socket_opts.benchmark_delayed_acks {
            return Ok(());
        }
        if !self.this_poll.transport_pending && self.ack_to_transmit() {
            self.force_immediate_ack("receive queue drained");
        }

        Ok(())
    }

    fn process_incoming_message(
        &mut self,
        cx: &mut std::task::Context<'_>,
        event: IngressEvent,
    ) -> crate::Result<ProcessIncomingMessageResult> {
        let payload_dropped = event.payload_dropped;
        let msg = event.message;
        let span = trace_span!("msg",
            seq_nr=%msg.header.seq_nr,
            ack_nr=%msg.header.ack_nr,
            len=msg.payload().len(),
            msgtype=?msg.header.get_type()
        );
        let _span_g = span.enter();

        trace!("processing message");

        // Process state changes and invalid packets.
        use Type::*;
        use VirtualSocketState::*;
        let hdr = &msg.header;

        let previously_seen_remote_fin = self.state.is_remote_fin_or_later();

        // Anchor the admitted interval to its oldest duplicate ACK. Neither
        // a future ACK nor an ambiguous half-space value belongs to it.
        let oldest_ack = self.user_tx_segments.acked_seq_nr() - 3;
        let ack_span = self.highest_sent_seq_nr.forward_distance_from(oldest_ack);
        if hdr.htype != Syn
            && (ack_span >= SEQUENCE_HALF_SPACE
                || hdr.ack_nr.forward_distance_from(oldest_ack) > ack_span)
        {
            trace!("dropping packet, its ack is outside the sent interval");
            self.socket.stats.invalid_packets.increment(1);
            return Ok(Default::default());
        }

        let max_packets_reorder =
            crate::stream_rx::reorder_capacity(self.socket_opts.vsock_rx_bufsize).get();
        let receive_delta = hdr
            .seq_nr
            .signed_offset_from(self.last_consumed_remote_seq_nr);
        if self.state != SynReceived
            && receive_delta.is_none_or(|offset| offset > max_packets_reorder as isize)
        {
            trace!("dropping packet, its sequence is past the receive reorder window");
            self.socket.stats.redundant_packets.increment(1);
            return Ok(Default::default());
        }

        match (self.state, hdr.get_type()) {
            // From real world packets: if ST_RESET acks our FIN, it's ok
            (LastAck { our_fin, .. }, Reset) if hdr.ack_nr == our_fin => {
                self.state = Closed;
                return Ok(Default::default());
            }
            (_, Reset) => {
                self.state = Closed;
                return Err(Error::StResetReceived);
            }
            (_, Syn) => {
                trace!("unexpected ST_SYN, ignoring");
                self.socket.stats.invalid_packets.increment(1);
                return Ok(Default::default());
            }
            (Closed, _) => {
                return Err(Error::BugRecvInClosed);
            }
            (SynReceived, _) => return Err(Error::BugUnexpectedPacketInSynReceived),
            (SynAckSent { .. }, Data | State) => {
                if hdr.ack_nr != self.seq_nr - 1 {
                    trace!("dropping packet, we expected a different ack_nr");
                    self.socket.stats.invalid_packets.increment(1);
                    return Ok(Default::default());
                }

                trace!("state: syn-ack-sent -> established");
                self.restart_remote_inactivity_timer();
                self.state = Established;
            }
            (SynAckSent { .. }, Fin) => {
                if hdr.ack_nr != self.seq_nr - 1
                    || hdr.seq_nr != self.last_consumed_remote_seq_nr + 1
                {
                    trace!("dropping FIN, its handshake acknowledgement or sequence is invalid");
                    self.socket.stats.invalid_packets.increment(1);
                    return Ok(Default::default());
                }
                trace!("state: syn-ack-sent -> close-wait");
                self.timers
                    .syn_ack_resend
                    .turn_off("handshake confirmed by FIN");
                self.restart_remote_inactivity_timer();
                self.state = CloseWait {
                    remote_fin: hdr.seq_nr,
                };
            }

            (Established, Data | State) => {}

            // Fin received in an expected state, but its sequence number is wrong
            (Established | FinWait1 { .. } | FinWait2, Fin)
                if hdr.seq_nr != self.last_consumed_remote_seq_nr + 1 =>
            {
                trace!(
                    hdr=%hdr.short_repr(),
                    "dropping FIN, expected seq_nr to be {}",
                    self.last_consumed_remote_seq_nr + 1
                );
                self.socket.stats.invalid_packets.increment(1);
                return Ok(Default::default());
            }

            (Established, Fin) => {
                trace!("state: established -> close-wait");
                self.state = CloseWait {
                    remote_fin: hdr.seq_nr,
                }
            }

            (FinWait1 { our_fin }, Fin) if hdr.ack_nr == our_fin => {
                trace!("state: fin-wait-1 -> closed");
                self.state = Closed;
            }
            (FinWait1 { our_fin }, Fin) => {
                trace!("state: fin-wait-1 -> last-ack");
                self.state = LastAck {
                    our_fin,
                    remote_fin: hdr.seq_nr,
                }
            }

            (FinWait1 { our_fin }, Data | State) if hdr.ack_nr == our_fin => {
                trace!("state: fin-wait-1 -> fin-wait-2");
                self.restart_remote_inactivity_timer();
                self.state = FinWait2;
            }
            (FinWait1 { .. }, Data | State) => {}
            (FinWait2, Fin) => {
                trace!("state: fin-wait-2 -> closed");
                self.restart_remote_inactivity_timer();
                self.state = Closed;
            }
            (FinWait2, Data | State) => {}

            (LastAck { our_fin, .. }, _) if hdr.ack_nr == our_fin => {
                trace!("state: last-ack -> closed");
                self.restart_remote_inactivity_timer();
                self.state = Closed;
            }

            (CloseWait { remote_fin } | LastAck { remote_fin, .. }, _)
                if hdr.seq_nr.is_after(remote_fin) =>
            {
                if hdr.htype == Data {
                    trace!(hdr=%hdr.short_repr(), ?remote_fin, "received higher seq nr than remote FIN, dropping packet");
                    self.socket.stats.invalid_packets.increment(1);
                    return Ok(Default::default());
                } else {
                    // We COULD drop this packet, but turns out it happens very often in the wild.
                    trace!(hdr=%hdr.short_repr(), ?remote_fin, "received higher seq nr than remote FIN, ignoring this");
                }
            }

            (CloseWait { .. } | LastAck { .. }, _) => {}
        }

        // A packet past every check: the remote is there.
        self.confirmed = true;
        self.last_remote_packet = self.this_poll.now;
        self.rtte.on_valid_packet();

        let in_flight_before = self
            .user_tx_segments
            .calc_flight_size(self.last_sent_seq_nr);
        let result = ProcessIncomingMessageResult {
            on_ack_result: self
                .user_tx_segments
                .remove_up_to_ack(msg.received, &msg.header),
        };
        self.segment_sizes
            .on_payload_delivered(result.on_ack_result.max_acked_payload_size);
        self.congestion_controller
            .set_mss(self.segment_sizes.mss() as usize);

        // Update RTT and RTO if not in recovery. In recovery we get very delayed info
        // for packets beyond sack depth.
        if let (false, Some(rtt)) = (self.recovery.is_recovering(), result.on_ack_result.new_rtt) {
            log_every_ms_if_changed!(
                500,
                RTTE_TRACING_LOG_LEVEL,
                "rtte:sample",
                self,
                |s| s.rtte,
                |s| s.rtte.sample(rtt)
            );
        }
        self.congestion_controller
            .set_remote_window(msg.header.wnd_size as usize);
        let sampled = self.delay.on_packet(
            msg.received,
            (msg.received - self.socket_created).as_micros() as u32,
            msg.header.timestamp_microseconds,
            msg.header.timestamp_difference_microseconds,
        );
        let acked_bytes =
            result.on_ack_result.acked_bytes + result.on_ack_result.newly_sacked_byte_count;
        if sampled.is_some() && acked_bytes > 0 && in_flight_before > 0 {
            self.congestion_controller.on_delay_sample(
                DelaySample {
                    delay: self.delay.controller_delay(result.on_ack_result.new_rtt),
                    acked_bytes,
                    in_flight_before,
                    in_flight_after: self
                        .user_tx_segments
                        .calc_flight_size(self.last_sent_seq_nr),
                    segment: self.segment_sizes.probe_size().into(),
                },
                &self.socket.stats,
            );
        }
        self.socket.stats.max_congestion_window_bytes.fetch_max(
            self.congestion_controller.cwnd_bytes() as u64,
            Ordering::Relaxed,
        );
        self.last_remote_window = msg.header.wnd_size;

        self.congestion_controller
            .advance_loss_gate(self.user_tx_segments.acked_seq_nr());
        self.recovery.on_ack(
            &msg.header,
            &result.on_ack_result,
            &mut self.user_tx_segments,
            &mut self.congestion_controller,
            crate::recovery::AckContext {
                last_sent_seq_nr: self.last_sent_seq_nr,
                next_seq_nr: self.highest_sent_seq_nr + 1,
                now: self.this_poll.now,
                rtt: self.rtte.roundtrip_time(),
                stats: &self.socket.stats,
            },
        );

        if payload_dropped {
            self.force_immediate_ack("inbox payload refused");
            return Ok(result);
        }

        let Some(receive_delta) = receive_delta else {
            return Ok(result);
        };
        let offset = receive_delta - 1;

        match msg.header.get_type() {
            Data => {
                if offset < 0 {
                    trace!(
                        %self.last_consumed_remote_seq_nr,
                        "ignoring message, we already processed it. There might be packet loss, resending ACK."
                    );
                    self.force_immediate_ack("duplicate ST_DATA");
                    self.socket.stats.redundant_packets.increment(1);
                    self.socket
                        .stats
                        .incoming_already_acked_data_packets
                        .increment(1);
                    return Ok(result);
                }

                trace!(payload_size = msg.payload().len(), "received ST_DATA");
                trace!(
                    offset,
                    %self.last_consumed_remote_seq_nr,
                    "adding ST_DATA message to assember"
                );

                let assembler_was_empty = self.user_rx.assembler_empty();
                let hdr = msg.header;

                self.segment_sizes.on_payload_delivered(msg.payload().len());
                self.congestion_controller
                    .set_mss(self.segment_sizes.mss() as usize);

                match self.user_rx.add_remove(cx, msg, offset as usize)? {
                    AssemblerAddRemoveResult::Consumed {
                        sequence_numbers,
                        bytes,
                    } => {
                        if sequence_numbers > 0 {
                            trace!(sequence_numbers, "consumed messages");
                            self.socket
                                .stats
                                .consumed_data_seq_nrs
                                .increment(sequence_numbers as u64);
                            self.socket.stats.consumed_bytes.increment(bytes as u64);
                        } else {
                            self.socket.stats.out_of_order_packets.increment(1);
                            debug_every_ms!(500, header=%hdr.short_repr(), offset, ?self.last_consumed_remote_seq_nr, "out of order");
                        }

                        self.restart_remote_inactivity_timer();
                        self.last_consumed_remote_seq_nr += sequence_numbers as u16;
                        self.consumed_but_unacked_bytes =
                            self.consumed_but_unacked_bytes.saturating_add(bytes);
                        trace!(self.consumed_but_unacked_bytes);
                    }
                    AssemblerAddRemoveResult::Unavailable => {
                        debug_every_ms!(500, header=%hdr.short_repr(), offset,
                            ?self.last_consumed_remote_seq_nr, "cannot reassemble message, ignoring it");
                    }
                    AssemblerAddRemoveResult::AlreadyPresent => {
                        debug_every_ms!(500, header=%hdr.short_repr(), offset,
                            ?self.last_consumed_remote_seq_nr, "already present in assembler");
                    }
                }

                // Send an immediate ACK when either:
                //  1) an out-of-order segment is received, or
                //  2) a segment arrives that fills in all or part of a gap in sequence space.
                if !self.user_rx.assembler_empty() || !assembler_was_empty {
                    trace!(
                        assembler_not_empty = !self.user_rx.assembler_empty(),
                        assembler_was_empty,
                        immediate_ack_to_transmit = self.immediate_ack_to_transmit(),
                        "forcing immediate ACK"
                    );

                    self.force_immediate_ack("out of order or filled gap");

                    // Send right away so that we send an ACK per segment. This is necessary so that
                    // the reciever gets duplicate ACKs as soon as possible to repair loss.
                    self.send_ack(cx)?;
                }
            }
            State => {}
            Reset => return Err(Error::StResetReceived),
            Fin => {
                if let Some(close_reason) = msg.header.extensions.close_reason {
                    debug!("remote closed with {close_reason:?}");
                }

                self.force_immediate_ack("ST_FIN received");

                if !previously_seen_remote_fin && offset >= 0 {
                    self.last_consumed_remote_seq_nr = hdr.seq_nr;
                    self.user_rx.add_remove(cx, msg, offset as usize)?;
                }
            }
            Syn => {
                trace!("ignoring unexpected ST_SYN packet: {:?}", msg.header);
            }
        }
        Ok(result)
    }

    fn should_send_window_update(&self) -> bool {
        if self.state.is_remote_fin_or_later() {
            return false;
        }
        // A remote sends only whole packets into the window we last
        // advertised, so announce a window that closed, opened from zero, or
        // grew by a packet since.
        let current = self.rx_window();
        let mss = u32::from(self.segment_sizes.mss());
        let changed = (current == 0) ^ (self.last_sent_window == 0)
            || current.saturating_sub(self.last_sent_window) >= mss;
        if changed {
            trace!(
                self.last_sent_window,
                current = self.rx_window(),
                "need to send a window update"
            );
            return true;
        }
        false
    }

    /// Return whether to send ACK immediately due to the amount of unacknowledged data.
    fn immediate_ack_to_transmit(&self) -> bool {
        self.consumed_but_unacked_bytes
            >= IMMEDIATE_ACK_EVERY_RMSS * self.segment_sizes.mss() as usize
    }

    fn force_immediate_ack(&mut self, reason: &'static str) {
        trace!(reason, "forcing immedaite ACK");
        self.consumed_but_unacked_bytes = usize::MAX;
    }

    fn ack_to_transmit(&self) -> bool {
        self.last_consumed_remote_seq_nr
            .is_after(self.last_sent_ack_nr)
    }

    fn restart_remote_inactivity_timer(&mut self) {
        self.timers.remote_inactivity_timer.arm(
            self.this_poll.now,
            self.socket_opts.remote_inactivity_timeout,
            true,
            "remote advanced",
        );
    }

    // When do we need to poll next time based on timers expiring
    // Whether the flight's tail may be probed: it is outstanding, nothing
    // new waits behind it, no probe went out for this flight, and neither
    // recovery nor the retransmission timer is resending.
    fn tail_probe_due(&self) -> bool {
        self.confirmed
            && self.rto_retransmissions == 0
            && self.tail_probe_seq_nr.is_none()
            && !self.recovery.is_recovering()
            && self.last_sent_seq_nr == self.highest_sent_seq_nr
            && self.user_tx_segments.next_seq_nr() - 1 == self.highest_sent_seq_nr
            && self.user_tx_segments.tail_is_outstanding_data()
    }

    // Two measured round trips after the tail went out or the last ACK
    // came, and the time a receiver may hold back an ACK, unless the
    // retransmission timer comes first (RFC 8985 section 7.2: 2 ms more,
    // 200 ms when one packet is out, which a receiver may ACK late).
    fn update_tail_probe_timer(&mut self) {
        if !self.rtte.has_sample() || !self.tail_probe_due() {
            self.timers.tail_probe.turn_off("no tail to probe");
            return;
        }
        if self.timers.tail_probe.poll_at().is_some() {
            return;
        }
        let one_out = self.user_tx_segments.acked_seq_nr() + 1 == self.highest_sent_seq_nr;
        let wait = self.rtte.roundtrip_time() * 2
            + if one_out {
                TAIL_PROBE_LATE_ACK
            } else {
                TAIL_PROBE_SLACK
            };
        if self
            .timers
            .retransmit
            .poll_at()
            .is_some_and(|rto| rto <= self.this_poll.now + wait)
        {
            self.timers.tail_probe.turn_off("the timer comes first");
            return;
        }
        self.timers
            .tail_probe
            .arm(self.this_poll.now, wait, false, "tail outstanding");
    }

    fn next_timer_to_poll(&mut self) -> Option<Instant> {
        // A send waits on the transport: try it again after SEND_RETRY at the
        // latest, as the transport's own wake may never come.
        if self.this_poll.transport_pending {
            let retry = self.this_poll.now + SEND_RETRY;
            return Some(
                self.timers
                    .remote_inactivity_timer
                    .poll_at()
                    .map_or(retry, |at| at.min(retry)),
            );
        }

        // Wait for the earliest of our timers to fire.
        [
            self.timers.ack_delay_timer.poll_at(),
            self.timers.retransmit.poll_at(),
            self.timers.remote_inactivity_timer.poll_at(),
            self.timers.recovery_pipe_expiry.take().poll_at(), // take() disarms it on every call
            self.timers.syn_ack_resend.poll_at(),
            self.timers.pace.take().poll_at(), // armed again by each pass the pacer stops
            self.timers.tail_probe.poll_at(),
        ]
        .into_iter()
        .flatten()
        .min()
    }

    /// If this is an incoming connection, send back an ACK.
    fn maybe_send_syn_ack(&mut self, cx: &mut std::task::Context<'_>) -> crate::Result<()> {
        let sent_count = match self.state {
            VirtualSocketState::SynReceived => 0,
            VirtualSocketState::SynAckSent { count }
                if self.timers.syn_ack_resend.expired(self.this_poll.now) =>
            {
                count
            }
            VirtualSocketState::SynAckSent { .. } => {
                // Timer hasn't expired yet.
                return Ok(());
            }
            _ => {
                self.timers.syn_ack_resend.turn_off("past SYN-ACK");
                return Ok(());
            }
        };
        if sent_count >= self.socket_opts.syn_resends {
            return Err(Error::MaxSynAckRetransmissionsReached);
        }
        if self.send_ack(cx)? {
            self.state = VirtualSocketState::SynAckSent {
                count: sent_count + 1,
            };

            if sent_count > 0 {
                self.socket.stats.synack_retransmissions.increment(1);
            }
            // After the last, the remote has the connect timeout to answer.
            let wait = if sent_count + 1 >= self.socket_opts.syn_resends {
                self.socket_opts.connect_timeout
            } else {
                SYNACK_RESEND_INTERNAL
            };
            self.timers
                .syn_ack_resend
                .arm(self.this_poll.now, wait, true, "retransmitted SYN-ACK");
        }
        Ok(())
    }

    fn transition_to_fin_wait_1(&mut self) {
        log_if_changed!(Level::DEBUG, "state", self, |s| s.state, |s| {
            s.state.transition_to_fin_wait_1(s.seq_nr);
        });
    }

    /// Once nothing holds the stream, the data it never sent goes, and the
    /// FIN follows what is already in flight. libtorrent v2.0.15
    /// utp_stream.cpp:955-993: destroy() closes, which sends the FIN now
    /// unless a packet waits to go (938-952), then clears the write buffer;
    /// packets already sent stay to be acknowledged or time out.
    fn drop_unsent_data(&mut self) {
        let dropped = self.user_tx_segments.drop_unsent();
        let unsegmented = self.this_poll.unsegmented_data;
        self.user_tx
            .keep_front(self.user_tx_segments.total_len_bytes());
        self.this_poll.unsegmented_data = 0;
        if dropped > 0 || unsegmented > 0 {
            debug!(dropped, unsegmented, "stream released: unsent data dropped");
        }
        self.seq_nr = self.user_tx_segments.next_seq_nr();
    }

    fn unsent_data_exists(&mut self) -> bool {
        // either unsegmented data exists, or unsent data exists or both
        self.this_poll.unsegmented_data > 0
            || self
                .user_tx_segments
                .iter_mut_for_sending(None)
                .any(|s| s.send_count() == 0)
    }

    /// Arms the timer for the earliest timer-driven poll. Every return of
    /// Pending calls this, a send the transport could not take included.
    fn arm_next_poll(&mut self, cx: &mut std::task::Context<'_>) {
        if let Some(instant) = self.next_timer_to_poll() {
            let duration = instant - self.this_poll.now;
            trace!(?duration, "will repoll in");
            if !self.timers.arm_in(cx, duration) {
                trace!(deadline = ?duration, "failed arming poll timer, waking to repoll");
                cx.waker().wake_by_ref();
            }
        }
    }

    fn poll(&mut self, cx: &mut std::task::Context<'_>) -> Poll<crate::Result<()>> {
        const INGRESS_BATCH: usize = 64;
        let mut ingress_remaining = INGRESS_BATCH;

        macro_rules! bail_if_err {
            ($e:expr) => {
                match $e {
                    Ok(()) => {
                        if self.this_poll.restart {
                            continue;
                        }
                    }
                    Err(e) => {
                        self.just_before_death(cx, Some(&e));
                        return Poll::Ready(Err(e));
                    }
                }
            };
        }

        macro_rules! pending_if_cannot_send {
            ($e:expr) => {{
                bail_if_err!($e);
                if self.this_poll.transport_pending {
                    self.arm_next_poll(cx);
                    return Poll::Pending;
                }
                if self.this_poll.restart {
                    continue;
                }
            }};
        }

        self.this_poll.restart = true;

        // Restart can be set by functions within.
        while self.this_poll.restart {
            if ingress_remaining == 0 {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            self.this_poll.transport_pending = false;
            self.this_poll.now = self.env.now();
            self.this_poll.restart = false;

            // If this is an incoming connection, send back an ACK.
            pending_if_cannot_send!(self.maybe_send_syn_ack(cx));

            // We must send an immediate ACK per out-of-order segment. If the loop stopped last
            // time on transport pending this will try again without consuming the RX queue.
            if self.immediate_ack_to_transmit() {
                pending_if_cannot_send!(self.send_ack(cx).map(|_| ()));
            }

            // Read incoming stream.
            pending_if_cannot_send!(self.process_all_incoming_messages(cx, &mut ingress_remaining));

            // Flow control: flush as many in-order messages to user RX as possible.
            bail_if_err!(self.user_rx.flush(cx).map(|_| ()));

            if self
                .timers
                .remote_inactivity_timer
                .expired(self.this_poll.now)
            {
                self.socket.stats.inactivity_timeouts.increment(1);
                let err = Error::RemoteInactiveForTooLong;
                trace!(state=?self.state, "remote was inactive for too long");
                self.just_before_death(cx, Some(&err));
                return Poll::Ready(Err(err));
            }

            bail_if_err!(self.split_tx_queue_into_segments(cx));

            // (Re)send tx queue.
            pending_if_cannot_send!(self.send_tx_queue(cx));

            let released = self.user_rx.is_reader_dropped() && self.user_tx.is_writer_dropped();
            if released && !self.state.is_local_fin_or_later() {
                self.drop_unsent_data();
            }
            if (released || self.user_tx.is_writer_shutdown())
                && !self.unsent_data_exists()
                && !self.state.is_local_fin_or_later()
            {
                debug!("consumer closed and no data to send, shutting down");
                self.transition_to_fin_wait_1();
            }

            // (Re)send a pending FIN if needed.
            pending_if_cannot_send!(self.maybe_send_fin(cx).map(|_| ()));

            // Send an ACK if nothing sent yet and sending an ACK is necessary.
            pending_if_cannot_send!(self.maybe_send_ack(cx).map(|_| ()));

            // Quit normally if both sides sent FIN and ACKed each other.
            if self.state_is_closed() {
                self.just_before_death(cx, None);
                return Poll::Ready(Ok(()));
            }

            // If we are done and there's nothing outstanding to send, give the remote last chance to
            // send a meaningful update (their FIN) or die.
            if self.state.is_local_fin_or_later() {
                const SHUTDOWN_FINAL_CHANCE_DELAY: Duration = Duration::from_secs(1);
                self.timers.remote_inactivity_timer.arm(
                    self.this_poll.now,
                    SHUTDOWN_FINAL_CHANCE_DELAY,
                    false,
                    "both reader and writer are dead",
                );
            }

            self.update_tail_probe_timer();
            self.arm_next_poll(cx);

            if ingress_remaining == 0 {
                cx.waker().wake_by_ref();
            }
            return Poll::Pending;
        }

        Poll::Ready(Err(Error::BugUnreachable))
    }
}

impl<T, E> Drop for VirtualSocket<T, E> {
    fn drop(&mut self) {
        self.socket.stats.live_virtual_sockets.decrement(1);
        self.user_tx.mark_vsock_closed();
        self.user_rx.mark_vsock_closed();
    }
}

// See field descriptions / meanings in struct VirtualSocket
pub struct StreamArgs {
    conn_id_send: SeqNr,
    last_remote_timestamp: u32,
    last_remote_received: Instant,

    seq_nr: SeqNr,
    last_sent_seq_nr: SeqNr,
    last_consumed_remote_seq_nr: SeqNr,
    last_sent_ack_nr: SeqNr,

    rtt: Option<Duration>,
    remote_window: u32,

    state: VirtualSocketState,
    parent_span: Option<tracing::Span>,
}

impl StreamArgs {
    /// The connection id the stream sends with: the remote's receive id.
    pub(crate) fn conn_id_send(&self) -> SeqNr {
        self.conn_id_send
    }

    pub fn new_outgoing(
        remote_ack: &UtpHeader,
        syn_sent_ts: Instant,
        ack_received_ts: Instant,
    ) -> Self {
        let conn_id = remote_ack.connection_id;
        Self {
            conn_id_send: conn_id + 1,
            last_remote_timestamp: remote_ack.timestamp_microseconds,
            last_remote_received: ack_received_ts,
            remote_window: remote_ack.wnd_size,

            // The next ST_DATA must be +1 from initial SYN.
            seq_nr: remote_ack.ack_nr + 1,
            last_sent_seq_nr: remote_ack.ack_nr,

            // On connect, the client sends a number that represents the next ST_DATA number.
            // Pretend that we saw the non-existing previous one.
            last_sent_ack_nr: remote_ack.seq_nr - 1,
            last_consumed_remote_seq_nr: remote_ack.seq_nr - 1,

            rtt: ack_received_ts.checked_duration_since(syn_sent_ts),

            state: VirtualSocketState::Established,

            parent_span: None,
        }
    }

    pub fn new_incoming(next_seq_nr: SeqNr, remote_syn: &UtpHeader, received: Instant) -> Self {
        Self {
            conn_id_send: remote_syn.connection_id,
            last_remote_timestamp: remote_syn.timestamp_microseconds,
            last_remote_received: received,
            remote_window: 0, // remote_syn.wnd_size should be 0 anyway. We can't send anything first.

            seq_nr: next_seq_nr,
            last_sent_seq_nr: next_seq_nr - 1,
            // The connecting client will send the next ST_DATA packet with seq_nr + 1.
            last_consumed_remote_seq_nr: remote_syn.seq_nr,
            last_sent_ack_nr: remote_syn.seq_nr,

            // For RTTE
            rtt: None,

            state: VirtualSocketState::SynReceived,
            parent_span: None,
        }
    }

    pub fn with_parent_span(mut self, parent_span: tracing::Span) -> Self {
        self.parent_span = Some(parent_span);
        self
    }
}

pub(crate) struct UtpStreamStarter<T, E> {
    stream: UtpStream,
    vsock: VirtualSocket<T, E>,
    cancellation_token: CancellationToken,
}

impl<T: Transport, E: UtpEnvironment> UtpStreamStarter<T, E> {
    pub fn start(self) -> crate::Result<UtpStream> {
        let Self {
            stream,
            mut vsock,
            cancellation_token,
        } = self;

        let parent = vsock.parent_span.as_ref().and_then(|s| s.id());
        let span = debug_span!(parent: parent, "utp_stream", remote=?vsock.remote);
        let runtime = vsock
            .socket
            .runtime
            .upgrade()
            .ok_or(Error::DispatcherDead)?;
        runtime.register(move |transport| {
            vsock.sender = transport.sender();
            vsock.transport = Some(transport);
            task_with_cancel(span, cancellation_token, vsock.run_forever())
        })?;
        Ok(stream)
    }

    #[cfg(test)]
    pub fn new(
        socket: &Arc<crate::UtpSocket<T, E>>,
        remote: SocketAddr,
        rx: IngressReceiver,
        args: StreamArgs,
    ) -> UtpStreamStarter<T, E> {
        Self::new_shared(&socket.state, remote, rx, args)
    }

    #[cfg(test)]
    pub(crate) fn new_shared(
        socket: &Arc<SocketState<T, E>>,
        remote: SocketAddr,
        rx: IngressReceiver,
        args: StreamArgs,
    ) -> Self {
        let (_, guard) = socket.retirements.registration();
        Self::new_registered(socket, remote, rx, args, guard)
    }

    pub(crate) fn new_registered(
        socket: &Arc<SocketState<T, E>>,
        remote: SocketAddr,
        rx: IngressReceiver,
        args: StreamArgs,
        guard: RetirementGuard,
    ) -> Self {
        let StreamArgs {
            conn_id_send,
            last_remote_timestamp,
            last_remote_received,
            seq_nr,
            last_sent_seq_nr,
            last_consumed_remote_seq_nr,
            last_sent_ack_nr,
            rtt,
            remote_window,
            parent_span,
            state,
        } = args;

        let ss = SegmentSizes::new(SegmentSizesConfig {
            is_ipv4: remote.is_ipv4(),
            link_mtu: socket.probe_link_mtu(remote.is_ipv4()),
            ..Default::default()
        });

        let (user_rx, read_half) = UserRx::build(
            socket.opts().vsock_rx_bufsize,
            NonZeroUsize::new(ss.mss() as usize).unwrap(),
        );

        let user_tx = UserTx::new(socket.opts().vsock_tx_bufsize_bytes_initial);
        let write_half = UtpStreamWriteHalf::new(user_tx.clone());

        let env = socket.env.copy();
        let now = env.now();
        // The gap on the packet that opened the stream, taken as it arrived.
        let reply_us = match last_remote_timestamp {
            0 => 0,
            sent => ((last_remote_received - socket.created).as_micros() as u32).wrapping_sub(sent),
        };

        let cancellation_token = socket.cancellation_token.child_token();

        let vsock = VirtualSocket {
            state,
            segment_sizes: ss,
            env,
            socket_opts: socket.opts().clone(),
            congestion_controller: {
                let mut ctrl = Ledbat::new(ss.mss() as usize, socket.opts().ledbat);
                ctrl.set_remote_window(remote_window as usize);
                socket
                    .stats
                    .max_congestion_window_bytes
                    .fetch_max(ctrl.cwnd_bytes() as u64, Ordering::Relaxed);
                ctrl
            },

            socket_created: socket.created,
            remote,
            conn_id_send,
            timers: Timers {
                retransmit: Timer::default(),
                sleep: Box::pin(tokio::time::sleep(Duration::from_secs(0))),
                ack_delay_timer: Timer::default(),
                recovery_pipe_expiry: Timer::default(),
                syn_ack_resend: Timer::default(),
                pace: Timer::default(),
                tail_probe: Timer::default(),
                remote_inactivity_timer: {
                    let mut timer = Timer::default();
                    if matches!(
                        state,
                        VirtualSocketState::SynReceived | VirtualSocketState::SynAckSent { .. }
                    ) {
                        timer.arm(
                            now,
                            socket.opts().remote_inactivity_timeout,
                            true,
                            "initial",
                        );
                    }
                    timer
                },
            },
            delay: DelayTracker::new(last_remote_received, reply_us),
            last_remote_window: remote_window,
            seq_nr,
            last_sent_seq_nr,
            highest_sent_seq_nr: last_sent_seq_nr,
            tail_probe_seq_nr: None,
            last_consumed_remote_seq_nr,
            last_sent_ack_nr,
            rto_retransmissions: 0,
            fast_resend_seq_nr: seq_nr,
            confirmed: !matches!(
                state,
                VirtualSocketState::SynReceived | VirtualSocketState::SynAckSent { .. }
            ),
            last_remote_packet: last_remote_received,
            fin_sends: 0,
            idle_since: None,
            consumed_but_unacked_bytes: 0,
            rx,
            user_tx_segments: Segments::new(seq_nr),
            user_tx,
            rtte: {
                let mut rtte = RttEstimator::new(socket.opts().min_timeout);
                if !matches!(
                    state,
                    VirtualSocketState::SynReceived | VirtualSocketState::SynAckSent { .. }
                ) {
                    rtte.confirm();
                }
                if let Some(rtt) = rtt {
                    rtte.sample(rtt);
                }
                rtte
            },
            this_poll: ThisPoll {
                now,
                tmp_buf: vec![0u8; (ss.max_ss() + UTP_HEADER) as usize],
                transport_pending: false,
                restart: false,
                unsegmented_data: 0,
            },
            parent_span,
            drop_guard: guard,
            user_rx,
            last_sent_window: if matches!(state, VirtualSocketState::Established) {
                // Pretend we sent the window so that it doesn't trigger a window update packet
                // without any data sent.
                socket.opts().vsock_rx_bufsize.get() as u32
            } else {
                0
            },

            socket: socket.clone(),
            transport: None,
            sender: None,
            recovery: Recovery::default(),
            pacer: Pacer::default(),
        };

        socket.stats.live_virtual_sockets.increment(1);

        let stream = UtpStream::new(read_half, write_half, vsock.remote);
        UtpStreamStarter {
            stream,
            vsock,
            cancellation_token,
        }
    }

    pub fn disarm(mut self) {
        self.vsock.drop_guard.disarm();
    }
}

const TIMER_RETRANSMIT: u8 = 0;
const TIMER_INACTIVITY: u8 = 1;
const TIMER_ACK_DELAY: u8 = 2;
const TIMER_RECOVERY_PIPE: u8 = 3;
// Decays past this many leave the window at its floor whatever it was.
const IDLE_DECAYS_MAX: u128 = 64;

const TIMER_SYN_ACK_RESEND: u8 = 4;
const TIMER_PACE: u8 = 5;
const TIMER_TAIL_PROBE: u8 = 6;
// What a tail probe waits past two round trips: RFC 8985 section 7.2.
const TAIL_PROBE_SLACK: Duration = Duration::from_millis(2);
const TAIL_PROBE_LATE_ACK: Duration = Duration::from_millis(200);

/// A send the transport could not take is tried again after this, whether
/// or not the transport wakes the stream first. A kernel datagram socket
/// wakes only the last task that found it full (tokio 1.54.1
/// `ScheduledIo` keeps one writer waker, scheduled_io.rs:119), and every
/// stream on the socket and the socket's own dispatcher send through it, so
/// that wake can go to another task.
pub(crate) const SEND_RETRY: Duration = Duration::from_millis(1);

#[derive(Default, Debug, Clone, Copy, PartialEq)]
enum Timer<const NAME: u8> {
    #[default]
    Idle,
    Armed {
        expires_at: Instant,
        expiry_reported: bool,
    },
}

impl<const NAME: u8> Timer<NAME> {
    const fn name(&self) -> &'static str {
        match NAME {
            TIMER_RETRANSMIT => "retransmit",
            TIMER_INACTIVITY => "inactivity",
            TIMER_ACK_DELAY => "ack delay",
            TIMER_RECOVERY_PIPE => "recovery pipe",
            TIMER_SYN_ACK_RESEND => "syn ack resend",
            TIMER_PACE => "pace",
            TIMER_TAIL_PROBE => "tail probe",
            _ => "unknown",
        }
    }

    fn expired(&self, now: Instant) -> bool {
        match self {
            Timer::Idle => false,
            Timer::Armed { expires_at, .. } => *expires_at <= now,
        }
    }

    fn report_expiry(&mut self) -> bool {
        match self {
            Timer::Armed {
                expiry_reported, ..
            } => !std::mem::replace(expiry_reported, true),
            Timer::Idle => false,
        }
    }

    fn take(&mut self) -> Self {
        let v = *self;
        *self = Timer::Idle;
        v
    }

    fn poll_at(&self) -> Option<Instant> {
        match *self {
            Timer::Idle => None,
            Timer::Armed { expires_at, .. } => Some(expires_at),
        }
    }

    fn turn_off(&mut self, reason: &'static str) {
        if !matches!(self, Timer::Idle) {
            trace!(reason, "turning off {} timer", self.name())
        }
        *self = Timer::Idle
    }

    fn set(&mut self, expires_at: Instant) {
        *self = Timer::Armed {
            expires_at,
            expiry_reported: false,
        }
    }

    fn arm(&mut self, now: Instant, delay: Duration, restart: bool, reason: &'static str) {
        *self = match *self {
            Timer::Idle => {
                trace!(?delay, reason, "arming {} timer", self.name());
                Timer::Armed {
                    expires_at: now + delay,
                    expiry_reported: false,
                }
            }
            Timer::Armed { .. } if restart => {
                trace!(?delay, reason, "arming {} timer", self.name());
                Timer::Armed {
                    expires_at: now + delay,
                    expiry_reported: false,
                }
            }
            Timer::Armed {
                expires_at,
                expiry_reported,
            } => {
                let new_expires = now + delay;
                if new_expires < expires_at {
                    trace!(
                        reason,
                        "rearming {} timer, expires in {:?}",
                        self.name(),
                        new_expires - now
                    );
                }
                Timer::Armed {
                    expires_at: expires_at.min(new_expires),
                    expiry_reported: expiry_reported && new_expires >= expires_at,
                }
            }
        };
    }
}

struct Timers {
    sleep: Pin<Box<Sleep>>,
    remote_inactivity_timer: Timer<TIMER_INACTIVITY>,
    recovery_pipe_expiry: Timer<TIMER_RECOVERY_PIPE>,
    retransmit: Timer<TIMER_RETRANSMIT>,
    ack_delay_timer: Timer<TIMER_ACK_DELAY>,
    syn_ack_resend: Timer<TIMER_SYN_ACK_RESEND>,
    // Data waits on the pacer until then.
    pace: Timer<TIMER_PACE>,
    // The flight's tail is resent then, if still unacknowledged.
    tail_probe: Timer<TIMER_TAIL_PROBE>,
}

impl Timers {
    /// Schedule the timer to re-poll the dispatcher at provided deadline.
    ///
    /// Returns true if the waker was registered (i.e. it's ok to return Poll::Pending).
    fn arm_in(&mut self, cx: &mut std::task::Context<'_>, duration: Duration) -> bool {
        let deadline = Instant::now() + duration;
        let mut sl = self.sleep.as_mut();
        sl.as_mut().reset(deadline);
        sl.as_mut().poll(cx) == Poll::Pending
    }
}

// The main dispatch loop for the virtual socket is here.
impl<T: Transport, Env: UtpEnvironment> std::future::Future for VirtualSocket<T, Env> {
    type Output = crate::Result<()>;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        self.get_mut().poll(cx)
    }
}
