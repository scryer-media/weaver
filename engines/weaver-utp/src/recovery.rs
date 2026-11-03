// Lifted from librqbit-utp 0.7.0 (Apache-2.0), Copyright (c) the librqbit authors.
// Modified by the Weaver authors, 2026-11-03.

// NewReno fast recovery, counting duplicates from SACKs.
use std::time::Duration;

use tokio::time::Instant;

use tracing::event;

use crate::{
    congestion::CongestionController,
    constants::{RECOVERY_TRACING_LOG_LEVEL, SACK_DUP_THRESH},
    raw::{Type, UtpHeader},
    seq_nr::SeqNr,
    stats::{Counter, UtpStats},
    stream_tx_segments::{OnAckResult, Pipe, Segments},
};

#[derive(Debug, Clone, Copy)]
pub struct Recovering {
    recovery_point: SeqNr,

    // The packet sender may modify these so they are pub.
    pub high_rxt: SeqNr,
    total_retransmitted_segments: usize,

    // conservative estimate of how many bytes are in transit
    pub pipe_estimate: Pipe,

    // During recovery we manage cwnd, not congestion controller.
    // After recovery is done, this will be used to configure
    // congestion controller.
    cwnd: usize,
}

impl Recovering {
    pub fn recovery_point(&self) -> SeqNr {
        self.recovery_point
    }

    pub fn cwnd(&self) -> usize {
        self.cwnd.saturating_sub(self.pipe_estimate.pipe)
    }

    pub fn total_retransmitted_segments(&self) -> usize {
        self.total_retransmitted_segments
    }

    pub fn increment_total_transmitted_segments(&mut self) {
        self.total_retransmitted_segments += 1;
    }
}

#[derive(Debug, Clone, Copy)]
enum RecoveryPhase {
    IgnoringUntilRecoveryPoint { recovery_point: SeqNr },
    CountingDuplicates { dup_acks: u8 },
    Recovering(Recovering),
}

#[derive(Debug, Clone, Copy)]
struct LastAck {
    // Tracks if an ACK is a window update before it switches to SACK mode.
    window: u32,
    // Tracks if the
    //      acknowledgment number is equal to the greatest acknowledgment
    //      received on the given connection
    ack_nr: SeqNr,
}

fn count_sack_duplicates(
    header: &UtpHeader,
    _on_ack_result: &OnAckResult,
    prev_dup_acks: u8,
    stats: &UtpStats,
) -> u8 {
    match &header.extensions.selective_ack {
        Some(sack) => {
            // If we received at least 3 segments above high_ack, we need to enter recovery
            // right away.
            let segments_past_first = sack.as_bitslice().count_ones();
            if segments_past_first >= SACK_DUP_THRESH as usize {
                event!(
                    RECOVERY_TRACING_LOG_LEVEL,
                    prev_dup_acks,
                    segments_past_first,
                    ?header.ack_nr,
                    "too many segments past first ACKed"
                );
                stats.duplicate_acks_received.increment(1);
                return SACK_DUP_THRESH;
            }

            // ONLY if the SACK carries new data we +1 the counter. I.e.
            // "a segment that arrives carrying a SACK block that
            // identifies previously unacknowledged and un-SACKed octets between
            // HighACK and HighData".
            //
            // However, uTP cannot convey any information past the SACK size limit
            // (usually 64 segments). So we assume it is trying to do that here, and
            // ignore actually looking at how many segments were ACKed.
            stats.duplicate_acks_received.increment(1);
            prev_dup_acks + 1
        }
        None => {
            // If the incoming ACK is a cumulative acknowledgment, the TCP MUST
            // reset DupAcks to zero.
            0
        }
    }
}

fn count_non_sack_duplicates(
    header: &UtpHeader,
    _on_ack_result: &OnAckResult,
    prev_dup_acks: u8,

    last_ack: &mut Option<LastAck>,
    stats: &UtpStats,
) -> u8 {
    let is_window_update = last_ack.is_some_and(|l| l.window != header.wnd_size);

    match *last_ack {
        Some(last)
            if header.htype == Type::ST_STATE
                && last.ack_nr == header.ack_nr
                && !is_window_update =>
        {
            stats.duplicate_acks_received.increment(1);
            let dup_acks = prev_dup_acks.saturating_add(1);
            event!(RECOVERY_TRACING_LOG_LEVEL, ?header.ack_nr, high_ack=?*last.ack_nr, ?dup_acks, "counted duplicate ACK in non-sack mode");
            dup_acks
        }
        _ => {
            *last_ack = Some(LastAck {
                window: header.wnd_size,
                ack_nr: header.ack_nr,
            });
            0
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Recovery {
    receiver_supports_sack: bool,
    last_ack: Option<LastAck>,
    phase: RecoveryPhase,
}

impl Recovery {
    pub fn new() -> Self {
        Self {
            receiver_supports_sack: false,
            last_ack: None,
            phase: RecoveryPhase::CountingDuplicates { dup_acks: 0 },
        }
    }

    #[allow(unused)]
    pub fn cwnd(&self) -> Option<usize> {
        match &self.phase {
            RecoveryPhase::Recovering(rec) => Some(rec.cwnd),
            _ => None,
        }
    }

    pub fn remaining_cwnd(&self, last_remote_window: u32) -> Option<usize> {
        match &self.phase {
            RecoveryPhase::Recovering(rec) => Some(
                rec.cwnd
                    .min(last_remote_window as usize)
                    .saturating_sub(rec.pipe_estimate.pipe),
            ),
            _ => None,
        }
    }

    pub fn is_recovering(&self) -> bool {
        matches!(self.phase, RecoveryPhase::Recovering { .. })
    }

    pub fn recovering_mut(&mut self) -> Option<&mut Recovering> {
        match &mut self.phase {
            RecoveryPhase::Recovering(rec) => Some(rec),
            _ => None,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn on_ack(
        &mut self,
        header: &UtpHeader,
        on_ack_result: &OnAckResult,
        tx_segs: &mut Segments,
        last_sent_seq_nr: SeqNr,
        congestion_controller: &mut dyn CongestionController,
        now: Instant,
        rtt: Duration,
        stats: &UtpStats,
    ) {
        self.receiver_supports_sack |= header.extensions.selective_ack.is_some();

        match &mut self.phase {
            RecoveryPhase::IgnoringUntilRecoveryPoint { recovery_point } => {
                if header.ack_nr >= *recovery_point {
                    event!(RECOVERY_TRACING_LOG_LEVEL, ?recovery_point, ?header.ack_nr, "exiting IgnoringUntilRecoveryPoint");
                    self.phase = RecoveryPhase::CountingDuplicates { dup_acks: 0 }
                }
            }
            RecoveryPhase::CountingDuplicates { dup_acks } => {
                // HighAck is the last sequence number fully consumed by receiver.
                let high_ack = match tx_segs.first_seq_nr() {
                    Some(s) => s - 1,
                    None => {
                        // The queue is empty, don't count ACKs.
                        *dup_acks = 0;
                        return;
                    }
                };

                if self.receiver_supports_sack {
                    *dup_acks = count_sack_duplicates(header, on_ack_result, *dup_acks, stats);
                } else {
                    *dup_acks = count_non_sack_duplicates(
                        header,
                        on_ack_result,
                        *dup_acks,
                        &mut self.last_ack,
                        stats,
                    );
                }

                let should_enter_recovery = *dup_acks >= SACK_DUP_THRESH;
                if !should_enter_recovery {
                    return;
                }

                // A lost probe was too large, which says nothing of congestion.
                if !tx_segs.first_is_mtu_probe() {
                    congestion_controller.on_enter_recovery(now, high_ack + 1, last_sent_seq_nr);
                }

                let high_rxt = high_ack;
                let pipe_estimate = tx_segs.calc_pipe(high_rxt, last_sent_seq_nr, rtt, now);
                let cwnd = congestion_controller.sshthresh();

                let rec = Recovering {
                    recovery_point: last_sent_seq_nr,

                    high_rxt: high_ack,
                    total_retransmitted_segments: 0,

                    pipe_estimate,
                    cwnd,
                };

                stats.recovery_enter_count.increment(1);

                event!(RECOVERY_TRACING_LOG_LEVEL, ?rec.recovery_point, ?high_ack, high_data=?last_sent_seq_nr, ack_nr=?header.ack_nr, ?pipe_estimate, ?cwnd, ?rtt, "entered recovery");
                self.phase = RecoveryPhase::Recovering(rec);
            }
            RecoveryPhase::Recovering(rec) => {
                if header.ack_nr >= rec.recovery_point {
                    // A full acknowledgement.
                    // On recover we set cwnd very conservatively not to cause a sudden burst of traffic.
                    // It will very quickly reach sshthresh.
                    let mss = congestion_controller.smss();
                    let cwnd = congestion_controller
                        .sshthresh()
                        .min(tx_segs.calc_flight_size(last_sent_seq_nr).max(mss) + mss);
                    let sshthresh = rec.cwnd;
                    congestion_controller.on_recovered(cwnd, sshthresh);

                    event!(RECOVERY_TRACING_LOG_LEVEL, ?rec.recovery_point, ?header.ack_nr, prev_cwnd=rec.cwnd,
                        ?congestion_controller, ?rec.total_retransmitted_segments, ?rtt, "exited recovery");
                    self.phase = RecoveryPhase::CountingDuplicates { dup_acks: 0 };
                }
            }
        }
    }

    pub(crate) fn on_rto_timeout(&mut self, last_sent_seq_nr: SeqNr, stats: &UtpStats) {
        if let RecoveryPhase::Recovering(rec) = &self.phase {
            event!(
                RECOVERY_TRACING_LOG_LEVEL,
                recovery_point = ?last_sent_seq_nr,
                recovery_state=?rec,
                "on_rto_timeout: RTO during recovery. Ignoring duplicate acks until new recovery point."
            );
            stats.recovery_rto_during_recovery_count.increment(1);
            self.phase = RecoveryPhase::IgnoringUntilRecoveryPoint {
                recovery_point: last_sent_seq_nr,
            }
        }
    }
}

impl Default for Recovery {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::Instant;

    use super::Recovery;
    use crate::{
        congestion::{
            CongestionController,
            ledbat::{Ledbat, LedbatParams},
        },
        raw::{Type, UtpHeader},
        stats::UtpStats,
        stream_tx_segments::{OnAckResult, Segments},
    };

    // Four ACKs of the packet before `first`: three duplicates, which
    // count the first packet in flight lost.
    fn lose_the_first(first_is_probe: bool) -> (usize, usize, bool) {
        let mut segs = Segments::new(10.into());
        assert!(segs.enqueue(1000, first_is_probe));
        for _ in 0..4 {
            assert!(segs.enqueue(500, false));
        }
        let mut cc = Ledbat::new(500, LedbatParams::default());
        cc.set_remote_window(usize::MAX);
        let before = cc.window();
        let mut recovery = Recovery::new();
        let stats = UtpStats::default();
        for _ in 0..4 {
            recovery.on_ack(
                &UtpHeader {
                    htype: Type::ST_STATE,
                    ack_nr: 9.into(),
                    wnd_size: 1 << 20,
                    ..Default::default()
                },
                &OnAckResult::default(),
                &mut segs,
                14.into(),
                &mut cc,
                Instant::now(),
                Duration::from_millis(50),
                &stats,
            );
        }
        (before, cc.window(), recovery.is_recovering())
    }

    #[test]
    fn a_lost_packet_cuts_the_window() {
        let (before, after, recovering) = lose_the_first(false);
        assert!(recovering);
        assert_eq!(after, before / 2);
    }

    #[test]
    fn a_lost_mtu_probe_does_not_cut_the_window() {
        let (before, after, recovering) = lose_the_first(true);
        assert!(recovering);
        assert_eq!(after, before);
    }
}
