// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
// Lifted from librqbit-utp 0.7.0 (Apache-2.0), Copyright (c) the librqbit authors.
// Modified by the Weaver authors, 2026-11-10.

// NewReno fast recovery, counting duplicates from SACKs.
use std::time::Duration;

use tokio::time::Instant;

use tracing::event;

use crate::{
    congestion::ledbat::Ledbat,
    constants::{RECOVERY_TRACING_LOG_LEVEL, SACK_DUP_THRESH},
    raw::{Type, UtpHeader},
    seq_nr::SeqNr,
    stats::{Counter, UtpStats},
    stream_tx_segments::{OnAckResult, Pipe, Segments},
};

pub struct AckContext<'a> {
    pub last_sent_seq_nr: SeqNr,
    pub next_seq_nr: SeqNr,
    pub now: Instant,
    pub rtt: Duration,
    pub stats: &'a UtpStats,
}

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

// Mirrors libtorrent v2.0.15 utp_stream.cpp. A SACK that names more than
// `SACK_DUP_THRESH` packets past the first missing one marks it lost at once
// (parse_sack, 1286-1320: "dups > dup_ack_limit"). Otherwise a STATE packet
// that repeats the cumulative ACK while data is outstanding counts as one
// duplicate, and `SACK_DUP_THRESH` of them mark it lost (2723-2728, 2814).
// An ACK that moves the cumulative ACK forward, or a DATA packet, is no
// duplicate.
fn count_sack_duplicates(
    header: &UtpHeader,
    on_ack_result: &OnAckResult,
    prev_dup_acks: u8,
    stats: &UtpStats,
) -> u8 {
    if on_ack_result.acked_segments_count > 0 {
        return 0;
    }
    let sacked = header
        .extensions
        .selective_ack
        .as_ref()
        .map_or(0, |sack| sack.as_bitslice().count_ones());
    if sacked > SACK_DUP_THRESH as usize {
        event!(
            RECOVERY_TRACING_LOG_LEVEL,
            prev_dup_acks,
            sacked,
            ?header.ack_nr,
            "more than the threshold of segments past the first SACKed"
        );
        stats.duplicate_acks_received.increment(1);
        return SACK_DUP_THRESH;
    }
    if header.htype == Type::State {
        stats.duplicate_acks_received.increment(1);
        return prev_dup_acks.saturating_add(1);
    }
    prev_dup_acks
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
            if header.htype == Type::State && last.ack_nr == header.ack_nr && !is_window_update =>
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

    pub fn on_ack(
        &mut self,
        header: &UtpHeader,
        on_ack_result: &OnAckResult,
        tx_segs: &mut Segments,
        congestion_controller: &mut Ledbat,
        context: AckContext<'_>,
    ) {
        let AckContext {
            last_sent_seq_nr,
            next_seq_nr,
            now,
            rtt,
            stats,
        } = context;
        self.receiver_supports_sack |= header.extensions.selective_ack.is_some();

        match &mut self.phase {
            RecoveryPhase::IgnoringUntilRecoveryPoint { recovery_point } => {
                if header.ack_nr.is_at_or_after(*recovery_point) {
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
                    stats.packets_lost.increment(1);
                    congestion_controller.on_enter_recovery(now, high_ack + 1, next_seq_nr);
                }

                let high_rxt = high_ack;
                let pipe_estimate = tx_segs.calc_pipe(high_rxt, last_sent_seq_nr, rtt, now);
                let cwnd = congestion_controller.recovery_window();

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
                if header.ack_nr.is_at_or_after(rec.recovery_point) {
                    // A full acknowledgement: LEDBAT keeps the window the loss left.
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
        congestion::ledbat::{Ledbat, LedbatParams},
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
                    htype: Type::State,
                    ack_nr: 9.into(),
                    wnd_size: 1 << 20,
                    ..Default::default()
                },
                &OnAckResult::default(),
                &mut segs,
                &mut cc,
                super::AckContext {
                    last_sent_seq_nr: 14.into(),
                    next_seq_nr: 15.into(),
                    now: Instant::now(),
                    rtt: Duration::from_millis(50),
                    stats: &stats,
                },
            );
        }
        (before, cc.window(), recovery.is_recovering())
    }

    // One SACK that names `sacked` packets past the missing first one.
    fn one_sack_past_the_first(sacked: usize) -> bool {
        let stats = UtpStats::default();
        let now = Instant::now();
        let mut segs = Segments::new(10.into());
        for _ in 0..8 {
            assert!(segs.enqueue(500, false));
        }
        for mut item in segs.iter_mut_for_sending(None) {
            item.on_sent(now, &stats);
        }
        let header = UtpHeader {
            htype: Type::State,
            ack_nr: 9.into(),
            wnd_size: 1 << 20,
            extensions: crate::raw::Extensions {
                selective_ack: crate::raw::selective_ack::SelectiveAck::new_test(0..sacked),
                ..Default::default()
            },
            ..Default::default()
        };
        let result = segs.remove_up_to_ack(now, &header);
        let mut cc = Ledbat::new(500, LedbatParams::default());
        cc.set_remote_window(usize::MAX);
        let mut recovery = Recovery::new();
        recovery.on_ack(
            &header,
            &result,
            &mut segs,
            &mut cc,
            super::AckContext {
                last_sent_seq_nr: 17.into(),
                next_seq_nr: 18.into(),
                now,
                rtt: Duration::from_millis(50),
                stats: &stats,
            },
        );
        recovery.is_recovering()
    }

    // libtorrent v2.0.15 utp_stream.cpp:1314: three SACKed packets past a
    // hole are reordering; a fourth makes it a loss.
    #[test]
    fn a_hole_is_lost_only_with_more_than_three_sacked_after_it() {
        assert!(!one_sack_past_the_first(3));
        assert!(one_sack_past_the_first(4));
    }

    // A DATA packet, or an ACK that moves forward, repeats nothing.
    #[test]
    fn only_a_repeated_state_ack_counts_as_a_duplicate() {
        let stats = UtpStats::default();
        let header = |htype| UtpHeader {
            htype,
            ack_nr: 9.into(),
            extensions: crate::raw::Extensions {
                selective_ack: crate::raw::selective_ack::SelectiveAck::new_test([0]),
                ..Default::default()
            },
            ..Default::default()
        };
        let none = OnAckResult::default();
        let advanced = OnAckResult {
            acked_segments_count: 1,
            ..Default::default()
        };
        assert_eq!(
            super::count_sack_duplicates(&header(Type::State), &none, 1, &stats),
            2
        );
        assert_eq!(
            super::count_sack_duplicates(&header(Type::Data), &none, 1, &stats),
            1
        );
        assert_eq!(
            super::count_sack_duplicates(&header(Type::State), &advanced, 2, &stats),
            0
        );
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
