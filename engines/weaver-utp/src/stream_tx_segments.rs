// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
// Lifted from librqbit-utp 0.7.0 (Apache-2.0), Copyright (c) the librqbit authors.
// Modified by the Weaver authors, 2026-11-10.

// uTP doesn't resegment, so we pre-segment TX once.
// We also store the delivered state of the segments here.
//
// When an ACK arrives this tells us how much (if any) bytes we can
// remove from the actual TX (bytes) that are stored in struct UserTx.
//
// The actual data is kept in UserTx, this only contains metadata about segments we created, sent and delivered.
// Tracks RTT and other stats per segment.
//
// Main flow:
// 1. New segment(s) to be sent is created via enqueue().
// 2. They are iterated to be sent via iter_mut_for_sending(), and marked as sent (if sent).
// 3. Once they are delivered as indicated by ACK, remove_up_to_ack() is called.
//
// The last segment MAY be an MTU probe, where we try to send a segment larger than
// a known working size, in attempt to increase throughput. If it's not delivered in time,
// it's removed and will need to be requeued again.
//
// The downside is no new segments can be enqueued while an MTU probe is outstanding.
use std::{collections::VecDeque, time::Duration};

use tokio::time::Instant;
use tracing::trace;

use crate::{
    constants::{MAX_RESERVED_DATA_PACKETS, SACK_DUP_THRESH, calc_pipe_expiry},
    raw::UtpHeader,
    seq_nr::SeqNr,
    stats::{Counter, UtpStats},
};

#[derive(Clone, Copy)]
enum SentStatus {
    NotSent,
    SentTime(Instant),
    Retransmitted { count: usize, last_send_ts: Instant },
}

pub struct Segment {
    payload_size: usize,
    payload_offset_absolute: u64,
    is_delivered: bool,
    sent: SentStatus,

    is_mtu_probe: bool,
    // Resends past the first that SACKs of later sends asked for.
    evidence_resends: usize,
    // Resends of the flight's tail as a tail-loss probe.
    probe_resends: usize,

    // Lost: three or more later segments were delivered.
    is_lost: bool,
    is_expired: bool,
}

pub fn rtt_min(rtt1: Option<Duration>, rtt2: Option<Duration>) -> Option<Duration> {
    match (rtt1, rtt2) {
        (None, None) => None,
        (None, Some(r)) | (Some(r), None) => Some(r),
        (Some(r1), Some(r2)) => Some(r1.min(r2)),
    }
}

impl Segment {
    fn update_rtt(&self, now: Instant, rtt: &mut Option<Duration>) {
        match self.sent {
            // This should not happen.
            SentStatus::NotSent | SentStatus::Retransmitted { .. } => {}
            SentStatus::SentTime(sent_ts) => {
                *rtt = rtt_min(*rtt, now.checked_duration_since(sent_ts));
            }
        }
    }

    pub fn retransmit_count(&self) -> usize {
        match self.sent {
            SentStatus::NotSent => 0,
            SentStatus::SentTime(..) => 0,
            SentStatus::Retransmitted { count, .. } => count,
        }
    }

    pub fn send_count(&self) -> usize {
        match self.sent {
            SentStatus::NotSent => 0,
            SentStatus::SentTime(..) => 1,
            SentStatus::Retransmitted { count, .. } => count + 1,
        }
    }

    pub fn is_mtu_probe(&self) -> bool {
        self.is_mtu_probe
    }

    fn last_sent(&self) -> Option<Instant> {
        match self.sent {
            SentStatus::NotSent => None,
            SentStatus::SentTime(instant) => Some(instant),
            SentStatus::Retransmitted { last_send_ts, .. } => Some(last_send_ts),
        }
    }
}

pub struct Segments {
    segments: VecDeque<Segment>,
    // This is including unsent. So it's not FlightSize
    len_bytes: usize,

    // absolute offset in bytes since creation.
    offset: u64,

    // how many bytes were consumed since creation.
    removed_offset: u64,

    // If a SACK seen, this will set how many sgements did it cover.
    sack_depth: usize,

    // SND.UNA - the sequence number of first unacknowledged segment.
    // If all acknowledged, this is the same as SND.NEXT.
    snd_una: SeqNr,
}

pub struct SegmentForSending<'a> {
    segment: &'a mut Segment,
    seq_nr: SeqNr,
    payload_offset: usize,
}

impl SegmentForSending<'_> {
    pub fn is_lost(&self) -> bool {
        self.segment.is_lost
    }

    // Resent on the evidence that marked it lost: it waits for new
    // evidence before it is lost again.
    pub fn on_resent_as_lost(&mut self) {
        self.segment.is_lost = false;
        if self.segment.retransmit_count() - self.segment.probe_resends > 1 {
            self.segment.evidence_resends += 1;
        }
    }

    // Resent as a tail-loss probe: the probe asks for an ACK, it is no
    // judgement that the packet is lost.
    pub fn on_resent_as_probe(&mut self) {
        self.segment.probe_resends += 1;
    }

    // Sends that count toward giving up: the first send, the first resend
    // and the timer's, as libtorrent counts them (utp_stream.cpp:3523);
    // resends of a resend that SACKs showed lost, and tail-loss probes, are
    // not among them.
    pub fn counted_sends(&self) -> usize {
        self.segment.send_count() - self.segment.evidence_resends - self.segment.probe_resends
    }

    pub fn is_expired(&self) -> bool {
        self.segment.is_expired
    }

    // Resends on the evidence of SACKs, beyond the first.
    pub fn evidence_resends(&self) -> usize {
        self.segment.evidence_resends
    }

    pub fn send_count(&self) -> usize {
        self.segment.send_count()
    }

    pub fn seq_nr(&self) -> SeqNr {
        self.seq_nr
    }

    pub fn is_mtu_probe(&self) -> bool {
        self.segment.is_mtu_probe()
    }

    pub fn is_delivered(&self) -> bool {
        self.segment.is_delivered
    }
    pub fn payload_size(&self) -> usize {
        self.segment.payload_size
    }
    pub fn payload_offset(&self) -> usize {
        self.payload_offset
    }

    pub fn on_sent(&mut self, now: Instant, stats: &UtpStats) {
        let ps = self.segment.payload_size as u64;
        self.segment.sent = match self.segment.sent {
            SentStatus::NotSent => {
                stats.send_count.increment(1);
                stats.sent_bytes.increment(ps);
                SentStatus::SentTime(now)
            }
            SentStatus::SentTime(_) => {
                stats.data_retransmissions.increment(1);
                stats.retransmitted_bytes.increment(ps);
                SentStatus::Retransmitted {
                    count: 1,
                    last_send_ts: now,
                }
            }
            SentStatus::Retransmitted { count, .. } => {
                stats.data_retransmissions.increment(1);
                stats.retransmitted_bytes.increment(ps);
                SentStatus::Retransmitted {
                    count: count + 1,
                    last_send_ts: now,
                }
            }
        };
    }
}

#[derive(Default)]
pub struct OnAckResult {
    // How many segments we can remove from the beginning of TX queue.
    pub acked_segments_count: usize,

    // How many bytes we can remove from the beginning of TX queue.
    pub acked_bytes: usize,

    pub max_acked_payload_size: usize,

    // How many segments in TX queue were marked delivered by SACK.
    pub newly_sacked_segment_count: usize,

    pub newly_sacked_byte_count: usize,
    pub new_rtt: Option<Duration>,
}

impl OnAckResult {
    pub(crate) fn update(&mut self, other: &OnAckResult) {
        self.acked_segments_count += other.acked_segments_count;
        self.acked_bytes += other.acked_bytes;
        self.new_rtt = rtt_min(self.new_rtt, other.new_rtt);
        self.newly_sacked_segment_count += other.newly_sacked_segment_count;
        self.newly_sacked_byte_count += other.newly_sacked_byte_count;
    }
}

impl core::fmt::Debug for OnAckResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "acked_segments={}, bytes={}, new_rtt={:?}",
            self.acked_segments_count, self.acked_bytes, self.new_rtt
        )
    }
}

#[derive(Clone, Copy)]
pub struct Pipe {
    pub pipe: usize,
    pub recalc_timer: Option<Instant>,
}

impl std::fmt::Debug for Pipe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "pipe={},recalc_timer={:?}",
            self.pipe,
            self.recalc_timer.map(|t| Instant::now() - t)
        )
    }
}

#[derive(Debug)]
pub enum PopExpiredProbe {
    Expired {
        rewind_to: SeqNr,
        payload_size: usize,
    },
    NotExpired,
    Empty,
}

impl Segments {
    pub fn new(snd_una: SeqNr) -> Self {
        Segments {
            segments: VecDeque::new(),
            len_bytes: 0,
            offset: 0,
            removed_offset: 0,
            sack_depth: 0,
            snd_una,
        }
    }

    pub fn acked_seq_nr(&self) -> SeqNr {
        self.snd_una - 1
    }

    pub fn first_seq_nr(&self) -> Option<SeqNr> {
        if self.segments.is_empty() {
            None
        } else {
            Some(self.snd_una)
        }
    }

    /// The sequence number the next new segment would take.
    pub fn next_seq_nr(&self) -> SeqNr {
        self.snd_una + self.segments.len() as u16
    }

    /// Drops the segments never sent, all at the back, and returns how many
    /// bytes they held. Sent segments stay until acknowledged.
    pub fn drop_unsent(&mut self) -> usize {
        let mut dropped = 0;
        while let Some(segment) = self.segments.back() {
            if segment.send_count() > 0 {
                break;
            }
            dropped += segment.payload_size;
            self.segments.pop_back();
        }
        self.offset -= dropped as u64;
        self.len_bytes -= dropped;
        dropped
    }

    pub fn first_is_mtu_probe(&self) -> bool {
        self.segments.front().is_some_and(|s| s.is_mtu_probe())
    }

    // Whether the last segment is data sent and not yet delivered: no
    // unsent segment waits behind it, and it is no MTU probe.
    pub fn tail_is_outstanding_data(&self) -> bool {
        self.segments
            .back()
            .is_some_and(|s| s.send_count() > 0 && !s.is_delivered && !s.is_mtu_probe)
    }

    pub fn sole_outstanding_is_mtu_probe(&self) -> bool {
        let mut outstanding = self
            .segments
            .iter()
            .filter(|s| !s.is_delivered && s.send_count() > 0);
        outstanding.next().is_some_and(|s| s.is_mtu_probe()) && outstanding.next().is_none()
    }

    #[cfg(test)]
    pub fn outstanding_packets(&self) -> usize {
        self.segments
            .iter()
            .filter(|s| !s.is_delivered && s.send_count() > 0)
            .count()
    }

    pub fn total_len_packets(&self) -> usize {
        self.segments.len()
    }

    #[cfg(test)]
    pub fn count_delivered_test(&self) -> usize {
        self.segments
            .iter()
            .map(|h| if h.is_delivered { 1 } else { 0 })
            .sum()
    }

    pub fn total_len_bytes(&self) -> usize {
        self.len_bytes
    }

    pub fn sack_depth(&self) -> usize {
        self.sack_depth
    }

    pub fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }

    /// Enqueue a segment for sending at the end. If it's an MTU probe, mtu_probe_expiry is not None.
    /// Returns if the segment was enqueued or not.
    #[must_use]
    pub fn enqueue(&mut self, payload_len: usize, is_mtu_probe: bool) -> bool {
        if self.segments.len() >= MAX_RESERVED_DATA_PACKETS {
            return false;
        }
        self.segments.push_back(Segment {
            payload_size: payload_len,
            payload_offset_absolute: self.offset,
            is_delivered: false,
            is_mtu_probe,
            sent: SentStatus::NotSent,
            evidence_resends: 0,
            probe_resends: 0,
            is_lost: false,
            is_expired: false,
        });
        self.offset += payload_len as u64;
        self.len_bytes += payload_len;
        true
    }

    pub fn pop_mtu_probe(&mut self, seq_nr: SeqNr) -> bool {
        let last_segment_seq_nr = self.snd_una + self.segments.len() as u16 - 1;
        match self.segments.pop_back() {
            Some(s) if last_segment_seq_nr == seq_nr && s.is_mtu_probe && !s.is_delivered => {
                self.len_bytes -= s.payload_size;
                self.offset = s.payload_offset_absolute;
                true
            }
            Some(s) => {
                self.segments.push_back(s);
                false
            }
            None => false,
        }
    }

    /// Try to pop an MTU probe if it's expired.
    pub fn pop_expired_mtu_probe(
        &mut self,
        retransmit_timed_out: bool,
        max_probe_retransmissions: usize,
    ) -> PopExpiredProbe {
        match self.segments.pop_back() {
            Some(s) => match (retransmit_timed_out, s.is_mtu_probe) {
                (_, _) if s.is_delivered => {
                    self.segments.push_back(s);
                    PopExpiredProbe::Empty
                }
                (true, true) if s.retransmit_count() >= max_probe_retransmissions => {
                    self.len_bytes -= s.payload_size;
                    self.offset = s.payload_offset_absolute;
                    PopExpiredProbe::Expired {
                        payload_size: s.payload_size,
                        rewind_to: self.snd_una + self.segments.len() as u16 - 1,
                    }
                }
                (_, true) => {
                    self.segments.push_back(s);
                    PopExpiredProbe::NotExpired
                }
                (_, false) => {
                    self.segments.push_back(s);
                    PopExpiredProbe::Empty
                }
            },
            None => PopExpiredProbe::Empty,
        }
    }

    // Returns number of removed headers, and their comibined payload size.
    pub fn remove_up_to_ack(&mut self, now: Instant, ack_header: &UtpHeader) -> OnAckResult {
        let mut removed = 0;
        let mut payload_size = 0;
        let mut newly_sacked_segment_count: usize = 0;
        let mut newly_sacked_byte_count: usize = 0;
        let mut max_acked_payload_size: usize = 0;

        let mut new_rtt: Option<Duration> = None;
        // The receiver holds what it got past a hole until the hole is
        // filled, and a SACK reaches only so far past the cumulative ACK, so
        // an ACK that reports a resent packet can report packets it got long
        // ago: such an ACK times nothing (Karn). Otherwise a packet sent once
        // and not reported before is a sample.
        let mut covers_resend = false;

        if let Some(offset) = ack_header
            .ack_nr
            .signed_offset_from(self.snd_una)
            .filter(|offset| *offset >= 0)
        {
            let drain_count = (offset as usize + 1).min(self.segments.len());
            for segment in self.segments.drain(0..drain_count) {
                if !segment.is_delivered {
                    covers_resend |= segment.retransmit_count() > 0;
                    segment.update_rtt(now, &mut new_rtt);
                }
                removed += 1;
                payload_size += segment.payload_size;
                self.snd_una += 1;
                self.len_bytes -= segment.payload_size;
                max_acked_payload_size = max_acked_payload_size.max(segment.payload_size);
            }
        }

        // If TX start matches with header ACK and it's a selective ACK, mark all segments delivered.
        match (self.first_seq_nr(), ack_header.extensions.selective_ack) {
            (Some(first_seq_nr), Some(sack)) if first_seq_nr.is_after(ack_header.ack_nr) => {
                self.sack_depth = sack.len();

                let sack_start_offset =
                    2 - first_seq_nr.forward_distance_from(ack_header.ack_nr) as isize;

                let mut process_sack = |segment: &mut Segment, is_sacked: bool| {
                    if !segment.is_delivered && is_sacked && segment.send_count() > 0 {
                        segment.is_delivered = true;
                        covers_resend |= segment.retransmit_count() > 0;
                        segment.update_rtt(now, &mut new_rtt);
                        max_acked_payload_size = max_acked_payload_size.max(segment.payload_size);
                        newly_sacked_segment_count += 1;
                        newly_sacked_byte_count += segment.payload_size;
                    }
                };

                if sack_start_offset >= 0 {
                    for (segment, acked) in self
                        .segments
                        .iter_mut()
                        .skip(sack_start_offset as usize)
                        .zip(sack.iter())
                    {
                        process_sack(segment, acked);
                    }
                } else {
                    for (segment, acked) in self
                        .segments
                        .iter_mut()
                        .zip(sack.iter().skip((-sack_start_offset) as usize))
                    {
                        process_sack(segment, acked);
                    }
                }
            }
            _ => {}
        };

        // Cleanup the beginning of the queue if an older ACK ends up marking it delivered.
        while let Some(segment) = self.segments.front() {
            if !segment.is_delivered {
                break;
            }
            removed += 1;
            payload_size += segment.payload_size;
            self.len_bytes -= segment.payload_size;
            self.snd_una += 1;
            self.segments.pop_front().unwrap();
        }

        self.removed_offset += payload_size as u64;
        if covers_resend {
            new_rtt = None;
        }

        OnAckResult {
            acked_segments_count: removed,
            acked_bytes: payload_size,
            max_acked_payload_size,
            newly_sacked_segment_count,
            newly_sacked_byte_count,
            new_rtt,
        }
    }

    // The total payload of sent and unacked segments.
    pub fn calc_flight_size(&self, last_sent_seq_nr: SeqNr) -> usize {
        // On RTO, we rewind back last_sent_seq_nr, however we don't reset the "sent" status
        // of packets. So instead of using "is_sent", just iterate up to last_sent_seq_nr, and pretend we
        // never sent them.
        let take = last_sent_seq_nr
            .signed_offset_from(self.snd_una)
            .map_or(0, |offset| (offset + 1).max(0) as usize);
        self.segments
            .iter()
            .take(take)
            .map(|s| if s.is_delivered { 0 } else { s.payload_size })
            .sum()
    }

    // Pipe is the estimate of how much outstanding data is in the network.
    // used during SACK-based recovery not to overwhelm the network.
    pub fn calc_pipe(
        &mut self,
        high_rxt: SeqNr,
        high_data: SeqNr,
        rtt: Duration,
        now: Instant,
    ) -> Pipe {
        let mut pipe = 0;
        let mut delivered_segs = 0;
        let mut recalc_timer = None;
        // The latest last sends among the delivered segments after the one
        // at hand, latest first: more than `SACK_DUP_THRESH` of them sent
        // after a resend say the resend was lost too.
        let mut latest = [None::<Instant>; SACK_DUP_THRESH as usize + 1];

        let take = high_data
            .signed_offset_from(self.snd_una)
            .map_or(0, |offset| offset.max(0) as usize);

        // After this we don't consider the packet being in the network.
        // rtt/2 might be a bit too aggressive.
        let expiry_threshold = calc_pipe_expiry(rtt);

        for (offset, seq_nr, segment, last_sent) in self
            .segments
            .range_mut(..take)
            .enumerate()
            .rev()
            .filter_map(|(offset, seg)| {
                seg.last_sent()
                    .map(|ts| (offset, self.snd_una + offset as u16, seg, ts))
            })
        {
            if segment.is_delivered {
                delivered_segs += 1;
                let mut carried = Some(last_sent);
                for slot in latest.iter_mut() {
                    if carried > *slot {
                        std::mem::swap(slot, &mut carried);
                    }
                }
                continue;
            }

            if seq_nr.is_at_or_before(high_rxt) {
                pipe += segment.payload_size;
            }

            segment.is_expired = now - last_sent >= expiry_threshold;

            // A segment is lost only when more than `SACK_DUP_THRESH`
            // segments after it were SACKed (libtorrent v2.0.15
            // utp_stream.cpp:1286-1320). Time in flight alone is no loss: a
            // late segment is reordering until the SACKs or the
            // retransmission timer say otherwise. Past the SACK's reach
            // nothing is known, so nothing is lost there.
            // A segment resent already is lost again only when more than
            // that many segments sent after its resend were SACKed: its own
            // evidence, not the evidence that resent it.
            let evidence = if segment.retransmit_count() > 0 {
                latest[SACK_DUP_THRESH as usize].is_some_and(|ts| ts > last_sent)
            } else {
                delivered_segs > SACK_DUP_THRESH as usize
            };
            segment.is_lost = offset <= self.sack_depth + 1 && evidence;

            // A segment in flight past the expiry has likely left the
            // network, so it no longer counts against the pipe.
            if !segment.is_lost && !segment.is_expired {
                pipe += segment.payload_size;
            }

            if !segment.is_expired && !segment.is_lost {
                let expires = last_sent + expiry_threshold;
                recalc_timer = match recalc_timer {
                    Some(ts) if expires < ts => Some(expires),
                    _ => Some(expires),
                }
            }
        }

        trace!(?pipe, ?recalc_timer, ?high_rxt, high_ack=?self.snd_una, ?high_data, "calc_pipe");
        Pipe { pipe, recalc_timer }
    }

    // Iterate stored data - headers and their payload offsets (as a function to copy payload to some other buffer).
    pub fn iter_mut_for_sending(
        &mut self,
        start: Option<SeqNr>,
    ) -> impl Iterator<Item = SegmentForSending<'_>> {
        let offset = match start {
            Some(start) => start
                .signed_offset_from(self.snd_una)
                .map_or(self.segments.len(), |offset| offset.max(0) as usize),
            None => 0,
        };
        let removed_abs = self.removed_offset;
        let snd_una = self.snd_una;
        let range = if offset >= self.segments.len() {
            self.segments.len()..
        } else {
            offset..
        };
        self.segments
            .range_mut(range)
            .enumerate()
            .map(move |(idx, seg)| SegmentForSending {
                seq_nr: snd_una + (offset + idx) as u16,
                payload_offset: seg
                    .payload_offset_absolute
                    .checked_sub(removed_abs)
                    .unwrap() as usize,
                segment: seg,
            })
            .filter(|s| !s.is_delivered())
    }

    // The oldest segment not delivered, as the retransmission timer finds
    // it, and how many segments sent after it were.
    #[cfg(test)]
    pub fn head_for_tests(&self) -> Option<HeadForTests> {
        let mut segments = self.segments.iter().skip_while(|s| s.is_delivered);
        let head = segments.next()?;
        let head_sent = head.last_sent();
        let mut delivered_after = 0;
        let mut delivered_sent_later = 0;
        for s in segments.filter(|s| s.is_delivered) {
            delivered_after += 1;
            if s.last_sent() > head_sent {
                delivered_sent_later += 1;
            }
        }
        Some(HeadForTests {
            sends: head.send_count(),
            probe_resends: head.probe_resends,
            payload_size: head.payload_size,
            is_lost: head.is_lost,
            delivered_after,
            delivered_sent_later,
        })
    }
}

#[cfg(test)]
pub struct HeadForTests {
    pub sends: usize,
    pub probe_resends: usize,
    pub payload_size: usize,
    pub is_lost: bool,
    pub delivered_after: usize,
    // Of those, the ones sent after the head's last send.
    pub delivered_sent_later: usize,
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::Instant;

    use crate::raw::{UtpHeader, selective_ack::SelectiveAck};
    use crate::stats::UtpStats;

    use super::{Segments, SeqNr};

    fn make_segments(start_seq_nr: u16, count: u16) -> Segments {
        let mut ftx = Segments::new(start_seq_nr.into());
        for _ in 0..count {
            assert!(ftx.enqueue(1, false));
        }

        ftx
    }

    fn make_sack_header(ack_nr: u16, acked: impl IntoIterator<Item = usize>) -> UtpHeader {
        UtpHeader {
            ack_nr: ack_nr.into(),
            extensions: crate::raw::Extensions {
                selective_ack: SelectiveAck::new_test(acked),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    #[test]
    fn reservation_pressure_counts_unsent_packets_and_delivered_holes() {
        use crate::constants::MAX_RESERVED_DATA_PACKETS;

        let first = SeqNr(65500);
        let mut segments = Segments::new(first);
        for _ in 0..MAX_RESERVED_DATA_PACKETS {
            assert!(segments.enqueue(1, false));
        }
        assert!(!segments.enqueue(1, false));
        let now = Instant::now();
        let stats = UtpStats::default();
        for mut item in segments.iter_mut_for_sending(None) {
            item.on_sent(now, &stats);
        }
        let ack = make_sack_header((first - 1).0, [0, 1]);
        let result = segments.remove_up_to_ack(now, &ack);
        assert_eq!(result.newly_sacked_segment_count, 2);
        assert_eq!(segments.total_len_packets(), MAX_RESERVED_DATA_PACKETS);
        assert!(!segments.enqueue(1, false));

        let result = segments.remove_up_to_ack(
            now,
            &UtpHeader {
                ack_nr: first,
                ..Default::default()
            },
        );
        assert_eq!(result.acked_segments_count, 3);
        for _ in 0..3 {
            assert!(segments.enqueue(1, false));
        }
        assert!(!segments.enqueue(1, false));
    }

    // A segment long in flight with fewer than four SACKed after it is late,
    // not lost: it leaves the pipe but is not resent.
    #[test]
    fn time_in_flight_alone_does_not_lose_a_segment() {
        let sent = Instant::now();
        let stats = UtpStats::default();
        let mut segments = make_segments(10, 8);
        for mut item in segments.iter_mut_for_sending(None) {
            item.on_sent(sent, &stats);
        }
        segments.remove_up_to_ack(sent, &make_sack_header(9, [0, 1, 2]));
        let rtt = Duration::from_millis(100);
        let pipe = segments.calc_pipe(9.into(), 17.into(), rtt, sent + rtt * 10);
        let head = segments.iter_mut_for_sending(None).next().unwrap();
        assert_eq!(head.seq_nr(), 10.into());
        assert!(head.is_expired());
        assert!(!head.is_lost());
        assert_eq!(pipe.pipe, 0);

        segments.remove_up_to_ack(sent, &make_sack_header(9, [0, 1, 2, 3]));
        segments.calc_pipe(9.into(), 17.into(), rtt, sent + rtt * 10);
        assert!(
            segments
                .iter_mut_for_sending(None)
                .next()
                .unwrap()
                .is_lost()
        );
    }

    // libtorrent sends a SACK as long as its receive buffer, often far past
    // 64 packets: every bit of it marks a packet delivered.
    #[test]
    fn a_sack_longer_than_ours_marks_every_packet_it_names() {
        let now = Instant::now();
        let stats = UtpStats::default();
        let mut segments = make_segments(1000, 400);
        for mut item in segments.iter_mut_for_sending(None) {
            item.on_sent(now, &stats);
        }
        // ack_nr 999: packet 1000 is missing; bit i names packet 1001 + i.
        let sacked = [0usize, 1, 2, 100, 150, 300];
        let mut sack = [0u8; 40];
        for i in sacked {
            sack[i / 8] |= 1 << (i % 8);
        }
        let mut wire = vec![0x21, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 7];
        wire.extend_from_slice(&999u16.to_be_bytes());
        wire.extend_from_slice(&[0, sack.len() as u8]);
        wire.extend_from_slice(&sack);
        let (header, _) = UtpHeader::deserialize(&wire).unwrap();

        let result = segments.remove_up_to_ack(now, &header);
        assert_eq!(result.newly_sacked_segment_count, sacked.len());
        assert_eq!(segments.count_delivered_test(), sacked.len());
        assert_eq!(segments.sack_depth(), 320);
        let delivered: Vec<u16> = segments
            .segments
            .iter()
            .enumerate()
            .filter(|(_, s)| s.is_delivered)
            .map(|(i, _)| 1000 + i as u16)
            .collect();
        assert_eq!(delivered, vec![1001, 1002, 1003, 1101, 1151, 1301]);
    }

    #[test]
    fn test_remove_up_to_ack() {
        // Test Case 1: Basic setup with 4 packets starting from sequence number 3
        let now = Instant::now();
        let make_segments = |start, count| {
            let mut segments = make_segments(start, count);
            for mut item in segments.iter_mut_for_sending(None) {
                item.on_sent(now, &UtpStats::default());
            }
            segments
        };
        let ftx = make_segments(3, 4);
        assert_eq!(ftx.total_len_bytes(), 4);
        assert_eq!(ftx.total_len_packets(), 4);
        assert_eq!(ftx.first_seq_nr().unwrap(), 3.into());
        assert_eq!(ftx.count_delivered_test(), 0);

        // Test Case 1.1: ACK with sequence number 4, should remove first two packets
        let mut ftx = make_segments(3, 4);
        ftx.remove_up_to_ack(now, &make_sack_header(4, []));
        assert_eq!(ftx.total_len_bytes(), 2);
        assert_eq!(ftx.total_len_packets(), 2);
        assert_eq!(ftx.first_seq_nr().unwrap(), 5.into());
        assert_eq!(ftx.count_delivered_test(), 0);

        // Test Case 1.2: ACK all packets, queue should be empty
        let mut ftx = make_segments(3, 4);
        ftx.remove_up_to_ack(now, &make_sack_header(6, []));
        assert_eq!(ftx.total_len_bytes(), 0);
        assert_eq!(ftx.total_len_packets(), 0);
        assert!(ftx.first_seq_nr().is_none());
        assert_eq!(ftx.count_delivered_test(), 0);

        // Test Case 2: ACK with sequence number 2 (below our start sequence)
        // Should not remove any packets or mark any as delivered
        let mut ftx = make_segments(3, 4);
        ftx.remove_up_to_ack(now, &make_sack_header(2, []));
        assert_eq!(ftx.total_len_bytes(), 4);
        assert_eq!(ftx.total_len_packets(), 4);
        assert_eq!(ftx.first_seq_nr().unwrap(), 3.into());
        assert_eq!(ftx.count_delivered_test(), 0);

        // Test Case 3: ACK with sequence number 3 (matches our start sequence)
        // Should remove the first packet, leaving 3 packets
        let mut ftx = make_segments(3, 4);
        ftx.remove_up_to_ack(now, &make_sack_header(3, []));
        assert_eq!(ftx.total_len_bytes(), 3);
        assert_eq!(ftx.total_len_packets(), 3);
        assert_eq!(ftx.first_seq_nr().unwrap(), 4.into());
        assert_eq!(ftx.count_delivered_test(), 0);

        // Test Case 4: ACK with sequence number 2 and selective ACK for next packet
        // Should mark the second packet as delivered but not remove any
        let mut ftx = make_segments(3, 4);
        ftx.remove_up_to_ack(now, &make_sack_header(2, [0]));
        assert_eq!(ftx.total_len_bytes(), 4);
        assert_eq!(ftx.total_len_packets(), 4);
        assert_eq!(ftx.first_seq_nr().unwrap(), 3.into());
        assert_eq!(ftx.count_delivered_test(), 1);
        assert!(ftx.segments.get(1).unwrap().is_delivered);

        // Test Case 4.1 (edge case): an older selective ACK should mark packets delivered.
        let mut ftx = make_segments(3, 4);
        ftx.remove_up_to_ack(now, &make_sack_header(1, [1])); // means 2 and 3 are not delivered, but 4 is
        assert_eq!(ftx.total_len_bytes(), 4);
        assert_eq!(ftx.total_len_packets(), 4);
        assert_eq!(ftx.first_seq_nr().unwrap(), 3.into());
        assert_eq!(ftx.count_delivered_test(), 1);
        assert!(ftx.segments.get(1).unwrap().is_delivered);

        // Test Case 5: ACK with sequence number 2 and selective ACK for second and fourth packets
        // Should mark both packets as delivered but not remove any
        let mut ftx = make_segments(3, 4);
        ftx.remove_up_to_ack(now, &make_sack_header(2, [0, 2]));
        assert_eq!(ftx.total_len_bytes(), 4);
        assert_eq!(ftx.total_len_packets(), 4);
        assert_eq!(ftx.first_seq_nr().unwrap(), 3.into());
        assert_eq!(ftx.count_delivered_test(), 2);
        assert!(ftx.segments.get(1).unwrap().is_delivered);
        assert!(ftx.segments.get(3).unwrap().is_delivered);

        // Test Case 6: Selective ACK with gaps
        let mut ftx = make_segments(3, 6); // Create 6 packets
        ftx.remove_up_to_ack(now, &make_sack_header(2, [0, 2, 4])); // ACK 2nd, 4th, and 6th packets
        assert_eq!(ftx.total_len_bytes(), 6);
        assert_eq!(ftx.total_len_packets(), 6);
        assert_eq!(ftx.count_delivered_test(), 3);
        assert!(ftx.segments.get(1).unwrap().is_delivered);
        assert!(ftx.segments.get(3).unwrap().is_delivered);
        assert!(ftx.segments.get(5).unwrap().is_delivered);
        assert!(!ftx.segments.front().unwrap().is_delivered);
        assert!(!ftx.segments.get(2).unwrap().is_delivered);
        assert!(!ftx.segments.get(4).unwrap().is_delivered);

        // Test Case 7: selective ACK should be able to clean up the queue
        let mut ftx = make_segments(3, 4);
        ftx.remove_up_to_ack(now, &make_sack_header(1, [0, 1, 2])); // means 3,4,5 were received
        assert_eq!(ftx.total_len_bytes(), 1);
        assert_eq!(ftx.total_len_packets(), 1);
        assert_eq!(ftx.count_delivered_test(), 0);
        assert_eq!(ftx.first_seq_nr().unwrap(), 6.into());

        // Test Case 7.1: selective ACK should be able to clean up the queue - even further away
        let mut ftx = make_segments(3, 4);
        ftx.remove_up_to_ack(now, &make_sack_header(0, [0, 1, 2, 3])); // means 3,4,5 were received
        assert_eq!(ftx.total_len_bytes(), 1);
        assert_eq!(ftx.total_len_packets(), 1);
        assert_eq!(ftx.count_delivered_test(), 0);
        assert_eq!(ftx.first_seq_nr().unwrap(), 6.into());
    }

    #[test]
    fn test_rtt() {
        // Test Case 1: Basic setup with 4 packets starting from sequence number 3
        let mut now = Instant::now();
        let mut ftx = make_segments(3, 5);
        for mut item in ftx.iter_mut_for_sending(None) {
            item.on_sent(now, &UtpStats::default());
        }
        now += Duration::from_secs(1);
        let res = ftx.remove_up_to_ack(now, &make_sack_header(3, []));
        assert_eq!(res.new_rtt, Some(Duration::from_secs(1)));

        // One second later an ACK arrives for later segment (delayed ACK)
        now += Duration::from_secs(1);
        let res = ftx.remove_up_to_ack(now, &make_sack_header(5, []));
        // This inflates rtt
        assert_eq!(res.new_rtt, Some(Duration::from_secs(2)));
        assert_eq!(ftx.first_seq_nr(), Some(6.into()));

        // We retransmit the rest
        for mut item in ftx.iter_mut_for_sending(None) {
            item.on_sent(now, &UtpStats::default());
        }

        // they are ACKed, but RTT should not be updated for retransmitted packets.
        now += Duration::from_secs(1);
        let res = ftx.remove_up_to_ack(now, &make_sack_header(7, []));
        assert_eq!(ftx.first_seq_nr(), None);
        assert_eq!(res.acked_segments_count, 2);
        // This inflates rtt
        assert_eq!(res.new_rtt, None);
    }

    // An ACK whose cumulative part or SACK reports a resent packet can
    // report packets the receiver got long before it: it gives no sample.
    #[test]
    fn an_ack_that_reports_a_resent_packet_times_nothing() {
        let stats = UtpStats::default();
        let sent = Instant::now();
        let mut ftx = make_segments(10, 8);
        for mut item in ftx.iter_mut_for_sending(None) {
            item.on_sent(sent, &stats);
        }
        let resent = sent + Duration::from_secs(5);
        ftx.iter_mut_for_sending(None)
            .next()
            .unwrap()
            .on_sent(resent, &stats);

        let now = resent + Duration::from_millis(10);
        let res = ftx.remove_up_to_ack(now, &make_sack_header(11, []));
        assert_eq!(res.acked_segments_count, 2);
        assert_eq!(res.new_rtt, None);

        // Packet 13 resent; a SACK reports it with packet 15.
        ftx.iter_mut_for_sending(None)
            .nth(1)
            .unwrap()
            .on_sent(resent, &stats);
        let res = ftx.remove_up_to_ack(now, &make_sack_header(11, [0, 2]));
        assert_eq!(res.newly_sacked_segment_count, 2);
        assert_eq!(res.new_rtt, None);

        // Packets sent once and not reported before still give a sample.
        let res = ftx.remove_up_to_ack(now, &make_sack_header(11, [4]));
        assert_eq!(res.newly_sacked_segment_count, 1);
        assert_eq!(res.new_rtt, Some(now - sent));
    }

    // A resent packet is lost again only when more than three packets sent
    // after its resend are SACKed; the SACKs that resent it do not count.
    #[test]
    fn a_resend_is_lost_again_only_on_its_own_evidence() {
        let stats = UtpStats::default();
        let rtt = Duration::from_millis(1);
        let t0 = Instant::now();
        let mut ftx = make_segments(10, 12);
        for mut item in ftx.iter_mut_for_sending(None).take(6) {
            item.on_sent(t0, &stats);
        }
        // 11-14 SACKed: 10 is lost and resent.
        ftx.remove_up_to_ack(t0, &make_sack_header(9, [0, 1, 2, 3]));
        ftx.calc_pipe(9.into(), 15.into(), rtt, t0);
        let t1 = t0 + rtt;
        {
            let mut head = ftx.iter_mut_for_sending(None).next().unwrap();
            assert!(head.is_lost());
            head.on_sent(t1, &stats);
            head.on_resent_as_lost();
        }
        // The same evidence again, plus 15, sent before the resend.
        ftx.remove_up_to_ack(t1, &make_sack_header(9, [0, 1, 2, 3, 4]));
        ftx.calc_pipe(10.into(), 15.into(), rtt, t1);
        assert!(!ftx.iter_mut_for_sending(None).next().unwrap().is_lost());

        // 16-21 go out after the resend; three SACKed are not enough, four
        // are.
        let t2 = t1 + rtt;
        for mut item in ftx.iter_mut_for_sending(Some(16.into())) {
            item.on_sent(t2, &stats);
        }
        ftx.remove_up_to_ack(t2, &make_sack_header(9, [0, 1, 2, 3, 4, 5, 6, 7]));
        ftx.calc_pipe(10.into(), 21.into(), rtt, t2);
        assert!(!ftx.iter_mut_for_sending(None).next().unwrap().is_lost());
        ftx.remove_up_to_ack(t2, &make_sack_header(9, [8]));
        ftx.calc_pipe(10.into(), 21.into(), rtt, t2);
        let head = ftx.iter_mut_for_sending(None).next().unwrap();
        assert_eq!(head.seq_nr(), 10.into());
        assert!(head.is_lost());
    }

    // Toward giving up count the first send, the first resend and the
    // timer's; a resend of a resend that SACKs showed lost does not.
    #[test]
    fn resends_on_new_evidence_do_not_count_toward_giving_up() {
        let stats = UtpStats::default();
        let now = Instant::now();
        let mut ftx = make_segments(10, 1);
        let mut head = ftx.iter_mut_for_sending(None).next().unwrap();
        head.on_sent(now, &stats);
        head.on_sent(now, &stats);
        head.on_resent_as_lost();
        assert_eq!((head.send_count(), head.counted_sends()), (2, 2));
        head.on_sent(now, &stats);
        head.on_resent_as_lost();
        head.on_sent(now, &stats);
        head.on_resent_as_lost();
        assert_eq!((head.send_count(), head.counted_sends()), (4, 2));
        // The timer's resend counts.
        head.on_sent(now, &stats);
        assert_eq!((head.send_count(), head.counted_sends()), (5, 3));
    }

    #[test]
    fn test_iter_mut_offsets() {
        let mut segs = Segments::new(0.into());
        assert!(segs.enqueue(1, false));
        assert!(segs.enqueue(2, false));
        assert!(segs.enqueue(3, false));

        assert_eq!(
            segs.iter_mut_for_sending(None)
                .map(|s| (s.seq_nr().0, s.payload_offset(), s.payload_size()))
                .collect::<Vec<_>>(),
            vec![(0, 0, 1), (1, 1, 2), (2, 3, 3)]
        );

        assert_eq!(
            segs.remove_up_to_ack(
                Instant::now(),
                &UtpHeader {
                    ack_nr: 0.into(),
                    ..Default::default()
                },
            )
            .acked_bytes,
            1
        );

        assert_eq!(
            segs.iter_mut_for_sending(None)
                .map(|s| (s.seq_nr().0, s.payload_offset(), s.payload_size()))
                .collect::<Vec<_>>(),
            vec![(1, 0, 2), (2, 2, 3)]
        );

        assert!(segs.enqueue(4, false));
        assert_eq!(
            segs.iter_mut_for_sending(None)
                .map(|s| (s.seq_nr().0, s.payload_offset(), s.payload_size()))
                .collect::<Vec<_>>(),
            vec![(1, 0, 2), (2, 2, 3), (3, 5, 4)]
        );
        assert_eq!(
            segs.remove_up_to_ack(
                Instant::now(),
                &UtpHeader {
                    ack_nr: 2.into(),
                    ..Default::default()
                },
            )
            .acked_bytes,
            5
        );

        assert_eq!(
            segs.iter_mut_for_sending(None)
                .map(|s| (s.seq_nr().0, s.payload_offset(), s.payload_size()))
                .collect::<Vec<_>>(),
            vec![(3, 0, 4)]
        );

        assert_eq!(
            segs.iter_mut_for_sending(Some(3.into()))
                .map(|s| (s.seq_nr().0, s.payload_offset(), s.payload_size()))
                .collect::<Vec<_>>(),
            vec![(3, 0, 4)]
        );

        assert_eq!(
            segs.iter_mut_for_sending(Some(4.into()))
                .map(|s| (s.seq_nr().0, s.payload_offset(), s.payload_size()))
                .collect::<Vec<_>>(),
            vec![]
        );

        // From the past
        assert_eq!(
            segs.iter_mut_for_sending(Some(0.into()))
                .map(|s| (s.seq_nr().0, s.payload_offset(), s.payload_size()))
                .collect::<Vec<_>>(),
            vec![(3, 0, 4)]
        );

        assert_eq!(
            segs.iter_mut_for_sending(Some(1000.into()))
                .map(|s| (s.seq_nr().0, s.payload_offset(), s.payload_size()))
                .collect::<Vec<_>>(),
            vec![]
        );
    }
}
