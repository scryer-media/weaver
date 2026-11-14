// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
// Lifted from librqbit-utp 0.7.0 (Apache-2.0), Copyright (c) the librqbit authors.
// Modified by the Weaver authors, 2026-11-14.

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
// it stops being a probe and goes again whole under its own sequence number, as
// ordinary data: the peer may hold it. One the host refused before it was ever sent
// is removed and cut again smaller.
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
    // A probe given up on as too large, resent whole as ordinary data. Its
    // delivery proves no size: the ceiling is already below it.
    size_given_up: bool,
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

    // The flight size kept as it changes, so a send pass or an ACK reads it
    // without walking the segments: `flight_end` segments from the front
    // are counted, and `flight_bytes` is the payload of those among them
    // not delivered. `calc_flight_size` moves the end to the watermark it
    // is given, walking only the segments it crosses.
    flight_end: usize,
    flight_bytes: usize,

    // The payload of segments a SACK reported delivered and still queued.
    delivered_bytes: usize,

    // The sequence number of the last MTU probe cut. It is the live probe
    // while that segment is queued, still a probe and not delivered; data
    // cut after it may sit behind it, as libtorrent sends floor packets
    // behind its probe (utp_stream.cpp v2.0.15:1528-1532).
    probe_seq_nr: Option<SeqNr>,
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

    // A probe given up on as too large, resent as ordinary data.
    pub fn size_given_up(&self) -> bool {
        self.segment.size_given_up
    }

    pub fn retransmit_count(&self) -> usize {
        self.segment.retransmit_count()
    }

    // Gives up on this probe as too large: it goes again whole as ordinary
    // data, and its delivery proves no size. Returns its payload size.
    pub fn give_up_probe(&mut self) -> usize {
        self.segment.is_mtu_probe = false;
        self.segment.size_given_up = true;
        self.segment.payload_size
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
        // A loss mark is about the send before this one: this send waits
        // for evidence of its own.
        self.segment.is_lost = false;
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

    // The largest payload acknowledged, a probe given up on left out.
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
    // When a resend that a later send's delivery already outran has been
    // out long enough to count lost, and the pipe must be counted again.
    pub reorder_at: Option<Instant>,
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
pub enum ExpiredProbe {
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
            flight_end: 0,
            flight_bytes: 0,
            delivered_bytes: 0,
            probe_seq_nr: None,
        }
    }

    // Takes the segment at the back off the counted prefix and the
    // delivered total, before it is popped.
    fn uncount_back(&mut self) {
        let Some(segment) = self.segments.back() else {
            return;
        };
        if self.flight_end == self.segments.len() {
            self.flight_end -= 1;
            if !segment.is_delivered {
                self.flight_bytes -= segment.payload_size;
            }
        }
        if segment.is_delivered {
            self.delivered_bytes -= segment.payload_size;
        }
    }

    // The flight size counted over the first `take` segments, by walking
    // them.
    fn walk_flight_size(&self, take: usize) -> usize {
        self.segments
            .iter()
            .take(take)
            .map(|s| if s.is_delivered { 0 } else { s.payload_size })
            .sum()
    }

    // The running totals match a walk of the segments.
    #[cfg(test)]
    fn check_running_totals(&self) {
        assert!(self.flight_end <= self.segments.len());
        assert_eq!(self.flight_bytes, self.walk_flight_size(self.flight_end));
        let delivered: usize = self
            .segments
            .iter()
            .filter(|s| s.is_delivered)
            .map(|s| s.payload_size)
            .sum();
        assert_eq!(self.delivered_bytes, delivered);
    }

    #[cfg(not(test))]
    fn check_running_totals(&self) {}

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

    /// The send numbered `sends` of `seq_nr` left the socket at `at`, after
    /// it was queued for the socket's I/O task: that send is timed from
    /// then. Nothing changes when the segment is acknowledged already or
    /// was sent again since.
    pub fn set_departure(&mut self, seq_nr: SeqNr, sends: usize, at: Instant) {
        let Some(segment) = seq_nr
            .signed_offset_from(self.snd_una)
            .and_then(|offset| usize::try_from(offset).ok())
            .and_then(|offset| self.segments.get_mut(offset))
        else {
            return;
        };
        if segment.send_count() != sends {
            return;
        }
        match &mut segment.sent {
            SentStatus::NotSent => {}
            SentStatus::SentTime(sent) => *sent = at,
            SentStatus::Retransmitted { last_send_ts, .. } => *last_send_ts = at,
        }
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
            self.uncount_back();
            self.segments.pop_back();
        }
        self.offset -= dropped as u64;
        self.len_bytes -= dropped;
        self.check_running_totals();
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

    // The bytes of segments not yet delivered, sent or not: those a SACK
    // reported delivered are out of the network.
    pub fn undelivered_bytes(&self) -> usize {
        self.len_bytes - self.delivered_bytes
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
        if is_mtu_probe {
            self.probe_seq_nr = Some(self.next_seq_nr());
        }
        self.segments.push_back(Segment {
            payload_size: payload_len,
            payload_offset_absolute: self.offset,
            is_delivered: false,
            is_mtu_probe,
            size_given_up: false,
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

    // The index of the live MTU probe: queued, a probe and not delivered.
    fn live_probe_index(&self) -> Option<usize> {
        let index = usize::from(self.probe_seq_nr?.forward_distance_from(self.snd_una));
        self.segments
            .get(index)
            .is_some_and(|s| s.is_mtu_probe && !s.is_delivered)
            .then_some(index)
    }

    // Whether an MTU probe is out and unanswered: no other is cut meanwhile.
    pub fn has_live_mtu_probe(&self) -> bool {
        self.live_probe_index().is_some()
    }

    // Drops the live probe at `seq_nr` the host refused, and the segments
    // cut after it, when none of those was ever sent: they are cut again
    // at the lowered size.
    pub fn pop_mtu_probe(&mut self, seq_nr: SeqNr) -> bool {
        let Some(index) = self.live_probe_index() else {
            return false;
        };
        if self.snd_una + index as u16 != seq_nr
            || self
                .segments
                .iter()
                .skip(index + 1)
                .any(|s| s.send_count() > 0)
        {
            return false;
        }
        while self.segments.len() > index {
            self.uncount_back();
            let s = self.segments.pop_back().unwrap();
            self.len_bytes -= s.payload_size;
            self.offset = s.payload_offset_absolute;
        }
        self.check_running_totals();
        true
    }

    /// Gives up on the live MTU probe once its timer has expired often
    /// enough. A probe that was sent keeps its bytes and its sequence
    /// number: the peer may hold it, and drops by sequence number alone
    /// anything sent again under it. It stays as ordinary data, to be resent
    /// whole, as libtorrent resends a timed-out probe (utp_stream.cpp
    /// v2.0.15:1975-1984, 3412-3421).
    pub fn expire_mtu_probe(
        &mut self,
        retransmit_timed_out: bool,
        max_probe_retransmissions: usize,
    ) -> ExpiredProbe {
        let Some(index) = self.live_probe_index() else {
            return ExpiredProbe::Empty;
        };
        let seq_nr = self.snd_una + index as u16;
        let s = &mut self.segments[index];
        if retransmit_timed_out && s.retransmit_count() >= max_probe_retransmissions {
            s.is_mtu_probe = false;
            s.size_given_up = true;
            ExpiredProbe::Expired {
                payload_size: s.payload_size,
                rewind_to: seq_nr - 1,
            }
        } else {
            ExpiredProbe::NotExpired
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
                if self.flight_end > 0 {
                    self.flight_end -= 1;
                    if !segment.is_delivered {
                        self.flight_bytes -= segment.payload_size;
                    }
                }
                if segment.is_delivered {
                    self.delivered_bytes -= segment.payload_size;
                }
                if !segment.is_delivered {
                    covers_resend |= segment.retransmit_count() > 0;
                    segment.update_rtt(now, &mut new_rtt);
                }
                removed += 1;
                payload_size += segment.payload_size;
                self.snd_una += 1;
                self.len_bytes -= segment.payload_size;
                if !segment.size_given_up {
                    max_acked_payload_size = max_acked_payload_size.max(segment.payload_size);
                }
            }
        }

        // If TX start matches with header ACK and it's a selective ACK, mark all segments delivered.
        match (self.first_seq_nr(), ack_header.extensions.selective_ack) {
            (Some(first_seq_nr), Some(sack)) if first_seq_nr.is_after(ack_header.ack_nr) => {
                self.sack_depth = sack.len();

                let sack_start_offset =
                    2 - first_seq_nr.forward_distance_from(ack_header.ack_nr) as isize;

                // Returns the payload of a segment this SACK newly delivered.
                let mut process_sack = |segment: &mut Segment, is_sacked: bool| -> usize {
                    if !segment.is_delivered && is_sacked && segment.send_count() > 0 {
                        segment.is_delivered = true;
                        covers_resend |= segment.retransmit_count() > 0;
                        segment.update_rtt(now, &mut new_rtt);
                        if !segment.size_given_up {
                            max_acked_payload_size =
                                max_acked_payload_size.max(segment.payload_size);
                        }
                        newly_sacked_segment_count += 1;
                        newly_sacked_byte_count += segment.payload_size;
                        segment.payload_size
                    } else {
                        0
                    }
                };

                let mut on_delivered = |index: usize, delivered: usize| {
                    self.delivered_bytes += delivered;
                    if index < self.flight_end {
                        self.flight_bytes -= delivered;
                    }
                };
                if sack_start_offset >= 0 {
                    let skip = sack_start_offset as usize;
                    for (index, (segment, acked)) in self
                        .segments
                        .iter_mut()
                        .skip(skip)
                        .zip(sack.iter())
                        .enumerate()
                    {
                        on_delivered(skip + index, process_sack(segment, acked));
                    }
                } else {
                    for (index, (segment, acked)) in self
                        .segments
                        .iter_mut()
                        .zip(sack.iter().skip((-sack_start_offset) as usize))
                        .enumerate()
                    {
                        on_delivered(index, process_sack(segment, acked));
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
            self.delivered_bytes -= segment.payload_size;
            // Delivered, so it adds nothing to the flight bytes.
            self.flight_end = self.flight_end.saturating_sub(1);
            self.snd_una += 1;
            self.segments.pop_front().unwrap();
        }
        self.check_running_totals();

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

    fn flight_take(&self, last_sent_seq_nr: SeqNr) -> usize {
        // On RTO, we rewind back last_sent_seq_nr, however we don't reset the "sent" status
        // of packets. So instead of using "is_sent", just count up to last_sent_seq_nr, and pretend we
        // never sent them.
        last_sent_seq_nr
            .signed_offset_from(self.snd_una)
            .map_or(0, |offset| (offset + 1).max(0) as usize)
            .min(self.segments.len())
    }

    // The total payload of sent and unacked segments. The counted prefix
    // moves to the watermark: forward over segments sent since the last
    // call, back only when the watermark was rewound.
    pub fn calc_flight_size(&mut self, last_sent_seq_nr: SeqNr) -> usize {
        let take = self.flight_take(last_sent_seq_nr);
        while self.flight_end < take {
            let segment = &self.segments[self.flight_end];
            if !segment.is_delivered {
                self.flight_bytes += segment.payload_size;
            }
            self.flight_end += 1;
        }
        while self.flight_end > take {
            self.flight_end -= 1;
            let segment = &self.segments[self.flight_end];
            if !segment.is_delivered {
                self.flight_bytes -= segment.payload_size;
            }
        }
        self.check_running_totals();
        self.flight_bytes
    }

    // The flight size up to a watermark other than the one sends follow,
    // walked without moving the counted prefix.
    pub fn walk_flight_size_to(&self, last_sent_seq_nr: SeqNr) -> usize {
        self.walk_flight_size(self.flight_take(last_sent_seq_nr))
    }

    // Pipe is the estimate of how much outstanding data is in the network.
    // used during SACK-based recovery not to overwhelm the network.
    // `high_sent` is the newest packet ever sent. After a timer expiry
    // rewound `high_data` to the head, the packets past it are marked lost
    // or not on the SACKs as well, so a hole among them can be resent on
    // that evidence as libtorrent resends it (v2.0.15 utp_stream.cpp:
    // 1235-1355, 3550-3551); they are not in the pipe, as libtorrent's
    // timeout takes them out of its bytes in flight (:3505-3513).
    pub fn calc_pipe(
        &mut self,
        high_rxt: SeqNr,
        high_data: SeqNr,
        high_sent: SeqNr,
        rtt: Duration,
        now: Instant,
    ) -> Pipe {
        let mut pipe = 0;
        let mut delivered_segs = 0;
        let mut recalc_timer = None;
        let mut reorder_at: Option<Instant> = None;
        // A resend outrun by a later send's delivery is lost once it has
        // been out a round trip and a quarter (RFC 8985 section 6.2).
        let reorder_wait = rtt + rtt / 4;
        // The latest last sends among the delivered segments after the one
        // at hand, latest first: more than `SACK_DUP_THRESH` of them sent
        // after a resend say the resend was lost too.
        let mut latest = [None::<Instant>; SACK_DUP_THRESH as usize + 1];

        // Every segment sent, the newest at `high_data` with them: the
        // flight size counts the same segments (RFC 6675 section 4,
        // SetPipe, runs from HighACK to HighData).
        let take = self.flight_take(high_data);
        let marked = self.flight_take(high_sent).max(take);

        // After this we don't consider the packet being in the network.
        // rtt/2 might be a bit too aggressive.
        let expiry_threshold = calc_pipe_expiry(rtt);

        for (offset, seq_nr, segment, last_sent) in self
            .segments
            .range_mut(..marked)
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

            let counted = offset < take;
            if counted && seq_nr.is_at_or_before(high_rxt) {
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
            // A resend is lost too when anything sent after it arrived and
            // it has been out a round trip and the reordering allowance
            // since; until then, the pipe is counted again when it has.
            let evidence = if segment.retransmit_count() > 0 {
                let outrun = latest[0].is_some_and(|ts| ts > last_sent);
                let due = last_sent + reorder_wait;
                if outrun && now < due {
                    reorder_at = Some(reorder_at.map_or(due, |at| at.min(due)));
                }
                latest[SACK_DUP_THRESH as usize].is_some_and(|ts| ts > last_sent)
                    || (outrun && now >= due)
            } else {
                delivered_segs > SACK_DUP_THRESH as usize
            };
            segment.is_lost = offset <= self.sack_depth + 1 && evidence;
            if !counted {
                continue;
            }

            // A segment in flight past the expiry has likely left the
            // network, so it no longer counts against the pipe.
            if !segment.is_lost && !segment.is_expired {
                pipe += segment.payload_size;
            }

            if !segment.is_expired && !segment.is_lost {
                let expires = last_sent + expiry_threshold;
                // The earliest expiry among the segments still counted.
                recalc_timer = Some(recalc_timer.map_or(expires, |ts: Instant| ts.min(expires)));
            }
        }

        trace!(?pipe, ?recalc_timer, ?high_rxt, high_ack=?self.snd_una, ?high_data, "calc_pipe");
        Pipe {
            pipe,
            recalc_timer,
            reorder_at,
        }
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
            evidence_resends: head.evidence_resends,
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
    pub evidence_resends: usize,
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
        let pipe = segments.calc_pipe(9.into(), 17.into(), 17.into(), rtt, sent + rtt * 10);
        let head = segments.iter_mut_for_sending(None).next().unwrap();
        assert_eq!(head.seq_nr(), 10.into());
        assert!(head.is_expired());
        assert!(!head.is_lost());
        assert_eq!(pipe.pipe, 0);

        segments.remove_up_to_ack(sent, &make_sack_header(9, [0, 1, 2, 3]));
        segments.calc_pipe(9.into(), 17.into(), 17.into(), rtt, sent + rtt * 10);
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
        ftx.calc_pipe(9.into(), 15.into(), 15.into(), rtt, t0);
        let t1 = t0 + rtt;
        {
            let mut head = ftx.iter_mut_for_sending(None).next().unwrap();
            assert!(head.is_lost());
            head.on_sent(t1, &stats);
            head.on_resent_as_lost();
        }
        // The same evidence again, plus 15, sent before the resend.
        ftx.remove_up_to_ack(t1, &make_sack_header(9, [0, 1, 2, 3, 4]));
        ftx.calc_pipe(10.into(), 15.into(), 15.into(), rtt, t1);
        assert!(!ftx.iter_mut_for_sending(None).next().unwrap().is_lost());

        // 16-21 go out after the resend; three SACKed are not enough, four
        // are.
        let t2 = t1 + rtt;
        for mut item in ftx.iter_mut_for_sending(Some(16.into())) {
            item.on_sent(t2, &stats);
        }
        ftx.remove_up_to_ack(t2, &make_sack_header(9, [0, 1, 2, 3, 4, 5, 6, 7]));
        ftx.calc_pipe(10.into(), 21.into(), 21.into(), rtt, t2);
        assert!(!ftx.iter_mut_for_sending(None).next().unwrap().is_lost());
        ftx.remove_up_to_ack(t2, &make_sack_header(9, [8]));
        ftx.calc_pipe(10.into(), 21.into(), 21.into(), rtt, t2);
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

    // The flight size and the delivered total, kept as segments are sent,
    // acknowledged, SACKed, rewound over by the timer, dropped unsent and
    // popped as probes, equal a walk of the segments after every change.
    // The sequence numbers wrap during the run.
    #[test]
    fn running_flight_size_equals_the_walk_through_loss_sacks_and_timeouts() {
        // splitmix64, seeded: the same run every time.
        let mut state: u64 = 0x5eed_f11e;
        let mut next = move |bound: u64| {
            state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
            let mut z = state;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            (z ^ (z >> 31)) % bound
        };
        let stats = UtpStats::default();
        let now = Instant::now();
        let mut segs = Segments::new(65_000.into());
        let mut last_sent = segs.acked_seq_nr();
        let (mut sacked, mut rewinds, mut dropped, mut probes_popped, mut acked) = (0, 0, 0, 0, 0);

        for _ in 0..50_000 {
            match next(100) {
                0..30 => {
                    let probe = next(20) == 0;
                    let _ = segs.enqueue(1 + next(1400) as usize, probe);
                }
                30..60 => {
                    if let Some(mut seg) = segs.iter_mut_for_sending(Some(last_sent + 1)).next() {
                        seg.on_sent(now, &stats);
                        last_sent = seg.seq_nr();
                    }
                }
                60..72 => {
                    let ahead = last_sent.forward_distance_from(segs.acked_seq_nr());
                    if ahead > 0 && ahead < 0x8000 {
                        let ack = segs.acked_seq_nr() + next(u64::from(ahead) + 1) as u16;
                        acked += segs
                            .remove_up_to_ack(now, &make_sack_header(ack.0, []))
                            .acked_segments_count;
                    }
                }
                72..87 => {
                    let ack = segs.acked_seq_nr();
                    let bits: Vec<usize> = (0..64).filter(|_| next(3) == 0).collect();
                    sacked += segs
                        .remove_up_to_ack(now, &make_sack_header(ack.0, bits))
                        .newly_sacked_segment_count;
                }
                87..92 => {
                    // The timer resends from the oldest: the watermark goes
                    // back to it.
                    if let Some(seg) = segs.iter_mut_for_sending(None).next()
                        && seg.send_count() > 0
                    {
                        last_sent = seg.seq_nr();
                        rewinds += 1;
                    }
                }
                92..95 => dropped += segs.drop_unsent(),
                95..98 => {
                    if segs.pop_mtu_probe(segs.next_seq_nr() - 1) {
                        probes_popped += 1;
                        if last_sent.is_at_or_after(segs.next_seq_nr()) {
                            last_sent = segs.next_seq_nr() - 1;
                        }
                    }
                }
                _ => {
                    let _ = segs.expire_mtu_probe(true, 0);
                }
            }
            if next(2) == 0 {
                assert_eq!(
                    segs.calc_flight_size(last_sent),
                    segs.walk_flight_size_to(last_sent)
                );
            }
        }
        assert!(
            sacked > 0 && rewinds > 0 && dropped > 0 && probes_popped > 0 && acked > 0,
            "sacked={sacked} rewinds={rewinds} dropped={dropped} probes_popped={probes_popped} acked={acked}"
        );
        // More removed from the front than lie before the wrap.
        assert!(acked > usize::from(u16::MAX - 65_000), "acked={acked}");
    }
}

#[cfg(test)]
mod pipe_tests {
    use std::time::Duration;

    use tokio::time::Instant;

    use super::{Segments, SeqNr};
    use crate::raw::{UtpHeader, selective_ack::SelectiveAck};
    use crate::stats::UtpStats;

    const SIZE: usize = 1000;
    const SRTT: Duration = Duration::from_millis(100);
    const MS: Duration = Duration::from_millis(1);

    fn sack(ack_nr: u16, seqs: impl IntoIterator<Item = u16>) -> UtpHeader {
        UtpHeader {
            ack_nr: ack_nr.into(),
            extensions: crate::raw::Extensions {
                selective_ack: SelectiveAck::new_test(
                    seqs.into_iter().map(|s| usize::from(s - ack_nr - 2)),
                ),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn queue(first: u16, last: u16) -> Segments {
        let mut segs = Segments::new(first.into());
        for _ in first..=last {
            assert!(segs.enqueue(SIZE, false));
        }
        segs
    }

    fn send(segs: &mut Segments, seqs: std::ops::RangeInclusive<u16>, at: Instant) {
        let stats = UtpStats::default();
        for mut item in segs.iter_mut_for_sending(None) {
            if seqs.contains(&item.seq_nr().0) {
                item.on_sent(at, &stats);
            }
        }
    }

    // As the recovery send path does: send again, then clear the loss mark.
    fn resend_as_lost(segs: &mut Segments, seq: u16, at: Instant) {
        let stats = UtpStats::default();
        let mut item = segs
            .iter_mut_for_sending(None)
            .find(|s| s.seq_nr() == SeqNr(seq))
            .unwrap();
        item.on_sent(at, &stats);
        item.on_resent_as_lost();
    }

    // Three packets sent and none acknowledged: all three are in the pipe,
    // the newest with them, as all three are in the flight.
    #[test]
    fn the_pipe_counts_the_newest_packet_sent() {
        let t0 = Instant::now();
        let mut segs = queue(10, 12);
        send(&mut segs, 10..=12, t0);
        assert_eq!(segs.walk_flight_size_to(12.into()), 3 * SIZE);
        let pipe = segs.calc_pipe(9.into(), 12.into(), 12.into(), SRTT, t0);
        assert_eq!(pipe.pipe, 3 * SIZE);
    }

    // Packet 10 is resent at t1. The only packet sent after the resend is
    // the newest, 20, and it arrives. The resend was outrun, so it is due a
    // round trip and a quarter after it left.
    #[test]
    fn a_resend_outrun_by_the_newest_packet_is_due() {
        let t0 = Instant::now();
        let t1 = t0 + SRTT;
        let mut segs = queue(10, 20);
        send(&mut segs, 10..=19, t0);
        segs.remove_up_to_ack(t1, &sack(9, 11..=15));
        segs.calc_pipe(9.into(), 19.into(), 19.into(), SRTT, t1);
        resend_as_lost(&mut segs, 10, t1);
        send(&mut segs, 20..=20, t1 + 10 * MS);
        segs.remove_up_to_ack(t1 + 110 * MS, &sack(9, 11..=20));
        let pipe = segs.calc_pipe(10.into(), 20.into(), 20.into(), SRTT, t1 + 110 * MS);
        assert_eq!(pipe.reorder_at, Some(t1 + 125 * MS));
    }

    // Packets 10 to 12 leave at t0 and 10 is sent again 50 ms later. The
    // pipe is counted again when the first of them expires: 11, at t0 +
    // 75 ms, not the resent 10 at t0 + 125 ms.
    #[test]
    fn the_pipe_timer_is_the_earliest_expiry() {
        let t0 = Instant::now();
        let mut segs = queue(10, 12);
        send(&mut segs, 10..=12, t0);
        let stats = UtpStats::default();
        segs.iter_mut_for_sending(None)
            .next()
            .unwrap()
            .on_sent(t0 + 50 * MS, &stats);
        let pipe = segs.calc_pipe(10.into(), 12.into(), 12.into(), SRTT, t0 + 60 * MS);
        assert_eq!(pipe.recalc_timer, Some(t0 + 75 * MS));
    }
}
