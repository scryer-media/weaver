// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A stream's inbox. The socket's receive loop queues the datagrams of one
// read of the UDP socket without waking the stream, then marks where that
// read ended and wakes it once. The mark is where the stream sends the ACK
// it owes, as libtorrent sends its deferred ACK when the socket is drained
// (v2.0.15 utp_socket_manager.cpp:255-261, session_impl.cpp:2727).

use std::{
    collections::VecDeque,
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Waker},
};

use parking_lot::Mutex;

use crate::{message::UtpMessage, raw::Type};

const CONTROL_PACKETS: usize = 32;

#[derive(Clone, Copy)]
struct Limits {
    bytes: usize,
    data_packets: usize,
    control_packets: usize,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Usage {
    bytes: usize,
    data_packets: usize,
    control_packets: usize,
}

struct Budget {
    limits: Limits,
    usage: Mutex<Usage>,
}

struct Reservation {
    budget: Arc<Budget>,
    bytes: usize,
    data: bool,
}

impl Budget {
    fn reserve(self: &Arc<Self>, bytes: usize, data: bool) -> Option<Reservation> {
        let mut usage = self.usage.lock();
        if data {
            if usage.data_packets >= self.limits.data_packets
                || bytes > self.limits.bytes.saturating_sub(usage.bytes)
            {
                return None;
            }
            usage.data_packets += 1;
            usage.bytes += bytes;
        } else {
            if usage.control_packets >= self.limits.control_packets {
                return None;
            }
            usage.control_packets += 1;
        }
        Some(Reservation {
            budget: Arc::clone(self),
            bytes,
            data,
        })
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        let mut usage = self.budget.usage.lock();
        if self.data {
            usage.data_packets -= 1;
            usage.bytes -= self.bytes;
        } else {
            usage.control_packets -= 1;
        }
    }
}

pub(crate) struct Event {
    pub message: UtpMessage,
    pub payload_dropped: bool,
    _reservation: Reservation,
}

struct Inbox {
    items: VecDeque<Event>,
    // Packets ever queued, and ever taken.
    pushed: u64,
    taken: u64,
    // Drain marks, each the count of packets queued when the receive loop
    // read the UDP socket empty or turned to another stream's packet. A
    // mark follows at least one packet, so there is at most one more mark
    // than packets queued, and the budget bounds both.
    marks: VecDeque<u64>,
    // A packet was queued since the last drain mark.
    unmarked: bool,
    waker: Option<Waker>,
    senders: usize,
    receiver_closed: bool,
    fault: Option<SendFault>,
}

struct Shared {
    budget: Arc<Budget>,
    inbox: Mutex<Inbox>,
    // A send the socket's I/O task made for the stream failed; the stream
    // looks at the slot only when this is set.
    faulted: AtomicBool,
}

/// How a datagram the socket's I/O task sent for a stream failed.
#[derive(Debug)]
pub(crate) enum SendFault {
    /// The host refused it as larger than its path carries: the size of
    /// the smallest payload refused. The datagram went again without
    /// don't-fragment.
    TooLarge(usize),
    /// The send failed for good, as a send of the stream's own would end it.
    Failed(std::io::Error),
}

pub(crate) struct Sender {
    shared: Arc<Shared>,
}

pub(crate) struct Receiver {
    shared: Arc<Shared>,
}

#[derive(Debug)]
pub(crate) struct Closed;

pub(crate) fn channel(receive_capacity: NonZeroUsize) -> (Sender, Receiver) {
    with_limits(Limits {
        bytes: receive_capacity.get(),
        data_packets: crate::stream_rx::reorder_capacity(receive_capacity).get(),
        control_packets: CONTROL_PACKETS,
    })
}

fn with_limits(limits: Limits) -> (Sender, Receiver) {
    let shared = Arc::new(Shared {
        budget: Arc::new(Budget {
            limits,
            usage: Mutex::new(Usage::default()),
        }),
        inbox: Mutex::new(Inbox {
            items: VecDeque::new(),
            pushed: 0,
            taken: 0,
            marks: VecDeque::new(),
            unmarked: false,
            waker: None,
            senders: 1,
            receiver_closed: false,
            fault: None,
        }),
        faulted: AtomicBool::new(false),
    });
    (
        Sender {
            shared: Arc::clone(&shared),
        },
        Receiver { shared },
    )
}

impl Clone for Sender {
    fn clone(&self) -> Self {
        self.shared.inbox.lock().senders += 1;
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl Drop for Sender {
    fn drop(&mut self) {
        let waker = {
            let mut inbox = self.shared.inbox.lock();
            inbox.senders -= 1;
            if inbox.senders > 0 {
                return;
            }
            inbox.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

impl Sender {
    // Queues a packet without waking the stream. Full inboxes drop
    // datagrams rather than blocking the shared socket. A refused DATA
    // payload can still carry its peer's ACK and window.
    pub fn push(&self, mut message: UtpMessage) -> Result<(), Closed> {
        let budget = &self.shared.budget;
        let (reservation, payload_dropped) = if message.header.htype == Type::Data {
            match budget.reserve(message.data.len(), true) {
                Some(reservation) => (Some(reservation), false),
                None => {
                    message.data = Vec::new();
                    (budget.reserve(0, false), true)
                }
            }
        } else {
            // Only DATA delivers a payload; any other packet still acts on
            // its header, a FIN with payload included.
            if !message.data.is_empty() {
                message.data = Vec::new();
            }
            (budget.reserve(0, false), false)
        };
        let Some(reservation) = reservation else {
            if self.shared.inbox.lock().receiver_closed {
                return Err(Closed);
            }
            return Ok(());
        };
        let event = Event {
            message,
            payload_dropped,
            _reservation: reservation,
        };
        let mut inbox = self.shared.inbox.lock();
        if inbox.receiver_closed {
            drop(inbox);
            drop(event);
            return Err(Closed);
        }
        inbox.items.push_back(event);
        inbox.pushed += 1;
        inbox.unmarked = true;
        Ok(())
    }

    // Marks the end of the packets queued so far and wakes the stream.
    pub fn flush(&self) {
        let waker = {
            let mut inbox = self.shared.inbox.lock();
            if !inbox.unmarked {
                return;
            }
            inbox.unmarked = false;
            let pushed = inbox.pushed;
            inbox.marks.push_back(pushed);
            inbox.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    // Wakes the stream to take what is queued, without a mark.
    pub fn wake(&self) {
        let waker = {
            let mut inbox = self.shared.inbox.lock();
            if inbox.items.is_empty() && inbox.marks.is_empty() {
                return;
            }
            inbox.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    // Hands the stream a failed send and wakes it. A failure for good
    // outranks a size refusal; of two size refusals the smaller is kept.
    pub fn report_send_fault(&self, fault: SendFault) {
        let waker = {
            let mut inbox = self.shared.inbox.lock();
            if inbox.receiver_closed {
                return;
            }
            inbox.fault = match (inbox.fault.take(), fault) {
                (Some(SendFault::Failed(error)), _) | (_, SendFault::Failed(error)) => {
                    Some(SendFault::Failed(error))
                }
                (Some(SendFault::TooLarge(a)), SendFault::TooLarge(b)) => {
                    Some(SendFault::TooLarge(a.min(b)))
                }
                (None, fault) => Some(fault),
            };
            self.shared.faulted.store(true, Ordering::Release);
            inbox.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    // One packet read alone: queued, marked and woken.
    #[cfg(test)]
    pub fn send(&self, message: UtpMessage) -> Result<(), Closed> {
        self.push(message)?;
        self.flush();
        Ok(())
    }

    #[cfg(test)]
    fn usage(&self) -> Usage {
        *self.shared.budget.usage.lock()
    }
}

// What the inbox holds next when it holds no packet to take first.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Stop {
    // Every packet before a drain mark is taken; the mark is consumed.
    DrainMark,
    // No packet will come: the socket dropped its senders.
    Closed,
}

impl Receiver {
    // A failed send the socket's I/O task reported, taken once.
    pub fn take_send_fault(&mut self) -> Option<SendFault> {
        if !self.shared.faulted.load(Ordering::Acquire) {
            return None;
        }
        let mut inbox = self.shared.inbox.lock();
        self.shared.faulted.store(false, Ordering::Relaxed);
        inbox.fault.take()
    }

    // Takes the next drain mark, or else the next packet, under one lock.
    pub fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Result<Event, Stop>> {
        let mut inbox = self.shared.inbox.lock();
        if inbox.marks.front() == Some(&inbox.taken) {
            inbox.marks.pop_front();
            return Poll::Ready(Err(Stop::DrainMark));
        }
        if let Some(event) = inbox.items.pop_front() {
            inbox.taken += 1;
            return Poll::Ready(Ok(event));
        }
        if inbox.senders == 0 {
            return Poll::Ready(Err(Stop::Closed));
        }
        Self::register(&mut inbox, cx);
        Poll::Pending
    }

    // Whether a packet is queued, not yet taken.
    pub fn has_packets_queued(&self) -> bool {
        !self.shared.inbox.lock().items.is_empty()
    }

    // Whether anything is queued, a mark included, or the senders are
    // gone; otherwise the stream is woken when something comes.
    pub fn poll_ready(&mut self, cx: &mut Context<'_>) -> bool {
        let mut inbox = self.shared.inbox.lock();
        if !inbox.items.is_empty() || !inbox.marks.is_empty() || inbox.senders == 0 {
            return true;
        }
        Self::register(&mut inbox, cx);
        false
    }

    fn register(inbox: &mut Inbox, cx: &mut Context<'_>) {
        match &mut inbox.waker {
            Some(waker) if waker.will_wake(cx.waker()) => {}
            slot => *slot = Some(cx.waker().clone()),
        }
    }

    #[cfg(test)]
    pub fn take_drain_mark(&mut self) -> bool {
        let mut inbox = self.shared.inbox.lock();
        if inbox.marks.front() == Some(&inbox.taken) {
            inbox.marks.pop_front();
            return true;
        }
        false
    }

    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        let inbox = self.shared.inbox.lock();
        inbox.items.is_empty() && inbox.marks.is_empty()
    }

    // Packets queued, drain marks left out.
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.shared.inbox.lock().items.len()
    }

    // What is queued, in order: `P` for a packet, `|` for a drain mark.
    #[cfg(test)]
    pub fn shape(&self) -> String {
        let inbox = self.shared.inbox.lock();
        let mut marks = inbox.marks.iter().peekable();
        let mut shape = String::new();
        for at in inbox.taken..=inbox.pushed {
            while marks.next_if(|&&mark| mark == at).is_some() {
                shape.push('|');
            }
            if at < inbox.pushed {
                shape.push('P');
            }
        }
        shape
    }

    // The next packet queued, past any drain marks.
    #[cfg(test)]
    pub fn try_recv(&mut self) -> Option<Event> {
        while self.take_drain_mark() {}
        let mut inbox = self.shared.inbox.lock();
        let event = inbox.items.pop_front()?;
        inbox.taken += 1;
        drop(inbox);
        while self.take_drain_mark() {}
        Some(event)
    }
}

impl Drop for Receiver {
    fn drop(&mut self) {
        let items = {
            let mut inbox = self.shared.inbox.lock();
            inbox.receiver_closed = true;
            inbox.unmarked = false;
            inbox.marks.clear();
            std::mem::take(&mut inbox.items)
        };
        // Releases each queued reservation outside the inbox lock.
        drop(items);
    }
}

#[cfg(test)]
mod tests {
    use super::{Limits, Usage, with_limits};
    use crate::{
        message::UtpMessage,
        raw::{Type, UtpHeader},
    };

    fn message(kind: Type, payload: &[u8]) -> UtpMessage {
        UtpMessage::new_test(
            UtpHeader {
                htype: kind,
                ..Default::default()
            },
            payload,
        )
    }

    #[test]
    fn payload_bytes_are_limited_independently_of_packet_slots() {
        let (tx, mut rx) = with_limits(Limits {
            bytes: 4,
            data_packets: 3,
            control_packets: 2,
        });
        tx.send(message(Type::Data, b"abcd")).unwrap();
        tx.send(message(Type::Data, b"e")).unwrap();
        assert_eq!(
            tx.usage(),
            Usage {
                bytes: 4,
                data_packets: 1,
                control_packets: 1
            }
        );
        let data = rx.try_recv().unwrap();
        assert_eq!(data.message.payload(), b"abcd");
        assert!(!data.payload_dropped);
        let header = rx.try_recv().unwrap();
        assert!(header.payload_dropped);
        assert_eq!(header.message.header.htype, Type::Data);
        assert!(header.message.payload().is_empty());
        assert_eq!(header.message.data.capacity(), 0);
        drop(data);
        assert_eq!(
            tx.usage(),
            Usage {
                control_packets: 1,
                ..Default::default()
            }
        );
        drop(header);
        assert_eq!(tx.usage(), Usage::default());
    }

    #[test]
    fn packet_slots_bound_tiny_payloads_before_the_byte_budget_is_full() {
        let (tx, mut rx) = with_limits(Limits {
            bytes: 1024,
            data_packets: 2,
            control_packets: 1,
        });
        for byte in b"abc" {
            tx.send(message(Type::Data, &[*byte])).unwrap();
        }
        assert_eq!(
            tx.usage(),
            Usage {
                bytes: 2,
                data_packets: 2,
                control_packets: 1
            }
        );
        assert_eq!(rx.len(), 3);
        assert_eq!(rx.try_recv().unwrap().message.payload(), b"a");
        assert_eq!(rx.try_recv().unwrap().message.payload(), b"b");
        assert!(rx.try_recv().unwrap().payload_dropped);
        assert_eq!(tx.usage(), Usage::default());
    }

    #[test]
    fn control_packets_are_bounded_without_using_payload_reservations() {
        let (tx, mut rx) = with_limits(Limits {
            bytes: 4,
            data_packets: 1,
            control_packets: 2,
        });
        for _ in 0..3 {
            tx.send(message(Type::State, b"")).unwrap();
        }
        tx.send(message(Type::Data, b"a")).unwrap();
        assert_eq!(
            tx.usage(),
            Usage {
                bytes: 1,
                data_packets: 1,
                control_packets: 2
            }
        );
        assert_eq!(rx.len(), 3);
        assert_eq!(rx.try_recv().unwrap().message.header.htype, Type::State);
        assert_eq!(rx.try_recv().unwrap().message.header.htype, Type::State);
        assert_eq!(rx.try_recv().unwrap().message.payload(), b"a");
        assert_eq!(tx.usage(), Usage::default());
    }

    #[test]
    fn a_control_packet_with_payload_keeps_its_header_without_the_payload() {
        let (tx, mut rx) = with_limits(Limits {
            bytes: 4,
            data_packets: 1,
            control_packets: 2,
        });
        tx.send(message(Type::Fin, b"abc")).unwrap();
        tx.send(message(Type::State, b"abc")).unwrap();
        assert_eq!(
            tx.usage(),
            Usage {
                control_packets: 2,
                ..Default::default()
            }
        );
        for kind in [Type::Fin, Type::State] {
            let event = rx.try_recv().unwrap();
            assert_eq!(event.message.header.htype, kind);
            assert!(event.message.payload().is_empty());
            assert!(!event.payload_dropped);
        }
    }

    #[test]
    fn dropping_the_receiver_releases_every_queued_reservation() {
        let (tx, rx) = with_limits(Limits {
            bytes: 4,
            data_packets: 1,
            control_packets: 1,
        });
        tx.send(message(Type::Data, b"abcd")).unwrap();
        tx.send(message(Type::State, b"")).unwrap();
        drop(rx);
        assert_eq!(tx.usage(), Usage::default());
        assert!(tx.send(message(Type::State, b"")).is_err());
    }

    #[test]
    fn a_full_inbox_cannot_block_admission_to_another_stream() {
        let limits = Limits {
            bytes: 1,
            data_packets: 1,
            control_packets: 1,
        };
        let (first, first_rx) = with_limits(limits);
        let (second, mut second_rx) = with_limits(limits);
        for _ in 0..100 {
            first.send(message(Type::Data, b"a")).unwrap();
        }
        second.send(message(Type::State, b"")).unwrap();
        assert_eq!(first_rx.len(), 2);
        assert_eq!(
            second_rx.try_recv().unwrap().message.header.htype,
            Type::State
        );
        assert_eq!(
            first.usage(),
            Usage {
                bytes: 1,
                data_packets: 1,
                control_packets: 1
            }
        );
    }

    // A mark follows only packets queued since the last one, so marks
    // never outnumber the packets the budget admits.
    #[test]
    fn drain_marks_follow_packets_and_never_pile_up() {
        let limits = Limits {
            bytes: 16,
            data_packets: 2,
            control_packets: 1,
        };
        let (tx, mut rx) = with_limits(limits);
        tx.flush();
        assert_eq!(rx.shape(), "");
        tx.push(message(Type::Data, b"a")).unwrap();
        tx.wake();
        assert_eq!(rx.shape(), "P");
        tx.push(message(Type::Data, b"b")).unwrap();
        tx.flush();
        tx.flush();
        assert_eq!(rx.shape(), "PP|");
        // Refused by the budget: nothing queued, so nothing to mark.
        tx.push(message(Type::Data, b"c")).unwrap();
        tx.push(message(Type::Fin, b"")).unwrap();
        tx.push(message(Type::State, b"")).unwrap();
        tx.flush();
        assert_eq!(rx.shape(), "PP|P|");
        for _ in 0..1000 {
            tx.flush();
        }
        assert_eq!(rx.shape(), "PP|P|");
        assert_eq!(rx.try_recv().unwrap().message.payload(), b"a");
        assert_eq!(rx.try_recv().unwrap().message.payload(), b"b");
        assert!(rx.try_recv().unwrap().payload_dropped);
        assert!(rx.try_recv().is_none());
        assert!(rx.is_empty());
    }
}
