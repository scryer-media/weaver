// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::{
    num::NonZeroUsize,
    sync::Arc,
    task::{Context, Poll},
};

use parking_lot::Mutex;
use tokio::sync::mpsc;

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

#[derive(Clone)]
pub(crate) struct Sender {
    tx: mpsc::Sender<Event>,
    budget: Arc<Budget>,
}

pub(crate) struct Receiver {
    rx: mpsc::Receiver<Event>,
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
    let (tx, rx) = mpsc::channel(limits.data_packets + limits.control_packets);
    let budget = Arc::new(Budget {
        limits,
        usage: Mutex::new(Usage::default()),
    });
    (Sender { tx, budget }, Receiver { rx })
}

impl Sender {
    // Full inboxes drop datagrams rather than blocking the shared socket.
    // A refused DATA payload can still carry its peer's ACK and window.
    pub fn send(&self, mut message: UtpMessage) -> Result<(), Closed> {
        if self.tx.is_closed() {
            return Err(Closed);
        }
        let (reservation, payload_dropped) = if message.header.htype == Type::Data {
            match self.budget.reserve(message.data.len(), true) {
                Some(reservation) => (Some(reservation), false),
                None => {
                    message.data = Vec::new();
                    (self.budget.reserve(0, false), true)
                }
            }
        } else {
            if !message.data.is_empty() {
                return Ok(());
            }
            (self.budget.reserve(0, false), false)
        };
        let Some(reservation) = reservation else {
            return Ok(());
        };
        match self.tx.try_send(Event {
            message,
            payload_dropped,
            _reservation: reservation,
        }) {
            Ok(()) | Err(mpsc::error::TrySendError::Full(_)) => Ok(()),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(Closed),
        }
    }

    #[cfg(test)]
    fn usage(&self) -> Usage {
        *self.budget.usage.lock()
    }
}

impl Receiver {
    pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<Event>> {
        self.rx.poll_recv(cx)
    }

    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.rx.is_empty()
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.rx.len()
    }

    #[cfg(test)]
    pub fn try_recv(&mut self) -> Result<Event, mpsc::error::TryRecvError> {
        self.rx.try_recv()
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
        let data = rx.rx.try_recv().unwrap();
        assert_eq!(data.message.payload(), b"abcd");
        assert!(!data.payload_dropped);
        let header = rx.rx.try_recv().unwrap();
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
        assert_eq!(rx.rx.try_recv().unwrap().message.payload(), b"a");
        assert_eq!(rx.rx.try_recv().unwrap().message.payload(), b"b");
        assert!(rx.rx.try_recv().unwrap().payload_dropped);
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
        assert_eq!(rx.rx.try_recv().unwrap().message.header.htype, Type::State);
        assert_eq!(rx.rx.try_recv().unwrap().message.header.htype, Type::State);
        assert_eq!(rx.rx.try_recv().unwrap().message.payload(), b"a");
        assert_eq!(tx.usage(), Usage::default());
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
            second_rx.rx.try_recv().unwrap().message.header.htype,
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
}
