// Lifted from librqbit-utp 0.7.0 (Apache-2.0), Copyright (c) the librqbit authors.

/// The receiving side of uTP stream abstraction.
///
/// Users read from UtpStreamReadHalf. When there's nothing more to read
/// the task waits until the dispatcher puts more data in there.
///
/// Messages are assembled from out of order here too.
///
/// The dispatcher puts data here via add_remove(). If the messages were in-order, or
/// became ready for in-order delivery, they'll get put into UtpStreamReadHalf.
use std::{
    collections::VecDeque,
    io::IoSliceMut,
    num::NonZeroUsize,
    pin::Pin,
    sync::Arc,
    task::{Poll, Waker},
};

use msgq::MsgQueue;
use parking_lot::Mutex;
use tokio::io::AsyncRead;
use tracing::{debug, trace};

#[derive(Debug, PartialEq, Eq)]
enum UserRxMessage {
    Payload(Payload),
    Eof,
    Error(String),
}

impl UserRxMessage {
    pub fn len_bytes(&self) -> usize {
        match &self {
            UserRxMessage::Payload(payload) => payload.len(),
            _ => 0,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum OoqMessage {
    Payload(Payload),
    Eof,
}

impl Default for OoqMessage {
    fn default() -> Self {
        OoqMessage::Payload(Vec::new())
    }
}

impl OoqMessage {
    pub fn len_bytes(&self) -> usize {
        match &self {
            OoqMessage::Payload(payload) => payload.len(),
            _ => 0,
        }
    }
}

mod msgq {
    use std::collections::VecDeque;

    use super::{OoqMessage, UserRxMessage};

    pub struct MsgQueue {
        queue: VecDeque<UserRxMessage>,
        len_bytes: usize,
        capacity: usize,
    }

    impl MsgQueue {
        pub fn new(capacity: usize) -> Self {
            Self {
                queue: Default::default(),
                len_bytes: 0,
                capacity,
            }
        }

        #[cfg(test)]
        pub fn len_bytes(&self) -> usize {
            self.len_bytes
        }

        pub fn window(&self) -> usize {
            self.capacity.saturating_sub(self.len_bytes)
        }

        #[cfg(test)]
        pub fn is_full(&self) -> bool {
            self.len_bytes >= self.capacity
        }

        pub fn pop_front(&mut self) -> Option<UserRxMessage> {
            let msg = self.queue.pop_front()?;
            self.len_bytes -= msg.len_bytes();
            Some(msg)
        }

        pub fn try_push_back(&mut self, msg: OoqMessage) -> Result<(), OoqMessage> {
            let len = msg.len_bytes();
            if self.capacity - self.len_bytes < len {
                return Err(msg);
            }
            self.queue.push_back(match msg {
                OoqMessage::Payload(payload) => UserRxMessage::Payload(payload),
                OoqMessage::Eof => UserRxMessage::Eof,
            });
            self.len_bytes += len;
            Ok(())
        }

        pub(crate) fn push_back(&mut self, msg: UserRxMessage) {
            self.len_bytes += msg.len_bytes();
            self.queue.push_back(msg);
        }
    }
}

use crate::{
    Error, Payload,
    message::UtpMessage,
    raw::{Type, selective_ack::SelectiveAck},
    utils::update_optional_waker,
};

pub struct UtpStreamReadHalf {
    current: Option<BeingRead>,
    is_eof: bool,
    shared: Arc<UserRxShared>,
}

impl UtpStreamReadHalf {
    pub fn poll_read_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        mut bufs: &mut [IoSliceMut<'_>],
    ) -> Poll<std::io::Result<usize>> {
        let mut written = 0usize;
        let mut dispatcher_dead = false;

        while let Some(current_buf) = bufs.first_mut() {
            if current_buf.is_empty() {
                bufs = &mut bufs[1..];
                continue;
            }
            // If there was a previous message we haven't read till the end, do it.
            if let Some(current) = self.current.as_mut() {
                let payload = &current.payload[current.offset..];
                if payload.is_empty() {
                    return Poll::Ready(Err(std::io::Error::other(
                        "bug in UtpStreamReadHalf: payload is empty",
                    )));
                }

                let len = current_buf.len().min(payload.len());
                current_buf[..len].copy_from_slice(&payload[..len]);
                current_buf.advance(len);

                written += len;
                current.offset += len;
                if current.offset == current.payload.len() {
                    self.current = None;
                }
                continue;
            }

            if self.is_eof {
                break;
            }

            let mut g = self.shared.locked.lock();
            if let Some(msg) = g.queue.pop_front() {
                match msg {
                    UserRxMessage::Eof => {
                        drop(g);
                        self.is_eof = true;
                        break;
                    }
                    UserRxMessage::Payload(payload) => {
                        drop(g);
                        self.current = Some(BeingRead { payload, offset: 0 })
                    }
                    UserRxMessage::Error(msg) => {
                        return Poll::Ready(Err(std::io::Error::other(msg)));
                    }
                }
            } else {
                if g.vsock_closed {
                    dispatcher_dead = true;
                } else {
                    update_optional_waker(&mut g.reader_waker, cx);
                }
                break;
            }
        }

        if written > 0 {
            let mut g = self.shared.locked.lock();
            let waker = g.dispatcher_waker.take();
            drop(g);
            if let Some(waker) = waker {
                waker.wake();
            }
            return Poll::Ready(Ok(written));
        }

        if self.is_eof {
            return Poll::Ready(Ok(0));
        }

        if dispatcher_dead {
            return Poll::Ready(Err(std::io::Error::other("dispatcher dead")));
        }

        Poll::Pending
    }

    #[cfg(test)]
    pub async fn read_all_available(&mut self) -> std::io::Result<Vec<u8>> {
        let mut buf = vec![0u8; 2 * 1024 * 1024];
        let mut offset = 0;
        let mut g = self.shared.locked.lock();
        while let Some(m) = g.queue.pop_front() {
            match m {
                UserRxMessage::Payload(payload) => {
                    buf[offset..offset + payload.len()].copy_from_slice(&payload);
                    offset += payload.len();
                }
                UserRxMessage::Eof => {
                    break;
                }
                UserRxMessage::Error(e) => return Err(std::io::Error::other(e)),
            }
        }
        buf.truncate(offset);
        Ok(buf)
    }
}

impl AsyncRead for UtpStreamReadHalf {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let mut iovecs = [IoSliceMut::new(buf.initialize_unfilled())];
        let len = std::task::ready!(self.poll_read_vectored(cx, &mut iovecs)?);
        buf.advance(len);
        Poll::Ready(Ok(()))
    }
}

struct BeingRead {
    payload: Payload,
    offset: usize,
}

struct UserRxSharedLocked {
    reader_dropped: bool,
    vsock_closed: bool,
    queue: MsgQueue,
    dispatcher_waker: Option<Waker>,
    reader_waker: Option<Waker>,
}

struct UserRxShared {
    locked: Mutex<UserRxSharedLocked>,
}

impl Drop for UtpStreamReadHalf {
    fn drop(&mut self) {
        let mut g = self.shared.locked.lock();
        g.reader_dropped = true;
        let waker = g.dispatcher_waker.take();
        drop(g);
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

impl Drop for UserRx {
    fn drop(&mut self) {
        self.mark_vsock_closed();
    }
}

impl UserRxShared {
    #[cfg(test)]
    pub fn is_full_test(&self) -> bool {
        self.locked.lock().queue.is_full()
    }
}

pub struct UserRx {
    shared: Arc<UserRxShared>,
    ooq: OutOfOrderQueue,
    max_incoming_payload: NonZeroUsize,
    last_remaining_rx_window: usize,
}

impl UserRx {
    pub fn build(
        max_rx_bytes: NonZeroUsize,
        max_incoming_payload: NonZeroUsize,
    ) -> (UserRx, UtpStreamReadHalf) {
        let shared = Arc::new(UserRxShared {
            locked: Mutex::new(UserRxSharedLocked {
                dispatcher_waker: None,
                reader_waker: None,
                queue: MsgQueue::new(max_rx_bytes.get()),
                reader_dropped: false,
                vsock_closed: false,
            }),
        });
        let read_half = UtpStreamReadHalf {
            current: None,
            shared: shared.clone(),
            is_eof: false,
        };
        let ooq_capacity = max_rx_bytes.get() / max_incoming_payload.get();
        let out_of_order_queue = OutOfOrderQueue::new(
            NonZeroUsize::new(ooq_capacity).unwrap_or_else(|| NonZeroUsize::new(64).unwrap()),
        );
        let write_half = UserRx {
            shared,
            ooq: out_of_order_queue,
            max_incoming_payload,
            last_remaining_rx_window: max_rx_bytes.get(),
        };
        (write_half, read_half)
    }

    pub fn is_reader_dropped(&self) -> bool {
        self.shared.locked.lock().reader_dropped
    }

    /// How many bytes does the user half have available. If the user is not reading, and the buffer is filled up,
    /// this will be 0.
    pub fn remaining_rx_window(&self) -> usize {
        if self.is_reader_dropped() {
            0
        } else {
            self.last_remaining_rx_window
                .saturating_sub(self.ooq.stored_bytes())
        }
    }

    /// Inform the read half that the socket is closed - there will be no more data.
    pub fn mark_vsock_closed(&self) {
        let mut g = self.shared.locked.lock();
        if !g.vsock_closed {
            trace!("user_rx: marking vsock closed");
            g.vsock_closed = true;
            let waker = g.reader_waker.take();
            drop(g);
            if let Some(waker) = waker {
                waker.wake();
            }
        }
    }

    /// Flush the outstanding messages to user read half.
    /// Returns the number of bytes flushed.
    pub fn flush(&mut self, cx: &mut std::task::Context<'_>) -> crate::Result<usize> {
        let filled_front_bytes: usize = self.ooq.filled_front_bytes();
        let mut remaining_rx_window = {
            let mut g = self.shared.locked.lock();
            let remaining_window = g.queue.window();
            if remaining_window.saturating_sub(filled_front_bytes) < self.max_incoming_payload.get()
            {
                update_optional_waker(&mut g.dispatcher_waker, cx);
            }
            remaining_window
        };

        // Flush as many items as possible from the beginning of out of order queue to the user RX
        let mut flushed_bytes = 0;
        let mut flushed_packets = 0;

        while let Some(len) = self.ooq.send_front_if_fits(remaining_rx_window, |msg| {
            let mut g = self.shared.locked.lock();
            if g.reader_dropped {
                debug_every_ms!(5000, "reader is dead, could not send UtpMesage to it");
                return Err(msg);
            }
            g.queue.try_push_back(msg).unwrap();
            Ok(())
        }) {
            flushed_bytes += len;
            remaining_rx_window -= len;
            flushed_packets += 1;
        }

        if flushed_bytes > 0 {
            let waker = self.shared.locked.lock().reader_waker.take();
            if let Some(w) = waker {
                w.wake();
            }
            trace!(
                packets = flushed_packets,
                bytes = flushed_bytes,
                "flushed from out-of-order user RX"
            );
        }

        if self.ooq.filled_front > 0 {
            trace!(
                flushed_bytes,
                flushed_packets,
                out_of_order_filled_front = self.ooq.filled_front,
                remaining_rx_window,
                "did not flush everything"
            );
        }

        self.last_remaining_rx_window = remaining_rx_window;
        Ok(flushed_bytes)
    }

    /// Enqueue an error into read half to be consumed by the user.
    pub fn enqueue_error(&self, msg: String) {
        let mut g = self.shared.locked.lock();
        g.queue.push_back(UserRxMessage::Error(msg));
        let waker = g.reader_waker.take();
        if let Some(waker) = waker {
            drop(g);
            waker.wake();
        }
    }

    /// Generate a selective ACK message if there are out of order packets.
    pub fn selective_ack(&self) -> Option<SelectiveAck> {
        self.ooq.selective_ack()
    }

    #[cfg(test)]
    pub fn len_test(&self) -> usize {
        self.shared.locked.lock().queue.len_bytes()
    }

    /// Is assembler empty
    pub fn assembler_empty(&self) -> bool {
        self.ooq.is_empty()
    }

    /// How many packets does ooq have
    #[cfg(test)]
    pub fn assembler_packets(&self) -> usize {
        self.ooq.stored_packets()
    }

    /// The main function for the dispatcher - gets called when new data arrives at a particular offset.
    /// It's lazy - doesn't flush right away for performance. The caller needs to flush() periodically.
    pub fn add_remove(
        &mut self,
        cx: &mut std::task::Context<'_>,
        msg: UtpMessage,
        offset: usize,
    ) -> crate::Result<AssemblerAddRemoveResult> {
        match self.ooq.add_remove(msg, offset)? {
            res @ AssemblerAddRemoveResult::Consumed {
                sequence_numbers, ..
            } if sequence_numbers > 0 && self.ooq.is_full() => {
                self.flush(cx)?;
                Ok(res)
            }
            res => Ok(res),
        }
    }

    #[cfg(test)]
    async fn add_remove_test(
        &mut self,
        msg: UtpMessage,
        offset: usize,
    ) -> crate::Result<AssemblerAddRemoveResult> {
        let mut msg = Some(msg);
        let msg = &mut msg;
        std::future::poll_fn(move |cx| {
            let res = self.add_remove(cx, msg.take().unwrap(), offset);
            Poll::Ready(res)
        })
        .await
    }

    #[cfg(test)]
    pub fn is_flush_waker_registered(&self) -> bool {
        self.shared.locked.lock().dispatcher_waker.is_some()
    }

    #[cfg(test)]
    fn enqueue_test(&self, msg: UserRxMessage) {
        let mut g = self.shared.locked.lock();
        g.queue.push_back(msg);
    }
}

pub struct OutOfOrderQueue {
    data: VecDeque<OoqMessage>,
    filled_front: usize,
    len: usize,
    len_bytes: usize,
    capacity: usize,
}

#[derive(Debug, PartialEq, Eq)]
pub enum AssemblerAddRemoveResult {
    Consumed {
        sequence_numbers: usize,
        bytes: usize,
    },
    AlreadyPresent,
    Unavailable(UtpMessage),
}

fn ooq_slot_is_default(slot: &OoqMessage) -> bool {
    match slot {
        OoqMessage::Payload(payload) => payload.is_empty(),
        OoqMessage::Eof => false,
    }
}

impl OutOfOrderQueue {
    pub fn new(capacity: NonZeroUsize) -> Self {
        Self {
            data: VecDeque::from(vec![Default::default(); capacity.get()]),
            filled_front: 0,
            len: 0,
            len_bytes: 0,
            capacity: capacity.get(),
        }
    }

    fn filled_front_bytes(&self) -> usize {
        self.data
            .iter()
            .take(self.filled_front)
            .map(|m| m.len_bytes())
            .sum()
    }

    fn send_front_if_fits(
        &mut self,
        window: usize,
        send_fn: impl FnOnce(OoqMessage) -> Result<(), OoqMessage>,
    ) -> Option<usize> {
        if self.filled_front == 0 {
            return None;
        }
        if self.data[0].len_bytes() > window {
            return None;
        }
        let msg = self.data.pop_front().unwrap();
        let len = msg.len_bytes();
        match send_fn(msg) {
            Ok(()) => {}
            Err(msg) => {
                self.data.push_front(msg);
                return None;
            }
        }
        self.filled_front -= 1;
        self.len -= 1;
        self.len_bytes -= len;
        self.data.push_back(Default::default());
        Some(len)
    }

    pub fn is_empty(&self) -> bool {
        self.filled_front == self.len
    }

    pub fn is_full(&self) -> bool {
        self.len == self.capacity
    }

    #[cfg(test)]
    fn filled_front(&self) -> usize {
        self.filled_front
    }

    #[cfg(test)]
    fn stored_packets(&self) -> usize {
        self.len
    }

    fn stored_bytes(&self) -> usize {
        self.len_bytes
    }

    #[cfg(test)]
    fn debug_string(&self, with_data: bool) -> impl std::fmt::Display + '_ {
        struct D<'a> {
            q: &'a OutOfOrderQueue,
            with_data: bool,
        }
        impl std::fmt::Display for D<'_> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(
                    f,
                    "len={}, len_bytes={}",
                    self.q.stored_packets(),
                    self.q.stored_bytes(),
                )?;

                if !self.with_data {
                    return Ok(());
                }

                write!(f, ", queue={:?}", self.q.data)?;
                Ok(())
            }
        }
        D { q: self, with_data }
    }

    pub fn selective_ack(&self) -> Option<SelectiveAck> {
        if self.is_empty() {
            return None;
        }

        let start = self.filled_front + 1;
        if start >= self.data.len() {
            return None;
        }
        let unacked = self
            .data
            .range(start..)
            .enumerate()
            .filter_map(|(idx, data)| {
                if ooq_slot_is_default(data) {
                    None
                } else {
                    Some(idx)
                }
            });

        Some(SelectiveAck::new(unacked))
    }

    pub fn add_remove(
        &mut self,
        msg: UtpMessage,
        offset: usize,
    ) -> crate::Result<AssemblerAddRemoveResult> {
        if self.is_full() {
            debug!(offset, "assembler buffer full");
            return Ok(AssemblerAddRemoveResult::Unavailable(msg));
        }

        let effective_offset = offset + self.filled_front;

        if effective_offset >= self.data.len() {
            trace!(
                offset,
                self.filled_front, effective_offset, "message is past assembler's window"
            );
            return Ok(AssemblerAddRemoveResult::Unavailable(msg));
        }

        let msg = match msg.header.htype {
            Type::ST_DATA if msg.payload().is_empty() => return Err(Error::ZeroPayloadStData),
            Type::ST_DATA => OoqMessage::Payload(msg.data),
            Type::ST_FIN => OoqMessage::Eof,
            _ => return Err(Error::BugInvalidMessageExpectedStDataOrFin),
        };

        let slot = self
            .data
            .get_mut(effective_offset)
            .ok_or(Error::BugAssemblerMissingSlot(effective_offset))?;
        if !ooq_slot_is_default(slot) {
            return Ok(AssemblerAddRemoveResult::AlreadyPresent);
        }

        self.len += 1;
        self.len_bytes += msg.len_bytes();
        *slot = msg;

        let range = self.filled_front..self.data.len();
        // Advance "filled" if a contiguous data range was found.
        let (consumed_segments, consumed_bytes) = self
            .data
            .range(range)
            .take_while(|msg| !ooq_slot_is_default(msg))
            .fold((0, 0), |mut state, msg| {
                state.0 += 1;
                state.1 += msg.len_bytes();
                state
            });
        self.filled_front += consumed_segments;
        Ok(AssemblerAddRemoveResult::Consumed {
            sequence_numbers: consumed_segments,
            bytes: consumed_bytes,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{future::poll_fn, num::NonZeroUsize, task::Poll};

    use tokio::io::AsyncReadExt;
    use tracing::trace;

    use crate::{
        message::UtpMessage,
        stream_rx::{AssemblerAddRemoveResult, OutOfOrderQueue, UserRxMessage},
        test_util::setup_test_logging,
    };

    use super::{UserRx, UtpStreamReadHalf};

    fn msg(seq_nr: u16, payload: &[u8]) -> UtpMessage {
        UtpMessage::new_test(
            crate::raw::UtpHeader {
                htype: crate::raw::Type::ST_DATA,
                seq_nr: seq_nr.into(),
                ..Default::default()
            },
            payload,
        )
    }

    fn user_rx(capacity_bytes: usize) -> (UserRx, UtpStreamReadHalf) {
        UserRx::build(
            NonZeroUsize::new(capacity_bytes).unwrap(),
            NonZeroUsize::new(1500).unwrap(),
        )
    }

    #[test]
    fn test_asm_add_one_in_order() {
        let mut asm = OutOfOrderQueue::new(NonZeroUsize::new(2).unwrap());
        assert_eq!(
            asm.add_remove(msg(0, b"a"), 0).unwrap(),
            AssemblerAddRemoveResult::Consumed {
                sequence_numbers: 1,
                bytes: 1
            }
        );
        assert_eq!(asm.stored_packets(), 1);
        assert_eq!(asm.stored_bytes(), 1);
        assert_eq!(asm.filled_front(), 1);
    }

    #[test]
    fn test_asm_add_one_out_of_order() {
        let mut asm = OutOfOrderQueue::new(NonZeroUsize::new(2).unwrap());
        assert_eq!(
            asm.add_remove(msg(100, b"a"), 1).unwrap(),
            AssemblerAddRemoveResult::Consumed {
                sequence_numbers: 0,
                bytes: 0
            }
        );
        assert_eq!(asm.stored_packets(), 1);
        assert_eq!(asm.stored_bytes(), 1);
        assert_eq!(asm.filled_front(), 0);
    }

    #[tokio::test]
    async fn test_asm_channel_full_asm_empty() {
        setup_test_logging();
        let (mut user_rx, _read) = user_rx(1);
        let msg = msg(0, b"a");

        // fill RX
        user_rx.enqueue_test(UserRxMessage::Payload(b"a".to_vec()));

        assert!(user_rx.shared.is_full_test());

        assert_eq!(
            user_rx.add_remove_test(msg.clone(), 0).await.unwrap(),
            AssemblerAddRemoveResult::Consumed {
                sequence_numbers: 1,
                bytes: 1
            }
        );
        assert_eq!(user_rx.ooq.stored_packets(), 1);
        assert_eq!(user_rx.ooq.stored_bytes(), 1);
        assert_eq!(user_rx.ooq.filled_front(), 1);
    }

    #[tokio::test]
    async fn test_asm_channel_full_asm_not_empty() {
        let (mut user_rx, _read) = user_rx(1);
        let msg = msg(0, b"a");

        // fill RX
        user_rx.enqueue_test(UserRxMessage::Payload(msg.data.clone()));

        assert_eq!(
            user_rx.add_remove_test(msg.clone(), 1).await.unwrap(),
            AssemblerAddRemoveResult::Consumed {
                sequence_numbers: 0,
                bytes: 0
            }
        );

        assert_eq!(user_rx.ooq.stored_packets(), 1);
        assert_eq!(user_rx.ooq.stored_bytes(), 1);
        assert_eq!(user_rx.ooq.filled_front(), 0);

        assert_eq!(
            user_rx.add_remove_test(msg.clone(), 0).await.unwrap(),
            AssemblerAddRemoveResult::Consumed {
                sequence_numbers: 2,
                bytes: 2
            }
        );
        assert_eq!(user_rx.ooq.stored_packets(), 2);
        assert_eq!(user_rx.ooq.stored_bytes(), 2);
        assert_eq!(user_rx.ooq.filled_front(), 2);
    }

    #[tokio::test]
    async fn test_asm_out_of_order() {
        setup_test_logging();

        let (mut user_rx, mut read) = user_rx(100);

        let msg_0 = msg(0, b"hello");
        let msg_1 = msg(1, b"world");
        let msg_2 = msg(2, b"test");

        assert_eq!(
            user_rx.add_remove_test(msg_1.clone(), 1).await.unwrap(),
            AssemblerAddRemoveResult::Consumed {
                sequence_numbers: 0,
                bytes: 0
            }
        );
        trace!(asm=%user_rx.ooq.debug_string(true));
        assert_eq!(user_rx.ooq.stored_packets(), 1);

        assert_eq!(
            user_rx.add_remove_test(msg_2.clone(), 2).await.unwrap(),
            AssemblerAddRemoveResult::Consumed {
                sequence_numbers: 0,
                bytes: 0
            }
        );
        trace!(asm=%user_rx.ooq.debug_string(true));

        assert_eq!(
            user_rx.add_remove_test(msg_0.clone(), 0).await.unwrap(),
            AssemblerAddRemoveResult::Consumed {
                sequence_numbers: 3,
                bytes: 14
            }
        );
        trace!(asm=%user_rx.ooq.debug_string(true));
        assert_eq!(user_rx.ooq.stored_packets(), 3);
        poll_fn(|cx| {
            assert_eq!(user_rx.flush(cx).unwrap(), 14);
            Poll::Ready(())
        })
        .await;
        assert_eq!(user_rx.ooq.stored_packets(), 0);

        let mut buf = [0u8; 1024];
        let sz = read.read(&mut buf).await.unwrap();
        assert_eq!(std::str::from_utf8(&buf[..sz]), Ok("helloworldtest"));
    }

    #[tokio::test]
    async fn test_asm_inorder() {
        setup_test_logging();
        let (mut user_rx, mut read) = user_rx(100);

        let msg_0 = msg(0, b"hello");
        let msg_1 = msg(1, b"world");
        let msg_2 = msg(2, b"test");

        assert_eq!(
            user_rx.add_remove_test(msg_0.clone(), 0).await.unwrap(),
            AssemblerAddRemoveResult::Consumed {
                sequence_numbers: 1,
                bytes: 5
            }
        );
        trace!(asm=%user_rx.ooq.debug_string(true));
        assert_eq!(user_rx.ooq.stored_packets(), 1);

        assert_eq!(
            user_rx.add_remove_test(msg_1.clone(), 0).await.unwrap(),
            AssemblerAddRemoveResult::Consumed {
                sequence_numbers: 1,
                bytes: 5
            }
        );
        trace!(asm=%user_rx.ooq.debug_string(true));

        assert_eq!(
            user_rx.add_remove_test(msg_2.clone(), 0).await.unwrap(),
            AssemblerAddRemoveResult::Consumed {
                sequence_numbers: 1,
                bytes: 4
            }
        );
        trace!(asm=%user_rx.ooq.debug_string(true));
        assert_eq!(user_rx.ooq.stored_packets(), 3);

        poll_fn(|cx| {
            assert_eq!(user_rx.flush(cx).unwrap(), 14);
            Poll::Ready(())
        })
        .await;

        let mut buf = [0u8; 1024];
        let sz = read.read(&mut buf).await.unwrap();
        assert_eq!(std::str::from_utf8(&buf[..sz]), Ok("helloworldtest"));
    }

    #[test]
    fn test_asm_write_out_of_bounds() {
        setup_test_logging();

        let mut asm = OutOfOrderQueue::new(NonZeroUsize::new(3).unwrap());

        let msg_2 = msg(2, b"test");
        let msg_3 = msg(3, b"test");

        assert_eq!(
            asm.add_remove(msg_2.clone(), 2).unwrap(),
            AssemblerAddRemoveResult::Consumed {
                sequence_numbers: 0,
                bytes: 0
            }
        );
        trace!(asm=%asm.debug_string(true));
        assert_eq!(asm.stored_packets(), 1);

        // A message that is out of bounds of the assembler should be dropped.
        assert_eq!(
            asm.add_remove(msg_3.clone(), 3).unwrap(),
            AssemblerAddRemoveResult::Unavailable(msg_3)
        );
        trace!(asm=%asm.debug_string(true));
        assert_eq!(asm.stored_packets(), 1);
    }

    #[test]
    fn test_asm_duplicate_msg_ignored() {
        setup_test_logging();

        let mut asm = OutOfOrderQueue::new(NonZeroUsize::new(10).unwrap());
        let msg_2 = msg(2, b"test");
        assert_eq!(
            asm.add_remove(msg_2, 2).unwrap(),
            AssemblerAddRemoveResult::Consumed {
                sequence_numbers: 0,
                bytes: 0
            }
        );

        let msg_2 = msg(2, b"test");
        assert_eq!(
            asm.add_remove(msg_2, 2).unwrap(),
            AssemblerAddRemoveResult::AlreadyPresent
        );
    }
}
