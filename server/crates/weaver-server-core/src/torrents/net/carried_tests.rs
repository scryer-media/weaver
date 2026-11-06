// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

struct ControlledSocket {
    sent: tokio::sync::mpsc::UnboundedSender<(Vec<u8>, SocketAddr)>,
    results: tokio::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<io::Result<()>>>,
    mtu: Option<u16>,
}

#[async_trait::async_trait]
impl TunnelSocket for ControlledSocket {
    async fn send_to(&self, bytes: &[u8], target: SocketAddr) -> io::Result<()> {
        self.sent.send((bytes.to_vec(), target)).unwrap();
        self.results.lock().await.recv().await.unwrap()
    }

    async fn recv_from(&self, _: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        std::future::pending().await
    }

    fn local_addr(&self) -> Option<SocketAddr> {
        Some(([192, 0, 2, 1], 6881).into())
    }

    fn link_mtu(&self) -> Option<u16> {
        self.mtu
    }

    fn is_closed(&self) -> bool {
        false
    }

    async fn closed(&self) {
        std::future::pending().await
    }
}

type Observed = tokio::sync::mpsc::UnboundedReceiver<(Vec<u8>, SocketAddr)>;
type SendResults = tokio::sync::mpsc::UnboundedSender<io::Result<()>>;

fn controlled(mtu: Option<u16>) -> (Arc<ControlledSocket>, Observed, SendResults) {
    let (sent, observed) = tokio::sync::mpsc::unbounded_channel();
    let (results, received) = tokio::sync::mpsc::unbounded_channel();
    (
        Arc::new(ControlledSocket {
            sent,
            results: tokio::sync::Mutex::new(received),
            mtu,
        }),
        observed,
        results,
    )
}

#[derive(Default)]
struct Woken(AtomicUsize);

impl std::task::Wake for Woken {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn polled(
    sender: &mut dyn DatagramSend,
    bytes: &[u8],
    target: SocketAddr,
    wake: &Arc<Woken>,
) -> Poll<io::Result<usize>> {
    let waker = Waker::from(Arc::clone(wake));
    sender.poll_send_to(&mut Context::from_waker(&waker), bytes, target)
}

fn accepted(socket: &Carried, bytes: &[u8], target: SocketAddr) {
    let mut sender = socket.send_handle().unwrap();
    assert!(
        matches!(polled(&mut sender, bytes, target, &Arc::default()), Poll::Ready(Ok(len)) if len == bytes.len())
    );
}

#[tokio::test]
async fn carried_utp_acceptance_uses_the_carrier_fifo_for_async_and_polled_sends() {
    let (socket, mut observed, results) = controlled(Some(1280));
    let carried = Carried::new(socket, ([0, 0, 0, 0], 0).into(), true);
    let first = ([198, 51, 100, 1], 7001).into();
    let second = ([198, 51, 100, 2], 7002).into();
    assert!(carried.dont_fragment());
    assert_eq!(carried.link_mtu(), Some(1280));
    assert_eq!(carried.send_to(b"handshake", first).await.unwrap(), 9);
    let waker = Waker::from(Arc::new(Woken::default()));
    assert!(matches!(
        carried.sender().unwrap().poll_send_to_vectored(
            &mut Context::from_waker(&waker),
            &[IoSlice::new(b"da"), IoSlice::new(b"ta")],
            second,
        ),
        Poll::Ready(Ok(4))
    ));
    assert_eq!(
        observed.recv().await.unwrap(),
        (b"handshake".to_vec(), first)
    );
    assert!(
        observed.try_recv().is_err(),
        "only one carrier send is active"
    );
    results.send(Ok(())).unwrap();
    assert_eq!(observed.recv().await.unwrap(), (b"data".to_vec(), second));
    carried.shutdown().await.unwrap();
}

#[tokio::test]
async fn carried_utp_packet_bound_includes_the_active_send_and_wakes_the_oldest_flow() {
    let (socket, mut observed, results) = controlled(None);
    let carried = Carried::new(socket, ([0, 0, 0, 0], 0).into(), true);
    let target = ([198, 51, 100, 1], 7001).into();
    accepted(&carried, b"a", target);
    observed.recv().await.unwrap();
    for _ in 1..CARRIED_SEND_PACKETS {
        accepted(&carried, b"a", target);
    }
    let one = Arc::<Woken>::default();
    let two = Arc::<Woken>::default();
    let three = Arc::<Woken>::default();
    let mut one_sender = carried.send_handle().unwrap();
    let mut two_sender = carried.send_handle().unwrap();
    let mut three_sender = carried.send_handle().unwrap();
    assert!(polled(&mut one_sender, b"b", target, &one).is_pending());
    assert!(polled(&mut two_sender, b"c", target, &two).is_pending());
    assert!(polled(&mut three_sender, b"d", target, &three).is_pending());
    let waker = Waker::from(Arc::clone(&one));
    assert!(
        one_sender
            .poll_enqueue(&mut Context::from_waker(&waker), 1, target, || panic!(
                "a full queue must not copy a packet"
            ))
            .is_pending()
    );
    let sends = carried.sends.as_ref().unwrap();
    assert_eq!(sends.pending.lock().unwrap().count, CARRIED_SEND_PACKETS);
    results.send(Ok(())).unwrap();
    observed.recv().await.unwrap();
    assert_eq!(one.0.load(Ordering::SeqCst), 1);
    assert_eq!(two.0.load(Ordering::SeqCst), 0);
    assert_eq!(three.0.load(Ordering::SeqCst), 0);
    assert!(matches!(
        polled(&mut one_sender, b"b", target, &one),
        Poll::Ready(Ok(1))
    ));
    results.send(Ok(())).unwrap();
    observed.recv().await.unwrap();
    assert_eq!(two.0.load(Ordering::SeqCst), 1);
    assert_eq!(three.0.load(Ordering::SeqCst), 0);
    carried.shutdown().await.unwrap();
    assert_eq!(three.0.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn carried_utp_a_waiter_that_loses_its_room_registers_again() {
    let (socket, mut observed, results) = controlled(None);
    let carried = Carried::new(socket, ([0, 0, 0, 0], 0).into(), true);
    let target = ([198, 51, 100, 1], 7001).into();
    accepted(&carried, b"a", target);
    observed.recv().await.unwrap();
    for _ in 1..CARRIED_SEND_PACKETS {
        accepted(&carried, b"a", target);
    }
    let waiter = Arc::<Woken>::default();
    let mut sender = carried.send_handle().unwrap();
    assert!(polled(&mut sender, b"b", target, &waiter).is_pending());
    results.send(Ok(())).unwrap();
    observed.recv().await.unwrap();
    assert_eq!(waiter.0.load(Ordering::SeqCst), 1);
    accepted(&carried, b"c", target);
    assert!(polled(&mut sender, b"b", target, &waiter).is_pending());
    results.send(Ok(())).unwrap();
    observed.recv().await.unwrap();
    assert_eq!(waiter.0.load(Ordering::SeqCst), 2);
    carried.shutdown().await.unwrap();
}

#[tokio::test]
async fn carried_utp_byte_bound_includes_the_active_send() {
    let (socket, mut observed, _results) = controlled(None);
    let carried = Carried::new(socket, ([0, 0, 0, 0], 0).into(), true);
    let target = ([198, 51, 100, 1], 7001).into();
    let bytes = vec![0; 4096];
    accepted(&carried, &bytes, target);
    observed.recv().await.unwrap();
    for _ in 1..CARRIED_SEND_BYTES / bytes.len() {
        accepted(&carried, &bytes, target);
    }
    let mut sender = carried.send_handle().unwrap();
    assert!(polled(&mut sender, b"a", target, &Arc::default()).is_pending());
    {
        let pending = carried.sends.as_ref().unwrap().pending.lock().unwrap();
        assert_eq!(pending.bytes, CARRIED_SEND_BYTES);
        assert!(pending.count < CARRIED_SEND_PACKETS);
    }
    carried.shutdown().await.unwrap();
}

#[tokio::test]
async fn carried_utp_mtu_rejection_accounts_for_each_ip_family_before_acceptance() {
    for (target, headers) in [
        (SocketAddr::from(([198, 51, 100, 1], 7001)), 28),
        ("[2001:db8::1]:7001".parse().unwrap(), 48),
    ] {
        let (socket, mut observed, _results) = controlled(Some(1280));
        let carried = Carried::new(socket, ([0, 0, 0, 0], 0).into(), true);
        let oversized = vec![0; 1280 - headers + 1];
        let mut sender = carried.send_handle().unwrap();
        let Poll::Ready(Err(error)) = polled(&mut sender, &oversized, target, &Arc::default())
        else {
            panic!("the oversized packet was accepted")
        };
        assert!(weaver_torrent::network::is_message_size_error(&error));
        assert_eq!(
            carried
                .sends
                .as_ref()
                .unwrap()
                .pending
                .lock()
                .unwrap()
                .count,
            0
        );
        accepted(&carried, &oversized[..oversized.len() - 1], target);
        assert_eq!(observed.recv().await.unwrap().0.len(), 1280 - headers);
        carried.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn carried_utp_deferred_socket_wide_error_is_a_sticky_terminal_socket_fault() {
    for failure in [
        weaver_torrent::network::message_size_error(),
        io::Error::other("the carrier stopped"),
    ] {
        let (socket, mut observed, results) = controlled(Some(1280));
        let carried = Carried::new(socket, ([0, 0, 0, 0], 0).into(), true);
        let target = ([198, 51, 100, 1], 7001).into();
        accepted(&carried, b"first", target);
        observed.recv().await.unwrap();
        accepted(&carried, b"queued", target);
        results.send(Err(failure)).unwrap();
        let receive_error = carried.recv_from(&mut [0; 1]).await.unwrap_err();
        assert_eq!(receive_error.kind(), io::ErrorKind::BrokenPipe);
        for _ in 0..2 {
            let error = carried.send_to(b"later", target).await.unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
            assert_eq!(error.raw_os_error(), None);
        }
        assert!(
            observed.try_recv().is_err(),
            "queued packets cannot bypass a failed carrier"
        );
        carried.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn carried_utp_deferred_target_error_fails_only_that_packet() {
    for failure in [
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "the tunnel has no address in that destination's family",
        ),
        io::Error::new(io::ErrorKind::AddrNotAvailable, "unaddressable"),
        io::Error::new(io::ErrorKind::HostUnreachable, "unreachable"),
        io::Error::new(io::ErrorKind::NetworkUnreachable, "unreachable"),
    ] {
        let (socket, mut observed, results) = controlled(Some(1280));
        let carried = Carried::new(socket, ([0, 0, 0, 0], 0).into(), true);
        let refused = "[2001:db8::1]:7001".parse().unwrap();
        let other = ([198, 51, 100, 2], 7002).into();
        accepted(&carried, b"refused", refused);
        assert_eq!(
            observed.recv().await.unwrap(),
            (b"refused".to_vec(), refused)
        );
        accepted(&carried, b"queued", other);
        results.send(Err(failure)).unwrap();
        assert_eq!(observed.recv().await.unwrap(), (b"queued".to_vec(), other));
        results.send(Ok(())).unwrap();
        assert_eq!(carried.send_to(b"later", other).await.unwrap(), 5);
        assert_eq!(observed.recv().await.unwrap(), (b"later".to_vec(), other));
        {
            let pending = carried.sends.as_ref().unwrap().pending.lock().unwrap();
            assert!(!pending.stopped);
            assert_eq!(pending.count, 1);
            assert_eq!(pending.bytes, 5);
        }
        carried.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn carried_utp_shutdown_is_joined_repeatable_and_releases_retained_socket_resources() {
    let (socket, mut observed, _results) = controlled(Some(1280));
    let weak = Arc::downgrade(&socket);
    let carried = Carried::new(socket, ([0, 0, 0, 0], 0).into(), true);
    accepted(&carried, b"blocked", ([198, 51, 100, 1], 7001).into());
    observed.recv().await.unwrap();
    let mut canceled = carried.shutdown();
    let mut survivor = carried.shutdown();
    let canceled_task = Arc::new(Woken::default());
    let canceled_task_weak = Arc::downgrade(&canceled_task);
    let canceled_wake = Waker::from(Arc::clone(&canceled_task));
    let survivor_wake = Waker::from(Arc::new(Woken::default()));
    assert!(
        canceled
            .as_mut()
            .poll(&mut Context::from_waker(&canceled_wake))
            .is_pending()
    );
    assert!(
        survivor
            .as_mut()
            .poll(&mut Context::from_waker(&survivor_wake))
            .is_pending()
    );
    drop(canceled);
    drop(canceled_wake);
    drop(canceled_task);
    assert!(canceled_task_weak.upgrade().is_none());
    survivor.await.unwrap();
    carried.shutdown().await.unwrap();
    assert!(weak.upgrade().is_none());
    assert!(carried.socket.lock().unwrap().is_none());
    assert!(carried.worker.lock().unwrap().task.is_none());
}

#[tokio::test]
async fn carried_utp_canceled_senders_unregister_while_live_senders_still_progress() {
    let (socket, mut observed, results) = controlled(None);
    let carried = Carried::new(socket, ([0, 0, 0, 0], 0).into(), true);
    let target = ([198, 51, 100, 1], 7001).into();
    accepted(&carried, b"held", target);
    observed.recv().await.unwrap();
    for _ in 1..CARRIED_SEND_PACKETS {
        accepted(&carried, b"queued", target);
    }
    let waiters = || {
        carried
            .sends
            .as_ref()
            .unwrap()
            .pending
            .lock()
            .unwrap()
            .waiters
            .len()
    };
    let mut one = carried.send_handle().unwrap();
    let mut two = carried.send_handle().unwrap();
    let one_wake = Arc::new(Woken::default());
    let two_wake = Arc::new(Woken::default());
    assert!(polled(&mut one, b"one", target, &one_wake).is_pending());
    assert!(polled(&mut two, b"two", target, &two_wake).is_pending());
    assert_eq!(waiters(), 2);

    for _ in 0..1024 {
        let mut canceled = carried.send_handle().unwrap();
        let previous_task = Arc::new(Woken::default());
        let previous_weak = Arc::downgrade(&previous_task);
        assert!(polled(&mut canceled, b"canceled", target, &previous_task).is_pending());
        drop(previous_task);
        let replacement_task = Arc::new(Woken::default());
        let replacement_weak = Arc::downgrade(&replacement_task);
        assert!(polled(&mut canceled, b"canceled", target, &replacement_task).is_pending());
        assert!(
            previous_weak.upgrade().is_none(),
            "repoll replaces the owned registration's waker"
        );
        assert_eq!(waiters(), 3);
        drop(canceled);
        drop(replacement_task);
        assert!(replacement_weak.upgrade().is_none());
        assert_eq!(waiters(), 2);

        let canceled_task = Arc::new(Woken::default());
        let canceled_weak = Arc::downgrade(&canceled_task);
        let waker = Waker::from(Arc::clone(&canceled_task));
        let mut canceled = carried.send_to(b"canceled async send", target);
        assert!(
            canceled
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        assert_eq!(waiters(), 3);
        drop(canceled);
        drop(waker);
        drop(canceled_task);
        assert!(canceled_weak.upgrade().is_none());
        assert_eq!(waiters(), 2);
    }

    let waker = Waker::from(Arc::new(Woken::default()));
    let raw = carried.poll_send_to(&mut Context::from_waker(&waker), b"raw", target);
    assert!(matches!(raw, Poll::Ready(Err(error)) if error.kind() == io::ErrorKind::Unsupported));
    assert_eq!(waiters(), 2);
    assert!(
        observed.try_recv().is_err(),
        "the same carrier send remains blocked throughout churn"
    );

    results.send(Ok(())).unwrap();
    observed.recv().await.unwrap();
    results.send(Ok(())).unwrap();
    observed.recv().await.unwrap();
    assert!(one_wake.0.load(Ordering::SeqCst) > 0);
    assert!(two_wake.0.load(Ordering::SeqCst) > 0);
    assert!(matches!(
        polled(&mut one, b"one", target, &one_wake),
        Poll::Ready(Ok(3))
    ));
    assert!(matches!(
        polled(&mut two, b"two", target, &two_wake),
        Poll::Ready(Ok(3))
    ));
    assert_eq!(waiters(), 0);
    carried.shutdown().await.unwrap();
}
