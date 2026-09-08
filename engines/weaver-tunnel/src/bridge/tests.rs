use super::*;
use crate::{
    TunnelStream,
    transport::{socks_connect, socks_request},
};
use std::sync::atomic::AtomicUsize;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::Notify,
};

#[derive(Default)]
struct Fixture {
    calls: AtomicUsize,
    started: Notify,
    resume: Notify,
    dropped: Arc<Notify>,
    stalled: bool,
}
struct DropNotice(Arc<Notify>);
impl Drop for DropNotice {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}
#[async_trait::async_trait]
impl TunnelProvider for Fixture {
    async fn dial(&self, _: &str, _: u16) -> Result<Box<dyn TunnelStream>, TunnelError> {
        let _notice = DropNotice(self.dropped.clone());
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.started.notify_one();
        if self.stalled {
            self.resume.notified().await;
        }
        let (client, mut server) = tokio::io::duplex(128);
        tokio::spawn(async move {
            let mut payload = [0; 4];
            if server.read_exact(&mut payload).await.is_ok() {
                let _ = server.write_all(&payload).await;
            }
        });
        Ok(Box::new(client))
    }
    fn describe(&self) -> String {
        "bridge fixture".into()
    }
}
fn bridge(fixture: Arc<Fixture>) -> Arc<Bridge> {
    Bridge::start(
        &tokio::runtime::Handle::current(),
        fixture,
        Duration::from_secs(5),
        ("fixture-user".into(), "secret-only-in-memory".into()),
    )
    .unwrap()
}
async fn authenticate(stream: &mut TcpStream, bridge: &Bridge) {
    stream.write_all(&[5, 1, 2]).await.unwrap();
    let mut reply = [0; 2];
    stream.read_exact(&mut reply).await.unwrap();
    assert_eq!(reply, [5, 2]);
    let (user, password) = bridge.credentials();
    let mut bytes = vec![1, user.len() as u8];
    bytes.extend_from_slice(user.as_bytes());
    bytes.push(password.len() as u8);
    bytes.extend_from_slice(password.as_bytes());
    stream.write_all(&bytes).await.unwrap();
    stream.read_exact(&mut reply).await.unwrap();
    assert_eq!(reply, [1, 0]);
}

#[tokio::test]
async fn unauthenticated_and_wrong_credentials_never_dial() {
    let fixture = Arc::new(Fixture::default());
    let bridge = bridge(fixture.clone());
    for credentials in [
        None,
        Some(("fixture-user", "wrong")),
        Some(("wrong", "secret-only-in-memory")),
    ] {
        let mut client = TcpStream::connect(bridge.addr().unwrap()).await.unwrap();
        assert!(
            socks_connect(&mut client, "private.invalid", 443, credentials)
                .await
                .is_err()
        );
    }
    assert_eq!(fixture.calls.load(Ordering::Relaxed), 0);
    assert!(!format!("{bridge:?}").contains("secret-only-in-memory"));
    let mut client = TcpStream::connect(bridge.addr().unwrap()).await.unwrap();
    socks_connect(
        &mut client,
        "private.invalid",
        443,
        Some(bridge.credentials()),
    )
    .await
    .unwrap();
    client.write_all(b"ping").await.unwrap();
    let mut payload = [0; 4];
    client.read_exact(&mut payload).await.unwrap();
    assert_eq!(&payload, b"ping");
    bridge.revoke().await;
}

#[tokio::test]
async fn caller_disconnect_cancels_pending_dial() {
    let fixture = Arc::new(Fixture {
        stalled: true,
        ..Default::default()
    });
    let bridge = bridge(fixture.clone());
    let mut client = TcpStream::connect(bridge.addr().unwrap()).await.unwrap();
    authenticate(&mut client, &bridge).await;
    client
        .write_all(&socks_request("unresolved.invalid", 119).unwrap())
        .await
        .unwrap();
    fixture.started.notified().await;
    drop(client);
    tokio::time::timeout(Duration::from_secs(1), fixture.dropped.notified())
        .await
        .unwrap();
    assert_eq!(fixture.calls.load(Ordering::Relaxed), 1);
    bridge.revoke().await;
}

#[tokio::test]
async fn dialing_preserves_pipelined_client_bytes() {
    let fixture = Arc::new(Fixture {
        stalled: true,
        ..Default::default()
    });
    let bridge = bridge(fixture.clone());
    let mut client = TcpStream::connect(bridge.addr().unwrap()).await.unwrap();
    authenticate(&mut client, &bridge).await;
    let mut request = socks_request("unresolved.invalid", 119).unwrap();
    request.extend_from_slice(b"ping");
    client.write_all(&request).await.unwrap();
    fixture.started.notified().await;
    fixture.resume.notify_one();
    let mut reply = [0; 10];
    client.read_exact(&mut reply).await.unwrap();
    assert_eq!(reply[1], 0);
    let mut payload = [0; 4];
    client.read_exact(&mut payload).await.unwrap();
    assert_eq!(&payload, b"ping");
    bridge.revoke().await;
}

#[tokio::test]
async fn direct_stream_works_without_the_listener_and_revokes_a_pending_read() {
    let bridge = bridge(Arc::new(Fixture::default()));
    let task = bridge.task.lock().unwrap().take().unwrap();
    task.abort();
    let _ = task.await;
    let (mut stream, _) = bridge.dial("unresolved.invalid", 119).await.unwrap();
    // A partial request leaves the fixture waiting for the last two bytes.
    stream.write_all(b"pi").await.unwrap();
    let reader = tokio::spawn(async move { stream.read_u8().await });
    tokio::task::yield_now().await;
    bridge.revoke().await;
    let error = tokio::time::timeout(Duration::from_secs(1), reader)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::ConnectionAborted);
    assert!(bridge.dial("unresolved.invalid", 119).await.is_err());
}

#[tokio::test]
async fn abandoning_direct_establishment_cancels_the_owned_runtime_task() {
    let fixture = Arc::new(Fixture {
        stalled: true,
        ..Default::default()
    });
    let bridge = bridge(fixture.clone());
    let dial = tokio::spawn({
        let bridge = bridge.clone();
        async move { bridge.dial("unresolved.invalid", 119).await }
    });
    fixture.started.notified().await;
    dial.abort();
    let _ = dial.await;
    tokio::time::timeout(Duration::from_secs(1), fixture.dropped.notified())
        .await
        .unwrap();
    assert_eq!(bridge.direct_slots.available_permits(), 1024);
    bridge.revoke().await;
}

#[tokio::test]
async fn direct_admission_is_bounded_before_dialing() {
    let fixture = Arc::new(Fixture::default());
    let bridge = bridge(fixture.clone());
    let _held = bridge
        .direct_slots
        .clone()
        .acquire_many_owned(1024)
        .await
        .unwrap();
    assert!(bridge.dial("unresolved.invalid", 119).await.is_err());
    assert_eq!(fixture.calls.load(Ordering::Relaxed), 0);
    bridge.revoke().await;
}

#[tokio::test]
async fn revocation_waits_for_pending_direct_dials_to_unwind() {
    let fixture = Arc::new(Fixture {
        stalled: true,
        ..Default::default()
    });
    let bridge = bridge(fixture.clone());
    let dial = tokio::spawn({
        let bridge = bridge.clone();
        async move { bridge.dial("unresolved.invalid", 119).await }
    });
    fixture.started.notified().await;
    tokio::time::timeout(Duration::from_secs(1), bridge.revoke())
        .await
        .unwrap();
    assert_eq!(bridge.direct_slots.available_permits(), 1024);
    assert!(dial.await.unwrap().is_err());
}
