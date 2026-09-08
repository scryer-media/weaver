//! Revocable in-process streams, without a forwarding task or loopback socket.
use crate::TunnelStream;
use std::{
    io,
    pin::Pin,
    sync::{Arc, Mutex, Weak},
    task::{Context, Poll, Waker},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

#[derive(Default)]
pub(crate) struct Streams {
    state: Mutex<Registry>,
    stopped: tokio::sync::Notify,
    drained: tokio::sync::Notify,
}
#[derive(Default)]
struct Registry {
    revoked: bool,
    pending: usize,
    streams: Vec<Weak<Mutex<State>>>,
}
struct State {
    stream: Option<Box<dyn TunnelStream>>,
    reader: Option<Waker>,
    writer: Option<Waker>,
}

impl Streams {
    pub(crate) fn begin_dial(self: &Arc<Self>) -> io::Result<PendingDial> {
        let mut registry = self.state.lock().expect("direct stream registry");
        if registry.revoked {
            return Err(revoked());
        }
        registry.pending += 1;
        Ok(PendingDial(self.clone()))
    }

    pub(crate) async fn wait_drained(&self) {
        loop {
            let notified = self.drained.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.state.lock().expect("direct stream registry").pending == 0 {
                return;
            }
            notified.await;
        }
    }

    pub(crate) fn track(
        &self,
        stream: Box<dyn TunnelStream>,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> io::Result<DirectStream> {
        let mut registry = self.state.lock().expect("direct stream registry");
        if registry.revoked {
            return Err(revoked());
        }
        registry.streams.retain(|stream| stream.strong_count() > 0);
        let state = Arc::new(Mutex::new(State {
            stream: Some(stream),
            reader: None,
            writer: None,
        }));
        registry.streams.push(Arc::downgrade(&state));
        Ok(DirectStream {
            state,
            _permit: permit,
        })
    }
    pub(crate) async fn cancelled(&self) {
        let notified = self.stopped.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if self.state.lock().expect("direct stream registry").revoked {
            return;
        }
        notified.await;
    }
    pub(crate) fn revoke(&self) {
        let streams = {
            let mut registry = self.state.lock().expect("direct stream registry");
            registry.revoked = true;
            std::mem::take(&mut registry.streams)
        };
        self.stopped.notify_waiters();
        for weak in streams {
            if let Some(state) = weak.upgrade() {
                let (stream, reader, writer) = {
                    let mut state = state.lock().expect("direct stream");
                    (
                        state.stream.take(),
                        state.reader.take(),
                        state.writer.take(),
                    )
                };
                drop(stream);
                if let Some(waker) = reader {
                    waker.wake();
                }
                if let Some(waker) = writer {
                    waker.wake();
                }
            }
        }
    }
}

pub(crate) struct PendingDial(Arc<Streams>);
impl Drop for PendingDial {
    fn drop(&mut self) {
        self.0.state.lock().expect("direct stream registry").pending -= 1;
        self.0.drained.notify_waiters();
    }
}

fn revoked() -> io::Error {
    io::Error::new(io::ErrorKind::ConnectionAborted, "proxy route was revoked")
}

/// A tunnel stream whose underlying connection can be synchronously revoked.
pub struct DirectStream {
    state: Arc<Mutex<State>>,
    _permit: tokio::sync::OwnedSemaphorePermit,
}
impl AsyncRead for DirectStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let mut state = self.state.lock().expect("direct stream");
        state.reader = Some(cx.waker().clone());
        match state.stream.as_mut() {
            Some(stream) => Pin::new(stream).poll_read(cx, buf),
            None => Poll::Ready(Err(revoked())),
        }
    }
}
impl AsyncWrite for DirectStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let mut state = self.state.lock().expect("direct stream");
        state.writer = Some(cx.waker().clone());
        match state.stream.as_mut() {
            Some(stream) => Pin::new(stream).poll_write(cx, buf),
            None => Poll::Ready(Err(revoked())),
        }
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut state = self.state.lock().expect("direct stream");
        state.writer = Some(cx.waker().clone());
        match state.stream.as_mut() {
            Some(stream) => Pin::new(stream).poll_flush(cx),
            None => Poll::Ready(Err(revoked())),
        }
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut state = self.state.lock().expect("direct stream");
        state.writer = Some(cx.waker().clone());
        match state.stream.as_mut() {
            Some(stream) => Pin::new(stream).poll_shutdown(cx),
            None => Poll::Ready(Err(revoked())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::{Future, poll_fn};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn revocation_drops_the_connection_and_wakes_a_backpressured_writer() {
        let registry = Streams::default();
        let slots = Arc::new(tokio::sync::Semaphore::new(1));
        let (client, mut server) = tokio::io::duplex(1);
        let mut stream = registry
            .track(
                Box::new(client),
                slots.clone().acquire_owned().await.unwrap(),
            )
            .unwrap();
        stream.write_all(b"a").await.unwrap();
        let mut write = Box::pin(stream.write_all(b"b"));
        poll_fn(|cx| {
            assert!(write.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        registry.revoke();
        assert_eq!(
            write.await.unwrap_err().kind(),
            io::ErrorKind::ConnectionAborted
        );
        // Buffered bytes remain readable by the fixture, then the peer sees EOF.
        assert_eq!(server.read_u8().await.unwrap(), b'a');
        assert!(server.read_u8().await.is_err());
        drop(stream);
        assert_eq!(slots.available_permits(), 1);
    }
}
