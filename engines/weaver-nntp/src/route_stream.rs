//! Socket-compatible access to a direct TCP connection or an in-process route.
use std::{
    io::{self, Read, Write},
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use weaver_tunnel::direct::DirectStream;

pub enum RouteStream {
    Tcp(tokio::net::TcpStream),
    Tunnel {
        stream: DirectStream,
        peeked: Option<Option<u8>>,
    },
}
impl From<tokio::net::TcpStream> for RouteStream {
    fn from(stream: tokio::net::TcpStream) -> Self {
        Self::Tcp(stream)
    }
}
impl From<DirectStream> for RouteStream {
    fn from(stream: DirectStream) -> Self {
        Self::Tunnel {
            stream,
            peeked: None,
        }
    }
}
impl RouteStream {
    pub(crate) async fn readable(&mut self) -> io::Result<()> {
        match self {
            Self::Tcp(stream) => stream.readable().await,
            Self::Tunnel { stream, peeked } => {
                if peeked.is_none() {
                    let mut byte = [0];
                    *peeked = Some(if stream.read(&mut byte).await? == 0 {
                        None
                    } else {
                        Some(byte[0])
                    });
                }
                Ok(())
            }
        }
    }
    pub(crate) fn try_read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        if let Self::Tcp(stream) = self {
            return stream.try_read(bytes);
        }
        let mut cx = Context::from_waker(std::task::Waker::noop());
        let mut buf = ReadBuf::new(bytes);
        match Pin::new(self).poll_read(&mut cx, &mut buf) {
            Poll::Ready(result) => result.map(|()| buf.filled().len()),
            Poll::Pending => Err(io::ErrorKind::WouldBlock.into()),
        }
    }
}
impl AsyncRead for RouteStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Tcp(stream) => Pin::new(stream).poll_read(cx, buf),
            Self::Tunnel { stream, peeked } => {
                if buf.remaining() == 0 {
                    return Poll::Ready(Ok(()));
                }
                if let Some(byte) = peeked.take() {
                    if let Some(byte) = byte {
                        buf.put_slice(&[byte]);
                    }
                    return Poll::Ready(Ok(()));
                }
                Pin::new(stream).poll_read(cx, buf)
            }
        }
    }
}
impl AsyncWrite for RouteStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Tcp(stream) => Pin::new(stream).poll_write(cx, buf),
            Self::Tunnel { stream, .. } => Pin::new(stream).poll_write(cx, buf),
        }
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Tcp(stream) => Pin::new(stream).poll_flush(cx),
            Self::Tunnel { stream, .. } => Pin::new(stream).poll_flush(cx),
        }
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Tcp(stream) => Pin::new(stream).poll_shutdown(cx),
            Self::Tunnel { stream, .. } => Pin::new(stream).poll_shutdown(cx),
        }
    }
}

pub(crate) enum BlockingSocket {
    Tcp(std::net::TcpStream),
    Tunnel {
        stream: DirectStream,
        runtime: tokio::runtime::Handle,
        read_timeout: std::cell::Cell<Option<Duration>>,
        write_timeout: std::cell::Cell<Option<Duration>>,
    },
}
impl From<std::net::TcpStream> for BlockingSocket {
    fn from(stream: std::net::TcpStream) -> Self {
        Self::Tcp(stream)
    }
}
impl BlockingSocket {
    pub(crate) fn tunnel(
        stream: DirectStream,
        runtime: tokio::runtime::Handle,
        timeout: Duration,
    ) -> Self {
        Self::Tunnel {
            stream,
            runtime,
            read_timeout: std::cell::Cell::new(Some(timeout)),
            write_timeout: std::cell::Cell::new(Some(timeout)),
        }
    }
    pub(crate) fn tcp(&self) -> Option<&std::net::TcpStream> {
        match self {
            Self::Tcp(tcp) => Some(tcp),
            _ => None,
        }
    }
    pub(crate) fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        match self {
            Self::Tcp(tcp) => tcp.set_read_timeout(timeout),
            Self::Tunnel { read_timeout, .. } => {
                read_timeout.set(timeout);
                Ok(())
            }
        }
    }
    pub(crate) fn set_write_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        match self {
            Self::Tcp(tcp) => tcp.set_write_timeout(timeout),
            Self::Tunnel { write_timeout, .. } => {
                write_timeout.set(timeout);
                Ok(())
            }
        }
    }
    pub(crate) fn set_nonblocking(&self, enabled: bool) -> io::Result<()> {
        match self {
            Self::Tcp(tcp) => tcp.set_nonblocking(enabled),
            Self::Tunnel { .. } if !enabled => Ok(()),
            _ => Err(io::Error::other("tunnel adapter requires blocking calls")),
        }
    }
}
async fn deadline<T>(
    timeout: Option<Duration>,
    future: impl std::future::Future<Output = io::Result<T>>,
) -> io::Result<T> {
    match timeout {
        Some(timeout) => tokio::time::timeout(timeout, future)
            .await
            .map_err(|_| io::Error::from(io::ErrorKind::TimedOut))?,
        None => future.await,
    }
}
impl Read for BlockingSocket {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Tcp(tcp) => tcp.read(buf),
            Self::Tunnel {
                stream,
                runtime,
                read_timeout,
                ..
            } => runtime.block_on(deadline(read_timeout.get(), stream.read(buf))),
        }
    }
}
impl Write for BlockingSocket {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Self::Tcp(tcp) => tcp.write(buf),
            Self::Tunnel {
                stream,
                runtime,
                write_timeout,
                ..
            } => runtime.block_on(deadline(write_timeout.get(), stream.write(buf))),
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Tcp(tcp) => tcp.flush(),
            Self::Tunnel {
                stream,
                runtime,
                write_timeout,
                ..
            } => runtime.block_on(deadline(write_timeout.get(), stream.flush())),
        }
    }
}
