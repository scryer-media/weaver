// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! The bytes that cross a peer's socket each way, counted below any
//! encryption, so the handshakes and every message count as the payload
//! does (libtorrent `peer_connection::on_receive_data` and `on_send_data`
//! account every byte the socket moved).

use std::{
    io::IoSliceMut,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll},
};

use tokio::io::{AsyncRead, AsyncWrite};

use crate::{
    type_aliases::{BoxAsyncReadVectored, BoxAsyncWrite},
    vectored_traits::AsyncReadVectored,
};

/// What has crossed one connection's socket, each way.
#[derive(Clone, Default)]
pub(crate) struct Meter {
    received: Arc<AtomicU64>,
    sent: Arc<AtomicU64>,
}

impl Meter {
    /// The halves of a connection just made, metered.
    pub fn wrap(
        reader: BoxAsyncReadVectored,
        writer: BoxAsyncWrite,
    ) -> (Self, BoxAsyncReadVectored, BoxAsyncWrite) {
        let meter = Self::default();
        let reader = Box::new(Metered {
            inner: reader,
            count: meter.received.clone(),
        });
        let writer = Box::new(Metered {
            inner: writer,
            count: meter.sent.clone(),
        });
        (meter, reader, writer)
    }

    pub fn received(&self) -> u64 {
        self.received.load(Ordering::Relaxed)
    }

    pub fn sent(&self) -> u64 {
        self.sent.load(Ordering::Relaxed)
    }
}

struct Metered<T> {
    inner: T,
    count: Arc<AtomicU64>,
}

impl<T> Metered<T> {
    fn add(&self, n: usize) {
        self.count.fetch_add(n as u64, Ordering::Relaxed);
    }
}

impl AsyncRead for Metered<BoxAsyncReadVectored> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        let before = buf.filled().len();
        std::task::ready!(Pin::new(&mut this.inner).poll_read(cx, buf))?;
        this.add(buf.filled().len() - before);
        Poll::Ready(Ok(()))
    }
}

impl AsyncReadVectored for Metered<BoxAsyncReadVectored> {
    fn poll_read_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        vec: &mut [IoSliceMut<'_>],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        let n = std::task::ready!(Pin::new(&mut this.inner).poll_read_vectored(cx, vec))?;
        this.add(n);
        Poll::Ready(Ok(n))
    }
}

impl AsyncWrite for Metered<BoxAsyncWrite> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        let n = std::task::ready!(Pin::new(&mut this.inner).poll_write(cx, buf))?;
        this.add(n);
        Poll::Ready(Ok(n))
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        let n = std::task::ready!(Pin::new(&mut this.inner).poll_write_vectored(cx, bufs))?;
        this.add(n);
        Poll::Ready(Ok(n))
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::Meter;
    use crate::vectored_traits::AsyncReadVectoredIntoCompat;

    // Every byte read or written through the halves is counted, each way.
    #[tokio::test(start_paused = true)]
    async fn the_meter_counts_what_crosses_each_way() {
        let (ours, theirs) = tokio::io::duplex(64);
        let (r, w) = tokio::io::split(ours);
        let (meter, mut r, mut w) = Meter::wrap(Box::new(r.into_vectored_compat()), Box::new(w));
        let (mut tr, mut tw) = tokio::io::split(theirs);
        w.write_all(b"hello").await.unwrap();
        let mut got = [0u8; 5];
        tr.read_exact(&mut got).await.unwrap();
        tw.write_all(b"abc").await.unwrap();
        let mut back = [0u8; 3];
        r.read_exact(&mut back).await.unwrap();
        assert_eq!((meter.sent(), meter.received()), (5, 3));
    }
}
