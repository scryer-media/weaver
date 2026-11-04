// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
// Lifted from librqbit-utp 0.7.0 (Apache-2.0), Copyright (c) the librqbit authors.

use std::{net::SocketAddr, pin::Pin, task::Poll};

use tokio::io::{AsyncRead, AsyncWrite};

use crate::{stream_rx::UtpStreamReadHalf, stream_tx::UtpStreamWriteHalf};

pub struct UtpStream {
    reader: UtpStreamReadHalf,
    writer: UtpStreamWriteHalf,
    remote_addr: SocketAddr,
}

impl UtpStream {
    pub(crate) fn new(
        reader: UtpStreamReadHalf,
        writer: UtpStreamWriteHalf,
        remote_addr: SocketAddr,
    ) -> Self {
        Self {
            reader,
            writer,
            remote_addr,
        }
    }

    pub fn remote_addr(&self) -> SocketAddr {
        self.remote_addr
    }

    pub fn split(self) -> (UtpStreamReadHalf, UtpStreamWriteHalf) {
        (self.reader, self.writer)
    }

    #[cfg(test)]
    pub async fn read_all_available(&mut self) -> std::io::Result<Vec<u8>> {
        self.reader.read_all_available().await
    }
}

impl AsyncRead for UtpStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().reader).poll_read(cx, buf)
    }
}

impl AsyncWrite for UtpStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        Pin::new(&mut self.get_mut().writer).poll_write(cx, buf)
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.get_mut().writer).poll_flush(cx)
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.get_mut().writer).poll_shutdown(cx)
    }
}
