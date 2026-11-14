// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// DNS over HTTPS (RFC 8484) as HTTP/1.1, written and read on the TLS
// stream the stage dialled: one POST per query. Every part of a reply is
// read under a fixed cap, so nothing a server sends makes a read or an
// allocation unbounded.
use super::{DnsServer, Failed, wire::REPLY_CAP};
use std::net::SocketAddr;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

// RFC 8484 §6.
const DNS_MESSAGE: &str = "application/dns-message";
// The whole of one response head, and the headers it may carry.
const HEAD_CAP: usize = 8192;
const HEADERS: usize = 32;
// Interim (1xx) responses passed over before the final one.
const INTERIM: usize = 4;
// A chunk-size line, extension included, and the hex digits of the size.
const CHUNK_LINE_CAP: usize = 256;
const CHUNK_DIGITS: usize = 16;
// The trailer section of a chunked body, read and dropped.
const TRAILER_CAP: usize = 1024;
// Bytes asked of the stream by each read.
const READ: usize = 4096;

// One HTTP/1.1 connection, with what was read past the last thing taken.
pub(super) struct Connection<S> {
    stream: S,
    buffer: Vec<u8>,
}

// How a response body is delimited.
enum Framing {
    Length(usize),
    Chunked,
}

// What a response head says that matters here.
struct Head {
    status: u16,
    // None when the body cannot be delimited safely: both length headers,
    // neither, a coding other than chunked, or a length past the cap.
    framing: Option<Framing>,
    // The content type is a DNS message.
    typed: bool,
    // The server closes the connection after this response.
    close: bool,
}

impl<S: AsyncRead + AsyncWrite + Unpin> Connection<S> {
    pub(super) fn new(stream: S) -> Self {
        Self {
            stream,
            buffer: Vec::new(),
        }
    }

    // The reply to `packet`, and whether the connection may carry the next
    // query: the response was read whole and did not say it closes.
    pub(super) async fn exchange(
        &mut self,
        server: &DnsServer,
        path: &str,
        packet: &[u8],
    ) -> Result<(Vec<u8>, bool), Failed> {
        let request = request(server.address, path, packet)?;
        self.stream
            .write_all(&request)
            .await
            .map_err(|_| Failed::Closed)?;
        self.stream.flush().await.map_err(|_| Failed::Closed)?;
        let head = self.final_head().await?;
        if head.status != 200 {
            return Err(Failed::Http(head.status));
        }
        if !head.typed {
            return Err(Failed::Invalid);
        }
        let body = match head.framing.ok_or(Failed::Invalid)? {
            Framing::Length(length) => self.take(length).await?,
            Framing::Chunked => self.chunked().await?,
        };
        // Bytes past the response would be read as the next one's.
        Ok((body, !head.close && self.buffer.is_empty()))
    }

    // Reads more of the stream into the buffer. A closed stream is a
    // connection closed before the whole reply came.
    async fn fill(&mut self) -> Result<(), Failed> {
        let start = self.buffer.len();
        self.buffer.resize(start + READ, 0);
        match self.stream.read(&mut self.buffer[start..]).await {
            Ok(read) if read > 0 => {
                self.buffer.truncate(start + read);
                Ok(())
            }
            _ => {
                self.buffer.truncate(start);
                Err(Failed::Closed)
            }
        }
    }

    // The head of the final response, after at most `INTERIM` interim ones.
    async fn final_head(&mut self) -> Result<Head, Failed> {
        for _ in 0..=INTERIM {
            let head = self.head().await?;
            if !(100..200).contains(&head.status) {
                return Ok(head);
            }
        }
        Err(Failed::Invalid)
    }

    async fn head(&mut self) -> Result<Head, Failed> {
        loop {
            let window = &self.buffer[..self.buffer.len().min(HEAD_CAP)];
            let mut headers = [httparse::EMPTY_HEADER; HEADERS];
            let mut response = httparse::Response::new(&mut headers);
            match response.parse(window) {
                Ok(httparse::Status::Complete(size)) => {
                    let head = judge(&response)?;
                    self.buffer.drain(..size);
                    return Ok(head);
                }
                Ok(httparse::Status::Partial) if self.buffer.len() < HEAD_CAP => {}
                // Past the cap, too many headers, or not a response head.
                _ => return Err(Failed::Invalid),
            }
            self.fill().await?;
        }
    }

    // Exactly `length` bytes, which the caller has checked against a cap.
    async fn take(&mut self, length: usize) -> Result<Vec<u8>, Failed> {
        while self.buffer.len() < length {
            self.fill().await?;
        }
        let mut taken = Vec::with_capacity(length);
        taken.extend(self.buffer.drain(..length));
        Ok(taken)
    }

    // One line, without its CRLF, no longer than `cap` with it.
    async fn line(&mut self, cap: usize) -> Result<Vec<u8>, Failed> {
        loop {
            let window = &self.buffer[..self.buffer.len().min(cap)];
            if let Some(at) = window.windows(2).position(|pair| pair == b"\r\n") {
                let line = self.buffer[..at].to_vec();
                self.buffer.drain(..at + 2);
                return Ok(line);
            }
            if self.buffer.len() >= cap {
                return Err(Failed::Invalid);
            }
            self.fill().await?;
        }
    }

    // A chunked body, no larger in all than the reply cap. The trailer
    // section is read and dropped.
    async fn chunked(&mut self) -> Result<Vec<u8>, Failed> {
        let mut body = Vec::new();
        loop {
            let line = self.line(CHUNK_LINE_CAP).await?;
            let size = chunk_size(&line).ok_or(Failed::Invalid)?;
            if size == 0 {
                break;
            }
            if size > REPLY_CAP - body.len() {
                return Err(Failed::Invalid);
            }
            body.extend(self.take(size).await?);
            if !self.line(2).await?.is_empty() {
                return Err(Failed::Invalid);
            }
        }
        let mut left = TRAILER_CAP;
        loop {
            let line = self.line(left).await?;
            left -= line.len() + 2;
            if line.is_empty() {
                return Ok(body);
            }
        }
    }
}

// What a final or interim response head says.
fn judge(response: &httparse::Response<'_, '_>) -> Result<Head, Failed> {
    let status = response.code.ok_or(Failed::Invalid)?;
    // An HTTP/1.0 response does not keep the connection.
    let mut close = response.version != Some(1);
    let mut lengths = Vec::new();
    let mut codings = Vec::new();
    let mut types = Vec::new();
    for header in response.headers.iter() {
        let Ok(value) = std::str::from_utf8(header.value) else {
            continue;
        };
        let name = header.name;
        if name.eq_ignore_ascii_case("content-length") {
            lengths.push(value);
        } else if name.eq_ignore_ascii_case("transfer-encoding") {
            codings.push(value);
        } else if name.eq_ignore_ascii_case("content-type") {
            types.push(value);
        } else if name.eq_ignore_ascii_case("connection")
            && value
                .split(',')
                .any(|token| token.trim().eq_ignore_ascii_case("close"))
        {
            close = true;
        }
    }
    let framing = match (lengths.as_slice(), codings.as_slice()) {
        ([length], []) => length_of(length).map(Framing::Length),
        ([], [coding]) if coding.trim().eq_ignore_ascii_case("chunked") => Some(Framing::Chunked),
        _ => None,
    };
    // The media type, without its parameters, compared without regard to case.
    let typed = matches!(types.as_slice(), [content] if content
        .split(';')
        .next()
        .is_some_and(|media| media.trim().eq_ignore_ascii_case(DNS_MESSAGE)));
    Ok(Head {
        status,
        framing,
        typed,
        close,
    })
}

// A content length within the reply cap.
fn length_of(value: &str) -> Option<usize> {
    let value = value.trim();
    if value.is_empty() || value.len() > 5 || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    value.parse().ok().filter(|length| *length <= REPLY_CAP)
}

// The size a chunk-size line gives, its extension ignored.
fn chunk_size(line: &[u8]) -> Option<usize> {
    let digits = line.split(|byte| *byte == b';').next()?;
    let digits = std::str::from_utf8(digits)
        .ok()?
        .trim_end_matches([' ', '\t']);
    if digits.is_empty()
        || digits.len() > CHUNK_DIGITS
        || !digits.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return None;
    }
    usize::try_from(u64::from_str_radix(digits, 16).ok()?).ok()
}

// The server's address as an HTTP authority: an IPv6 address in brackets,
// and the port only when it is not 443.
fn authority(address: SocketAddr) -> String {
    let host = match address {
        SocketAddr::V4(address) => address.ip().to_string(),
        SocketAddr::V6(address) => format!("[{}]", address.ip()),
    };
    match address.port() {
        443 => host,
        port => format!("{host}:{port}"),
    }
}

// The request for one query: these four headers and the query, nothing
// else.
fn request(address: SocketAddr, path: &str, packet: &[u8]) -> Result<Vec<u8>, Failed> {
    if !path.starts_with('/') || !path.bytes().all(|byte| byte.is_ascii_graphic()) {
        return Err(Failed::Invalid);
    }
    let mut request = format!(
        "POST {path} HTTP/1.1\r\nhost: {}\r\ncontent-type: {DNS_MESSAGE}\r\naccept: {DNS_MESSAGE}\r\ncontent-length: {}\r\n\r\n",
        authority(address),
        packet.len()
    )
    .into_bytes();
    request.extend_from_slice(packet);
    Ok(request)
}
