// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// DNS over HTTPS (RFC 8484), one POST per query, on the TLS stream the stage
// dialled: HTTP/2 when the server picks it in the handshake, else HTTP/1.1.
// hyper's client connection reads and writes the messages; it opens nothing
// and resolves nothing. Its connection, and the HTTP/2 work it hands to an
// executor, are polled here inside each exchange and never spawned, so they
// do no work outside a lookup and go when the lookup drops them. Every part
// of a reply is read under a fixed cap.
use super::{
    DnsServer, Failed,
    wire::{REPLY_CAP, Tls},
};
use hyper::{
    Method, Request, Response, StatusCode, Uri, Version,
    body::{Body, Bytes, Frame, Incoming, SizeHint},
    client::conn::{http1, http2},
    header::{
        ACCEPT, CONNECTION, CONTENT_LENGTH, CONTENT_TYPE, HOST, HeaderMap, TRANSFER_ENCODING,
    },
};
use std::convert::Infallible;
use std::future::{Future, poll_fn};
use std::net::SocketAddr;
use std::pin::{Pin, pin};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll, ready};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

// RFC 8484 §6.
const DNS_MESSAGE: &str = "application/dns-message";
// The whole of one response head, and the headers it may carry. The head cap
// also bounds a chunked body's trailer section, and an HTTP/2 header list.
const HEAD_CAP: usize = 8192;
const HEADERS: usize = 32;
// Interim (1xx) responses passed over before the final one.
const INTERIM: usize = 4;
// Bytes asked of the stream by each read.
const READ: usize = 4096;
// HTTP/2 flow control: a stream may hold one reply before it is read, and
// the connection the protocol's initial window.
const STREAM_WINDOW: u32 = REPLY_CAP as u32;
const CONNECTION_WINDOW: u32 = 65_535;
// Bytes one HTTP/2 exchange may read: the reply, its head, the interim
// heads allowed over HTTP/1.1, and a read's worth of framing. hyper does not
// pass HTTP/2 interim responses on to be counted, so this bounds them, and
// any other frames the server sends meanwhile.
const EXCHANGE_READ: usize = REPLY_CAP + HEAD_CAP * (INTERIM + 1) + READ;

// One connection to the server. `read` counts the bytes the server sent, so
// a failed HTTP/1.1 exchange can tell a connection found dead from a reply
// cut short.
pub(super) struct Connection {
    http: Http,
    read: Arc<AtomicUsize>,
}

// The connection futures hyper hands back, kept until they end.
type Driver1 = Pin<Box<http1::Connection<Io<Tls>, Query>>>;
type Driver2 = Pin<Box<http2::Connection<Io<Tls>, Query, Tasks>>>;

enum Http {
    One {
        send: http1::SendRequest<Query>,
        driver: Option<Driver1>,
    },
    Two {
        send: http2::SendRequest<Query>,
        driver: Option<Driver2>,
        tasks: Tasks,
        running: Vec<Task>,
    },
}

impl Connection {
    pub(super) async fn new(stream: Tls) -> Result<Self, Failed> {
        let two = stream.get_ref().1.alpn_protocol() == Some(b"h2");
        let read = Arc::new(AtomicUsize::new(0));
        let io = Io {
            stream,
            read: read.clone(),
        };
        let http = if two {
            let tasks = Tasks::default();
            // No pings or timers of its own, no push, fixed small windows.
            let (send, driver) = http2::Builder::new(tasks.clone())
                .initial_stream_window_size(STREAM_WINDOW)
                .initial_connection_window_size(CONNECTION_WINDOW)
                .max_header_list_size(HEAD_CAP as u32)
                .max_send_buf_size(REPLY_CAP)
                .handshake(io)
                .await
                .map_err(|_| Failed::Closed)?;
            Http::Two {
                send,
                driver: Some(Box::pin(driver)),
                tasks,
                running: Vec::new(),
            }
        } else {
            let (send, driver) = http1::Builder::new()
                .max_buf_size(HEAD_CAP)
                .max_header_size(HEAD_CAP)
                .max_headers(HEADERS)
                .handshake(io)
                .await
                .map_err(|_| Failed::Closed)?;
            Http::One {
                send,
                driver: Some(Box::pin(driver)),
            }
        };
        Ok(Self { http, read })
    }

    // The reply to `packet`, and whether the connection may carry the next
    // query: the response was read whole, the connection still runs, and
    // the server did not say it closes.
    pub(super) async fn exchange(
        &mut self,
        server: &DnsServer,
        path: &str,
        packet: &[u8],
    ) -> Result<(Vec<u8>, bool), Failed> {
        let two = matches!(self.http, Http::Two { .. });
        let mut request = request(server.address, path, packet, two)?;
        let interim = Arc::new(AtomicUsize::new(0));
        let counted = interim.clone();
        hyper::ext::on_informational(&mut request, move |_| {
            counted.fetch_add(1, Ordering::Relaxed);
        });
        let interims = || interim.load(Ordering::Relaxed) > INTERIM;
        match &mut self.http {
            Http::One { send, driver } => {
                let before = self.read.load(Ordering::Relaxed);
                let outcome = {
                    let mut call = pin!(call(
                        async {
                            send.ready().await.map_err(failed)?;
                            send.send_request(request).await.map_err(failed)
                        },
                        false,
                    ));
                    poll_fn(|cx| {
                        // The connection runs only while a query waits on
                        // it. Once it ends it is dropped, which fails a call
                        // still waiting on it.
                        if let Some(running) = driver.as_mut()
                            && running.as_mut().poll(cx).is_ready()
                        {
                            *driver = None;
                        }
                        if interims() {
                            return Poll::Ready(Err(Failed::Invalid));
                        }
                        call.as_mut().poll(cx)
                    })
                    .await
                };
                match outcome {
                    // Nothing came back: the connection was dead before the
                    // query.
                    Err(Failed::Cut) if self.read.load(Ordering::Relaxed) == before => {
                        Err(Failed::Closed)
                    }
                    Ok((reply, open)) => Ok((reply, open && driver.is_some())),
                    outcome => outcome,
                }
            }
            Http::Two {
                send,
                driver,
                tasks,
                running,
            } => {
                let before = self.read.load(Ordering::Relaxed);
                let outcome = {
                    let mut call = pin!(call(
                        async {
                            send.ready()
                                .await
                                .map_err(|error| failed_two(&error, false))?;
                            send.send_request(request)
                                .await
                                .map_err(|error| failed_two(&error, false))
                        },
                        true,
                    ));
                    poll_fn(|cx| {
                        if let Some(connection) = driver.as_mut()
                            && connection.as_mut().poll(cx).is_ready()
                        {
                            *driver = None;
                        }
                        tasks.run(running, cx);
                        if self.read.load(Ordering::Relaxed) - before > EXCHANGE_READ {
                            return Poll::Ready(Err(Failed::Invalid));
                        }
                        call.as_mut().poll(cx)
                    })
                    .await
                };
                outcome.map(|(reply, open)| (reply, open && driver.is_some() && !send.is_closed()))
            }
        }
    }

    // Whether several queries can share the connection at once: HTTP/2
    // streams, on the one connection, with no new one opened.
    pub(super) fn multiplexes(&self) -> bool {
        matches!(self.http, Http::Two { .. })
    }

    // The replies to `packets`, each on a stream of its own over this HTTP/2
    // connection, all in flight together, and whether the connection may
    // carry the next query. Each stream is read under the cap one exchange
    // has.
    pub(super) async fn exchange_all(
        &mut self,
        server: &DnsServer,
        path: &str,
        packets: &[Vec<u8>],
    ) -> (Vec<Result<Vec<u8>, Failed>>, bool) {
        let Http::Two {
            send,
            driver,
            tasks,
            running,
        } = &mut self.http
        else {
            unreachable!("only an HTTP/2 connection carries queries together")
        };
        type Call = Pin<Box<dyn Future<Output = Result<(Vec<u8>, bool), Failed>> + Send>>;
        let mut replies: Vec<Option<Result<Vec<u8>, Failed>>> = Vec::with_capacity(packets.len());
        let mut calls: Vec<Option<Call>> = Vec::with_capacity(packets.len());
        for packet in packets {
            match request(server.address, path, packet, true) {
                Ok(request) => {
                    let mut send = send.clone();
                    calls.push(Some(Box::pin(call(
                        async move {
                            send.ready()
                                .await
                                .map_err(|error| failed_two(&error, false))?;
                            send.send_request(request)
                                .await
                                .map_err(|error| failed_two(&error, false))
                        },
                        true,
                    ))));
                    replies.push(None);
                }
                Err(failed) => {
                    calls.push(None);
                    replies.push(Some(Err(failed)));
                }
            }
        }
        let before = self.read.load(Ordering::Relaxed);
        let bound = EXCHANGE_READ.saturating_mul(packets.len());
        poll_fn(|cx| {
            if let Some(connection) = driver.as_mut()
                && connection.as_mut().poll(cx).is_ready()
            {
                *driver = None;
            }
            tasks.run(running, cx);
            let over = self.read.load(Ordering::Relaxed) - before > bound;
            for (slot, reply) in calls.iter_mut().zip(replies.iter_mut()) {
                let Some(call) = slot else { continue };
                if over {
                    *reply = Some(Err(Failed::Invalid));
                } else if let Poll::Ready(outcome) = call.as_mut().poll(cx) {
                    *reply = Some(outcome.map(|(reply, _)| reply));
                } else {
                    continue;
                }
                *slot = None;
            }
            if calls.iter().all(Option::is_none) {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
        let open = driver.is_some() && !send.is_closed();
        let replies = replies
            .into_iter()
            .map(|reply| reply.unwrap_or(Err(Failed::Invalid)))
            .collect();
        (replies, open)
    }
}

// Reads the response `sent` resolves to.
async fn call(
    sent: impl Future<Output = Result<Response<Incoming>, Failed>>,
    two: bool,
) -> Result<(Vec<u8>, bool), Failed> {
    let response = sent.await?;
    if response.status() != StatusCode::OK {
        return Err(Failed::Http(response.status().as_u16()));
    }
    let length = judge(response.headers(), two)?;
    let close = !two && (response.version() != Version::HTTP_11 || closes(response.headers()));
    let mut body = response.into_body();
    let mut reply = Vec::with_capacity(length.unwrap_or(0));
    while let Some(frame) = poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await {
        let frame = frame.map_err(|error| {
            if two {
                failed_two(&error, true)
            } else {
                failed(error)
            }
        })?;
        let Ok(data) = frame.into_data() else {
            continue;
        };
        if data.len() > REPLY_CAP - reply.len() {
            return Err(Failed::Invalid);
        }
        reply.extend_from_slice(&data);
    }
    Ok((reply, !close))
}

// What a hyper error says of the exchange: a response that does not parse is
// invalid, anything else a reply cut short.
fn failed(error: hyper::Error) -> Failed {
    if error.is_parse() {
        Failed::Invalid
    } else {
        Failed::Cut
    }
}

// What a hyper error says of an HTTP/2 exchange, by the h2 error beneath
// it; `begun` once the response head came. The connection gone (closed,
// broken, or sent away by the server) leaves the query unanswered before
// the head and the reply cut short after it. The server resetting the stream
// says nothing of the connection. A response this side refused, such as a
// header list over the cap or a protocol error on the stream, is invalid.
fn failed_two(error: &hyper::Error, begun: bool) -> Failed {
    let gone = if begun { Failed::Cut } else { Failed::Closed };
    if error.is_parse() {
        return Failed::Invalid;
    }
    // hyper passes an h2 IO error on as the IO error itself.
    let cause = std::error::Error::source(error);
    if error.is_closed()
        || error.is_canceled()
        || cause.is_some_and(|cause| cause.is::<std::io::Error>())
    {
        return gone;
    }
    match cause.and_then(|cause| cause.downcast_ref::<h2::Error>()) {
        Some(h2) if h2.is_io() || (h2.is_go_away() && h2.is_remote()) => gone,
        Some(h2) if h2.is_reset() && h2.is_remote() => Failed::Reset,
        _ => Failed::Invalid,
    }
}

// The headers that frame and type a response, and its length when it gives
// one. Their values must be visible ASCII, the content type a DNS message,
// and the body delimited by a single length within the cap, by chunked alone
// over HTTP/1.1, or by the end of the stream over HTTP/2.
fn judge(headers: &HeaderMap, two: bool) -> Result<Option<usize>, Failed> {
    for name in [CONTENT_LENGTH, TRANSFER_ENCODING, CONNECTION, CONTENT_TYPE] {
        if headers.get_all(&name).iter().any(|value| {
            !value
                .as_bytes()
                .iter()
                .all(|byte| byte.is_ascii_graphic() || matches!(byte, b' ' | b'\t'))
        }) {
            return Err(Failed::Invalid);
        }
    }
    let text = |name| -> Vec<&str> {
        headers
            .get_all(name)
            .iter()
            .filter_map(|value| value.to_str().ok())
            .collect()
    };
    let types = text(CONTENT_TYPE);
    // The media type, without its parameters, compared without regard to case.
    let typed = matches!(types.as_slice(), [content] if content
        .split(';')
        .next()
        .is_some_and(|media| media.trim().eq_ignore_ascii_case(DNS_MESSAGE)));
    if !typed {
        return Err(Failed::Invalid);
    }
    match (
        text(CONTENT_LENGTH).as_slice(),
        text(TRANSFER_ENCODING).as_slice(),
    ) {
        ([length], []) => length_of(length).map(Some).ok_or(Failed::Invalid),
        ([], []) if two => Ok(None),
        ([], [coding]) if !two && coding.trim().eq_ignore_ascii_case("chunked") => Ok(None),
        _ => Err(Failed::Invalid),
    }
}

// Whether the response says the server closes the connection after it.
fn closes(headers: &HeaderMap) -> bool {
    headers.get_all(CONNECTION).iter().any(|value| {
        value.to_str().is_ok_and(|value| {
            value
                .split(',')
                .any(|token| token.trim().eq_ignore_ascii_case("close"))
        })
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

// The path a request may be sent to: it starts with '/' and holds only
// visible ASCII, so no space, control byte or line break reaches the
// request line.
fn target(path: &str) -> Result<Uri, Failed> {
    if !path.starts_with('/') || !path.bytes().all(|byte| byte.is_ascii_graphic()) {
        return Err(Failed::Invalid);
    }
    Uri::try_from(path).map_err(|_| Failed::Invalid)
}

// The request for one query: these four headers and the query, nothing
// else. HTTP/2 carries the host as the request's authority instead.
fn request(
    address: SocketAddr,
    path: &str,
    packet: &[u8],
    two: bool,
) -> Result<Request<Query>, Failed> {
    let path = target(path)?;
    let host = authority(address);
    let builder = if two {
        let uri = Uri::builder()
            .scheme("https")
            .authority(host)
            .path_and_query(path.path_and_query().map_or("/", |path| path.as_str()))
            .build()
            .map_err(|_| Failed::Invalid)?;
        Request::builder().uri(uri)
    } else {
        Request::builder().uri(path).header(HOST, host)
    };
    builder
        .method(Method::POST)
        .header(CONTENT_TYPE, DNS_MESSAGE)
        .header(ACCEPT, DNS_MESSAGE)
        .header(CONTENT_LENGTH, packet.len())
        .body(Query(Some(Bytes::copy_from_slice(packet))))
        .map_err(|_| Failed::Invalid)
}

// A query as a request body: one frame.
pub(super) struct Query(Option<Bytes>);

impl Body for Query {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        Poll::Ready(self.0.take().map(|packet| Ok(Frame::data(packet))))
    }
    fn is_end_stream(&self) -> bool {
        self.0.is_none()
    }
    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(self.0.as_ref().map_or(0, |packet| packet.len() as u64))
    }
}

// The HTTP/2 connection and its streams, which hyper hands to an executor.
// They are kept here, not spawned, and polled with the exchange that waits
// on them, so they run only inside a lookup and are dropped with it.
type Task = Pin<Box<dyn Future<Output = ()> + Send>>;

#[derive(Clone, Default)]
struct Tasks(Arc<Mutex<Vec<Task>>>);

impl<F: Future<Output = ()> + Send + 'static> hyper::rt::Executor<F> for Tasks {
    fn execute(&self, future: F) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(Box::pin(future));
    }
}

impl Tasks {
    // Polls each task, then those handed over meanwhile, until no new one
    // comes.
    fn run(&self, running: &mut Vec<Task>, cx: &mut Context<'_>) {
        running.retain_mut(|task| task.as_mut().poll(cx).is_pending());
        loop {
            let fresh = std::mem::take(&mut *self.0.lock().unwrap_or_else(PoisonError::into_inner));
            if fresh.is_empty() {
                return;
            }
            for mut task in fresh {
                if task.as_mut().poll(cx).is_pending() {
                    running.push(task);
                }
            }
        }
    }
}

// The TLS stream as hyper's IO, counting what it reads.
struct Io<S> {
    stream: S,
    read: Arc<AtomicUsize>,
}

impl<S: AsyncRead + Unpin> hyper::rt::Read for Io<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut buf: hyper::rt::ReadBufCursor<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        let mut scratch = [0; READ];
        let room = buf.remaining().min(READ);
        let mut read = ReadBuf::new(&mut scratch[..room]);
        ready!(Pin::new(&mut this.stream).poll_read(cx, &mut read))?;
        this.read.fetch_add(read.filled().len(), Ordering::Relaxed);
        buf.put_slice(read.filled());
        Poll::Ready(Ok(()))
    }
}

impl<S: AsyncWrite + Unpin> hyper::rt::Write for Io<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.get_mut().stream).poll_write(cx, buf)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_path_with_a_space_a_line_break_or_other_than_ascii_is_refused() {
        assert!(target("/dns-query").is_ok());
        for path in [
            "/dns query",
            "/dns\rquery",
            "/dns\nquery",
            "/dns\r\nhost: x",
            "/dns\tquery",
            "/dnsé",
            "dns-query",
            "",
        ] {
            assert!(matches!(target(path), Err(Failed::Invalid)), "{path:?}");
            for two in [false, true] {
                assert!(
                    request("192.0.2.53:443".parse().unwrap(), path, &[0; 12], two).is_err(),
                    "{path:?}"
                );
            }
        }
    }

    #[test]
    fn an_http_2_request_names_the_server_as_its_authority() {
        let Ok(request) = request("192.0.2.53:8443".parse().unwrap(), "/q?a=b", &[0; 12], true)
        else {
            panic!("a request");
        };
        assert_eq!(request.uri(), "https://192.0.2.53:8443/q?a=b");
        assert!(request.headers().get(HOST).is_none());
    }
}
