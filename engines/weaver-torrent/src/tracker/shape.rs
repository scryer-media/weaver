// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! How a torrent's announces are written: one row of plain values per
//! client a torrent may go as. The code that writes an announce reads the
//! row it is handed and never asks which client the row is.

use std::fmt::Write;

use anyhow::Context;
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, percent_encode};
use reqwest::header::{self, HeaderMap, HeaderValue};

use crate::EncryptionPolicy;
use crate::tracker::tracker_comms_http::{Family, TrackerRequest};

/// How a torrent's announces and scrapes are written.
#[derive(Debug, PartialEq, Eq)]
pub struct AnnounceShape {
    /// The arguments of an announce's query, in order.
    pub(crate) fields: &'static [Field],
    /// How a query value is escaped.
    pub(crate) escape: Escape,
    /// Where the tracker URL's own query goes beside the announce's.
    pub(crate) own_query: OwnQuery,
    /// The headers a GET to a tracker sets, in order.
    pub(crate) headers: &'static [Header],
    /// Whether header names go out with each word capitalised.
    pub(crate) title_case: bool,
    /// The `port` announced by a session that accepts no connections.
    pub(crate) no_listener_port: u16,
    /// Whether a torrent that has every piece it wants and is not a seed
    /// says `paused` to a tracker not yet told `started`, in place of it.
    pub(crate) paused_first: bool,
}

/// One argument of an announce's query.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Field {
    InfoHash,
    PeerId,
    /// `event`, when the announce carries one.
    Event,
    Port,
    Uploaded,
    Downloaded,
    Left,
    Corrupt,
    Redundant,
    /// `compact`: 1 whatever the request asks when `always`, else the
    /// request's own value.
    Compact {
        always: bool,
    },
    /// `no_peer_id`, as [`Field::Compact`].
    NoPeerId {
        always: bool,
    },
    Numwant,
    /// `supportcrypto=1` under a policy that encrypts, then
    /// `requirecrypto=1` under one that only encrypts when `require`.
    Crypto {
        require: bool,
    },
    /// `key`, when the announce carries one.
    Key(KeyForm),
    /// `trackerid`, when the tracker gave one.
    TrackerId,
    /// The session's own public addresses: every IPv4 one as `ipv4`, then
    /// every IPv6 one as `ipv6`.
    OwnAddrs,
}

/// How the `key` is written.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum KeyForm {
    Decimal,
    /// Eight upper-case hex digits.
    Hex8,
}

/// How a query value is escaped.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Escape {
    /// RFC 3986's unreserved characters as they are, every other byte as
    /// `%` and two upper-case hex digits.
    Unreserved,
    /// Alphanumerics and `-_!.~*()` as they are, every other byte as `%`
    /// and two lower-case hex digits.
    LowerHex,
}

/// Where the tracker URL's own query goes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OwnQuery {
    /// After the announce's arguments.
    Last,
    /// Before them.
    First,
}

/// One header a GET to a tracker sets.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Header {
    /// The host as the URL names it, its port left out when it is the
    /// scheme's own.
    Host,
    UserAgent,
    /// `Accept-Encoding: gzip`.
    AcceptGzip,
    /// `Connection: close`.
    ConnectionClose,
}

// Every byte but RFC 3986's unreserved characters is escaped.
const QUERY_VALUE: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

impl AnnounceShape {
    /// Weaver's own announces.
    pub const WEAVER: Self = Self {
        fields: &[
            Field::InfoHash,
            Field::PeerId,
            Field::Event,
            Field::Port,
            Field::Uploaded,
            Field::Downloaded,
            Field::Left,
            Field::Corrupt,
            Field::Redundant,
            Field::Compact { always: false },
            Field::NoPeerId { always: false },
            Field::Numwant,
            Field::Crypto { require: true },
            Field::Key(KeyForm::Decimal),
            Field::TrackerId,
        ],
        escape: Escape::Unreserved,
        own_query: OwnQuery::Last,
        headers: &[Header::UserAgent],
        title_case: false,
        no_listener_port: 0,
        paused_first: false,
    };

    /// `bytes` escaped as a query value, onto `s`.
    pub(crate) fn escape(&self, bytes: &[u8], s: &mut String) {
        match self.escape {
            Escape::Unreserved => s.extend(percent_encode(bytes, QUERY_VALUE)),
            Escape::LowerHex => {
                for &b in bytes {
                    if b.is_ascii_alphanumeric() || b"-_!.~*()".contains(&b) {
                        s.push(char::from(b));
                    } else {
                        write!(s, "%{b:02x}").unwrap();
                    }
                }
            }
        }
    }

    /// The query of `request`.
    pub(crate) fn query(&self, request: &TrackerRequest<'_>) -> String {
        let mut s = String::with_capacity(320);
        for field in self.fields {
            self.write_field(*field, request, &mut s);
        }
        s
    }

    fn write_field(&self, field: Field, r: &TrackerRequest<'_>, s: &mut String) {
        match field {
            Field::InfoHash => {
                arg(s, "info_hash");
                self.escape(&r.info_hash.0, s);
            }
            Field::PeerId => {
                arg(s, "peer_id");
                self.escape(&r.peer_id.0, s);
            }
            Field::Event => {
                if let Some(event) = r.event {
                    arg(s, "event");
                    s.push_str(event.as_str());
                }
            }
            Field::Port => number(s, "port", r.port),
            Field::Uploaded => number(s, "uploaded", r.uploaded),
            Field::Downloaded => number(s, "downloaded", r.downloaded),
            Field::Left => number(s, "left", r.left),
            Field::Corrupt => number(s, "corrupt", r.corrupt),
            Field::Redundant => number(s, "redundant", r.redundant),
            Field::Compact { always } => number(s, "compact", u8::from(always || r.compact)),
            Field::NoPeerId { always } => number(s, "no_peer_id", u8::from(always || r.no_peer_id)),
            Field::Numwant => number(s, "numwant", r.numwant),
            Field::Crypto { require } => {
                if r.encryption != EncryptionPolicy::Disabled {
                    number(s, "supportcrypto", 1);
                }
                if require && r.encryption == EncryptionPolicy::Require {
                    number(s, "requirecrypto", 1);
                }
            }
            Field::Key(form) => {
                if let Some(key) = r.key {
                    arg(s, "key");
                    match form {
                        KeyForm::Decimal => write!(s, "{key}").unwrap(),
                        KeyForm::Hex8 => write!(s, "{key:08X}").unwrap(),
                    }
                }
            }
            Field::TrackerId => {
                if let Some(trackerid) = r.trackerid {
                    arg(s, "trackerid");
                    self.escape(trackerid, s);
                }
            }
            Field::OwnAddrs => {
                for (family, name) in [(Family::V4, "ipv4"), (Family::V6, "ipv6")] {
                    for ip in r.own_addrs.iter().filter(|ip| Family::of(**ip) == family) {
                        arg(s, name);
                        self.escape(ip.to_string().as_bytes(), s);
                    }
                }
            }
        }
    }

    /// The query of a scrape for `info_hash`.
    pub(crate) fn scrape_query(&self, info_hash: &crate::core::hash_id::Id20) -> String {
        let mut s = String::from("info_hash=");
        self.escape(&info_hash.0, &mut s);
        s
    }

    /// `query` set on `url` beside the URL's own query `own`.
    pub(crate) fn place_query(&self, url: &mut url::Url, own: Option<&str>, query: &str) {
        match (own, self.own_query) {
            (Some(own), OwnQuery::Last) => url.set_query(Some(&format!("{query}&{own}"))),
            (Some(own), OwnQuery::First) => url.set_query(Some(&format!("{own}&{query}"))),
            (None, _) => url.set_query(Some(query)),
        }
    }

    /// The headers a GET of `url` sets, in order.
    pub(crate) fn headers(&self, url: &url::Url, user_agent: &str) -> anyhow::Result<HeaderMap> {
        let mut headers = HeaderMap::with_capacity(self.headers.len());
        for h in self.headers {
            match h {
                Header::Host => {
                    let mut host = url
                        .host_str()
                        .context("the tracker URL has no host")?
                        .to_owned();
                    if let Some(port) = url.port() {
                        write!(host, ":{port}").unwrap();
                    }
                    headers.insert(header::HOST, HeaderValue::from_str(&host)?);
                }
                Header::UserAgent => {
                    headers.insert(header::USER_AGENT, HeaderValue::from_str(user_agent)?);
                }
                Header::AcceptGzip => {
                    headers.insert(header::ACCEPT_ENCODING, HeaderValue::from_static("gzip"));
                }
                Header::ConnectionClose => {
                    headers.insert(header::CONNECTION, HeaderValue::from_static("close"));
                }
            }
        }
        Ok(headers)
    }
}

/// `&name=`, or `name=` first.
fn arg(s: &mut String, name: &str) {
    if !s.is_empty() {
        s.push('&');
    }
    s.push_str(name);
    s.push('=');
}

fn number(s: &mut String, name: &str, n: impl std::fmt::Display) {
    arg(s, name);
    write!(s, "{n}").unwrap();
}
