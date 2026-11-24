// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The test torrent's source and the report of its rungs: what a route's
// test carries back from the test torrent.

use std::time::Duration;

use weaver_server_net::torrents::net::Exposure;

// Where the test torrent is found: the directory of the current Debian
// netinst torrents.
pub const DEBIAN_TORRENTS: &str = "https://cdimage.debian.org/debian-cd/current/amd64/bt-cd/";

// Where the test torrent comes from: a directory listing the torrent
// among others.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TestTorrentSource {
    pub directory: reqwest::Url,
}

impl TestTorrentSource {
    pub fn debian() -> Self {
        Self {
            directory: reqwest::Url::parse(DEBIAN_TORRENTS).expect("the Debian URL parses"),
        }
    }

    // The host and port the source is fetched from.
    pub fn target(&self) -> (String, u16) {
        (
            self.directory.host_str().unwrap_or_default().to_owned(),
            self.directory.port_or_known_default().unwrap_or(443),
        )
    }
}

impl Default for TestTorrentSource {
    fn default() -> Self {
        Self::debian()
    }
}

// One step of the test.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum P2pStep {
    // The test torrent, fetched through the path.
    Torrent,
    // An announce over the path.
    Announce,
    // Peers dialled over the path and answering the handshake.
    Handshake,
    // One piece downloaded and verified.
    Piece,
    // Peers able to connect in.
    Inbound,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum P2pOutcome {
    Passed(String),
    // What stopped it, in the words of whatever did.
    Failed(String),
    // An earlier step failed.
    NotReached,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct P2pRung {
    pub step: P2pStep,
    pub outcome: P2pOutcome,
}

// Pieces served to the peers the tracker sent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PeerUpload {
    // No peer asked for anything.
    NoDemand,
    Measured { bytes: u64, elapsed: Duration },
    // The piece to serve never arrived.
    Failed(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct P2pReport {
    pub rungs: Vec<P2pRung>,
    // The port a gateway mapped to the test session's listener.
    pub mapped_port: Option<u16>,
    // What the test session showed trackers and peers, once it opened.
    pub exposure: Option<Exposure>,
    pub upload: Option<PeerUpload>,
}

impl P2pReport {
    // A test whose path could not be opened: its first step failed.
    pub fn failed(error: String) -> Self {
        failed_at(P2pStep::Torrent, error)
    }

    pub fn passed(&self, step: P2pStep) -> bool {
        self.rungs
            .iter()
            .any(|rung| rung.step == step && matches!(rung.outcome, P2pOutcome::Passed(_)))
    }
}

pub fn failed_at(step: P2pStep, error: String) -> P2pReport {
    let steps = [
        P2pStep::Torrent,
        P2pStep::Announce,
        P2pStep::Handshake,
        P2pStep::Piece,
        P2pStep::Inbound,
    ];
    P2pReport {
        rungs: steps
            .into_iter()
            .map(|each| P2pRung {
                step: each,
                outcome: if each == step {
                    P2pOutcome::Failed(error.clone())
                } else {
                    P2pOutcome::NotReached
                },
            })
            .collect(),
        mapped_port: None,
        exposure: None,
        upload: None,
    }
}
