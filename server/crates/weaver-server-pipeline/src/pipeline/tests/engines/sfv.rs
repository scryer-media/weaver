// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::harness::{Claim, Outcome, Post};

// A listing: a comment, then `name XXXXXXXX` per entry.
pub fn listing(name: &str, entries: &[(&str, u32)]) -> Post {
    let mut text = String::from("; generated listing\n");
    for (listed, crc) in entries {
        text.push_str(&format!("{listed} {crc:08X}\n"));
    }
    Post::plain(name, text.into_bytes())
}

// The messages of every listing note the run sent for the job, in order.
pub fn notes(graph: &Outcome) -> Vec<String> {
    graph
        .events
        .iter()
        .filter_map(|event| match event {
            weaver_server_base::events::model::PipelineEvent::SfvMismatch {
                job_id,
                file,
                listing,
            } if *job_id == super::state::JOB => Some(
                weaver_server_base::events::model::sfv_mismatch_message(file, listing),
            ),
            _ => None,
        })
        .collect()
}

pub fn crc(bytes: &[u8]) -> u32 {
    par2_rs::checksum::crc32(bytes)
}

// A listing as a checksum tool writes it, under any name.
pub fn listing_text(header: &str, entries: &[(&str, u32)]) -> Vec<u8> {
    let mut text = String::from(header);
    for (listed, crc) in entries {
        text.push_str(&format!("{listed} {crc:08x}\n"));
    }
    text.into_bytes()
}

// What a run delivered and claimed of the post's own files, by base name,
// leaving out the listings: what a twin without them must match.
type Posted = (Vec<(String, Vec<u8>)>, Vec<(String, Claim)>);

fn posted(graph: &Outcome) -> Posted {
    let base = |path: &str| path.rsplit('/').next().unwrap_or(path).to_owned();
    let listing = |name: &str| name.to_ascii_lowercase().ends_with(".sfv");
    let mut files: Vec<_> = graph
        .files
        .iter()
        .map(|(path, bytes)| (base(path), bytes.clone()))
        .filter(|(name, _)| !listing(name))
        .collect();
    files.sort();
    let claims = graph
        .graph()
        .claims
        .iter()
        .filter(|(name, _)| !listing(name))
        .map(|(name, claim)| (name.clone(), *claim))
        .collect();
    (files, claims)
}

// A listing never claims damage: a run with it ends as the same posts
// without it do, in status, failure kind and text, delivered files and
// claims.
pub fn assert_twin(graph: &Outcome, twin: &Outcome) {
    assert_twin_but_text(graph, twin);
    assert_eq!(graph.failure, twin.failure);
}

// As `assert_twin`, but for the failure's text, where the listing's own
// bytes are counted in the figures it gives.
pub fn assert_twin_but_text(graph: &Outcome, twin: &Outcome) {
    // A failed status carries the failure's text, compared by the caller.
    let shape = |outcome: &Outcome| match &outcome.status {
        Some(crate::jobs::model::JobStatus::Failed { .. }) => "Failed".to_owned(),
        status => format!("{status:?}"),
    };
    assert_eq!(
        (shape(graph), graph.failure_kind),
        (shape(twin), twin.failure_kind),
        "with: {:?} | without: {:?}",
        graph.failure,
        twin.failure
    );
    assert_eq!(posted(graph), posted(twin));
}
