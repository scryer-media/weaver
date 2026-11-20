// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use weaver_graph::{ByteRange, Provenance};

const NODE: NodeId = NodeId(3);

fn record(file: Option<&str>) -> KeptRecord {
    KeptRecord {
        node: NODE,
        envelope: 0,
        runs: vec![Run {
            range: ByteRange::new(0, 12),
            provenance: Provenance::Fresh,
        }],
        closed: Some(12),
        settled: true,
        file: file.map(str::to_owned),
    }
}

#[test]
fn a_missing_recorded_envelope_does_not_adopt_a_neighbor_or_advertise_bytes() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join(".weaver-chunks")).unwrap();
    std::fs::write(
        dir.path().join(".weaver-chunks/graph-envelope-3-0-1"),
        b"unrecorded",
    )
    .unwrap();
    let mut envelopes = Envelopes::new(dir.path().to_path_buf());
    envelopes.restore(&record(None)).unwrap();
    assert!(!envelopes.shelf(NODE).has_bytes());
    assert!(envelopes.records(NODE)[0].runs.is_empty());
    envelopes
        .keep(NODE, 0, 0, b"refetched-ok", Provenance::Fresh)
        .unwrap();
    let mut bytes = Vec::new();
    assert!(envelopes.read(NODE, 0, ByteRange::new(0, 12), &mut bytes));
    assert_eq!(bytes, b"refetched-ok");
    assert_eq!(
        std::fs::read(dir.path().join(".weaver-chunks/graph-envelope-3-0-1")).unwrap(),
        b"unrecorded"
    );
}

#[test]
fn a_truncated_recorded_envelope_keeps_only_its_surviving_ranges() {
    let dir = tempfile::tempdir().unwrap();
    let file = "recorded-volume";
    std::fs::write(dir.path().join(file), b"first").unwrap();
    let mut envelopes = Envelopes::new(dir.path().to_path_buf());
    envelopes.restore(&record(Some(file))).unwrap();
    let kept = envelopes.records(NODE);
    assert_eq!(kept[0].file.as_deref(), Some(file));
    assert_eq!(kept[0].runs[0].range, ByteRange::new(0, 5));
    assert!(envelopes.shelf(NODE).has_bytes());
    let mut bytes = Vec::new();
    assert!(!envelopes.read(NODE, 0, ByteRange::new(0, 12), &mut bytes));
    envelopes
        .keep(NODE, 0, 5, b"-suffix", Provenance::Fresh)
        .unwrap();
    assert!(envelopes.read(NODE, 0, ByteRange::new(0, 12), &mut bytes));
    assert_eq!(bytes, b"first-suffix");
}

#[test]
fn a_recorded_envelope_at_another_name_preserves_its_owned_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let file = "recorded-volume";
    std::fs::write(dir.path().join(file), b"recorded-ok!").unwrap();
    std::fs::create_dir_all(dir.path().join(".weaver-chunks")).unwrap();
    std::fs::write(dir.path().join(envelope_name(NODE, 0)), b"wrong-bytes!").unwrap();
    let mut envelopes = Envelopes::new(dir.path().to_path_buf());
    envelopes.restore(&record(Some(file))).unwrap();
    let mut bytes = Vec::new();
    assert!(envelopes.read(NODE, 0, ByteRange::new(0, 12), &mut bytes));
    assert_eq!(bytes, b"recorded-ok!");
    assert_eq!(envelopes.records(NODE), vec![record(Some(file))]);
}
