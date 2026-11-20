// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::{BTreeMap, BTreeSet};
use weaver_model::files::FileRole;

// A post can describe one complete numbered stream without saying that it
// was cut from an archive. Judge the visible sequence, not an unposted
// physical volume known only to the fixture's author.
pub(super) fn joined_posting<'a>(
    parts: impl IntoIterator<Item = (&'a str, &'a [u8])>,
) -> Option<(String, Vec<u8>)> {
    let mut base = None;
    let mut pieces = BTreeMap::new();
    for (name, bytes) in parts {
        let FileRole::SplitFile { number } = FileRole::from_filename(name) else {
            return None;
        };
        let (stem, _) = name.rsplit_once('.')?;
        if base.is_some_and(|base| base != stem) || pieces.insert(number, bytes).is_some() {
            return None;
        }
        base = Some(stem);
    }
    if pieces.keys().copied().ne(0..pieces.len() as u32) {
        return None;
    }
    Some((
        base?.to_string(),
        pieces
            .values()
            .flat_map(|bytes| bytes.iter().copied())
            .collect(),
    ))
}

// The file a numbered piece was cut from, as the job joins it: the piece's
// name with its last extension taken off when that extension is all digits.
// A partial job may publish it beside the members its unpack delivered.
pub(super) fn joined_container(name: &str) -> Option<&str> {
    let (stem, number) = name.rsplit_once('.')?;
    (!stem.is_empty() && !number.is_empty() && number.bytes().all(|byte| byte.is_ascii_digit()))
        .then_some(stem)
}

pub(super) fn signatureless_join(name: &str, bytes: &[u8]) -> bool {
    matches!(
        FileRole::from_filename(name),
        FileRole::Standalone | FileRole::Unknown
    ) && !matches!(
        weaver_graph::signed(bytes, Some(bytes.len() as u64)),
        weaver_graph::Sniff::Object(_)
    )
}

#[test]
fn numbered_posting_requires_one_contiguous_unambiguous_sequence() {
    assert_eq!(
        joined_posting([("opaque.001", b"ab".as_slice())]),
        Some(("opaque".into(), b"ab".to_vec()))
    );
    assert_eq!(
        joined_posting([
            ("opaque.002", b"cd".as_slice()),
            ("opaque.001", b"ab".as_slice())
        ]),
        Some(("opaque".into(), b"abcd".to_vec()))
    );
    for names in [
        ["opaque.002", "opaque.003"],
        ["opaque.001", "opaque.003"],
        ["opaque.001", "other.002"],
        ["opaque.001", "opaque.001"],
    ] {
        assert!(joined_posting(names.map(|name| (name, b"ab".as_slice()))).is_none());
    }
    assert_eq!(joined_container("Bundle.7z.001"), Some("Bundle.7z"));
    assert_eq!(joined_container("opaque.10"), Some("opaque"));
    for name in [
        "bundle.7z",
        "bundle.part01.rar",
        "bundle.r00",
        ".001",
        "bundle.",
    ] {
        assert_eq!(joined_container(name), None, "{name}");
    }
    assert!(!signatureless_join("bundle.7z", b"ab"));
    assert!(!signatureless_join("opaque", b"Rar!\x1a\x07\x00"));
    assert!(signatureless_join("opaque", b"ab"));
}

// Recognition does not prove piece order or that every piece was posted.
pub(super) fn names_show_archive<'a>(names: impl IntoIterator<Item = &'a str>) -> bool {
    names.into_iter().any(|name| {
        matches!(
            FileRole::from_filename(name),
            FileRole::RarVolume { .. }
                | FileRole::SevenZipArchive
                | FileRole::SevenZipSplit { .. }
                | FileRole::SplitFile { .. }
        )
    })
}

// The reference extraction supplies a lower bound. Whole outputs are proven
// byte-exact; a Partial job may retain damaged siblings beside those outputs.
pub(super) fn disagreements(
    required: &BTreeSet<String>,
    whole: &BTreeSet<String>,
    present: &BTreeSet<String>,
) -> Vec<String> {
    let mut out = Vec::new();
    if !required.is_subset(whole) {
        out.push(format!("delivered whole {whole:?}, required {required:?}"));
    }
    if required.is_empty() && whole.is_empty() && !present.is_empty() {
        out.push(format!("nothing usable, yet {present:?} is published"));
    }
    out
}

#[test]
fn byte_exact_recovery_may_exceed_the_reference_reader() {
    let whole = BTreeSet::from(["first.bin".into(), "second.bin".into()]);
    for required in [BTreeSet::new(), BTreeSet::from(["first.bin".into()])] {
        assert!(disagreements(&required, &whole, &whole).is_empty());
    }
}

#[test]
fn missing_reference_members_and_damaged_publications_are_rejected() {
    let one = BTreeSet::from(["first.bin".into()]);
    let none = BTreeSet::new();
    assert!(!disagreements(&one, &none, &none).is_empty());
    assert!(!disagreements(&none, &none, &one).is_empty());
}

#[test]
fn a_partial_job_may_retain_a_damaged_sibling_beside_usable_output() {
    let whole = BTreeSet::from(["first.bin".into()]);
    let present = BTreeSet::from(["first.bin".into(), "second.bin".into()]);
    assert!(disagreements(&whole, &whole, &present).is_empty());
}

#[test]
fn visible_piece_names_are_not_plain_payload_even_when_they_do_not_order_a_set() {
    for names in [
        ["bundle.7z", "bundle.7z.002"],
        ["8e6152c40a9d4d68b73122d5d3ecf690", "bundle.7z.002"],
        [
            "8e6152c40a9d4d68b73122d5d3ecf690.001",
            "8e6152c40a9d4d68b73122d5d3ecf690.003",
        ],
        [
            "8e6152c40a9d4d68b73122d5d3ecf690.002",
            "8e6152c40a9d4d68b73122d5d3ecf690.001",
        ],
    ] {
        assert!(names_show_archive(names));
    }
    assert!(!names_show_archive([
        "8e6152c40a9d4d68b73122d5d3ecf690",
        "ordinary.bin"
    ]));
}

// Failure diagnostics compare the bytes already collected by the driver.
// Acceptance and the successful path do not need another read or hash.
pub(super) fn output_difference(name: &str, expected: &[u8], actual: &[u8]) -> Option<String> {
    let difference = match expected.iter().zip(actual).position(|(a, b)| a != b) {
        Some(at) => format!("first differing offset {at}"),
        None if expected.len() != actual.len() => {
            format!(
                "matching prefix length {}",
                expected.len().min(actual.len())
            )
        }
        None => return None,
    };
    Some(format!(
        "output {name:?}: expected length {}, actual length {}, {difference}",
        expected.len(),
        actual.len()
    ))
}

#[test]
fn output_difference_reports_the_first_wrong_byte_without_dumping_payloads() {
    assert_eq!(
        output_difference("entry.bin", b"abcdef", b"abcxef").as_deref(),
        Some("output \"entry.bin\": expected length 6, actual length 6, first differing offset 3")
    );
    assert!(output_difference("entry.bin", b"abcdef", b"abcdef").is_none());
}

#[test]
fn output_difference_reports_the_matching_prefix_for_short_or_long_output() {
    assert_eq!(
        output_difference("entry.bin", b"abcdef", b"abc").as_deref(),
        Some("output \"entry.bin\": expected length 6, actual length 3, matching prefix length 3")
    );
    assert_eq!(
        output_difference("entry.bin", b"abc", b"abcdef").as_deref(),
        Some("output \"entry.bin\": expected length 3, actual length 6, matching prefix length 3")
    );
}
