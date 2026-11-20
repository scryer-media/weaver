// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::{BTreeMap, BTreeSet};
use weaver_model::files::FileRole;

// A post can describe one complete numbered stream without saying that it
// was cut from an archive. Judge the visible sequence, not an unposted
// physical volume known only to the fixture's author.
pub fn joined_posting<'a>(
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
pub fn joined_container(name: &str) -> Option<&str> {
    let (stem, number) = name.rsplit_once('.')?;
    (!stem.is_empty() && !number.is_empty() && number.bytes().all(|byte| byte.is_ascii_digit()))
        .then_some(stem)
}

pub fn signatureless_join(name: &str, bytes: &[u8]) -> bool {
    matches!(
        FileRole::from_filename(name),
        FileRole::Standalone | FileRole::Unknown
    ) && !matches!(
        weaver_graph::signed(bytes, Some(bytes.len() as u64)),
        weaver_graph::Sniff::Object(_)
    )
}

// Recognition does not prove piece order or that every piece was posted.
pub fn names_show_archive<'a>(names: impl IntoIterator<Item = &'a str>) -> bool {
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
pub fn disagreements(
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

// Failure diagnostics compare the bytes already collected by the driver.
// Acceptance and the successful path do not need another read or hash.
pub fn output_difference(name: &str, expected: &[u8], actual: &[u8]) -> Option<String> {
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
