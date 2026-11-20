// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::harness::ARTICLE;
use crate::pipeline::tests::direct_store::multi_member_store_set;

use sevenz_turbo::{ArchiveEntry, ArchiveWriter, EncoderConfiguration, EncoderMethod};
use std::io::Cursor;

// A stored 7z of `entries`, in order.
pub fn stored(entries: &[(&str, &[u8])]) -> Vec<u8> {
    let mut writer = ArchiveWriter::new(Cursor::new(Vec::new())).unwrap();
    writer.set_content_methods(vec![EncoderConfiguration::new(EncoderMethod::COPY)]);
    for (name, bytes) in entries {
        let mut entry = ArchiveEntry::new_file(name);
        entry.size = bytes.len() as u64;
        writer
            .push_archive_entry(entry, Some(Cursor::new(bytes.to_vec())))
            .unwrap();
    }
    writer.finish().unwrap().into_inner()
}

pub fn delivered(name: &str) -> String {
    format!("lantern beacon/{name}")
}

// A stored RAR5 volume of `entries`, in order.
pub(super) fn rar(entries: &[(&str, Vec<u8>)]) -> Vec<u8> {
    multi_member_store_set(entries, 1).remove(0).1
}

pub(super) fn sevenz(entries: &[(&str, Vec<u8>)]) -> Vec<u8> {
    let borrowed: Vec<(&str, &[u8])> = entries.iter().map(|(n, b)| (*n, b.as_slice())).collect();
    stored(&borrowed)
}

// `entries` in a stored RAR, `level.rar`, in a stored 7z posted as
// `lantern.7z`: what they hold sits two deep.
pub fn two_deep(entries: &[(&str, Vec<u8>)]) -> Vec<u8> {
    sevenz(&[("level.rar", rar(entries))])
}

// Every article of `outer` lying wholly inside the bytes of `inner`, past
// its front, so what the member is can still be told from its first bytes.
pub fn articles_inside(outer: &[u8], inner: &[u8]) -> Vec<u32> {
    let at = outer
        .windows(inner.len())
        .position(|window| window == inner)
        .expect("a stored container holds its member's bytes as they are");
    (at / ARTICLE + 1..)
        .take_while(|ordinal| (ordinal + 1) * ARTICLE <= at + inner.len())
        .map(|ordinal| ordinal as u32)
        .collect()
}
