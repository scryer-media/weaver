// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::harness::{ARTICLE, Post, payload};
use sevenz_turbo::encoder_options::AesEncoderOptions;
use sevenz_turbo::{ArchiveEntry, ArchiveWriter, EncoderConfiguration, Password};
use std::io::Cursor;

// The members every fixture holds, by name.
pub fn members() -> Vec<(&'static str, Vec<u8>)> {
    vec![
        ("ember.mkv", payload(5 * ARTICLE + 11, 41)),
        ("quill.mkv", payload(3 * ARTICLE - 3, 42)),
    ]
}

// One container of `members()` coded by `method`, enciphered under
// `password` when there is one.
pub fn container(method: EncoderConfiguration, password: Option<&str>) -> Vec<u8> {
    let mut writer = ArchiveWriter::new(Cursor::new(Vec::new())).unwrap();
    let mut methods = vec![method];
    if let Some(password) = password {
        // Encryption applies to coded bytes, so it sits at the output end.
        methods.insert(0, AesEncoderOptions::new(Password::new(password)).into());
    }
    writer.set_content_methods(methods);
    for (name, bytes) in members() {
        let mut entry = ArchiveEntry::new_file(name);
        entry.size = bytes.len() as u64;
        writer
            .push_archive_entry(entry, Some(Cursor::new(bytes)))
            .unwrap();
    }
    writer.finish().unwrap().into_inner()
}

// A pure byte split at a fixed size into `.7z.001` onward, the last volume
// the short tail.
pub fn split(archive: &[u8], count: usize) -> Vec<Post> {
    let chunk = archive.len().div_ceil(count);
    archive
        .chunks(chunk)
        .zip(1..)
        .map(|(bytes, number)| Post::plain(&format!("lantern.7z.{number:03}"), bytes.to_vec()))
        .collect()
}

// Every delivered file lands in the job's folder.
pub fn delivered(name: &str) -> String {
    format!("lantern beacon/{name}")
}

pub mod store {
    use super::super::harness::Outcome;
    use super::{container, delivered, members};
    use sevenz_turbo::encoder_options::AesEncoderOptions;
    use sevenz_turbo::{
        ArchiveEntry, ArchiveWriter, EncoderConfiguration, EncoderMethod, Password,
    };
    use std::io::Cursor;

    pub const SECRET: &str = "quill-ember-secret";

    pub fn stored(password: Option<&str>) -> Vec<u8> {
        container(EncoderConfiguration::new(EncoderMethod::COPY), password)
    }

    // The members, every byte as written, and nothing else.
    pub fn assert_members(outcome: &Outcome) {
        assert!(outcome.completed(), "{:?}", outcome.failure);
        let expected = members();
        assert_eq!(
            outcome.files.keys().cloned().collect::<Vec<_>>(),
            expected
                .iter()
                .map(|(name, _)| delivered(name))
                .collect::<Vec<_>>(),
            "delivered names",
        );
        for (name, bytes) in expected {
            assert!(outcome.files[&delivered(name)] == bytes, "{name} bytes",);
        }
    }

    // AES over the members, coded by `method`, and over the end header alike,
    // as 7zz writes `-mhe=on`.
    pub fn sealed_header(method: EncoderConfiguration) -> Vec<u8> {
        let mut writer = ArchiveWriter::new(Cursor::new(Vec::new())).unwrap();
        writer.set_content_methods(vec![
            AesEncoderOptions::new(Password::new(SECRET)).into(),
            method,
        ]);
        writer.set_encrypt_header(true);
        for (name, bytes) in members() {
            let mut entry = ArchiveEntry::new_file(name);
            entry.size = bytes.len() as u64;
            writer
                .push_archive_entry(entry, Some(Cursor::new(bytes)))
                .unwrap();
        }
        writer.finish().unwrap().into_inner()
    }

    // The packed streams end where the end header starts, which the signature
    // header names.
    pub fn packed_end(archive: &[u8]) -> usize {
        32 + u64::from_le_bytes(archive[12..20].try_into().unwrap()) as usize
    }

    // Articles of 60 bytes: past the 32-byte signature header the third,
    // `SENT_BACK`, starts 8 bytes into a cipher block of the folder and ends
    // inside another, and every byte of it is `ember.mkv`'s.
    pub const FRAME: usize = 60;
}
