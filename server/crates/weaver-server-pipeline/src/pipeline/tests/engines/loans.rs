// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Work lent out of the loop: a zip's keys over the ciphertext it holds, and
// the key a RAR or 7z store derives from the job's password. Each row posts
// one enciphered member, flat or inside a stored zip, serves it in one order,
// and wants it delivered whole. A ZipCrypto member whose sizes wait in a data
// descriptor is placed only as far as its keys reach, so its ciphertext comes
// down one article per pass and each pass makes the zip due again: the job
// keeps lending, or doing the work itself while a checkpoint is owed, until
// nothing is held, and never rules the member short while work is due.

use std::io::{Cursor, Write};

use ::zip::unstable::write::FileOptionsExt;
use ::zip::write::SimpleFileOptions;
use ::zip::{AesMode, ZipWriter};
use sevenz_turbo::EncoderMethod;

use super::harness::payload;
use super::zip::stored;
use crate::pipeline::tests::direct_store::scenarios::sevenz_store::{
    Entry, build_7z_shaped, split_volumes,
};
use crate::pipeline::tests::direct_store::scenarios::{
    HeaderCheck, encrypted_rar4_store_set, encrypted_store_set, header_encrypted_store_set,
};

pub const SECRET: &str = "lantern-loan-secret";
pub const MEMBER: &str = "feature.mkv";
// Small articles, so a member spans dozens and its ciphertext comes down in
// as many passes.
pub const FRAME: usize = 1024;
pub const BODY: usize = 40 * FRAME + 77;

#[derive(Clone, Copy)]
pub enum Kind {
    // ZipCrypto, sizes in a data descriptor: zero in the local header.
    ZipCryptoDescriptor,
    // ZipCrypto, sizes in the local header.
    ZipCryptoSized,
    ZipAes,
    SevenZipAes,
    Rar5,
    Rar5Sealed,
    Rar4,
}

pub fn body() -> Vec<u8> {
    payload(BODY, 61)
}

fn zip_of(options: SimpleFileOptions, name: &str, bytes: &[u8]) -> Vec<u8> {
    let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
    writer.start_file(name, options).unwrap();
    writer.write_all(bytes).unwrap();
    writer.finish().unwrap().into_inner()
}

// One stored entry: the outer zip a nested row posts.
pub fn stored_zip(name: &str, bytes: &[u8]) -> Vec<u8> {
    zip_of(stored(), name, bytes)
}

// A writer that cannot seek back writes the sizes after the data, in a
// descriptor, and zeroes them in the local header.
pub fn zipcrypto_descriptor(bytes: &[u8]) -> Vec<u8> {
    let options = stored()
        .with_deprecated_encryption(SECRET.as_bytes())
        .unwrap();
    let mut writer = ZipWriter::new_stream(Vec::new());
    writer.start_file(MEMBER, options).unwrap();
    writer.write_all(bytes).unwrap();
    let archive = writer.finish().unwrap().into_inner();
    let flags = u16::from_le_bytes(archive[6..8].try_into().unwrap());
    let sizes = &archive[18..26];
    assert!(
        flags & 0x9 == 0x9,
        "encrypted, sizes in a descriptor: {flags:#x}"
    );
    assert!(sizes.iter().all(|byte| *byte == 0), "zero local sizes");
    archive
}

// The member's posts, as `kind` writes it: one archive, or a set's volumes.
pub fn volumes(kind: Kind, bytes: &[u8], parts: usize) -> Vec<(String, Vec<u8>)> {
    match kind {
        Kind::ZipCryptoDescriptor => vec![("lantern.zip".into(), zipcrypto_descriptor(bytes))],
        Kind::ZipCryptoSized => {
            let options = stored()
                .with_deprecated_encryption(SECRET.as_bytes())
                .unwrap();
            vec![("lantern.zip".into(), zip_of(options, MEMBER, bytes))]
        }
        Kind::ZipAes => {
            let options = stored().with_aes_encryption(AesMode::Aes256, SECRET);
            vec![("lantern.zip".into(), zip_of(options, MEMBER, bytes))]
        }
        Kind::SevenZipAes => {
            let archive = build_7z_shaped(
                &[Entry::file(MEMBER, bytes.to_vec())],
                EncoderMethod::COPY,
                Some(SECRET),
                false,
            );
            split_volumes(&archive, parts)
        }
        Kind::Rar5 => encrypted_store_set(MEMBER, bytes, parts, SECRET, Some(SECRET), false),
        Kind::Rar5Sealed => {
            header_encrypted_store_set(MEMBER, bytes, parts, SECRET, HeaderCheck::For(SECRET))
        }
        Kind::Rar4 => encrypted_rar4_store_set(MEMBER, bytes, parts, SECRET, None),
    }
}
