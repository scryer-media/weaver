// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// One test per refusal the map takes, each over a header the writer made,
// changed where the writer cannot make the shape itself.

use std::io::Cursor;

use sevenz_turbo::{Archive, ArchiveEntry, ArchiveWriter, Password};
use weaver_graph::Why;

use super::super::map::build;
use super::{Entry, PASSWORD, aes, archive, copy, files, lzma2, write};

fn parsed(bytes: &[u8]) -> Archive {
    Archive::read(&mut Cursor::new(bytes), &Password::new(PASSWORD)).unwrap()
}

fn refused(archive: &Archive) -> Why {
    build(archive).err().expect("refused")
}

#[test]
fn stored_and_enciphered_folders_map() {
    assert!(build(&parsed(&archive(copy(), false))).is_ok());
    assert!(build(&parsed(&archive(aes(), false))).is_ok());
}

#[test]
fn a_copy_chained_to_a_copy_goes_to_an_engine() {
    let mut methods = copy();
    methods.extend(copy());
    assert_eq!(refused(&parsed(&archive(methods, false))), Why::Compressed);
}

#[test]
fn aes_over_a_compressor_is_enciphered_bytes_for_an_engine() {
    let mut methods = aes();
    methods.truncate(1);
    methods.extend(lzma2());
    assert_eq!(refused(&parsed(&archive(methods, false))), Why::Encrypted);
}

#[test]
fn coder_sizes_that_do_not_chain_are_refused() {
    // Each AES folder's coders swapped: the folder's sizes still agree, but
    // each coder's output is no longer the next one's input.
    let mut archive = parsed(&archive(aes(), false));
    for block in &mut archive.blocks {
        block.coders.swap(0, 1);
    }
    assert_eq!(refused(&archive), Why::Unsupported);
}

#[test]
fn an_enciphered_folder_of_several_members_is_refused() {
    let files = files();
    let mut writer = ArchiveWriter::new(Cursor::new(Vec::new())).unwrap();
    writer.set_content_methods(aes());
    writer
        .push_archive_entries(
            files
                .iter()
                .map(|(name, _)| ArchiveEntry::new_file(name))
                .collect(),
            files
                .iter()
                .map(|(_, bytes)| Cursor::new(bytes.clone()).into())
                .collect(),
        )
        .unwrap();
    let bytes = writer.finish().unwrap().into_inner();
    assert_eq!(refused(&parsed(&bytes)), Why::Unsupported);
}

#[test]
fn an_enciphered_member_without_a_crc_is_refused() {
    let mut archive = parsed(&archive(aes(), false));
    archive.files[1].has_crc = false;
    assert_eq!(refused(&archive), Why::Unsupported);
}

#[test]
fn an_anti_item_is_not_a_file() {
    let mut archive = parsed(&archive(copy(), false));
    archive.files[2].is_anti_item = true;
    assert_eq!(refused(&archive), Why::NotAFile);
}

#[test]
fn a_symlink_is_not_a_file() {
    let mut archive = parsed(&archive(copy(), false));
    archive.files[0].has_windows_attributes = true;
    archive.files[0].windows_attributes |= 0x400;
    assert_eq!(refused(&archive), Why::NotAFile);
}

#[test]
fn a_name_that_escapes_the_destination_is_refused() {
    let mut archive = parsed(&archive(copy(), false));
    archive.files[1].name = "../beta.bin".to_owned();
    assert_eq!(refused(&archive), Why::Unsupported);
}

#[test]
fn two_entries_with_one_name_are_refused() {
    let mut archive = parsed(&archive(copy(), false));
    archive.files[2].name = archive.files[0].name.clone();
    assert_eq!(refused(&archive), Why::Unsupported);
}

#[test]
fn a_container_of_dataless_entries_has_nothing_to_route() {
    let bytes = write(
        copy(),
        false,
        &[
            (None, Entry::Dir("deep")),
            (None, Entry::File("empty.bin", &[])),
        ],
    );
    assert_eq!(refused(&parsed(&bytes)), Why::Unsupported);
}
