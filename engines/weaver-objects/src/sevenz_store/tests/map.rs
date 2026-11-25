// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// One test per refusal the map takes, each over a header the writer made,
// changed where the writer cannot make the shape itself.

use std::io::Cursor;

use sevenz_turbo::{Archive, ArchiveEntry, ArchiveWriter, EncoderMethod, Password};
use weaver_graph::Why;

use super::super::map::{build, coded_by};
use super::{Entry, PASSWORD, aes, archive, copy, files, lzma2, write};

fn parsed(bytes: &[u8]) -> Archive {
    Archive::read(&mut Cursor::new(bytes), &Password::new(PASSWORD)).unwrap()
}

fn refused(archive: &Archive) -> Why {
    build(archive).expect_err("refused")
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

fn aes_over(
    methods: Vec<sevenz_turbo::EncoderConfiguration>,
) -> Vec<sevenz_turbo::EncoderConfiguration> {
    let mut sealed = aes();
    sealed.truncate(1);
    sealed.extend(methods);
    sealed
}

// Nothing stored, every folder coded: the set goes to an engine for the
// first coded folder's own reason, as it did before AES could be coded.
#[test]
fn a_set_with_nothing_stored_goes_to_an_engine_for_its_first_coded_folders_reason() {
    let files = files();
    let two = |first, second| {
        write(
            first,
            false,
            &[
                (None, Entry::File(files[0].0, &files[0].1)),
                (Some(second), Entry::File(files[1].0, &files[1].1)),
            ],
        )
    };
    assert_eq!(
        refused(&parsed(&two(lzma2(), aes_over(lzma2())))),
        Why::Compressed
    );
    assert_eq!(
        refused(&parsed(&two(aes_over(lzma2()), lzma2()))),
        Why::Encrypted
    );
}

// Which folders are coded, by their methods. Without AES, every folder
// that does not stream is, as before. Under AES, only a chain whose other
// methods the run decodes, with at least one that is not a copy: AES
// alone, AES over copies, and AES beside a method the run cannot decode
// are not, and their set goes to an engine for the password.
#[test]
fn a_folder_under_aes_is_coded_only_over_a_chain_the_run_decodes() {
    let aes = EncoderMethod::ID_AES256_SHA256;
    let copy = EncoderMethod::ID_COPY;
    let unknown: &[u8] = &[0x7f, 0x7f];
    for coded in [
        vec![EncoderMethod::ID_LZMA2],
        vec![unknown],
        vec![EncoderMethod::ID_DEFLATE64],
        vec![aes, EncoderMethod::ID_LZMA2],
        vec![aes, EncoderMethod::ID_LZMA],
        vec![aes, EncoderMethod::ID_BCJ_X86, EncoderMethod::ID_LZMA2],
        vec![aes, EncoderMethod::ID_DELTA],
        vec![aes, EncoderMethod::ID_ZSTD],
        vec![aes, copy, EncoderMethod::ID_PPMD],
    ] {
        assert!(coded_by(&coded), "{coded:?}");
    }
    for refused in [
        vec![aes],
        vec![aes, copy],
        vec![aes, copy, copy],
        vec![aes, EncoderMethod::ID_DEFLATE64],
        vec![aes, EncoderMethod::ID_LZS],
        vec![aes, EncoderMethod::ID_LIZARD],
        vec![aes, unknown],
        vec![aes, EncoderMethod::ID_LZMA2, unknown],
    ] {
        assert!(!coded_by(&refused), "{refused:?}");
    }
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
