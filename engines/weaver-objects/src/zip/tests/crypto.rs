// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use ::zip::unstable::write::FileOptionsExt;

const PASSWORD: &str = "zip-test-passphrase";

fn encrypted(plain: &[u8]) -> Vec<u8> {
    let mut archive = write(&[Entry::File("alpha.bin", plain, stored())]);
    let end = records::find_end(&archive).unwrap();
    let central = end.offset as usize;
    let data = records::local_len(&archive).unwrap() as usize;
    archive[6..8].copy_from_slice(&1u16.to_le_bytes());
    archive[18..22].copy_from_slice(&(plain.len() as u32 + 12).to_le_bytes());
    archive[central + 8..central + 10].copy_from_slice(&1u16.to_le_bytes());
    archive[central + 20..central + 24].copy_from_slice(&(plain.len() as u32 + 12).to_le_bytes());
    archive[end.at as usize + 16..end.at as usize + 20]
        .copy_from_slice(&(central as u32 + 12).to_le_bytes());
    let cipher = super::super::crypto::encrypt_fixture(PASSWORD, plain);
    archive.splice(data..data + plain.len(), cipher);
    // The independent library reader checks the fixture and its CRC.
    let mut reader = ::zip::ZipArchive::new(Cursor::new(&archive)).unwrap();
    let mut member = reader.by_index_decrypt(0, PASSWORD.as_bytes()).unwrap();
    let mut decoded = Vec::new();
    std::io::Read::read_to_end(&mut member, &mut decoded).unwrap();
    assert!(decoded == plain);
    drop(member);
    drop(reader);
    archive
}

fn encrypted_descriptor(plain: &[u8]) -> Vec<u8> {
    let mut archive = encrypted(plain);
    let end = records::find_end(&archive).unwrap();
    let central = end.offset as usize;
    let data = records::local_len(&archive).unwrap() as usize;
    archive[6..8].copy_from_slice(&9u16.to_le_bytes());
    archive[10..12].fill(0);
    archive[central + 8..central + 10].copy_from_slice(&9u16.to_le_bytes());
    archive[central + 12..central + 14].fill(0);
    let cipher = super::super::crypto::encrypt_fixture_with_check(PASSWORD, plain, 0);
    archive[data..central].copy_from_slice(&cipher);
    archive[end.at as usize + 16..end.at as usize + 20]
        .copy_from_slice(&(central as u32 + 16).to_le_bytes());
    let mut descriptor = 0x0807_4b50u32.to_le_bytes().to_vec();
    descriptor.extend(crate::grid::crc32(plain).to_le_bytes());
    descriptor.extend((plain.len() as u32 + 12).to_le_bytes());
    descriptor.extend((plain.len() as u32).to_le_bytes());
    archive.splice(central..central, descriptor);
    archive
}

fn offer(owner: &mut Owner, password: &str) {
    let mut out = Vec::new();
    owner.object.offer_password(password, &mut out);
    owner.apply(out);
    owner.settle();
}

fn false_password(archive: &[u8]) -> String {
    use zeroize::Zeroizing;
    let header = records::local(archive).unwrap();
    let data = records::local_len(archive).unwrap() as usize;
    let encrypted_header: [u8; 12] = archive[data..data + 12].try_into().unwrap();
    (0..65_536)
        .map(|index| format!("candidate-{index}"))
        .find(|password| {
            let mut cipher = super::super::crypto::Cipher::new(&header);
            cipher.header = Some(encrypted_header);
            cipher.unlock(&[Zeroizing::new(password.clone())], Provenance::Fresh)
        })
        .expect("a deterministic candidate passes the one-byte check")
}

#[test]
fn zipcrypto_streams_plaintext_before_the_remaining_ciphertext_arrives() {
    let plain = payload(81, 200_003);
    let archive = encrypted(&plain);
    let start = records::local_len(&archive).unwrap() + super::super::crypto::HEADER;
    let mut owner = Owner::single(Zip::new(), archive.clone());
    owner.pull = Pull::Unseekable;
    offer(&mut owner, PASSWORD);
    owner.deliver(0, ByteRange::new(0, start + 4096));
    assert!(
        !owner.members.is_empty(),
        "{:?}; {:?}",
        owner.said,
        records::local(&archive)
    );
    assert!(owner.member("alpha.bin").bytes == plain[..4096]);
    assert!(!owner.member("alpha.bin").closed);
    owner.deliver(0, ByteRange::new(start + 4096, archive.len() as u64));
    owner.close(0);
    assert!(owner.member("alpha.bin").bytes == plain);
    assert!(owner.member("alpha.bin").closed);
    assert!(owner.posted(0) == Some(archive));
    assert!(
        !owner
            .said
            .iter()
            .any(|emit| matches!(emit, Emit::NeedsEngine(_)))
    );
}

#[test]
fn zipcrypto_sparse_pieces_wait_for_the_prefix_and_reproduce_the_ciphertext() {
    let plain = payload(82, 210_007);
    let archive = encrypted(&plain);
    let start = records::local_len(&archive).unwrap() + super::super::crypto::HEADER;
    let mut owner = Owner::single(Zip::new(), archive.clone());
    offer(&mut owner, PASSWORD);
    owner.deliver(0, ByteRange::new(120_000, 160_000));
    owner.deliver(0, ByteRange::new(80_000, 120_000));
    owner.deliver(0, ByteRange::new(0, start + 4096));
    owner.deliver(0, ByteRange::new(start + 4096, 80_000));
    owner.deliver(0, ByteRange::new(160_000, archive.len() as u64));
    owner.close(0);
    assert!(
        owner.member("alpha.bin").bytes == plain,
        "len={} closed={} said={:?}",
        owner.member("alpha.bin").bytes.len(),
        owner.member("alpha.bin").closed,
        owner.said
    );
    assert!(owner.member("alpha.bin").closed);
    assert!(owner.posted(0) == Some(archive));
    assert_eq!(owner.held(), 0);
}

#[test]
fn zipcrypto_rejects_a_wrong_password_and_opens_with_the_next() {
    let plain = payload(83, 170_011);
    let archive = encrypted(&plain);
    let mut owner = Owner::single(Zip::new(), archive.clone());
    offer(&mut owner, "incorrect-passphrase");
    owner.whole(4096);
    assert!(!owner.member("alpha.bin").closed);
    assert!(owner.said.contains(&Emit::NeedsPassword));
    offer(&mut owner, PASSWORD);
    assert!(owner.member("alpha.bin").bytes == plain);
    assert!(owner.member("alpha.bin").closed);
    assert!(owner.posted(0) == Some(archive));
}

#[test]
fn zipcrypto_restores_without_secret_keys_and_rederives_them_from_plaintext() {
    let plain = payload(84, 220_003);
    let archive = encrypted(&plain);
    let mut owner = Owner::single(Zip::new(), archive.clone());
    owner.pull = Pull::Unseekable;
    offer(&mut owner, PASSWORD);
    owner.deliver(0, ByteRange::new(0, 100_000));
    let saved = rmp_serde::to_vec(&owner.object).unwrap();
    assert!(
        !saved
            .windows(PASSWORD.len())
            .any(|part| part == PASSWORD.as_bytes())
    );
    owner.object = rmp_serde::from_slice(&saved).unwrap();
    offer(&mut owner, PASSWORD);
    owner.deliver(0, ByteRange::new(100_000, archive.len() as u64));
    owner.close(0);
    assert!(owner.member("alpha.bin").bytes == plain, "{:?}", owner.said);
    assert!(owner.member("alpha.bin").closed);
    assert!(owner.posted(0) == Some(archive));
}

#[test]
fn stored_plain_encrypted_and_empty_entries_share_the_zip_map() {
    let alpha = payload(85, 100_003);
    let beta = payload(86, 130_007);
    let options = stored()
        .with_deprecated_encryption(PASSWORD.as_bytes())
        .unwrap();
    let archive = write(&[
        Entry::File("alpha.bin", &alpha, stored()),
        Entry::File("beta.bin", &beta, options),
        Entry::File("empty.bin", &[], options),
    ]);
    let mut owner = Owner::single(Zip::new(), archive.clone());
    offer(&mut owner, PASSWORD);
    owner.whole(4096);
    assert!(owner.member("alpha.bin").bytes == alpha);
    assert!(owner.member("beta.bin").bytes == beta);
    assert!(owner.member("empty.bin").closed);
    assert!(owner.posted(0) == Some(archive));
}

#[test]
fn zipcrypto_retries_a_password_that_passes_its_one_byte_header_check() {
    let plain = payload(87, 90_003);
    let archive = encrypted(&plain);
    let false_password = false_password(&archive);
    let mut owner = Owner::single(Zip::new(), archive.clone());
    offer(&mut owner, &false_password);
    owner.whole(4096);
    assert!(!owner.member("alpha.bin").closed);
    assert!(owner.said.contains(&Emit::NeedsPassword));
    offer(&mut owner, PASSWORD);
    assert!(owner.member("alpha.bin").bytes == plain);
    assert!(owner.member("alpha.bin").closed);
    assert!(owner.posted(0) == Some(archive));
    assert!(owner.said.contains(&Emit::Retract(MemberId(0))));
}

#[test]
fn repairing_a_zipcrypto_prefix_replays_its_feedback_suffix_before_closing() {
    let plain = payload(88, 180_007);
    let archive = encrypted(&plain);
    let start = records::local_len(&archive).unwrap() + 12;
    let mut damaged = archive.clone();
    damaged[start as usize + 20] ^= 0x5a;
    let mut owner = Owner::single(Zip::new(), archive.clone());
    owner.pull = Pull::Unseekable;
    offer(&mut owner, PASSWORD);
    owner.deliver(0, ByteRange::new(0, start + 20));
    owner.deliver_as(
        0,
        ByteRange::new(start + 20, start + 21),
        Provenance::Damaged,
        Some(&damaged[start as usize + 20..start as usize + 21]),
    );
    owner.deliver(0, ByteRange::new(start + 21, archive.len() as u64));
    assert!(!owner.member("alpha.bin").closed);
    assert!(owner.member("alpha.bin").bytes != plain);
    owner.deliver(0, ByteRange::new(start + 20, start + 21));
    assert!(owner.member("alpha.bin").bytes == plain, "{:?}", owner.said);
    assert!(owner.member("alpha.bin").closed, "{:?}", owner.said);
    assert!(owner.posted(0) == Some(archive));
}

#[test]
fn zipcrypto_inverse_postings_are_sparse_and_survive_live_key_invalidation() {
    use weaver_graph::Placement;
    let plain = payload(89, 200_003);
    let archive = encrypted(&plain);
    let start = records::local_len(&archive).unwrap() + 12;
    let mut owner = Owner::single(Zip::new(), archive.clone());
    offer(&mut owner, PASSWORD);
    owner.whole(4096);
    let whole = ByteRange::new(0, archive.len() as u64);
    let posted = owner.object.posting(&[(VolumeIndex(0), whole)]);
    owner
        .object
        .unposted(VolumeIndex(0), ByteRange::at(start + 70_003, 11));
    for range in [
        ByteRange::at(start + 130_001, 257),
        ByteRange::new(0, start + 3),
        whole,
        ByteRange::at(start + 64_999, 33),
    ] {
        let mut bytes = Vec::new();
        assert!(posted.posted(
            VolumeIndex(0),
            range,
            &mut |placement, into| {
                match placement {
                    Placement::Member { range, .. } => {
                        into.extend_from_slice(&plain[range.start as usize..range.end as usize])
                    }
                    Placement::Envelope { envelope, range } => {
                        let Some(bytes) = owner.object.envelope(*envelope, *range) else {
                            return false;
                        };
                        into.extend_from_slice(&bytes);
                    }
                    Placement::Held { .. } => return false,
                }
                true
            },
            &mut bytes
        ));
        assert!(bytes == archive[range.start as usize..range.end as usize]);
    }
}

#[test]
fn a_zipcrypto_local_header_starts_without_a_terminal_article_extent() {
    use weaver_graph::{Delivery, Evidenced};
    let plain = payload(90, 100_003);
    let archive = encrypted(&plain);
    let start = records::local_len(&archive).unwrap() + 12;
    let mut object = Zip::new();
    let volume = VolumeIndex(crate::zip_engine::naming::LAST);
    object.bound(volume, "alpha.zip", &archive[..32], None);
    object.offer_password(PASSWORD, &mut Vec::new());
    let mut out = Vec::new();
    object.deliver(
        Delivery {
            volume,
            offset: 0,
            bytes: &Evidenced::from(archive[..start as usize + 4096].to_vec()),
            provenance: Provenance::Fresh,
        },
        &mut |_, _| false,
        &mut out,
    );
    assert!(out.iter().any(
        |emit| matches!(emit, Emit::Bytes { offset: 0, bytes, .. } if bytes[..] == plain[..4096])
    ));
    assert!(
        !out.iter()
            .any(|emit| matches!(emit, Emit::NeedsEngine(_) | Emit::Refuse(_)))
    );
}

#[test]
fn zipcrypto_compression_falls_back_for_compression_and_unknown_local_sizes_for_extent() {
    let plain = payload(91, 20_003);
    let options = SimpleFileOptions::default()
        .compression_method(CompressionMethod::Deflated)
        .with_deprecated_encryption(PASSWORD.as_bytes())
        .unwrap();
    let mut owner = Owner::single(
        Zip::new(),
        write(&[Entry::File("alpha.bin", &plain, options)]),
    );
    owner.whole(4096);
    assert_eq!(owner.said, vec![Emit::NeedsEngine(Why::Compressed)]);
    let options = stored()
        .with_deprecated_encryption(PASSWORD.as_bytes())
        .unwrap();
    let mut owner = Owner::single(
        Zip::new(),
        write(&[Entry::File("alpha.bin", &plain, options)]),
    );
    owner.pull = Pull::Unseekable;
    owner.whole(4096);
    assert_eq!(owner.said, vec![Emit::NeedsEngine(Why::UnboundedEntry)]);
}

#[test]
fn completed_zipcrypto_restore_skips_an_earlier_false_positive_candidate() {
    let plain = payload(92, 100_003);
    let archive = encrypted(&plain);
    let wrong = false_password(&archive);
    let mut owner = Owner::single(Zip::new(), archive.clone());
    offer(&mut owner, &wrong);
    offer(&mut owner, PASSWORD);
    owner.whole(4096);
    assert!(owner.member("alpha.bin").closed);
    let saved = rmp_serde::to_vec(&owner.object).unwrap();
    owner.object = rmp_serde::from_slice(&saved).unwrap();
    offer(&mut owner, PASSWORD);
    assert!(owner.posted(0) == Some(archive.clone()));
    owner.object = rmp_serde::from_slice(&saved).unwrap();
    offer(&mut owner, &wrong);
    offer(&mut owner, PASSWORD);
    // An inverse read must prove the selected candidate before returning
    // ciphertext, even when no new delivery drove the restored store.
    assert!(owner.posted(0) == Some(archive.clone()));
    owner.deliver(0, ByteRange::new(80_000, 84_096));
    assert!(owner.member("alpha.bin").bytes == plain);
    assert!(owner.member("alpha.bin").closed);
    assert!(owner.posted(0) == Some(archive));
}

#[test]
fn partial_zipcrypto_restore_retries_the_original_false_positive_after_full_crc() {
    let plain = payload(93, 180_007);
    let archive = encrypted(&plain);
    let wrong = false_password(&archive);
    let mut owner = Owner::single(Zip::new(), archive.clone());
    owner.pull = Pull::Unseekable;
    offer(&mut owner, &wrong);
    owner.deliver(0, ByteRange::new(0, 80_000));
    assert!(!owner.member("alpha.bin").closed);
    assert!(owner.member("alpha.bin").bytes != plain[..owner.member("alpha.bin").bytes.len()]);
    owner.object = rmp_serde::from_slice(&rmp_serde::to_vec(&owner.object).unwrap()).unwrap();
    offer(&mut owner, &wrong);
    offer(&mut owner, PASSWORD);
    owner.deliver(0, ByteRange::new(80_000, archive.len() as u64));
    owner.close(0);
    assert!(owner.member("alpha.bin").bytes == plain, "{:?}", owner.said);
    assert!(owner.member("alpha.bin").closed);
    assert!(owner.posted(0) == Some(archive));
}

#[test]
fn a_changed_candidate_list_after_partial_restore_never_fabricates_ciphertext() {
    let plain = payload(94, 180_007);
    let archive = encrypted(&plain);
    let wrong = false_password(&archive);
    let mut owner = Owner::single(Zip::new(), archive.clone());
    owner.pull = Pull::Unseekable;
    offer(&mut owner, &wrong);
    owner.deliver(0, ByteRange::new(0, 80_000));
    owner.object = rmp_serde::from_slice(&rmp_serde::to_vec(&owner.object).unwrap()).unwrap();
    offer(&mut owner, PASSWORD);
    let mut recovered = Vec::new();
    assert!(!owner.read_posted(VolumeIndex(0), ByteRange::new(0, 80_000), &mut recovered));
    assert!(!owner.member("alpha.bin").closed);
}

#[test]
fn zipcrypto_spanned_volumes_reproduce_every_original_volume() {
    let plain = payload(95, 190_003);
    let volumes = spanned(&encrypted(&plain), 3);
    let mut owner = Owner::new(Zip::new(), volumes.clone());
    offer(&mut owner, PASSWORD);
    for volume in [2, 0, 1] {
        owner.frames(volume, 4096);
    }
    for volume in [2, 0, 1] {
        owner.close(volume);
    }
    assert!(owner.member("alpha.bin").bytes == plain);
    assert!(owner.member("alpha.bin").closed);
    for (volume, (_, bytes)) in volumes.into_iter().enumerate() {
        assert!(owner.posted(volume) == Some(bytes));
    }
}

#[test]
fn zipcrypto_metadata_mismatch_and_strong_encryption_are_explicit_fallbacks() {
    let plain = payload(96, 100_003);
    let mut archive = encrypted(&plain);
    let central = records::find_end(&archive).unwrap().offset as usize;
    archive[14] ^= 1;
    let mut owner = Owner::single(Zip::new(), archive);
    offer(&mut owner, PASSWORD);
    owner.whole(4096);
    assert!(owner.said.contains(&Emit::NeedsEngine(Why::Unsupported)));
    let mut archive = encrypted(&plain);
    archive[6..8].copy_from_slice(&65u16.to_le_bytes());
    archive[central + 8..central + 10].copy_from_slice(&65u16.to_le_bytes());
    let mut owner = Owner::single(Zip::new(), archive);
    owner.whole(4096);
    assert_eq!(owner.said, vec![Emit::NeedsEngine(Why::Unsupported)]);
}

#[test]
fn known_size_zipcrypto_with_descriptor_streams_before_its_tail() {
    let plain = payload(97, 200_003);
    let archive = encrypted_descriptor(&plain);
    let data = records::local_len(&archive).unwrap() + 12;
    let mut owner = Owner::single(Zip::new(), archive.clone());
    owner.pull = Pull::Unseekable;
    offer(&mut owner, PASSWORD);
    owner.deliver(0, ByteRange::new(0, data + 4096));
    assert!(owner.member("alpha.bin").bytes == plain[..4096]);
    assert!(!owner.member("alpha.bin").closed);
    owner.deliver(0, ByteRange::new(data + 4096, archive.len() as u64));
    owner.close(0);
    assert!(owner.member("alpha.bin").bytes == plain);
    assert!(owner.member("alpha.bin").closed);
    assert!(owner.posted(0) == Some(archive.clone()));
    assert!(
        !owner
            .said
            .iter()
            .any(|emit| matches!(emit, Emit::NeedsEngine(_)))
    );
    let mut reader = ::zip::ZipArchive::new(Cursor::new(archive)).unwrap();
    let mut member = reader.by_index_decrypt(0, PASSWORD.as_bytes()).unwrap();
    let mut decoded = Vec::new();
    std::io::Read::read_to_end(&mut member, &mut decoded).unwrap();
    assert!(decoded == plain);
}

#[test]
fn known_sizes_with_deferred_crc_wait_for_the_descriptor_without_condemning_the_password() {
    let plain = payload(98, 200_003);
    let mut archive = encrypted_descriptor(&plain);
    let data = records::local_len(&archive).unwrap() + 12;
    let descriptor = data + plain.len() as u64;
    archive[14..18].fill(0);
    let mut owner = Owner::single(Zip::new(), archive.clone());
    owner.pull = Pull::Unseekable;
    offer(&mut owner, PASSWORD);
    owner.deliver(0, ByteRange::new(0, descriptor));
    assert!(owner.member("alpha.bin").bytes == plain);
    assert!(!owner.member("alpha.bin").closed);
    owner.deliver(0, ByteRange::new(descriptor, archive.len() as u64));
    owner.close(0);
    assert!(owner.member("alpha.bin").closed);
    assert!(owner.posted(0) == Some(archive));
    assert!(!owner.said.contains(&Emit::NeedsPassword));
}

#[test]
fn a_pending_zipcrypto_repair_survives_a_checkpoint_before_its_replay() {
    use weaver_graph::{Delivery, Evidenced, Placement};
    let plain = payload(99, 180_007);
    let archive = encrypted(&plain);
    let start = records::local_len(&archive).unwrap() + 12;
    let repair = ByteRange::at(start + 20, 1);
    let mut owner = Owner::single(Zip::new(), archive.clone());
    owner.pull = Pull::Unseekable;
    offer(&mut owner, PASSWORD);
    owner.deliver(0, ByteRange::new(0, repair.start));
    owner.deliver_as(
        0,
        repair,
        Provenance::Damaged,
        Some(&[archive[repair.start as usize] ^ 0x5a]),
    );
    owner.deliver(0, ByteRange::new(repair.end, archive.len() as u64));
    let previous = owner.member("alpha.bin").bytes.clone();
    let mut out = Vec::new();
    owner.object.deliver(
        Delivery {
            volume: VolumeIndex(0),
            offset: repair.start,
            bytes: &Evidenced::from(vec![archive[repair.start as usize]]),
            provenance: Provenance::Fresh,
        },
        &mut |placement, into| {
            if let Placement::Member { range, .. } = placement {
                into.extend_from_slice(&previous[range.start as usize..range.end as usize]);
                true
            } else {
                false
            }
        },
        &mut out,
    );
    assert!(out.iter().any(|emit| matches!(emit, Emit::Again { .. })));
    assert!(
        !owner
            .object
            .map
            .as_ref()
            .unwrap()
            .entry(MemberId(0))
            .unwrap()
            .cipher
            .as_ref()
            .unwrap()
            .pending
            .is_empty()
    );
    owner
        .delivered
        .get_mut(&0)
        .unwrap()
        .write(repair, Provenance::Fresh);
    owner.object = rmp_serde::from_slice(&rmp_serde::to_vec(&owner.object).unwrap()).unwrap();
    offer(&mut owner, PASSWORD);
    assert!(owner.member("alpha.bin").bytes == plain);
    assert!(owner.member("alpha.bin").closed);
    assert!(owner.posted(0) == Some(archive));
    assert!(
        owner
            .object
            .map
            .as_ref()
            .unwrap()
            .entry(MemberId(0))
            .unwrap()
            .cipher
            .as_ref()
            .unwrap()
            .pending
            .is_empty()
    );
}

fn payload_with_crc(target: u32) -> Vec<u8> {
    let mut plain = payload(100, 10_003);
    let suffix = plain.len();
    plain.extend([0u8; 4]);
    let base = crate::grid::crc32(&plain);
    let mut basis = [(0u32, 0u32); 32];
    for bit in 0..32 {
        plain[suffix + bit / 8] = 1 << (bit % 8);
        let mut value = crate::grid::crc32(&plain) ^ base;
        plain[suffix + bit / 8] = 0;
        let mut mask = 1u32 << bit;
        for pivot in (0..32).rev() {
            if value & (1 << pivot) == 0 {
                continue;
            }
            if basis[pivot].0 == 0 {
                basis[pivot] = (value, mask);
                break;
            }
            value ^= basis[pivot].0;
            mask ^= basis[pivot].1;
        }
    }
    let mut value = target ^ base;
    let mut mask = 0u32;
    for pivot in (0..32).rev() {
        if value & (1 << pivot) != 0 {
            value ^= basis[pivot].0;
            mask ^= basis[pivot].1;
        }
    }
    assert_eq!(value, 0);
    plain[suffix..].copy_from_slice(&mask.to_le_bytes());
    assert_eq!(crate::grid::crc32(&plain), target);
    plain
}

#[test]
fn an_unsigned_descriptor_crc_can_equal_the_optional_signature() {
    let plain = payload_with_crc(0x0807_4b50);
    let mut archive = encrypted_descriptor(&plain);
    let descriptor = records::local_len(&archive).unwrap() as usize + 12 + plain.len();
    let end = records::find_end(&archive).unwrap();
    archive[end.at as usize + 16..end.at as usize + 20]
        .copy_from_slice(&(end.offset as u32 - 4).to_le_bytes());
    archive.drain(descriptor..descriptor + 4);
    let mut owner = Owner::single(Zip::new(), archive.clone());
    owner.pull = Pull::Unseekable;
    offer(&mut owner, PASSWORD);
    owner.whole(257);
    assert!(owner.member("alpha.bin").bytes == plain);
    assert!(owner.member("alpha.bin").closed);
    assert!(owner.posted(0) == Some(archive));
    assert!(!owner.said.iter().any(|emit| matches!(
        emit,
        Emit::Damaged { .. } | Emit::NeedsEngine(_) | Emit::Refuse(_)
    )));
}

#[test]
fn a_small_zipcrypto_entry_can_use_a_zip64_data_descriptor() {
    let plain = payload(101, 100_003);
    let mut archive = encrypted_descriptor(&plain);
    let local = records::local_len(&archive).unwrap() as usize;
    let old_directory = records::find_end(&archive).unwrap().offset;
    archive[18..22].copy_from_slice(&u32::MAX.to_le_bytes());
    archive[22..26].copy_from_slice(&u32::MAX.to_le_bytes());
    archive[28..30].copy_from_slice(&20u16.to_le_bytes());
    let mut extra = vec![1, 0, 16, 0];
    extra.extend((plain.len() as u64).to_le_bytes());
    extra.extend((plain.len() as u64 + 12).to_le_bytes());
    archive.splice(local..local, extra);
    let descriptor = local + 20 + 12 + plain.len();
    let mut wide = 0x0807_4b50u32.to_le_bytes().to_vec();
    wide.extend(crate::grid::crc32(&plain).to_le_bytes());
    wide.extend((plain.len() as u64 + 12).to_le_bytes());
    wide.extend((plain.len() as u64).to_le_bytes());
    archive.splice(descriptor..descriptor + 16, wide);
    let end = records::find_end(&archive).unwrap();
    archive[end.at as usize + 16..end.at as usize + 20]
        .copy_from_slice(&(old_directory as u32 + 28).to_le_bytes());
    let mut owner = Owner::single(Zip::new(), archive.clone());
    owner.pull = Pull::Unseekable;
    offer(&mut owner, PASSWORD);
    owner.whole(4096);
    assert!(owner.member("alpha.bin").bytes == plain);
    assert!(owner.member("alpha.bin").closed);
    assert!(owner.posted(0) == Some(archive));
    assert!(!owner.said.iter().any(|emit| matches!(
        emit,
        Emit::Damaged { .. } | Emit::NeedsEngine(_) | Emit::Refuse(_)
    )));
}

// A zip64 field in the local header makes the descriptor's sizes 8 bytes
// each though neither size is saturated, as APPNOTE 4.3.9.2 has it.
#[test]
fn a_zip64_field_widens_the_descriptor_whatever_the_sizes() {
    let plain = payload(103, 100_003);
    let mut archive = encrypted_descriptor(&plain);
    let local = records::local_len(&archive).unwrap() as usize;
    let old_directory = records::find_end(&archive).unwrap().offset;
    archive[28..30].copy_from_slice(&20u16.to_le_bytes());
    let mut extra = vec![1, 0, 16, 0];
    extra.extend((plain.len() as u64).to_le_bytes());
    extra.extend((plain.len() as u64 + 12).to_le_bytes());
    archive.splice(local..local, extra);
    let descriptor = local + 20 + 12 + plain.len();
    let mut wide = 0x0807_4b50u32.to_le_bytes().to_vec();
    wide.extend(crate::grid::crc32(&plain).to_le_bytes());
    wide.extend((plain.len() as u64 + 12).to_le_bytes());
    wide.extend((plain.len() as u64).to_le_bytes());
    archive.splice(descriptor..descriptor + 16, wide);
    let end = records::find_end(&archive).unwrap();
    archive[end.at as usize + 16..end.at as usize + 20]
        .copy_from_slice(&(old_directory as u32 + 28).to_le_bytes());
    let mut owner = Owner::single(Zip::new(), archive.clone());
    owner.pull = Pull::Unseekable;
    offer(&mut owner, PASSWORD);
    owner.whole(4096);
    assert!(owner.member("alpha.bin").bytes == plain);
    assert!(owner.member("alpha.bin").closed);
    assert!(owner.posted(0) == Some(archive));
    assert!(!owner.said.iter().any(|emit| matches!(
        emit,
        Emit::Damaged { .. } | Emit::NeedsEngine(_) | Emit::Refuse(_)
    )));
}

// A directory entry flagged encrypted, its twelve-byte encryption header
// for data: a directory holds no bytes to decrypt, so it closes with no
// password asked for, and its header is the envelope's.
fn encrypted_directory() -> Vec<u8> {
    let mut archive = write(&[Entry::Dir("folder/")]);
    let end = records::find_end(&archive).unwrap();
    let central = end.offset as usize;
    let data = records::local_len(&archive).unwrap() as usize;
    archive[6..8].copy_from_slice(&1u16.to_le_bytes());
    archive[18..22].copy_from_slice(&12u32.to_le_bytes());
    archive[central + 8..central + 10].copy_from_slice(&1u16.to_le_bytes());
    archive[central + 20..central + 24].copy_from_slice(&12u32.to_le_bytes());
    archive[end.at as usize + 16..end.at as usize + 20]
        .copy_from_slice(&(central as u32 + 12).to_le_bytes());
    let header = super::super::crypto::encrypt_fixture(PASSWORD, b"");
    assert_eq!(header.len(), 12);
    archive.splice(data..data, header);
    archive
}

#[test]
fn an_encrypted_directory_entry_closes_without_a_password() {
    for pull in [Pull::Moved(1), Pull::Unseekable] {
        let archive = encrypted_directory();
        let mut owner = Owner::single(Zip::new(), archive.clone());
        owner.pull = pull;
        owner.whole(4096);
        assert!(owner.member("folder/").closed, "{pull:?}");
        assert!(
            !owner.said.contains(&Emit::NeedsPassword),
            "{:?}",
            owner.said
        );
        assert!(owner.posted(0) == Some(archive));
    }
}

#[test]
fn an_inverse_frontier_is_bound_to_the_candidate_that_produced_it() {
    use weaver_graph::{Coverage, Placement};
    use zeroize::Zeroizing;
    let plain = payload(102, 100_003);
    let archive = encrypted(&plain);
    let wrong = false_password(&archive);
    let data = records::local_len(&archive).unwrap() as usize;
    let mut cipher = super::super::crypto::Cipher::new(&records::local(&archive).unwrap());
    cipher.header = Some(archive[data..data + 12].try_into().unwrap());
    assert!(cipher.unlock(&[Zeroizing::new(wrong.clone())], Provenance::Fresh));
    let wrong_plain = std::sync::Arc::make_mut(cipher.open.as_mut().unwrap())
        .decrypt(
            0,
            &archive[data + 12..data + 12 + plain.len()],
            Provenance::Fresh,
            &Coverage::new(),
            &mut |_, _| false,
        )
        .unwrap()
        .0;
    let mut owner = Owner::single(Zip::new(), archive.clone());
    offer(&mut owner, &wrong);
    offer(&mut owner, PASSWORD);
    owner.whole(4096);
    let body = ByteRange::at(data as u64 + 12, plain.len() as u64);
    let posted = owner.object.posting(&[(VolumeIndex(0), body)]);
    for (offset, source) in [(0u64, &wrong_plain), (4096, &plain)] {
        let range = ByteRange::at(body.start + offset, 4096);
        let mut output = Vec::new();
        assert!(posted.posted(
            VolumeIndex(0),
            range,
            &mut |placement, into| {
                if let Placement::Member { range, .. } = placement {
                    into.extend_from_slice(&source[range.start as usize..range.end as usize]);
                    true
                } else {
                    false
                }
            },
            &mut output
        ));
        assert!(output == archive[range.start as usize..range.end as usize]);
    }
}

#[test]
fn a_repaired_zipcrypto_header_rechecks_rejected_passwords() {
    let plain = payload(103, 200_003);
    let archive = encrypted(&plain);
    let header = records::local_len(&archive).unwrap();
    let data = header + 12;
    let mut broken = archive[..data as usize].to_vec();
    broken[data as usize - 1] ^= 1;
    let mut owner = Owner::single(Zip::new(), archive.clone());
    owner.pull = Pull::Unseekable;
    offer(&mut owner, PASSWORD);
    owner.deliver_as(0, ByteRange::new(0, data), Provenance::Fresh, Some(&broken));
    assert!(owner.said.contains(&Emit::NeedsPassword));
    owner.deliver_as(0, ByteRange::new(header, data), Provenance::Repaired, None);
    owner.deliver(0, ByteRange::new(data, archive.len() as u64));
    owner.close(0);
    assert!(owner.member("alpha.bin").bytes == plain, "{:?}", owner.said);
    assert!(owner.member("alpha.bin").closed);
    assert!(owner.posted(0) == Some(archive));
}

#[test]
fn an_ignored_encrypted_symlink_leaves_the_archive_complete() {
    let plain = payload(104, 1000);
    let options = stored()
        .with_deprecated_encryption(PASSWORD.as_bytes())
        .unwrap();
    let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
    writer.start_file("feature.mkv", stored()).unwrap();
    writer.write_all(&plain).unwrap();
    writer.add_symlink("link", "feature.mkv", options).unwrap();
    let archive = writer.finish().unwrap().into_inner();
    let mut owner = Owner::single(Zip::new(), archive.clone());
    owner.whole(4096);
    assert!(owner.member("feature.mkv").closed);
    assert_eq!(owner.member("feature.mkv").bytes, plain);
    let mut out = Vec::new();
    owner
        .object
        .quiet(&crate::testing::Quiet(Vec::new()), &mut out);
    assert!(out.is_empty(), "{out:?}");
    assert!(!owner.said.contains(&Emit::NeedsPassword));
    assert!(owner.posted(0) == Some(archive));
}

fn repaired_header_preserves_posted_ciphertext(restore: bool) {
    use zeroize::Zeroizing;
    let plain = payload(105, 200_003);
    let archive = encrypted(&plain);
    let header = records::local_len(&archive).unwrap() as usize;
    let data = header + 12;
    let mut broken = archive[..data].to_vec();
    broken[header] ^= 1;
    // A damaged header can pass its one-byte password check while giving
    // the payload different keys. Those keys must still seal its plaintext.
    let header_record = records::local(&archive).unwrap();
    let check = (0..=u8::MAX)
        .find(|&check| {
            broken[data - 1] = check;
            let mut cipher = super::super::crypto::Cipher::new(&header_record);
            cipher.header = Some(broken[header..data].try_into().unwrap());
            cipher.unlock(&[Zeroizing::new(PASSWORD.to_owned())], Provenance::Fresh)
        })
        .unwrap();
    broken[data - 1] = check;
    let mut owner = Owner::single(Zip::new(), archive.clone());
    owner.pull = Pull::Unseekable;
    offer(&mut owner, PASSWORD);
    owner.deliver_as(
        0,
        ByteRange::new(0, data as u64),
        Provenance::Fresh,
        Some(&broken),
    );
    let prefix = ByteRange::new(data as u64, data as u64 + 4096);
    owner.deliver(0, prefix);
    assert!(owner.member("alpha.bin").bytes != plain[..4096]);
    assert!(
        owner
            .member("alpha.bin")
            .coverage
            .covered(ByteRange::new(0, 4096))
    );
    if restore {
        let saved = rmp_serde::to_vec(&owner.object).unwrap();
        owner.object = rmp_serde::from_slice(&saved).unwrap();
        // Repair can arrive before passwords are reoffered after restore.
        owner.deliver_as(
            0,
            ByteRange::new(header as u64, data as u64),
            Provenance::Repaired,
            None,
        );
        assert!(!owner.said.contains(&Emit::NeedsEngine(Why::Unmapped)));
        offer(&mut owner, PASSWORD);
    } else {
        owner.deliver_as(
            0,
            ByteRange::new(header as u64, data as u64),
            Provenance::Repaired,
            None,
        );
    }
    assert!(owner.said.contains(&Emit::NeedsEngine(Why::Unmapped)));
    let mut posted = Vec::new();
    assert!(owner.read_posted(VolumeIndex(0), prefix, &mut posted));
    assert!(posted == archive[prefix.start as usize..prefix.end as usize]);
    let mut posted = Vec::new();
    assert!(owner.read_posted(VolumeIndex(0), ByteRange::new(0, prefix.end), &mut posted));
    assert!(posted == archive[..prefix.end as usize]);
}

#[test]
fn a_repaired_zipcrypto_header_preserves_the_ciphertext_for_the_engine() {
    repaired_header_preserves_posted_ciphertext(false);
}

#[test]
fn a_repaired_zipcrypto_header_after_restore_preserves_the_ciphertext_for_the_engine() {
    repaired_header_preserves_posted_ciphertext(true);
}

#[test]
fn a_restored_ignored_cipher_does_not_require_a_password() {
    let plain = payload(106, 1000);
    let options = stored()
        .with_deprecated_encryption(PASSWORD.as_bytes())
        .unwrap();
    let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
    writer.start_file("feature.mkv", stored()).unwrap();
    writer.write_all(&plain).unwrap();
    writer.add_symlink("link", "feature.mkv", options).unwrap();
    let archive = writer.finish().unwrap().into_inner();
    let mut owner = Owner::single(Zip::new(), archive.clone());
    owner.whole(4096);
    // Earlier snapshots kept a cipher even for an entry with no member.
    let ignored = owner
        .object
        .map
        .as_mut()
        .unwrap()
        .entries
        .iter_mut()
        .find(|entry| entry.member.is_none())
        .unwrap();
    ignored.cipher = Some(super::super::crypto::Cipher::new(
        &records::local(&archive).unwrap(),
    ));
    let saved = rmp_serde::to_vec(&owner.object).unwrap();
    owner.object = rmp_serde::from_slice(&saved).unwrap();
    let mut out = Vec::new();
    owner
        .object
        .quiet(&crate::testing::Quiet(Vec::new()), &mut out);
    assert!(out.is_empty(), "{out:?}");
    assert!(owner.member("feature.mkv").closed);
}

#[test]
fn a_verified_wrong_byte_keeps_the_only_passing_password_and_waits_for_repair() {
    let plain = payload(88, 300_007);
    let archive = encrypted(&plain);
    let start = records::local_len(&archive).unwrap() + 12;
    let bad = ByteRange::at(start + 20_000, 1);
    let mut owner = Owner::single(Zip::new(), archive.clone());
    owner.pull = Pull::Unseekable;
    offer(&mut owner, PASSWORD);
    owner.deliver(0, ByteRange::new(0, bad.start));
    // Bad as posted, with a good article checksum: verified but wrong.
    owner.deliver_as(
        0,
        bad,
        Provenance::Fresh,
        Some(&[archive[bad.start as usize] ^ 0x5a]),
    );
    owner.deliver(0, ByteRange::new(bad.end, archive.len() as u64));
    owner.close(0);
    assert!(!owner.member("alpha.bin").closed);
    assert!(
        owner
            .said
            .iter()
            .any(|emit| matches!(emit, Emit::Damaged { .. })),
        "{:?}",
        owner.said
    );
    // The recovery set repairs the byte; the password is not offered again.
    let before = owner.said.len();
    owner.deliver_as(0, bad, Provenance::Repaired, None);
    assert!(owner.member("alpha.bin").bytes == plain, "{:?}", owner.said);
    assert!(owner.member("alpha.bin").closed, "{:?}", owner.said);
    assert!(
        !owner.said[before..].contains(&Emit::NeedsPassword),
        "{:?}",
        owner.said
    );
    assert!(
        !owner.said.contains(&Emit::Retract(MemberId(0))),
        "{:?}",
        owner.said
    );
    let mut out = Vec::new();
    owner
        .object
        .quiet(&crate::testing::Quiet(Vec::new()), &mut out);
    assert!(out.is_empty(), "{out:?}");
    assert!(owner.posted(0) == Some(archive));
}

#[test]
fn a_restore_no_offered_candidate_proves_starts_the_member_over() {
    let plain = payload(94, 180_007);
    let archive = encrypted(&plain);
    let start = records::local_len(&archive).unwrap() + 12;
    let wrong = false_password(&archive);
    let mut owner = Owner::single(Zip::new(), archive.clone());
    owner.pull = Pull::Unseekable;
    offer(&mut owner, &wrong);
    owner.deliver(0, ByteRange::new(0, 80_000));
    owner.object = rmp_serde::from_slice(&rmp_serde::to_vec(&owner.object).unwrap()).unwrap();
    let before = owner.said.len();
    // Only the right password now: it does not reproduce what the false
    // positive said, so nothing may be derived from that plaintext.
    offer(&mut owner, PASSWORD);
    owner.deliver(0, ByteRange::new(80_000, archive.len() as u64));
    owner.close(0);
    let said = &owner.said[before..];
    assert!(said.contains(&Emit::Retract(MemberId(0))), "{said:?}");
    assert!(said.contains(&Emit::NeedsPassword), "{said:?}");
    assert!(
        said.iter().any(|emit| matches!(
            emit,
            Emit::Damaged { volume: VolumeIndex(0), range }
                if range.start <= start && range.end >= 80_000
        )),
        "{said:?}"
    );
    assert!(!owner.member("alpha.bin").closed);
    assert!(
        owner.member("alpha.bin").bytes.is_empty()
            || owner.member("alpha.bin").bytes[..]
                == plain[..owner.member("alpha.bin").bytes.len()]
    );
    // The recovery brings the damaged ciphertext again.
    owner.deliver_as(0, ByteRange::new(0, 80_000), Provenance::Repaired, None);
    owner.close(0);
    assert!(owner.member("alpha.bin").bytes == plain, "{:?}", owner.said);
    assert!(owner.member("alpha.bin").closed);
    assert!(owner.posted(0) == Some(archive));
}

// A restored member whose plaintext does not reach its front, made under a
// candidate since withdrawn: the front coming fresh under another proves
// nothing of the rest, so no key is derived through that plaintext. The
// member starts over and its ciphertext is fetched again.
#[test]
fn a_restore_without_its_front_never_derives_keys_through_unproven_plaintext() {
    let plain = payload(61, 180_007);
    let archive = encrypted(&plain);
    let start = records::local_len(&archive).unwrap() + 12;
    let wrong = false_password(&archive);
    let mut owner = Owner::single(Zip::new(), archive.clone());
    owner.pull = Pull::Unseekable;
    offer(&mut owner, &wrong);
    owner.deliver(0, ByteRange::new(0, 150_000));
    // The restore holds what the false positive said past the front only.
    owner.object.members[0]
        .coverage
        .erase(ByteRange::new(0, 50_000));
    owner.restore();
    let before = owner.said.len();
    offer(&mut owner, PASSWORD);
    owner.deliver(0, ByteRange::new(0, start + 50_000));
    owner.deliver(0, ByteRange::new(150_000, archive.len() as u64));
    owner.close(0);
    let said = &owner.said[before..];
    assert!(said.contains(&Emit::Retract(MemberId(0))), "{said:?}");
    assert!(
        said.iter().any(|emit| matches!(
            emit,
            Emit::Damaged { volume: VolumeIndex(0), range }
                if range.start <= start + 50_000 && range.end >= 150_000
        )),
        "{said:?}"
    );
    assert!(!owner.member("alpha.bin").closed);
    owner.deliver_as(
        0,
        ByteRange::new(start + 50_000, 150_000),
        Provenance::Repaired,
        None,
    );
    owner.close(0);
    assert!(owner.member("alpha.bin").bytes == plain, "{:?}", owner.said);
    assert!(owner.member("alpha.bin").closed);
    assert!(owner.posted(0) == Some(archive));
}

#[test]
fn an_unproven_partial_restore_continues_without_replaying_the_member() {
    let plain = payload(107, 300_007);
    let archive = encrypted(&plain);
    let mut owner = Owner::single(Zip::new(), archive.clone());
    owner.pull = Pull::Unseekable;
    offer(&mut owner, PASSWORD);
    owner.deliver(0, ByteRange::new(0, 200_000));
    owner.restore();
    let before = owner.said.len();
    offer(&mut owner, PASSWORD);
    owner.deliver(0, ByteRange::new(200_000, archive.len() as u64));
    owner.close(0);
    // The proof reconstructs the keys from the plaintext in hand: nothing
    // said before the restore is read back or said again.
    assert!(
        !owner.said[before..]
            .iter()
            .any(|emit| matches!(emit, Emit::Again { .. } | Emit::Retract(_))),
        "{:?}",
        &owner.said[before..]
    );
    assert_eq!(owner.replays, 0);
    assert!(owner.member("alpha.bin").bytes == plain);
    assert!(owner.member("alpha.bin").closed);
    assert!(owner.posted(0) == Some(archive));
}

// Two damaged bytes, each repaired in turn: each repair replays the member
// from the repaired byte on, never from its front.
#[test]
fn a_zipcrypto_repair_replays_only_from_the_repaired_byte() {
    let plain = payload(88, 300_007);
    let archive = encrypted(&plain);
    let start = records::local_len(&archive).unwrap() + 12;
    let end = start + plain.len() as u64;
    let a = ByteRange::at(start + 20_000, 1);
    let b = ByteRange::at(start + 200_000, 1);
    let mut owner = Owner::single(Zip::new(), archive.clone());
    owner.pull = Pull::Unseekable;
    offer(&mut owner, PASSWORD);
    owner.deliver(0, ByteRange::new(0, a.start));
    owner.deliver_as(
        0,
        a,
        Provenance::Damaged,
        Some(&[archive[a.start as usize] ^ 0x5a]),
    );
    owner.deliver(0, ByteRange::new(a.end, b.start));
    owner.deliver_as(
        0,
        b,
        Provenance::Damaged,
        Some(&[archive[b.start as usize] ^ 0x5a]),
    );
    owner.deliver(0, ByteRange::new(b.end, archive.len() as u64));
    let agains = |said: &[Emit]| -> Vec<ByteRange> {
        said.iter()
            .filter_map(|emit| match emit {
                Emit::Again { range, .. } => Some(*range),
                _ => None,
            })
            .collect()
    };
    let first = owner.said.len();
    owner.deliver_as(0, a, Provenance::Repaired, None);
    assert_eq!(
        agains(&owner.said[first..]),
        vec![ByteRange::new(a.start, end)]
    );
    let second = owner.said.len();
    owner.deliver_as(0, b, Provenance::Repaired, None);
    assert_eq!(
        agains(&owner.said[second..]),
        vec![ByteRange::new(b.start, end)]
    );
    assert!(
        !owner.said[first..]
            .iter()
            .any(|emit| matches!(emit, Emit::Retract(_) | Emit::NeedsPassword)),
        "{:?}",
        &owner.said[first..]
    );
    assert!(owner.member("alpha.bin").bytes == plain);
    assert!(owner.member("alpha.bin").closed);
    assert!(owner.posted(0) == Some(archive));
}

// A range in the middle of a closed member uncovered and said again: the
// keys after it come from it, so uncovering it uncovers the tail after it
// through the member's end, which is read back under the keys it was made
// with and said again with it, and the member closes whole.
#[test]
fn a_partial_uncover_of_a_closed_member_replays_its_tail() {
    let plain = payload(88, 300_007);
    let archive = encrypted(&plain);
    let start = records::local_len(&archive).unwrap() + 12;
    let end = start + plain.len() as u64;
    let window = ByteRange::at(start + 100_000, 4096);
    let mut owner = Owner::single(Zip::new(), archive.clone());
    offer(&mut owner, PASSWORD);
    owner.whole(4096);
    assert!(owner.member("alpha.bin").closed);
    assert_eq!(
        owner.forgets(&[(VolumeIndex(0), window)], false),
        vec![(VolumeIndex(0), ByteRange::new(window.start, end))]
    );
    let replays = owner.replays;
    owner.again(&[(VolumeIndex(0), window)]);
    owner.close(0);
    assert!(owner.replays > replays);
    assert!(owner.object.members[0].closed);
    assert!(owner.member("alpha.bin").closed);
    assert!(owner.member("alpha.bin").bytes == plain);
    assert!(owner.posted(0) == Some(archive));
}

// The ciphertext of `archive` with one article's bytes wrong, delivered
// with the right copy still to come: the tail after it is decrypted under
// keys fed the wrong plaintext.
fn sent_back(seed: u64) -> (Vec<u8>, Vec<u8>, ByteRange, Owner) {
    let plain = payload(seed, 300_007);
    let archive = encrypted(&plain);
    let start = records::local_len(&archive).unwrap() + 12;
    let window = ByteRange::at(start + 100_000, 4096);
    let wrong: Vec<u8> = archive[window.start as usize..window.end as usize]
        .iter()
        .map(|byte| byte ^ 0x5a)
        .collect();
    let mut owner = Owner::single(Zip::new(), archive.clone());
    owner.pull = Pull::Unseekable;
    offer(&mut owner, PASSWORD);
    owner.deliver(0, ByteRange::new(0, window.start));
    owner.deliver_as(0, window, Provenance::Fresh, Some(&wrong));
    owner.deliver(0, ByteRange::new(window.end, archive.len() as u64));
    assert!(owner.member("alpha.bin").bytes != plain);
    (plain, archive, window, owner)
}

// An article sent back for another copy: the tail after it, read back
// under the keys it was made with before the article is uncovered, waits
// for the right copy, and is said again right once it comes.
#[test]
fn a_sent_back_article_corrects_the_tail_after_it() {
    let (plain, archive, window, mut owner) = sent_back(89);
    let start = records::local_len(&archive).unwrap() + 12;
    owner.send_back(VolumeIndex(0), window);
    // The window and the tail after it are no longer said, and the tail
    // waits, read back, for the keys the right copy makes.
    let member = ByteRange::new(window.start - start, plain.len() as u64);
    assert!(
        owner
            .member("alpha.bin")
            .coverage
            .overlapping(member)
            .next()
            .is_none()
    );
    assert!(
        owner.object.members[0]
            .coverage
            .overlapping(member)
            .next()
            .is_none()
    );
    assert!(!owner.member("alpha.bin").closed);
    owner.deliver(0, window);
    owner.close(0);
    assert!(owner.member("alpha.bin").bytes == plain);
    assert!(owner.member("alpha.bin").closed);
    assert!(owner.posted(0) == Some(archive));
}

// A restart between an article sent back and its other copy: what the
// tail's uncover forgot is in the facts, so the copy that comes after the
// restore says the tail again, and the member closes right.
#[test]
fn a_restart_before_a_sent_back_article_comes_again_keeps_the_tail() {
    let (plain, archive, window, mut owner) = sent_back(90);
    owner.send_back(VolumeIndex(0), window);
    owner.restore();
    offer(&mut owner, PASSWORD);
    owner.deliver(0, window);
    owner.close(0);
    assert!(owner.member("alpha.bin").bytes == plain);
    assert!(owner.member("alpha.bin").closed);
    assert!(owner.posted(0) == Some(archive));
}

// Ciphertext held past a gap the keys wait at reads back through a posting
// of the whole volume as through one of its own range.
#[test]
fn a_whole_volume_posting_reads_held_ciphertext_past_the_frontier() {
    let plain = payload(88, 300_007);
    let archive = encrypted(&plain);
    let start = records::local_len(&archive).unwrap() + 12;
    let gap = ByteRange::at(start + 100_000, 4096);
    let mut owner = Owner::single(Zip::new(), archive.clone());
    owner.pull = Pull::Unseekable;
    offer(&mut owner, PASSWORD);
    owner.deliver(0, ByteRange::new(0, gap.start));
    owner.deliver(0, ByteRange::new(gap.end, archive.len() as u64));
    let after = ByteRange::at(gap.end + 10_000, 4096);
    let expected = &archive[after.start as usize..after.end as usize];
    let mut alone = Vec::new();
    assert!(owner.read_posted(VolumeIndex(0), after, &mut alone));
    assert!(alone == expected);
    let whole = ByteRange::new(0, archive.len() as u64);
    let posting = owner.object.posting(&[(VolumeIndex(0), whole)]);
    let mut through = Vec::new();
    assert!(owner.read_through_posting(posting.as_ref(), VolumeIndex(0), after, &mut through));
    assert!(through == expected);
    // A run from the front across the frontier into the held bytes.
    let across = ByteRange::new(gap.start - 4096, gap.start);
    let mut front = Vec::new();
    assert!(owner.read_through_posting(posting.as_ref(), VolumeIndex(0), across, &mut front));
    assert!(front == archive[across.start as usize..across.end as usize]);
}

// Bytes said already that come again the same are tried without a copy of
// the member's keys and checkpoints, and kept in place.
#[test]
fn a_covered_redelivery_tries_its_bytes_without_copying_the_keys() {
    let plain = payload(90, 300_007);
    let archive = encrypted(&plain);
    let start = records::local_len(&archive).unwrap() + 12;
    let mut owner = Owner::single(Zip::new(), archive.clone());
    offer(&mut owner, PASSWORD);
    owner.whole(4096);
    assert!(owner.member("alpha.bin").closed);
    let keys = |owner: &Owner| {
        let map = owner.object.map.as_ref().unwrap();
        let cipher = map.entry(MemberId(0)).unwrap().cipher.as_ref().unwrap();
        std::sync::Arc::as_ptr(cipher.open.as_ref().unwrap())
    };
    let before = keys(&owner);
    owner.deliver(0, ByteRange::at(start + 150_000, 4096));
    assert_eq!(keys(&owner), before);
    assert!(owner.member("alpha.bin").bytes == plain);
    assert!(owner.posted(0) == Some(archive));
}

// A posting over several runs of one member seals them all through one
// share of its coverage and keys, and makes no other candidate's keys
// while the open ones prove the member's front.
#[test]
fn a_posting_shares_one_members_keys_and_tries_no_other_candidate() {
    let plain = payload(91, 300_007);
    let archive = encrypted(&plain);
    let start = records::local_len(&archive).unwrap() + 12;
    let mut owner = Owner::single(Zip::new(), archive.clone());
    offer(&mut owner, &false_password(&archive));
    offer(&mut owner, PASSWORD);
    owner.whole(4096);
    assert!(owner.member("alpha.bin").closed);
    let runs = [
        ByteRange::at(start + 10_000, 4096),
        ByteRange::at(start + 200_000, 4096),
    ];
    let posting = owner
        .object
        .posted_over(&runs.map(|range| (VolumeIndex(0), range)));
    assert_eq!(posting.sealed.len(), 2);
    assert!(std::sync::Arc::ptr_eq(
        &posting.sealed[0].keys,
        &posting.sealed[1].keys
    ));
    for range in runs.into_iter().rev() {
        let mut bytes = Vec::new();
        assert!(owner.read_through_posting(&posting, VolumeIndex(0), range, &mut bytes));
        assert!(bytes == archive[range.start as usize..range.end as usize]);
    }
    assert!(posting.sealed[0].keys.alternatives.get().is_none());
}

// A repair in the member's last bytes lies inside the tail the end record
// was searched for in, but in no record the directory read: the map stands,
// and the repair replays from its byte once.
#[test]
fn a_repair_in_the_tail_search_window_keeps_the_map() {
    let plain = payload(88, 300_007);
    let archive = encrypted(&plain);
    let start = records::local_len(&archive).unwrap() + 12;
    let end = start + plain.len() as u64;
    let b = ByteRange::at(start + 250_000, 1);
    assert!(b.start + records::TAIL > archive.len() as u64);
    let mut owner = Owner::single(Zip::new(), archive.clone());
    owner.pull = Pull::Unseekable;
    offer(&mut owner, PASSWORD);
    owner.deliver(0, ByteRange::new(0, b.start));
    owner.deliver_as(
        0,
        b,
        Provenance::Damaged,
        Some(&[archive[b.start as usize] ^ 0x5a]),
    );
    owner.deliver(0, ByteRange::new(b.end, archive.len() as u64));
    let first = owner.said.len();
    owner.deliver_as(0, b, Provenance::Repaired, None);
    let said = &owner.said[first..];
    let agains: Vec<ByteRange> = said
        .iter()
        .filter_map(|emit| match emit {
            Emit::Again { range, .. } => Some(*range),
            _ => None,
        })
        .collect();
    assert_eq!(agains, vec![ByteRange::new(b.start, end)], "{said:?}");
    assert!(
        !said
            .iter()
            .any(|emit| matches!(emit, Emit::Retract(_) | Emit::NeedsPassword)),
        "{said:?}"
    );
    owner.close(0);
    assert!(owner.member("alpha.bin").bytes == plain);
    assert!(owner.member("alpha.bin").closed);
    assert!(owner.posted(0) == Some(archive));
}

// A second repair that comes before the first one's replay joins it: the
// one replay from the earlier repair carries both.
#[test]
fn zipcrypto_repairs_before_their_replay_are_replayed_once() {
    use weaver_graph::{Delivery, Evidenced, Placement};
    let plain = payload(108, 300_007);
    let archive = encrypted(&plain);
    let start = records::local_len(&archive).unwrap() + 12;
    let a = ByteRange::at(start + 20_000, 1);
    let b = ByteRange::at(start + 150_000, 1);
    let mut owner = Owner::single(Zip::new(), archive.clone());
    owner.pull = Pull::Unseekable;
    offer(&mut owner, PASSWORD);
    owner.deliver(0, ByteRange::new(0, a.start));
    owner.deliver_as(
        0,
        a,
        Provenance::Damaged,
        Some(&[archive[a.start as usize] ^ 0x5a]),
    );
    owner.deliver(0, ByteRange::new(a.end, b.start));
    owner.deliver_as(
        0,
        b,
        Provenance::Damaged,
        Some(&[archive[b.start as usize] ^ 0x5a]),
    );
    owner.deliver(0, ByteRange::new(b.end, archive.len() as u64));
    let previous = owner.member("alpha.bin").bytes.clone();
    let repair = |owner: &mut Owner, range: ByteRange| {
        let mut out = Vec::new();
        owner.object.deliver(
            Delivery {
                volume: VolumeIndex(0),
                offset: range.start,
                bytes: &Evidenced::from(archive[range.start as usize..range.end as usize].to_vec()),
                provenance: Provenance::Repaired,
            },
            &mut |placement, into| {
                if let Placement::Member { range, .. } = placement {
                    into.extend_from_slice(&previous[range.start as usize..range.end as usize]);
                    true
                } else {
                    false
                }
            },
            &mut out,
        );
        owner
            .delivered
            .get_mut(&0)
            .unwrap()
            .write(range, Provenance::Repaired);
        out
    };
    let first = repair(&mut owner, a);
    let second = repair(&mut owner, b);
    assert!(first.iter().any(|emit| matches!(emit, Emit::Again { .. })));
    assert!(
        !second.iter().any(|emit| matches!(emit, Emit::Again { .. })),
        "{second:?}"
    );
    owner.apply(first);
    owner.apply(second);
    owner.close(0);
    assert!(owner.member("alpha.bin").bytes == plain, "{:?}", owner.said);
    assert!(owner.member("alpha.bin").closed);
    assert!(owner.posted(0) == Some(archive));
    let cipher = owner
        .object
        .map
        .as_ref()
        .unwrap()
        .entry(MemberId(0))
        .unwrap();
    assert!(cipher.cipher.as_ref().unwrap().pending.is_empty());
}
