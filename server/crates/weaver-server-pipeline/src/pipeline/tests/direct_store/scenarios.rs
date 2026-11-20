// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Scenario fixtures shared by the direct-store test leaves below, plus
// the leaves themselves. Every leaf sees this module and its parent through
// `use super::*`.

use super::*;

pub(in crate::pipeline::tests) mod archive_schedules;

pub(in crate::pipeline::tests) mod sevenz_store;

// A RAR5 `FHEXTRA_CRYPT` record: `vint(size) || vint(type=1) || body`.
//
// The body is the format's: version, flags, the KDF count as a raw byte, the
// 16-byte salt, the 16-byte IV, and — when the flags claim one — the 8-byte
// password check followed by the first four bytes of its SHA-256, which is the
// tag the parser validates before it will hand the value to anyone.
fn build_test_rar_crypt_extra(psw_check: Option<&[u8; 8]>, keyed_checksum: bool) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&encode_test_rar_vint(0));
    let mut flags = 0u64;
    if psw_check.is_some() {
        flags |= 0x0001;
    }
    if keyed_checksum {
        flags |= 0x0002;
    }
    body.extend_from_slice(&encode_test_rar_vint(flags));
    body.push(TEST_CRYPT_KDF_LG2);
    body.extend_from_slice(&TEST_CRYPT_SALT);
    body.extend_from_slice(&TEST_CRYPT_IV);
    if let Some(check) = psw_check {
        body.extend_from_slice(check);
        let digest = <sha2::Sha256 as sha2::Digest>::digest(check);
        body.extend_from_slice(&digest[..4]);
    }
    let type_bytes = encode_test_rar_vint(1);
    let mut record = encode_test_rar_vint((type_bytes.len() + body.len()) as u64);
    record.extend_from_slice(&type_bytes);
    record.extend_from_slice(&body);
    record
}

// Split points that are deliberately **off** every 16-byte boundary.
//
// A real `rar -v` split lands wherever the volume filled up, and an encrypted
// member's parts are not individually block-aligned — only the member's total
// is. Aligned fixtures would never exercise the straddling block, which is the
// one shape the cipher-block holds exist for.
fn misaligned_parts(total: usize, count: usize) -> Vec<usize> {
    assert!(count >= 1 && total >= count);
    let base = total / count;
    let mut parts = Vec::with_capacity(count);
    let mut used = 0usize;
    for index in 0..count - 1 {
        let len = (base + 1 + index * 3).min(total - used - (count - 1 - index));
        parts.push(len);
        used += len;
    }
    parts.push(total - used);
    assert_eq!(parts.iter().sum::<usize>(), total);
    parts
}

// One `-m0 -p` stored member split across `volume_count` volumes.
//
// Mirrors what `rar a -m0 -p<password> -v<size>` writes, which is the recipe
// `rarpar`'s `tests/fixtures/generate_stored_layout.sh` records: the member's
// whole plaintext as one AES-256-CBC stream running unbroken across the volume
// boundaries, `align16(unpacked_size)` cipher bytes in total, one crypt record
// per part, plain packed CRC32s on the non-final parts and the whole-member
// checksum on the last.
//
// - `data_password` encrypts the bytes.
// - `check_for` is whose password check the headers carry, or `None` for a
//   writer that omitted it. Passing a *different* password here forges a check
//   that admits the wrong one.
// - `keyed_checksum` sets `FHEXTRA_CRYPT`'s hash-MAC flag on the **final part
//   alone** and folds the whole-member CRC32 with that password's hash key,
//   which is the shape RARLAB `rar` 7.20 writes: only the last part's checksum
//   is the whole member's, so only that one is keyed.
pub(in crate::pipeline) fn encrypted_store_set(
    member_name: &str,
    payload: &[u8],
    volume_count: usize,
    data_password: &str,
    check_for: Option<&str>,
    keyed_checksum: bool,
) -> Vec<(String, Vec<u8>)> {
    encrypted_store_set_with_recovery(
        member_name,
        payload,
        volume_count,
        data_password,
        check_for,
        keyed_checksum,
        0,
    )
}

// [`encrypted_store_set`] with a recovery record after each volume's payload.
//
// The RR is what makes PAR2 the *only* layer that can see a damaged byte: it
// belongs to no member, so neither the per-part packed CRC32 over cipher nor
// the keyed whole-member fold over plaintext covers it, and it is a service
// block's data rather than a header, so the walk still parses and the volume
// still confirms. Every byte of it is envelope, posted in the clear, and
// routing carries it through untouched — which is exactly why a damaged one
// survives to the pass.
#[allow(clippy::too_many_arguments)]
fn encrypted_store_set_with_recovery(
    member_name: &str,
    payload: &[u8],
    volume_count: usize,
    data_password: &str,
    check_for: Option<&str>,
    keyed_checksum: bool,
    rr_bytes: usize,
) -> Vec<(String, Vec<u8>)> {
    let material =
        unrar_rs::derive_rar5_material(data_password, &TEST_CRYPT_SALT, TEST_CRYPT_KDF_LG2)
            .expect("the fixture KDF count is derivable");
    let key = material.key;
    let hash_key = material.hash_key;
    let psw_check = check_for.map(|password| {
        unrar_rs::derive_rar5_material(password, &TEST_CRYPT_SALT, TEST_CRYPT_KDF_LG2)
            .expect("the fixture KDF count is derivable")
            .psw_check
    });

    let cipher_len = payload.len().div_ceil(16) * 16;
    let mut padded = payload.to_vec();
    padded.resize(cipher_len, 0);
    let cipher = unrar_rs::test_support::encrypt_aes256_cbc(&key, &TEST_CRYPT_IV, &padded);

    let member_crc = checksum::crc32(payload);
    let member_crc = if keyed_checksum {
        unrar_rs::convert_crc32_to_mac(member_crc, &hash_key)
    } else {
        member_crc
    };

    let mut offset = 0usize;
    misaligned_parts(cipher_len, volume_count)
        .into_iter()
        .enumerate()
        .map(|(volume, part_len)| {
            let part = &cipher[offset..offset + part_len];
            offset += part_len;
            let is_first = volume == 0;
            let is_last = volume + 1 == volume_count;

            let mut split_flags = 0u64;
            if !is_first {
                split_flags |= 0x0008;
            }
            if !is_last {
                split_flags |= 0x0010;
            }
            // Layer 1 over cipher bytes, plain: the packed hash on a non-final
            // part covers the packed (= cipher) bytes and `rar` does not key it.
            let data_crc = if is_last {
                member_crc
            } else {
                checksum::crc32(part)
            };
            let extra = build_test_rar_crypt_extra(psw_check.as_ref(), keyed_checksum && is_last);

            let mut bytes = Vec::new();
            bytes.extend_from_slice(&TEST_RAR5_SIG);
            bytes.extend_from_slice(&build_test_rar_main_header(
                if is_first { 0x0001 } else { 0x0001 | 0x0002 },
                (!is_first).then_some(volume as u64),
            ));
            bytes.extend_from_slice(&build_test_rar_file_header_with_extra(
                member_name,
                split_flags,
                part.len() as u64,
                payload.len() as u64,
                Some(data_crc),
                &extra,
            ));
            bytes.extend_from_slice(part);
            if rr_bytes > 0 {
                bytes.extend_from_slice(&build_test_rar_service_header("RR", rr_bytes as u64));
                bytes.extend((0..rr_bytes).map(|index| ((index * 7 + volume * 13) % 256) as u8));
            }
            bytes.extend_from_slice(&build_test_rar_end_header(!is_last));

            (format!("silver.horizon.part{:02}.rar", volume + 1), bytes)
        })
        .collect()
}

// The corpus-wide RAR4 fixture salt. RAR salts each *file* rather than the
// archive, which is why the KDF tuple is per member here where RAR5's is per
// archive.
pub(in crate::pipeline::tests) const TEST_RAR4_SALT: [u8; 8] = [0x9B; 8];

// A stored, encrypted RAR4 file header: [`build_test_rar4_file_header`] plus
// the `ENCRYPTED`/`SALT` flags and the 8-byte salt the format appends after the
// filename.
//
// `unpack_version` 29 is what selects "RAR 3.0" encryption — AES-128-CBC. The
// three older values select ciphers `unrar-rs` refuses to classify as an
// encrypted store at all, which is asserted in the library rather than here.
fn build_test_rar4_encrypted_file_header(
    filename: &str,
    split_flags: u16,
    packed_size: u32,
    unpacked_size: u32,
    data_crc: u32,
    salt: Option<[u8; 8]>,
) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&packed_size.to_le_bytes());
    body.extend_from_slice(&unpacked_size.to_le_bytes());
    body.push(3); // host OS: Unix
    body.extend_from_slice(&data_crc.to_le_bytes());
    body.extend_from_slice(&0u32.to_le_bytes()); // mtime
    body.push(29); // unpack version: RAR 3.0, i.e. AES-128
    body.push(0x30); // method: store
    body.extend_from_slice(&(filename.len() as u16).to_le_bytes());
    body.extend_from_slice(&0o644u32.to_le_bytes());
    body.extend_from_slice(filename.as_bytes());
    let mut flags = 0x8000 | 0x0004 | split_flags;
    if let Some(salt) = salt {
        flags |= 0x0400;
        body.extend_from_slice(&salt);
    }
    build_test_rar4_block(0x74, flags, &body)
}

// The RAR4 twin of [`encrypted_store_set`]: one `-m0 -p` stored member split
// across `volume_count` volumes.
//
// Mirrors what `rar a -ma4 -m0 -p<password> -v<size>` writes, which is the
// recipe `rarpar`'s `tests/fixtures/generate_encrypted.sh` records for
// `rar4_enc_mv_store`: the member's whole plaintext as **one** AES-128-CBC
// stream running unbroken across the volume boundaries — a property held
// against that real archive in rarpar's
// `a_rar4_encrypted_chain_is_one_cbc_stream_keyed_by_its_file_salt` — with
// `align16(unpacked_size)` cipher bytes in total, the salt repeated on every
// part's header, plain packed CRC32s over cipher on the non-final parts and the
// whole-member CRC32 on the last.
//
// Two things a RAR5 fixture can carry and this one cannot, by construction:
//
// - **no password-check value.** RAR4 has no such field, so no `check_for`
//   parameter exists here and admission can never refute a password.
// - **no keyed checksum.** RAR4 has no hash-MAC flag, so the whole-member
//   CRC32 below is the bare plaintext CRC32 and `convert_crc32_to_mac` is not
//   applied to it. That is that finding stated as a fixture.
pub(in crate::pipeline::tests) fn encrypted_rar4_store_set(
    member_name: &str,
    payload: &[u8],
    volume_count: usize,
    data_password: &str,
    salt: Option<[u8; 8]>,
) -> Vec<(String, Vec<u8>)> {
    let (key, iv) = unrar_rs::rar4_derive_key(data_password, salt.as_ref());

    let cipher_len = payload.len().div_ceil(16) * 16;
    let mut cipher = payload.to_vec();
    cipher.resize(cipher_len, 0);
    // The public range API, not a `#[doc(hidden)]` test helper: the posted bytes
    // a fixture claims to have been posted should come from the same surface the
    // overlay re-derives them with.
    unrar_rs::encrypt_cipher_range_rar4(&key, &iv, &mut cipher)
        .expect("the padded payload is block-aligned");

    let member_crc = checksum::crc32(payload);
    let mut offset = 0usize;
    misaligned_parts(cipher_len, volume_count)
        .into_iter()
        .enumerate()
        .map(|(volume, part_len)| {
            let part = &cipher[offset..offset + part_len];
            offset += part_len;
            let is_first = volume == 0;
            let is_last = volume + 1 == volume_count;

            let mut split_flags = 0u16;
            if !is_first {
                split_flags |= 0x0001;
            }
            if !is_last {
                split_flags |= 0x0002;
            }

            let mut bytes = Vec::new();
            bytes.extend_from_slice(&TEST_RAR4_SIG);
            bytes.extend_from_slice(&build_test_rar4_main_header(is_first));
            bytes.extend_from_slice(&build_test_rar4_encrypted_file_header(
                member_name,
                split_flags,
                part.len() as u32,
                payload.len() as u32,
                if is_last {
                    member_crc
                } else {
                    checksum::crc32(part)
                },
                salt,
            ));
            bytes.extend_from_slice(part);
            bytes.extend_from_slice(&build_test_rar4_end_header_numbered(
                !is_last,
                volume as u16,
            ));

            (format!("silver.horizon.part{:02}.rar", volume + 1), bytes)
        })
        .collect()
}

// The archive-level KDF tuple every `-hp` fixture here shares. `lg2 = 4` for
// the reason [`TEST_CRYPT_KDF_LG2`] is: real archives use 2^15 and up, and
// nothing under test reads the number.
const TEST_HP_SALT: [u8; 16] = [0x3C; 16];

const TEST_HP_KDF_LG2: u8 = 4;

// What a fixture's type-4 record claims about its password check.
#[derive(Clone, Copy)]
pub(in crate::pipeline::tests) enum HeaderCheck {
    // A check the parser will validate and hand out: the flag set, eight check
    // bytes from this password's KDF, and their real SHA-256 tag. What WinRAR
    // writes by default, and the only shape `-hp` admission accepts.
    For(&'static str),
    // The flag clear and no field at all — a writer that omitted it. Legal, and
    // unprovable: nothing here can distinguish a right password from a wrong
    // one.
    Absent,
}

// A RAR5 type-4 archive encryption header — plaintext, first thing after the
// signature, exactly as `-hp` writes it.
//
// Body: version, flags, the KDF count as a raw byte, the 16-byte salt, and —
// when the flags claim one — the 8-byte password check followed by the first
// four bytes of its SHA-256.
fn build_test_rar_crypt_header(kdf_lg2: u8, check: HeaderCheck) -> Vec<u8> {
    let checked = |password: &str| {
        unrar_rs::derive_rar5_material(password, &TEST_HP_SALT, kdf_lg2)
            .expect("the fixture KDF count is derivable")
            .psw_check
    };
    let mut type_body = Vec::new();
    type_body.extend_from_slice(&encode_test_rar_vint(0)); // AES-256.
    type_body.extend_from_slice(&encode_test_rar_vint(u64::from(!matches!(
        check,
        HeaderCheck::Absent
    ))));
    type_body.push(kdf_lg2);
    type_body.extend_from_slice(&TEST_HP_SALT);
    match check {
        HeaderCheck::Absent => {}
        HeaderCheck::For(password) => {
            let value = checked(password);
            type_body.extend_from_slice(&value);
            type_body.extend_from_slice(&<sha2::Sha256 as sha2::Digest>::digest(value)[..4]);
        }
    }
    build_test_rar_header(4, 0, &type_body, &[])
}

// Wraps one plaintext header the way `-hp` stores it:
// `[16-byte IV][AES-256-CBC(header padded to 16)]`.
//
// The padding's content is irrelevant to the parser — the CRC covers the size
// vint and the body only, and the body is read by its declared length — which
// is why zeros are as faithful as random bytes here.
fn seal_test_rar_header(key: &[u8; 32], iv: &[u8; 16], header: &[u8]) -> Vec<u8> {
    let mut block = header.to_vec();
    block.resize(header.len().div_ceil(16) * 16, 0);
    unrar_rs::encrypt_cipher_range(key, iv, &mut block)
        .expect("the padded header is block-aligned");
    let mut out = iv.to_vec();
    out.extend_from_slice(&block);
    out
}

// One `-hp -m0` stored member split across `volume_count` volumes.
//
// Mirrors what `rar a -m0 -hp<password> -v<size>` writes: the type-4 record in
// the clear at the front of every volume, every header after it AES-256-CBC
// under the archive key with its own inline IV, and the member's data area
// **not** header-encrypted — it is the ordinary `-p` cipher stream, one
// unbroken AES-CBC run across the volume boundaries, keyed from the file
// header's own `FHEXTRA_CRYPT` record. `-hp` is `-p` plus encrypted headers,
// and one password opens both.
//
// - `password` keys everything: the headers, the file data, and the checks.
// - `check` is what the *archive-level* record claims, which is what `-hp`
//   admission is decided on. The member-level check is always this password's,
//   because a member that refuted it would be testing the admission gate
//   rather than this one.
pub(in crate::pipeline::tests) fn header_encrypted_store_set(
    member_name: &str,
    payload: &[u8],
    volume_count: usize,
    password: &'static str,
    check: HeaderCheck,
) -> Vec<(String, Vec<u8>)> {
    header_encrypted_store_set_with_records(
        member_name,
        payload,
        volume_count,
        (password, check),
        |_| None,
    )
}

// [`header_encrypted_store_set`] with a RAR5 recovery record of `parity`
// units in each volume over every byte before its `RR` header, as
// `rar a -m0 -hp -rr` writes one: the header is enciphered with the rest,
// the record's chunks are not.
pub(in crate::pipeline::tests) fn protected_header_encrypted_store_set(
    member_name: &str,
    payload: &[u8],
    volume_count: usize,
    password: &'static str,
    parity: u16,
) -> Vec<(String, Vec<u8>)> {
    header_encrypted_store_set_with_records(
        member_name,
        payload,
        volume_count,
        (password, HeaderCheck::For(password)),
        |protected| Some(weaver_objects::rar_record::encode::rar5(protected, parity)),
    )
}

// [`header_encrypted_store_set`], each volume carrying the record `record`
// makes of its bytes before the record's header, when it makes one.
fn header_encrypted_store_set_with_records(
    member_name: &str,
    payload: &[u8],
    volume_count: usize,
    (password, check): (&'static str, HeaderCheck),
    record: impl Fn(&[u8]) -> Option<Vec<u8>>,
) -> Vec<(String, Vec<u8>)> {
    let header_key = unrar_rs::derive_rar5_material(password, &TEST_HP_SALT, TEST_HP_KDF_LG2)
        .expect("the fixture KDF count is derivable")
        .key;
    let member = unrar_rs::derive_rar5_material(password, &TEST_CRYPT_SALT, TEST_CRYPT_KDF_LG2)
        .expect("the fixture KDF count is derivable");

    let cipher_len = payload.len().div_ceil(16) * 16;
    let mut cipher = payload.to_vec();
    cipher.resize(cipher_len, 0);
    unrar_rs::encrypt_cipher_range(&member.key, &TEST_CRYPT_IV, &mut cipher)
        .expect("the padded payload is block-aligned");
    let member_crc = unrar_rs::convert_crc32_to_mac(checksum::crc32(payload), &member.hash_key);

    let mut offset = 0usize;
    misaligned_parts(cipher_len, volume_count)
        .into_iter()
        .enumerate()
        .map(|(volume, part_len)| {
            let part = &cipher[offset..offset + part_len];
            offset += part_len;
            let is_first = volume == 0;
            let is_last = volume + 1 == volume_count;

            let mut split_flags = 0u64;
            if !is_first {
                split_flags |= 0x0008;
            }
            if !is_last {
                split_flags |= 0x0010;
            }
            let data_crc = if is_last {
                member_crc
            } else {
                checksum::crc32(part)
            };
            let extra = build_test_rar_crypt_extra(Some(&member.psw_check), is_last);

            // A distinct IV per header, as a real writer emits.
            let mut iv = [0u8; 16];
            let seal = |index: u8, iv: &mut [u8; 16], header: &[u8]| {
                iv.fill(
                    0x40u8
                        .wrapping_add(index)
                        .wrapping_add((volume as u8).wrapping_mul(8)),
                );
                seal_test_rar_header(&header_key, iv, header)
            };

            let mut bytes = Vec::new();
            bytes.extend_from_slice(&TEST_RAR5_SIG);
            bytes.extend_from_slice(&build_test_rar_crypt_header(TEST_HP_KDF_LG2, check));
            bytes.extend_from_slice(&seal(
                0,
                &mut iv,
                &build_test_rar_main_header(
                    if is_first { 0x0001 } else { 0x0001 | 0x0002 },
                    (!is_first).then_some(volume as u64),
                ),
            ));
            bytes.extend_from_slice(&seal(
                1,
                &mut iv,
                &build_test_rar_file_header_with_extra(
                    member_name,
                    split_flags,
                    part.len() as u64,
                    payload.len() as u64,
                    Some(data_crc),
                    &extra,
                ),
            ));
            bytes.extend_from_slice(part);
            if let Some(record) = record(&bytes) {
                bytes.extend_from_slice(&seal(
                    3,
                    &mut iv,
                    &build_test_rar_service_header("RR", record.len() as u64),
                ));
                bytes.extend(record);
            }
            bytes.extend_from_slice(&seal(2, &mut iv, &build_test_rar_end_header(!is_last)));

            (format!("silver.horizon.part{:02}.rar", volume + 1), bytes)
        })
        .collect()
}
