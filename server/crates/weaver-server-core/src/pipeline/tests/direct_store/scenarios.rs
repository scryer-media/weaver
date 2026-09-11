//! Scenario fixtures shared by the direct-store test leaves below, plus
//! the leaves themselves. Every leaf sees this module and its parent through
//! `use super::*`.

use super::*;

mod classification_frontier;
#[cfg(unix)]
mod cross_device;
mod cross_device_probe;
mod header_encrypted_parse_cost;
mod quick_open;
mod rar4_rar3_file_encryption;
mod repaired_encrypted_spans;
mod restart;
mod uu_isolation;
mod waiting_for_targeted_recovery;

/// [`run_repairable_par2_gate`] with an article that never arrives.
///
/// A **lost article** is the only shape member-payload damage can reach PAR2
/// in. Corrupted member bytes are caught far earlier and far more cheaply by
/// direct-store's own gates — the per-part packed CRC32 at part completion, the
/// whole-member CRC32 at member completion — which demote the set during the
/// download, before a PAR2 index has even been parsed. What those gates cannot
/// do is *manufacture* bytes that never came, so a hole in a member's packed
/// range survives to the PAR2 pass, and repairing it is exactly what repair is
/// for.
async fn run_lost_article_gate(
    gate: DirectStoreGate,
    job_id: JobId,
    member_name: &str,
    volumes: &[(String, Vec<u8>)],
    par2_bytes: &[u8],
    lost: (u32, u32),
    password: Option<&str>,
) -> RepairGateOutcome {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(gate);

    let (mut spec, index_file_index) = par2_bearing_job_spec("Silver Horizon", volumes, par2_bytes);
    spec.password = password.map(str::to_owned);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    let mut volume_file_seen = false;
    for (file_index, segment_number) in in_order_arrivals(volumes.len()) {
        if (file_index, segment_number) == lost {
            continue;
        }
        submit_volume_article(&mut pipeline, job_id, volumes, file_index, segment_number).await;
        volume_file_seen |= volumes
            .iter()
            .any(|(filename, _)| working_dir.join(filename).exists());
    }
    submit_decoded_segment(
        &mut pipeline,
        NzbFileId {
            job_id,
            file_index: index_file_index,
        },
        0,
        0,
        par2_bytes,
        "silver.horizon.par2",
        None,
    )
    .await;
    let mut sets = format!("{:?}", pipeline.direct_store.sets_for(job_id));

    // The lost article is never coming, and the harness has no server to say so
    // — draining the queues is what makes the download pipeline look exhausted,
    // which is the condition every PAR2 gate waits for before treating a hole as
    // damage rather than as work in flight.
    if let Some(state) = pipeline.jobs.get_mut(&job_id) {
        state.download_queue = crate::DownloadQueue::new();
        state.recovery_queue = crate::DownloadQueue::new();
    }
    for _ in 0..48 {
        if matches!(
            job_status_for_assert(&pipeline, job_id),
            Some(JobStatus::Complete) | Some(JobStatus::Failed { .. })
        ) {
            break;
        }
        drain_rar_refreshes(&mut pipeline).await;
        pipeline.check_job_completion(job_id).await;
        sample_direct_sets(&pipeline, job_id, &mut sets);
        pump_pipeline_runtime_queues(&mut pipeline).await;
        sample_direct_sets(&pipeline, job_id, &mut sets);
        settle_inflight_moves(&mut pipeline).await;
        if let Ok(Some(done)) = tokio::time::timeout(
            std::time::Duration::from_millis(250),
            pipeline.extract_done_rx.recv(),
        )
        .await
        {
            pipeline.handle_extraction_done(done).await;
            pump_pipeline_runtime_queues(&mut pipeline).await;
            settle_inflight_moves(&mut pipeline).await;
        }
        volume_file_seen |= volumes
            .iter()
            .any(|(filename, _)| working_dir.join(filename).exists());
        sample_direct_sets(&pipeline, job_id, &mut sets);
    }

    let output_root =
        complete_dir.join(crate::jobs::working_dir::sanitize_dirname("Silver Horizon"));
    RepairGateOutcome {
        status: job_status_for_assert(&pipeline, job_id),
        member: std::fs::read(output_root.join(member_name))
            .ok()
            .or_else(|| staging_member(&complete_dir, member_name))
            .or_else(|| std::fs::read(working_dir.join(member_name)).ok()),
        volume_file_seen,
        repair_scratch_left: direct_scratch_left(&working_dir),
        sets,
        materialized: pipeline.direct_store.repair_materialized_volumes,
        finalized: pipeline.direct_store.finalized_sets,
        verify_read_splits: pipeline.direct_verify_read_splits.clone(),
    }
}

/// A RAR5 `FHEXTRA_CRYPT` record: `vint(size) || vint(type=1) || body`.
///
/// The body is the format's: version, flags, the KDF count as a raw byte, the
/// 16-byte salt, the 16-byte IV, and — when the flags claim one — the 8-byte
/// password check followed by the first four bytes of its SHA-256, which is the
/// tag the parser validates before it will hand the value to anyone.
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

/// Split points that are deliberately **off** every 16-byte boundary.
///
/// A real `rar -v` split lands wherever the volume filled up, and an encrypted
/// member's parts are not individually block-aligned — only the member's total
/// is. Aligned fixtures would never exercise the straddling block, which is the
/// one shape the cipher-block holds exist for.
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

/// One `-m0 -p` stored member split across `volume_count` volumes.
///
/// Mirrors what `rar a -m0 -p<password> -v<size>` writes, which is the recipe
/// `rarpar`'s `tests/fixtures/generate_stored_layout.sh` records: the member's
/// whole plaintext as one AES-256-CBC stream running unbroken across the volume
/// boundaries, `align16(unpacked_size)` cipher bytes in total, one crypt record
/// per part, plain packed CRC32s on the non-final parts and the whole-member
/// checksum on the last.
///
/// - `data_password` encrypts the bytes.
/// - `check_for` is whose password check the headers carry, or `None` for a
///   writer that omitted it. Passing a *different* password here forges a check
///   that admits the wrong one.
/// - `keyed_checksum` sets `FHEXTRA_CRYPT`'s hash-MAC flag on the **final part
///   alone** and folds the whole-member CRC32 with that password's hash key,
///   which is the shape RARLAB `rar` 7.20 writes: only the last part's checksum
///   is the whole member's, so only that one is keyed.
fn encrypted_store_set(
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

/// [`encrypted_store_set`] with a recovery record after each volume's payload.
///
/// The RR is what makes PAR2 the *only* layer that can see a damaged byte: it
/// belongs to no member, so neither the per-part packed CRC32 over cipher nor
/// the keyed whole-member fold over plaintext covers it, and it is a service
/// block's data rather than a header, so the walk still parses and the volume
/// still confirms. Every byte of it is envelope, posted in the clear, and
/// routing carries it through untouched — which is exactly why a damaged one
/// survives to the pass.
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

/// What one encrypted job's articles left behind, **without** driving
/// extraction to a terminal state.
///
/// Used where the point is what the router decided. Deliberately not the full
/// gate: a job whose password is wrong never reaches a terminal extraction on
/// *either* path — the archive cannot be opened, so the conventional side sits
/// in `Downloading` exactly as the demoted side does — and a helper that waited
/// for terminality would spend minutes proving it.
struct EncryptedRoutingOutcome {
    /// The direct sets' debug shape, which carries the demotion reason.
    shape: String,
    /// Whether any source volume was materialized, i.e. whether the demotion
    /// handed the bytes to the conventional path rather than dropping them.
    volume_file_seen: bool,
    /// Whether any `.direct.partial` exists.
    partial_seen: bool,
}

async fn encrypted_routing_outcome(
    job_id: JobId,
    volumes: &[(String, Vec<u8>)],
    arrivals: &[(u32, u32)],
    password: Option<&str>,
) -> EncryptedRoutingOutcome {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let mut spec = direct_store_job_spec("Silver Horizon", volumes);
    spec.password = password.map(str::to_owned);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    for (file_index, segment_number) in arrivals {
        submit_volume_article(&mut pipeline, job_id, volumes, *file_index, *segment_number).await;
    }
    drain_rar_refreshes(&mut pipeline).await;
    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    let volume_file_seen = volumes
        .iter()
        .any(|(filename, _)| working_dir.join(filename).exists());
    let partial_seen = any_direct_partial(&payload_root(&temp_dir, job_id));
    EncryptedRoutingOutcome {
        shape,
        volume_file_seen,
        partial_seen,
    }
}

/// The corpus-wide RAR4 fixture salt. RAR salts each *file* rather than the
/// archive, which is why the KDF tuple is per member here where RAR5's is per
/// archive.
const TEST_RAR4_SALT: [u8; 8] = [0x9B; 8];

/// A stored, encrypted RAR4 file header: [`build_test_rar4_file_header`] plus
/// the `ENCRYPTED`/`SALT` flags and the 8-byte salt the format appends after the
/// filename.
///
/// `unpack_version` 29 is what selects "RAR 3.0" encryption — AES-128-CBC. The
/// three older values select ciphers `unrar-rs` refuses to classify as an
/// encrypted store at all, which is asserted in the library rather than here.
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

/// The RAR4 twin of [`encrypted_store_set`]: one `-m0 -p` stored member split
/// across `volume_count` volumes.
///
/// Mirrors what `rar a -ma4 -m0 -p<password> -v<size>` writes, which is the
/// recipe `rarpar`'s `tests/fixtures/generate_encrypted.sh` records for
/// `rar4_enc_mv_store`: the member's whole plaintext as **one** AES-128-CBC
/// stream running unbroken across the volume boundaries — a property held
/// against that real archive in rarpar's
/// `a_rar4_encrypted_chain_is_one_cbc_stream_keyed_by_its_file_salt` — with
/// `align16(unpacked_size)` cipher bytes in total, the salt repeated on every
/// part's header, plain packed CRC32s over cipher on the non-final parts and the
/// whole-member CRC32 on the last.
///
/// Two things a RAR5 fixture can carry and this one cannot, by construction:
///
/// - **no password-check value.** RAR4 has no such field, so no `check_for`
///   parameter exists here and admission can never refute a password.
/// - **no keyed checksum.** RAR4 has no hash-MAC flag, so the whole-member
///   CRC32 below is the bare plaintext CRC32 and `convert_crc32_to_mac` is not
///   applied to it. That is that finding stated as a fixture.
fn encrypted_rar4_store_set(
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

/// The archive-level KDF tuple every `-hp` fixture here shares. `lg2 = 4` for
/// the reason [`TEST_CRYPT_KDF_LG2`] is: real archives use 2^15 and up, and
/// nothing under test reads the number.
const TEST_HP_SALT: [u8; 16] = [0x3C; 16];

const TEST_HP_KDF_LG2: u8 = 4;

/// What a fixture's type-4 record claims about its password check.
#[derive(Clone, Copy)]
enum HeaderCheck {
    /// A check the parser will validate and hand out: the flag set, eight check
    /// bytes from this password's KDF, and their real SHA-256 tag. What WinRAR
    /// writes by default, and the only shape `-hp` admission accepts.
    For(&'static str),
    /// The flag clear and no field at all — a writer that omitted it. Legal, and
    /// unprovable: nothing here can distinguish a right password from a wrong
    /// one.
    Absent,
    /// The flag set, twelve bytes present, and a tag that is **not** their
    /// SHA-256.
    ///
    /// The hostile shape, and the one that matters most: such a value refutes
    /// *no* password, so a router that read it as a check would find its very
    /// first candidate "verified" and hand a wrong key to the header parse.
    /// `header::encryption::parse` degrades it to `None` before anyone can, and
    /// `None` reads as `Unverifiable`.
    ForgedTag(&'static str),
}

/// A RAR5 type-4 archive encryption header — plaintext, first thing after the
/// signature, exactly as `-hp` writes it.
///
/// Body: version, flags, the KDF count as a raw byte, the 16-byte salt, and —
/// when the flags claim one — the 8-byte password check followed by the first
/// four bytes of its SHA-256.
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
        HeaderCheck::ForgedTag(password) => {
            type_body.extend_from_slice(&checked(password));
            // Four bytes that are not anyone's SHA-256 prefix.
            type_body.extend_from_slice(&[0u8; 4]);
        }
    }
    build_test_rar_header(4, 0, &type_body, &[])
}

/// Wraps one plaintext header the way `-hp` stores it:
/// `[16-byte IV][AES-256-CBC(header padded to 16)]`.
///
/// The padding's content is irrelevant to the parser — the CRC covers the size
/// vint and the body only, and the body is read by its declared length — which
/// is why zeros are as faithful as random bytes here.
fn seal_test_rar_header(key: &[u8; 32], iv: &[u8; 16], header: &[u8]) -> Vec<u8> {
    let mut block = header.to_vec();
    block.resize(header.len().div_ceil(16) * 16, 0);
    unrar_rs::encrypt_cipher_range(key, iv, &mut block)
        .expect("the padded header is block-aligned");
    let mut out = iv.to_vec();
    out.extend_from_slice(&block);
    out
}

/// One `-hp -m0` stored member split across `volume_count` volumes.
///
/// Mirrors what `rar a -m0 -hp<password> -v<size>` writes: the type-4 record in
/// the clear at the front of every volume, every header after it AES-256-CBC
/// under the archive key with its own inline IV, and the member's data area
/// **not** header-encrypted — it is the ordinary `-p` cipher stream, one
/// unbroken AES-CBC run across the volume boundaries, keyed from the file
/// header's own `FHEXTRA_CRYPT` record. `-hp` is `-p` plus encrypted headers,
/// and one password opens both.
///
/// - `password` keys everything: the headers, the file data, and the checks.
/// - `check` is what the *archive-level* record claims, which is what `-hp`
///   admission is decided on. The member-level check is always this password's,
///   because a member that refuted it would be testing the admission gate
///   rather than this one.
fn header_encrypted_store_set(
    member_name: &str,
    payload: &[u8],
    volume_count: usize,
    password: &'static str,
    check: HeaderCheck,
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
                iv.fill(0x40u8.wrapping_add(index).wrapping_add(volume as u8 * 8));
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
            bytes.extend_from_slice(&seal(2, &mut iv, &build_test_rar_end_header(!is_last)));

            (format!("silver.horizon.part{:02}.rar", volume + 1), bytes)
        })
        .collect()
}

/// [`header_encrypted_store_set`] plus an unsplit, encrypted, BLAKE2sp-only
/// member in the closing volume — the shape that puts a **tolerated** member
/// inside an `-hp` set.
///
/// Both members are encrypted, because `-hp` encrypts the data as well as the
/// headers; what makes the extra one ineligible is that its header states a
/// BLAKE2sp digest and no CRC32, which `classify_stored_chain` answers with
/// `Blake2OnlyNoCrc32` on the encrypted path exactly as on the plaintext one.
/// Unsplit for the reason [`ToleranceExtra::Blake2OnlyStore`] gives: the
/// classifier only reaches the hash fields once the chain closes, so a split
/// one would route into a partial before resolving.
fn header_encrypted_store_set_with_extra_member(
    member_name: &str,
    payload: &[u8],
    extra_name: &str,
    extra_payload: &[u8],
    volume_count: usize,
    password: &'static str,
    check: HeaderCheck,
) -> Vec<(String, Vec<u8>)> {
    let header_key = unrar_rs::derive_rar5_material(password, &TEST_HP_SALT, TEST_HP_KDF_LG2)
        .expect("the fixture KDF count is derivable")
        .key;
    let member = unrar_rs::derive_rar5_material(password, &TEST_CRYPT_SALT, TEST_CRYPT_KDF_LG2)
        .expect("the fixture KDF count is derivable");

    let mut volumes =
        header_encrypted_store_set(member_name, payload, volume_count, password, check);

    // The extra member's own cipher stream. Same key as the split member — one
    // salt for the set, which is legal and is what the rest of this fixture
    // family does — and `align16` padded, because that is the extent
    // `classify_stored_chain` requires an encrypted chain to sum to.
    let extra_cipher_len = extra_payload.len().div_ceil(16) * 16;
    let mut extra_cipher = extra_payload.to_vec();
    extra_cipher.resize(extra_cipher_len, 0);
    unrar_rs::encrypt_cipher_range(&member.key, &TEST_CRYPT_IV, &mut extra_cipher)
        .expect("the padded payload is block-aligned");

    let mut extra = build_test_rar_crypt_extra(Some(&member.psw_check), false);
    extra.extend_from_slice(&build_test_rar_blake2_extra(
        unrar_rs::crypto::blake2sp_hash(extra_payload),
    ));

    // Rebuild the closing volume with the extra member spliced in ahead of its
    // end header. The end header is re-sealed under the next IV index, which is
    // what keeps the sealed chain contiguous.
    // Sealing is deterministic in the header's length, so the size of the end
    // header already on the tail is what has to come off before the extra
    // member goes on and a fresh one is appended under the next IV index.
    let last = volume_count - 1;
    let iv_at = |index: u8| [0x40u8.wrapping_add(index).wrapping_add(last as u8 * 8); 16];
    let (name, bytes) = volumes[last].clone();
    let end_header = build_test_rar_end_header(false);
    let sealed_end_len = seal_test_rar_header(&header_key, &iv_at(2), &end_header).len();

    let mut rebuilt = bytes[..bytes.len() - sealed_end_len].to_vec();
    rebuilt.extend_from_slice(&seal_test_rar_header(
        &header_key,
        &iv_at(2),
        &build_test_rar_file_header_with_extra(
            extra_name,
            0,
            extra_cipher_len as u64,
            extra_payload.len() as u64,
            None,
            &extra,
        ),
    ));
    rebuilt.extend_from_slice(&extra_cipher);
    rebuilt.extend_from_slice(&seal_test_rar_header(&header_key, &iv_at(3), &end_header));
    volumes[last] = (name, rebuilt);
    volumes
}

/// A RAR4 `-hp` volume: the archive header's `ENCRYPTED_HEADERS` flag and then
/// ciphertext.
///
/// Deliberately not decryptable, and that costs nothing, because RAR4 `-hp` is
/// refused **at the flag** — the format carries no password-check value
/// anywhere, so there is nothing an admission gate could stand on and no
/// candidate can be proved before something is decrypted. What the bytes past
/// the flag are is exactly as irrelevant to this router as it is to a real
/// archive it has no key for.
fn header_encrypted_rar4_set(volume_count: usize, body_bytes: usize) -> Vec<(String, Vec<u8>)> {
    (0..volume_count)
        .map(|volume| {
            let is_first = volume == 0;
            // VOLUME | NEW_NUMBERING | ENCRYPTED_HEADERS, plus FIRST_VOLUME.
            let mut flags = 0x0001u16 | 0x0010 | 0x0080;
            if is_first {
                flags |= 0x0100;
            }
            let mut main_body = Vec::new();
            main_body.extend_from_slice(&0u16.to_le_bytes()); // high_pos_av
            main_body.extend_from_slice(&0u32.to_le_bytes()); // pos_av

            let mut bytes = Vec::new();
            bytes.extend_from_slice(&TEST_RAR4_SIG);
            bytes.extend_from_slice(&build_test_rar4_block(0x73, flags, &main_body));
            bytes.extend((0..body_bytes).map(|index| ((index * 31 + volume * 17) % 256) as u8));
            (format!("silver.horizon.part{:02}.rar", volume + 1), bytes)
        })
        .collect()
}

/// The `-hp` gate runner: [`run_gate_with_password`] plus the two candidate
/// sources that do not live on the job spec.
///
/// `nzb_zstd` is the job's persisted NZB, which is where `nzb.meta.password`
/// comes from, and `nzb_file_name` is the path whose stem carries a
/// `{{password}}` convention. Both are read by
/// `archive_password_candidates_for_job`, which is the harvest the header key
/// is derived from — so a fixture that sets neither is a job with only whatever
/// `spec.password` holds.
#[allow(clippy::too_many_arguments)]
async fn run_hp_gate(
    gate: DirectStoreGate,
    job_id: JobId,
    member_name: &str,
    volumes: &[(String, Vec<u8>)],
    arrivals: &[(u32, u32)],
    spec_password: Option<&str>,
    nzb_zstd: Vec<u8>,
    nzb_file_name: Option<&str>,
) -> GateOutcome {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(gate);

    let mut spec = direct_store_job_spec("Silver Horizon", volumes);
    spec.password = spec_password.map(str::to_owned);
    let working_dir = insert_active_job_with_persisted_nzb_named(
        &mut pipeline,
        job_id,
        spec,
        nzb_zstd,
        nzb_file_name,
    )
    .await;

    let mut volume_file_seen = false;
    for (file_index, segment_number) in arrivals {
        submit_volume_article(&mut pipeline, job_id, volumes, *file_index, *segment_number).await;
        for (filename, _) in volumes {
            if working_dir.join(filename).exists() {
                volume_file_seen = true;
            }
        }
    }
    drain_rar_refreshes(&mut pipeline).await;
    drive_extractions_to_terminal(&mut pipeline, job_id, 64).await;

    let status = job_status_for_assert(&pipeline, job_id);
    let (member, member_location) = member_after_gate(&complete_dir, &working_dir, member_name);
    GateOutcome {
        member,
        member_location,
        status,
        volume_file_seen,
    }
}

/// What a `-hp` job's routing decided, without driving extraction to terminal,
/// and with the persisted NZB's file name chosen so the `{{password}}`
/// convention can be exercised.
///
/// The refusal twin of [`run_hp_gate`], for the one case where the point is the
/// named demotion and nothing downstream of it.
///
/// It stops at the demotion because that is where its caller's claim ends, and
/// **not** because a refused set has nowhere to go: it very much does, and
/// [`hp_fallback_outcome`] is where that is proved. Every header-encryption
/// refusal reason the set can reach hands its current article back to the
/// conventional path, and for a job that holds the password that path then
/// opens the archive and produces the member. Anything
/// asserting *that* has to use the other helper; this one would report a job
/// still `Downloading`, because nothing here ever re-feeds the refetch the
/// demotion asked for.
async fn hp_routing_outcome_named(
    job_id: JobId,
    volumes: &[(String, Vec<u8>)],
    arrivals: &[(u32, u32)],
    spec_password: Option<&str>,
    nzb_zstd: Vec<u8>,
    nzb_file_name: Option<&str>,
) -> EncryptedRoutingOutcome {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let mut spec = direct_store_job_spec("Silver Horizon", volumes);
    spec.password = spec_password.map(str::to_owned);
    let working_dir = insert_active_job_with_persisted_nzb_named(
        &mut pipeline,
        job_id,
        spec,
        nzb_zstd,
        nzb_file_name,
    )
    .await;
    for (file_index, segment_number) in arrivals {
        submit_volume_article(&mut pipeline, job_id, volumes, *file_index, *segment_number).await;
    }
    drain_rar_refreshes(&mut pipeline).await;
    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    let volume_file_seen = volumes
        .iter()
        .any(|(filename, _)| working_dir.join(filename).exists());
    let partial_seen = any_direct_partial(&payload_root(&temp_dir, job_id));
    EncryptedRoutingOutcome {
        shape,
        volume_file_seen,
        partial_seen,
    }
}

/// A refused `-hp` job, followed all the way through the fallback it demoted
/// **into**.
///
/// # Why this exists
///
/// "Refuse and fall back to conventional extraction" is the guarantee the whole
/// `-hp` design leans on — it is the stated reason a check-less archive may
/// refuse rather than guess a key. [`hp_routing_outcome`] only ever observed the
/// first half of it: a demotion by name, and a volume file existing on disk. A
/// volume file existing is not the floor; the *member* coming out of it is.
///
/// So this keeps going. A demotion gives the article still held by the decoder
/// to conventional assembly and re-queues only previously committed coverage
/// that cannot be reconstructed; here [`dispatch_and_submit`] stands in for any
/// such refetch, exactly as the restart tests do.
/// The volumes then materialize, extraction runs, and the job reaches a terminal
/// state — and the volumes are byte-compared against the fixtures, because a
/// handoff that materialized *something* at every path is not the same as one
/// that handed over the archive that was posted.
///
/// `corrected_password` is written into the live [`JobSpec`] after the refusal
/// and before the refetch, for the case where the job genuinely had no usable
/// password at routing time. That is not a contrivance: `setJobPassword` and the
/// NZBGet facade's `*Unpack:Password` both mutate the spec in place, the direct
/// set is documented not to come back from a demotion, and the conventional path
/// re-harvests per volume parse — so this is the one seam through which a
/// late password still produces a member. `None` leaves the job exactly as the
/// router refused it.
struct HpFallbackOutcome {
    /// What the routing decided, read at the demotion and before the refetch.
    routing: EncryptedRoutingOutcome,
    /// Previously direct-owned articles the demotion put back on the queue.
    refetched: Vec<(u32, u32)>,
    /// Whether every source volume the fallback materialized is byte-identical
    /// to the fixture that was posted.
    volumes_byte_exact: bool,
    /// Which of the set's volumes reached disk at all.
    volumes_materialized: usize,
    member: Option<Vec<u8>>,
    member_location: Option<&'static str>,
    status: Option<JobStatus>,
}

async fn hp_fallback_outcome(
    job_id: JobId,
    member_name: &str,
    volumes: &[(String, Vec<u8>)],
    arrivals: &[(u32, u32)],
    spec_password: Option<&str>,
    nzb_zstd: Vec<u8>,
    corrected_password: Option<&str>,
) -> HpFallbackOutcome {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let mut spec = direct_store_job_spec("Silver Horizon", volumes);
    spec.password = spec_password.map(str::to_owned);
    let working_dir =
        insert_active_job_with_persisted_nzb(&mut pipeline, job_id, spec, nzb_zstd).await;
    // Dispatched rather than merely submitted, unlike every other `-hp` helper
    // here: the article has to leave the download queue on its way in, or the
    // job's *original* queue is still sitting there at demotion time and
    // "everything the demotion re-queued" would read every article back whether
    // the refetch ran or not. This is the difference between observing the
    // handoff and observing the harness.
    let mut corrected = false;
    for (file_index, segment_number) in arrivals {
        dispatch_and_submit(
            &mut pipeline,
            job_id,
            volumes,
            *file_index,
            *segment_number,
            2,
        )
        .await;
        // The operator's correction, applied the instant the refusal becomes
        // visible — which is where it happens in life, because that refusal is
        // what tells them a password is needed. It has to be in the spec before
        // the *later* volumes finish downloading: the conventional path harvests
        // per volume parse, and a `-hp` volume parsed without a password yields
        // no topology at all, so a correction applied after the last article
        // would leave the archive undetected rather than unextracted.
        if !corrected
            && let Some(password) = corrected_password
            && pipeline
                .direct_store
                .sets_for(job_id)
                .iter()
                .any(|set| set.is_demoted())
        {
            pipeline
                .jobs
                .get_mut(&job_id)
                .expect("the job is still active")
                .spec
                .password = Some(password.to_string());
            corrected = true;
        }
    }
    drain_rar_refreshes(&mut pipeline).await;
    assert!(
        corrected == corrected_password.is_some(),
        "a caller that supplied a corrected password expects a refusal to apply it to"
    );

    let routing = EncryptedRoutingOutcome {
        shape: format!("{:?}", pipeline.direct_store.sets_for(job_id)),
        volume_file_seen: volumes
            .iter()
            .any(|(filename, _)| working_dir.join(filename).exists()),
        partial_seen: any_direct_partial(&payload_root(&temp_dir, job_id)),
    };

    // Sampled as the refetch runs, not read at the end: a *successful* fallback
    // extraction deletes the source volumes it consumed, so by the time the job
    // is terminal there is nothing left on disk to compare. The largest image
    // ever observed for each volume is the one the extractor was handed.
    let mut materialized: Vec<Option<Vec<u8>>> = vec![None; volumes.len()];
    let sample = |materialized: &mut Vec<Option<Vec<u8>>>| {
        for (index, (filename, _)) in volumes.iter().enumerate() {
            let Ok(bytes) = std::fs::read(working_dir.join(filename)) else {
                continue;
            };
            if materialized[index]
                .as_ref()
                .is_none_or(|seen| seen.len() < bytes.len())
            {
                materialized[index] = Some(bytes);
            }
        }
    };
    sample(&mut materialized);

    // Any refetch the demotion still needs. Looped because materializing one
    // volume can put the next one's articles back on the queue, and bounded so a
    // pipeline that re-queued forever fails here rather than spinning.
    let refetched = peek_queued_segments(&mut pipeline, job_id);
    for _ in 0..8 {
        let queued = peek_queued_segments(&mut pipeline, job_id);
        if queued.is_empty() {
            break;
        }
        for (file_index, segment_number) in queued {
            dispatch_and_submit(
                &mut pipeline,
                job_id,
                volumes,
                file_index,
                segment_number,
                2,
            )
            .await;
            sample(&mut materialized);
        }
    }
    // Extraction is only driven once the fallback has something to extract. A
    // handoff that loses articles leaves the job in `Downloading` forever, and
    // driving it there would spend the harness's three-minute extraction timeout
    // to report a fact the caller's `refetched` and `volumes_byte_exact`
    // assertions state precisely and immediately.
    if materialized.iter().all(Option::is_some) {
        // Stands in for the download worker's own call, which this harness never
        // reaches: `submit_decoded_segment` enters the pipeline at
        // `handle_decode_success`, and it is the *download* side that schedules
        // a completion check once a job's download pipeline drains. The check
        // itself still decides; this only asks the question. (Several other test
        // modules here do exactly the same for the same reason.)
        pipeline.schedule_job_completion_check(job_id);
        drain_rar_refreshes(&mut pipeline).await;
        sample(&mut materialized);
        drive_extractions_to_terminal(&mut pipeline, job_id, 64).await;
        sample(&mut materialized);
    }

    let volumes_materialized = materialized.iter().filter(|bytes| bytes.is_some()).count();
    let volumes_byte_exact = volumes_materialized == volumes.len()
        && materialized
            .iter()
            .zip(volumes)
            .all(|(written, (_, posted))| written.as_deref() == Some(posted.as_slice()));

    let (member, member_location) = member_after_gate(&complete_dir, &working_dir, member_name);
    HpFallbackOutcome {
        routing,
        refetched,
        volumes_byte_exact,
        volumes_materialized,
        member,
        member_location,
        status: job_status_for_assert(&pipeline, job_id),
    }
}

/// Articles per volume in the grid fixtures.
const GRID_ARTICLES: usize = 2;

/// The CRC segments a decoder emits once the recovery set's block size is
/// known: one per block boundary the article crosses, based at the article's
/// placement in the file.
fn block_cut_segments(file_offset: u64, data: &[u8], block_size: u64) -> Vec<weaver_yenc::Segment> {
    let mut segments = Vec::new();
    let mut cursor = 0usize;
    while cursor < data.len() {
        let absolute = file_offset + cursor as u64;
        let to_boundary = (block_size - (absolute % block_size)) as usize;
        let end = (cursor + to_boundary).min(data.len());
        segments.push(weaver_yenc::Segment {
            file_offset: absolute,
            len: (end - cursor) as u64,
            crc32: checksum::crc32(&data[cursor..end]),
        });
        cursor = end;
    }
    segments
}

/// One volume article, carrying the decoder's block-grid segmentation.
///
/// `data` is what the wire delivered, which is the volume's own bytes for an
/// honest arrival and something else for a replay.
#[allow(clippy::too_many_arguments)]
async fn submit_grid_cut_article(
    pipeline: &mut Pipeline,
    job_id: JobId,
    file_index: u32,
    segment_number: u32,
    file_offset: u64,
    data: &[u8],
    filename: &str,
) {
    let segments = block_cut_segments(file_offset, data, PAR2_SLICE_BYTES);
    submit_decoded_segment_with_segments(
        pipeline,
        NzbFileId { job_id, file_index },
        segment_number,
        file_offset,
        data,
        filename,
        None,
        true,
        Some(segments),
    )
    .await;
}

/// The article extent one volume ordinal's segment covers, in the fixtures'
/// two-articles-per-volume shape.
fn grid_article_extent(
    volumes: &[(String, Vec<u8>)],
    ordinal: u32,
    segment_number: u32,
) -> (usize, usize) {
    article_extent(
        volumes[ordinal as usize].1.len(),
        segment_number,
        GRID_ARTICLES,
    )
}

/// A par2-bearing direct job whose recovery set parses **before** its volumes
/// arrive, with every volume article carrying block-grid CRC segments.
///
/// The index leads the NZB as well as the wire, so a volume's set-relative
/// index and its NZB file index never coincide — the coordinate confusion that
/// would otherwise pass unnoticed here, where evidence is keyed by one and the
/// grid by the other.
async fn grid_fed_direct_job(
    temp_dir: &TempDir,
    job_id: JobId,
    volumes: &[(String, Vec<u8>)],
    par2_bytes: &[u8],
    feed: GridFeed,
) -> (Pipeline, PathBuf, PathBuf) {
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    if feed.retained_session {
        // The retained session is off unless the environment turns it on, and a
        // test about that arm must not depend on the ambient default.
        pipeline.stateful_par2_session_forced = Some(true);
    }

    let (spec, index_file_index) = par2_bearing_job_spec_positioned(
        "Silver Horizon",
        volumes,
        par2_bytes,
        IndexPosition::First,
        GRID_ARTICLES,
    );
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    deliver_par2_index(&mut pipeline, job_id, index_file_index, par2_bytes).await;
    assert!(
        pipeline.par2_block_size(job_id).is_some(),
        "non-vacuity: the index has to be parsed before the volumes decode, or the \
         decoder has no grid to cut on and the whole fixture claims nothing"
    );

    let last_volume = volumes.len() as u32 - 1;
    for ordinal in 0..volumes.len() as u32 {
        for segment_number in 0..GRID_ARTICLES as u32 {
            if feed.withhold_last_article
                && ordinal == last_volume
                && segment_number + 1 == GRID_ARTICLES as u32
            {
                continue;
            }
            let (start, end) = grid_article_extent(volumes, ordinal, segment_number);
            let (filename, bytes) = &volumes[ordinal as usize];
            submit_grid_cut_article(
                &mut pipeline,
                job_id,
                IndexPosition::First.volume_file_index(ordinal),
                segment_number,
                start as u64,
                &bytes[start..end],
                filename,
            )
            .await;
        }
    }
    (pipeline, working_dir, complete_dir)
}

/// How [`grid_fed_direct_job`] delivers the job.
#[derive(Debug, Clone, Copy, Default)]
struct GridFeed {
    /// Force the retained PAR2 session on, so the zero-I/O arm is reachable.
    retained_session: bool,
    /// Hold back the last volume's last article, which keeps the set **live**:
    /// a set whose volumes all completed reaches its verdict inside the feed
    /// and has finalized (or repaired) by the time the test looks at it.
    withhold_last_article: bool,
}

/// Every block verdict one volume carries, or an empty map when it carries
/// none at all.
fn verdicts_for(
    pipeline: &Pipeline,
    job_id: JobId,
    ordinal: u32,
) -> std::collections::BTreeMap<u32, crate::pipeline::integrity::BlockVerdict> {
    pipeline
        .block_crc_verdicts(NzbFileId {
            job_id,
            file_index: IndexPosition::First.volume_file_index(ordinal),
        })
        .unwrap_or_default()
}

/// Drives a job that has already been fed to whatever terminal state it
/// reaches, in the shape the repairable gate uses.
async fn drive_grid_fed_job_to_terminal(pipeline: &mut Pipeline, job_id: JobId) {
    if let Some(state) = pipeline.jobs.get_mut(&job_id) {
        state.download_queue = crate::DownloadQueue::new();
        state.recovery_queue = crate::DownloadQueue::new();
    }
    for _ in 0..48 {
        if matches!(
            job_status_for_assert(pipeline, job_id),
            Some(JobStatus::Complete) | Some(JobStatus::Failed { .. })
        ) {
            break;
        }
        drain_rar_refreshes(pipeline).await;
        pipeline.check_job_completion(job_id).await;
        pump_pipeline_runtime_queues(pipeline).await;
        settle_inflight_moves(pipeline).await;
        if let Ok(Some(done)) = tokio::time::timeout(
            std::time::Duration::from_millis(250),
            pipeline.extract_done_rx.recv(),
        )
        .await
        {
            pipeline.handle_extraction_done(done).await;
            pump_pipeline_runtime_queues(pipeline).await;
            settle_inflight_moves(pipeline).await;
        }
    }
}

/// Absolute path of one source volume's sparse envelope, found by suffix.
///
/// The name is built from the set's plan, so a test that hard-coded it would
/// pin a private naming scheme rather than the behaviour under test.
fn envelope_path_for_volume(pipeline: &Pipeline, job_id: JobId, volume_index: u32) -> PathBuf {
    pipeline
        .direct_store
        .sets_for(job_id)
        .iter()
        .find_map(|set| {
            set.plan()
                .volumes
                .contains_key(&volume_index)
                .then(|| set.plan().envelope_path(volume_index))
        })
        .expect("the set owns this volume")
}

/// One routed article, which is what admits the set: the router is what builds
/// the set state a demotion acts on, and it is only reached by bytes.
async fn admit_first_volume_article(
    pipeline: &mut Pipeline,
    job_id: JobId,
    volumes: &[(String, Vec<u8>)],
) {
    take_queued_segment(
        pipeline,
        job_id,
        SegmentId {
            file_id: NzbFileId {
                job_id,
                file_index: 0,
            },
            segment_number: 0,
        },
    );
    submit_volume_article(pipeline, job_id, volumes, 0, 0).await;
    assert!(
        pipeline.direct_store.set_mut(job_id, 0).is_some(),
        "the fixture must have an admitted direct set to demote"
    );
}
