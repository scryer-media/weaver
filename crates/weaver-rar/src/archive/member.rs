use std::cell::RefCell;
use std::io::{BufReader, BufWriter, Write};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use blake2::{Blake2s256, Digest};
use tracing::debug;

use super::*;
use crate::volume::VolumeProvider;

const STREAMING_STORE_CHUNK_BUFFER_BYTES: usize = 4 * 1024 * 1024;

impl RarArchive {
    fn advance_solid_cursor_to(&mut self, index: usize, fh: &FileHeader) -> RarResult<()> {
        if index < self.solid_next_index {
            return Err(RarError::SolidOrderViolation {
                required: format!("member index {}", self.solid_next_index),
                requested: fh.name.clone(),
            });
        }

        while self.solid_next_index < index {
            let skip_idx = self.solid_next_index;
            let skip_entry = self.members[skip_idx].clone();
            let skip_fh = skip_entry.file_header;

            if skip_fh.compression.method != CompressionMethod::Store {
                let skip_unpacked = skip_fh.unpacked_size.unwrap_or(0);
                let base_reader = ArchiveSegmentReader::new(
                    &mut self.volumes,
                    &self.limits,
                    &skip_entry.segments,
                    &skip_fh.name,
                );

                if skip_entry.is_encrypted {
                    let password = self
                        .password
                        .as_deref()
                        .ok_or_else(|| RarError::EncryptedMember {
                            member: skip_fh.name.clone(),
                        })?
                        .to_owned();

                    if self.format == ArchiveFormat::Rar4 {
                        let salt =
                            skip_entry
                                .rar4_salt
                                .ok_or_else(|| RarError::CorruptArchive {
                                    detail: format!(
                                        "RAR4 member {} is marked encrypted but has no salt",
                                        skip_fh.name,
                                    ),
                                })?;
                        let (key, iv) = self.kdf_cache.derive_key_rar4(&password, &salt);
                        let reader =
                            crate::crypto::DecryptingReader::new_rar4(base_reader, &key, &iv);
                        Self::solid_decode_reader_to_sink(
                            &mut self.solid_decoder_rar4,
                            &mut self.solid_decoder,
                            self.limits.max_dict_size,
                            reader,
                            skip_unpacked,
                            &skip_fh,
                        )?;
                    } else {
                        let enc_info = skip_entry.file_encryption.as_ref().ok_or_else(|| {
                            RarError::CorruptArchive {
                                detail: format!(
                                    "member {} is marked encrypted but has no encryption parameters",
                                    skip_fh.name,
                                ),
                            }
                        })?;
                        if let Some(ref check_data) = enc_info.check_data
                            && !self.kdf_cache.verify_password_rar5(
                                &password,
                                &enc_info.salt,
                                enc_info.kdf_count,
                                check_data,
                            )
                        {
                            return Err(RarError::WrongPassword {
                                member: skip_fh.name.clone(),
                            });
                        }
                        let key = self.kdf_cache.derive_key_rar5(
                            &password,
                            &enc_info.salt,
                            enc_info.kdf_count,
                        );
                        let reader = crate::crypto::DecryptingReader::new_rar5(
                            base_reader,
                            &key,
                            &enc_info.iv,
                        );
                        Self::solid_decode_reader_to_sink(
                            &mut self.solid_decoder_rar4,
                            &mut self.solid_decoder,
                            self.limits.max_dict_size,
                            reader,
                            skip_unpacked,
                            &skip_fh,
                        )?;
                    }
                } else {
                    Self::solid_decode_reader_to_sink(
                        &mut self.solid_decoder_rar4,
                        &mut self.solid_decoder,
                        self.limits.max_dict_size,
                        base_reader,
                        skip_unpacked,
                        &skip_fh,
                    )?;
                }
            }

            self.solid_next_index += 1;
        }

        Ok(())
    }

    fn normalized_provider_segments(segments: &[DataSegment]) -> (Vec<DataSegment>, usize) {
        let mut sorted_segments = segments.to_vec();
        sorted_segments.sort_by_key(|segment| segment.volume_index);
        let volume_base = sorted_segments
            .first()
            .map_or(0, |segment| segment.volume_index);
        for segment in &mut sorted_segments {
            segment.volume_index -= volume_base;
        }
        (sorted_segments, volume_base)
    }

    fn advance_solid_cursor_to_streaming(
        &mut self,
        index: usize,
        fh: &FileHeader,
        provider: &dyn VolumeProvider,
    ) -> RarResult<()> {
        if index < self.solid_next_index {
            return Err(RarError::SolidOrderViolation {
                required: format!("member index {}", self.solid_next_index),
                requested: fh.name.clone(),
            });
        }

        while self.solid_next_index < index {
            let skip_idx = self.solid_next_index;
            let skip_entry = self.members[skip_idx].clone();
            let skip_fh = skip_entry.file_header;

            if skip_fh.compression.method != CompressionMethod::Store {
                let skip_unpacked = skip_fh.unpacked_size.unwrap_or(0);
                let (segments, _) = Self::normalized_provider_segments(&skip_entry.segments);
                let base_reader = ChainedSegmentReader::new(&segments, provider).with_continuation(
                    skip_fh.split_after,
                    self.format,
                    self.password.clone(),
                );

                if skip_entry.is_encrypted {
                    let password = self
                        .password
                        .as_deref()
                        .ok_or_else(|| RarError::EncryptedMember {
                            member: skip_fh.name.clone(),
                        })?
                        .to_owned();

                    if self.format == ArchiveFormat::Rar4 {
                        let salt =
                            skip_entry
                                .rar4_salt
                                .ok_or_else(|| RarError::CorruptArchive {
                                    detail: format!(
                                        "RAR4 member {} is marked encrypted but has no salt",
                                        skip_fh.name,
                                    ),
                                })?;
                        let (key, iv) = self.kdf_cache.derive_key_rar4(&password, &salt);
                        let reader =
                            crate::crypto::DecryptingReader::new_rar4(base_reader, &key, &iv);
                        Self::solid_decode_reader_to_sink(
                            &mut self.solid_decoder_rar4,
                            &mut self.solid_decoder,
                            self.limits.max_dict_size,
                            reader,
                            skip_unpacked,
                            &skip_fh,
                        )?;
                    } else {
                        let enc_info = skip_entry.file_encryption.as_ref().ok_or_else(|| {
                            RarError::CorruptArchive {
                                detail: format!(
                                    "member {} is marked encrypted but has no encryption parameters",
                                    skip_fh.name,
                                ),
                            }
                        })?;
                        if let Some(ref check_data) = enc_info.check_data
                            && !self.kdf_cache.verify_password_rar5(
                                &password,
                                &enc_info.salt,
                                enc_info.kdf_count,
                                check_data,
                            )
                        {
                            return Err(RarError::WrongPassword {
                                member: skip_fh.name.clone(),
                            });
                        }
                        let key = self.kdf_cache.derive_key_rar5(
                            &password,
                            &enc_info.salt,
                            enc_info.kdf_count,
                        );
                        let reader = crate::crypto::DecryptingReader::new_rar5(
                            base_reader,
                            &key,
                            &enc_info.iv,
                        );
                        Self::solid_decode_reader_to_sink(
                            &mut self.solid_decoder_rar4,
                            &mut self.solid_decoder,
                            self.limits.max_dict_size,
                            reader,
                            skip_unpacked,
                            &skip_fh,
                        )?;
                    }
                } else {
                    Self::solid_decode_reader_to_sink(
                        &mut self.solid_decoder_rar4,
                        &mut self.solid_decoder,
                        self.limits.max_dict_size,
                        base_reader,
                        skip_unpacked,
                        &skip_fh,
                    )?;
                }
            }

            self.solid_next_index += 1;
        }

        Ok(())
    }

    fn copy_reader_to_writer<R: Read, W: Write>(
        mut reader: R,
        writer: &mut W,
        limit: Option<u64>,
    ) -> RarResult<u64> {
        let mut buffer = vec![0u8; STREAMING_STORE_CHUNK_BUFFER_BYTES];
        let mut written = 0u64;
        let mut remaining = limit;

        loop {
            let to_read = match remaining {
                Some(0) => break,
                Some(remaining) => buffer.len().min(remaining.min(usize::MAX as u64) as usize),
                None => buffer.len(),
            };
            let read = reader.read(&mut buffer[..to_read]).map_err(RarError::Io)?;
            if read == 0 {
                if remaining.is_some_and(|left| left > 0) {
                    return Err(RarError::Io(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "unexpected EOF while copying member data",
                    )));
                }
                break;
            }
            writer.write_all(&buffer[..read]).map_err(RarError::Io)?;
            written += read as u64;
            if let Some(left) = remaining.as_mut() {
                *left -= read as u64;
            }
        }

        Ok(written)
    }

    fn copy_reader_to_writer_chunked<R: Read, F>(
        mut reader: R,
        volume_tracker: Arc<AtomicUsize>,
        first_volume_index: usize,
        mut writer_factory: F,
        limit: Option<u64>,
    ) -> RarResult<Vec<(usize, u64)>>
    where
        F: FnMut(usize) -> RarResult<Box<dyn Write>>,
    {
        let mut buffer = vec![0u8; STREAMING_STORE_CHUNK_BUFFER_BYTES];
        let mut current_volume = first_volume_index;
        let mut current_writer = Some(writer_factory(current_volume)?);
        let mut current_written = 0u64;
        let mut chunks = Vec::new();
        let mut remaining = limit;

        loop {
            let observed_volume = volume_tracker.load(Ordering::Acquire);
            if observed_volume != current_volume {
                if let Some(mut writer) = current_writer.take() {
                    writer.flush().map_err(RarError::Io)?;
                }
                chunks.push((current_volume, current_written));
                current_volume = observed_volume;
                current_writer = Some(writer_factory(current_volume)?);
                current_written = 0;
            }

            let to_read = match remaining {
                Some(0) => break,
                Some(remaining) => buffer.len().min(remaining.min(usize::MAX as u64) as usize),
                None => buffer.len(),
            };
            let read = reader.read(&mut buffer[..to_read]).map_err(RarError::Io)?;
            if read == 0 {
                if remaining.is_some_and(|left| left > 0) {
                    return Err(RarError::Io(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "unexpected EOF while copying chunked member data",
                    )));
                }
                break;
            }

            if let Some(writer) = current_writer.as_mut() {
                writer.write_all(&buffer[..read]).map_err(RarError::Io)?;
            }
            current_written += read as u64;
            if let Some(left) = remaining.as_mut() {
                *left -= read as u64;
            }
        }

        if let Some(mut writer) = current_writer {
            writer.flush().map_err(RarError::Io)?;
            if current_written > 0 || chunks.is_empty() {
                chunks.push((current_volume, current_written));
            }
        }

        Ok(chunks)
    }

    /// Extract a member by index, handling any supported compression method.
    ///
    /// For multi-volume archives, this seamlessly reads data across volumes.
    /// Returns the decompressed data as a `Vec<u8>`.
    pub fn extract_member(
        &mut self,
        index: usize,
        options: &ExtractOptions,
        progress: Option<&dyn ProgressHandler>,
    ) -> RarResult<crate::extract::ExtractedMember> {
        let entry = self
            .members
            .get(index)
            .ok_or_else(|| RarError::CorruptArchive {
                detail: format!("member index {index} out of range"),
            })?;

        // Check for encryption and resolve password.
        let member_password = if entry.is_encrypted {
            let pwd = options
                .password
                .as_deref()
                .or(self.password.as_deref())
                .ok_or_else(|| RarError::EncryptedMember {
                    member: entry.file_header.name.clone(),
                })?;
            Some(pwd.to_string())
        } else {
            None
        };
        let file_enc = entry.file_encryption.clone();
        let rar4_salt = entry.rar4_salt;
        let fh = entry.file_header.clone();
        let hash = entry.hash.clone();
        let mi = self.member_info(index);
        let is_solid = fh.compression.solid;
        let archive_format = self.format;
        let unpacked_size = fh.unpacked_size.unwrap_or(0);
        // When HASHMAC is set, CRC/BLAKE2 values are HMAC-transformed and
        // cannot be verified without HashKey from unrar's custom PBKDF2 chain.
        let skip_hash_verify = file_enc.as_ref().is_some_and(|fe| fe.use_hash_mac);

        // Report progress start.
        if let (Some(p), Some(mi)) = (progress, mi.as_ref()) {
            p.on_member_start(mi);
        }

        if fh.compression.method == CompressionMethod::Store {
            let expected_crc = if options.verify && !skip_hash_verify {
                fh.data_crc32
            } else {
                None
            };
            let expected_blake = if options.verify && !skip_hash_verify {
                match hash.as_ref() {
                    Some(FileHash::Blake2sp(expected)) => Some(*expected),
                    _ => None,
                }
            } else {
                None
            };
            let capacity = unpacked_size.min(usize::MAX as u64) as usize;
            let mut output = crate::extract::ExtractedMemberSink::with_capacity_hint(capacity)?;

            let (written, actual_crc, actual_blake) = {
                let segments = self.members[index].segments.clone();
                let base_reader =
                    ArchiveSegmentReader::new(&mut self.volumes, &self.limits, &segments, &fh.name);
                let mut hash_writer = HashingWriter::new(
                    &mut output,
                    expected_crc.is_some(),
                    expected_blake.is_some(),
                );

                let written = if let Some(ref pwd) = member_password {
                    if archive_format == ArchiveFormat::Rar4 {
                        let salt = rar4_salt.ok_or_else(|| RarError::CorruptArchive {
                            detail: format!(
                                "RAR4 member {} is marked encrypted but has no salt",
                                fh.name,
                            ),
                        })?;
                        let (key, iv) = self.kdf_cache.derive_key_rar4(pwd, &salt);
                        let reader =
                            crate::crypto::DecryptingReader::new_rar4(base_reader, &key, &iv);
                        Self::copy_reader_to_writer(reader, &mut hash_writer, Some(unpacked_size))?
                    } else {
                        let enc_info = file_enc.as_ref().ok_or_else(|| RarError::CorruptArchive {
                            detail: format!(
                                "member {} is marked encrypted but has no encryption parameters",
                                fh.name,
                            ),
                        })?;
                        if let Some(ref check_data) = enc_info.check_data
                            && !self.kdf_cache.verify_password_rar5(
                                pwd,
                                &enc_info.salt,
                                enc_info.kdf_count,
                                check_data,
                            )
                        {
                            return Err(RarError::WrongPassword {
                                member: fh.name.clone(),
                            });
                        }
                        let key =
                            self.kdf_cache
                                .derive_key_rar5(pwd, &enc_info.salt, enc_info.kdf_count);
                        let reader = crate::crypto::DecryptingReader::new_rar5(
                            base_reader,
                            &key,
                            &enc_info.iv,
                        );
                        Self::copy_reader_to_writer(reader, &mut hash_writer, Some(unpacked_size))?
                    }
                } else {
                    Self::copy_reader_to_writer(base_reader, &mut hash_writer, None)?
                };

                (
                    written,
                    expected_crc.map(|_| hash_writer.finalize_crc()),
                    expected_blake.map(|_| hash_writer.finalize_blake2()),
                )
            };

            if let (Some(expected), Some(actual)) = (expected_crc, actual_crc)
                && actual != expected
            {
                return Err(RarError::DataCrcMismatch {
                    member: fh.name.clone(),
                    expected,
                    actual,
                });
            }

            if let (Some(expected), Some(actual)) = (expected_blake, actual_blake)
                && actual != expected
            {
                return Err(RarError::Blake2Mismatch {
                    member: fh.name.clone(),
                });
            }

            if let (Some(p), Some(mi)) = (progress, mi.as_ref()) {
                p.on_member_progress(mi, written);
            }

            let output = output.into_extracted()?;

            if let (Some(p), Some(mi)) = (progress, mi.as_ref()) {
                p.on_member_complete(mi, &Ok(()));
            }

            return Ok(output);
        }

        if archive_format == ArchiveFormat::Rar5
            && fh.compression.method != CompressionMethod::Store
            && !is_solid
        {
            let expected_crc = if options.verify && !skip_hash_verify {
                fh.data_crc32
            } else {
                None
            };
            let capacity = unpacked_size.min(usize::MAX as u64) as usize;
            let mut output = crate::extract::ExtractedMemberSink::with_capacity_hint(capacity)?;

            let decrypt_key = if let Some(ref pwd) = member_password {
                let enc_info = file_enc.as_ref().ok_or_else(|| RarError::CorruptArchive {
                    detail: format!(
                        "member {} is marked encrypted but has no encryption parameters",
                        fh.name,
                    ),
                })?;

                if let Some(ref check_data) = enc_info.check_data
                    && !self.kdf_cache.verify_password_rar5(
                        pwd,
                        &enc_info.salt,
                        enc_info.kdf_count,
                        check_data,
                    )
                {
                    return Err(RarError::WrongPassword {
                        member: fh.name.clone(),
                    });
                }

                Some((
                    self.kdf_cache
                        .derive_key_rar5(pwd, &enc_info.salt, enc_info.kdf_count),
                    enc_info.iv,
                ))
            } else {
                None
            };

            let actual_crc = {
                let segments = self.members[index].segments.clone();
                let base_reader =
                    ArchiveSegmentReader::new(&mut self.volumes, &self.limits, &segments, &fh.name);
                let mut crc_writer = CrcWriter::new(&mut output, expected_crc.is_some());

                if let Some((key, iv)) = decrypt_key {
                    let reader = crate::crypto::DecryptingReader::new_rar5(base_reader, &key, &iv);
                    crate::decompress::lz::decompress_lz_reader_to_writer(
                        reader,
                        unpacked_size,
                        &fh.compression,
                        &mut crc_writer,
                    )?;
                } else {
                    crate::decompress::lz::decompress_lz_reader_to_writer(
                        base_reader,
                        unpacked_size,
                        &fh.compression,
                        &mut crc_writer,
                    )?;
                }

                crc_writer.flush().map_err(RarError::Io)?;
                expected_crc.map(|_| crc_writer.finalize_crc())
            };

            if let (Some(expected), Some(actual)) = (expected_crc, actual_crc)
                && actual != expected
            {
                return Err(RarError::DataCrcMismatch {
                    member: fh.name.clone(),
                    expected,
                    actual,
                });
            }

            if let (Some(p), Some(mi)) = (progress, mi.as_ref()) {
                p.on_member_progress(mi, output.len() as u64);
            }

            let output = output.into_extracted()?;

            if options.verify
                && !skip_hash_verify
                && let Some(FileHash::Blake2sp(expected)) = hash.as_ref()
                && !crate::extract::verify_blake2_member(&output, expected)?
            {
                return Err(RarError::Blake2Mismatch {
                    member: fh.name.clone(),
                });
            }

            if let (Some(p), Some(mi)) = (progress, mi.as_ref()) {
                p.on_member_complete(mi, &Ok(()));
            }

            return Ok(output);
        }

        if archive_format == ArchiveFormat::Rar4
            && fh.compression.method != CompressionMethod::Store
            && !is_solid
        {
            let expected_crc = if options.verify && !skip_hash_verify {
                fh.data_crc32
            } else {
                None
            };
            let capacity = unpacked_size.min(usize::MAX as u64) as usize;
            let mut output = crate::extract::ExtractedMemberSink::with_capacity_hint(capacity)?;

            let actual_crc = {
                let segments = self.members[index].segments.clone();
                let base_reader =
                    ArchiveSegmentReader::new(&mut self.volumes, &self.limits, &segments, &fh.name);
                let mut crc_writer = CrcWriter::new(&mut output, expected_crc.is_some());
                if let Some(ref pwd) = member_password {
                    let salt = rar4_salt.ok_or_else(|| RarError::CorruptArchive {
                        detail: format!(
                            "RAR4 member {} is marked encrypted but has no salt",
                            fh.name,
                        ),
                    })?;
                    let (key, iv) = self.kdf_cache.derive_key_rar4(pwd, &salt);
                    let reader = crate::crypto::DecryptingReader::new_rar4(base_reader, &key, &iv);
                    crate::decompress::rar4::decompress_rar4_lz_reader_to_writer(
                        reader,
                        unpacked_size,
                        fh.compression.dict_size,
                        &mut crc_writer,
                    )?;
                } else {
                    crate::decompress::rar4::decompress_rar4_lz_reader_to_writer(
                        base_reader,
                        unpacked_size,
                        fh.compression.dict_size,
                        &mut crc_writer,
                    )?;
                }
                crc_writer.flush().map_err(RarError::Io)?;
                expected_crc.map(|_| crc_writer.finalize_crc())
            };

            if let (Some(expected), Some(actual)) = (expected_crc, actual_crc)
                && actual != expected
            {
                return Err(RarError::DataCrcMismatch {
                    member: fh.name.clone(),
                    expected,
                    actual,
                });
            }

            if let (Some(p), Some(mi)) = (progress, mi.as_ref()) {
                p.on_member_progress(mi, output.len() as u64);
            }

            let output = output.into_extracted()?;

            if options.verify
                && !skip_hash_verify
                && let Some(FileHash::Blake2sp(expected)) = hash.as_ref()
                && !crate::extract::verify_blake2_member(&output, expected)?
            {
                return Err(RarError::Blake2Mismatch {
                    member: fh.name.clone(),
                });
            }

            if let (Some(p), Some(mi)) = (progress, mi.as_ref()) {
                p.on_member_complete(mi, &Ok(()));
            }

            return Ok(output);
        }

        if is_solid && fh.compression.method != CompressionMethod::Store {
            let expected_crc = if options.verify && !skip_hash_verify {
                fh.data_crc32
            } else {
                None
            };
            let expected_blake = if options.verify && !skip_hash_verify {
                match hash.as_ref() {
                    Some(FileHash::Blake2sp(expected)) => Some(*expected),
                    _ => None,
                }
            } else {
                None
            };
            let capacity = unpacked_size.min(usize::MAX as u64) as usize;
            let mut output = crate::extract::ExtractedMemberSink::with_capacity_hint(capacity)?;

            let (actual_crc, actual_blake) = {
                let mut hash_writer = HashingWriter::new(
                    &mut output,
                    expected_crc.is_some(),
                    expected_blake.is_some(),
                );
                self.advance_solid_cursor_to(index, &fh)?;
                let segments = self.members[index].segments.clone();
                let base_reader =
                    ArchiveSegmentReader::new(&mut self.volumes, &self.limits, &segments, &fh.name);
                if let Some(ref pwd) = member_password {
                    if archive_format == ArchiveFormat::Rar4 {
                        let salt = rar4_salt.ok_or_else(|| RarError::CorruptArchive {
                            detail: format!(
                                "RAR4 member {} is marked encrypted but has no salt",
                                fh.name,
                            ),
                        })?;
                        let (key, iv) = self.kdf_cache.derive_key_rar4(pwd, &salt);
                        let reader =
                            crate::crypto::DecryptingReader::new_rar4(base_reader, &key, &iv);
                        Self::solid_decode_reader_to_writer(
                            &mut self.solid_decoder_rar4,
                            &mut self.solid_decoder,
                            self.limits.max_dict_size,
                            reader,
                            unpacked_size,
                            &fh,
                            &mut hash_writer,
                        )?;
                    } else {
                        let enc_info = file_enc.as_ref().ok_or_else(|| RarError::CorruptArchive {
                            detail: format!(
                                "member {} is marked encrypted but has no encryption parameters",
                                fh.name,
                            ),
                        })?;
                        let key =
                            self.kdf_cache
                                .derive_key_rar5(pwd, &enc_info.salt, enc_info.kdf_count);
                        let reader = crate::crypto::DecryptingReader::new_rar5(
                            base_reader,
                            &key,
                            &enc_info.iv,
                        );
                        Self::solid_decode_reader_to_writer(
                            &mut self.solid_decoder_rar4,
                            &mut self.solid_decoder,
                            self.limits.max_dict_size,
                            reader,
                            unpacked_size,
                            &fh,
                            &mut hash_writer,
                        )?;
                    }
                } else {
                    Self::solid_decode_reader_to_writer(
                        &mut self.solid_decoder_rar4,
                        &mut self.solid_decoder,
                        self.limits.max_dict_size,
                        base_reader,
                        unpacked_size,
                        &fh,
                        &mut hash_writer,
                    )?;
                }
                hash_writer.flush().map_err(RarError::Io)?;
                (
                    expected_crc.map(|_| hash_writer.finalize_crc()),
                    expected_blake.map(|_| hash_writer.finalize_blake2()),
                )
            };

            if let (Some(expected), Some(actual)) = (expected_crc, actual_crc)
                && actual != expected
            {
                return Err(RarError::DataCrcMismatch {
                    member: fh.name.clone(),
                    expected,
                    actual,
                });
            }

            if let (Some(expected), Some(actual)) = (expected_blake, actual_blake)
                && actual != expected
            {
                return Err(RarError::Blake2Mismatch {
                    member: fh.name.clone(),
                });
            }

            self.solid_next_index = index + 1;

            if let (Some(p), Some(mi)) = (progress, mi.as_ref()) {
                p.on_member_progress(mi, output.len() as u64);
            }

            let output = output.into_extracted()?;

            if let (Some(p), Some(mi)) = (progress, mi.as_ref()) {
                p.on_member_complete(mi, &Ok(()));
            }

            return Ok(output);
        }

        Err(RarError::CorruptArchive {
            detail: format!(
                "unsupported extraction path for member {} (method {:?}, solid={is_solid}, format={archive_format:?})",
                fh.name, fh.compression.method,
            ),
        })
    }

    /// Extract a member directly to a file, streaming data to disk.
    ///
    /// This is the memory-efficient alternative to `extract_member()`. Instead of
    /// returning the full decompressed content as a `Vec<u8>`, it writes directly
    /// to disk via a `BufWriter`. Memory usage is bounded to dict_size (max 256 MB)
    /// for LZ-compressed files, or ~8 MB for Store (uncompressed) files.
    ///
    /// For Store method without encryption: reads segments directly from volumes
    /// and writes to disk without buffering the full file.
    pub fn extract_member_to_file(
        &mut self,
        index: usize,
        options: &ExtractOptions,
        progress: Option<&dyn ProgressHandler>,
        out_path: &std::path::Path,
    ) -> RarResult<u64> {
        let entry = self
            .members
            .get(index)
            .ok_or_else(|| RarError::CorruptArchive {
                detail: format!("member index {index} out of range"),
            })?;

        let member_password = if entry.is_encrypted {
            let pwd = options
                .password
                .as_deref()
                .or(self.password.as_deref())
                .ok_or_else(|| RarError::EncryptedMember {
                    member: entry.file_header.name.clone(),
                })?;
            Some(pwd.to_string())
        } else {
            None
        };

        let fh = entry.file_header.clone();
        let hash = entry.hash.clone();
        let file_enc = entry.file_encryption.clone();
        let rar4_salt = entry.rar4_salt;
        let mi = self.member_info(index);
        let is_solid = fh.compression.solid;
        let archive_format = self.format;
        let unpacked_size = fh.unpacked_size.unwrap_or(0);
        let skip_hash_verify = file_enc.as_ref().is_some_and(|fe| fe.use_hash_mac);

        if let (Some(p), Some(mi)) = (progress, mi.as_ref()) {
            p.on_member_start(mi);
        }

        // Create output file with buffered writer.
        let file = std::fs::File::create(out_path).map_err(RarError::Io)?;
        let mut writer = BufWriter::with_capacity(8 * 1024 * 1024, file);

        if fh.compression.method == CompressionMethod::Store {
            let expected_crc = if options.verify && !skip_hash_verify {
                fh.data_crc32
            } else {
                None
            };
            let expected_blake = if options.verify && !skip_hash_verify {
                match hash.as_ref() {
                    Some(FileHash::Blake2sp(expected)) => Some(*expected),
                    _ => None,
                }
            } else {
                None
            };

            let (written, actual_crc, actual_blake) = {
                let segments = self.members[index].segments.clone();
                let base_reader =
                    ArchiveSegmentReader::new(&mut self.volumes, &self.limits, &segments, &fh.name);
                let mut hash_writer = HashingWriter::new(
                    &mut writer,
                    expected_crc.is_some(),
                    expected_blake.is_some(),
                );

                let written = if let Some(ref pwd) = member_password {
                    if archive_format == ArchiveFormat::Rar4 {
                        let salt = rar4_salt.ok_or_else(|| RarError::CorruptArchive {
                            detail: format!(
                                "RAR4 member {} is marked encrypted but has no salt",
                                fh.name,
                            ),
                        })?;
                        let (key, iv) = self.kdf_cache.derive_key_rar4(pwd, &salt);
                        let reader =
                            crate::crypto::DecryptingReader::new_rar4(base_reader, &key, &iv);
                        Self::copy_reader_to_writer(reader, &mut hash_writer, Some(unpacked_size))?
                    } else {
                        let enc_info = file_enc.as_ref().ok_or_else(|| RarError::CorruptArchive {
                            detail: format!(
                                "member {} is marked encrypted but has no encryption parameters",
                                fh.name,
                            ),
                        })?;
                        if let Some(ref check_data) = enc_info.check_data
                            && !self.kdf_cache.verify_password_rar5(
                                pwd,
                                &enc_info.salt,
                                enc_info.kdf_count,
                                check_data,
                            )
                        {
                            return Err(RarError::WrongPassword {
                                member: fh.name.clone(),
                            });
                        }
                        let key =
                            self.kdf_cache
                                .derive_key_rar5(pwd, &enc_info.salt, enc_info.kdf_count);
                        let reader = crate::crypto::DecryptingReader::new_rar5(
                            base_reader,
                            &key,
                            &enc_info.iv,
                        );
                        Self::copy_reader_to_writer(reader, &mut hash_writer, Some(unpacked_size))?
                    }
                } else {
                    Self::copy_reader_to_writer(base_reader, &mut hash_writer, None)?
                };

                (
                    written,
                    expected_crc.map(|_| hash_writer.finalize_crc()),
                    expected_blake.map(|_| hash_writer.finalize_blake2()),
                )
            };

            if let (Some(expected), Some(actual)) = (expected_crc, actual_crc)
                && actual != expected
            {
                return Err(RarError::DataCrcMismatch {
                    member: fh.name.clone(),
                    expected,
                    actual,
                });
            }

            if let (Some(expected), Some(actual)) = (expected_blake, actual_blake)
                && actual != expected
            {
                return Err(RarError::Blake2Mismatch {
                    member: fh.name.clone(),
                });
            }

            if let (Some(p), Some(mi)) = (progress, mi.as_ref()) {
                p.on_member_progress(mi, written);
                p.on_member_complete(mi, &Ok(()));
            }

            return Ok(written);
        }

        if archive_format == ArchiveFormat::Rar5
            && fh.compression.method != CompressionMethod::Store
            && !is_solid
        {
            let expected_crc = if options.verify && !skip_hash_verify {
                fh.data_crc32
            } else {
                None
            };

            let decrypt_key = if let Some(ref pwd) = member_password {
                let enc_info = file_enc.as_ref().ok_or_else(|| RarError::CorruptArchive {
                    detail: format!(
                        "member {} is marked encrypted but has no encryption parameters",
                        fh.name
                    ),
                })?;
                if let Some(ref check_data) = enc_info.check_data
                    && !self.kdf_cache.verify_password_rar5(
                        pwd,
                        &enc_info.salt,
                        enc_info.kdf_count,
                        check_data,
                    )
                {
                    return Err(RarError::WrongPassword {
                        member: fh.name.clone(),
                    });
                }
                Some((
                    self.kdf_cache
                        .derive_key_rar5(pwd, &enc_info.salt, enc_info.kdf_count),
                    enc_info.iv,
                ))
            } else {
                None
            };

            let segments = self.members[index].segments.clone();
            let base_reader =
                ArchiveSegmentReader::new(&mut self.volumes, &self.limits, &segments, &fh.name);

            let do_crc = expected_crc.is_some();
            let actual_crc = {
                let mut crc_writer = CrcWriter::new(&mut writer, do_crc);
                if let Some((key, iv)) = decrypt_key {
                    let reader = crate::crypto::DecryptingReader::new_rar5(base_reader, &key, &iv);
                    crate::decompress::lz::decompress_lz_reader_to_writer(
                        reader,
                        unpacked_size,
                        &fh.compression,
                        &mut crc_writer,
                    )?;
                } else {
                    crate::decompress::lz::decompress_lz_reader_to_writer(
                        base_reader,
                        unpacked_size,
                        &fh.compression,
                        &mut crc_writer,
                    )?;
                }
                crc_writer.flush().map_err(RarError::Io)?;
                expected_crc.map(|_| crc_writer.finalize_crc())
            };

            if let (Some(expected), Some(actual)) = (expected_crc, actual_crc)
                && actual != expected
            {
                return Err(RarError::DataCrcMismatch {
                    member: fh.name.clone(),
                    expected,
                    actual,
                });
            }

            if let (Some(p), Some(mi)) = (progress, mi.as_ref()) {
                p.on_member_progress(mi, unpacked_size);
                p.on_member_complete(mi, &Ok(()));
            }

            return Ok(unpacked_size);
        }

        if archive_format == ArchiveFormat::Rar4
            && fh.compression.method != CompressionMethod::Store
            && !is_solid
        {
            let expected_crc = if options.verify && !skip_hash_verify {
                fh.data_crc32
            } else {
                None
            };

            let segments = self.members[index].segments.clone();
            let base_reader =
                ArchiveSegmentReader::new(&mut self.volumes, &self.limits, &segments, &fh.name);

            let do_crc = expected_crc.is_some();
            let actual_crc = {
                let mut crc_writer = CrcWriter::new(&mut writer, do_crc);
                if let Some(ref pwd) = member_password {
                    let salt = rar4_salt.ok_or_else(|| RarError::CorruptArchive {
                        detail: format!(
                            "RAR4 member {} is marked encrypted but has no salt",
                            fh.name,
                        ),
                    })?;
                    let (key, iv) = self.kdf_cache.derive_key_rar4(pwd, &salt);
                    let reader = crate::crypto::DecryptingReader::new_rar4(base_reader, &key, &iv);
                    crate::decompress::rar4::decompress_rar4_lz_reader_to_writer(
                        reader,
                        unpacked_size,
                        fh.compression.dict_size,
                        &mut crc_writer,
                    )?;
                } else {
                    crate::decompress::rar4::decompress_rar4_lz_reader_to_writer(
                        base_reader,
                        unpacked_size,
                        fh.compression.dict_size,
                        &mut crc_writer,
                    )?;
                }
                crc_writer.flush().map_err(RarError::Io)?;
                expected_crc.map(|_| crc_writer.finalize_crc())
            };

            if let (Some(expected), Some(actual)) = (expected_crc, actual_crc)
                && actual != expected
            {
                return Err(RarError::DataCrcMismatch {
                    member: fh.name.clone(),
                    expected,
                    actual,
                });
            }

            if let (Some(p), Some(mi)) = (progress, mi.as_ref()) {
                p.on_member_progress(mi, unpacked_size);
                p.on_member_complete(mi, &Ok(()));
            }

            return Ok(unpacked_size);
        }

        if is_solid && fh.compression.method != CompressionMethod::Store {
            let expected_crc = if options.verify && !skip_hash_verify {
                fh.data_crc32
            } else {
                None
            };
            let expected_blake = if options.verify && !skip_hash_verify {
                match hash.as_ref() {
                    Some(FileHash::Blake2sp(expected)) => Some(*expected),
                    _ => None,
                }
            } else {
                None
            };

            let written = {
                let mut hash_writer = HashingWriter::new(
                    &mut writer,
                    expected_crc.is_some(),
                    expected_blake.is_some(),
                );
                self.advance_solid_cursor_to(index, &fh)?;
                let segments = self.members[index].segments.clone();
                let base_reader =
                    ArchiveSegmentReader::new(&mut self.volumes, &self.limits, &segments, &fh.name);
                let written = if let Some(ref pwd) = member_password {
                    if archive_format == ArchiveFormat::Rar4 {
                        let salt = rar4_salt.ok_or_else(|| RarError::CorruptArchive {
                            detail: format!(
                                "RAR4 member {} is marked encrypted but has no salt",
                                fh.name,
                            ),
                        })?;
                        let (key, iv) = self.kdf_cache.derive_key_rar4(pwd, &salt);
                        let reader =
                            crate::crypto::DecryptingReader::new_rar4(base_reader, &key, &iv);
                        Self::solid_decode_reader_to_writer(
                            &mut self.solid_decoder_rar4,
                            &mut self.solid_decoder,
                            self.limits.max_dict_size,
                            reader,
                            unpacked_size,
                            &fh,
                            &mut hash_writer,
                        )?
                    } else {
                        let enc_info = file_enc.as_ref().ok_or_else(|| RarError::CorruptArchive {
                            detail: format!(
                                "member {} is marked encrypted but has no encryption parameters",
                                fh.name,
                            ),
                        })?;
                        let key =
                            self.kdf_cache
                                .derive_key_rar5(pwd, &enc_info.salt, enc_info.kdf_count);
                        let reader = crate::crypto::DecryptingReader::new_rar5(
                            base_reader,
                            &key,
                            &enc_info.iv,
                        );
                        Self::solid_decode_reader_to_writer(
                            &mut self.solid_decoder_rar4,
                            &mut self.solid_decoder,
                            self.limits.max_dict_size,
                            reader,
                            unpacked_size,
                            &fh,
                            &mut hash_writer,
                        )?
                    }
                } else {
                    Self::solid_decode_reader_to_writer(
                        &mut self.solid_decoder_rar4,
                        &mut self.solid_decoder,
                        self.limits.max_dict_size,
                        base_reader,
                        unpacked_size,
                        &fh,
                        &mut hash_writer,
                    )?
                };
                hash_writer.flush().map_err(RarError::Io)?;

                if let Some(expected) = expected_crc {
                    let actual = hash_writer.finalize_crc();
                    if actual != expected {
                        return Err(RarError::DataCrcMismatch {
                            member: fh.name.clone(),
                            expected,
                            actual,
                        });
                    }
                }

                if let Some(expected) = expected_blake {
                    let actual = hash_writer.finalize_blake2();
                    if actual != expected {
                        return Err(RarError::Blake2Mismatch {
                            member: fh.name.clone(),
                        });
                    }
                }

                written
            };

            self.solid_next_index = index + 1;

            if let (Some(p), Some(mi)) = (progress, mi.as_ref()) {
                p.on_member_progress(mi, written);
                p.on_member_complete(mi, &Ok(()));
            }
            return Ok(written);
        }

        Err(RarError::CorruptArchive {
            detail: format!(
                "unsupported file extraction path for member {} (method {:?}, solid={is_solid}, format={archive_format:?})",
                fh.name, fh.compression.method,
            ),
        })
    }

    /// Extract a solid member into per-volume chunk writers while preserving
    /// the archive's solid decoder state across sequential members.
    pub fn extract_member_solid_chunked<F>(
        &mut self,
        index: usize,
        options: &ExtractOptions,
        writer_factory: F,
    ) -> RarResult<Vec<(usize, u64)>>
    where
        F: FnMut(usize) -> RarResult<Box<dyn Write>>,
    {
        let entry = self
            .members
            .get(index)
            .ok_or_else(|| RarError::CorruptArchive {
                detail: format!("member index {index} out of range"),
            })?;

        let member_password = if entry.is_encrypted {
            let pwd = options
                .password
                .as_deref()
                .or(self.password.as_deref())
                .ok_or_else(|| RarError::EncryptedMember {
                    member: entry.file_header.name.clone(),
                })?;
            Some(pwd.to_string())
        } else {
            None
        };

        let fh = entry.file_header.clone();
        let file_enc = entry.file_encryption.clone();
        let hash = entry.hash.clone();
        let rar4_salt = entry.rar4_salt;
        let archive_format = self.format;
        let unpacked_size = fh.unpacked_size.unwrap_or(0);
        let skip_hash_verify = file_enc.as_ref().is_some_and(|fe| fe.use_hash_mac);

        if !self.is_solid {
            return Err(RarError::CorruptArchive {
                detail: format!(
                    "member {} is not in a solid archive; use non-solid chunked extraction",
                    fh.name
                ),
            });
        }

        let segments = self.members[index].segments.clone();

        if fh.compression.method != CompressionMethod::Store {
            self.advance_solid_cursor_to(index, &fh)?;

            let volume_tracker = Arc::new(AtomicUsize::new(
                segments.first().map_or(0, |segment| segment.volume_index),
            ));
            let shared_transitions = Arc::new(std::sync::Mutex::new(Vec::new()));
            let base_reader =
                ArchiveSegmentReader::new(&mut self.volumes, &self.limits, &segments, &fh.name)
                    .with_volume_tracker(Arc::clone(&volume_tracker));
            let tracking_reader = VolumeTrackingReader::new(base_reader, volume_tracker)
                .with_shared_transitions(Arc::clone(&shared_transitions));

            let chunks = if let Some(ref pwd) = member_password {
                if archive_format == ArchiveFormat::Rar4 {
                    let salt = rar4_salt.ok_or_else(|| RarError::CorruptArchive {
                        detail: format!(
                            "RAR4 member {} is marked encrypted but has no salt",
                            fh.name,
                        ),
                    })?;
                    let (key, iv) = self.kdf_cache.derive_key_rar4(pwd, &salt);
                    let reader =
                        crate::crypto::DecryptingReader::new_rar4(tracking_reader, &key, &iv);
                    Self::solid_decode_reader_to_writer_chunked(
                        &mut self.solid_decoder_rar4,
                        &mut self.solid_decoder,
                        self.limits.max_dict_size,
                        reader,
                        &fh,
                        unpacked_size,
                        segments.first().map_or(0, |segment| segment.volume_index),
                        shared_transitions,
                        writer_factory,
                        options.verify && !skip_hash_verify,
                    )?
                } else {
                    let enc_info = file_enc.as_ref().ok_or_else(|| RarError::CorruptArchive {
                        detail: format!(
                            "member {} is marked encrypted but has no encryption parameters",
                            fh.name,
                        ),
                    })?;
                    let key =
                        self.kdf_cache
                            .derive_key_rar5(pwd, &enc_info.salt, enc_info.kdf_count);
                    let reader = crate::crypto::DecryptingReader::new_rar5(
                        tracking_reader,
                        &key,
                        &enc_info.iv,
                    );
                    Self::solid_decode_reader_to_writer_chunked(
                        &mut self.solid_decoder_rar4,
                        &mut self.solid_decoder,
                        self.limits.max_dict_size,
                        reader,
                        &fh,
                        unpacked_size,
                        segments.first().map_or(0, |segment| segment.volume_index),
                        shared_transitions,
                        writer_factory,
                        options.verify && !skip_hash_verify,
                    )?
                }
            } else {
                Self::solid_decode_reader_to_writer_chunked(
                    &mut self.solid_decoder_rar4,
                    &mut self.solid_decoder,
                    self.limits.max_dict_size,
                    tracking_reader,
                    &fh,
                    unpacked_size,
                    segments.first().map_or(0, |segment| segment.volume_index),
                    shared_transitions,
                    writer_factory,
                    options.verify && !skip_hash_verify,
                )?
            };

            self.solid_next_index = index + 1;
            return Ok(chunks);
        }

        let expected_crc = if options.verify && !skip_hash_verify {
            fh.data_crc32
        } else {
            None
        };
        let expected_blake = if options.verify && !skip_hash_verify {
            match hash.as_ref() {
                Some(FileHash::Blake2sp(expected)) => Some(expected),
                _ => None,
            }
        } else {
            None
        };

        let volume_tracker = Arc::new(AtomicUsize::new(
            segments.first().map_or(0, |segment| segment.volume_index),
        ));
        let base_reader =
            ArchiveSegmentReader::new(&mut self.volumes, &self.limits, &segments, &fh.name)
                .with_volume_tracker(Arc::clone(&volume_tracker));

        let shared_crc =
            expected_crc.map(|_| Arc::new(std::sync::Mutex::new(crc32fast::Hasher::new())));
        let shared_blake: Option<Arc<std::sync::Mutex<Blake2s256>>> =
            expected_blake.map(|_| Arc::new(std::sync::Mutex::new(Blake2s256::new())));
        let mut writer_factory = writer_factory;

        let chunks = if let Some(ref pwd) = member_password {
            if archive_format == ArchiveFormat::Rar4 {
                let salt = rar4_salt.ok_or_else(|| RarError::CorruptArchive {
                    detail: format!(
                        "RAR4 member {} is marked encrypted but has no salt",
                        fh.name
                    ),
                })?;
                let (key, iv) = self.kdf_cache.derive_key_rar4(pwd, &salt);
                let reader = crate::crypto::DecryptingReader::new_rar4(base_reader, &key, &iv);
                Self::copy_reader_to_writer_chunked(
                    reader,
                    Arc::clone(&volume_tracker),
                    segments.first().map_or(0, |segment| segment.volume_index),
                    |volume_index| {
                        let writer = writer_factory(volume_index)?;
                        Ok(Box::new(HashTrackingWriter {
                            inner: writer,
                            crc: shared_crc.as_ref().map(Arc::clone),
                            blake2: shared_blake.as_ref().map(Arc::clone),
                        }))
                    },
                    Some(unpacked_size),
                )?
            } else {
                let enc_info = file_enc.as_ref().ok_or_else(|| RarError::CorruptArchive {
                    detail: format!(
                        "member {} is marked encrypted but has no encryption parameters",
                        fh.name,
                    ),
                })?;
                if let Some(ref check_data) = enc_info.check_data
                    && !self.kdf_cache.verify_password_rar5(
                        pwd,
                        &enc_info.salt,
                        enc_info.kdf_count,
                        check_data,
                    )
                {
                    return Err(RarError::WrongPassword {
                        member: fh.name.clone(),
                    });
                }
                let key = self
                    .kdf_cache
                    .derive_key_rar5(pwd, &enc_info.salt, enc_info.kdf_count);
                let reader =
                    crate::crypto::DecryptingReader::new_rar5(base_reader, &key, &enc_info.iv);
                Self::copy_reader_to_writer_chunked(
                    reader,
                    Arc::clone(&volume_tracker),
                    segments.first().map_or(0, |segment| segment.volume_index),
                    |volume_index| {
                        let writer = writer_factory(volume_index)?;
                        Ok(Box::new(HashTrackingWriter {
                            inner: writer,
                            crc: shared_crc.as_ref().map(Arc::clone),
                            blake2: shared_blake.as_ref().map(Arc::clone),
                        }))
                    },
                    Some(unpacked_size),
                )?
            }
        } else {
            Self::copy_reader_to_writer_chunked(
                base_reader,
                Arc::clone(&volume_tracker),
                segments.first().map_or(0, |segment| segment.volume_index),
                |volume_index| {
                    let writer = writer_factory(volume_index)?;
                    Ok(Box::new(HashTrackingWriter {
                        inner: writer,
                        crc: shared_crc.as_ref().map(Arc::clone),
                        blake2: shared_blake.as_ref().map(Arc::clone),
                    }))
                },
                None,
            )?
        };

        if let (Some(expected), Some(shared)) = (expected_crc, shared_crc) {
            let actual = Arc::try_unwrap(shared)
                .unwrap()
                .into_inner()
                .unwrap()
                .finalize();
            if actual != expected {
                return Err(RarError::DataCrcMismatch {
                    member: fh.name.clone(),
                    expected,
                    actual,
                });
            }
        }

        if let (Some(expected), Some(shared)) = (expected_blake, shared_blake) {
            let actual: [u8; 32] = Arc::try_unwrap(shared)
                .unwrap()
                .into_inner()
                .unwrap()
                .finalize()
                .into();
            if actual != *expected {
                return Err(RarError::Blake2Mismatch {
                    member: fh.name.clone(),
                });
            }
        }

        Ok(chunks)
    }
    fn solid_decode_reader_to_sink<R: Read>(
        solid_decoder_rar4: &mut Option<Rar4LzDecoder>,
        solid_decoder: &mut Option<LzDecoder>,
        max_dict_size: u64,
        compressed: R,
        unpacked_size: u64,
        fh: &FileHeader,
    ) -> RarResult<()> {
        let mut sink = std::io::sink();
        Self::solid_decode_reader_to_writer(
            solid_decoder_rar4,
            solid_decoder,
            max_dict_size,
            compressed,
            unpacked_size,
            fh,
            &mut sink,
        )?;
        Ok(())
    }

    fn solid_decode_reader_to_writer<R: Read, W: Write>(
        solid_decoder_rar4: &mut Option<Rar4LzDecoder>,
        solid_decoder: &mut Option<LzDecoder>,
        max_dict_size: u64,
        compressed: R,
        unpacked_size: u64,
        fh: &FileHeader,
        writer: &mut W,
    ) -> RarResult<u64> {
        let dict_size = fh.compression.dict_size;
        if dict_size > max_dict_size {
            return Err(RarError::DictionaryTooLarge {
                size: dict_size,
                max: max_dict_size,
            });
        }

        let dict_size = dict_size as usize;
        if fh.compression.format == ArchiveFormat::Rar4 {
            if let Some(decoder) = solid_decoder_rar4 {
                decoder.prepare_solid_continuation();
            } else {
                *solid_decoder_rar4 = Some(Rar4LzDecoder::new(dict_size));
            }
            solid_decoder_rar4
                .as_mut()
                .unwrap()
                .decompress_reader_to_writer(compressed, unpacked_size, writer)
        } else {
            if let Some(decoder) = solid_decoder {
                decoder.prepare_solid_continuation();
            } else {
                *solid_decoder = Some(LzDecoder::new(dict_size));
            }
            solid_decoder.as_mut().unwrap().decompress_reader_to_writer(
                compressed,
                unpacked_size,
                writer,
            )
        }
    }

    fn solid_decode_reader_to_writer_chunked<R: Read, F>(
        solid_decoder_rar4: &mut Option<Rar4LzDecoder>,
        solid_decoder: &mut Option<LzDecoder>,
        max_dict_size: u64,
        compressed: R,
        fh: &FileHeader,
        unpacked_size: u64,
        first_volume_index: usize,
        shared_transitions: Arc<std::sync::Mutex<Vec<crate::decompress::VolumeTransition>>>,
        writer_factory: F,
        verify_crc: bool,
    ) -> RarResult<Vec<(usize, u64)>>
    where
        F: FnMut(usize) -> RarResult<Box<dyn Write>>,
    {
        let dict_size = fh.compression.dict_size;
        if dict_size > max_dict_size {
            return Err(RarError::DictionaryTooLarge {
                size: dict_size,
                max: max_dict_size,
            });
        }

        let dict_size = dict_size as usize;
        let shared_hasher = if verify_crc {
            Some(Arc::new(std::sync::Mutex::new(crc32fast::Hasher::new())))
        } else {
            None
        };
        let mut writer_factory = writer_factory;

        let chunks = if fh.compression.format == ArchiveFormat::Rar4 {
            if let Some(decoder) = solid_decoder_rar4 {
                decoder.prepare_solid_continuation();
            } else {
                *solid_decoder_rar4 = Some(Rar4LzDecoder::new(dict_size));
            }
            let decoder = solid_decoder_rar4.as_mut().unwrap();
            let hasher_clone = shared_hasher.clone();
            decoder.decompress_reader_to_writer_chunked(
                compressed,
                unpacked_size,
                first_volume_index,
                Arc::clone(&shared_transitions),
                |volume_index| {
                    let writer = writer_factory(volume_index)?;
                    if let Some(ref hasher) = hasher_clone {
                        Ok(Box::new(HashTrackingWriter {
                            inner: writer,
                            crc: Some(Arc::clone(hasher)),
                            blake2: None,
                        }))
                    } else {
                        Ok(writer)
                    }
                },
            )?
        } else {
            if let Some(decoder) = solid_decoder {
                decoder.prepare_solid_continuation();
            } else {
                *solid_decoder = Some(LzDecoder::new(dict_size));
            }
            let decoder = solid_decoder.as_mut().unwrap();
            let hasher_clone = shared_hasher.clone();
            decoder.decompress_reader_to_writer_chunked(
                compressed,
                unpacked_size,
                first_volume_index,
                Arc::clone(&shared_transitions),
                |volume_index| {
                    let writer = writer_factory(volume_index)?;
                    if let Some(ref hasher) = hasher_clone {
                        Ok(Box::new(HashTrackingWriter {
                            inner: writer,
                            crc: Some(Arc::clone(hasher)),
                            blake2: None,
                        }))
                    } else {
                        Ok(writer)
                    }
                },
            )?
        };

        if let Some(hasher_arc) = shared_hasher
            && let Some(expected) = fh.data_crc32
        {
            let hasher = Arc::try_unwrap(hasher_arc).unwrap().into_inner().unwrap();
            let actual = hasher.finalize();
            if actual != expected {
                return Err(RarError::DataCrcMismatch {
                    member: fh.name.clone(),
                    expected,
                    actual,
                });
            }
        }

        Ok(chunks)
    }
    /// Extract a member by name, handling any supported compression method.
    ///
    /// Returns the decompressed data as a `Vec<u8>`.
    pub fn extract_by_name(
        &mut self,
        name: &str,
        options: &ExtractOptions,
        progress: Option<&dyn ProgressHandler>,
    ) -> RarResult<crate::extract::ExtractedMember> {
        let index = self
            .find_member(name)
            .ok_or_else(|| RarError::MemberNotFound {
                name: name.to_string(),
            })?;
        self.extract_member(index, options, progress)
    }

    /// Extract a member by streaming segments through a [`VolumeProvider`].
    ///
    /// Unlike `extract_member_to_file`, this does not require all volumes to be
    /// present upfront. It reads segments sequentially, requesting each volume
    /// from the provider as needed — which may block until the volume finishes
    /// downloading.
    ///
    /// Currently supports:
    /// - **Store** (uncompressed): streams directly, minimal memory.
    /// - **LZ** (compressed, non-solid): reads compressed data through a
    ///   [`ChainedSegmentReader`], then streams decompressed output.
    /// - **Encrypted** (Store or LZ): wraps the reader in [`DecryptingReader`]
    ///   for on-the-fly AES-CBC decryption.
    ///
    /// Falls back to the buffered path for solid archives.
    pub fn extract_member_streaming<W: Write>(
        &mut self,
        index: usize,
        options: &ExtractOptions,
        provider: &dyn VolumeProvider,
        writer: &mut W,
    ) -> RarResult<u64> {
        let entry = self
            .members
            .get(index)
            .ok_or_else(|| RarError::CorruptArchive {
                detail: format!("member index {index} out of range"),
            })?;

        let fh = entry.file_header.clone();
        let is_encrypted = entry.is_encrypted;
        let is_solid = fh.compression.solid;
        let hash = entry.hash.clone();
        let file_encryption = entry.file_encryption.clone();
        let rar4_salt = entry.rar4_salt;
        let unpacked_size = fh.unpacked_size.unwrap_or(0);

        let member_password = if is_encrypted {
            let pwd = options
                .password
                .as_deref()
                .or(self.password.as_deref())
                .ok_or_else(|| RarError::EncryptedMember {
                    member: fh.name.clone(),
                })?;
            Some(pwd.to_string())
        } else {
            None
        };

        if is_solid {
            return self.extract_member_streaming_solid(
                index,
                &fh,
                hash.as_ref(),
                options,
                provider,
                writer,
                member_password.as_deref(),
                file_encryption.as_ref(),
                rar4_salt,
                entry.file_header.split_after,
            );
        }

        // For LZ, prefer a reader-backed path when the decoder supports it.
        // ChainedSegmentReader with continuation discovers volumes on demand.
        if fh.compression.method != CompressionMethod::Store {
            let entry = &self.members[index];
            let split_after = entry.file_header.split_after;
            let mut sorted_segs = entry.segments.clone();
            sorted_segs.sort_by_key(|s| s.volume_index);
            // Normalize volume indices to 0-based for the provider.
            // Archive segments use absolute volume numbers (from main header),
            // but the streaming provider uses 0-based local indices.
            let vol_base = sorted_segs.first().map_or(0, |s| s.volume_index);
            for seg in &mut sorted_segs {
                seg.volume_index -= vol_base;
            }

            debug!(
                member = %fh.name,
                method = ?fh.compression.method,
                segments = sorted_segs.len(),
                split_after,
                unpacked_size,
                "streaming LZ extraction starting"
            );

            return self.extract_member_streaming_lz(
                &fh,
                hash.as_ref(),
                options,
                provider,
                &sorted_segs,
                unpacked_size,
                writer,
                member_password.as_deref(),
                file_encryption.as_ref(),
                rar4_salt,
                split_after,
            );
        }

        // Store mode: stream through ChainedSegmentReader with on-demand volume discovery.
        // One continuous reader → one DecryptingReader → maintains AES-CBC state across volumes.
        let entry = &self.members[index];
        let split_after = entry.file_header.split_after;
        let mut sorted_segs = entry.segments.clone();
        sorted_segs.sort_by_key(|s| s.volume_index);
        // Normalize volume indices to 0-based (see LZ path comment above).
        let vol_base = sorted_segs.first().map_or(0, |s| s.volume_index);
        for seg in &mut sorted_segs {
            seg.volume_index -= vol_base;
        }

        debug!(
            member = %fh.name,
            encrypted = is_encrypted,
            segments = sorted_segs.len(),
            split_after,
            unpacked_size,
            "streaming Store extraction starting"
        );

        self.extract_member_streaming_store(
            &fh,
            hash.as_ref(),
            options,
            provider,
            &sorted_segs,
            unpacked_size,
            writer,
            member_password.as_deref(),
            file_encryption.as_ref(),
            rar4_salt,
            split_after,
        )
    }

    /// Streaming extraction for Store (uncompressed) members.
    ///
    /// Uses `ChainedSegmentReader` with on-demand volume discovery. A single
    /// `DecryptingReader` wraps the entire stream, maintaining continuous
    /// AES-CBC state across volume boundaries.
    #[allow(clippy::too_many_arguments)]
    fn extract_member_streaming_solid<W: Write>(
        &mut self,
        index: usize,
        fh: &FileHeader,
        hash: Option<&FileHash>,
        options: &ExtractOptions,
        provider: &dyn VolumeProvider,
        writer: &mut W,
        password: Option<&str>,
        file_encryption: Option<&FileEncryptionInfo>,
        rar4_salt: Option<[u8; 8]>,
        split_after: bool,
    ) -> RarResult<u64> {
        let skip_hash_verify = file_encryption.is_some_and(|fe| fe.use_hash_mac);
        let (segments, _) = Self::normalized_provider_segments(&self.members[index].segments);

        if fh.compression.method == CompressionMethod::Store {
            return self.extract_member_streaming_store(
                fh,
                hash,
                options,
                provider,
                &segments,
                fh.unpacked_size.unwrap_or(0),
                writer,
                password,
                file_encryption,
                rar4_salt,
                split_after,
            );
        }

        let expected_blake = if options.verify && !skip_hash_verify {
            match hash {
                Some(FileHash::Blake2sp(expected)) => Some(*expected),
                _ => None,
            }
        } else {
            None
        };

        self.advance_solid_cursor_to_streaming(index, fh, provider)?;

        let cont_meta = Rc::new(RefCell::new(ContinuationMetadata::default()));
        let chained = ChainedSegmentReader::new(&segments, provider)
            .with_continuation(split_after, self.format, self.password.clone())
            .with_metadata_sink(Rc::clone(&cont_meta));
        let unpacked_size = fh.unpacked_size.unwrap_or(0);

        let (written, actual_crc, actual_blake) = {
            let mut hash_writer = HashingWriter::new(
                writer,
                options.verify && !skip_hash_verify,
                expected_blake.is_some(),
            );

            let written = if let Some(pwd) = password {
                if self.format == ArchiveFormat::Rar4 {
                    let salt = rar4_salt.ok_or_else(|| RarError::CorruptArchive {
                        detail: format!(
                            "RAR4 member {} is marked encrypted but has no salt",
                            fh.name,
                        ),
                    })?;
                    let (key, iv) = self.kdf_cache.derive_key_rar4(pwd, &salt);
                    let reader = crate::crypto::DecryptingReader::new_rar4(chained, &key, &iv);
                    Self::solid_decode_reader_to_writer(
                        &mut self.solid_decoder_rar4,
                        &mut self.solid_decoder,
                        self.limits.max_dict_size,
                        reader,
                        unpacked_size,
                        fh,
                        &mut hash_writer,
                    )?
                } else {
                    let enc_info = file_encryption.ok_or_else(|| RarError::CorruptArchive {
                        detail: format!(
                            "member {} is marked encrypted but has no encryption parameters",
                            fh.name,
                        ),
                    })?;
                    if let Some(ref check_data) = enc_info.check_data
                        && !self.kdf_cache.verify_password_rar5(
                            pwd,
                            &enc_info.salt,
                            enc_info.kdf_count,
                            check_data,
                        )
                    {
                        return Err(RarError::WrongPassword {
                            member: fh.name.clone(),
                        });
                    }
                    let key =
                        self.kdf_cache
                            .derive_key_rar5(pwd, &enc_info.salt, enc_info.kdf_count);
                    let reader =
                        crate::crypto::DecryptingReader::new_rar5(chained, &key, &enc_info.iv);
                    Self::solid_decode_reader_to_writer(
                        &mut self.solid_decoder_rar4,
                        &mut self.solid_decoder,
                        self.limits.max_dict_size,
                        reader,
                        unpacked_size,
                        fh,
                        &mut hash_writer,
                    )?
                }
            } else {
                Self::solid_decode_reader_to_writer(
                    &mut self.solid_decoder_rar4,
                    &mut self.solid_decoder,
                    self.limits.max_dict_size,
                    chained,
                    unpacked_size,
                    fh,
                    &mut hash_writer,
                )?
            };

            (
                written,
                hash_writer.finalize_crc(),
                expected_blake.map(|_| hash_writer.finalize_blake2()),
            )
        };

        let final_meta = cont_meta.borrow();
        let effective_crc = final_meta.data_crc32.or(fh.data_crc32);
        let final_skip_hash = final_meta.use_hash_mac;
        drop(final_meta);

        if options.verify
            && !final_skip_hash
            && let Some(expected) = effective_crc
            && actual_crc != expected
        {
            return Err(RarError::DataCrcMismatch {
                member: fh.name.clone(),
                expected,
                actual: actual_crc,
            });
        }

        if !final_skip_hash
            && let (Some(expected), Some(actual)) = (expected_blake, actual_blake)
            && actual != expected
        {
            return Err(RarError::Blake2Mismatch {
                member: fh.name.clone(),
            });
        }

        self.solid_next_index = index + 1;
        Ok(written)
    }

    #[allow(clippy::too_many_arguments)]
    fn extract_member_streaming_store<W: Write>(
        &self,
        fh: &FileHeader,
        hash: Option<&FileHash>,
        options: &ExtractOptions,
        provider: &dyn VolumeProvider,
        segments: &[DataSegment],
        unpacked_size: u64,
        writer: &mut W,
        password: Option<&str>,
        file_encryption: Option<&FileEncryptionInfo>,
        rar4_salt: Option<[u8; 8]>,
        split_after: bool,
    ) -> RarResult<u64> {
        let cont_meta = Rc::new(RefCell::new(ContinuationMetadata::default()));
        let chained = ChainedSegmentReader::new(segments, provider)
            .with_continuation(split_after, self.format, self.password.clone())
            .with_metadata_sink(Rc::clone(&cont_meta));
        let skip_hash_verify = file_encryption.is_some_and(|fe| fe.use_hash_mac);

        // Wrap in DecryptingReader if encrypted, otherwise read directly.
        let mut hasher = if options.verify && !skip_hash_verify {
            Some(crc32fast::Hasher::new())
        } else {
            None
        };
        let mut blake_hasher = if options.verify && !skip_hash_verify {
            match hash {
                Some(FileHash::Blake2sp(_)) => Some(Blake2s256::new()),
                _ => None,
            }
        } else {
            None
        };
        let mut written = 0u64;
        let mut chunk = vec![0u8; STREAMING_STORE_CHUNK_BUFFER_BYTES];

        // For encrypted Store members, use unpacked_size to know when to stop
        // (decrypted data may have AES padding at the end).
        let max_bytes = if password.is_some() {
            unpacked_size
        } else {
            u64::MAX
        };

        let mut reader: Box<dyn Read> = if let Some(pwd) = password {
            if self.format == ArchiveFormat::Rar4 {
                let salt = rar4_salt.ok_or_else(|| RarError::CorruptArchive {
                    detail: format!(
                        "RAR4 member {} is marked encrypted but has no salt",
                        fh.name,
                    ),
                })?;
                let (key, iv) = self.kdf_cache.derive_key_rar4(pwd, &salt);
                Box::new(crate::crypto::DecryptingReader::new_rar4(
                    chained, &key, &iv,
                ))
            } else {
                let enc_info = file_encryption.ok_or_else(|| RarError::CorruptArchive {
                    detail: format!(
                        "member {} is marked encrypted but has no encryption parameters",
                        fh.name,
                    ),
                })?;
                if let Some(ref check_data) = enc_info.check_data
                    && !self.kdf_cache.verify_password_rar5(
                        pwd,
                        &enc_info.salt,
                        enc_info.kdf_count,
                        check_data,
                    )
                {
                    return Err(RarError::WrongPassword {
                        member: fh.name.clone(),
                    });
                }
                let key = self
                    .kdf_cache
                    .derive_key_rar5(pwd, &enc_info.salt, enc_info.kdf_count);
                Box::new(crate::crypto::DecryptingReader::new_rar5(
                    chained,
                    &key,
                    &enc_info.iv,
                ))
            }
        } else {
            Box::new(chained)
        };

        loop {
            let to_read = chunk.len().min((max_bytes - written) as usize);
            if to_read == 0 {
                break;
            }
            let n = reader.read(&mut chunk[..to_read]).map_err(RarError::Io)?;
            if n == 0 {
                break;
            }
            writer.write_all(&chunk[..n]).map_err(RarError::Io)?;
            if let Some(ref mut h) = hasher {
                h.update(&chunk[..n]);
            }
            if let Some(ref mut h) = blake_hasher {
                h.update(&chunk[..n]);
            }
            written += n as u64;
        }

        writer.flush().map_err(RarError::Io)?;

        // Use final volume's CRC and HMAC flag if continuations were discovered.
        let final_meta = cont_meta.borrow();
        let effective_crc = final_meta.data_crc32.or(fh.data_crc32);
        let final_skip_hash = final_meta.use_hash_mac;
        drop(final_meta);

        // Verify CRC32 (skip if final volume uses HMAC-transformed hashes).
        if let Some(h) = hasher
            && !final_skip_hash
            && let Some(expected) = effective_crc
        {
            let actual = h.finalize();
            if actual != expected {
                return Err(RarError::DataCrcMismatch {
                    member: fh.name.clone(),
                    expected,
                    actual,
                });
            }
        }

        if let Some(h) = blake_hasher
            && !final_skip_hash
            && let Some(FileHash::Blake2sp(expected)) = hash
        {
            let actual: [u8; 32] = h.finalize().into();
            if actual != *expected {
                return Err(RarError::Blake2Mismatch {
                    member: fh.name.clone(),
                });
            }
        }

        Ok(written)
    }

    /// Streaming extraction for LZ-compressed (non-solid) members.
    ///
    /// Uses a `ChainedSegmentReader` with on-demand volume discovery to provide
    /// the compressed bitstream. Volumes are fetched as the decompressor consumes
    /// data. For encrypted members, wraps in `DecryptingReader`.
    #[allow(clippy::too_many_arguments)]
    fn extract_member_streaming_lz<W: Write>(
        &self,
        fh: &FileHeader,
        hash: Option<&FileHash>,
        options: &ExtractOptions,
        provider: &dyn VolumeProvider,
        segments: &[DataSegment],
        unpacked_size: u64,
        writer: &mut W,
        password: Option<&str>,
        file_encryption: Option<&FileEncryptionInfo>,
        rar4_salt: Option<[u8; 8]>,
        split_after: bool,
    ) -> RarResult<u64> {
        let skip_hash_verify = file_encryption.is_some_and(|fe| fe.use_hash_mac);
        // Build the chained reader first. For RAR5 this can feed the decoder
        // directly; RAR4 still falls back to a buffered compressed-input path
        // because its PPM path depends on contiguous remaining bytes.
        let cont_meta = Rc::new(RefCell::new(ContinuationMetadata::default()));
        let chained = ChainedSegmentReader::new(segments, provider)
            .with_continuation(split_after, self.format, self.password.clone())
            .with_metadata_sink(Rc::clone(&cont_meta));

        // Wrap in DecryptingReader if encrypted.
        let inner: Box<dyn Read> = if let Some(pwd) = password {
            if self.format == ArchiveFormat::Rar4 {
                let salt = rar4_salt.ok_or_else(|| RarError::CorruptArchive {
                    detail: format!(
                        "RAR4 member {} is marked encrypted but has no salt",
                        fh.name,
                    ),
                })?;
                let (key, iv) = self.kdf_cache.derive_key_rar4(pwd, &salt);
                Box::new(crate::crypto::DecryptingReader::new_rar4(
                    chained, &key, &iv,
                ))
            } else {
                let enc_info = file_encryption.ok_or_else(|| RarError::CorruptArchive {
                    detail: format!(
                        "member {} is marked encrypted but has no encryption parameters",
                        fh.name,
                    ),
                })?;
                if let Some(ref check_data) = enc_info.check_data
                    && !self.kdf_cache.verify_password_rar5(
                        pwd,
                        &enc_info.salt,
                        enc_info.kdf_count,
                        check_data,
                    )
                {
                    return Err(RarError::WrongPassword {
                        member: fh.name.clone(),
                    });
                }
                let key = self
                    .kdf_cache
                    .derive_key_rar5(pwd, &enc_info.salt, enc_info.kdf_count);
                Box::new(crate::crypto::DecryptingReader::new_rar5(
                    chained,
                    &key,
                    &enc_info.iv,
                ))
            }
        } else {
            Box::new(chained)
        };

        let do_crc = options.verify && !skip_hash_verify;
        let expected_blake = if options.verify && !skip_hash_verify {
            match hash {
                Some(FileHash::Blake2sp(expected)) => Some(*expected),
                _ => None,
            }
        } else {
            None
        };
        let mut hash_writer = HashingWriter::new(writer, do_crc, expected_blake.is_some());

        if self.format == ArchiveFormat::Rar5 {
            let mut buf_reader = BufReader::with_capacity(1024 * 1024, inner);
            crate::decompress::lz::decompress_lz_reader_to_writer(
                &mut buf_reader,
                unpacked_size,
                &fh.compression,
                &mut hash_writer,
            )?;
        } else {
            let mut buf_reader = BufReader::with_capacity(1024 * 1024, inner);
            crate::decompress::rar4::decompress_rar4_lz_reader_to_writer(
                &mut buf_reader,
                unpacked_size,
                fh.compression.dict_size,
                &mut hash_writer,
            )?;
        }

        hash_writer.flush().map_err(RarError::Io)?;

        // Use final volume's CRC and HMAC flag if continuations were discovered.
        let final_meta = cont_meta.borrow();
        let effective_crc = final_meta.data_crc32.or(fh.data_crc32);
        let final_skip_hash = final_meta.use_hash_mac;
        drop(final_meta);

        // Verify CRC32 (skip if final volume uses HMAC-transformed hashes).
        if let Some(expected) = effective_crc.filter(|_| do_crc && !final_skip_hash) {
            let actual = hash_writer.finalize_crc();
            if actual != expected {
                return Err(RarError::DataCrcMismatch {
                    member: fh.name.clone(),
                    expected,
                    actual,
                });
            }
        }

        if !final_skip_hash && let Some(expected) = expected_blake {
            let actual = hash_writer.finalize_blake2();
            if actual != expected {
                return Err(RarError::Blake2Mismatch {
                    member: fh.name.clone(),
                });
            }
        }

        Ok(unpacked_size)
    }

    /// Extract a member with per-volume output splitting.
    ///
    /// Calls `writer_factory(volume_index)` at each volume transition to get a
    /// new writer. Each writer receives that volume's decompressed contribution.
    /// Returns `Vec<(volume_index, bytes_written)>` for each chunk.
    ///
    /// For Store mode: detects volume transitions via the volume tracker and
    /// switches writers at each boundary.
    ///
    /// For LZ mode: wraps the compressed reader in a `VolumeTrackingReader`
    /// to record compressed byte offsets at volume transitions, then uses
    /// `decompress_to_writer_chunked` to split output at those boundaries.
    pub fn extract_member_streaming_chunked<F>(
        &mut self,
        index: usize,
        options: &ExtractOptions,
        provider: &dyn VolumeProvider,
        writer_factory: F,
    ) -> RarResult<Vec<(usize, u64)>>
    where
        F: FnMut(usize) -> RarResult<Box<dyn Write>>,
    {
        let entry = self
            .members
            .get(index)
            .ok_or_else(|| RarError::CorruptArchive {
                detail: format!("member index {index} out of range"),
            })?;

        let fh = entry.file_header.clone();
        let is_encrypted = entry.is_encrypted;
        let is_solid = fh.compression.solid;
        let file_encryption = entry.file_encryption.clone();
        let rar4_salt = entry.rar4_salt;
        let unpacked_size = fh.unpacked_size.unwrap_or(0);

        let member_password = if is_encrypted {
            let pwd = options
                .password
                .as_deref()
                .or(self.password.as_deref())
                .ok_or_else(|| RarError::EncryptedMember {
                    member: fh.name.clone(),
                })?;
            Some(pwd.to_string())
        } else {
            None
        };

        if is_solid {
            return self.extract_member_streaming_solid_chunked(
                index,
                &fh,
                options,
                provider,
                writer_factory,
                member_password.as_deref(),
                file_encryption.as_ref(),
                rar4_salt,
                entry.file_header.split_after,
            );
        }

        let entry = &self.members[index];
        let split_after = entry.file_header.split_after;
        let mut sorted_segs = entry.segments.clone();
        sorted_segs.sort_by_key(|s| s.volume_index);
        let vol_base = sorted_segs.first().map_or(0, |s| s.volume_index);
        for seg in &mut sorted_segs {
            seg.volume_index -= vol_base;
        }
        let first_vol = sorted_segs.first().map_or(0, |s| s.volume_index);

        if fh.compression.method != CompressionMethod::Store {
            debug!(
                member = %fh.name,
                method = ?fh.compression.method,
                segments = sorted_segs.len(),
                split_after,
                unpacked_size,
                "streaming chunked LZ extraction starting"
            );
            return self.extract_member_streaming_lz_chunked(
                &fh,
                options,
                provider,
                &sorted_segs,
                unpacked_size,
                first_vol,
                writer_factory,
                member_password.as_deref(),
                file_encryption.as_ref(),
                rar4_salt,
                split_after,
            );
        }

        debug!(
            member = %fh.name,
            encrypted = is_encrypted,
            segments = sorted_segs.len(),
            split_after,
            unpacked_size,
            "streaming chunked Store extraction starting"
        );
        self.extract_member_streaming_store_chunked(
            &fh,
            options,
            provider,
            &sorted_segs,
            unpacked_size,
            first_vol,
            writer_factory,
            member_password.as_deref(),
            file_encryption.as_ref(),
            rar4_salt,
            split_after,
        )
    }

    /// Chunked Store extraction: switches writers at volume boundaries.
    #[allow(clippy::too_many_arguments)]
    fn extract_member_streaming_store_chunked<F>(
        &self,
        fh: &FileHeader,
        options: &ExtractOptions,
        provider: &dyn VolumeProvider,
        segments: &[DataSegment],
        unpacked_size: u64,
        first_vol: usize,
        mut writer_factory: F,
        password: Option<&str>,
        file_encryption: Option<&FileEncryptionInfo>,
        rar4_salt: Option<[u8; 8]>,
        split_after: bool,
    ) -> RarResult<Vec<(usize, u64)>>
    where
        F: FnMut(usize) -> RarResult<Box<dyn Write>>,
    {
        let volume_tracker = Arc::new(AtomicUsize::new(first_vol));
        let cont_meta = Rc::new(RefCell::new(ContinuationMetadata::default()));
        let chained = ChainedSegmentReader::new(segments, provider)
            .with_continuation(split_after, self.format, self.password.clone())
            .with_metadata_sink(Rc::clone(&cont_meta))
            .with_volume_tracker(Arc::clone(&volume_tracker));
        let skip_hash_verify = file_encryption.is_some_and(|fe| fe.use_hash_mac);

        let mut hasher = if options.verify && !skip_hash_verify {
            Some(crc32fast::Hasher::new())
        } else {
            None
        };

        let max_bytes = if password.is_some() {
            unpacked_size
        } else {
            u64::MAX
        };

        let mut reader: Box<dyn Read> = if let Some(pwd) = password {
            if self.format == ArchiveFormat::Rar4 {
                let salt = rar4_salt.ok_or_else(|| RarError::CorruptArchive {
                    detail: format!(
                        "RAR4 member {} is marked encrypted but has no salt",
                        fh.name
                    ),
                })?;
                let (key, iv) = self.kdf_cache.derive_key_rar4(pwd, &salt);
                Box::new(crate::crypto::DecryptingReader::new_rar4(
                    chained, &key, &iv,
                ))
            } else {
                let enc_info = file_encryption.ok_or_else(|| RarError::CorruptArchive {
                    detail: format!(
                        "member {} is marked encrypted but has no encryption parameters",
                        fh.name
                    ),
                })?;
                if let Some(ref check_data) = enc_info.check_data
                    && !self.kdf_cache.verify_password_rar5(
                        pwd,
                        &enc_info.salt,
                        enc_info.kdf_count,
                        check_data,
                    )
                {
                    return Err(RarError::WrongPassword {
                        member: fh.name.clone(),
                    });
                }
                let key = self
                    .kdf_cache
                    .derive_key_rar5(pwd, &enc_info.salt, enc_info.kdf_count);
                Box::new(crate::crypto::DecryptingReader::new_rar5(
                    chained,
                    &key,
                    &enc_info.iv,
                ))
            }
        } else {
            Box::new(chained)
        };

        let mut chunks: Vec<(usize, u64)> = Vec::new();
        let mut current_vol = first_vol;
        let mut current_writer = writer_factory(current_vol)?;
        let mut chunk_bytes: u64 = 0;
        let mut total_written = 0u64;
        let mut chunk = vec![0u8; 256 * 1024];

        loop {
            let to_read = chunk.len().min((max_bytes - total_written) as usize);
            if to_read == 0 {
                break;
            }
            let n = reader.read(&mut chunk[..to_read]).map_err(RarError::Io)?;
            if n == 0 {
                break;
            }

            if let Some(ref mut h) = hasher {
                h.update(&chunk[..n]);
            }

            // Check for volume transition.
            let new_vol = volume_tracker.load(Ordering::Acquire);
            if new_vol != current_vol {
                current_writer.flush().map_err(RarError::Io)?;
                chunks.push((current_vol, chunk_bytes));
                current_vol = new_vol;
                current_writer = writer_factory(current_vol)?;
                chunk_bytes = 0;
            }

            current_writer
                .write_all(&chunk[..n])
                .map_err(RarError::Io)?;
            chunk_bytes += n as u64;
            total_written += n as u64;
        }

        current_writer.flush().map_err(RarError::Io)?;
        if chunk_bytes > 0 || chunks.is_empty() {
            chunks.push((current_vol, chunk_bytes));
        }

        // Verify CRC32.
        let final_meta = cont_meta.borrow();
        let effective_crc = final_meta.data_crc32.or(fh.data_crc32);
        let final_skip_hash = final_meta.use_hash_mac;
        drop(final_meta);

        if let Some(h) = hasher
            && !final_skip_hash
            && let Some(expected) = effective_crc
        {
            let actual = h.finalize();
            if actual != expected {
                return Err(RarError::DataCrcMismatch {
                    member: fh.name.clone(),
                    expected,
                    actual,
                });
            }
        }

        Ok(chunks)
    }

    #[allow(clippy::too_many_arguments)]
    fn extract_member_streaming_solid_chunked<F>(
        &mut self,
        index: usize,
        fh: &FileHeader,
        options: &ExtractOptions,
        provider: &dyn VolumeProvider,
        writer_factory: F,
        password: Option<&str>,
        file_encryption: Option<&FileEncryptionInfo>,
        rar4_salt: Option<[u8; 8]>,
        split_after: bool,
    ) -> RarResult<Vec<(usize, u64)>>
    where
        F: FnMut(usize) -> RarResult<Box<dyn Write>>,
    {
        let skip_hash_verify = file_encryption.is_some_and(|fe| fe.use_hash_mac);
        let (segments, first_vol) =
            Self::normalized_provider_segments(&self.members[index].segments);

        if fh.compression.method == CompressionMethod::Store {
            return self.extract_member_streaming_store_chunked(
                fh,
                options,
                provider,
                &segments,
                fh.unpacked_size.unwrap_or(0),
                first_vol,
                writer_factory,
                password,
                file_encryption,
                rar4_salt,
                split_after,
            );
        }

        self.advance_solid_cursor_to_streaming(index, fh, provider)?;

        let volume_tracker = Arc::new(AtomicUsize::new(first_vol));
        let shared_transitions = Arc::new(std::sync::Mutex::new(Vec::new()));
        let cont_meta = Rc::new(RefCell::new(ContinuationMetadata::default()));
        let chained = ChainedSegmentReader::new(&segments, provider)
            .with_continuation(split_after, self.format, self.password.clone())
            .with_metadata_sink(Rc::clone(&cont_meta))
            .with_volume_tracker(Arc::clone(&volume_tracker));
        let tracking_reader = VolumeTrackingReader::new(chained, volume_tracker)
            .with_shared_transitions(Arc::clone(&shared_transitions));
        let unpacked_size = fh.unpacked_size.unwrap_or(0);

        let chunks = if let Some(pwd) = password {
            if self.format == ArchiveFormat::Rar4 {
                let salt = rar4_salt.ok_or_else(|| RarError::CorruptArchive {
                    detail: format!(
                        "RAR4 member {} is marked encrypted but has no salt",
                        fh.name,
                    ),
                })?;
                let (key, iv) = self.kdf_cache.derive_key_rar4(pwd, &salt);
                let reader = crate::crypto::DecryptingReader::new_rar4(tracking_reader, &key, &iv);
                Self::solid_decode_reader_to_writer_chunked(
                    &mut self.solid_decoder_rar4,
                    &mut self.solid_decoder,
                    self.limits.max_dict_size,
                    reader,
                    fh,
                    unpacked_size,
                    first_vol,
                    shared_transitions,
                    writer_factory,
                    options.verify && !skip_hash_verify,
                )?
            } else {
                let enc_info = file_encryption.ok_or_else(|| RarError::CorruptArchive {
                    detail: format!(
                        "member {} is marked encrypted but has no encryption parameters",
                        fh.name,
                    ),
                })?;
                if let Some(ref check_data) = enc_info.check_data
                    && !self.kdf_cache.verify_password_rar5(
                        pwd,
                        &enc_info.salt,
                        enc_info.kdf_count,
                        check_data,
                    )
                {
                    return Err(RarError::WrongPassword {
                        member: fh.name.clone(),
                    });
                }
                let key = self
                    .kdf_cache
                    .derive_key_rar5(pwd, &enc_info.salt, enc_info.kdf_count);
                let reader =
                    crate::crypto::DecryptingReader::new_rar5(tracking_reader, &key, &enc_info.iv);
                Self::solid_decode_reader_to_writer_chunked(
                    &mut self.solid_decoder_rar4,
                    &mut self.solid_decoder,
                    self.limits.max_dict_size,
                    reader,
                    fh,
                    unpacked_size,
                    first_vol,
                    shared_transitions,
                    writer_factory,
                    options.verify && !skip_hash_verify,
                )?
            }
        } else {
            Self::solid_decode_reader_to_writer_chunked(
                &mut self.solid_decoder_rar4,
                &mut self.solid_decoder,
                self.limits.max_dict_size,
                tracking_reader,
                fh,
                unpacked_size,
                first_vol,
                shared_transitions,
                writer_factory,
                options.verify && !skip_hash_verify,
            )?
        };

        self.solid_next_index = index + 1;
        Ok(chunks)
    }

    /// Chunked LZ extraction: records volume transitions during compressed read,
    /// then splits decompressed output at those boundaries.
    #[allow(clippy::too_many_arguments)]
    fn extract_member_streaming_lz_chunked<F>(
        &self,
        fh: &FileHeader,
        options: &ExtractOptions,
        provider: &dyn VolumeProvider,
        segments: &[DataSegment],
        unpacked_size: u64,
        first_vol: usize,
        mut writer_factory: F,
        password: Option<&str>,
        file_encryption: Option<&FileEncryptionInfo>,
        rar4_salt: Option<[u8; 8]>,
        split_after: bool,
    ) -> RarResult<Vec<(usize, u64)>>
    where
        F: FnMut(usize) -> RarResult<Box<dyn Write>>,
    {
        let skip_hash_verify = file_encryption.is_some_and(|fe| fe.use_hash_mac);
        let volume_tracker = Arc::new(AtomicUsize::new(first_vol));
        let cont_meta = Rc::new(RefCell::new(ContinuationMetadata::default()));
        let chained = ChainedSegmentReader::new(segments, provider)
            .with_continuation(split_after, self.format, self.password.clone())
            .with_metadata_sink(Rc::clone(&cont_meta))
            .with_volume_tracker(Arc::clone(&volume_tracker));

        // Build reader chain, wrapping in DecryptingReader if encrypted.
        let inner: Box<dyn Read> = if let Some(pwd) = password {
            if self.format == ArchiveFormat::Rar4 {
                let salt = rar4_salt.ok_or_else(|| RarError::CorruptArchive {
                    detail: format!(
                        "RAR4 member {} is marked encrypted but has no salt",
                        fh.name
                    ),
                })?;
                let (key, iv) = self.kdf_cache.derive_key_rar4(pwd, &salt);
                Box::new(crate::crypto::DecryptingReader::new_rar4(
                    chained, &key, &iv,
                ))
            } else {
                let enc_info = file_encryption.ok_or_else(|| RarError::CorruptArchive {
                    detail: format!(
                        "member {} is marked encrypted but has no encryption parameters",
                        fh.name
                    ),
                })?;
                if let Some(ref check_data) = enc_info.check_data
                    && !self.kdf_cache.verify_password_rar5(
                        pwd,
                        &enc_info.salt,
                        enc_info.kdf_count,
                        check_data,
                    )
                {
                    return Err(RarError::WrongPassword {
                        member: fh.name.clone(),
                    });
                }
                let key = self
                    .kdf_cache
                    .derive_key_rar5(pwd, &enc_info.salt, enc_info.kdf_count);
                Box::new(crate::crypto::DecryptingReader::new_rar5(
                    chained,
                    &key,
                    &enc_info.iv,
                ))
            }
        } else {
            Box::new(chained)
        };

        // Decompress with chunked output splitting.
        let do_crc = options.verify && !skip_hash_verify;
        let shared_hasher: Option<Arc<std::sync::Mutex<crc32fast::Hasher>>> = if do_crc {
            Some(Arc::new(std::sync::Mutex::new(crc32fast::Hasher::new())))
        } else {
            None
        };

        let chunks = if self.format == ArchiveFormat::Rar5 {
            let shared_transitions = Arc::new(std::sync::Mutex::new(Vec::new()));
            let tracking_reader = VolumeTrackingReader::new(
                BufReader::with_capacity(1024 * 1024, inner),
                volume_tracker,
            )
            .with_shared_transitions(Arc::clone(&shared_transitions));

            let hasher_clone = shared_hasher.clone();
            crate::decompress::lz::decompress_lz_reader_to_writer_chunked(
                tracking_reader,
                unpacked_size,
                &fh.compression,
                first_vol,
                shared_transitions,
                |vol_idx| {
                    let writer = writer_factory(vol_idx)?;
                    if let Some(ref h) = hasher_clone {
                        Ok(Box::new(HashTrackingWriter {
                            inner: writer,
                            crc: Some(Arc::clone(h)),
                            blake2: None,
                        }))
                    } else {
                        Ok(writer)
                    }
                },
            )?
        } else {
            let shared_transitions = Arc::new(std::sync::Mutex::new(Vec::new()));
            let tracking_reader = VolumeTrackingReader::new(
                BufReader::with_capacity(1024 * 1024, inner),
                volume_tracker,
            )
            .with_shared_transitions(Arc::clone(&shared_transitions));

            let hasher_clone = shared_hasher.clone();
            crate::decompress::rar4::decompress_rar4_lz_reader_to_writer_chunked(
                tracking_reader,
                unpacked_size,
                fh.compression.dict_size,
                first_vol,
                shared_transitions,
                |vol_idx| {
                    let writer = writer_factory(vol_idx)?;
                    if let Some(ref h) = hasher_clone {
                        Ok(Box::new(HashTrackingWriter {
                            inner: writer,
                            crc: Some(Arc::clone(h)),
                            blake2: None,
                        }))
                    } else {
                        Ok(writer)
                    }
                },
            )?
        };

        // Verify CRC32.
        let final_meta = cont_meta.borrow();
        let effective_crc = final_meta.data_crc32.or(fh.data_crc32);
        let final_skip_hash = final_meta.use_hash_mac;
        drop(final_meta);

        if let Some(hasher_arc) = shared_hasher
            && !final_skip_hash
            && let Some(expected) = effective_crc
        {
            let h = Arc::try_unwrap(hasher_arc).unwrap().into_inner().unwrap();
            let actual = h.finalize();
            if actual != expected {
                return Err(RarError::DataCrcMismatch {
                    member: fh.name.clone(),
                    expected,
                    actual,
                });
            }
        }

        Ok(chunks)
    }
}

/// Writer wrapper that updates a shared CRC hasher.
struct HashingWriter<'a, W: Write> {
    inner: &'a mut W,
    crc: Option<crc32fast::Hasher>,
    blake2: Option<Blake2s256>,
}

impl<'a, W: Write> HashingWriter<'a, W> {
    fn new(inner: &'a mut W, compute_crc: bool, compute_blake2: bool) -> Self {
        Self {
            inner,
            crc: compute_crc.then(crc32fast::Hasher::new),
            blake2: compute_blake2.then(Blake2s256::new),
        }
    }

    fn finalize_crc(&self) -> u32 {
        self.crc
            .as_ref()
            .map(|hasher| hasher.clone().finalize())
            .unwrap_or(0)
    }

    fn finalize_blake2(&self) -> [u8; 32] {
        self.blake2
            .as_ref()
            .map(|hasher| {
                let digest: [u8; 32] = hasher.clone().finalize().into();
                digest
            })
            .unwrap_or([0; 32])
    }
}

impl<W: Write> Write for HashingWriter<'_, W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let written = self.inner.write(buf)?;
        if let Some(ref mut hasher) = self.crc {
            hasher.update(&buf[..written]);
        }
        if let Some(ref mut hasher) = self.blake2 {
            hasher.update(&buf[..written]);
        }
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

/// Writer wrapper that updates shared hashes while forwarding writes.
struct HashTrackingWriter<W: Write> {
    inner: W,
    crc: Option<Arc<std::sync::Mutex<crc32fast::Hasher>>>,
    blake2: Option<Arc<std::sync::Mutex<Blake2s256>>>,
}

impl<W: Write> Write for HashTrackingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        if let Some(ref hasher) = self.crc {
            hasher.lock().unwrap().update(&buf[..n]);
        }
        if let Some(ref hasher) = self.blake2 {
            hasher.lock().unwrap().update(&buf[..n]);
        }
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

/// Metadata captured from continuation headers discovered during streaming.
///
/// As `ChainedSegmentReader` discovers continuation volumes, it updates
/// this with the latest header's CRC and encryption flags. After all
/// segments are consumed, the values here are from the final volume —
/// which is authoritative for whole-file CRC verification.
#[derive(Debug, Clone, Default)]
pub(super) struct ContinuationMetadata {
    /// CRC32 from the most recently discovered continuation header.
    data_crc32: Option<u32>,
    /// Whether the most recently discovered continuation has HASHMAC set.
    use_hash_mac: bool,
}

/// A `Read` adapter that chains data segments across volumes.
///
/// When the current segment is exhausted, it fetches the next volume from the
/// `VolumeProvider` — which may block if that volume hasn't finished downloading.
///
/// If `split_after` is true, when all known segments are consumed it will
/// fetch the next volume from the provider, parse its headers to discover
/// the continuation segment, and keep reading. This enables incremental
/// extraction — bytes flow to the output as each volume arrives.
pub struct ChainedSegmentReader<'a> {
    segments: Vec<DataSegment>,
    provider: &'a dyn VolumeProvider,
    current_seg: usize,
    current_reader: Option<Box<dyn ReadSeek>>,
    remaining_in_segment: u64,
    /// Whether the member continues into more volumes.
    split_after: bool,
    /// Next volume index to discover.
    next_discover_vol: usize,
    /// Archive format (needed to parse continuation headers).
    format: ArchiveFormat,
    /// Password for encrypted header parsing.
    password: Option<String>,
    /// Shared metadata sink updated when continuation headers are discovered.
    /// The caller holds an Rc clone to read final values after streaming.
    metadata_sink: Option<Rc<RefCell<ContinuationMetadata>>>,
    /// Shared volume index tracker. Updated whenever the reader advances to a
    /// new segment, allowing callers (even behind wrapper layers like
    /// DecryptingReader) to observe which volume is currently being read.
    volume_tracker: Option<Arc<AtomicUsize>>,
}

struct ArchiveSegmentReader<'a> {
    volumes: &'a mut [Option<VolumeData>],
    limits: &'a Limits,
    segments: Vec<DataSegment>,
    member_name: String,
    current_seg: usize,
    remaining_in_segment: u64,
    volume_tracker: Option<Arc<AtomicUsize>>,
}

impl<'a> ArchiveSegmentReader<'a> {
    fn new(
        volumes: &'a mut [Option<VolumeData>],
        limits: &'a Limits,
        segments: &[DataSegment],
        member_name: &str,
    ) -> Self {
        let mut sorted = segments.to_vec();
        sorted.sort_by_key(|segment| segment.volume_index);
        Self {
            volumes,
            limits,
            segments: sorted,
            member_name: member_name.to_string(),
            current_seg: 0,
            remaining_in_segment: 0,
            volume_tracker: None,
        }
    }

    fn with_volume_tracker(mut self, tracker: Arc<AtomicUsize>) -> Self {
        self.volume_tracker = Some(tracker);
        self
    }

    fn advance_segment(&mut self) -> std::io::Result<bool> {
        if self.current_seg >= self.segments.len() {
            return Ok(false);
        }

        let seg = &self.segments[self.current_seg];
        if seg.data_size > self.limits.max_data_segment {
            return Err(std::io::Error::other(format!(
                "data segment size {} exceeds limit {}",
                seg.data_size, self.limits.max_data_segment
            )));
        }

        let vol = self
            .volumes
            .get_mut(seg.volume_index)
            .and_then(|v| v.as_mut())
            .ok_or_else(|| {
                std::io::Error::other(format!(
                    "missing volume {} for member {}",
                    seg.volume_index, self.member_name
                ))
            })?;

        vol.reader.seek(SeekFrom::Start(seg.data_offset))?;
        if let Some(ref tracker) = self.volume_tracker {
            tracker.store(seg.volume_index, Ordering::Release);
        }
        self.remaining_in_segment = seg.data_size;
        self.current_seg += 1;
        Ok(true)
    }
}

impl Read for ArchiveSegmentReader<'_> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        loop {
            if self.remaining_in_segment > 0 {
                let seg = &self.segments[self.current_seg - 1];
                let vol = self
                    .volumes
                    .get_mut(seg.volume_index)
                    .and_then(|v| v.as_mut())
                    .ok_or_else(|| {
                        std::io::Error::other(format!(
                            "missing volume {} for member {}",
                            seg.volume_index, self.member_name
                        ))
                    })?;
                let to_read = buf.len().min(self.remaining_in_segment as usize);
                let n = vol.reader.read(&mut buf[..to_read])?;
                if n == 0 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "unexpected EOF in archive volume segment",
                    ));
                }
                self.remaining_in_segment -= n as u64;
                return Ok(n);
            }

            if !self.advance_segment()? {
                return Ok(0);
            }
        }
    }
}

impl<'a> ChainedSegmentReader<'a> {
    pub fn new(segments: &[DataSegment], provider: &'a dyn VolumeProvider) -> Self {
        let next_vol = segments.iter().map(|s| s.volume_index).max().unwrap_or(0) + 1;
        Self {
            segments: segments.to_vec(),
            provider,
            current_seg: 0,
            current_reader: None,
            remaining_in_segment: 0,
            split_after: false,
            next_discover_vol: next_vol,
            format: ArchiveFormat::Rar5,
            password: None,
            metadata_sink: None,
            volume_tracker: None,
        }
    }

    /// Enable on-demand volume discovery for multi-volume members.
    pub fn with_continuation(
        mut self,
        split_after: bool,
        format: ArchiveFormat,
        password: Option<String>,
    ) -> Self {
        self.split_after = split_after;
        self.format = format;
        self.password = password;
        self
    }

    /// Attach a metadata sink that receives CRC/encryption info from continuation headers.
    pub fn with_metadata_sink(mut self, sink: Rc<RefCell<ContinuationMetadata>>) -> Self {
        self.metadata_sink = Some(sink);
        self
    }

    /// Attach a shared volume tracker that is updated with the current
    /// volume index each time the reader advances to a new segment.
    pub fn with_volume_tracker(mut self, tracker: Arc<AtomicUsize>) -> Self {
        self.volume_tracker = Some(tracker);
        self
    }

    fn advance_segment(&mut self) -> std::io::Result<bool> {
        if self.current_seg >= self.segments.len() {
            if !self.split_after {
                return Ok(false);
            }
            // Discover the next volume's continuation segment.
            if !self.discover_next_segment()? {
                return Ok(false);
            }
        }

        let seg = &self.segments[self.current_seg];
        if let Some(ref tracker) = self.volume_tracker {
            tracker.store(seg.volume_index, Ordering::Release);
        }
        let mut reader = self
            .provider
            .get_volume(seg.volume_index)
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        reader
            .seek(SeekFrom::Start(seg.data_offset))
            .map_err(std::io::Error::other)?;
        self.current_reader = Some(reader);
        self.remaining_in_segment = seg.data_size;
        self.current_seg += 1;
        Ok(true)
    }

    /// Fetch the next volume, parse its headers, and extract the continuation segment.
    fn discover_next_segment(&mut self) -> std::io::Result<bool> {
        let vol_idx = self.next_discover_vol;
        let mut reader = self
            .provider
            .get_volume(vol_idx)
            .map_err(|e| std::io::Error::other(format!("volume {vol_idx}: {e}")))?;

        reader
            .seek(SeekFrom::Start(0))
            .map_err(std::io::Error::other)?;
        let format = crate::signature::read_signature(&mut reader)
            .map_err(|e| std::io::Error::other(e.to_string()))?;

        if format == ArchiveFormat::Rar4 {
            // RAR4: parse headers to find the continuation file entry.
            let parsed = crate::rar4::parse_rar4_headers(&mut reader, self.password.as_deref())
                .map_err(|e| std::io::Error::other(e.to_string()))?;
            // Find the first file with split_before (continuation).
            for fh in &parsed.files {
                if fh.split_before {
                    self.segments.push(DataSegment {
                        volume_index: vol_idx,
                        data_offset: fh.data_offset,
                        data_size: fh.packed_size,
                    });
                    self.split_after = fh.split_after;
                    self.next_discover_vol = vol_idx + 1;
                    if let Some(ref sink) = self.metadata_sink {
                        let mut meta = sink.borrow_mut();
                        meta.data_crc32 = Some(fh.crc32);
                        meta.use_hash_mac = false; // RAR4 has no HMAC
                    }
                    return Ok(true);
                }
            }
        } else {
            // RAR5: parse headers.
            let parsed = crate::header::parse_all_headers(&mut reader, self.password.as_deref())
                .map_err(|e| std::io::Error::other(e.to_string()))?;
            // Find the first file header with split_before.
            for pf in &parsed.files {
                if pf.header.split_before {
                    self.segments.push(DataSegment {
                        volume_index: vol_idx,
                        data_offset: pf.header.data_offset,
                        data_size: pf.header.data_size,
                    });
                    self.split_after = pf.header.split_after;
                    self.next_discover_vol = vol_idx + 1;
                    if let Some(ref sink) = self.metadata_sink {
                        let mut meta = sink.borrow_mut();
                        meta.data_crc32 = pf.header.data_crc32;
                        meta.use_hash_mac = pf
                            .file_encryption
                            .as_ref()
                            .is_some_and(|fe| fe.use_hash_mac);
                    }
                    return Ok(true);
                }
            }
        }

        // No continuation found — member is complete.
        self.split_after = false;
        Ok(false)
    }
}

impl Read for ChainedSegmentReader<'_> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        loop {
            if self.remaining_in_segment > 0 {
                let reader = self.current_reader.as_mut().unwrap();
                let to_read = buf.len().min(self.remaining_in_segment as usize);
                let n = reader.read(&mut buf[..to_read])?;
                if n == 0 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "unexpected EOF in volume segment",
                    ));
                }
                self.remaining_in_segment -= n as u64;
                return Ok(n);
            }

            // Advance to next segment (may block on VolumeProvider).
            if !self.advance_segment()? {
                return Ok(0); // All segments consumed — EOF.
            }
        }
    }
}

/// A `Read` wrapper that monitors a shared volume tracker and records
/// compressed byte offsets at each volume transition.
///
/// Used in the LZ extraction path: wraps the reader chain (ChainedSegmentReader
/// + optional DecryptingReader), runs `read_to_end`, then provides the recorded
///   transitions for splitting decompressed output at volume boundaries.
pub struct VolumeTrackingReader<R: Read> {
    inner: R,
    volume_tracker: Arc<AtomicUsize>,
    bytes_read: u64,
    last_volume: usize,
    transitions: Vec<crate::decompress::VolumeTransition>,
    shared_transitions: Option<Arc<std::sync::Mutex<Vec<crate::decompress::VolumeTransition>>>>,
}

impl<R: Read> VolumeTrackingReader<R> {
    pub fn new(inner: R, volume_tracker: Arc<AtomicUsize>) -> Self {
        let initial_vol = volume_tracker.load(Ordering::Acquire);
        Self {
            inner,
            volume_tracker,
            bytes_read: 0,
            last_volume: initial_vol,
            transitions: Vec::new(),
            shared_transitions: None,
        }
    }

    pub fn with_shared_transitions(
        mut self,
        transitions: Arc<std::sync::Mutex<Vec<crate::decompress::VolumeTransition>>>,
    ) -> Self {
        self.shared_transitions = Some(transitions);
        self
    }
}

impl<R: Read> Read for VolumeTrackingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        if n > 0 {
            self.bytes_read += n as u64;
            let current_vol = self.volume_tracker.load(Ordering::Acquire);
            if current_vol != self.last_volume {
                let transition = crate::decompress::VolumeTransition {
                    volume_index: current_vol,
                    compressed_offset: self.bytes_read - n as u64,
                };
                self.transitions.push(transition.clone());
                if let Some(ref shared) = self.shared_transitions
                    && let Ok(mut guard) = shared.lock()
                {
                    guard.push(transition);
                }
                self.last_volume = current_vol;
            }
        }
        Ok(n)
    }
}
