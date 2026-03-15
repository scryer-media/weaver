use std::cell::RefCell;
use std::io::{BufReader, BufWriter};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tracing::debug;

use super::*;
use crate::volume::VolumeProvider;

const STREAMING_STORE_CHUNK_BUFFER_BYTES: usize = 4 * 1024 * 1024;

impl RarArchive {
    fn compressed_capacity_hint(segments: &[DataSegment]) -> usize {
        segments
            .iter()
            .map(|segment| segment.data_size)
            .sum::<u64>()
            .min(usize::MAX as u64) as usize
    }

    fn solid_volume_transitions(
        segments: &[DataSegment],
    ) -> (usize, Vec<crate::decompress::VolumeTransition>) {
        let mut sorted_segments = segments.to_vec();
        sorted_segments.sort_by_key(|segment| segment.volume_index);
        let first_volume = sorted_segments
            .first()
            .map_or(0, |segment| segment.volume_index);
        let mut compressed_offset = 0u64;
        let mut transitions = Vec::new();
        for &[ref prev, ref next] in sorted_segments.array_windows() {
            compressed_offset = compressed_offset.saturating_add(prev.data_size);
            transitions.push(crate::decompress::VolumeTransition {
                volume_index: next.volume_index,
                compressed_offset,
            });
        }
        (first_volume, transitions)
    }

    fn advance_solid_cursor_to(&mut self, index: usize, fh: &FileHeader) -> RarResult<()> {
        if index < self.solid_next_index {
            return Err(RarError::SolidOrderViolation {
                required: format!("member index {}", self.solid_next_index),
                requested: fh.name.clone(),
            });
        }

        while self.solid_next_index < index {
            let skip_idx = self.solid_next_index;
            let skip_entry = &self.members[skip_idx];
            let skip_fh = skip_entry.file_header.clone();
            if skip_fh.compression.method != CompressionMethod::Store {
                let skip_data = self.read_member_data(skip_idx)?;
                let skip_unpacked = skip_fh.unpacked_size.unwrap_or(0);
                self.run_solid_decoder(&skip_data, skip_unpacked, &skip_fh)?;
            }
            self.solid_next_index += 1;
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn extract_solid_store_chunked<F>(
        &self,
        fh: &FileHeader,
        options: &ExtractOptions,
        segments: &[DataSegment],
        data: &[u8],
        mut writer_factory: F,
        skip_hash_verify: bool,
    ) -> RarResult<Vec<(usize, u64)>>
    where
        F: FnMut(usize) -> RarResult<Box<dyn Write>>,
    {
        let mut sorted_segments = segments.to_vec();
        sorted_segments.sort_by_key(|segment| segment.volume_index);

        let mut hasher = if options.verify && !skip_hash_verify {
            Some(crc32fast::Hasher::new())
        } else {
            None
        };
        let mut chunks = Vec::with_capacity(sorted_segments.len());
        let mut offset = 0usize;

        for segment in &sorted_segments {
            let remaining = data.len().saturating_sub(offset);
            let write_len = remaining.min(segment.data_size as usize);
            let mut writer = writer_factory(segment.volume_index)?;
            if write_len > 0 {
                writer
                    .write_all(&data[offset..offset + write_len])
                    .map_err(RarError::Io)?;
                if let Some(ref mut crc) = hasher {
                    crc.update(&data[offset..offset + write_len]);
                }
            }
            writer.flush().map_err(RarError::Io)?;
            chunks.push((segment.volume_index, write_len as u64));
            offset += write_len;
        }

        if offset != data.len() {
            return Err(RarError::CorruptArchive {
                detail: format!(
                    "solid store member {} left {} bytes unmapped to volume chunks",
                    fh.name,
                    data.len().saturating_sub(offset)
                ),
            });
        }

        if let Some(crc) = hasher
            && let Some(expected) = fh.data_crc32
        {
            let actual = crc.finalize();
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

    /// Read all data segments for a member, concatenating them into a single buffer.
    ///
    /// This handles cross-volume data by reading each segment from its
    /// respective volume reader.
    pub(super) fn read_member_data(&mut self, entry_index: usize) -> RarResult<Vec<u8>> {
        let entry = &self.members[entry_index];
        let mut segments = entry.segments.clone();
        let name = entry.file_header.name.clone();

        // Sort segments by volume index to handle out-of-order volume addition.
        segments.sort_by_key(|s| s.volume_index);

        let mut data = Vec::new();

        for seg in &segments {
            // Guard against unreasonably large data segments.
            if seg.data_size > self.limits.max_data_segment {
                return Err(RarError::ResourceLimit {
                    detail: format!(
                        "data segment size {} exceeds limit {}",
                        seg.data_size, self.limits.max_data_segment
                    ),
                });
            }

            let vol = self
                .volumes
                .get_mut(seg.volume_index)
                .and_then(|v| v.as_mut())
                .ok_or_else(|| RarError::MissingVolume {
                    volume: seg.volume_index,
                    member: name.clone(),
                })?;

            vol.reader
                .seek(SeekFrom::Start(seg.data_offset))
                .map_err(RarError::Io)?;
            let mut buf = vec![0u8; seg.data_size as usize];
            if seg.data_size > 0 {
                vol.reader.read_exact(&mut buf).map_err(|e| {
                    if e.kind() == std::io::ErrorKind::UnexpectedEof {
                        RarError::TruncatedData {
                            offset: seg.data_offset,
                        }
                    } else {
                        RarError::Io(e)
                    }
                })?;
            }
            data.extend_from_slice(&buf);
        }

        Ok(data)
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
    ) -> RarResult<Vec<u8>> {
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
        // When HASHMAC is set, CRC/BLAKE2 values are HMAC-transformed and
        // cannot be verified without HashKey from unrar's custom PBKDF2 chain.
        let skip_hash_verify = file_enc.as_ref().is_some_and(|fe| fe.use_hash_mac);

        // Report progress start.
        if let (Some(p), Some(mi)) = (progress, mi.as_ref()) {
            p.on_member_start(mi);
        }

        // Read all data segments for this member.
        let mut compressed = self.read_member_data(index)?;

        // Decrypt the data if the member is encrypted.
        if let Some(ref pwd) = member_password {
            if archive_format == ArchiveFormat::Rar4 {
                // RAR4: AES-128-CBC with SHA-1-based key derivation.
                let salt = rar4_salt.ok_or_else(|| RarError::CorruptArchive {
                    detail: format!(
                        "RAR4 member {} is marked encrypted but has no salt",
                        fh.name,
                    ),
                })?;
                let (key, iv) = crate::crypto::rar4_derive_key(pwd, &salt);
                compressed = crate::crypto::rar4_decrypt_data(&key, &iv, &compressed)?;
            } else {
                // RAR5: AES-256-CBC with PBKDF2-HMAC-SHA256.
                let enc_info = file_enc.as_ref().ok_or_else(|| RarError::CorruptArchive {
                    detail: format!(
                        "member {} is marked encrypted but has no encryption parameters",
                        fh.name,
                    ),
                })?;

                // Pre-check password using check_data if available.
                // This catches wrong passwords before expensive decryption,
                // and is the only detection mechanism for encrypted Store+HASHMAC
                // (where CRC verification is skipped).
                if let Some(ref check_data) = enc_info.check_data
                    && !crate::crypto::verify_password_check(
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

                let (key, _) = crate::crypto::derive_key(pwd, &enc_info.salt, enc_info.kdf_count);
                compressed = crate::crypto::decrypt_data(&key, &enc_info.iv, &compressed)?;
            }
        }

        let unpacked_size = fh.unpacked_size.unwrap_or(0);

        // For encrypted store files, decrypted data may be padded to AES block
        // boundary. Truncate to the actual unpacked size before decompression.
        if member_password.is_some()
            && fh.compression.method == CompressionMethod::Store
            && compressed.len() as u64 > unpacked_size
        {
            compressed.truncate(unpacked_size as usize);
        }

        // Dispatch to decompressor.
        let output = match fh.compression.method {
            CompressionMethod::Store => {
                let expected_crc = if options.verify && !skip_hash_verify {
                    fh.data_crc32
                } else {
                    None
                };
                crate::decompress::decompress(
                    &compressed,
                    unpacked_size,
                    &fh.compression,
                    expected_crc,
                )?
            }
            _ => {
                // For solid archives, we need to maintain decoder state across members.
                let decompressed = if is_solid {
                    self.decompress_solid(index, &compressed, unpacked_size, &fh)?
                } else {
                    crate::decompress::decompress(
                        &compressed,
                        unpacked_size,
                        &fh.compression,
                        None,
                    )?
                };

                // Verify CRC32 of decompressed data (skip if HASHMAC — CRC is HMAC-transformed).
                if options.verify
                    && !skip_hash_verify
                    && let Some(expected) = fh.data_crc32
                {
                    let mut hasher = crc32fast::Hasher::new();
                    hasher.update(&decompressed);
                    let actual = hasher.finalize();
                    if actual != expected {
                        return Err(RarError::DataCrcMismatch {
                            member: fh.name.clone(),
                            expected,
                            actual,
                        });
                    }
                }

                decompressed
            }
        };

        // Report progress.
        if let (Some(p), Some(mi)) = (progress, mi.as_ref()) {
            p.on_member_progress(mi, output.len() as u64);
        }

        // Verify BLAKE2sp hash if provided (skip if HASHMAC — hash is HMAC-transformed).
        if options.verify
            && !skip_hash_verify
            && let Some(FileHash::Blake2sp(expected)) = hash.as_ref()
            && !extract::verify_blake2(&output, expected)
        {
            return Err(RarError::Blake2Mismatch {
                member: fh.name.clone(),
            });
        }

        // Report completion.
        if let (Some(p), Some(mi)) = (progress, mi.as_ref()) {
            p.on_member_complete(mi, &Ok(()));
        }

        Ok(output)
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

        // Fast path: Store method, no encryption, not solid — stream segments
        // directly from volumes to output without buffering the full file.
        if fh.compression.method == CompressionMethod::Store
            && member_password.is_none()
            && !is_solid
        {
            let mut hasher = if options.verify {
                Some(crc32fast::Hasher::new())
            } else {
                None
            };
            let mut blake_hasher: Option<blake2::Blake2s256> = if options.verify {
                use blake2::Digest;
                hash.as_ref().map(|h| match h {
                    FileHash::Blake2sp(_) => blake2::Blake2s256::new(),
                })
            } else {
                None
            };
            let mut written = 0u64;

            let segments = self.members[index].segments.clone();
            let name = fh.name.clone();
            let mut sorted_segs = segments;
            sorted_segs.sort_by_key(|s| s.volume_index);

            for seg in &sorted_segs {
                if seg.data_size > self.limits.max_data_segment {
                    return Err(RarError::ResourceLimit {
                        detail: format!(
                            "data segment size {} exceeds limit {}",
                            seg.data_size, self.limits.max_data_segment
                        ),
                    });
                }

                let vol = self
                    .volumes
                    .get_mut(seg.volume_index)
                    .and_then(|v| v.as_mut())
                    .ok_or_else(|| RarError::MissingVolume {
                        volume: seg.volume_index,
                        member: name.clone(),
                    })?;

                vol.reader
                    .seek(SeekFrom::Start(seg.data_offset))
                    .map_err(RarError::Io)?;

                // Stream in chunks instead of reading the entire segment.
                let mut remaining = seg.data_size as usize;
                let mut chunk = vec![0u8; (256 * 1024).min(remaining)];
                while remaining > 0 {
                    let to_read = chunk.len().min(remaining);
                    vol.reader.read_exact(&mut chunk[..to_read]).map_err(|e| {
                        if e.kind() == std::io::ErrorKind::UnexpectedEof {
                            RarError::TruncatedData {
                                offset: seg.data_offset,
                            }
                        } else {
                            RarError::Io(e)
                        }
                    })?;
                    writer.write_all(&chunk[..to_read]).map_err(RarError::Io)?;
                    if let Some(ref mut h) = hasher {
                        h.update(&chunk[..to_read]);
                    }
                    if let Some(ref mut h) = blake_hasher {
                        use blake2::Digest;
                        h.update(&chunk[..to_read]);
                    }
                    written += to_read as u64;
                    remaining -= to_read;
                }
            }

            writer.flush().map_err(RarError::Io)?;

            // Verify CRC32.
            if let Some(h) = hasher
                && let Some(expected) = fh.data_crc32
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

            // Verify BLAKE2sp.
            if let Some(h) = blake_hasher
                && let Some(FileHash::Blake2sp(expected)) = hash.as_ref()
            {
                use blake2::Digest;
                let actual: [u8; 32] = h.finalize().into();
                if actual != *expected {
                    return Err(RarError::Blake2Mismatch {
                        member: fh.name.clone(),
                    });
                }
            }

            if let (Some(p), Some(mi)) = (progress, mi.as_ref()) {
                p.on_member_progress(mi, written);
                p.on_member_complete(mi, &Ok(()));
            }

            return Ok(written);
        }

        // Slow path: encrypted or compressed — need compressed data in memory,
        // but stream decompressed output to disk.
        let mut compressed = self.read_member_data(index)?;

        // Decrypt if needed.
        if let Some(ref pwd) = member_password {
            if archive_format == ArchiveFormat::Rar4 {
                let salt = rar4_salt.ok_or_else(|| RarError::CorruptArchive {
                    detail: format!(
                        "RAR4 member {} is marked encrypted but has no salt",
                        fh.name
                    ),
                })?;
                let (key, iv) = crate::crypto::rar4_derive_key(pwd, &salt);
                compressed = crate::crypto::rar4_decrypt_data(&key, &iv, &compressed)?;
            } else {
                let enc_info = file_enc.as_ref().ok_or_else(|| RarError::CorruptArchive {
                    detail: format!(
                        "member {} is marked encrypted but has no encryption parameters",
                        fh.name
                    ),
                })?;
                if let Some(ref check_data) = enc_info.check_data
                    && !crate::crypto::verify_password_check(
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
                let (key, _) = crate::crypto::derive_key(pwd, &enc_info.salt, enc_info.kdf_count);
                compressed = crate::crypto::decrypt_data(&key, &enc_info.iv, &compressed)?;
            }
        }

        // Truncate padded store files.
        if member_password.is_some()
            && fh.compression.method == CompressionMethod::Store
            && compressed.len() as u64 > unpacked_size
        {
            compressed.truncate(unpacked_size as usize);
        }

        // Use streaming decompression for non-solid, or fall back to buffered for solid.
        if is_solid {
            // Solid archives need the full Vec path since the decoder state persists.
            let decompressed = self.decompress_solid(index, &compressed, unpacked_size, &fh)?;
            if options.verify
                && !skip_hash_verify
                && let Some(expected) = fh.data_crc32
            {
                let mut hasher = crc32fast::Hasher::new();
                hasher.update(&decompressed);
                let actual = hasher.finalize();
                if actual != expected {
                    return Err(RarError::DataCrcMismatch {
                        member: fh.name.clone(),
                        expected,
                        actual,
                    });
                }
            }
            writer.write_all(&decompressed).map_err(RarError::Io)?;
            writer.flush().map_err(RarError::Io)?;

            if options.verify
                && !skip_hash_verify
                && let Some(FileHash::Blake2sp(expected)) = hash.as_ref()
                && !extract::verify_blake2(&decompressed, expected)
            {
                return Err(RarError::Blake2Mismatch {
                    member: fh.name.clone(),
                });
            }

            if let (Some(p), Some(mi)) = (progress, mi.as_ref()) {
                p.on_member_progress(mi, decompressed.len() as u64);
                p.on_member_complete(mi, &Ok(()));
            }
            return Ok(decompressed.len() as u64);
        }

        // Non-solid: stream decompressed output to writer.
        // For CRC verification, we wrap the writer in a hasher.
        let do_crc = options.verify && !skip_hash_verify;
        let mut crc_writer = CrcWriter::new(&mut writer, do_crc);
        let expected_crc = if do_crc { fh.data_crc32 } else { None };

        crate::decompress::decompress_to_writer(
            &compressed,
            unpacked_size,
            &fh.compression,
            expected_crc,
            &mut crc_writer,
        )?;

        // Drop compressed data now — no longer needed.
        drop(compressed);

        crc_writer.flush().map_err(RarError::Io)?;

        // Verify CRC from streaming hasher.
        if let Some(expected) = fh.data_crc32.filter(|_| do_crc) {
            let actual = crc_writer.finalize_crc();
            if actual != expected {
                return Err(RarError::DataCrcMismatch {
                    member: fh.name.clone(),
                    expected,
                    actual,
                });
            }
        }

        // BLAKE2sp verification requires re-reading (can't stream with current API).
        // For non-solid LZ files this is rare — skip for now.

        if let (Some(p), Some(mi)) = (progress, mi.as_ref()) {
            p.on_member_progress(mi, unpacked_size);
            p.on_member_complete(mi, &Ok(()));
        }

        Ok(unpacked_size)
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
        let mut compressed = self.read_member_data(index)?;

        if let Some(ref pwd) = member_password {
            if archive_format == ArchiveFormat::Rar4 {
                let salt = rar4_salt.ok_or_else(|| RarError::CorruptArchive {
                    detail: format!(
                        "RAR4 member {} is marked encrypted but has no salt",
                        fh.name
                    ),
                })?;
                let (key, iv) = crate::crypto::rar4_derive_key(pwd, &salt);
                compressed = crate::crypto::rar4_decrypt_data(&key, &iv, &compressed)?;
            } else {
                let enc_info = file_enc.as_ref().ok_or_else(|| RarError::CorruptArchive {
                    detail: format!(
                        "member {} is marked encrypted but has no encryption parameters",
                        fh.name
                    ),
                })?;
                if let Some(ref check_data) = enc_info.check_data
                    && !crate::crypto::verify_password_check(
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
                let (key, _) = crate::crypto::derive_key(pwd, &enc_info.salt, enc_info.kdf_count);
                compressed = crate::crypto::decrypt_data(&key, &enc_info.iv, &compressed)?;
            }
        }

        if member_password.is_some()
            && fh.compression.method == CompressionMethod::Store
            && compressed.len() as u64 > unpacked_size
        {
            compressed.truncate(unpacked_size as usize);
        }

        self.decompress_solid_chunked(
            index,
            &compressed,
            unpacked_size,
            &fh,
            &segments,
            options,
            writer_factory,
            skip_hash_verify,
        )
    }

    /// Decompress a member in a solid archive, maintaining LZ decoder state
    /// across sequential members.
    ///
    /// In a solid archive, the LZ dictionary carries over between files.
    /// Members MUST be extracted in order — you cannot skip to file N without
    /// first decoding files 0..N-1.
    pub(super) fn decompress_solid(
        &mut self,
        index: usize,
        compressed: &[u8],
        unpacked_size: u64,
        fh: &FileHeader,
    ) -> RarResult<Vec<u8>> {
        self.advance_solid_cursor_to(index, fh)?;

        let result = self.run_solid_decoder(compressed, unpacked_size, fh)?;
        self.solid_next_index = index + 1;
        Ok(result)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn decompress_solid_chunked<F>(
        &mut self,
        index: usize,
        compressed: &[u8],
        unpacked_size: u64,
        fh: &FileHeader,
        segments: &[DataSegment],
        options: &ExtractOptions,
        writer_factory: F,
        skip_hash_verify: bool,
    ) -> RarResult<Vec<(usize, u64)>>
    where
        F: FnMut(usize) -> RarResult<Box<dyn Write>>,
    {
        self.advance_solid_cursor_to(index, fh)?;

        let result = if fh.compression.method == CompressionMethod::Store {
            self.extract_solid_store_chunked(
                fh,
                options,
                segments,
                compressed,
                writer_factory,
                skip_hash_verify,
            )?
        } else {
            let (first_volume, boundaries) = Self::solid_volume_transitions(segments);
            self.run_solid_decoder_chunked(
                compressed,
                fh,
                SolidChunkDecodeConfig {
                    unpacked_size,
                    first_volume_index: first_volume,
                    boundaries: &boundaries,
                    verify_crc: options.verify && !skip_hash_verify,
                },
                writer_factory,
            )?
        };

        self.solid_next_index = index + 1;
        Ok(result)
    }

    /// Run the solid LZ decoder (creating it on first use).
    ///
    /// On subsequent calls, prepares for solid continuation (keeps the
    /// sliding window / dictionary state but resets the bitstream block flag).
    /// Dispatches to the correct decoder based on archive format.
    pub(super) fn run_solid_decoder(
        &mut self,
        compressed: &[u8],
        unpacked_size: u64,
        fh: &FileHeader,
    ) -> RarResult<Vec<u8>> {
        let dict_size = fh.compression.dict_size;

        // Enforce dictionary size limit (same as non-solid path).
        if dict_size > self.limits.max_dict_size {
            return Err(RarError::DictionaryTooLarge {
                size: dict_size,
                max: self.limits.max_dict_size,
            });
        }

        let dict_size = dict_size as usize;

        if fh.compression.format == ArchiveFormat::Rar4 {
            if let Some(decoder) = &mut self.solid_decoder_rar4 {
                decoder.prepare_solid_continuation();
            } else {
                self.solid_decoder_rar4 = Some(Rar4LzDecoder::new(dict_size));
            }
            self.solid_decoder_rar4
                .as_mut()
                .unwrap()
                .decompress(compressed, unpacked_size)
        } else {
            if let Some(decoder) = &mut self.solid_decoder {
                decoder.prepare_solid_continuation();
            } else {
                self.solid_decoder = Some(LzDecoder::new(dict_size));
            }
            self.solid_decoder
                .as_mut()
                .unwrap()
                .decompress(compressed, unpacked_size)
        }
    }

    pub(super) fn run_solid_decoder_chunked<F>(
        &mut self,
        compressed: &[u8],
        fh: &FileHeader,
        config: SolidChunkDecodeConfig<'_>,
        writer_factory: F,
    ) -> RarResult<Vec<(usize, u64)>>
    where
        F: FnMut(usize) -> RarResult<Box<dyn Write>>,
    {
        let SolidChunkDecodeConfig {
            unpacked_size,
            first_volume_index,
            boundaries,
            verify_crc,
        } = config;
        let dict_size = fh.compression.dict_size;
        if dict_size > self.limits.max_dict_size {
            return Err(RarError::DictionaryTooLarge {
                size: dict_size,
                max: self.limits.max_dict_size,
            });
        }

        let shared_hasher = if verify_crc {
            Some(Arc::new(std::sync::Mutex::new(crc32fast::Hasher::new())))
        } else {
            None
        };

        let mut writer_factory = writer_factory;
        let chunks = if fh.compression.format == ArchiveFormat::Rar4 {
            if let Some(decoder) = &mut self.solid_decoder_rar4 {
                decoder.prepare_solid_continuation();
            } else {
                self.solid_decoder_rar4 = Some(Rar4LzDecoder::new(dict_size as usize));
            }
            let decoder = self.solid_decoder_rar4.as_mut().unwrap();
            let hasher_clone = shared_hasher.clone();
            decoder.decompress_to_writer_chunked(
                compressed,
                unpacked_size,
                first_volume_index,
                boundaries,
                |volume_index| {
                    let writer = writer_factory(volume_index)?;
                    if let Some(ref hasher) = hasher_clone {
                        Ok(Box::new(CrcTrackingWriter {
                            inner: writer,
                            hasher: Arc::clone(hasher),
                        }))
                    } else {
                        Ok(writer)
                    }
                },
            )?
        } else {
            if let Some(decoder) = &mut self.solid_decoder {
                decoder.prepare_solid_continuation();
            } else {
                self.solid_decoder = Some(LzDecoder::new(dict_size as usize));
            }
            let decoder = self.solid_decoder.as_mut().unwrap();
            let hasher_clone = shared_hasher.clone();
            decoder.decompress_to_writer_chunked(
                compressed,
                unpacked_size,
                first_volume_index,
                boundaries,
                |volume_index| {
                    let writer = writer_factory(volume_index)?;
                    if let Some(ref hasher) = hasher_clone {
                        Ok(Box::new(CrcTrackingWriter {
                            inner: writer,
                            hasher: Arc::clone(hasher),
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
    ) -> RarResult<Vec<u8>> {
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
        let file_encryption = entry.file_encryption.clone();
        let rar4_salt = entry.rar4_salt;
        let unpacked_size = fh.unpacked_size.unwrap_or(0);

        if is_solid {
            return Err(RarError::CorruptArchive {
                detail: "streaming extraction not supported for solid archives".into(),
            });
        }

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

        // For LZ, we need all compressed data before decompression.
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
    fn extract_member_streaming_store<W: Write>(
        &self,
        fh: &FileHeader,
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
                let (key, iv) = crate::crypto::rar4_derive_key(pwd, &salt);
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
                    && !crate::crypto::verify_password_check(
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
                let (key, _) = crate::crypto::derive_key(pwd, &enc_info.salt, enc_info.kdf_count);
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
        // Read all compressed data through the chained reader.
        // The ChainedSegmentReader blocks on the VolumeProvider as needed.
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
                let (key, iv) = crate::crypto::rar4_derive_key(pwd, &salt);
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
                    && !crate::crypto::verify_password_check(
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
                let (key, _) = crate::crypto::derive_key(pwd, &enc_info.salt, enc_info.kdf_count);
                Box::new(crate::crypto::DecryptingReader::new_rar5(
                    chained,
                    &key,
                    &enc_info.iv,
                ))
            }
        } else {
            Box::new(chained)
        };

        let mut buf_reader = BufReader::with_capacity(1024 * 1024, inner);
        let mut compressed = Vec::with_capacity(Self::compressed_capacity_hint(segments));
        std::io::Read::read_to_end(&mut buf_reader, &mut compressed).map_err(|e| {
            // Check if this is a VolumeProvider error wrapped in io::Error.
            RarError::Io(e)
        })?;

        debug!(
            compressed_bytes = compressed.len(),
            "streaming LZ: all compressed data read"
        );

        // Stream decompressed output through a CRC writer.
        let do_crc = options.verify && !skip_hash_verify;
        let mut crc_writer = CrcWriter::new(writer, do_crc);

        crate::decompress::decompress_to_writer(
            &compressed,
            unpacked_size,
            &fh.compression,
            None, // CRC checked separately via CrcWriter
            &mut crc_writer,
        )?;

        crc_writer.flush().map_err(RarError::Io)?;

        // Use final volume's CRC and HMAC flag if continuations were discovered.
        let final_meta = cont_meta.borrow();
        let effective_crc = final_meta.data_crc32.or(fh.data_crc32);
        let final_skip_hash = final_meta.use_hash_mac;
        drop(final_meta);

        // Verify CRC32 (skip if final volume uses HMAC-transformed hashes).
        if let Some(expected) = effective_crc.filter(|_| do_crc && !final_skip_hash) {
            let actual = crc_writer.finalize_crc();
            if actual != expected {
                return Err(RarError::DataCrcMismatch {
                    member: fh.name.clone(),
                    expected,
                    actual,
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

        if is_solid {
            return Err(RarError::CorruptArchive {
                detail: "streaming extraction not supported for solid archives".into(),
            });
        }

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
                let (key, iv) = crate::crypto::rar4_derive_key(pwd, &salt);
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
                    && !crate::crypto::verify_password_check(
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
                let (key, _) = crate::crypto::derive_key(pwd, &enc_info.salt, enc_info.kdf_count);
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
                let (key, iv) = crate::crypto::rar4_derive_key(pwd, &salt);
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
                    && !crate::crypto::verify_password_check(
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
                let (key, _) = crate::crypto::derive_key(pwd, &enc_info.salt, enc_info.kdf_count);
                Box::new(crate::crypto::DecryptingReader::new_rar5(
                    chained,
                    &key,
                    &enc_info.iv,
                ))
            }
        } else {
            Box::new(chained)
        };

        // Wrap in VolumeTrackingReader to capture transitions during read_to_end.
        let mut tracking_reader =
            VolumeTrackingReader::new(BufReader::with_capacity(1024 * 1024, inner), volume_tracker);
        let mut compressed = Vec::with_capacity(Self::compressed_capacity_hint(segments));
        std::io::Read::read_to_end(&mut tracking_reader, &mut compressed).map_err(RarError::Io)?;

        let transitions = tracking_reader.into_transitions();

        debug!(
            compressed_bytes = compressed.len(),
            transitions = transitions.len(),
            "streaming chunked LZ: compressed data read"
        );

        // Decompress with chunked output splitting.
        let do_crc = options.verify && !skip_hash_verify;
        let shared_hasher: Option<Arc<std::sync::Mutex<crc32fast::Hasher>>> = if do_crc {
            Some(Arc::new(std::sync::Mutex::new(crc32fast::Hasher::new())))
        } else {
            None
        };

        let chunks = {
            let hasher_clone = shared_hasher.clone();
            crate::decompress::decompress_to_writer_chunked(
                &compressed,
                unpacked_size,
                &fh.compression,
                first_vol,
                &transitions,
                |vol_idx| {
                    let writer = writer_factory(vol_idx)?;
                    if let Some(ref h) = hasher_clone {
                        Ok(Box::new(CrcTrackingWriter {
                            inner: writer,
                            hasher: Arc::clone(h),
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
struct CrcTrackingWriter<W: Write> {
    inner: W,
    hasher: Arc<std::sync::Mutex<crc32fast::Hasher>>,
}

impl<W: Write> Write for CrcTrackingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.hasher.lock().unwrap().update(&buf[..n]);
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
            let parsed = crate::rar4::parse_rar4_headers(&mut reader)
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
        }
    }

    /// Consume the wrapper and return the recorded volume transitions.
    pub fn into_transitions(self) -> Vec<crate::decompress::VolumeTransition> {
        self.transitions
    }
}

impl<R: Read> Read for VolumeTrackingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        if n > 0 {
            self.bytes_read += n as u64;
            let current_vol = self.volume_tracker.load(Ordering::Acquire);
            if current_vol != self.last_volume {
                self.transitions.push(crate::decompress::VolumeTransition {
                    volume_index: current_vol,
                    compressed_offset: self.bytes_read - n as u64,
                });
                self.last_volume = current_vol;
            }
        }
        Ok(n)
    }
}

pub(super) struct SolidChunkDecodeConfig<'a> {
    pub unpacked_size: u64,
    pub first_volume_index: usize,
    pub boundaries: &'a [crate::decompress::VolumeTransition],
    pub verify_crc: bool,
}
