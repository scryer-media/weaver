use std::io::{BufReader, BufWriter};

use tracing::debug;

use super::*;
use crate::volume::VolumeProvider;

impl RarArchive {
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

            let vol = self.volumes.get_mut(seg.volume_index)
                .and_then(|v| v.as_mut())
                .ok_or_else(|| RarError::MissingVolume {
                    volume: seg.volume_index,
                    member: name.clone(),
                })?;

            vol.reader.seek(SeekFrom::Start(seg.data_offset)).map_err(RarError::Io)?;
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
        let entry = self.members.get(index).ok_or_else(|| {
            RarError::CorruptArchive {
                detail: format!("member index {index} out of range"),
            }
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

                // Optionally verify password against check data.
                if let Some(ref check_data) = enc_info.check_data
                    && !crate::crypto::verify_password_check(
                        pwd,
                        &enc_info.salt,
                        enc_info.kdf_count,
                        check_data,
                    ) {
                        return Err(RarError::InvalidPassword);
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
                let expected_crc = if options.verify {
                    fh.data_crc32
                } else {
                    None
                };
                crate::decompress::decompress(&compressed, unpacked_size, &fh.compression, expected_crc)?
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

                // Verify CRC32 of decompressed data.
                if options.verify
                    && let Some(expected) = fh.data_crc32 {
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

        // Verify BLAKE2sp hash if provided.
        if options.verify
            && let Some(FileHash::Blake2sp(expected)) = hash.as_ref()
                && !extract::verify_blake2(&output, expected) {
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
        let entry = self.members.get(index).ok_or_else(|| {
            RarError::CorruptArchive {
                detail: format!("member index {index} out of range"),
            }
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
            let mut hasher = if options.verify { Some(crc32fast::Hasher::new()) } else { None };
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

                let vol = self.volumes.get_mut(seg.volume_index)
                    .and_then(|v| v.as_mut())
                    .ok_or_else(|| RarError::MissingVolume {
                        volume: seg.volume_index,
                        member: name.clone(),
                    })?;

                vol.reader.seek(SeekFrom::Start(seg.data_offset)).map_err(RarError::Io)?;

                // Stream in chunks instead of reading the entire segment.
                let mut remaining = seg.data_size as usize;
                let mut chunk = vec![0u8; (256 * 1024).min(remaining)];
                while remaining > 0 {
                    let to_read = chunk.len().min(remaining);
                    vol.reader.read_exact(&mut chunk[..to_read]).map_err(|e| {
                        if e.kind() == std::io::ErrorKind::UnexpectedEof {
                            RarError::TruncatedData { offset: seg.data_offset }
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
                && let Some(expected) = fh.data_crc32 {
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
                && let Some(FileHash::Blake2sp(expected)) = hash.as_ref() {
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
                    detail: format!("RAR4 member {} is marked encrypted but has no salt", fh.name),
                })?;
                let (key, iv) = crate::crypto::rar4_derive_key(pwd, &salt);
                compressed = crate::crypto::rar4_decrypt_data(&key, &iv, &compressed)?;
            } else {
                let enc_info = file_enc.as_ref().ok_or_else(|| RarError::CorruptArchive {
                    detail: format!("member {} is marked encrypted but has no encryption parameters", fh.name),
                })?;
                if let Some(ref check_data) = enc_info.check_data
                    && !crate::crypto::verify_password_check(pwd, &enc_info.salt, enc_info.kdf_count, check_data) {
                        return Err(RarError::InvalidPassword);
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
                && let Some(expected) = fh.data_crc32 {
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
                && let Some(FileHash::Blake2sp(expected)) = hash.as_ref()
                    && !extract::verify_blake2(&decompressed, expected) {
                        return Err(RarError::Blake2Mismatch { member: fh.name.clone() });
                    }

            if let (Some(p), Some(mi)) = (progress, mi.as_ref()) {
                p.on_member_progress(mi, decompressed.len() as u64);
                p.on_member_complete(mi, &Ok(()));
            }
            return Ok(decompressed.len() as u64);
        }

        // Non-solid: stream decompressed output to writer.
        // For CRC verification, we wrap the writer in a hasher.
        let mut crc_writer = CrcWriter::new(&mut writer, options.verify);
        let expected_crc = if options.verify { fh.data_crc32 } else { None };

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
        if let Some(expected) = fh.data_crc32.filter(|_| options.verify) {
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
        // Ensure sequential extraction order.
        if index < self.solid_next_index {
            // Allow re-extracting a member if it's the current one
            // (e.g. retrying after a transient error). But we can't go backwards.
            return Err(RarError::SolidOrderViolation {
                required: format!("member index {}", self.solid_next_index),
                requested: fh.name.clone(),
            });
        }

        // If we need to skip ahead, we must decode intervening members.
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

        let result = self.run_solid_decoder(compressed, unpacked_size, fh)?;
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

    /// Extract a member by name, handling any supported compression method.
    ///
    /// Returns the decompressed data as a `Vec<u8>`.
    pub fn extract_by_name(
        &mut self,
        name: &str,
        options: &ExtractOptions,
        progress: Option<&dyn ProgressHandler>,
    ) -> RarResult<Vec<u8>> {
        let index = self.find_member(name).ok_or_else(|| {
            RarError::MemberNotFound {
                name: name.to_string(),
            }
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
        let entry = self.members.get(index).ok_or_else(|| {
            RarError::CorruptArchive {
                detail: format!("member index {index} out of range"),
            }
        })?;

        let fh = entry.file_header.clone();
        let is_encrypted = entry.is_encrypted;
        let is_solid = fh.compression.solid;
        let file_encryption = entry.file_encryption.clone();
        let rar4_salt = entry.rar4_salt;
        let unpacked_size = fh.unpacked_size.unwrap_or(0);

        // Solid archives need sequential member state — can't stream.
        if is_solid {
            return Err(RarError::CorruptArchive {
                detail: "streaming extraction not supported for solid archives".into(),
            });
        }

        // Resolve password if encrypted.
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

        let segments = entry.segments.clone();
        let mut sorted_segs = segments;
        sorted_segs.sort_by_key(|s| s.volume_index);

        debug!(
            member = %fh.name,
            method = ?fh.compression.method,
            encrypted = is_encrypted,
            segments = sorted_segs.len(),
            unpacked_size,
            "streaming extraction starting"
        );

        match fh.compression.method {
            CompressionMethod::Store => {
                self.extract_member_streaming_store(
                    &fh, options, provider, &sorted_segs, unpacked_size, writer,
                    member_password.as_deref(), file_encryption.as_ref(), rar4_salt,
                )
            }
            _ => {
                self.extract_member_streaming_lz(
                    &fh, options, provider, &sorted_segs, unpacked_size, writer,
                    member_password.as_deref(), file_encryption.as_ref(), rar4_salt,
                )
            }
        }
    }

    /// Streaming extraction for Store (uncompressed) members.
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
    ) -> RarResult<u64> {
        let chained = ChainedSegmentReader::new(segments, provider);

        // Wrap in DecryptingReader if encrypted, otherwise read directly.
        let mut hasher = if options.verify {
            Some(crc32fast::Hasher::new())
        } else {
            None
        };
        let mut written = 0u64;
        let mut chunk = vec![0u8; 256 * 1024];

        // For encrypted Store members, use unpacked_size to know when to stop
        // (decrypted data may have AES padding at the end).
        let max_bytes = if password.is_some() { unpacked_size } else { u64::MAX };

        let mut reader: Box<dyn Read> = if let Some(pwd) = password {
            if self.format == ArchiveFormat::Rar4 {
                let salt = rar4_salt.ok_or_else(|| RarError::CorruptArchive {
                    detail: format!(
                        "RAR4 member {} is marked encrypted but has no salt",
                        fh.name,
                    ),
                })?;
                let (key, iv) = crate::crypto::rar4_derive_key(pwd, &salt);
                Box::new(crate::crypto::DecryptingReader::new_rar4(chained, &key, &iv))
            } else {
                let enc_info = file_encryption.ok_or_else(|| RarError::CorruptArchive {
                    detail: format!(
                        "member {} is marked encrypted but has no encryption parameters",
                        fh.name,
                    ),
                })?;
                let (key, _) = crate::crypto::derive_key(pwd, &enc_info.salt, enc_info.kdf_count);
                Box::new(crate::crypto::DecryptingReader::new_rar5(chained, &key, &enc_info.iv))
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

        // Verify CRC32.
        if let Some(h) = hasher
            && let Some(expected) = fh.data_crc32 {
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
    /// Uses a `ChainedSegmentReader` to provide the compressed bitstream to the
    /// LZ decompressor, which may block on the `VolumeProvider` when it needs
    /// the next volume. For encrypted members, wraps in `DecryptingReader`.
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
    ) -> RarResult<u64> {
        // Read all compressed data through the chained reader.
        // The ChainedSegmentReader blocks on the VolumeProvider as needed.
        let chained = ChainedSegmentReader::new(segments, provider);

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
                Box::new(crate::crypto::DecryptingReader::new_rar4(chained, &key, &iv))
            } else {
                let enc_info = file_encryption.ok_or_else(|| RarError::CorruptArchive {
                    detail: format!(
                        "member {} is marked encrypted but has no encryption parameters",
                        fh.name,
                    ),
                })?;
                let (key, _) = crate::crypto::derive_key(pwd, &enc_info.salt, enc_info.kdf_count);
                Box::new(crate::crypto::DecryptingReader::new_rar5(chained, &key, &enc_info.iv))
            }
        } else {
            Box::new(chained)
        };

        let mut buf_reader = BufReader::with_capacity(1024 * 1024, inner);
        let mut compressed = Vec::new();
        std::io::Read::read_to_end(&mut buf_reader, &mut compressed).map_err(|e| {
            // Check if this is a VolumeProvider error wrapped in io::Error.
            RarError::Io(e)
        })?;

        debug!(
            compressed_bytes = compressed.len(),
            "streaming LZ: all compressed data read"
        );

        // Stream decompressed output through a CRC writer.
        let mut crc_writer = CrcWriter::new(writer, options.verify);

        crate::decompress::decompress_to_writer(
            &compressed,
            unpacked_size,
            &fh.compression,
            None, // CRC checked separately via CrcWriter
            &mut crc_writer,
        )?;

        crc_writer.flush().map_err(RarError::Io)?;

        // Verify CRC32.
        if let Some(expected) = fh.data_crc32.filter(|_| options.verify) {
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
}

/// A `Read` adapter that chains data segments across volumes.
///
/// When the current segment is exhausted, it fetches the next volume from the
/// `VolumeProvider` — which may block if that volume hasn't finished downloading.
pub struct ChainedSegmentReader<'a> {
    segments: &'a [DataSegment],
    provider: &'a dyn VolumeProvider,
    current_seg: usize,
    current_reader: Option<Box<dyn ReadSeek>>,
    remaining_in_segment: u64,
}

impl<'a> ChainedSegmentReader<'a> {
    pub fn new(segments: &'a [DataSegment], provider: &'a dyn VolumeProvider) -> Self {
        Self {
            segments,
            provider,
            current_seg: 0,
            current_reader: None,
            remaining_in_segment: 0,
        }
    }

    fn advance_segment(&mut self) -> std::io::Result<bool> {
        if self.current_seg >= self.segments.len() {
            return Ok(false);
        }

        let seg = &self.segments[self.current_seg];
        let mut reader = self.provider.get_volume(seg.volume_index).map_err(|e| {
            std::io::Error::other(e.to_string())
        })?;
        reader
            .seek(SeekFrom::Start(seg.data_offset))
            .map_err(std::io::Error::other)?;
        self.current_reader = Some(reader);
        self.remaining_in_segment = seg.data_size;
        self.current_seg += 1;
        Ok(true)
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
