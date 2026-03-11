use super::*;

impl RarArchive {
    /// Open a multi-volume archive from multiple readers.
    ///
    /// The readers should be provided in volume order (index 0 is the first volume).
    /// All volumes are parsed and the unified member list is built.
    pub fn open_volumes(readers: Vec<Box<dyn ReadSeek>>) -> RarResult<Self> {
        if readers.is_empty() {
            return Err(RarError::CorruptArchive {
                detail: "no volumes provided".into(),
            });
        }

        let mut readers = readers.into_iter();
        let first = readers.next().unwrap();
        let mut archive = Self::open_boxed_inner(first, None)?;

        for (i, reader) in readers.enumerate() {
            archive.add_volume(i + 1, reader)?;
        }

        Ok(archive)
    }

    /// Add a volume to the archive incrementally.
    ///
    /// `index` is the volume number (0-based). The reader should be positioned
    /// at the start of the volume.
    ///
    /// This parses the volume's headers and integrates its file entries into
    /// the unified member list. Supports both RAR5 and RAR4 volumes.
    pub fn add_volume(&mut self, index: usize, reader: Box<dyn ReadSeek>) -> RarResult<()> {
        let mut reader = reader;

        // Seek to start and read signature.
        reader.seek(SeekFrom::Start(0)).map_err(RarError::Io)?;
        let format = signature::read_signature(&mut reader)?;

        if format == ArchiveFormat::Rar4 {
            return self.add_volume_rar4(index, reader);
        }

        // Parse headers (RAR5).
        let parsed = header::parse_all_headers(&mut reader, self.password.as_deref())?;

        let vol_num = parsed
            .main
            .as_ref()
            .and_then(|m| m.volume_number)
            .unwrap_or(index as u64) as usize;

        // Register this volume.
        self.volume_set.add_volume(vol_num);

        let more_volumes = parsed.end.as_ref().is_some_and(|e| e.more_volumes);

        if !more_volumes {
            self.volume_set.set_last_volume_seen();
            self.more_volumes = false;
        }

        if parsed.end.is_some() {
            self.end_reached = true;
        }

        // Process file headers from this volume.
        for pf in parsed.files {
            let segment = DataSegment {
                volume_index: vol_num,
                data_offset: pf.header.data_offset,
                data_size: pf.header.data_size,
            };

            let mut file_header = pf.header;
            file_header.is_encrypted = pf.is_encrypted;
            let entry = MemberEntry {
                file_header,
                is_encrypted: pf.is_encrypted,
                file_encryption: pf.file_encryption.map(|fe| FileEncryptionInfo {
                    kdf_count: fe.kdf_count,
                    salt: fe.salt,
                    iv: fe.iv,
                    check_data: fe.check_data,
                    use_hash_mac: fe.use_hash_mac,
                }),
                rar4_salt: None,
                hash: pf.hash,
                redirection: pf.redirection,
                segments: vec![segment],
            };

            self.integrate_member(vol_num, entry);
        }

        // Store the reader.
        self.store_volume_reader(vol_num, reader);

        Ok(())
    }

    /// Add a RAR4 volume incrementally.
    pub(super) fn add_volume_rar4(
        &mut self,
        index: usize,
        mut reader: Box<dyn ReadSeek>,
    ) -> RarResult<()> {
        let parsed = crate::rar4::parse_rar4_headers(&mut reader)?;

        // Use volume number from ENDARC header if available, otherwise fall back to index.
        let vol_num = parsed
            .end
            .as_ref()
            .and_then(|e| e.volume_number)
            .map(|v| v as usize)
            .unwrap_or(index);
        self.volume_set.add_volume(vol_num);

        let more_volumes = parsed.end.as_ref().is_some_and(|e| e.more_volumes);
        if !more_volumes {
            self.volume_set.set_last_volume_seen();
            self.more_volumes = false;
        }
        if parsed.end.is_some() {
            self.end_reached = true;
        }

        for fh in &parsed.files {
            let segment = DataSegment {
                volume_index: vol_num,
                data_offset: fh.data_offset,
                data_size: fh.packed_size,
            };

            let entry = MemberEntry {
                file_header: Self::rar4_to_file_header(fh),
                is_encrypted: fh.is_encrypted,
                file_encryption: None,
                rar4_salt: fh.salt,
                hash: None,
                redirection: None,
                segments: vec![segment],
            };

            self.integrate_member(vol_num, entry);
        }

        self.store_volume_reader(vol_num, reader);
        Ok(())
    }

    /// Integrate a member entry from a new volume into the unified member list.
    ///
    /// Handles split_before/split_after reconciliation for files spanning volumes.
    /// Continuation headers (split_before) in real RAR archives often have empty
    /// names, so we match by split state and volume adjacency, not by name.
    pub(super) fn integrate_member(&mut self, vol_num: usize, entry: MemberEntry) {
        let segment = entry.segments[0].clone();

        if entry.file_header.split_before {
            // Continuation segment — find the existing member that expects a
            // continuation. Match criteria:
            // 1. The member still has split_after=true (waiting for more data)
            // 2. Its last segment is from the preceding volume (or same name)
            let existing = self.members.iter_mut().find(|m| {
                m.file_header.split_after && {
                    // Prefer volume-adjacency match.
                    let last_vol = m.segments.last().map(|s| s.volume_index);
                    last_vol == Some(vol_num.wrapping_sub(1))
                        // Fall back to name match for out-of-order volumes.
                        || (!entry.file_header.name.is_empty()
                            && m.file_header.name == entry.file_header.name)
                }
            });

            if let Some(existing) = existing {
                Self::merge_continuation(existing, &entry, segment);
            } else {
                // No prior entry — volumes arrived out of order.
                self.members.push(entry);
            }
        } else {
            // New member starting in this volume.
            // Check if we already have a continuation-only entry from a later
            // volume (out-of-order arrival).
            let existing = self.members.iter_mut().find(|m| {
                !entry.file_header.name.is_empty()
                    && m.file_header.name == entry.file_header.name
                    && m.segments[0].volume_index > vol_num
            });

            if let Some(existing) = existing {
                // We already have a later continuation — insert this segment at front.
                existing.segments.insert(0, segment);
                existing.file_header = entry.file_header;
                existing.is_encrypted = entry.is_encrypted;
                existing.file_encryption = entry.file_encryption;
                existing.rar4_salt = entry.rar4_salt;
                if entry.hash.is_some() {
                    existing.hash = entry.hash;
                }
                if entry.redirection.is_some() {
                    existing.redirection = entry.redirection;
                }
            } else {
                self.members.push(entry);
            }
        }
    }

    /// Merge a continuation entry's metadata into an existing member.
    fn merge_continuation(existing: &mut MemberEntry, entry: &MemberEntry, segment: DataSegment) {
        existing.segments.push(segment);
        if !entry.file_header.split_after {
            existing.file_header.split_after = false;
        }
        // In multi-volume archives, each volume's file header stores the CRC of
        // that volume's data segment. Only the LAST continuation (split_after=false)
        // has the whole-file CRC. Always prefer the final segment's CRC.
        if (!entry.file_header.split_after && entry.file_header.data_crc32.is_some())
            || existing.file_header.data_crc32.is_none()
        {
            existing.file_header.data_crc32 = entry.file_header.data_crc32;
            // When we adopt the CRC from a different segment, also update
            // use_hash_mac from that segment's encryption info. RAR7 sets
            // HASHMAC (enc_flags & 0x0002) only on the final segment of
            // encrypted multi-volume files, meaning the CRC there is
            // HMAC-transformed. We must know this to skip verification.
            if let Some(ref entry_enc) = entry.file_encryption
                && entry_enc.use_hash_mac
                && let Some(ref mut existing_enc) = existing.file_encryption
            {
                existing_enc.use_hash_mac = true;
            }
        }
        if existing.file_header.unpacked_size.is_none() {
            existing.file_header.unpacked_size = entry.file_header.unpacked_size;
        }
        // Carry over the name if the existing entry still has an empty name
        // (can happen with out-of-order volume arrival).
        if existing.file_header.name.is_empty() && !entry.file_header.name.is_empty() {
            existing.file_header.name = entry.file_header.name.clone();
        }
        if !existing.is_encrypted && entry.is_encrypted {
            existing.is_encrypted = true;
        }
        if existing.file_encryption.is_none() {
            existing.file_encryption = entry.file_encryption.clone();
        }
        if existing.rar4_salt.is_none() {
            existing.rar4_salt = entry.rar4_salt;
        }
        if existing.hash.is_none() {
            existing.hash = entry.hash.clone();
        }
        if existing.redirection.is_none() {
            existing.redirection = entry.redirection.clone();
        }
    }

    /// Store a volume reader at the given index.
    pub(super) fn store_volume_reader(&mut self, vol_num: usize, reader: Box<dyn ReadSeek>) {
        while self.volumes.len() <= vol_num {
            self.volumes.push(None);
        }
        self.volumes[vol_num] = Some(VolumeData {
            reader,
            index: vol_num,
        });
    }
}
