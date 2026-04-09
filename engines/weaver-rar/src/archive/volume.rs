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
        let parsed = crate::rar4::parse_rar4_headers(&mut reader, self.password.as_deref())?;

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
            // continuation in the immediately preceding volume.
            let existing = self.members.iter_mut().find(|m| {
                m.file_header.split_after
                    && m.segments.last().map(|s| s.volume_index) == Some(vol_num.wrapping_sub(1))
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

        self.reconcile_member_chains();
    }

    fn reconcile_member_chains(&mut self) {
        while let Some((left_idx, right_idx)) = self.find_mergeable_member_pair() {
            debug_assert_ne!(left_idx, right_idx);
            let right = self.members.remove(right_idx);
            let adjusted_left_idx = if right_idx < left_idx {
                left_idx - 1
            } else {
                left_idx
            };
            let left = &mut self.members[adjusted_left_idx];
            Self::merge_member_chain(left, right);
        }
    }

    fn find_mergeable_member_pair(&self) -> Option<(usize, usize)> {
        for left_idx in 0..self.members.len() {
            let left = &self.members[left_idx];
            if !left.file_header.split_after {
                continue;
            }
            let Some(last_vol) = left.segments.last().map(|s| s.volume_index) else {
                continue;
            };

            for right_idx in 0..self.members.len() {
                if left_idx == right_idx {
                    continue;
                }
                let right = &self.members[right_idx];
                let Some(first_vol) = right.segments.first().map(|s| s.volume_index) else {
                    continue;
                };

                if !right.file_header.split_before || first_vol != last_vol + 1 {
                    continue;
                }

                if Self::members_name_compatible(left, right) {
                    return Some((left_idx, right_idx));
                }
            }
        }

        None
    }

    fn members_name_compatible(left: &MemberEntry, right: &MemberEntry) -> bool {
        left.file_header.name.is_empty()
            || right.file_header.name.is_empty()
            || left.file_header.name == right.file_header.name
    }

    fn merge_member_chain(existing: &mut MemberEntry, mut continuation: MemberEntry) {
        existing.segments.append(&mut continuation.segments);
        existing
            .segments
            .sort_by_key(|segment| segment.volume_index);
        Self::merge_member_metadata(existing, &continuation.file_header, &continuation);
    }

    /// Merge a continuation entry's metadata into an existing member.
    fn merge_continuation(existing: &mut MemberEntry, entry: &MemberEntry, segment: DataSegment) {
        existing.segments.push(segment);
        existing
            .segments
            .sort_by_key(|existing_segment| existing_segment.volume_index);
        Self::merge_member_metadata(existing, &entry.file_header, entry);
    }

    fn merge_member_metadata(
        existing: &mut MemberEntry,
        incoming_header: &crate::header::file::FileHeader,
        incoming_entry: &MemberEntry,
    ) {
        if !incoming_header.split_after {
            existing.file_header.split_after = false;
        }
        if incoming_header.unpacked_size.is_some() {
            existing.file_header.unpacked_size = incoming_header.unpacked_size;
        }
        // In multi-volume archives, each volume's file header stores the CRC of
        // that volume's data segment. Only the LAST continuation (split_after=false)
        // has the whole-file CRC. Always prefer the final segment's CRC.
        if (!incoming_header.split_after && incoming_header.data_crc32.is_some())
            || existing.file_header.data_crc32.is_none()
        {
            existing.file_header.data_crc32 = incoming_header.data_crc32;
            // When we adopt the CRC from a different segment, also update
            // use_hash_mac from that segment's encryption info. RAR7 sets
            // HASHMAC (enc_flags & 0x0002) only on the final segment of
            // encrypted multi-volume files, meaning the CRC there is
            // HMAC-transformed. We must know this to skip verification.
            if let Some(ref entry_enc) = incoming_entry.file_encryption
                && entry_enc.use_hash_mac
                && let Some(ref mut existing_enc) = existing.file_encryption
            {
                existing_enc.use_hash_mac = true;
            }
        }
        // Carry over the name if the existing entry still has an empty name
        // (can happen with out-of-order volume arrival).
        if existing.file_header.name.is_empty() && !incoming_header.name.is_empty() {
            existing.file_header.name = incoming_header.name.clone();
        }
        if !existing.is_encrypted && incoming_entry.is_encrypted {
            existing.is_encrypted = true;
        }
        if existing.file_encryption.is_none() && incoming_entry.file_encryption.is_some() {
            existing.file_encryption = incoming_entry.file_encryption.clone();
        }
        if existing.rar4_salt.is_none() && incoming_entry.rar4_salt.is_some() {
            existing.rar4_salt = incoming_entry.rar4_salt;
        }
        if existing.hash.is_none() && incoming_entry.hash.is_some() {
            existing.hash = incoming_entry.hash.clone();
        }
        if existing.redirection.is_none() && incoming_entry.redirection.is_some() {
            existing.redirection = incoming_entry.redirection.clone();
        }
    }

    /// Store a volume reader at the given index.
    pub(super) fn store_volume_reader(&mut self, vol_num: usize, reader: Box<dyn ReadSeek>) {
        while self.volumes.len() <= vol_num {
            self.volumes.push(None);
        }
        self.volumes[vol_num] = Some(VolumeData { reader });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::header::file::FileHeader;
    use crate::limits::Limits;
    use crate::types::{ArchiveFormat, CompressionInfo, CompressionMethod, FileAttributes, HostOs};

    fn test_file_header(
        name: &str,
        split_before: bool,
        split_after: bool,
        data_crc32: Option<u32>,
    ) -> FileHeader {
        FileHeader {
            name: name.to_string(),
            unpacked_size: Some(16),
            attributes: FileAttributes(0),
            mtime: None,
            data_crc32,
            compression: CompressionInfo {
                format: ArchiveFormat::Rar5,
                version: 0,
                solid: false,
                method: CompressionMethod::Store,
                dict_size: 0,
            },
            host_os: HostOs::Unix,
            is_directory: false,
            file_flags: 0,
            data_size: 8,
            split_before,
            split_after,
            data_offset: 0,
            is_encrypted: false,
        }
    }

    fn test_member(
        name: &str,
        first_volume: usize,
        split_before: bool,
        split_after: bool,
        data_crc32: Option<u32>,
    ) -> MemberEntry {
        MemberEntry {
            file_header: test_file_header(name, split_before, split_after, data_crc32),
            is_encrypted: false,
            file_encryption: None,
            rar4_salt: None,
            hash: None,
            redirection: None,
            segments: vec![DataSegment {
                volume_index: first_volume,
                data_offset: 0,
                data_size: 8,
            }],
        }
    }

    fn empty_archive(members: Vec<MemberEntry>) -> RarArchive {
        RarArchive {
            format: ArchiveFormat::Rar5,
            is_solid: false,
            is_encrypted: false,
            volume_set: VolumeSet::new(),
            members,
            more_volumes: true,
            volumes: Vec::new(),
            solid_decoder: None,
            solid_decoder_rar4: None,
            solid_next_index: 0,
            limits: Limits::default(),
            password: None,
            kdf_cache: crate::crypto::KdfCache::new(),
        }
    }

    #[test]
    fn reconcile_member_chains_handles_right_index_before_left_index() {
        let mut archive = empty_archive(vec![
            test_member("", 2, true, false, Some(0x2222_2222)),
            test_member("episode.mkv", 1, false, true, None),
        ]);

        archive.reconcile_member_chains();

        assert_eq!(archive.members.len(), 1);
        let merged = &archive.members[0];
        assert_eq!(merged.file_header.name, "episode.mkv");
        assert!(!merged.file_header.split_after);
        assert_eq!(merged.file_header.data_crc32, Some(0x2222_2222));
        assert_eq!(
            merged
                .segments
                .iter()
                .map(|segment| segment.volume_index)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
    }
}
