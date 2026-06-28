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
        let first = readers.next().ok_or_else(|| RarError::CorruptArchive {
            detail: "no volumes provided".into(),
        })?;
        let mut archive = Self::open_boxed_inner(
            first,
            None,
            std::sync::Arc::new(crate::crypto::KdfCache::new()),
        )?;

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

        if format == ArchiveFormat::Rar14 {
            return self.add_volume_rar14(index, reader);
        }
        if format == ArchiveFormat::Rar4 {
            return self.add_volume_rar4(index, reader);
        }

        // Parse headers (RAR5).
        let parsed = header::parse_all_headers_with_kdf_cache(
            &mut reader,
            self.password.as_deref(),
            &self.kdf_cache,
        )?;

        let vol_num = parsed
            .main
            .as_ref()
            .and_then(|m| m.volume_number)
            .unwrap_or(index as u64) as usize;
        self.ensure_volume_header_encryption_matches(vol_num, parsed.is_encrypted)?;

        // Register this volume.
        self.volume_set.add_volume(vol_num);

        let more_volumes = parsed.end.as_ref().is_some_and(|e| e.more_volumes);

        if !more_volumes {
            self.volume_set.set_last_volume_seen();
            self.more_volumes = false;
        }

        // Process file headers from this volume.
        for pf in parsed.files {
            let packed_hash = Self::packed_hash_for_split_segment(&pf.header, pf.hash.as_ref());
            let packed_hash_uses_mac = pf
                .file_encryption
                .as_ref()
                .is_some_and(|enc| enc.use_hash_mac);
            let segment = DataSegment::with_packed_hash(
                vol_num,
                pf.header.data_offset,
                pf.header.data_size,
                packed_hash,
                packed_hash_uses_mac,
            );

            let mut file_header = pf.header;
            file_header.is_encrypted = pf.is_encrypted;
            let entry = MemberEntry {
                file_header,
                is_encrypted: pf.is_encrypted,
                file_encryption: pf.file_encryption.map(Self::file_encryption_info),
                rar4_salt: None,
                hash: pf.hash,
                redirection: pf.redirection,
                owner: pf.owner,
                segments: vec![segment],
            };

            self.integrate_member(vol_num, entry);
        }

        for service in parsed.services {
            let packed_hash =
                Self::packed_hash_for_split_segment(&service.header.inner, service.hash.as_ref());
            let packed_hash_uses_mac = service
                .file_encryption
                .as_ref()
                .is_some_and(|enc| enc.use_hash_mac);
            let segment = DataSegment::with_packed_hash(
                vol_num,
                service.header.inner.data_offset,
                service.header.inner.data_size,
                packed_hash,
                packed_hash_uses_mac,
            );
            let mut file_header = service.header.inner;
            file_header.is_encrypted = service.is_encrypted;
            let entry = ServiceEntry {
                header_offset: service.header.header_offset,
                file_header,
                is_encrypted: service.is_encrypted,
                file_encryption: service.file_encryption.map(Self::file_encryption_info),
                hash: service.hash,
                comment_crc16: None,
                segments: vec![segment],
            };
            Self::integrate_service_entry(&mut self.services, vol_num, entry);
        }
        self.refresh_rar5_recovery_records_from_services();

        // Store the reader.
        self.store_volume_reader(vol_num, reader);

        Ok(())
    }

    /// Re-parse and replace the cached topology contribution for a volume.
    ///
    /// This is used after external placement correction swaps volume contents
    /// while keeping the same filenames. Reader attachment is not enough in
    /// that case because the cached member segments still describe the old
    /// content at this volume index.
    pub fn refresh_volume(&mut self, index: usize, reader: Box<dyn ReadSeek>) -> RarResult<()> {
        self.remove_volume_segments(index);
        self.add_volume(index, reader)
    }

    fn remove_volume_segments(&mut self, index: usize) {
        for member in &mut self.members {
            member
                .segments
                .retain(|segment| segment.volume_index != index);
        }
        self.members.retain(|member| !member.segments.is_empty());
        for service in &mut self.services {
            service
                .segments
                .retain(|segment| segment.volume_index != index);
        }
        self.services.retain(|service| !service.segments.is_empty());
        self.reconcile_member_chains();
        Self::reconcile_service_chains(&mut self.services);
        self.refresh_rar5_recovery_records_from_services();
    }

    /// Add a RAR4 volume incrementally.
    pub(super) fn add_volume_rar4(
        &mut self,
        index: usize,
        mut reader: Box<dyn ReadSeek>,
    ) -> RarResult<()> {
        let parsed = crate::rar4::parse_rar4_headers_with_kdf_cache(
            &mut reader,
            self.password.as_deref(),
            &self.kdf_cache,
        )?;
        self.add_volume_rar4_parsed(index, reader, parsed)
    }

    /// Add a RAR 1.4 volume incrementally.
    pub(super) fn add_volume_rar14(
        &mut self,
        index: usize,
        mut reader: Box<dyn ReadSeek>,
    ) -> RarResult<()> {
        let parsed = crate::rar4::parse_rar14_headers(&mut reader)?;
        self.add_volume_rar4_parsed(index, reader, parsed)
    }

    fn add_volume_rar4_parsed(
        &mut self,
        index: usize,
        reader: Box<dyn ReadSeek>,
        parsed: crate::rar4::Rar4ParsedVolume,
    ) -> RarResult<()> {
        // Use volume number from ENDARC header if available, otherwise fall back to index.
        let vol_num = parsed
            .end
            .as_ref()
            .and_then(|e| e.volume_number)
            .map(|v| v as usize)
            .unwrap_or(index);
        self.ensure_volume_header_encryption_matches(vol_num, parsed.archive_header.is_encrypted)?;
        self.volume_set.add_volume(vol_num);

        let more_volumes = parsed.end.as_ref().is_some_and(|e| e.more_volumes);
        if !more_volumes {
            self.volume_set.set_last_volume_seen();
            self.more_volumes = false;
        }
        for fh in &parsed.files {
            let file_header = Self::rar4_to_file_header(fh, parsed.archive_header.is_solid);
            let segment = DataSegment::with_packed_hash(
                vol_num,
                fh.data_offset,
                fh.packed_size,
                Self::packed_hash_for_split_segment(&file_header, None),
                false,
            );

            let entry = MemberEntry {
                file_header,
                is_encrypted: fh.is_encrypted,
                file_encryption: None,
                rar4_salt: fh.salt,
                hash: None,
                redirection: None,
                owner: fh.owner.clone(),
                segments: vec![segment],
            };

            self.integrate_member(vol_num, entry);
        }

        for fh in &parsed.services {
            let file_header = Self::rar4_to_file_header(fh, parsed.archive_header.is_solid);
            let entry = ServiceEntry {
                header_offset: fh.data_offset,
                file_header: file_header.clone(),
                is_encrypted: fh.is_encrypted,
                file_encryption: None,
                hash: None,
                comment_crc16: None,
                segments: vec![DataSegment::with_packed_hash(
                    vol_num,
                    fh.data_offset,
                    fh.packed_size,
                    Self::packed_hash_for_split_segment(&file_header, None),
                    false,
                )],
            };
            Self::integrate_service_entry(&mut self.services, vol_num, entry);
        }
        for comment in &parsed.comments {
            let entry = Self::rar4_comment_to_service_entry(comment, vol_num, self.format);
            Self::integrate_service_entry(&mut self.services, vol_num, entry);
        }

        self.store_volume_reader(vol_num, reader);
        self.hydrate_rar4_uowner_payloads();
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
                let authoritative_data_crc32 =
                    (!existing.file_header.split_after).then_some(existing.file_header.data_crc32);
                let authoritative_data_hash =
                    (!existing.file_header.split_after).then_some(existing.file_header.data_hash);
                let authoritative_hash =
                    (!existing.file_header.split_after).then(|| existing.hash.clone());
                let authoritative_encryption =
                    (!existing.file_header.split_after).then(|| existing.file_encryption.clone());
                existing.segments.insert(0, segment);
                existing.file_header = entry.file_header;
                existing.is_encrypted = entry.is_encrypted;
                existing.file_encryption = entry.file_encryption;
                existing.rar4_salt = entry.rar4_salt;
                if let Some(data_crc32) = authoritative_data_crc32 {
                    existing.file_header.data_crc32 = data_crc32;
                }
                if let Some(data_hash) = authoritative_data_hash {
                    existing.file_header.data_hash = data_hash;
                }
                if let Some(hash) = authoritative_hash {
                    existing.hash = hash;
                } else if entry.hash.is_some() {
                    existing.hash = entry.hash;
                }
                if let Some(Some(final_encryption)) = authoritative_encryption {
                    if existing.file_encryption.is_none() {
                        existing.file_encryption = Some(final_encryption);
                    } else if final_encryption.use_hash_mac
                        && let Some(existing_encryption) = existing.file_encryption.as_mut()
                    {
                        existing_encryption.use_hash_mac = true;
                    }
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

    pub(crate) fn sort_members_by_physical_order(&mut self) {
        self.members.sort_by_key(Self::member_physical_order_key);
    }

    fn member_physical_order_key(member: &MemberEntry) -> (usize, u64) {
        member
            .segments
            .iter()
            .map(|segment| (segment.volume_index, segment.data_offset))
            .min()
            .unwrap_or((usize::MAX, member.file_header.data_offset))
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
        if self.solid_next_index == 0 {
            self.sort_members_by_physical_order();
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
        if !incoming_header.split_after
            || (existing.hash.is_none() && incoming_entry.hash.is_some())
        {
            existing.hash = incoming_entry.hash.clone();
        }
        if existing.redirection.is_none() && incoming_entry.redirection.is_some() {
            existing.redirection = incoming_entry.redirection.clone();
        }
    }

    pub(super) fn integrate_service_entry(
        services: &mut Vec<ServiceEntry>,
        vol_num: usize,
        entry: ServiceEntry,
    ) {
        let segment = entry.segments[0].clone();

        if entry.file_header.split_before {
            let existing = services.iter_mut().find(|service| {
                service.file_header.split_after
                    && service.segments.last().map(|s| s.volume_index)
                        == Some(vol_num.wrapping_sub(1))
                    && Self::service_names_compatible(service, &entry)
            });

            if let Some(existing) = existing {
                Self::merge_service_continuation(existing, &entry, segment);
            } else {
                services.push(entry);
            }
        } else {
            let existing = services.iter_mut().find(|service| {
                !entry.file_header.name.is_empty()
                    && service.file_header.name == entry.file_header.name
                    && service.segments[0].volume_index > vol_num
            });

            if let Some(existing) = existing {
                let authoritative_data_crc32 =
                    (!existing.file_header.split_after).then_some(existing.file_header.data_crc32);
                let authoritative_data_hash =
                    (!existing.file_header.split_after).then_some(existing.file_header.data_hash);
                let authoritative_hash =
                    (!existing.file_header.split_after).then(|| existing.hash.clone());
                let authoritative_comment_crc16 =
                    (!existing.file_header.split_after).then_some(existing.comment_crc16);
                let authoritative_encryption =
                    (!existing.file_header.split_after).then(|| existing.file_encryption.clone());
                existing.segments.insert(0, segment);
                existing.file_header = entry.file_header;
                existing.is_encrypted = entry.is_encrypted;
                existing.file_encryption = entry.file_encryption;
                if let Some(data_crc32) = authoritative_data_crc32 {
                    existing.file_header.data_crc32 = data_crc32;
                }
                if let Some(data_hash) = authoritative_data_hash {
                    existing.file_header.data_hash = data_hash;
                }
                if let Some(hash) = authoritative_hash {
                    existing.hash = hash;
                } else if entry.hash.is_some() {
                    existing.hash = entry.hash;
                }
                if let Some(comment_crc16) = authoritative_comment_crc16 {
                    existing.comment_crc16 = comment_crc16;
                } else if entry.comment_crc16.is_some() {
                    existing.comment_crc16 = entry.comment_crc16;
                }
                if let Some(Some(final_encryption)) = authoritative_encryption {
                    if existing.file_encryption.is_none() {
                        existing.file_encryption = Some(final_encryption);
                    } else if final_encryption.use_hash_mac
                        && let Some(existing_encryption) = existing.file_encryption.as_mut()
                    {
                        existing_encryption.use_hash_mac = true;
                    }
                }
            } else {
                services.push(entry);
            }
        }

        Self::reconcile_service_chains(services);
    }

    fn reconcile_service_chains(services: &mut Vec<ServiceEntry>) {
        while let Some((left_idx, right_idx)) = Self::find_mergeable_service_pair(services) {
            debug_assert_ne!(left_idx, right_idx);
            let right = services.remove(right_idx);
            let adjusted_left_idx = if right_idx < left_idx {
                left_idx - 1
            } else {
                left_idx
            };
            let left = &mut services[adjusted_left_idx];
            Self::merge_service_chain(left, right);
        }
        services.sort_by_key(Self::service_physical_order_key);
    }

    fn find_mergeable_service_pair(services: &[ServiceEntry]) -> Option<(usize, usize)> {
        for left_idx in 0..services.len() {
            let left = &services[left_idx];
            if !left.file_header.split_after {
                continue;
            }
            let Some(last_vol) = left.segments.last().map(|s| s.volume_index) else {
                continue;
            };

            for (right_idx, right) in services.iter().enumerate() {
                if left_idx == right_idx {
                    continue;
                }
                let Some(first_vol) = right.segments.first().map(|s| s.volume_index) else {
                    continue;
                };

                if !right.file_header.split_before || first_vol != last_vol + 1 {
                    continue;
                }

                if Self::service_names_compatible(left, right) {
                    return Some((left_idx, right_idx));
                }
            }
        }

        None
    }

    fn merge_service_chain(existing: &mut ServiceEntry, mut continuation: ServiceEntry) {
        existing.segments.append(&mut continuation.segments);
        existing
            .segments
            .sort_by_key(|segment| segment.volume_index);
        Self::merge_service_metadata(existing, &continuation);
    }

    fn service_physical_order_key(service: &ServiceEntry) -> (usize, u64) {
        service
            .segments
            .iter()
            .map(|segment| (segment.volume_index, segment.data_offset))
            .min()
            .unwrap_or((usize::MAX, service.file_header.data_offset))
    }

    fn service_names_compatible(left: &ServiceEntry, right: &ServiceEntry) -> bool {
        left.file_header.name.is_empty()
            || right.file_header.name.is_empty()
            || left.file_header.name == right.file_header.name
    }

    fn merge_service_continuation(
        existing: &mut ServiceEntry,
        incoming: &ServiceEntry,
        segment: DataSegment,
    ) {
        existing.segments.push(segment);
        existing
            .segments
            .sort_by_key(|existing_segment| existing_segment.volume_index);
        Self::merge_service_metadata(existing, incoming);
    }

    fn merge_service_metadata(existing: &mut ServiceEntry, incoming: &ServiceEntry) {
        let incoming_header = &incoming.file_header;

        if !incoming_header.split_after {
            existing.file_header.split_after = false;
        }
        if incoming_header.unpacked_size.is_some() {
            existing.file_header.unpacked_size = incoming_header.unpacked_size;
        }
        if (!incoming_header.split_after && incoming_header.data_crc32.is_some())
            || existing.file_header.data_crc32.is_none()
        {
            existing.file_header.data_crc32 = incoming_header.data_crc32;
            if let Some(ref incoming_enc) = incoming.file_encryption
                && incoming_enc.use_hash_mac
                && let Some(ref mut existing_enc) = existing.file_encryption
            {
                existing_enc.use_hash_mac = true;
            }
        }
        if existing.file_header.name.is_empty() && !incoming_header.name.is_empty() {
            existing.file_header.name = incoming_header.name.clone();
        }
        if !existing.is_encrypted && incoming.is_encrypted {
            existing.is_encrypted = true;
        }
        if existing.file_encryption.is_none() && incoming.file_encryption.is_some() {
            existing.file_encryption = incoming.file_encryption.clone();
        }
        if !incoming_header.split_after || (existing.hash.is_none() && incoming.hash.is_some()) {
            existing.hash = incoming.hash.clone();
        }
        if !incoming_header.split_after
            || (existing.comment_crc16.is_none() && incoming.comment_crc16.is_some())
        {
            existing.comment_crc16 = incoming.comment_crc16;
        }
    }

    fn ensure_volume_header_encryption_matches(
        &self,
        vol_num: usize,
        next_is_encrypted: bool,
    ) -> RarResult<()> {
        if self.is_encrypted != next_is_encrypted {
            return Err(RarError::CorruptArchive {
                detail: format!(
                    "volume {vol_num} changed header encryption state from {} to {}",
                    self.is_encrypted, next_is_encrypted
                ),
            });
        }
        Ok(())
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
    use crate::types::{
        ArchiveFormat, CompressionInfo, CompressionMethod, FileAttributes, FileHash, HostOs,
        RecoveryRecordKind,
    };

    fn test_file_header(
        name: &str,
        split_before: bool,
        split_after: bool,
        data_crc32: Option<u32>,
    ) -> FileHeader {
        FileHeader {
            name: name.to_string(),
            name_raw: Some(name.as_bytes().to_vec()),
            unpacked_size: Some(16),
            attributes: FileAttributes(0),
            mtime: None,
            ctime: None,
            atime: None,
            data_crc32,
            data_hash: data_crc32.map(crate::types::DataHash::Crc32),
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
            version: None,
            service_subdata: None,
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
            owner: None,
            segments: vec![DataSegment::new(first_volume, 0, 8)],
        }
    }

    fn test_service(
        name: &str,
        first_volume: usize,
        split_before: bool,
        split_after: bool,
    ) -> ServiceEntry {
        ServiceEntry {
            header_offset: 100 + first_volume as u64,
            file_header: test_file_header(name, split_before, split_after, Some(0xAABB_CCDD)),
            is_encrypted: false,
            file_encryption: None,
            hash: None,
            comment_crc16: Some(0xCCDD),
            segments: vec![DataSegment::new(first_volume, 16, 8)],
        }
    }

    fn test_member_with_offset(
        name: &str,
        first_volume: usize,
        split_before: bool,
        split_after: bool,
        data_crc32: Option<u32>,
        data_offset: u64,
    ) -> MemberEntry {
        let mut member = test_member(name, first_volume, split_before, split_after, data_crc32);
        member.file_header.data_offset = data_offset;
        member.segments[0].data_offset = data_offset;
        member
    }

    fn empty_archive(members: Vec<MemberEntry>) -> RarArchive {
        RarArchive {
            format: ArchiveFormat::Rar5,
            is_solid: false,
            is_encrypted: false,
            has_recovery_record: false,
            recovery_records: Vec::new(),
            is_locked: false,
            has_authenticity_verification: false,
            has_locator: false,
            quick_open_offset: None,
            recovery_record_offset: None,
            original_name: None,
            original_name_raw: None,
            original_creation_time: None,
            volume_set: VolumeSet::new(),
            members,
            services: Vec::new(),
            more_volumes: true,
            volumes: Vec::new(),
            solid_decoder: None,
            solid_decoder_rar4: None,
            solid_next_index: 0,
            limits: Limits::default(),
            password: None,
            kdf_cache: std::sync::Arc::new(crate::crypto::KdfCache::new()),
        }
    }

    #[test]
    fn volume_header_encryption_state_must_match_unrar_merge_guard() {
        let mut archive = empty_archive(Vec::new());
        assert!(
            archive
                .ensure_volume_header_encryption_matches(1, false)
                .is_ok()
        );
        let err = archive
            .ensure_volume_header_encryption_matches(1, true)
            .unwrap_err();
        assert!(
            matches!(err, RarError::CorruptArchive { ref detail } if detail.contains("changed header encryption state")),
            "expected header encryption mismatch, got {err:?}"
        );

        archive.is_encrypted = true;
        assert!(
            archive
                .ensure_volume_header_encryption_matches(2, true)
                .is_ok()
        );
        let err = archive
            .ensure_volume_header_encryption_matches(2, false)
            .unwrap_err();
        assert!(
            matches!(err, RarError::CorruptArchive { ref detail } if detail.contains("volume 2")),
            "expected volume-specific mismatch, got {err:?}"
        );
    }

    #[test]
    fn refresh_volume_removes_stale_service_segments() {
        let mut archive = empty_archive(Vec::new());
        archive.services = vec![
            test_service("CMT", 0, false, false),
            test_service("RR", 1, false, false),
        ];

        archive.remove_volume_segments(1);

        assert_eq!(archive.services.len(), 1);
        assert_eq!(archive.services[0].file_header.name, "CMT");
        assert_eq!(archive.services[0].segments[0].volume_index, 0);
    }

    #[test]
    fn rar5_recovery_records_refresh_from_integrated_services() {
        let mut archive = empty_archive(Vec::new());
        let mut rr = test_service("RR", 2, false, false);
        rr.header_offset = 0x1234;
        rr.file_header.data_offset = 0x1300;
        rr.file_header.data_size = 0x2000;
        rr.file_header.service_subdata = Some(crate::vint::encode_vint(375));
        archive.services = vec![rr];

        archive.refresh_rar5_recovery_records_from_services();

        assert!(archive.has_recovery_record);
        assert_eq!(archive.recovery_records.len(), 1);
        let record = &archive.recovery_records[0];
        assert_eq!(record.format, ArchiveFormat::Rar5);
        assert_eq!(record.kind, RecoveryRecordKind::Rar5Service);
        assert_eq!(record.offset, 0x1234);
        assert_eq!(record.data_offset, 0x1300);
        assert_eq!(record.data_size, 0x2000);
        assert_eq!(record.protected_size, Some(0x1234));
        assert_eq!(record.recovery_percent, Some(375));
    }

    #[test]
    fn removing_rr_service_drops_stale_rar5_recovery_records() {
        let mut archive = empty_archive(Vec::new());
        archive.services = vec![test_service("RR", 1, false, false)];
        archive.refresh_rar5_recovery_records_from_services();
        assert_eq!(archive.recovery_records.len(), 1);

        archive.remove_volume_segments(1);

        assert!(archive.recovery_records.is_empty());
    }

    #[test]
    fn reconcile_service_chains_handles_right_index_before_left_index() {
        let mut archive = empty_archive(Vec::new());
        archive.services = vec![
            test_service("", 2, true, false),
            test_service("RR", 1, false, true),
        ];
        archive.services[0].file_header.data_crc32 = Some(0x2222_2222);
        archive.services[0].comment_crc16 = Some(0x2222);
        archive.services[1].file_header.data_crc32 = None;
        archive.services[1].comment_crc16 = None;

        RarArchive::reconcile_service_chains(&mut archive.services);

        assert_eq!(archive.services.len(), 1);
        let merged = &archive.services[0];
        assert_eq!(merged.file_header.name, "RR");
        assert!(!merged.file_header.split_after);
        assert_eq!(merged.file_header.data_crc32, Some(0x2222_2222));
        assert_eq!(merged.comment_crc16, Some(0x2222));
        assert_eq!(
            merged
                .segments
                .iter()
                .map(|segment| segment.volume_index)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
    }

    #[test]
    fn integrate_service_preserves_out_of_order_final_hash_and_crc() {
        let first_hash = [0x11; 32];
        let final_hash = [0x22; 32];
        let mut continuation = test_service("CMT", 2, true, false);
        continuation.file_header.data_crc32 = Some(0x2222_2222);
        continuation.hash = Some(FileHash::Blake2sp(final_hash));
        continuation.comment_crc16 = Some(0x2222);
        let mut services = vec![continuation];

        let mut first = test_service("CMT", 1, false, true);
        first.file_header.data_crc32 = Some(0x1111_1111);
        first.hash = Some(FileHash::Blake2sp(first_hash));
        first.comment_crc16 = Some(0x1111);
        RarArchive::integrate_service_entry(&mut services, 1, first);

        assert_eq!(services.len(), 1);
        assert_eq!(services[0].file_header.data_crc32, Some(0x2222_2222));
        assert_eq!(services[0].hash, Some(FileHash::Blake2sp(final_hash)));
        assert_eq!(services[0].comment_crc16, Some(0x2222));
        assert_eq!(
            services[0]
                .segments
                .iter()
                .map(|segment| segment.volume_index)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
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

    #[test]
    fn merge_member_metadata_prefers_final_continuation_blake2_hash() {
        let first_hash = [0x11; 32];
        let final_hash = [0x22; 32];
        let mut archive = empty_archive(vec![test_member(
            "episode.mkv",
            1,
            false,
            true,
            Some(0x1111_1111),
        )]);
        archive.members[0].hash = Some(FileHash::Blake2sp(first_hash));
        let mut continuation = test_member("", 2, true, false, Some(0x2222_2222));
        continuation.hash = Some(FileHash::Blake2sp(final_hash));

        archive.integrate_member(2, continuation);

        assert_eq!(
            archive.members[0].hash,
            Some(FileHash::Blake2sp(final_hash))
        );
    }

    #[test]
    fn integrate_member_preserves_out_of_order_final_blake2_hash() {
        let first_hash = [0x11; 32];
        let final_hash = [0x22; 32];
        let mut continuation = test_member("episode.mkv", 2, true, false, Some(0x2222_2222));
        continuation.hash = Some(FileHash::Blake2sp(final_hash));
        let mut archive = empty_archive(vec![continuation]);
        let mut first = test_member("episode.mkv", 1, false, true, Some(0x1111_1111));
        first.hash = Some(FileHash::Blake2sp(first_hash));

        archive.integrate_member(1, first);

        assert_eq!(archive.members[0].file_header.data_crc32, Some(0x2222_2222));
        assert_eq!(
            archive.members[0].file_header.data_hash,
            Some(crate::types::DataHash::Crc32(0x2222_2222))
        );
        assert_eq!(
            archive.members[0].hash,
            Some(FileHash::Blake2sp(final_hash))
        );
    }

    #[test]
    fn cached_solid_multivolume_keeps_prior_member_before_split_member() {
        let archive = empty_archive(vec![
            test_member_with_offset("sample.mkv", 0, false, true, Some(0x1111_1111), 200),
            test_member_with_offset("ep1.mkv", 0, false, false, Some(0x2222_2222), 100),
        ]);

        let headers = archive.serialize_headers();
        let mut restored = RarArchive::deserialize_headers(&headers).unwrap();

        assert_eq!(restored.member_names(), vec!["ep1.mkv", "sample.mkv"]);
        assert_eq!(restored.find_member_sanitized("sample.mkv"), Some(1));

        restored.integrate_member(
            1,
            test_member_with_offset("sample.mkv", 1, true, true, Some(0x3333_3333), 0),
        );
        restored.integrate_member(
            2,
            test_member_with_offset("sample.mkv", 2, true, false, Some(0x4444_4444), 0),
        );

        assert_eq!(restored.member_names(), vec!["ep1.mkv", "sample.mkv"]);
        assert_eq!(restored.find_member_sanitized("sample.mkv"), Some(1));
    }
}
