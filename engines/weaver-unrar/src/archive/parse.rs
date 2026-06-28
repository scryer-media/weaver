use super::*;
use std::sync::Arc;

impl RarArchive {
    pub(super) fn file_encryption_info(
        params: crate::header::FileEncryptionParams,
    ) -> FileEncryptionInfo {
        FileEncryptionInfo {
            version: params.version,
            kdf_count: params.kdf_count,
            salt: params.salt,
            iv: params.iv,
            check_data: params.check_data,
            use_hash_mac: params.use_hash_mac,
        }
    }

    pub(super) fn open_boxed_inner(
        mut reader: Box<dyn ReadSeek>,
        password: Option<&str>,
        kdf_cache: Arc<crate::crypto::KdfCache>,
    ) -> RarResult<Self> {
        // Seek to start
        reader.seek(SeekFrom::Start(0)).map_err(RarError::Io)?;

        // Read and validate signature
        let format = signature::read_signature(&mut reader)?;
        tracing::debug!("detected format: {:?}", format);

        // Dispatch based on format.
        if format == ArchiveFormat::Rar14 {
            return Self::open_rar14(reader, password, kdf_cache);
        }
        if format == ArchiveFormat::Rar4 {
            return Self::open_rar4(reader, password, kdf_cache);
        }

        // Parse all headers (RAR5)
        let mut parsed =
            header::parse_all_headers_with_kdf_cache(&mut reader, password, &kdf_cache)?;

        if let Some(rr_offset) = parsed.main.as_ref().and_then(|m| m.recovery_record_offset) {
            let already_parsed = parsed
                .services
                .iter()
                .any(|service| service.header.service_name() == "RR");
            if !already_parsed
                && let Some(service) = Self::rar5_locator_rr_service(&mut reader, rr_offset)?
            {
                parsed.services.push(service);
            }
        }

        let main = parsed.main.as_ref();
        let is_solid = main.is_some_and(|m| m.is_solid);
        let mut recovery_records = Self::rar5_recovery_records(&parsed.services);
        let has_recovery_record =
            main.is_some_and(|m| m.has_recovery_record) || !recovery_records.is_empty();
        let is_locked = main.is_some_and(|m| m.is_locked);

        let is_volume = main.is_some_and(|m| m.is_volume);

        let volume_number = main.and_then(|m| m.volume_number).unwrap_or(0) as usize;

        let mut volume_set = if is_volume {
            let mut vs = VolumeSet::new();
            vs.add_volume(volume_number);
            vs
        } else {
            VolumeSet::single()
        };

        let more_volumes = parsed.end.as_ref().is_some_and(|e| e.more_volumes);

        if !more_volumes {
            volume_set.set_last_volume_seen();
        }

        // Build member entries from file headers.
        // For the first volume, each file header that doesn't have split_before
        // starts a new member. Files with split_before are continuations from
        // a previous volume (we'll reconcile when that volume is added).
        let mut members = Vec::new();

        for pf in parsed.files {
            let packed_hash = Self::packed_hash_for_split_segment(&pf.header, pf.hash.as_ref());
            let packed_hash_uses_mac = pf
                .file_encryption
                .as_ref()
                .is_some_and(|enc| enc.use_hash_mac);
            let segment = DataSegment::with_packed_hash(
                volume_number,
                pf.header.data_offset,
                pf.header.data_size,
                packed_hash,
                packed_hash_uses_mac,
            );
            let mut file_header = pf.header;
            file_header.is_encrypted = pf.is_encrypted;
            members.push(MemberEntry {
                file_header,
                is_encrypted: pf.is_encrypted,
                file_encryption: pf.file_encryption.map(Self::file_encryption_info),
                rar4_salt: None,
                hash: pf.hash,
                redirection: pf.redirection,
                owner: pf.owner,
                segments: vec![segment],
            });
        }

        let mut services = Vec::new();
        for service in parsed.services {
            let entry = {
                let packed_hash = Self::packed_hash_for_split_segment(
                    &service.header.inner,
                    service.hash.as_ref(),
                );
                let packed_hash_uses_mac = service
                    .file_encryption
                    .as_ref()
                    .is_some_and(|enc| enc.use_hash_mac);
                let segment = DataSegment::with_packed_hash(
                    volume_number,
                    service.header.inner.data_offset,
                    service.header.inner.data_size,
                    packed_hash,
                    packed_hash_uses_mac,
                );
                let mut file_header = service.header.inner;
                file_header.is_encrypted = service.is_encrypted;
                let ntfs_stream_name = (file_header.name == "STM")
                    .then(|| {
                        file_header
                            .service_subdata
                            .as_deref()
                            .map(crate::header::common::decode_utf8_prefix_until_nul)
                    })
                    .flatten();
                ServiceEntry {
                    header_offset: service.header.header_offset,
                    file_header,
                    is_encrypted: service.is_encrypted,
                    file_encryption: service.file_encryption.map(Self::file_encryption_info),
                    hash: service.hash,
                    comment_crc16: None,
                    ntfs_stream_name,
                    segments: vec![segment],
                }
            };
            Self::integrate_service_entry(&mut services, volume_number, entry);
        }

        // Store the volume reader.
        let mut volumes: Vec<Option<VolumeData>> = Vec::new();
        while volumes.len() <= volume_number {
            volumes.push(None);
        }
        volumes[volume_number] = Some(VolumeData { reader });

        Ok(RarArchive {
            format,
            is_solid,
            is_encrypted: parsed.is_encrypted,
            has_recovery_record,
            recovery_records: std::mem::take(&mut recovery_records),
            is_locked,
            has_authenticity_verification: false,
            has_locator: main.is_some_and(|m| m.has_locator),
            quick_open_offset: main.and_then(|m| m.quick_open_offset),
            recovery_record_offset: main.and_then(|m| m.recovery_record_offset),
            original_name: main.and_then(|m| m.original_name.clone()),
            original_name_raw: main.and_then(|m| m.original_name_raw.clone()),
            original_creation_time: main.and_then(|m| m.original_creation_time),
            volume_set,
            members,
            services,
            more_volumes,
            volumes,
            solid_decoder: None,
            solid_decoder_rar4: None,
            solid_next_index: 0,
            limits: Limits::default(),
            password: password.map(String::from),
            kdf_cache,
        })
    }

    /// Open a RAR4 archive.
    pub(super) fn open_rar4(
        mut reader: Box<dyn ReadSeek>,
        password: Option<&str>,
        kdf_cache: Arc<crate::crypto::KdfCache>,
    ) -> RarResult<Self> {
        let parsed =
            crate::rar4::parse_rar4_headers_with_kdf_cache(&mut reader, password, &kdf_cache)?;
        Self::open_rar4_parsed(reader, password, kdf_cache, ArchiveFormat::Rar4, parsed)
    }

    /// Open a RAR 1.4 archive.
    pub(super) fn open_rar14(
        mut reader: Box<dyn ReadSeek>,
        password: Option<&str>,
        kdf_cache: Arc<crate::crypto::KdfCache>,
    ) -> RarResult<Self> {
        let parsed = crate::rar4::parse_rar14_headers(&mut reader)?;
        Self::open_rar4_parsed(reader, password, kdf_cache, ArchiveFormat::Rar14, parsed)
    }

    pub(super) fn open_rar4_parsed(
        reader: Box<dyn ReadSeek>,
        password: Option<&str>,
        kdf_cache: Arc<crate::crypto::KdfCache>,
        format: ArchiveFormat,
        parsed: crate::rar4::Rar4ParsedVolume,
    ) -> RarResult<Self> {
        let is_solid = parsed.archive_header.is_solid;
        let is_volume = parsed.archive_header.is_volume;
        let recovery_records = Self::rar4_recovery_records(&parsed.recovery_records);

        let mut volume_set = if is_volume {
            let mut vs = VolumeSet::new();
            vs.add_volume(0);
            vs
        } else {
            VolumeSet::single()
        };

        let more_volumes = parsed.end.as_ref().is_some_and(|e| e.more_volumes);
        if !more_volumes {
            volume_set.set_last_volume_seen();
        }

        let mut members = Vec::new();
        for fh in &parsed.files {
            let file_header = Self::rar4_to_file_header(fh, parsed.archive_header.is_solid);
            let segment = DataSegment::with_packed_hash(
                0,
                fh.data_offset,
                fh.packed_size,
                Self::packed_hash_for_split_segment(&file_header, None),
                false,
            );
            members.push(MemberEntry {
                file_header,
                is_encrypted: fh.is_encrypted,
                file_encryption: None,
                rar4_salt: fh.salt,
                hash: None,
                redirection: None,
                owner: fh.owner.clone(),
                segments: vec![segment],
            });
        }

        let mut services = Vec::new();
        for fh in &parsed.services {
            let file_header = Self::rar4_to_file_header(fh, parsed.archive_header.is_solid);
            let entry = ServiceEntry {
                header_offset: fh.data_offset,
                segments: vec![DataSegment::with_packed_hash(
                    0,
                    fh.data_offset,
                    fh.packed_size,
                    Self::packed_hash_for_split_segment(&file_header, None),
                    false,
                )],
                file_header,
                is_encrypted: fh.is_encrypted,
                file_encryption: None,
                hash: None,
                comment_crc16: None,
                ntfs_stream_name: None,
            };
            Self::integrate_service_entry(&mut services, 0, entry);
        }
        for old_service in &parsed.old_services {
            if let Some(entry) = Self::rar4_old_service_to_service_entry(old_service, 0, format) {
                Self::integrate_service_entry(&mut services, 0, entry);
            }
        }
        for comment in &parsed.comments {
            let entry = Self::rar4_comment_to_service_entry(comment, 0, format);
            Self::integrate_service_entry(&mut services, 0, entry);
        }

        let volumes: Vec<Option<VolumeData>> = vec![Some(VolumeData { reader })];

        let mut archive = RarArchive {
            format,
            is_solid,
            is_encrypted: parsed.archive_header.is_encrypted,
            has_recovery_record: parsed.archive_header.has_recovery_record
                || !recovery_records.is_empty(),
            recovery_records,
            is_locked: parsed.archive_header.is_locked,
            has_authenticity_verification: parsed.archive_header.has_authenticity_verification,
            has_locator: false,
            quick_open_offset: None,
            recovery_record_offset: None,
            original_name: None,
            original_name_raw: None,
            original_creation_time: None,
            volume_set,
            members,
            services,
            more_volumes,
            volumes,
            solid_decoder: None,
            solid_decoder_rar4: None,
            solid_next_index: 0,
            limits: Limits::default(),
            password: password.map(String::from),
            kdf_cache,
        };
        archive.hydrate_rar4_uowner_payloads();
        Ok(archive)
    }

    fn rar5_recovery_records(services: &[crate::header::ParsedService]) -> Vec<RecoveryRecordInfo> {
        services
            .iter()
            .filter(|service| service.header.service_name() == "RR")
            .map(|service| {
                Self::rar5_recovery_record_info(
                    service.header.header_offset,
                    service.header.inner.data_offset,
                    service.header.inner.data_size,
                    service.header.inner.service_subdata.as_deref(),
                )
            })
            .collect()
    }

    fn rar5_service_entry_recovery_records(services: &[ServiceEntry]) -> Vec<RecoveryRecordInfo> {
        services
            .iter()
            .filter(|service| service.file_header.name == "RR")
            .map(|service| {
                Self::rar5_recovery_record_info(
                    service.header_offset,
                    service.file_header.data_offset,
                    service.file_header.data_size,
                    service.file_header.service_subdata.as_deref(),
                )
            })
            .collect()
    }

    fn rar5_recovery_record_info(
        header_offset: u64,
        data_offset: u64,
        data_size: u64,
        service_subdata: Option<&[u8]>,
    ) -> RecoveryRecordInfo {
        RecoveryRecordInfo {
            format: ArchiveFormat::Rar5,
            kind: RecoveryRecordKind::Rar5Service,
            offset: header_offset,
            data_offset,
            data_size,
            protected_size: Some(header_offset),
            recovery_sectors: None,
            total_blocks: None,
            recovery_percent: service_subdata
                .and_then(|data| crate::vint::read_vint(data).ok().map(|(value, _)| value)),
        }
    }

    pub(super) fn refresh_rar5_recovery_records_from_services(&mut self) {
        if self.format != ArchiveFormat::Rar5 {
            return;
        }

        self.recovery_records
            .retain(|record| record.format != ArchiveFormat::Rar5);
        let records = Self::rar5_service_entry_recovery_records(&self.services);
        if !records.is_empty() {
            self.has_recovery_record = true;
            self.recovery_records.extend(records);
        }
    }

    fn rar5_locator_rr_service(
        reader: &mut Box<dyn ReadSeek>,
        offset: u64,
    ) -> RarResult<Option<crate::header::ParsedService>> {
        let current = reader.stream_position().map_err(RarError::Io)?;
        let mut parsed = None;

        if reader.seek(SeekFrom::Start(offset)).is_ok() {
            let raw_result = {
                let mut rr_reader = reader.as_mut();
                crate::header::common::read_raw_header(&mut rr_reader)
            };

            if let Ok(Some(raw)) = raw_result
                && raw.header_type == crate::header::common::HeaderType::Service
            {
                let data_offset =
                    raw.offset + 4 + raw.header_size_vint_len as u64 + raw.header_size;
                if let Ok(service) = crate::header::parse_service_from_raw(&raw, data_offset)
                    && service.header.service_name() == "RR"
                {
                    parsed = Some(service);
                }
            }
        }

        reader
            .seek(SeekFrom::Start(current))
            .map_err(RarError::Io)?;
        Ok(parsed)
    }

    fn rar4_recovery_records(
        records: &[crate::rar4::types::Rar4RecoveryRecord],
    ) -> Vec<RecoveryRecordInfo> {
        records
            .iter()
            .map(|record| RecoveryRecordInfo {
                format: ArchiveFormat::Rar4,
                kind: RecoveryRecordKind::Rar4Protect,
                offset: record.header_offset,
                data_offset: record.data_offset,
                data_size: record.data_size,
                protected_size: Some(record.header_offset),
                recovery_sectors: Some(record.recovery_sectors),
                total_blocks: Some(record.total_blocks),
                recovery_percent: None,
            })
            .collect()
    }

    pub(super) fn rar4_comment_to_service_entry(
        comment: &crate::rar4::types::Rar4CommentHeader,
        volume_index: usize,
        format: crate::types::ArchiveFormat,
    ) -> ServiceEntry {
        ServiceEntry {
            header_offset: comment.header_offset,
            file_header: FileHeader {
                name: "CMT".to_string(),
                name_raw: Some(b"CMT".to_vec()),
                unpacked_size: Some(comment.unpacked_size as u64),
                attributes: crate::types::FileAttributes(0),
                mtime: None,
                ctime: None,
                atime: None,
                data_crc32: None,
                data_hash: None,
                compression: crate::types::CompressionInfo {
                    format,
                    version: comment.unpack_version,
                    solid: false,
                    method: match comment.method {
                        crate::rar4::types::Rar4Method::Store => CompressionMethod::Store,
                        crate::rar4::types::Rar4Method::Fastest => CompressionMethod::Fastest,
                        crate::rar4::types::Rar4Method::Fast => CompressionMethod::Fast,
                        crate::rar4::types::Rar4Method::Normal => CompressionMethod::Normal,
                        crate::rar4::types::Rar4Method::Good => CompressionMethod::Good,
                        crate::rar4::types::Rar4Method::Best => CompressionMethod::Best,
                        crate::rar4::types::Rar4Method::Unknown(c) => CompressionMethod::Unknown(c),
                    },
                    dict_size: 0x10000,
                },
                host_os: crate::types::HostOs::Unknown(0),
                is_directory: false,
                file_flags: 0,
                data_size: comment.packed_size,
                split_before: false,
                split_after: false,
                data_offset: comment.data_offset,
                is_encrypted: false,
                version: None,
                service_subdata: None,
            },
            is_encrypted: false,
            file_encryption: None,
            hash: None,
            comment_crc16: (format != crate::types::ArchiveFormat::Rar14).then_some(comment.crc16),
            ntfs_stream_name: None,
            segments: vec![DataSegment::new(
                volume_index,
                comment.data_offset,
                comment.packed_size,
            )],
        }
    }

    pub(super) fn rar4_old_service_to_service_entry(
        service: &crate::rar4::types::Rar4OldServiceHeader,
        volume_index: usize,
        format: crate::types::ArchiveFormat,
    ) -> Option<ServiceEntry> {
        let (
            name,
            unpacked_size,
            unpack_version,
            method,
            crc32,
            ntfs_stream_name,
            service_subdata,
            host_os,
        ) = match &service.data {
            crate::rar4::types::Rar4OldServiceData::UnixOwner => (
                "UOW",
                service.data_size.min(u64::from(u32::MAX)) as u32,
                0,
                crate::rar4::types::Rar4Method::Store,
                None,
                None,
                None,
                crate::types::HostOs::Unix,
            ),
            crate::rar4::types::Rar4OldServiceData::NtAcl {
                unpacked_size,
                unpack_version,
                method,
                crc32,
            } => (
                "ACL",
                *unpacked_size,
                *unpack_version,
                *method,
                Some(*crc32),
                None,
                None,
                crate::types::HostOs::Windows,
            ),
            crate::rar4::types::Rar4OldServiceData::Stream {
                unpacked_size,
                unpack_version,
                method,
                crc32,
                stream_name,
                stream_name_raw,
            } => (
                "STM",
                *unpacked_size,
                *unpack_version,
                *method,
                Some(*crc32),
                Some(stream_name.clone()),
                Some(stream_name_raw.clone()),
                crate::types::HostOs::Windows,
            ),
            crate::rar4::types::Rar4OldServiceData::Unknown => return None,
        };

        let method = match method {
            crate::rar4::types::Rar4Method::Store => CompressionMethod::Store,
            crate::rar4::types::Rar4Method::Fastest => CompressionMethod::Fastest,
            crate::rar4::types::Rar4Method::Fast => CompressionMethod::Fast,
            crate::rar4::types::Rar4Method::Normal => CompressionMethod::Normal,
            crate::rar4::types::Rar4Method::Good => CompressionMethod::Good,
            crate::rar4::types::Rar4Method::Best => CompressionMethod::Best,
            crate::rar4::types::Rar4Method::Unknown(c) => CompressionMethod::Unknown(c),
        };

        Some(ServiceEntry {
            header_offset: service.header_offset,
            file_header: FileHeader {
                name: name.to_string(),
                name_raw: Some(name.as_bytes().to_vec()),
                unpacked_size: Some(u64::from(unpacked_size)),
                attributes: crate::types::FileAttributes(0),
                mtime: None,
                ctime: None,
                atime: None,
                data_crc32: crc32,
                data_hash: None,
                compression: crate::types::CompressionInfo {
                    format,
                    version: unpack_version,
                    solid: false,
                    method,
                    dict_size: 0x10000,
                },
                host_os,
                is_directory: false,
                file_flags: 0,
                data_size: service.data_size,
                split_before: false,
                split_after: false,
                data_offset: service.data_offset,
                is_encrypted: false,
                version: None,
                service_subdata,
            },
            is_encrypted: false,
            file_encryption: None,
            hash: None,
            comment_crc16: None,
            ntfs_stream_name,
            segments: vec![DataSegment::new(
                volume_index,
                service.data_offset,
                service.data_size,
            )],
        })
    }

    /// Convert a RAR4 file header to the unified FileHeader type.
    pub(super) fn rar4_to_file_header(
        fh: &crate::rar4::types::Rar4FileHeader,
        archive_solid: bool,
    ) -> FileHeader {
        FileHeader {
            name: fh.name.clone(),
            name_raw: fh.name_raw.clone(),
            unpacked_size: fh.unpacked_size,
            attributes: crate::types::FileAttributes(fh.attributes as u64),
            mtime: fh
                .mtime_precise
                .or_else(|| crate::rar4::dos_datetime_to_system_time(fh.mtime)),
            ctime: fh.ctime,
            atime: fh.atime,
            data_crc32: (!fh.is_rar14).then_some(fh.crc32),
            data_hash: Some(if fh.is_rar14 {
                crate::types::DataHash::Rar14(fh.crc32 as u16)
            } else {
                crate::types::DataHash::Crc32(fh.crc32)
            }),
            compression: crate::types::CompressionInfo {
                format: if fh.is_rar14 {
                    crate::types::ArchiveFormat::Rar14
                } else {
                    crate::types::ArchiveFormat::Rar4
                },
                version: fh.unpack_version,
                solid: Self::rar4_effective_solid(fh, archive_solid),
                method: match fh.method {
                    crate::rar4::types::Rar4Method::Store => CompressionMethod::Store,
                    crate::rar4::types::Rar4Method::Fastest => CompressionMethod::Fastest,
                    crate::rar4::types::Rar4Method::Fast => CompressionMethod::Fast,
                    crate::rar4::types::Rar4Method::Normal => CompressionMethod::Normal,
                    crate::rar4::types::Rar4Method::Good => CompressionMethod::Good,
                    crate::rar4::types::Rar4Method::Best => CompressionMethod::Best,
                    crate::rar4::types::Rar4Method::Unknown(c) => CompressionMethod::Unknown(c),
                },
                dict_size: fh.dict_size,
            },
            is_directory: fh.is_directory,
            host_os: match fh.host_os {
                crate::rar4::types::Rar4HostOs::Windows => crate::types::HostOs::Windows,
                crate::rar4::types::Rar4HostOs::Unix => crate::types::HostOs::Unix,
                crate::rar4::types::Rar4HostOs::MacOs => crate::types::HostOs::Darwin,
                // Scryer only needs Darwin/macOS, Linux/Unix, and Windows
                // behavior. Other RAR4 host ids are legacy archive origins,
                // not supported extraction targets, so keep them intentionally
                // unmapped.
                crate::rar4::types::Rar4HostOs::MsDos => crate::types::HostOs::Unknown(0),
                crate::rar4::types::Rar4HostOs::Os2 => crate::types::HostOs::Unknown(1),
                crate::rar4::types::Rar4HostOs::BeOs => crate::types::HostOs::Unknown(5),
                crate::rar4::types::Rar4HostOs::Unknown(v) => {
                    crate::types::HostOs::Unknown(v as u64)
                }
            },
            split_before: fh.split_before,
            split_after: fh.split_after,
            file_flags: u64::from(fh.flags),
            data_size: fh.packed_size,
            data_offset: fh.data_offset,
            is_encrypted: fh.is_encrypted,
            version: fh.version,
            service_subdata: fh.service_subdata.clone(),
        }
    }

    pub(super) fn rar4_effective_solid(
        fh: &crate::rar4::types::Rar4FileHeader,
        archive_solid: bool,
    ) -> bool {
        // UnRAR's extract path uses the archive-level solid flag for RAR 1.3
        // through RAR 1.5 because those formats do not reliably store a
        // per-file solid bit.
        fh.is_solid || (archive_solid && fh.unpack_version <= 15)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::header::file::FileHeader as Rar5FileHeader;
    use crate::header::{ParsedService, service::ServiceHeader};
    use crate::types::{CompressionInfo, CompressionMethod, FileAttributes, HostOs};

    fn service_header(name: &str, subdata: Option<Vec<u8>>) -> ServiceHeader {
        ServiceHeader {
            header_offset: 123,
            inner: Rar5FileHeader {
                name: name.to_string(),
                name_raw: Some(name.as_bytes().to_vec()),
                unpacked_size: Some(0),
                attributes: FileAttributes(0),
                mtime: None,
                ctime: None,
                atime: None,
                data_crc32: None,
                data_hash: None,
                compression: CompressionInfo {
                    format: ArchiveFormat::Rar5,
                    version: 0,
                    solid: false,
                    method: CompressionMethod::Store,
                    dict_size: 0,
                },
                host_os: HostOs::Unknown(0),
                is_directory: false,
                file_flags: 0,
                data_size: 4096,
                split_before: false,
                split_after: false,
                data_offset: 456,
                is_encrypted: false,
                version: None,
                service_subdata: subdata,
            },
        }
    }

    fn parsed_service(name: &str, subdata: Option<Vec<u8>>) -> ParsedService {
        ParsedService {
            header: service_header(name, subdata),
            is_encrypted: false,
            file_encryption: None,
            hash: None,
        }
    }

    #[test]
    fn rar5_rr_service_is_exposed_as_recovery_metadata() {
        let percent = crate::vint::encode_vint(250);
        let records = RarArchive::rar5_recovery_records(&[parsed_service("RR", Some(percent))]);

        assert_eq!(records.len(), 1);
        let record = &records[0];
        assert_eq!(record.format, ArchiveFormat::Rar5);
        assert_eq!(record.kind, RecoveryRecordKind::Rar5Service);
        assert_eq!(record.offset, 123);
        assert_eq!(record.data_offset, 456);
        assert_eq!(record.data_size, 4096);
        assert_eq!(record.protected_size, Some(123));
        assert_eq!(record.recovery_percent, Some(250));
    }

    #[test]
    fn non_rr_service_is_not_recovery_metadata() {
        assert!(RarArchive::rar5_recovery_records(&[parsed_service("CMT", None)]).is_empty());
    }

    #[test]
    fn rar4_protect_header_is_exposed_as_recovery_metadata() {
        let records =
            RarArchive::rar4_recovery_records(&[crate::rar4::types::Rar4RecoveryRecord {
                header_offset: 1000,
                data_offset: 1026,
                data_size: 8192,
                version: 3,
                recovery_sectors: 7,
                total_blocks: 99,
                mark: *b"Protect!",
            }]);

        assert_eq!(records.len(), 1);
        let record = &records[0];
        assert_eq!(record.format, ArchiveFormat::Rar4);
        assert_eq!(record.kind, RecoveryRecordKind::Rar4Protect);
        assert_eq!(record.offset, 1000);
        assert_eq!(record.protected_size, Some(1000));
        assert_eq!(record.recovery_sectors, Some(7));
        assert_eq!(record.total_blocks, Some(99));
    }

    #[test]
    fn rar4_old_stream_service_becomes_ntfs_stream_entry() {
        let service = crate::rar4::types::Rar4OldServiceHeader {
            header_offset: 100,
            data_offset: 200,
            data_size: 12,
            subtype: 2,
            level: 0,
            data: crate::rar4::types::Rar4OldServiceData::Stream {
                unpacked_size: 12,
                unpack_version: 20,
                method: crate::rar4::types::Rar4Method::Store,
                crc32: 0x1234_5678,
                stream_name: ":stream".to_string(),
                stream_name_raw: b":stream".to_vec(),
            },
        };

        let entry = RarArchive::rar4_old_service_to_service_entry(&service, 2, ArchiveFormat::Rar4)
            .expect("stream service should convert");

        assert_eq!(entry.file_header.name, "STM");
        assert_eq!(entry.file_header.data_crc32, Some(0x1234_5678));
        assert_eq!(entry.file_header.unpacked_size, Some(12));
        assert_eq!(entry.file_header.compression.version, 20);
        assert_eq!(
            entry.file_header.service_subdata.as_deref(),
            Some(&b":stream"[..])
        );
        assert_eq!(entry.ntfs_stream_name.as_deref(), Some(":stream"));
        assert_eq!(entry.segments[0].volume_index, 2);
        assert_eq!(entry.segments[0].data_offset, 200);
    }

    #[test]
    fn rar4_old_uowner_service_becomes_unix_owner_entry() {
        let service = crate::rar4::types::Rar4OldServiceHeader {
            header_offset: 100,
            data_offset: 200,
            data_size: 17,
            subtype: 0x101,
            level: 0,
            data: crate::rar4::types::Rar4OldServiceData::UnixOwner,
        };

        let entry = RarArchive::rar4_old_service_to_service_entry(&service, 2, ArchiveFormat::Rar4)
            .expect("UOW service should convert");

        assert_eq!(entry.file_header.name, "UOW");
        assert_eq!(entry.file_header.data_crc32, None);
        assert_eq!(entry.file_header.unpacked_size, Some(17));
        assert_eq!(entry.file_header.compression.version, 0);
        assert_eq!(
            entry.file_header.compression.method,
            CompressionMethod::Store
        );
        assert_eq!(entry.file_header.host_os, HostOs::Unix);
        assert_eq!(entry.file_header.service_subdata, None);
        assert_eq!(entry.ntfs_stream_name, None);
        assert_eq!(entry.segments[0].volume_index, 2);
        assert_eq!(entry.segments[0].data_offset, 200);
        assert_eq!(entry.segments[0].data_size, 17);
    }

    #[test]
    fn rar4_old_acl_service_becomes_ntfs_acl_entry() {
        let service = crate::rar4::types::Rar4OldServiceHeader {
            header_offset: 100,
            data_offset: 200,
            data_size: 12,
            subtype: 1,
            level: 0,
            data: crate::rar4::types::Rar4OldServiceData::NtAcl {
                unpacked_size: 34,
                unpack_version: 20,
                method: crate::rar4::types::Rar4Method::Normal,
                crc32: 0x1234_5678,
            },
        };

        let entry = RarArchive::rar4_old_service_to_service_entry(&service, 2, ArchiveFormat::Rar4)
            .expect("ACL service should convert");

        assert_eq!(entry.file_header.name, "ACL");
        assert_eq!(entry.file_header.data_crc32, Some(0x1234_5678));
        assert_eq!(entry.file_header.unpacked_size, Some(34));
        assert_eq!(entry.file_header.compression.version, 20);
        assert_eq!(
            entry.file_header.compression.method,
            CompressionMethod::Normal
        );
        assert_eq!(entry.file_header.host_os, HostOs::Windows);
        assert_eq!(entry.file_header.service_subdata, None);
        assert_eq!(entry.ntfs_stream_name, None);
        assert_eq!(entry.segments[0].volume_index, 2);
        assert_eq!(entry.segments[0].data_offset, 200);
    }

    #[test]
    fn rar4_file_header_conversion_preserves_times_and_version() {
        let mtime = std::time::UNIX_EPOCH + std::time::Duration::from_secs(10);
        let ctime = std::time::UNIX_EPOCH + std::time::Duration::from_secs(20);
        let atime = std::time::UNIX_EPOCH + std::time::Duration::from_secs(30);
        let fh = crate::rar4::types::Rar4FileHeader {
            is_rar14: false,
            flags: crate::rar4::types::file_flags::VERSION,
            packed_size: 0,
            unpacked_size: Some(0),
            host_os: crate::rar4::types::Rar4HostOs::Unix,
            crc32: 0,
            mtime: 0,
            mtime_precise: Some(mtime),
            ctime: Some(ctime),
            atime: Some(atime),
            version: Some(42),
            unpack_version: 29,
            method: crate::rar4::types::Rar4Method::Store,
            dict_size: 0,
            name: "report.txt;42".to_string(),
            name_raw: Some(b"report.txt;42".to_vec()),
            is_directory: false,
            is_unix_symlink: false,
            is_encrypted: false,
            encryption_method: None,
            is_solid: false,
            is_subblock: false,
            split_before: false,
            split_after: false,
            comment_in_header: false,
            data_offset: 0,
            salt: None,
            attributes: 0,
            service_subdata: None,
            owner: None,
        };

        let converted = RarArchive::rar4_to_file_header(&fh, false);
        assert_eq!(converted.name_raw.as_deref(), Some(&b"report.txt;42"[..]));
        assert_eq!(converted.mtime, Some(mtime));
        assert_eq!(converted.ctime, Some(ctime));
        assert_eq!(converted.atime, Some(atime));
        assert_eq!(converted.version, Some(42));
        assert_eq!(
            converted.file_flags,
            u64::from(crate::rar4::types::file_flags::VERSION)
        );
    }

    #[test]
    fn rar4_file_header_conversion_host_system_matches_unrar() {
        let mut fh = crate::rar4::types::Rar4FileHeader {
            is_rar14: false,
            flags: 0,
            packed_size: 0,
            unpacked_size: Some(0),
            host_os: crate::rar4::types::Rar4HostOs::Unix,
            crc32: 0,
            mtime: 0,
            mtime_precise: None,
            ctime: None,
            atime: None,
            version: None,
            unpack_version: 29,
            method: crate::rar4::types::Rar4Method::Store,
            dict_size: 0,
            name: "host.txt".to_string(),
            name_raw: Some(b"host.txt".to_vec()),
            is_directory: false,
            is_unix_symlink: false,
            is_encrypted: false,
            encryption_method: None,
            is_solid: false,
            is_subblock: false,
            split_before: false,
            split_after: false,
            comment_in_header: false,
            data_offset: 0,
            salt: None,
            attributes: 0,
            service_subdata: None,
            owner: None,
        };

        fh.host_os = crate::rar4::types::Rar4HostOs::Windows;
        assert_eq!(
            RarArchive::rar4_to_file_header(&fh, false).host_os,
            HostOs::Windows
        );

        fh.host_os = crate::rar4::types::Rar4HostOs::Unix;
        assert_eq!(
            RarArchive::rar4_to_file_header(&fh, false).host_os,
            HostOs::Unix
        );

        fh.host_os = crate::rar4::types::Rar4HostOs::MacOs;
        assert_eq!(
            RarArchive::rar4_to_file_header(&fh, false).host_os,
            HostOs::Darwin
        );

        for (host, expected) in [
            (crate::rar4::types::Rar4HostOs::MsDos, 0),
            (crate::rar4::types::Rar4HostOs::Os2, 1),
            (crate::rar4::types::Rar4HostOs::BeOs, 5),
        ] {
            fh.host_os = host;
            assert_eq!(
                RarArchive::rar4_to_file_header(&fh, false).host_os,
                HostOs::Unknown(expected)
            );
        }

        fh.host_os = crate::rar4::types::Rar4HostOs::Unknown(42);
        assert_eq!(
            RarArchive::rar4_to_file_header(&fh, false).host_os,
            HostOs::Unknown(42)
        );
    }

    #[test]
    fn rar15_effective_solid_uses_archive_flag_like_unrar() {
        let mut fh = crate::rar4::types::Rar4FileHeader {
            is_rar14: false,
            flags: 0,
            packed_size: 0,
            unpacked_size: Some(0),
            host_os: crate::rar4::types::Rar4HostOs::Unix,
            crc32: 0,
            mtime: 0,
            mtime_precise: None,
            ctime: None,
            atime: None,
            version: None,
            unpack_version: 15,
            method: crate::rar4::types::Rar4Method::Normal,
            dict_size: 0x10000,
            name: "old-solid.txt".to_string(),
            name_raw: Some(b"old-solid.txt".to_vec()),
            is_directory: false,
            is_unix_symlink: false,
            is_encrypted: false,
            encryption_method: None,
            is_solid: false,
            is_subblock: false,
            split_before: false,
            split_after: false,
            comment_in_header: false,
            data_offset: 0,
            salt: None,
            attributes: 0,
            service_subdata: None,
            owner: None,
        };

        assert!(RarArchive::rar4_to_file_header(&fh, true).compression.solid);
        assert!(
            !RarArchive::rar4_to_file_header(&fh, false)
                .compression
                .solid
        );

        fh.unpack_version = 20;
        assert!(!RarArchive::rar4_to_file_header(&fh, true).compression.solid);

        fh.unpack_version = 29;
        fh.is_solid = true;
        assert!(
            RarArchive::rar4_to_file_header(&fh, false)
                .compression
                .solid
        );
    }

    fn build_rar14_store_archive(name: &str, data: &[u8], checksum: Option<u16>) -> Vec<u8> {
        build_rar14_store_archive_ex(name, data, checksum, 0, 2)
    }

    fn build_rar14_store_archive_ex(
        name: &str,
        data: &[u8],
        checksum: Option<u16>,
        flags: u8,
        unpack_marker: u8,
    ) -> Vec<u8> {
        let checksum = checksum.unwrap_or_else(|| crate::rar4::header::checksum14_update(0, data));
        let name_bytes = name.as_bytes();
        assert!(name_bytes.len() <= u8::MAX as usize);
        let head_size = 21u16 + name_bytes.len() as u16;

        let mut out = Vec::new();
        out.extend_from_slice(&crate::signature::RAR14_SIGNATURE);
        out.extend_from_slice(&7u16.to_le_bytes());
        out.push(0); // main flags

        out.extend_from_slice(&(data.len() as u32).to_le_bytes());
        out.extend_from_slice(&(data.len() as u32).to_le_bytes());
        out.extend_from_slice(&checksum.to_le_bytes());
        out.extend_from_slice(&head_size.to_le_bytes());
        out.extend_from_slice(&0u32.to_le_bytes()); // DOS mtime
        out.push(0); // attrs
        out.push(flags);
        out.push(unpack_marker);
        out.push(name_bytes.len() as u8);
        out.push(0x30); // Store
        out.extend_from_slice(name_bytes);
        out.extend_from_slice(data);
        out
    }

    fn rar13_encrypt(password: &str, data: &[u8]) -> Vec<u8> {
        let mut key = [0u8; 3];
        for &byte in password.as_bytes() {
            key[0] = key[0].wrapping_add(byte);
            key[1] ^= byte;
            key[2] = key[2].wrapping_add(byte).rotate_left(1);
        }

        let mut encrypted = Vec::with_capacity(data.len());
        for &byte in data {
            key[1] = key[1].wrapping_add(key[2]);
            key[0] = key[0].wrapping_add(key[1]);
            encrypted.push(byte.wrapping_add(key[0]));
        }
        encrypted
    }

    fn build_rar14_archive_with_raw_comment(comment: &[u8]) -> Vec<u8> {
        assert!(comment.len() <= u16::MAX as usize);
        let head_size = 7u16 + 2 + comment.len() as u16;

        let mut out = Vec::new();
        out.extend_from_slice(&crate::signature::RAR14_SIGNATURE);
        out.extend_from_slice(&head_size.to_le_bytes());
        out.push(crate::rar4::types::archive_flags::COMMENT as u8);
        out.extend_from_slice(&(comment.len() as u16).to_le_bytes());
        out.extend_from_slice(comment);
        out
    }

    #[test]
    fn rar14_raw_archive_comment_is_exposed() {
        let bytes = build_rar14_archive_with_raw_comment(b"hello from rar14");
        let mut archive = RarArchive::open(std::io::Cursor::new(bytes)).unwrap();

        assert_eq!(
            archive.comment().unwrap().as_deref(),
            Some("hello from rar14")
        );
    }

    #[test]
    fn rar14_store_archive_opens_and_extracts_with_checksum14() {
        let bytes = build_rar14_store_archive("HELLO.TXT", b"hello rar14", None);
        let mut archive = RarArchive::open(std::io::Cursor::new(bytes)).unwrap();
        let metadata = archive.metadata();

        assert_eq!(metadata.format, ArchiveFormat::Rar14);
        assert_eq!(metadata.members.len(), 1);
        assert_eq!(metadata.members[0].name, "HELLO.TXT");
        assert_eq!(metadata.members[0].crc32, None);
        assert!(matches!(
            archive.members[0].file_header.data_hash,
            Some(crate::types::DataHash::Rar14(_))
        ));

        let extracted = archive
            .extract_member(0, &crate::extract::ExtractOptions::default(), None)
            .unwrap()
            .to_bytes()
            .unwrap();
        assert_eq!(extracted, b"hello rar14");
    }

    #[test]
    fn rar14_encrypted_store_uses_rar13_even_for_version10_marker() {
        let plaintext = b"rar14 encrypted store";
        let encrypted = rar13_encrypt("secret", plaintext);
        let checksum = crate::rar4::header::checksum14_update(0, plaintext);
        let bytes = build_rar14_store_archive_ex(
            "CRYPT.TXT",
            &encrypted,
            Some(checksum),
            crate::rar4::types::file_flags::ENCRYPTED as u8,
            0,
        );
        let mut archive =
            RarArchive::open_with_password(std::io::Cursor::new(bytes), "secret").unwrap();

        assert_eq!(archive.members[0].file_header.compression.version, 10);
        assert!(archive.members[0].file_header.is_encrypted);

        let extracted = archive
            .extract_member(0, &crate::extract::ExtractOptions::default(), None)
            .unwrap()
            .to_bytes()
            .unwrap();
        assert_eq!(extracted, plaintext);
    }

    #[test]
    fn rar14_store_archive_rejects_checksum14_mismatch() {
        let bytes = build_rar14_store_archive("BAD.TXT", b"hello rar14", Some(0));
        let mut archive = RarArchive::open(std::io::Cursor::new(bytes)).unwrap();
        let err = archive
            .extract_member(0, &crate::extract::ExtractOptions::default(), None)
            .unwrap_err();

        assert!(matches!(err, RarError::DataCrcMismatch { .. }));
    }
}
