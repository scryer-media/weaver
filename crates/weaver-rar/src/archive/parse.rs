use super::*;

impl RarArchive {
    pub(super) fn open_boxed_inner(
        mut reader: Box<dyn ReadSeek>,
        password: Option<&str>,
    ) -> RarResult<Self> {
        // Seek to start
        reader.seek(SeekFrom::Start(0)).map_err(RarError::Io)?;

        // Read and validate signature
        let format = signature::read_signature(&mut reader)?;
        tracing::debug!("detected format: {:?}", format);

        // Dispatch based on format.
        if format == ArchiveFormat::Rar4 {
            return Self::open_rar4(reader, password);
        }

        // Parse all headers (RAR5)
        let parsed = header::parse_all_headers(&mut reader, password)?;

        let is_solid = parsed.main.as_ref().is_some_and(|m| m.is_solid);

        let is_volume = parsed.main.as_ref().is_some_and(|m| m.is_volume);

        let volume_number = parsed
            .main
            .as_ref()
            .and_then(|m| m.volume_number)
            .unwrap_or(0) as usize;

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

        let end_reached = parsed.end.is_some();

        // Build member entries from file headers.
        // For the first volume, each file header that doesn't have split_before
        // starts a new member. Files with split_before are continuations from
        // a previous volume (we'll reconcile when that volume is added).
        let mut members = Vec::new();

        for pf in parsed.files {
            let segment = DataSegment {
                volume_index: volume_number,
                data_offset: pf.header.data_offset,
                data_size: pf.header.data_size,
            };
            let mut file_header = pf.header;
            file_header.is_encrypted = pf.is_encrypted;
            members.push(MemberEntry {
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
            });
        }

        // Store the volume reader.
        let mut volumes: Vec<Option<VolumeData>> = Vec::new();
        while volumes.len() <= volume_number {
            volumes.push(None);
        }
        volumes[volume_number] = Some(VolumeData {
            reader,
            index: volume_number,
        });

        Ok(RarArchive {
            format,
            is_solid,
            is_encrypted: parsed.is_encrypted,
            volume_set,
            members,
            end_reached,
            more_volumes,
            volumes,
            solid_decoder: None,
            solid_decoder_rar4: None,
            solid_next_index: 0,
            limits: Limits::default(),
            password: password.map(String::from),
        })
    }

    /// Open a RAR4 archive.
    pub(super) fn open_rar4(
        mut reader: Box<dyn ReadSeek>,
        password: Option<&str>,
    ) -> RarResult<Self> {
        let parsed = crate::rar4::parse_rar4_headers(&mut reader)?;

        let is_solid = parsed.archive_header.is_solid;
        let is_volume = parsed.archive_header.is_volume;

        let mut volume_set = if is_volume {
            let mut vs = VolumeSet::new();
            vs.add_volume(0);
            vs
        } else {
            VolumeSet::single()
        };

        let more_volumes = parsed.end.as_ref().is_some_and(|e| e.more_volumes);
        let end_reached = parsed.end.is_some();

        if !more_volumes {
            volume_set.set_last_volume_seen();
        }

        let mut members = Vec::new();
        for fh in &parsed.files {
            let segment = DataSegment {
                volume_index: 0,
                data_offset: fh.data_offset,
                data_size: fh.packed_size,
            };
            members.push(MemberEntry {
                file_header: Self::rar4_to_file_header(fh),
                is_encrypted: fh.is_encrypted,
                file_encryption: None,
                rar4_salt: fh.salt,
                hash: None,
                redirection: None,
                segments: vec![segment],
            });
        }

        let volumes: Vec<Option<VolumeData>> = vec![Some(VolumeData { reader, index: 0 })];

        Ok(RarArchive {
            format: ArchiveFormat::Rar4,
            is_solid,
            is_encrypted: parsed.archive_header.is_encrypted,
            volume_set,
            members,
            end_reached,
            more_volumes,
            volumes,
            solid_decoder: None,
            solid_decoder_rar4: None,
            solid_next_index: 0,
            limits: Limits::default(),
            password: password.map(String::from),
        })
    }

    /// Convert a RAR4 file header to the unified FileHeader type.
    pub(super) fn rar4_to_file_header(fh: &crate::rar4::types::Rar4FileHeader) -> FileHeader {
        FileHeader {
            name: fh.name.clone(),
            unpacked_size: Some(fh.unpacked_size),
            data_crc32: Some(fh.crc32),
            data_offset: fh.data_offset,
            data_size: fh.packed_size,
            compression: crate::types::CompressionInfo {
                format: crate::types::ArchiveFormat::Rar4,
                version: 29,
                solid: fh.is_solid,
                method: match fh.method {
                    crate::rar4::types::Rar4Method::Store => CompressionMethod::Store,
                    crate::rar4::types::Rar4Method::Fastest => CompressionMethod::Fastest,
                    crate::rar4::types::Rar4Method::Fast => CompressionMethod::Fast,
                    crate::rar4::types::Rar4Method::Normal => CompressionMethod::Normal,
                    crate::rar4::types::Rar4Method::Good => CompressionMethod::Good,
                    crate::rar4::types::Rar4Method::Best => CompressionMethod::Best,
                    crate::rar4::types::Rar4Method::Unknown(c) => CompressionMethod::Unknown(c),
                },
                dict_size: 4 * 1024 * 1024,
            },
            is_directory: fh.is_directory,
            host_os: match fh.host_os {
                crate::rar4::types::Rar4HostOs::Windows
                | crate::rar4::types::Rar4HostOs::MsDos
                | crate::rar4::types::Rar4HostOs::Os2 => crate::types::HostOs::Windows,
                crate::rar4::types::Rar4HostOs::Unix
                | crate::rar4::types::Rar4HostOs::MacOs
                | crate::rar4::types::Rar4HostOs::BeOs => crate::types::HostOs::Unix,
                crate::rar4::types::Rar4HostOs::Unknown(v) => {
                    crate::types::HostOs::Unknown(v as u64)
                }
            },
            attributes: crate::types::FileAttributes(fh.attributes as u64),
            mtime: None,
            split_before: fh.split_before,
            split_after: fh.split_after,
            file_flags: 0,
            is_encrypted: fh.is_encrypted,
        }
    }
}
