// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Spike: the run for a set that stores some folders and codes others. The
// store streams the stored folders' members and keeps the coded folders'
// packs and the headers in its envelope, at their container offsets; this
// run reads that envelope as the container and decodes the coded folders
// alone, folder by folder, so no byte of a stored folder is ever read. Its
// members are the store's, numbered as the store numbers them.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use sevenz_turbo::{Archive, ArchiveReader, Error, Password, ReadAtCursor};
use weaver_graph::{Budget, Emit, Extraction, MemberId, Provenance, Sink, Volumes};

use super::map::Map;
use crate::engine::Joined;

const CHUNK: usize = 256 << 10;

pub(crate) struct CodedRun {
    password: Option<String>,
    // The coded folders, by index.
    folders: Vec<usize>,
    // Each coded member, by name.
    members: HashMap<String, MemberId>,
}

impl CodedRun {
    pub(super) fn new(map: &Map, password: Option<String>) -> Self {
        let folders: Vec<usize> = map.coded().collect();
        let members = folders
            .iter()
            .flat_map(|folder| map.folders[*folder].entries.iter())
            .map(|index| (map.entries[*index].name.clone(), MemberId(*index as u32)))
            .collect();
        Self {
            password,
            folders,
            members,
        }
    }
}

impl Extraction for CodedRun {
    fn run(self: Box<Self>, volumes: Arc<dyn Volumes>, sink: &mut Sink<'_>, budget: Budget) {
        let input = Arc::new(Joined::new(volumes));
        let password = self
            .password
            .as_deref()
            .map_or_else(Password::empty, Password::new);
        let limits = super::limits(&budget);
        let walked = (|| -> Result<(), Error> {
            let mut cursor = ReadAtCursor::new(Arc::clone(&input));
            let archive = Archive::read_with_limits(&mut cursor, &password, &limits)?;
            let mut reader =
                ArchiveReader::from_archive_with_limits(archive, cursor, password, limits)?;
            reader.set_positional_source(Arc::clone(&input));
            reader.set_verify_checksums(true);
            let mut buf = vec![0u8; CHUNK];
            for &folder in &self.folders {
                let mut stopped = false;
                reader
                    .block_decoder(folder)?
                    .for_each_entries(&mut |entry, read| {
                        let Some(&member) = self.members.get(entry.name()) else {
                            return Ok(true);
                        };
                        let mut offset = 0u64;
                        loop {
                            let got = read.read(&mut buf)?;
                            if got == 0 {
                                break;
                            }
                            if !sink(Emit::Bytes {
                                member,
                                offset,
                                bytes: Bytes::copy_from_slice(&buf[..got]).into(),
                                provenance: Provenance::Fresh,
                            }) {
                                stopped = true;
                                return Ok(false);
                            }
                            offset += got as u64;
                        }
                        if !sink(Emit::MemberClosed(member)) {
                            stopped = true;
                            return Ok(false);
                        }
                        Ok(true)
                    })?;
                if stopped {
                    return Ok(());
                }
            }
            Ok(())
        })();
        if let Err(error) = walked {
            if sink(Emit::Failed(error.to_string())) {
                sink(crate::verdict::sevenz(&error));
            }
        }
    }
}
