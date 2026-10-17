// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The container's map, from its parsed header: which folders are stored
// bytes, plain or through AES, where each lies, and which members their
// bytes are. A folder whose coders do anything but copy or decipher is not
// streamable, and one is enough to hand the whole container to an engine.

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sevenz_turbo::{AesProperties, Archive, EncoderMethod};
use weaver_graph::{ByteRange, MemberId};

use super::header::SIGNATURE;
use crate::crypt::BLOCK;

// Why the map cannot be built.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Shape {
    // A folder's coders compress or filter.
    Compressed,
    // Stored, but in a layout this object does not stream.
    Unsupported,
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct Folder {
    // Where its one pack stream lies in the container.
    pub(super) pack: ByteRange,
    // How many bytes its files hold between them.
    pub(super) plain_len: u64,
    #[serde(with = "aes")]
    pub(super) aes: Option<AesProperties>,
    // Its streamed entries, by index, in offset order.
    pub(super) entries: Vec<usize>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct Entry {
    pub(super) name: String,
    // `None` for an entry the object skips: an anti-item or a link. A skipped
    // entry with a body still takes its bytes in its folder.
    pub(super) member: Option<MemberId>,
    pub(super) length: u64,
    // Its folder and where in that folder's plaintext it starts.
    pub(super) folder: Option<usize>,
    pub(super) offset: u64,
    pub(super) crc: Option<u32>,
}

impl Entry {
    pub(super) fn span(&self) -> ByteRange {
        ByteRange::at(self.offset, self.length)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct Map {
    pub(super) folders: Vec<Folder>,
    pub(super) entries: Vec<Entry>,
    // Entry index per member id.
    pub(super) members: Vec<usize>,
}

impl Map {
    // Whether every index it holds lands inside it, as a restored map's must.
    pub(super) fn is_sound(&self) -> bool {
        let entries = self.entries.len();
        self.members.iter().all(|entry| *entry < entries)
            && self
                .folders
                .iter()
                .all(|folder| folder.entries.iter().all(|entry| *entry < entries))
            && self.entries.iter().all(|entry| {
                entry
                    .folder
                    .is_none_or(|folder| folder < self.folders.len())
            })
    }
}

// A folder's AES properties as the coder's own property bytes, which is all
// they are: a work factor, a salt and an IV, read back by the same parse.
mod aes {
    use super::*;

    pub(super) fn serialize<S: Serializer>(
        aes: &Option<AesProperties>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        aes.as_ref().map(encode).serialize(serializer)
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<AesProperties>, D::Error> {
        Option::<Vec<u8>>::deserialize(deserializer)?
            .map(|bytes| AesProperties::parse(&bytes).map_err(serde::de::Error::custom))
            .transpose()
    }

    // The first byte holds the work factor and the high bits of the salt
    // and IV sizes, the second the rest of them, less one each.
    fn encode(aes: &AesProperties) -> Vec<u8> {
        let salt = aes.salt();
        let mut bytes = match salt.len() {
            0 => vec![aes.cycles_power() | 0x40, 0x0f],
            len => vec![aes.cycles_power() | 0xc0, ((len as u8 - 1) << 4) | 0x0f],
        };
        bytes.extend_from_slice(salt);
        bytes.extend_from_slice(aes.iv());
        bytes
    }
}

fn copies(id: &[u8]) -> bool {
    id == EncoderMethod::ID_COPY
}

fn deciphers(id: &[u8]) -> bool {
    id == EncoderMethod::ID_AES256_SHA256
}

pub(super) fn build(archive: &Archive) -> Result<Map, Shape> {
    // Any folder that compresses decides for the whole container, whatever
    // else is wrong with the others.
    let compressed = archive.blocks.iter().any(|block| {
        block.coders.iter().any(|coder| {
            !copies(coder.encoder_method_id()) && !deciphers(coder.encoder_method_id())
        })
    });
    if compressed {
        return Err(Shape::Compressed);
    }
    let firsts = archive.stream_map.block_first_pack_stream_index();
    let offsets = archive.stream_map.pack_stream_offsets();
    let sizes = archive.pack_sizes();
    let mut folders = Vec::with_capacity(archive.blocks.len());
    for (index, block) in archive.blocks.iter().enumerate() {
        let first = *firsts.get(index).ok_or(Shape::Unsupported)?;
        let next = firsts.get(index + 1).copied().unwrap_or(sizes.len());
        if next != first + 1 {
            return Err(Shape::Unsupported);
        }
        let mut aes = None;
        for coder in &block.coders {
            if coder.num_input_streams() != 1 || coder.num_output_streams() != 1 {
                return Err(Shape::Unsupported);
            }
            if let Some(properties) = coder.aes_properties().map_err(|_| Shape::Unsupported)? {
                if aes.is_some() {
                    return Err(Shape::Unsupported);
                }
                aes = Some(properties);
            }
        }
        let size = *sizes.get(first).ok_or(Shape::Unsupported)?;
        let offset = *offsets.get(first).ok_or(Shape::Unsupported)?;
        let plain_len = block.get_unpack_size();
        let expected = match aes {
            Some(_) => plain_len.div_ceil(BLOCK) * BLOCK,
            None => plain_len,
        };
        if size != expected {
            return Err(Shape::Unsupported);
        }
        let start = SIGNATURE
            .end
            .checked_add(archive.pack_pos())
            .and_then(|at| at.checked_add(offset))
            .ok_or(Shape::Unsupported)?;
        let end = start.checked_add(size).ok_or(Shape::Unsupported)?;
        folders.push(Folder {
            pack: ByteRange::new(start, end),
            plain_len,
            aes,
            entries: Vec::new(),
        });
    }
    let mut cursors = vec![0u64; folders.len()];
    let mut entries = Vec::with_capacity(archive.files.len());
    let mut members = Vec::new();
    for (index, file) in archive.files.iter().enumerate() {
        let skipped = file.is_anti_item() || file.is_symlink();
        let mut entry = Entry {
            name: file.name().to_owned(),
            member: None,
            length: if file.has_stream() { file.size() } else { 0 },
            folder: None,
            offset: 0,
            crc: (file.has_stream() && file.has_crc).then_some(file.crc as u32),
        };
        if file.has_stream() {
            let folder = archive
                .stream_map
                .file_block_index
                .get(index)
                .copied()
                .flatten()
                .ok_or(Shape::Unsupported)?;
            let cursor = cursors.get_mut(folder).ok_or(Shape::Unsupported)?;
            entry.folder = Some(folder);
            entry.offset = *cursor;
            *cursor = cursor.checked_add(entry.length).ok_or(Shape::Unsupported)?;
            folders[folder].entries.push(index);
        }
        if file.is_directory() && !entry.name.ends_with(['/', '\\']) {
            entry.name.push('/');
        }
        if !skipped {
            entry.member = Some(MemberId(members.len() as u32));
            members.push(index);
        }
        entries.push(entry);
    }
    // Every folder's bytes are its files', end to end.
    if folders
        .iter()
        .zip(&cursors)
        .any(|(folder, cursor)| folder.plain_len != *cursor)
    {
        return Err(Shape::Unsupported);
    }
    // Lookups by container offset walk the packs in order: they must follow
    // one another without overlap.
    if folders
        .windows(2)
        .any(|pair| pair[0].pack.end > pair[1].pack.start)
    {
        return Err(Shape::Unsupported);
    }
    Ok(Map {
        folders,
        entries,
        members,
    })
}

impl Map {
    pub(super) fn entry(&self, member: MemberId) -> Option<&Entry> {
        self.members
            .get(member.0 as usize)
            .map(|index| &self.entries[*index])
    }

    // The folders whose pack streams overlap a container range, in order.
    pub(super) fn folders_in(&self, range: ByteRange) -> impl Iterator<Item = (usize, &Folder)> {
        let first = self
            .folders
            .partition_point(|folder| folder.pack.end <= range.start);
        self.folders[first..]
            .iter()
            .enumerate()
            .map(move |(at, folder)| (first + at, folder))
            .take_while(move |(_, folder)| folder.pack.start < range.end)
    }

    // The streamed entries of a folder that overlap a range of its
    // plaintext, in order.
    pub(super) fn entries_in(
        &self,
        folder: usize,
        range: ByteRange,
    ) -> impl Iterator<Item = (usize, &Entry)> {
        let indices = &self.folders[folder].entries;
        let first = indices.partition_point(|index| self.entries[*index].span().end <= range.start);
        indices[first..]
            .iter()
            .map(|index| (*index, &self.entries[*index]))
            .take_while(move |(_, entry)| entry.offset < range.end)
            .filter(|(_, entry)| entry.length > 0)
    }

    // The container range a member's range lies in, before any rounding to
    // cipher blocks.
    pub(super) fn container(
        &self,
        member: MemberId,
        range: ByteRange,
    ) -> Option<(usize, ByteRange)> {
        let entry = self.entry(member)?;
        let folder = entry.folder?;
        let range = range.intersect(ByteRange::new(0, entry.length))?;
        let start = self.folders[folder].pack.start + entry.offset;
        Some((folder, range.shift(start as i64)))
    }
}
