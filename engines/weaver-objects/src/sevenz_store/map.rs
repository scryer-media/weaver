// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The container's map, from its parsed header: which folders are stored
// bytes, plain or through AES, where each lies, and which members their
// bytes are, or why it cannot stream: the first refusal, in the order the
// header states folders and then files.

use std::collections::HashSet;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sevenz_turbo::{AesProperties, Archive, Block, EncoderMethod};
use weaver_graph::{ByteRange, MemberId, Why};

use super::header::SIGNATURE;
use crate::crypt::BLOCK;

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

// Every entry is a member, numbered by its index.
#[derive(Debug, Serialize, Deserialize)]
pub(super) struct Map {
    pub(super) folders: Vec<Folder>,
    pub(super) entries: Vec<Entry>,
}

impl Map {
    // Whether every index it holds lands inside it, as a restored map's must.
    pub(super) fn is_sound(&self) -> bool {
        let entries = self.entries.len();
        self.folders
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

const COPY: &[u8] = EncoderMethod::ID_COPY;
const AES: &[u8] = EncoderMethod::ID_AES256_SHA256;

// Why a folder whose coders are not exactly a copy, or a copy and AES, does
// not stream: AES among them is enciphered bytes an engine with the password
// reads, and anything else is bytes that are not their output bytes.
fn coder_refusal(block: &Block) -> Why {
    if block
        .coders
        .iter()
        .any(|coder| coder.encoder_method_id() == AES)
    {
        Why::Encrypted
    } else {
        Why::Compressed
    }
}

fn align(len: u64) -> Result<u64, Why> {
    len.checked_next_multiple_of(BLOCK).ok_or(Why::Unsupported)
}

// The whole decision is taken over the set: folders are the unit of coding
// and several entries share one, so a container with anything but stored
// folders goes to an engine whole, with its bytes still in hand.
pub(super) fn build(archive: &Archive) -> Result<Map, Why> {
    let firsts = archive.stream_map.block_first_pack_stream_index();
    let offsets = archive.stream_map.pack_stream_offsets();
    let sizes = archive.pack_sizes();
    let mut member_counts = vec![0usize; archive.blocks.len()];
    for (file, block) in archive
        .files
        .iter()
        .zip(&archive.stream_map.file_block_index)
    {
        if file.has_stream() {
            *block
                .and_then(|index| member_counts.get_mut(index))
                .ok_or(Why::Unsupported)? += 1;
        }
    }
    let mut folders = Vec::with_capacity(archive.blocks.len());
    for (index, block) in archive.blocks.iter().enumerate() {
        if block
            .coders
            .iter()
            .any(|coder| coder.num_input_streams() != 1 || coder.num_output_streams() != 1)
        {
            return Err(coder_refusal(block));
        }
        let methods: Vec<_> = block
            .ordered_coder_iter()
            .map(|(_, coder)| coder.encoder_method_id())
            .collect();
        let aes = if methods == [COPY] {
            None
        } else if methods == [AES, COPY] || methods == [COPY, AES] {
            // One member to a folder: its CBC chain is that member's alone.
            if member_counts[index] != 1 {
                return Err(Why::Unsupported);
            }
            let coder = block
                .coders
                .iter()
                .find(|coder| coder.encoder_method_id() == AES)
                .ok_or(Why::Unsupported)?;
            Some(
                coder
                    .aes_properties()
                    .map_err(|_| Why::Unsupported)?
                    .ok_or(Why::Unsupported)?,
            )
        } else {
            return Err(coder_refusal(block));
        };
        // Exactly one pack stream. Nothing in the format says several are
        // adjacent, and a member placed across a gap that is not there is
        // silent corruption rather than a failed extraction.
        let first = *firsts.get(index).ok_or(Why::Unsupported)?;
        let next = firsts.get(index + 1).copied().unwrap_or(sizes.len());
        if next != first + 1 {
            return Err(Why::Unsupported);
        }
        // Each coder's input is the output of the one before it, from the packed
        // size to the unpacked one: Copy is the identity, AES pads to a block.
        let size = *sizes.get(first).ok_or(Why::Unsupported)?;
        let plain_len = block.get_unpack_size();
        let mut input = size;
        for (_, coder) in block.ordered_coder_iter() {
            let output = block.get_unpack_size_for_coder(coder);
            let required = match coder.encoder_method_id() {
                COPY => output,
                _ => align(output)?,
            };
            if input != required {
                return Err(Why::Unsupported);
            }
            input = output;
        }
        if input != plain_len {
            return Err(Why::Unsupported);
        }
        let offset = *offsets.get(first).ok_or(Why::Unsupported)?;
        let start = SIGNATURE
            .end
            .checked_add(archive.pack_pos())
            .and_then(|at| at.checked_add(offset))
            .ok_or(Why::Unsupported)?;
        let end = start.checked_add(size).ok_or(Why::Unsupported)?;
        folders.push(Folder {
            pack: ByteRange::new(start, end),
            plain_len,
            aes,
            entries: Vec::new(),
        });
    }
    // Each folder's running cursor: the entries sharing one are laid out end
    // to end in its plaintext, in file order.
    let mut cursors = vec![0u64; folders.len()];
    let mut entries = Vec::with_capacity(archive.files.len());
    let mut seen = HashSet::with_capacity(archive.files.len());
    for (index, file) in archive.files.iter().enumerate() {
        if file.is_anti_item() || file.is_symlink() {
            return Err(Why::NotAFile);
        }
        // The reader's own name check, before a name reaches a destination
        // that would have to guess what the writer meant.
        if file.unsafe_path_reason().is_some() {
            return Err(Why::Unsupported);
        }
        // Two entries claiming one name resolve by write order in an
        // extractor and by nothing at all here.
        if !seen.insert(file.name()) {
            return Err(Why::Unsupported);
        }
        let mut entry = Entry {
            name: file.name().to_owned(),
            length: 0,
            folder: None,
            offset: 0,
            crc: None,
        };
        if file.is_directory() && !entry.name.ends_with(['/', '\\']) {
            entry.name.push('/');
        }
        // A zero-length entry that claims a stream is dataless too: it takes
        // none of its folder, and a member with no bytes has none to route.
        if file.has_stream() && !file.is_directory() && file.size() > 0 {
            let folder = archive
                .stream_map
                .file_block_index
                .get(index)
                .copied()
                .flatten()
                .ok_or(Why::Unsupported)?;
            let cursor = cursors.get_mut(folder).ok_or(Why::Unsupported)?;
            let end = cursor.checked_add(file.size()).ok_or(Why::Unsupported)?;
            if end > folders[folder].plain_len {
                return Err(Why::Unsupported);
            }
            // An enciphered member is proved by its CRC alone.
            if folders[folder].aes.is_some() && !file.has_crc {
                return Err(Why::Unsupported);
            }
            entry.folder = Some(folder);
            entry.offset = *cursor;
            entry.length = file.size();
            entry.crc = file.has_crc.then_some(file.crc as u32);
            *cursor = end;
            folders[folder].entries.push(index);
        }
        entries.push(entry);
    }
    // Every folder's bytes are its files', end to end: bytes no entry
    // accounts for are bytes the layout and the archive disagree about.
    if folders
        .iter()
        .zip(&cursors)
        .any(|(folder, cursor)| folder.plain_len != *cursor)
    {
        return Err(Why::Unsupported);
    }
    // Nothing to route: every entry is dataless.
    if entries.iter().all(|entry| entry.folder.is_none()) {
        return Err(Why::Unsupported);
    }
    // Lookups by container offset walk the packs in order: they must follow
    // one another without overlap.
    if folders
        .windows(2)
        .any(|pair| pair[0].pack.end > pair[1].pack.start)
    {
        return Err(Why::Unsupported);
    }
    Ok(Map { folders, entries })
}

impl Map {
    pub(super) fn entry(&self, member: MemberId) -> Option<&Entry> {
        self.entries.get(member.0 as usize)
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
    ) -> impl Iterator<Item = (MemberId, &Entry)> {
        let indices = &self.folders[folder].entries;
        let first = indices.partition_point(|index| self.entries[*index].span().end <= range.start);
        indices[first..]
            .iter()
            .map(|index| (MemberId(*index as u32), &self.entries[*index]))
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
