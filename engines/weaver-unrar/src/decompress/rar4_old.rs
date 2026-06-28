//! RAR4 compatibility decoders for pre-RAR29 compressed members.
//!
//! RAR 3.x/4.x use the RAR29 unpacker implemented in `rar4.rs`. Older
//! RAR 2.0/2.6 and RAR 1.5 members use different bitstreams. This module
//! ports unrar's `Unpack20` and `Unpack15` control flow closely enough to
//! keep those members out of the RAR29 decoder.

use std::io::{Read, Write};
use std::sync::{Arc, Mutex};

use super::lz::bitstream::{BitRead, BitReader, StreamingBitReader};
use super::lz::huffman::HuffmanTable;
use super::lz::window::Window;
use super::rar4::Rar4LzDecoder;
use crate::error::{RarError, RarResult};

const MAX_DICT_SIZE: u64 = 256 * 1024 * 1024;
const MIN_RAR4_UNPACK_WINDOW: u64 = 0x40000;
const UNPACK_MAX_WRITE: usize = 0x400000;
const OLD_FLUSH_GUARD: usize = 0x1004;

const NC20: usize = 298;
const DC20: usize = 48;
const RC20: usize = 28;
const BC20: usize = 19;
const MC20: usize = 257;
const TABLE20_SIZE: usize = MC20 * 4;

const LDECODE20: [usize; 28] = [
    0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 14, 16, 20, 24, 28, 32, 40, 48, 56, 64, 80, 96, 112, 128,
    160, 192, 224,
];
const LBITS20: [u8; 28] = [
    0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5,
];
const DDECODE20: [usize; DC20] = [
    0, 1, 2, 3, 4, 6, 8, 12, 16, 24, 32, 48, 64, 96, 128, 192, 256, 384, 512, 768, 1024, 1536,
    2048, 3072, 4096, 6144, 8192, 12288, 16384, 24576, 32768, 49152, 65536, 98304, 131072, 196608,
    262144, 327680, 393216, 458752, 524288, 589824, 655360, 720896, 786432, 851968, 917504, 983040,
];
const DBITS20: [u8; DC20] = [
    0, 0, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 10, 11, 11, 12, 12, 13,
    13, 14, 14, 15, 15, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16,
];
const SDDECODE20: [usize; 8] = [0, 4, 8, 16, 32, 64, 128, 192];
const SDBITS20: [u8; 8] = [2, 2, 3, 4, 5, 6, 6, 6];

const STARTL1: usize = 2;
const DECL1: [u32; 11] = [
    0x8000, 0xa000, 0xc000, 0xd000, 0xe000, 0xea00, 0xee00, 0xf000, 0xf200, 0xf200, 0xffff,
];
const POSL1: [usize; 13] = [0, 0, 0, 2, 3, 5, 7, 11, 16, 20, 24, 32, 32];

const STARTL2: usize = 3;
const DECL2: [u32; 10] = [
    0xa000, 0xc000, 0xd000, 0xe000, 0xea00, 0xee00, 0xf000, 0xf200, 0xf240, 0xffff,
];
const POSL2: [usize; 13] = [0, 0, 0, 0, 5, 7, 9, 13, 18, 22, 26, 34, 36];

const STARTHF0: usize = 4;
const DECHF0: [u32; 9] = [
    0x8000, 0xc000, 0xe000, 0xf200, 0xf200, 0xf200, 0xf200, 0xf200, 0xffff,
];
const POSHF0: [usize; 13] = [0, 0, 0, 0, 0, 8, 16, 24, 33, 33, 33, 33, 33];

const STARTHF1: usize = 5;
const DECHF1: [u32; 8] = [
    0x2000, 0xc000, 0xe000, 0xf000, 0xf200, 0xf200, 0xf7e0, 0xffff,
];
const POSHF1: [usize; 13] = [0, 0, 0, 0, 0, 0, 4, 44, 60, 76, 80, 80, 127];

const STARTHF2: usize = 5;
const DECHF2: [u32; 8] = [
    0x1000, 0x2400, 0x8000, 0xc000, 0xfa00, 0xffff, 0xffff, 0xffff,
];
const POSHF2: [usize; 13] = [0, 0, 0, 0, 0, 0, 2, 7, 53, 117, 233, 0, 0];

const STARTHF3: usize = 6;
const DECHF3: [u32; 7] = [0x800, 0x2400, 0xee00, 0xfe80, 0xffff, 0xffff, 0xffff];
const POSHF3: [usize; 13] = [0, 0, 0, 0, 0, 0, 0, 2, 16, 218, 251, 0, 0];

const STARTHF4: usize = 8;
const DECHF4: [u32; 6] = [0xff00, 0xffff, 0xffff, 0xffff, 0xffff, 0xffff];
const POSHF4: [usize; 13] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 0, 0, 0];

pub(crate) enum Rar4Decoder {
    V15(Box<Rar15Decoder>),
    V20(Box<Rar20Decoder>),
    V29(Box<Rar4LzDecoder>),
}

impl Rar4Decoder {
    pub(crate) fn new(version: u8, dict_size: usize, method: u8) -> RarResult<Self> {
        let dict_size = old_rar_window_size(dict_size);
        match version {
            13..=15 => Ok(Self::V15(Box::new(Rar15Decoder::try_new(dict_size)?))),
            20 | 26 => Ok(Self::V20(Box::new(Rar20Decoder::try_new(dict_size)?))),
            29 => Ok(Self::V29(Box::new(Rar4LzDecoder::try_new(dict_size)?))),
            _ => Err(RarError::UnsupportedCompression { method, version }),
        }
    }

    pub(crate) fn prepare_solid_continuation(&mut self) {
        if let Self::V29(decoder) = self {
            decoder.prepare_solid_continuation();
        }
    }

    pub(crate) fn supports_version(&self, version: u8) -> bool {
        matches!(
            (self, version),
            (Self::V15(_), 13..=15) | (Self::V20(_), 20 | 26) | (Self::V29(_), 29)
        )
    }

    pub(crate) fn decompress_to_writer<W: Write>(
        &mut self,
        input: &[u8],
        unpacked_size: u64,
        writer: &mut W,
    ) -> RarResult<u64> {
        match self {
            Self::V15(decoder) => decoder.decompress_to_writer(input, unpacked_size, writer),
            Self::V20(decoder) => decoder.decompress_to_writer(input, unpacked_size, writer),
            Self::V29(decoder) => decoder.decompress_to_writer(input, unpacked_size, writer),
        }
    }

    pub(crate) fn decompress_reader_to_writer<R: Read, W: Write>(
        &mut self,
        input: R,
        unpacked_size: u64,
        writer: &mut W,
    ) -> RarResult<u64> {
        match self {
            Self::V15(decoder) => decoder.decompress_reader_to_writer(input, unpacked_size, writer),
            Self::V20(decoder) => decoder.decompress_reader_to_writer(input, unpacked_size, writer),
            Self::V29(decoder) => decoder.decompress_reader_to_writer(input, unpacked_size, writer),
        }
    }

    pub(crate) fn decompress_reader_to_writer_chunked<R: Read, F>(
        &mut self,
        input: R,
        unpacked_size: u64,
        first_volume_index: usize,
        shared_transitions: Arc<Mutex<Vec<super::VolumeTransition>>>,
        writer_factory: F,
    ) -> RarResult<Vec<(usize, u64)>>
    where
        F: FnMut(usize) -> RarResult<Box<dyn Write>>,
    {
        match self {
            Self::V15(decoder) => decoder.decompress_reader_to_writer_chunked(
                input,
                unpacked_size,
                first_volume_index,
                shared_transitions,
                writer_factory,
            ),
            Self::V20(decoder) => decoder.decompress_reader_to_writer_chunked(
                input,
                unpacked_size,
                first_volume_index,
                shared_transitions,
                writer_factory,
            ),
            Self::V29(decoder) => decoder.decompress_reader_to_writer_chunked(
                input,
                unpacked_size,
                first_volume_index,
                shared_transitions,
                writer_factory,
            ),
        }
    }
}

pub(crate) fn ensure_supported_rar4_version(version: u8, method: u8) -> RarResult<()> {
    match version {
        13..=15 | 20 | 26 | 29 => Ok(()),
        _ => Err(RarError::UnsupportedCompression { method, version }),
    }
}

pub(crate) fn decompress_rar4_to_writer<W: Write>(
    input: &[u8],
    unpacked_size: u64,
    version: u8,
    method: u8,
    dict_size: u64,
    writer: &mut W,
) -> RarResult<u64> {
    check_dict_size(dict_size)?;
    let mut decoder = Rar4Decoder::new(version, dict_size as usize, method)?;
    decoder.decompress_to_writer(input, unpacked_size, writer)
}

pub(crate) fn decompress_rar4_reader_to_writer<R: Read, W: Write>(
    input: R,
    unpacked_size: u64,
    version: u8,
    method: u8,
    dict_size: u64,
    writer: &mut W,
) -> RarResult<u64> {
    check_dict_size(dict_size)?;
    let mut decoder = Rar4Decoder::new(version, dict_size as usize, method)?;
    decoder.decompress_reader_to_writer(input, unpacked_size, writer)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn decompress_rar4_reader_to_writer_chunked<R: Read, F>(
    input: R,
    unpacked_size: u64,
    version: u8,
    method: u8,
    dict_size: u64,
    first_volume_index: usize,
    shared_transitions: Arc<Mutex<Vec<super::VolumeTransition>>>,
    writer_factory: F,
) -> RarResult<Vec<(usize, u64)>>
where
    F: FnMut(usize) -> RarResult<Box<dyn Write>>,
{
    check_dict_size(dict_size)?;
    let mut decoder = Rar4Decoder::new(version, dict_size as usize, method)?;
    decoder.decompress_reader_to_writer_chunked(
        input,
        unpacked_size,
        first_volume_index,
        shared_transitions,
        writer_factory,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn decompress_rar4_to_writer_chunked<F>(
    input: &[u8],
    unpacked_size: u64,
    version: u8,
    method: u8,
    dict_size: u64,
    first_volume_index: usize,
    boundaries: &[super::VolumeTransition],
    writer_factory: F,
) -> RarResult<Vec<(usize, u64)>>
where
    F: FnMut(usize) -> RarResult<Box<dyn Write>>,
{
    check_dict_size(dict_size)?;
    let mut decoder = Rar4Decoder::new(version, dict_size as usize, method)?;
    match &mut decoder {
        Rar4Decoder::V15(decoder) => decoder.decompress_to_writer_chunked(
            input,
            unpacked_size,
            first_volume_index,
            boundaries,
            writer_factory,
        ),
        Rar4Decoder::V20(decoder) => decoder.decompress_to_writer_chunked(
            input,
            unpacked_size,
            first_volume_index,
            boundaries,
            writer_factory,
        ),
        Rar4Decoder::V29(decoder) => decoder.decompress_to_writer_chunked(
            input,
            unpacked_size,
            first_volume_index,
            boundaries,
            writer_factory,
        ),
    }
}

fn check_dict_size(dict_size: u64) -> RarResult<()> {
    let effective_size = effective_rar4_window_size(dict_size);
    if effective_size > MAX_DICT_SIZE {
        return Err(RarError::DictionaryTooLarge {
            size: effective_size,
            max: MAX_DICT_SIZE,
        });
    }
    Ok(())
}

pub(crate) fn effective_rar4_window_size(dict_size: u64) -> u64 {
    dict_size.max(MIN_RAR4_UNPACK_WINDOW)
}

fn old_rar_window_size(dict_size: usize) -> usize {
    effective_rar4_window_size(dict_size as u64) as usize
}

fn old_flush_threshold(window: &Window) -> usize {
    window
        .dict_size()
        .saturating_sub(OLD_FLUSH_GUARD)
        .clamp(1, UNPACK_MAX_WRITE)
}

fn copy_match(
    window: &mut Window,
    distance: usize,
    length: usize,
    output_size: u64,
    unpacked_size: u64,
    rar15_zero_distance: bool,
) -> RarResult<u64> {
    let remaining = (unpacked_size - output_size) as usize;
    let visible_len = length.min(remaining);
    if rar15_zero_distance {
        window.copy_rar15_with_visible_len(distance, length, visible_len)?;
    } else {
        window.copy_with_visible_len(distance, length, visible_len)?;
    }
    Ok(output_size + visible_len as u64)
}

#[derive(Clone, Copy, Default)]
struct AudioVariables {
    k1: i32,
    k2: i32,
    k3: i32,
    k4: i32,
    k5: i32,
    d1: i32,
    d2: i32,
    d3: i32,
    d4: i32,
    last_delta: i32,
    dif: [u32; 11],
    byte_count: u32,
    last_char: u32,
}

pub(crate) struct Rar20Decoder {
    window: Window,
    old_dist: [usize; 4],
    old_dist_ptr: usize,
    last_dist: usize,
    last_length: usize,
    ld_table: Option<HuffmanTable>,
    dd_table: Option<HuffmanTable>,
    rd_table: Option<HuffmanTable>,
    md_tables: [Option<HuffmanTable>; 4],
    old_table: [u8; TABLE20_SIZE],
    audio_block: bool,
    cur_channel: usize,
    channels: usize,
    channel_delta: i32,
    audio_vars: [AudioVariables; 4],
    tables_read: bool,
    started: bool,
}

impl Rar20Decoder {
    fn try_new(dict_size: usize) -> RarResult<Self> {
        Ok(Self {
            window: Window::try_new(dict_size)?,
            old_dist: [usize::MAX; 4],
            old_dist_ptr: 0,
            last_dist: usize::MAX,
            last_length: 0,
            ld_table: None,
            dd_table: None,
            rd_table: None,
            md_tables: std::array::from_fn(|_| None),
            old_table: [0; TABLE20_SIZE],
            audio_block: false,
            cur_channel: 0,
            channels: 1,
            channel_delta: 0,
            audio_vars: [AudioVariables::default(); 4],
            tables_read: false,
            started: false,
        })
    }

    fn begin_file_decode(&mut self) {
        if !self.started {
            self.started = true;
            self.old_dist = [usize::MAX; 4];
            self.old_dist_ptr = 0;
            self.last_dist = usize::MAX;
            self.last_length = 0;
            self.tables_read = false;
            self.audio_block = false;
            self.channel_delta = 0;
            self.cur_channel = 0;
            self.channels = 1;
            self.audio_vars = [AudioVariables::default(); 4];
            self.old_table.fill(0);
            self.md_tables = std::array::from_fn(|_| None);
        }
        self.window.mark_flushed(self.window.total_written());
    }

    fn decompress_to_writer<W: Write>(
        &mut self,
        input: &[u8],
        unpacked_size: u64,
        writer: &mut W,
    ) -> RarResult<u64> {
        let mut reader = BitReader::new(input);
        self.decompress_with_reader(&mut reader, unpacked_size, writer)
    }

    fn decompress_reader_to_writer<R: Read, W: Write>(
        &mut self,
        input: R,
        unpacked_size: u64,
        writer: &mut W,
    ) -> RarResult<u64> {
        let mut reader = StreamingBitReader::new(input);
        self.decompress_with_reader(&mut reader, unpacked_size, writer)
    }

    fn decompress_with_reader<R: BitRead, W: Write>(
        &mut self,
        reader: &mut R,
        unpacked_size: u64,
        writer: &mut W,
    ) -> RarResult<u64> {
        if unpacked_size == 0 {
            return Ok(0);
        }

        self.begin_file_decode();
        if !self.tables_read {
            self.read_tables(reader)?;
        }

        let mut output_size = 0;
        let flush_threshold = old_flush_threshold(&self.window);
        while output_size < unpacked_size {
            if !reader.has_bits() {
                break;
            }
            let prev = output_size;
            output_size = self.decode_symbol(reader, output_size, unpacked_size)?;
            if output_size == prev && !reader.has_bits() {
                break;
            }
            if self.window.unflushed_bytes() as usize >= flush_threshold {
                self.window.flush_to_writer(writer).map_err(RarError::Io)?;
            }
        }

        self.read_last_tables(reader)?;
        self.window.flush_to_writer(writer).map_err(RarError::Io)?;
        Ok(output_size)
    }

    fn decompress_to_writer_chunked<F>(
        &mut self,
        input: &[u8],
        unpacked_size: u64,
        first_volume_index: usize,
        boundaries: &[super::VolumeTransition],
        writer_factory: F,
    ) -> RarResult<Vec<(usize, u64)>>
    where
        F: FnMut(usize) -> RarResult<Box<dyn Write>>,
    {
        let mut reader = BitReader::new(input);
        self.decompress_chunked_with_reader(
            &mut reader,
            unpacked_size,
            first_volume_index,
            |idx| boundaries.get(idx).cloned(),
            writer_factory,
        )
    }

    fn decompress_reader_to_writer_chunked<Rd: Read, F>(
        &mut self,
        input: Rd,
        unpacked_size: u64,
        first_volume_index: usize,
        shared_transitions: Arc<Mutex<Vec<super::VolumeTransition>>>,
        writer_factory: F,
    ) -> RarResult<Vec<(usize, u64)>>
    where
        F: FnMut(usize) -> RarResult<Box<dyn Write>>,
    {
        let mut reader = StreamingBitReader::new(input);
        self.decompress_chunked_with_reader(
            &mut reader,
            unpacked_size,
            first_volume_index,
            |idx| shared_transitions.lock().unwrap().get(idx).cloned(),
            writer_factory,
        )
    }

    fn decompress_chunked_with_reader<R: BitRead, F, G>(
        &mut self,
        reader: &mut R,
        unpacked_size: u64,
        first_volume_index: usize,
        mut boundary_at: G,
        mut writer_factory: F,
    ) -> RarResult<Vec<(usize, u64)>>
    where
        F: FnMut(usize) -> RarResult<Box<dyn Write>>,
        G: FnMut(usize) -> Option<super::VolumeTransition>,
    {
        if unpacked_size == 0 {
            return Ok(Vec::new());
        }

        self.begin_file_decode();
        if !self.tables_read {
            self.read_tables(reader)?;
        }

        let mut chunks = Vec::new();
        let mut current_vol = first_volume_index;
        let mut current_writer = writer_factory(current_vol)?;
        let mut chunk_bytes = 0u64;
        let mut boundary_idx = 0usize;
        let mut pending_boundary_volume = None;
        let mut output_size = 0u64;
        let flush_threshold = old_flush_threshold(&self.window);

        while output_size < unpacked_size {
            if !reader.has_bits() {
                break;
            }
            let prev = output_size;
            output_size = self.decode_symbol(reader, output_size, unpacked_size)?;
            chunk_bytes += output_size - prev;

            if pending_boundary_volume.is_none()
                && let Some(boundary) = boundary_at(boundary_idx)
                && reader.byte_position() as u64 >= boundary.compressed_offset
            {
                pending_boundary_volume = Some(boundary.volume_index);
                boundary_idx += 1;
            }

            if pending_boundary_volume.is_some()
                || self.window.unflushed_bytes() as usize >= flush_threshold
            {
                self.window
                    .flush_to_writer(&mut *current_writer)
                    .map_err(RarError::Io)?;
            }

            if let Some(next_vol) = pending_boundary_volume.take() {
                chunks.push((current_vol, chunk_bytes));
                current_vol = next_vol;
                current_writer = writer_factory(current_vol)?;
                chunk_bytes = 0;
            }
        }

        self.read_last_tables(reader)?;
        self.window
            .flush_to_writer(&mut *current_writer)
            .map_err(RarError::Io)?;
        if chunk_bytes > 0 || chunks.is_empty() {
            chunks.push((current_vol, chunk_bytes));
        }

        Ok(chunks)
    }

    fn decode_symbol<R: BitRead>(
        &mut self,
        reader: &mut R,
        output_size: u64,
        unpacked_size: u64,
    ) -> RarResult<u64> {
        if self.audio_block {
            let audio_number = self.md_tables[self.cur_channel]
                .as_ref()
                .ok_or_else(|| RarError::CorruptArchive {
                    detail: "RAR20: missing audio Huffman table".into(),
                })?
                .decode(reader)? as usize;
            if audio_number == 256 {
                self.read_tables(reader)?;
                return Ok(output_size);
            }

            let byte = self.decode_audio(audio_number as i32);
            self.window.put_byte(byte);
            if self.channels != 0 {
                self.cur_channel = (self.cur_channel + 1) % self.channels;
            }
            return Ok(output_size + 1);
        }

        let number = self
            .ld_table
            .as_ref()
            .ok_or_else(|| RarError::CorruptArchive {
                detail: "RAR20: missing literal/length Huffman table".into(),
            })?
            .decode(reader)? as usize;

        if number < 256 {
            self.window.put_byte(number as u8);
            return Ok(output_size + 1);
        }

        if number > 269 {
            let idx = number - 270;
            let mut length = LDECODE20[idx] + 3;
            if LBITS20[idx] > 0 {
                length += reader.read_bits(LBITS20[idx])? as usize;
            }

            let dist_number = self
                .dd_table
                .as_ref()
                .ok_or_else(|| RarError::CorruptArchive {
                    detail: "RAR20: missing distance Huffman table".into(),
                })?
                .decode(reader)? as usize;
            let mut distance = DDECODE20[dist_number] + 1;
            if DBITS20[dist_number] > 0 {
                distance += reader.read_bits(DBITS20[dist_number])? as usize;
            }

            if distance >= 0x2000 {
                length += 1;
                if distance >= 0x40000 {
                    length += 1;
                }
            }

            return self.copy_string20(distance, length, output_size, unpacked_size);
        }

        if number == 269 {
            self.read_tables(reader)?;
            return Ok(output_size);
        }

        if number == 256 {
            return self.copy_string20(
                self.last_dist,
                self.last_length,
                output_size,
                unpacked_size,
            );
        }

        if number < 261 {
            let cache_idx = (self.old_dist_ptr + 4 - (number - 256)) & 3;
            let distance = self.old_dist[cache_idx];
            let length_number = self
                .rd_table
                .as_ref()
                .ok_or_else(|| RarError::CorruptArchive {
                    detail: "RAR20: missing repeat Huffman table".into(),
                })?
                .decode(reader)? as usize;
            let mut length = LDECODE20[length_number] + 2;
            if LBITS20[length_number] > 0 {
                length += reader.read_bits(LBITS20[length_number])? as usize;
            }
            if distance >= 0x101 {
                length += 1;
                if distance >= 0x2000 {
                    length += 1;
                    if distance >= 0x40000 {
                        length += 1;
                    }
                }
            }
            return self.copy_string20(distance, length, output_size, unpacked_size);
        }

        if number < 270 {
            let idx = number - 261;
            let mut distance = SDDECODE20[idx] + 1;
            if SDBITS20[idx] > 0 {
                distance += reader.read_bits(SDBITS20[idx])? as usize;
            }
            return self.copy_string20(distance, 2, output_size, unpacked_size);
        }

        Err(RarError::CorruptArchive {
            detail: format!("RAR20: invalid symbol {number}"),
        })
    }

    fn copy_string20(
        &mut self,
        distance: usize,
        length: usize,
        output_size: u64,
        unpacked_size: u64,
    ) -> RarResult<u64> {
        self.last_dist = distance;
        self.old_dist[self.old_dist_ptr] = distance;
        self.old_dist_ptr = (self.old_dist_ptr + 1) & 3;
        self.last_length = length;
        copy_match(
            &mut self.window,
            distance,
            length,
            output_size,
            unpacked_size,
            false,
        )
    }

    fn read_tables<R: BitRead>(&mut self, reader: &mut R) -> RarResult<()> {
        let bit_field = reader.peek_16_raw()?;
        self.audio_block = (bit_field & 0x8000) != 0;
        if (bit_field & 0x4000) == 0 {
            self.old_table.fill(0);
        }
        reader.consume_bits(2)?;

        let table_size = if self.audio_block {
            self.channels = (((bit_field >> 12) & 3) + 1) as usize;
            if self.cur_channel >= self.channels {
                self.cur_channel = 0;
            }
            reader.consume_bits(2)?;
            MC20 * self.channels
        } else {
            NC20 + DC20 + RC20
        };

        let mut bit_length = [0u8; BC20];
        for length in &mut bit_length {
            *length = reader.read_bits(4)? as u8;
        }
        let bd_table =
            HuffmanTable::build(&bit_length).map_err(|err| RarError::CorruptArchive {
                detail: format!("RAR20: invalid bit-length table: {err}"),
            })?;

        let mut table = [0u8; TABLE20_SIZE];
        let mut i = 0usize;
        while i < table_size {
            let number = bd_table.decode(reader)? as usize;
            if number < 16 {
                table[i] = ((number as u8).wrapping_add(self.old_table[i])) & 0x0f;
                i += 1;
            } else if number == 16 {
                let mut count = (reader.peek_16_raw()? >> 14) as usize + 3;
                reader.consume_bits(2)?;
                if i == 0 {
                    return Err(RarError::CorruptArchive {
                        detail: "RAR20: repeat-previous code at table start".into(),
                    });
                }
                while count > 0 && i < table_size {
                    table[i] = table[i - 1];
                    i += 1;
                    count -= 1;
                }
            } else {
                let mut count = if number == 17 {
                    let count = (reader.peek_16_raw()? >> 13) as usize + 3;
                    reader.consume_bits(3)?;
                    count
                } else {
                    let count = (reader.peek_16_raw()? >> 9) as usize + 11;
                    reader.consume_bits(7)?;
                    count
                };
                while count > 0 && i < table_size {
                    table[i] = 0;
                    i += 1;
                    count -= 1;
                }
            }
        }

        if self.audio_block {
            for channel in 0..self.channels {
                let start = channel * MC20;
                let end = start + MC20;
                self.md_tables[channel] =
                    Some(HuffmanTable::build(&table[start..end]).map_err(|err| {
                        RarError::CorruptArchive {
                            detail: format!("RAR20: invalid audio table: {err}"),
                        }
                    })?);
            }
        } else {
            self.ld_table = Some(HuffmanTable::build(&table[..NC20]).map_err(|err| {
                RarError::CorruptArchive {
                    detail: format!("RAR20: invalid LD table: {err}"),
                }
            })?);
            self.dd_table = Some(HuffmanTable::build(&table[NC20..NC20 + DC20]).map_err(
                |err| RarError::CorruptArchive {
                    detail: format!("RAR20: invalid DD table: {err}"),
                },
            )?);
            self.rd_table = Some(
                HuffmanTable::build(&table[NC20 + DC20..NC20 + DC20 + RC20]).map_err(|err| {
                    RarError::CorruptArchive {
                        detail: format!("RAR20: invalid RD table: {err}"),
                    }
                })?,
            );
        }

        self.old_table[..table_size].copy_from_slice(&table[..table_size]);
        self.tables_read = true;
        Ok(())
    }

    fn read_last_tables<R: BitRead>(&mut self, reader: &mut R) -> RarResult<()> {
        if !reader.has_exact_bits(40)? {
            return Ok(());
        }

        if self.audio_block {
            let Some(table) = self.md_tables[self.cur_channel].as_ref() else {
                return Ok(());
            };
            if table.decode(reader)? as usize == 256 {
                self.read_tables(reader)?;
            }
        } else {
            let Some(table) = self.ld_table.as_ref() else {
                return Ok(());
            };
            if table.decode(reader)? as usize == 269 {
                self.read_tables(reader)?;
            }
        }

        Ok(())
    }

    fn decode_audio(&mut self, delta: i32) -> u8 {
        let v = &mut self.audio_vars[self.cur_channel];
        v.byte_count = v.byte_count.wrapping_add(1);
        v.d4 = v.d3;
        v.d3 = v.d2;
        v.d2 = v.last_delta - v.d1;
        v.d1 = v.last_delta;

        let predicted = 8u32
            .wrapping_mul(v.last_char)
            .wrapping_add((v.k1 * v.d1) as u32)
            .wrapping_add((v.k2 * v.d2) as u32)
            .wrapping_add((v.k3 * v.d3) as u32)
            .wrapping_add((v.k4 * v.d4) as u32)
            .wrapping_add((v.k5 * self.channel_delta) as u32)
            >> 3
            & 0xff;
        let ch = predicted.wrapping_sub(delta as u32);
        let d = ((delta as i8 as u32) << 3) as i32;

        let samples = [
            d,
            d - v.d1,
            d + v.d1,
            d - v.d2,
            d + v.d2,
            d - v.d3,
            d + v.d3,
            d - v.d4,
            d + v.d4,
            d - self.channel_delta,
            d + self.channel_delta,
        ];
        for (dst, sample) in v.dif.iter_mut().zip(samples) {
            *dst = dst.wrapping_add(sample.unsigned_abs());
        }

        self.channel_delta = (ch.wrapping_sub(v.last_char) as u8) as i8 as i32;
        v.last_delta = self.channel_delta;
        v.last_char = ch;

        if (v.byte_count & 0x1f) == 0 {
            let mut min_dif = v.dif[0];
            let mut num_min_dif = 0usize;
            v.dif[0] = 0;
            for i in 1..v.dif.len() {
                if v.dif[i] < min_dif {
                    min_dif = v.dif[i];
                    num_min_dif = i;
                }
                v.dif[i] = 0;
            }
            match num_min_dif {
                1 if v.k1 >= -16 => v.k1 -= 1,
                2 if v.k1 < 16 => v.k1 += 1,
                3 if v.k2 >= -16 => v.k2 -= 1,
                4 if v.k2 < 16 => v.k2 += 1,
                5 if v.k3 >= -16 => v.k3 -= 1,
                6 if v.k3 < 16 => v.k3 += 1,
                7 if v.k4 >= -16 => v.k4 -= 1,
                8 if v.k4 < 16 => v.k4 += 1,
                9 if v.k5 >= -16 => v.k5 -= 1,
                10 if v.k5 < 16 => v.k5 += 1,
                _ => {}
            }
        }

        ch as u8
    }
}

pub(crate) struct Rar15Decoder {
    window: Window,
    old_dist: [usize; 4],
    old_dist_ptr: usize,
    last_dist: usize,
    last_length: usize,
    ch_set: [u16; 256],
    ch_set_b: [u16; 256],
    ch_set_a: [u16; 256],
    ch_set_c: [u16; 257],
    n_to_pl: [u8; 256],
    n_to_pl_b: [u8; 256],
    n_to_pl_c: [u8; 256],
    avr_plc_b: u32,
    avr_ln1: u32,
    avr_ln2: u32,
    avr_ln3: u32,
    avr_plc: u32,
    max_dist3: usize,
    nhfb: u32,
    nlzb: u32,
    num_huf: u32,
    buf60: usize,
    flags_cnt: i32,
    flag_buf: u16,
    st_mode: bool,
    l_count: u32,
    started: bool,
}

impl Rar15Decoder {
    fn try_new(dict_size: usize) -> RarResult<Self> {
        let mut decoder = Self {
            window: Window::try_new(dict_size)?,
            old_dist: [usize::MAX; 4],
            old_dist_ptr: 0,
            last_dist: usize::MAX,
            last_length: 0,
            ch_set: [0; 256],
            ch_set_b: [0; 256],
            ch_set_a: [0; 256],
            ch_set_c: [0; 257],
            n_to_pl: [0; 256],
            n_to_pl_b: [0; 256],
            n_to_pl_c: [0; 256],
            avr_plc_b: 0,
            avr_ln1: 0,
            avr_ln2: 0,
            avr_ln3: 0,
            avr_plc: 0x3500,
            max_dist3: 0x2001,
            nhfb: 0x80,
            nlzb: 0x80,
            num_huf: 0,
            buf60: 0,
            flags_cnt: 0,
            flag_buf: 0,
            st_mode: false,
            l_count: 0,
            started: false,
        };
        decoder.init_non_solid_state();
        decoder.init_huff();
        Ok(decoder)
    }

    fn init_non_solid_state(&mut self) {
        self.old_dist = [usize::MAX; 4];
        self.old_dist_ptr = 0;
        self.last_dist = usize::MAX;
        self.last_length = 0;
        self.avr_plc_b = 0;
        self.avr_ln1 = 0;
        self.avr_ln2 = 0;
        self.avr_ln3 = 0;
        self.num_huf = 0;
        self.buf60 = 0;
        self.avr_plc = 0x3500;
        self.max_dist3 = 0x2001;
        self.nhfb = 0x80;
        self.nlzb = 0x80;
    }

    fn begin_file_decode(&mut self) {
        if !self.started {
            self.init_non_solid_state();
            self.init_huff();
            self.started = true;
        }
        self.flags_cnt = 0;
        self.flag_buf = 0;
        self.st_mode = false;
        self.l_count = 0;
        self.window.mark_flushed(self.window.total_written());
    }

    fn decompress_to_writer<W: Write>(
        &mut self,
        input: &[u8],
        unpacked_size: u64,
        writer: &mut W,
    ) -> RarResult<u64> {
        let mut reader = BitReader::new(input);
        self.decompress_with_reader(&mut reader, unpacked_size, writer)
    }

    fn decompress_reader_to_writer<R: Read, W: Write>(
        &mut self,
        input: R,
        unpacked_size: u64,
        writer: &mut W,
    ) -> RarResult<u64> {
        let mut reader = StreamingBitReader::new(input);
        self.decompress_with_reader(&mut reader, unpacked_size, writer)
    }

    fn decompress_with_reader<R: BitRead, W: Write>(
        &mut self,
        reader: &mut R,
        unpacked_size: u64,
        writer: &mut W,
    ) -> RarResult<u64> {
        if unpacked_size == 0 {
            return Ok(0);
        }

        self.begin_file_decode();
        let mut output_size = 0u64;
        if output_size < unpacked_size {
            self.get_flags_buf(reader)?;
            self.flags_cnt = 8;
        }

        let flush_threshold = old_flush_threshold(&self.window);
        while output_size < unpacked_size {
            if !reader.has_bits() {
                break;
            }

            output_size = self.decode_next(reader, output_size, unpacked_size)?;
            if self.window.unflushed_bytes() as usize >= flush_threshold {
                self.window.flush_to_writer(writer).map_err(RarError::Io)?;
            }
        }

        self.window.flush_to_writer(writer).map_err(RarError::Io)?;
        Ok(output_size)
    }

    fn decompress_to_writer_chunked<F>(
        &mut self,
        input: &[u8],
        unpacked_size: u64,
        first_volume_index: usize,
        boundaries: &[super::VolumeTransition],
        writer_factory: F,
    ) -> RarResult<Vec<(usize, u64)>>
    where
        F: FnMut(usize) -> RarResult<Box<dyn Write>>,
    {
        let mut reader = BitReader::new(input);
        self.decompress_chunked_with_reader(
            &mut reader,
            unpacked_size,
            first_volume_index,
            |idx| boundaries.get(idx).cloned(),
            writer_factory,
        )
    }

    fn decompress_reader_to_writer_chunked<Rd: Read, F>(
        &mut self,
        input: Rd,
        unpacked_size: u64,
        first_volume_index: usize,
        shared_transitions: Arc<Mutex<Vec<super::VolumeTransition>>>,
        writer_factory: F,
    ) -> RarResult<Vec<(usize, u64)>>
    where
        F: FnMut(usize) -> RarResult<Box<dyn Write>>,
    {
        let mut reader = StreamingBitReader::new(input);
        self.decompress_chunked_with_reader(
            &mut reader,
            unpacked_size,
            first_volume_index,
            |idx| shared_transitions.lock().unwrap().get(idx).cloned(),
            writer_factory,
        )
    }

    fn decompress_chunked_with_reader<R: BitRead, F, G>(
        &mut self,
        reader: &mut R,
        unpacked_size: u64,
        first_volume_index: usize,
        mut boundary_at: G,
        mut writer_factory: F,
    ) -> RarResult<Vec<(usize, u64)>>
    where
        F: FnMut(usize) -> RarResult<Box<dyn Write>>,
        G: FnMut(usize) -> Option<super::VolumeTransition>,
    {
        if unpacked_size == 0 {
            return Ok(Vec::new());
        }

        self.begin_file_decode();
        self.get_flags_buf(reader)?;
        self.flags_cnt = 8;

        let mut chunks = Vec::new();
        let mut current_vol = first_volume_index;
        let mut current_writer = writer_factory(current_vol)?;
        let mut chunk_bytes = 0u64;
        let mut boundary_idx = 0usize;
        let mut pending_boundary_volume = None;
        let mut output_size = 0u64;
        let flush_threshold = old_flush_threshold(&self.window);

        while output_size < unpacked_size {
            if !reader.has_bits() {
                break;
            }

            let prev = output_size;
            output_size = self.decode_next(reader, output_size, unpacked_size)?;
            chunk_bytes += output_size - prev;

            if pending_boundary_volume.is_none()
                && let Some(boundary) = boundary_at(boundary_idx)
                && reader.byte_position() as u64 >= boundary.compressed_offset
            {
                pending_boundary_volume = Some(boundary.volume_index);
                boundary_idx += 1;
            }

            if pending_boundary_volume.is_some()
                || self.window.unflushed_bytes() as usize >= flush_threshold
            {
                self.window
                    .flush_to_writer(&mut *current_writer)
                    .map_err(RarError::Io)?;
            }

            if let Some(next_vol) = pending_boundary_volume.take() {
                chunks.push((current_vol, chunk_bytes));
                current_vol = next_vol;
                current_writer = writer_factory(current_vol)?;
                chunk_bytes = 0;
            }
        }

        self.window
            .flush_to_writer(&mut *current_writer)
            .map_err(RarError::Io)?;
        if chunk_bytes > 0 || chunks.is_empty() {
            chunks.push((current_vol, chunk_bytes));
        }
        Ok(chunks)
    }

    fn decode_next<R: BitRead>(
        &mut self,
        reader: &mut R,
        output_size: u64,
        unpacked_size: u64,
    ) -> RarResult<u64> {
        if self.st_mode {
            return self.huff_decode(reader, output_size, unpacked_size);
        }

        self.flags_cnt -= 1;
        if self.flags_cnt < 0 {
            self.get_flags_buf(reader)?;
            self.flags_cnt = 7;
        }

        if (self.flag_buf & 0x80) != 0 {
            self.flag_buf <<= 1;
            if self.nlzb > self.nhfb {
                self.long_lz(reader, output_size, unpacked_size)
            } else {
                self.huff_decode(reader, output_size, unpacked_size)
            }
        } else {
            self.flag_buf <<= 1;
            self.flags_cnt -= 1;
            if self.flags_cnt < 0 {
                self.get_flags_buf(reader)?;
                self.flags_cnt = 7;
            }
            if (self.flag_buf & 0x80) != 0 {
                self.flag_buf <<= 1;
                if self.nlzb > self.nhfb {
                    self.huff_decode(reader, output_size, unpacked_size)
                } else {
                    self.long_lz(reader, output_size, unpacked_size)
                }
            } else {
                self.flag_buf <<= 1;
                self.short_lz(reader, output_size, unpacked_size)
            }
        }
    }

    fn short_lz<R: BitRead>(
        &mut self,
        reader: &mut R,
        output_size: u64,
        unpacked_size: u64,
    ) -> RarResult<u64> {
        const SHORT_LEN1: [usize; 16] = [1, 3, 4, 4, 5, 6, 7, 8, 8, 4, 4, 5, 6, 6, 4, 0];
        const SHORT_XOR1: [u32; 15] = [
            0, 0xa0, 0xd0, 0xe0, 0xf0, 0xf8, 0xfc, 0xfe, 0xff, 0xc0, 0x80, 0x90, 0x98, 0x9c, 0xb0,
        ];
        const SHORT_LEN2: [usize; 16] = [2, 3, 3, 3, 4, 4, 5, 6, 6, 4, 4, 5, 6, 6, 4, 0];
        const SHORT_XOR2: [u32; 15] = [
            0, 0x40, 0x60, 0xa0, 0xd0, 0xe0, 0xf0, 0xf8, 0xfc, 0xc0, 0x80, 0x90, 0x98, 0x9c, 0xb0,
        ];

        self.num_huf = 0;
        let mut bit_field = reader.peek_16_raw()?;
        if self.l_count == 2 {
            reader.consume_bits(1)?;
            if bit_field >= 0x8000 {
                return self.copy_string15(
                    self.last_dist,
                    self.last_length,
                    output_size,
                    unpacked_size,
                );
            }
            bit_field <<= 1;
            self.l_count = 0;
        }

        bit_field >>= 8;
        let mut length = 0usize;
        if self.avr_ln1 < 37 {
            loop {
                let len = if length == 1 {
                    self.buf60 + 3
                } else {
                    SHORT_LEN1[length]
                };
                if ((bit_field ^ SHORT_XOR1[length]) & (!(0xffu32 >> len))) == 0 {
                    reader.consume_bits(len as u8)?;
                    break;
                }
                length += 1;
            }
        } else {
            loop {
                let len = if length == 3 {
                    self.buf60 + 3
                } else {
                    SHORT_LEN2[length]
                };
                if ((bit_field ^ SHORT_XOR2[length]) & (!(0xffu32 >> len))) == 0 {
                    reader.consume_bits(len as u8)?;
                    break;
                }
                length += 1;
            }
        }

        if length >= 9 {
            if length == 9 {
                self.l_count += 1;
                return self.copy_string15(
                    self.last_dist,
                    self.last_length,
                    output_size,
                    unpacked_size,
                );
            }
            if length == 14 {
                self.l_count = 0;
                let length = decode_num(reader, STARTL2, &DECL2, &POSL2)? + 5;
                let distance = ((reader.peek_16_raw()? >> 1) | 0x8000) as usize;
                reader.consume_bits(15)?;
                self.last_length = length;
                self.last_dist = distance;
                return self.copy_string15(distance, length, output_size, unpacked_size);
            }

            self.l_count = 0;
            let save_length = length;
            let distance = self.old_dist[(self.old_dist_ptr + 4 - (length - 9)) & 3];
            let mut length = decode_num(reader, STARTL1, &DECL1, &POSL1)? + 2;
            if length == 0x101 && save_length == 10 {
                self.buf60 ^= 1;
                return Ok(output_size);
            }
            if distance > 256 {
                length += 1;
            }
            if distance >= self.max_dist3 {
                length += 1;
            }
            self.insert_old_dist(distance);
            self.last_length = length;
            self.last_dist = distance;
            return self.copy_string15(distance, length, output_size, unpacked_size);
        }

        self.l_count = 0;
        self.avr_ln1 += length as u32;
        self.avr_ln1 -= self.avr_ln1 >> 4;

        let distance_place = decode_num(reader, STARTHF2, &DECHF2, &POSHF2)? & 0xff;
        let mut distance = self.ch_set_a[distance_place] as usize;
        if distance_place > 0 {
            let new_place = distance_place - 1;
            let last_distance = self.ch_set_a[new_place];
            self.ch_set_a[new_place + 1] = last_distance;
            self.ch_set_a[new_place] = distance as u16;
        }
        length += 2;
        distance += 1;
        self.insert_old_dist(distance);
        self.last_length = length;
        self.last_dist = distance;
        self.copy_string15(distance, length, output_size, unpacked_size)
    }

    fn long_lz<R: BitRead>(
        &mut self,
        reader: &mut R,
        output_size: u64,
        unpacked_size: u64,
    ) -> RarResult<u64> {
        self.num_huf = 0;
        self.nlzb += 16;
        if self.nlzb > 0xff {
            self.nlzb = 0x90;
            self.nhfb >>= 1;
        }
        let old_avr2 = self.avr_ln2;

        let bit_field = reader.peek_16_raw()?;
        let mut length = if self.avr_ln2 >= 122 {
            decode_num_from(reader, bit_field, STARTL2, &DECL2, &POSL2)?
        } else if self.avr_ln2 >= 64 {
            decode_num_from(reader, bit_field, STARTL1, &DECL1, &POSL1)?
        } else if bit_field < 0x100 {
            reader.consume_bits(16)?;
            bit_field as usize
        } else {
            let mut len = 0usize;
            while ((bit_field << len) & 0x8000) == 0 {
                len += 1;
            }
            reader.consume_bits((len + 1) as u8)?;
            len
        };

        self.avr_ln2 += length as u32;
        self.avr_ln2 -= self.avr_ln2 >> 5;

        let bit_field = reader.peek_16_raw()?;
        let distance_place = if self.avr_plc_b > 0x28ff {
            decode_num_from(reader, bit_field, STARTHF2, &DECHF2, &POSHF2)?
        } else if self.avr_plc_b > 0x6ff {
            decode_num_from(reader, bit_field, STARTHF1, &DECHF1, &POSHF1)?
        } else {
            decode_num_from(reader, bit_field, STARTHF0, &DECHF0, &POSHF0)?
        };

        self.avr_plc_b += distance_place as u32;
        self.avr_plc_b -= self.avr_plc_b >> 8;
        let mut distance_place = distance_place & 0xff;
        let mut distance;
        let mut new_distance_place;
        loop {
            distance = self.ch_set_b[distance_place] as usize;
            new_distance_place = self.bump_place_b(distance & 0xff);
            distance = distance.wrapping_add(1);
            if (distance & 0xff) == 0 {
                Self::corr_huff(&mut self.ch_set_b, &mut self.n_to_pl_b);
            } else {
                break;
            }
            distance_place &= 0xff;
        }

        self.ch_set_b[distance_place] = self.ch_set_b[new_distance_place];
        self.ch_set_b[new_distance_place] = distance as u16;

        let distance = ((distance & 0xff00) | ((reader.peek_16_raw()? >> 8) as usize)) >> 1;
        reader.consume_bits(7)?;

        let old_avr3 = self.avr_ln3;
        if length != 1 && length != 4 {
            if length == 0 && distance <= self.max_dist3 {
                self.avr_ln3 += 1;
                self.avr_ln3 -= self.avr_ln3 >> 8;
            } else if self.avr_ln3 > 0 {
                self.avr_ln3 -= 1;
            }
        }
        length += 3;
        if distance >= self.max_dist3 {
            length += 1;
        }
        if distance <= 256 {
            length += 8;
        }
        if old_avr3 > 0xb0 || (self.avr_plc >= 0x2a00 && old_avr2 < 0x40) {
            self.max_dist3 = 0x7f00;
        } else {
            self.max_dist3 = 0x2001;
        }

        self.insert_old_dist(distance);
        self.last_length = length;
        self.last_dist = distance;
        self.copy_string15(distance, length, output_size, unpacked_size)
    }

    fn huff_decode<R: BitRead>(
        &mut self,
        reader: &mut R,
        output_size: u64,
        unpacked_size: u64,
    ) -> RarResult<u64> {
        let mut bit_field = reader.peek_16_raw()?;
        let mut byte_place = if self.avr_plc > 0x75ff {
            decode_num_from(reader, bit_field, STARTHF4, &DECHF4, &POSHF4)?
        } else if self.avr_plc > 0x5dff {
            decode_num_from(reader, bit_field, STARTHF3, &DECHF3, &POSHF3)?
        } else if self.avr_plc > 0x35ff {
            decode_num_from(reader, bit_field, STARTHF2, &DECHF2, &POSHF2)?
        } else if self.avr_plc > 0x0dff {
            decode_num_from(reader, bit_field, STARTHF1, &DECHF1, &POSHF1)?
        } else {
            decode_num_from(reader, bit_field, STARTHF0, &DECHF0, &POSHF0)?
        };
        byte_place &= 0xff;

        if self.st_mode {
            if byte_place == 0 && bit_field > 0xfff {
                byte_place = 0x100;
            }
            if byte_place == 0 {
                bit_field = reader.peek_16_raw()?;
                reader.consume_bits(1)?;
                if (bit_field & 0x8000) != 0 {
                    self.num_huf = 0;
                    self.st_mode = false;
                    return Ok(output_size);
                }
                let length = if (bit_field & 0x4000) != 0 { 4 } else { 3 };
                reader.consume_bits(1)?;
                let distance = decode_num(reader, STARTHF2, &DECHF2, &POSHF2)?;
                let distance = (distance << 5) | ((reader.peek_16_raw()? >> 11) as usize);
                reader.consume_bits(5)?;
                return self.copy_string15(distance, length, output_size, unpacked_size);
            }
            byte_place -= 1;
        } else {
            if self.num_huf >= 16 && self.flags_cnt == 0 {
                self.st_mode = true;
            }
            self.num_huf += 1;
        }

        self.avr_plc += byte_place as u32;
        self.avr_plc -= self.avr_plc >> 8;
        self.nhfb += 16;
        if self.nhfb > 0xff {
            self.nhfb = 0x90;
            self.nlzb >>= 1;
        }

        let byte = (self.ch_set[byte_place] >> 8) as u8;
        self.window.put_byte(byte);

        loop {
            let mut cur_byte = self.ch_set[byte_place] as usize;
            let new_byte_place = self.bump_place(cur_byte & 0xff);
            cur_byte = cur_byte.wrapping_add(1);
            if (cur_byte & 0xff) > 0xa1 {
                Self::corr_huff(&mut self.ch_set, &mut self.n_to_pl);
            } else {
                self.ch_set[byte_place] = self.ch_set[new_byte_place];
                self.ch_set[new_byte_place] = cur_byte as u16;
                break;
            }
        }

        Ok((output_size + 1).min(unpacked_size))
    }

    fn get_flags_buf<R: BitRead>(&mut self, reader: &mut R) -> RarResult<()> {
        let flags_place = decode_num(reader, STARTHF2, &DECHF2, &POSHF2)?;
        if flags_place >= self.ch_set_c.len() {
            return Ok(());
        }

        loop {
            let mut flags = self.ch_set_c[flags_place] as usize;
            self.flag_buf = (flags >> 8) as u16;
            let new_flags_place = self.bump_place_c(flags & 0xff);
            flags = flags.wrapping_add(1);
            if (flags & 0xff) == 0 {
                Self::corr_huff_257(&mut self.ch_set_c, &mut self.n_to_pl_c);
            } else {
                self.ch_set_c[flags_place] = self.ch_set_c[new_flags_place];
                self.ch_set_c[new_flags_place] = flags as u16;
                break;
            }
        }
        Ok(())
    }

    fn init_huff(&mut self) {
        for i in 0..256usize {
            self.ch_set[i] = (i << 8) as u16;
            self.ch_set_b[i] = (i << 8) as u16;
            self.ch_set_a[i] = i as u16;
            self.ch_set_c[i] = ((((!i).wrapping_add(1)) & 0xff) << 8) as u16;
        }
        self.ch_set_c[256] = 0;
        self.n_to_pl.fill(0);
        self.n_to_pl_b.fill(0);
        self.n_to_pl_c.fill(0);
        Self::corr_huff(&mut self.ch_set_b, &mut self.n_to_pl_b);
    }

    fn corr_huff(char_set: &mut [u16; 256], num_to_place: &mut [u8; 256]) {
        let mut pos = 0usize;
        for i in (0..=7usize).rev() {
            for _ in 0..32 {
                char_set[pos] = (char_set[pos] & !0xff) | i as u16;
                pos += 1;
            }
        }
        num_to_place.fill(0);
        for i in (0..=6usize).rev() {
            num_to_place[i] = ((7 - i) * 32) as u8;
        }
    }

    fn corr_huff_257(char_set: &mut [u16; 257], num_to_place: &mut [u8; 256]) {
        let mut tmp = [0u16; 256];
        tmp.copy_from_slice(&char_set[..256]);
        Self::corr_huff(&mut tmp, num_to_place);
        char_set[..256].copy_from_slice(&tmp);
    }

    fn copy_string15(
        &mut self,
        distance: usize,
        length: usize,
        output_size: u64,
        unpacked_size: u64,
    ) -> RarResult<u64> {
        copy_match(
            &mut self.window,
            distance,
            length,
            output_size,
            unpacked_size,
            true,
        )
    }

    fn insert_old_dist(&mut self, distance: usize) {
        self.old_dist[self.old_dist_ptr] = distance;
        self.old_dist_ptr = (self.old_dist_ptr + 1) & 3;
    }

    fn bump_place(&mut self, symbol: usize) -> usize {
        let place = self.n_to_pl[symbol] as usize;
        self.n_to_pl[symbol] = self.n_to_pl[symbol].wrapping_add(1);
        place
    }

    fn bump_place_b(&mut self, symbol: usize) -> usize {
        let place = self.n_to_pl_b[symbol] as usize;
        self.n_to_pl_b[symbol] = self.n_to_pl_b[symbol].wrapping_add(1);
        place
    }

    fn bump_place_c(&mut self, symbol: usize) -> usize {
        let place = self.n_to_pl_c[symbol] as usize;
        self.n_to_pl_c[symbol] = self.n_to_pl_c[symbol].wrapping_add(1);
        place
    }
}

fn decode_num<R: BitRead>(
    reader: &mut R,
    start_pos: usize,
    dec_tab: &[u32],
    pos_tab: &[usize],
) -> RarResult<usize> {
    let num = reader.peek_16_raw()?;
    decode_num_from(reader, num, start_pos, dec_tab, pos_tab)
}

fn decode_num_from<R: BitRead>(
    reader: &mut R,
    num: u32,
    mut start_pos: usize,
    dec_tab: &[u32],
    pos_tab: &[usize],
) -> RarResult<usize> {
    let num = num & 0xfff0;
    let mut i = 0usize;
    while i < dec_tab.len() && dec_tab[i] <= num {
        start_pos += 1;
        i += 1;
    }
    if start_pos >= pos_tab.len() || start_pos > 16 {
        return Err(RarError::CorruptArchive {
            detail: "RAR15: decode table position out of range".into(),
        });
    }
    reader.consume_bits(start_pos as u8)?;
    let prev = if i != 0 { dec_tab[i - 1] } else { 0 };
    Ok(((num - prev) >> (16 - start_pos)) as usize + pos_tab[start_pos])
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn old_rar_version_gate_matches_unrar_dispatch() {
        assert!(ensure_supported_rar4_version(13, 3).is_ok());
        assert!(ensure_supported_rar4_version(14, 3).is_ok());
        assert!(ensure_supported_rar4_version(15, 3).is_ok());
        assert!(ensure_supported_rar4_version(20, 3).is_ok());
        assert!(ensure_supported_rar4_version(26, 3).is_ok());
        assert!(ensure_supported_rar4_version(29, 3).is_ok());
        assert!(matches!(
            ensure_supported_rar4_version(12, 3),
            Err(RarError::UnsupportedCompression { version: 12, .. })
        ));
        assert!(matches!(
            ensure_supported_rar4_version(30, 3),
            Err(RarError::UnsupportedCompression { version: 30, .. })
        ));
    }

    #[test]
    fn effective_rar4_window_size_applies_unrar_minimum() {
        assert_eq!(effective_rar4_window_size(0), 0x40000);
        assert_eq!(effective_rar4_window_size(128 * 1024), 0x40000);
        assert_eq!(effective_rar4_window_size(512 * 1024), 512 * 1024);
    }

    #[test]
    fn rar20_audio_keeps_unrar_raw_unsigned_last_char_after_underflow() {
        let mut decoder = Rar20Decoder::try_new(0x40000).unwrap();
        assert_eq!(decoder.decode_audio(1), 0xff);
        assert_eq!(decoder.audio_vars[0].last_char, u32::MAX);
        assert_eq!(decoder.audio_vars[0].last_delta, -1);

        assert_eq!(decoder.decode_audio(0), 0xff);
        assert_eq!(decoder.audio_vars[0].last_char, 0xff);
        assert_eq!(decoder.audio_vars[0].last_delta, 0);
    }

    #[test]
    fn rar4_decoder_uses_unrar_minimum_window_for_legacy_methods() {
        match Rar4Decoder::new(20, 128 * 1024, 3).unwrap() {
            Rar4Decoder::V20(decoder) => assert_eq!(decoder.window.dict_size(), 0x40000),
            _ => panic!("expected RAR20 decoder"),
        }

        match Rar4Decoder::new(14, 128 * 1024, 3).unwrap() {
            Rar4Decoder::V15(decoder) => assert_eq!(decoder.window.dict_size(), 0x40000),
            _ => panic!("expected RAR15-family decoder"),
        }

        match Rar4Decoder::new(29, 128 * 1024, 3).unwrap() {
            Rar4Decoder::V29(decoder) => assert_eq!(decoder.window_size(), 0x40000),
            _ => panic!("expected RAR29 decoder"),
        }
    }

    #[test]
    fn rar4_decoder_rejects_future_old_format_unpack_versions_like_unrar() {
        assert!(matches!(
            Rar4Decoder::new(30, 128 * 1024, 3),
            Err(RarError::UnsupportedCompression { version: 30, .. })
        ));
    }

    #[test]
    fn direct_rar4_dictionary_limit_uses_effective_window_size() {
        let mut out = Vec::new();
        let result = decompress_rar4_to_writer(&[], 0, 29, 3, 128 * 1024, &mut out);

        assert!(result.is_ok());

        let result = decompress_rar4_to_writer(&[], 0, 29, 3, MAX_DICT_SIZE + 1, &mut out);

        assert!(matches!(
            result,
            Err(RarError::DictionaryTooLarge {
                size,
                max: MAX_DICT_SIZE
            }) if size == MAX_DICT_SIZE + 1
        ));
    }

    #[test]
    fn rar15_decode_num_matches_unrar_table_shape() {
        let data = [0x80, 0x00];
        let mut reader = BitReader::new(&data);
        let value = decode_num(&mut reader, STARTL1, &DECL1, &POSL1).unwrap();
        assert_eq!(value, 2);
        assert_eq!(reader.position(), 3);
    }

    #[test]
    fn rar20_rejects_truncated_table_instead_of_rar29_dispatch() {
        let mut out = Vec::new();
        let result = decompress_rar4_to_writer(&[], 1, 20, 3, 128 * 1024, &mut out);
        assert!(matches!(result, Err(RarError::CorruptArchive { .. })));
    }

    #[test]
    fn rar20_read_last_tables_ignores_short_tail_like_unrar() {
        let mut decoder = Rar20Decoder::try_new(128 * 1024).unwrap();
        let mut ld_lengths = vec![0u8; NC20];
        ld_lengths[269] = 1;
        decoder.ld_table = Some(HuffmanTable::build(&ld_lengths).unwrap());

        let mut reader = BitReader::new(&[0; 4]);

        decoder.read_last_tables(&mut reader).unwrap();

        assert_eq!(reader.position(), 0);
    }

    #[test]
    fn rar20_read_last_tables_ignores_streaming_short_tail_like_unrar() {
        let mut decoder = Rar20Decoder::try_new(128 * 1024).unwrap();
        let mut ld_lengths = vec![0u8; NC20];
        ld_lengths[269] = 1;
        decoder.ld_table = Some(HuffmanTable::build(&ld_lengths).unwrap());

        let cursor = Cursor::new(vec![0; 4]);
        let mut reader = StreamingBitReader::new(cursor);

        decoder.read_last_tables(&mut reader).unwrap();

        assert_eq!(reader.position(), 0);
    }

    #[test]
    fn rar20_read_last_tables_honors_trailing_table_marker() {
        let mut decoder = Rar20Decoder::try_new(128 * 1024).unwrap();
        let mut ld_lengths = vec![0u8; NC20];
        ld_lengths[269] = 1;
        decoder.ld_table = Some(HuffmanTable::build(&ld_lengths).unwrap());

        let mut reader = BitReader::new(&[0; 10]);
        let result = decoder.read_last_tables(&mut reader);

        assert!(result.is_err());
        assert!(reader.position() > 0);
    }
}
