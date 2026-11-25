// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A small RAR writer for the tests: stored files and directories, split
// across volumes at a data budget, AES through the formats' own key
// derivations, enciphered headers, a recovery record, and the shapes the
// store must hand to an engine (solid, compressed). A packed item carries
// data a real RAR coded, or data `literal` coded: every byte a literal, no
// match, so any bytes can be a compressed member.

use aws_lc_rs::cipher::{AES_128, AES_256, EncryptingKey, EncryptionContext, UnboundCipherKey};
use aws_lc_rs::iv::FixedLength;

use std::io::Cursor;
use std::num::NonZeroUsize;

use crate::grid::crc32;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Version {
    Rar4,
    Rar5,
}

#[derive(Clone, Debug)]
pub enum Item {
    File(&'static str, Vec<u8>),
    Dir(&'static str),
    // A file whose data is `packed`, coded as `codec` says: packed bytes a
    // real RAR wrote, lifted from a fixture, so a decoder makes `plain`.
    Packed {
        name: &'static str,
        plain: Vec<u8>,
        packed: Vec<u8>,
        codec: Codec,
    },
}

// How a file's data is coded: a RAR method (1 to 5), the RAR5 algorithm
// version, and the dictionary its header names, a power of two from the
// format's least (128 KiB for RAR5, 64 KiB for RAR4).
#[derive(Clone, Copy, Debug)]
pub struct Codec {
    pub method: u8,
    pub version: u8,
    pub dict: u64,
}

impl Codec {
    // What `options.compressed` names: method 3 at the least dictionary.
    const MARKED: Codec = Codec {
        method: 3,
        version: 0,
        dict: 0,
    };

    // The dictionary's bits above `least`.
    fn dict_bits(&self, least: u64) -> u64 {
        match self.dict {
            0 => 0,
            dict => {
                assert!(dict.is_power_of_two() && dict >= least, "{dict}");
                u64::from((dict / least).trailing_zeros())
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct Options {
    pub version: Version,
    // File data bytes per volume; `None` for one volume.
    pub split: Option<usize>,
    pub password: Option<&'static str>,
    pub encrypt_headers: bool,
    pub solid: bool,
    // The item written with a compression method instead of store.
    pub compressed: Option<usize>,
    pub recovery: bool,
    // The items written in the clear in a set whose files are enciphered.
    pub clear: Vec<usize>,
    // How many recovery headers each volume carries when it has a record.
    pub recovery_headers: usize,
    // RAR5: a password check value, and checksums keyed by the hash key.
    pub check: bool,
    pub hash_key: bool,
    // RAR4: each volume's end record states its number, as rar writes a
    // set's volumes; without it a RAR4 volume states no number anywhere.
    pub numbered: bool,
    // The last volume ends with its end record, as rar writes it; without
    // it the set ends where its last file's data does.
    pub ended: bool,
}

impl Options {
    pub fn new(version: Version) -> Self {
        Self {
            version,
            split: None,
            password: None,
            encrypt_headers: false,
            solid: false,
            compressed: None,
            recovery: false,
            clear: Vec::new(),
            recovery_headers: 1,
            check: true,
            hash_key: false,
            numbered: false,
            ended: true,
        }
    }
}

// A cheap RAR5 derivation, so a test derives in microseconds.
pub const COUNT: u8 = 4;
pub const SALT5: [u8; 16] = *b"salt-for-a-test!";
pub const SALT4: [u8; 8] = *b"rar4salt";
const IV5: [u8; 16] = *b"iv-for-each-file";

fn encipher(key: &[u8], iv: [u8; 16], plain: &[u8]) -> Vec<u8> {
    let mut blocks = plain.to_vec();
    blocks.resize(plain.len().div_ceil(16) * 16, 0);
    if blocks.is_empty() {
        return blocks;
    }
    let algorithm = if key.len() == 16 { &AES_128 } else { &AES_256 };
    let key = EncryptingKey::cbc(UnboundCipherKey::new(algorithm, key).unwrap()).unwrap();
    key.less_safe_encrypt(&mut blocks, EncryptionContext::Iv128(FixedLength::from(iv)))
        .unwrap();
    blocks
}

fn vint(mut value: u64) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return out;
        }
        out.push(byte | 0x80);
    }
}

// One part of a file in one volume.
struct Part<'a> {
    name: &'a str,
    dir: bool,
    length: u64,
    data: &'a [u8],
    crc: u32,
    before: bool,
    after: bool,
    codec: Option<Codec>,
    // Written without the password in a set whose files have one.
    clear: bool,
}

struct Keys5 {
    key: [u8; 32],
    hash_key: [u8; 32],
    check: [u8; 8],
}

trait Layout {
    fn signature(&self) -> &'static [u8];
    fn main(&self, volume: usize, multi: bool) -> Vec<u8>;
    fn file(&self, part: &Part<'_>) -> Vec<u8>;
    fn recovery(&self, data: &[u8]) -> Vec<u8>;
    fn end(&self, more: bool, volume: usize) -> Vec<u8>;
    // The bytes before the headers that are enciphered, after the main
    // header for RAR4, after the signature for RAR5.
    fn crypt_header(&self) -> Vec<u8>;
    fn seal_header(&self, header: Vec<u8>, index: usize) -> Vec<u8>;
    fn cipher(&self, plain: &[u8], clear: bool) -> Vec<u8>;
    fn keyed(&self, crc: u32, clear: bool) -> u32;
}

struct Rar5<'a> {
    options: &'a Options,
    keys: Option<Keys5>,
}

impl Rar5<'_> {
    fn header(kind: u64, flags: u64, extra: &[u8], data: Option<u64>, body: &[u8]) -> Vec<u8> {
        let mut flags = flags;
        if !extra.is_empty() {
            flags |= 0x1;
        }
        if data.is_some() {
            flags |= 0x2;
        }
        let mut inner = vint(kind);
        inner.extend(vint(flags));
        if !extra.is_empty() {
            inner.extend(vint(extra.len() as u64));
        }
        if let Some(data) = data {
            inner.extend(vint(data));
        }
        inner.extend_from_slice(body);
        inner.extend_from_slice(extra);
        let mut sized = vint(inner.len() as u64);
        sized.extend(inner);
        let mut out = crc32(&sized).to_le_bytes().to_vec();
        out.extend(sized);
        out
    }

    fn check_record(&self) -> Vec<u8> {
        let keys = self.keys.as_ref().unwrap();
        let mut out = keys.check.to_vec();
        let digest = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, &keys.check);
        out.extend_from_slice(&digest.as_ref()[..4]);
        out
    }

    fn file_like(&self, kind: u64, part: &Part<'_>) -> Vec<u8> {
        let mut file_flags = 0x4;
        if part.dir {
            file_flags |= 0x1;
        }
        let mut body = vint(file_flags);
        body.extend(vint(part.length));
        body.extend(vint(0));
        body.extend(part.crc.to_le_bytes());
        let info = part.codec.map_or(0, |codec| {
            u64::from(codec.version)
                | u64::from(codec.method) << 7
                | codec.dict_bits(128 << 10) << 10
        });
        let solid = if self.options.solid { 0x40 } else { 0 };
        body.extend(vint(info | solid));
        body.extend(vint(1));
        body.extend(vint(part.name.len() as u64));
        body.extend_from_slice(part.name.as_bytes());
        let mut extra = Vec::new();
        if self.keys.is_some() && kind == 2 && !part.dir && !part.clear {
            let mut flags = 0;
            if self.options.check {
                flags |= 0x1;
            }
            if self.options.hash_key {
                flags |= 0x2;
            }
            let mut record = vint(1);
            record.extend(vint(0));
            record.extend(vint(flags));
            record.push(COUNT);
            record.extend_from_slice(&SALT5);
            record.extend_from_slice(&IV5);
            if self.options.check {
                record.extend(self.check_record());
            }
            extra = vint(record.len() as u64);
            extra.extend(record);
        }
        let mut flags = 0;
        if part.before {
            flags |= 0x8;
        }
        if part.after {
            flags |= 0x10;
        }
        Self::header(kind, flags, &extra, Some(part.data.len() as u64), &body)
    }
}

impl Layout for Rar5<'_> {
    fn signature(&self) -> &'static [u8] {
        b"Rar!\x1A\x07\x01\x00"
    }

    fn main(&self, volume: usize, multi: bool) -> Vec<u8> {
        let mut flags = 0;
        if multi {
            flags |= 0x1;
        }
        if volume > 0 {
            flags |= 0x2;
        }
        if self.options.solid {
            flags |= 0x4;
        }
        if self.options.recovery {
            flags |= 0x8;
        }
        let mut body = vint(flags);
        if volume > 0 {
            body.extend(vint(volume as u64));
        }
        Self::header(1, 0, &[], None, &body)
    }

    fn file(&self, part: &Part<'_>) -> Vec<u8> {
        self.file_like(2, part)
    }

    fn recovery(&self, data: &[u8]) -> Vec<u8> {
        self.file_like(
            3,
            &Part {
                name: "RR",
                dir: false,
                length: data.len() as u64,
                data,
                crc: crc32(data),
                before: false,
                after: false,
                codec: None,
                clear: false,
            },
        )
    }

    fn end(&self, more: bool, _volume: usize) -> Vec<u8> {
        Self::header(5, 0, &[], None, &vint(u64::from(more)))
    }

    fn crypt_header(&self) -> Vec<u8> {
        let mut body = vint(0);
        body.extend(vint(u64::from(self.options.check)));
        body.push(COUNT);
        body.extend_from_slice(&SALT5);
        if self.options.check {
            body.extend(self.check_record());
        }
        Self::header(4, 0, &[], None, &body)
    }

    fn seal_header(&self, header: Vec<u8>, index: usize) -> Vec<u8> {
        let keys = self.keys.as_ref().unwrap();
        let mut iv = [0u8; 16];
        iv[..8].copy_from_slice(&(index as u64).to_le_bytes());
        iv[8..].copy_from_slice(b"headeriv");
        let mut out = iv.to_vec();
        out.extend(encipher(&keys.key, iv, &header));
        out
    }

    fn cipher(&self, plain: &[u8], clear: bool) -> Vec<u8> {
        match &self.keys {
            Some(keys) if !clear => encipher(&keys.key, IV5, plain),
            _ => plain.to_vec(),
        }
    }

    fn keyed(&self, crc: u32, clear: bool) -> u32 {
        match (&self.keys, self.options.hash_key) {
            (Some(keys), true) if !clear => unrar_rs::convert_crc32_to_mac(crc, &keys.hash_key),
            _ => crc,
        }
    }
}

struct Rar4<'a> {
    options: &'a Options,
}

impl Rar4<'_> {
    fn block(kind: u8, flags: u16, body: &[u8]) -> Vec<u8> {
        let size = (7 + body.len()) as u16;
        let mut rest = vec![kind];
        rest.extend(flags.to_le_bytes());
        rest.extend(size.to_le_bytes());
        rest.extend_from_slice(body);
        let mut out = (crc32(&rest) as u16).to_le_bytes().to_vec();
        out.extend(rest);
        out
    }

    fn file_like(&self, kind: u8, part: &Part<'_>) -> Vec<u8> {
        let mut flags = 0x8000;
        if part.before {
            flags |= 0x1;
        }
        if part.after {
            flags |= 0x2;
        }
        if part.dir {
            flags |= 0xe0;
        }
        let encrypted = self.options.password.is_some() && kind == 0x74 && !part.dir && !part.clear;
        if encrypted {
            flags |= 0x4 | 0x400;
        }
        if self.options.solid {
            flags |= 0x10;
        }
        if let Some(codec) = part.codec {
            flags |= (codec.dict_bits(64 << 10) as u16) << 5;
        }
        let mut body = (part.data.len() as u32).to_le_bytes().to_vec();
        body.extend((part.length as u32).to_le_bytes());
        body.push(3);
        body.extend(part.crc.to_le_bytes());
        body.extend(0x5a21_0000u32.to_le_bytes());
        body.push(29);
        body.push(0x30 + part.codec.map_or(0, |codec| codec.method));
        body.extend((part.name.len() as u16).to_le_bytes());
        body.extend(0u32.to_le_bytes());
        body.extend_from_slice(part.name.as_bytes());
        if encrypted {
            body.extend_from_slice(&SALT4);
        }
        Self::block(kind, flags, &body)
    }

    // The AES-128 key and the IV.
    fn keys(&self, salt: &[u8; 8]) -> ([u8; 16], [u8; 16]) {
        unrar_rs::KdfCache::new().derive_key_rar4(self.options.password.unwrap(), Some(salt))
    }
}

impl Layout for Rar4<'_> {
    fn signature(&self) -> &'static [u8] {
        b"Rar!\x1A\x07\x00"
    }

    fn main(&self, volume: usize, multi: bool) -> Vec<u8> {
        let mut flags = 0x10;
        if multi {
            flags |= 0x1;
        }
        if volume == 0 {
            flags |= 0x100;
        }
        if self.options.solid {
            flags |= 0x8;
        }
        if self.options.encrypt_headers {
            flags |= 0x80;
        }
        Self::block(0x73, flags, &[0; 6])
    }

    fn file(&self, part: &Part<'_>) -> Vec<u8> {
        self.file_like(0x74, part)
    }

    fn recovery(&self, data: &[u8]) -> Vec<u8> {
        self.file_like(
            0x7a,
            &Part {
                name: "RR",
                dir: false,
                length: data.len() as u64,
                data,
                crc: crc32(data),
                before: false,
                after: false,
                codec: None,
                clear: false,
            },
        )
    }

    fn end(&self, more: bool, volume: usize) -> Vec<u8> {
        match self.options.numbered {
            true => Self::block(0x7b, u16::from(more) | 0x8, &(volume as u16).to_le_bytes()),
            false => Self::block(0x7b, u16::from(more), &[]),
        }
    }

    fn crypt_header(&self) -> Vec<u8> {
        Vec::new()
    }

    fn seal_header(&self, header: Vec<u8>, _index: usize) -> Vec<u8> {
        let (key, iv) = self.keys(&SALT4);
        let mut out = SALT4.to_vec();
        out.extend(encipher(&key, iv, &header));
        out
    }

    fn cipher(&self, plain: &[u8], clear: bool) -> Vec<u8> {
        match self.options.password {
            Some(_) if !clear => {
                let (key, iv) = self.keys(&SALT4);
                encipher(&key, iv, plain)
            }
            _ => plain.to_vec(),
        }
    }

    fn keyed(&self, crc: u32, _clear: bool) -> u32 {
        crc
    }
}

struct Volumes<'a> {
    layout: &'a dyn Layout,
    options: &'a Options,
    multi: bool,
    done: Vec<Vec<u8>>,
    current: Vec<u8>,
    sealed: usize,
    used: usize,
}

impl Volumes<'_> {
    fn start(&mut self) {
        let volume = self.done.len();
        self.current = self.layout.signature().to_vec();
        let main = self.layout.main(volume, self.multi);
        let rar5 = self.options.version == Version::Rar5;
        if self.options.encrypt_headers && rar5 {
            self.current.extend(self.layout.crypt_header());
            let sealed = self.seal(main);
            self.current.extend(sealed);
        } else {
            self.current.extend(main);
        }
        if self.options.recovery {
            let data = vec![0xa5; 40 + volume];
            for _ in 0..self.options.recovery_headers {
                let header = self.layout.recovery(&data);
                let header = self.seal(header);
                self.current.extend(header);
                self.current.extend(&data);
            }
        }
        self.used = 0;
    }

    fn seal(&mut self, header: Vec<u8>) -> Vec<u8> {
        if !self.options.encrypt_headers {
            return header;
        }
        self.sealed += 1;
        self.layout.seal_header(header, self.sealed)
    }

    fn header(&mut self, header: Vec<u8>, data: &[u8]) {
        let header = self.seal(header);
        self.current.extend(header);
        self.current.extend_from_slice(data);
    }

    fn finish(&mut self, more: bool) {
        if more || self.options.ended {
            let end = self.layout.end(more, self.done.len());
            let end = self.seal(end);
            self.current.extend(end);
        }
        self.done.push(std::mem::take(&mut self.current));
    }

    fn room(&self) -> usize {
        self.options
            .split
            .map_or(usize::MAX, |split| split.saturating_sub(self.used))
    }
}

pub fn write(items: &[Item], options: &Options) -> Vec<Vec<u8>> {
    let rar5;
    let rar4;
    let layout: &dyn Layout = match options.version {
        Version::Rar5 => {
            let keys = options.password.map(|password| {
                let derived = unrar_rs::KdfCache::new()
                    .derive_material_rar5(password, &SALT5, COUNT)
                    .unwrap();
                Keys5 {
                    key: derived.key,
                    hash_key: derived.hash_key,
                    check: derived.psw_check,
                }
            });
            rar5 = Rar5 { options, keys };
            &rar5
        }
        Version::Rar4 => {
            rar4 = Rar4 { options };
            &rar4
        }
    };
    let mut volumes = Volumes {
        layout,
        options,
        multi: options.split.is_some(),
        done: Vec::new(),
        current: Vec::new(),
        sealed: 0,
        used: 0,
    };
    volumes.start();
    for (index, item) in items.iter().enumerate() {
        let (name, dir, plain, packed) = match item {
            Item::File(name, bytes) => (*name, false, bytes.as_slice(), None),
            Item::Dir(name) => (*name, true, &[][..], None),
            Item::Packed {
                name,
                plain,
                packed,
                codec,
            } => (
                *name,
                false,
                plain.as_slice(),
                Some((packed.as_slice(), *codec)),
            ),
        };
        let codec = packed
            .map(|(_, codec)| codec)
            .or((options.compressed == Some(index)).then_some(Codec::MARKED));
        let clear = options.clear.contains(&index);
        let stream = layout.cipher(packed.map_or(plain, |(packed, _)| packed), clear);
        let whole = layout.keyed(crc32(plain), clear);
        let mut offset = 0;
        loop {
            if volumes.room() == 0 && offset < stream.len() {
                volumes.finish(true);
                volumes.start();
            }
            let take = (stream.len() - offset).min(volumes.room());
            let data = &stream[offset..offset + take];
            let last = offset + take == stream.len();
            let part = Part {
                name,
                dir,
                length: plain.len() as u64,
                data,
                crc: if last {
                    whole
                } else {
                    layout.keyed(crc32(data), clear)
                },
                before: offset > 0,
                after: !last,
                codec,
                clear,
            };
            let header = layout.file(&part);
            volumes.header(header, data);
            volumes.used += take;
            offset += take;
            if last {
                break;
            }
        }
    }
    volumes.finish(false);
    volumes.done
}

// A set's volumes as unrar reads them: what it opens a member from.
struct Memory(Vec<Vec<u8>>);

impl unrar_rs::VolumeProvider for Memory {
    fn get_volume(
        &self,
        index: usize,
    ) -> Result<Box<dyn unrar_rs::ReadSeek>, unrar_rs::VolumeProviderError> {
        self.0
            .get(index)
            .map(|bytes| Box::new(Cursor::new(bytes.clone())) as Box<dyn unrar_rs::ReadSeek>)
            .ok_or(unrar_rs::VolumeProviderError::Unavailable {
                volume: index,
                reason: "no such volume".into(),
            })
    }
}

pub fn unrar(volumes: &[Vec<u8>], password: Option<&str>) -> Vec<(String, Vec<u8>)> {
    let mut archive = unrar_rs::RarArchive::open_prefix(
        Cursor::new(volumes[0].clone()),
        password,
        NonZeroUsize::MIN,
    )
    .unwrap();
    archive.set_verify(true);
    for (volume, bytes) in volumes.iter().enumerate() {
        let mut headers = NonZeroUsize::MIN;
        loop {
            let found = archive
                .extend_volume_prefix(volume, Box::new(Cursor::new(bytes.clone())), headers)
                .unwrap();
            if found < headers.get() {
                break;
            }
            headers = headers.checked_add(1).unwrap();
        }
    }
    let provider = Memory(volumes.to_vec());
    let mut out = Vec::new();
    for index in 0..archive.len() {
        let info = archive.member_info(index).unwrap();
        if info.is_directory {
            continue;
        }
        let mut bytes = Vec::new();
        archive
            .by_index_via(index, &provider)
            .unwrap()
            .copy_to(&mut bytes)
            .unwrap();
        out.push((info.raw_name, bytes));
    }
    out
}

// The compressed member `member` RAR itself wrote in a single-volume
// fixture: its plain bytes, packed bytes and how they are coded, to write
// again as an `Item::Packed`.
pub fn lifted(bytes: &[u8], member: &str) -> (Vec<u8>, Vec<u8>, Codec) {
    let extracted = unrar(&[bytes.to_vec()], None);
    let plain = extracted
        .into_iter()
        .find(|(name, _)| name == member)
        .unwrap_or_else(|| panic!("no member {member}"))
        .1;
    let facts = unrar_rs::RarArchive::parse_volume_facts_walk_with_shared_kdf_cache(
        Cursor::new(bytes.to_vec()),
        None,
        std::sync::Arc::new(unrar_rs::KdfCache::new()),
    )
    .expect("a RAR volume")
    .facts;
    assert!(!facts.is_solid);
    let found = facts
        .members
        .iter()
        .find(|part| part.name == member)
        .unwrap_or_else(|| panic!("no header {member}"));
    assert!(found.compression_method != 0 && !found.compression_solid);
    let at = found.data_offset as usize;
    let packed = bytes[at..at + found.data_size as usize].to_vec();
    let codec = Codec {
        method: found.compression_method,
        version: found.compression_version,
        dict: found.dict_size,
    };
    (plain, packed, codec)
}

// MSB-first bits, as both formats' decoders read them.
#[derive(Default)]
struct Bits {
    bytes: Vec<u8>,
    used: usize,
}

impl Bits {
    fn put(&mut self, value: u32, count: usize) {
        for bit in (0..count).rev() {
            if self.used.is_multiple_of(8) {
                self.bytes.push(0);
            }
            if value >> bit & 1 == 1 {
                *self.bytes.last_mut().unwrap() |= 0x80 >> (self.used % 8);
            }
            self.used += 1;
        }
    }
}

// The code lengths of a table of `total` symbols in which the 256 literals
// are 8 bits long and nothing else has a code: each literal's canonical code
// is its own byte. Coded with the bit-length codes both formats share: a
// length, 17 to repeat the one before (7 bits, 11 more), 18 and 19 for runs
// of zeros (3 bits, 3 more; 7 bits, 11 more). The bit-length table gives
// 8, 17, 18 and 19 two bits each, so their codes are 00, 01, 10 and 11.
fn literal_tables(bits: &mut Bits, total: usize) {
    for symbol in 0..20u32 {
        bits.put(u32::from(matches!(symbol, 8 | 17 | 18 | 19)) * 2, 4);
    }
    bits.put(0b00, 2);
    for run in [138, 117] {
        bits.put(0b01, 2);
        bits.put(run - 11, 7);
    }
    let mut zeros = total as u32 - 256;
    while zeros > 0 {
        if zeros >= 11 {
            let run = zeros.min(138);
            bits.put(0b11, 2);
            bits.put(run - 11, 7);
            zeros -= run;
        } else {
            assert!(zeros >= 3, "{zeros}");
            bits.put(0b10, 2);
            bits.put(zeros - 3, 3);
            zeros = 0;
        }
    }
}

// `plain` coded as `version` codes a compressed file, every byte a literal:
// an item the store must leave to a run, which any decoder makes `plain`
// from. RAR5: one last block with its tables (306 + 64 + 16 + 44 lengths)
// under a header whose checksum is 0x5A over its flags and size bytes.
// RAR4: an LZ block that resets the tables (299 + 60 + 17 + 28 lengths).
// Both stop at the file's unpacked size.
pub fn literal(version: Version, name: &'static str, plain: &[u8]) -> Item {
    let mut bits = Bits::default();
    let packed = match version {
        Version::Rar5 => {
            literal_tables(&mut bits, 430);
            for &byte in plain {
                bits.put(u32::from(byte), 8);
            }
            let size = bits.bytes.len();
            let last_bits = bits.used - (size - 1) * 8;
            let size_bytes = (size as u32).to_le_bytes();
            let count = match size {
                0..=0xff => 1,
                0x100..=0xffff => 2,
                _ => 3,
            };
            assert!(size < 1 << 24, "{size}");
            let flags = 0x80 | 0x40 | ((count - 1) << 3) as u8 | (last_bits - 1) as u8;
            let checksum = size_bytes[..count]
                .iter()
                .fold(0x5a ^ flags, |sum, byte| sum ^ byte);
            let mut packed = vec![flags, checksum];
            packed.extend_from_slice(&size_bytes[..count]);
            packed.extend(bits.bytes);
            packed
        }
        Version::Rar4 => {
            bits.put(0b00, 2);
            literal_tables(&mut bits, 404);
            for &byte in plain {
                bits.put(u32::from(byte), 8);
            }
            bits.bytes
        }
    };
    let codec = Codec {
        method: 3,
        version: match version {
            Version::Rar5 => 0,
            Version::Rar4 => 29,
        },
        dict: 0,
    };
    Item::Packed {
        name,
        plain: plain.to_vec(),
        packed,
        codec,
    }
}
