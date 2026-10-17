// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A small RAR writer for the tests: stored files and directories, split
// across volumes at a data budget, AES through the formats' own key
// derivations, enciphered headers, a recovery record, and the shapes the
// store must hand to an engine (solid, compressed). Nothing here compresses.

use aws_lc_rs::cipher::{AES_128, AES_256, EncryptingKey, EncryptionContext, UnboundCipherKey};
use aws_lc_rs::iv::FixedLength;

use crate::grid::crc32;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Version {
    Rar4,
    Rar5,
}

#[derive(Clone, Debug)]
pub(crate) enum Item {
    File(&'static str, Vec<u8>),
    Dir(&'static str),
}

#[derive(Clone, Debug)]
pub(crate) struct Options {
    pub(crate) version: Version,
    // File data bytes per volume; `None` for one volume.
    pub(crate) split: Option<usize>,
    pub(crate) password: Option<&'static str>,
    pub(crate) encrypt_headers: bool,
    pub(crate) solid: bool,
    // The item written with a compression method instead of store.
    pub(crate) compressed: Option<usize>,
    pub(crate) recovery: bool,
    // RAR5: a password check value, and checksums keyed by the hash key.
    pub(crate) check: bool,
    pub(crate) hash_key: bool,
}

impl Options {
    pub(crate) fn new(version: Version) -> Self {
        Self {
            version,
            split: None,
            password: None,
            encrypt_headers: false,
            solid: false,
            compressed: None,
            recovery: false,
            check: true,
            hash_key: false,
        }
    }
}

// A cheap RAR5 derivation, so a test derives in microseconds.
pub(crate) const COUNT: u8 = 4;
pub(crate) const SALT5: [u8; 16] = *b"salt-for-a-test!";
pub(crate) const SALT4: [u8; 8] = *b"rar4salt";
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
    compressed: bool,
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
    fn end(&self, more: bool) -> Vec<u8>;
    // The bytes before the headers that are enciphered, after the main
    // header for RAR4, after the signature for RAR5.
    fn crypt_header(&self) -> Vec<u8>;
    fn seal_header(&self, header: Vec<u8>, index: usize) -> Vec<u8>;
    fn cipher(&self, plain: &[u8]) -> Vec<u8>;
    fn keyed(&self, crc: u32) -> u32;
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
        let method = if part.compressed { 3 } else { 0 };
        let solid = if self.options.solid { 0x40 } else { 0 };
        body.extend(vint(method << 7 | solid));
        body.extend(vint(1));
        body.extend(vint(part.name.len() as u64));
        body.extend_from_slice(part.name.as_bytes());
        let mut extra = Vec::new();
        if self.keys.is_some() && kind == 2 && !part.dir {
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
                compressed: false,
            },
        )
    }

    fn end(&self, more: bool) -> Vec<u8> {
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

    fn cipher(&self, plain: &[u8]) -> Vec<u8> {
        match &self.keys {
            Some(keys) => encipher(&keys.key, IV5, plain),
            None => plain.to_vec(),
        }
    }

    fn keyed(&self, crc: u32) -> u32 {
        match (&self.keys, self.options.hash_key) {
            (Some(keys), true) => unrar_rs::convert_crc32_to_mac(crc, &keys.hash_key),
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
        let encrypted = self.options.password.is_some() && kind == 0x74 && !part.dir;
        if encrypted {
            flags |= 0x4 | 0x400;
        }
        if self.options.solid {
            flags |= 0x10;
        }
        let mut body = (part.data.len() as u32).to_le_bytes().to_vec();
        body.extend((part.length as u32).to_le_bytes());
        body.push(3);
        body.extend(part.crc.to_le_bytes());
        body.extend(0x5a21_0000u32.to_le_bytes());
        body.push(29);
        body.push(if part.compressed { 0x33 } else { 0x30 });
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
                compressed: false,
            },
        )
    }

    fn end(&self, more: bool) -> Vec<u8> {
        Self::block(0x7b, u16::from(more), &[])
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

    fn cipher(&self, plain: &[u8]) -> Vec<u8> {
        match self.options.password {
            Some(_) => {
                let (key, iv) = self.keys(&SALT4);
                encipher(&key, iv, plain)
            }
            None => plain.to_vec(),
        }
    }

    fn keyed(&self, crc: u32) -> u32 {
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
            let header = self.layout.recovery(&data);
            let header = self.seal(header);
            self.current.extend(header);
            self.current.extend(data);
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
        let end = self.layout.end(more);
        let end = self.seal(end);
        self.current.extend(end);
        self.done.push(std::mem::take(&mut self.current));
    }

    fn room(&self) -> usize {
        self.options
            .split
            .map_or(usize::MAX, |split| split.saturating_sub(self.used))
    }
}

pub(crate) fn write(items: &[Item], options: &Options) -> Vec<Vec<u8>> {
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
        let (name, dir, plain) = match item {
            Item::File(name, bytes) => (*name, false, bytes.as_slice()),
            Item::Dir(name) => (*name, true, &[][..]),
        };
        let stream = layout.cipher(plain);
        let whole = layout.keyed(crc32(plain));
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
                    layout.keyed(crc32(data))
                },
                before: offset > 0,
                after: !last,
                compressed: options.compressed == Some(index),
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
