// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What a RAR header says, in one shape for both formats. RAR4 and RAR5 lay
// their headers out differently, but the store needs the same few facts from
// either: what kind of block it is, where its data lies, and, for a file,
// whether its packed bytes are its own bytes.

pub(super) const RAR4: &[u8; 7] = b"Rar!\x1A\x07\x00";
pub(super) const RAR5: &[u8; 8] = b"Rar!\x1A\x07\x01\x00";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Format {
    Rar4,
    Rar5,
}

impl Format {
    pub(super) fn signature_len(self) -> u64 {
        match self {
            Format::Rar4 => RAR4.len() as u64,
            Format::Rar5 => RAR5.len() as u64,
        }
    }
}

// What a volume's first bytes are.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Signature {
    Short,
    Format(Format),
    Foreign,
}

pub(super) fn signature(bytes: &[u8]) -> Signature {
    if bytes.starts_with(RAR4) {
        Signature::Format(Format::Rar4)
    } else if bytes.starts_with(RAR5) {
        Signature::Format(Format::Rar5)
    } else if RAR4.starts_with(bytes) || RAR5.starts_with(bytes) {
        Signature::Short
    } else {
        Signature::Foreign
    }
}

// A header read from plaintext bytes: its length, the data area after it,
// and what it is.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Header {
    pub(super) len: u64,
    pub(super) data: u64,
    pub(super) what: What,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum What {
    Main(Main),
    File(File),
    // A recovery record: its data is Reed-Solomon over the volume.
    Recovery,
    // RAR5's archive encryption header: every header after it is enciphered.
    Crypt(HeaderCrypt),
    End { more: bool },
    // Comments, Quick Open, streams, ACLs: envelope.
    Other,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct Main {
    pub(super) volume: bool,
    // The volume this is, counted from zero, when the header says.
    pub(super) number: Option<u32>,
    pub(super) solid: bool,
    // RAR4's MHD_PASSWORD: every block after the main header is enciphered.
    pub(super) encrypted: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct File {
    pub(super) name: String,
    pub(super) dir: bool,
    pub(super) length: Option<u64>,
    pub(super) crc: Option<u32>,
    pub(super) before: bool,
    pub(super) after: bool,
    // Method store and not solid: the packed bytes are the file's bytes, or
    // their cipher.
    pub(super) stored: bool,
    pub(super) crypt: Option<FileCrypt>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum FileCrypt {
    Rar5 {
        count: u8,
        salt: [u8; 16],
        iv: [u8; 16],
        check: Option<[u8; 8]>,
        // The checksums are keyed, so they say nothing without the key.
        hash_key: bool,
    },
    Rar4 {
        salt: [u8; 8],
    },
    // A cipher this store does not implement.
    Other,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct HeaderCrypt {
    pub(super) count: u8,
    pub(super) salt: [u8; 16],
    pub(super) check: Option<[u8; 8]>,
}

// What reading one header from plaintext bytes found.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Read {
    // The header is at least this long; hand that many bytes.
    Short(u64),
    // The header's checksum failed over this many bytes.
    Bad(u64),
    Header(Header),
    // The checksum passed but the fields make no sense.
    Broken,
}

// RAR5 stores a password check as eight bytes and a four-byte digest of
// them; a check whose digest fails is damaged and proves nothing.
pub(super) fn check_value(bytes: [u8; 12]) -> Option<[u8; 8]> {
    let mut check = [0; 8];
    check.copy_from_slice(&bytes[..8]);
    let digest = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, &check);
    (digest.as_ref()[..4] == bytes[8..]).then_some(check)
}

// A name as the namespace numbers it: forward slashes, a directory with a
// trailing one, as the 7z store names them.
pub(super) fn member_name(raw: &str, dir: bool) -> String {
    let mut name = raw.replace('\\', "/");
    if dir && !name.ends_with('/') {
        name.push('/');
    }
    name
}
