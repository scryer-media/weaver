// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The sniffer: which object owns a new member, judged from its name and its
// first bytes. It knows signatures and file types and nothing else. Sizes,
// volume numbering and set grouping belong to the objects it names.
//
// Bytes decide wherever a kind has a signature, so a member keeps its kind
// under an obfuscated name. A name decides only once the bytes have ruled
// out every signature: tar, bzip2 and zip by their extensions too, and raw
// deflate, brotli and SFV, which have no signature, by theirs alone, as is
// the first numbered piece of a plain file split into pieces.
//
// Bytes read before a verdict, per kind:
//
//   tar 263   the ustar magic at 257 and its terminator; until then any
//             member might be a tar
//   7z 32     the signature header, whose major version must be one this reads
//             and whose start header must match its CRC
//   PAR2 64   the first packet header, whose length must be a packet's
//   bzip2 4   `BZh` and a block-size level
//   RAR5 8, RAR4 7, RAR5 recovery 8, PAR3 8, xz 6, zip 4, zstd 4, gzip 2   the signature
//
// Each is cut short by bytes that already contradict the signature, and by
// a known length too small to hold it.
//
// Engines (`RarEngine`, `SevenZipEngine`, `ZipEngine`) are never sniffed: an
// engine is chosen when a store object refuses, not by looking at bytes.

use crate::object::Kind;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Sniff {
    // An object of this kind owns the member.
    Object(Kind),
    // No object does: the member's bytes go to a file.
    Leaf,
    // Some kind is still possible and needs more bytes to be ruled in or out.
    Pending,
}

pub fn sniff(name: &str, first_bytes: &[u8], length: Option<u64>) -> Sniff {
    let mut waiting = false;
    for signature in SIGNATURES {
        match signature.test(first_bytes, length) {
            Test::Yes => return Sniff::Object(signature.kind),
            Test::Undecided => waiting = true,
            Test::No => {}
        }
    }
    // A name never outvotes bytes that might still turn out to be a signature.
    if waiting {
        return Sniff::Pending;
    }
    named(name).map_or(Sniff::Leaf, Sniff::Object)
}

struct Signature {
    kind: Kind,
    magic: &'static [u8],
    // How many bytes the verdict reads: the magic, or the whole header that
    // must hold before the kind is trusted.
    header: usize,
    holds: fn(&[u8], Option<u64>) -> bool,
    // Bytes short of the header that already rule the kind out.
    ruled_out: fn(&[u8]) -> bool,
}

enum Test {
    Yes,
    No,
    Undecided,
}

impl Signature {
    fn test(&self, seen: &[u8], length: Option<u64>) -> Test {
        if !self
            .magic
            .iter()
            .zip(seen)
            .all(|(expected, byte)| expected == byte)
            || (self.ruled_out)(seen)
        {
            return Test::No;
        }
        match seen.get(..self.header) {
            Some(header) if (self.holds)(header, length) => Test::Yes,
            Some(_) => Test::No,
            None if length.is_none_or(|length| length >= self.header as u64) => Test::Undecided,
            None => Test::No,
        }
    }
}

const fn signature(kind: Kind, magic: &'static [u8]) -> Signature {
    Signature {
        kind,
        magic,
        header: magic.len(),
        holds: |_, _| true,
        ruled_out: |_| false,
    }
}

const SEVEN_Z_SIGNATURE: [u8; 6] = [0x37, 0x7A, 0xBC, 0xAF, 0x27, 0x1C];
const SEVEN_Z_SIGNATURE_HEADER_LEN: usize = 32;
const PAR2_MAGIC: &[u8; 8] = b"PAR2\0PKT";
const PAR2_PACKET_ALIGNMENT: u64 = 4;
const PAR2_PACKET_HEADER_BYTES: usize = 64;
const USTAR_MAGIC_AT: usize = 257;
const USTAR_MAGIC: &[u8] = b"ustar";

// The most bytes any verdict reads. A contiguous front this long always
// decides, so this is how much of an undecided member is worth fetching.
pub const VERDICT_BYTES: usize = {
    let mut most = 0;
    let mut index = 0;
    while index < SIGNATURES.len() {
        if SIGNATURES[index].header > most {
            most = SIGNATURES[index].header;
        }
        index += 1;
    }
    most
};

// Every magic sits at offset zero but tar's, which its header check reads.
// No two of them agree on a whole magic across kinds, so the order of the
// table never changes a verdict.
const SIGNATURES: &[Signature] = &[
    signature(Kind::RarStore, b"Rar!\x1a\x07\x00"),
    signature(Kind::RarStore, b"Rar!\x1a\x07\x01\x00"),
    Signature {
        kind: Kind::SevenZipStore,
        magic: &SEVEN_Z_SIGNATURE,
        header: SEVEN_Z_SIGNATURE_HEADER_LEN,
        holds: sevenz_header_looks_valid,
        ruled_out: |_| false,
    },
    // A local file header, the end record alone of an empty archive, and the
    // marker that opens the first volume of a spanned archive.
    signature(Kind::Zip, b"PK\x03\x04"),
    signature(Kind::Zip, b"PK\x05\x06"),
    signature(Kind::Zip, b"PK\x07\x08"),
    signature(Kind::Xz, &[0xfd, b'7', b'z', b'X', b'Z', 0x00]),
    signature(Kind::Gzip, &[0x1f, 0x8b]),
    signature(Kind::Zstd, &[0x28, 0xb5, 0x2f, 0xfd]),
    Signature {
        kind: Kind::Bzip2,
        magic: b"BZh",
        header: 4,
        holds: |header, _| matches!(header.get(3), Some(b'1'..=b'9')),
        ruled_out: |_| false,
    },
    Signature {
        kind: Kind::Tar,
        magic: b"",
        header: USTAR_MAGIC_AT + USTAR_MAGIC.len() + 1,
        holds: ustar_header,
        ruled_out: tar_checksum_ruled_out,
    },
    Signature {
        kind: Kind::Par2,
        magic: PAR2_MAGIC,
        header: PAR2_PACKET_HEADER_BYTES,
        holds: par2_header_looks_valid,
        ruled_out: |_| false,
    },
    signature(Kind::Par3, b"PAR3\x00PKT"),
    // A RAR5 recovery volume. A RAR3 one opens with parity and is known by
    // its name alone.
    signature(Kind::RarRecovery, b"Rar!\x1aRev"),
];

// A major version this build does not know places its fields elsewhere, and
// a start header whose CRC fails is bytes that only open like a 7z.
fn sevenz_header_looks_valid(header: &[u8], _known_file_len: Option<u64>) -> bool {
    let stated_crc = header
        .get(8..12)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u32::from_le_bytes);
    header.get(6) == Some(&0)
        && header
            .get(12..)
            .is_some_and(|start_header| stated_crc == Some(crc32(start_header)))
}

fn crc32(bytes: &[u8]) -> u32 {
    crc_fast::checksum(crc_fast::CrcAlgorithm::Crc32IsoHdlc, bytes) as u32
}

// POSIX writes "ustar\0", GNU writes "ustar " before its own version.
fn ustar_header(header: &[u8], _known_file_len: Option<u64>) -> bool {
    let end = USTAR_MAGIC_AT + USTAR_MAGIC.len();
    header.get(USTAR_MAGIC_AT..end) == Some(USTAR_MAGIC)
        && matches!(header.get(end), Some(0 | b' '))
}

// The checksum field is octal text in every tar dialect, so a stray byte in
// it rules tar out long before the magic is due. Without that, every member
// would wait for its 263rd byte, which a lost article can withhold.
fn tar_checksum_ruled_out(seen: &[u8]) -> bool {
    seen.get(148..)
        .unwrap_or_default()
        .iter()
        .take(8)
        .any(|&byte| !matches!(byte, b'0'..=b'7' | b' ' | 0))
}

// A cheap, non-authoritative admission check. Packet hashes remain the
// authority: this only decides whether the member is worth handing to the
// PAR2 object's authenticated packet scanner.
fn par2_header_looks_valid(prefix: &[u8], known_file_len: Option<u64>) -> bool {
    let Some(packet_len) = prefix
        .get(8..16)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u64::from_le_bytes)
    else {
        return false;
    };
    packet_len >= PAR2_PACKET_HEADER_BYTES as u64
        && packet_len.is_multiple_of(PAR2_PACKET_ALIGNMENT)
        && known_file_len.is_none_or(|file_len| packet_len <= file_len)
}

// Kinds by the extensions their files are posted under, for members whose
// bytes open no signature. A compound name such as `.tar.gz` is the signature's to decide; its
// object announces the `.tar` inside.
const NAMED: &[(Kind, &[&str])] = &[
    (Kind::Tar, &[".tar"]),
    (Kind::Bzip2, &[".bz2", ".tbz", ".tbz2", ".tar.bzip2"]),
    (Kind::Deflate, &[".deflate"]),
    (Kind::Brotli, &[".br"]),
    (Kind::Sfv, &[".sfv"]),
    (Kind::Zip, &[".zip"]),
    (Kind::Split, &[".001"]),
    (Kind::RarRecovery, &[".rev"]),
];

fn named(name: &str) -> Option<Kind> {
    // Posted names often trail quotes, underscores or dots past the extension.
    let name = name.trim_end_matches(|ch: char| {
        ch.is_ascii_whitespace() || matches!(ch, '"' | '\'' | '`' | '_' | '.')
    });
    NAMED
        .iter()
        .find(|(_, extensions)| {
            extensions
                .iter()
                .any(|extension| has_extension(name, extension))
        })
        .map(|(kind, _)| *kind)
}

fn has_extension(name: &str, extension: &str) -> bool {
    let name = name.as_bytes();
    name.len()
        .checked_sub(extension.len())
        .and_then(|start| name.get(start..))
        .is_some_and(|tail| tail.eq_ignore_ascii_case(extension.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;

    const RAR4: &[u8] = b"Rar!\x1A\x07\x00";
    const RAR5: &[u8] = b"Rar!\x1A\x07\x01\x00";
    const XZ: &[u8] = &[0xFD, 0x37, 0x7A, 0x58, 0x5A, 0x00, 0x00, 0x04];
    const PAR3: &[u8] = b"PAR3\0PKT\x40\x00\x00\x00";

    fn object(kind: Kind) -> Sniff {
        Sniff::Object(kind)
    }

    // A filler that opens no signature, tar's included once it reaches the
    // checksum field, which it fills with a byte no tar writes there.
    fn plain(len: usize) -> Vec<u8> {
        vec![b'x'; len]
    }

    // A tar header block with the ustar magic `terminator` writer puts there.
    fn ustar(terminator: u8) -> Vec<u8> {
        let mut header = vec![0_u8; 512];
        header[..11].copy_from_slice(b"episode.mkv");
        header[257..262].copy_from_slice(USTAR_MAGIC);
        header[262] = terminator;
        header
    }

    // A 7z signature header: magic, version 0.4, the start header's CRC, and
    // a start header of filler the CRC covers.
    fn seven_z() -> Vec<u8> {
        let mut bytes = SEVEN_Z_SIGNATURE.to_vec();
        bytes.extend_from_slice(&[0, 4]);
        bytes.resize(SEVEN_Z_SIGNATURE_HEADER_LEN, 0xAA);
        let crc = crc32(&bytes[12..]);
        bytes[8..12].copy_from_slice(&crc.to_le_bytes());
        bytes
    }

    // A PAR2 packet header stating `packet_len`, followed by nothing.
    fn par2(packet_len: u64) -> Vec<u8> {
        let mut bytes = PAR2_MAGIC.to_vec();
        bytes.extend_from_slice(&packet_len.to_le_bytes());
        bytes.resize(PAR2_PACKET_HEADER_BYTES, 0);
        bytes
    }

    #[test]
    fn rar4_signature() {
        assert_eq!(sniff("a.rar", RAR4, None), object(Kind::RarStore));
    }

    #[test]
    fn rar5_signature() {
        assert_eq!(sniff("a.part01.rar", RAR5, None), object(Kind::RarStore));
    }

    #[test]
    fn sevenz_signature_header() {
        assert_eq!(sniff("a.7z", &seven_z(), None), object(Kind::SevenZipStore));
    }

    #[test]
    fn sevenz_waits_for_its_whole_signature_header() {
        let head = seven_z();
        assert_eq!(sniff("a.7z", &head[..31], None), Sniff::Pending);
        assert_eq!(sniff("a.7z", &head[..31], Some(31)), Sniff::Leaf);
    }

    #[test]
    fn sevenz_start_header_failing_its_crc_is_leaf() {
        let mut head = seven_z();
        head[20] ^= 0x01;
        assert_eq!(sniff("a.7z", &head, Some(32)), Sniff::Leaf);
    }

    #[test]
    fn sevenz_of_an_unknown_major_version_is_leaf() {
        let mut head = seven_z();
        head[6] = 1;
        assert_eq!(sniff("a.7z", &head, Some(32)), Sniff::Leaf);
    }

    #[test]
    fn zip_local_header_empty_archive_and_spanned_marker() {
        for magic in [b"PK\x03\x04", b"PK\x05\x06", b"PK\x07\x08"] {
            assert_eq!(sniff("a.zip", magic, None), object(Kind::Zip));
        }
    }

    #[test]
    fn xz_signature() {
        assert_eq!(sniff("a.xz", XZ, None), object(Kind::Xz));
    }

    #[test]
    fn gzip_signature() {
        assert_eq!(
            sniff("a.gz", &[0x1F, 0x8B, 0x08, 0x00], None),
            object(Kind::Gzip)
        );
    }

    #[test]
    fn zstd_frame() {
        assert_eq!(
            sniff("a.zst", &[0x28, 0xB5, 0x2F, 0xFD], None),
            object(Kind::Zstd)
        );
    }

    #[test]
    fn zstd_skippable_frame_is_not_a_signature() {
        let mut bytes = vec![0x50, 0x2A, 0x4D, 0x18];
        bytes.extend(plain(600));
        assert_eq!(sniff("a.bin", &bytes, None), Sniff::Leaf);
    }

    #[test]
    fn par2_and_par3_signatures() {
        assert_eq!(sniff("a.par2", &par2(64), None), object(Kind::Par2));
        assert_eq!(sniff("a.vol00+01.par2", PAR3, None), object(Kind::Par3));
    }

    #[test]
    fn rar_recovery_volumes_by_signature_and_by_name() {
        let mut rev5 = b"Rar!\x1aRev".to_vec();
        rev5.extend(plain(64));
        assert_eq!(sniff("a.part1.rev", &rev5, None), object(Kind::RarRecovery));
        assert_eq!(sniff("9f3a7c1e", &rev5, None), object(Kind::RarRecovery));
        assert_eq!(sniff("a.rev", &plain(600), None), object(Kind::RarRecovery));
        assert_eq!(sniff("a.part1.rev", RAR5, None), object(Kind::RarStore));
    }

    #[test]
    fn par2_waits_for_its_whole_packet_header() {
        let header = par2(64);
        assert_eq!(sniff("a.par2", &header[..63], None), Sniff::Pending);
        assert_eq!(sniff("a.par2", &header[..63], Some(63)), Sniff::Leaf);
    }

    #[test]
    fn par2_packet_length_must_be_a_packet() {
        assert_eq!(sniff("a.par2", &par2(63), Some(64)), Sniff::Leaf);
        assert_eq!(sniff("a.par2", &par2(66), Some(66)), Sniff::Leaf);
        assert_eq!(sniff("a.par2", &par2(128), Some(100)), Sniff::Leaf);
        assert_eq!(sniff("a.par2", &par2(128), Some(128)), object(Kind::Par2));
    }

    #[test]
    fn par2_name_over_wrong_magic_is_leaf() {
        let mut bytes = b"PAR9\0PKT".to_vec();
        bytes.extend(plain(600));
        assert_eq!(sniff("a.par2", &bytes, None), Sniff::Leaf);
    }

    #[test]
    fn name_lies_about_bytes() {
        let mut not_sevenz = b"plain text, not an archive".to_vec();
        not_sevenz.extend(plain(600));
        assert_eq!(sniff("a.7z", &not_sevenz, None), Sniff::Leaf);
        assert_eq!(sniff("a.mkv", RAR5, None), object(Kind::RarStore));
        assert_eq!(
            sniff("a.nfo", &seven_z(), None),
            object(Kind::SevenZipStore)
        );
        assert_eq!(sniff("a.br", XZ, None), object(Kind::Xz));
    }

    #[test]
    fn too_few_bytes_is_pending() {
        assert_eq!(sniff("a.rar", b"Rar!", None), Sniff::Pending);
        assert_eq!(sniff("a", b"PK", None), Sniff::Pending);
        assert_eq!(sniff("a", b"", None), Sniff::Pending);
    }

    #[test]
    fn known_short_length_is_leaf() {
        assert_eq!(sniff("a.rar", b"Rar!", Some(4)), Sniff::Leaf);
        assert_eq!(sniff("a", b"PK", Some(3)), Sniff::Leaf);
        assert_eq!(sniff("a", b"", Some(0)), Sniff::Leaf);
        assert_eq!(sniff("a.bin", &plain(100), Some(100)), Sniff::Leaf);
    }

    #[test]
    fn any_member_might_be_a_tar_until_its_ustar_magic_is_due() {
        assert_eq!(sniff("a.bin", &[b'0'; 262], None), Sniff::Pending);
        assert_eq!(sniff("a.bin", &[b'0'; 263], None), Sniff::Leaf);
    }

    #[test]
    fn a_front_of_the_verdict_bytes_always_decides() {
        assert_eq!(VERDICT_BYTES, 263);
        assert_ne!(sniff("a.bin", &[b'0'; VERDICT_BYTES], None), Sniff::Pending);
    }

    #[test]
    fn a_stray_checksum_byte_rules_tar_out_early() {
        assert_eq!(sniff("a.bin", &plain(148), None), Sniff::Pending);
        assert_eq!(sniff("a.bin", &plain(149), None), Sniff::Leaf);
    }

    #[test]
    fn tar_by_its_ustar_magic() {
        for terminator in [0, b' '] {
            let header = ustar(terminator);
            assert_eq!(sniff("a.tar", &header, None), object(Kind::Tar));
            assert_eq!(sniff("a.tar", &header[..263], None), object(Kind::Tar));
        }
        assert_eq!(sniff("a.bin", &ustar(b'x'), None), Sniff::Leaf);
    }

    #[test]
    fn bzip2_signature_needs_a_level_digit() {
        assert_eq!(sniff("a.bz2", b"BZh9", None), object(Kind::Bzip2));
        assert_eq!(sniff("a.bin", b"BZh1", None), object(Kind::Bzip2));
        let mut zero_level = b"BZh0".to_vec();
        zero_level.extend(plain(600));
        assert_eq!(sniff("a.bin", &zero_level, None), Sniff::Leaf);
    }

    #[test]
    fn kinds_without_a_signature_by_name() {
        let bytes = plain(600);
        for (name, kind) in [
            ("a.tar", Kind::Tar),
            ("a.bz2", Kind::Bzip2),
            ("a.tar.bz2", Kind::Bzip2),
            ("a.tbz", Kind::Bzip2),
            ("a.tbz2", Kind::Bzip2),
            ("a.tar.bzip2", Kind::Bzip2),
            ("a.deflate", Kind::Deflate),
            ("a.br", Kind::Brotli),
            ("a.sfv", Kind::Sfv),
            ("A.SFV", Kind::Sfv),
        ] {
            assert_eq!(sniff(name, &bytes, None), object(kind), "{name}");
        }
        assert_eq!(sniff("a.bin", &bytes, None), Sniff::Leaf);
    }

    #[test]
    fn a_name_waits_for_bytes_that_might_be_a_signature() {
        assert_eq!(sniff("a.sfv", b"", None), Sniff::Pending);
        assert_eq!(sniff("a.br", &[0x1F, 0x8B], None), object(Kind::Gzip));
        assert_eq!(sniff("a.sfv", b"", Some(0)), object(Kind::Sfv));
    }

    #[test]
    fn hex_named_tar_and_bzip2_by_their_bytes() {
        assert_eq!(sniff("9f3a7c1e", &ustar(0), None), object(Kind::Tar));
        assert_eq!(sniff("9f3a7c1e", b"BZh9", None), object(Kind::Bzip2));
    }

    #[test]
    fn bytes_win_over_names() {
        assert_eq!(sniff("a.bz2", &ustar(0), None), object(Kind::Tar));
        assert_eq!(sniff("a.tar", b"BZh9", None), object(Kind::Bzip2));
        assert_eq!(sniff("a.tar", XZ, None), object(Kind::Xz));
    }

    #[test]
    fn sfv_by_name_whatever_its_bytes() {
        let text = b"; made by hand\r\nepisode.mkv 1a2b3c4d\r\n";
        let length = u64::try_from(text.len()).ok();
        assert_eq!(sniff("a.sfv", text, length), object(Kind::Sfv));
        assert_eq!(
            sniff("a.sfv", &[0xFF, 0xFE, 0x41], Some(3)),
            object(Kind::Sfv)
        );
    }

    // Damage at its front hides a zip's signature, not its end records.
    #[test]
    fn a_zip_whose_front_opens_no_signature_is_a_zip_by_name() {
        assert_eq!(sniff("a.zip", &[0; 300], Some(4096)), object(Kind::Zip));
        assert_eq!(sniff("a.zip", &[0; 300], None), object(Kind::Zip));
    }

    #[test]
    fn a_name_trailing_quotes_underscores_or_dots_keeps_its_extension() {
        let bytes = plain(600);
        assert_eq!(sniff("a.sfv\"", &bytes, None), object(Kind::Sfv));
        assert_eq!(sniff("a.tar_ ", &bytes, None), object(Kind::Tar));
        assert_eq!(sniff("a.br..", &bytes, None), object(Kind::Brotli));
    }

    #[test]
    fn sevenz_continuation_is_pending_then_leaf() {
        // Bytes that agree with the 7z signature so far keep it pending.
        assert_eq!(sniff("a.7z.002", &[0x37, 0x7A], None), Sniff::Pending);
        let mut continuation = vec![0x37, 0x7A, 0x00, 0x00, 0x00, 0x00];
        continuation.extend(plain(600));
        assert_eq!(sniff("a.7z.002", &continuation, None), Sniff::Leaf);
        assert_eq!(
            sniff("a.7z.001", &seven_z(), None),
            object(Kind::SevenZipStore)
        );
    }

    #[test]
    fn split_volume_names_are_just_their_format() {
        assert_eq!(sniff("a.r00", RAR4, None), object(Kind::RarStore));
        assert_eq!(sniff("a.PART02.RAR", RAR5, None), object(Kind::RarStore));
        assert_eq!(sniff("a.z01", b"PK\x07\x08", None), object(Kind::Zip));
        assert_eq!(sniff("a.z02", &plain(600), None), Sniff::Leaf);
    }

    #[test]
    fn never_an_engine() {
        let seven_z = seven_z();
        let par2 = par2(64);
        let samples: [&[u8]; 6] = [RAR4, RAR5, &seven_z, XZ, &par2, b"PK\x03\x04"];
        for bytes in samples {
            for name in ["a.rar", "a.7z", "a.deflate", "a.br", "a.sfv"] {
                if let Sniff::Object(kind) = sniff(name, bytes, None) {
                    assert!(!matches!(
                        kind,
                        Kind::RarEngine | Kind::SevenZipEngine | Kind::ZipEngine
                    ));
                    assert_ne!(kind, Kind::Deflate);
                }
            }
        }
    }
}
