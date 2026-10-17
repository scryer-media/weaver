// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The sniffer: which object owns a new member, judged from its name and its
// first bytes. It knows magic bytes and file types and nothing else. Sizes,
// volume numbering and set grouping belong to the objects it names.
//
// The bytes decide. A name only decides for the two kinds that have no magic
// at all (SFV and brotli), and only once the bytes have ruled out every kind
// that does.
//
// Minimum bytes before a verdict, per kind:
//
//   RAR4 7, RAR5 8, 7z 6, xz 6, PAR2 8, PAR3 8   their full signature
//   zip 4, zstd 4, bzip2 4 (`BZh` and a level)   their full signature
//   gzip 2                                       its full signature
//   tar 263 with the ustar magic at 257, else 512 for the header checksum
//   SFV 1, brotli 1                              any byte, once nothing else can match
//
// Each minimum is cut short by bytes that already contradict the kind, and
// by a known length too small to hold it.
//
// Engines (`RarEngine`, `SevenZipEngine`, `ZipEngine`) are never sniffed: an
// engine is chosen when a store object refuses, not by looking at bytes. Raw
// deflate is never sniffed either: it has no magic and only a zip object
// knows where it lies.

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
    let bytes = Bytes {
        seen: first_bytes,
        length,
    };
    let mut waiting = false;

    for magic in MAGICS {
        match magic.test(first_bytes) {
            Test::Full => return Sniff::Object(magic.kind),
            Test::Prefix => waiting |= bytes.can_reach(magic.pattern.len()),
            Test::No => {}
        }
    }

    match tar(&bytes) {
        Verdict::Yes => return Sniff::Object(Kind::Tar),
        Verdict::Undecided => waiting = true,
        Verdict::No => {}
    }

    // A name never outvotes bytes that might still turn out to be a magic.
    if waiting {
        return Sniff::Pending;
    }

    // Split and continuation volumes (`.r00`, `.part02.rar`, `.7z.002`,
    // `.z01`) get no help from their names here. Those that carry their
    // format's signature are claimed by it above; those that do not, such as
    // a `.7z.002` or a `.z02`, come out `Leaf` once their bytes rule every
    // magic out. That is deliberate: the format's object groups its volumes
    // and claims continuations by name itself, so the sniffer must not guess
    // at volume sets.
    if has_extension(name, ".sfv") {
        return by_name(&bytes, Kind::Sfv, is_text(&bytes));
    }
    if has_extension(name, ".br") {
        return by_name(&bytes, Kind::Brotli, true);
    }
    Sniff::Leaf
}

// What the caller has seen of the member so far, and how long it will be
// when that is known.
struct Bytes<'a> {
    seen: &'a [u8],
    length: Option<u64>,
}

impl Bytes<'_> {
    // Whether a kind needing `need` bytes can still be decided: the member is
    // long enough to hold them, or its length is not known yet.
    fn can_reach(&self, need: usize) -> bool {
        match (self.length, u64::try_from(need)) {
            (None, _) => true,
            (Some(length), Ok(need)) => length >= need,
            (Some(_), Err(_)) => false,
        }
    }

    // Whether every byte the member will ever have is already here.
    fn complete(&self) -> bool {
        match (self.length, u64::try_from(self.seen.len())) {
            (Some(length), Ok(seen)) => seen >= length,
            (Some(_), Err(_)) => true,
            (None, _) => false,
        }
    }
}

// A kind with no magic, decided by name once the bytes allow it. It needs at
// least one byte, because an empty member is no stream and no checksum list.
fn by_name(bytes: &Bytes<'_>, kind: Kind, fits: bool) -> Sniff {
    match (bytes.seen.is_empty(), bytes.complete()) {
        (true, true) => Sniff::Leaf,
        (true, false) => Sniff::Pending,
        (false, _) if fits => Sniff::Object(kind),
        (false, _) => Sniff::Leaf,
    }
}

// SFV is a text file. A multi-byte character cut off at the end of the bytes
// seen so far is still text, since the rest of it may simply not be here yet.
fn is_text(bytes: &Bytes<'_>) -> bool {
    if bytes.seen.contains(&0) {
        return false;
    }
    match std::str::from_utf8(bytes.seen) {
        Ok(_) => true,
        Err(error) => error.error_len().is_none() && !bytes.complete(),
    }
}

fn has_extension(name: &str, extension: &str) -> bool {
    let name = name.as_bytes();
    name.len()
        .checked_sub(extension.len())
        .and_then(|start| name.get(start..))
        .is_some_and(|tail| tail.eq_ignore_ascii_case(extension.as_bytes()))
}

// One position of a signature. Most are a fixed byte; zstd's skippable frames
// reserve a whole range of first bytes, and bzip2 ends in a level digit.
#[derive(Clone, Copy)]
enum Byte {
    Is(u8),
    Between(u8, u8),
}

impl Byte {
    fn admits(self, byte: u8) -> bool {
        match self {
            Byte::Is(expected) => byte == expected,
            Byte::Between(low, high) => (low..=high).contains(&byte),
        }
    }
}

struct Magic {
    kind: Kind,
    pattern: &'static [Byte],
}

enum Test {
    // The whole signature is present.
    Full,
    // Every byte seen so far agrees, but the signature is longer.
    Prefix,
    No,
}

impl Magic {
    fn test(&self, seen: &[u8]) -> Test {
        let agrees = self
            .pattern
            .iter()
            .zip(seen)
            .all(|(expected, &byte)| expected.admits(byte));
        match (agrees, seen.len() >= self.pattern.len()) {
            (false, _) => Test::No,
            (true, true) => Test::Full,
            (true, false) => Test::Prefix,
        }
    }
}

use Byte::{Between, Is};

// Every signature that sits at offset zero. No two of them share a prefix
// across kinds, so the order of the table never changes a verdict.
const MAGICS: &[Magic] = &[
    Magic {
        kind: Kind::RarStore,
        pattern: &[
            Is(b'R'),
            Is(b'a'),
            Is(b'r'),
            Is(b'!'),
            Is(0x1A),
            Is(0x07),
            Is(0x00),
        ],
    },
    Magic {
        kind: Kind::RarStore,
        pattern: &[
            Is(b'R'),
            Is(b'a'),
            Is(b'r'),
            Is(b'!'),
            Is(0x1A),
            Is(0x07),
            Is(0x01),
            Is(0x00),
        ],
    },
    Magic {
        kind: Kind::SevenZipStore,
        pattern: &[Is(0x37), Is(0x7A), Is(0xBC), Is(0xAF), Is(0x27), Is(0x1C)],
    },
    // A local file header, the end record alone of an empty archive, and the
    // marker that opens the first volume of a spanned archive.
    Magic {
        kind: Kind::Zip,
        pattern: &[Is(b'P'), Is(b'K'), Is(0x03), Is(0x04)],
    },
    Magic {
        kind: Kind::Zip,
        pattern: &[Is(b'P'), Is(b'K'), Is(0x05), Is(0x06)],
    },
    Magic {
        kind: Kind::Zip,
        pattern: &[Is(b'P'), Is(b'K'), Is(0x07), Is(0x08)],
    },
    Magic {
        kind: Kind::Xz,
        pattern: &[Is(0xFD), Is(0x37), Is(0x7A), Is(0x58), Is(0x5A), Is(0x00)],
    },
    Magic {
        kind: Kind::Gzip,
        pattern: &[Is(0x1F), Is(0x8B)],
    },
    Magic {
        kind: Kind::Bzip2,
        pattern: &[Is(b'B'), Is(b'Z'), Is(b'h'), Between(b'1', b'9')],
    },
    Magic {
        kind: Kind::Zstd,
        pattern: &[Is(0x28), Is(0xB5), Is(0x2F), Is(0xFD)],
    },
    // A stream may open with skippable frames before its first real one.
    Magic {
        kind: Kind::Zstd,
        pattern: &[Between(0x50, 0x5F), Is(0x2A), Is(0x4D), Is(0x18)],
    },
    Magic {
        kind: Kind::Par2,
        pattern: &[
            Is(b'P'),
            Is(b'A'),
            Is(b'R'),
            Is(b'2'),
            Is(0x00),
            Is(b'P'),
            Is(b'K'),
            Is(b'T'),
        ],
    },
    Magic {
        kind: Kind::Par3,
        pattern: &[
            Is(b'P'),
            Is(b'A'),
            Is(b'R'),
            Is(b'3'),
            Is(0x00),
            Is(b'P'),
            Is(b'K'),
            Is(b'T'),
        ],
    },
];

enum Verdict {
    Yes,
    No,
    Undecided,
}

const TAR_BLOCK: usize = 512;
const USTAR_MAGIC_AT: usize = 257;
const USTAR_MAGIC: &[u8] = b"ustar";
const CHECKSUM_FIELD: std::ops::Range<usize> = 148..156;

// Tar has no signature at offset zero, and pre-POSIX tars have none at all,
// so any bytes might open one until the header proves otherwise. That is why
// tar keeps short members pending even under names that say nothing of tar:
// obfuscated names are routine, and the bytes must decide.
fn tar(bytes: &Bytes<'_>) -> Verdict {
    let seen = bytes.seen;

    // POSIX writes "ustar\0", GNU writes "ustar " before its own version.
    let magic_end = USTAR_MAGIC_AT + USTAR_MAGIC.len();
    if let (Some(magic), Some(&terminator)) =
        (seen.get(USTAR_MAGIC_AT..magic_end), seen.get(magic_end))
        && magic == USTAR_MAGIC
        && matches!(terminator, 0 | b' ')
    {
        return Verdict::Yes;
    }

    // The checksum field is octal text in every tar dialect, so a stray byte
    // in it rules tar out long before the whole header has arrived.
    let checksum_seen = seen.get(CHECKSUM_FIELD.start..).unwrap_or_default();
    if checksum_seen
        .iter()
        .take(CHECKSUM_FIELD.len())
        .any(|&byte| !matches!(byte, b'0'..=b'7' | b' ' | 0))
    {
        return Verdict::No;
    }

    match seen.get(..TAR_BLOCK) {
        Some(header) if checksum_matches(header) => Verdict::Yes,
        Some(_) => Verdict::No,
        None if bytes.can_reach(TAR_BLOCK) => Verdict::Undecided,
        None => Verdict::No,
    }
}

// The header's checksum is the sum of its 512 bytes with the checksum field
// counted as spaces. POSIX sums unsigned bytes; some historic tars summed
// signed ones, and their archives are still about, so either is accepted.
fn checksum_matches(header: &[u8]) -> bool {
    let Some(stored) = header.get(CHECKSUM_FIELD).and_then(parse_octal) else {
        return false;
    };
    let mut unsigned: u64 = 0;
    let mut signed: i64 = 0;
    for (at, &byte) in header.iter().enumerate() {
        let byte = if CHECKSUM_FIELD.contains(&at) {
            b' '
        } else {
            byte
        };
        unsigned += u64::from(byte);
        signed += i64::from(i8::from_ne_bytes([byte]));
    }
    stored == unsigned || i64::try_from(stored).is_ok_and(|stored| stored == signed)
}

// Leading spaces, then octal digits, then only spaces and NULs.
fn parse_octal(field: &[u8]) -> Option<u64> {
    let digits_start = field.iter().position(|&byte| byte != b' ')?;
    let digits = field.get(digits_start..)?;
    let digits_len = digits
        .iter()
        .position(|byte| !byte.is_ascii_digit())
        .unwrap_or(digits.len());
    let (number, rest) = digits.split_at(digits_len);
    if number.is_empty() || rest.iter().any(|&byte| !matches!(byte, b' ' | 0)) {
        return None;
    }
    number.iter().try_fold(0_u64, |value, &digit| {
        let digit = match digit {
            b'0'..=b'7' => u64::from(digit - b'0'),
            _ => return None,
        };
        value.checked_mul(8)?.checked_add(digit)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const RAR4: &[u8] = b"Rar!\x1A\x07\x00";
    const RAR5: &[u8] = b"Rar!\x1A\x07\x01\x00";
    const SEVEN_Z: &[u8] = &[0x37, 0x7A, 0xBC, 0xAF, 0x27, 0x1C, 0x00, 0x04];
    const XZ: &[u8] = &[0xFD, 0x37, 0x7A, 0x58, 0x5A, 0x00, 0x00, 0x04];
    const PAR2: &[u8] = b"PAR2\0PKT\x40\x00\x00\x00";
    const PAR3: &[u8] = b"PAR3\0PKT\x40\x00\x00\x00";

    fn object(kind: Kind) -> Sniff {
        Sniff::Object(kind)
    }

    // A filler that is no magic and puts a non-octal byte in the tar checksum
    // field, so neither the magics nor tar keep it pending.
    fn plain(len: usize) -> Vec<u8> {
        vec![b'x'; len]
    }

    fn tar_header(with_magic: bool, checksum_ok: bool) -> Vec<u8> {
        let mut header = vec![0_u8; TAR_BLOCK];
        let fields: [(usize, &[u8]); 6] = [
            (0, b"episode.mkv"),
            (100, b"0000644\0"),
            (108, b"0001750\0"),
            (116, b"0001750\0"),
            (124, b"00000000012\0"),
            (136, b"14712345670\0"),
        ];
        for (at, value) in fields {
            header[at..at + value.len()].copy_from_slice(value);
        }
        header[156] = b'0';
        if with_magic {
            header[257..265].copy_from_slice(b"ustar\x0000");
        }
        header[CHECKSUM_FIELD].fill(b' ');
        let sum: u64 = header.iter().map(|&byte| u64::from(byte)).sum();
        let stored = if checksum_ok { sum } else { sum + 1 };
        let text = format!("{stored:06o}\0 ");
        header[CHECKSUM_FIELD].copy_from_slice(text.as_bytes());
        header
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
    fn sevenz_signature() {
        assert_eq!(sniff("a.7z", SEVEN_Z, None), object(Kind::SevenZipStore));
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
    fn bzip2_signature_needs_a_level_digit() {
        assert_eq!(sniff("a.bz2", b"BZh9", None), object(Kind::Bzip2));
        assert_eq!(sniff("a.bz2", b"BZh1", None), object(Kind::Bzip2));
        let mut zero_level = b"BZh0".to_vec();
        zero_level.extend(plain(600));
        assert_eq!(sniff("a.bz2", &zero_level, None), Sniff::Leaf);
    }

    #[test]
    fn zstd_frame_and_skippable_frame() {
        assert_eq!(
            sniff("a.zst", &[0x28, 0xB5, 0x2F, 0xFD], None),
            object(Kind::Zstd)
        );
        for first in [0x50, 0x57, 0x5F] {
            assert_eq!(
                sniff("a.zst", &[first, 0x2A, 0x4D, 0x18], None),
                object(Kind::Zstd)
            );
        }
    }

    #[test]
    fn par2_and_par3_signatures() {
        assert_eq!(sniff("a.par2", PAR2, None), object(Kind::Par2));
        assert_eq!(sniff("a.vol00+01.par2", PAR3, None), object(Kind::Par3));
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
        assert_eq!(sniff("a.nfo", SEVEN_Z, None), object(Kind::SevenZipStore));
        assert_eq!(sniff("a.br", XZ, None), object(Kind::Xz));
    }

    #[test]
    fn too_few_bytes_is_pending() {
        assert_eq!(sniff("a.rar", b"Rar!", None), Sniff::Pending);
        assert_eq!(sniff("a", b"PK", None), Sniff::Pending);
        assert_eq!(sniff("a", b"", None), Sniff::Pending);
        // No magic matches, but tar cannot be ruled out under 512 bytes.
        assert_eq!(sniff("a.bin", &[b'0'; 100], None), Sniff::Pending);
    }

    #[test]
    fn known_short_length_is_leaf() {
        assert_eq!(sniff("a.rar", b"Rar!", Some(4)), Sniff::Leaf);
        assert_eq!(sniff("a", b"PK", Some(3)), Sniff::Leaf);
        assert_eq!(sniff("a.bin", &[b'0'; 100], Some(100)), Sniff::Leaf);
        assert_eq!(sniff("a", b"", Some(0)), Sniff::Leaf);
    }

    #[test]
    fn a_stray_checksum_byte_rules_tar_out_early() {
        assert_eq!(sniff("a.bin", &plain(200), None), Sniff::Leaf);
    }

    #[test]
    fn tar_with_ustar_magic() {
        let header = tar_header(true, true);
        assert_eq!(sniff("a.tar", &header, None), object(Kind::Tar));
        assert_eq!(sniff("a.tar", &header[..263], None), object(Kind::Tar));
    }

    #[test]
    fn pre_posix_tar_by_checksum_alone() {
        let header = tar_header(false, true);
        assert_eq!(sniff("obfuscated", &header, None), object(Kind::Tar));
        assert_eq!(sniff("obfuscated", &header[..511], None), Sniff::Pending);
    }

    #[test]
    fn tar_with_wrong_checksum_is_leaf() {
        let header = tar_header(false, false);
        assert_eq!(sniff("a.tar", &header, None), Sniff::Leaf);
    }

    #[test]
    fn tar_checksum_summed_over_signed_bytes() {
        let mut header = tar_header(false, true);
        header[20] = 0xC8;
        header[CHECKSUM_FIELD].fill(b' ');
        let signed: i64 = header
            .iter()
            .map(|&byte| i64::from(i8::from_ne_bytes([byte])))
            .sum();
        let text = format!("{signed:06o}\0 ");
        header[CHECKSUM_FIELD].copy_from_slice(text.as_bytes());
        assert_eq!(sniff("a.tar", &header, None), object(Kind::Tar));
    }

    #[test]
    fn all_zero_block_is_not_tar() {
        assert_eq!(sniff("a.tar", &[0; TAR_BLOCK], None), Sniff::Leaf);
    }

    #[test]
    fn sfv_with_text() {
        let text = b"; made by hand\r\nepisode.mkv 1a2b3c4d\r\n";
        let length = u64::try_from(text.len()).ok();
        assert_eq!(sniff("a.sfv", text, length), object(Kind::Sfv));
        assert_eq!(sniff("A.SFV", text, length), object(Kind::Sfv));
        // Unknown length: the bytes could still be the start of a tar.
        assert_eq!(sniff("a.sfv", text, None), Sniff::Pending);
    }

    #[test]
    fn long_sfv_with_unknown_length() {
        let line = b"episode.part01.rar deadbeef\n";
        let text: Vec<u8> = line.iter().copied().cycle().take(300).collect();
        assert_eq!(sniff("a.sfv", &text, None), object(Kind::Sfv));
    }

    #[test]
    fn sfv_with_binary_is_leaf() {
        let mut bytes = b"episode.mkv\0\x01\x02".to_vec();
        bytes.extend(plain(600));
        assert_eq!(sniff("a.sfv", &bytes, None), Sniff::Leaf);
        assert_eq!(sniff("a.sfv", &[0xFF, 0xFE, 0x41], Some(3)), Sniff::Leaf);
    }

    #[test]
    fn sfv_cut_inside_a_character_is_still_text() {
        let mut text = b"\xC3\xA9pisode.mkv 1a2b3c4d\n".to_vec();
        text.extend(plain(300));
        text.push(0xC3);
        assert_eq!(sniff("a.sfv", &text, None), object(Kind::Sfv));
    }

    #[test]
    fn brotli_by_name_only_when_no_magic_matches() {
        let bytes = plain(600);
        assert_eq!(sniff("a.br", &bytes, None), object(Kind::Brotli));
        assert_eq!(sniff("A.BR", &bytes, None), object(Kind::Brotli));
        assert_eq!(sniff("a.bin", &bytes, None), Sniff::Leaf);
        assert_eq!(sniff("a.br", &[0x1F, 0x8B], None), object(Kind::Gzip));
    }

    #[test]
    fn sevenz_continuation_is_pending_then_leaf() {
        // Bytes that agree with the 7z signature so far keep it pending.
        assert_eq!(sniff("a.7z.002", &[0x37, 0x7A], None), Sniff::Pending);
        let mut continuation = vec![0x37, 0x7A, 0x00, 0x00, 0x00, 0x00];
        continuation.extend(plain(600));
        assert_eq!(sniff("a.7z.002", &continuation, None), Sniff::Leaf);
        assert_eq!(
            sniff("a.7z.001", SEVEN_Z, None),
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
    fn never_an_engine_or_deflate() {
        let samples: [&[u8]; 6] = [RAR4, RAR5, SEVEN_Z, XZ, PAR2, b"PK\x03\x04"];
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
