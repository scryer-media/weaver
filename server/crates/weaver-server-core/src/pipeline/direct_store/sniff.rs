//! Byte-sniffing an unclassified file's first bytes for a RAR volume head.
//!
//! The identity seam's second rung. The first rung binds by PAR2 fingerprint
//! and needs the recovery set's descriptions; a post with no PAR2 anywhere
//! has only one remaining source of set structure, and it is the volumes'
//! own headers: a RAR5 volume states in its main archive header whether it
//! belongs to a volume set and — past the first volume — which position it
//! holds. That header sits in the first few dozen bytes, so the same
//! offset-zero prefix the fingerprint rung hashes carries everything this
//! parser reads.
//!
//! Deliberately RAR5-only for set positions. RAR4 headers carry no volume
//! number, and the interior volumes of a stored RAR4 set are identical in
//! every header field that could place one — a measured property of the
//! format, not a parsing gap — so a RAR4 answer here is "it is RAR4",
//! which the caller declines to bind on and the conventional path owns.
//!
//! Everything here treats its input as hostile bytes an anonymous poster
//! chose: every read is bounds-checked, varints are length-capped, and any
//! malformation answers [`PrefixSniff::NotRar`] rather than guessing.

/// RAR5 signature: `Rar!\x1a\x07\x01\x00`.
const RAR5_SIGNATURE: [u8; 8] = *b"Rar!\x1a\x07\x01\x00";
/// RAR4 (1.5–4.x) signature: `Rar!\x1a\x07\x00`.
const RAR4_SIGNATURE: [u8; 7] = *b"Rar!\x1a\x07\x00";

/// Archive-flags bit: this archive is part of a volume set.
const RAR5_ARCHIVE_FLAG_VOLUME: u64 = 0x0001;
/// Archive-flags bit: a volume-number field follows (absent on the first
/// volume, whose number is zero by definition).
const RAR5_ARCHIVE_FLAG_VOLUME_NUMBER: u64 = 0x0002;
/// Header-flags bit: an extra-area size field follows.
const RAR5_HEADER_FLAG_EXTRA: u64 = 0x0001;
/// Header-flags bit: a data-area size field follows.
const RAR5_HEADER_FLAG_DATA: u64 = 0x0002;
/// Block type of the main archive header.
const RAR5_HEADER_TYPE_MAIN: u64 = 1;
/// Block type of the archive-encryption header a `-hp` archive opens with.
const RAR5_HEADER_TYPE_CRYPT: u64 = 4;

/// Ceiling on a declared volume number. Far above any real posting — the
/// largest sets in the field run to a few thousand volumes — and low enough
/// that a hostile header cannot make the caller book absurd positions.
const VOLUME_NUMBER_CEILING: u64 = 100_000;

/// What one offset-zero prefix says the file is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PrefixSniff {
    /// A readable RAR5 archive head.
    Rar5 {
        /// Declared volume position, zero for the first volume of a set and
        /// for a standalone archive.
        volume_number: u32,
        /// Whether the archive declares itself part of a volume set at all.
        /// A standalone archive is a set of one; the caller decides whether
        /// that is worth admitting.
        is_volume: bool,
    },
    /// A RAR5 archive whose headers are encrypted (`-hp`): nothing about the
    /// layout is readable without a key, volume position included.
    Rar5EncryptedHeaders,
    /// A RAR4-family archive. Recognized so the caller can say *why* it
    /// declines — the format states no position to bind on.
    Rar4,
    /// Not a RAR head, or one too malformed to trust.
    NotRar,
}

/// Reads one RAR5 variable-length integer: 7 bits per byte, low byte first,
/// high bit means another byte follows, at most ten bytes.
fn read_vint(bytes: &[u8], position: &mut usize) -> Option<u64> {
    let mut value: u64 = 0;
    for count in 0..10 {
        let byte = *bytes.get(*position)?;
        *position += 1;
        value |= u64::from(byte & 0x7F) << (7 * count);
        if byte & 0x80 == 0 {
            return Some(value);
        }
    }
    None
}

/// Classifies an offset-zero prefix. `prefix` is however much of the file's
/// first bytes the caller holds; a prefix long enough to carry the 16 KiB
/// fingerprint window is orders of magnitude longer than the main header
/// this walks.
pub(crate) fn sniff_rar_prefix(prefix: &[u8]) -> PrefixSniff {
    if prefix.starts_with(&RAR4_SIGNATURE) {
        return PrefixSniff::Rar4;
    }
    if !prefix.starts_with(&RAR5_SIGNATURE) {
        return PrefixSniff::NotRar;
    }
    let mut position = RAR5_SIGNATURE.len();
    // First block: [crc32][head_size][head_type][head_flags]...
    position += 4;
    let Some(_head_size) = read_vint(prefix, &mut position) else {
        return PrefixSniff::NotRar;
    };
    let Some(head_type) = read_vint(prefix, &mut position) else {
        return PrefixSniff::NotRar;
    };
    if head_type == RAR5_HEADER_TYPE_CRYPT {
        return PrefixSniff::Rar5EncryptedHeaders;
    }
    if head_type != RAR5_HEADER_TYPE_MAIN {
        return PrefixSniff::NotRar;
    }
    let Some(head_flags) = read_vint(prefix, &mut position) else {
        return PrefixSniff::NotRar;
    };
    if head_flags & RAR5_HEADER_FLAG_EXTRA != 0 && read_vint(prefix, &mut position).is_none() {
        return PrefixSniff::NotRar;
    }
    if head_flags & RAR5_HEADER_FLAG_DATA != 0 {
        // A main header carries no data area; a prefix claiming one is not
        // an archive head this parser should vouch for.
        return PrefixSniff::NotRar;
    }
    let Some(archive_flags) = read_vint(prefix, &mut position) else {
        return PrefixSniff::NotRar;
    };
    let is_volume = archive_flags & RAR5_ARCHIVE_FLAG_VOLUME != 0;
    let volume_number = if archive_flags & RAR5_ARCHIVE_FLAG_VOLUME_NUMBER != 0 {
        let Some(declared) = read_vint(prefix, &mut position) else {
            return PrefixSniff::NotRar;
        };
        if !is_volume || declared == 0 || declared > VOLUME_NUMBER_CEILING {
            // A number without the volume flag, an explicit zero (the first
            // volume states its position by omission), or an absurd claim:
            // each is a header no real writer produces.
            return PrefixSniff::NotRar;
        }
        declared as u32
    } else {
        0
    };
    PrefixSniff::Rar5 {
        volume_number,
        is_volume,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vint(mut value: u64) -> Vec<u8> {
        let mut out = Vec::new();
        loop {
            let byte = (value & 0x7F) as u8;
            value >>= 7;
            if value == 0 {
                out.push(byte);
                break;
            }
            out.push(byte | 0x80);
        }
        out
    }

    fn rar5_head(head_type: u64, archive_flags: u64, volume_number: Option<u64>) -> Vec<u8> {
        let mut body = vint(head_type);
        body.extend(vint(0)); // head_flags: no extra, no data
        body.extend(vint(archive_flags));
        if let Some(number) = volume_number {
            body.extend(vint(number));
        }
        let mut bytes = RAR5_SIGNATURE.to_vec();
        bytes.extend_from_slice(&[0xAA; 4]); // crc32: unchecked by the sniff
        bytes.extend(vint(body.len() as u64));
        bytes.extend(body);
        bytes
    }

    #[test]
    fn a_first_volume_sniffs_as_volume_zero() {
        assert_eq!(
            sniff_rar_prefix(&rar5_head(RAR5_HEADER_TYPE_MAIN, 0x0001, None)),
            PrefixSniff::Rar5 {
                volume_number: 0,
                is_volume: true
            }
        );
    }

    #[test]
    fn a_numbered_volume_sniffs_its_declared_position() {
        assert_eq!(
            sniff_rar_prefix(&rar5_head(RAR5_HEADER_TYPE_MAIN, 0x0003, Some(7))),
            PrefixSniff::Rar5 {
                volume_number: 7,
                is_volume: true
            }
        );
    }

    #[test]
    fn a_standalone_archive_sniffs_as_no_volume() {
        assert_eq!(
            sniff_rar_prefix(&rar5_head(RAR5_HEADER_TYPE_MAIN, 0, None)),
            PrefixSniff::Rar5 {
                volume_number: 0,
                is_volume: false
            }
        );
    }

    #[test]
    fn encrypted_headers_are_named_not_guessed() {
        assert_eq!(
            sniff_rar_prefix(&rar5_head(RAR5_HEADER_TYPE_CRYPT, 0, None)),
            PrefixSniff::Rar5EncryptedHeaders
        );
    }

    #[test]
    fn rar4_and_junk_are_refused() {
        let mut rar4 = RAR4_SIGNATURE.to_vec();
        rar4.extend_from_slice(&[0u8; 32]);
        assert_eq!(sniff_rar_prefix(&rar4), PrefixSniff::Rar4);
        assert_eq!(sniff_rar_prefix(b"not an archive"), PrefixSniff::NotRar);
        assert_eq!(sniff_rar_prefix(&RAR5_SIGNATURE), PrefixSniff::NotRar);
    }

    #[test]
    fn hostile_claims_are_refused() {
        // A declared zero, a number without the volume flag, and a number
        // past the ceiling: none is a header a real writer produces.
        assert_eq!(
            sniff_rar_prefix(&rar5_head(RAR5_HEADER_TYPE_MAIN, 0x0003, Some(0))),
            PrefixSniff::NotRar
        );
        assert_eq!(
            sniff_rar_prefix(&rar5_head(RAR5_HEADER_TYPE_MAIN, 0x0002, Some(3))),
            PrefixSniff::NotRar
        );
        assert_eq!(
            sniff_rar_prefix(&rar5_head(
                RAR5_HEADER_TYPE_MAIN,
                0x0003,
                Some(VOLUME_NUMBER_CEILING + 1)
            )),
            PrefixSniff::NotRar
        );
    }
}
